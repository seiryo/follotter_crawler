require 'rubygems'
require 'open-uri'
require "time"
require "json"
require "pp"
require 'date'
require 'time'
require 'parsedate'
require 'tokyocabinet'
require 'yaml'
require 'webrick'
require 'amqp'
require 'mq'

include TokyoCabinet

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterUpdater < FollotterDatabase

  @@CONFIG_FILE_PATH = "/home/seiryo/work/follotter/follotter_config.yml"


  def self.start
    Signal.trap('INT') { AMQP.stop{ EM.stop } }

    config  = YAML.load_file(@@CONFIG_FILE_PATH)

    host_mq = config['HOST_MQ']

    AMQP.start(:host => host_mq) do
      q = MQ.new.queue('updater')
      q.pop do |msg|
        unless msg
          EM.add_timer(1){ q.pop }
        else
          begin
            hash =  Marshal.load(msg)
            pp hash[:api]
            updater = FollotterUpdater.new(Marshal.load(msg), config)
            if updater.update_parse_result
            # 
            end
          rescue => ex
            #
          end
          q.pop
        end
      end
    end
  end


  def initialize(queue, config)
    @queue         = queue
    @hdb_file_path = config["HDB_FILE_PATH"]
    @created_at    = Time.now.strftime("%Y-%m-%d %H:%M:%S")
  end

  def update_parse_result
    @hdb = HDB::new
    if !@hdb.open(@hdb_file_path, HDB::OWRITER | HDB::OCREAT)
      ecode = @hdb.ecode
      raise "HDB Open Error:" + @hdb.errmsg(ecode)
    end

    begin
      update_ids      if ("ids"      == @queue[:api])
      update_statuses if ("statuses" == @queue[:api])
    rescue => ex
      raise ex
    ensure
      if !@hdb.close
        ecode = @hdb.ecode
        raise "HDB Close Error:" + @hdb.errmsg(ecode)
      end
    end

    return
  end

  private

  def update_ids
    now_ids = @queue[:parse_result]
    return unless 0 < now_ids.size
    before_ids = @queue[:relations] 

    newcomers = (now_ids - before_ids)
    newcomers.each do |target_id|
      #next if target_ids.index(target_id)
      f =   Friend.new if "friends"   == @queue[:target]
      f = Follower.new if "followers" == @queue[:target]
      f.user_id    = @queue[:user_id]
      f.target_id  = target_id
      f.created_at = @created_at
      f.save
    end
  end

  def update_statuses

    # パース結果(新リレーション)を取得
    users_hash = @queue[:parse_result]

    user = User.find_by_id(@queue[:user_id])
    return unless user

    # 過去の対象ID配列を取得
    before_ids = @queue[:relations]

    # 現在の対象ID配列を取得
    now_ids = Array.new
    users_hash.each do |user_id, user_hash|
      user_id = user_id.to_i
      next if @queue[:user_id] == user_id

      hdb_user = @hdb.get(user_id)
      # HDBに存在した場合、情報の更新を確認
      if hdb_user
        hu = Marshal.load(hdb_user)
        if (user_hash[:profile_image_url] != hu[:profile_image_url] ||
            user_hash[:screen_name] != hu[:screen_name])
          update_user = User.find_by_id(user_id)
          next unless update_user
          # MySQLを更新
          update_user.screen_name       = hu[:screen_name]       if user_hash[:screen_name]
          update_user.profile_image_url = hu[:profile_image_url] if user_hash[:profile_image_url]
          next unless update_user.save
          # HDBを更新
          hu[:screen_name]       = user_hash[:screen_name]
          hu[:profile_image_url] = user_hash[:profile_image_url]
          @hdb.put(user_id, Marshal.dump(hu))
        end 
        now_ids << user_id
        next
      end
      # 未知のユーザの場合
      user = User.new
      user.id                = user_id 
      user.screen_name       = user_hash[:screen_name]       if user_hash[:screen_name]
      user.protected         = user_hash[:protected]         if user_hash[:protected]
      user.statuses_count    = user_hash[:statuses_count]    if user_hash[:statuses_count]
      user.profile_image_url = user_hash[:profile_image_url] if user_hash[:profile_image_url]
      user.last_posted_at    = DateTime.now
      if user.save
        hu = { :screen_name       => user_hash[:screen_name],
               :profile_image_url => user_hash[:profile_image_url] }
        @hdb.put(user_id, Marshal.dump(hu))
        now_ids << user_id
        next
      end
    end
    return unless 0 < now_ids.size

    # 比較
    welcome_ids = now_ids - before_ids
    goodbye_ids = before_ids - now_ids

    protected_flag = 0
    protected_flag = 1 if (1 >= before_ids.size || 0 == now_ids.size) 

    friend_values   = Array.new
    follower_values = Array.new
    timeline_values = Array.new
    welcome_ids.each do |target_id|
      #next if _find_user_relation(user.id, target_id)
      #unless user_relations.index(target_id)
        friend_values, follower_values = _acquire_user_value(user.id, target_id, friend_values, follower_values)
        timeline_values << _acquire_timeline_value(user.id, target_id, protected_flag) if 0 == protected_flag
      #end
    end

    sql = "INSERT INTO friends   (user_id, target_id, created_at) VALUES " + friend_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < friend_values.size

    sql = "INSERT INTO followers (user_id, target_id, created_at) VALUES " + follower_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < follower_values.size

    sql = "INSERT INTO timelines (user_id, target_id, action, created_at, protected) VALUES " + timeline_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < timeline_values.size

    #queue_id = now_ids.sort_by{rand}.first
    #_create_statuses_queue("friends",   queue_id)
    #_create_statuses_queue("followers", queue_id)
  end

  private

  def _acquire_timeline_value(user_id, target_id, is_protected)
    act = nil
    act = 0 if "friends"   == @queue[:target]
    act = 1 if "followers" == @queue[:target]
    raise   if nil == act
    return "(#{user_id.to_s}, #{target_id.to_s}, #{act.to_s}, '#{@created_at}', #{is_protected.to_s})"
    #return "(#{user_id.to_s}, #{target_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "friends"   == @api_type
    #return "(#{target_id.to_s}, #{user_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "followers" == @api_type
  end

  def _acquire_user_value(user_id, target_id, friend_values, follower_values)
    value = "(#{user_id.to_s}, #{target_id.to_s}, '#{@created_at}')"
    friend_values   << value if "friends"   == @queue[:target]
    follower_values << value if "followers" == @queue[:target]
    return friend_values, follower_values
  end

  def _acquire_target_value(user_id, target_id, friend_values, follower_values)
    value = "(#{target_id.to_s}, #{user_id.to_s})"
    follower_values << value if "friends"   == @queue[:target]
    friend_values   << value if "followers" == @queue[:target]
    return friend_values, follower_values
  end

end

WEBrick::Daemon.start { 
  FollotterUpdater.start
}

