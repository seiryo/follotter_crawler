require 'rubygems'
require 'open-uri'
require "time"
require "json"
require "pp"
require 'date'
require 'time'
require 'parsedate'
require 'yaml'
require 'webrick'
require 'amqp'
require 'mq'
require 'tokyotyrant'

include TokyoTyrant

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterUpdater < FollotterDatabase

  @@CONFIG_FILE_PATH = "/home/seiryo/work/follotter/follotter_config.yml"


  def self.start
    Signal.trap('INT') { AMQP.stop{ EM.stop } }

    config  = YAML.load_file(@@CONFIG_FILE_PATH)

    host_mq = config['HOST_MQ']

    AMQP.start(:host => host_mq) do
      q = MQ.queue('updater')
      q.pop do |msg|
        unless msg
          EM.add_timer(1){ q.pop }
        else
          begin
            updater = FollotterUpdater.new(Marshal.load(msg), config)
            unless updater.update_parse_result
              #フェッチキュー再発行
              MQ.queue('fetcher').publish(Marshal.dump(updater.queue))
            end
          rescue => ex
            pp ex
          end
          q.pop
        end
      end
    end
  end

  attr_reader :queue

  def initialize(queue, config)
    @queue         = queue
    @host_tt       = config["HOST_TT"]
    @created_at    = Time.now.strftime("%Y-%m-%d %H:%M:%S")
  end

  def update_parse_result
    @rdb = RDB::new
    if !@rdb.open(@host_tt, 1978)
      ecode = rdb.ecode
      raise "rdb open error" + @rdb.errmsg(ecode)
    end
    #@hdb = HDB::new
    #if !@hdb.open(@hdb_file_path, HDB::OWRITER | HDB::OCREAT)
    #  ecode = @hdb.ecode
    #  raise "HDB Open Error:" + @hdb.errmsg(ecode)
    #end

    result = false
    begin
      result = update_ids      if ("ids"      == @queue[:api])
      result = update_statuses if ("statuses" == @queue[:api])
    rescue => ex
      raise ex
    ensure
      if !@rdb.close
        ecode = @rdb.ecode
        raise "rdb close error" + @rdb.errmsg(ecode)
      end
      #if !@hdb.close
      #  ecode = @hdb.ecode
      #  raise "HDB Close Error:" + @hdb.errmsg(ecode)
      #end
    end

    return result
  end

  private

  def update_ids
    user = User.find_by_id(@queue[:user_id])
    return true unless user

    now_ids = @queue[:parse_result]
    now_ids = (now_ids | @queue[:new_relations])
    return true unless 0 < now_ids.size
    before_ids = @queue[:relations] 

    # next_cursorに従いfetch続行
    if '0' != @queue[:next_cursor]
      @queue[:url]  = "http://twitter.com/#{@queue[:target]}/#{@queue[:api]}.json"
      @queue[:url] += "?id=#{@queue[:user_id].to_s}&cursor=#{@queue[:next_cursor]}"
      @queue[:new_relations] = now_ids
      @queue[:parse_result]  = nil
      @queue[:next_cursor]   = nil
      return false
    end

    # relations登録処理
    if 0 == before_ids.size
      # 初回のみ登録処理を行う
      newcomers = (now_ids - before_ids)
      friend_values   = Array.new
      follower_values = Array.new
      newcomers.each do |target_id|
        next unless @queue[:user_id] != target_id
        friend_values, follower_values = _acquire_user_value(@queue[:user_id], target_id, friend_values, follower_values)
      end

      sql = "INSERT INTO friends   (user_id, target_id, created_at) VALUES " + friend_values.join(",")
      ActiveRecord::Base.connection.execute(sql) if 0 < friend_values.size

      sql = "INSERT INTO followers (user_id, target_id, created_at) VALUES " + follower_values.join(",")
      ActiveRecord::Base.connection.execute(sql) if 0 < follower_values.size

      _update_relation_count(user, newcomers.size)

      return true
    end
    # 2回目以降の処理はremoveのあぶり出し
    removed_user_ids = Array.new
    status_values    = Array.new
    (before_ids - now_ids).each do |target_id|
      next unless @queue[:user_id] != target_id
      table_name = @queue[:target] 
      next if ("friends" != table_name && "followers" != table_name)
      sql  = "UPDATE #{table_name} SET removed = 1 "
      sql += "WHERE user_id = #{@queue[:user_id].to_s} AND target_id = #{target_id.to_s}"
      ActiveRecord::Base.connection.execute(sql)
      removed_user_ids << target_id
      status_values    << _acquire_status_value(@queue[:user_id], target_id)
    end
    return true unless 0 < status_values.size

    sql = "INSERT INTO remove_statuses (user_id, target_id, action, created_at) VALUES " + status_values.join(",")
    ActiveRecord::Base.connection.execute(sql)

    sql  = "INSERT INTO remove_lines (user_id, target_ids, action, created_at) VALUES "
    sql += _acquire_status_value(@queue[:user_id], "'#{removed_user_ids.join(',')}'")
    ActiveRecord::Base.connection.execute(sql)

    return true
  end

  def update_statuses

    # パース結果(新リレーション)を取得
    users_hash = @queue[:parse_result]

    user = User.find_by_id(@queue[:user_id])
    return true unless user

    # 過去の対象ID配列を取得
    before_ids = @queue[:relations]

    # 現在の対象ID配列を取得
    now_ids = Array.new
    #users_hash.each do |user_id, user_hash|
    #end
    now_ids = users_hash.keys
    return true unless 0 < now_ids.size

    # 比較
    welcome_ids = Array.new
    ##goodbye_ids = before_ids - now_ids

    friend_values   = Array.new
    follower_values = Array.new
    status_values   = Array.new
    (now_ids - before_ids).each do |target_id|
      next unless @queue[:user_id] != target_id
      next unless users_hash[target_id]
      next unless _update_user(target_id, users_hash[target_id]) 
      #next if _find_user_relation(user.id, target_id)
      #unless user_relations.index(target_id)
      friend_values, follower_values = _acquire_user_value(@queue[:user_id], target_id, friend_values, follower_values)
      status_values << _acquire_status_value(@queue[:user_id], target_id)
      welcome_ids << target_id
    end

    sql = "INSERT INTO friends   (user_id, target_id, created_at) VALUES " + friend_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < friend_values.size

    sql = "INSERT INTO followers (user_id, target_id, created_at) VALUES " + follower_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < follower_values.size

    sql = "INSERT INTO follow_statuses (user_id, target_id, action, created_at) VALUES " + status_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < status_values.size

    if 0 < status_values.size
      sql  = "INSERT INTO follow_lines (user_id, target_ids, action, created_at) VALUES "
      sql += _acquire_status_value(@queue[:user_id], "'#{welcome_ids.join(',')}'")
      ActiveRecord::Base.connection.execute(sql)
    end

    if 0 < status_values.size
      _update_relation_count(user, (now_ids | before_ids).size)
    end

    return true
  end

  private

  def _update_relation_count(user, now_count)
    before_count = 0
    before_count = user.friends_count   if "friends"   == @queue[:target]
    before_count = user.followers_count if "followers" == @queue[:target]
    return false unless before_count != now_count
    user.friends_count   = now_count if "friends"   == @queue[:target]
    user.followers_count = now_count if "followers" == @queue[:target]
    return user.save
  end

  def _update_user(user_id, user_hash)
    user_id = user_id.to_i

    rdb_user = @rdb.get(user_id)
    if rdb_user
      # HDBに存在した場合、情報の更新を確認
      hu = Marshal.load(rdb_user)
      if (user_hash[:profile_image_url] != hu[:profile_image_url] ||
          user_hash[:statuses_count]    != hu[:statuses_count]    ||
          user_hash[:screen_name]       != hu[:screen_name])
        # 情報の更新があった場合
        update_user = User.find_by_id(user_id)
        return false unless update_user
        # MySQLを更新
        update_user.screen_name       = hu[:screen_name]       if user_hash[:screen_name]
        update_user.statuses_count    = hu[:statuses_count]    if user_hash[:statuses_count]
        update_user.profile_image_url = hu[:profile_image_url] if user_hash[:profile_image_url]
        return false unless update_user.save
        # HDBを更新
        hu[:screen_name]       = user_hash[:screen_name]
        hu[:statuses_count]    = user_hash[:statuses_count]
        hu[:profile_image_url] = user_hash[:profile_image_url]
        @rdb.put(user_id, Marshal.dump(hu))
      end 
      return true
    end
    # 未知のユーザの場合
    ## RDB挿入
    hu = { :screen_name       => user_hash[:screen_name],
           :statuses_count    => user_hash[:statuses_count],
           :profile_image_url => user_hash[:profile_image_url] }
    @rdb.put(user_id, Marshal.dump(hu))
    ## MySQL挿入
    new_user = User.new
    new_user.id                = user_id 
    new_user.screen_name       = user_hash[:screen_name]       if user_hash[:screen_name]
    new_user.protected         = user_hash[:protected]         if user_hash[:protected]
    new_user.statuses_count    = user_hash[:statuses_count]    if user_hash[:statuses_count]
    new_user.profile_image_url = user_hash[:profile_image_url] if user_hash[:profile_image_url]
    new_user.last_posted_at    = DateTime.now
    unless new_user.save
      return false
    end
    return true
  end

  def _acquire_status_value(user_id, target_id)
    act = nil
    act = 0 if "friends"   == @queue[:target]
    act = 1 if "followers" == @queue[:target]
    raise   if nil == act
    return "(#{user_id.to_s}, #{target_id.to_s}, #{act.to_s}, '#{@created_at}')"
    #return "(#{user_id.to_s}, #{target_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "friends"   == @api_type
    #return "(#{target_id.to_s}, #{user_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "followers" == @api_type
  end

  def _acquire_user_value(user_id, target_id, friend_values, follower_values)
    value = "(#{user_id.to_s}, #{target_id.to_s}, '#{@created_at}')"
    friend_values   << value if "friends"   == @queue[:target]
    follower_values << value if "followers" == @queue[:target]
    return friend_values, follower_values
  end

end

WEBrick::Daemon.start { 
  FollotterUpdater.start
}

