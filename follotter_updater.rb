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
              if "lookup" == updater.queue[:api]
                updater.queue[:next_queues].each do |next_queue|
                  MQ.queue('broker').publish(Marshal.dump(next_queue))
                end
              else
                MQ.queue('fetcher').publish(Marshal.dump(updater.queue))
              end
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
    #@rdb = RDB::new
    #if !@rdb.open(@host_tt, 1978)
    #  ecode = rdb.ecode
    #  raise "rdb open error" + @rdb.errmsg(ecode)
    #end

    result = false
    begin
      result = update_ids      if ("ids"      == @queue[:api])
      result = update_statuses if ("statuses" == @queue[:api])
      result = update_lookup   if ("lookup" == @queue[:api])
    rescue => ex
      raise ex
    ensure
      #if !@rdb.close
      #  ecode = @rdb.ecode
      #  raise "rdb close error" + @rdb.errmsg(ecode)
      #end
    end

    return result
  end

  private

  def update_lookup
    @queue[:next_queues] = Array.new
    users_hash = @queue[:parse_result]
    users_hash.each do |user_id, user_hash|
      user_id = user_id.to_i
      next unless @queue[:lookup_users_hash].has_key?(user_id)
      #ユーザ情報更新

      ## MySQL更新
      update_user = @queue[:lookup_users_hash][user_id]
      next unless User.judge_changing(update_user, user_hash)
      lookup_values = _acquire_lookup_value(update_user, user_hash)
      if 0 < lookup_values.size
        sql  = "INSERT INTO follow_lines (user_id, target_ids, action, created_at) VALUES "
        sql += lookup_values.join(",")
        ActiveRecord::Base.connection.execute(sql)
      end
      update_user = User.set_user_hash(update_user, user_hash) 
      update_user.save
      ## RDB更新
      #hu = { :screen_name       => user_hash[:screen_name],
      #       :statuses_count    => user_hash[:statuses_count],
      #       :profile_image_url => user_hash[:profile_image_url] }
      #@rdb.put(user_id, Marshal.dump(hu))

      _acquire_next_crawl_hash(@queue[:lookup_relations][user_id][:normal], user_hash).each do |target, api|
        queue                    = Hash.new
        queue[:user_id]          = user_id
        queue[:target]           = target.to_s
        queue[:api]              = api
        queue[:relations]        = @queue[:lookup_relations][user_id][:normal][target]
        queue[:remove_relations] = @queue[:lookup_relations][user_id][:remove][target]
        queue[:new_relations]    = []
        if    ("ids" == api)
          queue[:url] = "http://api.twitter.com/1/#{queue[:target]}/#{queue[:api]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
        elsif ("statuses" == api)
          queue[:url] = "http://api.twitter.com/1/#{queue[:api]}/#{queue[:target]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
        end 
        @queue[:next_queues] << queue
      end
    end
    return true if 0 == @queue[:next_queues].size
    return false
  end

  def _acquire_lookup_value(user, user_hash)
    return_values = Array.new
    value_hash    = Hash.new
    user_hash.each do |key, value|
      before_value = User.get_param_from_key(user, key) 
      next if (nil == value || nil == before_value)
      next if (value == before_value)
      act = nil
      act = 9 if :screen_name       == key
      act = 8 if :name              == key
      act = 7 if :location          == key
      act = 6 if :description       == key
      act = 5 if :profile_image_url == key
      act = 4 if :url               == key
      next    if nil == act
      target_ids = [value, before_value].join("\t\n")
      return_values << "(#{user.id.to_s}, #{target_ids}, #{act.to_s}, '#{@created_at}')"
    end
    return return_values
  end

  def _acquire_next_crawl_hash(lookup_relations, user_hash)
    next_hash = Hash.new
    if    (lookup_relations[:friends].size < user_hash[:friends_count].to_i)
      next_hash[:friends] = "ids"
    elsif (lookup_relations[:friends].size > user_hash[:friends_count].to_i)
      next_hash[:friends] = "statuses"
    end
    if    (lookup_relations[:followers].size < user_hash[:friends_count].to_i)
      next_hash[:followers] = "ids"
    elsif (lookup_relations[:followers].size > user_hash[:friends_count].to_i)
      next_hash[:followers] = "statuses"
    end
    return next_hash
  end

  def update_ids
    user = User.find_by_id(@queue[:user_id])
    return true unless user

    now_ids = @queue[:parse_result]
    now_ids = (now_ids | @queue[:new_relations])
    return true unless 0 < now_ids.size
    before_ids = @queue[:relations] 

    # next_cursorに従いfetch続行
    if '0' != @queue[:next_cursor]
      @queue[:url]  = "http://api.twitter.com/1/#{@queue[:target]}/#{@queue[:api]}.json"
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

      #_update_relation_count(user, newcomers.size)

      return true
    end
    # 2回目以降の処理はremoveのあぶり出し
    removed_user_ids = Array.new
    status_values    = Array.new
    (before_ids - now_ids).each do |target_id|
      next unless @queue[:user_id] != target_id
      table_name = @queue[:target] 
      next if ("friends" != table_name && "followers" != table_name)
      next if nil != @queue[:remove_relations].index(target_id)
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

    # 新参UserHash取得
    newcomer_ids  = (now_ids - before_ids)
    newcomer_hash = User.acquire_users_hash(newcomer_ids)
    newcomer_ids.each do |target_id|
      next unless @queue[:user_id] != target_id
      next unless users_hash[target_id]
      next unless _update_user(target_id, users_hash[target_id], newcomer_hash[target_id]) 
      if nil != @queue[:remove_relations].index(target_id)
        sql  = "UPDATE #{table_name} SET removed = 0 "
        sql += "WHERE user_id = #{@queue[:user_id].to_s} AND target_id = #{target_id.to_s}"
        status_values << _acquire_status_value(@queue[:user_id], target_id)
        next
      end
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
      #_update_relation_count(user, (now_ids | before_ids).size)
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

  def _update_user(user_id, user_hash, user_model)
    user_id = user_id.to_i

    if user_model
      return true
    end
    # 未知のユーザの場合
    ## MySQL挿入
    new_user                = User.new
    new_user.id             = user_id
    new_user                = User.set_user_hash(new_user, user_hash) 
    new_user.last_posted_at = DateTime.now
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

