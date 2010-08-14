require 'rubygems'
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
require 'tokyocabinet'

include TokyoCabinet

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterStreamer < FollotterDatabase

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  @@ok_count = 0
  @@ng_count = 0

  def self.start
    config           = YAML.load_file(@@CONFIG_FILE_PATH)

    sync_threshold   = config['SYNC_FOLLOW_THRESHOLD']
    stream_max_limit = config['FOLLOW_STREAM_MAX_LIMIT']
    stream_file_path = config['FOLLOW_STREAM_FILE_PATH']

    stat = FollotterStreamer.new(sync_threshold, stream_max_limit, stream_file_path)

    statuses_limit   = config['STATUSES_LOWER_LIMIT']
    host_mq          = config['HOST_MQ']

    result = stat.set_follow_statuses_hash
    #return unless result
    #stat.put_follow_statuses_hash

    AMQP.start(:host => host_mq) do
      q = MQ.queue('streamer')
      q.pop do |msg|
        unless msg 
          EM.add_timer(1){ q.pop }
        else
          message = Marshal.load(msg)
          if String == message.class  
            name = Time.now.strftime("%Y%m%d%H%M%S")
            file = File.open("/tmp/ffs_#{name}",'w')
            file.puts  "OK:#{@@ok_count.to_s}"
            file.puts  "NG:#{@@ng_count.to_s}"
            file.close
            @@ok_count = 0
            @@ng_count = 0
            stat = FollotterStreamer.new(sync_threshold, stream_file_path)
            stat.set_follow_statuses_hash
          else
            message[:lookup_users_hash].keys.each do |user_id|
              user = message[:lookup_users_hash][user_id]
              next unless user
              next unless 0 < user.friends_count
              next unless statuses_limit < user.statuses_count
              friend_ids = message[:lookup_relations][user.id][:normal][:friends]
              next unless stat.set_friend_ids(friend_ids)
              sync_hash, other_hash = stat.get_relations_hash(user.id)
              next if (sync_hash.size == 0 && other_hash.size == 0)
              stat.create_follow_streams(user.id, sync_hash, other_hash)      
            end
          end
          q.pop
        end 
      end 
    end 

  end

  #attr_reader :follow_hash

  def initialize(sync_threshold, stream_max_limit, stream_file_path)
    @today            = DateTime.now
    @yesterday        = @today - 1
    @sync_threshold   = sync_threshold
    @stream_max_limit = stream_max_limit
    @stream_file_path = stream_file_path
    batch = Batch.find(:first, :order => "id DESC",
                       :conditions => ["created_at < ? AND created_at > ? AND exception = ?",
                                       @today - Rational(1, 24),  @yesterday, "reset"])
    @yesterday = batch.created_at if batch
  end

  def create_follow_streams(user_id, h_streams, v_streams)
    sql = "INSERT INTO follow_streams (user_id, target_ids, action, created_at) VALUES "
    act = "0"
    h_values = Array.new
    h_streams.each do |target_id, friend_ids|
      ###h_values << [friend_ids.join(','), target_id].join(':')
    end
    created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    sql_value = "(#{user_id.to_s}, '#{h_values.join('/')}', #{act}, '#{created_at}')" if 0 < h_values.size
    begin
      ActiveRecord::Base.connection.execute(sql + sql_value) if 0 < h_values.size
      @@ok_count += 1
    rescue
      @@ng_count += 1
    end
    ###
    insert_values = Array.new
    v_streams.each do |target_id, friend_ids|
      act            = friend_ids.size.to_s
      value          = [friend_ids.join(','), target_id].join(':')
      created_at     = Time.now.strftime("%Y-%m-%d %H:%M:%S")
      insert_values << "(#{user_id.to_s}, '#{value}', #{act}, '#{created_at}')"
    end
    sql += insert_values.join(",")
    begin
      ActiveRecord::Base.connection.execute(sql) if 0 < insert_values.size
      @@ok_count += 1
    rescue
      @@ng_count += 1
    end
  end

  def get_relations_hash(user_id)
    # 自分のfriends間のfollowing
    sync_hash  = Hash.new
    # その他のfollowing
    other_hash = Hash.new

    @friends_ids.each do |f_id|
      next unless @follow_hash.has_key?(f_id)
      @follow_hash[f_id].each do |target_id|
        #unless nil ==  @friends_ids.index(target_id)
        #  # 自分のfriends間のfollowing
        #  sync_hash[target_id] = [] unless sync_hash.has_key?(target_id)
        #  sync_hash[target_id] << f_id
        #  next
        #end
        # その他のfollowing
        other_hash[target_id] = [] unless other_hash.has_key?(target_id)
        other_hash[target_id] << f_id
        next
      end
    end
    follow_counts = Array.new
    true_other_hash = Hash.new
    other_hash.keys.each do |target_id|
      uniq_ids = other_hash[target_id].uniq
      next unless @sync_threshold <= uniq_ids.size
      true_other_hash[target_id]   = uniq_ids
      follow_counts               << uniq_ids.size
    end

    #
    max_limit = @sync_threshold
    if true_other_hash.size > @stream_max_limit
      follow_counts = follow_counts.sort.reverse
      max_limit = follow_counts[@stream_max_limit - 1]
      true_other_hash.delete_if {|k, v| v.size < max_limit }
      if true_other_hash.size > @stream_max_limit
        true_other_hash.delete_if {|k, v| v.size <= max_limit }
      end
    end
    #

    return sync_hash, true_other_hash
  end

  def set_friend_ids(friend_ids)
    return false unless friend_ids
    @friends_ids = friend_ids
    return 0 < @friends_ids.size
    #Friend.find_all_by_user_id(user_id).each do |f|
    #  next unless false == f.removed
    #  @friends_ids << f.target_id
    #end
  end

  def set_follow_statuses_hash
    @follow_hash = Hash.new
    FollowStatus.find(:all,
      :conditions => ["created_at <= ? AND created_at > ?", @today, @yesterday]).each do |fs|
      next unless 0 == fs.action
      unless @follow_hash.has_key?(fs.user_id)
        @follow_hash[fs.user_id] = Array.new
      end
      @follow_hash[fs.user_id] << fs.target_id
    end
    return 0 < @follow_hash.size
  end

  def put_follow_statuses_hash
    hdb = HDB.new
    hdb.open(@stream_file_path + ".put.tmp", HDB::OWRITER | HDB::OCREAT)

    @follow_hash.each do |user_id, target_ids|
      hdb.put(user_id, Marshal.dump(target_ids))
    end
    @follow_hash.each do |user_id, target_ids|
      puts "ERROR" unless hdb.get(user_id)
    end

    hdb.close

    File.rename(@stream_file_path + ".put.tmp", @stream_file_path)
  end

end

WEBrick::Daemon.start {
  FollotterStreamer.start
}
