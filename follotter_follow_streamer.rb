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

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterFollowStreamer < FollotterDatabase

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  def self.start
    name = Time.now.strftime("%Y%m%d")
    file = File.open("/tmp/ffs_#{name}",'w')
    file.puts Time.now.strftime("%Y-%m-%d %H:%M:%S")
    file.close

    # SYNC_FOLLOW_THRESHOLD
    config         = YAML.load_file(@@CONFIG_FILE_PATH)
    statuses_limit = config['STATUSES_LOWER_LIMIT']
    sync_threshold = config['SYNC_FOLLOW_THRESHOLD']

    stat = FollotterFollowStreamer.new(sync_threshold)

    result = stat.set_follow_statuses_hash
    return unless result

    last_id = User.find(:first, :order => "id DESC").id

    now_id = 0
    loop do
      break if last_id < now_id
      users = User.find(:all, :select => "id, friends_count, statuses_count",
                        :conditions => ["id > ? AND id <= ?", now_id, now_id + 100000])
      now_id += 100000
      next unless 0 < users.size

      users.each do |user|
        next unless 0 < user.friends_count
        next unless statuses_limit < user.statuses_count

        result = stat.set_friend_ids(user.id)
        next unless result

        sync_hash, other_hash = stat.get_relations_hash(user.id)
        next if (sync_hash.size == 0 && other_hash.size == 0)
        stat.create_follow_streams(user.id, sync_hash, other_hash)
      end
    end

    file = File.open("/tmp/ffs_#{name}",'a')
    file.puts Time.now.strftime("%Y-%m-%d %H:%M:%S")
    file.close
  end

  #attr_reader :follow_hash

  def initialize(sync_threshold)
    @today          = DateTime.now
    @yesterday      = @today - 1
    @sync_threshold = sync_threshold
  end

  def create_follow_streams(user_id, h_streams, v_streams)
    sql = "INSERT INTO follow_streams (user_id, target_ids, action, created_at) VALUES "
    insert_values = Array.new
    act = 0
    h_values = Array.new
    h_streams.each do |target_id, friend_ids|
      h_values << [friend_ids.join(','), target_id].join(':')
    end
    created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    insert_values << "(#{user_id.to_s}, '#{h_values.join('/')}', #{act}, '#{created_at}')" if 0 < h_values.size
    ###
    v_streams.each do |target_id, friend_ids|
      act            = friend_ids.size
      value          = [friend_ids.join(','), target_id].join(':')
      created_at     = Time.now.strftime("%Y-%m-%d %H:%M:%S")
      insert_values << "(#{user_id.to_s}, '#{value}', #{act}, '#{created_at}')"
    end
    sql += insert_values.join(",")
    ActiveRecord::Base.connection.execute(sql) if 0 < insert_values.size
  end

  def get_relations_hash(user_id)
    # 自分のfriends間のfollowing
    sync_hash  = Hash.new
    # その他のfollowing
    other_hash = Hash.new

    @friends_ids.each do |f_id|
      next unless @follow_hash.has_key?(f_id)
      @follow_hash[f_id].each do |target_id|
        unless nil ==  @friends_ids.index(target_id)
          # 自分のfriends間のfollowing
          sync_hash[target_id] = [] unless sync_hash.has_key?(target_id)
          sync_hash[target_id] << f_id
          next
        end
        # その他のfollowing
        other_hash[target_id] = [] unless other_hash.has_key?(target_id)
        other_hash[target_id] << f_id
        next
      end
    end
    true_other_hash = Hash.new
    other_hash.keys.each do |target_id|
      next unless @sync_threshold <= other_hash[target_id].size
      true_other_hash[target_id]   = other_hash[target_id]
    end
    return sync_hash, true_other_hash
  end

  def set_friend_ids(user_id)
    @friends_ids = Array.new
    Friend.find_all_by_user_id(user_id).each do |f|
      next unless false == f.removed
      @friends_ids << f.target_id
    end
    return 0 < @friends_ids.size
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

end

FollotterFollowStreamer.start
