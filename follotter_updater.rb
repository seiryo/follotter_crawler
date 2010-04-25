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
include TokyoCabinet

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterUpdater < FollotterDatabase

  @@DIR_HOME  = "/home/seiryo/work/follotter/"
  @@DIR_QUEUE = @@DIR_HOME + "queue/updater/"
  @@DIR_TEMP  = @@DIR_HOME + "temp/updater/"
  @@DIR_NEXT  = @@DIR_HOME + "queue/broker/"

  def self.start(keyword)
    updater = FollotterUpdater.new(keyword)
    while true
      filename = updater.acquire_queue
      next unless filename
      begin
        updater.acquire_params(filename)
        updater.execute
      rescue => e
        begin
          f = File.open(@@DIR_HOME + "logs/updater_#{Time.now.strftime("%Y%m%d")}", "a")
          f.puts filename +":"+ e
          f.close
        end
      ensure
        File.unlink("#{@@DIR_TEMP}#{filename}")
      end
    end
  end

  def initialize(keyword)
    @created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    if keyword
      @@PIPE = "grep #{keyword.to_s} | head"
    else
      @@PIPE = "head"
    end
  end

  def acquire_params(filename)
    filenames = filename.split("_")
    raise unless 6 == filenames.size
    @yyyymmdd    = filenames.shift
    @api_section = filenames.shift
    @api_type    = filenames.shift
    @user_id     = filenames.shift
    @target_id   = filenames.shift
    @cursor      = filenames.shift
    @user_id     = @user_id.to_i
    @target_id   = @target_id.to_i
    @filename    = filename
  end

  def acquire_queue
    lists = `ls -1 #{@@DIR_QUEUE} | #{@@PIPE}`
    return false if "" == lists
    filename = lists.split("\n").first
    begin
      File.rename("#{@@DIR_QUEUE}#{filename}", "#{@@DIR_TEMP}#{filename}")
    rescue
      return false
    end
    return filename
  end

  def execute
    @created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")

    f = File.open("#{@@DIR_TEMP}#{@filename}")
    doc = f.read
    f.close

    @hdb = HDB::new
    if !@hdb.open(@@DIR_HOME + "users.tch", HDB::OWRITER | HDB::OCREAT)
      ecode = @hdb.ecode
      raise "HDB Open Error:" + @hdb.errmsg(ecode)
    end

    exception = nil
    begin
      update_ids(doc)      if ("ids" == @api_type && ("friends" == @api_section || "followers" == @api_section))
      update_statuses(doc) if ("statuses" == @api_section && ("friends" == @api_type || "followers" == @api_type))
    rescue => ex
      exception = ex
    ensure
      if !@hdb.close
        ecode = @hdb.ecode
        raise "HDB Close Error:" + @hdb.errmsg(ecode)
      end
    end

    raise exception if exception
    return
  end

  private

  def update_ids(doc)
    now_ids = Array.new
    doc.split(",").each { |target_id| now_ids << target_id.to_i }
    return unless 0 < now_ids.size
    relations =   Friend.find_all_by_user_id(@user_id) if "friends"   == @api_section
    relations = Follower.find_all_by_user_id(@user_id) if "followers" == @api_section

    before_ids = Array.new
    relations.each { |relation| before_ids << relation.target_id }
    newcomers = (now_ids - before_ids)

    newcomers.each do |target_id|
      #next if target_ids.index(target_id)
      f =   Friend.new if "friends"   == @api_section
      f = Follower.new if "followers" == @api_section
      f.user_id    = @user_id
      f.target_id  = target_id
      f.created_at = @created_at
      f.save
    end
    # HDB更新
    user_values = @hdb.get(@user_id)
    unless user_values
      raise "user_id is not found in HDB [#{@user_id.to_s}]"
    end
    user_values = user_values.split(":")
    raise "user_values's size is not _2_" unless 2 == user_values.size
    new_values = (now_ids | before_ids)
    new_values = new_values.join(",")
    new_values = [new_values, user_values[1]].join(":") if "friends"   == @api_section
    new_values = [user_values[0], new_values].join(":") if "followers" == @api_section
    @hdb.put(@user_id,  new_values)
  end

  def update_statuses(doc)
    users_hash = Marshal.load(doc)
    #known_user_hash = Hash.new
    #User.find(:all, :select => "id", :conditions => ["id IN (?)", users_hash.keys]).each do |u|
    #  known_user_hash[u.id] = u
    #end
    user = User.find_by_id(@user_id)
    return unless user

    user_values = @hdb.get(@user_id)
    unless user_values
      raise "user_id is not found in HDB [#{@user_id.to_s}]"
    end

    now_ids = Array.new
    users_hash.each do |user_id, user_hash|
      user_id = user_id.to_i
      next if @user_id == user_id

      result = @hdb.get(user_id)
      if result
        if "0:0" == result && user_hash[:is_crawl_target]
          queue_id = user_id
          _create_ids_queue("friends",   queue_id)
          _create_ids_queue("followers", queue_id)
          #_create_statuses_queue("friends",   queue_id)
          #_create_statuses_queue("followers", queue_id)
        elsif user_hash[:is_crawl_target]
          _create_statuses_queue("friends",   queue_id)
          _create_statuses_queue("followers", queue_id)
        end
        now_ids << user_id
        next
      end

      user = User.new
      #user = known_user_hash[user_id]      if known_user_hash.has_key?(user_id)
      user.id                = user_id #unless known_user_hash.has_key?(user_id)
      user.screen_name       = user_hash[:screen_name]       if user_hash[:screen_name]
      user.protected         = user_hash[:protected]         if user_hash[:protected]
      user.statuses_count    = user_hash[:statuses_count]    if user_hash[:statuses_count]
      user.profile_image_url = user_hash[:profile_image_url] if user_hash[:profile_image_url]
      user.last_posted_at    = DateTime.now
      if user.save
        @hdb.put(user_id, "0:0")
        now_ids << user_id
        next unless user_hash[:is_crawl_target]
        queue_id = user_id
        _create_ids_queue("friends",   queue_id)
        _create_ids_queue("followers", queue_id)
        #_create_statuses_queue("friends",   queue_id)
        #_create_statuses_queue("followers", queue_id)
      end
    end
    return unless 0 < now_ids.size

    user_values = user_values.split(":")
    raise "user_values's size is not _2_" unless 2 == user_values.size
    user_relations = user_values[0].split(",") if "friends"   == @api_type
    user_relations = user_values[1].split(",") if "followers" == @api_type

    before_ids = Array.new
    #_find_relations.each do |f|
    user_relations.each do |t_id|
      before_ids << t_id.to_i
    end

    # HDB更新
    new_values = (now_ids | before_ids)
    new_values = new_values.join(",")
    new_values = [new_values, user_values[1]].join(":") if "friends"   == @api_type
    new_values = [user_values[0], new_values].join(":") if "followers" == @api_type
    @hdb.put(@user_id,  new_values)

    welcome_ids = now_ids - before_ids
    goodbye_ids = before_ids - now_ids

    protected_flag = 0
    protected_flag = 1 if (1 >= before_ids.size || 0 == now_ids.size) 

    friend_values   = Array.new
    follower_values = Array.new
    timeline_values = Array.new
    welcome_ids.each do |target_id|
      #next if _find_user_relation(user.id, target_id)
      unless user_relations.index(target_id)
        friend_values, follower_values = _acquire_user_value(user.id, target_id, friend_values, follower_values)
        timeline_values << _acquire_timeline_value(user.id, target_id, protected_flag) if 0 == protected_flag
      end
=begin
      target_values = hdb.get(target_id)
      if target_values
        target_values = target_values.split(":")
        next unless 2 == target_values.size
        new_target_values = target_values[1].split(",") if "friends"   == @api_type
        new_target_values = target_values[0].split(",") if "followers" == @api_type
        next if new_target_values.index(@user_id)
        new_target_values << @user_id
        new_target_values =  new_target_values.join(",")
        new_target_values =  [target_values[0], new_target_values].join(":") if "friends"   == @api_type
        new_target_values =  [new_target_values, target_values[1]].join(":") if "followers" == @api_type
        hdb.put(target_id, new_target_values)
        #
        friend_values, follower_values = _acquire_target_value(user.id, target_id, friend_values, follower_values)
      end
=end
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

  def _create_statuses_queue(api_type, target_id)
    ymdhms = Time.now.strftime("%Y%m%d%H%M%S")
    ymdh   = Time.now.strftime("%Y%m%d%H")
    filename = [ymdhms, "statuses", api_type, target_id, @user_id, "-1"].join("_")
    f = File.open("#{@@DIR_NEXT}#{ymdh}", 'a')
    f.puts [@user_id.to_s + "statuses" + api_type, filename].join(",")
    f.close
    #File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
  end

  def _create_ids_queue(api_section, target_id)
    ymdhms = Time.now.strftime("%Y%m%d%H%M%S")
    ymdh   = Time.now.strftime("%Y%m%d%H")
    filename = [ymdhms, api_section, "ids", target_id, @user_id, "-1"].join("_")
    f = File.open("#{@@DIR_NEXT}#{ymdh}", 'a')
    f.puts [@user_id.to_s + api_section + "ids", filename].join(",")
    f.close
    #File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
  end


  def _acquire_timeline_value(user_id, target_id, is_protected)
    act = nil
    act = 0 if "friends"   == @api_type
    act = 1 if "followers" == @api_type
    raise   if nil == act
    return "(#{user_id.to_s}, #{target_id.to_s}, #{act.to_s}, '#{@created_at}', #{is_protected.to_s})"
    #return "(#{user_id.to_s}, #{target_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "friends"   == @api_type
    #return "(#{target_id.to_s}, #{user_id.to_s}, 0, '#{created_at}', #{is_protected.to_s})" if "followers" == @api_type
  end

  def _acquire_user_value(user_id, target_id, friend_values, follower_values)
    value = "(#{user_id.to_s}, #{target_id.to_s}, '#{@created_at}')"
    friend_values   << value if "friends"   == @api_type
    follower_values << value if "followers" == @api_type
    return friend_values, follower_values
  end

  def _acquire_target_value(user_id, target_id, friend_values, follower_values)
    value = "(#{target_id.to_s}, #{user_id.to_s})"
    follower_values << value if "friends"   == @api_type
    friend_values   << value if "followers" == @api_type
    return friend_values, follower_values
  end

end

WEBrick::Daemon.start { 
  FollotterUpdater.start(ARGV.shift)
}

