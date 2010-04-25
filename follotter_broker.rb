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

class FollotterBroker < FollotterDatabase

  #@@DIR_QUEUE = "queue/updater/"
  @@DIR_HOME  = "/home/seiryo/work/follotter/"
  @@DIR_QUEUE = @@DIR_HOME + "queue/broker/"
  @@DIR_TEMP  = @@DIR_HOME + "temp/broker/"
  @@DIR_NEXT  = @@DIR_HOME + "queue/fetcher/"

  def self.start
    self.acquire_active_users

    @@limit = self.acquire_api_limit
    return unless 0 < @@limit

    #begin
    #  f = File.open(@@DIR_HOME + "temp/results/updater_#{Time.now.strftime("%Y%m%d")}", "r")
    #  exception = f.read
    #  f.close
    #  f = File.open(@@DIR_HOME + "temp/results/updater_#{Time.now.strftime("%Y%m%d")}", "w+")
    #  f.close
    #rescue
    #  exception = nil
    #end

    fetch_count, parse_count, update_count = self.acquire_queue_count
    batch = Batch.create(
              :api_limit => @@limit,
              :fetcher   => fetch_count,
              :parser    => parse_count,
              :updater   => update_count)
    return if (0 != (fetch_count + update_count + update_count))

    self.optimize_tch

    ids = self.acquire_friends_ids(4016161)
    ids = [4016161, ids].flatten
    ids.each { |user_id| self.create_queue(user_id) }

    self.broke_fetch_queue

    batch.queue = batch.api_limit - @@limit
    batch.save
  end

  def self.create_queue(user_id)
    ymdhms = Time.now.strftime("%Y%m%d%H%M%S")
    filenames = Array.new

    api = "friends"
    if Friend.find_all_by_user_id(user_id)  
      filenames << [ymdhms, "statuses", api, user_id, 1, -1].join("_")
    else
      filenames << [ymdhms, api, "ids", user_id, 1, -1].join("_")
    end
    api = "followers"
    if Follower.find_all_by_user_id(user_id)  
      filenames << [ymdhms, "statuses", api, user_id, 1, -1].join("_")
    else
      filenames << [ymdhms, api, "ids", user_id, 1, -1].join("_")
    end

    filenames.each do |filename|
      file = File.open("#{@@DIR_TEMP}#{filename}", 'w')
      file.close
      begin
        File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
      rescue
        File.unlink("#{@@DIR_TEMP}#{filename}")
      end
      @@limit -= 1
    end
  end

  def self.acquire_queue_count
    fetch_count  = `ls -1 #{@@DIR_HOME}queue/fetcher/ | wc -l`
    fetch_count  = fetch_count.chomp.to_i
    parse_count  = `ls -1 #{@@DIR_HOME}queue/parser/  | wc -l`
    parse_count  = parse_count.chomp.to_i
    update_count = `ls -1 #{@@DIR_HOME}queue/updater/ | wc -l`
    update_count = update_count.chomp.to_i
    return fetch_count, parse_count, update_count
  end

  def self.broke_fetch_queue 
    ymdh = Time.now.strftime("%Y%m%d%H")
    return unless File.exist?("#{@@DIR_QUEUE}#{ymdh}")
    File.rename("#{@@DIR_QUEUE}#{ymdh}", "#{@@DIR_TEMP}#{ymdh}")
    filename_hash = Hash.new
    f = File.open("#{@@DIR_TEMP}#{ymdh}", 'r')
    while line = f.gets
      line.chomp!
      lines = line.split(',')
      next unless 2 == lines.size
      user_key = lines.shift
      filename = lines.shift
      filename_hash[user_key] = filename
      @@limit -= 1
      break if @@limit <= 0
    end
    filename_hash.each do |user_key, filename|
      f = File.open("#{@@DIR_TEMP}#{filename}", 'w')
      f.close
      File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
    end
    File.unlink("#{@@DIR_TEMP}#{ymdh}")
  end

  def self.acquire_friends_ids(user_id, cursor = "-1")
    # 初期処理
    url =  "http://twitter.com/friends/ids.json"
    url += "?id=#{user_id.to_s}&cursor=#{cursor}"
    doc = ""
    # APIを叩く
    doc = self._open_uri(url)
    # JSONパース
    target_ids = [user_id]
    json       = JSON.parse(doc)
    doc        = nil
    json_users = json["ids"]
    json_users.each do |json_user|
      target_ids << json_user.to_i if json_user
    end
    return target_ids
  end

  def self.acquire_api_limit
    url = "http://twitter.com/account/rate_limit_status.json"
    doc = self._open_uri(url)
    res = JSON.parse(doc)
    limit = res["remaining_hits"]
    return limit.to_i
  end

  def self.acquire_active_users
    url = "http://pcod.no-ip.org/yats/public_timeline?rss&json"
    doc = self._open_uri(url)
    res = JSON.parse(doc)
    screen_names = res.map{ |user| user["user"] }
    users = User.find(:all, :conditions => ["screen_name IN (?)", screen_names])
    return users.map{ |user| user.id }
  end

  def self.optimize_tch
    hdb = HDB::new
    if !hdb.open(@@DIR_HOME + "users.tch", HDB::OWRITER | HDB::OCREAT)
      ecode = hdb.ecode
      raise "HDB Open Error:" + @hdb.errmsg(ecode)
    end
    hdb.optimize
    hdb.close
  end

  private

  def self._open_uri(url)
    begin
      doc = open(url, :http_basic_authentication => [@API_USER, @API_PASSWORD]) do |f|
        f.read
      end
    rescue => ex
      raise ex
    rescue Timeout::Error => ex
      raise ex
    rescue OpenURI::HTTPError => ex
      raise ex
    end
    return doc
  end
end

FollotterBroker.start

