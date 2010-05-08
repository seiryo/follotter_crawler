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

class FollotterParser

  @@DIR_HOME  = "/home/seiryo/work/follotter/"
  @@DIR_QUEUE = @@DIR_HOME + "queue/parser/"
  @@DIR_TEMP  = @@DIR_HOME + "temp/parser/"
  @@DIR_NEXT  = @@DIR_HOME + "queue/updater/"

  def self.start
    fetcher = FollotterParser.new
    while true
      filename = fetcher.acquire_queue
      next unless filename
      begin
        # 
        fetcher.acquire_params(filename)
        fetcher.execute
        File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
      rescue => e
        begin
          f = File.open(@@DIR_HOME + "logs/parser_#{Time.now.strftime("%Y%m%d")}", "a")
          f.puts filename +":"+ e
          f.close
        end
      #ensure
        File.unlink("#{@@DIR_TEMP}#{filename}")
      end
    end
  end

  def initialize
  end

  def acquire_params(filename)
    filenames = filename.split("_")
    raise "filename format" unless 6 == filenames.size
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
    lists = `ls -1 #{@@DIR_QUEUE}`
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
    f = open("#{@@DIR_TEMP}#{@filename}")
    doc = f.read
    f.close
    if ("ids" == @api_type && ("friends" == @api_section || "followers" == @api_section))
      results = parse_ids(doc)
      raise "ids result" unless results
      result = results.join(",")
      file   = File.open("#{@@DIR_TEMP}#{@filename}", 'w')
      file.puts(result)
      file.close
      return
    end
    if ("statuses" == @api_section && ("friends" == @api_type || "followers" == @api_type))
      results = parse_statuses(doc)
      raise "statuses result" unless results
      f = File.open("#{@@DIR_TEMP}#{@filename}", 'w')
      f.puts Marshal.dump(results)
      f.close
      #YAML.dump( results, File.open("#{@@DIR_TEMP}#{@filename}", 'w') )
      return
    end

    raise
  end

  def parse_statuses(doc)
    # JSONパース
    user_hash = Hash.new
    json      = JSON.parse(doc)
    doc       = nil
    json["users"].each do |json_user|
      next unless json_user["id"]
      user_hash[json_user["id"].to_i] = json_user
    end
    return false unless user_hash.size > 0

    results_hash = Hash.new
    user_hash.keys.each do |target_id|
      new_user                     = Hash.new
      new_user[:id]                = target_id
      new_user[:screen_name]       = user_hash[target_id]["screen_name"]
      new_user[:protected]         = user_hash[target_id]["protected"]
      new_user[:statuses_count]    = user_hash[target_id]["statuses_count"]
      new_user[:profile_image_url] = user_hash[target_id]["profile_image_url"]
      #new_user["last_posted_at"]    = DateTime.now
      new_user[:is_crawl_target]   = true
      # 
      results_hash[target_id] = new_user
    end
    #cursor = json["next_cursor"]
    return false unless 0 < results_hash.size
    return results_hash
  end

  # APIを叩いてフレンドなりフォロワーなりの配列を返す(再帰アリ)
  def parse_ids(doc)

    # JSONパース
    target_ids = []
    json       = JSON.parse(doc)
    doc        = nil
    json_users = json["ids"]
    json_users.each do |json_user|
      target_ids << json_user.to_i if json_user
    end
    return false unless target_ids.size > 0

    # 終了処理
    cursor = json["next_cursor"]
    json            = nil
    json_user       = nil
    user_hash       = nil
    exist_user_hash = nil
    if cursor != nil && cursor != "" && cursor.to_s != "0"
      #再帰
      #現在は再帰的に取らないのでコメントアウト
      #results << self.acquire(user, api, cursor)
    end
    return false unless 0 < target_ids.size
    return target_ids.flatten
  end

end

WEBrick::Daemon.start { 
  FollotterParser.start
}

