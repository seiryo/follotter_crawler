require 'rubygems'
require 'open-uri'
require "json"
require "pp"
require 'date'
require 'time'
require 'parsedate'
require 'tokyocabinet'
require 'yaml'
require 'webrick'

class FollotterFetcher

  @@DIR_HOME  = "/home/seiryo/work/follotter/"
  @@DIR_QUEUE = @@DIR_HOME + "queue/fetcher/"
  @@DIR_TEMP  = @@DIR_HOME + "temp/fetcher/"
  @@DIR_NEXT  = @@DIR_HOME + "queue/parser/"

  def self.start(keyword)
    fetcher = FollotterFetcher.new(keyword)
    while true
      filename = fetcher.acquire_queue
      next unless filename
      begin
        fetcher.acquire_params(filename)
        # 
        result = fetcher.execute
        file = File.open("#{@@DIR_TEMP}#{filename}", 'w')
        file.puts(result)
        file.close
        File.rename("#{@@DIR_TEMP}#{filename}", "#{@@DIR_NEXT}#{filename}")
      rescue => e
        begin
          f = File.open(@@DIR_HOME + "logs/fetcher_#{Time.now.strftime("%Y%m%d")}", "a")
          f.puts filename +":"+ e
          f.close
        end
        File.unlink("#{@@DIR_TEMP}#{filename}")
      end
    end
  end

  def initialize(keyword)
    if keyword
      @@PIPE = "grep #{keyword.to_s} | head"
    else
      @@PIPE = "head"
    end
    account = YAML.load_file("/home/seiryo/work/follotter/follotter_account.yml")
    @API_USER     = account["API_USER"]
    @API_PASSWORD = account["API_PASSWORD"]
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
    lists = `ls -1 #{@@DIR_QUEUE} | #{@@PIPE}`
    return false if "" == lists
    return false if  0 == lists.size
    filename = lists.split("\n").first
    begin
      File.rename("#{@@DIR_QUEUE}#{filename}", "#{@@DIR_TEMP}#{filename}")
    rescue
      return false
    end
    return filename
  end

  def execute 
    if ("ids" == @api_type && ("friends" == @api_section || "followers" == @api_section))
      result = fetch_api
      raise "execute ids result" unless result
      return result
    end
    if ("statuses" == @api_section && ("friends" == @api_type || "followers" == @api_type))
      result = fetch_api
      raise "execute ids result" unless result
      return result
    end
    raise "?"
  end

  # APIを叩いてフレンドなりフォロワーなりの配列を返す(再帰アリ)
  def fetch_api
    # 初期処理
    url =  "http://twitter.com/#{@api_section}/#{@api_type}.json"
    url += "?id=#{@user_id.to_s}&cursor=#{@cursor}"
    doc = ""
    # APIを叩く
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
    return false if "" == doc
    return doc
  end

end

WEBrick::Daemon.start { 
  FollotterFetcher.start(ARGV.shift)
}
