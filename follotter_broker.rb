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
require 'amqp'
require 'mq'

include TokyoCabinet

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterBroker < FollotterDatabase

  def self.start
    config = YAML.load_file("/home/seiryo/work/follotter/follotter_config.yml")
    @@API_USER        = config["API_USER"]
    @@API_PASSWORD    = config["API_PASSWORD"]
    @@HOST_MQ         = config["HOST_MQ"]
    @@HDB_FILE_PATH   = config["HDB_FILE_PATH"]
    @@QUEUE_FILE_PATH = config['QUEUE_COUNTER_FILE_PATH']

    ["follotter_fetcher.rb", "follotter_parser.rb", "follotter_updater.rb"].each do |name|
      self.check_process(name)
    end

    fetch_count, parse_count, update_count = self.acquire_queue_count(@@QUEUE_FILE_PATH)

    AMQP.start(:host => @@HOST_MQ) do

      batch = self.create_batch(fetch_count, parse_count, update_count)
      first_api_limit = batch.api_limit

      return if (0 != (fetch_count + update_count + update_count))
      #self.optimize_tch
      return unless 0 < batch.api_limit

      # アクティブユーザをクロールするためのキューを発行
      active_users = ActiveUser.find(:all, :order => 'updated DESC', :limit => batch.api_limit)
      active_users.each do |au|
        au_id = au.id
        au.destroy
        next unless User.find_by_id(au_id)
        queues = self.create_queues(au_id)
        queues.each do |queue|
          pp queue
          MQ.queue('fetcher').publish(Marshal.dump(queue))
          batch.api_limit -= 1
        end
      end

      # 発行したキューの数を記録し、保存
      batch.queue = first_api_limit - batch.api_limit
      batch.save

      # 終了
      AMQP.stop { EM.stop }
    end

  end

  def self.check_process(name)
    count = `ps aux | grep #{name} | grep -v grep | wc -l`
    count = count.chomp.to_i
    return if count > 0
    `ruby #{name}`
  end

  def self.create_queues(user_id, target = nil)
    unless target
      queues = Array.new
      ["friends", "followers"].each do |t|
        queues << self.create_queues(user_id, t)
      end
      return queues
    end

    queue = Hash.new
    queue[:user_id]   = user_id
    queue[:target]    = target
    queue[:relations] = self.acquire_relations(user_id, target)
    if queue[:relations]
      queue[:api] = "statuses"
      queue[:url] = "http://twitter.com/#{queue[:api]}/#{queue[:target]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
    else
      queue[:api] = "ids"
      queue[:url] = "http://twitter.com/#{queue[:target]}/#{queue[:api]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
    end
    return queue
  end

  def self.acquire_queue_count(queue_counter)
    results = `ruby #{queue_counter}`
    results = results.split("\n")
    pp results
    raise unless 3 == results.size
    fetch_count  = (results.shift).to_i
    parse_count  = (results.shift).to_i
    update_count = (results.shift).to_i
    return fetch_count, parse_count, update_count
  end

  def self.acquire_relations(user_id, target)
    if "friends" == target
      relations = Friend.find_all_by_user_id(user_id)
    elsif "followers" == target
      relations = Follower.find_all_by_user_id(user_id)
    else
      raise
    end
    return relations.map { |r| r.target_id }
  end

  def self.create_batch(fetch_count, parse_count, update_count)
    # 未クロールのアクティブユーザ数カウント
    begin
      finish_count = ActiveUser.count
    rescue
      finish_count = 0
    end
    # アクティブユーザが居ない場合は取得
    if 0 == finish_count
      `/bin/sh /home/seiryo/work/follotter/follotter_yats.sh`
      finish_count = ActiveUser.count
    end

    # API残り回数を取得
    api_limit = self.acquire_api_limit

    # 本バッチ情報を記録、作成
    batch = Batch.create(
              :api_limit => api_limit,
              :finisher  => finish_count,
              :fetcher   => fetch_count,
              :parser    => parse_count,
              :updater   => update_count)

    return batch
  end

  def self.acquire_api_limit
    url = "http://twitter.com/account/rate_limit_status.json"
    doc = self._open_uri(url)
    res = JSON.parse(doc)
    limit = res["remaining_hits"]
    return limit.to_i
  end

  def self.optimize_tch
    hdb = HDB::new
    if !hdb.open(@@HDB_FILE_PATH, HDB::OWRITER | HDB::OCREAT)
      ecode = hdb.ecode
      raise "HDB Open Error:" + @hdb.errmsg(ecode)
    end
    hdb.optimize
    hdb.close
  end

  private

  def self._open_uri(url)
    begin
      doc = open(url, :http_basic_authentication => [@@API_USER, @@API_PASSWORD]) do |f|
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

