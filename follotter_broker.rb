require 'rubygems'
require 'open-uri'
require "time"
require "json"
require "pp"
require 'date'
require 'time'
require 'parsedate'
require 'yaml'
require 'oauth'
require 'amqp'
require 'mq'
require 'carrot'

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
    @@LOWER_LIMIT     = config['STATUSES_LOWER_LIMIT']
    @@CONFIG          = config
    @@REMOVE_STATUS   = config['REMOVE_CRAWL_STATUS']

    ["follotter_fetcher.rb", "follotter_parser.rb", "follotter_updater.rb"].each do |name|
      self.check_process(name)
    end

    fetch_count, parse_count, update_count = self.acquire_queue_count(@@QUEUE_FILE_PATH)

    batch = self.create_batch(fetch_count, parse_count, update_count)
    first_api_limit = batch.api_limit

    return if (0 != (fetch_count + update_count + update_count))
    #self.optimize_tch
    return unless 0 < batch.api_limit

    carrot = Carrot.new(:host => @@HOST_MQ)
    # アクティブユーザをクロールするためのキューを発行
    active_users = ActiveUser.find(:all, :order => 'updated DESC', :limit => batch.api_limit / 2)
    active_users.each do |au|
      au_id = au.id
      au.destroy
      user = User.find_by_id(au_id)
      next unless user
      next unless @@LOWER_LIMIT <= user.statuses_count
      remove_flag = self.change_crawl_status_for_remove(user)
      queues = self.create_queues(user.id, remove_flag)
      queues.each do |queue|
        # pp queue
        qq = carrot.queue('fetcher')
        qq.publish(Marshal.dump(queue))
        batch.api_limit -= 1
      end
    end
    carrot.stop

    # 発行したキューの数を記録し、保存
    batch.queue = first_api_limit - batch.api_limit
    batch.save

    # 終了
  end

  def self.change_crawl_status_for_remove(user)
    if @@REMOVE_STATUS <= user.crawl_status
      user.crawl_status = 0
      user.save
      return true
    end
    user.crawl_status += 1
    user.save
    return false
  end

  def self.check_process(name)
    count = `ps aux | grep #{name} | grep -v grep | wc -l`
    count = count.chomp.to_i
    return if count > 0
    `ruby #{name}`
  end

  def self.create_queues(user_id, remove_flag, target = nil)
    unless target
      queues = Array.new
      ["friends", "followers"].each do |t|
        queues << self.create_queues(user_id, remove_flag, t)
      end
      return queues
    end

    queue = Hash.new
    queue[:user_id]   = user_id
    queue[:target]    = target
    queue[:relations] = self.acquire_relations(user_id, target)
    if (true == remove_flag || 0 == queue[:relations].size)
      queue[:api] = "ids"
      queue[:url] = "http://twitter.com/#{queue[:target]}/#{queue[:api]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
      queue[:new_relations] = []
    else
      queue[:api] = "statuses"
      queue[:url] = "http://twitter.com/#{queue[:api]}/#{queue[:target]}.json?id=#{queue[:user_id].to_s}&cursor=-1"
      queue[:new_relations] = []
    end
    #pp queue
    return queue
  end

  def self.acquire_queue_count(queue_counter)
    results = `ruby #{queue_counter}`
    results = results.split("\n")
    #pp results
    raise unless 3 == results.size
    fetch_count  = (results.shift).to_i
    parse_count  = (results.shift).to_i
    update_count = (results.shift).to_i
    return fetch_count, parse_count, update_count
  end

  def self.acquire_relations(user_id, target)
    if "friends" == target
      all_relations = Friend.find_all_by_user_id(user_id)
    elsif "followers" == target
      all_relations = Follower.find_all_by_user_id(user_id)
    else
      raise
    end
    relations = Array.new
    all_relations.each do |r|
      relations << r.target_id if 0 == r.removed
    end
    return relations
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
    consumer = OAuth::Consumer.new(
      @@CONFIG['CONSUMER_KEY'],
      @@CONFIG['CONSUMER_SECRET'],
      :site => 'http://twitter.com'
    )
    access_token = OAuth::AccessToken.new(
      consumer,
      @@CONFIG['ACCESS_TOKEN'],
      @@CONFIG['ACCESS_TOKEN_SECRET']
    )

    url = "http://twitter.com/account/rate_limit_status.json"
    response = access_token.get(url)
    res = JSON.parse(response.body)
    limit = res["remaining_hits"]
    return limit.to_i
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

