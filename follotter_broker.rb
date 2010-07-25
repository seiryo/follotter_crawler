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
    @@STORE_API_LIMIT = config['STORE_API_LIMIT']
    @@ACTIVE_RATE     = config['ACTIVE_USER_FETCH_RATE']
    @@CONFIG          = config

    broke_count, fetch_count, parse_count, update_count = self.acquire_queue_count(@@QUEUE_FILE_PATH)

    batch = self.create_batch(broke_count, fetch_count, parse_count, update_count)
    first_api_limit = batch.api_limit
    # API制限を少し残しておく
    batch.api_limit -= @@STORE_API_LIMIT

    return if (0 != (fetch_count + update_count + update_count))
    #self.optimize_tch
    return unless 0 < batch.api_limit

    # RabbitMQ接続
    carrot = Carrot.new(:host => @@HOST_MQ)

    # updater(lookup)で出力されたキューを再発行
    if 0 < broke_count
      batch = self.enqueue_relation(carrot, batch)
    end
    # アクティブユーザをクロールするためのキューを発行
    if 0 < batch.api_limit
      batch = self.enqueue_lookup(carrot, batch)
    end

    # RabbitMQ切断
    carrot.stop

    # 発行したキューの数を記録し、保存
    batch.queue     = first_api_limit - batch.api_limit
    batch.api_limit = first_api_limit
    batch.save
    # 終了
  end

  def self.enqueue_lookup(carrot, batch)
    crawl_users = Array.new
    active_users = ActiveUser.find(:all, :order => 'updated DESC',
                                   :limit => @@ACTIVE_RATE * batch.api_limit)
    active_users.each do |au|
      au_id = au.id
      au.destroy
      user = User.find_by_id(au_id)
      unless user
        user    = User.new
        user.id = au_id.to_i
      end
      #next unless @@LOWER_LIMIT <= user.statuses_count

      crawl_users << user
      next if 100 > crawl_users.size

      queue = self.create_queue(crawl_users)
      qq = carrot.queue('fetcher')
      qq.publish(Marshal.dump(queue))
      batch.api_limit -= 1
      crawl_users = Array.new
    end
    if 0 < crawl_users.size
      queue = self.create_queue(crawl_users)
      qq = carrot.queue('fetcher')
      qq.publish(Marshal.dump(queue))
      batch.api_limit -= 1
    end
    return batch
  end

  def self.enqueue_relation(carrot, batch)
    broke = carrot.queue('broker')
    fetch = carrot.queue('fetcher')
    while batch.api_limit > 0
      msg = broke.pop(:ack => false)
      break unless msg
      queue = Marshal.load(msg)
      user = queue[:lookup_user]
      # 発言数が一定以下のユーザはスキップ
      next if @@LOWER_LIMIT > user.statuses_count
      fetch.publish(msg)
      batch.api_limit -= 1
    end
    return batch
  end

  def self.check_process(name)
    count = `ps aux | grep #{name} | grep -v grep | wc -l`
    count = count.chomp.to_i
    return if count > 0
    `ruby #{name}`
  end

  def self.create_queue(crawl_users)
    queue = Hash.new
    #queue[:user_id]   = false
    #queue[:target]    = false
    queue[:api] = "lookup"
    queue[:lookup_users_hash] = Hash.new
    queue[:lookup_relations]  = Hash.new

    crawl_users.each do |user|
      queue[:lookup_users_hash][user.id] = user if nil != user.screen_name
      queue[:lookup_relations][user.id]  = self.acquire_relations(user.id)
    end

    user_ids = crawl_users.map{ |u| u.id }
    queue[:url] = "http://api.twitter.com/1/users/lookup.json?user_id=#{user_ids.join(",")}"
    #queue[:new_relations] = []
    return queue
  end

  def self.acquire_queue_count(queue_counter)
    results = `ruby #{queue_counter}`
    results = results.split("\n")
    #pp results
    raise unless 4 == results.size
    broke_count  = (results.shift).to_i
    fetch_count  = (results.shift).to_i
    parse_count  = (results.shift).to_i
    update_count = (results.shift).to_i
    return broke_count, fetch_count, parse_count, update_count
  end

  def self.acquire_relations(user_id)
    relations = Hash.new
    relations[:normal] = Hash.new
    relations[:remove] = Hash.new
    relations[:normal][:friends]   = Array.new
    relations[:normal][:followers] = Array.new
    relations[:remove][:friends]   = Array.new
    relations[:remove][:followers] = Array.new

    Friend.find_all_by_user_id(user_id).each do |f|
      relations[:normal][:friends] << f.target_id if false == f.removed
      relations[:remove][:friends] << f.target_id if true  == f.removed
    end
    Follower.find_all_by_user_id(user_id).each do |f|
      relations[:normal][:followers] << f.target_id if false == f.removed
      relations[:remove][:followers] << f.target_id if true  == f.removed
    end
    return relations
  end

  def self.create_batch(broke_count, fetch_count, parse_count, update_count)
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
              :broker    => broke_count,
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

    url = "http://api.twitter.com/1/account/rate_limit_status.json"
    response = access_token.get(url)
    raise if 300 <= response.code.to_i
    res = JSON.parse(response.body)
    limit = res["remaining_hits"]
    return limit.to_i
  end

end

FollotterBroker.start

