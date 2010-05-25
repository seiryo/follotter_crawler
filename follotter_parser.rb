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

class FollotterParser

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  def self.start
    Signal.trap('INT') { AMQP.stop{ EM.stop } }

    config  = YAML.load_file(@@CONFIG_FILE_PATH)

    host_mq = config['HOST_MQ']

    AMQP.start(:host => host_mq) do
      q = MQ.queue('parser')
      q.pop do |msg|
        unless msg
          EM.add_timer(1){ q.pop }
        else
          begin
            parser = FollotterParser.new(Marshal.load(msg), config)
            if parser.parse_fecth_result
              MQ.queue('updater').publish(Marshal.dump(parser.queue))
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
    @queue   = queue
    @host_mq = config['HOST_MQ']
  end

  def parse_fecth_result
    if "ids" == @queue[:api]
      return parse_ids
    elsif "statuses" == @queue[:api]
      return parse_statuses
    end
    raise
  end

  private

  def parse_statuses
    # JSONパース
    user_hash = Hash.new
    json      = JSON.parse(@queue[:fetch_result])
    @queue[:fetch_result] = nil
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
    @queue[:parse_result] = results_hash
    return true
  end

  # APIを叩いてフレンドなりフォロワーなりの配列を返す(再帰アリ)
  def parse_ids

    # JSONパース
    target_ids = []
    json = JSON.parse(@queue[:fetch_result])
    @queue[:fetch_result] = nil

    json_users = json['ids']
    json_users.each do |json_user|
      target_ids << json_user.to_i if json_user
    end
    return false unless target_ids.size > 0

    # 終了処理
    cursor = json['next_cursor']
    json            = nil
    json_user       = nil
    user_hash       = nil
    exist_user_hash = nil
    if cursor != nil && cursor != '' && cursor.to_s != '0' 
      @queue[:next_cursor] = cursor.to_s
    else
      @queue[:next_cursor] = '0'
    end
    return false unless 0 < target_ids.size
    @queue[:parse_result] = target_ids.flatten
    return true
  end

end

WEBrick::Daemon.start { 
  FollotterParser.start
}

