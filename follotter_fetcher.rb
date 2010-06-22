require 'rubygems'
require 'open-uri'
require 'yaml'
require 'webrick'
require 'oauth'
require 'amqp'
require 'mq'
require 'pp'
require 'carrot'

class FollotterFetcher

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  def self.start
    Signal.trap('INT') { Carrot.stop }

    # 設定ファイル読込
    config  = YAML.load_file(@@CONFIG_FILE_PATH)
    # 設定値読込 
    thread_limit = config['FETCH_THREAD_LIMIT'].to_i
    host_mq      = config['HOST_MQ']
    # OAUTH認証
    consumer = OAuth::Consumer.new(
      config['CONSUMER_KEY'],
      config['CONSUMER_SECRET'],
      :site => 'http://twitter.com'
    )
    access_token = OAuth::AccessToken.new(
      consumer,
      config['ACCESS_TOKEN'],
      config['ACCESS_TOKEN_SECRET']
    )
    # RabbitMQ接続
    carrot = Carrot.new(:host => host_mq )
    q = carrot.queue('fetcher')
    loop do
      msg = q.pop(:ack => false)
      unless msg
        sleep 1
        next
      else
        loop do
          break if thread_limit > Thread::list.size
          sleep 1
        end
        Thread.new(msg, config, host_mq, access_token) do |m, conf, host, atoken|
          fetcher = FollotterFetcher.new(Marshal.load(m), conf, atoken)
          if fetcher.fetch_api
            crrt = Carrot.new(:host => host )
            qq = crrt.queue('parser')
            qq.publish(Marshal.dump(fetcher.queue))
            crrt.stop
          end
        end
      end
    end
  end

  attr_reader :queue

  def initialize(queue, config, access_token)
    @user     = config['API_USER']
    @password = config['API_PASSWORD']
    @host_mq  = config['HOST_MQ']
    @queue        = queue
    @access_token = access_token 
  end

  def fetch_api
    begin
      @queue[:fetch_result] = @access_token.get(@queue[:url]).body
    rescue Timeout::Error => ex
      return false
    rescue OpenURI::HTTPError => ex
      return false
    rescue => ex
      return false
    end
    return false if "" == @queue[:fecth_result]
    return true
  end

end

WEBrick::Daemon.start { 
  FollotterFetcher.start
}
