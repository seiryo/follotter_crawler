require 'rubygems'
require 'open-uri'
require 'yaml'
require 'webrick'
require 'amqp'
require 'mq'
require 'pp'

class FollotterFetcher

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  def self.start
    Signal.trap('INT') { AMQP.stop{ EM.stop } }

    config  = YAML.load_file(@@CONFIG_FILE_PATH)
    
    thread_limit = config['FETCH_THREAD_LIMIT']
    host_mq      = config['HOST_MQ']
    pp thread_limit
    pp host_mq
    AMQP.start(:host => host_mq ) do
      q = MQ.queue('fetcher')
      q.pop do |msg|
        unless msg
          EM.add_timer(1){ q.pop }
        else
          loop do
            break if thread_limit > Thread::list.size
            sleep 1
          end
          Thread.new(msg, config) do |m, conf|
            fetcher = FollotterFetcher.new(Marshal.load(m), conf)
            if fetcher.fetch_api
              pp fetcher.queue
              MQ.queue('parser').publish(Marshal.dump(fetcher.queue))
            end
          end
          q.pop
        end
      end
    end
  end

  attr_reader :queue

  def initialize(queue, config)
    @user     = config['API_USER']
    @password = config['API_PASSWORD']
    @host_mq  = config['HOST_MQ']
    @queue    = queue
  end

  def fetch_api
    begin
      @queue[:fetch_result] = open(@queue[:url], :http_basic_authentication => [@user, @password]) do |f|
        f.read
      end
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
