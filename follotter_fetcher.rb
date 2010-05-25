require 'rubygems'
require 'open-uri'
require 'yaml'
require 'webrick'
require 'amqp'
require 'mq'
require 'pp'
require 'carrot'

class FollotterFetcher

  @@CONFIG_FILE_PATH = '/home/seiryo/work/follotter/follotter_config.yml'

  def self.start
    Signal.trap('INT') { Carrot.stop }

    config  = YAML.load_file(@@CONFIG_FILE_PATH)
    
    thread_limit = config['FETCH_THREAD_LIMIT'].to_i
    host_mq      = config['HOST_MQ']
    pp thread_limit
    pp host_mq
    carrot = Carrot.new(:host => host_mq )
    q = carrot.queue('fetcher')
    #pp carrot
    loop do
      msg = q.pop(:ack => false)
      unless msg
        sleep 1
        next
      else
        pp Marshal.load(msg)
        loop do
          break if thread_limit > Thread::list.size
          sleep 1
        end
        Thread.new(msg, config, host_mq) do |m, conf, host|
          fetcher = FollotterFetcher.new(Marshal.load(m), conf)
          if fetcher.fetch_api
            crrt = Carrot.new(:host => host )
            pp "ok"
            qq = crrt.queue('parser')
            qq.publish(Marshal.dump(fetcher.queue))
            crrt.stop
          end
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
