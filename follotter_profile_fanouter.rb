require 'rubygems'
require 'yaml'
require 'amqp'
require 'mq'
require 'webrick'

def fanout
  config = YAML.load_file("/home/seiryo/work/follotter/follotter_config.yml")
  Signal.trap('INT') do
    puts "closing now..."
    AMQP.stop{ EM.stop }
    exit
  end
  AMQP.start(:host => config['HOST_MQ']) do
    q = MQ.queue('profiler')
    begin
      q.delete 
    rescue
    end
    q.pop do |msg|
      unless msg 
        EM.add_timer(1){ q.pop }
      else
        puts Marshal.load(msg)
        begin
          MQ.new.fanout('indexer').publish(msg)
        rescue
        end
        p.pop
      end
    end
  end
end

WEBrick::Daemon.start {
  fanout
}
  
