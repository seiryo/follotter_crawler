require 'rubygems'
require 'yaml'
require 'amqp'
require 'mq'

config = YAML.load_file("/home/seiryo/work/follotter/follotter_config.yml")
AMQP.start(:host => config['HOST_MQ']) do
  MQ.queue("streamer").delete
  MQ.queue('streamer').publish(Marshal.dump("reset"))
  AMQP.stop { EM.stop }
end
  
