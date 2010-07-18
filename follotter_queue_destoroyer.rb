require 'rubygems'
require 'yaml'
require 'amqp'
require 'mq'

config = YAML.load_file("/home/seiryo/work/follotter/follotter_config.yml")
AMQP.start(:host => config['HOST_MQ']) do
  MQ.queue("broker" ).delete
  MQ.queue("fetcher").delete
  MQ.queue("parser" ).delete
  MQ.queue("updater").delete
  AMQP.stop { EM.stop }
end
  
