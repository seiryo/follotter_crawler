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
require 'webrick'
include TokyoCabinet

$:.unshift(File.dirname(__FILE__))
require 'follotter_database'

class FollotterSupporter < FollotterDatabase

  #@@DIR_QUEUE = "queue/updater/"
  @@DIR_HOME  = "/home/seiryo/work/follotter/"
  @@DIR_QUEUE = @@DIR_HOME + "queue/broker/"
  @@DIR_TEMP  = @@DIR_HOME + "temp/broker/"
  @@DIR_NEXT  = @@DIR_HOME + "queue/fetcher/"


  def self.start
    users = self.acquire_active_users
    self.update_last_posted_at(users)
    ["follotter_fetcher.rb", "follotter_parser.rb", "follotter_updater.rb"].each do |name|
      self.check_process(name)
    end
  end

  def self.check_process(name)
    count = `ps aux | grep #{name} | wc -l`
    count = count.chomp.to_i
    return if count > 0
    `ruby #{name}`
  end

  def self.update_last_posted_at(users)
    users.each do |user|
      user.last_posted_at = DateTime.now
      user.save
    end
  end

  def self.acquire_active_users
    url = "http://pcod.no-ip.org/yats/public_timeline?rss&json"
    doc = self._open_uri(url)
    res = JSON.parse(doc)
    screen_names = res.map{ |user| user["user"] }
    users = User.find(:all, :conditions => ["screen_name IN (?)", screen_names])
    return users.map{ |user| user.id }
  end

  private

  def self._open_uri(url)
    begin
      doc = open(url, :http_basic_authentication => [@API_USER, @API_PASSWORD]) do |f|
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

