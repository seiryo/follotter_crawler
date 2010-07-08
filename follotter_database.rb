#!/usr/bin/ruby

require "rubygems"
require "active_record"
require "date"

class FollotterDatabase

  config = YAML.load_file("/home/seiryo/work/follotter/follotter_config.yml")

  ActiveRecord::Base.establish_connection(
    :adapter   => "mysql",
    :host      => config["HOST_DB"],
    :username  => config["DB_USER"],
    :password  => config["DB_PASSWORD"],
    :database  => "follotter",
    :socket    => "/var/lib/mysql/mysql.sock",
    :encoding  => "utf8",
    :reconnect => true
  )  

  class User < ActiveRecord::Base
    has_many :friends
    has_many :followers

    def self.judge_changing(new_user, user_hash)
      return ( new_user.screen_name       != user_hash[:screen_name]       ||
               new_user.statuses_count    != user_hash[:statuses_count]    ||
               new_user.profile_image_url != user_hash[:profile_image_url] ||
               new_user.friends_count     != user_hash[:friends_count]     ||
               new_user.followers_count   != user_hash[:followers_count]   ||
               new_user.url               != user_hash[:url]               ||
               new_user.location          != user_hash[:location]          ||
               new_user.description       != user_hash[:description])
    end

    def self.set_user_hash(new_user, user_hash)
      new_user.screen_name       = user_hash[:screen_name]       if user_hash[:screen_name]
      new_user.protected         = user_hash[:protected]         if user_hash[:protected]
      new_user.statuses_count    = user_hash[:statuses_count]    if user_hash[:statuses_count]
      new_user.profile_image_url = user_hash[:profile_image_url] if user_hash[:profile_image_url]
      new_user.friends_count     = user_hash[:friends_count]     if user_hash[:friends_count]
      new_user.followers_count   = user_hash[:followers_count]   if user_hash[:followers_count]
      new_user.url               = user_hash[:url]               if user_hash[:url]
      new_user.location          = user_hash[:location]          if user_hash[:location]
      new_user.description       = user_hash[:description]       if user_hash[:description]
      return new_user 
    end

  end

  class Friend < ActiveRecord::Base
    belongs_to :user
  end

  class Follower < ActiveRecord::Base
    belongs_to :user
  end

  class FollowStatus < ActiveRecord::Base
  end

  class FollowLine < ActiveRecord::Base
  end

  class RemoveStatus < ActiveRecord::Base
  end

  class RemoveLine < ActiveRecord::Base
  end

  class Batch < ActiveRecord::Base
  end

  class ActiveUser < ActiveRecord::Base
    set_table_name  "users_maxid"
    set_primary_key "internal_id"
  end

end

