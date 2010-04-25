#!/usr/bin/ruby

require "rubygems"
require "active_record"
#require "/home/seiryo/work/cabinet/app/models/cabinet"

class FollotterDatabase

  yaml = YAML.load_file("/home/seiryo/work/follotter/follotter_account.yml")

  ActiveRecord::Base.establish_connection(
    :adapter  => "mysql",
    :host     => "192.168.11.148",
    :username => yaml["DB_USER"],
    :password => yaml["DB_PASSWORD"],
    :database => "follotter",
    :socket   => "/var/lib/mysql/mysql.sock",
    :encoding => "utf8"
  )  

  class User < ActiveRecord::Base
    has_many :friends
    has_many :followers
    has_many :friend_histories
    has_many :follower_histories
  end

  class Friend < ActiveRecord::Base
    belongs_to :user
  end

  class Follower < ActiveRecord::Base
    belongs_to :user
  end

  class Timeline < ActiveRecord::Base
    def self.select(user_id, target_id, action, batch_id)
      where = "user_id = ? AND target_id = ? AND action = ? AND batch_id = ?"
      Timeline.find(:first, :conditions => [where, user_id, target_id, action, batch_id]) 
    end
  end

  class Batch < ActiveRecord::Base
    has_many :friend_histories
    has_many :follower_histories
  end

  class FriendHistory < ActiveRecord::Base
    belongs_to :user
    belongs_to :batch
  end

  class FollowerHistory < ActiveRecord::Base
    belongs_to :user
    belongs_to :batch
  end


end

