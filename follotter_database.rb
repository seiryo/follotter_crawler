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
  end

  class ActiveUser < ActiveRecord::Base
    set_table_name  "users_maxid"
    set_primary_key "internal_id"
  end

end

