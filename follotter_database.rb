#!/usr/bin/ruby

require "rubygems"
require "active_record"

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

