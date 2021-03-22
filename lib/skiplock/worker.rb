module Skiplock
  class Worker < ActiveRecord::Base
    self.table_name = 'skiplock.workers'
  end
end