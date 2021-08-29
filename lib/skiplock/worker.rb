module Skiplock
  class Worker < ActiveRecord::Base
    self.implicit_order_column = 'created_at'
  end
end