module Skiplock
  class Counter < ActiveRecord::Base
    self.implicit_order_column = 'day'
  end
end