module Skiplock
  module Patch
    def enqueue(options = {})
      self.instance_variable_set('@skiplock_options', options)
      super
    end
  end
end
