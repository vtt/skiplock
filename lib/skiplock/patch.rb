module Skiplock
  module Patch
    def enqueue(options = {})
      @skiplock_options = options
      super
    end
  end
end
