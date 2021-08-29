module Skiplock
  module Extension
    class Proxy < BasicObject
      def initialize(target, options = {})
        @target = target
        @options = options
      end

      def method_missing(name, *args)
        ProxyJob.set(@options).perform_later(::YAML.dump([ @target, name, args ]))
      end
    end

    class ProxyJob < ActiveJob::Base
      def perform(yml)
        target, method_name, args = ::YAML.load(yml)
        target.__send__(method_name, *args)
      end
    end

    def skiplock(options = {})
      Proxy.new(self, options)
    end
  end
end
