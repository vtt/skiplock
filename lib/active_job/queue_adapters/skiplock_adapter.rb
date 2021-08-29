module ActiveJob
  module QueueAdapters
    class SkiplockAdapter
      def initialize
        Rails.application.config.after_initialize { Skiplock::Manager.new }
      end

      def enqueue(job)
        Skiplock::Job.enqueue(job)
      end

      def enqueue_at(job, timestamp)
        Skiplock::Job.enqueue_at(job, timestamp)
      end
    end
  end
end
