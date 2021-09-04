module ActiveJob
  module QueueAdapters
    class SkiplockAdapter
      def initialize
        Rails.application.config.after_initialize { Rails.application.config.skiplock = Skiplock::Manager.new }
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
