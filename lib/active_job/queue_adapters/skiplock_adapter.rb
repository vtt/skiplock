module ActiveJob
  module QueueAdapters
    class SkiplockAdapter
      def initialize
        Skiplock::Manager.start
      end

      def enqueue(job)
        enqueue_at(job, nil)
      end

      def enqueue_at(job, timestamp)
        Skiplock::Job.enqueue_at(job, timestamp)
      end
    end
  end
end
