module Skiplock
  class Dispatcher
    def initialize(master: true)
	    @executor = Concurrent::ThreadPoolExecutor.new(min_threads: Settings['min_threads'], max_threads: Settings['max_threads'], max_queue: Settings['max_threads'], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      @master = master
      @next_schedule_at = Time.now.to_f
      @running = false
    end
    
    def run
      Thread.new do
        Rails.application.reloader.wrap do
          @running = true
          sleep(0.1) while @running && !Rails.application.initialized?
          ActiveRecord::Base.connection_pool.with_connection do |connection|
            connection.execute 'LISTEN skiplock'
            if @master
              # reset retries schedules on startup
              Job.where('scheduled_at > NOW() AND executions IS NOT NULL AND expired_at IS NULL AND finished_at IS NULL').update_all(scheduled_at: nil, updated_at: Time.now)
              Cron.setup
            end
            error = false
            while @running
              begin
                if error
                  unless connection.active?
                    connection.reconnect!
                    sleep(0.5)
                    connection.execute 'LISTEN skiplock'
                    @next_schedule_at = Time.now
                  end
                  error = false
                end
                if Time.now.to_f >= @next_schedule_at && @executor.remaining_capacity > 0
                  @executor.post { do_work }
                end
                notifications = []
                connection.raw_connection.wait_for_notify(0.1) do |channel, pid, payload|
                  notifications << payload if payload
                  loop do
                    payload = connection.raw_connection.notifies
                    break unless @running && payload
                    notifications << payload[:extra]
                  end
                  notifications.each do |n|
                    op, id, queue, priority, time = n.split(',')
                    if time.to_f <= Time.now.to_f
                      @next_schedule_at = Time.now.to_f
                    elsif time.to_f < @next_schedule_at
                      @next_schedule_at = time.to_f
                    end
                  end
                end
              rescue Exception => ex
                # TODO: Report exception
                error = true
                timestamp = Time.now
                while @running
                  sleep(0.5)
                  break if Time.now - timestamp > 10
                end              
              end
              sleep(0.1)
            end
            connection.execute "UNLISTEN *"
          end
        end
      end
    end

    def shutdown(wait: true)
      @running = false
      @executor.shutdown
      @executor.wait_for_termination if wait
    end

    private

    def do_work
      connection = ActiveRecord::Base.connection_pool.checkout
      while @running
        result = Job.dispatch(connection: connection)
        next if result.is_a?(Hash)
        @next_schedule_at = result if result.is_a?(Float)
        break
      end
    rescue Exception => e
      # TODO: Report exception
    ensure
      ActiveRecord::Base.connection_pool.checkin(connection)
    end
  end
end