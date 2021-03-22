module Skiplock
  class Dispatcher
    def initialize(master: true, worker_num: nil, worker_pids: [])
      @queues_order_query = Skiplock::Settings['queues'].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ')
      @executor = Concurrent::ThreadPoolExecutor.new(min_threads: Settings['min_threads'], max_threads: Settings['max_threads'], max_queue: Settings['max_threads'], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      @master = master
      if @master
        @worker_pids = worker_pids + [ Process.pid ]
      else
        @worker_num = worker_num
      end
      @next_schedule_at = Time.now.to_f
      @running = true
    end
    
    def run
      Thread.new do
        Rails.application.reloader.wrap do
          sleep(0.1) while @running && !Rails.application.initialized?
          Process.setproctitle("skiplock-#{@master ? 'master' : 'worker[' + @worker_num.to_s + ']'}") if Settings['workers'] > 0
          ActiveRecord::Base.connection_pool.with_connection do |connection|
            connection.exec_query('LISTEN "skiplock::jobs"')
            hostname = `hostname`.strip
            @worker = Worker.create!(pid: Process.pid, ppid: (@master ? nil : Process.ppid), capacity: Settings['max_threads'], hostname: hostname)
            if @master
              # get worker ids that were not shutdown properly on the host
              dead_worker_ids = Worker.where(hostname: hostname).where.not(pid: @worker_pids).ids
              if dead_worker_ids.count > 0
                # reset orphaned jobs of the dead worker ids for retry
                Job.where(running: true, worker_id: dead_worker_ids).update_all(running: false, worker_id: nil)
                Worker.where(id: dead_worker_ids).delete_all
              end
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
                    connection.exec_query('LISTEN "skiplock::jobs"')
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
                    op, id, worker_id, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                    next if op == 'DELETE' || running == 'true' || expired_at || finished_at
                    if scheduled_at.to_f <= Time.now.to_f
                      @next_schedule_at = Time.now.to_f
                    elsif scheduled_at.to_f < @next_schedule_at
                      @next_schedule_at = scheduled_at.to_f
                    end
                  end
                end
              rescue Exception => ex
                # TODO: Report exception
                error = true
                t = Time.now
                while @running
                  sleep(0.5)
                  break if Time.now - t > 5
                end              
              end
              sleep(0.1)
            end
            connection.exec_query('UNLISTEN *')
          end
        end
      end
    end

    def shutdown(wait: true)
      @running = false
      @executor.shutdown
      @executor.wait_for_termination if wait
      @worker.delete if @worker
    end

    private

    def do_work
      while @running
        result = Job.dispatch(queues_order_query: @queues_order_query, worker_id: @worker.id)
        next if result.is_a?(Hash)
        @next_schedule_at = result if result.is_a?(Float)
        break
      end
    rescue Exception => e
      puts "Exception => #{e.message}   #{e.backtrace}"
      # TODO: Report exception
    end
  end
end