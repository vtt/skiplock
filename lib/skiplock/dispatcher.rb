module Skiplock
  class Dispatcher
    def initialize(master: true, worker_num: nil, worker_pids: [])
      @queues_order_query = Settings['queues'].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ') if Settings['queues'].is_a?(Hash) && Settings['queues'].count > 0
      @executor = Concurrent::ThreadPoolExecutor.new(min_threads: Settings['min_threads'], max_threads: Settings['max_threads'], max_queue: Settings['max_threads'], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      @master = master
      if @master
        @worker_pids = worker_pids + [ Process.pid ]
      else
        @worker_num = worker_num
      end
      @last_dispatch_at = 0
      @next_schedule_at = Time.now.to_f
      @running = true
    end
    
    def run
      Thread.new do
        Rails.application.reloader.wrap do
          sleep(1) while @running && !Rails.application.initialized?
          Rails.application.eager_load!
          Process.setproctitle("skiplock-#{@master ? 'master[0]' : 'worker[' + @worker_num.to_s + ']'}") if Settings['workers'] > 0 && !Rails.env.development?
          ActiveRecord::Base.connection_pool.with_connection do |connection|
            connection.exec_query('LISTEN "skiplock::jobs"')
            hostname = `hostname -f`.strip
            @worker = Worker.create!(pid: Process.pid, ppid: (@master ? nil : Process.ppid), capacity: Settings['max_threads'], hostname: hostname)
            if @master
              connection.exec_query('LISTEN "skiplock::workers"')
              Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
              check_sync_errors
              # get dead worker ids
              dead_worker_ids = Worker.where(hostname: hostname).where.not(pid: @worker_pids).ids
              if dead_worker_ids.count > 0
                # reset orphaned jobs of the dead worker ids for retry
                Job.where(running: true).where(worker_id: dead_worker_ids).update_all(running: false, worker_id: nil)
                # remove dead workers
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
                    connection.exec_query('LISTEN "skiplock::workers"') if @master
                    @next_schedule_at = Time.now.to_f
                  end
                  check_sync_errors
                  error = false
                end
                notifications = { 'skiplock::jobs' => [], 'skiplock::workers' => [] }
                connection.raw_connection.wait_for_notify(0.1) do |channel, pid, payload|
                  notifications[channel] << payload if payload
                  loop do
                    payload = connection.raw_connection.notifies
                    break unless @running && payload
                    notifications[payload[:relname]] << payload[:extra]
                  end
                  notifications['skiplock::jobs'].each do |n|
                    op, id, worker_id, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                    if @master
                      # TODO: report job status to action cable
                    end
                    next if op == 'DELETE' || running == 'true' || expired_at.to_f > 0 || finished_at.to_f > 0  || scheduled_at.to_f < @last_dispatch_at
                    if scheduled_at.to_f < Time.now.to_f
                      @next_schedule_at = Time.now.to_f
                    elsif scheduled_at.to_f < @next_schedule_at
                      @next_schedule_at = scheduled_at.to_f
                    end
                  end
                  if @master
                    # TODO: report worker status to action cable
                    notifications['skiplock::workers'].each do |n|
                    end
                  end
                end
                if Time.now.to_f >= @next_schedule_at && @executor.remaining_capacity > 0
                  @executor.post { do_work }
                end
              rescue Exception => ex
                # most likely error with database connection
                STDERR.puts ex.message
                STDERR.puts ex.backtrace
                Skiplock.on_errors.each { |p| p.call(ex, @last_exception) }
                error = true
                t = Time.now
                while @running
                  sleep(0.5)
                  break if Time.now - t > 5
                end
                @last_exception = ex
              end
              sleep(0.2)
            end
            connection.exec_query('UNLISTEN *')
            @executor.shutdown
            @executor.wait_for_termination if @wait
            @worker.delete if @worker
          end
        end
      end
    end

    def shutdown(wait: true)
      @running = false
      @wait = wait
    end

    private

    def check_sync_errors
      # get performed jobs that could not sync with database
      Dir.glob('tmp/skiplock/*').each do |f|
        job_from_db = Job.find_by(id: File.basename(f), running: true)
        disposed = true
        if job_from_db
          job, ex = YAML.load_file(f) rescue nil
          disposed = job.dispose(ex)
        end
        File.delete(f) if disposed
      end
    end

    def do_work
      while @running
        @last_dispatch_at = Time.now.to_f - 1  # 1 second allowance for timedrift
        result = Job.dispatch(queues_order_query: @queues_order_query, worker_id: @worker.id)
        next if result.is_a?(Job)
        @next_schedule_at = result if result.is_a?(Float)
        break
      end
    rescue Exception => ex
      STDERR.puts ex.message
      STDERR.puts ex.backtrace
      Skiplock.on_errors.each { |p| p.call(ex, @last_exception) }
      @last_exception = ex
    end
  end
end