module Skiplock
  class Dispatcher
    def initialize(worker:, worker_num: nil, **config)
      @config = config
      @worker = worker
      @queues_order_query = @config[:queues].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ') if @config[:queues].is_a?(Hash) && @config[:queues].count > 0
      @executor = Concurrent::ThreadPoolExecutor.new(min_threads: @config[:min_threads], max_threads: @config[:max_threads], max_queue: @config[:max_threads], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      @last_dispatch_at = 0
      @next_schedule_at = Time.now.to_f
      Process.setproctitle("skiplock-#{@worker.master ? 'master[0]' : 'worker[' + worker_num.to_s + ']'}") if @config[:standalone]
    end
    
    def run
      @running = true
      Thread.new do
        ActiveRecord::Base.connection_pool.with_connection do |connection|
          connection.exec_query('LISTEN "skiplock::jobs"')
          if @worker.master
            Rails.application.eager_load! if Rails.env.development?
            Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
            check_sync_errors
            Cron.setup
          end
          error = false
          timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          while @running
            begin
              if error
                unless connection.active?
                  connection.reconnect!
                  sleep(0.5)
                  connection.exec_query('LISTEN "skiplock::jobs"')
                  @next_schedule_at = Time.now.to_f
                end
                check_sync_errors
                error = false
              end
              job_notifications = []
              connection.raw_connection.wait_for_notify(0.1) do |channel, pid, payload|
                job_notifications << payload if payload
                loop do
                  payload = connection.raw_connection.notifies
                  break unless @running && payload
                  job_notifications << payload[:extra]
                end
                job_notifications.each do |n|
                  op, id, worker_id, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                  next if op == 'DELETE' || running == 'true' || expired_at.to_f > 0 || finished_at.to_f > 0  || scheduled_at.to_f < @last_dispatch_at
                  if scheduled_at.to_f <= Time.now.to_f
                    @next_schedule_at = Time.now.to_f
                  elsif scheduled_at.to_f < @next_schedule_at
                    @next_schedule_at = scheduled_at.to_f
                  end
                end
              end
              if Time.now.to_f >= @next_schedule_at && @executor.remaining_capacity > 0
                @executor.post { do_work }
              end
              if Process.clock_gettime(Process::CLOCK_MONOTONIC) - timestamp > 60
                @worker.touch
                timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              end
            rescue Exception => ex
              # most likely error with database connection
              Skiplock.logger.error(ex)
              Skiplock.on_errors.each { |p| p.call(ex, @last_exception) }
              error = true
              t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              while @running
                sleep(0.5)
                break if Process.clock_gettime(Process::CLOCK_MONOTONIC) - t > 5
              end
              @last_exception = ex
            end
            sleep(0.2)
          end
          connection.exec_query('UNLISTEN *')
          @executor.shutdown
          @executor.kill unless @executor.wait_for_termination(@config[:graceful_shutdown])
        end
      end
    end

    def shutdown
      @running = false
    end

    private

    def check_sync_errors
      # get performed jobs that could not sync with database
      Dir.glob('tmp/skiplock/*').each do |f|
        job_from_db = Job.find_by(id: File.basename(f), running: true)
        disposed = true
        if job_from_db
          job, ex = YAML.load_file(f) rescue nil
          disposed = job.dispose(ex, purge_completion: @config[:purge_completion], max_retries: @config[:max_retries])
        end
        File.delete(f) if disposed
      end
    end

    def do_work
      while @running
        @last_dispatch_at = Time.now.to_f - 1  # 1 second allowance for time drift
        result = Job.dispatch(queues_order_query: @queues_order_query, worker_id: @worker.id, purge_completion: @config[:purge_completion], max_retries: @config[:max_retries])
        next if result.is_a?(Job) && Time.now.to_f >= @next_schedule_at
        @next_schedule_at = result if result.is_a?(Float)
        break
      end
    rescue Exception => ex
      Skiplock.logger.error(ex)
      Skiplock.on_errors.each { |p| p.call(ex, @last_exception) }
      @last_exception = ex
    end
  end
end