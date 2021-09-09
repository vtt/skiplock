module Skiplock
  class Worker < ActiveRecord::Base
    self.implicit_order_column = 'created_at'
    has_many :jobs, inverse_of: :worker

    def start(worker_num: 0, **config)
      @config = config
      @queues_order_query = @config[:queues].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ') if @config[:queues].is_a?(Hash) && @config[:queues].count > 0
      @next_schedule_at = Time.now.to_f
      @executor = Concurrent::ThreadPoolExecutor.new(min_threads: @config[:min_threads] + 1, max_threads: @config[:max_threads] + 1, max_queue: @config[:max_threads], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      if self.master
        Job.cleanup(purge_completion: @config[:purge_completion], max_retries: @config[:max_retries])
        Cron.setup
      end
      @running = true
      Process.setproctitle("skiplock-#{self.master ? 'master[0]' : 'worker[' + worker_num.to_s + ']'}") if @config[:standalone]
      @executor.post { run }
    end

    def shutdown
      @running = false
      @executor.shutdown
      @executor.kill unless @executor.wait_for_termination(@config[:graceful_shutdown])
      self.delete
    end

    private

    def get_next_available_job
      @connection.transaction do
        job = Job.find_by_sql("SELECT id, scheduled_at FROM skiplock.jobs WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{@queues_order_query ? ' CASE ' + @queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
        if job && job.scheduled_at.to_f <= Time.now.to_f
          job = Job.find_by_sql("UPDATE skiplock.jobs SET running = TRUE, worker_id = '#{self.id}', updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
        end
        job
      end
    end

    def run
      error = false
      listen = false
      timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      while @running
        Rails.application.reloader.wrap do
          begin
            unless listen
              @connection = self.class.connection
              @connection.exec_query('LISTEN "skiplock::jobs"')
              listen = true
            end
            if error
              unless @connection.active?
                @connection.reconnect!
                sleep(0.5)
                @connection.exec_query('LISTEN "skiplock::jobs"')
                @next_schedule_at = Time.now.to_f
              end
              Job.cleanup if self.master
              error = false
            end
            if Time.now.to_f >= @next_schedule_at && @executor.remaining_capacity > 0
              job = get_next_available_job
              if job.try(:running)
                @executor.post { Rails.application.reloader.wrap { job.execute(purge_completion: @config[:purge_completion], max_retries: @config[:max_retries]) } }
              else
                @next_schedule_at = (job ? job.scheduled_at.to_f : Float::INFINITY)
              end
            end
            job_notifications = []
            @connection.raw_connection.wait_for_notify(0.4) do |channel, pid, payload|
              job_notifications << payload if payload
              loop do
                payload = @connection.raw_connection.notifies
                break unless @running && payload
                job_notifications << payload[:extra]
              end
              job_notifications.each do |n|
                op, id, worker_id, job_class, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                next if op == 'DELETE' || running == 'true' || expired_at.to_f > 0 || finished_at.to_f > 0
                @next_schedule_at = scheduled_at.to_f if scheduled_at.to_f < @next_schedule_at
              end
            end
            if Process.clock_gettime(Process::CLOCK_MONOTONIC) - timestamp > 60
              self.touch
              timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            end
          rescue Exception => ex
            # most likely error with database connection
            Skiplock.logger.error(ex.to_s)
            Skiplock.logger.error(ex.backtrace.join("\n"))
            Skiplock.on_errors.each { |p| p.call(ex, @last_exception) }
            error = true
            wait(5)
            @last_exception = ex
          end
          sleep(0.3)
        end
      end
      @connection.exec_query('UNLISTEN *')
    end

    def wait(timeout)
      t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      while @running
        sleep(0.5)
        break if Process.clock_gettime(Process::CLOCK_MONOTONIC) - t > timeout
      end
    end
  end
end