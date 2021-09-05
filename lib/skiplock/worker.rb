module Skiplock
  class Worker < ActiveRecord::Base
    self.implicit_order_column = 'created_at'

    def run(worker_num: 0, **config)
      Thread.new do
        execution_context = Rails.application.reloader.run!
        @config = config
        @worker_num = worker_num
        @queues_order_query = @config[:queues].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ') if @config[:queues].is_a?(Hash) && @config[:queues].count > 0
        @next_schedule_at = Time.now.to_f
        @executor = Concurrent::ThreadPoolExecutor.new(min_threads: @config[:min_threads], max_threads: @config[:max_threads], max_queue: @config[:max_threads], idletime: 60, auto_terminate: true, fallback_policy: :discard)
        @running = true
        Process.setproctitle("skiplock-#{self.master ? 'master[0]' : 'worker[' + @worker_num.to_s + ']'}") if @config[:standalone]
        @connection = self.class.connection
        @connection.exec_query('LISTEN "skiplock::jobs"')
        if self.master
          Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
          check_sync_errors
          Cron.setup
        end
        error = false
        timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        while @running
          Rails.application.reloader.wrap do
            begin
              if error
                unless @connection.active?
                  @connection.reconnect!
                  sleep(0.5)
                  @connection.exec_query('LISTEN "skiplock::jobs"')
                  @next_schedule_at = Time.now.to_f
                end
                check_sync_errors if self.master
                error = false
              end
              job_notifications = []
              @connection.raw_connection.wait_for_notify(0.1) do |channel, pid, payload|
                job_notifications << payload if payload
                loop do
                  payload = @connection.raw_connection.notifies
                  break unless @running && payload
                  job_notifications << payload[:extra]
                end
                job_notifications.each do |n|
                  op, id, worker_id, job_class, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                  next if op == 'DELETE' || running == 'true' || expired_at.to_f > 0 || finished_at.to_f > 0
                  if scheduled_at.to_f <= Time.now.to_f
                    @next_schedule_at = Time.now.to_f
                  elsif scheduled_at.to_f < @next_schedule_at
                    @next_schedule_at = scheduled_at.to_f
                  end
                end
              end
              if Time.now.to_f >= @next_schedule_at && @executor.remaining_capacity > 0
                job = dispatch_job
                if job.is_a?(Job)
                  @executor.post(job, @config[:purge_completion], @config[:max_retries]) do |job, purge_completion, max_retries|
                    job.execute(purge_completion: purge_completion, max_retries: max_retries)
                  end
                else
                  @next_schedule_at = job
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
              t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              while @running
                sleep(0.5)
                break if Process.clock_gettime(Process::CLOCK_MONOTONIC) - t > 5
              end
              @last_exception = ex
            end
            sleep(0.2)
          end
        end
        @connection.exec_query('UNLISTEN *')
        @executor.shutdown
        @executor.kill unless @executor.wait_for_termination(@config[:graceful_shutdown])
      ensure
        execution_context.complete! if execution_context
      end
    end

    def shutdown
      @running = false
    end

    private

    def check_sync_errors
      # get executed jobs that could not sync with database
      Dir.glob('tmp/skiplock/*').each do |f|
        job_from_db = Job.find_by(id: File.basename(f), running: true)
        disposed = true
        if job_from_db
          job, ex = YAML.load_file(f) rescue nil
          disposed = job.dispose(ex, purge_completion: @config[:purge_completion], max_retries: @config[:max_retries]) if job
        end
        File.delete(f) if disposed
      end
    end

    def dispatch_job
      @connection.transaction do
        job = Job.find_by_sql("SELECT id, scheduled_at FROM skiplock.jobs WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{@queues_order_query ? ' CASE ' + @queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
        return (job ? job.scheduled_at.to_f : Float::INFINITY) if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        Job.find_by_sql("UPDATE skiplock.jobs SET running = TRUE, worker_id = '#{self.id}', updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
      end
    end
  end
end