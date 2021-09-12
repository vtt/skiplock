module Skiplock
  class Worker < ActiveRecord::Base
    self.implicit_order_column = 'updated_at'
    has_many :jobs, inverse_of: :worker

    def self.cleanup(hostname = nil)
      delete_ids = []
      self.where(hostname: hostname || Socket.gethostname).each do |worker|
        sid = Process.getsid(worker.pid) rescue nil
        delete_ids << worker.id if worker.sid != sid || worker.updated_at < 10.minutes.ago
      end
      self.where(id: delete_ids).delete_all if delete_ids.count > 0
    end

    def self.generate(capacity:, hostname:, master: true)
      self.create!(pid: Process.pid, sid: Process.getsid(), master: master, hostname: hostname, capacity: capacity)
    rescue
      self.create!(pid: Process.pid, sid: Process.getsid(), master: false, hostname: hostname, capacity: capacity)
    end

    def start(worker_num: 0, **config)
      if self.master
        Job.flush
        Cron.setup
      end
      @config = config
      @queues_order_query = @config[:queues].map { |q,v| "WHEN queue_name = '#{q}' THEN #{v}" }.join(' ') if @config[:queues].is_a?(Hash) && @config[:queues].count > 0
      @running = true
      @executor = Concurrent::ThreadPoolExecutor.new(min_threads: @config[:min_threads] + 1, max_threads: @config[:max_threads] + 1, max_queue: @config[:max_threads], idletime: 60, auto_terminate: true, fallback_policy: :discard)
      @executor.post { run }
      Process.setproctitle("skiplock-#{self.master ? 'master[0]' : 'worker[' + worker_num.to_s + ']'}") if @config[:standalone]
    end

    def shutdown
      @running = false
      @executor.shutdown
      @executor.kill unless @executor.wait_for_termination(@config[:graceful_shutdown])
      self.delete
    end

    private

    def get_next_available_job
    end

    def run
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.exec_query('LISTEN "skiplock::jobs"')
        error = false
        next_schedule_at = Time.now.to_f
        timestamp = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        while @running
          Rails.application.reloader.wrap do
            begin
              if error
                unless connection.active?
                  connection.reconnect!
                  sleep(0.5)
                  connection.exec_query('LISTEN "skiplock::jobs"')
                  next_schedule_at = Time.now.to_f
                end
                Job.flush if self.master
                error = false
              end
              if Time.now.to_f >= next_schedule_at && @executor.remaining_capacity > 0
                job = get_next_available_job
                connection.transaction do
                  job = Job.find_by_sql("SELECT id, running, scheduled_at FROM skiplock.jobs WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{@queues_order_query ? ' CASE ' + @queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
                  job = Job.find_by_sql("UPDATE skiplock.jobs SET running = TRUE, worker_id = '#{self.id}', updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first if job && job.scheduled_at.to_f <= Time.now.to_f
                end
                if job.try(:running)
                  @executor.post do
                    job.execute(purge_completion: @config[:purge_completion], max_retries: @config[:max_retries])
                  end
                else
                  next_schedule_at = (job ? job.scheduled_at.to_f : Float::INFINITY)
                end
              end
              job_notifications = []
              connection.raw_connection.wait_for_notify(0.4) do |channel, pid, payload|
                job_notifications << payload if payload
                loop do
                  payload = connection.raw_connection.notifies
                  break unless @running && payload
                  job_notifications << payload[:extra]
                end
                job_notifications.each do |n|
                  op, id, worker_id, job_class, queue_name, running, expired_at, finished_at, scheduled_at = n.split(',')
                  next if op == 'DELETE' || running == 'true' || expired_at.to_f > 0 || finished_at.to_f > 0
                  next_schedule_at = scheduled_at.to_f if scheduled_at.to_f < next_schedule_at
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
        connection.exec_query('UNLISTEN *')
      end
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