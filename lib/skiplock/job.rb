module Skiplock
  class Job < ActiveRecord::Base
    self.implicit_order_column = 'created_at'
    attr_accessor :activejob_retry
    belongs_to :worker, inverse_of: :jobs, required: false

    def self.dispatch(purge_completion: true, max_retries: 20)
      job = nil
      self.connection.transaction do
        job = self.find_by_sql("SELECT id, scheduled_at FROM skiplock.jobs WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST, priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
        return if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        job = self.find_by_sql("UPDATE skiplock.jobs SET running = TRUE, updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
      end
      self.dispatch(purge_completion: purge_completion, max_retries: max_retries) if job.execute(purge_completion: purge_completion, max_retries: max_retries)
    end

    def self.enqueue(activejob)
      self.enqueue_at(activejob, nil)
    end

    def self.enqueue_at(activejob, timestamp)
      timestamp = Time.at(timestamp) if timestamp
      if Thread.current[:skiplock_job].try(:id) == activejob.job_id
        Thread.current[:skiplock_job].activejob_retry = true
        Thread.current[:skiplock_job].executions = activejob.executions
        Thread.current[:skiplock_job].exception_executions = activejob.exception_executions
        Thread.current[:skiplock_job].scheduled_at = timestamp
        Thread.current[:skiplock_job]
      else
        options = activejob.instance_variable_get('@skiplock_options') || {}
        serialize = activejob.serialize
        self.create!(serialize.slice(*self.column_names).merge('id' => serialize['job_id'], 'data' => { 'arguments' => serialize['arguments'], 'options' => options }, 'scheduled_at' => timestamp))
      end
    end

    # resynchronize jobs that could not commit to database and retry any abandoned jobs
    def self.flush
      Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
      Dir.glob('tmp/skiplock/*').each do |f|
        disposed = true
        if self.exists?(id: File.basename(f), running: true)
          job = Marshal.load(File.binread(f)) rescue nil
          disposed = job.dispose if job.is_a?(Skiplock::Job)
        end
        (File.delete(f) rescue nil) if disposed
      end
      self.where(running: true).where.not(worker_id: Worker.ids).update_all(running: false, worker_id: nil)
      true
    end

    def self.reset_retry_schedules
      self.where('scheduled_at > NOW() AND executions > 0 AND expired_at IS NULL AND finished_at IS NULL').update_all(scheduled_at: nil, updated_at: Time.now)
    end

    def dispose
      return unless @max_retries
      dump = Marshal.dump(self)
      purging = false
      self.running = false
      self.worker_id = nil
      self.updated_at = Time.now > self.updated_at ? Time.now : self.updated_at + 1 # in case of clock drifting
      if @exception
        self.exception_executions["[#{@exception.class.name}]"] = self.exception_executions["[#{@exception.class.name}]"].to_i + 1 unless self.activejob_retry
        if (self.executions.to_i >= @max_retries + 1) || self.activejob_retry
          self.expired_at = Time.now
        else
          self.scheduled_at = Time.now + (5 * 2**self.executions.to_i)
        end
      elsif self.finished_at
        if self.cron
          self.data['cron'] ||= {}
          self.data['cron']['executions'] = self.data['cron']['executions'].to_i + 1
          self.data['cron']['last_finished_at'] = self.finished_at.utc.to_s
          next_cron_at = Cron.next_schedule_at(self.cron)
          if next_cron_at
            # update job to record completions counter before resetting finished_at to nil
            self.update_columns(self.attributes.slice(*self.changes.keys))
            self.finished_at = nil
            self.executions = nil
            self.exception_executions = nil
            self.scheduled_at = Time.at(next_cron_at)
          else
            Skiplock.logger.error("[Skiplock] ERROR: Invalid CRON '#{self.cron}' for Job #{self.job_class}") if Skiplock.logger
            purging = true
          end
        elsif @purge == true
          purging = true
        end
      end
      purging ? self.delete : self.update_columns(self.attributes.slice(*self.changes.keys))
    rescue Exception => e
      File.binwrite("tmp/skiplock/#{self.id}", dump) rescue nil
      if Skiplock.logger
        Skiplock.logger.error(e.to_s)
        Skiplock.logger.error(e.backtrace.join("\n"))
      end
      Skiplock.on_errors.each { |p| p.call(e) }
      nil
    end

    def execute(purge_completion: true, max_retries: 20)
      raise 'Job has already been completed' if self.finished_at
      Skiplock.logger.info("[Skiplock] Performing #{self.job_class} (#{self.id}) from queue '#{self.queue_name || 'default'}'...") if Skiplock.logger
      self.data ||= {}
      self.data.delete('result')
      self.exception_executions ||= {}
      self.activejob_retry = false
      @max_retries = (self.data['options'].key?('max_retries') ? self.data['options']['max_retries'].to_i : max_retries) rescue max_retries
      @max_retries = 20 if @max_retries < 0 || @max_retries > 20
      @purge = (self.data['options'].key?('purge') ? self.data['options']['purge'] : purge_completion) rescue purge_completion
      job_data = self.attributes.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions', 'exception_executions').merge('job_id' => self.id, 'enqueued_at' => self.updated_at, 'arguments' => (self.data['arguments'] || []))
      self.executions = self.executions.to_i + 1
      Thread.current[:skiplock_job] = self
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        self.data['result'] = ActiveJob::Base.execute(job_data)
      rescue Exception => ex
        @exception = ex
        Skiplock.on_errors.each { |p| p.call(@exception) }
      end
      if Skiplock.logger
        if @exception || self.activejob_retry
          Skiplock.logger.error("[Skiplock] Job #{self.job_class} (#{self.id}) was interrupted by an exception#{ ' (rescued and retried by ActiveJob)' if self.activejob_retry }")
          if @exception
            Skiplock.logger.error(@exception.to_s)
            Skiplock.logger.error(@exception.backtrace.join("\n"))
          end
        else
          end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          job_name = self.job_class
          if self.job_class == 'Skiplock::Extension::ProxyJob'
            target, method_name = ::YAML.load(self.data['arguments'].first)
            job_name = "'#{target.name}.#{method_name}'"
          end
          Skiplock.logger.info "[Skiplock] Performed #{job_name} (#{self.id}) from queue '#{self.queue_name || 'default'}' in #{end_time - start_time} seconds"
        end
      end
    ensure
      self.finished_at ||= Time.now if self.data.key?('result') && !self.activejob_retry
      self.dispose
    end
  end
end