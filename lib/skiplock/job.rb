module Skiplock
  class Job < ActiveRecord::Base
    self.implicit_order_column = 'updated_at'
    attribute :activejob_error
    attribute :exception
    attribute :max_retries
    attribute :purge
    belongs_to :worker, inverse_of: :jobs, required: false

    def self.dispatch(purge_completion: true, max_retries: 20)
      namespace_query = Skiplock.namespace.nil? ? "namespace IS NULL" : "namespace = '#{Skiplock.namespace}'"
      job = nil
      self.connection.transaction do
        job = self.find_by_sql("SELECT id, scheduled_at FROM skiplock.jobs WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL AND #{namespace_query} ORDER BY scheduled_at ASC NULLS FIRST, priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
        return if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        job = self.find_by_sql("UPDATE skiplock.jobs SET running = TRUE, updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
      end
      self.dispatch(purge_completion: purge_completion, max_retries: max_retries) if job.execute(purge_completion: purge_completion, max_retries: max_retries)
    end

    def self.enqueue(activejob)
      self.enqueue_at(activejob, nil)
    end

    def self.enqueue_at(activejob, timestamp)
      options = activejob.instance_variable_get('@skiplock_options') || {}
      timestamp = Time.at(timestamp) if timestamp
      if Thread.current[:skiplock_job].try(:id) == activejob.job_id
        Thread.current[:skiplock_job].activejob_error = options[:error]
        Thread.current[:skiplock_job].executions = activejob.executions
        Thread.current[:skiplock_job].exception_executions = activejob.exception_executions
        Thread.current[:skiplock_job].exception_executions['activejob_error'] = true
        Thread.current[:skiplock_job].scheduled_at = timestamp
        Thread.current[:skiplock_job]
      else
        serialize = activejob.serialize
        self.create!(serialize.slice(*self.column_names).merge('id' => serialize['job_id'], 'data' => { 'arguments' => serialize['arguments'], 'options' => options }, 'namespace' => Skiplock.namespace, 'scheduled_at' => timestamp))
      end
    end

    # resynchronize jobs that could not commit to database and reset any abandoned jobs for retry
    def self.flush
      Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
      Dir.glob('tmp/skiplock/*').each do |f|
        disposed = true
        if self.exists?(id: File.basename(f), running: true)
          job = YAML.load_file(f) rescue nil
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
      return unless self.max_retries
      yaml = self.to_yaml
      purging = false
      self.running = false
      self.worker_id = nil
      self.updated_at = Time.now > self.updated_at ? Time.now : self.updated_at + 1 # in case of clock drifting
      if self.exception
        self.exception_executions["[#{self.exception.class.name}]"] = self.exception_executions["[#{self.exception.class.name}]"].to_i + 1 unless self.exception_executions.key?('activejob_error')
        if (self.executions.to_i >= self.max_retries + 1) || self.exception_executions.key?('activejob_error') || self.exception.is_a?(Skiplock::Extension::ProxyError)
          self.expired_at = Time.now
        else
          self.scheduled_at = Time.now + (5 * 2**self.executions.to_i)
        end
      elsif self.finished_at
        if self.cron
          self.data['cron'] ||= {}
          self.data['cron']['executions'] = self.data['cron']['executions'].to_i + 1
          self.data['cron']['last_finished_at'] = self.finished_at.utc.to_s
          self.data['cron']['last_result'] = self.data['result']
          next_cron_at = Cron.next_schedule_at(self.cron)
          if next_cron_at
            # update job to record completions counter before resetting finished_at to nil
            self.update_columns(self.attributes.slice(*(self.changes.keys & self.class.column_names)))
            self.finished_at = nil
            self.executions = nil
            self.exception_executions = nil
            self.data.delete('result')
            self.scheduled_at = Time.at(next_cron_at)
          else
            Skiplock.logger.error("[Skiplock] ERROR: Invalid CRON '#{self.cron}' for Job #{self.job_class}") if Skiplock.logger
            purging = true
          end
        elsif self.purge == true
          purging = true
        end
      end
      purging ? self.delete : self.update_columns(self.attributes.slice(*(self.changes.keys & self.class.column_names)))
    rescue Exception => e
      File.write("tmp/skiplock/#{self.id}", yaml) rescue nil
      if Skiplock.logger
        Skiplock.logger.error(e.to_s)
        Skiplock.logger.error(e.backtrace.join("\n"))
      end
      Skiplock.on_errors.each { |p| p.call(e) }
      nil
    end

    def execute(purge_completion: true, max_retries: 20)
      raise 'Job has already been completed' if self.finished_at
      self.update_columns(running: true, updated_at: Time.now) unless self.running
      Skiplock.logger.info("[Skiplock] Performing #{self.job_class} (#{self.id}) from queue '#{self.queue_name || 'default'}'...") if Skiplock.logger
      self.data ||= {}
      self.data.delete('result')
      self.exception_executions ||= {}
      self.activejob_error = nil
      self.max_retries = (self.data['options'].key?('max_retries') ? self.data['options']['max_retries'].to_i : max_retries) rescue max_retries
      self.max_retries = 20 if self.max_retries < 0 || self.max_retries > 20
      self.purge = (self.data['options'].key?('purge') ? self.data['options']['purge'] : purge_completion) rescue purge_completion
      job_data = { 'job_class' => self.job_class, 'queue_name' => self.queue_name, 'locale' => self.locale, 'timezone' => self.timezone, 'priority' => self.priority, 'executions' => self.executions, 'exception_executions' => self.exception_executions.dup, 'job_id' => self.id, 'enqueued_at' => self.updated_at.iso8601, 'arguments' => (self.data['arguments'] || []) }
      self.executions = self.executions.to_i + 1
      self.data_will_change!
      Thread.current[:skiplock_job] = self
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        self.data['result'] = ActiveJob::Base.execute(job_data)
      rescue Exception => ex
        self.exception = ex
        Skiplock.on_errors.each { |p| p.call(ex) }
      end
      if Skiplock.logger
        if self.exception || self.activejob_error
          Skiplock.logger.error("[Skiplock] Job #{self.job_class} (#{self.id}) was interrupted by an exception#{ ' (rescued and retried by ActiveJob)' if self.activejob_error }")
          if self.exception
            Skiplock.logger.error(self.exception.to_s)
            Skiplock.logger.error(self.exception.backtrace.join("\n"))
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
      self.exception || self.activejob_error || self.data['result']
    ensure
      self.finished_at ||= Time.now if self.data.key?('result') && !self.activejob_error
      self.dispose
    end
  end
end
