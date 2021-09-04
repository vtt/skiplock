module Skiplock
  class Job < ActiveRecord::Base
    self.implicit_order_column = 'created_at'

    def self.enqueue(activejob)
      self.enqueue_at(activejob, nil)
    end

    def self.enqueue_at(activejob, timestamp)
      timestamp = Time.at(timestamp) if timestamp
      if Thread.current[:skiplock_dispatch_job].try(:id) == activejob.job_id
        Thread.current[:skiplock_dispatch_job].exception_executions = activejob.exception_executions.merge('activejob_retry' => true)
        Thread.current[:skiplock_dispatch_job].executions = activejob.executions
        Thread.current[:skiplock_dispatch_job].scheduled_at = timestamp
        Thread.current[:skiplock_dispatch_job]
      else
        serialize = activejob.serialize
        self.create!(serialize.slice(*self.column_names).merge('id' => serialize['job_id'], 'data' => { 'arguments' => serialize['arguments'] }, 'scheduled_at' => timestamp))
      end
    end

    def self.reset_retry_schedules
      self.where('scheduled_at > NOW() AND executions IS NOT NULL AND expired_at IS NULL AND finished_at IS NULL').update_all(scheduled_at: nil, updated_at: Time.now)
    end

    def dispose(ex, purge_completion: true, max_retries: 20)
      yaml = [self, ex].to_yaml
      self.running = false
      self.worker_id = nil
      self.updated_at = (Time.now > self.updated_at ? Time.now : self.updated_at + 1)
      if ex
        self.exception_executions["[#{ex.class.name}]"] = (self.exception_executions["[#{ex.class.name}]"] || 0) + 1 unless self.exception_executions.key?('activejob_retry')
        if self.executions >= max_retries || self.exception_executions.key?('activejob_retry')
          self.expired_at = Time.now
        else
          self.scheduled_at = Time.now + (5 * 2**self.executions)
        end
        self.save!
        Skiplock.on_errors.each { |p| p.call(ex) }
      elsif self.exception_executions.try(:key?, 'activejob_retry')
        self.save!
      elsif self.cron
        self.data ||= {}
        self.data['crons'] = (self.data['crons'] || 0) + 1
        self.data['last_cron_at'] = Time.now.utc.to_s
        next_cron_at = Cron.next_schedule_at(self.cron)
        if next_cron_at
          self.executions = nil
          self.exception_executions = nil
          self.scheduled_at = Time.at(next_cron_at)
          self.save!
        else
          Skiplock.logger.error "[Skiplock] ERROR: Invalid CRON '#{self.cron}' for Job #{self.job_class}"
          self.delete
        end
      elsif purge_completion
        self.delete
      else
        self.finished_at = Time.now
        self.exception_executions = nil
        self.save!
      end
      self
    rescue Exception => e
      Skiplock.logger.error(e.name)
      Skiplock.logger.error(e.backtrace.join("\n"))
      File.write("tmp/skiplock/#{self.id}", yaml)
      nil
    end

    def execute(purge_completion: true, max_retries: 20)
      Skiplock.logger.info "[Skiplock] Performing #{self.job_class} (#{self.id}) from queue '#{self.queue_name || 'default'}'..."
      self.data ||= {}
      self.exception_executions ||= {}
      job_data = self.attributes.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions', 'exception_executions').merge('job_id' => self.id, 'enqueued_at' => self.updated_at, 'arguments' => (self.data['arguments'] || []))
      self.executions = (self.executions || 0) + 1
      Thread.current[:skiplock_dispatch_job] = self
      activejob = ActiveJob::Base.deserialize(job_data)
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        activejob.perform_now
      rescue Exception => ex
      end
      if ex || self.exception_executions.key?('activejob_retry')
        Skiplock.logger.error("[Skiplock] Job #{self.job_class} (#{self.id}) was interrupted by an exception#{ ' (rescued and retried by ActiveJob)' if self.exception_executions.key?('activejob_retry') }")
        if ex
          Skiplock.logger.error(ex)
          Skiplock.logger.error(ex.backtrace.join("\n"))
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
      self.dispose(ex, purge_completion: purge_completion, max_retries: max_retries)
    end
  end
end