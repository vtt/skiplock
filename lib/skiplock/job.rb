module Skiplock
  class Job < ActiveRecord::Base
    self.implicit_order_column = 'created_at'

    def self.dispatch(queues_order_query: nil, worker_id: nil, purge_completion: true, max_retries: 20)
      job = nil
      self.transaction do
        job = self.find_by_sql("SELECT id, scheduled_at FROM #{self.table_name} WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{queues_order_query ? ' CASE ' + queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
        return (job ? job.scheduled_at.to_f : Float::INFINITY) if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        job = Skiplock::Job.find_by_sql("UPDATE #{self.table_name} SET running = TRUE, worker_id = #{self.connection.quote(worker_id)}, updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
      end
      job.data ||= {}
      job.exception_executions ||= {}
      job_data = job.attributes.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions', 'exception_executions').merge('job_id' => job.id, 'enqueued_at' => job.updated_at, 'arguments' => (job.data['arguments'] || []))
      job.executions = (job.executions || 0) + 1
      Skiplock.logger.info "[Skiplock] Performing #{job.job_class} (#{job.id}) from queue '#{job.queue_name || 'default'}'..."
      Thread.current[:skiplock_dispatch_job] = job
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        ActiveJob::Base.execute(job_data)
      rescue Exception => ex
        Skiplock.logger.error(ex)
      end
      unless ex
        end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        job_name = job.job_class
        if job.job_class == 'Skiplock::Extension::ProxyJob'
          target, method_name = ::YAML.load(job.data['arguments'].first)
          job_name = "'#{target.name}.#{method_name}'"
        end
        Skiplock.logger.info "[Skiplock] Performed #{job_name} (#{job.id}) from queue '#{job.queue_name || 'default'}' in #{end_time - start_time} seconds"
      end
      job.dispose(ex, purge_completion: purge_completion, max_retries: max_retries)
    ensure
      Thread.current[:skiplock_dispatch_job] = nil
    end

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
        Job.create!(serialize.slice(*self.column_names).merge('id' => serialize['job_id'], 'data' => { 'arguments' => serialize['arguments'] }, 'scheduled_at' => timestamp))
      end
    end

    def self.reset_retry_schedules
      self.where('scheduled_at > NOW() AND executions IS NOT NULL AND expired_at IS NULL AND finished_at IS NULL').update_all(scheduled_at: nil, updated_at: Time.now)
    end

    def dispose(ex, purge_completion: true, max_retries: 20)
      dup = self.dup
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
      Skiplock.logger.error(e)
      File.write("tmp/skiplock/#{self.id}", [dup, ex].to_yaml)
      nil
    end
  end
end