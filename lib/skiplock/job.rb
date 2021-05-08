module Skiplock
  class Job < ActiveRecord::Base
    def self.dispatch(queues_order_query: nil, worker_id: nil)
      self.connection.exec_query('BEGIN')
      job = self.find_by_sql("SELECT id, scheduled_at FROM #{self.table_name} WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{queues_order_query ? ' CASE ' + queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
      if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        self.connection.exec_query('END')
        return (job ? job.scheduled_at.to_f : Float::INFINITY)
      end
      job = Skiplock::Job.find_by_sql("UPDATE #{self.table_name} SET running = TRUE, worker_id = #{self.connection.quote(worker_id)}, updated_at = NOW() WHERE id = '#{job.id}' RETURNING *").first
      self.connection.exec_query('END')
      job.data ||= {}
      job.exception_executions ||= {}
      job_data = job.attributes.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions', 'exception_executions').merge('job_id' => job.id, 'enqueued_at' => job.updated_at, 'arguments' => (job.data['arguments'] || []))
      job.executions = (job.executions || 0) + 1
      Thread.current[:skiplock_dispatch_job] = job
      begin
        ActiveJob::Base.execute(job_data)
      rescue Exception => ex
      end
      job.dispose(ex)
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
        Job.create!(id: activejob.job_id, job_class: activejob.class.name, queue_name: activejob.queue_name, locale: activejob.locale, timezone: activejob.timezone, priority: activejob.priority, data: { 'arguments' => activejob.serialize['arguments'] }, scheduled_at: timestamp)
      end
    end

    def dispose(ex)
      self.running = false
      self.updated_at = (Time.now > self.updated_at ? Time.now : self.updated_at + 1)
      if ex
        self.exception_executions["[#{ex.class.name}]"] = (self.exception_executions["[#{ex.class.name}]"] || 0) + 1 unless self.exception_executions.key?('activejob_retry')
        if self.executions >= Settings['max_retries'] || self.exception_executions.key?('activejob_retry')
          self.expired_at = Time.now
          self.save!
        else
          self.scheduled_at = Time.now + (5 * 2**self.executions)
          self.save!
        end
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
          self.delete
        end
      elsif Settings['purge_completion']
        self.delete
      else
        self.finished_at = Time.now
        self.exception_executions = nil
        self.save!
      end
      self
    rescue
      File.write("tmp/skiplock/#{self.id}", [self, ex].to_yaml)
      nil
    end
  end
end