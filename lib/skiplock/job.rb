module Skiplock
  class Job < ActiveRecord::Base
    self.table_name = 'skiplock.jobs'

    # Return: Skiplock::Job if it was executed; otherwise returns the next Job's schedule time in FLOAT
    def self.dispatch(queues_order_query: nil, worker_id: nil)
      self.connection.exec_query('BEGIN')
      job = self.find_by_sql("SELECT id, scheduled_at FROM #{self.table_name} WHERE running = FALSE AND expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST,#{queues_order_query ? 'CASE ' + queues_order_query + ' ELSE NULL END ASC NULLS LAST,' : ''} priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
      if job.nil? || job.scheduled_at.to_f > Time.now.to_f
        self.connection.exec_query('END')
        return (job ? job.scheduled_at.to_f : Float::INFINITY)
      end
      job = Skiplock::Job.find_by_sql("UPDATE #{self.table_name} SET running = TRUE, worker_id = #{self.connection.quote(worker_id)} WHERE id = '#{job.id}' RETURNING *").first
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
      job.running = false
      if ex
        # TODO: report exception
        job.exception_executions["[#{ex.class.name}]"] = (job.exception_executions["[#{ex.class.name}]"] || 0) + 1 unless job.exception_executions.key?('activejob_retry')
        if job.executions >= Settings['max_retries'] || job.exception_executions.key?('activejob_retry')
          job.expired_at = Time.now
          job.save!
        else
          job.scheduled_at = Time.now + (5 * 2**job.executions)
          job.save!
        end
      elsif job.exception_executions.key?('activejob_retry')
        job.save!
      elsif job['cron']
        job.data['last_cron_run'] = Time.now.utc.to_s
        next_cron_at = Cron.next_schedule_at(job['cron'])
        if next_cron_at
          job.executions = 1
          job.exception_executions = nil
          job.scheduled_at = Time.at(next_cron_at)
          job.save!
        else
          job.delete
        end
      elsif Settings['purge_completion']
        job.delete
      else
        job.finished_at = Time.now
        job.exception_executions = nil
        job.save!
      end
      job
    ensure
      Thread.current[:skiplock_dispatch_job] = nil
    end

    def self.enqueue_at(activejob, timestamp)
      timestamp = Time.at(timestamp) if timestamp
      if Thread.current[:skiplock_dispatch_job].try(:id) == activejob.job_id
        Thread.current[:skiplock_dispatch_job].exception_executions = activejob.exception_executions.merge('activejob_retry' => true)
        Thread.current[:skiplock_dispatch_job].executions = activejob.executions
        Thread.current[:skiplock_dispatch_job].scheduled_at = timestamp
        Thread.current[:skiplock_dispatch_job]
      else
        Job.create!(id: activejob.job_id, job_class: activejob.class.name, queue_name: activejob.queue_name, locale: activejob.locale, timezone: activejob.timezone, priority: activejob.priority, executions: activejob.executions, data: { 'arguments' => activejob.serialize['arguments'] }, scheduled_at: timestamp)
      end
    end
  end
end