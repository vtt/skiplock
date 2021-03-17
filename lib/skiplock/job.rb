module Skiplock
  class Job < ActiveRecord::Base
    self.table_name = 'skiplock.jobs'

    # Accept: An active ActiveRecord database connection (eg. ActiveRecord::Base.connection)
    #         The connection should be checked out using ActiveRecord::Base.connection_pool.checkout, and be checked
    #         in using ActiveRecord::Base.conection_pool.checkin once all of the job dispatches have been completed.
    #         *** IMPORTANT: This connection cannot be shared with the job's execution
    #
    # Return: Attributes hash of the Job if it was executed; otherwise returns the next Job's schedule time in FLOAT
    def self.dispatch(connection: ActiveRecord::Base.connection)
      connection.execute('BEGIN')
      job = connection.execute("SELECT * FROM #{self.table_name} WHERE expired_at IS NULL AND finished_at IS NULL ORDER BY scheduled_at ASC NULLS FIRST, priority ASC NULLS LAST, created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1").first
      if job && job['scheduled_at'].to_f <= Time.now.to_f
        executions = (job['executions'] || 0) + 1
        exceptions = job['exception_executions'] ? JSON.parse(job['exception_executions']) : {}
        data = job['data'] ? JSON.parse(job['data']) : {}
        job_data = job.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions').merge('job_id' => job['id'], 'exception_executions' => exceptions, 'enqueued_at' => job['updated_at']).merge(data)
        Thread.current[:skiplock_dispatch_data] = job_data
        begin
          ActiveJob::Base.execute(job_data)
        rescue Exception => ex
        end
        if ex
          # TODO: report exception
          exceptions["[#{ex.class.name}]"] = (exceptions["[#{ex.class.name}]"] || 0) + 1 unless exceptions.key?('activejob_retry')
          if executions >= Settings['max_retries'] || exceptions.key?('activejob_retry')
            connection.execute("UPDATE #{self.table_name} SET executions = #{executions}, exception_executions = '#{connection.quote_string(exceptions.to_json.to_s)}', expired_at = NOW(), updated_at = NOW() WHERE id = '#{job['id']}' RETURNING *").first
          else
            timestamp = Time.now + (5 * 2**executions)
            connection.execute("UPDATE #{self.table_name} SET executions = #{executions}, exception_executions = '#{connection.quote_string(exceptions.to_json.to_s)}', scheduled_at = TO_TIMESTAMP(#{timestamp.to_f}), updated_at = NOW() WHERE id = '#{job['id']}' RETURNING *").first
          end
        elsif exceptions.key?('activejob_retry')
          connection.execute("UPDATE #{self.table_name} SET executions = #{job_data['executions']}, exception_executions = '#{connection.quote_string(job_data['exception_executions'].to_json.to_s)}', scheduled_at = TO_TIMESTAMP(#{job_data['scheduled_at'].to_f}), updated_at = NOW() WHERE id = '#{job['id']}' RETURNING *").first
        elsif job['cron']
          data['last_cron_run'] = Time.now.utc.to_s
          next_cron_at = Cron.next_schedule_at(job['cron'])
          if next_cron_at
            connection.execute("UPDATE #{self.table_name} SET scheduled_at = TO_TIMESTAMP(#{next_cron_at}), executions = 1, exception_executions = NULL, data = '#{connection.quote_string(data.to_json.to_s)}', updated_at = NOW() WHERE id = '#{job['id']}' RETURNING *").first
          else
            connection.execute("DELETE FROM #{self.table_name} WHERE id = '#{job['id']}' RETURNING *").first
          end
        elsif Settings['purge_completion']
          connection.execute("DELETE FROM #{self.table_name} WHERE id = '#{job['id']}' RETURNING *").first
        else
          connection.execute("UPDATE #{self.table_name} SET executions = #{executions}, exception_executions = NULL, finished_at = NOW(), updated_at = NOW() WHERE id = '#{job['id']}' RETURNING *").first
        end
      else
        job ? job['scheduled_at'].to_f : Float::INFINITY
      end
    ensure
      connection.execute('END')
      Thread.current[:skiplock_dispatch_data] = nil
    end

    def self.enqueue_at(job, timestamp)
      if Thread.current[:skiplock_dispatch_data]
        job.exception_executions['activejob_retry'] = true
        Thread.current[:skiplock_dispatch_data]['executions'] = job.executions
        Thread.current[:skiplock_dispatch_data]['exception_executions'] = job.exception_executions
        Thread.current[:skiplock_dispatch_data]['scheduled_at'] = Time.at(timestamp)
      else
        timestamp = Time.at(timestamp) if timestamp
        Job.create!(id: job.job_id, job_class: job.class.name, queue_name: job.queue_name, locale: job.locale, timezone: job.timezone, priority: job.priority, executions: job.executions, data: { 'arguments' => job.arguments }, scheduled_at: timestamp)
      end
      false
    end
  end
end