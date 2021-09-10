module Skiplock
  class Job < ActiveRecord::Base
    self.implicit_order_column = 'created_at'
    attr_accessor :activejob_retry
    belongs_to :worker, inverse_of: :jobs, required: false

    # resynchronize jobs that could not commit to database and retry any abandoned jobs
    def self.cleanup(purge_completion: true, max_retries: 20)
      Dir.mkdir('tmp/skiplock') unless Dir.exist?('tmp/skiplock')
      Dir.glob('tmp/skiplock/*').each do |f|
        job_from_db = self.find_by(id: File.basename(f), running: true)
        disposed = true
        if job_from_db
          job, ex = YAML.load_file(f) rescue nil
          disposed = job.dispose(ex, purge_completion: purge_completion, max_retries: max_retries) if job
        end
        (File.delete(f) rescue nil) if disposed
      end
      self.where(running: true).where.not(worker_id: Worker.ids).update_all(running: false, worker_id: nil)
    end

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
      if Thread.current[:skiplock_dispatch_job].try(:id) == activejob.job_id
        Thread.current[:skiplock_dispatch_job].activejob_retry = true
        Thread.current[:skiplock_dispatch_job].executions = activejob.executions
        Thread.current[:skiplock_dispatch_job].exception_executions = activejob.exception_executions
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
      purging = false
      self.running = false
      self.worker_id = nil
      self.updated_at = Time.now > self.updated_at ? Time.now : self.updated_at + 1 # in case of clock drifting
      if ex
        self.exception_executions ||= {}
        self.exception_executions["[#{ex.class.name}]"] = self.exception_executions["[#{ex.class.name}]"].to_i + 1 unless self.activejob_retry
        if self.executions.to_i >= max_retries || self.activejob_retry
          self.expired_at = Time.now
        else
          self.scheduled_at = Time.now + (5 * 2**self.executions.to_i)
        end
        Skiplock.on_errors.each { |p| p.call(ex) }
      elsif self.finished_at
        if self.cron
          self.data ||= {}
          self.data['crons'] = (self.data['crons'] || 0) + 1
          self.data['last_cron_at'] = Time.now.utc.to_s
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
        elsif purge_completion
          purging = true
        end
      end
      purging ? self.delete : self.update_columns(self.attributes.slice(*self.changes.keys))
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
      Skiplock.logger.info("[Skiplock] Performing #{self.job_class} (#{self.id}) from queue '#{self.queue_name || 'default'}'...") if Skiplock.logger
      self.data ||= {}
      self.exception_executions ||= {}
      self.activejob_retry = false
      job_data = self.attributes.slice('job_class', 'queue_name', 'locale', 'timezone', 'priority', 'executions', 'exception_executions').merge('job_id' => self.id, 'enqueued_at' => self.updated_at, 'arguments' => (self.data['arguments'] || []))
      self.executions = self.executions.to_i + 1
      Thread.current[:skiplock_dispatch_job] = self
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        ActiveJob::Base.execute(job_data)
        self.finished_at = Time.now unless self.activejob_retry
      rescue Exception => ex
      end
      if Skiplock.logger
        if ex || self.activejob_retry
          Skiplock.logger.error("[Skiplock] Job #{self.job_class} (#{self.id}) was interrupted by an exception#{ ' (rescued and retried by ActiveJob)' if self.activejob_retry }")
          if ex
            Skiplock.logger.error(ex.to_s)
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
      end
    ensure
      self.dispose(ex, purge_completion: purge_completion, max_retries: max_retries)
    end
  end
end