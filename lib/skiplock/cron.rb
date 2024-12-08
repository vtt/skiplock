require 'cron_parser'
module Skiplock
  class Cron
    def self.setup
      cronjobs = []
      Rails.application.eager_load!
      ActiveJob::Base.descendants.each do |j|
        next unless j.const_defined?('CRON')
        cron = j.const_get('CRON')
        job = Job.find_by('job_class = ? AND cron IS NOT NULL', j.name) || Job.new(job_class: j.name, queue_name: j.queue_as, cron: cron, locale: I18n.locale, timezone: Time.zone.name)
        time = self.next_schedule_at(cron)
        if time
          job.cron = cron
          job.running = false
          job.scheduled_at = Time.at(time) unless job.try(:executions).to_i > 0  # do not update schedule of retrying cron jobs
          job.save
          cronjobs << j.name
        end
      end
      query = Job.where('cron IS NOT NULL')
      query = query.where('job_class NOT IN (?)', cronjobs) if cronjobs.count > 0
      query.delete_all
    rescue
    end

    def self.next_schedule_at(cron)
      time = CronParser.new(cron).next
      time = time + (time <= Time.now ? 60 : Time.now.sec)
      time.to_f  
    rescue
    end
  end
end