require 'cron_parser'
module Skiplock
  class Cron
    def self.setup
      cronjobs = []
      ActiveJob::Base.descendants.each do |j|
        next unless j.const_defined?('CRON')
        cron = j.const_get('CRON')
        job = Job.find_by('job_class = ? AND cron IS NOT NULL', j.name) || Job.new(job_class: j.name, cron: cron, locale: I18n.locale, timezone: Time.zone.name)
        time = self.next_schedule_at(cron)
        if time
          job.cron = cron
          job.running = false
          job.scheduled_at = Time.at(time)
          job.save
          cronjobs << j.name
        end
      end
      query = Job.where('cron IS NOT NULL')
      query = query.where('job_class NOT IN (?)', cronjobs) if cronjobs.count > 0
      query.delete_all
    end

    def self.next_schedule_at(cron)
      time = CronParser.new(cron).next
      time = time + (time <= Time.now ? 60 : Time.now.sec)
      time.to_f  
    rescue
    end
  end
end