module Skiplock
  class Manager
    def initialize(**config)
      @config = Skiplock::DEFAULT_CONFIG.dup
      @config.merge!(YAML.load_file('config/skiplock.yml')) rescue nil
      @config.symbolize_keys!
      @config.transform_values! {|v| v.is_a?(String) ? v.downcase : v}
      @config.merge!(config)
      Module.__send__(:include, Skiplock::Extension) if @config[:extensions] == true
      return unless @config[:standalone] || (caller.any?{ |l| l =~ %r{/rack/} } && @config[:workers] == 0)
      @config[:hostname] = `hostname -f`.strip
      do_config
      banner
      cleanup_workers
      create_worker
      ActiveJob::Base.logger = nil
      if @config[:standalone]
        standalone
      else
        dispatcher = Dispatcher.new(worker: @worker, **@config)
        thread = dispatcher.run
        at_exit do
          dispatcher.shutdown
          thread.join(@config[:graceful_shutdown])
          @worker.delete
        end
      end
    end

  private

    def banner
      title = "[Skiplock] V#{Skiplock::VERSION} (Rails #{Rails::VERSION::STRING} | Ruby #{RUBY_VERSION}-p#{RUBY_PATCHLEVEL})"
      @logger.info "-"*(title.length)
      @logger.info title
      @logger.info "-"*(title.length)
      @logger.info "ClassMethod Extensions: #{@config[:extensions]}"
      @logger.info "      Purge completion: #{@config[:purge_completion]}"
      @logger.info "          Notification: #{@config[:notification]}#{(' (' + @notification + ')') if @config[:notification] == 'auto'}"
      @logger.info "           Max retries: #{@config[:max_retries]}"
      @logger.info "           Min threads: #{@config[:min_threads]}"
      @logger.info "           Max threads: #{@config[:max_threads]}"
      @logger.info "           Environment: #{Rails.env}"
      @logger.info "               Logging: #{@config[:logging]}"
      @logger.info "               Workers: #{@config[:workers]}"
      @logger.info "                Queues: #{@config[:queues].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}" if @config[:queues].is_a?(Hash)
      @logger.info "                   PID: #{Process.pid}"
      @logger.info "-"*(title.length)
    end

    def cleanup_workers
      delete_ids = []
      Worker.where(hostname: @config[:hostname]).each do |worker|
        sid = Process.getsid(worker.pid) rescue nil
        delete_ids << worker.id if worker.sid != sid || worker.updated_at < 30.minutes.ago
      end
      if delete_ids.count > 0
        Job.where(running: true, worker_id: delete_ids).update_all(running: false, worker_id: nil)
        Worker.where(id: delete_ids).delete_all
      end
    end

    def create_worker(pid: Process.pid, sid: Process.getsid(), master: true)
      @worker = Worker.create!(pid: pid, sid: sid, master: master, hostname: @config[:hostname], capacity: @config[:max_threads])
    rescue
      @worker = Worker.create!(pid: pid, sid: sid, master: false, hostname: @config[:hostname], capacity: @config[:max_threads])
    end

    def do_config
      @config[:graceful_shutdown] = 300 if @config[:graceful_shutdown] > 300
      @config[:graceful_shutdown] = nil if @config[:graceful_shutdown] < 0
      @config[:max_retries] = 20 if @config[:max_retries] > 20
      @config[:max_retries] = 0 if @config[:max_retries] < 0
      @config[:max_threads] = 1 if @config[:max_threads] < 1
      @config[:max_threads] = 20 if @config[:max_threads] > 20
      @config[:min_threads] = 0 if @config[:min_threads] < 0
      @config[:workers] = 0 if @config[:workers] < 0
      @config[:workers] = 1 if @config[:standalone] && @config[:workers] <= 0
      @config[:queues].values.each { |v| raise 'Queue value must be an integer' unless v.is_a?(Integer) } if @config[:queues].is_a?(Hash)
      @notification = @config[:notification]
      if @notification == 'auto'
        if defined?(Airbrake)
          @notification = 'airbrake'
        elsif defined?(Bugsnag)
          @notification = 'bugsnag'
        elsif defined?(ExceptionNotifier)
          @notification = 'exception_notification'
        else
          @logger.info "Unable to detect any known exception notification gem. Please define custom 'on_error' callback function and disable 'auto' notification in 'config/skiplock.yml'"
          exit
        end
      end
      case @notification
      when 'airbrake'
        raise 'airbrake gem not found' unless defined?(Airbrake)
        Skiplock.on_error do |ex, previous|
          Airbrake.notify_sync(ex) unless ex.backtrace == previous.try(:backtrace)
        end
      when 'bugsnag'
        raise 'bugsnag gem not found' unless defined?(Bugsnag)
        Skiplock.on_error do |ex, previous|
          Bugsnag.notify(ex) unless ex.backtrace == previous.try(:backtrace)
        end
      when 'exception_notification'
        raise 'exception_notification gem not found' unless defined?(ExceptionNotifier)
        Skiplock.on_error do |ex, previous|
          ExceptionNotifier.notify_exception(ex) unless ex.backtrace == previous.try(:backtrace)
        end
      end
      Skiplock.logger = ActiveSupport::Logger.new(STDOUT)
      Skiplock.logger.level = Rails.logger.level
      @logger = Skiplock.logger
      if @config[:logging]
        Skiplock.logger.extend(ActiveSupport::Logger.broadcast(::Logger.new('log/skiplock.log')))
        if @config[:standalone]
          Rails.logger.reopen('/dev/null')
          Rails.logger.extend(ActiveSupport::Logger.broadcast(Skiplock.logger))
        end
      end
      Skiplock.on_errors.freeze unless Skiplock.on_errors.frozen?
    end

    def standalone
      parent_id = Process.pid
      shutdown = false
      Signal.trap("INT") { shutdown = true }
      Signal.trap("TERM") { shutdown = true }
      (@config[:workers] - 1).times do |n|
        fork do
          sleep 1
          worker = create_worker(master: false)
          dispatcher = Dispatcher.new(worker: worker, worker_num: n + 1, **@config)
          thread = dispatcher.run
          loop do
            sleep 0.5
            break if shutdown || Process.ppid != parent_id
          end
          dispatcher.shutdown
          thread.join(@config[:graceful_shutdown])
          worker.delete
          exit
        end
      end
      dispatcher = Dispatcher.new(worker: @worker, **@config)
      thread = dispatcher.run
      loop do
        sleep 0.5
        break if shutdown
      end
      @logger.info "[Skiplock] Terminating signal... Waiting for jobs to finish (up to #{@config[:graceful_shutdown]} seconds)..." if @config[:graceful_shutdown]
      Process.waitall
      dispatcher.shutdown
      thread.join(@config[:graceful_shutdown])
      @worker.delete
      @logger.info "[Skiplock] Shutdown completed."
    end
  end
end