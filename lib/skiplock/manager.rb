module Skiplock
  class Manager
    def initialize(**config)
      @config = Skiplock::DEFAULT_CONFIG.dup
      @config.merge!(YAML.load_file('config/skiplock.yml')) rescue nil
      @config.symbolize_keys!
      @config.transform_values! {|v| v.is_a?(String) ? v.downcase : v}
      @config.merge!(config)
      Module.__send__(:include, Skiplock::Extension) if @config[:extensions] == true
      return unless @config[:standalone] || (caller.any?{ |l| l =~ %r{/rack/} } && (@config[:workers] == 0 || Rails.env.development?))
      @config[:hostname] = `hostname -f`.strip
      do_config
      banner if @config[:standalone]
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
    rescue Exception => ex
      @logger.error(ex)
    end

  private

    def banner
      title = "Skiplock #{Skiplock::VERSION} (Rails #{Rails::VERSION::STRING} | Ruby #{RUBY_VERSION}-p#{RUBY_PATCHLEVEL})"
      @logger.info "-"*(title.length)
      @logger.info title
      @logger.info "-"*(title.length)
      @logger.info "ClassMethod extensions: #{@config[:extensions]}"
      @logger.info "      Purge completion: #{@config[:purge_completion]}"
      @logger.info "          Notification: #{@config[:notification]}"
      @logger.info "           Max retries: #{@config[:max_retries]}"
      @logger.info "           Min threads: #{@config[:min_threads]}"
      @logger.info "           Max threads: #{@config[:max_threads]}"
      @logger.info "           Environment: #{Rails.env}"
      @logger.info "               Logfile: #{@config[:logfile] || '(disabled)'}"
      @logger.info "               Workers: #{@config[:workers]}"
      @logger.info "                Queues: #{@config[:queues].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}" if @config[:queues].is_a?(Hash)
      @logger.info "                   PID: #{Process.pid}"
      @logger.info "-"*(title.length)
      @logger.warn "[Skiplock] Custom notification has no registered 'on_error' callback" if Skiplock.on_errors.count == 0
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
      @logger = ActiveSupport::Logger.new(STDOUT)
      @logger.level = Rails.logger.level
      Skiplock.logger = @logger
      raise "Cannot create logfile '#{@config[:logfile]}'" if @config[:logfile] && !File.writable?(File.dirname(@config[:logfile]))
      @config[:logfile] = nil if @config[:logfile].to_s.length == 0
      if @config[:logfile]
        @logger.extend(ActiveSupport::Logger.broadcast(::Logger.new(@config[:logfile])))
        if @config[:standalone]
          Rails.logger.reopen('/dev/null')
          Rails.logger.extend(ActiveSupport::Logger.broadcast(@logger))
        end
      end
      @config[:queues].values.each { |v| raise 'Queue value must be an integer' unless v.is_a?(Integer) } if @config[:queues].is_a?(Hash)
      if @config[:notification] == 'auto'
        if defined?(Airbrake)
          @config[:notification] = 'airbrake'
        elsif defined?(Bugsnag)
          @config[:notification] = 'bugsnag'
        elsif defined?(ExceptionNotifier)
          @config[:notification] = 'exception_notification'
        else
          raise "Unable to detect any known exception notification library. Please define custom 'on_error' event callbacks and change to 'custom' notification in 'config/skiplock.yml'"
        end
      end
      case @config[:notification]
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
      else
        @config[:notification] = 'custom'
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