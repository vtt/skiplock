module Skiplock
  class Manager
    def initialize(**config)
      @config = Skiplock::DEFAULT_CONFIG.dup
      @config.merge!(YAML.load_file('config/skiplock.yml')) rescue nil
      @config.symbolize_keys!
      @config.transform_values! {|v| v.is_a?(String) ? v.downcase : v}
      @config.merge!(config)
      @config[:hostname] = `hostname -f`.strip
      configure
      setup_logger
      Module.__send__(:include, Skiplock::Extension) if @config[:extensions] == true
      if (caller.any?{ |l| l =~ %r{/rack/} } && @config[:workers] == 0)
        cleanup_workers
        @worker = create_worker
        @worker.start(**@config)
        at_exit { @worker.shutdown }
      end
    rescue Exception => ex
      @logger.error(ex.to_s)
      @logger.error(ex.backtrace.join("\n"))
    end

    def standalone(**options)
      @config.merge!(options)
      Rails.logger.reopen('/dev/null')
      Rails.logger.extend(ActiveSupport::Logger.broadcast(@logger))
      @config[:workers] = 1 if @config[:workers] <= 0
      @config[:standalone] = true
      banner
      cleanup_workers
      @worker = create_worker
      @parent_id = Process.pid
      @shutdown = false
      Signal.trap('INT') { @shutdown = true }
      Signal.trap('TERM') { @shutdown = true }
      Signal.trap('HUP') { setup_logger }
      (@config[:workers] - 1).times do |n|
        fork do
          sleep 1
          worker = create_worker(master: false)
          worker.start(worker_num: n + 1, **@config)
          loop do
            sleep 0.5
            break if @shutdown || Process.ppid != @parent_id
          end
          worker.shutdown
        end
      end
      @worker.start(**@config)
      loop do
        sleep 0.5
        break if @shutdown
      end
      @logger.info "[Skiplock] Terminating signal... Waiting for jobs to finish (up to #{@config[:graceful_shutdown]} seconds)..." if @config[:graceful_shutdown]
      Process.waitall
      @worker.shutdown
      @logger.info "[Skiplock] Shutdown completed."
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
      @logger.info "              Loglevel: #{@config[:loglevel]}"
      @logger.info "               Logfile: #{@config[:logfile] || '(disabled)'}"
      @logger.info "               Workers: #{@config[:workers]}"
      @logger.info "                Queues: #{@config[:queues].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}" if @config[:queues].is_a?(Hash)
      @logger.info "                   PID: #{Process.pid}"
      @logger.info "-"*(title.length)
      @logger.warn "[Skiplock] Custom notification has no registered 'on_error' callback" if Skiplock.on_errors.count == 0
    end

    def cleanup_workers
      Rails.application.eager_load! if Rails.env.development?
      delete_ids = []
      Worker.where(hostname: @config[:hostname]).each do |worker|
        sid = Process.getsid(worker.pid) rescue nil
        delete_ids << worker.id if worker.sid != sid || worker.updated_at < 10.minutes.ago
      end
      Worker.where(id: delete_ids).delete_all if delete_ids.count > 0
    end

    def create_worker(master: true)
      Worker.create!(pid: Process.pid, sid: Process.getsid(), master: master, hostname: @config[:hostname], capacity: @config[:max_threads])
    rescue
      Worker.create!(pid: Process.pid, sid: Process.getsid(), master: false, hostname: @config[:hostname], capacity: @config[:max_threads])
    end

    def configure
      @config[:graceful_shutdown] = 300 if @config[:graceful_shutdown] > 300
      @config[:graceful_shutdown] = nil if @config[:graceful_shutdown] < 0
      @config[:max_retries] = 20 if @config[:max_retries] > 20
      @config[:max_retries] = 0 if @config[:max_retries] < 0
      @config[:max_threads] = 1 if @config[:max_threads] < 1
      @config[:max_threads] = 20 if @config[:max_threads] > 20
      @config[:min_threads] = 0 if @config[:min_threads] < 0
      @config[:workers] = 0 if @config[:workers] < 0
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

    def setup_logger
      @config[:loglevel] = 'info' unless ['debug','info','warn','error','fatal','unknown'].include?(@config[:loglevel].to_s)
      @logger = ActiveSupport::Logger.new(STDOUT)
      @logger.level = @config[:loglevel].to_sym
      Skiplock.logger = @logger
      if @config[:logfile].to_s.length > 0
        @logger.extend(ActiveSupport::Logger.broadcast(::Logger.new(File.join(Rails.root, 'log', @config[:logfile].to_s), 'daily')))
        ActiveJob::Base.logger = nil
      end
    rescue Exception => ex
      puts "Exception with logger: #{ex.to_s}"
      puts ex.backtrace.join("\n")
      Skiplock.on_errors.each { |p| p.call(ex) }
    end
  end
end