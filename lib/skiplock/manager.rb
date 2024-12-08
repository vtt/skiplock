module Skiplock
  class Manager
    def initialize
      @config = Skiplock::DEFAULT_CONFIG.dup
      @config.merge!(YAML.load_file('config/skiplock.yml')) rescue nil
      @config.symbolize_keys!
      async if (caller.any?{ |l| l =~ %r{/rack/} } && @config[:workers] == 0)
    end

    def async
      setup_logger
      configure
      Worker.cleanup(@hostname)
      @worker = Worker.generate(capacity: @config[:max_threads], hostname: @hostname)
      Cron.setup if @worker.master
      @worker.start(**@config)
      at_exit { @worker.shutdown }
    rescue Exception => ex
      @logger.error(ex.to_s)
      @logger.error(ex.backtrace.join("\n"))
    end

    def standalone(**options)
      @config.merge!(options)
      @config[:standalone] = true
      @config[:workers] = 1 if @config[:workers] <= 0
      setup_logger
      configure
      banner
      @parent_id = Process.pid
      @shutdown = false
      Signal.trap('INT') { @shutdown = true }
      Signal.trap('TERM') { @shutdown = true }
      Signal.trap('HUP') { setup_logger }
      Worker.cleanup(@hostname)
      @worker = Worker.generate(capacity: @config[:max_threads], hostname: @hostname)
      ActiveRecord::Base.connection.disconnect! if @config[:workers] > 1
      (@config[:workers] - 1).times do |n|
        fork do
          sleep(0.25*n + 1)
          ActiveRecord::Base.establish_connection
          worker = Worker.generate(capacity: @config[:max_threads], hostname: @hostname, master: false)
          worker.start(worker_num: n + 1, **@config)
          loop do
            sleep 0.5
            break if @shutdown || Process.ppid != @parent_id
          end
          worker.shutdown
        end
      end
      ActiveRecord::Base.establish_connection if @config[:workers] > 1
      @worker.start(**@config)
      loop do
        sleep 0.5
        break if @shutdown
      end
      @logger.info "[Skiplock] Terminating signal... Waiting for jobs to finish (up to #{@config[:graceful_shutdown]} seconds)..." if @config[:graceful_shutdown]
      Process.waitall
      @worker.shutdown
    rescue Exception => ex
      @logger.error(ex.to_s)
      @logger.error(ex.backtrace.join("\n"))
    end

    private

    def banner
      title = "Skiplock #{Skiplock::VERSION} (Rails #{Rails::VERSION::STRING} | Ruby #{RUBY_VERSION}-p#{RUBY_PATCHLEVEL})"
      @logger.info "-"*(title.length)
      @logger.info title
      @logger.info "-"*(title.length)
      @logger.info "  ClassMethod extensions: #{@config[:extensions]}"
      @logger.info "        Purge completion: #{@config[:purge_completion]}"
      @logger.info "            Notification: #{@config[:notification]}"
      @logger.info "             Max retries: #{@config[:max_retries]}"
      @logger.info "             Min threads: #{@config[:min_threads]}"
      @logger.info "             Max threads: #{@config[:max_threads]}"
      @logger.info "             Environment: #{Rails.env}"
      @logger.info "               Namespace: #{@config[:namespace] || '(nil)'}"
      @logger.info "               Log level: #{@config[:log_level]}"
      if @config[:log_file]
        @logger.info "               Log count: #{@config[:log_count]}"
        @logger.info "                Log size: #{@config[:log_size]}"
      end
      @logger.info "                Log file: #{@config[:log_file] || '(disabled)'}"
      @logger.info "                 Workers: #{@config[:workers]}"
      @logger.info "                  Queues: #{@config[:queues].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}" if @config[:queues].is_a?(Hash)
      @logger.info "                     PID: #{Process.pid}"
      @logger.info "-"*(title.length)
      @logger.warn "[Skiplock] Custom notification has no registered 'on_error' callback" if Skiplock.on_errors.count == 0
    end

    def configure
      if @config[:extensions] == true
        Module.__send__(:include, Skiplock::Extension)
      elsif @config[:extensions].is_a?(Array)
        @config[:extensions].each { |n| n.constantize.__send__(:extend, Skiplock::Extension) if n.safe_constantize }
      end
      ActiveJob::Base.__send__(:include, Skiplock::Patch)
      Skiplock.namespace = @config[:namespace]
      @hostname = "#{`hostname -f`.strip}|#{Socket.ip_address_list.reject(&:ipv4_loopback?).reject(&:ipv6?).map(&:ip_address).join('|')}"
      @config.transform_values! {|v| v.is_a?(String) ? v.downcase : v}
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
        Skiplock.on_error do |ex|
          Airbrake.notify_sync(ex)
        end
      when 'bugsnag'
        raise 'bugsnag gem not found' unless defined?(Bugsnag)
        Skiplock.on_error do |ex|
          Bugsnag.notify(ex)
        end
      when 'exception_notification'
        raise 'exception_notification gem not found' unless defined?(ExceptionNotifier)
        Skiplock.on_error do |ex|
          ExceptionNotifier.notify_exception(ex)
        end
      else
        @config[:notification] = 'custom'
      end
      Skiplock.on_errors.freeze
    end

    def setup_logger
      @config[:log_level] = 'info' unless ['debug','info','warn','error','fatal','unknown'].include?(@config[:log_level].to_s)
      if defined?(ActiveSupport::BroadcastLogger)
        @logger = ActiveSupport::BroadcastLogger.new(::Logger.new(STDOUT))
      else
        @logger = ActiveSupport::Logger.new(STDOUT)
      end
      @logger.level = @config[:log_level].to_sym
      Skiplock.logger = @logger
      if @config[:log_file].to_s.length > 0
        if defined?(ActiveSupport::BroadcastLogger)
          @logger.broadcast_to(::Logger.new(File.join(Rails.root, 'log', @config[:log_file].to_s), @config[:log_count] || 5, @config[:log_size] || 10485760))
        else
          @logger.extend(ActiveSupport::Logger.broadcast(::Logger.new(File.join(Rails.root, 'log', @config[:log_file].to_s), @config[:log_count] || 5, @config[:log_size] || 10485760)))
        end
        ActiveJob::Base.logger = @logger
      end
      if @config[:standalone]
        if defined?(ActiveSupport::BroadcastLogger)
          Rails.logger = @logger
        else
          Rails.logger.reopen('/dev/null') rescue Rails.logger.reopen('NUL') # supports Windows NUL device
          Rails.logger.level = @logger.level
          Rails.logger.extend(ActiveSupport::Logger.broadcast(@logger))
        end
      end
    rescue Exception => ex
      @logger.error "Exception with logger: #{ex.to_s}"
      @logger.error ex.backtrace.join("\n")
      Skiplock.on_errors.each { |p| p.call(ex) }
    end
  end
end
