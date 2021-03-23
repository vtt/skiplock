module Skiplock
  class Manager
    def self.start(standalone: false, workers: nil, max_retries: nil, max_threads: nil, min_threads: nil, logging: nil)
      unless Settings.frozen?
        load_settings
        Settings['logging'] = logging if logging
        Settings['max_retries'] = max_retries if max_retries
        Settings['max_threads'] = max_threads if max_threads
        Settings['min_threads'] = min_threads if min_threads
        Settings['workers'] = workers if workers
        Settings['max_retries'] = 20 if Settings['max_retries'] > 20
        Settings['max_retries'] = 0 if Settings['max_retries'] < 0
        Settings['max_threads'] = 1 if Settings['max_threads'] < 1
        Settings['max_threads'] = 20 if Settings['max_threads'] > 20
        Settings['min_threads'] = 0 if Settings['min_threads'] < 0
        Settings['workers'] = 0 if Settings['workers'] < 0
        Settings.freeze
      end
      return unless standalone || (caller.any?{|l| l =~ %r{/rack/}} && (Settings['workers'] == 0 || Rails.env.development?))
      if standalone
        self.standalone
      else
        @dispatcher = Dispatcher.new
        @thread = @dispatcher.run
        at_exit { self.shutdown }
      end
    end
    
    def self.shutdown(wait: true)
      if @dispatcher && @thread
        @dispatcher.shutdown(wait: wait)
        @thread.join
      end
    end

  private

    def self.load_settings
      config = YAML.load_file('config/skiplock.yml') rescue {}
      Settings.merge!(config)
      Settings['queues'].values.each { |v| raise 'Queue value must be an integer' unless v.is_a?(Integer) } if Settings['queues'].is_a?(Hash)
      case Settings['notification'].to_s.downcase
      when 'auto'
        if defined?(Airbrake)
          Skiplock.on_error = -> (ex, previous = nil) { Airbrake.notify_sync(ex) unless ex.backtrace == previous.try(:backtrace) }
        elsif defined?(Bugsnag)
          Skiplock.on_error = -> (ex, previous = nil) { Bugsnag.notify(ex) unless ex.backtrace == previous.try(:backtrace) }
        elsif defined?(ExceptionNotifier)
          Skiplock.on_error = -> (ex, previous = nil) { ExceptionNotifier.notify_exception(ex) unless ex.backtrace == previous.try(:backtrace) }
        else
          puts "Unable to detect any known exception notification gem. Please define custom 'on_error' function and disable notification in 'config/skiplock.yml'"
          exit
        end
      when 'airbrake'
        raise 'airbrake gem not found' unless defined?(Airbrake)
        Skiplock.on_error = -> (ex, previous = nil) { Airbrake.notify_sync(ex) unless ex.backtrace == previous.try(:backtrace) }
      when 'bugsnag'
        raise 'bugsnag gem not found' unless defined?(Bugsnag)
        Skiplock.on_error = -> (ex, previous = nil) { Bugsnag.notify(ex) unless ex.backtrace == previous.try(:backtrace) }
      when 'exception_notification'
        raise 'exception_notification gem not found' unless defined?(ExceptionNotifier)
        Skiplock.on_error = -> (ex, previous = nil) { ExceptionNotifier.notify_exception(ex) unless ex.backtrace == previous.try(:backtrace) }
      end
    rescue Exception => e
      STDERR.puts "Invalid configuration 'config/skiplock.yml': #{e.message}"
      exit
    end

    def self.standalone
      if Settings['logging']
        log_timestamp = (Settings['logging'].to_s == 'timestamp')
        logfile = File.open('log/skiplock.log', 'a')
        logfile.sync = true
        $stdout = Demux.new(logfile, STDOUT, timestamp: log_timestamp)
        errfile = File.open('log/skiplock.error.log', 'a')
        errfile.sync = true
        $stderr = Demux.new(errfile, STDERR, timestamp: log_timestamp)
        logger = ActiveSupport::Logger.new($stdout)
        logger.level = Rails.logger.level
        Rails.logger.reopen('/dev/null')
        Rails.logger.extend(ActiveSupport::Logger.broadcast(logger))
      end
      title = "Skiplock version: #{Skiplock::VERSION} (Ruby #{RUBY_VERSION}-p#{RUBY_PATCHLEVEL})"
      puts "-"*(title.length)
      puts title
      puts "-"*(title.length)
      puts "Purge completion: #{Settings['purge_completion']}"
      puts "    Notification: #{Settings['notification']}"
      puts "     Max retries: #{Settings['max_retries']}"
      puts "     Min threads: #{Settings['min_threads']}"
      puts "     Max threads: #{Settings['max_threads']}"
      puts "     Environment: #{Rails.env}"
      puts "         Logging: #{Settings['logging']}"
      puts "         Workers: #{Settings['workers']}"
      puts "          Queues: #{Settings['queues'].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}" if Settings['queues'].is_a?(Hash)
      puts "             PID: #{Process.pid}"
      puts "-"*(title.length)
      parent_id = Process.pid
      shutdown = false
      Signal.trap("INT") { shutdown = true }
      Signal.trap("TERM") { shutdown = true }
      worker_pids = []
      (Settings['workers']-1).times do |n|
        worker_pids << fork do
          dispatcher = Dispatcher.new(master: false, worker_num: n+1)
          thread = dispatcher.run
          loop do
            sleep 0.5
            break if shutdown || Process.ppid != parent_id
          end
          dispatcher.shutdown(wait: true)
          thread.join
          exit
        end
      end
      sleep 0.1
      dispatcher = Dispatcher.new(worker_pids: worker_pids)
      thread = dispatcher.run
      loop do
        sleep 0.5
        break if shutdown
      end
      Process.waitall
      dispatcher.shutdown(wait: true)
      thread.join
    end

    class Demux
      def initialize(*targets, timestamp: true)
        @targets = targets
        @timestamp = timestamp
      end

      def close
        @targets.each(&:close)
      end

      def flush
        @targets.each(&:flush)
      end
  
      def tty?
        true
      end

      def write(*args)
        args.prepend("[#{Time.now.utc}]: ") if @timestamp
        @targets.each {|t| t.write(*args)}
      end
    end
  end
end