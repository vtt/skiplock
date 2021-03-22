module Skiplock
  class Manager
    def self.start(standalone: false)
      load_settings
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
      return if Settings.frozen?
      config = YAML.load_file('config/skiplock.yml') rescue {}
      Settings.merge!(config)
      Settings['max_retries'] = 20 if Settings['max_retries'] > 20
      Settings['max_retries'] = 0 if Settings['max_retries'] < 0
      Settings['max_threads'] = 1 if Settings['max_threads'] < 1
      Settings['max_threads'] = 20 if Settings['max_threads'] > 20
      Settings['min_threads'] = 0 if Settings['min_threads'] < 0
      Settings['workers'] = 0 if Settings['workers'] < 0
      Settings['queues'].values.each { |v| raise 'Queue value must be an integer' unless v.is_a?(Integer) }
      Settings.freeze
    rescue Exception => e
      STDERR.puts "Invalid configuration 'config/skiplock.yml': #{e.message}"
      exit
    end

    def self.standalone
      title = "Skiplock version: #{Skiplock::VERSION} (Ruby #{RUBY_VERSION}-p#{RUBY_PATCHLEVEL})"
      puts "-"*(title.length)
      puts title
      puts "-"*(title.length)
      puts "Additional workers: #{Settings['workers']}"
      puts "  Purge completion: #{Settings['purge_completion']}"
      puts "       Max retries: #{Settings['max_retries']}"
      puts "       Min threads: #{Settings['min_threads']}"
      puts "       Max threads: #{Settings['max_threads']}"
      puts "       Environment: #{Rails.env}"
      puts "           Logging: #{Settings['logging']}"
      puts "            Queues: #{Settings['queues'].map {|k,v| k + '(' + v.to_s + ')'}.join(', ')}"
      puts "               PID: #{Process.pid}"
      puts "-"*(title.length)
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
      parent_id = Process.pid
      shutdown = false
      Signal.trap("INT") { shutdown = true }
      Signal.trap("TERM") { shutdown = true }
      worker_pids = []
      Settings['workers'].times do |n|
        worker_pids << fork do
          dispatcher = Dispatcher.new(master: false, worker_num: n)
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