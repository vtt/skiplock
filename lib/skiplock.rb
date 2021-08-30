require 'active_job'
require 'active_job/queue_adapters/skiplock_adapter'
require 'active_record'
require 'skiplock/counter'
require 'skiplock/cron'
require 'skiplock/dispatcher'
require 'skiplock/extension'
require 'skiplock/job'
require 'skiplock/manager'
require 'skiplock/worker'
require 'skiplock/version'

module Skiplock
  DEFAULT_CONFIG = { 'extensions' => false, 'logfile' => 'log/skiplock.log', 'loglevel' => 'info', 'graceful_shutdown' => 15, 'min_threads' => 1, 'max_threads' => 5, 'max_retries' => 20, 'notification' => 'custom', 'purge_completion' => true, 'queues' => { 'default' => 100, 'mailers' => 999 }, 'workers' => 0 }.freeze

  def self.logger=(l)
    @logger = l
  end

  def self.logger
    @logger
  end

  def self.on_error(&block)
    @on_errors ||= []
    @on_errors << block
    block
  end

  def self.on_errors
    @on_errors || []
  end

  def self.table_name_prefix
    'skiplock.'
  end
end