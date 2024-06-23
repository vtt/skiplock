require 'active_job'
require 'active_job/queue_adapters/skiplock_adapter'
require 'active_record'
require 'skiplock/counter'
require 'skiplock/cron'
require 'skiplock/extension'
require 'skiplock/job'
require 'skiplock/manager'
require 'skiplock/patch'
require 'skiplock/worker'
require 'skiplock/version'

module Skiplock
  DEFAULT_CONFIG = { 'graceful_shutdown' => 15, 'min_threads' => 1, 'max_threads' => 10, 'max_retries' => 20, 'log_file' => 'skiplock.log', 'log_level' => 'info', 'log_count' => 5, 'log_size' => 10485760, 'namespace' => nil, 'notification' => 'custom', 'extensions' => false, 'purge_completion' => true, 'queues' => { 'default' => 100, 'mailers' => 999 }, 'workers' => 0 }.freeze

  def self.logger=(l)
    @logger = l
  end

  def self.logger
    @logger
  end

  def self.namespace=(n)
    @namespace = n
  end

  def self.namespace
    @namespace || ''
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