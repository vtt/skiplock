require 'active_job'
require 'active_job/queue_adapters/skiplock_adapter'
require 'active_record'
require 'skiplock/counter'
require 'skiplock/cron'
require 'skiplock/dispatcher'
require 'skiplock/job'
require 'skiplock/manager'
require 'skiplock/worker'
require 'skiplock/version'

module Skiplock
  Settings = { 
    'logging' => 'timestamp',
    'min_threads' => 1,
    'max_threads' => 5,
    'max_retries' => 20,
    'notification' => 'auto',
    'purge_completion' => true,
    'queues' => {
      'default' => 200,
      'mailers' => 100
    },
    'workers' => 0
  }
  mattr_reader :on_errors, default: []

  def self.on_error(&block)
    @@on_errors << block
  end

  def self.table_name_prefix
    'skiplock.'
  end
end