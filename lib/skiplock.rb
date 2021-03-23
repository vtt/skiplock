require 'active_job'
require 'active_job/queue_adapters/skiplock_adapter'
require 'active_record'
require 'skiplock/cron'
require 'skiplock/dispatcher'
require 'skiplock/manager'
require 'skiplock/job'
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
  mattr_accessor :on_error
end