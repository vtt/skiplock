require 'active_job'
require 'active_job/queue_adapters/skiplock_adapter'
require 'active_record'
require 'skiplock/cron'
require 'skiplock/dispatcher'
require 'skiplock/manager'
require 'skiplock/notification'
require 'skiplock/job'
require 'skiplock/version'

module Skiplock
  Settings = { 
    'logging' => :timestamp,
    'min_threads' => 1,
    'max_threads' => 5,
    'max_retries' => 20,
    'purge_completion' => true,
    'workers' => 0
  }
end