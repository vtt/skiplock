# Skiplock

`Skiplock` is a background job queuing system that improves the performance and reliability of the job executions while providing the same ACID guarantees as the rest of your data.  It is designed for Active Jobs with Ruby on Rails using PostgreSQL database adapter, but it can be modified to work with other frameworks easily.

It only uses the `LISTEN/NOTIFY/SKIP LOCKED` features provided natively on PostgreSQL 9.5+ to efficiently and reliably dispatch jobs to worker processes and threads ensuring that each job can be completed successfully **only once**.  No other polling or timer is needed.

The library is quite small compared to other PostgreSQL job queues (eg. *delay_job*, *queue_classic*, *que*, *good_job*) with less than 400 lines of codes; and it still provides similar set of features and more...

#### Compatibility:

- MRI Ruby 2.5+
- PostgreSQL 9.5+
- Rails 5.2+

## Installation

1. Add `Skiplock` to your application's Gemfile:

    ```ruby
    gem 'skiplock'
    ```

2. Install the gem:

    ```bash
    $ bundle install
    ```

3. Run the `Skiplock` install generator. This will generate a configuration file and database migration to store the job records:

    ```bash
    $ rails g skiplock:install
    ```

4. Run the migration:

    ```bash
    $ rails db:migrate
    ```

## Configuration

1. Configure the ActiveJob adapter:

    ```ruby
    # config/application.rb
    config.active_job.queue_adapter = :skiplock
    ```
2. `Skiplock` configuration
    ```yaml
    # config/skiplock.yml (default settings)
    ---
    min_threads: 1
    max_threads: 5
    max_retries: 20
    logfile: log/skiplock.log
    notification: custom
    extensions: false
    purge_completion: true
    queues:
      default: 200
      mailers: 999
    workers: 0
    ```
    Available configuration options are:
    - **min_threads** (*integer*): sets minimum number of threads staying idle
    - **max_threads** (*integer*): sets the maximum number of threads allowed to run jobs
    - **max_retries** (*integer*): sets the maximum attempt a job will be retrying before it is marked expired.  See `Retry system` for more details
    - **logfile** (*string*): path filename for skiplock logs; empty logfile will disable logging
    - **notification** (*string*): sets the library to be used for notifying errors and exceptions (`auto, airbrake, bugsnag, exception_notification, custom`); using `auto` will detect library if available.  See `Notification system` for more details
    - **extensions** (*boolean*): enable or disable the class method extension.  See `ClassMethod extension` for more details
    - **purge_completion** (*boolean*): when set to **true** will delete jobs after they were completed successfully; if set to **false** then the completed jobs should be purged periodically to maximize performance (eg. clean up old jobs after 3 months)
    - **queues** (*hash*): defines the set of queues with priorities; lower priority takes precedence
    - **workers** (*integer*) sets the maximum number of processes when running in standalone mode using the `skiplock` executable; setting this to **0** will enable **async mode**

    #### **Async mode**
    When **workers** is set to **0** then the jobs will be performed in the web server process using separate threads.  If using multi-worker cluster mode web server like Puma, then all the Puma workers will also be able to perform `Skiplock` jobs.

    #### **Standalone mode**
    `Skiplock` standalone mode can be launched by using the `skiplock` executable; command line options can be provided to override the `Skiplock` configuration file.  
    ```
    $ bundle exec skiplock -h
    Usage: skiplock [options]
      -e, --environment STRING         Rails environment
      -l, --logfile STRING             Full path to logfile
      -s, --graceful-shutdown NUM      Number of seconds to wait for graceful shutdown
      -r, --max-retries NUM            Number of maxixum retries
      -t, --max-threads NUM            Number of maximum threads
      -T, --min-threads NUM            Number of minimum threads
      -w, --workers NUM                Number of workers
      -h, --help                       Show this message
    ```

## Usage
Inside the Rails application:
- queue your job
    ```ruby
    MyJob.perform_later
    ```
- Skiplock supports all ActiveJob features
    ```ruby
    MyJob.set(queue: 'my_queue', wait: 5.minutes, priority: 10).perform_later(1,2,3)
    ```
Outside the Rails application:
- queue the jobs by inserting the job records directly to the database table
    ```sql
    INSERT INTO skiplock.jobs(job_class) VALUES ('MyJob');
    ```
- with scheduling, priority, queue and arguments
    ```sql
    INSERT INTO skiplock.jobs(job_class, queue_name, priority, scheduled_at, data)
      VALUES ('MyJob', 'my_queue', 10, NOW() + INTERVAL '5 min', '{"arguments":[1,2,3]}');
    ```
## Queue priority vs Job priority
*Why do queues use priorities when jobs already have priorities?*
- Jobs are only prioritized with other jobs from the same queue
- Queues, on the other hand, are prioritized with other queues
- Rails has built-in queues that dispatch jobs without priorities (eg. Mail Delivery will queue as **mailers** with no priority)

## Cron system
`Skiplock` provides the capability to setup cron jobs for running tasks periodically.  It fully supports the cron syntax to specify the frequency of the jobs.  To setup a cron job, simply assign a valid cron schedule to the constant `CRON` for the Job Class.
- setup `MyJob` to run as cron job every hour at 30 minutes past

    ```ruby
    class MyJob < ActiveJob::Base
      CRON = "30 * * * *"
      # ...
    end
    ```
- setup `CleanupJob` to run at midnight every Wednesdays
    ```ruby
    class CleanupJob < ApplicationJob
      CRON = "0 0 * * 3"
      # ...
    end
    ```
- to remove the cron schedule from the job, simply comment out the constant definition or delete the line then re-deploy the application. At startup, the cron jobs that were undefined will be removed automatically

## Retry system
`Skiplock` fully supports ActiveJob built-in retry system.  It also has its own retry system for fallback.  To use ActiveJob retry system, define the `retry_on` block per ActiveJob's documentation.
- configures `MyJob` to retry at maximum 20 attempts on StandardError with fixed delay of 5 seconds
    ```ruby
    class MyJob < ActiveJob::Base
      retry_on StandardError, wait: 5, attempts: 20
    # ...
    end
    ```

- configures `MyJob` to retry at maximum 10 attempts on StandardError with exponential delay
    ```ruby
    class MyJob < ActiveJob::Base
      retry_on StandardError, wait: :exponentially_longer, attempts: 10
    # ...
    end
    ```
If the retry attempt limit configured in ActiveJob has been reached, then the control will be passed back to `skiplock` to be marked as an expired job.

If the `retry_on` block is not defined, then the built-in retry system of `skiplock` will kick in automatically.  The retrying schedule is using an exponential formula (5 + 2**attempt).  The `skiplock` configuration `max_retries` determines the the limit of attempts before the failing job is marked as expired.  The maximum retry limit can be set as high as 20; this allows up to 12 days of retrying before the job is marked as expired.

## Notification system
`Skiplock` can use existing exception notification library to notify errors and exceptions.  It supports `airbrake`, `bugsnag`, and `exception_notification`.  Custom notification can also be called whenever an exception occurs; it can be configured in an initializer like below:
```ruby
  # config/initializers/skiplock.rb
  Skiplock.on_error do |ex, previous|
    if ex.backtrace != previous.try(:backtrace)
      # sends custom email on new exceptions only
      # the same repeated exceptions will only be sent once to avoid SPAM
      # NOTE: exceptions generated from Job executions will not provide 'previous' exceptions
    end
  end
  # supports multiple 'on_error' event callbacks
```
## ClassMethod extension
`Skiplock` can add extension to allow all class methods to be performed as a background job; it is disabled in the default configuration.  To enable, edit the `config/skiplock.yml` configuration file and change `extensions` to `true`.
- Queue class method `generate_thumbnails` of class `Image` as background job to run as soon as possible
  ```ruby
  Image.skiplock.generate_thumbnails(height: 100, ratio: true)
  ```
- Queue class method `cleanup` of class `Session` as background job on queue `maintenance` to run after 5 minutes
  ```ruby
  Session.skiplock(wait: 5.minutes, queue: 'maintenance').cleanup
  ```
- Queue class method `charge` of class `Subscription` as background job to run tomorrow at noon
  ```ruby
  Subscription.skiplock(wait_until: Date.tomorrow.noon).charge(amount: 100)
  ```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/vtt/skiplock.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
