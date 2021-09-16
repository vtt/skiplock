# Skiplock

`Skiplock` is a background job queuing system that improves the performance and reliability of the job executions while providing the same ACID guarantees as the rest of your data.  It is designed for Active Jobs with Ruby on Rails using PostgreSQL database adapter, but it can be modified to work with other frameworks easily.

It only uses the `LISTEN/NOTIFY/SKIP LOCKED` features provided natively on PostgreSQL 9.5+ to efficiently and reliably dispatch jobs to worker processes and threads ensuring that each job can be completed successfully **only once**.  No other polling or timer is needed.

The library is quite small compared to other PostgreSQL job queues (eg. *delay_job*, *queue_classic*, *que*, *good_job*) with less than 500 lines of codes; and it still provides similar set of features and more...

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
    max_threads: 10
    max_retries: 20
    logfile: skiplock.log
    loglevel: info
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
    - **logfile** (*string*): filename for skiplock logs; empty logfile will disable logging
    - **loglevel** (*string*): sets logging level (`debug, info, warn, error, fatal, unknown`)
    - **notification** (*string*): sets the library to be used for notifying errors and exceptions (`auto, airbrake, bugsnag, exception_notification, custom`); using `auto` will detect library if available.  See `Notification system` for more details
    - **extensions** (*multi*): enable or disable the class method extension.  See `ClassMethod extension` for more details
    - **purge_completion** (*boolean*): when set to **true** will delete jobs after they were completed successfully; if set to **false** then the completed jobs should be purged periodically to maximize performance (eg. clean up old jobs after 3 months); queued jobs can manually override using `purge` option
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
      -l, --logfile STRING             Log filename
      -s, --graceful-shutdown NUM      Number of seconds to wait for graceful shutdown
      -r, --max-retries NUM            Number of maxixum retries
      -t, --max-threads NUM            Number of maximum threads
      -T, --min-threads NUM            Number of minimum threads
      -v, --version                    Show version information
      -w, --workers NUM                Number of workers
      -h, --help                       Show this message
    ```

## Usage
Inside the Rails application:
- queue your job
    ```ruby
    MyJob.perform_later
    ```
- Skiplock supports all ActiveJob features and options
    ```ruby
    MyJob.set(queue: 'my_queue', wait: 5.minutes, priority: 10).perform_later(1,2,3)
    MyJob.set(wait_until: Day.tomorrow.noon).perform_later(1,2,3)
    ```
- Skiplock supports custom options which override the global `Skiplock` configuration options for specified jobs
    - **purge** (*boolean*): whether to remove this job after it has completed successfully
    - **max_retries** (*integer*): set maximum retry attempt for this job
    ```ruby
    MyJob.set(purge: false, max_retries: 5).perform_later(1,2,3)
    ```
Outside the Rails application:
- queue the jobs by inserting the job records directly to the database table
    ```sql
    INSERT INTO skiplock.jobs(job_class) VALUES ('MyJob');
    ```
- with scheduling, priority, queue, arguments and custom options
    ```sql
    INSERT INTO skiplock.jobs(job_class, queue_name, priority, scheduled_at, data)
      VALUES ('MyJob', 'my_queue', 10, NOW() + INTERVAL '5 min',
      '{"arguments":[1,2,3],"options":{"purge":false,"max_retries":5}}');
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
If the retry attempt limit configured in ActiveJob has been reached, then the control will be passed back to `Skiplock` to be marked as an expired job.

If the `retry_on` block is not defined, then the built-in retry system of `Skiplock` will kick in automatically.  The retrying schedule is using an exponential formula (5 + 2**attempt).  The `Skiplock` configuration `max_retries` determines the the limit of attempts before the failing job is marked as expired.  The maximum retry limit can be set as high as 20; this allows up to 12 days of retrying before the job is marked as expired.

## Notification system
`Skiplock` can use existing exception notification library to notify errors and exceptions.  It supports `airbrake`, `bugsnag`, and `exception_notification`.  Custom notification can also be called whenever an exception occurs; it can be configured in an initializer like below:
```ruby
  # config/initializers/skiplock.rb
  Skiplock.on_error do |ex|
    # sends text message using Amazon SNS
    sms = Aws::SNS::Client.new(region: 'us-west-2', access_key_id: Rails.application.credentials[:aws][:access_key_id], secret_access_key: Rails.application.credentials[:aws][:secret_access_key])
    sms.publish(phone_number: '+12223334444', message: "Exception: #{ex.message}"[0..130])
  end
  # supports multiple 'on_error' event callbacks
```
## ClassMethod extension
`Skiplock` can add extension to allow class methods to be performed as a background job; it is disabled in the default configuration.  To enable globally for all classes and modules, edit the `config/skiplock.yml` configuration file and change `extensions` to `true`; this can expose remote code execution if the `skiplock.jobs` database table is not secured properly.

To enable extension for specific classes and modules only then set the configuration to an array of names of the classes and modules eg. `['MyClass', 'MyModule']`
- An example of remote code execution if the extension is enabled globally (ie: configuration is set to `true`) and attacker can insert `skiplock.jobs`
  ```sql
  INSERT INTO skiplock.jobs(job_class, data)
    VALUES ('Skiplock::Extension::ProxyJob',
    '{"arguments":["---\n- !ruby/module ''Kernel''\n- :system\n- - rm -rf /tmp/*\n"]}');
  ```
- Queue class method `generate_thumbnails` of class `Image` as background job to run as soon as possible
  ```ruby
  Image.skiplock.generate_thumbnails(height: 100, ratio: true)
  ```
- Queue class method `cleanup` of class `Session` as background job on queue `maintenance` to run after 5 minutes
  ```ruby
  Session.skiplock(wait: 5.minutes, queue: 'maintenance').cleanup
  ```
- Queue class method `charge` of class `Subscription` as background job to run tomorrow at noon without purging
  ```ruby
  Subscription.skiplock(purge: false, wait_until: Date.tomorrow.noon).charge(amount: 100)
  ```

## Fault tolerant
`Skiplock` ensures that jobs will be executed sucessfully only once even if database connection is lost during or after the job was dispatched.  Successful jobs are marked as completed or removed (with `purge_completion` global configuration or `purge` job option); failed or interrupted jobs are marked for retry.

However, when the database connection is dropped for any reasons and the commit is lost, `Skiplock` will then save the commit data to local disk (as `tmp/skiplock/<job_id>`) and synchronize with the database when the connection resumes.

This also protects long running in-progress jobs that are terminated abruptly during a graceful shutdown with timeout; these will be queued for retry.

## Scalability
`Skiplock` can scale both vertically and horizontally.  To scale vertically, simply increase the number of `Skiplock` workers per host.  To scale horizontally, simply deploy `Skiplock` to multiple hosts sharing the same PostgreSQL database.

## Statistics, analytics and counters
The `skiplock.workers` database table contains all the `Skiplock` workers running on all the hosts.  Active worker will update its timestamp column (`updated_at`) every minute; and dispatched jobs would be associated with the running workers.  At any given time, a list of active workers running a list of jobs can be determined using the database table.

The `skiplock.jobs` database table contains all the `Skiplob` jobs.  Each job's successful execution stores the result to its `data['result']` field column.  If job completions are not purged then their execution results can be used for analytic purposes.

The `skiplock.counters` database table contains all the counters for job dispatches, completions, expiries, failures and retries.  The counters are recorded by dates; so it's possible to get statistical data for any given day or range of dates.
  - **completions**: numbers of jobs completed successfully
  - **dispatches**: number of jobs dispatched for the first time (**retries** are not counted here)
  - **expiries**: number of jobs exceeded `max_retries` and still failed to complete
  - **failures**: number of jobs interrupted by graceful shutdown or unable to complete due to errors (exceptions)
  - **retries**: number of jobs dispatched for retrying

Code examples of gathering counters information:
  - get counter information for today
    ```ruby
    Skiplock::Counter.where(day: Date.today).first
    ```
  - get total number of successfully completed jobs within the past 30 days
    ```ruby
    Skiplock::Counter.where("day >= ?", 30.days.ago).sum(:completions)
    ```
  - get total number of expired jobs
    ```ruby
    Skiplock::Counter.sum(:expiries)
    ```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/vtt/skiplock.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
