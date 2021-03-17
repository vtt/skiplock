# Skiplock

Skiplock is a background job queuing system that greatly improves the performance and reliability of the job executions at the same time providing the same ACID guarantees as the rest of your data.  It is designed for Active Jobs with Ruby on Rails using PostgreSQL database adapter; but it can be modified easily to work with other frameworks.

It only uses the **LISTEN/NOTIFY/SKIP LOCKED** features provided natively on PostgreSQL 9.2+ to efficiently and reliably dispatch jobs to worker processes and threads ensuring that a job can be completed successfully only once.  No other polling or timer is needed.

The library is quite small compared to other PostgreSQL job queues (eg. *delay_job*, *queue_classic*, *que*, *good_job*) with less than 400 lines of codes; and it still provides similar set of features and more...

## Installation

1. Add `skiplock` to your application's Gemfile:

    ```ruby
    gem 'skiplock'
    ```

2. Install the gem:

    ```bash
    bundle install
    ```

3. Run the Skiplock install generator. This will generate a configuration file and database migration to store the job records:

    ```bash
    rails g skiplock:install
    ```

4. Run the migration:

    ```bash
    rails db:migrate
    ```

## Configuration

1. Configure the ActiveJob adapter:

    ```ruby
    # config/application.rb
    config.active_job.queue_adapter = :skiplock
    ```
2. Skiplock configuration
    ```yaml
    # config/skiplock.yml
    ---
    logging: timestamp
    min_threads: 1
    max_threads: 5
    max_retries: 20
    purge_completion: true
    workers: 0
    ```
    Available configuration options are:
    - **logging** (*enumeration*): sets the logging capability to **true** or **false**; setting to **timestamp** will enable logging with timestamps
    - **min_threads** (*integer*): sets minimum number of threads staying idle
    - **max_threads** (*integer*): sets the maximum number of threads allowed to run jobs
    - **max_retries** (*integer*): sets the maximum attempt a job will be retrying before it is marked expired (see Retry System for more details)
    - **purge_completion** (*boolean*): when set to **true** will delete jobs after they were completed successfully; if set to **false** then the completed jobs should be purged periodically to maximize performance (eg. clean up old jobs after 3 months)
    - **workers** (*integer*) sets the maximum number of processes when running in standalone mode using the **skiplock** executable; setting this to 0 will enable async mode
    
    #### Async mode
    When **workers** is set to **0** then the jobs will be performed in the web server process using separate threads.  If using multi-worker cluster web server like Puma, then it should be configured as below:
    ```ruby
    # config/puma.rb
    before_fork do
      # ...
      Skiplock::Manager.shutdown
    end

    on_worker_boot do
      # ...
      Skiplock::Manager.start
    end

    on_worker_shutdown do
      # ...
      Skiplock::Manager.shutdown
    end
    ```

## Usage

- Inside the Rails application, queue your job:
    ```ruby
    MyJob.perform_later
    ```
- Skiplock supports all ActiveJob features:
    ```ruby
    MyJob.set(wait: 5.minutes, priority: 10).perform_later(1,2,3)
    ```
- Outside of Rails application, queue the jobs by inserting the Job records directly to the database table eg:
    ```sql
    INSERT INTO skiplock.jobs(job_class) VALUES ('MyJob');
    ```
- Or with scheduling, priority and arguments:
    ```sql
    INSERT INTO skiplock.jobs(job_class, priority, scheduled_at, data) VALUES ('MyJob', 10, NOW() + INTERVAL '5 min', '{"arguments":[1,2,3]}');
    ```
## Cron system
Skiplock supports cron jobs for running tasks periodically.  It fully supports the cron syntax to specify the frequency of the jobs.  To setup a job with cron capability, simply assign a valid cron expression to constant CRON for the Job Class.
- setup MyJob to run as cron job every hour at 30 minutes past
    ```ruby
    class MyJob < ActiveJob::Base
      CRON = "30 * * * *"
      # ...
    end
    ```
- setup MyJob to run at midnight every Wednesday
    ```
    class MyJob < ActiveJob::Base
      CRON = "0 0 * * 3"
      # ...
    end
    ```
## Retry system   
...
## Notification system    
...
## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/vtt/skiplock.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
