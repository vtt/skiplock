require 'rails/generators'
require 'rails/generators/active_record'

module Skiplock
  class InstallGenerator < Rails::Generators::Base
    include Rails::Generators::Migration
    source_paths << File.join(File.dirname(__FILE__), 'templates')
    desc 'Add configuration & migration for Skiplock'

    def self.next_migration_number(path)
      ActiveRecord::Generators::Base.next_migration_number(path)
    end

    def create_config_file
      create_file 'config/skiplock.yml', Skiplock::Settings.to_yaml
    end

    def create_migration_file
      migration_template 'migration.rb.erb', 'db/migrate/create_skiplock_schema.rb'
    end
  end
end
