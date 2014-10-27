#!/usr/bin/env ruby
# encoding: utf-8

## Достаточно запуска только sidekiq
# Run: export APP_ENV=development && bundle exec sidekiq -C ./config/sidekiq.yml -r ./runner.rb
# Run: export APP_ENV=production && bundle exec sidekiq -d -C ./config/sidekiq.yml -r ./runner_credit.rb --logfile log/bk_credit_production.log --pidfile tmp/sidekiq_credit.pid
# Run: export APP_ENV=production && bundle exec sidekiq -C ./config/sidekiq.yml -r ./runner_credit.rb > /dev/null 2>&1 &


if ENV['APP_ENV']
  # require 'pry'
  
  require_relative 'config/config'
  $config = Configuration.load_config

  require 'logger'
  current_logger = Logger.new("#{File.dirname(__FILE__)}/log/bk_#{ENV['APP_ENV']}.log")
  current_logger.info "Started"
  
  require_relative 'lib/workers'
  require_relative 'lib/db'
  
  require 'active_record'
  ActiveRecord::Base.establish_connection(
        :adapter  => $config['database']['adapter'],
        :database => $config['database']['database'],
        :username => $config['database']['username'],
        :password => $config['database']['password'],
        :host     => $config['database']['host'])

  
  require 'redis'
  $redis = Redis.new(host: $config['redis_cache']['host'], port: $config['redis_cache']['port'])
  current_logger.info "$redis #{$redis}"
  $redis_alarm = Redis.new(host: $config['redis_alarm']['host'], port: $config['redis_alarm']['port'])
  current_logger.info "$redis_alarm #{$redis_alarm}"
  
  require 'sidekiq' 
  instances = 0
  while instances < $config['runner']['instances'].to_i
    BkWorkers::Credit.perform_async(instances)
    # BkWorkers::Debit.perform_async(instances)
    instances += 1
  end
else
  puts 'Error: not found "APP_ENV"!'
end