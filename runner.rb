#!/usr/bin/env ruby
# encoding: utf-8

# Run: export APP_ENV=development && ruby ./runner.rb
# Run: export APP_ENV=test && ruby ./runner.rb
# Run: export APP_ENV=production && ruby ./runner.rb

if ENV['APP_ENV']
  # require 'pry'
  require 'logger'

  require_relative 'config/config'
  $config = Configuration.load_config

  require 'active_record'
  current_logger = Logger.new("#{File.dirname(__FILE__)}/log/bk_#{ENV['APP_ENV']}.log")
  current_logger.info "Started"

  p $config

  
else
  puts 'Error: not found "APP_ENV"!'
end