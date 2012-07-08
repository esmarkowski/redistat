require 'bundler/setup'
# add project-relative load paths
$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))



require 'simplecov'
SimpleCov.start do
  add_filter '/spec'
  add_filter '/vendor'
end

# require stuff
require 'redistat'
require 'rspec'
require 'rspec/autorun'

# use the test Redistat instance
#default_options = {:db => 15, :host => 'localhost'}
#Redistat.connect [default_options.merge(:port => 8379, :id => 'larl'), default_options.merge(:port => 8380, :id => 'bang')]
Redistat.connect(:port => 8379, :db => 15, :thread_safe => true)
Redistat.redis.flushdb
