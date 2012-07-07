require 'monitor'

module Redistat
  module Connection

    REQUIRED_SERVER_VERSION = "1.3.10"
    MIN_EXPIRE_SERVER_VERSION = "2.1.3"

    # TODO: Create a ConnectionPool instance object using Sychronize mixin to replace Connection class

    class << self

      # TODO: clean/remove all ref-less connections

      def get(ref = nil)
        ref ||= :default
        synchronize do
          connections[references[ref]] || create
        end
      end

      def add(conn, ref = nil)
        ref ||= :default
        synchronize do
          check_redis_version(conn)
          references[ref] = safe_connection_id(conn)
          connections[safe_connection_id(conn)] = conn
        end
      end

      def create(options = {})
        synchronize do
          check_distributed_config( options ) 
          options = options.clone
          ref = options.delete(:ref) || :default
          options.reverse_merge!(default_options) unless distributed?
          conn = (connections[connection_id(options)] ||= connection(options))
          references[ref] = safe_connection_id( conn )
          conn
        end
      end

      def connections
        @connections ||= {}
      end

      def references
        @references ||= {}
      end

      private

      def monitor
        @monitor ||= Monitor.new
      end

      def synchronize(&block)
        monitor.synchronize(&block)
      end

      def connection(options)
        check_redis_version single_or_distributed(options)
      end

      def connection_id(options = {})
        return safe_connection_id if distributed?
        options = options.reverse_merge(default_options)
        redis_url( options )
      end
      
      def safe_connection_id(conn = nil)
        return :distributed if distributed?
        conn.client.id
      end
      
      def single_or_distributed(options)
        return Redis::Distributed.new(options.collect{|node| redis_url( node )}) if distributed?
        Redis.new( options )
      end
      
      def redis_url( options )
        "redis://#{options[:host]}:#{options[:port]}/#{options[:db]}"
      end
      
      def check_distributed_config(options)
        @distributed = options.is_a? Array
      end
      
      def distributed?
        @distributed
      end

      def check_redis_version(conn)
        raise RedisServerIsTooOld if conn.info["redis_version"] < REQUIRED_SERVER_VERSION
        if conn.info["redis_version"] < MIN_EXPIRE_SERVER_VERSION
          STDOUT.puts "WARNING: You MUST upgrade Redis to v2.1.3 or later " +
            "if you are using key expiry."
        end
        conn
      end

      def default_options
        {
          :host => '127.0.0.1',
          :port => 6379,
          :db => 0,
          :timeout => 5
        }
      end

    end
  end
end
