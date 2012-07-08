require 'monitor'

module Redistat
  module Connection

    REQUIRED_SERVER_VERSION = "1.3.10"
    MIN_EXPIRE_SERVER_VERSION = "2.1.3"
    MIN_DISTRIBUTED_CLIENT_VERSION = '3.0.1'

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
          options = options.clone
          check_distributed_config( options )
          ref = extract_ref( options )
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

      def close( *refs )
        synchronize do
          if refs.nil?
            @connections, @references = {}
          else
            @references.delete_if{|k,v| refs.include?( k) and @connections.delete( v) }
          end
        end
      end

      private

      def monitor
        @monitor ||= Monitor.new
      end

      def synchronize(&block)
        monitor.synchronize(&block)
      end

      def connection(options)
        check_redis_version (distributed? && Redis::Distributed.new(options)) || Redis.new(options) 
      end

      def connection_id(options = {})
        return options.collect{|node| redis_url(node) }.join("_") if distributed?
        options = options.reverse_merge(default_options)
        redis_url( options )
      end

      def redis_url( options = {} )
        return options[:id] if options.has_key? :id 
        "redis://#{options[:host]}:#{options[:port]}/#{options[:db]}"
      end
      
      def safe_connection_id(conn = nil)
        return conn.nodes.map(&:client).map(&:id).join("_") if distributed?
        conn.client.id
      end
      
      def check_distributed_config(options)
        @distributed = options.is_a? Array
      end
      
      def distributed?
        @distributed
      end

      def extract_ref( options )
        if distributed?
          distributed_ref = options.select{|node| node.has_key?(:ref) and node.keys.length == 1 }.first
          ref = distributed_ref[:ref] unless distributed_ref.nil?
          options.delete_if{|node| node.has_key?(:ref) and node.keys.length == 1 }
          #attempt to extract ref from individual node config
          ref ||= options.map{|node| node.delete(:ref)}.uniq.compact.first || :default
        else 
          ref = options.delete(:ref) || :default
        end
        ref
      end

      def check_redis_version(conn)
        redis_version = distributed? && conn.info.collect{|inf| inf["redis_version"]}.min || conn.info["redis_version"]
        if distributed?
          raise RedisDistributedClientIsTooOld if conn.nodes.first.class::VERSION < MIN_DISTRIBUTED_CLIENT_VERSION
        end
        raise RedisServerIsTooOld if redis_version < REQUIRED_SERVER_VERSION
        if redis_version < MIN_EXPIRE_SERVER_VERSION
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
