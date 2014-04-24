require 'hyperion'
require 'hyperion/key'
require 'hyperion/riak/map_reduce_js'
require 'riak'
require 'hyperion/riak/optimized_filter_order'

module Hyperion
  module Riak
    class Datastore
      def initialize(opts={})
        opts ||= {}
        @app = opts[:app] || ''
        @client = ::Riak::Client.new(opts.reject {|k, v| k == :app})
        @buckets = {}
      end

      def save(records)
        records.map do |record|
          Hyperion.new?(record) ? create_one(record) : update_one(record)
        end
      end
      
      def create(records)
        records.map do |record|
          create_one(record)
        end
      end

      def find_by_key(kind, key)
        robject = bucket(kind.to_s).get(key.to_s)
        record_from_db(kind, robject.key, robject.data)
      rescue ::Riak::ProtobuffsFailedRequest => e
        raise e unless e.not_found?
      end

      def find(query)
        mr = new_mapreduce_with_returned_result(query)
        mr.run.map do |record|
          record_from_db(query.kind, record.delete('riak_key'), record)
        end
      end

      def delete_by_key(kind, key)
        delete_with_riak_key(kind.to_s, key.to_s)
        nil
      end

      def delete(query)
        mr = new_mapreduce_with_returned_result(query)
        mr.run.each do |record|
          delete_with_riak_key(query.kind, record['riak_key'])
        end
        nil
      end

      def count(query)
        mr = new_mapreduce(query)
        mr.reduce(MapReduceJs.count, :keep => true)
        mr.run.first
      end

      private

      def create_one(record)
        kind = record[:kind]
        riak_key = record[:key]
        robject = bucket(kind).new(riak_key)
        store(kind, robject, record_to_db(record))
      end

      def update_one(record)
        kind = record[:kind]
        key = record[:key]
        robject = bucket(kind).get(key)
        store(kind, robject, robject.data.merge(record_to_db(record)))
      end

      def store(kind, robject, record_data)
        robject.data = record_data
        robject.indexes = record_data_to_index(record_data)
        robject.store
        record_from_db(kind, robject.key, robject.data)
      end

      def record_data_to_index(data)
        data.reduce({}) do |new_record, (key, value)|
          new_record[index_name(key)] = value
          new_record
        end
      end

      def delete_with_riak_key(kind, key)
        bucket(kind).delete(key)
      end

      def new_mapreduce(query)
        mr = ::Riak::MapReduce.new(@client)
        add_query_filters(mr, query)
        sorts = query.sorts
        mr.reduce(MapReduceJs.sort(sorts)) unless sorts.empty?
        mr.reduce(MapReduceJs.offset(query.offset)) if query.offset
        mr.reduce(MapReduceJs.limit(query.limit)) if query.limit
        mr
      end

      def add_query_filters(mr, query)
        bucket_name = bucket_name(query.kind)
        optimizer = OptimizedFilterOrder.new(query.filters, bucket_name)
        field_index = index_name(optimizer.optimal_index_field)
        mr.index(bucket_name, field_index, optimizer.optimal_index_value)
        mr.map(MapReduceJs.filter(optimizer.filters))
      end

      def index_name(field_name)
        if field_name != '$bucket'
          "#{field_name}_bin"
        else
          field_name
        end
      end

      def new_mapreduce_with_returned_result(query)
        mr = new_mapreduce(query)
        mr.reduce(MapReduceJs.pass_thru, :keep => true)
      end

      def record_to_db(record)
        record.reject {|k, v| [:kind, :key].include?(k)}
      end

      def record_from_db(kind, key, data)
        data.merge(:kind => kind, :key => key)
      end

      def bucket(kind)
        name = bucket_name(kind)
        @buckets[name] ||= @client.bucket(name)
      end

      def bucket_name(kind)
        @app.to_s + kind
      end
    end
  end
end
