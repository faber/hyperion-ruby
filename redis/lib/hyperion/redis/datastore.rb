require 'hyperion'
require 'hyperion/key'
require 'hyperion/memory/helper'
require 'redis'
require 'json'

module Hyperion
  module Redis
    class Datastore
      def initialize(opts)
        @client = ::Redis.new(opts)
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
        record = @client.hgetall(key)
        return nil if record.empty?
        metarecord = @client.hgetall(meta_key(key))
        cast_record(record, metarecord)
      end

      def find(query)
        raw_records = find_by_kind(query.kind)
        records = raw_records.map { |(record, metarecord)| cast_record(record, metarecord) }
        records = Hyperion::Memory::Helper.apply_filters(query.filters, records)
        records = Hyperion::Memory::Helper.apply_sorts(query.sorts, records)
        records = Hyperion::Memory::Helper.apply_offset(query.offset, records)
        records = Hyperion::Memory::Helper.apply_limit(query.limit, records)
        records
      end

      def delete_by_key(kind, key)
        kind = @client.hget(key, "kind")
        @client.multi do
          @client.del(key)
          @client.del(meta_key(key))
          @client.srem(kind_set_key(kind), key)
        end
        nil
      end

      def delete(query)
        find(query).each { |record| delete_by_key(record[:kind], record[:key]) }
        nil
      end

      def count(query)
        find(query).count
      end

      private

      def create_one(record)
        kind = record[:kind]
        record[:key] ||= Hyperion::Key.generate_id
        persist_record(record)
        record
      end

      def update_one(record)
        persist_record(record)
        record
      end

      def persist_record(record)
        json_record = record.reduce({}) do |acc, (key, val)|
          if val.class == Hash || val.class == Array
            acc[key] = JSON.dump(val)
          else
            acc[key] = val
          end
          acc
        end
        key = record[:key]
        @client.multi do
          @client.hmset(key, *json_record.to_a.flatten)
          @client.hmset(meta_key(key), *meta_record(record).to_a.flatten)
          @client.sadd(kind_set_key(record[:kind]), key)
        end
      end

      def find_by_kind(kind)
        # keys = @client.keys "#{kind}:*"
        keys = @client.smembers kind_set_key(kind)
        @client.multi do
          keys.each do |key|
            @client.hgetall(key)
            @client.hgetall(meta_key(key))
          end
        end.each_slice(2).to_a
      end

      def meta_record(record)
        record.reduce({}) do |acc, (key, val)|
          acc[key] = to_db_type(val)
          acc
        end
      end

      def meta_key(key)
        "__metadata__#{key}"
      end
      
      def kind_set_key(kind)
        "__kindset__:#{kind}"
      end

      def cast_record(record, metarecord)
        record.reduce({}) do |acc, (key, value)|
          type = metarecord[key]
          acc[key.to_sym] = from_db_type(value, type)
          acc
        end
      end

      def to_db_type(value)
        if value.class.to_s == "String"
          "String"
        elsif value.class.to_s == "Fixnum"
          "Integer"
        elsif value.class.to_s == "Float"
          "Number"
        elsif value.class.to_s == "TrueClass" || value.class.to_s == "FalseClass"
          "Boolean"
        elsif value.class.to_s == "NilClass"
          "Null"
        elsif value.class.to_s == "Array"
          "Array"
        elsif value.class.to_s == "Hash"
          "Object"
        else
          "Any"
        end
      end

      def from_db_type(value, type)
        if type == "String"
          value
        elsif type == "Integer"
          value.to_i
        elsif type == "Number"
          value.to_f
        elsif type == "Boolean"
          value == 'true' ? true : false
        elsif type == "Null"
          nil
        elsif type == "Array"
          JSON.load(value)
        elsif type == "Object"
          JSON.load(value)
        elsif type == "Any"
          value
        end
      end
    end
  end
end
