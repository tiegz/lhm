# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/sql_helper'

module Lhm
  class Table
    attr_reader :name, :columns, :indices, :pk, :ddl

    def initialize(name, pk = 'id', ddl = nil)
      @name = name
      @columns = {}
      @indices = {}
      @pk = pk
      @ddl = ddl
    end

    def satisfies_id_column_requirement?
      !!((id = columns['id']) &&
        id[:type] =~ /(bigint|integer)/) # TODO: is the (\d+\) from the original needed?
    end

    def destination_name
      "lhmn_#{ @name }"
    end

    def self.parse(table_name, connection)
      Parser.new(table_name, connection).parse
    end

    class Parser
      include SqlHelper

      def initialize(table_name, connection)
        @table_name = table_name.to_s
        @schema_name = 'public' # connection.current_database (NB in Postgres the schema will be a part of the db, not the db itself)
        @database_name = connection.current_database
        @connection = connection
      end

      def ddl
        # TODO is there a better way to do this?
        "CREATE TABLE #{@schema_name}.#{@table_name} (\n" +
        @connection.select_all(%Q{
          select table_schema, table_name, column_name, data_type, is_nullable, column_default, collation_name
          from information_schema.columns
          where table_name = '#{ @table_name }';
        }).map { |col| 
          [
            "  ", 
            col["column_name"], 
            col["data_type"] =~ /timestamp without time zone/ ? "timestamp" : col["data_type"], 
            col["is_nullable"] ? "" : "NOT NULL", 
            col["column_default"] ? "DEFAULT #{col["column_default"]}" : "DEFAULT NULL"
          ].join(" ")
        }.join(",\n") +
      "\n)"
      end

#       CREATE TABLE public.users (
#   id serial primary key NOT NULL,
#   reference integer DEFAULT NULL,
#   username character varying(255) DEFAULT NULL,
#   groupname character varying(255) DEFAULT 'Superfriends',
#   created_at timestamp DEFAULT NULL,
#   comment character varying(20) DEFAULT NULL,
#   description text,
#   UNIQUE(reference)
# );


      def parse
        schema = read_information_schema

        Table.new(@table_name, extract_primary_key(schema), ddl).tap do |table|
          schema.each do |defn|
            column_name    = struct_key(defn, 'column_name')
            column_type    = struct_key(defn, 'data_type')
            is_nullable    = struct_key(defn, 'is_nullable')
            column_default = struct_key(defn, 'column_default')

            table.columns[defn[column_name]] = {
              :type => defn[column_type],
              :is_nullable => defn[is_nullable],
              :column_default => defn[column_default],
            }
          end

          extract_indices(read_indices).each do |idx, columns|
            table.indices[idx] = columns
          end
        end
      end

      private

      def read_information_schema
        @connection.select_all %Q{
          select *
            from information_schema.columns
           where table_name = '#{ @table_name }'
             and table_schema = '#{ @schema_name }'
        }
      end

      def read_indices
        # TODO is it safe to use _pkey suffix as an indicator of primary key index?
        @connection.select_all %Q{
          select * from pg_indexes
            where schemaname = 'public'
            and tablename = '#{ @table_name }'
            and indexname != '#{ @table_name }_pkey'
        }
      end

      def extract_indices(indices)
        indices.
          map do |row|
            key_name = struct_key(row, 'indexname')
            # TODO a more reliable way might be querying information_schema.table_constraints/key_column_usage instead, and aggregating by index to get colum names.
            column_name = struct_key(row, 'indexdef')
            [row[key_name], "(" + row[column_name].split('(').last.split(')').first.split(',').map(&:strip).join(', ') + ')']
          end.
          inject(Hash.new { |h, k| h[k] = [] }) do |memo, (idx, column)|
            memo[idx] << column
            memo
          end
      end

      def extract_primary_key(schema)
        cols = schema.select do |defn|
          column_key = struct_key(defn, 'COLUMN_KEY')
          defn[column_key] == 'PRI'
        end

        keys = cols.map do |defn|
          column_name = struct_key(defn, 'COLUMN_NAME')
          defn[column_name]
        end

        keys.length == 1 ? keys.first : keys
      end
    end
  end
end
