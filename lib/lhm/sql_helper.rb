# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

module Lhm
  module SqlHelper
    extend self

    def annotation
      '/* large hadron migration */'
    end

    def idx_name(table_name, cols)
      column_names = column_definition(cols).map(&:first)
      "index_#{ table_name }_on_#{ column_names.join('_and_') }"
    end

    def idx_spec(cols)
      column_definition(cols).map do |name, length|
        "#{ name }#{ length }"
      end.join(', ')
    end

    def version_string
      row = connection.select_one("show variables like 'version'")
      value = struct_key(row, 'Value')
      row[value]
    end

    def tagged(statement)
      "#{ statement } #{ SqlHelper.annotation }"
    end

    private

    def column_definition(cols)
      Array(cols).map do |column|
        column.to_s.match(/([^\(]+)(\([^\)]+\))?/).captures
      end
    end

    def struct_key(struct, key)
      keys = if struct.is_a? Hash
               struct.keys
             else
               struct.members
             end

      keys.find { |k| k.to_s.downcase == key.to_s.downcase }
    end
  end
end
