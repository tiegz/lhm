# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/sql_helper'

module Lhm
  class Entangler
    include Command
    include SqlHelper

    attr_reader :connection

    # Creates entanglement between two tables. All creates, updates and deletes
    # to origin will be repeated on the destination table.
    def initialize(migration, connection = nil)
      @archive_name = migration.archive_name
      @intersection = migration.intersection
      @origin = migration.origin
      @destination = migration.destination
      @connection = connection
    end

    def entangle
      [
        create_delete_function,
        create_delete_trigger,
        create_insert_function,
        create_insert_trigger,
        create_update_function,
        create_update_trigger
      ]
    end

    def untangle
      puts "Looking for archive_name #{@archive_name}, and origin_name #{@origin.name}"
      [
        "drop trigger if exists #{ trigger(:del) } ON #{ @archive_name }",
        "drop trigger if exists #{ trigger(:ins) } ON #{ @archive_name }",
        "drop trigger if exists #{ trigger(:upd) } ON #{ @archive_name }",
        "drop function on_insert_#{@origin.name}()",
        "drop function on_update_#{@origin.name}()",
        "drop function on_delete_#{@origin.name}()"
      ]
    end

    # TODO cleanup function afteward
    def create_insert_function
      strip %Q{
        CREATE FUNCTION on_insert_#{ @origin.name }() 
          RETURNS TRIGGER 
          LANGUAGE plpgsql
          AS $$
          BEGIN
            insert into #{ @destination.name } (#{ @intersection.destination.joined })
            values (#{ @intersection.origin.typed('NEW') })
            on conflict (id) do update
              set #{ @intersection.origin.escaped.map { |c| "#{@destination.name}.#{c} = NEW.#{c}" }.join(', ') };
          END;
          $$
      }
    end

    def create_insert_trigger
      strip %Q{
        create trigger #{ trigger(:ins) }
        after insert on #{ @origin.name } for each row
        EXECUTE PROCEDURE on_insert_#{@origin.name}()
      }
    end

    # TODO cleanup function afteward
    def create_update_function
      # ORIG: 
      # replace into #{ @destination.name } (#{ @intersection.destination.joined }) 
      # #{ SqlHelper.annotation } values (#{ @intersection.origin.typed('NEW') })";
      strip %Q{
        CREATE FUNCTION on_update_#{ @origin.name }() 
          RETURNS TRIGGER 
          LANGUAGE plpgsql
          AS $$
          BEGIN
            insert into #{ @destination.name } (#{ @intersection.destination.joined })
            values (#{ @intersection.origin.typed('NEW') })
            on conflict (id) do update
              set #{ @intersection.origin.escaped.map { |c| "#{@destination.name}.#{c} = NEW.#{c}" }.join(', ') };
          END;
          $$          
      }
    end

    def create_update_trigger
      strip %Q{
        create trigger #{ trigger(:upd) }
        after update on #{ @origin.name } for each row
        EXECUTE PROCEDURE on_update_#{@origin.name}()
      }
    end

    # TODO cleanup function afteward
    def create_delete_function
      # Had to remove the 'ignore' directive from deletion here. Is it possible in postgres to ignore errors while deleting??
      strip %Q{
        CREATE FUNCTION on_delete_#{ @origin.name }() 
          RETURNS TRIGGER 
          LANGUAGE plpgsql
          AS $$
          BEGIN
            delete from #{ @destination.name } where #{ @destination.name }.id = OLD.id;
          END;
          $$;          
      }
    end
    
    def create_delete_trigger
      strip %Q{
        create trigger #{ trigger(:del) }
        after delete on #{ @origin.name } for each row
        EXECUTE PROCEDURE on_delete_#{@origin.name}();
      }
    end

    def trigger(type)
      "lhmt_#{ type }_#{ @origin.name }"[0...64]
    end

    def validate
      unless @connection.table_exists?(@origin.name)
        error("#{ @origin.name } does not exist")
      end

      unless @connection.table_exists?(@destination.name)
        error("#{ @destination.name } does not exist")
      end
    end

    def before
      entangle.each do |stmt|
        @connection.execute(tagged(stmt))
      end
    end

    def after
      untangle.each do |stmt|
        @connection.execute(tagged(stmt))
      end
    end

    def revert
      after
    end

    private

    def strip(sql)
      sql.strip.gsub(/\n */, "\n")
    end
  end
end
