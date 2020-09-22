# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/table'
require 'lhm/invoker'
require 'lhm/throttler'
require 'lhm/version'
require 'logger'

# Large hadron migrator - online schema change tool
#
# @example
#
#   Lhm.change_table(:users) do |m|
#     m.add_column(:arbitrary, "integer")
#     m.add_index([:arbitrary, :created_at])
#     m.ddl("alter table %s add column flag smallint" % m.name)
#   end
#
module Lhm
  extend Throttler
  extend self

  DEFAULT_LOGGER_OPTIONS =  { level: Logger::INFO, file: STDOUT }

  # Alters a table with the changes described in the block
  #
  # @param [String, Symbol] table_name Name of the table
  # @param [Hash] options Optional options to alter the chunk / switch behavior
  # @option options [Fixnum] :stride
  #   Size of a chunk (defaults to: 40,000)
  # @option options [Fixnum] :throttle
  #   Time to wait between chunks in milliseconds (defaults to: 100)
  # @option options [Fixnum] :start
  #   Primary Key position at which to start copying chunks
  # @option options [Fixnum] :limit
  #   Primary Key position at which to stop copying chunks
  # @option options [Boolean] :atomic_switch
  #   Use atomic switch to rename tables (defaults to: true)
  # @yield [Migrator] Yielded Migrator object records the changes
  # @return [Boolean] Returns true if the migration finishes
  # @raise [Error] Raises Lhm::Error in case of a error and aborts the migration
  def change_table(table_name, options = {}, &block)
    origin = Table.parse(table_name, connection)
    invoker = Invoker.new(origin, connection)
    block.call(invoker.migrator)
    invoker.run(options)
    true
  end

  # Cleanup tables and triggers
  #
  # @param [Boolean] run execute now or just display information
  # @param [Hash] options Optional options to alter cleanup behaviour
  # @option options [Time] :until
  #   Filter to only remove tables up to specified time (defaults to: nil)
  def cleanup(run = false, options = {})
    lhm_tables = connection.select_values("select tablename from pg_catalog.pg_tables where schemaname='public'").select { |name| name =~ /^lhm(a|n)_/ }
    if options[:until]
      lhm_tables.select! do |table|
        table_date_time = Time.strptime(table, 'lhma_%Y_%m_%d_%H_%M_%S')
        table_date_time <= options[:until]
      end
    end

    lhm_triggers = connection.select_values('select * from pg_trigger').collect do |trigger|
      trigger.respond_to?(:trigger) ? trigger.trigger : trigger
    end.select { |name| name =~ /^lhmt/ }

    if run
      lhm_triggers.each do |trigger|
        connection.execute("drop trigger if exists #{trigger} on table #{table}")
      end
      lhm_tables.each do |table|
        connection.execute("drop function if exists on_insert_#{table}()")
        connection.execute("drop function if exists on_update_#{table}()")
        connection.execute("drop function if exists on_delete_#{table}()")
        connection.execute("drop table if exists #{table}")
      end
      true
    elsif lhm_tables.empty? && lhm_triggers.empty?
      puts 'Everything is clean. Nothing to do.'
      true
    else
      puts "Existing LHM backup tables: #{lhm_tables.join(', ')}."
      puts "Existing LHM triggers: #{lhm_triggers.join(', ')}."
      puts 'Run Lhm.cleanup(:run) to drop them all.'
      false
    end
  end

  def setup(connection)
    @@connection = connection
  end

  def connection
    @@connection ||=
      begin
        raise 'Please call Lhm.setup' unless defined?(ActiveRecord)
        ActiveRecord::Base.connection
      end
  end

  def self.logger=(new_logger)
    @@logger = new_logger
  end

  def self.logger
    @@logger ||=
      begin
        logger = Logger.new(DEFAULT_LOGGER_OPTIONS[:file])
        logger.level = DEFAULT_LOGGER_OPTIONS[:level]
        logger.formatter = nil
        logger
      end
  end
end
