# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/migration'
require 'lhm/sql_helper'

module Lhm
  # Switches origin with destination table using an atomic rename.
  class AtomicSwitcher
    include Command
    RETRY_SLEEP_TIME = 10
    MAX_RETRIES = 600

    attr_reader :connection, :retries
    attr_writer :max_retries, :retry_sleep_time

    def initialize(migration, connection = nil)
      @migration = migration
      @connection = connection
      @origin = migration.origin
      @destination = migration.destination
      @retries = 0
      @max_retries = MAX_RETRIES
      @retry_sleep_time = RETRY_SLEEP_TIME
    end

    def statements
      atomic_switch
    end

    def atomic_switch
      [
        "begin",
        "lock table #{ @origin.name }, #{ @destination.name }", # TODO make sure that this is using most restrictive lock mode by default
        "alter table #{ @origin.name } rename to #{ @migration.archive_name }",
        "alter table #{ @destination.name } rename to #{ @origin.name }",
        "commit"
      ]
    end

    def validate
      unless @connection.table_exists?(@origin.name) &&
             @connection.table_exists?(@destination.name)
        error "#{ @origin.name } and #{ @destination.name } must exist"
      end
    end

    private

    def execute
      begin
        statements.each do |stmt|
          @connection.execute(SqlHelper.tagged(stmt))
        end
      rescue ActiveRecord::StatementInvalid => error
        if error =~ /Lock wait timeout exceeded/ && (@retries += 1) < @max_retries
          sleep(@retry_sleep_time)
          Lhm.logger.warn "Retrying sql=#{statements} error=#{error} retries=#{@retries}"
          retry
        else
          raise
        end
      end
    end
  end
end
