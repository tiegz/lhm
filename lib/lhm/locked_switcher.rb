# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/command'
require 'lhm/migration'
require 'lhm/sql_helper'

module Lhm
  # Switches origin with destination table nonatomically using a locked write.
  # LockedSwitcher adopts the Facebook strategy, with the following caveat:
  #
  #   "Since alter table causes an implicit commit in innodb, innodb locks get
  #   released after the first alter table. So any transaction that sneaks in
  #   after the first alter table and before the second alter table gets
  #   a 'table not found' error. The second alter table is expected to be very
  #   fast though because copytable is not visible to other transactions and so
  #   there is no need to wait."
  #
  class LockedSwitcher
    include Command
    include SqlHelper

    attr_reader :connection

    def initialize(migration, connection = nil)
      throw "LockedSwitcher not supported yet!"
      @migration = migration
      @connection = connection
      @origin = migration.origin
      @destination = migration.destination
    end

    def statements
      uncommitted { switch }
    end

    def switch
      [
        "begin",
        "lock table #{ @origin.name } write, #{ @destination.name } write", # TODO do we need "IN EXCLUSIVE MODE" here?
        "alter table #{ @origin.name } rename #{ @migration.archive_name }",
        "alter table #{ @destination.name } rename #{ @origin.name }",
        "commit"
      ]
    end

    def uncommitted
      [
        "set session lhm_auto_commit to autocommit;",
        'set session autocommit = off',
        yield,
        'set session autocommit = lhm_auto_commit'
      ].flatten
    end

    def validate
      unless @connection.table_exists?(@origin.name) &&
             @connection.table_exists?(@destination.name)
        error "#{ @origin.name } and #{ @destination.name } must exist"
      end
    end

    private

    def revert
      @connection.execute(tagged('commit work'))
    end

    def execute
      statements.each do |stmt|
        @connection.execute(tagged(stmt))
      end
    end
  end
end
