# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/chunker'
require 'lhm/entangler'
require 'lhm/atomic_switcher'
require 'lhm/locked_switcher'
require 'lhm/migrator'

module Lhm
  # Copies an origin table to an altered destination table. Live activity is
  # synchronized into the destination table using triggers.
  #
  # Once the origin and destination tables have converged, origin is archived
  # and replaced by destination.
  class Invoker
    include SqlHelper
    LOCK_WAIT_TIMEOUT_DELTA = -2

    attr_reader :migrator, :connection

    def initialize(origin, connection)
      @connection = connection
      @migrator = Migrator.new(origin, connection)
    end

    def set_session_lock_wait_timeouts
      global_lock_wait_timeout = @connection.select_one("SHOW lock_timeout")

      if global_lock_wait_timeout
        @connection.execute("SET LOCAL lock_timeout=#{[global_lock_wait_timeout['lock_timeout'].to_i + LOCK_WAIT_TIMEOUT_DELTA, 0].max}")
      end
    end

    def run(options = {})
      normalize_options(options)
      set_session_lock_wait_timeouts
      migration = @migrator.run

      Entangler.new(migration, @connection).run do
        Chunker.new(migration, @connection, options).run
        if options[:atomic_switch]
          AtomicSwitcher.new(migration, @connection).run
        else
          throw "LockedSwitcher not fully implemented for Postgres yet."
          LockedSwitcher.new(migration, @connection).run
        end
      end
    end

    private

    def normalize_options(options)
      Lhm.logger.info "Starting LHM run on table=#{@migrator.name}"

      unless options.include?(:atomic_switch)
        options[:atomic_switch] = true
      end

      # TODO: implement LockedSwitcher and remove this line.
      options[:atomic_switch] = true

      if options[:throttler]
        options[:throttler] = Throttler::Factory.create_throttler(options[:throttler])
      else
        options[:throttler] = Lhm.throttler
      end

    rescue => e
      Lhm.logger.error "LHM run failed with exception=#{e.class} message=#{e.message}"
      raise
    end
  end
end
