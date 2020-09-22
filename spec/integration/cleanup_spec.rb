# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

describe Lhm, 'cleanup' do
  include IntegrationHelper
  before(:each) { connect_master! }

  describe 'changes' do
    before(:each) do
      table_create(:users)
      simulate_failed_migration do
        Lhm.change_table(:users, :atomic_switch => false) do |t|
          t.add_column(:logins, "integer DEFAULT '0'")
          t.add_index(:logins)
        end
      end
    end

    after(:each) do
      Lhm.cleanup(:run)
    end

    it 'should show temporary tables' do
      output = capture_stdout do
        Lhm.cleanup
      end
      output.must_include('Existing LHM backup tables')
      output.must_match(/lhma_[0-9_]*_users/)
    end

    it 'should show temporary tables within range' do
      table = OpenStruct.new(:name => 'users')
      table_name = Lhm::Migration.new(table, nil, nil, {}, Time.now - 172800).archive_name
      table_rename(:users, table_name)

      output = capture_stdout do
        Lhm.cleanup false, { :until => Time.now - 86400 }
      end
      output.must_include('Existing LHM backup tables')
      output.must_match(/lhma_[0-9_]*_users/)
    end

    it 'should exclude temporary tables outside range' do
      table = OpenStruct.new(:name => 'users')
      table_name = Lhm::Migration.new(table, nil, nil, {}, Time.now).archive_name
      table_rename(:users, table_name)

      output = capture_stdout do
        Lhm.cleanup false, { :until => Time.now - 172800 }
      end
      output.must_include('Existing LHM backup tables')
      output.wont_match(/lhma_[0-9_]*_users/)
    end

    it 'should show temporary triggers' do
      output = capture_stdout do
        Lhm.cleanup
      end
      output.must_include('Existing LHM triggers')
      output.must_include('lhmt_ins_users')
      output.must_include('lhmt_del')
      output.must_include('lhmt_upd_users')
    end

    it 'should delete temporary tables' do
      Lhm.cleanup(:run).must_equal(true)
      Lhm.cleanup.must_equal(true)
    end
  end
end
