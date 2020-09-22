# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require File.expand_path(File.dirname(__FILE__)) + '/integration_helper'

describe Lhm do
  include IntegrationHelper

  before(:each) { connect_master!; Lhm.cleanup(:run) }

  describe 'id column requirement' do
    it 'should migrate the table when id is pk' do
      table_create(:users)

      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_column(:logins, "integer default '0'")
      end

      slave do
        table_read(:users).columns['logins'].must_equal({
          :type           => 'integer',
          :is_nullable    => 'YES',
          :column_default => '0',
        })
      end
    end

    it 'should migrate the table when id is not pk' do
      table_create(:custom_primary_key)

      Lhm.change_table(:custom_primary_key, :atomic_switch => false) do |t|
        t.add_column(:logins, "integer default '0'")
      end

      slave do
        table_read(:custom_primary_key).columns['logins'].must_equal({
          :type           => 'integer',
          :is_nullable    => 'YES',
          :column_default => '0',
        })
      end
    end
  end

  describe 'changes' do
    before(:each) do
      table_create(:users)
      table_create(:tracks)
      table_create(:permissions)
    end

    describe 'when providing a subset of data to copy' do

      before do
        execute('insert into tracks (id, public) values(13, 0)')
        11.times { |n| execute("insert into tracks (id, public) values(#{n + 1}, 1)") }
        11.times { |n| execute("insert into permissions (track_id) values(#{n+1})") }

        Lhm.change_table(:permissions, :atomic_switch => false) do |t|
          t.filter('inner join tracks on tracks.id = permissions.track_id and tracks.public = 1')
        end
      end

      describe 'when no additional data is inserted into the table' do

        it 'migrates the existing data' do
          slave do
            count_all(:permissions).must_equal(11)
          end
        end
      end

      describe 'when additional data is inserted' do

        before do
          execute('insert into tracks (id, public) values (14, 0)')
          execute('insert into tracks (id, public) values (15, 1)')
          execute('insert into permissions (track_id) values (14)')
          execute('insert into permissions (track_id) values (15)')
        end

        it 'migrates all data' do
          slave do
            count_all(:permissions).must_equal(13)
          end
        end
      end
    end

    it 'should add a column' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_column(:logins, "integer DEFAULT '0'")
      end

      slave do
        table_read(:users).columns['logins'].must_equal({
          :type => 'integer',
          :is_nullable => 'YES',
          :column_default => '0',
        })
      end
    end

    it 'should copy all rows' do
      23.times { |n| execute("insert into users (reference) values (#{ n })") }

      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_column(:logins, "integer DEFAULT '0'")
      end

      slave do
        count_all(:users).must_equal(23)
      end
    end

    it 'should remove a column' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.remove_column(:comment)
      end

      slave do
        table_read(:users).columns['comment'].must_equal nil
      end
    end

    it 'should add an index' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_index([:comment, :created_at])
      end

      slave do
        index_on_columns?(:users, [:comment, :created_at]).must_equal(true)
      end
    end

    it 'should add an index with a custom name' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_index([:comment, :created_at], :my_index_name)
      end

      slave do
        index?(:users, :my_index_name).must_equal(true)
      end
    end

    it 'should add an index on a column with a reserved name' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_index(:groupname)
      end

      slave do
        index_on_columns?(:users, :groupname).must_equal(true)
      end
    end

    it 'should add a unqiue index' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.add_unique_index(:comment)
      end

      slave do
        index_on_columns?(:users, :comment, :unique).must_equal(true)
      end
    end

    it 'should remove an index' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.remove_index([:username, :created_at])
      end

      slave do
        index_on_columns?(:users, [:username, :created_at]).must_equal(false)
      end
    end

    it 'should remove an index with a custom name' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.remove_index([:username, :groupname])
      end

      slave do
        index?(:users, :index_with_a_custom_name).must_equal(false)
      end
    end

    it 'should remove an index with a custom name by name' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.remove_index(:irrelevant_column_name, :index_with_a_custom_name)
      end

      slave do
        index?(:users, :index_with_a_custom_name).must_equal(false)
      end
    end

    it 'should apply a ddl statement' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.ddl('alter table %s add column flag smallint' % t.name)
      end

      slave do
        table_read(:users).columns['flag'].must_equal({
          :type => 'smallint',
          :is_nullable => 'YES',
          :column_default => nil,
        })
      end
    end

    it 'should change a column' do
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.change_column(:comment, "TYPE character varying(20) DEFAULT 'none' NOT NULL")
      end

      slave do
        table_read(:users).columns['comment'].must_equal({
          :type => 'character varying(20)',
          :is_nullable => 'NO',
          :column_default => 'none',
        })
      end
    end

    it 'should change the last column in a table' do
      table_create(:small_table)

      Lhm.change_table(:small_table, :atomic_switch => false) do |t|
        t.change_column(:id, 'TYPE integer')
      end

      slave do
        table_read(:small_table).columns['id'].must_equal({
          :type => 'integer',
          :is_nullable => 'NO',
          :column_default => nil,
        })
      end
    end

    it 'should rename a column' do
      table_create(:users)

      execute("INSERT INTO users (username) VALUES ('a user')")
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.rename_column(:username, :login)
      end

      slave do
        table_data = table_read(:users)
        table_data.columns['username'].must_equal(nil)
        table_read(:users).columns['login'].must_equal({
          :type => 'character varying(255)',
          :is_nullable => 'YES',
          :column_default => nil,
        })

        result = select_one('SELECT login from users')
        result = result['login'] if result.respond_to?(:has_key?)
        result.must_equal('a user')
      end
    end

    it 'should rename a column with a default' do
      table_create(:users)

      execute("INSERT INTO users (username) VALUES ('a user')")
      Lhm.change_table(:users, :atomic_switch => false) do |t|
        t.rename_column(:groupname, :fnord)
      end

      slave do
        table_data = table_read(:users)
        table_data.columns['groupname'].must_equal(nil)
        table_read(:users).columns['fnord'].must_equal({
          :type => 'character varying(255)',
          :is_nullable => 'YES',
          :column_default => 'Superfriends',
        })

        result = select_one('SELECT `fnord` from users')
        result = result['fnord'] if result.respond_to?(:has_key?)
        result.must_equal('Superfriends')
      end
    end

    it 'works when postgres reserved words are used' do
      table_create(:lines)
      execute("insert into lines (id, between) values (1, 'foo')")
      execute("insert into lines (id, between) values (2, 'bar')")

      Lhm.change_table(:lines) do |t|
        t.add_column('by', 'character varying(10)')
        t.remove_column('lines')
        t.add_index('by')
        t.add_unique_index('between')
        t.remove_index('by')
      end

      slave do
        table_read(:lines).columns.must_include 'by'
        table_read(:lines).columns.wont_include 'lines'
        index_on_columns?(:lines, ['between'], :unique).must_equal true
        index_on_columns?(:lines, ['by']).must_equal false
        count_all(:lines).must_equal(2)
      end
    end

    describe 'parallel' do
      it 'should perserve inserts during migration' do
        50.times { |n| execute("insert into users (reference) values (#{ n })") }

        insert = Thread.new do
          10.times do |n|
            connect_master!
            execute("insert into users (reference) values (#{ 100 + n })")
            sleep(0.17)
          end
        end
        sleep 2

        options = { :stride => 10, :throttle => 97, :atomic_switch => false }
        Lhm.change_table(:users, options) do |t|
          t.add_column(:parallel, "integer DEFAULT '0'")
        end

        insert.join

        slave do
          count_all(:users).must_equal(60)
        end
      end

      it 'should perserve deletes during migration' do
        50.times { |n| execute("insert into users (reference) values (#{ n })") }

        delete = Thread.new do
          10.times do |n|
            execute("delete from users where id = '#{ n + 1 }'")
            sleep(0.17)
          end
        end
        sleep 2

        options = { :stride => 10, :throttle => 97, :atomic_switch => false }
        Lhm.change_table(:users, options) do |t|
          t.add_column(:parallel, "integer DEFAULT 0")
        end

        delete.join

        slave do
          count_all(:users).must_equal(40)
        end
      end
    end
  end
end
