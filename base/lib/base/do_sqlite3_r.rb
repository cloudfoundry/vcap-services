# Copyright (c) 2009-2011 VMware, Inc.
require "do_sqlite3"

# By default, sqlite3 is threadsafe. But occasionally, in multi-thread
# environment, intensive concurrent accesses still cause SQLITE_BUSY
# exception. What's more, if some other process accesses the same db
# file, it could also cause the same exception.
#
# This is a retry version of do_sqlite3, it will retry NUM_RETRY times
# if SQLITE_BUSY and SQLITE_LOCKED are caught.
#
# XXX: It works well with do_sqlite3 0.10.3 and sqlite3 3.7.5
class DataObjects::Sqlite3::Command
  alias original_execute_non_query execute_non_query
  alias original_execute_reader execute_reader

  NUM_RETRY = 10
  RETRY_INTERVAL = 0.1

  SQLITE_BUSY = 5
  SQLITE_LOCKED = 6

  def execute_non_query(*args)
    (1..NUM_RETRY).each do |t|
      begin
        return original_execute_non_query(*args)
      rescue DataObjects::SQLError => e
        raise e if !is_locked?(e) or t == NUM_RETRY
        sleep RETRY_INTERVAL
      end
    end
  end

  def execute_reader(*args)
    (1..NUM_RETRY).each do |t|
      begin
        return original_execute_reader(*args)
      rescue DataObjects::SQLError => e
        raise e if !is_locked?(e) or t == NUM_RETRY
        sleep RETRY_INTERVAL
      end
    end
  end

  def is_locked?(e)
    e.code == SQLITE_BUSY or e.code == SQLITE_LOCKED
  end
end
