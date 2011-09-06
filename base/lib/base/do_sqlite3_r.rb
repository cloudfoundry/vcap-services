require "do_sqlite3"

# Sqlite3 cannot handle concurrent read/write of db, by
# simply throwing a 'database is locked' exception.
#
# This is a retry version of do_sqlite3, it will retry
# certain times when exceptions are caught.
#
# XXX: It works well with do_sqlite3 0.10
class DataObjects::Sqlite3::Command
  alias original_execute_non_query execute_non_query
  alias original_execute_reader execute_reader

  NUM_RETRY = 10
  RETRY_INTERVAL = 0.1

  def execute_non_query(*args)
    (1..NUM_RETRY).each do |t|
      begin
        return original_execute_non_query(*args)
      rescue => e
        raise e if t == NUM_RETRY
        sleep RETRY_INTERVAL
      end
    end
  end

  def execute_reader(*args)
    (1..NUM_RETRY).each do |t|
      begin
        return original_execute_reader(*args)
      rescue => e
        raise e if t == NUM_RETRY
        sleep RETRY_INTERVAL
      end
    end
  end
end
