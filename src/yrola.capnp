@0x9a225adf6a40fef7;
# Journal layer

# MVP repository consists of a lock file, logs, and blobs
# blobs are unstructured
# log consists of a sequence of checksummed log records, followed by an incomplete record / zeros / end of file
# log record = length (u32) length checksum (u32) checksum (u64) data(...)

struct LogBlock {
  union {
    segmentHeader :group {
      previousSegmentIds @0 :List(UInt64);
      highestEverItemId @6 :UInt64;
    }
    commit :group {
      newInline @1 :List(InlineItem);
      newExternal @2 :List(ExternalItem);
      deleted @3 :List(UInt64);
      obsoleteSegmentIds @4 :List(UInt64);
    }
    eof @5 :Void;
  }
}

# we miiiight provide structured headers later if it proves necessary
struct InlineItem {
  id @0 :UInt64;
  header @1 :Data;
  data @2 :Data;
}

struct ExternalItem {
  id @0 :UInt64;
  header @1 :Data;
  size @2 :UInt64;
  hash @3 : UInt64;
}

# Table manager

struct ItemHeader {
  union {
    level :group {
      startStamp @0 :UInt64;
      endStamp @1 :UInt64;
      height @2 :UInt32;
    }
    bundle :group {
      startStamp @3 :UInt64;
      endStamp @4 :UInt64;
      height @5 :UInt32;
    }
    configuration :group {
      tableOfTablesId @6 :UInt64;
      # CREATE TABLE _tables ("name" STRING PRIMARY KEY, "desc" BLOB)
      historyRetentionSeconds @7 :Float64;
    }
  }
}

# a level might not know anything about stamps, if it's part of an uncommited txn
struct Level {
  tablesChanged @0 :List(LevelTableChange);
}

struct Bundle {
  levels @0 :List(Level);
}

# creates table, asserts appropriate column orders
struct LevelTableChange {
  tableId @0 :UInt64;
  dropped @5 :Bool;
  created @6 :Bool; # if dropped and not created, everything else is dead

  keyCount @1 :UInt32;
  columnOrder @2 :List(UInt32);

  upsertData @3 :List(LevelColumn); # keys, column data
  deleteData @4 :List(LevelColumn); # just the keys

  # TODO (soon): Timeout columns for snapshot isolation
}

struct LevelColumn {
  bits @0 :Data;
}

# Schemas

struct TableSpec {
  lowId @0 :UInt64;
  columnsSpec @1 :List(ColumnSpec);
  indicesSpec @2 :List(IndexSpec);
}

struct ColumnSpec {
  name @0 :Text;
  lowId @1 :UInt32;
  isPrimaryKey @2 :Bool;
}

struct IndexSpec {
  lowId @0 :UInt64;
  keyColumns @1 :List(UInt32);
}
