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
      config @8 :JournalConfig;
    }
    commit :group {
      newInline @1 :List(InlineItem);
      newExternal @2 :List(ExternalItem);
      deleted @3 :List(UInt64);
      obsoleteSegmentIds @4 :List(UInt64);
      newConfig @7 :JournalConfig;
    }
    eof @5 :Void;
  }
}

struct JournalConfig {
  appName @0 :Text;
  appVersion @1 :UInt32;
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

# a level might not know anything about stamps, if it's part of an uncommitted txn
struct Level {
  tablesChanged @0 :List(LevelTableChange);
}

struct Bundle {
  levels @0 :List(Level);
}

# creates table, asserts appropriate column orders
struct LevelTableChange {
  tableId @0 :UInt64;
  dropped @1 :Bool;
  created @2 :Bool; # if dropped and not created, everything else is dead

  primaryKeys @3 :List(LevelPk);
  upsertData @4 :List(LevelColumn);

  # TODO (soon): Timeout columns for snapshot isolation
  # we'll store a general write stamp, a schema write stamp, and individual
  # write stamps per pk.  or, we can use the fast-forward data; will probably
  # aggressively retire the timestamp data, and use ff data if a very old tx
  # dares to commit
}

struct LevelPk {
  upsertBits @0 :Data;
  deleteBits @1 :Data;
  excludeFromMatch @2 :Bool;
}

struct LevelColumn {
  lowId @3 :UInt32;
  default @0 :Data;
  erased @1 :Bool;
  data @2 :Data;
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
