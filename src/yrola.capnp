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

struct InlineItem {
  id @0 :UInt64;
  type @1 :UInt16;
  data @2 :Data;
}

struct ExternalItem {
  id @0 :UInt64;
  type @1 :UInt16;
  size @2 :UInt64;
  hash @3 : UInt64;
}

# Table manager

struct ColumnRun {
  # will need to turn this into a b-tree in some cases. page alignment bah
  bits @0 :Data;
}

struct TableRun {
  tableSchema @0 :UInt64;
  firstPkey @1 :Data;
  #tableInstance @0 :UInt64; # 0 = multi-instance, instance ID added after pkey

  hasTimestamps @2 :Bool; # adds timestamp after PK
  pkeyCount @3 :UInt32;
  columnCount @4 :UInt32;

  upserts @5 :List(ColumnRun);
  deletes @6 :List(ColumnRun);
  touches @7 :List(ColumnRun);
  watches @8 :List(ColumnRun);
}

struct Run {
  tables @0 :List(TableRun);
}

# Schemas
