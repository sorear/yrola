# Journal layer

# MVP repository consists of a lock file, logs, and blobs
# blobs are unstructured
# log consists of a sequence of checksummed log records, followed by an incomplete record / zeros / end of file
# log record = length (u64) checksum (u64) data(...)

struct LogRecord {
    type @0 :UInt32;
    id @1 :UInt64;
    union {
        data @2 :Data;
        indirect @3 :IndirectRecord;
        deleted @4 :Void;
    }
}

struct IndirectRecord {
    size @0 :UInt64;
    hash @1 :UInt64;
}

# Table manager

struct ColumnRun {
    # will need to turn this into a b-tree in some cases.  page alignment bah
    bits @0 :Data;
}

struct TableRun {
    tableSchema @0 :UInt64;
    firstPkey @0 :Data;
    #tableInstance @0 :UInt64; # 0 = multi-instance, instance ID added after pkey

    hasTimestamps @0 :Bool; # adds timestamp after PK
    pkeyCount @0 :UInt32;
    columnCount @0 :UInt32;

    upserts @0 :List(ColumnRun);
    deletes @0 :List(ColumnRun);
    touches @0 :List(ColumnRun);
    watches @0 :List(ColumnRun);
}

struct Run {
    tables @0 :List(TableRun);
}

# Schemas
