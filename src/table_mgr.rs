use persist::ValueHandle;

// a levelhandle does NOT know its own place...
struct LevelHandle {
    value: ValueHandle,
    bundle_index: Option<i32>,
}

enum LevelOrSavepoint {
    Level(LevelHandle),
    Savepoint(String),
}

struct Transaction {
    start_stamp: u64,
    committed: Vec<LevelHandle>,
    uncomitted: Vec<LevelOrSavepoint>,
}
