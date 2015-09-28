//! Module documentation goes here

use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc,Mutex};

use ::Result;

// TODO add resource accounting, allocations/time (Jobs)
// TODO vectors should be owned by jobs, with a fictitious duplicate for recycling and columns
// TODO vector operations should be lazy to allow for parallelism
// Minimum viable vectors, TODO blittable, fixed-stride, scattered, and inline vectors

pub struct Engine {
    // A home for all global tuning parameters for the execution
    placeholder: u8,
}
pub type EngineRef = Arc<Engine>;

impl Engine {
    pub fn new() -> Engine {
        Engine { placeholder: 0 }
    }
}

// Jobs and temporaries must not be shared between threads; the vector system is internally threaded
pub struct Job {
    stats: Mutex<JobStatistics>,
    name: String,
    engine: EngineRef,
}
pub type JobRef = Arc<Job>;

impl Job {
    pub fn new(engine: &EngineRef, name: String) -> Job {
        Job {
            stats: Mutex::new(JobStatistics {
                aborted: false,
                done: false,
            }),
            name: name,
            engine: engine.clone(),
        }
    }
}

fn ptr_eq<T>(x: *const T, y: *const T) -> bool { x == y }

struct JobStatistics {
    aborted: bool,
    done: bool,
}

//TempVector will eventually be lazy, which means that every possible use of it can trigger arbitrary computations and fail as a result.  Even fetching the length.  Except those that can wrap the failure in a new vector.
#[derive(Clone)]
pub struct TempVector {
    body: Rc<TempVectorBody>,
}

struct TempVectorBody {
    job: Arc<Job>,
    data: Arc<Vec<Vec<u8>>>,
}

#[derive(Clone)]
pub struct PersistVector {
    data: Arc<Vec<Vec<u8>>>,
}

fn new_temp(job: &JobRef, data: Arc<Vec<Vec<u8>>>) -> TempVector {
    TempVector { body: Rc::new(TempVectorBody { job: job.clone(), data: data }) }
}

impl TempVector {
    pub fn to_persistent(self) -> Result<PersistVector> {
        Ok(PersistVector { data: self.body.data.clone() })
    }

    pub fn job(&self) -> &Job { self.body.job.deref() }

    pub fn concat(job: &JobRef, vecs: &Vec<TempVector>) -> TempVector {
        assert!(vecs.iter().all(|v| ptr_eq::<Job>(job.deref(), v.job())),
            "trying to concatenate temporaries spanning jobs");
        new_temp(job, Arc::new(vecs.iter().flat_map(|v| v.body.data.iter().cloned()).collect()))
    }

    // don't expect these two to be performant
    pub fn get_entries(&self) -> Result<Vec<Vec<u8>>> {
        Ok((*self.body.data).clone())
    }

    pub fn new_from_entries(job_ref: &JobRef, entries: Vec<Vec<u8>>) -> TempVector {
        new_temp(job_ref, Arc::new(entries))
    }

    // Returns a vector with the length of keys where each element of keys has been replaced with an index into table (host endian) or -1
    pub fn join_index(table: &TempVector, keys: &TempVector) -> TempVector {
        assert!(ptr_eq::<Job>(table.job(), keys.job()));
        unimplemented!()
    }

    // Returns table in the same order but with elements indexed by remove removed
    pub fn index_antijoin(table: &TempVector, remove: &TempVector) -> TempVector {
        assert!(ptr_eq::<Job>(table.job(), remove.job()));
        unimplemented!()
    }

    // Returns all indices which match the given values
    #[allow(unused_variables)]
    pub fn point_index(table: &TempVector, key: &[u8]) -> TempVector {
        unimplemented!()
    }

    // Returns query in same order with elements replaced by those from table
    pub fn index_join(table: &TempVector, query: &TempVector) -> TempVector {
        assert!(ptr_eq::<Job>(table.job(), query.job()));
        unimplemented!()
    }

    #[allow(unused_variables)]
    pub fn seq_u64_native(start: u64, count: u64) -> TempVector {
        unimplemented!()
    }

    pub fn len(&self) -> Result<u64> { Ok(self.body.data.len() as u64) }
}

impl PersistVector {
    pub fn to_temporary(&self, job_ref: &JobRef) -> TempVector {
        new_temp(job_ref, self.data.clone())
    }
}
