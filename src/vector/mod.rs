//! Module documentation goes here

use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc,Mutex};

use byteorder::{NativeEndian,ByteOrder};

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

    pub fn name(&self) -> &String { &self.name }

    pub fn engine(&self) -> &EngineRef { &self.engine }
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

fn u64vec(x: u64) -> Vec<u8> {
    let mut buf = [0; 8];
    NativeEndian::write_u64(&mut buf, x);
    buf.to_vec()
}

impl TempVector {
    pub fn to_persistent(self) -> Result<PersistVector> {
        Ok(PersistVector { data: self.body.data.clone() })
    }

    pub fn job_ref(&self) -> &JobRef { &self.body.job }
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
    pub fn seq_u64_native(job: &JobRef, start: u64, count: u64) -> TempVector {
        unimplemented!()
    }

    pub fn len(&self) -> Result<u64> { Ok(self.body.data.len() as u64) }
}

impl PersistVector {
    pub fn to_temporary(&self, job_ref: &JobRef) -> TempVector {
        new_temp(job_ref, self.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::u64;
    use super::*;
    use super::u64vec;

    fn test_job() -> JobRef {
        JobRef::new(Job::new(&EngineRef::new(Engine::new()), From::from("Test job")))
    }

    #[test]
    fn job_name() { assert_eq!(test_job().name(), "Test job") }

    #[test]
    fn temp_entries() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        assert_eq!(tv.get_entries().unwrap(), vec![vec![1,2],vec![3,4]]);
    }

    #[test]
    fn temp_len() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        assert_eq!(tv.len().unwrap(), 2);
    }

    #[test]
    fn test_persist() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        let jr = tv.job_ref().clone();
        let pv = tv.to_persistent().unwrap();
        let tv2 = pv.to_temporary(&jr);
        assert_eq!(tv2.get_entries().unwrap(), vec![vec![1,2],vec![3,4]]);
    }

    #[test]
    fn test_concat_1() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        let tv2 = TempVector::new_from_entries(&tv.job_ref().clone(), vec![vec![1,2]]);
        let tv3 = TempVector::concat(&tv.job_ref().clone(), &vec![tv,tv2]);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![1,2],vec![1,2]]);
    }

    #[test]
    #[should_panic(expected="spanning jobs")]
    fn test_concat_2() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        let tv2 = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        TempVector::concat(&tv.job_ref().clone(), &vec![tv,tv2]);
    }

    #[test]
    fn test_join_index() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3]]);
        let tv2 = TempVector::new_from_entries(tv.job_ref(), vec![vec![3],vec![1,2],vec![4]]);
        let tv3 = TempVector::join_index(&tv, &tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![u64vec(1),u64vec(0),u64vec(u64::MAX)]);
    }

    #[test]
    fn test_index_antijoin() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![5]]);
        let tv2 = TempVector::new_from_entries(tv.job_ref(), vec![u64vec(1)]);
        let tv3 = TempVector::index_antijoin(&tv, &tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![3],vec![5]]);
    }

    #[test]
    fn test_point_index() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![3]]);
        let tv2 = TempVector::point_index(&tv, &[3]);
        assert_eq!(tv2.get_entries().unwrap(), vec![u64vec(0),u64vec(1)]);
    }

    #[test]
    fn test_index_join() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![5]]);
        let tv2 = TempVector::new_from_entries(tv.job_ref(), vec![u64vec(2),u64vec(0)]);
        let tv3 = TempVector::index_join(&tv, &tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![5],vec![3]]);
    }

    #[test]
    fn test_seq_u64() {
        let tv = TempVector::seq_u64_native(&test_job(), 3, 2);
        assert_eq!(tv.get_entries().unwrap(), vec![u64vec(3),u64vec(4)]);
    }
}
