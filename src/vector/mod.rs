// Module documentation goes here

use std::cell::RefCell;
use std::collections::{HashSet,HashMap};
use std::mem;
use std::rc::Rc;
use std::sync::{Arc,Mutex};
use std::iter::FromIterator;

use misc;
use {Result,Error};

// TODO add resource accounting, allocations/time (Jobs)
// TODO vectors should be owned by jobs, with a fictitious duplicate for recycling and columns
// TODO vector operations should be lazy to allow for parallelism
// Minimum viable vectors, TODO blittable, fixed-stride, scattered, and inline vectors

// TODO byte level representations: blob64, varblob, fixed(N), swab{2,4,8}, dictblob, constant,
// range representation issue is also closely related to the NUMA/clustering angle, and may need to
// "recurse"; a sharded column could be a join in each shard

// A home for all global tuning parameters for the execution
#[derive(Debug)]
struct EngineBody;
#[derive(Clone,Debug)]
pub struct Engine(Arc<EngineBody>);

impl Engine {
    pub fn new() -> Engine {
        Engine(Arc::new(EngineBody))
    }
}

// Jobs and temporaries must not be shared between threads; the vector system is internally threaded
#[derive(Debug)]
struct JobBody {
    stats: Mutex<JobStatistics>,
    name: String,
    engine: Engine,
}
#[derive(Clone,Debug)]
pub struct Job(Arc<JobBody>);

impl Job {
    pub fn new(engine: &Engine, name: String) -> Job {
        Job(Arc::new(JobBody {
            stats: Mutex::new(JobStatistics {
                aborted: false,
                done: false,
            }),
            name: name,
            engine: engine.clone(),
        }))
    }

    pub fn name(&self) -> &String { &self.0.name }

    pub fn engine(&self) -> &Engine { &self.0.engine }
}

eq_rcwrapper!(Job);

#[derive(Debug)]
struct JobStatistics {
    aborted: bool,
    done: bool,
}

#[derive(Clone)]
pub struct TempVector(Rc<TempVectorBody>);

enum TempVectorState {
    Data(Arc<Vec<Vec<u8>>>),
    Thunk(Box<FnMut(&Job) -> Result<TempVector>>), // TODO FnBox once stable
    Failed(Error),
    Blackhole,
}

struct TempVectorBody {
    job: Job,
    state: RefCell<TempVectorState>,
}

// TODO Consider adding a separate sort of accounting for PersistVector (recycler, uncommitted txn, mmapped)
#[derive(Clone)]
pub struct PersistVector {
    data: Arc<Vec<Vec<u8>>>,
}

fn new_temp_state(job: &Job, state: TempVectorState) -> TempVector {
    TempVector(Rc::new(TempVectorBody {
        job: job.clone(),
        state: RefCell::new(state),
    }))
}
fn new_temp(job: &Job, data: Arc<Vec<Vec<u8>>>) -> TempVector {
    new_temp_state(job, TempVectorState::Data(data))
}

impl TempVector {
    fn force(self) -> Result<Arc<Vec<Vec<u8>>>> {
        use std::ops::DerefMut;
        let mut borrow = self.0.state.borrow_mut();
        let rv = match mem::replace(&mut *borrow, TempVectorState::Blackhole) {
            TempVectorState::Blackhole => panic!("Vector depends on itself"),
            TempVectorState::Failed(err) => Err(err),
            TempVectorState::Data(data) => Ok(data),
            TempVectorState::Thunk(mut fnbox) => fnbox.deref_mut()(&self.0.job).and_then(|v| v.force()),
        };
        *borrow = match rv {
            Err(ref err) => TempVectorState::Failed(err.clone()),
            Ok(ref data) => TempVectorState::Data(data.clone()),
        };
        rv
    }

    pub fn to_persistent(self) -> Result<PersistVector> {
        Ok(PersistVector { data: try!(self.force()) })
    }

    pub fn job(&self) -> &Job { &self.0.job }

    pub fn lazy<T>(job: &Job, func: T) -> TempVector
                   where T : 'static + FnOnce(&Job) -> Result<TempVector> {
        let mut mfunc = Some(func);
        let f2 : Box<FnMut(&Job) -> Result<TempVector>> =
            Box::new(move |jr| mfunc.take().unwrap()(jr));
        new_temp_state(job, TempVectorState::Thunk(f2))
    }

    pub fn concat(job: &Job, vecs: Vec<TempVector>) -> TempVector {
        assert!(vecs.iter().all(|v| job == v.job()),
            "trying to concatenate temporaries spanning jobs");
        Self::lazy(job, move |job| {
            let datas : Vec<Arc<Vec<Vec<u8>>>> =
                try!(vecs.into_iter().map(|v| v.force()).collect());
            Ok(new_temp(job, Arc::new(datas.iter().flat_map(|v| v.iter().cloned()).collect())))
        })
    }

    // don't expect these two to be performant
    pub fn get_entries(self) -> Result<Vec<Vec<u8>>> {
        Ok((*try!(self.force())).clone())
    }

    pub fn new_from_entries(job: &Job, entries: Vec<Vec<u8>>) -> TempVector {
        new_temp(job, Arc::new(entries))
    }

    // Returns a vector with the length of keys where each element of keys has been replaced with an index into table
    pub fn join_index(table: TempVector, keys: TempVector) -> TempVector {
        assert_eq!(table.job(), keys.job());
        let job = table.job().clone();
        Self::lazy(&job, move |job| {
            let tdata = try!(table.force());
            let kdata = try!(keys.force());
            let lookup = HashMap::<Vec<u8>,usize>::from_iter(tdata.iter().cloned().enumerate()
                .map(|(a,b)| (b,a)));
            Ok(new_temp(job, Arc::new(kdata.iter()
                .filter_map(|kk| lookup.get(kk).map(|v| misc::u64_to_bytes(*v as u64))).collect())))
        })
    }

    // Returns table in the same order but with elements indexed by remove removed
    pub fn index_antijoin(table: TempVector, remove: TempVector) -> TempVector {
        assert_eq!(table.job(), remove.job());
        let job = table.job().clone();
        Self::lazy(&job, move |job| {
            let tdata = try!(table.force());
            let rdata = try!(remove.force());
            let lookup = HashSet::<Vec<u8>>::from_iter(rdata.iter().cloned());
            Ok(new_temp(job, Arc::new(tdata.iter().enumerate()
                .filter_map(|(ix,v)| if lookup.contains(&misc::u64_to_bytes(ix as u64)) {
                    None
                } else {
                    Some(v)
                }).cloned().collect())))
        })
    }

    // Returns all indices which match the given values
    pub fn point_index(table: TempVector, key: Vec<u8>) -> TempVector {
        let job = table.job().clone();
        Self::lazy(&job, move |job| {
            let tdata = try!(table.force());
            Ok(new_temp(job, Arc::new(tdata.iter().enumerate()
                .filter_map(|(ix,v)| if *v == key { Some(misc::u64_to_bytes(ix as u64)) } else { None })
                .collect())))
        })
    }

    // Returns query in same order with elements replaced by those from table
    pub fn index_join(table: TempVector, query: TempVector) -> TempVector {
        assert_eq!(table.job(), query.job());
        let job = table.job().clone();
        Self::lazy(&job, move |job| {
            let tdata = try!(table.force());
            let qdata = try!(query.force());
            Ok(new_temp(job, Arc::new(qdata.iter()
                .filter_map(|ixv| misc::bytes_to_u64(ixv).and_then(|ix| tdata.get(ix as usize).cloned()))
                .collect())))
        })
    }

    pub fn seq_u64(job: &Job, start: u64, count: u64) -> TempVector {
        Self::lazy(&job, move |job| {
            Ok(new_temp(job, Arc::new((0 .. count).map(|ix| misc::u64_to_bytes(ix + start)).collect())))
        })
    }

    pub fn len(self) -> Result<u64> { Ok(try!(self.force()).len() as u64) }
}

impl PersistVector {
    pub fn to_temporary(&self, job: &Job) -> TempVector {
        new_temp(job, self.data.clone())
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use ::{Error};
    use super::*;
    use ::misc;

    fn test_job() -> Job {
        Job::new(&Engine::new(), From::from("Test job"))
    }

    #[test]
    fn job_name() { assert_eq!(test_job().name(), "Test job") }

    #[test]
    fn temp_entries() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        assert_eq!(tv.get_entries().unwrap(), vec![vec![1,2],vec![3,4]]);
    }

    #[test]
    fn test_lazy() {
        let jr = test_job();
        let tv1 = TempVector::lazy(&jr,
            |jr| Ok(TempVector::new_from_entries(&jr, vec![vec![1,2]])));
        let tv2 = TempVector::lazy(&jr, |_| Err(Error::Conflict));
        assert_eq!(tv1.get_entries().unwrap(), vec![vec![1,2]]);
        assert!(tv2.get_entries().is_err());
    }

    #[test]
    fn temp_len() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        assert_eq!(tv.len().unwrap(), 2);
    }

    #[test]
    fn test_persist() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3,4]]);
        let jr = tv.job().clone();
        let pv = tv.to_persistent().unwrap();
        let tv2 = pv.to_temporary(&jr);
        assert_eq!(tv2.get_entries().unwrap(), vec![vec![1,2],vec![3,4]]);
    }

    #[test]
    fn test_concat_1() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        let tv2 = TempVector::new_from_entries(&tv.job().clone(), vec![vec![1,2]]);
        let tv3 = TempVector::concat(&tv.job().clone(), vec![tv,tv2]);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![1,2],vec![1,2]]);
    }

    #[test]
    #[should_panic(expected="spanning jobs")]
    fn test_concat_2() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        let tv2 = TempVector::new_from_entries(&test_job(), vec![vec![1,2]]);
        TempVector::concat(&tv.job().clone(), vec![tv,tv2]);
    }

    #[test]
    fn test_join_index() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![1,2],vec![3]]);
        let tv2 = TempVector::new_from_entries(tv.job(), vec![vec![3],vec![1,2],vec![4]]);
        let tv3 = TempVector::join_index(tv, tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![misc::u64_to_bytes(1),misc::u64_to_bytes(0)]);
    }

    #[test]
    fn test_index_antijoin() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![5]]);
        let tv2 = TempVector::new_from_entries(tv.job(), vec![misc::u64_to_bytes(1)]);
        let tv3 = TempVector::index_antijoin(tv, tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![3],vec![5]]);
    }

    #[test]
    fn test_point_index() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![3]]);
        let tv2 = TempVector::point_index(tv, vec![3]);
        assert_eq!(tv2.get_entries().unwrap(), vec![misc::u64_to_bytes(0),misc::u64_to_bytes(2)]);
    }

    #[test]
    fn test_index_join() {
        let tv = TempVector::new_from_entries(&test_job(), vec![vec![3],vec![4],vec![5]]);
        let tv2 = TempVector::new_from_entries(tv.job(), vec![misc::u64_to_bytes(2),misc::u64_to_bytes(0)]);
        let tv3 = TempVector::index_join(tv, tv2);
        assert_eq!(tv3.get_entries().unwrap(), vec![vec![5],vec![3]]);
    }

    #[test]
    fn test_seq_u64() {
        let tv = TempVector::seq_u64(&test_job(), 3, 2);
        assert_eq!(tv.get_entries().unwrap(), vec![misc::u64_to_bytes(3),misc::u64_to_bytes(4)]);
    }
}
