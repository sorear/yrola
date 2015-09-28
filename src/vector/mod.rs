//! Module documentation goes here

use std::sync::Arc;
use ::Result;

// TODO add resource accounting, allocations/time (Jobs)
// TODO vectors should be owned by jobs, with a fictitious duplicate for recycling and columns
// TODO vector operations should be lazy to allow for parallelism
// TODO vector maybe *shouldn't* be thread-safe, since the parallelism we want is all internal
// (may require splitting temporary from persistant at the type level; persistant must be Sync)

// Minimum viable vectors, TODO blittable, fixed-stride, scattered, and inline vectors
#[derive(Default,Clone)]
pub struct Vector {
    pub data: Arc<Vec<Vec<u8>>>, // TODO encapsulate
}

impl Vector {
    pub fn concat(vecs: &Vec<Vector>) -> Vector {
        Vector { data: Arc::new(vecs.iter().flat_map(|v| v.data.iter().cloned()).collect()) }
    }

    pub fn get_entries(&self) -> Vec<Vec<u8>> {
        (*self.data).clone()
    }

    // Returns a vector with the length of keys where each element of keys has been replaced with an index into table (host endian) or -1
    #[allow(unused_variables)]
    pub fn join_index(table: &Vector, keys: &Vector) -> Result<Vector> {
        unimplemented!()
    }

    // Returns table in the same order but with elements indexed by remove removed
    #[allow(unused_variables)]
    pub fn index_antijoin(table: &Vector, remove: &Vector) -> Result<Vector> {
        unimplemented!()
    }

    // Returns all indices which match the given values
    #[allow(unused_variables)]
    pub fn point_index(table: &Vector, key: &[u8]) -> Result<Vector> {
        unimplemented!()
    }

    // Returns query in same order with elements replaced by those from table
    #[allow(unused_variables)]
    pub fn index_join(table: &Vector, query: &Vector) -> Result<Vector> {
        unimplemented!()
    }

    #[allow(unused_variables)]
    pub fn seq_u64_native(start: u64, count: u64) -> Result<Vector> {
        unimplemented!()
    }

    pub fn len(&self) -> u64 { self.data.len() as u64 }
}
