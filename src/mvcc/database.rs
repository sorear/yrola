use std::sync::Arc;

use durability::{DurProvider,DurReadSection};
use vector::{TempVector,JobRef};
use ::{Result};
use misc;

pub struct AttachedDatabase {
    provider: Arc<DurProvider>,
}

enum VectorSource {
    NA,
    Unit(Vec<u8>),
    External(u64),
}

impl VectorSource {
    fn get_vector(&self, job: &JobRef, section: &mut DurReadSection) -> TempVector {
        match *self {
            VectorSource::NA => TempVector::new_from_entries(job, vec![]),
            VectorSource::Unit(ref v) => TempVector::new_from_entries(job, vec![v.clone()]),
            VectorSource::External(id) => {
                let rse = section.get_vector(id);
                TempVector::lazy(job, move |jr| Ok(try!(rse).to_temporary(jr)))
            }
        }
    }
}

struct Plank {
    insert_count: u64,
    tombstone_count: u64,
    vid_vector: VectorSource,
    tombstone_vector: VectorSource,
    column_vectors: Vec<VectorSource>,
    // TODO MVCC data
}

impl Plank {
    fn from_bytes(_data: &[u8]) -> Result<Plank> {
        unimplemented!()
    }
}

impl DurProvider {
    fn with_read_section<'s,F,R>(&'s self, mut func: F) -> Result<R> where F : FnMut(&mut DurReadSection) -> Result<R>, F : 's, R : 's {
        let mut rv = None;
        {
            let prv = &mut rv;
            let bx = Box::new(move |section: &mut DurReadSection| {
                func(section).map(|success| *prv = Some(success))
            });
            try!(self.read_section(bx));
        }
        Ok(rv.unwrap())
    }
}

// TODO aggressive caching for cords
impl AttachedDatabase {
    pub fn materialize_table(&self, job: &JobRef, table_id: u64, indices: Vec<u32>) -> Result<Vec<TempVector>> {
        let jobr = job.clone();
        self.provider.with_read_section(|section: &mut DurReadSection| {
            let cords = try!(self.get_cords(section));
            let planks_raw = self.point_query(&jobr, section, &cords[..], 0, &misc::u64_to_bytes(table_id)[..], &[1]).pop().unwrap();
            let planks_bytes = try!(planks_raw.get_entries());
            let planks : Vec<Plank> =
                try!(planks_bytes.into_iter().map(|data| Plank::from_bytes(&data[..])).collect());
            Ok(self.materialize_columns(&jobr, section, &planks[..], &indices[..]))
        })
    }

    fn get_cords(&self, section: &mut DurReadSection) -> Result<Vec<Plank>> {
        let raw = try!(section.get_journal_items(0));
        raw.into_iter().map(|(_id,data)|
            Plank::from_bytes(&data[..])).collect()
    }

    fn materialize_columns(&self, job: &JobRef, section: &mut DurReadSection, planks: &[Plank], indices: &[u32]) -> Vec<TempVector> {
        let merged_vid = TempVector::concat(job, planks.iter()
            .map(|plank| plank.vid_vector.get_vector(job, section)).collect());
        let merged_tombstone = TempVector::concat(job, planks.iter()
            .map(|plank| plank.tombstone_vector.get_vector(job, section)).collect());
        let zap = TempVector::join_index(merged_vid, merged_tombstone);
        indices.iter().map(|ix_p| {
            let ix = *ix_p;
            let merged_column = TempVector::concat(job, planks.iter()
                .map(|plank| {
                    match plank.column_vectors.get(ix as usize) {
                        None => TempVector::lazy(job, |_| Err(::corruption("short plank"))),
                        Some(col) => col.get_vector(job, section),
                    }
                }).collect());
            TempVector::index_antijoin(merged_column, zap.clone())
        }).collect()
    }

    fn point_query(&self, job: &JobRef, section: &mut DurReadSection, planks: &[Plank], query_col: u32, point: &[u8], indices: &[u32]) -> Vec<TempVector> {
        let mut vindices = Vec::from(indices);
        vindices.push(query_col);
        let mut cols = self.materialize_columns(job, section, planks, &vindices[..]);
        let query_data = cols.pop().unwrap();
        let index = TempVector::point_index(query_data, Vec::from(point));
        cols.into_iter().map(|cvec| TempVector::index_join(cvec, index.clone())).collect()
    }
}
