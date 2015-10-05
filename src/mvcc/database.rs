use std::sync::Arc;

use durability::{DurProvider,DurReadSection};
use vector::{TempVector,Job};
use Result;
use misc;

struct AttachedDatabaseBody {
    provider: Arc<DurProvider>,
}
pub struct AttachedDatabase(Arc<AttachedDatabaseBody>);

enum VectorSource {
    NA,
    Unit(Vec<u8>),
    External(u64),
}

impl VectorSource {
    fn get_vector(&self, job: &Job, section: &mut DurReadSection) -> TempVector {
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

// TODO: Are undo logs a kind of DataPlank, or a totally different thing?
struct DataPlank {
    insert_count: u64,
    tombstone_count: u64,
    write_stamp: u64,
    next_vid: u64,
    next_pkey: u64,
    vid_vector: VectorSource,
    tombstone_vector: VectorSource,
    column_vectors: Vec<VectorSource>,
}

struct LowSchemaPlank {
    column_count: u32,
    schema_stamp: u64,
    // also belongs here: undo logs flag, VID elision flag(s), dynamic columns, possibly indices (unless they are fully dynamic)
    // prototype pointer belongs at the next level up for latency hiding (query two table_ids at once)
    // default (lowest overhead) concurrency is READ COMMITTED with table (or prototype) level conflict detection
}

enum Plank {
    LowSchema(LowSchemaPlank),
    Data(DataPlank),
}

impl Plank {
    fn from_bytes(_data: &[u8]) -> Result<Plank> {
        unimplemented!()
    }
}

// TODO aggressive caching for cords
impl AttachedDatabase {
    pub fn new(p: Arc<DurProvider>) -> Result<AttachedDatabase> {
        let db = AttachedDatabase(Arc::new(AttachedDatabaseBody { provider: p }));
        Ok(db)
    }

    pub fn materialize_table(&self, job: &Job, table_id: u64, indices: Vec<u32>) -> Result<Vec<TempVector>> {
        let jobr = job.clone();
        self.0.provider.with_read_section(|section: &mut DurReadSection| {
            let cords = try!(self.get_cords(section));
            let planks_raw = self.point_query(&jobr, section, &cords[..], 0, &misc::u64_to_bytes(table_id)[..], &[1]).pop().unwrap();
            let planks_bytes = try!(planks_raw.get_entries());
            let planks : Vec<Plank> =
                try!(planks_bytes.into_iter().map(|data| Plank::from_bytes(&data[..])).collect());
            Ok(self.materialize_columns(&jobr, section, &planks[..], &indices[..]))
        })
    }

    pub fn create_table(&self, _job: &Job, _column_count: u32) -> Result<u64> {
        unimplemented!()
    }

    fn get_cords(&self, section: &mut DurReadSection) -> Result<Vec<Plank>> {
        let raw = try!(section.get_journal_items(0));
        raw.into_iter().map(|(_id,data)|
            Plank::from_bytes(&data[..])).collect()
    }

    fn materialize_columns(&self, job: &Job, section: &mut DurReadSection, planks: &[Plank], indices: &[u32]) -> Vec<TempVector> {
        let data_planks : Vec<_> = planks.iter().filter_map(|pl| {
            match *pl {
                Plank::Data(ref dp) => Some(dp),
                _ => None,
            }
        }).collect();
        let merged_vid = TempVector::concat(job, data_planks.iter()
            .map(|plank| plank.vid_vector.get_vector(job, section)).collect());
        let merged_tombstone = TempVector::concat(job, data_planks.iter()
            .map(|plank| plank.tombstone_vector.get_vector(job, section)).collect());
        let zap = TempVector::join_index(merged_vid, merged_tombstone);
        indices.iter().map(|ix_p| {
            let ix = *ix_p;
            let merged_column = TempVector::concat(job, data_planks.iter()
                .map(|plank| {
                    match plank.column_vectors.get(ix as usize) {
                        None => TempVector::lazy(job, |_| Err(::corruption("short plank"))),
                        Some(col) => col.get_vector(job, section),
                    }
                }).collect());
            TempVector::index_antijoin(merged_column, zap.clone())
        }).collect()
    }

    fn point_query(&self, job: &Job, section: &mut DurReadSection, planks: &[Plank], query_col: u32, point: &[u8], indices: &[u32]) -> Vec<TempVector> {
        let mut vindices = Vec::from(indices);
        vindices.push(query_col);
        let mut cols = self.materialize_columns(job, section, planks, &vindices[..]);
        let query_data = cols.pop().unwrap();
        let index = TempVector::point_index(query_data, Vec::from(point));
        cols.into_iter().map(|cvec| TempVector::index_join(cvec, index.clone())).collect()
    }
}

#[cfg(test)]
mod test {
    use durability::NoneDurProvider;
    use vector::{Job,Engine};
    use std::sync::Arc;
    use super::AttachedDatabase;

    fn new_db() -> AttachedDatabase {
        AttachedDatabase::new(Arc::new(NoneDurProvider::new())).unwrap()
    }
    fn new_job() -> Job {
        Job::new(&Engine::new(), From::from("Test job"))
    }

    #[test]
    fn instantiate() {
        let _db = new_db();
    }

    #[test]
    fn create_table() {
        let db = new_db();
        let job = new_job();
        let _tbl = db.create_table(&job, 3).unwrap();
    }
}
