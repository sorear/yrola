// Multiversion concurrency control and table management for Yrola

use std::cell::Cell;
use std::cmp;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::ops::Deref;
use std::result::Result::{Ok};
use std::sync::{Arc,RwLock};
use std::{u32,u64};

use byteorder::{NativeEndian,ByteOrder};
use rmp;

use ::{Result,Error,corruption};
use durability::DurabilityProvider;
use vector::{TempVector,PersistVector,JobRef};

// Yrola MVCC engine - one per process
pub struct MVCC {
    current_stamp: RwLock<Cell<u64>>,
    attached: RwLock<Vec<RwLock<Attachment>>>,
}

impl MVCC {
    pub fn new_transaction(dbref: &Arc<MVCC>) -> Transaction {
        return Transaction {
            mvcc: dbref.clone(),
            start_stamp: dbref.current_stamp.read().unwrap().get(),
            uncommitted: HashMap::new(),
        }
    }

    fn next_stamp(&self, bump: u64) -> u64 {
        let st = self.current_stamp.write().unwrap();
        let new = cmp::max(st.get(), bump) + 1;
        st.set(new);
        new
    }

    // TODO read-only mode?
    pub fn attach_database(&self, provider: Box<DurabilityProvider>) -> Result<u32> {
        let mut list = self.attached.write().unwrap();

        let cords : Vec<OldPlank> = try!(provider.get_journal_items().values()
            .map(|ji| OldPlank::parse_bytes(ji)).collect());

        if cords.len() == 0 {
            // TODO initialize the database
        }

        let last_stamp = cords.iter().map(|c| c.max_stamp).max().unwrap_or(0);
        let attach_stamp = self.next_stamp(last_stamp);
        let new_id = list.len() as u32;

        if new_id == u32::MAX {
            return Err(Error::Conflict);
        }

        list.push(RwLock::new(Attachment {
            provider: provider,
            db_id: new_id,
            attached_at_stamp: attach_stamp,
        }));

        Ok(new_id)
    }

    #[allow(unused_variables)]
    pub fn detach_database(&self, db_id: u32) -> Result<()> {
        unimplemented!()
    }
}

struct Plank {
    id: u64,
    internal: Vec<u8>,
    external: Vec<LazyVector>,
}

trait TableDescriptor {
    fn get_planks(&mut self) -> Result<Vec<Plank>>; // call this rarely
    fn update_planks(&mut self, delete: &[u64], append: &[Plank]) -> Result<()>; // will fill in id fields
}

trait TableControl<D: TableDescriptor> {
    fn materialize_column(&mut self, column_index: u32, at_stamp: u64) { }
}

struct Attachment {
    provider: Box<DurabilityProvider>,
    db_id: u32,
    attached_at_stamp: u64,
}

impl Attachment {
    fn get_cords(&self) -> Result<Vec<OldPlank>> {
        self.provider.get_journal_items().values().map(|v| OldPlank::parse_bytes(v)).collect()
    }

    fn materialize_column(&self, job: &JobRef, planks: &mut [OldPlank], column_index: u32)
                          -> Result<TempVector> {
        // TODO this assumes the vid / ts / data layout
        if planks.len() == 0 {
            return Err(Error::Corruption { detail: "attempt to read from nonexistant table".to_string() });
        }
        if planks[0].segments.len() < 2 || column_index as usize > planks[0].segments.len() - 2 {
            return Err(Error::Corruption { detail: "attempt to read out of range column".to_string() });
        }
        let vid_merged = TempVector::concat(job, try!(planks.iter_mut()
            .map(|p| p.segments[0].load_bind(self, job)).collect()));
        let ts_merged = TempVector::concat(job, try!(planks.iter_mut()
            .map(|p| p.segments[1].load_bind(self, job)).collect()));
        let data_merged = TempVector::concat(job, try!(planks.iter_mut()
            .map(|p| p.segments[(column_index as usize) + 2].load_bind(self, job)).collect()));
        // TODO a multi-materialize would allow sharing the antijoin work
        let ts_indices = TempVector::join_index(vid_merged, ts_merged);
        Ok(TempVector::index_antijoin(data_merged, ts_indices))
    }

    // This is separate so that it can have a more efficient implementation (Not fully materializing)
    fn point_query(&self, job: &JobRef, planks: &mut [OldPlank], lookup_column_index: u32,
                   lookup_column_value: Vec<u8>, fetch_column_indices: &[u32])
                   -> Result<Vec<TempVector>> {
        let lookup_col = try!(self.materialize_column(job, planks, lookup_column_index));
        let indices = TempVector::point_index(lookup_col, lookup_column_value);

        fetch_column_indices.iter().map(|ix| {
            let data_col = try!(self.materialize_column(job, planks, *ix));
            Ok(TempVector::index_join(data_col, indices.clone()))
        }).collect()
    }
}

// The data which needs to be kept by an uncommitted transaction
pub struct Transaction {
    mvcc: Arc<MVCC>,
    start_stamp: u64,

    uncommitted: HashMap<(u32, u64), Vec<OldPlank>>,
    // TODO need a way to say "nuke all existing planks" for TRUNCATE/DROP TABLE
    // TODO also need a way to nuke the planks but keep the vectors for zero-copy ALTER (?)
}

const MAX_TABLE_COLUMNS: u32 = u32::MAX / 4;

impl Transaction {
    fn materialize_table(&self, job: &JobRef, attach: &Attachment, table_id: u64)
                         -> Result<Vec<OldPlank>> {
        // Need to hold the read lock for the duration of this function to prevent external vectors from being deleted out from under us
        let mut cords = try!(attach.get_cords());

        let mut ix = [0; 8];
        NativeEndian::write_u64(&mut ix, table_id);
        let mut tbl_planks_vecs = try!(attach.point_query(job, &mut cords[..], 0, Vec::from(&ix as &[u8]), &[1]));
        // TODO consider making the planks table three-column and having max_stamp be volatile for efficient merging

        let plank_data = try!(tbl_planks_vecs.remove(0).get_entries()); // TODO can slice patterns be used here?
        let mut planks : Vec<OldPlank> = try!(plank_data.iter()
            .map(|pb| OldPlank::parse_bytes(pb)).collect());
        planks.retain(|pl| self.start_stamp >= pl.min_stamp && self.start_stamp < pl.max_stamp);
        if let Some(unc) = self.uncommitted.get(&(attach.db_id, table_id)) {
            planks.extend(unc.clone()); // TODO this defeats the caching in LazyVector :(
        };

        Ok(planks)
    }

    // Trying to access a nonexistant table or column returns Error::Corruption, on the assumption that you followed a broken link
    pub fn materialize_column(&self, job: &JobRef, db_id: u32, table_id: u64, column_index: u32)
                              -> Result<TempVector> {
        let list_g = self.mvcc.attached.read().unwrap();
        let attach_g = try!(list_g.get(db_id as usize).ok_or(Error::DatabaseDetached)).read().unwrap();
        let mut planks = try!(self.materialize_table(job, attach_g.deref(), table_id));
        attach_g.materialize_column(job, &mut planks[..], column_index)
    }

    pub fn mutate_table(&mut self, job: &JobRef, db_id: u32, table_id: u64,
                        vids_to_delete: TempVector, row_count: u64, append: Vec<TempVector>)
                        -> Result<u64> {
        // TODO interface is unnecessarily restricted, not all tables have vids
        let list_g = self.mvcc.attached.read().unwrap();
        let attach_g = try!(list_g.get(db_id as usize).ok_or(Error::DatabaseDetached)).read().unwrap();
        let planks = try!(self.materialize_table(job, attach_g.deref(), table_id));

        if planks.len() == 0 {
            return Err(corruption("modifying nonexistant table"));
        }

        if append.len() > MAX_TABLE_COLUMNS as usize || planks[0].segments.len() != append.len() + 2 {
            return Err(corruption("insert with wrong column count"));
        }

        // TODO using the last committed vid will create concurrency problems
        let last_vid = planks.iter().map(|p| p.last_vid).max().unwrap();
        if row_count > u64::MAX - last_vid {
            return Err(Error::Conflict); // will be more interesting once VID size is configurable
        }

        let mut segments = Vec::new();
        segments.push(LazyVector::Loaded {
            vector: try!(TempVector::seq_u64(job, last_vid + 1, row_count).to_persistent())
        });
        let tombstone_count = try!(vids_to_delete.clone().len());
        segments.push(LazyVector::Loaded { vector: try!(vids_to_delete.to_persistent()) });
        for aseg in append {
            if try!(aseg.clone().len()) as u64 != row_count {
                return Err(corruption("mismatched lengths in insert"));
            }
            segments.push(LazyVector::Loaded { vector: try!(aseg.to_persistent()) });
        }

        let new_plank = OldPlank {
            min_stamp: 0, max_stamp: 0, // will be filled in on commit
            last_vid: last_vid + row_count,
            row_count: row_count,
            tombstone_count: tombstone_count,
            segments: segments,
        };

        self.uncommitted.entry((db_id, table_id)).or_insert_with(|| Vec::new()).push(new_plank);

        Ok(last_vid + 1)
    }

    #[allow(unused_variables)]
    pub fn create_table(&mut self, db_id: u32, num_columns: u32) -> Result<u64> {
        unimplemented!()
    }

    pub fn commit(self) -> Result<()> {
        // TODO lock all attachments, do conflict check, create new cord
        unimplemented!()
    }
}

#[derive(Clone)]
enum LazyVector {
    Unloaded { filenum: u64 },
    Loaded { vector: PersistVector },
}

impl LazyVector {
    fn load(&mut self, att: &Attachment) -> Result<PersistVector> {
        match *self {
            LazyVector::Loaded { ref vector } => Ok(vector.clone()),
            LazyVector::Unloaded { filenum } => {
                let vec = try!(att.provider.get_vector(filenum));
                *self = LazyVector::Loaded { vector: vec.clone() };
                Ok(vec)
            }
        }
    }

    fn load_bind(&mut self, att: &Attachment, job: &JobRef) -> Result<TempVector> {
        Ok(try!(self.load(att)).to_temporary(job))
    }

    // TODO: inline short vectors
    // TODO: not sure about rmp.
    fn parse<R>(rd: &mut R) -> Result<LazyVector> where R: Read {
        let nr = try!(rmp::decode::read_u64_fit(rd));
        Ok(LazyVector::Unloaded { filenum: nr })
    }
}

// A section of a table; cannot usefully be interpreted without schema information
#[derive(Clone)]
struct OldPlank {
    // TODO ughh we need to move these into the segments table to allow for merging segments with different ages
    min_stamp: u64,
    max_stamp: u64,
    last_vid: u64,
    row_count: u64,
    tombstone_count: u64,
    // TODO add LowSchema.  For now, assume that 0: vid 1: ts 2...: cols
    // TODO actually it may make sense for the LowSchema to live in a separate segment.  TBD
    segments: Vec<LazyVector>,
}

impl OldPlank {
    fn parse<R>(rd: &mut R) -> Result<OldPlank> where R: Read {
        Ok(OldPlank {
            min_stamp: try!(rmp::decode::read_i64_fit(rd)) as u64,
            max_stamp: try!(rmp::decode::read_i64_fit(rd)) as u64,
            last_vid: try!(rmp::decode::read_u64_fit(rd)),
            row_count: try!(rmp::decode::read_u64_fit(rd)),
            tombstone_count: try!(rmp::decode::read_u64_fit(rd)),
            segments: {
                let count = try!(rmp::decode::read_array_size(rd));
                let mut segs = Vec::new();
                for _ in 0 .. count {
                    segs.push(try!(LazyVector::parse(rd)));
                }
                segs
            }
        })
    }

    fn parse_bytes(data: &[u8]) -> Result<OldPlank> {
        let mut csr = Cursor::new(data);
        let plank = try!(OldPlank::parse(&mut csr));
        if csr.position() == data.len() as u64 {
            Ok(plank)
        } else {
            Err(Error::Corruption { detail: "Trailing garbage after plank".to_string() })
        }
    }
}
