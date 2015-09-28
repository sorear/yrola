//! Multiversion concurrency control and table management for Yrola

use std::cell::Cell;
use std::cmp;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::ops::Deref;
use std::result::Result::{Ok};
use std::sync::{Arc,RwLock,RwLockReadGuard};
use std::{u32,u64};

use byteorder::{NativeEndian,ByteOrder};
use rmp;

use ::{Result,Error,corruption};
use durability::DurabilityProvider;
use vector::Vector;

/// Yrola MVCC engine - one per process
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
        let mut st = self.current_stamp.write().unwrap();
        let new = cmp::max(st.get(), bump) + 1;
        st.set(new);
        new
    }

    // TODO read-only mode?
    pub fn attach_database(&self, provider: Box<DurabilityProvider>) -> Result<u32> {
        let mut list = self.attached.write().unwrap();

        let cords : Vec<Plank> = try!(provider.get_journal_items().values()
            .map(|ji| Plank::parse_bytes(ji)).collect());

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

    pub fn detach_database(&self, db_id: u32) -> Result<()> {
        unimplemented!()
    }
}

struct Attachment {
    provider: Box<DurabilityProvider>,
    db_id: u32,
    attached_at_stamp: u64,
}

impl Attachment {
    fn get_cords(&self) -> Result<Vec<Plank>> {
        self.provider.get_journal_items().values().map(|v| Plank::parse_bytes(v)).collect()
    }

    fn materialize_column(&self, planks: &mut [Plank], column_index: u32) -> Result<Vector> {
        // TODO this assumes the vid / ts / data layout
        if planks.len() == 0 {
            return Err(Error::Corruption { detail: "attempt to read from nonexistant table".to_string() });
        }
        if planks[0].segments.len() < 2 || column_index as usize > planks[0].segments.len() - 2 {
            return Err(Error::Corruption { detail: "attempt to read out of range column".to_string() });
        }
        let vid_merged = Vector::concat(&try!(planks.iter_mut()
            .map(|p| p.segments[0].load(self)).collect()));
        let ts_merged = Vector::concat(&try!(planks.iter_mut()
            .map(|p| p.segments[1].load(self)).collect()));
        let data_merged = Vector::concat(&try!(planks.iter_mut()
            .map(|p| p.segments[(column_index as usize) + 2].load(self)).collect()));
        // TODO a multi-materialize would allow sharing the antijoin work
        let ts_indices = try!(Vector::join_index(&vid_merged, &ts_merged));
        Vector::index_antijoin(&data_merged, &ts_indices)
    }

    // This is separate so that it can have a more efficient implementation (Not fully materializing)
    fn point_query(&self, planks: &mut [Plank], lookup_column_index: u32,
                   lookup_column_value: &[u8], fetch_column_indices: &[u32])
                   -> Result<Vec<Vector>> {
        let lookup_col = try!(self.materialize_column(planks, lookup_column_index));
        let indices = try!(Vector::point_index(&lookup_col, lookup_column_value));

        fetch_column_indices.iter().map(|ix| {
            let data_col = try!(self.materialize_column(planks, *ix));
            Vector::index_join(&data_col, &indices)
        }).collect()
    }
}

/// The data which needs to be kept by an uncommitted transaction
pub struct Transaction {
    mvcc: Arc<MVCC>,
    start_stamp: u64,

    uncommitted: HashMap<(u32, u64), Vec<Plank>>,
    // TODO need a way to say "nuke all existing planks" for TRUNCATE/DROP TABLE
    // TODO also need a way to nuke the planks but keep the vectors for zero-copy ALTER (?)
}

const MIN_USER_TABLE: u64 = 256;
const PLANK_TABLE: u64 = 0;
const MAX_TABLE_COLUMNS: u32 = u32::MAX / 4;

impl Transaction {
    fn materialize_table(&self, attach: &Attachment, table_id: u64) -> Result<Vec<Plank>> {
        // Need to hold the read lock for the duration of this function to prevent external vectors from being deleted out from under us
        let mut cords = try!(attach.get_cords());

        if table_id == PLANK_TABLE {
            return Ok(cords);
        }

        let mut ix = [0; 8];
        NativeEndian::write_u64(&mut ix, table_id);
        let tbl_planks_vecs = try!(attach.point_query(&mut cords[..], 0, &ix, &[1]));
        // TODO consider making the planks table three-column and having max_stamp be volatile for efficient merging

        let mut planks : Vec<Plank> = try!(tbl_planks_vecs[0].get_entries().iter()
            .map(|pb| Plank::parse_bytes(pb)).collect());
        planks.retain(|pl| self.start_stamp >= pl.min_stamp && self.start_stamp < pl.max_stamp);
        if let Some(unc) = self.uncommitted.get(&(attach.db_id, table_id)) {
            planks.extend(unc.clone()); // TODO this defeats the caching in LazyVector :(
        };

        Ok(planks)
    }

    // Trying to access a nonexistant table or column returns Error::Corruption, on the assumption that you followed a broken link
    pub fn materialize_column(&self, db_id: u32, table_id: u64, column_index: u32)
                              -> Result<Vector> {
        let list_g = self.mvcc.attached.read().unwrap();
        let attach_g = try!(list_g.get(db_id as usize).ok_or(Error::DatabaseDetached)).read().unwrap();
        let mut planks = try!(self.materialize_table(attach_g.deref(), table_id));
        attach_g.materialize_column(&mut planks[..], column_index)
    }

    pub fn mutate_table(&mut self, db_id: u32, table_id: u64, vids_to_delete: &Vector,
                        row_count: u64, append: &Vec<Vector>) -> Result<u64> {
        // TODO interface is unnecessarily restricted, not all tables have vids
        let list_g = self.mvcc.attached.read().unwrap();
        let attach_g = try!(list_g.get(db_id as usize).ok_or(Error::DatabaseDetached)).read().unwrap();
        let mut planks = try!(self.materialize_table(attach_g.deref(), table_id));

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
            vector: try!(Vector::seq_u64_native(last_vid + 1, row_count))
        });
        segments.push(LazyVector::Loaded { vector: vids_to_delete.clone() });
        for aseg in append {
            if aseg.len() as u64 != row_count {
                return Err(corruption("mismatched lengths in insert"));
            }
            segments.push(LazyVector::Loaded { vector: aseg.clone() });
        }

        let new_plank = Plank {
            min_stamp: 0, max_stamp: 0, // will be filled in on commit
            last_vid: last_vid + row_count,
            row_count: row_count,
            tombstone_count: vids_to_delete.len(),
            segments: segments,
        };

        self.uncommitted.entry((db_id, table_id)).or_insert_with(|| Vec::new()).push(new_plank);

        Ok(last_vid + 1)
    }

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
    Loaded { vector: Vector },
}

impl LazyVector {
    fn load(&mut self, att: &Attachment) -> Result<Vector> {
        match *self {
            LazyVector::Loaded { ref vector } => Ok(vector.clone()),
            LazyVector::Unloaded { filenum } => {
                let vec = try!(att.provider.get_vector(filenum));
                *self = LazyVector::Loaded { vector: vec.clone() };
                Ok(vec)
            }
        }
    }

    // TODO: inline short vectors
    // TODO: not sure about rmp.
    fn parse<R>(rd: &mut R) -> Result<LazyVector> where R: Read {
        let nr = try!(rmp::decode::read_u64_fit(rd));
        Ok(LazyVector::Unloaded { filenum: nr })
    }
}

/// A section of a table; cannot usefully be interpreted without schema information
#[derive(Clone)]
struct Plank {
    min_stamp: u64,
    max_stamp: u64,
    last_vid: u64,
    row_count: u64,
    tombstone_count: u64,
    // TODO add LowSchema.  For now, assume that 0: vid 1: ts 2...: cols
    // TODO actually it may make sense for the LowSchema to live in a separate segment.  TBD
    segments: Vec<LazyVector>,
}

impl Plank {
    fn parse<R>(rd: &mut R) -> Result<Plank> where R: Read {
        Ok(Plank {
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

    fn parse_bytes(data: &[u8]) -> Result<Plank> {
        let mut csr = Cursor::new(data);
        let plank = try!(Plank::parse(&mut csr));
        if csr.position() == data.len() as u64 {
            Ok(plank)
        } else {
            Err(Error::Corruption { detail: "Trailing garbage after plank".to_string() })
        }
    }
}
