use std::sync::{Arc};
use std::io::{Read,Write,Cursor};
use std::collections::HashMap;

use durability::{DurProvider,DurReadSection,DurPrepareSection,DurPrewriteVector};
use vector::{PersistVector,TempVector,Job};
use {Result,Error,corruption};
use misc;
use rmp;

// we favor roll-forward over roll-back as it is a better conceptual match to the notion of an append-only database
pub struct TransactionData {
    next_cord_id: u64,
    new_cords: Vec<Vec<u8>>,
    new_files: HashMap<u64, Box<DurPrewriteVector>>,
}

impl TransactionData {
    fn new() -> Self {
        TransactionData { next_cord_id: 0, new_cords: Vec::new(), new_files: HashMap::new() }
    }
}

// TODO : Consider drawing a type distinction between VectorSource and Plank which is used for export, import, or consumption
enum VectorSource {
    NA,
    Unit(Vec<u8>),
    External(u64),
    Loaded(PersistVector),
}

impl VectorSource {
    fn get_vector(&self, trans: &mut TransactionData, job: &Job, section: &mut DurReadSection) -> TempVector {
        match *self {
            VectorSource::NA => TempVector::new_from_entries(job, vec![]),
            VectorSource::Unit(ref v) => TempVector::new_from_entries(job, vec![v.clone()]),
            VectorSource::External(id) => {
                match trans.new_files.get(&id) {
                    None => {
                        let rse = section.get_vector(id);
                        TempVector::lazy(job, move |jr| Ok(try!(rse).to_temporary(jr)))
                    },
                    Some(ref prewrite) => {
                        prewrite.data().to_temporary(job)
                    }
                }
            },
            VectorSource::Loaded(ref pv) => {
                pv.to_temporary(job)
            }
        }
    }

    fn parse<R>(rd: &mut R) -> Result<VectorSource> where R: Read {
        let tag = try!(rmp::decode::read_u64_fit(rd));
        match tag {
            1 => Ok(VectorSource::NA),
            2 => {
                let len = try!(rmp::decode::read_bin_len(rd));
                let mut copy = vec![0; len as usize];
                try!(rd.read(&mut copy[..]));
                Ok(VectorSource::Unit(copy))
            },
            3 => Ok(VectorSource::External(try!(rmp::decode::read_u64_fit(rd)))),
            _ => Err(corruption("Unknown tag")),
        }
    }

    fn write<W>(&self, wr: &mut W, section: &mut DurReadSection, trans: &mut TransactionData) -> Result<()> where W: Write {
        match *self {
            VectorSource::NA => {
                try!(rmp::encode::write_uint(wr, 1));
            },
            VectorSource::Unit(ref data) => {
                try!(rmp::encode::write_uint(wr, 2));
                try!(rmp::encode::write_bin_len(wr, data.len() as u32));
                try!(wr.write(&data[..]));
            },
            VectorSource::External(id) => {
                try!(rmp::encode::write_uint(wr, 3));
                try!(rmp::encode::write_uint(wr, id));
            },
            VectorSource::Loaded(ref pvec) => {
                try!(rmp::encode::write_uint(wr, 3));
                let cookie = try!(section.alloc_vector_id(&pvec));
                try!(rmp::encode::write_uint(wr, cookie.id()));
                trans.new_files.insert(cookie.id(), cookie);
            },
        }
        Ok(())
    }
}

// TODO: Are undo logs a kind of DataPlank, or a totally different thing?
struct DataPlank {
    insert_count: u64,
    tombstone_count: u64,
    //write_stamp: u64,
    last_vid: u64,
    vid_vector: VectorSource,
    tombstone_vector: VectorSource,
    column_vectors: Vec<VectorSource>,
}

impl DataPlank {
    fn parse<R>(rd: &mut R) -> Result<DataPlank> where R: Read {
        Ok(DataPlank {
            insert_count: try!(rmp::decode::read_u64_fit(rd)),
            tombstone_count: try!(rmp::decode::read_u64_fit(rd)),
            last_vid: try!(rmp::decode::read_u64_fit(rd)),
            vid_vector: try!(VectorSource::parse(rd)),
            tombstone_vector: try!(VectorSource::parse(rd)),
            column_vectors: {
                let count = try!(rmp::decode::read_u64_fit(rd));
                let mut out = Vec::new();
                for _ in 0 .. count {
                    out.push(try!(VectorSource::parse(rd)));
                }
                out
            }
        })
    }

    fn write<W>(&self, wr: &mut W, section: &mut DurReadSection, trans: &mut TransactionData) -> Result<()> where W: Write {
        try!(rmp::encode::write_uint(wr, self.insert_count));
        try!(rmp::encode::write_uint(wr, self.tombstone_count));
        try!(rmp::encode::write_uint(wr, self.last_vid));
        try!(self.vid_vector.write(wr, section, trans));
        try!(self.tombstone_vector.write(wr, section, trans));
        try!(rmp::encode::write_uint(wr, self.column_vectors.len() as u64));
        for cv in &self.column_vectors {
            try!(cv.write(wr, section, trans));
        }
        Ok(())
    }
}

struct LowSchemaPlank {
    column_count: u32,
    //schema_stamp: u64,
    // also belongs here: undo logs flag, VID elision flag(s), dynamic columns, possibly indices (unless they are fully dynamic)
    // prototype pointer belongs at the next level up for latency hiding (query two table_ids at once)
    // default (lowest overhead) concurrency is READ COMMITTED with table (or prototype) level conflict detection
}

impl LowSchemaPlank {
    fn parse<R>(rd: &mut R) -> Result<LowSchemaPlank> where R: Read {
        Ok(LowSchemaPlank {
            column_count: try!(rmp::decode::read_u32_fit(rd)),
        })
    }

    fn write<W>(&self, wr: &mut W) -> Result<()> where W: Write {
        try!(rmp::encode::write_uint(wr, self.column_count as u64));
        Ok(())
    }
}

enum Plank {
    LowSchema(LowSchemaPlank),
    Data(DataPlank),
}

impl Plank {
    fn from_bytes(data: &[u8]) -> Result<Plank> {
        let mut rd = Cursor::new(data);
        let plank = try!(Plank::parse(&mut rd));
        if (rd.position() as usize) < data.len() {
            return Err(corruption("trailing garbage"));
        }
        Ok(plank)
    }

    fn to_bytes(self: &Plank, section: &mut DurReadSection, trans: &mut TransactionData) -> Result<Vec<u8>> {
        let mut wr = Cursor::new(Vec::new());
        try!(self.write(&mut wr, section, trans));
        Ok(wr.into_inner())
    }

    fn parse<R>(rd: &mut R) -> Result<Plank> where R: Read {
        let tag = try!(rmp::decode::read_u8_fit(rd));
        match tag {
            1 => Ok(Plank::LowSchema(try!(LowSchemaPlank::parse(rd)))),
            2 => Ok(Plank::Data(try!(DataPlank::parse(rd)))),
            _ => Err(corruption("Invalid tag")),
        }
    }

    fn write<W>(&self, wr: &mut W, section: &mut DurReadSection, trans: &mut TransactionData) -> Result<()> where W: Write {
        match *self {
            Plank::LowSchema(ref ls) => {
                try!(rmp::encode::write_uint(wr, 1));
                try!(ls.write(wr));
            },
            Plank::Data(ref pd) => {
                try!(rmp::encode::write_uint(wr, 2));
                try!(pd.write(wr, section, trans));
            }
        };
        Ok(())
    }
}

struct AttachedDatabaseBody {
    provider: Arc<DurProvider>, // TODO: this duplicates an Arc in DurProvider
}
pub struct AttachedDatabase(Arc<AttachedDatabaseBody>);

// TODO aggressive caching for cords
impl AttachedDatabase {
    pub fn new(p: Arc<DurProvider>) -> Result<AttachedDatabase> {
        let db = AttachedDatabase(Arc::new(AttachedDatabaseBody {
            provider: p,
        }));
        Ok(db)
    }

    pub fn materialize_table(&self, trans: &mut TransactionData, job: &Job, table_id: u64, indices: Vec<u32>) -> Result<Vec<TempVector>> {
        let mut section = self.0.provider.read_section();
        let planks = try!(self.planks_for_table(trans, job, &mut *section, table_id));
        Ok(self.materialize_columns(trans, job, &mut *section, &planks[..], &indices[..]))
    }

    pub fn create_table(&self, trans: &mut TransactionData, job: &Job, column_count: u32) -> Result<u64> {
        let mut section = self.0.provider.read_section();
        let cords = try!(self.get_cords(trans, &mut *section));
        let new_table_id = try!(try!(self.get_last_vid(job, &mut *section, &cords[..]))
            .checked_add(1).ok_or_else::<Error,_>(|| unimplemented!()));
        try!(self.append_plank(trans, job, &mut *section, new_table_id, &Plank::LowSchema(LowSchemaPlank {
            column_count: column_count,
        })));
        Ok(new_table_id)
    }

    pub fn insert_rows(&self, trans: &mut TransactionData, job: &Job, table_id: u64, row_count: u64, data: Vec<TempVector>) -> Result<()> {
        let mut section = self.0.provider.read_section();
        let planks = try!(self.planks_for_table(trans, job, &mut *section, table_id));
        let schema = try!(self.get_table_schema(&planks[..]));
        let last_vid = try!(self.get_last_vid(job, &mut *section, &planks[..]));
        if last_vid.checked_add(row_count).is_none() {
            unimplemented!()
        }
        if data.len() != (schema.column_count as usize) {
            unimplemented!()
        }
        let mut persist_copies = Vec::new();
        for temp_vec in data {
            let persist_vec = try!(temp_vec.to_persistent());
            if persist_vec.len() != row_count {
                unimplemented!();
            }
            persist_copies.push(VectorSource::Loaded(persist_vec));
        }
        let new_plank = Plank::Data(DataPlank {
            insert_count: row_count, last_vid: last_vid + row_count, tombstone_count: 0,
            vid_vector: VectorSource::Loaded(try!(TempVector::seq_u64(job, last_vid + 1, row_count).to_persistent())),
            tombstone_vector: VectorSource::NA,
            column_vectors: persist_copies,
        });
        try!(self.append_plank(trans, job, &mut *section, table_id, &new_plank));
        Ok(())
    }

    pub fn commit(&self, trans: TransactionData) -> Result<()> {
        if trans.new_cords.len() == 0 {
            return Ok(());
        }

        {
            let mut section = self.0.provider.prepare_section();
            if section.is_read().next_journal_id() != trans.next_cord_id {
                unimplemented!();
            }
            for vec in trans.new_cords {
                try!(section.add_journal_item(0, vec.as_ref()));
            }
            for (_, cookie) in trans.new_files {
                try!(section.save_vector(cookie));
            }
            // unimplemented!() TODO atomic commit
        }

        Ok(())
    }

    fn planks_for_table(&self, trans: &mut TransactionData, job: &Job, section: &mut DurReadSection, table_id: u64) -> Result<Vec<Plank>> {
        let cords = try!(self.get_cords(trans, section));
        let planks_raw = self.point_query(trans, job, section, &cords[..], 0, misc::u64_to_bytes(table_id).as_ref(), &[1]).pop().unwrap();
        let planks_bytes = try!(planks_raw.get_entries());
        planks_bytes.into_iter().map(|data| Plank::from_bytes(&data[..])).collect()
    }

    fn append_plank(&self, trans: &mut TransactionData, job: &Job, section: &mut DurReadSection, table_id: u64, new_plank: &Plank) -> Result<u64> {
        let cords = try!(self.get_cords(trans, section));
        let last_id = try!(self.get_last_vid(job, section, &cords[..]));
        let new_id = try!(last_id.checked_add(1).ok_or_else::<Error,_>(|| unimplemented!()));
        let new_cord = Plank::Data(DataPlank {
            insert_count: 1, tombstone_count: 0, last_vid: new_id,
            vid_vector: VectorSource::Unit(misc::u64_to_bytes(new_id)),
            tombstone_vector: VectorSource::NA,
            column_vectors: vec![
                VectorSource::Unit(misc::u64_to_bytes(table_id)),
                VectorSource::Unit(try!(Plank::to_bytes(new_plank, section, trans))),
            ]
        });
        try!(self.append_cord(trans, job, section, &new_cord));
        Ok(new_id)
    }

    fn append_cord(&self, trans: &mut TransactionData, _job: &Job, section: &mut DurReadSection, new_cord: &Plank) -> Result<u64> {
        let cord_bytes = try!(Plank::to_bytes(new_cord, section, trans));
        if trans.new_cords.len() == 0 {
            trans.next_cord_id = section.next_journal_id();
        }
        trans.new_cords.push(cord_bytes);
        Ok(trans.next_cord_id + (trans.new_cords.len() as u64) - 1)
    }

    fn get_cords(&self, trans: &mut TransactionData, section: &mut DurReadSection) -> Result<Vec<Plank>> {
        let mut raw : Vec<Vec<u8>> = try!(section.get_journal_items(0)).into_iter().map(|(_id,data)| data).collect();
        raw.extend(trans.new_cords.clone());
        raw.into_iter().map(|data| Plank::from_bytes(&data[..])).collect()
    }

    fn get_last_vid(&self, _job: &Job, _section: &mut DurReadSection, planks: &[Plank]) -> Result<u64> {
        Ok(planks.iter().filter_map(|pl| {
            if let Plank::Data(ref pd) = *pl {
                Some(pd.last_vid)
            } else {
                None
            }
        }).max().unwrap_or(0))
    }

    fn get_table_schema<'a,'b>(&'a self, planks: &'b [Plank]) -> Result<&'b LowSchemaPlank> {
        let mut found = None;
        for plank in planks {
            if let Plank::LowSchema(ref low_schema) = *plank {
                if found.is_some() { unimplemented!(); }
                found = Some(low_schema);
            }
        }
        found.ok_or_else(|| unimplemented!())
    }

    fn materialize_columns(&self, trans: &mut TransactionData, job: &Job, section: &mut DurReadSection, planks: &[Plank], indices: &[u32]) -> Vec<TempVector> {
        let data_planks : Vec<_> = planks.iter().filter_map(|pl| {
            match *pl {
                Plank::Data(ref dp) => Some(dp),
                _ => None,
            }
        }).collect();
        let merged_vid = TempVector::concat(job, data_planks.iter()
            .map(|plank| plank.vid_vector.get_vector(trans, job, section)).collect());
        let merged_tombstone = TempVector::concat(job, data_planks.iter()
            .map(|plank| plank.tombstone_vector.get_vector(trans, job, section)).collect());
        let zap = TempVector::join_index(merged_vid, merged_tombstone);
        indices.iter().map(|ix_p| {
            let ix = *ix_p;
            let merged_column = TempVector::concat(job, data_planks.iter()
                .map(|plank| {
                    match plank.column_vectors.get(ix as usize) {
                        None => TempVector::lazy(job, |_| Err(::corruption("short plank"))),
                        Some(col) => col.get_vector(trans, job, section),
                    }
                }).collect());
            TempVector::index_antijoin(merged_column, zap.clone())
        }).collect()
    }

    fn point_query(&self, trans: &mut TransactionData, job: &Job, section: &mut DurReadSection, planks: &[Plank], query_col: u32, point: &[u8], indices: &[u32]) -> Vec<TempVector> {
        let mut vindices = Vec::from(indices);
        vindices.push(query_col);
        let mut cols = self.materialize_columns(trans, job, section, planks, &vindices[..]);
        let query_data = cols.pop().unwrap();
        let index = TempVector::point_index(query_data, Vec::from(point));
        cols.into_iter().map(|cvec| TempVector::index_join(cvec, index.clone())).collect()
    }
}

#[cfg(test)]
mod test {
    use durability::NoneDurProvider;
    use vector::{Job,Engine,TempVector};
    use std::sync::Arc;
    use super::{AttachedDatabase,TransactionData};

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
    fn create_table_basic() {
        let db = new_db();
        let job = new_job();
        let mut trans = TransactionData::new();
        let tbl1 = db.create_table(&mut trans, &job, 3).unwrap();
        let tbl2 = db.create_table(&mut trans, &job, 3).unwrap();
        assert!(tbl1 != tbl2);
    }

    #[test]
    fn insert_rows_1() {
        let db = new_db();
        let job = new_job();
        let mut trans = TransactionData::new();
        let tbl1 = db.create_table(&mut trans, &job, 1).unwrap();
        assert_eq!(db.materialize_table(&mut trans, &job, tbl1, vec![0]).unwrap()[0].clone().len().unwrap(), 0);
        db.insert_rows(&mut trans, &job, tbl1, 3,
            vec![TempVector::new_from_entries(&job, vec![vec![1], vec![3], vec![2]])]).unwrap();
        assert_eq!(db.materialize_table(&mut trans, &job, tbl1, vec![0]).unwrap()[0].clone().get_entries().unwrap(),
            vec![vec![1], vec![3], vec![2]]);
        db.insert_rows(&mut trans, &job, tbl1, 1,
            vec![TempVector::new_from_entries(&job, vec![vec![4]])]).unwrap();
        assert_eq!(db.materialize_table(&mut trans, &job, tbl1, vec![0]).unwrap()[0].clone().get_entries().unwrap(),
            vec![vec![1], vec![3], vec![2], vec![4]]);
    }

    #[test]
    fn trans_1() {
        let db = new_db();
        let job = new_job();
        let tbl1;
        {
            let mut trans1 = TransactionData::new();
            tbl1 = db.create_table(&mut trans1, &job, 1).unwrap();
            db.insert_rows(&mut trans1, &job, tbl1, 1,
                vec![TempVector::new_from_entries(&job, vec![vec![4]])]).unwrap();
            db.commit(trans1).unwrap();
        }
        {
            let mut trans2 = TransactionData::new();
            assert_eq!(db.materialize_table(&mut trans2, &job, tbl1, vec![0]).unwrap()[0].clone().get_entries().unwrap(),
                vec![vec![4]]);
        }
    }

    #[test]
    fn trans_2() {
        let db = new_db();
        let job = new_job();
        let tbl1;
        {
            let mut trans1 = TransactionData::new();
            tbl1 = db.create_table(&mut trans1, &job, 1).unwrap();
            db.insert_rows(&mut trans1, &job, tbl1, 1,
                vec![TempVector::new_from_entries(&job, vec![vec![4]])]).unwrap();
            // NO commit
        }
        {
            let mut trans2 = TransactionData::new();
            assert_eq!(db.materialize_table(&mut trans2, &job, tbl1, vec![0]).unwrap()[0].clone().get_entries().unwrap(),
                Vec::<Vec<u8>>::new());
        }
    }
}
