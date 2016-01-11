use borrow_segments::{self, BorrowSegments};
use capnp;
use capnp::message;
use capnp::serialize;
use capnp::struct_list;
use capnp::traits::{FromPointerReader, IndexMove};
use misc;
use persist::{self, ValueHandle, ValuePin};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::result;
use std::sync::Mutex;
use std::u64;
use yrola_capnp::{level, bundle, level_table_change};

// a levelhandle does NOT know its own place...
#[derive(Clone)]
struct LevelHandle {
    value: ValueHandle,
    bundle_index: Option<u32>,
}

struct LevelReader {
    reader: message::Reader<BorrowSegments<ValuePin>>,
    bundle_index: Option<u32>,
}

pub struct Transaction {
    start_stamp: u64,
    committed: Vec<LevelHandle>,
    uncommitted: Vec<LevelHandle>,
}

#[derive(Default)]
pub struct TableCreateOptions {
    key_count: u32,
}

pub struct TableMetadata {
    exists: bool,
}

// TODO(soon): cleverly factor
pub enum Error {
    Persist(persist::Error),
    Capnp(capnp::Error),
    DbTooOld(u32),
    DbTooNew(u32),
    WrongApp(String),
    NoSuchTable(u64),
    TableIdsExhausted,
    MergePkeyMismatch(u64, u32, u32),
    // TODO(soon): need a way to identify levels
    // TODO(soon): merge does not own this logic
    MergeDupDelCol(u64, u32),
    MergeDupInsCol(u64, u32),
    MergeDelNotIns(u64, u32),
    MergeDelNotMatched(u64, u32),
    WrongVecLen,
}

pub type Result<T> = result::Result<T, Error>;

pub struct Connection {
    pconn: Mutex<persist::Connection>,
}

impl LevelHandle {
    fn get_level_reader(&self) -> Result<LevelReader> {
        let pin = try!(self.value.pin());
        Ok(LevelReader {
            reader: try!(borrow_segments::read_message_from_owner(pin,
                                                                  message::ReaderOptions::new())),
            bundle_index: self.bundle_index,
        })
    }
}

fn null_table_change() -> level_table_change::Reader<'static> {
    let nullp = capnp::private::layout::PointerReader::new_default();
    level_table_change::Reader::get_from_pointer(&nullp).unwrap()
}

impl LevelReader {
    fn get_level_ptr<'a>(&'a self) -> Result<level::Reader<'a>> {
        match self.bundle_index {
            None => Ok(try!(self.reader.get_root())),
            Some(ix) => {
                let bundle_r: bundle::Reader<'a> = try!(self.reader.get_root());
                let list_r = try!(bundle_r.get_levels());
                if ix >= list_r.len() {
                    unimplemented!()
                }
                Ok(list_r.get(ix))
            }
        }
    }

    fn get_table_ptr<'a>(&'a self, table_id: u64) -> Result<level_table_change::Reader<'a>> {
        let level_reader = try!(self.get_level_ptr());
        let changed_reader = try!(level_reader.get_tables_changed());
        match misc::binary_search_index(changed_reader.len() as usize, |ix| {
            changed_reader.clone().get(ix as u32).get_table_id().cmp(&table_id)
        }) {
            Ok(ix) => Ok(changed_reader.clone().get(ix as u32)),
            Err(_) => Ok(null_table_change()),
        }
    }
}

fn merge_levels(fst_l: &LevelHandle, snd_l: &LevelHandle) -> Result<LevelHandle> {
    let mut message_b = message::Builder::new_default();

    {
        let level_b = message_b.init_root::<level::Builder>();

        let fst_lr = try!(fst_l.get_level_reader());
        let fst_lp = try!(fst_lr.get_level_ptr());
        let fst_change_r = try!(fst_lp.get_tables_changed());
        let snd_lr = try!(snd_l.get_level_reader());
        let snd_lp = try!(snd_lr.get_level_ptr());
        let snd_change_r = try!(snd_lp.get_tables_changed());

        try!(merge_levels_tc(level_b, fst_change_r, snd_change_r));
    }

    let words = serialize::write_message_to_words(&message_b);
    Ok(LevelHandle {
        bundle_index: None,
        value: ValueHandle::new_words(&words[..]),
    })
}

// TODO(soon): Should capnp-rust generate this automatically?
fn copy_table_change<'a>(mut out: level_table_change::Builder<'a>,
                         inp: level_table_change::Reader<'a>)
                         -> capnp::Result<()> {
    out.set_table_id(inp.get_table_id());
    out.set_dropped(inp.get_dropped());
    out.set_created(inp.get_created());
    out.set_key_count(inp.get_key_count());
    try!(out.set_match_keys(try!(inp.get_match_keys())));
    try!(out.set_column_order(try!(inp.get_column_order())));
    try!(out.set_columns_deleted(try!(inp.get_columns_deleted())));
    try!(out.set_upsert_data(try!(inp.get_upsert_data())));
    try!(out.set_delete_data(try!(inp.get_delete_data())));
    Ok(())
}

fn merge_levels_table<'a>(mut out_t: level_table_change::Builder<'a>,
                          fst_t: level_table_change::Reader<'a>,
                          snd_t: level_table_change::Reader<'a>)
                          -> Result<()> {
    if snd_t.get_dropped() {
        // it's absolute...
        try!(copy_table_change(out_t, snd_t));
        return Ok(());
    }

    if !snd_t.get_created() {
        // no update?  shouldn't really happen
        try!(copy_table_change(out_t, fst_t));
        return Ok(());
    }

    // snd is now known to be created & !dropped

    if !fst_t.get_created() {
        // starting from blankness
        try!(copy_table_change(out_t.borrow(), snd_t));
        out_t.set_dropped(fst_t.get_dropped());
        return Ok(());
    }

    // at this point we are known to be merging creates
    out_t.set_dropped(fst_t.get_dropped());
    out_t.set_created(true);
    out_t.set_table_id(fst_t.get_table_id());
    out_t.set_key_count(1);

    let nkeys = 1;
    let table_id = fst_t.get_table_id();
    if fst_t.get_key_count() != 1 || snd_t.get_key_count() != 1 {
        unimplemented!();
    }

    if fst_t.has_match_keys() || snd_t.has_match_keys() {
        // this is for upserts to secondary indices...note that the secondary index design is
        // kinda meh since it should be packed
        unimplemented!();
    }

    let fst_co = try!(fst_t.get_column_order());
    let snd_co = try!(snd_t.get_column_order());
    let fst_cd = try!(fst_t.get_columns_deleted());
    let snd_cd = try!(snd_t.get_columns_deleted());
    let fst_ud = try!(fst_t.get_upsert_data());
    let snd_ud = try!(snd_t.get_upsert_data());
    let fst_dd = try!(fst_t.get_delete_data());
    let snd_dd = try!(snd_t.get_delete_data());

    if fst_ud.len() != nkeys + fst_co.len() {
        return Err(Error::WrongVecLen);
    }

    if fst_dd.len() != nkeys {
        return Err(Error::WrongVecLen);
    }

    if snd_ud.len() != nkeys + snd_co.len() {
        return Err(Error::WrongVecLen);
    }

    if snd_dd.len() != nkeys {
        return Err(Error::WrongVecLen);
    }

    let mut col_del_set = HashSet::new();
    let mut col_left_data = HashMap::new();
    let mut col_right_data = HashMap::new();
    let mut stale_col = HashSet::new();

    for (colix, colid) in misc::prim_list_iter(fst_co).enumerate() {
        if col_left_data.insert(colid, fst_ud.index_move(colix as u32 + nkeys)).is_some() {
            return Err(Error::MergeDupDelCol(table_id, colid));
        }
        stale_col.insert(colid);
    }

    for delid in misc::prim_list_iter(fst_cd) {
        if !col_left_data.contains_key(&delid) {
            return Err(Error::MergeDelNotIns(table_id, delid));
        }
        if col_del_set.insert(delid) {
            return Err(Error::MergeDupInsCol(table_id, delid));
        }
    }

    // TODO(now): need to add column default value data to the column order so that we can
    // appropriately materialize the new columns
    for (colix, colid) in misc::prim_list_iter(snd_co).enumerate() {
        if col_right_data.insert(colid, fst_ud.index_move(colix as u32 + nkeys)).is_some() {
            return Err(Error::MergeDupInsCol(table_id, colid));
        }
        if col_left_data.contains_key(&colid) {
            stale_col.remove(&colid);
        } else {
            col_left_data.insert(colid, unimplemented!());
        }
    }

    for colid in stale_col {
        col_left_data.remove(&colid);
        col_del_set.remove(&colid);
    }

    assert!(col_left_data.keys().eq(col_right_data.keys()));

    for delid in misc::prim_list_iter(snd_cd) {
        if !col_left_data.contains_key(&delid) {
            return Err(Error::MergeDelNotMatched(table_id, delid));
        }
        col_del_set.insert(delid);
        col_left_data.insert(delid, unimplemented!());
    }

    unimplemented!()
}

fn merge_levels_tc<'a>(out_lp: level::Builder<'a>,
                       fst_l: struct_list::Reader<'a, level_table_change::Owned>,
                       snd_l: struct_list::Reader<'a, level_table_change::Owned>)
                       -> Result<()> {
    // TODO(someday): This pass would be unneeded if we had an orphanage
    let merged_count = misc::merge_iters(fst_l.iter(),
                                         snd_l.iter(),
                                         |a, b| a.get_table_id().cmp(&b.get_table_id()))
                           .count() as u32;

    let mut out_changes_p = out_lp.init_tables_changed(merged_count);
    let mut merge_ix = 0;

    for align in misc::merge_iters(fst_l.iter(),
                                   snd_l.iter(),
                                   |a, b| a.get_table_id().cmp(&b.get_table_id())) {
        let out_change_p = out_changes_p.borrow().get(merge_ix);
        merge_ix += 1;
        match align {
            misc::MergeRow::Left(table_l) => try!(copy_table_change(out_change_p, table_l)),
            misc::MergeRow::Right(table_r) => try!(copy_table_change(out_change_p, table_r)),
            misc::MergeRow::Match(table_l, table_r) => {
                try!(merge_levels_table(out_change_p, table_l, table_r));
            }
        }
    }

    Ok(())
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::Capnp(err)
    }
}

impl From<persist::Error> for Error {
    fn from(err: persist::Error) -> Error {
        Error::Persist(err)
    }
}

macro_rules! for_level {
    ($self_:expr, $id:ident in $block:block) => {
        for level in $self_.get_changelog().iter().rev() {
            let $id = try!(level.get_level_reader());
            $block
        }
    }
}

macro_rules! for_table {
    ($self_:expr, $table_id:expr, $id:ident in $block:block) => {
        for_level!($self_, level_r in {
            let $id = try!(level_r.get_table_ptr($table_id));
            if $id.get_created() {
                $block
            }
            if $id.get_dropped() {
                break;
            }
        });
    }
}

impl Transaction {
    fn get_changelog(&self) -> Vec<&LevelHandle> {
        let mut log = Vec::new();
        for levelp in &self.committed {
            log.push(levelp);
        }
        for levelp in &self.uncommitted {
            log.push(levelp);
        }
        log
    }

    fn table_exists(&self, table_id: u64) -> Result<bool> {
        for_table!(self, table_id, level_r in {
            return Ok(true);
        });
        return Ok(false);
    }

    fn push_uncommitted_level(&mut self,
                              message_b: &message::Builder<message::HeapAllocator>)
                              -> Result<()> {
        let words = serialize::write_message_to_words(&message_b);
        self.uncommitted.push(LevelHandle {
            bundle_index: None,
            value: ValueHandle::new_words(&words[..]),
        });
        // TODO(soon): compaction
        Ok(())
    }

    pub fn create_table(&mut self, options: TableCreateOptions) -> Result<u64> {
        let mut highwater = 0;
        for_level!(self, level_r in {
            let level_reader = try!(level_r.get_level_ptr());
            let changed_reader = try!(level_reader.get_tables_changed());
            if changed_reader.len() > 0 {
                let last_reader = changed_reader.get(changed_reader.len() - 1);
                highwater = cmp::max(highwater, last_reader.get_table_id());
            }
        });

        if highwater == u64::MAX {
            return Err(Error::TableIdsExhausted);
        }

        let mut message_b = message::Builder::new_default();
        {
            let level_b = message_b.init_root::<level::Builder>();
            let mut tc_b = level_b.init_tables_changed(1).get(0);
            tc_b.set_created(true);
            tc_b.set_table_id(highwater + 1);
            tc_b.set_key_count(options.key_count);
        }
        try!(self.push_uncommitted_level(&message_b));
        Ok(highwater + 1)
    }

    pub fn drop_table(&mut self, id: u64) -> Result<()> {
        if !try!(self.table_exists(id)) {
            return Err(Error::NoSuchTable(id));
        }

        let mut message_b = message::Builder::new_default();
        {
            let level_b = message_b.init_root::<level::Builder>();
            let mut tc_b = level_b.init_tables_changed(1).get(0);
            tc_b.set_dropped(true);
            tc_b.set_table_id(id);
        }
        try!(self.push_uncommitted_level(&message_b));
        Ok(())
    }


    pub fn commit(self) -> Result<()> {
        unimplemented!()
    }
}

const DB_APP_NAME: &'static str = "YrolaDatabase";
const DB_READ_VERSION: u32 = 0;
const DB_WRITE_VERSION: u32 = 0;

impl Connection {
    pub fn open(db_path: &Path, read_only: bool) -> Result<Self> {
        let mut pconn = try!(persist::Connection::open(db_path, read_only));

        {
            let mut tx = pconn.transaction();

            let is_blank = tx.config().app_name == "";
            if is_blank && !read_only {
                tx.new_config().app_name = DB_APP_NAME.to_owned();
                tx.new_config().app_version = DB_WRITE_VERSION;
            }

            if tx.config().app_name != DB_APP_NAME {
                return Err(Error::WrongApp(tx.config().app_name.clone()));
            }
            if tx.config().app_version < DB_READ_VERSION {
                return Err(Error::DbTooOld(tx.config().app_version));
            }
            if tx.config().app_version > DB_WRITE_VERSION {
                return Err(Error::DbTooNew(tx.config().app_version));
            }

            tx.new_config().app_version = DB_WRITE_VERSION;
            try!(tx.commit());
        }

        Ok(Connection { pconn: Mutex::new(pconn) })
    }

    pub fn transaction(&self) -> Transaction {
        let mut conn_lock = self.pconn.lock().unwrap();
        let mut read_tx = conn_lock.transaction();

        if read_tx.list_items().count() != 0 {
            unimplemented!()
        }

        Transaction {
            start_stamp: 1,
            committed: Vec::new(),
            uncommitted: Vec::new(),
        }
    }
}
