use borrow_segments::{self, BorrowSegments};
use capnp;
use capnp::message;
use capnp::serialize;
use capnp::struct_list;
use capnp::traits::FromPointerReader;
use misc;
use persist::{self, ValueHandle, ValuePin};
use std::cmp;
use std::collections::{HashSet, HashMap};
use std::path::Path;
use std::result;
use std::sync::Mutex;
use std::u64;
use vector::{self, Column};
use yrola_capnp::{level, bundle, level_table_change};

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct TableId(pub u64);
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct Timestamp(pub u64);
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ColumnId(pub u32);

// a levelhandle does NOT know its own place...
#[derive(Clone)]
struct LevelHandle {
    value: ValueHandle,
    merge_count: i32,
    bundle_index: Option<u32>,
}

struct LevelReader {
    pin: ValuePin,
    reader: message::Reader<BorrowSegments<ValuePin>>,
    bundle_index: Option<u32>,
}

pub struct Transaction {
    start_stamp: Timestamp,
    committed: Vec<LevelHandle>,
    uncommitted: Vec<LevelHandle>,
}

#[derive(Default)]
pub struct TableCreateOptions;

pub struct TableMetadata {
    exists: bool,
}

// TODO(soon): cleverly factor
pub enum Error {
    Persist(persist::Error),
    Capnp(capnp::Error),
    Vector(vector::Error),
    DbTooOld(u32),
    DbTooNew(u32),
    WrongApp(String),
    NoSuchTable(TableId),
    TableIdsExhausted,
    MergePkeyMismatch(TableId, u32, u32),
    // TODO(soon): need a way to identify levels
    // TODO(soon): merge does not own this logic
    MergeDupCol(TableId, ColumnId),
    MergeDupDelCol(TableId, ColumnId),
    MergeDupInsCol(TableId, ColumnId),
    MergeDelNotIns(TableId, ColumnId),
    MergeDelNotMatched(TableId, ColumnId),
    WrongVecLen,
    MergeUpdateNotExists,
    MergeUpdateColNotExists,
}

pub type Result<T> = result::Result<T, Error>;

pub struct Connection {
    pconn: Mutex<persist::Connection>,
}

impl LevelHandle {
    fn get_level_reader(&self) -> Result<LevelReader> {
        let pin = try!(self.value.pin());
        Ok(LevelReader {
            pin: pin.clone(),
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

    fn get_table_ptr<'a>(&'a self, table_id: TableId) -> Result<level_table_change::Reader<'a>> {
        let level_reader = try!(self.get_level_ptr());
        let changed_reader = try!(level_reader.get_tables_changed());
        match misc::binary_search_index(changed_reader.len() as usize, |ix| {
            changed_reader.clone().get(ix as u32).get_table_id().cmp(&table_id.0)
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

        try!(merge_levels_tc(level_b,
                             &fst_lr.pin,
                             fst_change_r,
                             &snd_lr.pin,
                             snd_change_r));
    }

    let words = serialize::write_message_to_words(&message_b);
    Ok(LevelHandle {
        bundle_index: None,
        merge_count: 1 + cmp::max(fst_l.merge_count, snd_l.merge_count),
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
    try!(out.set_primary_keys(try!(inp.get_primary_keys())));
    try!(out.set_upsert_data(try!(inp.get_upsert_data())));
    Ok(())
}

struct MergeColumnInfo {
    low_id: ColumnId,
    erase: Option<Option<Vec<u8>>>,
    data: Column,
}

struct MergePkInfo {
    upsert_key: Column,
    delete_key: Column,
    exclude_match: bool,
}

struct MergeTableInfo {
    table_id: TableId,
    keys: Vec<MergePkInfo>,
    columns: HashMap<ColumnId, MergeColumnInfo>,
}

fn parse_merge_table<'a>(pin: &ValuePin,
                         ltcr: level_table_change::Reader<'a>,
                         filter: Option<&HashSet<ColumnId>>)
                         -> Result<MergeTableInfo> {
    let mut columns = HashMap::new();
    let table_id = TableId(ltcr.get_table_id());
    for lcolr in try!(ltcr.get_upsert_data()).iter() {
        let col_id = ColumnId(lcolr.get_low_id());
        if columns.contains_key(&col_id) {
            return Err(Error::MergeDupCol(table_id, col_id));
        }
        if let Some(filter_h) = filter {
            if filter_h.contains(&col_id) {
                continue;
            }
        }
        let deflt = if lcolr.has_default() {
            Some(Vec::from(try!(lcolr.get_default())))
        } else {
            None
        };
        let erase = if lcolr.get_erased() {
            Some(deflt)
        } else {
            None
        };
        columns.insert(col_id,
                       MergeColumnInfo {
                           low_id: col_id,
                           erase: erase,
                           data: try!(vector::parse(pin.clone(), try!(lcolr.get_data()))),
                       });
    }

    let pkinfo: Vec<_> = try!(try!(ltcr.get_primary_keys())
                                  .iter()
                                  .map(|kr| {
                                      Ok::<_, Error>(MergePkInfo {
            upsert_key: try!(vector::parse(pin.clone(), try!(kr.get_upsert_bits()))),
            delete_key: try!(vector::parse(pin.clone(), try!(kr.get_delete_bits()))),
            exclude_match: kr.get_exclude_from_match(),
        })
                                  })
                                  .collect());

    if pkinfo.len() != 1 || pkinfo[0].exclude_match {
        unimplemented!();
    }

    Ok(MergeTableInfo {
        table_id: table_id,
        keys: pkinfo,
        columns: columns,
    })
}

fn merge_table_view(p1: MergeTableInfo, p2: MergeTableInfo) -> Result<MergeTableInfo> {
    let mut alignment: Vec<(ColumnId, Option<Option<Vec<u8>>>, Column, Column)> = Vec::new();

    for col2 in p2.columns.values() {
        let (erase, lvec) = match col2.erase {
            None => {
                let col1 = try!(p1.columns.get(&col2.low_id).ok_or(Error::MergeUpdateColNotExists));
                (col1.erase.clone(), col1.data.clone())
            }
            Some(ref er2) => {
                (Some(er2.clone()),
                 try!(vector::constant(er2.clone(), p1.keys[0].upsert_key.len())))
            }
        };

        alignment.push((col2.low_id, erase, lvec, col2.data.clone()));
    }

    let dead_keys = try!(vector::sorted_merge((&p2.keys[0].upsert_key, &p2.keys[0].delete_key),
                                              &[(&p2.keys[0].upsert_key, &p2.keys[0].delete_key)]));
    let left_refs: Vec<&Column> = Some(&p1.keys[0].upsert_key)
                                      .into_iter()
                                      .chain(alignment.iter().map(|tp| &tp.2))
                                      .collect();
    let filtered_left = try!(vector::sorted_semijoin(&p1.keys[0].upsert_key,
                                                     &dead_keys[0],
                                                     &*left_refs,
                                                     true));
    let filtered_del = try!(vector::sorted_semijoin(&p1.keys[0].delete_key,
                                                    &dead_keys[0],
                                                    &[&p1.keys[0].delete_key],
                                                    true));

    let new_del = try!(vector::sorted_merge((&filtered_del[0], &p2.keys[0].delete_key),
                                            &[(&filtered_del[0], &p2.keys[0].delete_key)]));
    let ups_ref: Vec<_> = filtered_left.iter()
                                       .zip(Some(&p2.keys[0].upsert_key)
                                                .into_iter()
                                                .chain(alignment.iter().map(|tp| &tp.3)))
                                       .collect();
    let new_ups = try!(vector::sorted_merge((&filtered_left[0], &p2.keys[0].upsert_key),
                                            &*ups_ref));

    // TODO(someday): Can we ever annihilate deletes?

    let new_keys = vec![
        MergePkInfo {
            upsert_key: new_ups[0].clone(), delete_key: new_del[0].clone(), exclude_match: false
        },
    ];
    let new_columns = alignment.iter()
                               .zip((&new_ups[1..]).into_iter())
                               .map(|(&(low_id, ref erase, _, _), data)| {
                                   MergeColumnInfo {
                                       low_id: low_id,
                                       erase: erase.clone(),
                                       data: data.clone(),
                                   }
                               });

    Ok(MergeTableInfo {
        table_id: p1.table_id,
        keys: new_keys,
        columns: new_columns.into_iter().map(|mci| (mci.low_id, mci)).collect(),
    })
}

fn merge_levels_table<'a>(mut out_t: level_table_change::Builder<'a>,
                          fst_pin: &ValuePin,
                          fst_t: level_table_change::Reader<'a>,
                          snd_pin: &ValuePin,
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
        return Err(Error::MergeUpdateNotExists);
    }

    // at this point we are known to be merging creates
    out_t.set_dropped(fst_t.get_dropped());
    out_t.set_created(true);
    out_t.set_table_id(fst_t.get_table_id());

    let p1 = try!(parse_merge_table(fst_pin, fst_t, None));
    let p2 = try!(parse_merge_table(snd_pin, snd_t, None));

    let p12 = try!(merge_table_view(p1, p2));
    // try to align the two column sets

    {
        let mut pkey_w = out_t.borrow().init_primary_keys(p12.keys.len() as u32);
        for (pkey_ix, pkeyi) in p12.keys.into_iter().enumerate() {
            let mut pkeyi_w = pkey_w.borrow().get(pkey_ix as u32);
            pkeyi_w.set_exclude_from_match(pkeyi.exclude_match);
            let seru = try!(vector::serialized(&pkeyi.upsert_key));
            pkeyi_w.set_upsert_bits(&seru);
            let serd = try!(vector::serialized(&pkeyi.delete_key));
            pkeyi_w.set_delete_bits(&serd);
        }
    }

    {
        let mut cols_w = out_t.borrow().init_upsert_data(p12.columns.len() as u32);
        for (cols_ix, (_, coli)) in p12.columns.into_iter().enumerate() {
            let mut coli_w = cols_w.borrow().get(cols_ix as u32);
            coli_w.set_low_id(coli.low_id.0);
            if let Some(er) = coli.erase {
                coli_w.set_erased(true);
                if let Some(bits) = er {
                    coli_w.set_default(&bits);
                }
            }
            let serv = try!(vector::serialized(&coli.data));
            coli_w.set_data(&serv);
        }
    }

    Ok(())
}

fn merge_levels_tc<'a>(out_lp: level::Builder<'a>,
                       fst_pin: &ValuePin,
                       fst_l: struct_list::Reader<'a, level_table_change::Owned>,
                       snd_pin: &ValuePin,
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
                try!(merge_levels_table(out_change_p, fst_pin, table_l, snd_pin, table_r));
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

impl From<vector::Error> for Error {
    fn from(err: vector::Error) -> Error {
        Error::Vector(err)
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
    ($self_:expr, $table_id:expr, $pinid:ident, $id:ident in $block:block) => {
        for_level!($self_, level_r in {
            let $id = try!(level_r.get_table_ptr($table_id));
            let $pinid = &level_r.pin;
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

    fn table_exists(&self, table_id: TableId) -> Result<bool> {
        for_table!(self, table_id, _level_pin, table_r in {
            return Ok(true);
        });
        return Ok(false);
    }

    fn materialize(&self, table_id: TableId, cols: &HashSet<ColumnId>) -> Result<MergeTableInfo> {
        let mut layers = Vec::new();
        for_table!(self, table_id, level_pin, table_r in {
            layers.push(try!(parse_merge_table(level_pin, table_r, Some(cols))));
        });
        while layers.len() >= 2 {
            let bos = layers.pop().unwrap();
            let nbos = layers.pop().unwrap();
            layers.push(try!(merge_table_view(bos, nbos)));
        }
        if layers.is_empty() {
            return Err(Error::NoSuchTable(table_id));
        } else {
            return Ok(layers.pop().unwrap());
        }
    }

    fn push_uncommitted_level(&mut self,
                              message_b: &message::Builder<message::HeapAllocator>)
                              -> Result<()> {
        let words = serialize::write_message_to_words(&message_b);
        self.uncommitted.push(LevelHandle {
            bundle_index: None,
            merge_count: 0,
            value: ValueHandle::new_words(&words[..]),
        });
        let unc_len = self.uncommitted.len();
        if unc_len >= 2 &&
           self.uncommitted[unc_len - 1].merge_count >= self.uncommitted[unc_len - 2].merge_count {
            let tos = self.uncommitted.pop().unwrap();
            let ntos = self.uncommitted.pop().unwrap();
            // compaction
            self.uncommitted.push(try!(merge_levels(&ntos, &tos)));
        }
        Ok(())
    }

    pub fn create_table(&mut self, _options: TableCreateOptions) -> Result<TableId> {
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
            tc_b.set_dropped(true);
            tc_b.set_table_id(highwater + 1);
            tc_b.borrow().init_upsert_data(0);
            {
                let mut pk_lb = tc_b.borrow().init_primary_keys(1);
                let mut pk_b = pk_lb.borrow().get(0);
                let empty_vec = try!(vector::constant(None, 0));
                let empty_data = try!(vector::serialized(&empty_vec));
                pk_b.set_upsert_bits(&empty_data);
                pk_b.set_delete_bits(&empty_data);
            }
        }
        try!(self.push_uncommitted_level(&message_b));
        Ok(TableId(highwater + 1))
    }

    pub fn drop_table(&mut self, id: TableId) -> Result<()> {
        if !try!(self.table_exists(id)) {
            return Err(Error::NoSuchTable(id));
        }

        let mut message_b = message::Builder::new_default();
        {
            let level_b = message_b.init_root::<level::Builder>();
            let mut tc_b = level_b.init_tables_changed(1).get(0);
            tc_b.set_dropped(true);
            tc_b.set_table_id(id.0);
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
            start_stamp: Timestamp(1),
            committed: Vec::new(),
            uncommitted: Vec::new(),
        }
    }
}
