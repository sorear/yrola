use borrow_segments::{self, BorrowSegments};
use capnp;
use capnp::message;
use capnp::serialize;
use capnp::struct_list;
use capnp::traits::FromPointerReader;
use persist::{self, ValueHandle, ValuePin};
use std::cmp;
use std::cmp::Ordering;
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

pub enum Error {
    Persist(persist::Error),
    Capnp(capnp::Error),
    DbTooOld(u32),
    DbTooNew(u32),
    WrongApp(String),
    NoSuchTable(u64),
    TableIdsExhausted,
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

// hacked from the slice version in libcore
fn binary_search_index<F>(to: usize, mut f: F) -> result::Result<usize, usize>
    where F: FnMut(usize) -> Ordering
{
    let mut base: usize = 0;
    let mut lim: usize = to;

    while lim != 0 {
        let ix = base + (lim >> 1);
        match f(ix) {
            Ordering::Equal => return Ok(ix),
            Ordering::Less => {
                base = ix + 1;
                lim -= 1;
            }
            Ordering::Greater => (),
        }
        lim >>= 1;
    }
    Err(base)
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
        match binary_search_index(changed_reader.len() as usize, |ix| {
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

fn merge_levels_tc<'a>(out_lp: level::Builder<'a>,
                       fst_l: struct_list::Reader<'a, level_table_change::Owned>,
                       snd_l: struct_list::Reader<'a, level_table_change::Owned>)
                       -> Result<()> {
    // TODO(someday): This pass would be unneeded if we had an orphanage
    let mut merged_count = 0;
    {
        let mut fst_ix = 0;
        let mut snd_ix = 0;

        loop {
            if fst_ix == fst_l.len() {
                merged_count += snd_l.len() - snd_ix;
                break;
            }

            if snd_ix == snd_l.len() {
                merged_count += fst_l.len() - fst_ix;
                break;
            }

            let fst_tbl = fst_l.clone().get(fst_ix).get_table_id();
            let snd_tbl = snd_l.clone().get(snd_ix).get_table_id();

            if fst_tbl == cmp::min(fst_tbl, snd_tbl) {
                fst_ix += 1;
            }

            if snd_tbl == cmp::min(fst_tbl, snd_tbl) {
                snd_ix += 1;
            }

            merged_count += 1;
        }
    }

    let _out_change_p = out_lp.init_tables_changed(merged_count);
    unimplemented!()
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
