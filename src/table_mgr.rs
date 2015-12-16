use persist::{self, ValueHandle};
use std::result;
use yrola_capnp::{level, bundle, level_table_change};
use capnp::serialize;
use capnp::message;
use capnp;
use std::cmp::Ordering;
use capnp::traits::FromPointerReader;

// a levelhandle does NOT know its own place...
#[derive(Clone)]
struct LevelHandle {
    value: ValueHandle,
    bundle_index: Option<u32>,
}

struct LevelReader<'a> {
    reader: message::Reader<serialize::SliceSegments<'a>>,
    bundle_index: Option<u32>,
}

impl LevelHandle {
    fn get_level_reader<'a>(&'a self) -> Result<LevelReader<'a>> {
        let slice = try!(self.value.data_words());
        Ok(LevelReader {
            reader: try!(serialize::read_message_from_words(slice, message::ReaderOptions::new())),
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

impl<'b> LevelReader<'b> {
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
            Err(_) => Ok(try!(level_table_change::Reader::get_from_pointer(&capnp::private::layout::PointerReader::new_default()))),
        }
    }
}

#[derive(Clone)]
enum LevelOrSavepoint {
    Level(LevelHandle),
    Savepoint(String),
}

struct Transaction {
    start_stamp: u64,
    committed: Vec<LevelHandle>,
    uncomitted: Vec<LevelOrSavepoint>,
}

#[derive(Default)]
pub struct TableCreateOptions {
    key_count: u32,
}

pub struct TableMetadata {
    exists: bool,
}

pub enum Error {}

impl From<capnp::Error> for Error {
    fn from(_err: capnp::Error) -> Error {
        unimplemented!()
    }
}

impl From<persist::Error> for Error {
    fn from(_err: persist::Error) -> Error {
        unimplemented!()
    }
}

pub type Result<T> = result::Result<T, Error>;

impl Transaction {
    fn get_changelog(&self) -> Vec<&LevelHandle> {
        let mut log = Vec::new();
        for levelp in &self.committed {
            log.push(levelp);
        }
        for lors in &self.uncomitted {
            if let LevelOrSavepoint::Level(ref l) = *lors {
                log.push(l);
            }
        }
        log
    }

    fn table_exists(&self, table_id: u64) -> Result<bool> {
        for level in self.get_changelog().iter().rev() {
            let level_r = try!(level.get_level_reader());
            let table_r = try!(level_r.get_table_ptr(table_id));
            if table_r.get_created() {
                return Ok(true);
            }
            if table_r.get_dropped() {
                break;
            }
        }
        return Ok(false);
    }

    pub fn create_table(&mut self, _options: TableCreateOptions) -> Result<u64> {
        unimplemented!()
    }

    pub fn drop_table(&mut self, id: u64) -> Result<()> {
        if try!(self.table_exists(id)) {
            unimplemented!();
        }

        let mut message_b = message::Builder::new_default();
        {
            let level_b = message_b.init_root::<level::Builder>();
            let mut tc_b = level_b.init_tables_changed(1).get(0);
            tc_b.set_dropped(true);
            tc_b.set_table_id(id);
        }

        let words = serialize::write_message_to_words(&message_b);
        self.uncomitted.push(LevelOrSavepoint::Level(LevelHandle {
            bundle_index: None,
            value: ValueHandle::new_words(&words[..]),
        }));
        // TODO(soon): compaction
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        unimplemented!()
    }
}
