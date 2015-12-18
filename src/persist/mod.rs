use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, ErrorKind, Read, Write, SeekFrom, Seek};
use std::result;
use std::str::FromStr;
use std::collections::{HashSet, HashMap};
use fs2::FileExt;
use std::sync::Arc;
use std::slice;
use std::ffi::OsStr;
use capnp;
use std::ops::Deref;
use byteorder::{LittleEndian, ByteOrder};
use std::hash::{SipHasher, Hasher};
use std::u32;

#[derive(Debug)]
pub enum Error {
    BaseIsNotDir(PathBuf),
    BaseStatFailed(PathBuf, io::Error),
    CreateBaseFailed(PathBuf, io::Error),
    CreateLogDirFailed(PathBuf, io::Error),
    CreateObjDirFailed(PathBuf, io::Error),
    CreateLockFileFailed(PathBuf, io::Error),
    LockOpenFailed(PathBuf, io::Error),
    LockFailed(PathBuf, io::Error),
    JournalOpendirFailed(PathBuf, io::Error),
    JournalReaddirFailed(PathBuf, io::Error),
    DeleteBadJournalFailed(PathBuf, io::Error),
    JournalOpenFailed(PathBuf, io::Error),
    JournalReadFailed(PathBuf, io::Error),
    JournalWriteFailed(PathBuf, io::Error),
    JournalSyncFailed(PathBuf, io::Error),
    JournalTruncateFailed(PathBuf, io::Error),
}

fn parse_object_filename(filename: &OsStr) -> Option<u64> {
    filename.to_str().and_then(|filename_utf8| {
        u64::from_str(filename_utf8).ok().and_then(|i| {
            if format!("{:06}", i) == filename_utf8 {
                Some(i)
            } else {
                None
            }
        })
    })
}

pub type Result<T> = result::Result<T, Error>;

struct FileControl;

#[derive(Clone)]
pub enum ValueHandle {
    SmallData {
        data: Arc<Vec<u8>>,
    },
    LargeData {
        file: Arc<FileControl>,
    },
}

fn word_to_byte_slice(sl: &[capnp::Word]) -> &[u8] {
    unsafe { slice::from_raw_parts(sl.as_ptr() as *const u8, sl.len() << 3) }
}

// only use this if you have probable cause to assume it's aligned
// most allocators will 64-bit-align all memory
fn byte_to_word_slice(byteslice: &[u8]) -> &[capnp::Word] {
    unsafe {
        assert!(((byteslice.as_ptr() as usize) & 7) == 0);
        slice::from_raw_parts(byteslice.as_ptr() as *const capnp::Word,
                              byteslice.len() >> 3)
    }
}

impl ValueHandle {
    pub fn new(data: &[u8]) -> ValueHandle {
        ValueHandle::SmallData { data: Arc::new(Vec::from(data)) }
    }

    pub fn new_words(data: &[capnp::Word]) -> ValueHandle {
        ValueHandle::new(word_to_byte_slice(data))
    }

    // will be retooled when adding mmap support ...
    pub fn data(&self) -> Result<&[u8]> {
        match *self {
            ValueHandle::SmallData { data: ref rc } => Ok(rc.deref()),
            ValueHandle::LargeData { file: ref _fl } => unimplemented!(),
        }
    }

    pub fn data_words(&self) -> Result<&[capnp::Word]> {
        self.data().map(byte_to_word_slice)
    }
}

#[derive(Clone)]
pub struct ItemHandle {
    id: u64,
    header: ValueHandle,
    body: ValueHandle,
}

impl ItemHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn header(&self) -> &ValueHandle {
        &self.header
    }

    pub fn body(&self) -> &ValueHandle {
        &self.body
    }
}

pub struct ItemIterator<'a> {
    guts: &'a (),
}
impl<'a> Iterator for ItemIterator<'a> {
    type Item = ItemHandle;
    fn next(&mut self) -> Option<ItemHandle> {
        unimplemented!()
    }
}

pub struct Transaction {
    guts: (),
}

impl Transaction {
    pub fn add_item(&mut self, _header: Vec<u8>, _data: Vec<u8>) -> Result<u64> {
        unimplemented!()
    }

    pub fn del_item(&mut self, _id: u64) -> Result<()> {
        unimplemented!()
    }

    pub fn get_item(&mut self, _id: u64) -> Result<ItemHandle> {
        unimplemented!()
    }

    pub fn list_items<'a>(&'a mut self) -> Result<ItemIterator<'a>> {
        unimplemented!()
    }

    pub fn commit(self) -> Result<()> {
        unimplemented!()
    }
}

struct ObjectInfo {
    journal_id: u64,
    data: ItemHandle,
}

struct TombstoneInfo {
    journal_id: u64,
}

struct SegmentInfo {
    // dead objects are important because they force us to keep the tombstones around.
    // dead tombstones have no such requirement.
    live_object_ids: HashSet<u64>,
    dead_object_ids: HashSet<u64>,
    live_tombstone_ids: HashSet<u64>,
    on_disk_size: u64,
}

pub struct Persister {
    lock_path: PathBuf,
    wals_path: PathBuf,
    objs_path: PathBuf,

    lock_file: File,

    segments: HashMap<u64, SegmentInfo>,
    objects: HashMap<u64, ObjectInfo>,
    tombstones: HashMap<u64, TombstoneInfo>,
}

#[derive(Debug)]
pub enum LogBlockError {
    IncompleteHeader,
    IncompleteBody,
    BadHeaderType,
    BadHeaderHash,
    BadBodyHash,
}

// we're using SipHash for its engineering properties, so a hard-coded key is not a big deal
const BLOCK_K0: u64 = 0xd66db8e2464451f1;
const BLOCK_K1: u64 = 0x217f5ed82cc28caa;
const LEADER_K0: u64 = 0xaaba64c0b2f8bf29;
const LEADER_K1: u64 = 0x520c5e4629fdf1b0;
const EXTERNAL_K0: u64 = 0xae880699c0628ab8;
const EXTERNAL_K1: u64 = 0x3f9c216ed189d0f3;

fn leader_hash(journal_id: u64, journal_offset: u64, leader: u32) -> u32 {
    let mut hasher = SipHasher::new_with_keys(LEADER_K0, LEADER_K1);
    hasher.write_u64(journal_id);
    hasher.write_u64(journal_offset);
    hasher.write_u32(leader);
    hasher.finish() as u32
}

fn external_hash(file_id: u64, data: &[u8]) -> u64 {
    let mut hasher = SipHasher::new_with_keys(EXTERNAL_K0, EXTERNAL_K1);
    hasher.write_u64(file_id);
    hasher.write(data);
    hasher.finish()
}

fn log_block_hash(journal_id: u64, journal_position: u64, data: &[u8]) -> u64 {
    let mut hasher = SipHasher::new_with_keys(BLOCK_K0, BLOCK_K1);
    hasher.write_u64(journal_id);
    hasher.write_u64(journal_position);
    hasher.write(data);
    hasher.finish()
}

fn read_log_blocks_file(mut journal: File,
                        journal_path: &PathBuf,
                        journal_id: u64)
                        -> Result<(LogBlockError, u64, Vec<Vec<capnp::Word>>)> {
    let mut jdata = Vec::new();
    try!(journal.read_to_end(&mut jdata)
                .map_err(|e| Error::JournalReadFailed(journal_path.clone(), e)));

    let mut read_ptr = 0;
    let mut blocks = Vec::new();
    let break_code;
    loop {
        if jdata.len() - read_ptr < 16 {
            break_code = LogBlockError::IncompleteHeader;
            break;
        }

        let leader = LittleEndian::read_u64(&jdata[read_ptr..(read_ptr + 8)]);
        let hash = LittleEndian::read_u64(&jdata[(read_ptr + 8)..(read_ptr + 16)]);
        let lhash = (leader >> 32) as u32;
        let block_len = leader as u32;

        if (block_len & 7) != 0 {
            break_code = LogBlockError::BadHeaderType;
            break;
        }

        if leader_hash(journal_id, read_ptr as u64, block_len) != lhash {
            break_code = LogBlockError::BadHeaderHash;
            break;
        }

        read_ptr += 16;

        if jdata.len() - read_ptr < (block_len as usize) {
            break_code = LogBlockError::IncompleteBody;
            break;
        }

        let block_slice = &jdata[read_ptr..(read_ptr + (block_len as usize))];

        if log_block_hash(journal_id, read_ptr as u64 - 16, block_slice) != hash {
            break_code = LogBlockError::BadBodyHash;
            break;
        }

        blocks.push(Vec::from(byte_to_word_slice(block_slice)));
    }

    Ok((break_code, read_ptr as u64, blocks))
}

fn write_log_block(journal: &mut File,
                   journal_path: &PathBuf,
                   journal_id: u64,
                   journal_position: &mut u64,
                   block: &[capnp::Word])
                   -> Result<()> {
    let byte_block = word_to_byte_slice(block);
    assert!(byte_block.len() <= u32::MAX as usize);
    let block_hash = log_block_hash(journal_id, *journal_position, byte_block);
    let leader = byte_block.len() as u64 |
                 ((leader_hash(journal_id, *journal_position, byte_block.len() as u32) as u64) <<
                  32);

    let mut header = [0u8; 16];
    LittleEndian::write_u64(&mut header[0..8], leader);
    LittleEndian::write_u64(&mut header[8..16], block_hash);
    try!(journal.seek(SeekFrom::Start(*journal_position))
                .map_err(|e| Error::JournalWriteFailed(journal_path.clone(), e)));
    try!(journal.write_all(&header)
                .map_err(|e| Error::JournalWriteFailed(journal_path.clone(), e)));
    try!(journal.write_all(byte_block)
                .map_err(|e| Error::JournalWriteFailed(journal_path.clone(), e)));
    try!(journal.sync_data().map_err(|e| Error::JournalSyncFailed(journal_path.clone(), e)));
    *journal_position += 16 + byte_block.len() as u64;
    Ok(())
}

// call this if you fail to write an entry
fn truncate_log(journal: &mut File, journal_path: &PathBuf, journal_position: u64) -> Result<()> {
    try!(journal.seek(SeekFrom::Start(journal_position))
                .map_err(|e| Error::JournalWriteFailed(journal_path.clone(), e)));
    try!(journal.write_all(&[0xFFu8, 16])
                .map_err(|e| Error::JournalWriteFailed(journal_path.clone(), e)));
    try!(journal.sync_data().map_err(|e| Error::JournalSyncFailed(journal_path.clone(), e)));
    Ok(())
}

impl Persister {
    pub fn transaction(&self) -> Transaction {
        unimplemented!()
    }

    pub fn open(root: &Path, readonly: bool) -> Result<Self> {
        let lock_path = root.join("yrola.lock");
        let wals_path = root.join("wals");
        let objs_path = root.join("objs");

        match fs::metadata(root) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(Error::BaseIsNotDir(root.to_owned()));
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound && !readonly {
                    try!(fs::create_dir_all(root)
                             .map_err(|e| Error::CreateBaseFailed(root.to_owned(), e)));
                    try!(fs::create_dir_all(&wals_path)
                             .map_err(|e| Error::CreateLogDirFailed(wals_path.clone(), e)));
                    try!(fs::create_dir_all(&objs_path)
                             .map_err(|e| Error::CreateObjDirFailed(objs_path.clone(), e)));
                    try!(File::create(&lock_path)
                             .map_err(|e| Error::CreateLockFileFailed(lock_path.clone(), e)));
                } else {
                    return Err(Error::BaseStatFailed(root.to_owned(), err));
                }
            }
        };

        let lock_file = try!(OpenOptions::new()
                                 .write(true)
                                 .open(&lock_path)
                                 .map_err(|e| Error::LockOpenFailed(lock_path.clone(), e)));
        try!(lock_file.lock_exclusive()
                      .map_err(|e| Error::LockFailed(lock_path.clone(), e)));

        let mut pers = Persister {
            lock_file: lock_file,
            lock_path: lock_path.clone(),
            objs_path: objs_path.clone(),
            wals_path: wals_path.clone(),
            segments: HashMap::new(),
            objects: HashMap::new(),
            tombstones: HashMap::new(),
        };

        try!(pers.read_all_journals());
        if !readonly {
            try!(pers.clean_directories());
        }

        Ok(pers)
    }

    fn read_all_journals(&mut self) -> Result<()> {
        // find the journal with the highest number containing at least one valid block
        let mut journal_names = Vec::new();
        let wals_iter = try!(fs::read_dir(&self.wals_path).map_err(|e| {
            Error::JournalOpendirFailed(self.wals_path.clone(), e)
        }));
        for rentry in wals_iter {
            let entry = try!(rentry.map_err(|e| {
                Error::JournalReaddirFailed(self.wals_path.clone(), e)
            }));
            if let Some(jnum) = parse_object_filename(&*entry.file_name()) {
                journal_names.push(jnum);
            }
        }

        journal_names.sort();
        journal_names.reverse();

        for jnum in &journal_names {
            let jpath = self.wals_path.join(format!("{:06}", jnum));
            let jfile = try!(File::open(&jpath)
                                 .map_err(|e| Error::JournalOpenFailed(jpath.clone(), e)));
            let (_err, write_pos, jblocks) = try!(read_log_blocks_file(jfile, &jpath, *jnum));

            if jblocks.is_empty() {
                continue;
            }
            // TODO(now): Read the journal
            unimplemented!();
        }
        // read any other logs specified

        Ok(())
    }

    fn clean_directories(&mut self) -> Result<()> {
        // for badpath in bad_journal_names {
        //     try!(fs::remove_file(&badpath)
        //              .map_err(|e| Error::DeleteBadJournalFailed(badpath.clone(), e)));
        // }

        // TODO(now): Remove old objects
        unimplemented!()
    }
}

#[cfg(test)]
mod tests;
