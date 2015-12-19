use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, ErrorKind, Read, Write, SeekFrom, Seek};
use std::result;
use std::str::FromStr;
use std::collections::{HashSet, HashMap};
use fs2::FileExt;
use std::sync::{Arc, Mutex, RwLock};
use std::slice;
use std::ffi::OsStr;
use capnp;
use byteorder::{LittleEndian, ByteOrder};
use std::hash::{SipHasher, Hasher};
use std::u32;
use yrola_capnp;
use std::cmp;
use std::borrow::Borrow;

#[derive(Debug)]
pub enum Error {
    Corrupt(Corruption, PathBuf),
    Io(IoType, PathBuf, io::Error),
    Disconnected,
}

#[derive(Debug)]
pub enum IoType {
    BaseStat,
    CreateBase,
    CreateLogDir,
    CreateObjDir,
    CreateLockFile,
    LockOpen,
    Lock,
    JournalOpendir,
    JournalReaddir,
    JournalOpen,
    JournalStat,
    JournalRead,
    JournalWrite,
    JournalSync,
    CleanupOpendir,
    CleanupReaddir,
    CleanupDelete,
    ObjectOpen,
    ObjectRead,
}

#[derive(Debug)]
pub enum Corruption {
    BaseNotDirectory,
    LogBlockAfterEof,
    DuplicateSegmentHeader,
    MissingSegmentHeader,
    CapnpError(capnp::Error),
    DupObject {
        id: u64,
        other_file: PathBuf,
    },
    DupTombstone {
        id: u64,
        other_file: PathBuf,
    },
    UnclosedJournal {
        read_error: LogBlockError,
        read_pos: u64,
    },
    SegmentIncludesSelf,
    BadSignature(String),
}

pub type Result<T> = result::Result<T, Error>;

struct ObjDirControl {
    disconnected: RwLock<bool>,
    base_dir: PathBuf,
}

struct FileControl {
    dir_control: Arc<ObjDirControl>,
    id: u64,
    size: u64,
    hash: u64,
    loaded_data: Mutex<Option<Arc<Vec<u8>>>>,
}

#[derive(Clone)]
pub enum ValueHandle {
    SmallData {
        data: Arc<Vec<u8>>,
    },
    LargeData {
        file: Arc<FileControl>,
    },
}

#[derive(Clone)]
pub struct ValuePin {
    data: Arc<Vec<u8>>,
}

#[derive(Clone)]
pub struct ItemHandle {
    id: u64,
    header: ValueHandle,
    body: ValueHandle,
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

struct OpenSegmentInfo {
    id: u64,
    offset: u64,
}

pub struct Persister {
    lock_path: PathBuf,
    wals_path: PathBuf,
    objs_path: PathBuf,

    lock_file: File,

    dir_control: Arc<ObjDirControl>,
    segments: HashMap<u64, SegmentInfo>,
    objects: HashMap<u64, ObjectInfo>,
    tombstones: HashMap<u64, TombstoneInfo>,
    highwater_object_id: u64,
    open_segment: Option<OpenSegmentInfo>,
}

#[derive(Debug)]
pub enum LogBlockError {
    IncompleteHeader,
    IncompleteBody,
    BadHeaderType,
    BadHeaderHash,
    BadBodyHash,
}

fn wrap_capnp<S, E>(path: &PathBuf, res: result::Result<S, E>) -> Result<S>
    where capnp::Error: From<E>
{
    res.map_err(|e| Error::Corrupt(Corruption::CapnpError(capnp::Error::from(e)), path.clone()))
}

fn wrap_io<S>(path: &PathBuf, iotype: IoType, res: io::Result<S>) -> Result<S> {
    res.map_err(|e| Error::Io(iotype, path.clone(), e))
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

impl ObjDirControl {
    fn new(path: &PathBuf) -> Self {
        ObjDirControl {
            base_dir: path.clone(),
            disconnected: RwLock::new(false),
        }
    }

    fn close(&self) {
        let mut closed_lock = self.disconnected.write().unwrap();
        *closed_lock = true;
    }
}

impl ValueHandle {
    pub fn new(data: &[u8]) -> ValueHandle {
        ValueHandle::SmallData { data: Arc::new(Vec::from(data)) }
    }

    pub fn new_words(data: &[capnp::Word]) -> ValueHandle {
        ValueHandle::new(word_to_byte_slice(data))
    }

    fn new_external(dir: Arc<ObjDirControl>, id: u64, size: u64, hash: u64) -> ValueHandle {
        let fc = Arc::new(FileControl {
            dir_control: dir,
            id: id, size: size, hash: hash,
            loaded_data: Mutex::new(None)
        });
        ValueHandle::LargeData { file: fc }
    }

    // will be retooled when adding mmap support ...
    pub fn pin(&self) -> Result<ValuePin> {
        match *self {
            ValueHandle::SmallData { data: ref rc } => Ok(ValuePin { data: rc.clone() }),
            ValueHandle::LargeData { file: ref fl } => {
                let mut data_lock = fl.loaded_data.lock().unwrap();
                if let Some(ref data) = *data_lock {
                    return Ok(ValuePin { data: data.clone() });
                }

                let dcon_lock = fl.dir_control.disconnected.read().unwrap();
                if *dcon_lock {
                    return Err(Error::Disconnected);
                }

                let path = fl.dir_control.base_dir.join(format!("{:06}", fl.id));
                let mut buf = Vec::new();
                let mut fileh = try!(wrap_io(&path, IoType::ObjectOpen, File::open(&path)));
                try!(wrap_io(&path, IoType::ObjectRead, fileh.read_to_end(&mut buf)));
                let buf_arc = Arc::new(buf);
                *data_lock = Some(buf_arc.clone());
                Ok(ValuePin { data: buf_arc })
            }
        }
    }
}

impl ValuePin {
    pub fn data(&self) -> &[u8] {
        &**self.data
    }
}

impl Borrow<[capnp::Word]> for ValuePin {
    fn borrow(&self) -> &[capnp::Word] {
        byte_to_word_slice(self.data())
    }
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
    try!(wrap_io(&journal_path,
                 IoType::JournalRead,
                 journal.read_to_end(&mut jdata)));

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
    try!(wrap_io(journal_path,
                 IoType::JournalWrite,
                 journal.seek(SeekFrom::Start(*journal_position))));
    try!(wrap_io(journal_path,
                 IoType::JournalWrite,
                 journal.write_all(&header)));
    try!(wrap_io(journal_path,
                 IoType::JournalWrite,
                 journal.write_all(byte_block)));
    try!(wrap_io(journal_path, IoType::JournalSync, journal.sync_data()));
    *journal_position += 16 + byte_block.len() as u64;
    Ok(())
}

// call this if you fail to write an entry
fn truncate_log(journal: &mut File, journal_path: &PathBuf, journal_position: u64) -> Result<()> {
    try!(wrap_io(journal_path,
                 IoType::JournalWrite,
                 journal.seek(SeekFrom::Start(journal_position))));
    try!(wrap_io(journal_path,
                 IoType::JournalWrite,
                 journal.write_all(&[0xFFu8, 16])));
    try!(wrap_io(journal_path, IoType::JournalSync, journal.sync_data()));
    Ok(())
}

struct JournalReadResult {
    end_code: LogBlockError,
    end_pos: u64,
    saw_eof: bool,
}

const YROLA_SIGNATURE: &'static [u8] = b"Yrola Journal format <0>\n";

impl Persister {
    pub fn transaction(&self) -> Transaction {
        unimplemented!()
    }

    pub fn open(root: &Path, readonly: bool) -> Result<Self> {
        let root_path = root.to_owned();
        let lock_path = root.join("yrola.lock");
        let wals_path = root.join("wals");
        let objs_path = root.join("objs");

        match fs::metadata(root) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(Error::Corrupt(Corruption::BaseNotDirectory, root.to_owned()));
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound && !readonly {
                    try!(wrap_io(&root_path, IoType::CreateBase, fs::create_dir_all(root)));
                    try!(wrap_io(&wals_path,
                                 IoType::CreateLogDir,
                                 fs::create_dir_all(&wals_path)));
                    try!(wrap_io(&objs_path,
                                 IoType::CreateObjDir,
                                 fs::create_dir_all(&objs_path)));
                    let mut lock_file = try!(wrap_io(&lock_path,
                                                     IoType::CreateLockFile,
                                                     File::create(&lock_path)));
                    try!(wrap_io(&lock_path,
                                 IoType::CreateLockFile,
                                 lock_file.write(YROLA_SIGNATURE)));
                    try!(wrap_io(&lock_path, IoType::CreateLockFile, lock_file.sync_all()));
                } else {
                    return wrap_io(&root_path, IoType::BaseStat, Err(err));
                }
            }
        };

        let mut lock_file = try!(wrap_io(&lock_path,
                                         IoType::LockOpen,
                                         OpenOptions::new().write(true).open(&lock_path)));
        try!(wrap_io(&lock_path, IoType::Lock, lock_file.lock_exclusive()));
        {
            let mut content = Vec::new();
            try!(wrap_io(&lock_path,
                         IoType::LockOpen,
                         lock_file.read_to_end(&mut content)));
            if content != YROLA_SIGNATURE {
                let content_s = String::from_utf8_lossy(&*content).into_owned();
                return Err(Error::Corrupt(Corruption::BadSignature(content_s), lock_path.clone()));
            }
        }

        let mut pers = Persister {
            lock_file: lock_file,
            lock_path: lock_path.clone(),
            objs_path: objs_path.clone(),
            wals_path: wals_path.clone(),
            segments: HashMap::new(),
            objects: HashMap::new(),
            tombstones: HashMap::new(),
            highwater_object_id: 0,
            open_segment: None,
            dir_control: Arc::new(ObjDirControl::new(&objs_path)),
        };

        try!(pers.read_all_journals());
        if !readonly {
            try!(pers.clean_directories());
        }

        Ok(pers)
    }

    fn journal_path(&self, id: u64) -> PathBuf {
        self.wals_path.join(format!("{:06}", id))
    }

    fn read_journal(&mut self,
                    journal_id: u64,
                    expect_closed: bool,
                    mut list_out: Option<&mut HashSet<u64>>)
                    -> Result<(bool, u64)> {
        let jpath = self.journal_path(journal_id);
        let jfile = try!(wrap_io(&jpath, IoType::JournalOpen, File::open(&jpath)));
        let jmeta = try!(wrap_io(&jpath, IoType::JournalStat, jfile.metadata()));
        let (end_code, end_pos, jblocks) = try!(read_log_blocks_file(jfile, &jpath, journal_id));

        assert!(!self.segments.contains_key(&journal_id));
        self.segments.insert(journal_id,
                             SegmentInfo {
                                 live_object_ids: HashSet::new(),
                                 dead_object_ids: HashSet::new(),
                                 live_tombstone_ids: HashSet::new(),
                                 on_disk_size: jmeta.len(),
                             });

        let mut saw_header = false;
        let mut saw_eof = false;

        for block in jblocks {
            if saw_eof {
                return Err(Error::Corrupt(Corruption::LogBlockAfterEof, jpath.clone()));
            }

            let rdr = try!(wrap_capnp(&jpath, capnp::serialize::read_message_from_words(&*block,
                capnp::message::ReaderOptions::new())));
            let log_block = try!(wrap_capnp(&jpath,
                                            rdr.get_root::<yrola_capnp::log_block::Reader>()));

            match try!(wrap_capnp(&jpath, log_block.which())) {
                yrola_capnp::log_block::Which::Eof(_) => {
                    saw_eof = true;
                }

                yrola_capnp::log_block::Which::SegmentHeader(seg_hdr) => {
                    if saw_header {
                        return Err(Error::Corrupt(Corruption::DuplicateSegmentHeader,
                                                  jpath.clone()));
                    }
                    saw_header = true;
                    self.highwater_object_id = cmp::max(self.highwater_object_id,
                                                        seg_hdr.get_highest_ever_item_id());
                    let list_r = try!(wrap_capnp(&jpath, seg_hdr.get_previous_segment_ids()));
                    if let Some(ref mut id_set) = list_out {
                        for ix in 0..list_r.len() {
                            id_set.insert(list_r.get(ix));
                        }
                    }
                }
                yrola_capnp::log_block::Which::Commit(commit) => {
                    let newi_rr = try!(wrap_capnp(&jpath, commit.get_new_inline()));
                    for newi_r in newi_rr.iter() {
                        try!(self.load_object(journal_id, ItemHandle {
                            id: newi_r.get_id(),
                            header: ValueHandle::new(try!(wrap_capnp(&jpath, newi_r.get_header()))),
                            body: ValueHandle::new(try!(wrap_capnp(&jpath, newi_r.get_data()))),
                        }));
                    }

                    let newe_rr = try!(wrap_capnp(&jpath, commit.get_new_external()));
                    for newe_r in newe_rr.iter() {
                        let dc = self.dir_control.clone();
                        try!(self.load_object(journal_id, ItemHandle {
                            id: newe_r.get_id(),
                            header: ValueHandle::new(try!(wrap_capnp(&jpath, newe_r.get_header()))),
                            body: ValueHandle::new_external(
                                dc,
                                newe_r.get_id(),
                                newe_r.get_size(),
                                newe_r.get_hash(),
                            )
                        }));
                    }

                    let del_r = try!(wrap_capnp(&jpath, commit.get_deleted()));
                    for ix in 0..del_r.len() {
                        let del_id = del_r.get(ix);
                        try!(self.load_tombstone(journal_id, del_id));
                    }

                    let obs_r = try!(wrap_capnp(&jpath, commit.get_obsolete_segment_ids()));
                    if let Some(ref mut id_set) = list_out {
                        for ix in 0..obs_r.len() {
                            id_set.remove(&obs_r.get(ix));
                        }
                    }
                }
            }

            if !saw_header {
                return Err(Error::Corrupt(Corruption::MissingSegmentHeader, jpath.clone()));
            }
        }

        if expect_closed {
            if !saw_eof {
                return Err(Error::Corrupt(Corruption::UnclosedJournal {
                                              read_error: end_code,
                                              read_pos: end_pos,
                                          },
                                          jpath.clone()));
            }
        }

        Ok((saw_eof, end_pos))
    }

    fn load_object(&mut self, journal_id: u64, objh: ItemHandle) -> Result<()> {
        let object_id = objh.id();
        if let Some(old) = self.objects.get(&object_id) {
            return Err(Error::Corrupt(Corruption::DupObject {
                                          id: object_id,
                                          other_file: self.journal_path(old.journal_id),
                                      },
                                      self.journal_path(journal_id)));
        }

        self.objects.insert(object_id,
                            ObjectInfo {
                                journal_id: journal_id,
                                data: objh,
                            });

        self.segments.get_mut(&journal_id).unwrap().live_object_ids.insert(object_id);
        Ok(())
    }

    fn load_tombstone(&mut self, journal_id: u64, object_id: u64) -> Result<()> {
        if let Some(old) = self.tombstones.get(&object_id) {
            return Err(Error::Corrupt(Corruption::DupTombstone {
                                          id: object_id,
                                          other_file: self.journal_path(old.journal_id),
                                      },
                                      self.journal_path(journal_id)));
        }

        self.tombstones.insert(object_id, TombstoneInfo { journal_id: journal_id });

        self.segments.get_mut(&journal_id).unwrap().live_tombstone_ids.insert(object_id);
        Ok(())
    }

    fn read_all_journals(&mut self) -> Result<()> {
        // find the journal with the highest number containing at least one valid block
        let mut journal_names = Vec::new();
        let wals_iter = try!(wrap_io(&self.wals_path,
                                     IoType::JournalOpendir,
                                     fs::read_dir(&self.wals_path)));
        for rentry in wals_iter {
            let entry = try!(wrap_io(&self.wals_path, IoType::JournalReaddir, rentry));
            if let Some(jnum) = parse_object_filename(&*entry.file_name()) {
                journal_names.push(jnum);
            }
        }

        journal_names.sort();
        journal_names.reverse();

        let mut also_to_read = HashSet::new();

        for jnum in &journal_names {
            let (closed, write_pos) = try!(self.read_journal(*jnum,
                                                             false,
                                                             Some(&mut also_to_read)));

            if write_pos == 0 {
                continue;
            }

            if !closed {
                self.open_segment = Some(OpenSegmentInfo {
                    id: *jnum,
                    offset: write_pos,
                });
            }

            if also_to_read.contains(jnum) {
                return Err(Error::Corrupt(Corruption::SegmentIncludesSelf,
                                          self.journal_path(*jnum)));
            }
        }

        for also_jnum in also_to_read {
            try!(self.read_journal(also_jnum, true, None));
        }

        for (deleted_id, tsinfo) in &self.tombstones {
            match self.objects.get(deleted_id) {
                Some(obj_info) => {
                    // this is a deleted object
                    // segment must exist for any object or tombstone
                    let mut oseg = self.segments.get_mut(&obj_info.journal_id).unwrap();
                    oseg.live_object_ids.remove(deleted_id);
                    oseg.dead_object_ids.insert(*deleted_id);
                }
                None => {
                    // no object so the tombstone is dead
                    let mut tseg = self.segments.get_mut(&tsinfo.journal_id).unwrap();
                    tseg.live_tombstone_ids.remove(deleted_id);
                }
            }
        }

        Ok(())
    }

    fn clean_directories(&mut self) -> Result<()> {
        let wals_iter = try!(wrap_io(&self.wals_path,
                                     IoType::CleanupOpendir,
                                     fs::read_dir(&self.wals_path)));
        for rentry in wals_iter {
            let entry = try!(wrap_io(&self.wals_path, IoType::CleanupReaddir, rentry));
            if let Some(jnum) = parse_object_filename(&*entry.file_name()) {
                if self.segments.contains_key(&jnum) {
                    continue;
                }
                try!(wrap_io(&entry.path(),
                             IoType::CleanupDelete,
                             fs::remove_file(entry.path())));
            }
        }

        let objs_iter = try!(wrap_io(&self.objs_path,
                                     IoType::CleanupOpendir,
                                     fs::read_dir(&self.objs_path)));
        for rentry in objs_iter {
            let entry = try!(wrap_io(&self.objs_path, IoType::CleanupReaddir, rentry));
            if let Some(jnum) = parse_object_filename(&*entry.file_name()) {
                if self.objects.contains_key(&jnum) && !self.tombstones.contains_key(&jnum) {
                    continue;
                }
                try!(wrap_io(&entry.path(),
                             IoType::CleanupDelete,
                             fs::remove_file(entry.path())));
            }
        }

        Ok(())
    }
}

impl Drop for Persister {
    fn drop(&mut self) {
        self.dir_control.close();
    }
}

#[cfg(test)]
mod tests;
