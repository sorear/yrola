use byteorder::{LittleEndian, ByteOrder};
use capnp;
use fs2::FileExt;
use std::borrow::Borrow;
use std::cmp;
use std::collections::{HashSet, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::hash::{SipHasher, Hasher};
use std::io::{self, ErrorKind, Read, Write, SeekFrom, Seek};
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::u32;
use std::u64;
use yrola_capnp;

#[derive(Debug)]
pub enum Error {
    Corrupt(Corruption, PathBuf),
    Io(IoType, PathBuf, io::Error),
    Exhausted(Exhaustion),
    NotFound(u64),
    Disconnected,
}

#[derive(Debug)]
pub enum Exhaustion {
    SegmentId,
    ObjectId,
    SegmentCount,
    CommitSize(usize, usize),
    HeaderSize(usize, usize),
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
    SegmentOpendir,
    SegmentReaddir,
    SegmentOpen,
    SegmentStat,
    SegmentRead,
    SegmentWrite,
    SegmentSync,
    SegmentCreate,
    SegmentDelete,
    CleanupOpendir,
    CleanupReaddir,
    CleanupDelete,
    ObjectCreate,
    ObjectWrite,
    ObjectSync,
    ObjectOpen,
    ObjectRead,
    ObjectDelete,
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
    UnclosedSegment {
        read_error: LogBlockError,
        read_pos: u64,
    },
    SegmentIncludesSelf,
    BadSignature(String),
    OverlongSegment,
}

pub type Result<T> = result::Result<T, Error>;

struct ObjDirControl {
    base_dir: PathBuf,
    delete_queue: Mutex<Vec<u64>>,
}

struct FileControl {
    dir_control: Arc<ObjDirControl>,
    id: u64,
    size: u64,
    hash: u64,
    loaded_data: Mutex<Option<Arc<Vec<u8>>>>,
    pending_delete: Mutex<bool>,
}

#[derive(Clone)]
pub enum ValueHandle {
    SmallData(Arc<Vec<u8>>),
    LargeData(Arc<FileControl>),
}

#[derive(Clone)]
pub struct ValuePin {
    data: Arc<Vec<u8>>,
}

#[derive(Clone)]
pub struct ItemHandle {
    id: u64,
    header: Arc<Vec<u8>>,
    body: ValueHandle,
}

struct ObjectInfo {
    segment_id: u64,
    data: ItemHandle,
}

struct TombstoneInfo {
    segment_id: u64,
}

#[derive(Clone)]
struct SegmentInfo {
    // dead objects are important because they force us to keep the tombstones around.
    // dead tombstones have no such requirement.
    //
    // must exist in .objects
    live_object_ids: HashSet<u64>,
    // must not exist in .objects, must exist in .tombstones
    dead_object_ids: HashSet<u64>,
    // must not exist in .objects, must exist in .tombstones
    live_tombstone_ids: HashSet<u64>,

    // not valid for the open segment
    on_disk_size: u64,
    // a guess of the space that will be used after evacuation (except constant overhead)
    live_size: u64,
}

struct OpenSegmentInfo {
    id: u64,
    handle: File,
    path: PathBuf,
    offset: u64,
    changes: u32,
    pending_truncate: bool,
}

#[derive(Clone, Default)]
struct JournalConfig {
    app_name: String,
    app_version: u32,
}

// Naturally we need to track the live objects.  To support evacuation, we need
// to be able to ask for any segment what live objects it contains and what
// live tombstones it contains; a tombstone is live if a live segment contains
// a matching dead object.

pub struct Connection {
    lock_path: PathBuf,
    segs_path: PathBuf,
    objs_path: PathBuf,

    lock_file: File,

    dir_control: Arc<ObjDirControl>,
    segments: HashMap<u64, SegmentInfo>,
    // objects and tombstones are live only (after initial journal read)
    objects: HashMap<u64, ObjectInfo>,
    tombstones: HashMap<u64, TombstoneInfo>,
    highwater_object_id: u64,
    highwater_segment_id: u64,
    open_segment: Option<OpenSegmentInfo>,
    config: JournalConfig,
    pending_delete_segment: Vec<u64>,

    live_size: u64,
    on_disk_size: u64,
}

#[derive(Debug)]
pub enum LogBlockError {
    IncompleteHeader,
    IncompleteBody,
    BadHeaderType,
    BadHeaderHash,
    BadBodyHash,
}

const YROLA_SIGNATURE: &'static [u8] = b"Yrola Journal format <0>\n";

const MAX_SEGMENT_CHANGES: u32 = 1 << 20;
const MAX_SEGMENT_COUNT: usize = 1 << 20;
const MAX_HEADER_LEN: usize = 1 << 20;
const TOMBSTONE_SIZE: u64 = 8;
const EXTERNAL_OBJ_SIZE: u64 = 32;
const INLINE_OBJ_SIZE: u64 = 24;
const FLAT_OVERHEAD: u64 = 4000;
const OVERHEAD_PER_SEGMENT: u64 = 8;

// TODO(someday): Tuning parameter
const MAX_INLINE_LEN: usize = 1 << 15;
const TARGET_SEGMENT_SIZE: u64 = 1 << 24;
const TARGET_RATIO: f64 = 0.3;

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

impl ObjDirControl {
    fn new(path: &PathBuf) -> Self {
        ObjDirControl {
            base_dir: path.clone(),
            delete_queue: Mutex::new(Vec::new()),
        }
    }
}

impl Drop for FileControl {
    fn drop(&mut self) {
        let lock_del = self.pending_delete.lock().unwrap();
        if *lock_del {
            let mut lock_q = self.dir_control.delete_queue.lock().unwrap();
            lock_q.push(self.id);
            // the actual delete happens on the next commit.
        }
    }
}

impl ValueHandle {
    pub fn new(data: &[u8]) -> ValueHandle {
        ValueHandle::SmallData(Arc::new(Vec::from(data)))
    }

    pub fn new_words(data: &[capnp::Word]) -> ValueHandle {
        ValueHandle::new(capnp::Word::words_to_bytes(data))
    }

    fn is_external(&self) -> bool {
        match *self {
            ValueHandle::SmallData(_) => false,
            ValueHandle::LargeData(_) => true,
        }
    }

    fn mark_delete(&self, should_delete: bool) {
        match *self {
            ValueHandle::SmallData(_) => {}
            ValueHandle::LargeData(ref file) => {
                let mut lock = file.pending_delete.lock().unwrap();
                *lock = should_delete;
            }
        }
    }

    fn new_external(dir: Arc<ObjDirControl>, id: u64, size: u64, hash: u64) -> ValueHandle {
        let fc = Arc::new(FileControl {
            dir_control: dir,
            id: id,
            size: size,
            hash: hash,
            loaded_data: Mutex::new(None),
            pending_delete: Mutex::new(false),
        });
        ValueHandle::LargeData(fc)
    }

    // will be retooled when adding mmap support ...
    pub fn pin(&self) -> Result<ValuePin> {
        match *self {
            ValueHandle::SmallData(ref rc) => Ok(ValuePin { data: rc.clone() }),
            ValueHandle::LargeData(ref fl) => {
                let mut data_lock = fl.loaded_data.lock().unwrap();
                if let Some(ref data) = *data_lock {
                    return Ok(ValuePin { data: data.clone() });
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
        capnp::Word::bytes_to_words(self.data())
    }
}

impl ItemHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn header(&self) -> &[u8] {
        &*self.header
    }

    pub fn body(&self) -> &ValueHandle {
        &self.body
    }

    fn live_size(&self) -> u64 {
        let header = (self.header.len() as u64 + 7) & !7;
        match self.body {
            ValueHandle::SmallData(ref data) => {
                INLINE_OBJ_SIZE + header + ((data.len() as u64 + 7) & !7)
            }
            ValueHandle::LargeData(_) => EXTERNAL_OBJ_SIZE + header,
        }
    }
}

pub struct ItemIterator<'a> {
    new_iter: ::std::collections::hash_map::Values<'a, u64, ItemHandle>,
    existing_iter: ::std::collections::hash_map::Values<'a, u64, ObjectInfo>,
    deleted: &'a HashSet<u64>,
    new_iter_done: bool,
    existing_iter_done: bool,
}

impl<'a> Iterator for ItemIterator<'a> {
    type Item = &'a ItemHandle;
    fn next(&mut self) -> Option<&'a ItemHandle> {
        loop {
            if !self.new_iter_done {
                match self.new_iter.next() {
                    r@Some(_) => return r,
                    None => self.new_iter_done = true,
                }
            }

            if !self.existing_iter_done {
                match self.existing_iter.next() {
                    Some(obji) if self.deleted.contains(&obji.data.id) => continue,
                    Some(obji) => return Some(&obji.data),
                    None => self.existing_iter_done = true,
                }
            }

            return None;
        }
    }
}

pub struct Transaction<'a> {
    journal: &'a mut Connection,
    new_items: HashMap<u64, ItemHandle>,
    del_items: HashSet<u64>,
    large_objects: Vec<(File, PathBuf, ValueHandle)>,
}

impl<'a> Transaction<'a> {
    pub fn add_item(&mut self, header: Vec<u8>, data: Vec<u8>) -> Result<u64> {
        if self.journal.highwater_object_id == u64::MAX {
            return Err(Error::Exhausted(Exhaustion::ObjectId));
        }

        if header.len() > MAX_HEADER_LEN {
            return Err(Error::Exhausted(Exhaustion::HeaderSize(header.len(), MAX_HEADER_LEN)));
        }

        self.journal.highwater_object_id += 1;
        let id = self.journal.highwater_object_id;

        if data.len() > MAX_INLINE_LEN {
            let path = self.journal.object_path(id);
            let mut handle = try!(wrap_io(&path, IoType::ObjectCreate, File::create(&path)));
            let hash = external_hash(id, &*data);
            try!(wrap_io(&path, IoType::ObjectWrite, handle.write_all(&*data)));
            let file_struct = Arc::new(FileControl {
                id: id,
                size: data.len() as u64,
                hash: hash,
                dir_control: self.journal.dir_control.clone(),
                pending_delete: Mutex::new(true),
                loaded_data: Mutex::new(Some(Arc::new(data))),
            });
            let new_ih = ItemHandle {
                id: id,
                header: Arc::new(header),
                body: ValueHandle::LargeData(file_struct),
            };
            self.large_objects.push((handle, path, new_ih.body.clone()));
            self.new_items.insert(id, new_ih);
        } else {
            let new_ih = ItemHandle {
                id: id,
                header: Arc::new(header),
                body: ValueHandle::SmallData(Arc::new(data)),
            };
            self.new_items.insert(id, new_ih);
        }

        Ok(id)
    }

    pub fn del_item(&mut self, id: u64) -> Result<()> {
        if self.new_items.remove(&id).is_some() {
            Ok(())
        } else if self.journal.objects.contains_key(&id) && !self.del_items.contains(&id) {
            self.del_items.insert(id);
            Ok(())
        } else {
            Err(Error::NotFound(id))
        }
    }

    pub fn get_item(&mut self, id: u64) -> Result<&ItemHandle> {
        if let Some(ref_ith) = self.new_items.get(&id) {
            return Ok(ref_ith);
        }

        if let Some(ref_objinfo) = self.journal.objects.get(&id) {
            if !self.del_items.contains(&id) {
                return Ok(&ref_objinfo.data);
            }
        }

        return Err(Error::NotFound(id));
    }

    pub fn list_items<'b>(&'b mut self) -> ItemIterator<'b> {
        ItemIterator {
            existing_iter: self.journal.objects.values(),
            new_iter: self.new_items.values(),
            existing_iter_done: false,
            new_iter_done: false,
            deleted: &self.del_items,
        }
    }

    pub fn commit(self) -> Result<()> {
        let count = self.new_items.len() + self.del_items.len();
        if count > MAX_SEGMENT_CHANGES as usize {
            let exhtype = Exhaustion::CommitSize(count, MAX_SEGMENT_CHANGES as usize);
            return Err(Error::Exhausted(exhtype));
        }

        try!(self.journal.flush_deletes());
        try!(self.journal.vacuum());

        let mut msg_builder = capnp::message::Builder::new_default();

        {
            let block_builder = msg_builder.init_root::<yrola_capnp::log_block::Builder>();
            let mut commit_builder = block_builder.init_commit();

            let mut inline_count = 0;
            for item in self.new_items.values() {
                if !item.body.is_external() {
                    inline_count += 1;
                }
            }
            if inline_count > 0 {
                // TODO(someday): Without the &mut the borrowck balks. ???
                let mut inline_builder = (&mut commit_builder)
                                             .borrow()
                                             .init_new_inline(inline_count);
                let mut inline_ix = 0;

                for (obj_id, item_hdl) in &self.new_items {
                    if item_hdl.body.is_external() {
                        continue;
                    }
                    let mut nin_builder = (&mut inline_builder).borrow().get(inline_ix);
                    inline_ix += 1;
                    nin_builder.set_id(*obj_id);
                    nin_builder.set_header(&*item_hdl.header);
                    match item_hdl.body {
                        ValueHandle::SmallData(ref body) => {
                            nin_builder.set_data(&*body);
                        }
                        ValueHandle::LargeData(_) => unreachable!(),
                    }
                }
            }

            let mut external_count = 0;
            for item in self.new_items.values() {
                if item.body.is_external() {
                    external_count += 1;
                }
            }
            if external_count > 0 {
                // TODO(someday): Without the &mut the borrowck balks. ???
                let mut extern_builder = (&mut commit_builder)
                                             .borrow()
                                             .init_new_external(external_count);
                let mut extern_ix = 0;

                for (obj_id, item_hdl) in &self.new_items {
                    if !item_hdl.body.is_external() {
                        continue;
                    }
                    let mut nex_builder = (&mut extern_builder).borrow().get(extern_ix);
                    extern_ix += 1;
                    nex_builder.set_id(*obj_id);
                    nex_builder.set_header(&*item_hdl.header);
                    match item_hdl.body {
                        ValueHandle::SmallData(_) => unreachable!(),
                        ValueHandle::LargeData(ref file) => {
                            nex_builder.set_hash(file.hash);
                            nex_builder.set_size(file.size);
                        }
                    }
                }
            }

            if !self.del_items.is_empty() {
                let mut ts_builder = (&mut commit_builder)
                                         .borrow()
                                         .init_deleted(self.del_items.len() as u32);
                let mut ts_ix = 0;

                for ts_id in &self.del_items {
                    ts_builder.set(ts_ix, *ts_id);
                    ts_ix += 1;
                }
            }
        }

        let msg_words = capnp::serialize::write_message_to_words(&msg_builder);
        let write_seg_id = try!(self.journal.ensure_open_segment(count as u32));

        // make sure the objects are written and not deleted before the segment
        for &(ref hdl, ref path, ref struc) in &self.large_objects {
            try!(wrap_io(path, IoType::ObjectSync, hdl.sync_data()));
            struc.mark_delete(false);
        }

        match self.journal.write_block(count as u32, &*msg_words) {
            Ok(_) => {}
            e@Err(_) => {
                // whoops, we do want these deleted after all
                for &(ref _hdl, ref _path, ref struc) in &self.large_objects {
                    struc.mark_delete(true);
                }
                return e;
            }
        }

        // we wrote it to the journal, now update our data structures
        for (obj_id, ith) in self.new_items {
            let mut wr_seg = self.journal.segments.get_mut(&write_seg_id).unwrap();
            wr_seg.live_object_ids.insert(obj_id);
            wr_seg.live_size += ith.live_size();
            self.journal.live_size += ith.live_size();
            self.journal.objects.insert(obj_id,
                                        ObjectInfo {
                                            segment_id: write_seg_id,
                                            data: ith,
                                        });
        }

        for ts_id in self.del_items {
            let obj_info = self.journal.objects.remove(&ts_id).unwrap();

            if obj_info.segment_id == write_seg_id {
                // killing an object in the same segment it was written does not create a live
                // tombstone (it does need to be written, though)
                {
                    let mut obj_seg = self.journal.segments.get_mut(&obj_info.segment_id).unwrap();
                    obj_seg.live_object_ids.remove(&ts_id);
                    obj_seg.live_size -= obj_info.data.live_size();
                    self.journal.live_size -= obj_info.data.live_size();
                }
            } else {
                // we need to track this tombstone if the segment is evacuated later, and track the
                // object so that the tombstone can be removed when the object is purged
                {
                    let mut obj_seg = self.journal.segments.get_mut(&obj_info.segment_id).unwrap();
                    obj_seg.live_object_ids.remove(&ts_id);
                    obj_seg.dead_object_ids.insert(ts_id);
                    obj_seg.live_size -= obj_info.data.live_size();
                    self.journal.live_size -= obj_info.data.live_size();
                }
                self.journal.tombstones.insert(ts_id, TombstoneInfo { segment_id: write_seg_id });
                let mut wseg_info = self.journal
                                        .segments
                                        .get_mut(&write_seg_id)
                                        .unwrap();

                wseg_info.live_tombstone_ids
                         .insert(ts_id);
                wseg_info.live_size += TOMBSTONE_SIZE;
                self.journal.live_size += TOMBSTONE_SIZE;
            }

            obj_info.data.body.mark_delete(true);
        }

        // if this fails, we'll try again and fail the next commit
        let _ = self.journal.flush_deletes();

        Ok(())
    }
}

// we're using SipHash for its engineering properties, so a hard-coded key is not a big deal
const BLOCK_K0: u64 = 0xd66db8e2464451f1;
const BLOCK_K1: u64 = 0x217f5ed82cc28caa;
const LEADER_K0: u64 = 0xaaba64c0b2f8bf29;
const LEADER_K1: u64 = 0x520c5e4629fdf1b0;
const EXTERNAL_K0: u64 = 0xae880699c0628ab8;
const EXTERNAL_K1: u64 = 0x3f9c216ed189d0f3;

fn leader_hash(segment_id: u64, segment_offset: u64, leader: u32) -> u32 {
    let mut hasher = SipHasher::new_with_keys(LEADER_K0, LEADER_K1);
    hasher.write_u64(segment_id);
    hasher.write_u64(segment_offset);
    hasher.write_u32(leader);
    hasher.finish() as u32
}

fn external_hash(file_id: u64, data: &[u8]) -> u64 {
    let mut hasher = SipHasher::new_with_keys(EXTERNAL_K0, EXTERNAL_K1);
    hasher.write_u64(file_id);
    hasher.write(data);
    hasher.finish()
}

fn log_block_hash(segment_id: u64, segment_position: u64, data: &[u8]) -> u64 {
    let mut hasher = SipHasher::new_with_keys(BLOCK_K0, BLOCK_K1);
    hasher.write_u64(segment_id);
    hasher.write_u64(segment_position);
    hasher.write(data);
    hasher.finish()
}

fn read_log_blocks_file(mut segment: File,
                        segment_path: &PathBuf,
                        segment_id: u64)
                        -> Result<(LogBlockError, u64, Vec<Vec<capnp::Word>>)> {
    let mut jdata = Vec::new();
    try!(wrap_io(&segment_path,
                 IoType::SegmentRead,
                 segment.read_to_end(&mut jdata)));

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

        if leader_hash(segment_id, read_ptr as u64, block_len) != lhash {
            break_code = LogBlockError::BadHeaderHash;
            break;
        }

        read_ptr += 16;

        if jdata.len() - read_ptr < (block_len as usize) {
            break_code = LogBlockError::IncompleteBody;
            break;
        }

        let block_slice = &jdata[read_ptr..(read_ptr + (block_len as usize))];

        if log_block_hash(segment_id, read_ptr as u64 - 16, block_slice) != hash {
            break_code = LogBlockError::BadBodyHash;
            break;
        }

        blocks.push(Vec::from(capnp::Word::bytes_to_words(block_slice)));
    }

    Ok((break_code, read_ptr as u64, blocks))
}

fn write_log_block(segment: &mut File,
                   segment_path: &PathBuf,
                   segment_id: u64,
                   segment_position: &mut u64,
                   block: &[capnp::Word])
                   -> Result<()> {
    let byte_block = capnp::Word::words_to_bytes(block);
    assert!(byte_block.len() <= u32::MAX as usize);
    let block_hash = log_block_hash(segment_id, *segment_position, byte_block);
    let leader = byte_block.len() as u64 |
                 ((leader_hash(segment_id, *segment_position, byte_block.len() as u32) as u64) <<
                  32);

    let mut header = [0u8; 16];
    LittleEndian::write_u64(&mut header[0..8], leader);
    LittleEndian::write_u64(&mut header[8..16], block_hash);
    try!(wrap_io(segment_path,
                 IoType::SegmentWrite,
                 segment.seek(SeekFrom::Start(*segment_position))));
    try!(wrap_io(segment_path,
                 IoType::SegmentWrite,
                 segment.write_all(&header)));
    try!(wrap_io(segment_path,
                 IoType::SegmentWrite,
                 segment.write_all(byte_block)));
    try!(wrap_io(segment_path, IoType::SegmentSync, segment.sync_data()));
    *segment_position += 16 + byte_block.len() as u64;
    Ok(())
}

// call this if you fail to write an entry
fn truncate_log(segment: &mut File, segment_path: &PathBuf, segment_position: u64) -> Result<()> {
    try!(wrap_io(segment_path,
                 IoType::SegmentWrite,
                 segment.seek(SeekFrom::Start(segment_position))));
    try!(wrap_io(segment_path,
                 IoType::SegmentWrite,
                 segment.write_all(&[0xFFu8, 16])));
    try!(wrap_io(segment_path, IoType::SegmentSync, segment.sync_data()));
    Ok(())
}

impl Connection {
    pub fn transaction(&mut self) -> Transaction {
        Transaction {
            journal: self,
            new_items: HashMap::new(),
            del_items: HashSet::new(),
            large_objects: Vec::new(),
        }
    }

    pub fn open(root: &Path, readonly: bool) -> Result<Self> {
        let root_path = root.to_owned();
        let lock_path = root.join("yrola.lock");
        let segs_path = root.join("segments");
        let objs_path = root.join("objects");

        match fs::metadata(root) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(Error::Corrupt(Corruption::BaseNotDirectory, root.to_owned()));
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound && !readonly {
                    try!(wrap_io(&root_path, IoType::CreateBase, fs::create_dir_all(root)));
                    try!(wrap_io(&segs_path,
                                 IoType::CreateLogDir,
                                 fs::create_dir_all(&segs_path)));
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

        let mut pers = Connection {
            lock_file: lock_file,
            lock_path: lock_path.clone(),
            objs_path: objs_path.clone(),
            segs_path: segs_path.clone(),
            segments: HashMap::new(),
            objects: HashMap::new(),
            tombstones: HashMap::new(),
            highwater_object_id: 0,
            highwater_segment_id: 0,
            open_segment: None,
            dir_control: Arc::new(ObjDirControl::new(&objs_path)),
            pending_delete_segment: Vec::new(),
            config: JournalConfig::default(),
            live_size: 0,
            on_disk_size: 0,
        };

        try!(pers.read_all_segments());
        if !readonly {
            try!(pers.clean_directories());
        }

        Ok(pers)
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.segs_path.join(format!("{:06}", id))
    }

    fn object_path(&self, id: u64) -> PathBuf {
        self.objs_path.join(format!("{:06}", id))
    }

    fn read_segment(&mut self,
                    segment_id: u64,
                    closed_segment: bool,
                    mut list_out: Option<&mut HashSet<u64>>)
                    -> Result<(bool, u64, u32)> {
        let jpath = self.segment_path(segment_id);
        let jfile = try!(wrap_io(&jpath, IoType::SegmentOpen, File::open(&jpath)));
        let jmeta = try!(wrap_io(&jpath, IoType::SegmentStat, jfile.metadata()));
        let (end_code, end_pos, jblocks) = try!(read_log_blocks_file(jfile, &jpath, segment_id));

        assert!(!self.segments.contains_key(&segment_id));
        self.segments.insert(segment_id,
                             SegmentInfo {
                                 live_object_ids: HashSet::new(),
                                 dead_object_ids: HashSet::new(),
                                 live_tombstone_ids: HashSet::new(),
                                 on_disk_size: jmeta.len(),
                                 live_size: 0,
                             });
        if closed_segment {
            self.on_disk_size += jmeta.len();
        }

        let mut saw_header = false;
        let mut saw_eof = false;
        let mut change_counter = 0u32;

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

                    if !closed_segment {
                        try!(self.load_config(&jpath,
                                              try!(wrap_capnp(&jpath, seg_hdr.get_config()))));
                    }
                }
                yrola_capnp::log_block::Which::Commit(commit) => {
                    let newi_rr = try!(wrap_capnp(&jpath, commit.get_new_inline()));
                    change_counter += newi_rr.len();
                    for newi_r in newi_rr.iter() {
                        let hdr = try!(wrap_capnp(&jpath, newi_r.get_header()));
                        try!(self.load_object(segment_id, ItemHandle {
                            id: newi_r.get_id(),
                            header: Arc::new(Vec::from(hdr)),
                            body: ValueHandle::new(try!(wrap_capnp(&jpath, newi_r.get_data()))),
                        }));
                    }

                    let newe_rr = try!(wrap_capnp(&jpath, commit.get_new_external()));
                    change_counter += newe_rr.len();
                    for newe_r in newe_rr.iter() {
                        let dc = self.dir_control.clone();
                        let hdr = try!(wrap_capnp(&jpath, newe_r.get_header()));
                        try!(self.load_object(segment_id, ItemHandle {
                            id: newe_r.get_id(),
                            header: Arc::new(Vec::from(hdr)),
                            body: ValueHandle::new_external(
                                dc,
                                newe_r.get_id(),
                                newe_r.get_size(),
                                newe_r.get_hash(),
                            )
                        }));
                    }

                    let del_r = try!(wrap_capnp(&jpath, commit.get_deleted()));
                    change_counter += del_r.len();
                    for ix in 0..del_r.len() {
                        let del_id = del_r.get(ix);
                        try!(self.load_tombstone(segment_id, del_id));
                    }

                    let obs_r = try!(wrap_capnp(&jpath, commit.get_obsolete_segment_ids()));
                    if let Some(ref mut id_set) = list_out {
                        for ix in 0..obs_r.len() {
                            id_set.remove(&obs_r.get(ix));
                        }
                    }

                    if change_counter > MAX_SEGMENT_CHANGES {
                        return Err(Error::Corrupt(Corruption::OverlongSegment, jpath.clone()));
                    }

                    if !closed_segment && commit.has_new_config() {
                        try!(self.load_config(&jpath,
                                              try!(wrap_capnp(&jpath, commit.get_new_config()))));
                    }
                }
            }

            if !saw_header {
                return Err(Error::Corrupt(Corruption::MissingSegmentHeader, jpath.clone()));
            }
        }

        if closed_segment {
            if !saw_eof {
                return Err(Error::Corrupt(Corruption::UnclosedSegment {
                                              read_error: end_code,
                                              read_pos: end_pos,
                                          },
                                          jpath.clone()));
            }
        }

        Ok((saw_eof, end_pos, change_counter))
    }

    fn load_config<'a>(&mut self,
                       filename: &PathBuf,
                       reader: yrola_capnp::journal_config::Reader<'a>)
                       -> Result<()> {
        self.config.app_name = try!(wrap_capnp(filename, reader.get_app_name())).to_owned();
        self.config.app_version = reader.get_app_version();
        Ok(())
    }

    fn save_config<'a>(&self, mut builder: yrola_capnp::journal_config::Builder<'a>) {
        builder.set_app_name(&*self.config.app_name);
        builder.set_app_version(self.config.app_version);
    }

    fn load_object(&mut self, segment_id: u64, objh: ItemHandle) -> Result<()> {
        let object_id = objh.id();
        self.highwater_object_id = cmp::max(self.highwater_object_id, object_id);
        if let Some(old) = self.objects.get(&object_id) {
            return Err(Error::Corrupt(Corruption::DupObject {
                                          id: object_id,
                                          other_file: self.segment_path(old.segment_id),
                                      },
                                      self.segment_path(segment_id)));
        }

        let mut seg_info = self.segments.get_mut(&segment_id).unwrap();
        seg_info.live_object_ids.insert(object_id);
        seg_info.live_size += objh.live_size();
        self.live_size += objh.live_size();

        self.objects.insert(object_id,
                            ObjectInfo {
                                segment_id: segment_id,
                                data: objh,
                            });

        Ok(())
    }

    fn load_tombstone(&mut self, segment_id: u64, object_id: u64) -> Result<()> {
        if let Some(old) = self.tombstones.get(&object_id) {
            return Err(Error::Corrupt(Corruption::DupTombstone {
                                          id: object_id,
                                          other_file: self.segment_path(old.segment_id),
                                      },
                                      self.segment_path(segment_id)));
        }

        self.tombstones.insert(object_id, TombstoneInfo { segment_id: segment_id });

        let mut seg_info = self.segments.get_mut(&segment_id).unwrap();
        seg_info.live_tombstone_ids.insert(object_id);
        seg_info.live_size += TOMBSTONE_SIZE;
        self.live_size += TOMBSTONE_SIZE;
        Ok(())
    }

    fn read_all_segments(&mut self) -> Result<()> {
        // find the segment with the highest number containing at least one valid block
        let mut segment_names = Vec::new();
        let segs_iter = try!(wrap_io(&self.segs_path,
                                     IoType::SegmentOpendir,
                                     fs::read_dir(&self.segs_path)));
        for rentry in segs_iter {
            let entry = try!(wrap_io(&self.segs_path, IoType::SegmentReaddir, rentry));
            if let Some(jnum) = parse_object_filename(&*entry.file_name()) {
                segment_names.push(jnum);
            }
        }

        segment_names.sort();
        segment_names.reverse();

        let mut also_to_read = HashSet::new();

        for jnum in &segment_names {
            let (closed, write_pos, changes) = try!(self.read_segment(*jnum,
                                                                      false,
                                                                      Some(&mut also_to_read)));

            if write_pos == 0 {
                continue;
            }

            if !closed {
                let spath = self.segment_path(*jnum);
                let writeh = try!(wrap_io(&spath,
                                          IoType::SegmentOpen,
                                          OpenOptions::new().write(true).open(&spath)));
                self.open_segment = Some(OpenSegmentInfo {
                    id: *jnum,
                    offset: write_pos,
                    handle: writeh,
                    changes: changes,
                    path: spath,
                    pending_truncate: false,
                });
            }

            if also_to_read.contains(jnum) {
                return Err(Error::Corrupt(Corruption::SegmentIncludesSelf,
                                          self.segment_path(*jnum)));
            }

            self.highwater_segment_id = *jnum;

            break;
        }

        for also_jnum in also_to_read {
            try!(self.read_segment(also_jnum, true, None));
        }

        let mut stale_ts = Vec::new();
        for (deleted_id, tsinfo) in &self.tombstones {
            match self.objects.remove(deleted_id) {
                Some(obj_info) => {
                    // this is a deleted object
                    // segment must exist for any object or tombstone
                    let mut oseg = self.segments.get_mut(&obj_info.segment_id).unwrap();
                    oseg.live_object_ids.remove(deleted_id);
                    oseg.live_size -= obj_info.data.live_size();
                    self.live_size -= obj_info.data.live_size();
                    oseg.dead_object_ids.insert(*deleted_id);
                }
                None => {
                    // no object so the tombstone is dead
                    let mut tseg = self.segments.get_mut(&tsinfo.segment_id).unwrap();
                    stale_ts.push(*deleted_id);
                    tseg.live_tombstone_ids.remove(deleted_id);
                    tseg.live_size -= TOMBSTONE_SIZE;
                    self.live_size -= TOMBSTONE_SIZE;
                }
            }
        }

        for deleted_id in stale_ts {
            self.tombstones.remove(&deleted_id);
        }

        Ok(())
    }

    fn clean_directories(&mut self) -> Result<()> {
        let segs_iter = try!(wrap_io(&self.segs_path,
                                     IoType::CleanupOpendir,
                                     fs::read_dir(&self.segs_path)));
        for rentry in segs_iter {
            let entry = try!(wrap_io(&self.segs_path, IoType::CleanupReaddir, rentry));
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
                if self.objects.contains_key(&jnum) {
                    continue;
                }
                try!(wrap_io(&entry.path(),
                             IoType::CleanupDelete,
                             fs::remove_file(entry.path())));
            }
        }

        Ok(())
    }

    fn segment_full(&self, changes: u32) -> bool {
        assert!(changes <= MAX_SEGMENT_CHANGES);

        if let Some(osi) = self.open_segment.as_ref() {
            assert!(osi.changes <= MAX_SEGMENT_CHANGES);
            if changes > MAX_SEGMENT_CHANGES - osi.changes {
                return true;
            }

            if osi.offset >= TARGET_SEGMENT_SIZE {
                return true;
            }
        }

        return false;
    }

    // returns the number of the open segment, you're likely to need it
    fn ensure_open_segment(&mut self, changes: u32) -> Result<u64> {
        if self.segment_full(changes) {
            // try to write EOF, if it takes, end the segment
            let mut msg_builder = capnp::message::Builder::new_default();

            {
                let mut block_builder = msg_builder.init_root::<yrola_capnp::log_block::Builder>();
                block_builder.set_eof(());
            }

            let msg_words = capnp::serialize::write_message_to_words(&msg_builder);
            try!(self.write_block(0, &*msg_words));

            let final_state = self.open_segment.take().unwrap();
            let mut finished_seg = self.segments.get_mut(&final_state.id).unwrap();
            finished_seg.on_disk_size = final_state.offset;
            self.on_disk_size += final_state.offset;
        }

        if self.open_segment.is_none() {
            // open a brand-new segment file
            if self.highwater_segment_id == u64::MAX {
                return Err(Error::Exhausted(Exhaustion::SegmentId));
            }

            if self.segments.len() >= MAX_SEGMENT_COUNT {
                return Err(Error::Exhausted(Exhaustion::SegmentCount));
            }

            self.highwater_segment_id = self.highwater_segment_id + 1;
            let new_seg_id = self.highwater_segment_id;
            let new_seg_path = self.segment_path(new_seg_id);
            let writeh = try!(wrap_io(&new_seg_path,
                                      IoType::SegmentCreate,
                                      File::create(&new_seg_path)));

            self.open_segment = Some(OpenSegmentInfo {
                id: new_seg_id,
                handle: writeh,
                path: new_seg_path,
                offset: 0,
                changes: 0,
                pending_truncate: false,
            });
            self.segments.insert(new_seg_id,
                                 SegmentInfo {
                                     live_object_ids: HashSet::new(),
                                     dead_object_ids: HashSet::new(),
                                     live_tombstone_ids: HashSet::new(),
                                     on_disk_size: 0,
                                     live_size: 0,
                                 });
        }

        let open_seg_id = self.open_segment.as_ref().unwrap().id;
        if self.open_segment.as_ref().unwrap().offset == 0 {
            // log a SegmentHeader before anything else
            let mut msg_builder = capnp::message::Builder::new_default();

            {
                let block_builder = msg_builder.init_root::<yrola_capnp::log_block::Builder>();
                let mut header_builder = block_builder.init_segment_header();

                {
                    let prev_cnt = (self.segments.len() - 1) as u32;
                    let mut previd_builder = (&mut header_builder)
                                                 .borrow()
                                                 .init_previous_segment_ids(prev_cnt);
                    let mut prev_ix = 0;
                    for prev_seg_id in self.segments.keys() {
                        if *prev_seg_id != open_seg_id {
                            previd_builder.set(prev_ix, *prev_seg_id);
                            prev_ix += 1;
                        }
                    }
                }

                header_builder.set_highest_ever_item_id(self.highwater_object_id);
                self.save_config((&mut header_builder).borrow().init_config());
            }

            let msg_words = capnp::serialize::write_message_to_words(&msg_builder);
            try!(self.write_block(0, &*msg_words));
        }

        Ok(open_seg_id)
    }

    // call ensure_open_segment first!
    fn write_block(&mut self, count: u32, block: &[capnp::Word]) -> Result<()> {
        let mut osinfo = self.open_segment.as_mut().unwrap();

        if osinfo.pending_truncate {
            // maintain the charade by refusing to commit anything if we think the last commit
            // failed, until we can convince the storage layer that the commit did fail

            try!(truncate_log(&mut osinfo.handle, &osinfo.path, osinfo.offset));
            // hey, we have consensus
            osinfo.pending_truncate = false;
        }

        match write_log_block(&mut osinfo.handle,
                              &osinfo.path,
                              osinfo.id,
                              &mut osinfo.offset,
                              block) {
            Ok(_) => {
                osinfo.changes += count;
                Ok(())
            }
            e@Err(_) => {
                // We might or might not have successfully committed.  Try to roll back.
                // If we crash before the rollback attempt, then the app will never find out
                // whether succeeded or not and either outcome is consistent.
                // If we rollback successfully, then we'll effectively never have committed, and
                // the app will see the commit having cleanly failed.  (This is for ENOSPC).
                // If the rollback fails, we're in a sticky situation; we need to report something
                // to the app, but we don't know.  Pretend it failed, and make sure nothing else
                // is sent to the log until the rollback succeeds.  If the rollback never suceeds
                // before the next crash, *and* the operation actually took, then we'll be stuck
                // with a commit that the app thinks failed; some communication of expectations is
                // required here.
                match truncate_log(&mut osinfo.handle, &osinfo.path, osinfo.offset) {
                    Ok(_) => {}
                    Err(_) => {
                        // Both the commit and the uncommit failed... mark the log
                        osinfo.pending_truncate = true;
                    }
                }
                e
            }
        }
    }

    fn vacuum(&mut self) -> Result<()> {
        let disk_size = self.on_disk_size;
        let mut live_data_size = self.live_size;

        if let Some(oseginf) = self.open_segment.as_ref() {
            live_data_size -= self.segments.get(&oseginf.id).unwrap().live_size;
        }

        let segment_count = 1 + 2 * live_data_size / TARGET_SEGMENT_SIZE;
        live_data_size += segment_count * FLAT_OVERHEAD;

        if live_data_size < (disk_size as f64 * TARGET_RATIO) as u64 {
            let mut emptiest_segid = None;
            let mut emptiest_bytes = u64::MAX;
            for (seg_id, seg_info) in &self.segments {
                if seg_info.live_size < emptiest_bytes {
                    emptiest_bytes = seg_info.live_size;
                    emptiest_segid = Some(*seg_id);
                }
            }

            if let Some(id) = emptiest_segid {
                try!(self.evacuate(id));
            }
        }

        Ok(())
    }

    fn evacuate(&mut self, segment_id: u64) -> Result<()> {
        // TODO(someday): the allocation here is almost certainly avoidable
        let seginfo = self.segments.get(&segment_id).unwrap().clone();

        let mut ext_count = 0usize;
        for obj_id in &seginfo.live_object_ids {
            if self.objects.get(obj_id).unwrap().data.body.is_external() {
                ext_count += 1;
            }
        }
        let inl_count = seginfo.live_object_ids.len() - ext_count;

        let mut msg_builder = capnp::message::Builder::new_default();

        {
            let block_builder = msg_builder.init_root::<yrola_capnp::log_block::Builder>();
            let mut commit_builder = block_builder.init_commit();

            {
                // TODO(someday): Without the &mut the borrowck balks. ???
                let mut inline_builder = (&mut commit_builder)
                                             .borrow()
                                             .init_new_inline(inl_count as u32);
                let mut inline_ix = 0;

                for obj_id in &seginfo.live_object_ids {
                    let obj_data = &self.objects.get(obj_id).unwrap().data;
                    match obj_data.body {
                        ValueHandle::SmallData(ref data) => {
                            let mut nin_builder = (&mut inline_builder).borrow().get(inline_ix);
                            inline_ix += 1;
                            nin_builder.set_id(obj_data.id);
                            nin_builder.set_header(&*obj_data.header);
                            nin_builder.set_data(&*data);
                        }
                        ValueHandle::LargeData(_) => {}
                    }
                }
            }

            {
                let mut extern_builder = (&mut commit_builder)
                                             .borrow()
                                             .init_new_external(ext_count as u32);
                let mut extern_ix = 0;

                for obj_id in &seginfo.live_object_ids {
                    let obj_data = &self.objects.get(obj_id).unwrap().data;
                    match obj_data.body {
                        ValueHandle::SmallData(_) => {}
                        ValueHandle::LargeData(ref fc) => {
                            let mut nex_builder = (&mut extern_builder).borrow().get(extern_ix);
                            extern_ix += 1;
                            nex_builder.set_id(obj_data.id);
                            nex_builder.set_hash(fc.hash);
                            nex_builder.set_size(fc.size);
                            nex_builder.set_header(&*obj_data.header);
                        }
                    }
                }
            }

            {
                let mut ts_builder = (&mut commit_builder)
                                         .borrow()
                                         .init_deleted(seginfo.live_tombstone_ids.len() as u32);
                let mut ts_ix = 0;
                for ts_id in &seginfo.live_tombstone_ids {
                    ts_builder.set(ts_ix, *ts_id);
                    ts_ix += 1;
                }
            }

            {
                let mut obsseg_builder = (&mut commit_builder)
                                             .borrow()
                                             .init_obsolete_segment_ids(1);
                obsseg_builder.set(0, segment_id);
            }
        }

        let msg_words = capnp::serialize::write_message_to_words(&msg_builder);

        let evac_count = seginfo.live_object_ids.len() + seginfo.live_tombstone_ids.len();
        assert!(evac_count < MAX_SEGMENT_CHANGES as usize);
        let write_seg_id = try!(self.ensure_open_segment(evac_count as u32));
        try!(self.write_block(evac_count as u32, &*msg_words));
        // if we get here, the old segment should be considered *not* part of the image

        // TODO(someday): repeatedly looking up the current segment is sub-optimal
        for moved_obj_id in &seginfo.live_object_ids {
            let mut obj_info = self.objects.get_mut(moved_obj_id).unwrap();
            obj_info.segment_id = write_seg_id;
            let mut wr_seg_info = self.segments.get_mut(&write_seg_id).unwrap();
            wr_seg_info.live_object_ids.insert(*moved_obj_id);
            wr_seg_info.live_size += obj_info.data.live_size();
            self.live_size += obj_info.data.live_size();
        }

        for moved_ts_id in &seginfo.live_tombstone_ids {
            self.tombstones.get_mut(moved_ts_id).unwrap().segment_id = write_seg_id;
            let mut wr_seg_info = self.segments.get_mut(&write_seg_id).unwrap();
            wr_seg_info.live_tombstone_ids.insert(*moved_ts_id);
            wr_seg_info.live_size += TOMBSTONE_SIZE;
            self.live_size += TOMBSTONE_SIZE;
        }

        // these objects will never be seen again, so the corresponding tombstones are dead
        for retired_obj_id in &seginfo.dead_object_ids {
            let old_ts_info = self.tombstones.remove(retired_obj_id).unwrap();
            if old_ts_info.segment_id != segment_id {
                let mut seg_info = self.segments
                                       .get_mut(&old_ts_info.segment_id)
                                       .unwrap();

                seg_info.live_tombstone_ids
                        .remove(retired_obj_id);
                seg_info.live_size -= TOMBSTONE_SIZE;
                self.live_size -= TOMBSTONE_SIZE;
            }
        }

        let dead_seg = self.segments.remove(&segment_id).unwrap();
        self.live_size -= dead_seg.live_size;
        self.on_disk_size -= dead_seg.on_disk_size;
        self.pending_delete_segment.push(segment_id);
        try!(self.flush_deletes());
        Ok(())
    }

    fn flush_deletes(&mut self) -> Result<()> {
        let mut obj_delete_lock = self.dir_control.delete_queue.lock().unwrap();

        while !obj_delete_lock.is_empty() {
            let obj_id = obj_delete_lock[obj_delete_lock.len() - 1];
            let obj_path = self.object_path(obj_id);
            try!(wrap_io(&obj_path, IoType::ObjectDelete, fs::remove_file(&obj_path)));
            obj_delete_lock.pop();
        }

        while !self.pending_delete_segment.is_empty() {
            let seg_id = self.pending_delete_segment[self.pending_delete_segment.len() - 1];
            let seg_path = self.segment_path(seg_id);
            try!(wrap_io(&seg_path, IoType::SegmentDelete, fs::remove_file(&seg_path)));
            self.pending_delete_segment.pop();
        }

        Ok(())
    }
}

// dropping a Connection guarantees no further *writes*, but any existing ValueHandle can
// cause a read.  We could implement a hard disconnect in the future, but it'd be ugly with mmap.

#[cfg(test)]
mod tests;
