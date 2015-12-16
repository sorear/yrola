use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, ErrorKind, Read};
use std::result;
use std::str::FromStr;
use std::collections::{HashSet, HashMap};
use fs2::FileExt;
use std::sync::Arc;
use std::slice;
use std::ffi::OsStr;
use capnp;
use std::ops::Deref;

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

impl ValueHandle {
    pub fn new(data: &[u8]) -> ValueHandle {
        ValueHandle::SmallData { data: Arc::new(Vec::from(data)) }
    }

    pub fn new_words(_data: &[capnp::Word]) -> ValueHandle {
        unimplemented!()
    }

    // will be retooled when adding mmap support ...
    pub fn data(&self) -> Result<&[u8]> {
        match *self {
            ValueHandle::SmallData { data: ref rc } => Ok(rc.deref()),
            ValueHandle::LargeData { file: ref _fl } => unimplemented!(),
        }
    }

    pub fn data_words(&self) -> Result<&[capnp::Word]> {
        let byteslice = try!(self.data());
        Ok(unsafe {
            assert!(((byteslice.as_ptr() as usize) & 7) == 0); // most allocators will align all memory this much
            slice::from_raw_parts(byteslice.as_ptr() as *const capnp::Word,
                                  byteslice.len() >> 3)
        })
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
    // dead objects are important because they force us to keep the tombstones around.  dead tombstones have no such requirement
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

        let pers = Persister {
            lock_file: lock_file,
            lock_path: lock_path.clone(),
            objs_path: objs_path.clone(),
            wals_path: wals_path.clone(),
            segments: HashMap::new(),
            objects: HashMap::new(),
            tombstones: HashMap::new(),
        };

        // read the logs
        let mut bad_journal_names = HashSet::new();
        let mut journal_names = Vec::new();

        let wals_iter = try!(fs::read_dir(&wals_path)
                                 .map_err(|e| Error::JournalOpendirFailed(wals_path.clone(), e)));
        for rentry in wals_iter {
            let entry = try!(rentry.map_err(|e| Error::JournalReaddirFailed(wals_path.clone(), e)));
            match parse_object_filename(&*entry.file_name()) {
                Some(jnum) => {
                    journal_names.push(jnum);
                }
                None => {
                    bad_journal_names.insert(entry.path());
                }
            };
        }

        journal_names.sort();
        for jnum in &journal_names {
            let jpath = root.join("wals").join(format!("{:06}", jnum));
            let mut jfile = try!(File::open(&jpath)
                                     .map_err(|e| Error::JournalOpenFailed(jpath.clone(), e)));
            let mut jdata = Vec::new();
            try!(jfile.read_to_end(&mut jdata)
                      .map_err(|e| Error::JournalReadFailed(jpath.clone(), e)));
            // TODO(now): Read the journal
            unimplemented!();
        }

        if !readonly {
            for badpath in bad_journal_names {
                try!(fs::remove_file(&badpath)
                         .map_err(|e| Error::DeleteBadJournalFailed(badpath.clone(), e)));
            }

            // TODO(now): Remove old objects
        }

        Ok(pers)
    }
}

#[cfg(test)]
mod tests;
