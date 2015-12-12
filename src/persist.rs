use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{self, ErrorKind, Read};
use std::result;
use std::str::FromStr;
use std::collections::{HashSet, HashMap};
use fs2::FileExt;
use std::sync::Arc;

pub struct Persister;

pub enum Error {
    BaseIsNotDir(PathBuf),
    BaseStatFailed(PathBuf, io::Error),
    LockOpenFailed(PathBuf, io::Error),
    LockFailed(PathBuf, io::Error),
    JournalOpendirFailed(PathBuf, io::Error),
    JournalReaddirFailed(PathBuf, io::Error),
    DeleteBadJournalFailed(PathBuf, io::Error),
    JournalOpenFailed(PathBuf, io::Error),
    JournalReadFailed(PathBuf, io::Error),
}

pub type Result<T> = result::Result<T, Error>;

pub enum ValueHandle {
    SmallData {
        data: (),
    },
    LargeData {
        pointer: (),
    },
}

impl ValueHandle {
    pub fn get_type(&self) -> u16 {
        unimplemented!()
    }

    pub fn read_copy(&self) -> Result<Vec<u8>> {
        unimplemented!()
    }
}

pub struct ValueIterator<'a> {
    guts: &'a (),
}
impl<'a> Iterator for ValueIterator<'a> {
    type Item = ValueHandle;
    fn next(&mut self) -> Option<ValueHandle> {
        unimplemented!()
    }
}

pub struct Transaction {
    guts: (),
}
impl Transaction {
    pub fn add_item(&mut self, type_code: u16, data: Vec<u8>) -> Result<u64> {
        unimplemented!()
    }

    pub fn del_item(&mut self, id: u64) -> Result<()> {
        unimplemented!()
    }

    pub fn get_item(&mut self, id: u64) -> Result<ValueHandle> {
        unimplemented!()
    }

    pub fn list_items<'a>(&'a mut self, type_code: u16) -> Result<ValueIterator<'a>> {
        unimplemented!()
    }

    pub fn commit(self) -> Result<()> {
        unimplemented!()
    }
}

enum ObjectStorage {
    Memory {
        buf: Arc<Vec<u8>>,
    },
    File {
        id: u64,
        size: u64,
        hash: u64,
    },
}

struct ObjectInfo {
    type_code: u16,
    journal_id: u64,
    storage: ObjectStorage,
}

struct TombstoneInfo {
    journal_id: u64,
}

struct SegmentInfo {
    live_object_ids: HashSet<u64>,
    dead_object_ids: HashSet<u64>,
    tombstone_ids: HashSet<u64>,
    on_disk_size: u64,
}

struct JournalInfo {
    segments: HashMap<u64, SegmentInfo>,
    objects: HashMap<u64, ObjectInfo>,
    tombstones: HashMap<u64, TombstoneInfo>,
}

impl Persister {
    pub fn transaction(&self) -> Transaction {
        unimplemented!()
    }

    pub fn open(root: &Path, readonly: bool) -> Result<Self> {
        match fs::metadata(root) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(Error::BaseIsNotDir(root.to_owned()));
                }
            }
            Err(err) => {
                if err.kind() == ErrorKind::NotFound && !readonly {
                    unimplemented!();
                } else {
                    return Err(Error::BaseStatFailed(root.to_owned(), err));
                }
            }
        };

        let locking_path = root.join("yrola.lock");
        let locking_handle = try!(OpenOptions::new()
                                      .write(true)
                                      .open(&locking_path)
                                      .map_err(|e| Error::LockOpenFailed(locking_path.clone(), e)));
        try!(locking_handle.lock_exclusive()
                           .map_err(|e| Error::LockFailed(locking_path.clone(), e)));

        // read the logs
        let mut bad_journal_names = HashSet::new();
        let mut journal_names = Vec::new();

        let wals_path = root.join("wals");
        let wals_iter = try!(fs::read_dir(&wals_path)
                                 .map_err(|e| Error::JournalOpendirFailed(wals_path.clone(), e)));
        for rentry in wals_iter {
            let entry = try!(rentry.map_err(|e| Error::JournalReaddirFailed(wals_path.clone(), e)));
            match entry.file_name().to_str().and_then(|s| {
                u64::from_str(s).ok().and_then(|i| {
                    if format!("{:06}", i) == s {
                        Some(i)
                    } else {
                        None
                    }
                })
            }) {
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

        unimplemented!();

        if !readonly {
            for badpath in bad_journal_names {
                try!(fs::remove_file(&badpath)
                         .map_err(|e| Error::DeleteBadJournalFailed(badpath.clone(), e)));
            }
        }

        unimplemented!()
    }
}
