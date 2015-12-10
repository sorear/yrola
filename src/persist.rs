use std::fs::{self,File,OpenOptions};
use std::path::{Path};
use std::io::{self,ErrorKind};
use std::result;
use std::str::FromStr;
use std::collections::{HashSet};
use fs2::FileExt;

struct Persister;

enum Error {
    BaseIsNotDir,
    BaseStatFailed(io::Error),
    LockOpenFailed(io::Error),
    LockFailed(io::Error),
    JournalOpendirFailed(io::Error),
    JournalReaddirFailed(io::Error),
    DeleteBadJournalFailed(io::Error),
    JournalOpenFailed(io::Error),
}
type Result<T> = result::Result<T, Error>;

impl Persister {
    fn open(root: &Path, readonly: bool) -> Result<Self> {
        match fs::metadata(root) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(Error::BaseIsNotDir);
                }
            },
            Err(err) => {
                if err.kind() == ErrorKind::NotFound && !readonly {
                    unimplemented!();
                } else {
                    return Err(Error::BaseStatFailed(err));
                }
            },
        };

        let locking_handle = try!(OpenOptions::new().write(true).open(root.join("yrola.lock")).map_err(Error::LockOpenFailed));
        try!(locking_handle.lock_exclusive().map_err(Error::LockFailed));

        // read the logs
        let mut bad_journal_names = HashSet::new();
        let mut journal_names = Vec::new();

        for rentry in try!(fs::read_dir(root.join("wals")).map_err(Error::JournalOpendirFailed)) {
            let entry = try!(rentry.map_err(Error::JournalReaddirFailed));
            match entry.file_name().to_str().and_then(|s|
                u64::from_str(s).ok().and_then(|i| if format!("{:06}",i) == s { Some(i) } else { None })) {
                    Some(jnum) => { journal_names.push(jnum); },
                    None => { bad_journal_names.insert(entry.path()); },
                };
        }

        journal_names.sort();
        for jnum in &journal_names {
            let mut jfile = try!(File::open(root.join("wals").join(format!("{:06}", jnum))).map_err(Error::JournalOpenFailed));
            // TODO(now): Read the journal
        }

        unimplemented!();

        if !readonly {
            for badpath in bad_journal_names {
                try!(fs::remove_file(badpath).map_err(Error::DeleteBadJournalFailed));
            }
        }

        unimplemented!()
    }
}
