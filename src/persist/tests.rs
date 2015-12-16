extern crate tempdir;
use self::tempdir::TempDir;
use std::fs::File;
use super::*;

#[test]
fn open_not_dir() {
    let dir = TempDir::new("yrola_test").unwrap();
    File::create(dir.path().join("blah")).unwrap();
    match Persister::open(&*dir.path().join("blah"), false) {
        Err(Error::BaseIsNotDir(_)) => (),
        _ => panic!(),
    }
}

#[test]
fn open_not_exists() {
    let dir = TempDir::new("yrola_test").unwrap();
    match Persister::open(&*dir.path().join("blah"), true) {
        Err(Error::BaseStatFailed(_, _)) => (),
        _ => panic!(),
    }
}

#[test]
fn open_create() {
    let dir = TempDir::new("yrola_test").unwrap();
    let _pst = Persister::open(&*dir.path().join("blah"), false).unwrap();
}
