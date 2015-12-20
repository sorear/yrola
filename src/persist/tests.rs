extern crate tempdir;
use self::tempdir::TempDir;
use std::fs::File;
use super::*;

#[test]
fn open_not_dir() {
    let dir = TempDir::new("yrola_test").unwrap();
    File::create(dir.path().join("blah")).unwrap();
    match Connection::open(&*dir.path().join("blah"), false) {
        Err(Error::Corrupt(Corruption::BaseNotDirectory, _)) => (),
        _ => panic!(),
    }
}

#[test]
fn open_not_exists() {
    let dir = TempDir::new("yrola_test").unwrap();
    match Connection::open(&*dir.path().join("blah"), true) {
        Err(Error::Io(IoType::BaseStat, _, _)) => (),
        _ => panic!(),
    }
}

#[test]
fn open_create() {
    let dir = TempDir::new("yrola_test").unwrap();
    let _pst = Connection::open(&*dir.path().join("blah"), false).unwrap();
}

fn mkvector(count: usize, fill: u8) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0 .. count {
        out.push(fill);
    }
    out
}

#[test]
fn basic_permanence() {
    fn check_item(id: u64, item: &ItemHandle) {
        assert_eq!(id, item.id());
        let pin = item.body().pin().unwrap();

        match id {
            1 => {
                assert_eq!(item.header(), &[100u8,0,0,0]);
                assert_eq!(pin.data(), &*mkvector(11, 0x45));
            }
            2 => {
                assert_eq!(item.header(), &[200u8,0,0,0]);
                assert_eq!(pin.data(), &*mkvector(1 << 20, 0x54));
            }
            _ => {
                panic!("shouldn't exist");
            }
        }
    }

    fn check_content(con: &mut Connection) {
        let mut tx = con.transaction();
        assert_eq!(tx.list_items().count(), 2);
        check_item(1, tx.get_item(1).unwrap());
        check_item(2, tx.get_item(2).unwrap());
        for item in tx.list_items() {
            check_item(item.id(), item);
        }
    }

    let dir = TempDir::new("yrola_test").unwrap();

    {
        let mut con = Connection::open(&*dir.path().join("blah"), false).unwrap();
        assert_eq!(con.transaction().list_items().count(), 0);
    }

    {
        let mut con = Connection::open(&*dir.path().join("blah"), false).unwrap();
        assert_eq!(con.transaction().list_items().count(), 0);
        {
            let mut tx = con.transaction();
            assert_eq!(tx.add_item(vec![100,0,0,0], mkvector(11, 0x45)).unwrap(), 1);
            assert_eq!(tx.add_item(vec![200,0,0,0], mkvector(1 << 20, 0x54)).unwrap(), 2);
            tx.commit().unwrap();
        }
        check_content(&mut con);
    }

    {
        let mut con = Connection::open(&*dir.path().join("blah"), false).unwrap();
        check_content(&mut con);
    }

    {
        let mut con = Connection::open(&*dir.path().join("blah"), false).unwrap();
        check_content(&mut con);
    }
}
