// The durability mechanism for Yrola
use vector::PersistVector;
use std::collections::HashMap;
use std::sync::{Mutex,Arc};
use misc::ArcMutexGuard;
use misc;
use ::{Result,Error};

pub mod file;

pub trait DurReadSection {
    // Given a file number that was previously added (possibly in a previous session), load it
    // Until the resulting PersistVector is dropped, the file can not be deleted
    fn get_vector(&mut self, filenum: u64) -> Result<PersistVector>;

    // we expect you'll cache this and call it very rarely
    fn get_journal_items(&mut self, jtype: u8) -> Result<Vec<(u64, Vec<u8>)>>;
}

pub trait DurPrepareSection : DurReadSection {
    // file may not actually be written here, regardless a ref will be taken
    // TODO: we may want richer file identifications
    fn save_vector(&mut self, data: &PersistVector) -> Result<u64>;

    fn del_vector(&mut self, filenum: u64) -> Result<()>;

    // TODO we may want types, ability to store fancy metadata like version and endianness
    fn add_journal_item(&mut self, jtype: u8, data: &[u8]) -> Result<u64>;

    fn del_journal_item(&mut self, jinum: u64) -> Result<()>;

    // TODO Rust does not subtype here
    fn is_read(&mut self) -> &mut DurReadSection;
}

// TODO: Refactor synchronization here.  We don't want to entirely duplicate the locking responsibility.
pub trait DurProvider {
    fn prepare_section(&self) -> Box<DurPrepareSection>;
    fn read_section(&self) -> Box<DurReadSection>;
    fn low_commit(&self) -> Result<()> {
        Ok(())
    }
    fn detach(&self) -> Result<()> {
        Ok(())
    }
}

struct NoneDurabilityData {
    files: HashMap<u64, PersistVector>,
    items: HashMap<u64, (u8, Vec<u8>)>,
    next_file: u64,
    next_item: u64,
}
#[derive(Clone)]
pub struct NoneDurProvider(Arc<Mutex<NoneDurabilityData>>);

impl DurReadSection for ArcMutexGuard<NoneDurabilityData> {
    fn get_vector(&mut self, filenum: u64) -> Result<PersistVector> {
        self.files.get(&filenum).cloned()
            .ok_or(Error::Corruption { detail: format!("Vector {} not found", filenum) })
    }

    fn get_journal_items(&mut self, jtype: u8) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut list : Vec<(u64, Vec<u8>)> = self.items.iter().filter_map(|(fno,tydata)|
            if tydata.0 == jtype { Some((*fno,tydata.1.clone())) } else { None }).collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(list)
    }
}

impl DurPrepareSection for ArcMutexGuard<NoneDurabilityData> {
    fn save_vector(&mut self, data: &PersistVector) -> Result<u64> {
        self.next_file += 1;
        let filenum = self.next_file;
        self.files.insert(filenum, data.clone());
        Ok(filenum)
    }

    fn is_read(&mut self) -> &mut DurReadSection { self }

    fn del_vector(&mut self, filenum: u64) -> Result<()> {
        self.files.remove(&filenum).map(|_| ()).ok_or_else(|| ::corruption("no such vector"))
    }

    fn add_journal_item(&mut self, jtype: u8, data: &[u8]) -> Result<u64> {
        self.next_item += 1;
        let num = self.next_item;
        self.items.insert(num, (jtype, Vec::from(data)));
        Ok(self.next_item)
    }

    fn del_journal_item(&mut self, jinum: u64) -> Result<()> {
        self.items.remove(&jinum);
        Ok(())
    }
}

impl NoneDurProvider {
    pub fn new() -> Self {
        NoneDurProvider(Arc::new(Mutex::new(NoneDurabilityData {
            files: HashMap::new(),
            items: HashMap::new(),
            next_file: 0,
            next_item: 0,
        })))
    }
}

impl DurProvider for NoneDurProvider {
    fn prepare_section(&self) -> Box<DurPrepareSection> {
        Box::new(misc::lock_arc_mutex(&self.0).unwrap())
    }
    fn read_section(&self) -> Box<DurReadSection> {
        Box::new(misc::lock_arc_mutex(&self.0).unwrap())
    }
}
