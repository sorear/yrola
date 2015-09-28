//! The durability mechanism for Yrola
use vector::PersistVector;
use std::collections::HashMap;
use ::{Result,Error};

/// For now this requires wholly external synchronization.  We might later have a way to have
/// unsynchronized get_vector.
pub trait DurabilityProvider {
    /// Given a file number that was previously added (possibly in a previous session), load it
    // TODO add a "prevents delete lock" and use it to shrink the critical section on this
    // TODO add
    fn get_vector(&self, filenum: u64) -> Result<PersistVector>;

    fn save_vector(&mut self, filenum: u64, data: &PersistVector);

    fn del_vector(&mut self, filenum: u64);

    fn alloc_filenum(&mut self) -> u64;

    // TODO we may want types, ability to store fancy metadata like version and endianness
    fn add_journal_item(&mut self, data: &[u8]);

    fn del_journal_item(&mut self, jinum: u64);

    fn get_journal_items(&self) -> HashMap<u64, Vec<u8>>;

    fn prepare_open(&mut self);

    fn prepare_close(&mut self);

    // TODO keeping this in the prepare lock prevents group commit!
    fn commit(&mut self);
}

pub struct NoneDurability {
    files: HashMap<u64, PersistVector>,
    items: HashMap<u64, Vec<u8>>,
    next_file: u64,
    next_item: u64,
}

impl NoneDurability {
    pub fn new() -> Self {
        NoneDurability {
            files: HashMap::new(),
            items: HashMap::new(),
            next_file: 0,
            next_item: 0,
        }
    }
}

impl DurabilityProvider for NoneDurability {
    fn get_vector(&self, filenum: u64) -> Result<PersistVector> {
        self.files.get(&filenum).cloned()
            .ok_or(Error::Corruption { detail: format!("Vector {} not found", filenum) })
    }

    fn save_vector(&mut self, filenum: u64, data: &PersistVector) {
        self.files.insert(filenum, data.clone());
    }

    fn del_vector(&mut self, filenum: u64) {
        self.files.remove(&filenum);
    }

    fn alloc_filenum(&mut self) -> u64 {
        self.next_file += 1;
        self.next_file
    }

    fn add_journal_item(&mut self, data: &[u8]) {
        self.next_item += 1;
        let num = self.next_item;
        self.items.insert(num, Vec::from(data));
    }

    fn del_journal_item(&mut self, jinum: u64) {
        self.items.remove(&jinum);
    }

    fn get_journal_items(&self) -> HashMap<u64, Vec<u8>> {
        self.items.clone()
    }

    fn prepare_open(&mut self) { }
    fn prepare_close(&mut self) { }
    fn commit(&mut self) { }
}
