// The durability mechanism for Yrola
use vector::PersistVector;
use std::collections::HashMap;
use std::sync::Mutex;
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
}

// TODO: Refactor synchronization here.  We don't want to entirely duplicate the locking responsibility.
pub trait DurProvider {
    fn prepare_section<'s>(&'s self, fnc: Box<FnMut(&mut DurPrepareSection) -> Result<()> + 's>) -> Result<()>;
    fn read_section<'s>(&'s self, fnc: Box<FnMut(&mut DurReadSection) -> Result<()> + 's>) -> Result<()>;
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
pub struct NoneDurProvider(Mutex<NoneDurabilityData>);

impl DurReadSection for NoneDurabilityData {
    fn get_vector(&mut self, filenum: u64) -> Result<PersistVector> {
        self.files.get(&filenum).cloned()
            .ok_or(Error::Corruption { detail: format!("Vector {} not found", filenum) })
    }

    fn get_journal_items(&mut self, jtype: u8) -> Result<Vec<(u64, Vec<u8>)>> {
        Ok(self.items.iter().filter_map(|(fno,tydata)|
            if tydata.0 == jtype { Some((*fno,tydata.1.clone())) } else { None }).collect())
    }
}

impl DurPrepareSection for NoneDurabilityData {
    fn save_vector(&mut self, data: &PersistVector) -> Result<u64> {
        self.next_file += 1;
        let filenum = self.next_file;
        self.files.insert(filenum, data.clone());
        Ok(filenum)
    }

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
        NoneDurProvider(Mutex::new(NoneDurabilityData {
            files: HashMap::new(),
            items: HashMap::new(),
            next_file: 0,
            next_item: 0,
        }))
    }
}

impl DurProvider for NoneDurProvider {
    fn prepare_section<'s>(&'s self, mut f: Box<FnMut(&mut DurPrepareSection) -> Result<()> + 's>) -> Result<()> {
        let mut lock = self.0.lock().unwrap();
        f(&mut *lock)
    }
    fn read_section<'s>(&self, mut f: Box<FnMut(&mut DurReadSection) -> Result<()> + 's>) -> Result<()> {
        let mut lock = self.0.lock().unwrap();
        f(&mut *lock)
    }
}
