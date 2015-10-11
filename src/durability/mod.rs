// The durability mechanism for Yrola
use vector::PersistVector;
use std::collections::HashMap;
use std::sync::{Mutex,Arc};
use std::any::Any;
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

    // may lead to prewrites
    fn alloc_vector_id(&mut self, data: &PersistVector) -> Result<Box<DurPrewriteVector>>;

    fn next_journal_id(&mut self) -> u64;
}

pub trait DurPrewriteVector {
    fn id(&self) -> u64;
    fn data(&self) -> &PersistVector;
    fn dynamic(self: Box<Self>) -> Box<Any>;
}

pub trait DurPrepareSection {
    // file may not actually be written here, regardless a ref will be taken
    // TODO: we may want richer file identifications
    fn save_vector(&mut self, cookie: Box<DurPrewriteVector>) -> Result<()>;

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

struct NonePrewrite {
    id: u64,
    data: PersistVector,
    provider: NoneDurProvider,
}

impl DurPrewriteVector for NonePrewrite {
    fn id(&self) -> u64 { self.id }
    fn data(&self) -> &PersistVector { &self.data }
    fn dynamic(self: Box<Self>) -> Box<Any> { self }
}

struct NoneDurabilityData {
    files: HashMap<u64, PersistVector>,
    items: HashMap<u64, (u8, Vec<u8>)>,
    next_file: u64,
    next_item: u64,
}
#[derive(Clone)]
pub struct NoneDurProvider(Arc<Mutex<NoneDurabilityData>>);
eq_rcwrapper!(NoneDurProvider);
struct NoneSection(ArcMutexGuard<NoneDurabilityData>, NoneDurProvider);

impl DurReadSection for NoneSection {
    fn get_vector(&mut self, filenum: u64) -> Result<PersistVector> {
        self.0.files.get(&filenum).cloned()
            .ok_or(Error::Corruption { detail: format!("Vector {} not found", filenum) })
    }

    fn get_journal_items(&mut self, jtype: u8) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut list : Vec<(u64, Vec<u8>)> = self.0.items.iter().filter_map(|(fno,tydata)|
            if tydata.0 == jtype { Some((*fno,tydata.1.clone())) } else { None }).collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(list)
    }

    fn alloc_vector_id(&mut self, data: &PersistVector) -> Result<Box<DurPrewriteVector>> {
        self.0.next_file += 1;
        Ok(Box::new(NonePrewrite { id: self.0.next_file, data: data.clone(), provider: self.1.clone() }))
    }

    fn next_journal_id(&mut self) -> u64 {
        self.0.next_item + 1
    }
}

impl DurPrepareSection for NoneSection {
    fn save_vector(&mut self, cookie: Box<DurPrewriteVector>) -> Result<()> {
        let prewrite : Box<NonePrewrite> = cookie.dynamic().downcast().unwrap(); // will panic if you passed a bogus cookie
        assert!(prewrite.provider == self.1);
        self.0.files.insert(prewrite.id, prewrite.data);
        Ok(())
    }

    fn is_read(&mut self) -> &mut DurReadSection { self }

    fn del_vector(&mut self, filenum: u64) -> Result<()> {
        self.0.files.remove(&filenum).map(|_| ()).ok_or_else(|| ::corruption("no such vector"))
    }

    fn add_journal_item(&mut self, jtype: u8, data: &[u8]) -> Result<u64> {
        self.0.next_item += 1;
        let num = self.0.next_item;
        self.0.items.insert(num, (jtype, Vec::from(data)));
        Ok(self.0.next_item)
    }

    fn del_journal_item(&mut self, jinum: u64) -> Result<()> {
        self.0.items.remove(&jinum);
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
        Box::new(NoneSection(misc::lock_arc_mutex(&self.0).unwrap(), self.clone()))
    }
    fn read_section(&self) -> Box<DurReadSection> {
        Box::new(NoneSection(misc::lock_arc_mutex(&self.0).unwrap(), self.clone()))
    }
}
