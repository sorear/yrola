#![macro_use]

use std::sync::{Arc,Mutex,MutexGuard,LockResult,PoisonError};
use std::ops::{Deref,DerefMut};
use std::mem;

use byteorder::{BigEndian,ByteOrder};

// Yrola is uniformly big-endian, but representations are used to cheat this
pub fn u64_to_bytes(x: u64) -> Vec<u8> {
    let mut buf = [0; 8];
    BigEndian::write_u64(&mut buf, x);
    buf.to_vec()
}

pub fn bytes_to_u64(x: &Vec<u8>) -> Option<u64> {
    if x.len() != 8 { return None; }
    Some(BigEndian::read_u64(&x[..]))
}

pub struct ArcMutexGuard<T: ?Sized + 'static> {
    mutex: Arc<Mutex<T>>,
    guard: MutexGuard<'static,T>,
}

impl<T: ?Sized> Deref for ArcMutexGuard<T> {
    type Target = T;
    fn deref(&self) -> &T { self.guard.deref() }
}

impl<T: ?Sized> DerefMut for ArcMutexGuard<T> {
    fn deref_mut(&mut self) -> &mut T { self.guard.deref_mut() }
}

pub fn lock_arc_mutex<T: 'static + ?Sized>(mutex: &Arc<Mutex<T>>) -> LockResult<ArcMutexGuard<T>> {
    // We transmute away the lifetime on the mutex guard, which cannot outlive the mutex due to bundling
    match mutex.lock() {
        Ok(guard) => Ok(ArcMutexGuard {
            mutex: mutex.clone(),
            guard: unsafe { mem::transmute(guard) },
        }),
        Err(pguard) => Err(PoisonError::new(ArcMutexGuard {
            mutex: mutex.clone(),
            guard: unsafe { mem::transmute(pguard.into_inner()) },
        })),
    }
}

pub fn ptr_eq<T>(a: *const T, b: *const T) -> bool { a == b }
macro_rules! eq_rcwrapper {
    ($ty:ty) => {
        impl PartialEq for $ty {
            fn eq(&self, other: &$ty) -> bool { ::misc::ptr_eq(&*self.0, &*other.0) }
        }
    }
}
