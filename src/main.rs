#![allow(dead_code)] // TODO until completion

extern crate byteorder;
extern crate rmp;

use std::io::Error as IoError;
use std::sync::Arc;

mod vector;
mod durability;
mod mvcc;
mod misc;

// TODO provide a central log mechanism
// TODO provide a mechanism for corruption errors to trigger read only
#[derive(Debug,Clone)]
pub enum Error {
    // TODO Corruption errors frequently have less information than they could.  Audit
    Corruption { detail: String },
    IO { cause: Arc<IoError> }, // TODO convince rust people to make IoError Clone
    QuotaExceeded,
    Conflict,
    DatabaseDetached,
}

// TODO this will likely have to go when errors are richer
fn corruption<T>(_detail: T) -> Error where String : From<T> {
    unimplemented!()
    // Error::Corruption { detail: From::from(detail) }
}

macro_rules! corruptiony {
    ( $typ:ty ) => {
        impl From<$typ> for Error {
            fn from(e: $typ) -> Error {
                corruption(format!("{:?}", e))
            }
        }
    }
}

corruptiony! { rmp::encode::ValueWriteError }
corruptiony! { rmp::decode::ValueReadError }
corruptiony! { IoError }

pub type Result<T> = ::std::result::Result<T,Error>;

#[cfg(not(test))]
fn main() {
}
