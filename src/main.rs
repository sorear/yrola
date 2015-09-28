#![allow(dead_code)] // TODO until completion

extern crate byteorder;
extern crate rmp;

mod vector;
mod durability;
mod mvcc;

// TODO provide a central log mechanism
// TODO provide a mechanism for corruption errors to trigger read only
#[derive(Debug)]
pub enum Error {
    // TODO Corruption errors frequently have less information than they could.  Audit
    Corruption { detail: String },
    IO { cause: ::std::io::Error },
    QuotaExceeded,
    Conflict,
    DatabaseDetached,
}

// TODO this will likely have to go when errors are richer
fn corruption<T>(detail: T) -> Error where String : From<T> {
    Error::Corruption { detail: From::from(detail) }
}

impl From<rmp::decode::ValueReadError> for Error {
    #[allow(unused_variables)]
    fn from(e: rmp::decode::ValueReadError) -> Error {
        unimplemented!()
    }
}

pub type Result<T> = ::std::result::Result<T,Error>;

#[cfg(not(test))]
fn main() {
}
