#![allow(dead_code)] // TODO
extern crate fs2;
extern crate capnp;
extern crate byteorder;

pub mod persist;
pub mod table_mgr;
mod vector;
mod misc;
mod borrow_segments;

mod yrola_capnp {
    include!(concat!(env!("OUT_DIR"), "/yrola_capnp.rs"));
}
