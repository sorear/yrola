#![allow(dead_code)] // TODO(soon): remove
extern crate fs2;
extern crate capnp;
extern crate byteorder;

mod persist;
mod table_mgr;
mod borrow_segments;

mod yrola_capnp {
    include!(concat!(env!("OUT_DIR"), "/yrola_capnp.rs"));
}
