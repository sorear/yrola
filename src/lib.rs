#![allow(dead_code)] // TODO(soon): remove
extern crate fs2;
extern crate capnp;

mod persist;
mod table_mgr;

mod yrola_capnp {
    include!(concat!(env!("OUT_DIR"), "/yrola_capnp.rs"));
}
