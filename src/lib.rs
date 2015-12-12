#![allow(dead_code)] // TODO(soon): remove
extern crate fs2;
extern crate capnp;

mod persist;

pub mod yrola_capnp {
    include!(concat!(env!("OUT_DIR"), "/yrola_capnp.rs"));
}
