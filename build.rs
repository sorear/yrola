extern crate capnpc;

fn main() {
    ::capnpc::compile("src", &["src/yrola.capnp"]).unwrap();
}
