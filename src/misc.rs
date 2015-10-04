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
