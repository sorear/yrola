use capnp::message::{Reader, ReaderSegments, ReaderOptions};
use capnp::{Error, Result, Word};
use std::borrow::Borrow;
use std::io::{self, Read};
use byteorder::{ByteOrder, LittleEndian};

pub struct BorrowSegments<T> {
    words: T,
    segment_slices: Vec<(usize, usize)>,
}

impl<T: Borrow<[Word]>> ReaderSegments for BorrowSegments<T> {
    fn get_segment(&self, id: u32) -> Option<&[Word]> {
        if id < self.segment_slices.len() as u32 {
            let (a, b) = self.segment_slices[id as usize];
            Some(&self.words.borrow()[a..b])
        } else {
            None
        }
    }
}

/// Reads a serialized message from a slice of words.
pub fn read_message_from_owner<T: Borrow<[Word]>>(data: T,
                                                  options: ReaderOptions)
                                                  -> Result<Reader<BorrowSegments<T>>> {
    let offsets = {
        let words = data.borrow();
        let mut bytes = Word::words_to_bytes(words);
        let (num_words, offsets) = try!(read_segment_table(&mut bytes, options));
        if num_words != words.len() {
            let emsg = Some(format!("Header claimed {} words, but message has {} words",
                                    num_words,
                                    words.len()));
            return Err(Error::new_decode_error("Wrong number of words.", emsg));
        }
        offsets
    };
    Ok(Reader::new(BorrowSegments {
                       words: data,
                       segment_slices: offsets,
                   },
                   options))
}

// copied from capnp-rust

fn read_exact<R: Read>(read: &mut R, buf: &mut [u8]) -> io::Result<()> {
    let mut pos = 0;
    let len = buf.len();
    while pos < len {
        let buf1 = &mut buf[pos..];
        match read.read(buf1) {
            Ok(n) => {
                pos += n;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "Premature EOF"));
                }
            }
            Err(e) => {
                if e.kind() != io::ErrorKind::Interrupted {
                    return Err(e);
                }
                // Retry if we were interrupted.
            }
        }
    }
    Ok(())
}

fn read_segment_table<R>(read: &mut R,
                         options: ReaderOptions)
                         -> Result<(usize, Vec<(usize, usize)>)>
    where R: Read
{

    let mut buf: [u8; 8] = [0; 8];

    // read the first Word, which contains segment_count and the 1st segment length
    try!(read_exact(read, &mut buf));
    let segment_count = <LittleEndian as ByteOrder>::read_u32(&buf[0..4]).wrapping_add(1) as usize;

    if segment_count >= 512 {
        return Err(Error::new_decode_error("Too many segments.",
                                           Some(format!("{}", segment_count))));
    } else if segment_count == 0 {
        return Err(Error::new_decode_error("Too few segments.",
                                           Some(format!("{}", segment_count))));
    }

    let mut segment_slices = Vec::with_capacity(segment_count);
    let mut total_words = <LittleEndian as ByteOrder>::read_u32(&buf[4..8]) as usize;
    segment_slices.push((0, total_words));

    if segment_count > 1 {
        for _ in 0..((segment_count - 1) / 2) {
            // read two segment lengths at a time starting with the second
            // segment through the final full Word
            try!(read_exact(read, &mut buf));
            let segment_len_a =
                <LittleEndian as ByteOrder>::read_u32(&buf[0..4]) as usize;
            let segment_len_b =
                <LittleEndian as ByteOrder>::read_u32(&buf[4..8]) as usize;

            segment_slices.push((total_words, total_words + segment_len_a));
            total_words += segment_len_a;
            segment_slices.push((total_words, total_words + segment_len_b));
            total_words += segment_len_b;
        }

        if segment_count % 2 == 0 {
            // read the final Word containing the last segment length and padding
            try!(read_exact(read, &mut buf));
            let segment_len =
                <LittleEndian as ByteOrder>::read_u32(&buf[0..4]) as usize;
            segment_slices.push((total_words, total_words + segment_len));
            total_words += segment_len;
        }
    }

    // Don't accept a message which the receiver couldn't possibly traverse without hitting the
    // traversal limit. Without this check, a malicious client could transmit a very large segment
    // size to make the receiver allocate excessive space and possibly crash.
    if total_words as u64 > options.traversal_limit_in_words {
        return Err(Error::new_decode_error("Message is too large. To increase the limit on the \
                                            receiving end, see capnp::message::ReaderOptions.",
                                           Some(format!("{}", total_words))));
    }

    Ok((total_words, segment_slices))
}
