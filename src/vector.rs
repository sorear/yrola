// Yrola vector kernel.  While something identifiably derived from this will be found in the
// production version, there are a few places we know it will change:
//
// # More kinds of vector, e.g. zero copy
// # More detailed accounting of space and time usage
// # More data types (maybe) and more representations (definitely)
// # Future vectors *will* be densely stored
//
// Describing a vector requires a global type/representation, a list of segment lengths, and some
// additional data for each segment...

use persist::{ValueHandle,ValuePin};
use std::sync::Arc;
use std::ops::Range;
use std::result;
use std::borrow::Cow;
use byteorder::{BigEndian, ByteOrder};

// bucket for uninterpreted bytes
enum Span {
    Persist {
        handle: ValuePin,
        range: Range<usize>,
    },
    Temporary {
        storage: Vec<u8>,
    },
}

impl Span {
    fn data(&self) -> &[u8] {
        match *self {
            Span::Persist { ref handle, ref range } => &handle.data()[range.clone()],
            Span::Temporary { ref storage } => &*storage,
        }
    }
}

enum Representation {
    Fixed(usize),
    Blob32,
}

// in the future we'll want to tag these with topology information
struct Fragment {
    repr: Representation,
    length: usize,
    spans: Vec<Span>,
}

impl Fragment {
    fn index_any(&self, index: usize) -> Option<Cow<[u8]>> {
        match self.repr {
            Representation::Fixed(len) => Some(Cow::Borrowed(&self.spans[0].data()[index * len .. (index + 1) * len ])),
            Representation::Blob32 => {
                let offsets_b = self.spans[0].data();
                let bytes = self.spans[1].data();
                let offset = BigEndian::read_u32(&offsets_b[index*4 .. (index+1)*4]) as usize;
                let offset_next = BigEndian::read_u32(&offsets_b[(index+1)*4 .. (index+2)*4]) as usize;
                if (offset & 1) == 1 {
                    None
                } else {
                    Some(Cow::Borrowed(&bytes[(offset >> 1) .. (offset_next >> 1)]))
                }
            }
        }
    }
}

// future idea: meta-fragments which delegate the actual fragment data to another node (more
// useful when we have distribution, interesting interactions with pipelining)

// type, for now, mostly controls
enum DataType {
    Blob,
}

struct ColData {
    fragments: Vec<Fragment>,
}

pub struct Column(Arc<ColData>);

// TODO(soon): ColumnIter and ColumnBuilder are a mess
impl Column {
    fn index_any(&self, mut index: usize) -> Option<Cow<[u8]>> {
        let mut frag_ix = 0;
        while index >= self.0.fragments[frag_ix].length {
            index -= self.0.fragments[frag_ix].length;
            frag_ix += 1;
        }
        self.0.fragments[frag_ix].index_any(index)
    }

    fn len(&self) -> usize {
        let mut sum = 0;
        for frag in &self.0.fragments {
            sum += frag.length;
        }
        sum
    }

    fn iter(&self) -> ColumnIter {
        ColumnIter { col: self, index: 0 }
    }
}

impl<'a> IntoIterator for &'a Column {
    type IntoIter = ColumnIter<'a>;
    type Item = Option<Cow<'a,[u8]>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ColumnIter<'a> {
    col: &'a Column,
    index: usize,
}

impl<'a> Iterator for ColumnIter<'a> {
    type Item = Option<Cow<'a, [u8]>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.col.len() {
            None
        } else {
            let ix = self.index;
            self.index += 1;
            Some(self.col.index_any(ix))
        }
    }
}

struct ColumnBuilder {
    spans: Vec<Vec<u8>>,
}

impl ColumnBuilder {
    fn new() -> Self {
        ColumnBuilder { spans: vec![Vec::new(), Vec::new()] }
    }

    fn push_any_copy<'a,'b>(&'a mut self, data: Option<Cow<'b,[u8]>>) {
        let mut lenb = [0u8; 4];
        let mut coded_len = (self.spans[1].len() as u32) << 1;

        match data {
            Some(dptr) => {
                self.spans[1].extend(&*dptr);
                assert!(self.spans[1].len() < (1 << 31));
            }
            None => {
                coded_len += 1;
            }
        }

        BigEndian::write_u32(&mut lenb, coded_len);
        self.spans[0].extend(&lenb);
    }

    fn close(mut self) -> Column {
        let mut lenb = [0u8; 4];
        BigEndian::write_u32(&mut lenb, (self.spans[1].len() as u32) << 1);
        let els = self.spans[0].len();
        self.spans[0].extend(&lenb);

        let span0 = Span::Temporary { storage: self.spans.remove(0) };
        let span1 = Span::Temporary { storage: self.spans.remove(0) };

        let frag = Fragment { repr: Representation::Blob32, length: els, spans: vec![span0, span1] };
        Column(Arc::new(ColData { fragments: vec![frag] }))
    }
}

pub enum Error {}
pub type Result<T> = result::Result<T, Error>;

pub fn parse(_handle: ValueHandle, _range: Range<usize>) -> Result<Column> {
    unimplemented!()
}

pub fn serialized_size(_col: &Column) -> u64 {
    unimplemented!()
}
