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

use persist::{ValueHandle, ValuePin};
use std::sync::Arc;
use std::ops::Range;
use std::result;
use std::borrow::Cow;
use byteorder::{BigEndian, ByteOrder};
use misc;

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
            Representation::Fixed(len) => {
                Some(Cow::Borrowed(&self.spans[0].data()[index * len..(index + 1) * len]))
            }
            Representation::Blob32 => {
                let offsets_b = self.spans[0].data();
                let bytes = self.spans[1].data();
                let offset = BigEndian::read_u32(&offsets_b[index * 4..(index + 1) * 4]) as usize;
                let offset_next =
                    BigEndian::read_u32(&offsets_b[(index + 1) * 4..(index + 2) * 4]) as usize;
                if (offset & 1) == 1 {
                    None
                } else {
                    Some(Cow::Borrowed(&bytes[(offset >> 1)..(offset_next >> 1)]))
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
        ColumnIter {
            col: self,
            index: 0,
        }
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

    fn push_any_copy<'a, 'b>(&'a mut self, data: Option<Cow<'b, [u8]>>) {
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

        let frag = Fragment {
            repr: Representation::Blob32,
            length: els,
            spans: vec![span0, span1],
        };
        Column(Arc::new(ColData { fragments: vec![frag] }))
    }
}

// TODO(soon): details, factoring
pub enum Error {
    WrongLength,
}
pub type Result<T> = result::Result<T, Error>;

pub fn parse(_handle: ValueHandle, _range: Range<usize>) -> Result<Column> {
    unimplemented!()
}

pub fn serialized_size(_col: &Column) -> u64 {
    unimplemented!()
}

pub fn sorted_semijoin(key_col: &Column,
                       match_col: &Column,
                       data_cols: &[&Column],
                       antijoin: bool)
                       -> Result<Vec<Column>> {
    let mut copiers = Vec::new();
    for dcol in data_cols {
        if dcol.len() != key_col.len() {
            return Err(Error::WrongLength);
        }
        copiers.push((dcol.iter(), ColumnBuilder::new()));
    }

    for alignment in misc::merge_iters(key_col.iter(), match_col.iter(), |x, y| x.cmp(&y)) {
        use misc::MergeRow::*;
        let (scan, copy) = match alignment {
            Left(_) => (true, antijoin),
            Match(_, _) => (true, !antijoin),
            Right(_) => (false, false),
        };
        if scan {
            for &mut (ref mut iter, ref mut blder) in &mut copiers {
                let val = iter.next().expect("iteration length mismatch");
                // should have been caught by len() check
                if copy {
                    blder.push_any_copy(val);
                }
            }
        }
    }

    Ok(copiers.into_iter().map(|(_, b)| b.close()).collect())
}

pub fn sorted_merge(key_cols: (&Column, &Column),
                    data_cols: &[(&Column, &Column)])
                    -> Result<Vec<Column>> {
    let mut per_data_col = Vec::new();
    for &(dcl, dcr) in data_cols {
        if dcl.len() != key_cols.0.len() || dcr.len() != key_cols.1.len() {
            return Err(Error::WrongLength);
        }
        per_data_col.push((dcl.iter(), dcr.iter(), ColumnBuilder::new()));
    }

    for alignment in misc::merge_iters(key_cols.0.iter(), key_cols.1.iter(), |x, y| x.cmp(&y)) {
        use misc::MergeRow::*;
        let (copy_l, copy_r) = match alignment {
            Match(_, _) => (true, true),
            Left(_) => (true, false),
            Right(_) => (false, true),
        };
        for &mut (ref mut iterl, ref mut iterr, ref mut blder) in &mut per_data_col {
            if copy_l {
                blder.push_any_copy(iterl.next().expect("iteration length mismatch"));
            }
            if copy_r {
                blder.push_any_copy(iterr.next().expect("iteration length mismatch"));
            }
        }
    }

    Ok(per_data_col.into_iter().map(|(_, _, b)| b.close()).collect())
}
