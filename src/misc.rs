use capnp;
use std::cmp::Ordering;
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub unsafe trait POD {}

unsafe impl POD for u8 {}
unsafe impl POD for u16 {}
unsafe impl POD for u32 {}
unsafe impl POD for u64 {}
unsafe impl POD for usize {}
unsafe impl POD for i8 {}
unsafe impl POD for i16 {}
unsafe impl POD for i32 {}
unsafe impl POD for i64 {}
unsafe impl POD for isize {}

pub fn downcast_pod<T: POD>(bytes: &[u8]) -> &[T] {
    assert!((bytes.as_ptr() as usize % align_of::<T>()) == 0);
    unsafe {
        // this rounds down on purpose
        from_raw_parts(bytes.as_ptr() as *const T, bytes.len() / size_of::<T>())
    }
}

pub fn downcast_pod_mut<T: POD>(bytes: &mut [u8]) -> &mut [T] {
    assert!((bytes.as_ptr() as usize % align_of::<T>()) == 0);
    unsafe {
        // this rounds down on purpose
        from_raw_parts_mut(bytes.as_ptr() as *mut T, bytes.len() / size_of::<T>())
    }
}

pub fn upcast_pod<T: POD>(things: &[T]) -> &[u8] {
    unsafe { from_raw_parts(things.as_ptr() as *const u8, things.len() * size_of::<T>()) }
}

pub fn upcast_pod_mut<T: POD>(things: &mut [T]) -> &mut [u8] {
    unsafe { from_raw_parts_mut(things.as_ptr() as *mut u8, things.len() * size_of::<T>()) }
}

pub enum MergeRow<V1, V2> {
    Left(V1),
    Right(V2),
    Match(V1, V2),
}

pub struct MergeIter<I1: Iterator, I2: Iterator, CMP> {
    iter1: I1,
    iter2: I2,
    buffer1: Option<I1::Item>,
    buffer2: Option<I2::Item>,
    comparer: CMP,
}

impl<I1, I2, CMP> Iterator for MergeIter<I1, I2, CMP>
    where I1: Iterator,
          I2: Iterator,
          CMP: FnMut(&I1::Item, &I2::Item) -> Ordering
{
    type Item = MergeRow<I1::Item, I2::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let v1 = self.buffer1.take();
        let v2 = self.buffer2.take();

        match (v1, v2) {
            (None, None) => None,
            (Some(x), None) => {
                self.buffer1 = self.iter1.next();
                Some(MergeRow::Left(x))
            }
            (None, Some(x)) => {
                self.buffer2 = self.iter2.next();
                Some(MergeRow::Right(x))
            }
            (Some(x), Some(y)) => {
                match (self.comparer)(&x, &y) {
                    Ordering::Equal => {
                        self.buffer1 = self.iter1.next();
                        self.buffer2 = self.iter2.next();
                        Some(MergeRow::Match(x, y))
                    }
                    Ordering::Less => {
                        self.buffer1 = self.iter1.next();
                        self.buffer2 = Some(y);
                        Some(MergeRow::Left(x))
                    }
                    Ordering::Greater => {
                        self.buffer2 = self.iter2.next();
                        self.buffer1 = Some(x);
                        Some(MergeRow::Right(y))
                    }
                }
            }
        }
    }
}

pub fn merge_iters<I1, I2, CMP>(mut iter1: I1,
                                mut iter2: I2,
                                comparer: CMP)
                                -> MergeIter<I1, I2, CMP>
    where I1: Iterator,
          I2: Iterator,
          CMP: FnMut(&I1::Item, &I2::Item) -> Ordering
{
    let buffer1 = iter1.next();
    let buffer2 = iter2.next();
    MergeIter {
        iter1: iter1,
        iter2: iter2,
        buffer1: buffer1,
        buffer2: buffer2,
        comparer: comparer,
    }
}

// hacked from the slice version in libcore
pub fn binary_search_index<F>(to: usize, mut f: F) -> Result<usize, usize>
    where F: FnMut(usize) -> Ordering
{
    let mut base: usize = 0;
    let mut lim: usize = to;

    while lim != 0 {
        let ix = base + (lim >> 1);
        match f(ix) {
            Ordering::Equal => return Ok(ix),
            Ordering::Less => {
                base = ix + 1;
                lim -= 1;
            }
            Ordering::Greater => (),
        }
        lim >>= 1;
    }
    Err(base)
}

pub struct PrimitiveListIter<'a, T: capnp::private::layout::PrimitiveElement> {
    reader: capnp::primitive_list::Reader<'a, T>,
    index: u32,
}

impl<'a, T: capnp::private::layout::PrimitiveElement> Iterator for PrimitiveListIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.reader.len() {
            None
        } else {
            let value = self.reader.get(self.index);
            self.index += 1;
            Some(value)
        }
    }
}

pub fn prim_list_iter<'a, T>(reader: capnp::primitive_list::Reader<'a, T>)
                             -> PrimitiveListIter<'a, T>
    where T: capnp::private::layout::PrimitiveElement
{
    PrimitiveListIter {
        reader: reader,
        index: 0,
    }
}

pub fn slice_unindex<T>(storage: &[T], slice: &[T]) -> Option<Range<usize>> {
    let esize = size_of::<T>();
    if esize == 0 {
        return Some(0..0);
    }

    let p1 = storage.as_ptr() as usize;
    let p2 = slice.as_ptr() as usize;

    if p2 < p2 {
        return None;
    }
    let offset_b = p2 - p1;
    let offset_e = offset_b / esize;

    if (offset_b % esize) == 0 && offset_e <= storage.len() &&
       slice.len() <= (storage.len() - offset_e) {
        Some(offset_e..(offset_e + slice.len()))
    } else {
        None
    }
}
