use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::mem::{align_of, size_of};

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
    unsafe {
        from_raw_parts(things.as_ptr() as *const u8, things.len() * size_of::<T>())
    }
}

pub fn upcast_pod_mut<T: POD>(things: &mut [T]) -> &mut [u8] {
    unsafe {
        from_raw_parts_mut(things.as_ptr() as *mut u8, things.len() * size_of::<T>())
    }
}
