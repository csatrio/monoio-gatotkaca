//! Storage for the data of a dropped in-flight op, kept until the kernel is done with it.
//!
//! The data is stored inline in the slab entry when it fits ([`INLINE_SIZE`] bytes, alignment
//! at most [`INLINE_ALIGN`]): most op data is a `SharedFd`, a buffer handle and a few integers,
//! so dropping an in-flight op does not allocate. Larger data is boxed and the box is stored
//! inline instead.

use std::{
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ptr,
};

/// Bytes of op data stored inline. Covers `Recv`/`Send`/`Read`/`Write` with a `Vec<u8>`,
/// `Box<[u8]>`, `Bytes` or `BytesMut` buffer and the fixed-buffer ops.
///
/// This is what fits a one-line op slot: 64 bytes less the drop function and the word that
/// holds the state's tag and the slot's meta (`lifecycle::tests::slot_is_one_cache_line`).
/// One byte more costs every op slot a second cache line.
pub(crate) const INLINE_SIZE: usize = 48;

/// Largest alignment of inline data (that of the storage).
pub(crate) const INLINE_ALIGN: usize = mem::align_of::<Storage>();

/// Raw inline storage. The alignment of the struct is the alignment the data gets.
#[repr(C, align(8))]
struct Storage([MaybeUninit<u8>; INLINE_SIZE]);

/// Type-erased owned value, the inline counterpart of `Box<dyn Any>`.
///
/// Holds either a `T` (when it fits the storage) or a `Box<T>`, and drops it exactly once, when
/// this value is dropped. It is never read otherwise.
pub(crate) struct IgnoredData {
    storage: Storage,
    /// Drops the value in `storage` in place. Written together with `storage` and never
    /// changed, so it always matches the value stored.
    drop_fn: unsafe fn(*mut u8),
    /// The erased value may be neither `Send` nor `Sync` (like `Box<dyn Any>`).
    _not_send: PhantomData<*mut ()>,
}

/// Drop the `T` at `p` in place.
///
/// # Safety
/// `p` must point to a valid, properly aligned `T` that is not used afterwards.
unsafe fn drop_erased<T>(p: *mut u8) {
    // SAFETY: guaranteed by the caller.
    unsafe { ptr::drop_in_place(p.cast::<T>()) }
}

impl IgnoredData {
    /// Whether a `T` is stored inline (else it is boxed).
    #[inline]
    pub(crate) const fn fits<T>() -> bool {
        mem::size_of::<T>() <= INLINE_SIZE && mem::align_of::<T>() <= INLINE_ALIGN
    }

    /// Take ownership of `value`: inline if it fits, else in a `Box<T>` stored inline.
    #[inline]
    pub(crate) fn new<T: 'static>(value: T) -> Self {
        if Self::fits::<T>() {
            // SAFETY: `T` fits the storage in size and alignment (checked above).
            unsafe { Self::new_inline(value) }
        } else {
            // A `Box<T>` is one thin pointer, which always fits.
            // SAFETY: `Box<T>` (for a sized `T`) has the size and alignment of a pointer, at
            // most 8 bytes, and `INLINE_SIZE >= 8`, `INLINE_ALIGN == 8`.
            unsafe { Self::new_inline(Box::new(value)) }
        }
    }

    /// An empty value (stands for op data that was already taken). Never allocates.
    #[inline]
    pub(crate) fn empty() -> Self {
        // SAFETY: `()` is a ZST, which fits.
        unsafe { Self::new_inline(()) }
    }

    /// # Safety
    /// `size_of::<T>() <= INLINE_SIZE` and `align_of::<T>() <= INLINE_ALIGN`.
    #[inline]
    unsafe fn new_inline<T>(value: T) -> Self {
        debug_assert!(Self::fits::<T>());
        let mut storage = Storage([MaybeUninit::uninit(); INLINE_SIZE]);
        // SAFETY: the storage is large enough and aligned for `T` (caller contract, and the
        // storage is `align(INLINE_ALIGN)`), and it is uninitialized, so nothing is
        // overwritten. Ownership of `value` moves into the storage; from here on `drop_fn`
        // (which matches `T`) is the only thing that drops it.
        unsafe { ptr::write(storage.0.as_mut_ptr().cast::<T>(), value) };
        IgnoredData {
            storage,
            drop_fn: drop_erased::<T>,
            _not_send: PhantomData,
        }
    }
}

impl Drop for IgnoredData {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `storage` holds the valid value `new_inline` wrote, of the type `drop_fn` was
        // made for. Moving an `IgnoredData` moves the value bitwise along with it (no Rust value
        // may rely on its own address unless pinned, and op data is never pinned: `Op` moves
        // it too), so it is still valid and aligned here (the storage keeps its alignment
        // wherever it lives). `drop` runs once, so the value is dropped exactly once and never
        // used again.
        unsafe { (self.drop_fn)(self.storage.0.as_mut_ptr().cast::<u8>()) }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, rc::Rc};

    use super::*;

    struct Tracked<const N: usize>(Rc<Cell<usize>>, [u8; N]);

    impl<const N: usize> Drop for Tracked<N> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn fits() {
        assert!(IgnoredData::fits::<()>());
        assert!(IgnoredData::fits::<Vec<u8>>());
        assert!(IgnoredData::fits::<[u64; 6]>());
        assert!(!IgnoredData::fits::<[u64; 7]>());
        #[repr(align(16))]
        struct Aligned;
        assert!(!IgnoredData::fits::<Aligned>());
        assert_eq!(INLINE_ALIGN, 8);
    }

    #[test]
    fn drops_once_inline_and_boxed() {
        let count = Rc::new(Cell::new(0));
        let small = IgnoredData::new(Tracked::<8>(count.clone(), [1; 8]));
        let large = IgnoredData::new(Tracked::<256>(count.clone(), [2; 256]));
        // Moving them around neither drops nor duplicates the values.
        let moved = vec![small, large];
        assert_eq!(count.get(), 0);
        drop(moved);
        assert_eq!(count.get(), 2);
        assert_eq!(Rc::strong_count(&count), 1);
    }

    #[test]
    fn aligned_value_is_aligned() {
        #[repr(align(8))]
        struct A(u64, Rc<Cell<usize>>);
        impl Drop for A {
            fn drop(&mut self) {
                assert_eq!(self as *const A as usize % 8, 0);
                assert_eq!(self.0, 0xdead_beef);
                self.1.set(self.1.get() + 1);
            }
        }
        let count = Rc::new(Cell::new(0));
        let data: Vec<IgnoredData> = (0..4)
            .map(|_| IgnoredData::new(A(0xdead_beef, count.clone())))
            .collect();
        drop(data);
        assert_eq!(count.get(), 4);
    }
}
