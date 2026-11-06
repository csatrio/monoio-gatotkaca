//! Slab.
//! Part of code and design forked from tokio.
//!
//! Slots live in pages that double in size (64, 128, 256, ...), so the slab grows without
//! moving a slot. A lookup does not walk the pages: the page comes from the highest set bit of
//! the key ([`page_of`]), and a small table ([`PageIndex`]) gives, per page, a pointer that turns
//! the key into its slot with one multiply-add and the bound the key must stay under. The
//! pages themselves (free list, counters) are only touched by `insert` and `remove`.
//!
//! Entries are aligned to a cache line ([`CACHE_LINE`]): an entry of up to 64 bytes lies in
//! one line, and reaching it costs at most one cache miss.

use std::{
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::{self, NonNull},
};

/// Pre-allocated storage for a uniform data type
pub(crate) struct Slab<T> {
    // per page: where a key's slot is, and which keys have one (see `PageIndex`)
    index: [PageIndex<T>; INDEX_LEN],
    // pages of continued memory
    pages: [Option<Page<T>>; NUM_PAGES],
    // cached write page id
    w_page_id: usize,
    // current generation
    generation: u32,
}

const NUM_PAGES: usize = 26;
const PAGE_INITIAL_SIZE: usize = 64;
const PAGE_SHIFT: u32 = PAGE_INITIAL_SIZE.trailing_zeros();
const COMPACT_INTERVAL: u32 = 2048;
/// The alignment of an entry (the `repr(align)` of [`Entry`] repeats the number).
const CACHE_LINE: usize = 64;
/// One index entry for every value [`page_of`] can return, so that a lookup needs no range
/// check on the page id: the entries beyond `NUM_PAGES` stay empty and reject every key.
const INDEX_LEN: usize = (usize::BITS - PAGE_SHIFT) as usize;
const _: () = assert!(PAGE_INITIAL_SIZE.is_power_of_two() && NUM_PAGES <= INDEX_LEN);

/// What a lookup needs of a page: two words, read without touching the page.
struct PageIndex<T> {
    /// The slots of the page, moved back by the page's first key (in slots): the slot of key
    /// `k` of this page is `base.wrapping_add(k)`. Null while the page does not exist.
    base: *mut Slot<T>,
    /// First key of the page whose slot was never initialized (first key of the page plus its
    /// `initialized` count); 0 while the page does not exist. A key that [`page_of`] maps to
    /// this page has an initialized slot iff it is below `end`.
    end: usize,
}

impl<T> PageIndex<T> {
    const NONE: Self = PageIndex {
        base: ptr::null_mut(),
        end: 0,
    };
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Slab<T> {
    /// Size and alignment of an entry, for the layout tests of the types kept in a slab.
    #[cfg(test)]
    pub(crate) const ENTRY_SIZE: usize = std::mem::size_of::<Slot<T>>();
    #[cfg(test)]
    pub(crate) const ENTRY_ALIGN: usize = std::mem::align_of::<Slot<T>>();

    /// Create a new slab.
    pub(crate) const fn new() -> Slab<T> {
        Slab {
            index: [PageIndex::<T>::NONE; INDEX_LEN],
            pages: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None, None, None, None, None,
            ],
            w_page_id: 0,
            generation: 0,
        }
    }

    /// The slot of `key`, if `key` is in a page and its slot was initialized (it may be
    /// vacant).
    #[inline(always)]
    fn entry_ptr(&self, key: usize) -> Option<NonNull<Slot<T>>> {
        // SAFETY: `page_of` returns less than `INDEX_LEN`.
        let index = unsafe { self.index.get_unchecked(page_of(key)) };
        if key < index.end {
            // SAFETY: `key` is one of the page's keys with an initialized slot (the keys
            // `page_of` maps to a page are never below the page's first key), so the pointer
            // is inside the page's slots, hence not null.
            Some(unsafe { NonNull::new_unchecked(index.base.wrapping_add(key)) })
        } else {
            None
        }
    }

    /// The occupied slot of `key`, without the checks of [`get`](Self::get).
    ///
    /// # Safety
    /// The slot of `key` must be occupied.
    #[inline]
    pub(crate) unsafe fn get_unchecked(&mut self, key: usize) -> Ref<'_, T> {
        // SAFETY: an occupied slot is in an existing page, where `base` leads to it.
        let entry = unsafe {
            let index = self.index.get_unchecked(page_of(key));
            NonNull::new_unchecked(index.base.wrapping_add(key))
        };
        Ref {
            slab: self,
            entry,
            key,
        }
    }

    #[inline]
    pub(crate) fn get(&mut self, key: usize) -> Option<Ref<'_, T>> {
        let entry = self.entry_ptr(key)?;
        // SAFETY: `entry_ptr` returns initialized slots.
        match unsafe { &entry.as_ref().0 } {
            Entry::Vacant(_) => None,
            Entry::Occupied(_) => Some(Ref {
                slab: self,
                entry,
                key,
            }),
        }
    }

    /// Shared access to the value of `key`, if occupied.
    #[inline]
    pub(crate) fn peek(&self, key: usize) -> Option<&T> {
        let entry = self.entry_ptr(key)?;
        // SAFETY: `entry_ptr` returns initialized slots, and `&self` keeps the slot as it is.
        unsafe { entry.as_ref() }.0.as_ref()
    }

    /// Ask the CPU to pull the entry of `key` into the cache (a hint; nothing if `key` was never
    /// allocated). Lets a caller walking a batch of keys overlap their cache misses.
    #[inline]
    pub(crate) fn prefetch(&self, key: usize) {
        if let Some(entry) = self.entry_ptr(key) {
            let p = entry.as_ptr().cast::<u8>();
            // Entries start on a cache line: the first line is all of an entry of up to 64
            // bytes. A larger one gets its last line as well.
            prefetch_read(p);
            if std::mem::size_of::<Slot<T>>() > CACHE_LINE {
                prefetch_read(p.wrapping_add(std::mem::size_of::<Slot<T>>() - 1));
            }
        }
    }

    /// Insert an element into slab. The key is returned.
    /// Note: If the slab is out of slot, it will panic.
    ///
    /// Inlined, with the search for a page out of line and `val` never passed on: the caller's
    /// constructor then writes the fields it sets straight into the slot, instead of building
    /// the value on the stack and copying all of it.
    #[inline]
    pub(crate) fn insert(&mut self, val: T) -> usize {
        // Not dropped if `find_write_page` panics (out of slots: the value leaks). A drop on
        // that unwind path would take the value's address, which keeps it on the stack.
        let val = ManuallyDrop::new(val);
        let mut page_id = self.w_page_id;
        // SAFETY: `w_page_id` is always below `NUM_PAGES`.
        let has_room = match unsafe { self.pages.get_unchecked(page_id) } {
            Some(page) => !page.is_full(),
            None => false,
        };
        if !has_room {
            page_id = self.find_write_page();
        }
        // SAFETY: page `page_id` exists and is not full: checked above, or `find_write_page`
        // made it so. `next` of such a page is a vacant slot of its free list, or, if the list
        // is empty, the first slot that was never used.
        unsafe {
            let page = page_mut(&mut self.pages, page_id);
            let slot = page.next;
            let entry = page.slots.as_ptr().add(slot);
            if slot < page.initialized {
                page.next = match &(*entry).0 {
                    Entry::Vacant(next) => *next,
                    Entry::Occupied(_) => std::hint::unreachable_unchecked(),
                };
            } else {
                debug_assert_eq!(slot, page.initialized, "next should eq to initialized");
                page.initialized += 1;
                page.next = slot + 1;
                self.index.get_unchecked_mut(page_id).end += 1;
            }
            page.used += 1;
            entry.write(Slot(Entry::Occupied(ManuallyDrop::into_inner(val))));
            slot + page.first_key
        }
    }

    /// The first page, from the write page on, that has a free slot; it becomes the write
    /// page and is created if it does not exist. Panics if there is none.
    #[cold]
    #[inline(never)]
    fn find_write_page(&mut self) -> usize {
        for i in self.w_page_id..NUM_PAGES {
            // SAFETY: `i < NUM_PAGES <= INDEX_LEN`.
            let (page, index) = unsafe {
                (
                    self.pages.get_unchecked_mut(i),
                    self.index.get_unchecked_mut(i),
                )
            };
            let page = page.get_or_insert_with(|| {
                let page = Page::new(
                    PAGE_INITIAL_SIZE << i,
                    (PAGE_INITIAL_SIZE << i) - PAGE_INITIAL_SIZE,
                );
                *index = PageIndex {
                    // Out of the allocation for every page but the first, so `wrapping_sub`;
                    // `entry_ptr` only uses it with keys that lead back into the page.
                    base: page.slots.as_ptr().wrapping_sub(page.first_key),
                    end: page.first_key,
                };
                page
            });
            if !page.is_full() {
                self.w_page_id = i;
                return i;
            }
        }
        panic!("out of slot");
    }

    /// Put the occupied slot `entry` of `key` on the free list of its page. The value in it is
    /// neither dropped nor moved out.
    ///
    /// # Safety
    /// `entry` is the slot of `key` and occupied, and its value was moved out or needs no drop.
    #[inline]
    unsafe fn release(&mut self, entry: NonNull<Slot<T>>, key: usize) {
        // SAFETY: an occupied slot is in an existing page, the one `page_of` gives.
        unsafe {
            let page = page_mut(&mut self.pages, page_of(key));
            entry.as_ptr().write(Slot(Entry::Vacant(page.next)));
            page.next = key - page.first_key;
            page.used -= 1;
        }
        self.mark_remove();
    }

    #[inline]
    fn mark_remove(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation.is_multiple_of(COMPACT_INTERVAL) {
            self.compact();
        }
    }

    #[cold]
    #[inline(never)]
    fn compact(&mut self) {
        // reset write page index
        self.w_page_id = 0;
        // find the last allocated page and try to drop
        if let Some((id, last_page)) = self
            .pages
            .iter_mut()
            .enumerate()
            .rev()
            .find_map(|(id, p)| p.as_mut().map(|p| (id, p)))
        {
            if last_page.is_empty() && id > 0 {
                // The index entry first: nothing may point into the freed page.
                self.index[id] = PageIndex::NONE;
                self.pages[id] = None;
            }
        }
    }
}

/// Prefetch the cache line of `p` for reading (a hint, never faults; no-op off x86_64).
#[inline(always)]
pub(crate) fn prefetch_read<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    #[allow(unused_unsafe)]
    // SAFETY: a prefetch never faults, whatever the address; SSE is part of the x86_64 baseline.
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch::<_MM_HINT_T0>(p.cast::<i8>());
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// The page whose key range contains `key`. Always below `INDEX_LEN`; `NUM_PAGES` or more for
/// a key beyond the last page.
///
/// Page `i` has `PAGE_INITIAL_SIZE << i` slots and starts at key
/// `(PAGE_INITIAL_SIZE << i) - PAGE_INITIAL_SIZE`. So with `n = key + PAGE_INITIAL_SIZE`, the
/// page is the position of the highest set bit of `n`, counted from that of
/// `PAGE_INITIAL_SIZE`: one bit scan.
///
/// The last `PAGE_INITIAL_SIZE` keys of `usize` wrap `n` and come out as page 0, whose keys
/// they are not. They are far above the `end` of any page, which every lookup checks.
#[inline(always)]
fn page_of(key: usize) -> usize {
    let n = key.wrapping_add(PAGE_INITIAL_SIZE);
    // `| 1` makes the scan defined for 0 (a wrapped `n`) and changes no other result.
    ((n >> PAGE_SHIFT) | 1).ilog2() as usize
}

/// Page `page_id`, which the caller knows to exist.
///
/// A plain match: `as_mut().unwrap_unchecked()` leaves a test and a conditional move behind.
///
/// # Safety
/// `page_id < NUM_PAGES` and the page exists.
#[inline(always)]
unsafe fn page_mut<T>(pages: &mut [Option<Page<T>>; NUM_PAGES], page_id: usize) -> &mut Page<T> {
    // SAFETY: guaranteed by the caller.
    match unsafe { pages.get_unchecked_mut(page_id) } {
        Some(page) => page,
        None => unsafe { std::hint::unreachable_unchecked() },
    }
}

/// Ref point to a valid slot.
pub(crate) struct Ref<'a, T> {
    slab: &'a mut Slab<T>,
    // The slot of `key`: occupied, in a page of `slab`.
    entry: NonNull<Slot<T>>,
    key: usize,
}

impl<'a, T> Ref<'a, T> {
    /// The slab and the key of this slot; [`Slab::get_unchecked`] turns them back into the
    /// `Ref`. Two words that travel in registers, where a `Ref` argument goes through memory.
    #[inline]
    pub(crate) fn into_parts(self) -> (&'a mut Slab<T>, usize) {
        (self.slab, self.key)
    }

    #[inline]
    pub(crate) fn remove(self) -> T {
        // SAFETY: the slot is occupied; its value is moved out before the slot is freed.
        unsafe {
            let val = match self.entry.as_ptr().read().0 {
                Entry::Occupied(val) => val,
                Entry::Vacant(_) => std::hint::unreachable_unchecked(),
            };
            self.slab.release(self.entry, self.key);
            val
        }
    }

    /// Free the slot without moving its value out or dropping it (the value is leaked).
    ///
    /// # Safety
    /// Whatever the value owns must have been moved out already (or need no drop).
    #[inline]
    pub(crate) unsafe fn remove_forget(self) {
        // SAFETY: the slot is occupied; the caller vouches for the value.
        unsafe { self.slab.release(self.entry, self.key) };
    }
}

impl<T> AsRef<T> for Ref<'_, T> {
    #[inline]
    fn as_ref(&self) -> &T {
        // SAFETY: the slot is occupied, and borrowed through `self.slab` for as long as this
        // `Ref` lives.
        unsafe { self.entry.as_ref().0.as_ref().unwrap_unchecked() }
    }
}

impl<T> AsMut<T> for Ref<'_, T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        // SAFETY: as in `as_ref`, exclusively.
        unsafe { self.entry.as_mut().0.as_mut().unwrap_unchecked() }
    }
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for Ref<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

enum Entry<T> {
    Vacant(usize),
    Occupied(T),
}

/// An entry as the pages store it: aligned to a cache line, so its size is a multiple of 64
/// and it never shares a line with its neighbours.
///
/// A wrapper, not a `repr(align)` on [`Entry`]: that would align each variant, and a
/// `Vacant` padded to 64 bytes no longer fits next to the spare value of `T` that marks it,
/// which costs a 64-byte `T` a second line.
#[repr(align(64))]
struct Slot<T>(Entry<T>);

const _: () = assert!(std::mem::align_of::<Slot<u8>>() == CACHE_LINE);

impl<T> Entry<T> {
    #[inline]
    fn as_ref(&self) -> Option<&T> {
        match self {
            Entry::Vacant(_) => None,
            Entry::Occupied(inner) => Some(inner),
        }
    }

    #[inline]
    fn as_mut(&mut self) -> Option<&mut T> {
        match self {
            Entry::Vacant(_) => None,
            Entry::Occupied(inner) => Some(inner),
        }
    }
}

struct Page<T> {
    // continued buffer of fixed size: `cap` slots, the first `initialized` of them
    // initialized. Owned; a raw pointer because lookups reach the slots through
    // `PageIndex::base`, a pointer derived from this one.
    slots: NonNull<Slot<T>>,
    // number of slots
    cap: usize,
    // number of occupied slots
    used: usize,
    // number of initialized slots
    initialized: usize,
    // next slot to write
    next: usize,
    // sum of previous page's slots count: the key of the first slot
    first_key: usize,
}

impl<T> Page<T> {
    fn new(size: usize, first_key: usize) -> Self {
        let slots = Box::<[Slot<T>]>::new_uninit_slice(size);
        // SAFETY: a box is never null.
        let slots = unsafe { NonNull::new_unchecked(Box::into_raw(slots).cast::<Slot<T>>()) };
        Self {
            slots,
            cap: size,
            used: 0,
            initialized: 0,
            next: 0,
            first_key,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.used == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.used == self.cap
    }
}

impl<T> Drop for Page<T> {
    fn drop(&mut self) {
        // SAFETY: `slots` came from a boxed slice of `cap` slots whose first `initialized`
        // are initialized. Vacant slots own nothing, so an empty page skips the walk.
        unsafe {
            if !self.is_empty() {
                ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                    self.slots.as_ptr(),
                    self.initialized,
                ));
            }
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                self.slots.as_ptr().cast::<MaybeUninit<Slot<T>>>(),
                self.cap,
            )));
        }
    }
}

#[cfg(test)]
impl<T> Slab<T> {
    /// Get slab len.
    pub(crate) fn len(&self) -> usize {
        self.pages.iter().fold(0, |acc, page| match page {
            Some(page) => acc + page.used,
            None => acc,
        })
    }

    /// Remove an element from slab.
    pub(crate) fn remove(&mut self, key: usize) -> Option<T> {
        self.get(key).map(Ref::remove)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Entries are aligned to a cache line, and the alignment costs nothing for a value that
    /// fills the line and has a spare value for the vacant marker.
    #[test]
    fn entries_are_cache_line_aligned() {
        assert_eq!(Slab::<u8>::ENTRY_ALIGN, CACHE_LINE);
        assert_eq!(Slab::<u8>::ENTRY_SIZE, CACHE_LINE);
        assert_eq!(Slab::<[u64; 7]>::ENTRY_SIZE, CACHE_LINE);
        assert_eq!(Slab::<[u64; 8]>::ENTRY_SIZE, 2 * CACHE_LINE);
        // 64 bytes with a niche (the bool): the vacant marker goes into it.
        assert_eq!(Slab::<([u64; 7], u32, bool)>::ENTRY_SIZE, CACHE_LINE);

        let mut slab = Slab::new();
        for i in 0..PAGE_INITIAL_SIZE * 3 {
            let key = slab.insert(i);
            let p = slab.peek(key).unwrap() as *const usize as usize;
            assert_eq!(p / CACHE_LINE, (p + 7) / CACHE_LINE);
            assert_eq!(
                slab.get(key).unwrap().entry.as_ptr() as usize % CACHE_LINE,
                0
            );
        }
    }

    #[test]
    fn insert_get_remove_one() {
        let mut slab = Slab::default();
        let key = slab.insert(10);
        assert_eq!(slab.get(key).unwrap().as_mut(), &10);
        assert_eq!(slab.remove(key), Some(10));
        assert!(slab.get(key).is_none());
        assert_eq!(slab.len(), 0);
    }

    #[test]
    fn insert_get_remove_many() {
        let mut slab = Slab::new();
        let mut keys = vec![];

        for i in 0..10 {
            for j in 0..10 {
                let val = (i * 10) + j;

                let key = slab.insert(val);
                keys.push((key, val));
                assert_eq!(slab.get(key).unwrap().as_mut(), &val);
            }

            for (key, val) in keys.drain(..) {
                assert_eq!(val, slab.remove(key).unwrap());
            }
        }
    }

    #[test]
    fn get_not_exist() {
        let mut slab = Slab::<i32>::new();
        assert!(slab.get(0).is_none());
        assert!(slab.get(1).is_none());
        assert!(slab.get(usize::MAX).is_none());
        assert!(slab.remove(0).is_none());
        assert!(slab.remove(1).is_none());
        assert!(slab.remove(usize::MAX).is_none());
    }

    /// `page_of` against the definition: page `i` starts at `64 * 2^i - 64` and has `64 * 2^i`
    /// slots. Keys beyond the last page get a page id no page has, keys that wrap get page 0.
    #[test]
    fn page_of_matches_page_bounds() {
        let mut start = 0usize;
        for page in 0..NUM_PAGES {
            let len = PAGE_INITIAL_SIZE << page;
            assert_eq!(start, len - PAGE_INITIAL_SIZE);
            for index in [0, 1, len / 2, len - 2, len - 1] {
                assert_eq!(page_of(start + index), page);
            }
            start += len;
        }
        // `start` is now the first key no page holds.
        for key in [
            start,
            start + 1,
            1 << 40,
            1 << 62,
            usize::MAX - PAGE_INITIAL_SIZE - 1,
            usize::MAX - PAGE_INITIAL_SIZE,
        ] {
            let page = page_of(key);
            assert!((NUM_PAGES..INDEX_LEN).contains(&page), "key {key:#x}");
        }
        for key in [
            usize::MAX - PAGE_INITIAL_SIZE + 1,
            usize::MAX - 1,
            usize::MAX,
        ] {
            assert_eq!(page_of(key), 0, "key {key:#x}");
        }
    }

    /// Keys without a slot resolve to nothing, whatever the pages hold: never allocated,
    /// beyond the last page, wrapping.
    #[test]
    fn keys_without_slot() {
        let mut slab = Slab::new();
        let n = PAGE_INITIAL_SIZE * 3 + 5;
        for i in 0..n {
            assert_eq!(slab.insert(i), i);
        }
        let last = (PAGE_INITIAL_SIZE << NUM_PAGES) - PAGE_INITIAL_SIZE;
        let keys = (usize::MAX - PAGE_INITIAL_SIZE - 1..=usize::MAX).chain([
            n,
            n + 1,
            PAGE_INITIAL_SIZE * 7 - 1,
            PAGE_INITIAL_SIZE * 7,
            last - 1,
            last,
            1 << 40,
            1 << 62,
        ]);
        for key in keys {
            assert!(slab.get(key).is_none(), "key {key:#x}");
            assert!(slab.peek(key).is_none(), "key {key:#x}");
            slab.prefetch(key);
            assert!(slab.remove(key).is_none(), "key {key:#x}");
        }
        assert_eq!(slab.len(), n);
        for i in 0..n {
            assert_eq!(slab.peek(i), Some(&i));
        }
    }

    /// Slots are handed out page by page, freed slots are reused last-in first-out, and a
    /// full write page moves on to the next one.
    #[test]
    fn insert_fills_pages_and_reuses_freed_slots() {
        let mut slab = Slab::new();
        let n = PAGE_INITIAL_SIZE * 3 + 5;
        for i in 0..n {
            assert_eq!(slab.insert(i), i);
        }
        assert_eq!(slab.len(), n);
        // The write page is page 2; its freed slots come back in reverse order.
        let (a, b) = (PAGE_INITIAL_SIZE * 3 + 1, PAGE_INITIAL_SIZE * 3 + 3);
        assert_eq!(slab.remove(a), Some(a));
        assert!(slab.get(a).is_none());
        assert!(slab.peek(a).is_none());
        assert_eq!(slab.remove(b), Some(b));
        assert_eq!(slab.insert(1000), b);
        assert_eq!(slab.insert(1001), a);
        assert_eq!(slab.insert(1002), n);
        assert_eq!(slab.get(b).map(|mut r| *r.as_mut()), Some(1000));
        assert_eq!(slab.peek(a), Some(&1001));
        assert!(slab.peek(n + 1).is_none());
        assert_eq!(slab.len(), n + 1);
    }

    /// Every `COMPACT_INTERVAL` removals the write page goes back to the first page and an
    /// empty last page is freed; its keys then resolve to nothing until it is created again.
    #[test]
    fn compact_frees_the_last_page() {
        let mut slab = Slab::new();
        let n = PAGE_INITIAL_SIZE * 7; // pages 0, 1, 2 full
        for i in 0..n {
            assert_eq!(slab.insert(i), i);
        }
        let first_of_last = PAGE_INITIAL_SIZE * 3;
        for key in first_of_last..n {
            assert_eq!(slab.remove(key), Some(key));
        }
        assert!(slab.pages[2].is_some());
        // Removals up to the next compaction. The write page is still the last one, so each
        // insert lands there and each removal leaves it empty again.
        for _ in 0..COMPACT_INTERVAL {
            let key = slab.insert(0);
            assert!(key >= first_of_last);
            assert_eq!(slab.remove(key), Some(0));
            if slab.pages[2].is_none() {
                break;
            }
        }
        assert!(slab.pages[2].is_none(), "the empty last page is freed");
        assert!(slab.pages[1].is_some());
        for key in first_of_last..n {
            assert!(slab.get(key).is_none());
            assert!(slab.peek(key).is_none());
            slab.prefetch(key);
        }
        assert_eq!(slab.len(), first_of_last);
        // The page comes back when the slab grows again.
        assert_eq!(slab.insert(7), first_of_last);
        assert_eq!(slab.peek(first_of_last), Some(&7));
    }

    /// Dropping the slab drops the values still in it, once.
    #[test]
    fn drop_drops_occupied() {
        use std::rc::Rc;
        let count = Rc::new(());
        let mut slab = Slab::new();
        let keys: Vec<usize> = (0..200).map(|_| slab.insert(count.clone())).collect();
        for key in keys.iter().step_by(2) {
            drop(slab.remove(*key));
        }
        assert_eq!(Rc::strong_count(&count), 101);
        drop(slab);
        assert_eq!(Rc::strong_count(&count), 1);
    }

    #[test]
    fn insert_remove_big() {
        let mut slab = Slab::default();
        let keys = (0..1_000_000).map(|i| slab.insert(i)).collect::<Vec<_>>();
        keys.iter().zip(0..1_000_000).for_each(|(key, val)| {
            assert_eq!(slab.remove(*key).unwrap(), val);
        });
        keys.iter().for_each(|key| {
            assert!(slab.get(*key).is_none());
        });
        assert_eq!(slab.len(), 0);
    }
}
