/// Minimal vector-like operations used by collection helpers.
pub trait VecLike<T> {
    /// Pushes an item, returning it back on capacity failure.
    fn push(&mut self, item: T) -> Result<(), T>;
    /// Returns a shared reference to the item at `index`.
    fn get(&self, index: usize) -> Option<&T>;
    /// Returns the current logical length.
    fn len(&self) -> usize;
    /// Returns whether the collection is empty.
    fn is_empty(&self) -> bool;
    /// Returns the maximum number of stored items.
    fn capacity(&self) -> usize;
    /// Removes all items.
    fn clear(&mut self);
    /// Returns an iterator over stored items.
    fn iter(&self) -> core::slice::Iter<'_, T>;
    /// Returns a mutable iterator over stored items.
    fn iter_mut(&mut self) -> core::slice::IterMut<'_, T>;
    /// Returns the active slice.
    fn as_slice(&self) -> &[T];
    /// Returns the active mutable slice.
    fn as_mut_slice(&mut self) -> &mut [T];
}

/// Fixed-size slice-backed [`VecLike`] adapter.
pub struct VecLikeSlice<'a, T, const N: usize> {
    items: &'a mut [T; N],
    len: usize,
}

impl<'a, T, const N: usize> VecLikeSlice<'a, T, N> {
    /// Wraps a fixed-size array as a slice-backed vector.
    pub fn new(items: &'a mut [T; N]) -> Self {
        Self { items, len: 0 }
    }
}

impl<'a, T, const N: usize> VecLike<T> for VecLikeSlice<'a, T, N> {
    fn push(&mut self, item: T) -> Result<(), T> {
        if self.len < N {
            self.items[self.len] = item;
            self.len += 1;
            Ok(())
        } else {
            Err(item)
        }
    }

    fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn capacity(&self) -> usize {
        N
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn iter(&self) -> core::slice::Iter<'_, T> {
        self.items[..self.len].iter()
    }

    fn iter_mut(&mut self) -> core::slice::IterMut<'_, T> {
        self.items[..self.len].iter_mut()
    }

    fn as_slice(&self) -> &[T] {
        &self.items[..self.len]
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.items[..self.len]
    }
}
