
pub trait VecLike<T> {
    fn push(&mut self, item: T) -> Result<(), T>;
    fn get(&self, index: usize) -> Option<&T>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn capacity(&self) -> usize;
    fn clear(&mut self);
    fn iter(&self) -> core::slice::Iter<'_, T>;
    fn iter_mut(&mut self) -> core::slice::IterMut<'_, T>;
    fn as_slice(&self) -> &[T];
    fn as_mut_slice(&mut self) -> &mut [T];
}

pub struct VecLikeSlice<'a, T, const N: usize> {
    items: &'a mut [T; N],
    len: usize,
}

impl<'a, T, const N: usize> VecLikeSlice<'a, T, N> {
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
