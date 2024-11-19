#![no_std]


#[cfg(test)]
mod tests;

mod io;

#[derive(Debug)]
pub enum StorageError {
    EraseNotPageAligned,
    RegionNotPageAligned,
    RegionAlignmentError,
    SerializerError(postcard::Error),
    ArithmeticOverflow,
    InternalError,
}

pub struct Storage<'a, const MAX_HEADS: usize, IO: io::Io<MAX_HEADS>> {
    meta: &'a mut IO,
    sequence: u64,
    head: IO::head,
    free_list_head: u64,
    free_list_tail: u64,
}
