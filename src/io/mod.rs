
pub mod mem_io;

#[cfg(test)]
mod tests;


pub enum IoError {

}

pub trait StorageMeta {
    fn get_version(&self) -> u32;
}

pub trait Io<const MAX_HEADS: usize> {
    type head; 
    fn get_meta<'a>(&'a self) -> &'a StorageMeta;
    fn get_region<'a>(&'a self, index: u64) -> Result<Region<'a, MAX_HEADS>, IoError>;
}

