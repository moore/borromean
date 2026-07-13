// Archived core-pilot implementation snapshot. Not part of the compiled crate.
/// Static geometry and transfer constraints exposed by a v3 backing device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceGeometry {
    /// Size of the independent metadata erase unit.
    pub metadata_size: usize,
    /// Size of each data erase region.
    pub region_size: usize,
    /// Number of data regions, excluding the metadata erase unit.
    pub region_count: u32,
    /// Byte observed after erase.
    pub erased_byte: u8,
    /// Minimum address and length alignment for reads.
    pub read_alignment: usize,
    /// Minimum address and length alignment for programs.
    pub program_alignment: usize,
    /// Maximum bytes accepted by one read request.
    pub max_read_len: usize,
    /// Maximum bytes accepted by one program request.
    pub max_program_len: usize,
}

impl DeviceGeometry {
    /// Validates geometry needed by the storage kernel.
    pub fn validate(self) -> Result<Self, GeometryError> {
        if self.metadata_size == 0 {
            return Err(GeometryError::ZeroMetadataSize);
        }
        if self.region_size == 0 {
            return Err(GeometryError::ZeroRegionSize);
        }
        if self.region_count == 0 {
            return Err(GeometryError::ZeroRegionCount);
        }
        if self.read_alignment == 0 {
            return Err(GeometryError::ZeroReadAlignment);
        }
        if self.program_alignment == 0 {
            return Err(GeometryError::ZeroProgramAlignment);
        }
        if self.max_read_len == 0 {
            return Err(GeometryError::ZeroMaxReadLen);
        }
        if self.max_program_len == 0 {
            return Err(GeometryError::ZeroMaxProgramLen);
        }
        if !self.region_size.is_multiple_of(self.read_alignment) {
            return Err(GeometryError::RegionReadMisaligned);
        }
        if !self.region_size.is_multiple_of(self.program_alignment) {
            return Err(GeometryError::RegionProgramMisaligned);
        }
        Ok(self)
    }

    /// Returns whether a region range is valid for a read.
    pub fn valid_read(self, offset: usize, len: usize) -> bool {
        valid_transfer(
            self.region_size,
            self.read_alignment,
            self.max_read_len,
            offset,
            len,
        )
    }

    /// Returns whether a region range is valid for a program.
    pub fn valid_program(self, offset: usize, len: usize) -> bool {
        valid_transfer(
            self.region_size,
            self.program_alignment,
            self.max_program_len,
            offset,
            len,
        )
    }
}

fn valid_transfer(
    region_size: usize,
    alignment: usize,
    maximum: usize,
    offset: usize,
    len: usize,
) -> bool {
    len != 0
        && offset.is_multiple_of(alignment)
        && len.is_multiple_of(alignment)
        && len <= maximum
        && offset
            .checked_add(len)
            .is_some_and(|end| end <= region_size)
}

/// Invalid device geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeometryError {
    ZeroMetadataSize,
    ZeroRegionSize,
    ZeroRegionCount,
    ZeroReadAlignment,
    ZeroProgramAlignment,
    ZeroMaxReadLen,
    ZeroMaxProgramLen,
    RegionReadMisaligned,
    RegionProgramMisaligned,
}

/// Blocking raw device contract used by the v3 kernel.
pub trait RawFlash {
    /// Backend-specific failure type.
    type Error;

    /// Returns immutable device geometry.
    fn geometry(&self) -> DeviceGeometry;

    /// Reads bytes from the metadata erase unit.
    fn read_metadata<R>(
        &mut self,
        offset: usize,
        len: usize,
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Self::Error>;

    /// Programs bytes in the metadata erase unit.
    fn program_metadata(&mut self, offset: usize, bytes: &[u8]) -> Result<(), Self::Error>;

    /// Erases the metadata erase unit.
    fn erase_metadata(&mut self) -> Result<(), Self::Error>;

    /// Reads bytes from one data region.
    fn read_region<R>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Self::Error>;

    /// Programs bytes in one data region.
    fn program_region(
        &mut self,
        region_index: u32,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), Self::Error>;

    /// Erases one complete data region.
    fn erase_region(&mut self, region_index: u32) -> Result<(), Self::Error>;

    /// Makes every prior successful program and erase durable.
    fn sync(&mut self) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_enforces_alignment_length_and_bounds() {
        let geometry = DeviceGeometry {
            metadata_size: 512,
            region_size: 512,
            region_count: 8,
            erased_byte: 0xff,
            read_alignment: 4,
            program_alignment: 8,
            max_read_len: 128,
            max_program_len: 64,
        }
        .validate()
        .unwrap();

        assert!(geometry.valid_read(4, 128));
        assert!(!geometry.valid_read(2, 128));
        assert!(!geometry.valid_read(4, 132));
        assert!(geometry.valid_program(8, 64));
        assert!(!geometry.valid_program(8, 72));
        assert!(!geometry.valid_program(480, 64));
    }
}
