use crate::CollectionId;
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Stable storage metadata version for the current on-disk format.
pub const STORAGE_VERSION: u32 = 1;
/// Stable `collection_format` reserved for WAL regions.
pub const WAL_V1_FORMAT: u16 = 0;

/// Errors returned while encoding or decoding fixed on-disk structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskError {
    /// The provided buffer was not large enough for the requested structure.
    BufferTooSmall {
        /// Required buffer length in bytes.
        needed: usize,
        /// Available buffer length in bytes.
        available: usize,
    },
    /// A CRC-protected structure failed checksum validation.
    InvalidChecksum,
    /// Metadata used an invalid WAL record magic byte.
    InvalidWalRecordMagic,
    /// Metadata used an invalid WAL write granule.
    InvalidWalWriteGranule,
    /// A referenced region index was outside the formatted region range.
    InvalidRegionIndex {
        /// The offending region index.
        region_index: u32,
        /// The total formatted region count.
        region_count: u32,
    },
    /// A WAL prologue pointed at a region outside the formatted region range.
    InvalidWalHeadRegionIndex {
        /// The offending WAL head region index.
        region_index: u32,
        /// The total formatted region count.
        region_count: u32,
    },
    /// Metadata declared a storage version not understood by this build.
    UnsupportedStorageVersion(u32),
}

/// Top-level storage metadata persisted outside the ring regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageMetadata {
    /// Stable storage format version.
    pub storage_version: u32,
    /// Region size in bytes.
    pub region_size: u32,
    /// Number of regions in the store.
    pub region_count: u32,
    /// Minimum number of free regions the engine tries to preserve.
    pub min_free_regions: u32,
    /// WAL write alignment in bytes.
    pub wal_write_granule: u32,
    /// Erased flash byte value.
    pub erased_byte: u8,
    /// Magic byte that prefixes encoded WAL records.
    pub wal_record_magic: u8,
}

impl StorageMetadata {
    /// Encoded byte length of [`StorageMetadata`].
    pub const ENCODED_LEN: usize = size_of::<u32>() * 6 + size_of::<u8>() * 2;

    /// Constructs validated storage metadata for a formatted store.
    pub fn new(
        region_size: u32,
        region_count: u32,
        min_free_regions: u32,
        wal_write_granule: u32,
        erased_byte: u8,
        wal_record_magic: u8,
    ) -> Result<Self, DiskError> {
        let metadata = Self {
            storage_version: STORAGE_VERSION,
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            erased_byte,
            wal_record_magic,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    /// Validates metadata fields that must hold for any supported store.
    pub fn validate(&self) -> Result<(), DiskError> {
        if self.storage_version != STORAGE_VERSION {
            return Err(DiskError::UnsupportedStorageVersion(self.storage_version));
        }

        if self.wal_write_granule == 0 {
            return Err(DiskError::InvalidWalWriteGranule);
        }

        if self.wal_record_magic == self.erased_byte {
            return Err(DiskError::InvalidWalRecordMagic);
        }

        Ok(())
    }

    /// Encodes metadata into `buffer` and returns the encoded length.
    pub fn encode_into(&self, buffer: &mut [u8]) -> Result<usize, DiskError> {
        if buffer.len() < Self::ENCODED_LEN {
            return Err(DiskError::BufferTooSmall {
                needed: Self::ENCODED_LEN,
                available: buffer.len(),
            });
        }

        self.validate()?;

        let mut offset = 0;
        offset = write_u32(buffer, offset, self.storage_version)?;
        offset = write_u32(buffer, offset, self.region_size)?;
        offset = write_u32(buffer, offset, self.region_count)?;
        offset = write_u32(buffer, offset, self.min_free_regions)?;
        offset = write_u32(buffer, offset, self.wal_write_granule)?;
        offset = write_u8(buffer, offset, self.erased_byte)?;
        offset = write_u8(buffer, offset, self.wal_record_magic)?;

        let checksum = crc32(&buffer[..offset]);
        let offset = write_u32(buffer, offset, checksum)?;
        Ok(offset)
    }

    /// Decodes metadata from a CRC-protected byte slice.
    pub fn decode(buffer: &[u8]) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        let storage_version = read_u32(buffer, &mut offset)?;
        let region_size = read_u32(buffer, &mut offset)?;
        let region_count = read_u32(buffer, &mut offset)?;
        let min_free_regions = read_u32(buffer, &mut offset)?;
        let wal_write_granule = read_u32(buffer, &mut offset)?;
        let erased_byte = read_u8(buffer, &mut offset)?;
        let wal_record_magic = read_u8(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;

        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }

        let metadata = Self {
            storage_version,
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            erased_byte,
            wal_record_magic,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    /// Returns the first byte offset usable for WAL records in a WAL region.
    pub fn wal_record_area_offset(&self) -> Result<usize, DiskError> {
        let granule = usize::try_from(self.wal_write_granule)
            .map_err(|_| DiskError::InvalidWalWriteGranule)?;
        if granule == 0 {
            return Err(DiskError::InvalidWalWriteGranule);
        }

        let end = Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN;
        let aligned = end.div_ceil(granule) * granule;
        Ok(aligned)
    }
}

/// Per-region header shared by WAL and committed collection regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Monotonic region sequence number.
    pub sequence: u64,
    /// Collection id owning the region.
    pub collection_id: CollectionId,
    /// Collection-defined committed-region format identifier.
    pub collection_format: u16,
}

impl Header {
    /// Encoded byte length of [`Header`].
    pub const ENCODED_LEN: usize =
        size_of::<u64>() + size_of::<u64>() + size_of::<u16>() + size_of::<u32>();

    /// Encodes the header into `buffer` and returns the encoded length.
    pub fn encode_into(&self, buffer: &mut [u8]) -> Result<usize, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        offset = write_u64(buffer, offset, self.sequence)?;
        offset = write_u64(buffer, offset, self.collection_id.0)?;
        offset = write_u16(buffer, offset, self.collection_format)?;

        let checksum = crc32(&buffer[..offset]);
        let offset = write_u32(buffer, offset, checksum)?;
        Ok(offset)
    }

    /// Decodes a header from a CRC-protected byte slice.
    pub fn decode(buffer: &[u8]) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        let sequence = read_u64(buffer, &mut offset)?;
        let collection_id = CollectionId(read_u64(buffer, &mut offset)?);
        let collection_format = read_u16(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;

        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }

        Ok(Self {
            sequence,
            collection_id,
            collection_format,
        })
    }
}

/// WAL-specific prologue written after the region header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRegionPrologue {
    /// Region index that replay should treat as the logical WAL head.
    pub wal_head_region_index: u32,
}

impl WalRegionPrologue {
    /// Encoded byte length of [`WalRegionPrologue`].
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

    /// Encodes the WAL prologue and validates its region reference.
    pub fn encode_into(&self, buffer: &mut [u8], region_count: u32) -> Result<usize, DiskError> {
        if self.wal_head_region_index >= region_count {
            return Err(DiskError::InvalidWalHeadRegionIndex {
                region_index: self.wal_head_region_index,
                region_count,
            });
        }

        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        offset = write_u32(buffer, offset, self.wal_head_region_index)?;
        let checksum = crc32(&buffer[..offset]);
        let offset = write_u32(buffer, offset, checksum)?;
        Ok(offset)
    }

    /// Decodes a WAL prologue and validates its region reference.
    pub fn decode(buffer: &[u8], region_count: u32) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        let wal_head_region_index = read_u32(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;

        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }

        if wal_head_region_index >= region_count {
            return Err(DiskError::InvalidWalHeadRegionIndex {
                region_index: wal_head_region_index,
                region_count,
            });
        }

        Ok(Self {
            wal_head_region_index,
        })
    }
}

/// Footer stored in free-list regions to chain unused regions together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreePointerFooter {
    /// Successor free-list region, or `None` for the current tail.
    pub next_tail: Option<u32>,
}

impl FreePointerFooter {
    /// Encoded byte length of [`FreePointerFooter`].
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

    /// Encodes the free-list footer into `buffer`.
    pub fn encode_into(&self, buffer: &mut [u8], erased_byte: u8) -> Result<usize, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        match self.next_tail {
            Some(next_tail) => {
                let mut offset = 0;
                offset = write_u32(buffer, offset, next_tail)?;
                let checksum = crc32(&buffer[..offset]);
                let offset = write_u32(buffer, offset, checksum)?;
                Ok(offset)
            }
            None => {
                buffer[..Self::ENCODED_LEN].fill(erased_byte);
                Ok(Self::ENCODED_LEN)
            }
        }
    }

    /// Decodes a free-list footer from the supplied bytes.
    pub fn decode(buffer: &[u8], erased_byte: u8) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;
        if buffer[..Self::ENCODED_LEN]
            .iter()
            .all(|byte| *byte == erased_byte)
        {
            return Ok(Self { next_tail: None });
        }

        let mut offset = 0;
        let next_tail = read_u32(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;
        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }

        Ok(Self {
            next_tail: Some(next_tail),
        })
    }

    /// Decodes a free-list footer and validates the referenced region index.
    pub fn decode_with_region_count(
        buffer: &[u8],
        erased_byte: u8,
        region_count: u32,
    ) -> Result<Self, DiskError> {
        let footer = Self::decode(buffer, erased_byte)?;
        if let Some(next_tail) = footer.next_tail {
            if next_tail >= region_count {
                return Err(DiskError::InvalidRegionIndex {
                    region_index: next_tail,
                    region_count,
                });
            }
        }

        Ok(footer)
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}

fn ensure_len(buffer: &[u8], len: usize) -> Result<(), DiskError> {
    if buffer.len() < len {
        return Err(DiskError::BufferTooSmall {
            needed: len,
            available: buffer.len(),
        });
    }
    Ok(())
}

fn write_u8(buffer: &mut [u8], offset: usize, value: u8) -> Result<usize, DiskError> {
    ensure_len(buffer, offset + size_of::<u8>())?;
    buffer[offset] = value;
    Ok(offset + size_of::<u8>())
}

fn write_u16(buffer: &mut [u8], offset: usize, value: u16) -> Result<usize, DiskError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_u32(buffer: &mut [u8], offset: usize, value: u32) -> Result<usize, DiskError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_u64(buffer: &mut [u8], offset: usize, value: u64) -> Result<usize, DiskError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_bytes(buffer: &mut [u8], offset: usize, bytes: &[u8]) -> Result<usize, DiskError> {
    ensure_len(buffer, offset + bytes.len())?;
    buffer[offset..offset + bytes.len()].copy_from_slice(bytes);
    Ok(offset + bytes.len())
}

fn read_u8(buffer: &[u8], offset: &mut usize) -> Result<u8, DiskError> {
    ensure_len(buffer, *offset + size_of::<u8>())?;
    let value = buffer[*offset];
    *offset += size_of::<u8>();
    Ok(value)
}

fn read_u16(buffer: &[u8], offset: &mut usize) -> Result<u16, DiskError> {
    let bytes = read_array::<2>(buffer, offset)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(buffer: &[u8], offset: &mut usize) -> Result<u32, DiskError> {
    let bytes = read_array::<4>(buffer, offset)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(buffer: &[u8], offset: &mut usize) -> Result<u64, DiskError> {
    let bytes = read_array::<8>(buffer, offset)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_array<const N: usize>(buffer: &[u8], offset: &mut usize) -> Result<[u8; N], DiskError> {
    ensure_len(buffer, *offset + N)?;
    let mut bytes = [0u8; N];
    bytes.copy_from_slice(&buffer[*offset..*offset + N]);
    *offset += N;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
