use crate::CollectionId;
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub const STORAGE_VERSION: u32 = 1;
pub const WAL_V1_FORMAT: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskError {
    BufferTooSmall {
        needed: usize,
        available: usize,
    },
    InvalidChecksum,
    InvalidWalRecordMagic,
    InvalidWalWriteGranule,
    InvalidRegionIndex {
        region_index: u32,
        region_count: u32,
    },
    InvalidWalHeadRegionIndex {
        region_index: u32,
        region_count: u32,
    },
    UnsupportedStorageVersion(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageMetadata {
    pub storage_version: u32,
    pub region_size: u32,
    pub region_count: u32,
    pub min_free_regions: u32,
    pub wal_write_granule: u32,
    pub erased_byte: u8,
    pub wal_record_magic: u8,
}

impl StorageMetadata {
    pub const ENCODED_LEN: usize = size_of::<u32>() * 6 + size_of::<u8>() * 2;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub sequence: u64,
    pub collection_id: CollectionId,
    pub collection_format: u16,
}

impl Header {
    pub const ENCODED_LEN: usize =
        size_of::<u64>() + size_of::<u64>() + size_of::<u16>() + size_of::<u32>();

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRegionPrologue {
    pub wal_head_region_index: u32,
}

impl WalRegionPrologue {
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreePointerFooter {
    pub next_tail: Option<u32>,
}

impl FreePointerFooter {
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

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
