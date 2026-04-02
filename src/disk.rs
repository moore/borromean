use crate::CollectionId;
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

//= spec/ring.md#canonical-on-disk-encoding
//# RING-DISK-006 `metadata_checksum`, `header_checksum`, `prologue_checksum`, `footer_checksum`, and `record_checksum` MUST all use the standard CRC-32C (Castagnoli) parameters (`poly = 0x1edc6f41`, `init = 0xffffffff`, `refin = true`, `refout = true`, `xorout = 0xffffffff`) and MUST be stored little-endian.
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

//= spec/ring.md#storage-metadata
//# RING-META-001 The canonical on-disk `storage_version` defined by this specification MUST be `1`.
pub const STORAGE_VERSION: u32 = 1;
//= spec/ring.md#canonical-on-disk-encoding
//# RING-DISK-004 `collection_format` is a stable per-region `u16` namespace recorded durably in region headers. The pair `(collection_type, collection_format)` identifies a concrete committed region payload encoding. Borromean core reserves `collection_format = 0x0000` globally for `wal_v1`; every non-WAL collection format MUST be nonzero.
//= spec/ring.md#storage-requirements
//# `RING-STORAGE-005` Borromean core MUST reserve the canonical `collection_format` value `wal_v1` for WAL regions, and user collections MUST NOT use that identifier.
pub const WAL_V1_FORMAT: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskError {
    BufferTooSmall { needed: usize, available: usize },
    InvalidChecksum,
    InvalidWalRecordMagic,
    InvalidWalWriteGranule,
    InvalidRegionIndex { region_index: u32, region_count: u32 },
    InvalidWalHeadRegionIndex { region_index: u32, region_count: u32 },
    UnsupportedStorageVersion(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
//= spec/ring.md#storage-requirements
//# `RING-STORAGE-001` Storage MUST begin with a static metadata region that records version and configuration parameters that do not change after initialization.
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
    //= spec/implementation.md#memory-requirements
    //# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk format constants MUST be derivable from public constants, associated constants, or documented constructor contracts.
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
        //= spec/ring.md#wal-record-types
        //# `RING-WAL-ENC-002` `record_magic` MUST equal the storage's configured `wal_record_magic`, and `wal_record_magic` must not equal `erased_byte`, the byte value returned by erased flash.
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

    //= spec/ring.md#storage-metadata
    //# RING-META-002 `StorageMetadata` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    //= spec/ring.md#storage-metadata
    //# RING-META-003 `metadata_checksum` MUST be CRC-32C over every earlier `StorageMetadata` field in on-disk order.
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

    //= spec/ring.md#storage-metadata
    //# RING-META-004 Startup MUST reject the store if `metadata_checksum` is invalid or if `storage_version` is unsupported.
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
//= spec/ring.md#storage-requirements
//# `RING-STORAGE-002` Every region header MUST record the region `sequence`, `collection_id`, `collection_format`, and a checksum over the header itself.
pub struct Header {
    pub sequence: u64,
    pub collection_id: CollectionId,
    pub collection_format: u16,
}

impl Header {
    //= spec/implementation.md#memory-requirements
    //# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk format constants MUST be derivable from public constants, associated constants, or documented constructor contracts.
    pub const ENCODED_LEN: usize = size_of::<u64>() + size_of::<u64>() + size_of::<u16>() + size_of::<u32>();

    //= spec/ring.md#header
    //# RING-HEADER-001 `Header` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    //= spec/ring.md#header
    //# RING-HEADER-002 `header_checksum` MUST be CRC-32C over `sequence`, `collection_id`, and `collection_format` in on-disk order.
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
    //= spec/implementation.md#memory-requirements
    //# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk format constants MUST be derivable from public constants, associated constants, or documented constructor contracts.
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-001 `WalRegionPrologue` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-002 `prologue_checksum` MUST be CRC-32C over `wal_head_region_index`.
    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-003 `wal_head_region_index` MUST be strictly less than `region_count`.
    pub fn encode_into(
        &self,
        buffer: &mut [u8],
        region_count: u32,
    ) -> Result<usize, DiskError> {
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
    //= spec/ring.md#free-pointer-footer
    //# `RING-FREE-001` The free-pointer footer MUST occupy the final eight
    //# bytes of the region.
    //= spec/implementation.md#memory-requirements
    //# `RING-IMPL-MEM-005` Buffer-size requirements that depend on disk format constants MUST be derivable from public constants, associated constants, or documented constructor contracts.
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;

    //= spec/ring.md#free-pointer-footer
    //# RING-FREE-002 If all eight footer bytes equal `erased_byte`, the footer is uninitialized and represents `next_tail = none`.
    //= spec/ring.md#free-pointer-footer
    //# RING-FREE-003 Otherwise the footer MUST decode as `next_tail:u32, footer_checksum:u32`, both little-endian, with `footer_checksum` equal to CRC-32C over `next_tail`.
    pub fn encode_into(
        &self,
        buffer: &mut [u8],
        erased_byte: u8,
    ) -> Result<usize, DiskError> {
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
        //= spec/ring.md#free-pointer-footer
        //# RING-FREE-004 A checksum-valid non-erased footer MUST decode to a `u32 region_index` strictly less than `region_count`; any other value is malformed.
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

//= spec/ring.md#canonical-on-disk-encoding
//# RING-DISK-001 All fixed-width integer fields in `StorageMetadata`, `Header`, `WalRegionPrologue`, free-pointer footers, and logical WAL records MUST be encoded little-endian.
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

//= spec/ring.md#canonical-on-disk-encoding
//# RING-DISK-007 Unless a structure explicitly says otherwise, the checksum for that structure MUST cover the exact logical bytes of every earlier field in that structure, in on-disk order, and MUST exclude the checksum field itself and any later padding.
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

//= spec/implementation.md#architecture-requirements
//# `RING-IMPL-ARCH-004` Encoding and decoding code MUST be usable from pure tests without requiring live device I/O.
#[cfg(test)]
mod tests;
