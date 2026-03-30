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
    //# RING-FREE-001 The free-pointer footer MUST occupy the final eight bytes of the region.
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
mod tests {
    use super::*;

    //= spec/ring.md#storage-metadata
    //# RING-META-001 The canonical on-disk `storage_version` defined by this specification MUST be `1`.
    #[test]
    fn storage_metadata_uses_storage_version_1() {
        let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
        assert_eq!(metadata.storage_version, STORAGE_VERSION);
    }

    //= spec/ring.md#storage-metadata
    //# RING-META-002 `StorageMetadata` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    #[test]
    fn storage_metadata_encodes_fields_in_canonical_order() {
        let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
        let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
        let used = metadata.encode_into(&mut buffer).unwrap();
        assert_eq!(used, StorageMetadata::ENCODED_LEN);

        let expected_prefix = [
            1u32.to_le_bytes().as_slice(),
            4096u32.to_le_bytes().as_slice(),
            32u32.to_le_bytes().as_slice(),
            3u32.to_le_bytes().as_slice(),
            8u32.to_le_bytes().as_slice(),
            &[0xff],
            &[0xa5],
        ]
        .concat();
        assert_eq!(&buffer[..expected_prefix.len()], expected_prefix.as_slice());
    }

    //= spec/ring.md#storage-metadata
    //# RING-META-003 `metadata_checksum` MUST be CRC-32C over every earlier `StorageMetadata` field in on-disk order.
    #[test]
    fn storage_metadata_checksum_covers_prior_fields() {
        let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
        let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
        metadata.encode_into(&mut buffer).unwrap();

        let checksum_offset = StorageMetadata::ENCODED_LEN - size_of::<u32>();
        let expected = crc32(&buffer[..checksum_offset]);
        let mut checksum_bytes = [0u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
        assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
    }

    //= spec/ring.md#storage-metadata
    //# RING-META-004 Startup MUST reject the store if `metadata_checksum` is invalid or if `storage_version` is unsupported.
    #[test]
    fn storage_metadata_decode_rejects_bad_checksum() {
        let metadata = StorageMetadata::new(4096, 32, 3, 8, 0xff, 0xa5).unwrap();
        let mut buffer = [0u8; StorageMetadata::ENCODED_LEN];
        metadata.encode_into(&mut buffer).unwrap();
        buffer[0] ^= 0x01;

        let error = StorageMetadata::decode(&buffer).unwrap_err();
        assert_eq!(error, DiskError::InvalidChecksum);
    }

    //= spec/ring.md#header
    //# RING-HEADER-001 `Header` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    #[test]
    fn header_encodes_fields_in_canonical_order() {
        let header = Header {
            sequence: 9,
            collection_id: CollectionId(7),
            collection_format: WAL_V1_FORMAT,
        };
        let mut buffer = [0u8; Header::ENCODED_LEN];
        header.encode_into(&mut buffer).unwrap();

        let expected_prefix = [
            9u64.to_le_bytes().as_slice(),
            7u64.to_le_bytes().as_slice(),
            WAL_V1_FORMAT.to_le_bytes().as_slice(),
        ]
        .concat();
        assert_eq!(&buffer[..expected_prefix.len()], expected_prefix.as_slice());
    }

    //= spec/ring.md#header
    //# RING-HEADER-002 `header_checksum` MUST be CRC-32C over `sequence`, `collection_id`, and `collection_format` in on-disk order.
    #[test]
    fn header_checksum_covers_prefix_fields() {
        let header = Header {
            sequence: 9,
            collection_id: CollectionId(7),
            collection_format: WAL_V1_FORMAT,
        };
        let mut buffer = [0u8; Header::ENCODED_LEN];
        header.encode_into(&mut buffer).unwrap();

        let checksum_offset = Header::ENCODED_LEN - size_of::<u32>();
        let expected = crc32(&buffer[..checksum_offset]);
        let mut checksum_bytes = [0u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
        assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
    }

    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-001 `WalRegionPrologue` MUST be encoded as the exact byte sequence of the fields shown above, in that order, with no implicit padding.
    #[test]
    fn wal_prologue_encodes_fields_in_canonical_order() {
        let prologue = WalRegionPrologue {
            wal_head_region_index: 3,
        };
        let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
        prologue.encode_into(&mut buffer, 8).unwrap();

        assert_eq!(&buffer[..size_of::<u32>()], 3u32.to_le_bytes().as_slice());
    }

    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-002 `prologue_checksum` MUST be CRC-32C over `wal_head_region_index`.
    #[test]
    fn wal_prologue_checksum_covers_head_region_index() {
        let prologue = WalRegionPrologue {
            wal_head_region_index: 3,
        };
        let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];
        prologue.encode_into(&mut buffer, 8).unwrap();

        let checksum_offset = WalRegionPrologue::ENCODED_LEN - size_of::<u32>();
        let expected = crc32(&buffer[..checksum_offset]);
        let mut checksum_bytes = [0u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&buffer[checksum_offset..]);
        assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
    }

    //= spec/ring.md#wal-region-prologue
    //# RING-PROLOGUE-003 `wal_head_region_index` MUST be strictly less than `region_count`.
    #[test]
    fn wal_prologue_rejects_out_of_range_head() {
        let prologue = WalRegionPrologue {
            wal_head_region_index: 4,
        };
        let mut buffer = [0u8; WalRegionPrologue::ENCODED_LEN];

        let error = prologue.encode_into(&mut buffer, 4).unwrap_err();
        assert_eq!(
            error,
            DiskError::InvalidWalHeadRegionIndex {
                region_index: 4,
                region_count: 4,
            }
        );
    }

    //= spec/ring.md#free-pointer-footer
    //# RING-FREE-003 Otherwise the footer MUST decode as `next_tail:u32, footer_checksum:u32`, both little-endian, with `footer_checksum` equal to CRC-32C over `next_tail`.
    #[test]
    fn free_pointer_footer_uses_crc32c_for_non_erased_value() {
        let footer = FreePointerFooter { next_tail: Some(11) };
        let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
        footer.encode_into(&mut buffer, 0xff).unwrap();

        let expected = crc32(&11u32.to_le_bytes());
        let mut checksum_bytes = [0u8; size_of::<u32>()];
        checksum_bytes.copy_from_slice(&buffer[size_of::<u32>()..]);
        assert_eq!(u32::from_le_bytes(checksum_bytes), expected);
        assert_eq!(FreePointerFooter::decode(&buffer, 0xff).unwrap(), footer);
    }

    //= spec/ring.md#free-pointer-footer
    //# RING-FREE-002 If all eight footer bytes equal `erased_byte`, the footer is uninitialized and represents `next_tail = none`.
    #[test]
    fn free_pointer_footer_none_uses_erased_bytes() {
        let footer = FreePointerFooter { next_tail: None };
        let mut buffer = [0u8; FreePointerFooter::ENCODED_LEN];
        footer.encode_into(&mut buffer, 0xff).unwrap();

        assert!(buffer.iter().all(|byte| *byte == 0xff));
        let decoded = FreePointerFooter::decode(&buffer, 0xff).unwrap();
        assert_eq!(decoded, footer);
    }

    #[test]
    fn wal_record_area_offset_is_granule_aligned() {
        let metadata = StorageMetadata::new(4096, 32, 3, 16, 0xff, 0xa5).unwrap();
        let offset = metadata.wal_record_area_offset().unwrap();
        assert_eq!(offset % 16, 0);
        assert!(offset >= Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN);
    }
}
