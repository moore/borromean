use crate::CollectionId;
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Stable storage metadata version for the current on-disk format.
pub const STORAGE_VERSION: u32 = 2;
/// Stable `collection_format` reserved for main WAL regions.
pub const MAIN_WAL_V2_FORMAT: u16 = 0;
/// Stable `collection_format` reserved for transaction-log regions.
pub const TRANSACTION_LOG_V2_FORMAT: u16 = 1;
/// Stable `collection_format` reserved for free-space metadata regions.
pub const FREE_SPACE_V2_FORMAT: u16 = 2;
/// Backwards-compatible name for the current main-WAL format.
pub const WAL_V1_FORMAT: u16 = MAIN_WAL_V2_FORMAT;

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
    /// Metadata used an invalid transaction-log count.
    InvalidTransactionLogCount {
        /// Configured transaction-log count.
        transaction_log_count: u32,
        /// Total formatted region count.
        region_count: u32,
    },
    /// An optional region index tag had an invalid discriminant.
    InvalidOptRegionTag(u8),
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
    /// Maximum number of concurrently initialized transaction-log slots.
    pub transaction_log_count: u32,
    /// WAL write alignment in bytes.
    pub wal_write_granule: u32,
    /// Erased flash byte value.
    pub erased_byte: u8,
    /// Magic byte that prefixes encoded WAL records.
    pub wal_record_magic: u8,
}

impl StorageMetadata {
    /// Encoded byte length of [`StorageMetadata`].
    pub const ENCODED_LEN: usize = size_of::<u32>() * 7 + size_of::<u8>() * 2;

    /// Constructs validated storage metadata for a formatted store.
    pub fn new(
        region_size: u32,
        region_count: u32,
        min_free_regions: u32,
        wal_write_granule: u32,
        erased_byte: u8,
        wal_record_magic: u8,
    ) -> Result<Self, DiskError> {
        Self::new_with_transaction_logs(
            region_size,
            region_count,
            min_free_regions,
            1,
            wal_write_granule,
            erased_byte,
            wal_record_magic,
        )
    }

    /// Constructs validated storage metadata with an explicit transaction-log count.
    pub fn new_with_transaction_logs(
        region_size: u32,
        region_count: u32,
        min_free_regions: u32,
        transaction_log_count: u32,
        wal_write_granule: u32,
        erased_byte: u8,
        wal_record_magic: u8,
    ) -> Result<Self, DiskError> {
        let metadata = Self {
            storage_version: STORAGE_VERSION,
            region_size,
            region_count,
            min_free_regions,
            transaction_log_count,
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

        if self.transaction_log_count == 0 || self.transaction_log_count >= self.region_count {
            return Err(DiskError::InvalidTransactionLogCount {
                transaction_log_count: self.transaction_log_count,
                region_count: self.region_count,
            });
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
        offset = write_u32(buffer, offset, self.transaction_log_count)?;
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
        let transaction_log_count = read_u32(buffer, &mut offset)?;
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
            transaction_log_count,
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

        let end = Header::ENCODED_LEN + LogRegionPrologue::ENCODED_LEN;
        let aligned = end.div_ceil(granule) * granule;
        Ok(aligned)
    }
}

pub(crate) fn encode_log_region_prefix(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    collection_format: u16,
    log_head: u32,
    cursors: FreeSpaceCursors,
) -> Result<usize, DiskError> {
    let prefix_len = metadata.wal_record_area_offset()?;
    ensure_len(buffer, prefix_len)?;
    buffer[..prefix_len].fill(metadata.erased_byte);

    let header = Header {
        sequence,
        collection_id: CollectionId(0),
        collection_format,
    };
    header.encode_into(buffer)?;

    let prologue = LogRegionPrologue {
        log_head_region_index: log_head,
        allocation_head: cursors.allocation_head,
        ready_boundary: cursors.ready_boundary,
        append_tail: cursors.append_tail,
    };
    prologue.encode_into(
        &mut buffer[Header::ENCODED_LEN..Header::ENCODED_LEN + LogRegionPrologue::ENCODED_LEN],
        metadata.region_count,
    )?;
    Ok(prefix_len)
}

pub fn encode_wal_region_prefix(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    wal_head: u32,
) -> Result<usize, DiskError> {
    encode_log_region_prefix(
        buffer,
        metadata,
        sequence,
        MAIN_WAL_V2_FORMAT,
        wal_head,
        FreeSpaceCursors::ZERO,
    )
}

pub fn encode_wal_region_prefix_with_cursors(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    wal_head: u32,
    allocation_head: FreeQueuePosition,
    ready_boundary: FreeQueuePosition,
    append_tail: FreeQueuePosition,
) -> Result<usize, DiskError> {
    encode_log_region_prefix(
        buffer,
        metadata,
        sequence,
        MAIN_WAL_V2_FORMAT,
        wal_head,
        FreeSpaceCursors::new(allocation_head, ready_boundary, append_tail),
    )
}

pub fn encode_transaction_log_region_prefix_with_cursors(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    transaction_log_head: u32,
    allocation_head: FreeQueuePosition,
    ready_boundary: FreeQueuePosition,
    append_tail: FreeQueuePosition,
) -> Result<usize, DiskError> {
    encode_log_region_prefix(
        buffer,
        metadata,
        sequence,
        TRANSACTION_LOG_V2_FORMAT,
        transaction_log_head,
        FreeSpaceCursors::new(allocation_head, ready_boundary, append_tail),
    )
}

pub fn free_queue_position_for_contiguous_metadata(
    first_metadata_region: u32,
    metadata_region_count: u32,
    entries_per_region: usize,
    queue_index: u32,
) -> Result<FreeQueuePosition, DiskError> {
    if metadata_region_count == 0 || entries_per_region == 0 {
        return Err(DiskError::BufferTooSmall {
            needed: 1,
            available: 0,
        });
    }
    let entries_per_region =
        u32::try_from(entries_per_region).map_err(|_| DiskError::BufferTooSmall {
            needed: entries_per_region,
            available: u32::MAX as usize,
        })?;
    let segment = (queue_index / entries_per_region).min(metadata_region_count - 1);
    let region_index =
        first_metadata_region
            .checked_add(segment)
            .ok_or(DiskError::InvalidRegionIndex {
                region_index: u32::MAX,
                region_count: u32::MAX,
            })?;
    let base = segment
        .checked_mul(entries_per_region)
        .ok_or(DiskError::BufferTooSmall {
            needed: usize::MAX,
            available: entries_per_region as usize,
        })?;
    Ok(FreeQueuePosition {
        region_index,
        entry_index: queue_index - base,
    })
}

/// Encodes a single free-space metadata region with the supplied entries.
pub fn encode_free_space_region(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    region_index: u32,
    entries: &[u32],
) -> Result<usize, DiskError> {
    let entry_count = u32::try_from(entries.len()).map_err(|_| DiskError::BufferTooSmall {
        needed: entries.len(),
        available: u32::MAX as usize,
    })?;
    let tail = FreeQueuePosition {
        region_index,
        entry_index: entry_count,
    };
    encode_free_space_region_with_cursors(
        buffer,
        metadata,
        sequence,
        region_index,
        FreeSpaceCursors::new(
            FreeQueuePosition {
                region_index,
                entry_index: 0,
            },
            tail,
            tail,
        ),
        entries,
    )
}

/// Encodes a single free-space metadata region with explicit cursor state.
pub fn encode_free_space_region_with_cursors(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    region_index: u32,
    cursors: FreeSpaceCursors,
    entries: &[u32],
) -> Result<usize, DiskError> {
    encode_free_space_region_segment(
        buffer,
        metadata,
        sequence,
        region_index,
        cursors,
        None,
        entries,
    )
}

/// Encodes one region segment of the free-space metadata collection.
pub fn encode_free_space_region_segment(
    buffer: &mut [u8],
    metadata: StorageMetadata,
    sequence: u64,
    region_index: u32,
    cursors: FreeSpaceCursors,
    next_metadata_region: Option<u32>,
    entries: &[u32],
) -> Result<usize, DiskError> {
    ensure_len(
        buffer,
        Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN,
    )?;
    if region_index >= metadata.region_count {
        return Err(DiskError::InvalidRegionIndex {
            region_index,
            region_count: metadata.region_count,
        });
    }
    buffer.fill(metadata.erased_byte);

    let entry_count = u32::try_from(entries.len()).map_err(|_| DiskError::BufferTooSmall {
        needed: entries.len(),
        available: u32::MAX as usize,
    })?;
    let entries_len = entries
        .len()
        .checked_mul(FreeSpaceEntry::ENCODED_LEN)
        .ok_or(DiskError::BufferTooSmall {
            needed: usize::MAX,
            available: buffer.len(),
        })?;
    let entries_offset = Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN;
    ensure_len(buffer, entries_offset + entries_len)?;

    let header = Header {
        sequence,
        collection_id: CollectionId(0),
        collection_format: FREE_SPACE_V2_FORMAT,
    };
    header.encode_into(buffer)?;

    let mut offset = entries_offset;
    for entry in entries.iter().copied() {
        let encoded = FreeSpaceEntry {
            region_index: entry,
        };
        encoded.encode_into(
            &mut buffer[offset..offset + FreeSpaceEntry::ENCODED_LEN],
            metadata.region_count,
        )?;
        offset += FreeSpaceEntry::ENCODED_LEN;
    }
    let entries_checksum = crc32(&buffer[entries_offset..offset]);
    let prologue = FreeSpaceRegionPrologue {
        allocation_head: cursors.allocation_head,
        ready_boundary: cursors.ready_boundary,
        append_tail: cursors.append_tail,
        next_metadata_region,
        entry_count,
        entries_checksum,
    };
    prologue.encode_into(
        &mut buffer
            [Header::ENCODED_LEN..Header::ENCODED_LEN + FreeSpaceRegionPrologue::ENCODED_LEN],
        metadata.region_count,
    )?;
    Ok(offset)
}

/// Computes the checksum used for initialized free-space entries.
pub fn free_space_entries_checksum(entries: &[u8]) -> u32 {
    crc32(entries)
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

/// Log-specific prologue written after a main-WAL or transaction-log region header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRegionPrologue {
    /// Region index that replay should treat as the logical log head.
    pub log_head_region_index: u32,
    /// Free-space allocation cursor current when this log segment was initialized.
    pub allocation_head: FreeQueuePosition,
    /// Free-space ready/dirty boundary current when this log segment was initialized.
    pub ready_boundary: FreeQueuePosition,
    /// Free-space append cursor current when this log segment was initialized.
    pub append_tail: FreeQueuePosition,
}

impl LogRegionPrologue {
    /// Encoded byte length of [`LogRegionPrologue`].
    pub const ENCODED_LEN: usize =
        size_of::<u32>() + FreeQueuePosition::ENCODED_LEN * 3 + size_of::<u32>();

    /// Encodes the log prologue and validates its region references.
    pub fn encode_into(&self, buffer: &mut [u8], region_count: u32) -> Result<usize, DiskError> {
        if self.log_head_region_index >= region_count {
            return Err(DiskError::InvalidWalHeadRegionIndex {
                region_index: self.log_head_region_index,
                region_count,
            });
        }
        self.allocation_head.validate(region_count)?;
        self.ready_boundary.validate(region_count)?;
        self.append_tail.validate(region_count)?;

        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        offset = write_u32(buffer, offset, self.log_head_region_index)?;
        offset = self.allocation_head.encode_into_at(buffer, offset)?;
        offset = self.ready_boundary.encode_into_at(buffer, offset)?;
        offset = self.append_tail.encode_into_at(buffer, offset)?;
        let checksum = crc32(&buffer[..offset]);
        let offset = write_u32(buffer, offset, checksum)?;
        Ok(offset)
    }

    /// Decodes a log prologue and validates its region references.
    pub fn decode(buffer: &[u8], region_count: u32) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        let log_head_region_index = read_u32(buffer, &mut offset)?;
        let allocation_head = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let ready_boundary = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let append_tail = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;

        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }

        if log_head_region_index >= region_count {
            return Err(DiskError::InvalidWalHeadRegionIndex {
                region_index: log_head_region_index,
                region_count,
            });
        }
        allocation_head.validate(region_count)?;
        ready_boundary.validate(region_count)?;
        append_tail.validate(region_count)?;

        Ok(Self {
            log_head_region_index,
            allocation_head,
            ready_boundary,
            append_tail,
        })
    }
}

/// Backwards-compatible alias for code that still names the current log prologue as WAL-only.
pub type WalRegionPrologue = LogRegionPrologue;

/// Position inside the materialized free-space FIFO.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FreeQueuePosition {
    /// Free-space metadata region containing the entry slot.
    pub region_index: u32,
    /// Zero-based entry slot within that free-space metadata region.
    pub entry_index: u32,
}

impl FreeQueuePosition {
    /// Encoded byte length of [`FreeQueuePosition`].
    pub const ENCODED_LEN: usize = size_of::<u32>() * 2;
    /// Zero position used before free-space metadata has been loaded.
    pub const ZERO: Self = Self {
        region_index: 0,
        entry_index: 0,
    };

    /// Encodes this position into `buffer`.
    pub fn encode_into(&self, buffer: &mut [u8]) -> Result<usize, DiskError> {
        self.encode_into_at(buffer, 0)
    }

    fn encode_into_at(&self, buffer: &mut [u8], mut offset: usize) -> Result<usize, DiskError> {
        offset = write_u32(buffer, offset, self.region_index)?;
        offset = write_u32(buffer, offset, self.entry_index)?;
        Ok(offset)
    }

    /// Decodes a position from `buffer`.
    pub fn decode(buffer: &[u8]) -> Result<Self, DiskError> {
        let mut offset = 0;
        Self::decode_from(buffer, &mut offset)
    }

    fn decode_from(buffer: &[u8], offset: &mut usize) -> Result<Self, DiskError> {
        Ok(Self {
            region_index: read_u32(buffer, offset)?,
            entry_index: read_u32(buffer, offset)?,
        })
    }

    fn validate(self, region_count: u32) -> Result<(), DiskError> {
        if self.region_index >= region_count {
            return Err(DiskError::InvalidRegionIndex {
                region_index: self.region_index,
                region_count,
            });
        }
        Ok(())
    }
}

/// Cursor checkpoint for the materialized free-space FIFO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeSpaceCursors {
    /// Current allocation head.
    pub allocation_head: FreeQueuePosition,
    /// Current ready/dirty boundary.
    pub ready_boundary: FreeQueuePosition,
    /// Current append tail.
    pub append_tail: FreeQueuePosition,
}

impl FreeSpaceCursors {
    /// Zero cursor set used before free-space metadata has been loaded.
    pub const ZERO: Self = Self {
        allocation_head: FreeQueuePosition::ZERO,
        ready_boundary: FreeQueuePosition::ZERO,
        append_tail: FreeQueuePosition::ZERO,
    };

    /// Builds a cursor set from explicit positions.
    pub const fn new(
        allocation_head: FreeQueuePosition,
        ready_boundary: FreeQueuePosition,
        append_tail: FreeQueuePosition,
    ) -> Self {
        Self {
            allocation_head,
            ready_boundary,
            append_tail,
        }
    }
}

/// Prologue stored in each `free_space_v2` metadata region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeSpaceRegionPrologue {
    /// Current allocation head checkpoint.
    pub allocation_head: FreeQueuePosition,
    /// Current ready/dirty boundary checkpoint.
    pub ready_boundary: FreeQueuePosition,
    /// Current append tail checkpoint.
    pub append_tail: FreeQueuePosition,
    /// Next free-space metadata region in FIFO order.
    pub next_metadata_region: Option<u32>,
    /// Number of initialized entries in this metadata region.
    pub entry_count: u32,
    /// CRC-32C of initialized entry bytes.
    pub entries_checksum: u32,
}

impl FreeSpaceRegionPrologue {
    /// Encoded byte length of [`FreeSpaceRegionPrologue`].
    pub const ENCODED_LEN: usize =
        FreeQueuePosition::ENCODED_LEN * 3 + size_of::<u8>() + size_of::<u32>() * 4;

    /// Encodes this prologue into `buffer`.
    pub fn encode_into(&self, buffer: &mut [u8], region_count: u32) -> Result<usize, DiskError> {
        self.allocation_head.validate(region_count)?;
        self.ready_boundary.validate(region_count)?;
        self.append_tail.validate(region_count)?;
        if let Some(next) = self.next_metadata_region {
            if next >= region_count {
                return Err(DiskError::InvalidRegionIndex {
                    region_index: next,
                    region_count,
                });
            }
        }
        ensure_len(buffer, Self::ENCODED_LEN)?;

        let mut offset = 0;
        offset = self.allocation_head.encode_into_at(buffer, offset)?;
        offset = self.ready_boundary.encode_into_at(buffer, offset)?;
        offset = self.append_tail.encode_into_at(buffer, offset)?;
        offset = write_opt_region_index(buffer, offset, self.next_metadata_region)?;
        offset = write_u32(buffer, offset, self.entry_count)?;
        offset = write_u32(buffer, offset, self.entries_checksum)?;
        let checksum = crc32(&buffer[..offset]);
        let offset = write_u32(buffer, offset, checksum)?;
        Ok(offset)
    }

    /// Decodes this prologue from `buffer`.
    pub fn decode(buffer: &[u8], region_count: u32) -> Result<Self, DiskError> {
        ensure_len(buffer, Self::ENCODED_LEN)?;
        let mut offset = 0;
        let allocation_head = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let ready_boundary = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let append_tail = FreeQueuePosition::decode_from(buffer, &mut offset)?;
        let next_metadata_region = read_opt_region_index(buffer, &mut offset)?;
        let entry_count = read_u32(buffer, &mut offset)?;
        let entries_checksum = read_u32(buffer, &mut offset)?;
        let checksum = read_u32(buffer, &mut offset)?;

        let expected = crc32(&buffer[..offset - size_of::<u32>()]);
        if checksum != expected {
            return Err(DiskError::InvalidChecksum);
        }
        allocation_head.validate(region_count)?;
        ready_boundary.validate(region_count)?;
        append_tail.validate(region_count)?;
        if let Some(next) = next_metadata_region {
            if next >= region_count {
                return Err(DiskError::InvalidRegionIndex {
                    region_index: next,
                    region_count,
                });
            }
        }

        Ok(Self {
            allocation_head,
            ready_boundary,
            append_tail,
            next_metadata_region,
            entry_count,
            entries_checksum,
        })
    }
}

/// One materialized free-space FIFO entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeSpaceEntry {
    /// Physical free region named by this FIFO entry.
    pub region_index: u32,
}

impl FreeSpaceEntry {
    /// Encoded byte length of [`FreeSpaceEntry`].
    pub const ENCODED_LEN: usize = size_of::<u32>();

    /// Encodes this entry into `buffer`.
    pub fn encode_into(&self, buffer: &mut [u8], region_count: u32) -> Result<usize, DiskError> {
        if self.region_index >= region_count {
            return Err(DiskError::InvalidRegionIndex {
                region_index: self.region_index,
                region_count,
            });
        }
        write_u32(buffer, 0, self.region_index)
    }

    /// Decodes this entry from `buffer`.
    pub fn decode(buffer: &[u8], region_count: u32) -> Result<Self, DiskError> {
        let mut offset = 0;
        let region_index = read_u32(buffer, &mut offset)?;
        if region_index >= region_count {
            return Err(DiskError::InvalidRegionIndex {
                region_index,
                region_count,
            });
        }
        Ok(Self { region_index })
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

fn write_opt_region_index(
    buffer: &mut [u8],
    offset: usize,
    region_index: Option<u32>,
) -> Result<usize, DiskError> {
    match region_index {
        Some(region_index) => {
            let offset = write_u8(buffer, offset, 1)?;
            write_u32(buffer, offset, region_index)
        }
        None => {
            let offset = write_u8(buffer, offset, 0)?;
            write_u32(buffer, offset, 0)
        }
    }
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

fn read_opt_region_index(buffer: &[u8], offset: &mut usize) -> Result<Option<u32>, DiskError> {
    let tag = read_u8(buffer, offset)?;
    let value = read_u32(buffer, offset)?;
    match tag {
        0 => Ok(None),
        1 => Ok(Some(value)),
        tag => Err(DiskError::InvalidOptRegionTag(tag)),
    }
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
