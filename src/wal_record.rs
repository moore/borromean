use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

use crate::disk::{DiskError, FreeQueuePosition, StorageMetadata};
use crate::CollectionId;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Errors returned while encoding or decoding WAL records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordError {
    /// Disk metadata validation failed.
    Disk(DiskError),
    /// A scratch or output buffer was too small.
    BufferTooSmall {
        /// Required buffer length in bytes.
        needed: usize,
        /// Available buffer length in bytes.
        available: usize,
    },
    /// The record magic byte did not match storage metadata.
    InvalidRecordMagic {
        /// Byte found in the encoded record.
        found: u8,
        /// Byte expected from metadata.
        expected: u8,
    },
    /// An escaped byte sequence was malformed.
    InvalidEscapeSequence(u8),
    /// A reserved physical byte appeared unescaped in a WAL record body.
    InvalidUnescapedReservedByte {
        /// Reserved byte found in the encoded record body.
        found: u8,
    },
    /// Padding bytes after a record were not valid escape padding.
    InvalidPadding(u8),
    /// The record type byte was unknown.
    InvalidRecordType(u8),
    /// Decoding reached the payload header before a record type was known.
    MissingRecordType,
    /// An optional region tag had an invalid discriminant.
    InvalidOptRegionTag(u8),
    /// Record checksum validation failed.
    InvalidChecksum,
    /// A record payload length did not match the record type.
    PayloadLengthMismatch {
        /// Record type being decoded.
        record_type: WalRecordType,
        /// Payload length stored in the record.
        payload_len: u32,
    },
    /// A checked length conversion or addition overflowed.
    LengthOverflow,
}

impl From<DiskError> for WalRecordError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

/// Escape bytes derived from storage metadata for WAL encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEscapeCodes {
    /// Escape prefix byte.
    pub wal_escape_byte: u8,
    /// Escape code representing the erased byte value.
    pub wal_escape_code_erased: u8,
    /// Escape code representing the WAL magic byte.
    pub wal_escape_code_magic: u8,
    /// Escape code representing the escape byte itself.
    pub wal_escape_code_escape: u8,
}

impl WalEscapeCodes {
    /// Derives a stable set of escape bytes from metadata values.
    pub fn derive(erased_byte: u8, wal_record_magic: u8) -> Self {
        let mut values = [0u8; 4];
        let mut count = 0usize;
        for raw in 0u16..=u8::MAX as u16 {
            let byte = raw as u8;
            if byte == erased_byte || byte == wal_record_magic {
                continue;
            }

            values[count] = byte;
            count += 1;
            if count == values.len() {
                break;
            }
        }

        Self {
            wal_escape_byte: values[0],
            wal_escape_code_erased: values[1],
            wal_escape_code_magic: values[2],
            wal_escape_code_escape: values[3],
        }
    }
}

/// Stable logical WAL record kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    /// Creates a new collection.
    NewCollection,
    /// Appends a collection-specific update payload.
    Update,
    /// Appends a collection-specific snapshot payload.
    Snapshot,
    /// Reserves the current ready free-space entry.
    AllocateRegion,
    /// Commits a new collection or WAL head.
    Head,
    /// Drops a collection.
    DropCollection,
    /// Links one WAL region to the next.
    Link,
    /// Publishes erasure of one or more dirty free-space entries.
    EraseFreeRegionSpan,
    /// Opens a bounded inline transaction in the main WAL.
    BeginInlineTransaction,
    /// Commits a bounded inline transaction in the main WAL.
    CommitInlineTransaction,
    /// Marks a WAL recovery boundary.
    WalRecovery,
    /// Adds a detached region to the dirty free-space tail.
    FreeRegion,
    /// Starts a collection-scoped WAL transaction.
    BeginTransaction,
    /// Marks the transaction collection-state commit point.
    CommitTransaction,
    /// Marks transaction cleanup complete.
    TransactionFinished,
    /// Marks transaction pre-commit recovery complete.
    RollbackTransaction,
    /// Enrolls a collection into the active transaction-log state.
    AddTransactionCollection,
    /// Rolls back a bounded inline transaction in the main WAL.
    RollbackInlineTransaction,
    /// Stages a transaction-private free intent.
    FreeIntent,
}

impl WalRecordType {
    /// Returns the stable type byte used in WAL encoding.
    pub fn code(self) -> u8 {
        match self {
            Self::NewCollection => 0x01,
            Self::Update => 0x02,
            Self::Snapshot => 0x03,
            Self::AllocateRegion => 0x04,
            Self::Head => 0x05,
            Self::DropCollection => 0x06,
            Self::Link => 0x07,
            Self::EraseFreeRegionSpan => 0x08,
            Self::BeginInlineTransaction => 0x09,
            Self::CommitInlineTransaction => 0x0a,
            Self::WalRecovery => 0x0b,
            Self::FreeRegion => 0x0c,
            Self::BeginTransaction => 0x0d,
            Self::CommitTransaction => 0x0e,
            Self::TransactionFinished => 0x0f,
            Self::RollbackTransaction => 0x10,
            Self::AddTransactionCollection => 0x11,
            Self::RollbackInlineTransaction => 0x12,
            Self::FreeIntent => 0x13,
        }
    }

    /// Decodes a stable type byte into a [`WalRecordType`].
    pub fn decode(code: u8) -> Result<Self, WalRecordError> {
        match code {
            0x01 => Ok(Self::NewCollection),
            0x02 => Ok(Self::Update),
            0x03 => Ok(Self::Snapshot),
            0x04 => Ok(Self::AllocateRegion),
            0x05 => Ok(Self::Head),
            0x06 => Ok(Self::DropCollection),
            0x07 => Ok(Self::Link),
            0x08 => Ok(Self::EraseFreeRegionSpan),
            0x09 => Ok(Self::BeginInlineTransaction),
            0x0a => Ok(Self::CommitInlineTransaction),
            0x0b => Ok(Self::WalRecovery),
            0x0c => Ok(Self::FreeRegion),
            0x0d => Ok(Self::BeginTransaction),
            0x0e => Ok(Self::CommitTransaction),
            0x0f => Ok(Self::TransactionFinished),
            0x10 => Ok(Self::RollbackTransaction),
            0x11 => Ok(Self::AddTransactionCollection),
            0x12 => Ok(Self::RollbackInlineTransaction),
            0x13 => Ok(Self::FreeIntent),
            _ => Err(WalRecordError::InvalidRecordType(code)),
        }
    }

    fn has_collection_id(self) -> bool {
        matches!(
            self,
            Self::NewCollection
                | Self::Update
                | Self::Snapshot
                | Self::Head
                | Self::DropCollection
                | Self::AddTransactionCollection
                | Self::FreeIntent
        )
    }

    fn has_collection_type(self) -> bool {
        matches!(self, Self::NewCollection | Self::Snapshot | Self::Head)
    }
}

/// Position inside a main WAL or transaction-log chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    /// Region containing the position.
    pub region_index: u32,
    /// Byte offset within the region.
    pub offset: u32,
}

/// Frozen range inside one transaction log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionLogRange {
    /// Inclusive start position.
    pub start: LogPosition,
    /// Exclusive end position.
    pub end: LogPosition,
}

/// Final-segment seal carried by `commit_transaction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionCommitSeal {
    /// First byte of the final segment's sealed private suffix.
    pub final_free_intent_start: LogPosition,
    /// First byte after the final segment's sealed private suffix.
    pub final_segment_end: LogPosition,
}

/// Borrowed logical WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecord<'a> {
    /// `new_collection(collection_id, collection_type)`.
    NewCollection {
        /// Collection being created.
        collection_id: CollectionId,
        /// Stable collection type code.
        collection_type: u16,
    },
    /// `update(collection_id, payload)`.
    Update {
        /// Collection being updated.
        collection_id: CollectionId,
        /// Collection-specific update payload bytes.
        payload: &'a [u8],
    },
    /// `snapshot(collection_id, collection_type, payload)`.
    Snapshot {
        /// Collection whose basis is being snapshotted.
        collection_id: CollectionId,
        /// Stable collection type code.
        collection_type: u16,
        /// Collection-specific snapshot payload bytes.
        payload: &'a [u8],
    },
    /// `allocate_region(region_index, allocation_head_after)`.
    AllocateRegion {
        /// Region at the current ready allocation head.
        region_index: u32,
        /// Allocation cursor after the pop.
        allocation_head_after: FreeQueuePosition,
    },
    /// `head(collection_id, collection_type, region_index)`.
    Head {
        /// Collection whose head is being updated.
        collection_id: CollectionId,
        /// Stable collection type code.
        collection_type: u16,
        /// New committed region or WAL head region.
        region_index: u32,
    },
    /// `drop_collection(collection_id)`.
    DropCollection {
        /// Collection being dropped.
        collection_id: CollectionId,
    },
    /// `link(next_region_index, expected_sequence)`.
    Link {
        /// Next WAL region in the chain.
        next_region_index: u32,
        /// Sequence expected in the linked region header.
        expected_sequence: u64,
    },
    /// `erase_free_region_span(count, ready_boundary_after)`.
    EraseFreeRegionSpan {
        /// Number of dirty entries erased.
        count: u32,
        /// Ready boundary after the erased span.
        ready_boundary_after: FreeQueuePosition,
    },
    /// `begin_inline_transaction(record_count, encoded_len)`.
    BeginInlineTransaction {
        /// Number of body records in the bounded transaction.
        record_count: u32,
        /// Encoded physical length of the body records.
        encoded_len: u32,
    },
    /// `commit_inline_transaction(record_count)`.
    CommitInlineTransaction {
        /// Number of body records being committed.
        record_count: u32,
    },
    /// `free_region(region_index, append_tail_after)`.
    FreeRegion {
        /// Detached region being appended to the free-space collection.
        region_index: u32,
        /// Append cursor after the dirty entry is added.
        append_tail_after: FreeQueuePosition,
    },
    /// `begin_transaction(transaction_log_id, start)`.
    BeginTransaction {
        /// Transaction log slot selected for this transaction.
        transaction_log_id: u32,
        /// Transaction-log start position.
        start: LogPosition,
    },
    /// `commit_transaction(transaction_log_id, range, seal)`.
    CommitTransaction {
        /// Transaction log slot selected for this transaction.
        transaction_log_id: u32,
        /// Frozen transaction-log range being imported.
        range: TransactionLogRange,
        /// Final transaction-log segment seal.
        seal: TransactionCommitSeal,
    },
    /// `transaction_finished(transaction_log_id, range)`.
    TransactionFinished {
        /// Transaction log slot selected for this transaction.
        transaction_log_id: u32,
        /// Transaction-log range whose cleanup is complete.
        range: TransactionLogRange,
    },
    /// `rollback_transaction(transaction_log_id, range)`.
    RollbackTransaction {
        /// Transaction log slot selected for this transaction.
        transaction_log_id: u32,
        /// Transaction-log range that remained non-visible.
        range: TransactionLogRange,
    },
    /// `add_transaction_collection(collection_id, observed_collection_generation)`.
    AddTransactionCollection {
        /// Collection being enrolled into the transaction.
        collection_id: CollectionId,
        /// Committed generation observed when the collection was enrolled.
        observed_collection_generation: u64,
    },
    /// `rollback_inline_transaction(record_count)`.
    RollbackInlineTransaction {
        /// Number of body records being rolled back.
        record_count: u32,
    },
    /// `free_intent(collection_id, region_index)`.
    FreeIntent {
        /// Collection whose live region is being staged for transactional free.
        collection_id: CollectionId,
        /// Region that remains live until the transaction commits.
        region_index: u32,
    },
    /// `wal_recovery()`.
    WalRecovery,
}

impl<'a> WalRecord<'a> {
    /// Returns the logical type of this record.
    pub fn record_type(self) -> WalRecordType {
        match self {
            Self::NewCollection { .. } => WalRecordType::NewCollection,
            Self::Update { .. } => WalRecordType::Update,
            Self::Snapshot { .. } => WalRecordType::Snapshot,
            Self::AllocateRegion { .. } => WalRecordType::AllocateRegion,
            Self::Head { .. } => WalRecordType::Head,
            Self::DropCollection { .. } => WalRecordType::DropCollection,
            Self::Link { .. } => WalRecordType::Link,
            Self::EraseFreeRegionSpan { .. } => WalRecordType::EraseFreeRegionSpan,
            Self::BeginInlineTransaction { .. } => WalRecordType::BeginInlineTransaction,
            Self::CommitInlineTransaction { .. } => WalRecordType::CommitInlineTransaction,
            Self::WalRecovery => WalRecordType::WalRecovery,
            Self::FreeRegion { .. } => WalRecordType::FreeRegion,
            Self::BeginTransaction { .. } => WalRecordType::BeginTransaction,
            Self::CommitTransaction { .. } => WalRecordType::CommitTransaction,
            Self::TransactionFinished { .. } => WalRecordType::TransactionFinished,
            Self::RollbackTransaction { .. } => WalRecordType::RollbackTransaction,
            Self::AddTransactionCollection { .. } => WalRecordType::AddTransactionCollection,
            Self::RollbackInlineTransaction { .. } => WalRecordType::RollbackInlineTransaction,
            Self::FreeIntent { .. } => WalRecordType::FreeIntent,
        }
    }
}

/// Decoded WAL record plus physical and logical lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedWalRecord<'a> {
    /// Decoded logical record.
    pub record: WalRecord<'a>,
    /// Number of encoded bytes consumed from the physical WAL.
    pub encoded_len: usize,
    /// Number of decoded logical bytes before escaping and padding.
    pub logical_len: usize,
}

/// Encodes a logical record into a WAL byte buffer.
pub fn encode_record_into(
    record: WalRecord<'_>,
    metadata: StorageMetadata,
    output: &mut [u8],
    logical_scratch: &mut [u8],
) -> Result<usize, WalRecordError> {
    metadata.validate()?;

    let logical_len = encode_logical_record(record, logical_scratch)?;
    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| DiskError::InvalidWalWriteGranule)?;
    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);

    ensure_len(output, 1)?;
    output[0] = metadata.wal_record_magic;

    let mut physical_offset = 1usize;
    for logical_byte in logical_scratch[..logical_len].iter().copied() {
        physical_offset = encode_logical_byte(
            logical_byte,
            output,
            physical_offset,
            metadata,
            escape_codes,
        )?;
    }

    let aligned_end = align_up(physical_offset, granule)?;
    ensure_len(output, aligned_end)?;
    output[physical_offset..aligned_end].fill(escape_codes.wal_escape_code_escape);
    Ok(aligned_end)
}

/// Returns the encoded byte length of a logical WAL record.
pub fn encoded_record_len(
    record: WalRecord<'_>,
    metadata: StorageMetadata,
    physical_scratch: &mut [u8],
    logical_scratch: &mut [u8],
) -> Result<usize, WalRecordError> {
    encode_record_into(record, metadata, physical_scratch, logical_scratch)
}

/// Decodes a WAL record from the supplied physical bytes.
pub fn decode_record<'a>(
    input: &[u8],
    metadata: StorageMetadata,
    logical_scratch: &'a mut [u8],
) -> Result<DecodedWalRecord<'a>, WalRecordError> {
    metadata.validate()?;
    ensure_len(input, 1)?;
    if input[0] != metadata.wal_record_magic {
        return Err(WalRecordError::InvalidRecordMagic {
            found: input[0],
            expected: metadata.wal_record_magic,
        });
    }

    let granule = usize::try_from(metadata.wal_write_granule)
        .map_err(|_| DiskError::InvalidWalWriteGranule)?;
    let escape_codes = WalEscapeCodes::derive(metadata.erased_byte, metadata.wal_record_magic);

    let mut physical_offset = 1usize;
    let mut logical_offset = 0usize;
    let mut record_type = None;
    let mut payload_header_end = None;
    let mut total_logical_len = None;

    loop {
        if let Some(total_len) = total_logical_len {
            if logical_offset >= total_len {
                break;
            }
        }

        let logical_byte =
            decode_logical_byte(input, &mut physical_offset, metadata, escape_codes)?;
        ensure_len(logical_scratch, logical_offset + 1)?;
        logical_scratch[logical_offset] = logical_byte;
        logical_offset += 1;

        if record_type.is_none() {
            let decoded_type = WalRecordType::decode(logical_byte)?;
            let prefix_len = 1usize
                + if decoded_type.has_collection_id() {
                    size_of::<u64>()
                } else {
                    0
                }
                + if decoded_type.has_collection_type() {
                    size_of::<u16>()
                } else {
                    0
                };
            record_type = Some(decoded_type);
            payload_header_end = Some(prefix_len + size_of::<u32>());
        }

        let Some(payload_header_end) = payload_header_end else {
            continue;
        };

        if matches!(
            logical_offset.cmp(&payload_header_end),
            core::cmp::Ordering::Less
        ) {
            continue;
        }

        let payload_len = u32::from_le_bytes(
            logical_scratch[payload_header_end - size_of::<u32>()..payload_header_end]
                .try_into()
                .map_err(|_| WalRecordError::LengthOverflow)?,
        );
        let payload_len =
            usize::try_from(payload_len).map_err(|_| WalRecordError::LengthOverflow)?;
        let prefix_and_payload_len = payload_header_end
            .checked_add(payload_len)
            .ok_or(WalRecordError::LengthOverflow)?;

        let total_len = prefix_and_payload_len
            .checked_add(size_of::<u32>())
            .ok_or(WalRecordError::LengthOverflow)?;
        total_logical_len = Some(total_len);
    }

    let total_logical_len = total_logical_len.ok_or(WalRecordError::LengthOverflow)?;
    let checksum_offset = total_logical_len
        .checked_sub(size_of::<u32>())
        .ok_or(WalRecordError::LengthOverflow)?;
    let expected_checksum = crc32(&logical_scratch[..checksum_offset]);
    let actual_checksum = u32::from_le_bytes(
        logical_scratch[checksum_offset..total_logical_len]
            .try_into()
            .map_err(|_| WalRecordError::LengthOverflow)?,
    );
    if actual_checksum != expected_checksum {
        return Err(WalRecordError::InvalidChecksum);
    }

    let aligned_end = align_up(physical_offset, granule)?;
    ensure_len(input, aligned_end)?;
    for byte in input[physical_offset..aligned_end].iter().copied() {
        if byte != escape_codes.wal_escape_code_escape {
            return Err(WalRecordError::InvalidPadding(byte));
        }
    }

    let record = parse_logical_record(&logical_scratch[..total_logical_len])?;
    Ok(DecodedWalRecord {
        record,
        encoded_len: aligned_end,
        logical_len: total_logical_len,
    })
}

fn encode_logical_record(
    record: WalRecord<'_>,
    buffer: &mut [u8],
) -> Result<usize, WalRecordError> {
    let mut offset = 0usize;

    offset = write_u8(buffer, offset, record.record_type().code())?;
    match record {
        WalRecord::NewCollection {
            collection_id,
            collection_type,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u16(buffer, offset, collection_type)?;
            offset = write_u32(buffer, offset, 0)?;
        }
        WalRecord::Update {
            collection_id,
            payload,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u32(
                buffer,
                offset,
                u32::try_from(payload.len()).map_err(|_| WalRecordError::LengthOverflow)?,
            )?;
            offset = write_bytes(buffer, offset, payload)?;
        }
        WalRecord::Snapshot {
            collection_id,
            collection_type,
            payload,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u16(buffer, offset, collection_type)?;
            offset = write_u32(
                buffer,
                offset,
                u32::try_from(payload.len()).map_err(|_| WalRecordError::LengthOverflow)?,
            )?;
            offset = write_bytes(buffer, offset, payload)?;
        }
        WalRecord::AllocateRegion {
            region_index,
            allocation_head_after,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + free_queue_position_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, region_index)?;
            offset = write_free_queue_position(buffer, offset, allocation_head_after)?;
        }
        WalRecord::Head {
            collection_id,
            collection_type,
            region_index,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u16(buffer, offset, collection_type)?;
            offset = write_u32(buffer, offset, size_of::<u32>() as u32)?;
            offset = write_u32(buffer, offset, region_index)?;
        }
        WalRecord::DropCollection { collection_id } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u32(buffer, offset, 0)?;
        }
        WalRecord::Link {
            next_region_index,
            expected_sequence,
        } => {
            offset = write_u32(buffer, offset, (size_of::<u32>() + size_of::<u64>()) as u32)?;
            offset = write_u32(buffer, offset, next_region_index)?;
            offset = write_u64(buffer, offset, expected_sequence)?;
        }
        WalRecord::EraseFreeRegionSpan {
            count,
            ready_boundary_after,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + free_queue_position_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, count)?;
            offset = write_free_queue_position(buffer, offset, ready_boundary_after)?;
        }
        WalRecord::BeginInlineTransaction {
            record_count,
            encoded_len,
        } => {
            offset = write_u32(buffer, offset, (size_of::<u32>() * 2) as u32)?;
            offset = write_u32(buffer, offset, record_count)?;
            offset = write_u32(buffer, offset, encoded_len)?;
        }
        WalRecord::CommitInlineTransaction { record_count }
        | WalRecord::RollbackInlineTransaction { record_count } => {
            offset = write_u32(buffer, offset, size_of::<u32>() as u32)?;
            offset = write_u32(buffer, offset, record_count)?;
        }
        WalRecord::FreeRegion {
            region_index,
            append_tail_after,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + free_queue_position_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, region_index)?;
            offset = write_free_queue_position(buffer, offset, append_tail_after)?;
        }
        WalRecord::BeginTransaction {
            transaction_log_id,
            start,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + log_position_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, transaction_log_id)?;
            offset = write_log_position(buffer, offset, start)?;
        }
        WalRecord::CommitTransaction {
            transaction_log_id,
            range,
            seal,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + transaction_range_len() + transaction_commit_seal_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, transaction_log_id)?;
            offset = write_transaction_log_range(buffer, offset, range)?;
            offset = write_transaction_commit_seal(buffer, offset, seal)?;
        }
        WalRecord::TransactionFinished {
            transaction_log_id,
            range,
        }
        | WalRecord::RollbackTransaction {
            transaction_log_id,
            range,
        } => {
            offset = write_u32(
                buffer,
                offset,
                (size_of::<u32>() + transaction_range_len()) as u32,
            )?;
            offset = write_u32(buffer, offset, transaction_log_id)?;
            offset = write_transaction_log_range(buffer, offset, range)?;
        }
        WalRecord::AddTransactionCollection {
            collection_id,
            observed_collection_generation,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u32(buffer, offset, size_of::<u64>() as u32)?;
            offset = write_u64(buffer, offset, observed_collection_generation)?;
        }
        WalRecord::FreeIntent {
            collection_id,
            region_index,
        } => {
            offset = write_u64(buffer, offset, collection_id.0)?;
            offset = write_u32(buffer, offset, size_of::<u32>() as u32)?;
            offset = write_u32(buffer, offset, region_index)?;
        }
        WalRecord::WalRecovery => {
            offset = write_u32(buffer, offset, 0)?;
        }
    }

    let checksum = crc32(&buffer[..offset]);
    let offset = write_u32(buffer, offset, checksum)?;
    Ok(offset)
}

fn parse_logical_record(logical: &[u8]) -> Result<WalRecord<'_>, WalRecordError> {
    let mut offset = 0usize;
    let record_type = WalRecordType::decode(read_u8(logical, &mut offset)?)?;

    match record_type {
        WalRecordType::NewCollection => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let collection_type = read_u16(logical, &mut offset)?;
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != 0 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            Ok(WalRecord::NewCollection {
                collection_id,
                collection_type,
            })
        }
        WalRecordType::Update => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let payload = read_payload(logical, &mut offset)?;
            Ok(WalRecord::Update {
                collection_id,
                payload,
            })
        }
        WalRecordType::Snapshot => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let collection_type = read_u16(logical, &mut offset)?;
            let payload = read_payload(logical, &mut offset)?;
            Ok(WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload,
            })
        }
        WalRecordType::AllocateRegion => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() + free_queue_position_len()) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            let allocation_head_after = read_free_queue_position(logical, &mut offset)?;
            Ok(WalRecord::AllocateRegion {
                region_index,
                allocation_head_after,
            })
        }
        WalRecordType::Head => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let collection_type = read_u16(logical, &mut offset)?;
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u32>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            Ok(WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            })
        }
        WalRecordType::DropCollection => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != 0 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            Ok(WalRecord::DropCollection { collection_id })
        }
        WalRecordType::Link => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() + size_of::<u64>()) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let next_region_index = read_u32(logical, &mut offset)?;
            let expected_sequence = read_u64(logical, &mut offset)?;
            Ok(WalRecord::Link {
                next_region_index,
                expected_sequence,
            })
        }
        WalRecordType::EraseFreeRegionSpan => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() + free_queue_position_len()) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let count = read_u32(logical, &mut offset)?;
            let ready_boundary_after = read_free_queue_position(logical, &mut offset)?;
            Ok(WalRecord::EraseFreeRegionSpan {
                count,
                ready_boundary_after,
            })
        }
        WalRecordType::BeginInlineTransaction => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() * 2) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let record_count = read_u32(logical, &mut offset)?;
            let encoded_len = read_u32(logical, &mut offset)?;
            Ok(WalRecord::BeginInlineTransaction {
                record_count,
                encoded_len,
            })
        }
        WalRecordType::CommitInlineTransaction => {
            let record_count = read_inline_terminal_payload(logical, &mut offset, record_type)?;
            Ok(WalRecord::CommitInlineTransaction { record_count })
        }
        WalRecordType::FreeRegion => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() + free_queue_position_len()) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            let append_tail_after = read_free_queue_position(logical, &mut offset)?;
            Ok(WalRecord::FreeRegion {
                region_index,
                append_tail_after,
            })
        }
        WalRecordType::WalRecovery => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != 0 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            Ok(WalRecord::WalRecovery)
        }
        WalRecordType::BeginTransaction => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != (size_of::<u32>() + log_position_len()) as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let transaction_log_id = read_u32(logical, &mut offset)?;
            let start = read_log_position(logical, &mut offset)?;
            Ok(WalRecord::BeginTransaction {
                transaction_log_id,
                start,
            })
        }
        WalRecordType::CommitTransaction => {
            let (transaction_log_id, range, seal) =
                read_transaction_commit_payload(logical, &mut offset, record_type)?;
            Ok(WalRecord::CommitTransaction {
                transaction_log_id,
                range,
                seal,
            })
        }
        WalRecordType::TransactionFinished => {
            let (transaction_log_id, range) =
                read_transaction_log_control_payload(logical, &mut offset, record_type)?;
            Ok(WalRecord::TransactionFinished {
                transaction_log_id,
                range,
            })
        }
        WalRecordType::RollbackTransaction => {
            let (transaction_log_id, range) =
                read_transaction_log_control_payload(logical, &mut offset, record_type)?;
            Ok(WalRecord::RollbackTransaction {
                transaction_log_id,
                range,
            })
        }
        WalRecordType::AddTransactionCollection => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u64>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let observed_collection_generation = read_u64(logical, &mut offset)?;
            Ok(WalRecord::AddTransactionCollection {
                collection_id,
                observed_collection_generation,
            })
        }
        WalRecordType::RollbackInlineTransaction => {
            let record_count = read_inline_terminal_payload(logical, &mut offset, record_type)?;
            Ok(WalRecord::RollbackInlineTransaction { record_count })
        }
        WalRecordType::FreeIntent => {
            let collection_id = CollectionId(read_u64(logical, &mut offset)?);
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u32>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            Ok(WalRecord::FreeIntent {
                collection_id,
                region_index,
            })
        }
    }
}

fn read_inline_terminal_payload(
    logical: &[u8],
    offset: &mut usize,
    record_type: WalRecordType,
) -> Result<u32, WalRecordError> {
    let payload_len = read_u32(logical, offset)?;
    if payload_len != size_of::<u32>() as u32 {
        return Err(WalRecordError::PayloadLengthMismatch {
            record_type,
            payload_len,
        });
    }
    read_u32(logical, offset)
}

fn read_transaction_log_control_payload(
    logical: &[u8],
    offset: &mut usize,
    record_type: WalRecordType,
) -> Result<(u32, TransactionLogRange), WalRecordError> {
    let payload_len = read_u32(logical, offset)?;
    if payload_len != (size_of::<u32>() + transaction_range_len()) as u32 {
        return Err(WalRecordError::PayloadLengthMismatch {
            record_type,
            payload_len,
        });
    }
    let transaction_log_id = read_u32(logical, offset)?;
    let range = read_transaction_log_range(logical, offset)?;
    Ok((transaction_log_id, range))
}

fn read_transaction_commit_payload(
    logical: &[u8],
    offset: &mut usize,
    record_type: WalRecordType,
) -> Result<(u32, TransactionLogRange, TransactionCommitSeal), WalRecordError> {
    let payload_len = read_u32(logical, offset)?;
    if payload_len
        != (size_of::<u32>() + transaction_range_len() + transaction_commit_seal_len()) as u32
    {
        return Err(WalRecordError::PayloadLengthMismatch {
            record_type,
            payload_len,
        });
    }
    let transaction_log_id = read_u32(logical, offset)?;
    let range = read_transaction_log_range(logical, offset)?;
    let seal = read_transaction_commit_seal(logical, offset)?;
    Ok((transaction_log_id, range, seal))
}

fn encode_logical_byte(
    logical_byte: u8,
    output: &mut [u8],
    offset: usize,
    metadata: StorageMetadata,
    escape_codes: WalEscapeCodes,
) -> Result<usize, WalRecordError> {
    match logical_byte {
        byte if byte == metadata.erased_byte => {
            let offset = write_u8(output, offset, escape_codes.wal_escape_byte)?;
            write_u8(output, offset, escape_codes.wal_escape_code_erased)
        }
        byte if byte == metadata.wal_record_magic => {
            let offset = write_u8(output, offset, escape_codes.wal_escape_byte)?;
            write_u8(output, offset, escape_codes.wal_escape_code_magic)
        }
        byte if byte == escape_codes.wal_escape_byte => {
            let offset = write_u8(output, offset, escape_codes.wal_escape_byte)?;
            write_u8(output, offset, escape_codes.wal_escape_code_escape)
        }
        byte => write_u8(output, offset, byte),
    }
}

fn decode_logical_byte(
    input: &[u8],
    physical_offset: &mut usize,
    metadata: StorageMetadata,
    escape_codes: WalEscapeCodes,
) -> Result<u8, WalRecordError> {
    let byte = read_u8(input, physical_offset)?;
    if byte == metadata.erased_byte {
        return Err(WalRecordError::InvalidUnescapedReservedByte { found: byte });
    }
    if byte == metadata.wal_record_magic {
        return Err(WalRecordError::InvalidUnescapedReservedByte { found: byte });
    }

    if byte != escape_codes.wal_escape_byte {
        return Ok(byte);
    }

    let escape_code = read_u8(input, physical_offset)?;
    match escape_code {
        code if code == escape_codes.wal_escape_code_erased => Ok(metadata.erased_byte),
        code if code == escape_codes.wal_escape_code_magic => Ok(metadata.wal_record_magic),
        code if code == escape_codes.wal_escape_code_escape => Ok(escape_codes.wal_escape_byte),
        code => Err(WalRecordError::InvalidEscapeSequence(code)),
    }
}

fn log_position_len() -> usize {
    size_of::<u32>() * 2
}

fn transaction_range_len() -> usize {
    log_position_len() * 2
}

fn transaction_commit_seal_len() -> usize {
    log_position_len() * 2
}

fn free_queue_position_len() -> usize {
    FreeQueuePosition::ENCODED_LEN
}

fn write_free_queue_position(
    buffer: &mut [u8],
    offset: usize,
    position: FreeQueuePosition,
) -> Result<usize, WalRecordError> {
    let offset = write_u32(buffer, offset, position.region_index)?;
    write_u32(buffer, offset, position.entry_index)
}

fn write_log_position(
    buffer: &mut [u8],
    offset: usize,
    position: LogPosition,
) -> Result<usize, WalRecordError> {
    let offset = write_u32(buffer, offset, position.region_index)?;
    write_u32(buffer, offset, position.offset)
}

fn write_transaction_log_range(
    buffer: &mut [u8],
    offset: usize,
    range: TransactionLogRange,
) -> Result<usize, WalRecordError> {
    let offset = write_log_position(buffer, offset, range.start)?;
    write_log_position(buffer, offset, range.end)
}

fn write_transaction_commit_seal(
    buffer: &mut [u8],
    offset: usize,
    seal: TransactionCommitSeal,
) -> Result<usize, WalRecordError> {
    let offset = write_log_position(buffer, offset, seal.final_free_intent_start)?;
    write_log_position(buffer, offset, seal.final_segment_end)
}

fn read_free_queue_position(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<FreeQueuePosition, WalRecordError> {
    let region_index = read_u32(buffer, offset)?;
    let entry_index = read_u32(buffer, offset)?;
    Ok(FreeQueuePosition {
        region_index,
        entry_index,
    })
}

fn read_log_position(buffer: &[u8], offset: &mut usize) -> Result<LogPosition, WalRecordError> {
    let region_index = read_u32(buffer, offset)?;
    let offset_in_region = read_u32(buffer, offset)?;
    Ok(LogPosition {
        region_index,
        offset: offset_in_region,
    })
}

fn read_transaction_log_range(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<TransactionLogRange, WalRecordError> {
    let start = read_log_position(buffer, offset)?;
    let end = read_log_position(buffer, offset)?;
    Ok(TransactionLogRange { start, end })
}

fn read_transaction_commit_seal(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<TransactionCommitSeal, WalRecordError> {
    let final_free_intent_start = read_log_position(buffer, offset)?;
    let final_segment_end = read_log_position(buffer, offset)?;
    Ok(TransactionCommitSeal {
        final_free_intent_start,
        final_segment_end,
    })
}

fn read_payload<'a>(buffer: &'a [u8], offset: &mut usize) -> Result<&'a [u8], WalRecordError> {
    let payload_len =
        usize::try_from(read_u32(buffer, offset)?).map_err(|_| WalRecordError::LengthOverflow)?;
    let end = offset
        .checked_add(payload_len)
        .ok_or(WalRecordError::LengthOverflow)?;
    ensure_len(buffer, end)?;
    let payload = &buffer[*offset..end];
    *offset = end;
    Ok(payload)
}

fn align_up(value: usize, alignment: usize) -> Result<usize, WalRecordError> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or(WalRecordError::LengthOverflow)
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}

fn ensure_len(buffer: &[u8], needed: usize) -> Result<(), WalRecordError> {
    if buffer.len() < needed {
        return Err(WalRecordError::BufferTooSmall {
            needed,
            available: buffer.len(),
        });
    }
    Ok(())
}

fn write_u8(buffer: &mut [u8], offset: usize, value: u8) -> Result<usize, WalRecordError> {
    ensure_len(buffer, offset + size_of::<u8>())?;
    buffer[offset] = value;
    Ok(offset + size_of::<u8>())
}

fn write_u16(buffer: &mut [u8], offset: usize, value: u16) -> Result<usize, WalRecordError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_u32(buffer: &mut [u8], offset: usize, value: u32) -> Result<usize, WalRecordError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_u64(buffer: &mut [u8], offset: usize, value: u64) -> Result<usize, WalRecordError> {
    write_bytes(buffer, offset, &value.to_le_bytes())
}

fn write_bytes(buffer: &mut [u8], offset: usize, bytes: &[u8]) -> Result<usize, WalRecordError> {
    ensure_len(buffer, offset + bytes.len())?;
    buffer[offset..offset + bytes.len()].copy_from_slice(bytes);
    Ok(offset + bytes.len())
}

fn read_u8(buffer: &[u8], offset: &mut usize) -> Result<u8, WalRecordError> {
    ensure_len(buffer, *offset + size_of::<u8>())?;
    let value = buffer[*offset];
    *offset += size_of::<u8>();
    Ok(value)
}

fn read_u16(buffer: &[u8], offset: &mut usize) -> Result<u16, WalRecordError> {
    let bytes: [u8; 2] = read_array(buffer, offset)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(buffer: &[u8], offset: &mut usize) -> Result<u32, WalRecordError> {
    let bytes: [u8; 4] = read_array(buffer, offset)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(buffer: &[u8], offset: &mut usize) -> Result<u64, WalRecordError> {
    let bytes: [u8; 8] = read_array(buffer, offset)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_array<const N: usize>(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], WalRecordError> {
    ensure_len(buffer, *offset + N)?;
    let mut bytes = [0u8; N];
    bytes.copy_from_slice(&buffer[*offset..*offset + N]);
    *offset += N;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
