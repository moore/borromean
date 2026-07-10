use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

use crate::disk::{FreeQueuePosition, StorageMetadata};
use crate::wal_record::{LogPosition, WalRecord, WalRecordError, WalRecordType};
use crate::CollectionId;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Purpose recorded by a durable transaction-log allocation entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionAllocationPurpose {
    /// Data or collection-owned region allocated by the transaction.
    DataRegion,
    /// Region allocated to extend the transaction-log segment chain.
    TransactionSegment,
}

impl TransactionAllocationPurpose {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::DataRegion => 0x01,
            Self::TransactionSegment => 0x02,
        }
    }

    pub(crate) fn decode(code: u8) -> Result<Self, WalRecordError> {
        match code {
            0x01 => Ok(Self::DataRegion),
            0x02 => Ok(Self::TransactionSegment),
            _ => Err(WalRecordError::InvalidRecordType(code)),
        }
    }
}

/// Durable allocation entry stored at the front of a transaction segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionAllocationEntry {
    pub(crate) region_index: u32,
    pub(crate) allocation_head_after: FreeQueuePosition,
    pub(crate) purpose: TransactionAllocationPurpose,
}

impl TransactionAllocationEntry {
    const LOGICAL_LEN: usize =
        size_of::<u32>() + FreeQueuePosition::ENCODED_LEN + size_of::<u8>() + size_of::<u32>();
    const CHECKSUM_OFFSET: usize =
        size_of::<u32>() + FreeQueuePosition::ENCODED_LEN + size_of::<u8>();

    pub(crate) fn encoded_len(metadata: StorageMetadata) -> Result<usize, WalRecordError> {
        align_up(
            Self::LOGICAL_LEN,
            usize::try_from(metadata.wal_write_granule)
                .map_err(|_| WalRecordError::LengthOverflow)?,
        )
    }

    pub(crate) fn encode_into(
        self,
        metadata: StorageMetadata,
        output: &mut [u8],
    ) -> Result<usize, WalRecordError> {
        let encoded_len = Self::encoded_len(metadata)?;
        ensure_len(output, encoded_len)?;
        output[..encoded_len].fill(0);

        let mut offset = 0usize;
        offset = write_u32(output, offset, self.region_index)?;
        offset = write_free_queue_position(output, offset, self.allocation_head_after)?;
        offset = write_u8(output, offset, self.purpose.code())?;
        let checksum = crc32(&output[..offset]);
        offset = write_u32(output, offset, checksum)?;
        debug_assert_eq!(offset, Self::LOGICAL_LEN);
        Ok(encoded_len)
    }

    pub(crate) fn decode(metadata: StorageMetadata, input: &[u8]) -> Result<Self, WalRecordError> {
        let encoded_len = Self::encoded_len(metadata)?;
        ensure_len(input, encoded_len)?;
        let expected_checksum = crc32(&input[..Self::CHECKSUM_OFFSET]);
        let actual_checksum = u32::from_le_bytes(
            input[Self::CHECKSUM_OFFSET..Self::CHECKSUM_OFFSET + size_of::<u32>()]
                .try_into()
                .map_err(|_| WalRecordError::LengthOverflow)?,
        );
        if actual_checksum != expected_checksum {
            return Err(WalRecordError::InvalidChecksum);
        }

        let mut offset = 0usize;
        let region_index = read_u32(input, &mut offset)?;
        let allocation_head_after = read_free_queue_position(input, &mut offset)?;
        let purpose = TransactionAllocationPurpose::decode(read_u8(input, &mut offset)?)?;
        Ok(Self {
            region_index,
            allocation_head_after,
            purpose,
        })
    }
}

/// Non-final transaction segment seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionSegmentSeal {
    pub(crate) next_region_index: u32,
    pub(crate) expected_sequence: u64,
    pub(crate) free_intent_start: u32,
    pub(crate) segment_end: u32,
}

impl TransactionSegmentSeal {
    pub(crate) const ENCODED_LEN: usize =
        size_of::<u32>() + size_of::<u64>() + size_of::<u32>() * 3;
    const CHECKSUM_OFFSET: usize = size_of::<u32>() + size_of::<u64>() + size_of::<u32>() * 2;

    pub(crate) fn encode_into(self, output: &mut [u8]) -> Result<usize, WalRecordError> {
        ensure_len(output, Self::ENCODED_LEN)?;
        let mut offset = 0usize;
        offset = write_u32(output, offset, self.next_region_index)?;
        offset = write_u64(output, offset, self.expected_sequence)?;
        offset = write_u32(output, offset, self.free_intent_start)?;
        offset = write_u32(output, offset, self.segment_end)?;
        let checksum = crc32(&output[..offset]);
        offset = write_u32(output, offset, checksum)?;
        Ok(offset)
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, WalRecordError> {
        ensure_len(input, Self::ENCODED_LEN)?;
        let expected_checksum = crc32(&input[..Self::CHECKSUM_OFFSET]);
        let actual_checksum = u32::from_le_bytes(
            input[Self::CHECKSUM_OFFSET..Self::CHECKSUM_OFFSET + size_of::<u32>()]
                .try_into()
                .map_err(|_| WalRecordError::LengthOverflow)?,
        );
        if actual_checksum != expected_checksum {
            return Err(WalRecordError::InvalidChecksum);
        }
        let mut offset = 0usize;
        Ok(Self {
            next_region_index: read_u32(input, &mut offset)?,
            expected_sequence: read_u64(input, &mut offset)?,
            free_intent_start: read_u32(input, &mut offset)?,
            segment_end: read_u32(input, &mut offset)?,
        })
    }
}

/// Decoded transaction private suffix entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecodedPrivateSuffixEntry<'a> {
    pub(crate) record: WalRecord<'a>,
    pub(crate) encoded_len: usize,
}

/// Encodes one transaction-private suffix entry.
pub(crate) fn encode_private_suffix_entry(
    record: WalRecord<'_>,
    output: &mut [u8],
) -> Result<usize, WalRecordError> {
    let (record_type, collection_id, payload_len) = private_suffix_header(record)?;
    let prefix_len = size_of::<u8>() + size_of::<u64>() + size_of::<u32>();
    let checksum_len = size_of::<u32>();
    let total_len = prefix_len
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(checksum_len))
        .ok_or(WalRecordError::LengthOverflow)?;
    ensure_len(output, total_len)?;

    let mut offset = 0usize;
    offset = write_u8(output, offset, record_type.code())?;
    offset = write_u64(output, offset, collection_id.0)?;
    offset = write_u32(
        output,
        offset,
        u32::try_from(payload_len).map_err(|_| WalRecordError::LengthOverflow)?,
    )?;
    offset = write_private_suffix_payload(record, output, offset)?;
    let checksum = crc32(&output[..offset]);
    offset = write_u32(output, offset, checksum)?;
    Ok(offset)
}

/// Decodes one transaction-private suffix entry.
pub(crate) fn decode_private_suffix_entry(
    input: &[u8],
) -> Result<DecodedPrivateSuffixEntry<'_>, WalRecordError> {
    let mut offset = 0usize;
    let record_type = WalRecordType::decode(read_u8(input, &mut offset)?)?;
    let collection_id = CollectionId(read_u64(input, &mut offset)?);
    let payload_len = usize::try_from(read_u32(input, &mut offset)?)
        .map_err(|_| WalRecordError::LengthOverflow)?;
    let payload_start = offset;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or(WalRecordError::LengthOverflow)?;
    let checksum_end = payload_end
        .checked_add(size_of::<u32>())
        .ok_or(WalRecordError::LengthOverflow)?;
    ensure_len(input, checksum_end)?;
    let expected_checksum = crc32(&input[..payload_end]);
    let actual_checksum = u32::from_le_bytes(
        input[payload_end..checksum_end]
            .try_into()
            .map_err(|_| WalRecordError::LengthOverflow)?,
    );
    if actual_checksum != expected_checksum {
        return Err(WalRecordError::InvalidChecksum);
    }

    let payload = &input[payload_start..payload_end];
    let record = decode_private_suffix_record(record_type, collection_id, payload)?;
    Ok(DecodedPrivateSuffixEntry {
        record,
        encoded_len: checksum_end,
    })
}

pub(crate) fn private_suffix_entry_len(record: WalRecord<'_>) -> Result<usize, WalRecordError> {
    let (_, _, payload_len) = private_suffix_header(record)?;
    size_of::<u8>()
        .checked_add(size_of::<u64>())
        .and_then(|len| len.checked_add(size_of::<u32>()))
        .and_then(|len| len.checked_add(payload_len))
        .and_then(|len| len.checked_add(size_of::<u32>()))
        .ok_or(WalRecordError::LengthOverflow)
}

pub(crate) fn log_position(
    region_index: u32,
    offset: usize,
) -> Result<LogPosition, WalRecordError> {
    Ok(LogPosition {
        region_index,
        offset: u32::try_from(offset).map_err(|_| WalRecordError::LengthOverflow)?,
    })
}

fn private_suffix_header(
    record: WalRecord<'_>,
) -> Result<(WalRecordType, CollectionId, usize), WalRecordError> {
    match record {
        WalRecord::NewCollection {
            collection_id,
            collection_type: _,
        } => Ok((
            WalRecordType::NewCollection,
            collection_id,
            size_of::<u16>(),
        )),
        WalRecord::Update {
            collection_id,
            payload,
        } => Ok((WalRecordType::Update, collection_id, payload.len())),
        WalRecord::Snapshot {
            collection_id,
            collection_type: _,
            payload,
        } => Ok((
            WalRecordType::Snapshot,
            collection_id,
            size_of::<u16>()
                .checked_add(payload.len())
                .ok_or(WalRecordError::LengthOverflow)?,
        )),
        WalRecord::Head {
            collection_id,
            collection_type: _,
            region_index: _,
        } => Ok((
            WalRecordType::Head,
            collection_id,
            size_of::<u16>() + size_of::<u32>(),
        )),
        WalRecord::DropCollection { collection_id } => {
            Ok((WalRecordType::DropCollection, collection_id, 0))
        }
        WalRecord::AddTransactionCollection {
            collection_id,
            observed_collection_generation: _,
        } => Ok((
            WalRecordType::AddTransactionCollection,
            collection_id,
            size_of::<u64>(),
        )),
        WalRecord::FreeIntent {
            collection_id,
            region_index: _,
        } => Ok((WalRecordType::FreeIntent, collection_id, size_of::<u32>())),
        other => Err(WalRecordError::InvalidRecordType(
            other.record_type().code(),
        )),
    }
}

fn write_private_suffix_payload(
    record: WalRecord<'_>,
    output: &mut [u8],
    mut offset: usize,
) -> Result<usize, WalRecordError> {
    match record {
        WalRecord::NewCollection {
            collection_type, ..
        } => write_u16(output, offset, collection_type),
        WalRecord::Update { payload, .. } => write_bytes(output, offset, payload),
        WalRecord::Snapshot {
            collection_type,
            payload,
            ..
        } => {
            offset = write_u16(output, offset, collection_type)?;
            write_bytes(output, offset, payload)
        }
        WalRecord::Head {
            collection_type,
            region_index,
            ..
        } => {
            offset = write_u16(output, offset, collection_type)?;
            write_u32(output, offset, region_index)
        }
        WalRecord::DropCollection { .. } => Ok(offset),
        WalRecord::AddTransactionCollection {
            observed_collection_generation,
            ..
        } => write_u64(output, offset, observed_collection_generation),
        WalRecord::FreeIntent { region_index, .. } => write_u32(output, offset, region_index),
        other => Err(WalRecordError::InvalidRecordType(
            other.record_type().code(),
        )),
    }
}

fn decode_private_suffix_record<'a>(
    record_type: WalRecordType,
    collection_id: CollectionId,
    payload: &'a [u8],
) -> Result<WalRecord<'a>, WalRecordError> {
    let mut offset = 0usize;
    let record = match record_type {
        WalRecordType::NewCollection => {
            let collection_type = read_u16(payload, &mut offset)?;
            WalRecord::NewCollection {
                collection_id,
                collection_type,
            }
        }
        WalRecordType::Update => WalRecord::Update {
            collection_id,
            payload,
        },
        WalRecordType::Snapshot => {
            let collection_type = read_u16(payload, &mut offset)?;
            WalRecord::Snapshot {
                collection_id,
                collection_type,
                payload: &payload[offset..],
            }
        }
        WalRecordType::Head => {
            let collection_type = read_u16(payload, &mut offset)?;
            let region_index = read_u32(payload, &mut offset)?;
            WalRecord::Head {
                collection_id,
                collection_type,
                region_index,
            }
        }
        WalRecordType::DropCollection => WalRecord::DropCollection { collection_id },
        WalRecordType::AddTransactionCollection => {
            let observed_collection_generation = read_u64(payload, &mut offset)?;
            WalRecord::AddTransactionCollection {
                collection_id,
                observed_collection_generation,
            }
        }
        WalRecordType::FreeIntent => {
            let region_index = read_u32(payload, &mut offset)?;
            WalRecord::FreeIntent {
                collection_id,
                region_index,
            }
        }
        other => return Err(WalRecordError::InvalidRecordType(other.code())),
    };
    if offset != payload.len()
        && !matches!(
            record,
            WalRecord::Update { .. } | WalRecord::Snapshot { .. }
        )
    {
        return Err(WalRecordError::PayloadLengthMismatch {
            record_type,
            payload_len: u32::try_from(payload.len())
                .map_err(|_| WalRecordError::LengthOverflow)?,
        });
    }
    Ok(record)
}

fn align_up(value: usize, alignment: usize) -> Result<usize, WalRecordError> {
    if alignment == 0 {
        return Err(WalRecordError::LengthOverflow);
    }
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

fn write_free_queue_position(
    buffer: &mut [u8],
    offset: usize,
    position: FreeQueuePosition,
) -> Result<usize, WalRecordError> {
    let offset = write_u32(buffer, offset, position.region_index)?;
    write_u32(buffer, offset, position.entry_index)
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

fn read_free_queue_position(
    buffer: &[u8],
    offset: &mut usize,
) -> Result<FreeQueuePosition, WalRecordError> {
    Ok(FreeQueuePosition {
        region_index: read_u32(buffer, offset)?,
        entry_index: read_u32(buffer, offset)?,
    })
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
