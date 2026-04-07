use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

use crate::disk::{DiskError, StorageMetadata};
use crate::CollectionId;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordError {
    Disk(DiskError),
    BufferTooSmall {
        needed: usize,
        available: usize,
    },
    InvalidRecordMagic {
        found: u8,
        expected: u8,
    },
    InvalidEscapeSequence(u8),
    InvalidPadding(u8),
    InvalidRecordType(u8),
    MissingRecordType,
    InvalidOptRegionTag(u8),
    InvalidChecksum,
    PayloadLengthMismatch {
        record_type: WalRecordType,
        payload_len: u32,
    },
    LengthOverflow,
}

impl From<DiskError> for WalRecordError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEscapeCodes {
    pub wal_escape_byte: u8,
    pub wal_escape_code_erased: u8,
    pub wal_escape_code_magic: u8,
    pub wal_escape_code_escape: u8,
}

impl WalEscapeCodes {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    NewCollection,
    Update,
    Snapshot,
    AllocBegin,
    Head,
    DropCollection,
    Link,
    FreeListHead,
    ReclaimBegin,
    ReclaimEnd,
    WalRecovery,
}

impl WalRecordType {
    pub fn code(self) -> u8 {
        match self {
            Self::NewCollection => 0x01,
            Self::Update => 0x02,
            Self::Snapshot => 0x03,
            Self::AllocBegin => 0x04,
            Self::Head => 0x05,
            Self::DropCollection => 0x06,
            Self::Link => 0x07,
            Self::FreeListHead => 0x08,
            Self::ReclaimBegin => 0x09,
            Self::ReclaimEnd => 0x0a,
            Self::WalRecovery => 0x0b,
        }
    }

    pub fn decode(code: u8) -> Result<Self, WalRecordError> {
        match code {
            0x01 => Ok(Self::NewCollection),
            0x02 => Ok(Self::Update),
            0x03 => Ok(Self::Snapshot),
            0x04 => Ok(Self::AllocBegin),
            0x05 => Ok(Self::Head),
            0x06 => Ok(Self::DropCollection),
            0x07 => Ok(Self::Link),
            0x08 => Ok(Self::FreeListHead),
            0x09 => Ok(Self::ReclaimBegin),
            0x0a => Ok(Self::ReclaimEnd),
            0x0b => Ok(Self::WalRecovery),
            _ => Err(WalRecordError::InvalidRecordType(code)),
        }
    }

    fn has_collection_id(self) -> bool {
        matches!(
            self,
            Self::NewCollection | Self::Update | Self::Snapshot | Self::Head | Self::DropCollection
        )
    }

    fn has_collection_type(self) -> bool {
        matches!(self, Self::NewCollection | Self::Snapshot | Self::Head)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecord<'a> {
    NewCollection {
        collection_id: CollectionId,
        collection_type: u16,
    },
    Update {
        collection_id: CollectionId,
        payload: &'a [u8],
    },
    Snapshot {
        collection_id: CollectionId,
        collection_type: u16,
        payload: &'a [u8],
    },
    AllocBegin {
        region_index: u32,
        free_list_head_after: Option<u32>,
    },
    Head {
        collection_id: CollectionId,
        collection_type: u16,
        region_index: u32,
    },
    DropCollection {
        collection_id: CollectionId,
    },
    Link {
        next_region_index: u32,
        expected_sequence: u64,
    },
    FreeListHead {
        region_index: Option<u32>,
    },
    ReclaimBegin {
        region_index: u32,
    },
    ReclaimEnd {
        region_index: u32,
    },
    WalRecovery,
}

impl<'a> WalRecord<'a> {
    pub fn record_type(self) -> WalRecordType {
        match self {
            Self::NewCollection { .. } => WalRecordType::NewCollection,
            Self::Update { .. } => WalRecordType::Update,
            Self::Snapshot { .. } => WalRecordType::Snapshot,
            Self::AllocBegin { .. } => WalRecordType::AllocBegin,
            Self::Head { .. } => WalRecordType::Head,
            Self::DropCollection { .. } => WalRecordType::DropCollection,
            Self::Link { .. } => WalRecordType::Link,
            Self::FreeListHead { .. } => WalRecordType::FreeListHead,
            Self::ReclaimBegin { .. } => WalRecordType::ReclaimBegin,
            Self::ReclaimEnd { .. } => WalRecordType::ReclaimEnd,
            Self::WalRecovery => WalRecordType::WalRecovery,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedWalRecord<'a> {
    pub record: WalRecord<'a>,
    pub encoded_len: usize,
    pub logical_len: usize,
}

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

pub fn encoded_record_len(
    record: WalRecord<'_>,
    metadata: StorageMetadata,
    physical_scratch: &mut [u8],
    logical_scratch: &mut [u8],
) -> Result<usize, WalRecordError> {
    encode_record_into(record, metadata, physical_scratch, logical_scratch)
}

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

        if logical_offset < payload_header_end {
            continue;
        }

        let payload_len = u32::from_le_bytes(
            logical_scratch[payload_header_end - size_of::<u32>()..payload_header_end]
                .try_into()
                .map_err(|_| WalRecordError::LengthOverflow)?,
        );
        let payload_len =
            usize::try_from(payload_len).map_err(|_| WalRecordError::LengthOverflow)?;
        let record_type = record_type.ok_or(WalRecordError::MissingRecordType)?;

        let prefix_and_payload_len = payload_header_end
            .checked_add(payload_len)
            .ok_or(WalRecordError::LengthOverflow)?;

        if record_type != WalRecordType::AllocBegin {
            let total_len = prefix_and_payload_len
                .checked_add(size_of::<u32>())
                .ok_or(WalRecordError::LengthOverflow)?;
            total_logical_len = Some(total_len);
            continue;
        }

        let alloc_opt_tag_offset = prefix_and_payload_len;
        if logical_offset <= alloc_opt_tag_offset {
            continue;
        }

        let opt_len = match logical_scratch[alloc_opt_tag_offset] {
            0 => 1usize,
            1 => 1usize + size_of::<u32>(),
            tag => return Err(WalRecordError::InvalidOptRegionTag(tag)),
        };
        let total_len = prefix_and_payload_len
            .checked_add(opt_len)
            .and_then(|value| value.checked_add(size_of::<u32>()))
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
        WalRecord::AllocBegin {
            region_index,
            free_list_head_after,
        } => {
            offset = write_u32(buffer, offset, size_of::<u32>() as u32)?;
            offset = write_u32(buffer, offset, region_index)?;
            offset = write_opt_region_index(buffer, offset, free_list_head_after)?;
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
        WalRecord::FreeListHead { region_index } => {
            offset = write_u32(
                buffer,
                offset,
                u32::try_from(opt_region_index_len(region_index))
                    .map_err(|_| WalRecordError::LengthOverflow)?,
            )?;
            offset = write_opt_region_index(buffer, offset, region_index)?;
        }
        WalRecord::ReclaimBegin { region_index } | WalRecord::ReclaimEnd { region_index } => {
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
        WalRecordType::AllocBegin => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u32>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            let free_list_head_after = read_opt_region_index(logical, &mut offset)?;
            Ok(WalRecord::AllocBegin {
                region_index,
                free_list_head_after,
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
        WalRecordType::FreeListHead => {
            let payload = read_payload(logical, &mut offset)?;
            let mut payload_offset = 0usize;
            let region_index = read_opt_region_index(payload, &mut payload_offset)?;
            if payload_offset != payload.len() {
                return Err(WalRecordError::LengthOverflow);
            }
            Ok(WalRecord::FreeListHead { region_index })
        }
        WalRecordType::ReclaimBegin => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u32>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            Ok(WalRecord::ReclaimBegin { region_index })
        }
        WalRecordType::ReclaimEnd => {
            let payload_len = read_u32(logical, &mut offset)?;
            if payload_len != size_of::<u32>() as u32 {
                return Err(WalRecordError::PayloadLengthMismatch {
                    record_type,
                    payload_len,
                });
            }
            let region_index = read_u32(logical, &mut offset)?;
            Ok(WalRecord::ReclaimEnd { region_index })
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
    }
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
    if byte == metadata.erased_byte || byte == metadata.wal_record_magic {
        return Ok(byte);
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

fn opt_region_index_len(region_index: Option<u32>) -> usize {
    1 + usize::from(region_index.is_some()) * size_of::<u32>()
}

fn write_opt_region_index(
    buffer: &mut [u8],
    offset: usize,
    region_index: Option<u32>,
) -> Result<usize, WalRecordError> {
    match region_index {
        Some(region_index) => {
            let offset = write_u8(buffer, offset, 1)?;
            write_u32(buffer, offset, region_index)
        }
        None => write_u8(buffer, offset, 0),
    }
}

fn read_opt_region_index(buffer: &[u8], offset: &mut usize) -> Result<Option<u32>, WalRecordError> {
    match read_u8(buffer, offset)? {
        0 => Ok(None),
        1 => Ok(Some(read_u32(buffer, offset)?)),
        tag => Err(WalRecordError::InvalidOptRegionTag(tag)),
    }
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
