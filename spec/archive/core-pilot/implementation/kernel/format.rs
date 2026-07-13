// Archived core-pilot implementation snapshot. Not part of the compiled crate.
use core::mem::size_of;

use crc::{Crc, CRC_32_ISCSI};

use super::{DeviceGeometry, KernelError, RawFlash};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const METADATA_MAGIC: [u8; 4] = *b"BRM3";
const HEADER_MAGIC: [u8; 4] = *b"RGN3";
pub const STORAGE_FORMAT_V3: u32 = 3;
const REGION_HEADER_VERSION: u16 = 1;
const NO_NEXT_REGION: u32 = u32::MAX;

/// Immutable v3 format metadata published after bootstrap structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3Metadata {
    pub region_size: u32,
    pub region_count: u32,
    pub min_prepared_regions: u32,
    pub program_alignment: u32,
    pub bootstrap_basis_root: u32,
    pub bootstrap_basis_segments: u32,
    pub initial_wal_region: u32,
    pub wal_write_granule: u32,
    pub erased_byte: u8,
}

impl V3Metadata {
    pub const ENCODED_LEN: usize = 48;

    pub fn encode<E>(self, output: &mut [u8]) -> Result<usize, KernelError<E>> {
        ensure_buffer(output, Self::ENCODED_LEN)?;
        output[..Self::ENCODED_LEN].fill(self.erased_byte);
        output[..4].copy_from_slice(&METADATA_MAGIC);
        write_u32(output, 4, STORAGE_FORMAT_V3);
        write_u32(output, 8, self.region_size);
        write_u32(output, 12, self.region_count);
        write_u32(output, 16, self.min_prepared_regions);
        write_u32(output, 20, self.program_alignment);
        write_u32(output, 24, self.bootstrap_basis_root);
        write_u32(output, 28, self.bootstrap_basis_segments);
        write_u32(output, 32, self.initial_wal_region);
        write_u32(output, 36, self.wal_write_granule);
        output[40] = self.erased_byte;
        let checksum = crc32(&output[..44]);
        write_u32(output, 44, checksum);
        Ok(Self::ENCODED_LEN)
    }

    pub fn decode<E>(input: &[u8]) -> Result<Self, KernelError<E>> {
        ensure_buffer(input, Self::ENCODED_LEN)?;
        if input[..4] != METADATA_MAGIC {
            let legacy = read_u32(input, 0);
            if legacy != u32::MAX && legacy != 0 {
                return Err(KernelError::UnsupportedStorageVersion(legacy));
            }
            return Err(KernelError::Unformatted);
        }
        let version = read_u32(input, 4);
        if version != STORAGE_FORMAT_V3 {
            return Err(KernelError::UnsupportedStorageVersion(version));
        }
        if read_u32(input, 44) != crc32(&input[..44]) {
            return Err(KernelError::CorruptFormat);
        }
        let metadata = Self {
            region_size: read_u32(input, 8),
            region_count: read_u32(input, 12),
            min_prepared_regions: read_u32(input, 16),
            program_alignment: read_u32(input, 20),
            bootstrap_basis_root: read_u32(input, 24),
            bootstrap_basis_segments: read_u32(input, 28),
            initial_wal_region: read_u32(input, 32),
            wal_write_granule: read_u32(input, 36),
            erased_byte: input[40],
        };
        if metadata.bootstrap_basis_segments == 0
            || metadata.initial_wal_region >= metadata.region_count
            || metadata.bootstrap_basis_root >= metadata.region_count
            || metadata.program_alignment == 0
            || metadata.wal_write_granule == 0
        {
            return Err(KernelError::CorruptFormat);
        }
        Ok(metadata)
    }

    pub fn validate_geometry<E>(self, geometry: DeviceGeometry) -> Result<(), KernelError<E>> {
        if usize::try_from(self.region_size).ok() != Some(geometry.region_size)
            || self.region_count != geometry.region_count
            || self.erased_byte != geometry.erased_byte
            || usize::try_from(self.program_alignment).ok() != Some(geometry.program_alignment)
        {
            return Err(KernelError::CorruptFormat);
        }
        Ok(())
    }
}

/// Immutable role stored in each v3 region header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderPurpose {
    MainWal,
    TransactionLog,
    FreeSpaceBasis,
    CollectionData(u16),
}

/// Fixed header read from every region during startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionHeader {
    pub purpose: HeaderPurpose,
    pub sequence: u64,
    pub owner: u64,
    pub operation: u64,
    pub next_region: Option<u32>,
    pub payload_len: u32,
}

impl RegionHeader {
    pub const ENCODED_LEN: usize = 48;

    pub fn encode<E>(self, erased_byte: u8, output: &mut [u8]) -> Result<usize, KernelError<E>> {
        ensure_buffer(output, Self::ENCODED_LEN)?;
        output[..Self::ENCODED_LEN].fill(erased_byte);
        output[..4].copy_from_slice(&HEADER_MAGIC);
        write_u16(output, 4, REGION_HEADER_VERSION);
        write_u16(output, 6, encode_purpose(self.purpose)?);
        write_u64(output, 8, self.sequence);
        write_u64(output, 16, self.owner);
        write_u64(output, 24, self.operation);
        write_u32(output, 32, self.next_region.unwrap_or(NO_NEXT_REGION));
        write_u32(output, 36, self.payload_len);
        let checksum = crc32(&output[..44]);
        write_u32(output, 44, checksum);
        Ok(Self::ENCODED_LEN)
    }

    pub fn decode<E>(input: &[u8], erased_byte: u8) -> Result<Option<Self>, KernelError<E>> {
        ensure_buffer(input, Self::ENCODED_LEN)?;
        if input[..Self::ENCODED_LEN]
            .iter()
            .all(|byte| *byte == erased_byte)
        {
            return Ok(None);
        }
        if input[..4] != HEADER_MAGIC
            || read_u16(input, 4) != REGION_HEADER_VERSION
            || read_u32(input, 44) != crc32(&input[..44])
        {
            return Err(KernelError::CorruptFormat);
        }
        let next = read_u32(input, 32);
        Ok(Some(Self {
            purpose: decode_purpose(read_u16(input, 6))?,
            sequence: read_u64(input, 8),
            owner: read_u64(input, 16),
            operation: read_u64(input, 24),
            next_region: (next != NO_NEXT_REGION).then_some(next),
            payload_len: read_u32(input, 36),
        }))
    }
}

/// Formatting parameters not implied by device geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3FormatConfig {
    pub min_prepared_regions: u32,
    pub wal_write_granule: u32,
}

impl V3FormatConfig {
    pub const fn new(min_prepared_regions: u32, wal_write_granule: u32) -> Self {
        Self {
            min_prepared_regions,
            wal_write_granule,
        }
    }
}

pub(crate) const BASIS_PROLOGUE_LEN: usize = size_of::<u64>() * 4 + size_of::<u32>() * 2;
pub(crate) const WAL_PROLOGUE_LEN: usize = size_of::<u32>() * 4;

pub(crate) fn basis_segment_capacity(region_size: usize) -> Option<usize> {
    region_size
        .checked_sub(RegionHeader::ENCODED_LEN + BASIS_PROLOGUE_LEN)
        .map(|bytes| bytes / size_of::<u32>())
}

pub(crate) fn bootstrap_layout<E>(geometry: DeviceGeometry) -> Result<(u32, u32), KernelError<E>> {
    let capacity = basis_segment_capacity(geometry.region_size)
        .filter(|capacity| *capacity > 0)
        .ok_or(KernelError::InsufficientRegions)?;
    let region_count =
        usize::try_from(geometry.region_count).map_err(|_| KernelError::InsufficientRegions)?;
    for basis_segments in 1..region_count {
        let free_entries = region_count
            .checked_sub(basis_segments + 1)
            .ok_or(KernelError::InsufficientRegions)?;
        if free_entries <= basis_segments.saturating_mul(capacity) {
            let basis_segments =
                u32::try_from(basis_segments).map_err(|_| KernelError::InsufficientRegions)?;
            return Ok((basis_segments, basis_segments));
        }
    }
    Err(KernelError::InsufficientRegions)
}

pub(crate) fn encode_basis_segment<E>(
    output: &mut [u8],
    metadata: V3Metadata,
    segment_index: u32,
    first_entry_position: u64,
    entries: &[u32],
) -> Result<usize, KernelError<E>> {
    let next_region =
        (segment_index + 1 < metadata.bootstrap_basis_segments).then_some(segment_index + 1);
    let free_count = u64::from(metadata.region_count - metadata.initial_wal_region - 1);
    encode_free_basis_segment(
        output,
        metadata.erased_byte,
        u64::from(segment_index),
        next_region,
        0,
        free_count,
        free_count,
        first_entry_position,
        entries,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_free_basis_segment<E>(
    output: &mut [u8],
    erased_byte: u8,
    sequence: u64,
    next_region: Option<u32>,
    interval_start: u64,
    interval_end: u64,
    ready_position: u64,
    first_entry_position: u64,
    entries: &[u32],
) -> Result<usize, KernelError<E>> {
    let payload_len = BASIS_PROLOGUE_LEN
        .checked_add(
            entries
                .len()
                .checked_mul(size_of::<u32>())
                .ok_or(KernelError::CorruptFormat)?,
        )
        .ok_or(KernelError::CorruptFormat)?;
    let total = RegionHeader::ENCODED_LEN
        .checked_add(payload_len)
        .ok_or(KernelError::CorruptFormat)?;
    ensure_buffer(output, total)?;
    output[..total].fill(erased_byte);
    RegionHeader {
        purpose: HeaderPurpose::FreeSpaceBasis,
        sequence,
        owner: 0,
        operation: 0,
        next_region,
        payload_len: u32::try_from(payload_len).map_err(|_| KernelError::CorruptFormat)?,
    }
    .encode(erased_byte, output)?;
    let mut offset = RegionHeader::ENCODED_LEN;
    write_u64(output, offset, interval_start);
    offset += size_of::<u64>();
    write_u64(output, offset, interval_end);
    offset += size_of::<u64>();
    write_u64(output, offset, ready_position);
    offset += size_of::<u64>();
    write_u64(output, offset, first_entry_position);
    offset += size_of::<u64>();
    write_u32(
        output,
        offset,
        u32::try_from(entries.len()).map_err(|_| KernelError::CorruptFormat)?,
    );
    offset += size_of::<u32>() * 2;
    for entry in entries {
        write_u32(output, offset, *entry);
        offset += size_of::<u32>();
    }
    Ok(total)
}

pub(crate) fn encode_initial_wal<E>(
    output: &mut [u8],
    metadata: V3Metadata,
) -> Result<usize, KernelError<E>> {
    encode_wal_region(
        output,
        metadata,
        metadata.initial_wal_region,
        u64::from(metadata.bootstrap_basis_segments),
    )
}

pub(crate) fn encode_wal_region<E>(
    output: &mut [u8],
    metadata: V3Metadata,
    region_index: u32,
    sequence: u64,
) -> Result<usize, KernelError<E>> {
    let record_start = align_up(
        RegionHeader::ENCODED_LEN + WAL_PROLOGUE_LEN,
        usize::try_from(metadata.wal_write_granule).map_err(|_| KernelError::InvalidAlignment)?,
    )?;
    ensure_buffer(output, record_start)?;
    output[..record_start].fill(metadata.erased_byte);
    RegionHeader {
        purpose: HeaderPurpose::MainWal,
        sequence,
        owner: 0,
        operation: 0,
        next_region: None,
        payload_len: u32::try_from(WAL_PROLOGUE_LEN).map_err(|_| KernelError::CorruptFormat)?,
    }
    .encode(metadata.erased_byte, output)?;
    let offset = RegionHeader::ENCODED_LEN;
    write_u32(output, offset, metadata.initial_wal_region);
    write_u32(
        output,
        offset + size_of::<u32>(),
        u32::try_from(record_start).map_err(|_| KernelError::CorruptFormat)?,
    );
    write_u32(
        output,
        offset + size_of::<u32>() * 2,
        metadata.bootstrap_basis_root,
    );
    write_u32(output, offset + size_of::<u32>() * 3, region_index);
    Ok(record_start)
}

pub(crate) fn program_region_span<D: RawFlash>(
    device: &mut D,
    region_index: u32,
    offset: usize,
    bytes: &[u8],
) -> Result<(), KernelError<D::Error>> {
    let geometry = device.geometry().validate()?;
    program_chunks(
        geometry.program_alignment,
        geometry.max_program_len,
        offset,
        bytes,
        |chunk_offset, chunk| {
            device
                .program_region(region_index, chunk_offset, chunk)
                .map_err(KernelError::Device)
        },
    )
}

pub(crate) fn program_metadata_span<D: RawFlash>(
    device: &mut D,
    offset: usize,
    bytes: &[u8],
) -> Result<(), KernelError<D::Error>> {
    let geometry = device.geometry().validate()?;
    program_chunks(
        geometry.program_alignment,
        geometry.max_program_len,
        offset,
        bytes,
        |chunk_offset, chunk| {
            device
                .program_metadata(chunk_offset, chunk)
                .map_err(KernelError::Device)
        },
    )
}

fn program_chunks<E>(
    alignment: usize,
    maximum: usize,
    offset: usize,
    bytes: &[u8],
    mut program: impl FnMut(usize, &[u8]) -> Result<(), KernelError<E>>,
) -> Result<(), KernelError<E>> {
    if bytes.is_empty()
        || !offset.is_multiple_of(alignment)
        || !bytes.len().is_multiple_of(alignment)
        || maximum < alignment
    {
        return Err(KernelError::InvalidAlignment);
    }
    let chunk_limit = maximum - (maximum % alignment);
    let mut written = 0usize;
    while written < bytes.len() {
        let remaining = bytes.len() - written;
        let chunk_len = remaining.min(chunk_limit);
        program(offset + written, &bytes[written..written + chunk_len])?;
        written += chunk_len;
    }
    Ok(())
}

pub(crate) fn read_metadata_exact<D: RawFlash>(
    device: &mut D,
    output: &mut [u8],
) -> Result<(), KernelError<D::Error>> {
    read_chunks(device.geometry(), output, |offset, len, copy| {
        device
            .read_metadata(offset, len, copy)
            .map_err(KernelError::Device)
    })
}

pub(crate) fn read_region_exact<D: RawFlash>(
    device: &mut D,
    region_index: u32,
    offset: usize,
    output: &mut [u8],
) -> Result<(), KernelError<D::Error>> {
    read_chunks_at(
        device.geometry(),
        offset,
        output,
        |chunk_offset, len, copy| {
            device
                .read_region(region_index, chunk_offset, len, copy)
                .map_err(KernelError::Device)
        },
    )
}

fn read_chunks<E>(
    geometry: DeviceGeometry,
    output: &mut [u8],
    read: impl FnMut(usize, usize, &mut dyn FnMut(&[u8])) -> Result<(), KernelError<E>>,
) -> Result<(), KernelError<E>> {
    read_chunks_at(geometry, 0, output, read)
}

fn read_chunks_at<E>(
    geometry: DeviceGeometry,
    offset: usize,
    output: &mut [u8],
    mut read: impl FnMut(usize, usize, &mut dyn FnMut(&[u8])) -> Result<(), KernelError<E>>,
) -> Result<(), KernelError<E>> {
    let geometry = geometry.validate()?;
    if output.is_empty()
        || !offset.is_multiple_of(geometry.read_alignment)
        || !output.len().is_multiple_of(geometry.read_alignment)
    {
        return Err(KernelError::InvalidAlignment);
    }
    let chunk_limit = geometry.max_read_len - (geometry.max_read_len % geometry.read_alignment);
    if chunk_limit == 0 {
        return Err(KernelError::InvalidAlignment);
    }
    let mut copied = 0usize;
    while copied < output.len() {
        let chunk_len = (output.len() - copied).min(chunk_limit);
        let target = &mut output[copied..copied + chunk_len];
        let mut copier = |bytes: &[u8]| target.copy_from_slice(bytes);
        read(offset + copied, chunk_len, &mut copier)?;
        copied += chunk_len;
    }
    Ok(())
}

pub(crate) fn align_up<E>(value: usize, alignment: usize) -> Result<usize, KernelError<E>> {
    if alignment == 0 {
        return Err(KernelError::InvalidAlignment);
    }
    value
        .checked_add(alignment - 1)
        .map(|end| end / alignment * alignment)
        .ok_or(KernelError::InvalidAlignment)
}

fn ensure_buffer<E>(buffer: &[u8], needed: usize) -> Result<(), KernelError<E>> {
    if buffer.len() < needed {
        return Err(KernelError::BufferTooSmall {
            needed,
            available: buffer.len(),
        });
    }
    Ok(())
}

fn encode_purpose<E>(purpose: HeaderPurpose) -> Result<u16, KernelError<E>> {
    match purpose {
        HeaderPurpose::MainWal => Ok(1),
        HeaderPurpose::TransactionLog => Ok(2),
        HeaderPurpose::FreeSpaceBasis => Ok(3),
        HeaderPurpose::CollectionData(collection_type) if collection_type < 0x8000 => {
            Ok(0x8000 | collection_type)
        }
        HeaderPurpose::CollectionData(_) => Err(KernelError::CorruptFormat),
    }
}

fn decode_purpose<E>(encoded: u16) -> Result<HeaderPurpose, KernelError<E>> {
    match encoded {
        1 => Ok(HeaderPurpose::MainWal),
        2 => Ok(HeaderPurpose::TransactionLog),
        3 => Ok(HeaderPurpose::FreeSpaceBasis),
        value if value & 0x8000 != 0 => Ok(HeaderPurpose::CollectionData(value & 0x7fff)),
        _ => Err(KernelError::CorruptFormat),
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    CRC32C.checksum(bytes)
}

fn write_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}
fn write_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}
fn write_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}
fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([input[offset], input[offset + 1]])
}
fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
    ])
}
fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
        input[offset + 4],
        input[offset + 5],
        input[offset + 6],
        input[offset + 7],
    ])
}
