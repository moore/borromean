use embedded_storage_traits::nor_flash::{NorFlash, NorFlashError, NorFlashErrorKind};

use crate::disk::{
    encode_wal_region_prefix, DiskError, FreePointerFooter, Header, StorageMetadata,
    WalRegionPrologue,
};
use crate::flash_io::{FlashIo, StorageFormatError, StorageIoError};

/// Options for [`EmbeddedStorageFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedStorageOptions {
    /// Byte value observed after erasing the target flash.
    pub erased_byte: u8,
}

impl EmbeddedStorageOptions {
    /// Creates options for a target with the supplied erased byte.
    pub const fn new(erased_byte: u8) -> Self {
        Self { erased_byte }
    }
}

impl Default for EmbeddedStorageOptions {
    fn default() -> Self {
        Self::new(0xff)
    }
}

/// Metadata fields validated against [`EmbeddedStorageFlash`] geometry and options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedStorageMetadataField {
    /// `StorageMetadata.region_size`.
    RegionSize,
    /// `StorageMetadata.region_count`.
    RegionCount,
    /// `StorageMetadata.erased_byte`.
    ErasedByte,
}

/// Errors returned by [`EmbeddedStorageFlash`] operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedStorageError {
    /// Disk structure encoding or decoding failed.
    Disk(DiskError),
    /// The underlying `embedded-storage` flash returned an error kind.
    Flash(NorFlashErrorKind),
    /// Checked arithmetic overflowed or a flash address did not fit in `u32`.
    LengthOverflow,
    /// `REGION_SIZE` must be greater than zero.
    ZeroRegionSize,
    /// `ReadNorFlash::READ_SIZE` must be greater than zero.
    ZeroReadSize,
    /// `NorFlash::WRITE_SIZE` must be greater than zero.
    ZeroWriteSize,
    /// `NorFlash::ERASE_SIZE` must be greater than zero.
    ZeroEraseSize,
    /// The flash capacity is smaller than the configured Borromean layout.
    CapacityTooSmall {
        /// Required capacity in bytes.
        required: usize,
        /// Actual flash capacity in bytes.
        actual: usize,
    },
    /// `REGION_SIZE` was not aligned to the flash read size.
    RegionSizeNotReadAligned {
        /// Configured region size.
        region_size: usize,
        /// Required read alignment.
        read_size: usize,
    },
    /// `REGION_SIZE` was not aligned to the flash write size.
    RegionSizeNotWriteAligned {
        /// Configured region size.
        region_size: usize,
        /// Required write alignment.
        write_size: usize,
    },
    /// `REGION_SIZE` was not aligned to the flash erase size.
    RegionSizeNotEraseAligned {
        /// Configured region size.
        region_size: usize,
        /// Required erase alignment.
        erase_size: usize,
    },
    /// Metadata did not match this backing's const geometry or options.
    MetadataMismatch {
        /// Metadata field being checked.
        field: EmbeddedStorageMetadataField,
        /// Value expected by this backing.
        expected: u32,
        /// Value decoded from metadata.
        actual: u32,
    },
    /// The formatted WAL write granule was not aligned to the flash write size.
    WalWriteGranuleNotWriteAligned {
        /// WAL write granule from format configuration or metadata.
        wal_write_granule: u32,
        /// Required flash write size.
        write_size: usize,
    },
    /// The requested region index was outside the configured range.
    InvalidRegionIndex(u32),
    /// A byte-range operation exceeded the configured Borromean layout.
    OutOfBounds,
    /// Strict pad-only writes found a byte that was already programmed.
    ProgrammedByte {
        /// Absolute byte offset in the flash layout.
        offset: usize,
        /// Byte read from the flash.
        found: u8,
        /// Configured erased byte.
        erased_byte: u8,
    },
    /// An erase completed but did not leave the configured erased byte.
    EraseVerifyFailed {
        /// Absolute byte offset in the flash layout.
        offset: usize,
        /// Byte read from the flash.
        found: u8,
        /// Configured erased byte.
        erased_byte: u8,
    },
}

impl From<DiskError> for EmbeddedStorageError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

/// Errors returned while formatting [`EmbeddedStorageFlash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedStorageFormatError {
    /// Formatting failed while encoding a disk structure.
    Disk(DiskError),
    /// Formatting failed while mutating or validating the backing flash.
    Backing(EmbeddedStorageError),
    /// The configured store is too small for the requested free-region policy.
    InsufficientRegions {
        /// Total number of configured regions.
        region_count: u32,
        /// Requested minimum number of free regions.
        min_free_regions: u32,
    },
    /// `REGION_SIZE` is too small for required metadata, WAL, or free-list bytes.
    RegionSizeTooSmall {
        /// Configured region size in bytes.
        region_size: u32,
        /// Minimum required region size in bytes.
        min_region_size: u32,
    },
    /// `REGION_COUNT` does not fit in a `u32`.
    RegionCountTooLarge,
    /// `REGION_SIZE` does not fit in a `u32`.
    RegionSizeTooLarge,
}

impl From<DiskError> for EmbeddedStorageFormatError {
    fn from(error: DiskError) -> Self {
        Self::Disk(error)
    }
}

impl From<EmbeddedStorageError> for EmbeddedStorageFormatError {
    fn from(error: EmbeddedStorageError) -> Self {
        Self::Backing(error)
    }
}

/// Borromean backing adapter for `embedded-storage` NOR flash drivers.
pub struct EmbeddedStorageFlash<FLASH, const REGION_SIZE: usize, const REGION_COUNT: usize> {
    flash: FLASH,
    options: EmbeddedStorageOptions,
    scratch: [u8; REGION_SIZE],
}

impl<FLASH, const REGION_SIZE: usize, const REGION_COUNT: usize> core::fmt::Debug
    for EmbeddedStorageFlash<FLASH, REGION_SIZE, REGION_COUNT>
where
    FLASH: core::fmt::Debug,
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("EmbeddedStorageFlash")
            .field("flash", &self.flash)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<FLASH, const REGION_SIZE: usize, const REGION_COUNT: usize>
    EmbeddedStorageFlash<FLASH, REGION_SIZE, REGION_COUNT>
where
    FLASH: NorFlash,
{
    /// Creates a new adapter after validating static geometry against the flash.
    pub fn new(
        flash: FLASH,
        options: EmbeddedStorageOptions,
    ) -> Result<Self, EmbeddedStorageError> {
        let adapter = Self {
            flash,
            options,
            scratch: [options.erased_byte; REGION_SIZE],
        };
        adapter.validate_geometry()?;
        Ok(adapter)
    }

    /// Returns the configured options.
    pub fn options(&self) -> EmbeddedStorageOptions {
        self.options
    }

    /// Borrows the wrapped flash object.
    pub fn inner(&self) -> &FLASH {
        &self.flash
    }

    /// Mutably borrows the wrapped flash object.
    pub fn inner_mut(&mut self) -> &mut FLASH {
        &mut self.flash
    }

    /// Consumes the adapter and returns the wrapped flash object.
    pub fn into_inner(self) -> FLASH {
        self.flash
    }

    /// Reads from the metadata region plus data regions as one contiguous space.
    pub fn read_storage(
        &mut self,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), EmbeddedStorageError> {
        let mut copied = 0usize;
        while copied < buffer.len() {
            let absolute = offset
                .checked_add(copied)
                .ok_or(EmbeddedStorageError::LengthOverflow)?;
            let chunk_len = (buffer.len() - copied).min(REGION_SIZE);
            self.read_absolute(absolute, chunk_len, |bytes| {
                buffer[copied..copied + chunk_len].copy_from_slice(bytes);
            })?;
            copied = copied
                .checked_add(chunk_len)
                .ok_or(EmbeddedStorageError::LengthOverflow)?;
        }
        Ok(())
    }

    /// Reads formatted storage metadata.
    pub fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, EmbeddedStorageError> {
        self.read_absolute(0, REGION_SIZE, |_| ())?;
        if self.scratch[..REGION_SIZE]
            .iter()
            .all(|byte| *byte == self.options.erased_byte)
        {
            return Ok(None);
        }

        let metadata = StorageMetadata::decode(&self.scratch[..REGION_SIZE])?;
        self.validate_metadata(metadata)?;
        Ok(Some(metadata))
    }

    /// Writes formatted storage metadata into the metadata region.
    pub fn write_metadata(
        &mut self,
        metadata: StorageMetadata,
    ) -> Result<(), EmbeddedStorageError> {
        self.validate_metadata(metadata)?;
        self.strict_write_absolute_with(0, REGION_SIZE, |target, erased_byte| {
            target.fill(erased_byte);
            metadata.encode_into(target)?;
            Ok(())
        })
    }

    /// Reads bytes from a single data region and passes them to `read`.
    pub fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, EmbeddedStorageError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let absolute = self.region_absolute_offset(region_index, offset, len)?;
        self.read_absolute(absolute, len, read)
    }

    /// Writes bytes to a single data region.
    pub fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), EmbeddedStorageError> {
        let absolute = self.region_absolute_offset(region_index, offset, data.len())?;
        self.strict_write_absolute_with(absolute, data.len(), |target, _| {
            target.copy_from_slice(data);
            Ok(())
        })
    }

    /// Erases a single data region to the configured erased byte.
    pub fn erase_region(&mut self, region_index: u32) -> Result<(), EmbeddedStorageError> {
        let absolute = self.region_absolute_offset(region_index, 0, REGION_SIZE)?;
        self.erase_absolute_range(absolute, REGION_SIZE)
    }

    /// Applies any durability barrier required by the target medium.
    pub fn sync(&mut self) -> Result<(), EmbeddedStorageError> {
        Ok(())
    }

    /// Formats the flash as an empty Borromean store.
    pub fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, EmbeddedStorageFormatError> {
        self.validate_geometry()?;

        let region_size = u32::try_from(REGION_SIZE)
            .map_err(|_| EmbeddedStorageFormatError::RegionSizeTooLarge)?;
        let region_count = u32::try_from(REGION_COUNT)
            .map_err(|_| EmbeddedStorageFormatError::RegionCountTooLarge)?;

        if region_count < 2 + min_free_regions {
            return Err(EmbeddedStorageFormatError::InsufficientRegions {
                region_count,
                min_free_regions,
            });
        }

        let metadata = StorageMetadata::new(
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            self.options.erased_byte,
            wal_record_magic,
        )?;
        self.validate_wal_write_granule(metadata.wal_write_granule)?;

        let wal_header_len = Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN;
        let wal_record_area_offset = metadata.wal_record_area_offset()?;
        let min_region_size_usize = StorageMetadata::ENCODED_LEN
            .max(wal_header_len)
            .max(wal_record_area_offset)
            .max(FreePointerFooter::ENCODED_LEN);
        if REGION_SIZE < min_region_size_usize {
            let min_region_size = u32::try_from(min_region_size_usize).unwrap_or(u32::MAX);
            return Err(EmbeddedStorageFormatError::RegionSizeTooSmall {
                region_size,
                min_region_size,
            });
        }

        self.erase_metadata_region()?;
        self.write_metadata(metadata)?;
        self.sync()?;

        for region_index in 0..region_count {
            self.erase_region(region_index)?;
        }

        self.write_wal_region_prefix(0, metadata, 0, 0)?;

        let footer_offset = REGION_SIZE - FreePointerFooter::ENCODED_LEN;
        for region_index in 1..region_count {
            let next_tail = if region_index + 1 < region_count {
                Some(region_index + 1)
            } else {
                None
            };
            self.write_free_pointer_footer(region_index, footer_offset, metadata, next_tail)?;
        }

        self.sync()?;
        Ok(metadata)
    }

    fn validate_geometry(&self) -> Result<(), EmbeddedStorageError> {
        if REGION_SIZE == 0 {
            return Err(EmbeddedStorageError::ZeroRegionSize);
        }
        if FLASH::READ_SIZE == 0 {
            return Err(EmbeddedStorageError::ZeroReadSize);
        }
        if FLASH::WRITE_SIZE == 0 {
            return Err(EmbeddedStorageError::ZeroWriteSize);
        }
        if FLASH::ERASE_SIZE == 0 {
            return Err(EmbeddedStorageError::ZeroEraseSize);
        }
        let required = required_capacity::<REGION_SIZE, REGION_COUNT>()?;
        let actual = self.flash.capacity();
        if actual < required {
            return Err(EmbeddedStorageError::CapacityTooSmall { required, actual });
        }
        if !REGION_SIZE.is_multiple_of(FLASH::READ_SIZE) {
            return Err(EmbeddedStorageError::RegionSizeNotReadAligned {
                region_size: REGION_SIZE,
                read_size: FLASH::READ_SIZE,
            });
        }
        if !REGION_SIZE.is_multiple_of(FLASH::WRITE_SIZE) {
            return Err(EmbeddedStorageError::RegionSizeNotWriteAligned {
                region_size: REGION_SIZE,
                write_size: FLASH::WRITE_SIZE,
            });
        }
        if !REGION_SIZE.is_multiple_of(FLASH::ERASE_SIZE) {
            return Err(EmbeddedStorageError::RegionSizeNotEraseAligned {
                region_size: REGION_SIZE,
                erase_size: FLASH::ERASE_SIZE,
            });
        }
        Ok(())
    }

    fn validate_metadata(&self, metadata: StorageMetadata) -> Result<(), EmbeddedStorageError> {
        metadata.validate()?;
        let region_size =
            u32::try_from(REGION_SIZE).map_err(|_| EmbeddedStorageError::LengthOverflow)?;
        let region_count =
            u32::try_from(REGION_COUNT).map_err(|_| EmbeddedStorageError::LengthOverflow)?;
        if metadata.region_size != region_size {
            return Err(EmbeddedStorageError::MetadataMismatch {
                field: EmbeddedStorageMetadataField::RegionSize,
                expected: region_size,
                actual: metadata.region_size,
            });
        }
        if metadata.region_count != region_count {
            return Err(EmbeddedStorageError::MetadataMismatch {
                field: EmbeddedStorageMetadataField::RegionCount,
                expected: region_count,
                actual: metadata.region_count,
            });
        }
        if metadata.erased_byte != self.options.erased_byte {
            return Err(EmbeddedStorageError::MetadataMismatch {
                field: EmbeddedStorageMetadataField::ErasedByte,
                expected: u32::from(self.options.erased_byte),
                actual: u32::from(metadata.erased_byte),
            });
        }
        self.validate_wal_write_granule(metadata.wal_write_granule)
    }

    fn validate_wal_write_granule(
        &self,
        wal_write_granule: u32,
    ) -> Result<(), EmbeddedStorageError> {
        let granule =
            usize::try_from(wal_write_granule).map_err(|_| EmbeddedStorageError::LengthOverflow)?;
        if granule == 0 || !granule.is_multiple_of(FLASH::WRITE_SIZE) {
            return Err(EmbeddedStorageError::WalWriteGranuleNotWriteAligned {
                wal_write_granule,
                write_size: FLASH::WRITE_SIZE,
            });
        }
        Ok(())
    }

    fn erase_metadata_region(&mut self) -> Result<(), EmbeddedStorageError> {
        self.erase_absolute_range(0, REGION_SIZE)
    }

    fn erase_absolute_range(
        &mut self,
        offset: usize,
        len: usize,
    ) -> Result<(), EmbeddedStorageError> {
        let end = checked_end(offset, len)?;
        if end > required_capacity::<REGION_SIZE, REGION_COUNT>()? {
            return Err(EmbeddedStorageError::OutOfBounds);
        }
        if !offset.is_multiple_of(FLASH::ERASE_SIZE) || !len.is_multiple_of(FLASH::ERASE_SIZE) {
            return Err(EmbeddedStorageError::RegionSizeNotEraseAligned {
                region_size: len,
                erase_size: FLASH::ERASE_SIZE,
            });
        }
        let from = u32_offset(offset)?;
        let to = u32_offset(end)?;
        self.flash
            .erase(from, to)
            .map_err(|error| EmbeddedStorageError::Flash(error.kind()))?;
        self.verify_erased_absolute(offset, len)
    }

    fn verify_erased_absolute(
        &mut self,
        offset: usize,
        len: usize,
    ) -> Result<(), EmbeddedStorageError> {
        self.read_absolute(offset, len, |_| ())?;
        for index in 0..len {
            let found = self.scratch[index];
            if found != self.options.erased_byte {
                return Err(EmbeddedStorageError::EraseVerifyFailed {
                    offset: offset
                        .checked_add(index)
                        .ok_or(EmbeddedStorageError::LengthOverflow)?,
                    found,
                    erased_byte: self.options.erased_byte,
                });
            }
        }
        Ok(())
    }

    fn read_absolute<R, F>(
        &mut self,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, EmbeddedStorageError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let end = checked_end(offset, len)?;
        if end > required_capacity::<REGION_SIZE, REGION_COUNT>()? {
            return Err(EmbeddedStorageError::OutOfBounds);
        }
        if len == 0 {
            return Ok(read(&[]));
        }

        let read_start = align_down(offset, FLASH::READ_SIZE)?;
        let read_end = align_up(end, FLASH::READ_SIZE)?;
        let read_len = read_end
            .checked_sub(read_start)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        if read_len > REGION_SIZE {
            return Err(EmbeddedStorageError::OutOfBounds);
        }
        self.flash
            .read(u32_offset(read_start)?, &mut self.scratch[..read_len])
            .map_err(|error| EmbeddedStorageError::Flash(error.kind()))?;
        let local_start = offset
            .checked_sub(read_start)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        let local_end = local_start
            .checked_add(len)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        if local_start != 0 {
            self.scratch.copy_within(local_start..local_end, 0);
        }
        Ok(read(&self.scratch[..len]))
    }

    fn strict_write_absolute_with<F>(
        &mut self,
        offset: usize,
        len: usize,
        prepare: F,
    ) -> Result<(), EmbeddedStorageError>
    where
        F: FnOnce(&mut [u8], u8) -> Result<(), EmbeddedStorageError>,
    {
        let end = checked_end(offset, len)?;
        if end > required_capacity::<REGION_SIZE, REGION_COUNT>()? {
            return Err(EmbeddedStorageError::OutOfBounds);
        }
        if len == 0 {
            return Ok(());
        }

        let write_start = align_down(offset, FLASH::WRITE_SIZE)?;
        let write_end = align_up(end, FLASH::WRITE_SIZE)?;
        let write_len = write_end
            .checked_sub(write_start)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        if write_len > REGION_SIZE {
            return Err(EmbeddedStorageError::OutOfBounds);
        }

        self.read_absolute(write_start, write_len, |_| ())?;
        for index in 0..write_len {
            let found = self.scratch[index];
            if found != self.options.erased_byte {
                return Err(EmbeddedStorageError::ProgrammedByte {
                    offset: write_start
                        .checked_add(index)
                        .ok_or(EmbeddedStorageError::LengthOverflow)?,
                    found,
                    erased_byte: self.options.erased_byte,
                });
            }
        }

        self.scratch[..write_len].fill(self.options.erased_byte);
        let local_start = offset
            .checked_sub(write_start)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        let local_end = local_start
            .checked_add(len)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        prepare(
            &mut self.scratch[local_start..local_end],
            self.options.erased_byte,
        )?;
        self.flash
            .write(u32_offset(write_start)?, &self.scratch[..write_len])
            .map_err(|error| EmbeddedStorageError::Flash(error.kind()))
    }

    fn write_wal_region_prefix(
        &mut self,
        region_index: u32,
        metadata: StorageMetadata,
        sequence: u64,
        wal_head: u32,
    ) -> Result<(), EmbeddedStorageError> {
        let prefix_len = metadata.wal_record_area_offset()?;
        let absolute = self.region_absolute_offset(region_index, 0, prefix_len)?;
        self.strict_write_absolute_with(absolute, prefix_len, |target, erased_byte| {
            encode_wal_region_prefix(target, metadata, sequence, wal_head, erased_byte)?;
            Ok(())
        })
    }

    fn write_free_pointer_footer(
        &mut self,
        region_index: u32,
        footer_offset: usize,
        metadata: StorageMetadata,
        next_tail: Option<u32>,
    ) -> Result<(), EmbeddedStorageError> {
        let absolute = self.region_absolute_offset(
            region_index,
            footer_offset,
            FreePointerFooter::ENCODED_LEN,
        )?;
        self.strict_write_absolute_with(absolute, FreePointerFooter::ENCODED_LEN, |target, _| {
            let footer = FreePointerFooter { next_tail };
            footer.encode_into(target, metadata.erased_byte)?;
            Ok(())
        })
    }

    fn region_absolute_offset(
        &self,
        region_index: u32,
        offset: usize,
        len: usize,
    ) -> Result<usize, EmbeddedStorageError> {
        let index = usize::try_from(region_index)
            .map_err(|_| EmbeddedStorageError::InvalidRegionIndex(region_index))?;
        if index >= REGION_COUNT {
            return Err(EmbeddedStorageError::InvalidRegionIndex(region_index));
        }
        let region_end = checked_end(offset, len)?;
        if region_end > REGION_SIZE {
            return Err(EmbeddedStorageError::OutOfBounds);
        }
        let slot = index
            .checked_add(1)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        let region_start = REGION_SIZE
            .checked_mul(slot)
            .ok_or(EmbeddedStorageError::LengthOverflow)?;
        region_start
            .checked_add(offset)
            .ok_or(EmbeddedStorageError::LengthOverflow)
    }
}

impl<FLASH, const REGION_SIZE: usize, const REGION_COUNT: usize> FlashIo
    for EmbeddedStorageFlash<FLASH, REGION_SIZE, REGION_COUNT>
where
    FLASH: NorFlash,
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        Self::read_metadata(self).map_err(StorageIoError::from)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        Self::write_metadata(self, metadata).map_err(StorageIoError::from)
    }

    fn read_region<R, F>(
        &mut self,
        region_index: u32,
        offset: usize,
        len: usize,
        read: F,
    ) -> Result<R, StorageIoError>
    where
        F: FnOnce(&[u8]) -> R,
    {
        Self::read_region(self, region_index, offset, len, read).map_err(StorageIoError::from)
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        Self::write_region(self, region_index, offset, data).map_err(StorageIoError::from)
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        Self::erase_region(self, region_index).map_err(StorageIoError::from)
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        Self::sync(self).map_err(StorageIoError::from)
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        Self::format_empty_store(self, min_free_regions, wal_write_granule, wal_record_magic)
            .map_err(StorageFormatError::from)
    }
}

fn required_capacity<const REGION_SIZE: usize, const REGION_COUNT: usize>(
) -> Result<usize, EmbeddedStorageError> {
    let slots = REGION_COUNT
        .checked_add(1)
        .ok_or(EmbeddedStorageError::LengthOverflow)?;
    REGION_SIZE
        .checked_mul(slots)
        .ok_or(EmbeddedStorageError::LengthOverflow)
}

fn checked_end(offset: usize, len: usize) -> Result<usize, EmbeddedStorageError> {
    offset
        .checked_add(len)
        .ok_or(EmbeddedStorageError::LengthOverflow)
}

fn align_down(value: usize, alignment: usize) -> Result<usize, EmbeddedStorageError> {
    if alignment == 0 {
        return Err(EmbeddedStorageError::LengthOverflow);
    }
    Ok(value - value % alignment)
}

fn align_up(value: usize, alignment: usize) -> Result<usize, EmbeddedStorageError> {
    if alignment == 0 {
        return Err(EmbeddedStorageError::LengthOverflow);
    }
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or(EmbeddedStorageError::LengthOverflow)
    }
}

fn u32_offset(offset: usize) -> Result<u32, EmbeddedStorageError> {
    u32::try_from(offset).map_err(|_| EmbeddedStorageError::LengthOverflow)
}

#[cfg(test)]
mod tests;
