use heapless::Vec;

use super::*;
use crate::{LsmMap, LsmMapMemory, Storage, StorageFormatConfig, StorageMemory};
use embedded_storage_traits::nor_flash::{
    ErrorType, NorFlash, NorFlashError, NorFlashErrorKind, ReadNorFlash,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestNorError {
    NotAligned,
    OutOfBounds,
    ProgrammedByte,
    LogFull,
}

impl NorFlashError for TestNorError {
    fn kind(&self) -> NorFlashErrorKind {
        match self {
            Self::NotAligned => NorFlashErrorKind::NotAligned,
            Self::OutOfBounds => NorFlashErrorKind::OutOfBounds,
            Self::ProgrammedByte | Self::LogFull => NorFlashErrorKind::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestNorOperation {
    Read { offset: u32, len: usize },
    Write { offset: u32, len: usize },
    Erase { from: u32, to: u32 },
}

#[derive(Debug)]
struct TestNorFlash<
    const SIZE: usize,
    const READ_SIZE: usize,
    const WRITE_SIZE: usize,
    const ERASE_SIZE: usize,
    const MAX_LOG: usize,
> {
    bytes: [u8; SIZE],
    erased_byte: u8,
    operations: Vec<TestNorOperation, MAX_LOG>,
}

impl<
        const SIZE: usize,
        const READ_SIZE: usize,
        const WRITE_SIZE: usize,
        const ERASE_SIZE: usize,
        const MAX_LOG: usize,
    > TestNorFlash<SIZE, READ_SIZE, WRITE_SIZE, ERASE_SIZE, MAX_LOG>
{
    fn new(erased_byte: u8) -> Self {
        Self {
            bytes: [erased_byte; SIZE],
            erased_byte,
            operations: Vec::new(),
        }
    }

    fn operations(&self) -> &[TestNorOperation] {
        self.operations.as_slice()
    }

    fn push(&mut self, operation: TestNorOperation) -> Result<(), TestNorError> {
        self.operations
            .push(operation)
            .map_err(|_| TestNorError::LogFull)
    }

    fn range(offset: u32, len: usize) -> Result<core::ops::Range<usize>, TestNorError> {
        let start = usize::try_from(offset).map_err(|_| TestNorError::OutOfBounds)?;
        let end = start.checked_add(len).ok_or(TestNorError::OutOfBounds)?;
        if end > SIZE {
            return Err(TestNorError::OutOfBounds);
        }
        Ok(start..end)
    }
}

impl<
        const SIZE: usize,
        const READ_SIZE: usize,
        const WRITE_SIZE: usize,
        const ERASE_SIZE: usize,
        const MAX_LOG: usize,
    > ErrorType for TestNorFlash<SIZE, READ_SIZE, WRITE_SIZE, ERASE_SIZE, MAX_LOG>
{
    type Error = TestNorError;
}

impl<
        const SIZE: usize,
        const READ_SIZE: usize,
        const WRITE_SIZE: usize,
        const ERASE_SIZE: usize,
        const MAX_LOG: usize,
    > ReadNorFlash for TestNorFlash<SIZE, READ_SIZE, WRITE_SIZE, ERASE_SIZE, MAX_LOG>
{
    const READ_SIZE: usize = READ_SIZE;

    fn read(&mut self, offset: u32, bytes: &mut [u8]) -> Result<(), Self::Error> {
        if READ_SIZE == 0 || !(offset as usize).is_multiple_of(READ_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        if READ_SIZE == 0 || !bytes.len().is_multiple_of(READ_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        let range = Self::range(offset, bytes.len())?;
        bytes.copy_from_slice(&self.bytes[range.clone()]);
        self.push(TestNorOperation::Read {
            offset,
            len: range.len(),
        })
    }

    fn capacity(&self) -> usize {
        SIZE
    }
}

impl<
        const SIZE: usize,
        const READ_SIZE: usize,
        const WRITE_SIZE: usize,
        const ERASE_SIZE: usize,
        const MAX_LOG: usize,
    > NorFlash for TestNorFlash<SIZE, READ_SIZE, WRITE_SIZE, ERASE_SIZE, MAX_LOG>
{
    const WRITE_SIZE: usize = WRITE_SIZE;
    const ERASE_SIZE: usize = ERASE_SIZE;

    fn write(&mut self, offset: u32, bytes: &[u8]) -> Result<(), Self::Error> {
        if WRITE_SIZE == 0 || !(offset as usize).is_multiple_of(WRITE_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        if WRITE_SIZE == 0 || !bytes.len().is_multiple_of(WRITE_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        let range = Self::range(offset, bytes.len())?;
        if self.bytes[range.clone()]
            .iter()
            .any(|byte| *byte != self.erased_byte)
        {
            return Err(TestNorError::ProgrammedByte);
        }
        self.bytes[range.clone()].copy_from_slice(bytes);
        self.push(TestNorOperation::Write {
            offset,
            len: range.len(),
        })
    }

    fn erase(&mut self, from: u32, to: u32) -> Result<(), Self::Error> {
        if ERASE_SIZE == 0 || !(from as usize).is_multiple_of(ERASE_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        if ERASE_SIZE == 0 || !(to as usize).is_multiple_of(ERASE_SIZE) {
            return Err(TestNorError::NotAligned);
        }
        if to < from {
            return Err(TestNorError::OutOfBounds);
        }
        let range = Self::range(from, (to - from) as usize)?;
        self.bytes[range].fill(self.erased_byte);
        self.push(TestNorOperation::Erase { from, to })
    }
}

//= spec/embedded-storage.md#public-api
//= type=test
//# `RING-EMBEDDED-001` The crate MUST expose an `embedded-storage`
//# feature that enables `EmbeddedStorageFlash`, `EmbeddedStorageOptions`,
//# `EmbeddedStorageMetadataField`, `EmbeddedStorageError`, and
//# `EmbeddedStorageFormatError` without enabling the `std` feature.
//# `EmbeddedStorageFlash` MUST expose `new`, `options`, `inner`,
//# `inner_mut`, and `into_inner` accessors for constructing the adapter,
//# inspecting its options, and recovering the wrapped flash object.
#[test]
fn requirement_embedded_storage_feature_exposes_backend_api() {
    let flash = TestNorFlash::<256, 1, 8, 64, 16>::new(0x00);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 3>::new(flash, EmbeddedStorageOptions::new(0x00)).unwrap();
    let _: Option<EmbeddedStorageMetadataField> = None;
    let _: Option<EmbeddedStorageError> = None;
    let _: Option<EmbeddedStorageFormatError> = None;

    assert_eq!(
        backing.options(),
        EmbeddedStorageOptions { erased_byte: 0x00 }
    );
    assert_eq!(backing.inner().operations(), &[]);
    assert_eq!(backing.inner_mut().operations(), &[]);
    assert_eq!(backing.into_inner().operations(), &[]);
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-002` `EmbeddedStorageFlash` MUST use the configured
//# `erased_byte` for metadata empty checks, metadata padding, erase
//# verification, strict write padding, and formatted `StorageMetadata`.
#[test]
fn requirement_embedded_storage_uses_configured_erased_byte() {
    let flash = TestNorFlash::<256, 1, 8, 64, 64>::new(0x00);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 3>::new(flash, EmbeddedStorageOptions::new(0x00)).unwrap();

    assert_eq!(backing.read_metadata().unwrap(), None);

    let metadata = backing.format_empty_store(1, 8, 0xa5).unwrap();
    assert_eq!(metadata.erased_byte, 0x00);
    assert_eq!(backing.read_metadata().unwrap(), Some(metadata));

    let mut metadata_region = [0xff; 64];
    backing.read_storage(0, &mut metadata_region).unwrap();
    assert!(metadata_region[StorageMetadata::ENCODED_LEN..]
        .iter()
        .all(|byte| *byte == 0x00));
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-003` `EmbeddedStorageFlash` MUST map one metadata
//# region followed immediately by all data regions into the wrapped flash
//# address space.
#[test]
fn requirement_embedded_storage_maps_metadata_then_data_regions() {
    let flash = TestNorFlash::<256, 1, 8, 64, 32>::new(0xee);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 3>::new(flash, EmbeddedStorageOptions::new(0xee)).unwrap();

    backing.write_region(0, 0, &[0x11; 8]).unwrap();
    backing.write_region(2, 8, &[0x33; 8]).unwrap();

    let mut first_region_prefix = [0u8; 8];
    let mut third_region_bytes = [0u8; 8];
    backing.read_storage(64, &mut first_region_prefix).unwrap();
    backing
        .read_storage(64 * 3 + 8, &mut third_region_bytes)
        .unwrap();

    assert_eq!(first_region_prefix, [0x11; 8]);
    assert_eq!(third_region_bytes, [0x33; 8]);
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-004` `EmbeddedStorageFlash` MUST reject capacity,
//# region-alignment, and WAL write-granule configurations that cannot be
//# represented safely by the wrapped `NorFlash`.
#[test]
fn requirement_embedded_storage_rejects_invalid_geometry_and_wal_granule() {
    let small = TestNorFlash::<128, 1, 8, 64, 8>::new(0xff);
    assert_eq!(
        EmbeddedStorageFlash::<_, 64, 3>::new(small, EmbeddedStorageOptions::default())
            .unwrap_err(),
        EmbeddedStorageError::CapacityTooSmall {
            required: 256,
            actual: 128,
        }
    );

    let misaligned = TestNorFlash::<320, 1, 16, 64, 8>::new(0xff);
    assert_eq!(
        EmbeddedStorageFlash::<_, 72, 3>::new(misaligned, EmbeddedStorageOptions::default())
            .unwrap_err(),
        EmbeddedStorageError::RegionSizeNotWriteAligned {
            region_size: 72,
            write_size: 16,
        }
    );

    let flash = TestNorFlash::<256, 1, 8, 64, 32>::new(0xff);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 3>::new(flash, EmbeddedStorageOptions::default()).unwrap();
    assert_eq!(
        backing.format_empty_store(1, 4, 0xa5),
        Err(EmbeddedStorageFormatError::Backing(
            EmbeddedStorageError::WalWriteGranuleNotWriteAligned {
                wal_write_granule: 4,
                write_size: 8,
            },
        ))
    );
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-005` Strict pad-only writes MUST read the aligned
//# hardware write span first and reject the write if any byte in that span
//# is not the configured erased byte.
#[test]
fn requirement_embedded_storage_strict_pad_only_rejects_programmed_span() {
    let flash = TestNorFlash::<128, 1, 8, 64, 16>::new(0x00);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 1>::new(flash, EmbeddedStorageOptions::new(0x00)).unwrap();

    backing.write_region(0, 0, &[1, 2, 3]).unwrap();

    assert_eq!(
        backing.write_region(0, 4, &[4]),
        Err(EmbeddedStorageError::ProgrammedByte {
            offset: 64,
            found: 1,
            erased_byte: 0x00,
        })
    );
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-006` Formatting through `EmbeddedStorageFlash` MUST
//# initialize WAL region prefixes as one contiguous write containing
//# `Header`, `WalRegionPrologue`, and erased bytes up to
//# `wal_record_area_offset`.
#[test]
fn requirement_embedded_storage_format_writes_wal_prefix_contiguously() {
    let flash = TestNorFlash::<256, 1, 8, 64, 64>::new(0x00);
    let mut backing =
        EmbeddedStorageFlash::<_, 64, 3>::new(flash, EmbeddedStorageOptions::new(0x00)).unwrap();

    let metadata = backing.format_empty_store(1, 8, 0xa5).unwrap();
    let prefix_len = metadata.wal_record_area_offset().unwrap();
    let mut region = [0xff; 64];
    backing
        .read_region(0, 0, region.len(), |bytes| region.copy_from_slice(bytes))
        .unwrap();

    let header = Header::decode(&region[..Header::ENCODED_LEN]).unwrap();
    assert_eq!(header.collection_id, crate::CollectionId(0));
    assert_eq!(header.collection_format, crate::WAL_V1_FORMAT);

    let prologue = WalRegionPrologue::decode(
        &region[Header::ENCODED_LEN..Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN],
        metadata.region_count,
    )
    .unwrap();
    assert_eq!(prologue.log_head_region_index, 0);
    assert!(
        region[Header::ENCODED_LEN + WalRegionPrologue::ENCODED_LEN..prefix_len]
            .iter()
            .all(|byte| *byte == metadata.erased_byte)
    );
    assert!(backing
        .inner()
        .operations()
        .contains(&TestNorOperation::Write {
            offset: 64,
            len: prefix_len,
        }));
}

//= spec/embedded-storage.md#backend-behavior
//= type=test
//# `RING-EMBEDDED-007` Formatted `EmbeddedStorageFlash` storage MUST be
//# usable through the generic Borromean storage API with a non-`0xff`
//# erased byte.
#[test]
fn requirement_embedded_storage_works_with_generic_storage_api_non_ff() {
    const REGION_SIZE: usize = 256;
    const REGION_COUNT: usize = 8;
    let flash = TestNorFlash::<{ REGION_SIZE * (REGION_COUNT + 1) }, 1, 8, 64, 512>::new(0x00);
    let mut backing = EmbeddedStorageFlash::<_, REGION_SIZE, REGION_COUNT>::new(
        flash,
        EmbeddedStorageOptions::new(0x00),
    )
    .unwrap();

    let collection_id = {
        let mut storage_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, 8>::new();
        let mut storage = Storage::<_, REGION_SIZE, REGION_COUNT, 8>::format(
            &mut backing,
            StorageFormatConfig::new(2, 8, 0xa5),
            &mut storage_memory,
        )
        .unwrap();
        let mut map_memory = LsmMapMemory::<u16, u16>::new();
        let mut map = LsmMap::<u16, u16>::new(&mut storage, &mut map_memory).unwrap();

        map.set(&mut storage, 7, 70).unwrap();
        assert_eq!(
            map.get(&mut storage, &7, |_, value| *value).unwrap(),
            Some(70)
        );
        map.collection_id()
    };

    let mut reopen_memory = StorageMemory::<REGION_SIZE, REGION_COUNT, 8>::new();
    let mut reopened =
        Storage::<_, REGION_SIZE, REGION_COUNT, 8>::open(&mut backing, &mut reopen_memory).unwrap();
    let mut reopened_map_memory = LsmMapMemory::<u16, u16>::new();
    let mut reopened_map =
        LsmMap::<u16, u16>::open(collection_id, &mut reopened, &mut reopened_map_memory).unwrap();

    assert_eq!(
        reopened_map
            .get(&mut reopened, &7, |_, value| *value)
            .unwrap(),
        Some(70)
    );
}
