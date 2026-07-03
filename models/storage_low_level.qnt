// Low-level storage model for decoded metadata, region headers, and the
// region-local prologues that startup scanning consumes.
//
// This model intentionally starts below WAL-record replay. It models the
// physical storage facts that startup scanning consumes after decoding:
// StorageMetadata, per-region Header values, and whether each header decoded
// successfully. Raw bytes, CRC-32C, erased slots, prologues, and scan actions
// are introduced in later steps.

module storage_low_level {
  // Top-level store metadata stored outside the data-region ring. This mirrors
  // src/disk.rs StorageMetadata after checksum validation and decoding.
  type StorageMetadata = {
    // Stable on-disk metadata format version.
    storageVersion: int,
    // Size of each data region in bytes.
    regionSize: int,
    // Number of data regions in the ring, excluding the metadata block.
    regionCount: int,
    // Recovery reserve target configured at format time.
    minFreeRegions: int,
    // Number of transaction-log slots supported by the formatted store.
    transactionLogCount: int,
    // Alignment used for WAL-record starts and encoded record lengths.
    walWriteGranule: int,
    // Physical byte value produced by erasing the backing medium.
    erasedByte: int,
    // Physical byte that marks the start of an encoded WAL record.
    walRecordMagic: int,
  }

  // Decoded per-region header. Header validity is tracked separately on
  // Region so corrupt or erased regions can still carry placeholder fields.
  type RegionHeader = {
    // Monotonic physical-region sequence used to select the newest facts.
    sequence: int,
    // Collection that owns the region. Collection id 0 is internal storage.
    collectionId: int,
    // Collection-defined format for this region's payload/prologue.
    collectionFormat: int,
  }

  // Position inside the materialized free-space FIFO.
  type FreeQueuePosition = {
    // Free-space metadata region that contains the queue entry slot.
    regionIndex: int,
    // Zero-based slot inside that free-space metadata region.
    entryIndex: int,
  }

  // Prologue stored after the header in regions that carry WAL-record logs.
  // In this model that means main-WAL regions and transaction-log regions.
  // This identifies the log chain; logical WAL records select allocator basis.
  type WalPrologue = {
    // Region index that startup should treat as the head of this log.
    logHeadRegionIndex: int,
  }

  // Prologue stored after the header in free_space_v2 metadata regions. These
  // fields describe how to interpret the materialized free-space queue once a
  // retained WAL Head record selects this region as the current basis.
  //
  // Free-space materialization follows the same frontier discipline as the
  // object log: the next metadata region is allocated before the previous
  // region is materialized and flushed with a durable link to it. That makes
  // free-space cursor positions stable even when they point into the in-memory
  // frontier. Whether this successor has been materialized is not stored here;
  // it is recovered from retained WAL records.
  type FreeSpaceRegionPrologue = {
    // First allocatable ready entry in the materialized free-space FIFO.
    allocationHead: FreeQueuePosition,
    // Boundary between erased ready entries and dirty pending entries.
    readyBoundary: FreeQueuePosition,
    // Append cursor for newly detached regions.
    appendTail: FreeQueuePosition,
    // Preallocated successor metadata region. For a valid materialized
    // free-space region in this model this is a region id, even if the successor
    // is still only WAL-resident. NoRegion is only the neutral placeholder for
    // invalid regions.
    nextMetadataRegion: int,
    // Number of initialized freeEntries slots in this metadata region.
    entryCount: int,
  }

  type RegionType =
    | InvalidRegion
    | ErasedRegion
    | WalLogRegion
    | FreeSpaceRegion
    | CollectionRegion

  // Decoded physical-region view. Header decoding is shared by every region;
  // region-specific decoded data lives in the payload list named by regionType
  // and typeIndex. InvalidRegion and ErasedRegion have no payload.
  type Region = {
    // Whether Header::decode succeeded for this physical region.
    headerValid: bool,
    // Decoded header fields when headerValid is true; placeholder otherwise.
    header: RegionHeader,
    // Payload kind selected after decoding the header and region-local data.
    regionType: RegionType,
    // Index into the payload list for regionType, or NoRegion for
    // InvalidRegion/ErasedRegion.
    typeIndex: int,
  }

  type WalLogRegionPayload = {
    // Whether the WAL-record-log prologue decoded and validated.
    prologueValid: bool,
    // Decoded log prologue for main-WAL or transaction-log regions.
    prologue: WalPrologue,
  }

  type FreeSpaceRegionPayload = {
    // Whether the free-space metadata prologue decoded and validated.
    prologueValid: bool,
    // Decoded local free-space metadata prologue for free_space_v2 regions.
    prologue: FreeSpaceRegionPrologue,
    // Decoded free region indices stored in a free-space metadata region.
    freeEntries: List[int],
  }

  type CollectionRegionPayload = { marker: int }

  type WalRecordKind =
    | WalHead
    | WalSnapshot
    | WalPreallocateFreeSpaceRegion
    | WalAllocateRegion
    | WalFreeRegion
    | WalEraseFreeRegionSpan
    | WalMaterializeFreeSpaceRegion

  type FreeSpaceBasisKind =
    | NoBasis
    | WalResidentBasis
    | MaterializedRegionBasis
    | SnapshotBasis

  // Recovered logical free-space basis selected by retained WAL records.
  type FreeSpaceBasis = {
    // Whether this basis is absent, materialized in a region, or inline.
    kind: FreeSpaceBasisKind,
    // Materialized region selected by a Head record, or NoRegion.
    sourceRegionIndex: int,
    // Free-space queue entries visible from this basis.
    entries: List[int],
    // Cursor state that interprets entries.
    allocationHead: FreeQueuePosition,
    readyBoundary: FreeQueuePosition,
    appendTail: FreeQueuePosition,
    // Materialized free-space metadata regions selected or confirmed by WAL.
    materializedRegions: List[int],
    // Preallocated metadata region that currently receives WAL-resident
    // frontier entries.
    frontierRegionIndex: int,
    // True after replay sees a committed materialization record whose physical
    // region is missing or corrupt. Later recovery must rewrite the chain.
    needsChainRewrite: bool,
  }

  // Decoded logical WAL record view for the bounded model. Fields unused by a
  // kind contain neutral values, which keeps the record shape simple.
  type WalRecord = {
    kind: WalRecordKind,
    collectionId: int,
    collectionType: int,
    regionIndex: int,
    snapshotBasis: FreeSpaceBasis,
    allocationHeadAfter: FreeQueuePosition,
    appendTailAfter: FreeQueuePosition,
    eraseCount: int,
    readyBoundaryAfter: FreeQueuePosition,
    nextFrontierRegionIndex: int,
  }

  type FormatPhase =
    | FormatBlank
    | FormatMetadataWritten
    | FormatWalRegionWritten
    | FormatFreeSpacePreallocated
    | FormatFreeRegionsLogged
    | FormatDone

  pure val RegionIds = Set(0, 1, 2, 3, 4, 5, 6, 7, 8)
  pure val ModelRegionCount = RegionIds.size()
  pure val NoRegion = -1

  pure val WalRegionId = 0
  pure val FreeSpaceRegionId = 1
  pure val TransactionLogRegionId = 2
  pure val InitialCollectionRegionAId = 3
  pure val InitialCollectionRegionBId = 4
  pure val FreeSpaceFrontierRegionId = 5
  pure val InitialFreeLog = [6, 7, 8]
  pure val InitialUsedRegionIds =
    Set(
      WalRegionId,
      FreeSpaceRegionId,
      TransactionLogRegionId,
      InitialCollectionRegionAId,
      InitialCollectionRegionBId,
      FreeSpaceFrontierRegionId,
    )

  // Matches implementation uses of CollectionId(0) for internal storage
  // regions: main WAL, transaction logs, and free-space metadata.
  pure val InternalCollectionId = 0
  pure val StorageVersion = 2

  // Chosen erased flash byte for this bounded model. 255 == 0xff.
  pure val ErasedByte = 255

  // Chosen configured WAL record magic for this bounded model. The
  // implementation stores this value in StorageMetadata; 165 == 0xa5.
  pure val WalRecordMagic = 165

  // These mirror src/disk.rs constants:
  // MAIN_WAL_V2_FORMAT = 0
  // TRANSACTION_LOG_V2_FORMAT = 1
  // FREE_SPACE_V2_FORMAT = 2
  pure val MainWalV2Format = 0
  pure val TransactionLogV2Format = 1
  pure val FreeSpaceV2Format = 2

  // Mirrors src/collections/map/mod.rs MAP_REGION_V2_FORMAT = 4.
  pure val MapRegionV2Format = 4

  // Internal logical collection type used by this model for free-space basis
  // records. The Rust record field is collection_type; we reuse the private
  // free-space format code as the bounded model's type value.
  pure val FreeSpaceCollectionType = FreeSpaceV2Format

  pure val ModelCollectionId = 7

  pure val InitialMetadata = {
    storageVersion: StorageVersion,
    regionSize: 1024,
    regionCount: ModelRegionCount,
    minFreeRegions: 1,
    transactionLogCount: 1,
    walWriteGranule: 8,
    erasedByte: ErasedByte,
    walRecordMagic: WalRecordMagic,
  }

  pure val BlankMetadata = {
    storageVersion: 0,
    regionSize: 0,
    regionCount: ModelRegionCount,
    minFreeRegions: 0,
    transactionLogCount: 0,
    walWriteGranule: 0,
    erasedByte: ErasedByte,
    walRecordMagic: 0,
  }

  pure val EmptyHeader = {
    sequence: 0,
    collectionId: 0,
    collectionFormat: 0,
  }

  pure val ZeroPosition = {
    regionIndex: 0,
    entryIndex: 0,
  }

  pure def freeQueuePosition(
    regionIndex: int,
    entryIndex: int,
  ): FreeQueuePosition = {
    regionIndex: regionIndex,
    entryIndex: entryIndex,
  }

  pure val InitialFreeHead = freeQueuePosition(FreeSpaceRegionId, 0)

  pure val InitialFreeTail = freeQueuePosition(FreeSpaceFrontierRegionId, 0)

  pure val FormattedFreeLog = [2, 3, 4, 5, 6, 7, 8]

  pure val FormattedFreeHead = freeQueuePosition(FreeSpaceRegionId, 0)

  pure val FormattedFreeTail =
    freeQueuePosition(FreeSpaceRegionId, FormattedFreeLog.length())

  pure def internalHeader(sequence: int, collectionFormat: int): RegionHeader = {
    sequence: sequence,
    collectionId: InternalCollectionId,
    collectionFormat: collectionFormat,
  }

  pure def collectionHeader(sequence: int): RegionHeader = {
    sequence: sequence,
    collectionId: ModelCollectionId,
    collectionFormat: MapRegionV2Format,
  }

  pure def walRecordLogPrologue(
    headRegion: int,
  ): WalPrologue = {
    logHeadRegionIndex: headRegion,
  }

  pure val EmptyWalRecordLogPrologue = {
    logHeadRegionIndex: 0,
  }

  pure val InitialFreeSpacePrologue = {
    allocationHead: InitialFreeHead,
    readyBoundary: InitialFreeTail,
    appendTail: InitialFreeTail,
    nextMetadataRegion: FreeSpaceFrontierRegionId,
    entryCount: InitialFreeLog.length(),
  }

  pure val EmptyFreeSpacePrologue = {
    allocationHead: ZeroPosition,
    readyBoundary: ZeroPosition,
    appendTail: ZeroPosition,
    nextMetadataRegion: NoRegion,
    entryCount: 0,
  }

  pure val NoFreeSpaceBasis = {
    kind: NoBasis,
    sourceRegionIndex: NoRegion,
    entries: [],
    allocationHead: ZeroPosition,
    readyBoundary: ZeroPosition,
    appendTail: ZeroPosition,
    materializedRegions: [],
    frontierRegionIndex: NoRegion,
    needsChainRewrite: false,
  }

  pure def freeSpaceBasis(
    kind: FreeSpaceBasisKind,
    sourceRegionIndex: int,
    entries: List[int],
    allocationHead: FreeQueuePosition,
    readyBoundary: FreeQueuePosition,
    appendTail: FreeQueuePosition,
    materializedRegions: List[int],
    frontierRegionIndex: int,
    needsChainRewrite: bool,
  ): FreeSpaceBasis = {
    kind: kind,
    sourceRegionIndex: sourceRegionIndex,
    entries: entries,
    allocationHead: allocationHead,
    readyBoundary: readyBoundary,
    appendTail: appendTail,
    materializedRegions: materializedRegions,
    frontierRegionIndex: frontierRegionIndex,
    needsChainRewrite: needsChainRewrite,
  }

  pure def walHeadRecord(
    collectionId: int,
    collectionType: int,
    regionIndex: int,
  ): WalRecord = {
    kind: WalHead,
    collectionId: collectionId,
    collectionType: collectionType,
    regionIndex: regionIndex,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: ZeroPosition,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walSnapshotRecord(
    collectionId: int,
    collectionType: int,
    basis: FreeSpaceBasis,
  ): WalRecord = {
    kind: WalSnapshot,
    collectionId: collectionId,
    collectionType: collectionType,
    regionIndex: NoRegion,
    snapshotBasis: basis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: ZeroPosition,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walPreallocateFreeSpaceRegionRecord(
    regionIndex: int,
  ): WalRecord = {
    kind: WalPreallocateFreeSpaceRegion,
    collectionId: InternalCollectionId,
    collectionType: FreeSpaceCollectionType,
    regionIndex: regionIndex,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: ZeroPosition,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walAllocateRegionRecord(
    regionIndex: int,
    allocationHeadAfter: FreeQueuePosition,
  ): WalRecord = {
    kind: WalAllocateRegion,
    collectionId: InternalCollectionId,
    collectionType: FreeSpaceCollectionType,
    regionIndex: regionIndex,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: allocationHeadAfter,
    appendTailAfter: ZeroPosition,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walFreeRegionRecord(
    regionIndex: int,
    appendTailAfter: FreeQueuePosition,
  ): WalRecord = {
    kind: WalFreeRegion,
    collectionId: InternalCollectionId,
    collectionType: FreeSpaceCollectionType,
    regionIndex: regionIndex,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: appendTailAfter,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walEraseFreeRegionSpanRecord(
    count: int,
    readyBoundaryAfter: FreeQueuePosition,
  ): WalRecord = {
    kind: WalEraseFreeRegionSpan,
    collectionId: InternalCollectionId,
    collectionType: FreeSpaceCollectionType,
    regionIndex: NoRegion,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: ZeroPosition,
    eraseCount: count,
    readyBoundaryAfter: readyBoundaryAfter,
    nextFrontierRegionIndex: NoRegion,
  }

  pure def walMaterializeFreeSpaceRegionRecord(
    regionIndex: int,
    nextFrontierRegionIndex: int,
  ): WalRecord = {
    kind: WalMaterializeFreeSpaceRegion,
    collectionId: InternalCollectionId,
    collectionType: FreeSpaceCollectionType,
    regionIndex: regionIndex,
    snapshotBasis: NoFreeSpaceBasis,
    allocationHeadAfter: ZeroPosition,
    appendTailAfter: ZeroPosition,
    eraseCount: 0,
    readyBoundaryAfter: ZeroPosition,
    nextFrontierRegionIndex: nextFrontierRegionIndex,
  }

  pure val WalRegionPayloadIndex = 0
  pure val TransactionLogRegionPayloadIndex = 1
  pure val InitialFreeSpacePayloadIndex = 0
  pure val InitialCollectionPayloadAIndex = 0
  pure val InitialCollectionPayloadBIndex = 1

  pure val InitialWalRegion = {
    headerValid: true,
    header: internalHeader(1, MainWalV2Format),
    regionType: WalLogRegion,
    typeIndex: WalRegionPayloadIndex,
  }

  pure val InitialFreeSpaceRegion = {
    headerValid: true,
    header: internalHeader(0, FreeSpaceV2Format),
    regionType: FreeSpaceRegion,
    typeIndex: InitialFreeSpacePayloadIndex,
  }

  pure val InitialTransactionLogRegion = {
    headerValid: true,
    header: internalHeader(2, TransactionLogV2Format),
    regionType: WalLogRegion,
    typeIndex: TransactionLogRegionPayloadIndex,
  }

  pure def collectionRegion(
    sequence: int,
    payloadIndex: int,
  ): Region = {
    headerValid: true,
    header: collectionHeader(sequence),
    regionType: CollectionRegion,
    typeIndex: payloadIndex,
  }

  pure def invalidRegion(headerValid: bool, header: RegionHeader): Region = {
    headerValid: headerValid,
    header: header,
    regionType: InvalidRegion,
    typeIndex: NoRegion,
  }

  pure val ErasedPhysicalRegion = {
    headerValid: false,
    header: EmptyHeader,
    regionType: ErasedRegion,
    typeIndex: NoRegion,
  }

  pure val BlankRegions = [
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val RegionsAfterMainWalRegion = [
    InitialWalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val RegionsAfterFreeSpaceRoot = [
    ErasedPhysicalRegion,
    InitialFreeSpaceRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val RegionsAfterWalLogs = [
    InitialWalRegion,
    InitialFreeSpaceRegion,
    InitialTransactionLogRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val InitialRegions = [
    InitialWalRegion,
    InitialFreeSpaceRegion,
    InitialTransactionLogRegion,
    collectionRegion(3, InitialCollectionPayloadAIndex),
    collectionRegion(4, InitialCollectionPayloadBIndex),
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val InitialWalLogRegionPayloads = [
    {
      prologueValid: true,
      prologue: walRecordLogPrologue(WalRegionId),
    },
    {
      prologueValid: true,
      prologue: walRecordLogPrologue(TransactionLogRegionId),
    },
  ]

  pure val FormatWalLogRegionPayloads = [
    {
      prologueValid: true,
      prologue: walRecordLogPrologue(WalRegionId),
    },
  ]

  pure val InitialFreeSpaceRegionPayloads = [
    {
      prologueValid: true,
      prologue: InitialFreeSpacePrologue,
      freeEntries: InitialFreeLog,
    },
  ]

  pure val InitialCollectionRegionPayloads = [{ marker: 0 }, { marker: 0 }]

  // Top-level decoded metadata.
  var metadata: StorageMetadata

  // Data regions only. The static metadata region is outside this list,
  // matching the implementation's region_count semantics.
  var regions: List[Region]

  // Payloads for physical regions whose regionType is WalLogRegion.
  var walLogRegions: List[WalLogRegionPayload]

  // Payloads for physical regions whose regionType is FreeSpaceRegion.
  var freeSpaceRegions: List[FreeSpaceRegionPayload]

  // Payloads for physical regions whose regionType is CollectionRegion.
  var collectionRegions: List[CollectionRegionPayload]

  // Retained decoded logical WAL records. This log is the root source of truth
  // for selecting the current free-space basis.
  var walRecords: List[WalRecord]

  // Formatting progress for the bounded model. Startup/recovery actions are
  // added later; for now this lets init model an erased device and formatting
  // actions build the first durable layout in implementation order.
  var formatPhase: FormatPhase

  // Next region consumed by formatting. It starts at the WAL region, advances
  // through the free-space preallocation, and then walks remaining erased
  // regions as they are appended to the free-space WAL log.
  var formatNextRegion: int

  action init = all {
    metadata' = BlankMetadata,
    regions' = BlankRegions,
    walLogRegions' = [],
    freeSpaceRegions' = [],
    collectionRegions' = [],
    walRecords' = [],
    formatPhase' = FormatBlank,
    formatNextRegion' = 0,
  }

  // Write and validate the static storage metadata block.
  action FormatWriteMetadata = all {
    formatPhase == FormatBlank,
    formatNextRegion == 0,

    metadata' = InitialMetadata,
    formatPhase' = FormatMetadataWritten,

    regions' = regions,
    walLogRegions' = walLogRegions,
    freeSpaceRegions' = freeSpaceRegions,
    collectionRegions' = collectionRegions,
    walRecords' = walRecords,
    formatNextRegion' = formatNextRegion,
  }

  // Claim the first physical region as the main WAL. All other regions remain
  // physically erased until later operations allocate or materialize them.
  action FormatUseMainWalRegion = all {
    formatPhase == FormatMetadataWritten,
    formatNextRegion == WalRegionId,

    regions' = RegionsAfterMainWalRegion,
    walLogRegions' = FormatWalLogRegionPayloads,
    formatPhase' = FormatWalRegionWritten,
    formatNextRegion' = FreeSpaceRegionId,

    metadata' = metadata,
    freeSpaceRegions' = freeSpaceRegions,
    collectionRegions' = collectionRegions,
    walRecords' = walRecords,
  }

  // Reserve the next erased region as the WAL-resident free-space frontier.
  // This creates the logical free-space basis; no free-space metadata region is
  // materialized during format.
  action FormatPreallocateFreeSpaceRegion = all {
    formatPhase == FormatWalRegionWritten,
    formatNextRegion == FreeSpaceRegionId,

    walRecords' = walRecords.append(
      walPreallocateFreeSpaceRegionRecord(formatNextRegion)),
    formatPhase' = FormatFreeSpacePreallocated,
    formatNextRegion' = 2,

    metadata' = metadata,
    regions' = regions,
    walLogRegions' = walLogRegions,
    freeSpaceRegions' = freeSpaceRegions,
    collectionRegions' = collectionRegions,
  }

  // Append the next unused erased region to the free-space log. The append tail
  // advances in the WAL-resident frontier region.
  action FormatFreeOneRemainingRegion = {
    val appendTailAfter =
      freeQueuePosition(FreeSpaceRegionId, formatNextRegion - 1)
    all {
      formatPhase == FormatFreeSpacePreallocated
        or formatPhase == FormatFreeRegionsLogged,
      formatNextRegion >= 2,
      formatNextRegion < ModelRegionCount,

      walRecords' = walRecords.append(
        walFreeRegionRecord(formatNextRegion, appendTailAfter)),
      formatPhase' = FormatFreeRegionsLogged,
      formatNextRegion' = formatNextRegion + 1,

      metadata' = metadata,
      regions' = regions,
      walLogRegions' = walLogRegions,
      freeSpaceRegions' = freeSpaceRegions,
      collectionRegions' = collectionRegions,
    }
  }

  // Formatted storage starts erased, so all free entries logged during format
  // are immediately ready once this WAL record advances the ready boundary.
  action FormatMarkFreedRegionsReady = all {
    formatPhase == FormatFreeRegionsLogged,
    formatNextRegion == ModelRegionCount,

    walRecords' = walRecords.append(
      walEraseFreeRegionSpanRecord(
        FormattedFreeLog.length(),
        FormattedFreeTail)),
    formatPhase' = FormatDone,
    formatNextRegion' = formatNextRegion,

    metadata' = metadata,
    regions' = regions,
    walLogRegions' = walLogRegions,
    freeSpaceRegions' = freeSpaceRegions,
    collectionRegions' = collectionRegions,
  }

  action FormatDoneStutter = all {
    formatPhase == FormatDone,

    metadata' = metadata,
    regions' = regions,
    walLogRegions' = walLogRegions,
    freeSpaceRegions' = freeSpaceRegions,
    collectionRegions' = collectionRegions,
    walRecords' = walRecords,
    formatPhase' = formatPhase,
    formatNextRegion' = formatNextRegion,
  }

  action formatStep = any {
    FormatWriteMetadata,
    FormatUseMainWalRegion,
    FormatPreallocateFreeSpaceRegion,
    FormatFreeOneRemainingRegion,
    FormatMarkFreedRegionsReady,
    FormatDoneStutter,
  }

  pure def regionIndexInRange(
    regionLog: List[Region],
    regionIndex: int,
  ): bool =
    regionIndex.in(RegionIds) and regionIndex < regionLog.length()

  pure def payloadIndexInRange(typeIndex: int, payloadCount: int): bool =
    typeIndex >= 0 and typeIndex < payloadCount

  pure def walPayload(
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    regionIndex: int,
  ): WalLogRegionPayload =
    walPayloads.nth(regionLog.nth(regionIndex).typeIndex)

  pure def freeSpacePayload(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    regionIndex: int,
  ): FreeSpaceRegionPayload =
    freePayloads.nth(regionLog.nth(regionIndex).typeIndex)

  pure def collectionPayload(
    regionLog: List[Region],
    collectionPayloads: List[CollectionRegionPayload],
    regionIndex: int,
  ): CollectionRegionPayload =
    collectionPayloads.nth(regionLog.nth(regionIndex).typeIndex)

  pure def isMainWalRegion(
    regionLog: List[Region],
    regionIndex: int,
  ): bool =
    regionIndexInRange(regionLog, regionIndex)
    and regionLog.nth(regionIndex).headerValid
    and regionLog.nth(regionIndex).regionType == WalLogRegion
    and regionLog.nth(regionIndex).header.collectionId == InternalCollectionId
    and regionLog.nth(regionIndex).header.collectionFormat == MainWalV2Format

  pure def isTransactionLogRegion(
    regionLog: List[Region],
    regionIndex: int,
  ): bool =
    regionIndexInRange(regionLog, regionIndex)
    and regionLog.nth(regionIndex).headerValid
    and regionLog.nth(regionIndex).regionType == WalLogRegion
    and regionLog.nth(regionIndex).header.collectionId == InternalCollectionId
    and regionLog.nth(regionIndex).header.collectionFormat == TransactionLogV2Format

  pure def isFreeSpaceMetadataRegion(
    regionLog: List[Region],
    regionIndex: int,
  ): bool =
    regionIndexInRange(regionLog, regionIndex)
    and regionLog.nth(regionIndex).headerValid
    and regionLog.nth(regionIndex).regionType == FreeSpaceRegion
    and regionLog.nth(regionIndex).header.collectionId == InternalCollectionId
    and regionLog.nth(regionIndex).header.collectionFormat == FreeSpaceV2Format

  pure def validHeaderRegionIds(regionLog: List[Region]): Set[int] =
    RegionIds.filter(i =>
      i < regionLog.length() and regionLog.nth(i).headerValid)

  pure def maxSeenSequence(regionLog: List[Region]): int =
    RegionIds.fold(0, (maxSeq, i) =>
      if (
        i < regionLog.length()
        and regionLog.nth(i).headerValid
        and regionLog.nth(i).header.sequence > maxSeq
      ) {
        regionLog.nth(i).header.sequence
      } else {
        maxSeq
      })

  pure def basisFromFreeSpaceRegion(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    regionIndex: int,
  ): FreeSpaceBasis =
    if (
      isFreeSpaceMetadataRegion(regionLog, regionIndex)
      and payloadIndexInRange(
        regionLog.nth(regionIndex).typeIndex,
        freePayloads.length(),
      )
      and freeSpacePayload(
        regionLog,
        freePayloads,
        regionIndex,
      ).prologueValid
    ) {
      freeSpaceBasis(
        MaterializedRegionBasis,
        regionIndex,
        freeSpacePayload(regionLog, freePayloads, regionIndex).freeEntries,
        freeSpacePayload(
          regionLog,
          freePayloads,
          regionIndex,
        ).prologue.allocationHead,
        freeSpacePayload(
          regionLog,
          freePayloads,
          regionIndex,
        ).prologue.readyBoundary,
        freeSpacePayload(
          regionLog,
          freePayloads,
          regionIndex,
        ).prologue.appendTail,
        [regionIndex],
        freeSpacePayload(
          regionLog,
          freePayloads,
          regionIndex,
        ).prologue.nextMetadataRegion,
        false,
      )
    } else {
      NoFreeSpaceBasis
    }

  pure def basisFromSnapshot(basis: FreeSpaceBasis): FreeSpaceBasis =
    freeSpaceBasis(
      SnapshotBasis,
      NoRegion,
      basis.entries,
      basis.allocationHead,
      basis.readyBoundary,
      basis.appendTail,
      basis.materializedRegions,
      basis.frontierRegionIndex,
      basis.needsChainRewrite,
    )

  pure def basisFromPreallocatedFreeSpaceRegion(
    record: WalRecord,
  ): FreeSpaceBasis =
    if (
      isFreeSpaceBasisRecord(record)
      and record.regionIndex.in(RegionIds)
    ) {
      freeSpaceBasis(
        WalResidentBasis,
        NoRegion,
        [],
        freeQueuePosition(record.regionIndex, 0),
        freeQueuePosition(record.regionIndex, 0),
        freeQueuePosition(record.regionIndex, 0),
        [],
        record.regionIndex,
        false,
      )
    } else {
      NoFreeSpaceBasis
    }

  pure def isFreeSpaceBasisRecord(record: WalRecord): bool =
    record.collectionId == InternalCollectionId
    and record.collectionType == FreeSpaceCollectionType

  pure def applyAllocatorRecord(
    basis: FreeSpaceBasis,
    record: WalRecord,
  ): FreeSpaceBasis =
    if (basis.kind == NoBasis) {
      basis
    } else if (record.kind == WalAllocateRegion) {
      freeSpaceBasis(
        basis.kind,
        basis.sourceRegionIndex,
        basis.entries,
        record.allocationHeadAfter,
        basis.readyBoundary,
        basis.appendTail,
        basis.materializedRegions,
        basis.frontierRegionIndex,
        basis.needsChainRewrite,
      )
    } else if (record.kind == WalFreeRegion) {
      freeSpaceBasis(
        basis.kind,
        basis.sourceRegionIndex,
        basis.entries.append(record.regionIndex),
        basis.allocationHead,
        basis.readyBoundary,
        record.appendTailAfter,
        basis.materializedRegions,
        basis.frontierRegionIndex,
        basis.needsChainRewrite,
      )
    } else if (record.kind == WalEraseFreeRegionSpan) {
      freeSpaceBasis(
        basis.kind,
        basis.sourceRegionIndex,
        basis.entries,
        basis.allocationHead,
        record.readyBoundaryAfter,
        basis.appendTail,
        basis.materializedRegions,
        basis.frontierRegionIndex,
        basis.needsChainRewrite,
      )
    } else {
      basis
    }

  pure def physicalFreeSpaceRegionValid(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    regionIndex: int,
  ): bool =
    isFreeSpaceMetadataRegion(regionLog, regionIndex)
    and payloadIndexInRange(
      regionLog.nth(regionIndex).typeIndex,
      freePayloads.length(),
    )
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologueValid

  pure def markNeedsChainRewrite(basis: FreeSpaceBasis): FreeSpaceBasis =
    freeSpaceBasis(
      basis.kind,
      basis.sourceRegionIndex,
      basis.entries,
      basis.allocationHead,
      basis.readyBoundary,
      basis.appendTail,
      basis.materializedRegions,
      basis.frontierRegionIndex,
      true,
    )

  pure def applyMaterializeFreeSpaceRegionRecord(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    basis: FreeSpaceBasis,
    record: WalRecord,
  ): FreeSpaceBasis =
    if (basis.kind == NoBasis) {
      basis
    } else if (
      record.regionIndex == basis.frontierRegionIndex
      and physicalFreeSpaceRegionValid(
        regionLog,
        freePayloads,
        record.regionIndex,
      )
      and record.nextFrontierRegionIndex.in(RegionIds)
    ) {
      freeSpaceBasis(
        basis.kind,
        basis.sourceRegionIndex,
        basis.entries,
        basis.allocationHead,
        basis.readyBoundary,
        basis.appendTail,
        basis.materializedRegions.append(record.regionIndex),
        record.nextFrontierRegionIndex,
        basis.needsChainRewrite,
      )
    } else {
      markNeedsChainRewrite(basis)
    }

  pure def applyWalRecord(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    basis: FreeSpaceBasis,
    record: WalRecord,
  ): FreeSpaceBasis =
    if (
      record.kind == WalPreallocateFreeSpaceRegion
      and isFreeSpaceBasisRecord(record)
    ) {
      basisFromPreallocatedFreeSpaceRegion(record)
    } else if (record.kind == WalHead and isFreeSpaceBasisRecord(record)) {
      basisFromFreeSpaceRegion(regionLog, freePayloads, record.regionIndex)
    } else if (record.kind == WalSnapshot and isFreeSpaceBasisRecord(record)) {
      basisFromSnapshot(record.snapshotBasis)
    } else if (
      record.kind == WalAllocateRegion
      or record.kind == WalFreeRegion
      or record.kind == WalEraseFreeRegionSpan
    ) {
      applyAllocatorRecord(basis, record)
    } else if (record.kind == WalMaterializeFreeSpaceRegion) {
      applyMaterializeFreeSpaceRegionRecord(
        regionLog,
        freePayloads,
        basis,
        record,
      )
    } else {
      basis
    }

  pure def recoveredFreeSpaceBasisFrom(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    walLog: List[WalRecord],
  ): FreeSpaceBasis =
    walLog.foldl(NoFreeSpaceBasis, (basis, record) =>
      applyWalRecord(regionLog, freePayloads, basis, record))

  val recoveredFreeSpaceBasis =
    recoveredFreeSpaceBasisFrom(regions, freeSpaceRegions, walRecords)
}
