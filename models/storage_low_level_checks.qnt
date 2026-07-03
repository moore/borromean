// Assertion and invariant harness for the low-level storage model.
//
// Keep executable storage structure and actions in storage_low_level.qnt. This
// file owns audit predicates and run/typecheck targets that should not obscure
// the physical model.

module storage_low_level_checks {
  import storage_low_level.* from "./storage_low_level"

  pure def metadataValid(meta: StorageMetadata): bool =
    meta.storageVersion == StorageVersion
    and meta.regionCount == ModelRegionCount
    and meta.regionSize > 0
    and meta.minFreeRegions >= 0
    and meta.transactionLogCount >= 1
    and meta.transactionLogCount < meta.regionCount
    and meta.walWriteGranule > 0
    and meta.erasedByte == ErasedByte
    and meta.walRecordMagic == WalRecordMagic
    and not(meta.walRecordMagic == meta.erasedByte)

  pure def headerInRange(header: RegionHeader): bool =
    header.sequence >= 0
    and header.collectionId >= 0
    and header.collectionFormat >= 0

  pure def regionHeaderOK(region: Region): bool =
    not(region.headerValid) or headerInRange(region.header)

  pure def freeQueuePositionInRange(position: FreeQueuePosition): bool =
    position.regionIndex.in(RegionIds)
    and position.entryIndex >= 0

  pure def freeQueuePositionOrdinal(position: FreeQueuePosition): int =
    if (position.regionIndex == FreeSpaceRegionId) {
      position.entryIndex
    } else if (position.regionIndex == FreeSpaceFrontierRegionId) {
      InitialFreeLog.length() + position.entryIndex
    } else {
      ModelRegionCount + position.entryIndex
    }

  pure def freeQueuePositionAtOrBefore(
    left: FreeQueuePosition,
    right: FreeQueuePosition,
  ): bool =
    freeQueuePositionOrdinal(left) <= freeQueuePositionOrdinal(right)

  pure def freeQueueCursorsOrdered(
    allocationHead: FreeQueuePosition,
    readyBoundary: FreeQueuePosition,
    appendTail: FreeQueuePosition,
  ): bool =
    freeQueuePositionAtOrBefore(allocationHead, readyBoundary)
    and freeQueuePositionAtOrBefore(readyBoundary, appendTail)

  pure def walRecordLogPrologueInRange(
    prologue: WalPrologue,
  ): bool =
    prologue.logHeadRegionIndex.in(RegionIds)

  pure def freeSpacePrologueInRange(
    prologue: FreeSpaceRegionPrologue,
  ): bool =
    freeQueuePositionInRange(prologue.allocationHead)
    and freeQueuePositionInRange(prologue.readyBoundary)
    and freeQueuePositionInRange(prologue.appendTail)
    and freeQueueCursorsOrdered(
      prologue.allocationHead,
      prologue.readyBoundary,
      prologue.appendTail,
    )
    and (prologue.nextMetadataRegion == NoRegion
      or prologue.nextMetadataRegion.in(RegionIds))
    and prologue.entryCount >= 0

  pure def intListToSet(regionLog: List[int]): Set[int] =
    regionLog.foldl(Set(), (acc, r) => acc.union(Set(r)))

  pure def intListNoDuplicates(regionLog: List[int]): bool =
    regionLog.indices().forall(i =>
      regionLog.indices().forall(j =>
        i == j or not(regionLog.nth(i) == regionLog.nth(j))))

  pure def freeEntriesInRange(regionLog: List[int]): bool =
    regionLog.indices().forall(i => regionLog.nth(i).in(RegionIds))

  pure def regionPayloadIndexOK(
    region: Region,
    walPayloadCount: int,
    freePayloadCount: int,
    collectionPayloadCount: int,
  ): bool =
    if (region.regionType == InvalidRegion) {
      region.typeIndex == NoRegion
    } else if (region.regionType == ErasedRegion) {
      not(region.headerValid) and region.typeIndex == NoRegion
    } else if (region.regionType == WalLogRegion) {
      payloadIndexInRange(region.typeIndex, walPayloadCount)
    } else if (region.regionType == FreeSpaceRegion) {
      payloadIndexInRange(region.typeIndex, freePayloadCount)
    } else {
      region.regionType == CollectionRegion
      and payloadIndexInRange(region.typeIndex, collectionPayloadCount)
    }

  pure def payloadRefsUniqueForType(
    regionLog: List[Region],
    regionType: RegionType,
  ): bool =
    regionLog.indices().forall(i =>
      regionLog.indices().forall(j =>
        i == j
        or regionLog.nth(i).regionType != regionType
        or regionLog.nth(j).regionType != regionType
        or regionLog.nth(i).typeIndex != regionLog.nth(j).typeIndex))

  pure def payloadRefsUnique(regionLog: List[Region]): bool =
    payloadRefsUniqueForType(regionLog, WalLogRegion)
    and payloadRefsUniqueForType(regionLog, FreeSpaceRegion)
    and payloadRefsUniqueForType(regionLog, CollectionRegion)

  pure def walLogPayloadOK(payload: WalLogRegionPayload): bool =
    not(payload.prologueValid)
    or walRecordLogPrologueInRange(payload.prologue)

  pure def freeSpacePayloadOK(payload: FreeSpaceRegionPayload): bool =
    not(payload.prologueValid)
    or (
      freeSpacePrologueInRange(payload.prologue)
      and payload.prologue.entryCount == payload.freeEntries.length()
      and freeEntriesInRange(payload.freeEntries)
      and intListNoDuplicates(payload.freeEntries)
    )

  pure def freeSpaceBasisOK(basis: FreeSpaceBasis): bool =
    if (basis.kind == NoBasis) {
      basis.sourceRegionIndex == NoRegion
      and basis.entries.length() == 0
      and basis.materializedRegions.length() == 0
      and basis.frontierRegionIndex == NoRegion
      and not(basis.needsChainRewrite)
    } else if (basis.kind == WalResidentBasis) {
      basis.sourceRegionIndex == NoRegion
      and basis.materializedRegions.length() == 0
      and basis.frontierRegionIndex.in(RegionIds)
      and freeEntriesInRange(basis.entries)
      and intListNoDuplicates(basis.entries)
      and freeQueuePositionInRange(basis.allocationHead)
      and freeQueuePositionInRange(basis.readyBoundary)
      and freeQueuePositionInRange(basis.appendTail)
      and freeQueueCursorsOrdered(
        basis.allocationHead,
        basis.readyBoundary,
        basis.appendTail,
      )
    } else {
      (basis.kind == MaterializedRegionBasis
        or basis.kind == SnapshotBasis)
      and (basis.sourceRegionIndex == NoRegion
        or basis.sourceRegionIndex.in(RegionIds))
      and basis.frontierRegionIndex.in(RegionIds)
      and basis.materializedRegions.length() > 0
      and basis.materializedRegions.indices().forall(i =>
        basis.materializedRegions.nth(i).in(RegionIds))
      and intListNoDuplicates(basis.materializedRegions)
      and freeEntriesInRange(basis.entries)
      and intListNoDuplicates(basis.entries)
      and freeQueuePositionInRange(basis.allocationHead)
      and freeQueuePositionInRange(basis.readyBoundary)
      and freeQueuePositionInRange(basis.appendTail)
      and freeQueueCursorsOrdered(
        basis.allocationHead,
        basis.readyBoundary,
        basis.appendTail,
      )
    }

  pure def walRecordOK(record: WalRecord): bool =
    record.collectionId >= 0
    and record.collectionType >= 0
    and (record.regionIndex == NoRegion
      or record.regionIndex.in(RegionIds))
    and freeSpaceBasisOK(record.snapshotBasis)
    and freeQueuePositionInRange(record.allocationHeadAfter)
    and freeQueuePositionInRange(record.appendTailAfter)
    and record.eraseCount >= 0
    and freeQueuePositionInRange(record.readyBoundaryAfter)
    and (record.nextFrontierRegionIndex == NoRegion
      or record.nextFrontierRegionIndex.in(RegionIds))

  pure def initialWalRecordLogRegionOK(
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    regionIndex: int,
    expectedSequence: int,
    expectedFormat: int,
    expectedHeadRegion: int,
  ): bool =
    regionIndexInRange(regionLog, regionIndex)
    and regionLog.nth(regionIndex).headerValid
    and regionLog.nth(regionIndex).header.sequence == expectedSequence
    and regionLog.nth(regionIndex).header.collectionId == InternalCollectionId
    and regionLog.nth(regionIndex).header.collectionFormat == expectedFormat
    and regionLog.nth(regionIndex).regionType == WalLogRegion
    and payloadIndexInRange(
      regionLog.nth(regionIndex).typeIndex,
      walPayloads.length(),
    )
    and walPayload(
      regionLog,
      walPayloads,
      regionIndex,
    ).prologueValid
    and walPayload(
      regionLog,
      walPayloads,
      regionIndex,
    ).prologue.logHeadRegionIndex == expectedHeadRegion

  pure def initialFreeSpaceRegionOK(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    regionIndex: int,
  ): bool =
    isFreeSpaceMetadataRegion(regionLog, regionIndex)
    and regionLog.nth(regionIndex).header.sequence == 0
    and payloadIndexInRange(
      regionLog.nth(regionIndex).typeIndex,
      freePayloads.length(),
    )
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologueValid
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologue.allocationHead == InitialFreeHead
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologue.readyBoundary == InitialFreeTail
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologue.appendTail == InitialFreeTail
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologue.nextMetadataRegion == FreeSpaceFrontierRegionId
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).prologue.entryCount == InitialFreeLog.length()
    and freeSpacePayload(
      regionLog,
      freePayloads,
      regionIndex,
    ).freeEntries == InitialFreeLog
    and intListToSet(
      freeSpacePayload(
        regionLog,
        freePayloads,
        regionIndex,
      ).freeEntries,
    ) == RegionIds.exclude(InitialUsedRegionIds)

  pure def initialCollectionRegionOK(
    region: Region,
    expectedSequence: int,
    expectedPayloadIndex: int,
  ): bool =
    region.headerValid
    and region.header.sequence == expectedSequence
    and region.header.collectionId == ModelCollectionId
    and region.header.collectionFormat == MapRegionV2Format
    and region.regionType == CollectionRegion
    and region.typeIndex == expectedPayloadIndex

  pure def initialErasedRegionOK(region: Region): bool =
    not(region.headerValid)
    and region.regionType == ErasedRegion
    and region.typeIndex == NoRegion

  pure def initializedLayoutOK(
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    freePayloads: List[FreeSpaceRegionPayload],
  ): bool =
    regionLog.length() == ModelRegionCount
    and initialWalRecordLogRegionOK(
      regionLog,
      walPayloads,
      WalRegionId,
      1,
      MainWalV2Format,
      WalRegionId,
    )
    and initialFreeSpaceRegionOK(
      regionLog,
      freePayloads,
      FreeSpaceRegionId,
    )
    and initialWalRecordLogRegionOK(
      regionLog,
      walPayloads,
      TransactionLogRegionId,
      2,
      TransactionLogV2Format,
      TransactionLogRegionId,
    )
    and initialCollectionRegionOK(
      regionLog.nth(InitialCollectionRegionAId),
      3,
      InitialCollectionPayloadAIndex,
    )
    and initialCollectionRegionOK(
      regionLog.nth(InitialCollectionRegionBId),
      4,
      InitialCollectionPayloadBIndex,
    )
    and initialErasedRegionOK(regionLog.nth(FreeSpaceFrontierRegionId))
    and RegionIds
      .exclude(InitialUsedRegionIds)
      .forall(i => initialErasedRegionOK(regionLog.nth(i)))

  pure def initialWalRecordsSelectFreeSpaceBasisOK(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    walLog: List[WalRecord],
  ): bool =
    walLog == expectedInitialWalRecords
    and recoveredFreeSpaceBasisFrom(regionLog, freePayloads, walLog)
      == freeSpaceBasis(
        WalResidentBasis,
        NoRegion,
        FormattedFreeLog,
        FormattedFreeHead,
        FormattedFreeTail,
        FormattedFreeTail,
        [],
        FreeSpaceRegionId,
        false,
      )

  pure def selectedFreeSpaceBasisOK(
    regionLog: List[Region],
    freePayloads: List[FreeSpaceRegionPayload],
    walLog: List[WalRecord],
  ): bool =
    freeSpaceBasisOK(recoveredFreeSpaceBasisFrom(
      regionLog,
      freePayloads,
      walLog,
    ))

  pure def formattedBootstrapPhysicalLayoutOK(
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    freePayloads: List[FreeSpaceRegionPayload],
    collectionPayloads: List[CollectionRegionPayload],
  ): bool =
    regionLog.length() == ModelRegionCount
    and walPayloads == FormatWalLogRegionPayloads
    and freePayloads.length() == 0
    and collectionPayloads.length() == 0
    and initialWalRecordLogRegionOK(
      regionLog,
      walPayloads,
      WalRegionId,
      1,
      MainWalV2Format,
      WalRegionId,
    )
    and RegionIds
      .exclude(Set(WalRegionId))
      .forall(i => initialErasedRegionOK(regionLog.nth(i)))

  pure def expectedFormatWalRecordsBeforeReady(
    nextRegion: int,
  ): List[WalRecord] =
    FormattedFreeLog.foldl(
      [walPreallocateFreeSpaceRegionRecord(FreeSpaceRegionId)],
      (records, regionIndex) =>
        if (regionIndex < nextRegion) {
          records.append(
            walFreeRegionRecord(
              regionIndex,
              freeQueuePosition(FreeSpaceRegionId, regionIndex - 1)
            ))
        } else {
          records
        }
    )

  pure val expectedInitialWalRecords =
    expectedFormatWalRecordsBeforeReady(ModelRegionCount).append(
      walEraseFreeRegionSpanRecord(
        FormattedFreeLog.length(),
        FormattedFreeTail
      ))

  pure def emptyFormatPayloadsOK(
    walPayloads: List[WalLogRegionPayload],
    freePayloads: List[FreeSpaceRegionPayload],
    collectionPayloads: List[CollectionRegionPayload],
    walLog: List[WalRecord],
  ): bool =
    walPayloads.length() == 0
    and freePayloads.length() == 0
    and collectionPayloads.length() == 0
    and walLog.length() == 0

  pure def formattedLayoutOK(
    meta: StorageMetadata,
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    freePayloads: List[FreeSpaceRegionPayload],
    collectionPayloads: List[CollectionRegionPayload],
    walLog: List[WalRecord],
  ): bool =
    metadataValid(meta)
    and formattedBootstrapPhysicalLayoutOK(
      regionLog,
      walPayloads,
      freePayloads,
      collectionPayloads,
    )
    and initialWalRecordsSelectFreeSpaceBasisOK(
      regionLog,
      freePayloads,
      walLog,
    )

  pure def formatPhaseLayoutOK(
    phase: FormatPhase,
    meta: StorageMetadata,
    regionLog: List[Region],
    walPayloads: List[WalLogRegionPayload],
    freePayloads: List[FreeSpaceRegionPayload],
    collectionPayloads: List[CollectionRegionPayload],
    walLog: List[WalRecord],
    nextRegion: int,
  ): bool =
    if (phase == FormatBlank) {
      meta == BlankMetadata
      and regionLog == BlankRegions
      and nextRegion == 0
      and emptyFormatPayloadsOK(
        walPayloads,
        freePayloads,
        collectionPayloads,
        walLog,
      )
    } else if (phase == FormatMetadataWritten) {
      meta == InitialMetadata
      and regionLog == BlankRegions
      and nextRegion == 0
      and emptyFormatPayloadsOK(
        walPayloads,
        freePayloads,
        collectionPayloads,
        walLog,
      )
    } else if (phase == FormatWalRegionWritten) {
      meta == InitialMetadata
      and regionLog == RegionsAfterMainWalRegion
      and walPayloads == FormatWalLogRegionPayloads
      and freePayloads.length() == 0
      and collectionPayloads.length() == 0
      and walLog.length() == 0
      and nextRegion == FreeSpaceRegionId
    } else if (phase == FormatFreeSpacePreallocated) {
      meta == InitialMetadata
      and regionLog == RegionsAfterMainWalRegion
      and walPayloads == FormatWalLogRegionPayloads
      and freePayloads.length() == 0
      and collectionPayloads.length() == 0
      and walLog == expectedFormatWalRecordsBeforeReady(nextRegion)
      and nextRegion == 2
    } else if (phase == FormatFreeRegionsLogged) {
      meta == InitialMetadata
      and regionLog == RegionsAfterMainWalRegion
      and walPayloads == FormatWalLogRegionPayloads
      and freePayloads.length() == 0
      and collectionPayloads.length() == 0
      and nextRegion >= 3
      and nextRegion <= ModelRegionCount
      and walLog == expectedFormatWalRecordsBeforeReady(nextRegion)
    } else {
      phase == FormatDone
      and nextRegion == ModelRegionCount
      and formattedLayoutOK(
        meta,
        regionLog,
        walPayloads,
        freePayloads,
        collectionPayloads,
        walLog,
      )
    }

  pure val PositionAfterFirstAllocate = {
    regionIndex: FreeSpaceRegionId,
    entryIndex: 1,
  }

  pure val PositionAfterFirstAppend = {
    regionIndex: FreeSpaceFrontierRegionId,
    entryIndex: 1,
  }

  pure val InitialAllocatedRegion = 6

  pure val InitialFreeLogAfterAppend = [
    6,
    7,
    8,
    InitialCollectionRegionAId,
  ]

  pure val ExpectedBasisAfterAllocate =
    freeSpaceBasis(
      MaterializedRegionBasis,
      FreeSpaceRegionId,
      InitialFreeLog,
      PositionAfterFirstAllocate,
      InitialFreeTail,
      InitialFreeTail,
      [FreeSpaceRegionId],
      FreeSpaceFrontierRegionId,
      false,
    )

  pure val ExpectedBasisAfterFree =
    freeSpaceBasis(
      MaterializedRegionBasis,
      FreeSpaceRegionId,
      InitialFreeLogAfterAppend,
      InitialFreeHead,
      InitialFreeTail,
      PositionAfterFirstAppend,
      [FreeSpaceRegionId],
      FreeSpaceFrontierRegionId,
      false,
    )

  pure val ExpectedBasisAfterFreeAndErase =
    freeSpaceBasis(
      MaterializedRegionBasis,
      FreeSpaceRegionId,
      InitialFreeLogAfterAppend,
      InitialFreeHead,
      PositionAfterFirstAppend,
      PositionAfterFirstAppend,
      [FreeSpaceRegionId],
      FreeSpaceFrontierRegionId,
      false,
    )

  pure val MaterializedFrontierRegion = {
    headerValid: true,
    header: internalHeader(1, FreeSpaceV2Format),
    regionType: FreeSpaceRegion,
    typeIndex: 1,
  }

  pure val MaterializedFrontierPayload = {
    prologueValid: true,
    prologue: {
      allocationHead: PositionAfterFirstAllocate,
      readyBoundary: InitialFreeTail,
      appendTail: PositionAfterFirstAppend,
      nextMetadataRegion: InitialAllocatedRegion,
      entryCount: 1,
    },
    freeEntries: [InitialCollectionRegionAId],
  }

  pure val FreeSpacePayloadsWithMaterializedFrontier = [
    {
      prologueValid: true,
      prologue: InitialFreeSpacePrologue,
      freeEntries: InitialFreeLog,
    },
    MaterializedFrontierPayload,
  ]

  pure val RegionsWithMaterializedFrontier = [
    InitialWalRegion,
    InitialFreeSpaceRegion,
    InitialTransactionLogRegion,
    collectionRegion(3, InitialCollectionPayloadAIndex),
    collectionRegion(4, InitialCollectionPayloadBIndex),
    MaterializedFrontierRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val InvalidMaterializedFrontierRegion =
    invalidRegion(true, internalHeader(1, FreeSpaceV2Format))

  pure val RegionsWithInvalidFrontier = [
    InitialWalRegion,
    InitialFreeSpaceRegion,
    InitialTransactionLogRegion,
    collectionRegion(3, InitialCollectionPayloadAIndex),
    collectionRegion(4, InitialCollectionPayloadBIndex),
    InvalidMaterializedFrontierRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
    ErasedPhysicalRegion,
  ]

  pure val ExpectedBasisAfterValidMaterialize =
    freeSpaceBasis(
      MaterializedRegionBasis,
      FreeSpaceRegionId,
      InitialFreeLogAfterAppend,
      PositionAfterFirstAllocate,
      InitialFreeTail,
      PositionAfterFirstAppend,
      [FreeSpaceRegionId, FreeSpaceFrontierRegionId],
      InitialAllocatedRegion,
      false,
    )

  pure val ExpectedBasisAfterBadMaterialize =
    freeSpaceBasis(
      MaterializedRegionBasis,
      FreeSpaceRegionId,
      InitialFreeLogAfterAppend,
      PositionAfterFirstAllocate,
      InitialFreeTail,
      PositionAfterFirstAppend,
      [FreeSpaceRegionId],
      FreeSpaceFrontierRegionId,
      true,
    )

  pure val WalAfterAllocate = [
    walHeadRecord(
      InternalCollectionId,
      FreeSpaceCollectionType,
      FreeSpaceRegionId,
    ),
    walAllocateRegionRecord(
      InitialAllocatedRegion,
      PositionAfterFirstAllocate,
    ),
  ]

  pure val WalAfterFree = [
    walHeadRecord(
      InternalCollectionId,
      FreeSpaceCollectionType,
      FreeSpaceRegionId,
    ),
    walFreeRegionRecord(
      InitialCollectionRegionAId,
      PositionAfterFirstAppend,
    ),
  ]

  pure val WalAfterFreeAndErase = [
    walHeadRecord(
      InternalCollectionId,
      FreeSpaceCollectionType,
      FreeSpaceRegionId,
    ),
    walFreeRegionRecord(
      InitialCollectionRegionAId,
      PositionAfterFirstAppend,
    ),
    walEraseFreeRegionSpanRecord(
      1,
      PositionAfterFirstAppend,
    ),
  ]

  pure val WalAfterAllocateFreeAndMaterialize = [
    walHeadRecord(
      InternalCollectionId,
      FreeSpaceCollectionType,
      FreeSpaceRegionId,
    ),
    walAllocateRegionRecord(
      InitialAllocatedRegion,
      PositionAfterFirstAllocate,
    ),
    walFreeRegionRecord(
      InitialCollectionRegionAId,
      PositionAfterFirstAppend,
    ),
    walMaterializeFreeSpaceRegionRecord(
      FreeSpaceFrontierRegionId,
      InitialAllocatedRegion,
    ),
  ]

  val replayHelpersOK =
    recoveredFreeSpaceBasisFrom(
      InitialRegions,
      InitialFreeSpaceRegionPayloads,
      WalAfterAllocate,
    ) == ExpectedBasisAfterAllocate
    and recoveredFreeSpaceBasisFrom(
      InitialRegions,
      InitialFreeSpaceRegionPayloads,
      WalAfterFree,
    ) == ExpectedBasisAfterFree
    and recoveredFreeSpaceBasisFrom(
      InitialRegions,
      InitialFreeSpaceRegionPayloads,
      WalAfterFreeAndErase,
    ) == ExpectedBasisAfterFreeAndErase
    and recoveredFreeSpaceBasisFrom(
      RegionsWithMaterializedFrontier,
      FreeSpacePayloadsWithMaterializedFrontier,
      WalAfterAllocateFreeAndMaterialize,
    ) == ExpectedBasisAfterValidMaterialize
    and recoveredFreeSpaceBasisFrom(
      RegionsWithInvalidFrontier,
      InitialFreeSpaceRegionPayloads,
      WalAfterAllocateFreeAndMaterialize,
    ) == ExpectedBasisAfterBadMaterialize

  val metadataOK =
    if (formatPhase == FormatBlank) {
      metadata == BlankMetadata
    } else {
      metadataValid(metadata)
    }

  val regionsOK =
    regions.length() == ModelRegionCount
    and regions.indices().forall(i =>
      regionHeaderOK(regions.nth(i))
      and regionPayloadIndexOK(
        regions.nth(i),
        walLogRegions.length(),
        freeSpaceRegions.length(),
        collectionRegions.length(),
      ))
    and payloadRefsUnique(regions)

  val walLogRegionPayloadsOK =
    walLogRegions.indices().forall(i =>
      walLogPayloadOK(walLogRegions.nth(i)))

  val freeSpaceRegionPayloadsOK =
    freeSpaceRegions.indices().forall(i =>
      freeSpacePayloadOK(freeSpaceRegions.nth(i)))

  val walRecordsOK =
    walRecords.indices().forall(i => walRecordOK(walRecords.nth(i)))

  val selectedBasisOK =
    selectedFreeSpaceBasisOK(regions, freeSpaceRegions, walRecords)

  val initialLayoutOK =
    formatPhase == FormatDone
    and formattedLayoutOK(
      metadata,
      regions,
      walLogRegions,
      freeSpaceRegions,
      collectionRegions,
      walRecords,
    )

  val formatDoneLayoutOK =
    formatPhase != FormatDone or initialLayoutOK

  val formatStateOK =
    typeOK
    and formatPhaseLayoutOK(
      formatPhase,
      metadata,
      regions,
      walLogRegions,
      freeSpaceRegions,
      collectionRegions,
      walRecords,
      formatNextRegion,
    )

  val typeOK =
    metadataOK
    and formatNextRegion >= 0
    and formatNextRegion <= ModelRegionCount
    and regionsOK
    and walLogRegionPayloadsOK
    and freeSpaceRegionPayloadsOK
    and walRecordsOK
    and selectedBasisOK
}
