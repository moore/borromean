// Archived core-pilot model. It is not current design authority.
// Shared bounded mechanical storage model.
//
// This is a v3-corrected port of the state vocabulary and useful transition
// structure in models/transaction_free_recovery.qnt. It is mechanical with
// respect to ownership durability: issued media work, sync, runtime apply,
// crash outcomes, and replay are distinct steps. Collection payload semantics
// remain abstract until their own refinement models are added.

module storage_mechanical {
  pure val NoneId = -1
  pure val RegionIndexes = [0, 1, 2, 3, 4, 5]
  pure val Regions = Set(0, 1, 2, 3, 4, 5)
  pure val TransactionIndexes = [0, 1]
  pure val Transactions = Set(0, 1)
  pure val CollectionIndexes = [0, 1]
  pure val Collections = Set(0, 1)
  pure val PermanentRegions = Set(0)
  // Quint 0.32 does not pass --max-steps to TLC. The executable wrapper uses
  // model fuel so exhaustive TLC exploration reaches a finite fixpoint.
  pure val MechanicalVerificationBound = 6

  type RegionPhase =
    | MErasedPrepared
    | MReserved
    | MPublished
    | MDirty

  type PurposeKind =
    | MNoPurpose
    | MMainWal
    | MTransactionLog
    | MFreeBasis
    | MBootstrap
    | MCollectionData

  type OwnerKind =
    | MNoOwner
    | MMainWalOwner
    | MTransactionOwner
    | MFreeBasisOwner
    | MBootstrapOwner
    | MCollectionOwner

  type EventKind =
    | MReserveFact
    | MPublishFact
    | MReleaseFact
    | MReadinessFact

  type CommandKind =
    | MNoCommand
    | MReserveCommand
    | MPublishCommand
    | MReleaseCommand
    | MReadinessCommand
    | MCommitCommand
    | MRollbackCommand
    | MCleanupCommand
    | MFinishCommand

  type OperationPhase =
    | MQuiescent
    | MEventsIssued
    | MEventsDurable
    | MContentIssued
    | MEraseIssued
    | MEraseComplete

  type MechanicalError =
    | MOk
    | MRegionOutOfRange
    | MInvalidSourceState
    | MReservationMismatch
    | MReservationTokenMissing
    | MPurposeOwnerMismatch
    | MPublishedOwnerMismatch
    | MPermanentRegion
    | MAllocationOrderInvalid
    | MCollectionWriteLocked
    | MFinishPressure
    | MContentNotDurable
    | MEraseNotComplete
    | MTransactionStateInvalid
    | MPurposeInvalid

  type Region = {
    phase: RegionPhase,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
  }

  type Token = {
    region: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
  }

  type MechanicalEvent = {
    kind: EventKind,
    region: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
    transaction: int,
    allocationSequence: int,
    allocationHeadAfter: int,
  }

  type DurableCommand = {
    kind: CommandKind,
    transaction: int,
    events: List[MechanicalEvent],
  }

  type CollectionState = {
    live: Set[int],
    generation: int,
    collectionType: int,
  }

  type TxPhase =
    | MTxIdle
    | MTxOpen
    | MTxCommitCleanup
    | MTxRollbackCleanup

  type TxState = {
    phase: TxPhase,
    enrolled: Set[int],
    observedGeneration0: int,
    observedGeneration1: int,
    allocatedRegions: List[int],
    freeIntents: List[int],
    bufferedFreeIntents: List[int],
    cleanupIndex: int,
    beginOrder: int,
  }

  type RuntimeState = {
    regions: List[Region],
    tokens: Set[Token],
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    allocationSequence: int,
    finishOwner: int,
    nextBeginOrder: int,
    collections: List[CollectionState],
    transactions: List[TxState],
  }

  type RuntimeResult = {
    runtime: RuntimeState,
    error: MechanicalError,
  }

  type MechanicalState = {
    runtime: RuntimeState,
    durableCommands: List[DurableCommand],
    pendingCommand: DurableCommand,
    phase: OperationPhase,
    durableContent: Set[Token],
    physicallyErased: Set[int],
    // Per-lifecycle erase authority. Unlike eraseEvidence, this is consumed by
    // the next reservation and cannot justify readiness in a later lifecycle.
    freshEraseEvidence: Set[int],
    eraseEvidence: Set[int],
    activeToken: Token,
    activeRegion: int,
    tornSpans: int,
    lastError: MechanicalError,
    lastAttemptedEvent: MechanicalEvent,
  }

  ////// Constructors and bounded state helpers //////

  pure def region(
    phase: RegionPhase,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
  ): Region = {
    phase: phase,
    purposeKind: purposeKind,
    purposeIdentity: purposeIdentity,
    operation: operation,
    ownerKind: ownerKind,
    ownerIdentity: ownerIdentity,
    collectionType: collectionType,
  }

  pure val EmptyRegion =
    region(MErasedPrepared, MNoPurpose, NoneId, NoneId, MNoOwner, NoneId, NoneId)

  pure def token(
    regionIndex: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
  ): Token = {
    region: regionIndex,
    purposeKind: purposeKind,
    purposeIdentity: purposeIdentity,
    operation: operation,
  }

  pure val EmptyToken = token(NoneId, MNoPurpose, NoneId, NoneId)

  pure def event(
    kind: EventKind,
    regionIndex: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
    transaction: int,
    allocationSequence: int,
    allocationHeadAfter: int,
  ): MechanicalEvent = {
    kind: kind,
    region: regionIndex,
    purposeKind: purposeKind,
    purposeIdentity: purposeIdentity,
    operation: operation,
    ownerKind: ownerKind,
    ownerIdentity: ownerIdentity,
    collectionType: collectionType,
    transaction: transaction,
    allocationSequence: allocationSequence,
    allocationHeadAfter: allocationHeadAfter,
  }

  pure val EmptyEvent =
    event(
      MReadinessFact,
      NoneId,
      MNoPurpose,
      NoneId,
      NoneId,
      MNoOwner,
      NoneId,
      NoneId,
      NoneId,
      0,
      0,
    )

  pure def reserveEvent(
    runtime: RuntimeState,
    regionIndex: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
    transaction: int,
  ): MechanicalEvent =
    event(
      MReserveFact,
      regionIndex,
      purposeKind,
      purposeIdentity,
      operation,
      MNoOwner,
      NoneId,
      NoneId,
      transaction,
      runtime.allocationSequence + 1,
      runtime.allocationHead + 1,
    )

  pure def publishEvent(
    reservationToken: Token,
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
  ): MechanicalEvent =
    event(
      MPublishFact,
      reservationToken.region,
      reservationToken.purposeKind,
      reservationToken.purposeIdentity,
      reservationToken.operation,
      ownerKind,
      ownerIdentity,
      collectionType,
      if (ownerKind == MTransactionOwner) { ownerIdentity } else { NoneId },
      0,
      0,
    )

  pure def releaseEvent(current: Region, regionIndex: int): MechanicalEvent =
    event(
      MReleaseFact,
      regionIndex,
      current.purposeKind,
      current.purposeIdentity,
      current.operation,
      current.ownerKind,
      current.ownerIdentity,
      current.collectionType,
      if (current.ownerKind == MTransactionOwner) {
        current.ownerIdentity
      } else {
        NoneId
      },
      0,
      0,
    )

  pure def readinessEvent(regionIndex: int): MechanicalEvent =
    event(
      MReadinessFact,
      regionIndex,
      MNoPurpose,
      NoneId,
      NoneId,
      MNoOwner,
      NoneId,
      NoneId,
      NoneId,
      0,
      0,
    )

  pure def command(
    kind: CommandKind,
    transaction: int,
    events: List[MechanicalEvent],
  ): DurableCommand = {
    kind: kind,
    transaction: transaction,
    events: events,
  }

  pure val EmptyCommand = command(MNoCommand, NoneId, [])

  pure def collection(
    live: Set[int],
    generation: int,
    collectionType: int,
  ): CollectionState = {
    live: live,
    generation: generation,
    collectionType: collectionType,
  }

  pure val EmptyTx = {
    phase: MTxIdle,
    enrolled: Set(),
    observedGeneration0: 0,
    observedGeneration1: 0,
    allocatedRegions: [],
    freeIntents: [],
    bufferedFreeIntents: [],
    cleanupIndex: 0,
    beginOrder: NoneId,
  }

  pure def openTx(runtime: RuntimeState): TxState = {
    phase: MTxOpen,
    enrolled: Set(),
    observedGeneration0: runtime.collections.nth(0).generation,
    observedGeneration1: runtime.collections.nth(1).generation,
    allocatedRegions: [],
    freeIntents: [],
    bufferedFreeIntents: [],
    cleanupIndex: 0,
    beginOrder: runtime.nextBeginOrder,
  }

  pure def replaceRegion(
    regions: List[Region],
    regionIndex: int,
    nextRegion: Region,
  ): List[Region] =
    RegionIndexes.foldl([], (updated, i) =>
      updated.append(if (i == regionIndex) { nextRegion } else { regions.nth(i) }))

  pure def replaceCollection(
    collections: List[CollectionState],
    collectionIndex: int,
    nextCollection: CollectionState,
  ): List[CollectionState] =
    CollectionIndexes.foldl([], (updated, i) =>
      updated.append(
        if (i == collectionIndex) { nextCollection } else { collections.nth(i) }))

  pure def replaceTransaction(
    transactions: List[TxState],
    transaction: int,
    nextTransaction: TxState,
  ): List[TxState] =
    TransactionIndexes.foldl([], (updated, i) =>
      updated.append(
        if (i == transaction) { nextTransaction } else { transactions.nth(i) }))

  pure def listToSet(values: List[int]): Set[int] =
    values.foldl(Set(), (acc, value) => acc.union(Set(value)))

  pure def appendAll(base: List[int], suffix: List[int]): List[int] =
    suffix.foldl(base, (acc, value) => acc.append(value))

  pure def intAt(values: List[int], index: int): int =
    if (index >= 0 and index < values.length()) {
      values.nth(index)
    } else {
      NoneId
    }

  pure def transactionAt(runtime: RuntimeState, transaction: int): TxState =
    if (transaction.in(Transactions)) {
      runtime.transactions.nth(transaction)
    } else {
      EmptyTx
    }

  pure def txAllFreeIntents(tx: TxState): List[int] =
    appendAll(tx.freeIntents, tx.bufferedFreeIntents)

  pure def ownerCompatible(eventToCheck: MechanicalEvent): bool =
    if (eventToCheck.purposeKind == MMainWal) {
      eventToCheck.ownerKind == MMainWalOwner
    } else if (eventToCheck.purposeKind == MTransactionLog) {
      eventToCheck.ownerKind == MTransactionOwner
      and eventToCheck.ownerIdentity == eventToCheck.purposeIdentity
    } else if (eventToCheck.purposeKind == MFreeBasis) {
      eventToCheck.ownerKind == MFreeBasisOwner
    } else if (eventToCheck.purposeKind == MBootstrap) {
      eventToCheck.ownerKind == MBootstrapOwner
    } else if (eventToCheck.purposeKind == MCollectionData) {
      eventToCheck.ownerKind == MCollectionOwner
      and eventToCheck.collectionType == eventToCheck.purposeIdentity
    } else {
      false
    }

  pure def tokenOfEvent(eventToConvert: MechanicalEvent): Token =
    token(
      eventToConvert.region,
      eventToConvert.purposeKind,
      eventToConvert.purposeIdentity,
      eventToConvert.operation,
    )

  pure val InitialRegions = [
    region(MPublished, MBootstrap, 0, 0, MBootstrapOwner, 0, NoneId),
    region(MPublished, MMainWal, 0, 1, MMainWalOwner, 0, NoneId),
    region(MPublished, MCollectionData, 7, 2, MCollectionOwner, 0, 7),
    region(MPublished, MCollectionData, 8, 3, MCollectionOwner, 1, 8),
    EmptyRegion,
    EmptyRegion,
  ]

  pure val InitialRuntime: RuntimeState = {
    regions: InitialRegions,
    tokens: Set(),
    freeQueue: [4, 5],
    allocationHead: 0,
    readyBoundary: 2,
    appendTail: 2,
    allocationSequence: 0,
    finishOwner: NoneId,
    nextBeginOrder: 0,
    collections: [collection(Set(2), 0, 7), collection(Set(3), 0, 8)],
    transactions: [EmptyTx, EmptyTx],
  }

  pure val InitialMechanical: MechanicalState = {
    runtime: InitialRuntime,
    durableCommands: [],
    pendingCommand: EmptyCommand,
    phase: MQuiescent,
    durableContent: Set(),
    physicallyErased: Set(4, 5),
    freshEraseEvidence: Set(4, 5),
    eraseEvidence: Set(4, 5),
    activeToken: EmptyToken,
    activeRegion: NoneId,
    tornSpans: 0,
    lastError: MOk,
    lastAttemptedEvent: EmptyEvent,
  }

  ////// Pure durable command application and replay //////

  pure def runtimeResult(
    runtime: RuntimeState,
    error: MechanicalError,
  ): RuntimeResult = {
    runtime: runtime,
    error: error,
  }

  pure def applyValidReserve(
    runtime: RuntimeState,
    allocation: MechanicalEvent,
  ): RuntimeResult = {
    val reservationToken = tokenOfEvent(allocation)
    val reservedRegion =
      region(
        MReserved,
        allocation.purposeKind,
        allocation.purposeIdentity,
        allocation.operation,
        MNoOwner,
        NoneId,
        NoneId,
      )
    val advanced =
      runtime
        .with(
          "regions",
          replaceRegion(runtime.regions, allocation.region, reservedRegion)
        )
        .with("tokens", runtime.tokens.union(Set(reservationToken)))
        .with("allocationHead", allocation.allocationHeadAfter)
        .with("allocationSequence", allocation.allocationSequence)
    val existing =
      if (allocation.transaction.in(Transactions)) {
        advanced.transactions.nth(allocation.transaction)
      } else {
        EmptyTx
      }
    val base =
      if (existing.phase == MTxIdle) { openTx(advanced) } else { existing }
    val nextTx =
      base
        .with("phase", MTxOpen)
        .with(
          "allocatedRegions",
          base.allocatedRegions.append(allocation.region)
        )
    val advancedWithTx =
      advanced.with(
        "transactions",
        replaceTransaction(
          advanced.transactions,
          allocation.transaction,
          nextTx
        )
      )
    if (allocation.transaction.in(Transactions)) {
      runtimeResult(advancedWithTx, MOk)
    } else {
      runtimeResult(advanced, MOk)
    }
  }

  pure def applyReserve(
    runtime: RuntimeState,
    allocation: MechanicalEvent,
  ): RuntimeResult =
    if (not(allocation.region.in(Regions))) {
      runtimeResult(runtime, MRegionOutOfRange)
    } else if (allocation.region.in(PermanentRegions)) {
      runtimeResult(runtime, MPermanentRegion)
    } else if (allocation.purposeKind == MNoPurpose) {
      runtimeResult(runtime, MPurposeInvalid)
    } else if (
      runtime.allocationHead >= runtime.readyBoundary
      or intAt(runtime.freeQueue, runtime.allocationHead) != allocation.region
      or allocation.allocationSequence != runtime.allocationSequence + 1
      or allocation.allocationHeadAfter != runtime.allocationHead + 1
    ) {
      runtimeResult(runtime, MAllocationOrderInvalid)
    } else if (runtime.regions.nth(allocation.region).phase != MErasedPrepared) {
      runtimeResult(runtime, MInvalidSourceState)
    } else {
      applyValidReserve(runtime, allocation)
    }

  pure def applyCollectionPublication(
    runtime: RuntimeState,
    publication: MechanicalEvent,
  ): RuntimeResult = {
    val collectionIndex = publication.ownerIdentity
    val oldCollection = runtime.collections.nth(collectionIndex)
    val nextCollection =
      oldCollection.with(
        "live",
        oldCollection.live.union(Set(publication.region))
      )
    runtimeResult(
      runtime.with(
        "collections",
        replaceCollection(
          runtime.collections,
          collectionIndex,
          nextCollection
        )
      ),
      MOk,
    )
  }

  pure def applyMatchingPublication(
    runtime: RuntimeState,
    publication: MechanicalEvent,
  ): RuntimeResult = {
    val publicationToken = tokenOfEvent(publication)
    val published =
      region(
        MPublished,
        publication.purposeKind,
        publication.purposeIdentity,
        publication.operation,
        publication.ownerKind,
        publication.ownerIdentity,
        publication.collectionType,
      )
    if (
      publication.ownerKind == MCollectionOwner
      and not(publication.ownerIdentity.in(Collections))
    ) {
      runtimeResult(runtime, MPurposeOwnerMismatch)
    } else {
      val withOwner =
        runtime
          .with(
            "regions",
            replaceRegion(runtime.regions, publication.region, published)
          )
          .with("tokens", runtime.tokens.exclude(Set(publicationToken)))
      if (publication.ownerKind == MCollectionOwner) {
        applyCollectionPublication(withOwner, publication)
      } else {
        runtimeResult(withOwner, MOk)
      }
    }
  }

  pure def applyKnownPublication(
    runtime: RuntimeState,
    publication: MechanicalEvent,
  ): RuntimeResult = {
    val current = runtime.regions.nth(publication.region)
    if (current.phase != MReserved) {
      runtimeResult(runtime, MInvalidSourceState)
    } else if (
      current.purposeKind != publication.purposeKind
      or current.purposeIdentity != publication.purposeIdentity
      or current.operation != publication.operation
    ) {
      runtimeResult(runtime, MReservationMismatch)
    } else {
      applyMatchingPublication(runtime, publication)
    }
  }

  pure def applyPublish(
    runtime: RuntimeState,
    publication: MechanicalEvent,
  ): RuntimeResult = {
    val publicationToken = tokenOfEvent(publication)
    if (not(publication.region.in(Regions))) {
      runtimeResult(runtime, MRegionOutOfRange)
    } else {
      val current = runtime.regions.nth(publication.region)
      if (current.phase != MReserved) {
        runtimeResult(runtime, MInvalidSourceState)
      } else if (
        current.purposeKind != publication.purposeKind
        or current.purposeIdentity != publication.purposeIdentity
        or current.operation != publication.operation
      ) {
        runtimeResult(runtime, MReservationMismatch)
      } else if (not(publicationToken.in(runtime.tokens))) {
        runtimeResult(runtime, MReservationTokenMissing)
      } else if (not(ownerCompatible(publication))) {
        runtimeResult(runtime, MPurposeOwnerMismatch)
      } else {
        applyMatchingPublication(runtime, publication)
      }
    }
  }

  pure def removeFromOwnerCollection(
    runtime: RuntimeState,
    released: MechanicalEvent,
  ): RuntimeState =
    if (
      released.ownerKind == MCollectionOwner
      and released.ownerIdentity.in(Collections)
    ) {
      val oldCollection = runtime.collections.nth(released.ownerIdentity)
      val nextCollection =
        oldCollection.with(
          "live",
          oldCollection.live.exclude(Set(released.region))
        )
      runtime.with(
        "collections",
        replaceCollection(
          runtime.collections,
          released.ownerIdentity,
          nextCollection
        )
      )
    } else {
      runtime
    }

  pure def applyKnownRelease(
    runtime: RuntimeState,
    released: MechanicalEvent,
  ): RuntimeResult = {
    val current = runtime.regions.nth(released.region)
    val withoutLiveOwner = removeFromOwnerCollection(runtime, released)
    val dirtied =
      region(MDirty, MNoPurpose, NoneId, NoneId, MNoOwner, NoneId, NoneId)
    if (current.phase != MPublished) {
      runtimeResult(runtime, MInvalidSourceState)
    } else if (
      current.ownerKind != released.ownerKind
      or current.ownerIdentity != released.ownerIdentity
      or current.collectionType != released.collectionType
    ) {
      runtimeResult(runtime, MPublishedOwnerMismatch)
    } else {
      runtimeResult(
        withoutLiveOwner
          .with(
            "regions",
            replaceRegion(
              withoutLiveOwner.regions,
              released.region,
              dirtied
            )
          )
          .with(
            "freeQueue",
            withoutLiveOwner.freeQueue.append(released.region)
          )
          .with("appendTail", withoutLiveOwner.appendTail + 1),
        MOk,
      )
    }
  }

  pure def applyRelease(
    runtime: RuntimeState,
    released: MechanicalEvent,
  ): RuntimeResult =
    if (not(released.region.in(Regions))) {
      runtimeResult(runtime, MRegionOutOfRange)
    } else if (released.region.in(PermanentRegions)) {
      runtimeResult(runtime, MPermanentRegion)
    } else {
      applyKnownRelease(runtime, released)
    }

  pure def applyReadiness(
    runtime: RuntimeState,
    ready: MechanicalEvent,
  ): RuntimeResult =
    if (not(ready.region.in(Regions))) {
      runtimeResult(runtime, MRegionOutOfRange)
    } else if (
      runtime.readyBoundary >= runtime.appendTail
      or intAt(runtime.freeQueue, runtime.readyBoundary) != ready.region
    ) {
      runtimeResult(runtime, MAllocationOrderInvalid)
    } else if (runtime.regions.nth(ready.region).phase != MDirty) {
      runtimeResult(runtime, MInvalidSourceState)
    } else {
      runtimeResult(
        runtime
          .with(
            "regions",
            replaceRegion(runtime.regions, ready.region, EmptyRegion)
          )
          .with("readyBoundary", runtime.readyBoundary + 1),
        MOk,
      )
    }

  pure def applyEvent(
    runtime: RuntimeState,
    eventToApply: MechanicalEvent,
  ): RuntimeResult =
    if (eventToApply.kind == MReserveFact) {
      applyReserve(runtime, eventToApply)
    } else if (eventToApply.kind == MPublishFact) {
      applyPublish(runtime, eventToApply)
    } else if (eventToApply.kind == MReleaseFact) {
      applyRelease(runtime, eventToApply)
    } else {
      applyReadiness(runtime, eventToApply)
    }

  pure def applyEvents(
    runtime: RuntimeState,
    events: List[MechanicalEvent],
  ): RuntimeResult =
    events.foldl(runtimeResult(runtime, MOk), (acc, nextEvent) =>
      if (acc.error == MOk) {
        applyEvent(acc.runtime, nextEvent)
      } else {
        acc
      })

  pure def consumeFreshEraseEvidence(
    evidence: Set[int],
    events: List[MechanicalEvent],
  ): Set[int] =
    events.foldl(evidence, (remaining, nextEvent) =>
      if (nextEvent.kind == MReserveFact) {
        remaining.exclude(Set(nextEvent.region))
      } else {
        remaining
      })

  pure def txGenerationsCurrent(runtime: RuntimeState, tx: TxState): bool =
    (not(0.in(tx.enrolled))
      or tx.observedGeneration0 == runtime.collections.nth(0).generation)
    and (not(1.in(tx.enrolled))
      or tx.observedGeneration1 == runtime.collections.nth(1).generation)

  pure def advanceEnrolledGenerations(
    collections: List[CollectionState],
    enrolled: Set[int],
  ): List[CollectionState] =
    CollectionIndexes.foldl(collections, (current, i) =>
      if (i.in(enrolled)) {
        replaceCollection(
          current,
          i,
          current.nth(i).with(
            "generation",
            current.nth(i).generation + 1
          ),
        )
      } else {
        current
      })

  pure def applyCommitPhase(
    runtime: RuntimeState,
    transaction: int,
  ): RuntimeResult = {
    val tx = runtime.transactions.nth(transaction)
    val committedCollections =
      advanceEnrolledGenerations(runtime.collections, tx.enrolled)
    val committedTx =
      tx
        .with("phase", MTxCommitCleanup)
        .with("cleanupIndex", 0)
        .with("freeIntents", txAllFreeIntents(tx))
        .with("bufferedFreeIntents", [])
    if (
      tx.phase != MTxOpen
      or runtime.finishOwner != NoneId
      or not(txGenerationsCurrent(runtime, tx))
    ) {
      runtimeResult(runtime, MTransactionStateInvalid)
    } else {
      runtimeResult(
        runtime
          .with("collections", committedCollections)
          .with("finishOwner", transaction)
          .with(
            "transactions",
            replaceTransaction(runtime.transactions, transaction, committedTx)
          ),
        MOk,
      )
    }
  }

  pure def applyRollbackPhase(
    runtime: RuntimeState,
    transaction: int,
  ): RuntimeResult = {
    val tx = runtime.transactions.nth(transaction)
    val rolledBack =
      tx
        .with("phase", MTxRollbackCleanup)
        .with("cleanupIndex", 0)
        .with("freeIntents", [])
        .with("bufferedFreeIntents", [])
    if (tx.phase != MTxOpen or runtime.finishOwner != NoneId) {
      runtimeResult(runtime, MTransactionStateInvalid)
    } else {
      runtimeResult(
        runtime
          .with("finishOwner", transaction)
          .with(
            "transactions",
            replaceTransaction(runtime.transactions, transaction, rolledBack)
          ),
        MOk,
      )
    }
  }

  pure def applyCleanupPhase(
    runtime: RuntimeState,
    transaction: int,
  ): RuntimeResult = {
    val tx = runtime.transactions.nth(transaction)
    val advanced = tx.with("cleanupIndex", tx.cleanupIndex + 1)
    if (
      runtime.finishOwner != transaction
      or not(tx.phase == MTxCommitCleanup or tx.phase == MTxRollbackCleanup)
    ) {
      runtimeResult(runtime, MTransactionStateInvalid)
    } else {
      runtimeResult(
        runtime.with(
          "transactions",
          replaceTransaction(runtime.transactions, transaction, advanced)
        ),
        MOk,
      )
    }
  }

  pure def applyFinishPhase(
    runtime: RuntimeState,
    transaction: int,
  ): RuntimeResult = {
    val tx = runtime.transactions.nth(transaction)
    if (
      runtime.finishOwner != transaction
      or not(tx.phase == MTxCommitCleanup or tx.phase == MTxRollbackCleanup)
    ) {
      runtimeResult(runtime, MTransactionStateInvalid)
    } else {
      runtimeResult(
        runtime
          .with("finishOwner", NoneId)
          .with(
            "transactions",
            replaceTransaction(runtime.transactions, transaction, EmptyTx)
          ),
        MOk,
      )
    }
  }

  pure def applyCommandPhase(
    runtime: RuntimeState,
    durable: DurableCommand,
  ): RuntimeResult =
    if (
      durable.kind == MCommitCommand
      or durable.kind == MRollbackCommand
      or durable.kind == MCleanupCommand
      or durable.kind == MFinishCommand
    ) {
      if (not(durable.transaction.in(Transactions))) {
        runtimeResult(runtime, MTransactionStateInvalid)
      } else if (durable.kind == MCommitCommand) {
        applyCommitPhase(runtime, durable.transaction)
      } else if (durable.kind == MRollbackCommand) {
        applyRollbackPhase(runtime, durable.transaction)
      } else if (durable.kind == MCleanupCommand) {
        applyCleanupPhase(runtime, durable.transaction)
      } else {
        applyFinishPhase(runtime, durable.transaction)
      }
    } else {
      runtimeResult(runtime, MOk)
    }

  pure def applyCommand(
    runtime: RuntimeState,
    durable: DurableCommand,
  ): RuntimeResult = {
    val afterEvents = applyEvents(runtime, durable.events)
    if (afterEvents.error == MOk) {
      applyCommandPhase(afterEvents.runtime, durable)
    } else {
      afterEvents
    }
  }

  pure def replay(commands: List[DurableCommand]): RuntimeResult =
    commands.foldl(runtimeResult(InitialRuntime, MOk), (acc, durable) =>
      if (acc.error == MOk) {
        applyCommand(acc.runtime, durable)
      } else {
        acc
      })

  ////// Mechanical issue, sync, apply, crash, and replay actions //////

  var mechanical: MechanicalState
  var verificationDepth: int

  action init = all {
    mechanical' = InitialMechanical,
    verificationDepth' = 0,
  }

  action BeginTransaction = {
    nondet txIndex = Transactions.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      tx.phase == MTxIdle,
      mechanical' =
        mechanical.with(
          "runtime",
          mechanical.runtime
            .with(
              "transactions",
              replaceTransaction(
                mechanical.runtime.transactions,
                txIndex,
                openTx(mechanical.runtime),
              )
            )
            .with("nextBeginOrder", mechanical.runtime.nextBeginOrder + 1)
        ),
    }
  }

  action EnrollCollection = {
    nondet txIndex = Transactions.oneOf()
    nondet collectionIndex = Collections.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      tx.phase == MTxOpen,
      mechanical.runtime.transactions.indices().forall(other =>
        other == txIndex
        or not(
          collectionIndex.in(
            mechanical.runtime.transactions.nth(other).enrolled
          )
        )),
      mechanical' =
        mechanical.with(
          "runtime",
          mechanical.runtime.with(
            "transactions",
            replaceTransaction(
              mechanical.runtime.transactions,
              txIndex,
              tx.with("enrolled", tx.enrolled.union(Set(collectionIndex))),
            )
          )
        ),
    }
  }

  action StageFreeIntent = {
    nondet txIndex = Transactions.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    nondet collectionIndex = tx.enrolled.oneOf()
    nondet regionIndex = mechanical.runtime.collections.nth(collectionIndex).live.oneOf()
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      tx.phase == MTxOpen,
      not(regionIndex.in(listToSet(txAllFreeIntents(tx)))),
      mechanical' =
        mechanical.with(
          "runtime",
          mechanical.runtime.with(
            "transactions",
            replaceTransaction(
              mechanical.runtime.transactions,
              txIndex,
              tx.with(
                "bufferedFreeIntents",
                tx.bufferedFreeIntents.append(regionIndex)
              ),
            )
          )
        ),
    }
  }

  action SealPrivateSuffix = {
    nondet txIndex = Transactions.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    all {
      mechanical.phase == MQuiescent,
      tx.phase == MTxOpen,
      mechanical' =
        mechanical.with(
          "runtime",
          mechanical.runtime.with(
            "transactions",
            replaceTransaction(
              mechanical.runtime.transactions,
              txIndex,
              tx
                .with("freeIntents", txAllFreeIntents(tx))
                .with("bufferedFreeIntents", []),
            )
          )
        ),
    }
  }

  action IssueCollectionAllocation = {
    nondet collectionIndex = Collections.oneOf()
    val runtime = mechanical.runtime
    val nextRegion = intAt(runtime.freeQueue, runtime.allocationHead)
    val purposeIdentity = runtime.collections.nth(collectionIndex).collectionType
    val allocation =
      reserveEvent(
        runtime,
        nextRegion,
        MCollectionData,
        purposeIdentity,
        100 + runtime.allocationSequence,
        NoneId,
      )
    all {
      mechanical.phase == MQuiescent,
      runtime.finishOwner == NoneId,
      runtime.allocationHead < runtime.readyBoundary,
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MReserveCommand, NoneId, [allocation])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action IssueTransactionAllocation = {
    nondet txIndex = Transactions.oneOf()
    val runtime = mechanical.runtime
    val tx = runtime.transactions.nth(txIndex)
    val nextRegion = intAt(runtime.freeQueue, runtime.allocationHead)
    val allocation =
      reserveEvent(
        runtime,
        nextRegion,
        MTransactionLog,
        txIndex,
        200 + runtime.allocationSequence,
        txIndex,
      )
    all {
      mechanical.phase == MQuiescent,
      runtime.finishOwner == NoneId,
      tx.phase == MTxOpen,
      tx.enrolled != Set(),
      runtime.allocationHead < runtime.readyBoundary,
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MReserveCommand, txIndex, [allocation])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action SyncEvents = all {
    mechanical.phase == MEventsIssued,
    mechanical' =
      mechanical
        .with(
          "durableCommands",
          mechanical.durableCommands.append(mechanical.pendingCommand)
        )
        .with("phase", MEventsDurable),
  }

  action ApplyDurableEvents = {
    val applied = applyCommand(mechanical.runtime, mechanical.pendingCommand)
    all {
      mechanical.phase == MEventsDurable,
      applied.error == MOk,
      mechanical' =
        mechanical
          .with("runtime", applied.runtime)
          .with(
            "freshEraseEvidence",
            consumeFreshEraseEvidence(
              mechanical.freshEraseEvidence,
              mechanical.pendingCommand.events,
            )
          )
          .with("pendingCommand", EmptyCommand)
          .with("phase", MQuiescent)
          .with("activeRegion", NoneId)
          .with("lastError", applied.error),
    }
  }

  action IssueContent = {
    nondet reservationToken = mechanical.runtime.tokens.oneOf()
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.regions.nth(reservationToken.region).phase == MReserved,
      not(reservationToken.in(mechanical.durableContent)),
      mechanical' =
        mechanical
          .with("activeToken", reservationToken)
          .with("activeRegion", reservationToken.region)
          .with("phase", MContentIssued),
    }
  }

  action SyncContent = all {
    mechanical.phase == MContentIssued,
    mechanical' =
      mechanical
        .with(
          "durableContent",
          mechanical.durableContent.union(Set(mechanical.activeToken))
        )
        .with(
          "physicallyErased",
          mechanical.physicallyErased.exclude(Set(mechanical.activeToken.region))
        )
        .with("activeToken", EmptyToken)
        .with("activeRegion", NoneId)
        .with("phase", MQuiescent),
  }

  pure def publicationForToken(reservationToken: Token): MechanicalEvent =
    if (reservationToken.purposeKind == MTransactionLog) {
      publishEvent(
        reservationToken,
        MTransactionOwner,
        reservationToken.purposeIdentity,
        NoneId,
      )
    } else if (reservationToken.purposeKind == MMainWal) {
      publishEvent(reservationToken, MMainWalOwner, 0, NoneId)
    } else if (reservationToken.purposeKind == MFreeBasis) {
      publishEvent(reservationToken, MFreeBasisOwner, 0, NoneId)
    } else if (reservationToken.purposeKind == MCollectionData) {
      publishEvent(
        reservationToken,
        MCollectionOwner,
        if (reservationToken.purposeIdentity == 7) { 0 } else { 1 },
        reservationToken.purposeIdentity,
      )
    } else {
      publishEvent(reservationToken, MBootstrapOwner, 0, NoneId)
    }

  action IssuePublication = {
    nondet reservationToken = mechanical.durableContent.oneOf()
    val publication = publicationForToken(reservationToken)
    all {
      mechanical.phase == MQuiescent,
      reservationToken.in(mechanical.runtime.tokens),
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MPublishCommand, publication.transaction, [publication])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  // This action demonstrates that one synced record can atomically publish a
  // batch. It is enabled only when regions 4 and 5 are both durably complete.
  action IssuePublicationPair = {
    val token4 = token(
      4,
      mechanical.runtime.regions.nth(4).purposeKind,
      mechanical.runtime.regions.nth(4).purposeIdentity,
      mechanical.runtime.regions.nth(4).operation,
    )
    val token5 = token(
      5,
      mechanical.runtime.regions.nth(5).purposeKind,
      mechanical.runtime.regions.nth(5).purposeIdentity,
      mechanical.runtime.regions.nth(5).operation,
    )
    all {
      mechanical.phase == MQuiescent,
      token4.in(mechanical.runtime.tokens),
      token5.in(mechanical.runtime.tokens),
      token4.in(mechanical.durableContent),
      token5.in(mechanical.durableContent),
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(
              MPublishCommand,
              NoneId,
              [publicationForToken(token4), publicationForToken(token5)],
            )
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action IssueRelease = {
    nondet regionIndex = Regions.exclude(PermanentRegions).oneOf()
    val current = mechanical.runtime.regions.nth(regionIndex)
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      current.phase == MPublished,
      current.ownerKind != MMainWalOwner,
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MReleaseCommand, NoneId, [releaseEvent(current, regionIndex)])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  pure def allTxAllocationsPublished(runtime: RuntimeState, tx: TxState): bool =
    tx.allocatedRegions.indices().forall(i => {
      val regionIndex = tx.allocatedRegions.nth(i)
      val current = runtime.regions.nth(regionIndex)
      current.phase == MPublished
      and current.ownerKind == MTransactionOwner
    })

  action IssueCommit = {
    nondet txIndex = Transactions.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      tx.phase == MTxOpen,
      tx.enrolled != Set(),
      tx.allocatedRegions.length() > 0,
      txGenerationsCurrent(mechanical.runtime, tx),
      allTxAllocationsPublished(mechanical.runtime, tx),
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MCommitCommand, txIndex, [])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action IssueRollback = {
    nondet txIndex = Transactions.oneOf()
    val tx = mechanical.runtime.transactions.nth(txIndex)
    all {
      mechanical.phase == MQuiescent,
      mechanical.runtime.finishOwner == NoneId,
      tx.phase == MTxOpen,
      tx.allocatedRegions.length() > 0,
      allTxAllocationsPublished(mechanical.runtime, tx),
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MRollbackCommand, txIndex, [])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  pure def cleanupRegions(tx: TxState): List[int] =
    if (tx.phase == MTxCommitCleanup) {
      tx.freeIntents
    } else {
      tx.allocatedRegions
    }

  action IssueCleanup = {
    val txIndex = mechanical.runtime.finishOwner
    val tx = transactionAt(mechanical.runtime, txIndex)
    val obligations = cleanupRegions(tx)
    val regionIndex = intAt(obligations, tx.cleanupIndex)
    val current =
      if (regionIndex.in(Regions)) {
        mechanical.runtime.regions.nth(regionIndex)
      } else {
        EmptyRegion
      }
    all {
      mechanical.phase == MQuiescent,
      txIndex.in(Transactions),
      tx.cleanupIndex < obligations.length(),
      current.phase == MPublished,
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(
              MCleanupCommand,
              txIndex,
              [releaseEvent(current, regionIndex)],
            )
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action IssueFinish = {
    val txIndex = mechanical.runtime.finishOwner
    val tx = transactionAt(mechanical.runtime, txIndex)
    val obligations = cleanupRegions(tx)
    all {
      mechanical.phase == MQuiescent,
      txIndex.in(Transactions),
      tx.cleanupIndex == obligations.length(),
      mechanical' =
        mechanical
          .with(
            "pendingCommand",
            command(MFinishCommand, txIndex, [])
          )
          .with("phase", MEventsIssued)
          .with("lastError", MOk),
    }
  }

  action IssueErase = {
    val runtime = mechanical.runtime
    val regionIndex = intAt(runtime.freeQueue, runtime.readyBoundary)
    all {
      mechanical.phase == MQuiescent,
      runtime.readyBoundary < runtime.appendTail,
      runtime.regions.nth(regionIndex).phase == MDirty,
      mechanical' =
        mechanical
          .with("activeRegion", regionIndex)
          .with("phase", MEraseIssued),
    }
  }

  action CompleteErase = all {
    mechanical.phase == MEraseIssued,
    mechanical' =
      mechanical
        .with(
          "physicallyErased",
          mechanical.physicallyErased.union(Set(mechanical.activeRegion))
        )
        .with(
          "eraseEvidence",
          mechanical.eraseEvidence.union(Set(mechanical.activeRegion))
        )
        .with(
          "freshEraseEvidence",
          mechanical.freshEraseEvidence.union(Set(mechanical.activeRegion))
        )
        .with("phase", MEraseComplete),
  }

  action IssueReadiness = all {
    mechanical.phase == MEraseComplete,
    mechanical.activeRegion.in(mechanical.physicallyErased),
    mechanical.activeRegion.in(mechanical.freshEraseEvidence),
    mechanical' =
      mechanical
        .with(
          "pendingCommand",
          command(
            MReadinessCommand,
            NoneId,
            [readinessEvent(mechanical.activeRegion)],
          )
        )
        .with("phase", MEventsIssued)
          .with("lastError", MOk),
  }

  pure def normalizeFreshEraseEvidence(
    runtime: RuntimeState,
    evidence: Set[int],
  ): Set[int] =
    RegionIndexes.foldl(Set(), (normalized, regionIndex) =>
      if (
        regionIndex.in(evidence)
        and (
          runtime.regions.nth(regionIndex).phase == MErasedPrepared
          or runtime.regions.nth(regionIndex).phase == MDirty
        )
      ) {
        normalized.union(Set(regionIndex))
      } else {
        normalized
      })

  pure def recoveredMechanical(
    state: MechanicalState,
    durable: List[DurableCommand],
    content: Set[Token],
    erased: Set[int],
    freshEraseEvidence: Set[int],
    eraseEvidence: Set[int],
    tornSpans: int,
  ): MechanicalState = {
    val recovered = replay(durable)
    state
      .with("runtime", recovered.runtime)
      .with("durableCommands", durable)
      .with("pendingCommand", EmptyCommand)
      .with("phase", MQuiescent)
      .with("durableContent", content)
      .with("physicallyErased", erased)
      .with(
        "freshEraseEvidence",
        normalizeFreshEraseEvidence(recovered.runtime, freshEraseEvidence)
      )
      .with("eraseEvidence", eraseEvidence)
      .with("activeToken", EmptyToken)
      .with("activeRegion", NoneId)
      .with("tornSpans", tornSpans)
      .with("lastError", recovered.error)
      .with("lastAttemptedEvent", EmptyEvent)
  }

  action CrashDropsUnsynced = {
    val erasedAfterCrash =
      if (
        mechanical.phase == MEraseIssued
        or mechanical.phase == MEraseComplete
      ) {
        mechanical.physicallyErased.exclude(Set(mechanical.activeRegion))
      } else {
        mechanical.physicallyErased
      }
    val freshAfterCrash =
      if (
        mechanical.phase == MEraseIssued
        or mechanical.phase == MEraseComplete
      ) {
        mechanical.freshEraseEvidence.exclude(Set(mechanical.activeRegion))
      } else {
        mechanical.freshEraseEvidence
      }
    all {
      mechanical' =
        recoveredMechanical(
          mechanical,
          mechanical.durableCommands,
          mechanical.durableContent,
          erasedAfterCrash,
          freshAfterCrash,
          mechanical.eraseEvidence,
          mechanical.tornSpans,
        ),
    }
  }

  action CrashKeepsCompleteUnsynced = {
    val keptCommands =
      if (mechanical.phase == MEventsIssued) {
        mechanical.durableCommands.append(mechanical.pendingCommand)
      } else {
        mechanical.durableCommands
      }
    val keptContent =
      if (mechanical.phase == MContentIssued) {
        mechanical.durableContent.union(Set(mechanical.activeToken))
      } else {
        mechanical.durableContent
      }
    val keptErase =
      if (mechanical.phase == MContentIssued) {
        mechanical.physicallyErased.exclude(Set(mechanical.activeToken.region))
      } else if (
        mechanical.phase == MEraseIssued
        or mechanical.phase == MEraseComplete
      ) {
        mechanical.physicallyErased.union(Set(mechanical.activeRegion))
      } else {
        mechanical.physicallyErased
      }
    all {
      mechanical' =
        recoveredMechanical(
          mechanical,
          keptCommands,
          keptContent,
          keptErase,
          if (
            mechanical.phase == MEraseIssued
            or mechanical.phase == MEraseComplete
          ) {
            mechanical.freshEraseEvidence.union(Set(mechanical.activeRegion))
          } else {
            mechanical.freshEraseEvidence
          },
          if (
            mechanical.phase == MEraseIssued
            or mechanical.phase == MEraseComplete
          ) {
            mechanical.eraseEvidence.union(Set(mechanical.activeRegion))
          } else {
            mechanical.eraseEvidence
          },
          mechanical.tornSpans,
        ),
    }
  }

  action CrashKeepsTornUnsynced = all {
    mechanical.phase == MEventsIssued or mechanical.phase == MContentIssued,
    mechanical' =
      recoveredMechanical(
        mechanical,
        mechanical.durableCommands,
        mechanical.durableContent,
        if (mechanical.phase == MContentIssued) {
          mechanical.physicallyErased.exclude(Set(mechanical.activeToken.region))
        } else {
          mechanical.physicallyErased
        },
        mechanical.freshEraseEvidence,
        mechanical.eraseEvidence,
        mechanical.tornSpans + 1,
      ),
  }

  action RejectMismatchedPublication = {
    nondet reservationToken = mechanical.runtime.tokens.oneOf()
    val wrong = reservationToken.with("operation", reservationToken.operation + 1)
    val attempted = publicationForToken(wrong)
    val rejected = applyEvent(mechanical.runtime, attempted)
    all {
      mechanical.phase == MQuiescent,
      rejected.error != MOk,
      mechanical' =
        mechanical
          .with("lastError", rejected.error)
          .with("lastAttemptedEvent", attempted),
    }
  }

  action RejectIncompatibleOwnerPublication = {
    nondet reservationToken = mechanical.runtime.tokens.oneOf()
    val attempted =
      if (reservationToken.purposeKind == MCollectionData) {
        publishEvent(reservationToken, MMainWalOwner, 0, NoneId)
      } else {
        publishEvent(reservationToken, MCollectionOwner, 0, 7)
      }
    val rejected = applyEvent(mechanical.runtime, attempted)
    all {
      mechanical.phase == MQuiescent,
      rejected.error == MPurposeOwnerMismatch,
      mechanical' =
        mechanical
          .with("lastError", rejected.error)
          .with("lastAttemptedEvent", attempted),
    }
  }

  action RejectOutOfRangeCollectionOwner = {
    nondet reservationToken = mechanical.runtime.tokens.oneOf()
    val attempted =
      publishEvent(
        reservationToken,
        MCollectionOwner,
        2,
        reservationToken.purposeIdentity,
      )
    val rejected = applyEvent(mechanical.runtime, attempted)
    all {
      mechanical.phase == MQuiescent,
      reservationToken.purposeKind == MCollectionData,
      rejected.error == MPurposeOwnerMismatch,
      mechanical' =
        mechanical
          .with("lastError", rejected.error)
          .with("lastAttemptedEvent", attempted),
    }
  }

  action RejectWrongTransactionOwnerPublication = {
    nondet reservationToken = mechanical.runtime.tokens.oneOf()
    val wrongOwner =
      if (reservationToken.purposeIdentity == 0) { 1 } else { 0 }
    val attempted =
      publishEvent(
        reservationToken,
        MTransactionOwner,
        wrongOwner,
        NoneId,
      )
    val rejected = applyEvent(mechanical.runtime, attempted)
    all {
      mechanical.phase == MQuiescent,
      reservationToken.purposeKind == MTransactionLog,
      rejected.error == MPurposeOwnerMismatch,
      mechanical' =
        mechanical
          .with("lastError", rejected.error)
          .with("lastAttemptedEvent", attempted),
    }
  }

  action RejectInvalidPurposeReservation = {
    val runtime = mechanical.runtime
    val nextRegion = intAt(runtime.freeQueue, runtime.allocationHead)
    val attempted =
      reserveEvent(
        runtime,
        nextRegion,
        MNoPurpose,
        NoneId,
        700 + runtime.allocationSequence,
        NoneId,
      )
    val rejected = applyEvent(runtime, attempted)
    all {
      mechanical.phase == MQuiescent,
      runtime.allocationHead < runtime.readyBoundary,
      rejected.error == MPurposeInvalid,
      mechanical' =
        mechanical
          .with("lastError", rejected.error)
          .with("lastAttemptedEvent", attempted),
    }
  }

  ////// Deliberately unsafe actions used only by expected-failure checks //////

  action UnsafePublishWithoutContent = {
    val allocation =
      reserveEvent(
        InitialRuntime,
        4,
        MCollectionData,
        7,
        900,
        NoneId,
      )
    val allocationCommand = command(MReserveCommand, NoneId, [allocation])
    val afterAllocation = applyCommand(InitialRuntime, allocationCommand)
    val reservationToken = tokenOfEvent(allocation)
    val publication =
      publishEvent(reservationToken, MCollectionOwner, 0, 7)
    val publicationCommand =
      command(MPublishCommand, NoneId, [publication])
    val afterPublication =
      applyCommand(afterAllocation.runtime, publicationCommand)
    val unsafeState =
      mechanical
        .with("runtime", afterPublication.runtime)
        .with(
          "durableCommands",
          [allocationCommand, publicationCommand]
        )
        .with("pendingCommand", EmptyCommand)
        .with("phase", MQuiescent)
    all {
      mechanical == InitialMechanical,
      verificationDepth == 0,
      afterAllocation.error == MOk,
      afterPublication.error == MOk,
      mechanical' = unsafeState,
      verificationDepth' = verificationDepth + 1,
    }
  }

  action UnsafeReadinessWithoutErase = {
    val released = releaseEvent(InitialRuntime.regions.nth(2), 2)
    val releaseCommand = command(MReleaseCommand, NoneId, [released])
    val afterRelease = applyCommand(InitialRuntime, releaseCommand)
    val readyCommand =
      command(MReadinessCommand, NoneId, [readinessEvent(2)])
    val afterReadiness = applyCommand(afterRelease.runtime, readyCommand)
    val unsafeState =
      mechanical
        .with("runtime", afterReadiness.runtime)
        .with("durableCommands", [releaseCommand, readyCommand])
        .with("pendingCommand", EmptyCommand)
        .with("phase", MQuiescent)
    all {
      mechanical == InitialMechanical,
      verificationDepth == 0,
      afterRelease.error == MOk,
      afterReadiness.error == MOk,
      mechanical' = unsafeState,
      verificationDepth' = verificationDepth + 1,
    }
  }

  action UnsafeReusePublished = {
    val forgedToken = token(2, MCollectionData, 7, 901)
    val forgedRegion =
      region(MReserved, MCollectionData, 7, 901, MNoOwner, NoneId, NoneId)
    val forgedRuntime =
      InitialRuntime
        .with(
          "regions",
          replaceRegion(InitialRuntime.regions, 2, forgedRegion)
        )
        .with("tokens", Set(forgedToken))
    val unsafeState = mechanical.with("runtime", forgedRuntime)
    all {
      mechanical == InitialMechanical,
      verificationDepth == 0,
      mechanical' = unsafeState,
      verificationDepth' = verificationDepth + 1,
    }
  }

  action UnsafeMismatchedTokenPublication = {
    val allocation =
      reserveEvent(
        InitialRuntime,
        4,
        MCollectionData,
        7,
        902,
        NoneId,
      )
    val allocationCommand = command(MReserveCommand, NoneId, [allocation])
    val afterAllocation = applyCommand(InitialRuntime, allocationCommand)
    val wrongToken = token(4, MCollectionData, 7, 999)
    val wrongPublication =
      publishEvent(wrongToken, MCollectionOwner, 0, 7)
    val publicationCommand =
      command(MPublishCommand, NoneId, [wrongPublication])
    val unsafeState =
      mechanical
        .with("runtime", afterAllocation.runtime)
        .with(
          "durableCommands",
          [allocationCommand, publicationCommand]
        )
        .with("pendingCommand", EmptyCommand)
        .with("phase", MQuiescent)
    all {
      mechanical == InitialMechanical,
      verificationDepth == 0,
      afterAllocation.error == MOk,
      mechanical' = unsafeState,
      verificationDepth' = verificationDepth + 1,
    }
  }

  action Stutter = all { mechanical' = mechanical }

  action unboundedStep = any {
    BeginTransaction,
    EnrollCollection,
    StageFreeIntent,
    SealPrivateSuffix,
    IssueCollectionAllocation,
    IssueTransactionAllocation,
    SyncEvents,
    ApplyDurableEvents,
    IssueContent,
    SyncContent,
    IssuePublication,
    IssuePublicationPair,
    IssueRelease,
    IssueCommit,
    IssueRollback,
    IssueCleanup,
    IssueFinish,
    IssueErase,
    CompleteErase,
    IssueReadiness,
    CrashDropsUnsynced,
    CrashKeepsCompleteUnsynced,
    CrashKeepsTornUnsynced,
    RejectMismatchedPublication,
    RejectIncompatibleOwnerPublication,
    RejectOutOfRangeCollectionOwner,
    RejectWrongTransactionOwnerPublication,
    RejectInvalidPurposeReservation,
    Stutter,
  }

  action step =
    if (verificationDepth < MechanicalVerificationBound) {
      all {
        unboundedStep,
        verificationDepth' = verificationDepth + 1,
      }
    } else {
      all {
        mechanical' = mechanical,
        verificationDepth' = verificationDepth,
      }
    }

  ////// Mechanical invariants //////

  pure def purposeIdentityWellFormed(current: Region): bool =
    if (current.purposeKind == MTransactionLog) {
      current.purposeIdentity.in(Transactions)
    } else if (current.purposeKind == MCollectionData) {
      current.purposeIdentity >= 0
    } else {
      current.purposeKind != MNoPurpose
    }

  pure def publishedIdentityWellFormed(current: Region): bool = {
    val publication =
      event(
        MPublishFact,
        0,
        current.purposeKind,
        current.purposeIdentity,
        current.operation,
        current.ownerKind,
        current.ownerIdentity,
        current.collectionType,
        NoneId,
        0,
        0,
      )
    purposeIdentityWellFormed(current)
    and current.ownerKind != MNoOwner
    and ownerCompatible(publication)
    and (current.ownerKind != MCollectionOwner
      or current.ownerIdentity.in(Collections))
  }

  pure def regionFieldsWellFormed(current: Region): bool =
    if (current.phase == MErasedPrepared or current.phase == MDirty) {
      current.purposeKind == MNoPurpose
      and current.purposeIdentity == NoneId
      and current.operation == NoneId
      and current.ownerKind == MNoOwner
      and current.ownerIdentity == NoneId
      and current.collectionType == NoneId
    } else if (current.phase == MReserved) {
      purposeIdentityWellFormed(current)
      and current.operation != NoneId
      and current.ownerKind == MNoOwner
      and current.ownerIdentity == NoneId
      and current.collectionType == NoneId
    } else {
      publishedIdentityWellFormed(current)
    }

  pure def freshEraseEvidenceWellFormed(state: MechanicalState): bool =
    state.freshEraseEvidence.forall(regionIndex =>
      regionIndex.in(Regions)
      and regionIndex.in(state.physicallyErased)
      and (
        state.runtime.regions.nth(regionIndex).phase == MErasedPrepared
        or state.runtime.regions.nth(regionIndex).phase == MDirty
      ))
    and Regions.forall(regionIndex =>
      (state.runtime.regions.nth(regionIndex).phase == MErasedPrepared)
        .implies(regionIndex.in(state.freshEraseEvidence)))

  pure def contentEvidenceWellFormed(state: MechanicalState): bool =
    (state.phase == MContentIssued).implies(
      state.activeToken.region.in(Regions)
      and not(state.activeToken.in(state.durableContent))
    )

  pure def runtimeOwnershipWellFormed(runtime: RuntimeState): bool =
    runtime.regions.length() == RegionIndexes.length()
    and runtime.collections.length() == CollectionIndexes.length()
    and runtime.transactions.length() == TransactionIndexes.length()
    and runtime.regions.nth(0).phase == MPublished
    and runtime.regions.nth(0).ownerKind == MBootstrapOwner
    and runtime.regions.indices().forall(regionIndex =>
      regionFieldsWellFormed(runtime.regions.nth(regionIndex)))
    and runtime.tokens.forall(reservationToken =>
      reservationToken.region.in(Regions)
      and runtime.regions.nth(reservationToken.region).phase == MReserved
      and runtime.regions.nth(reservationToken.region).purposeKind
        == reservationToken.purposeKind
      and runtime.regions.nth(reservationToken.region).purposeIdentity
        == reservationToken.purposeIdentity
      and runtime.regions.nth(reservationToken.region).operation
        == reservationToken.operation)
    and Regions.forall(regionIndex => {
      val current = runtime.regions.nth(regionIndex)
      if (current.phase == MReserved) {
        token(
          regionIndex,
          current.purposeKind,
          current.purposeIdentity,
          current.operation,
        ).in(runtime.tokens)
      } else {
        runtime.tokens.forall(reservationToken =>
          reservationToken.region != regionIndex)
      }
    })

  pure def sameOwnership(left: RuntimeState, right: RuntimeState): bool =
    left.regions == right.regions
    and left.tokens == right.tokens
    and left.freeQueue == right.freeQueue
    and left.allocationHead == right.allocationHead
    and left.readyBoundary == right.readyBoundary
    and left.appendTail == right.appendTail
    and left.allocationSequence == right.allocationSequence

  pure def runtimeMatchesDurableState(state: MechanicalState): bool = {
    val recovered = replay(state.durableCommands)
    val applied = applyCommand(state.runtime, state.pendingCommand)
    if (recovered.error != MOk) {
      false
    } else if (state.phase == MEventsDurable) {
      applied.error == MOk and sameOwnership(applied.runtime, recovered.runtime)
    } else {
      sameOwnership(state.runtime, recovered.runtime)
    }
  }

  pure def enrollmentExclusive(runtime: RuntimeState): bool =
    Transactions.forall(left =>
      Transactions.forall(right =>
        (left != right).implies(
          runtime.transactions.nth(left).enrolled.intersect(
            runtime.transactions.nth(right).enrolled
          ) == Set()
        )))

  pure def finishLockWellFormed(runtime: RuntimeState): bool =
    if (runtime.finishOwner == NoneId) {
      Transactions.forall(txIndex =>
        not(
          runtime.transactions.nth(txIndex).phase == MTxCommitCleanup
          or runtime.transactions.nth(txIndex).phase == MTxRollbackCleanup
        ))
    } else {
      runtime.finishOwner.in(Transactions)
      and Transactions.forall(txIndex =>
        if (txIndex == runtime.finishOwner) {
          runtime.transactions.nth(txIndex).phase == MTxCommitCleanup
          or runtime.transactions.nth(txIndex).phase == MTxRollbackCleanup
        } else {
          not(
            runtime.transactions.nth(txIndex).phase == MTxCommitCleanup
            or runtime.transactions.nth(txIndex).phase == MTxRollbackCleanup
          )
        })
    }

  pure def everyDurablePublicationHasContent(state: MechanicalState): bool =
    state.durableCommands.indices().forall(commandIndex =>
      state.durableCommands.nth(commandIndex).events.indices().forall(eventIndex => {
        val durableEvent =
          state.durableCommands.nth(commandIndex).events.nth(eventIndex)
        (durableEvent.kind == MPublishFact).implies(
          tokenOfEvent(durableEvent).in(state.durableContent)
        )
      }))

  pure def everyDurableReadinessHasErase(state: MechanicalState): bool =
    state.durableCommands.indices().forall(commandIndex =>
      state.durableCommands.nth(commandIndex).events.indices().forall(eventIndex => {
        val durableEvent =
          state.durableCommands.nth(commandIndex).events.nth(eventIndex)
        (durableEvent.kind == MReadinessFact).implies(
          durableEvent.region.in(state.eraseEvidence)
        )
      }))

  val mechanicalWellFormed =
    runtimeOwnershipWellFormed(mechanical.runtime)
    and mechanical.runtime.allocationHead <= mechanical.runtime.readyBoundary
    and mechanical.runtime.readyBoundary <= mechanical.runtime.appendTail
    and mechanical.runtime.appendTail <= mechanical.runtime.freeQueue.length()
    and enrollmentExclusive(mechanical.runtime)
    and finishLockWellFormed(mechanical.runtime)
    and runtimeMatchesDurableState(mechanical)
    and freshEraseEvidenceWellFormed(mechanical)
    and contentEvidenceWellFormed(mechanical)
    and everyDurablePublicationHasContent(mechanical)
    and everyDurableReadinessHasErase(mechanical)

  val safety =
    mechanicalWellFormed
    and verificationDepth >= 0
    and verificationDepth <= MechanicalVerificationBound
}
