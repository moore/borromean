// Semantic model for allocator WAL commands, transaction-owned allocations,
// transaction-private free intents, and transaction cleanup.
//
// The collection payload semantics are intentionally abstracted away. The
// model assumes any collection effect that must be undone before transaction
// commit is either volatile frontier state or a write to a transaction-owned
// region. It does not model durable mutation of preexisting collection-owned
// regions, because that requires a collection-specific undo rule rather than
// allocator-only recovery.
//
// The trace is the durable WAL prefix. Each action models both making one WAL
// command durable and applying that command to replayed runtime state. A crash
// cut is therefore any prefix of the action trace: online apply and crash
// replay are intentionally the same transition relation in this model.

module transaction_free_recovery {
  type TxPhase =
    | TxIdle
    | TxOpen
    | TxRollbackPreparing
    | TxCommittedCleanup
    | TxRolledBackCleanup

  // Vocabulary used throughout the model:
  //
  // - A physical region is a bare region index. Collection live sets and the
  //   active free-space range own physical regions.
  // - A logical region is a physical region plus the sequence recorded in that
  //   region's current valid header. Free and cleanup authority is modeled as
  //   a logical region so old cleanup evidence cannot free a later reuse of
  //   the same physical region.
  // - Transaction allocation lists start as physical regions. Rollback
  //   preparation turns them into logical cleanup refs after headers are
  //   written.
  type LogicalRegionRef = {
    region: int,
    sequence: int,
  }

  type RegionHeader = {
    valid: bool,
    sequence: int,
  }

  type TxState = {
    phase: TxPhase,
    collectionIndex: int,
    observedGeneration: int,
    cleanupIndex: int,
    allocations: List[int],
    freeIntents: List[LogicalRegionRef],
    rollbackAllocations: List[LogicalRegionRef],
  }

  type CollectionState = {
    live: Set[int],
    generation: int,
  }

  type StorageState = {
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    regions: List[RegionHeader],
    nextSequence: int,
    collections: List[CollectionState],
    transactions: List[TxState],
  }

  pure val NoCollection = -1

  // *Indexes lists are ordered rebuild lists used with foldl([], ...).
  pure val RegionIndexes = [0, 1, 2, 3, 4, 5]
  pure val TransactionIndexes = [0, 1]
  pure val CollectionIndexes = [0, 1]

  // Regions is the modeled region universe. Dynamic list index sets should
  // come from .indices() on the current list state.
  pure val Regions = Set(0, 1, 2, 3, 4, 5)

  pure def validHeader(sequence: int): RegionHeader = {
    valid: true,
    sequence: sequence,
  }

  pure def invalidHeader(sequence: int): RegionHeader = {
    valid: false,
    sequence: sequence,
  }

  pure def replaceRegionHeader(
    headers: List[RegionHeader],
    region: int,
    header: RegionHeader,
  ): List[RegionHeader] =
    RegionIndexes.foldl([], (acc, i) =>
      acc.append(if (i == region) { header } else { headers.nth(i) }))

  pure def collectionState(live: Set[int], generation: int): CollectionState = {
    live: live,
    generation: generation,
  }

  pure def collectionAt(
    collections: List[CollectionState],
    collectionIndex: int,
  ): CollectionState =
    if (collectionIndex.in(collections.indices())) {
      collections.nth(collectionIndex)
    } else {
      collectionState(Set(), -1)
    }

  pure def collectionLiveAt(
    collections: List[CollectionState],
    collectionIndex: int,
  ): Set[int] =
    collectionAt(collections, collectionIndex).live

  pure def collectionGenerationAt(
    collections: List[CollectionState],
    collectionIndex: int,
  ): int =
    collectionAt(collections, collectionIndex).generation

  pure def replaceCollection(
    collections: List[CollectionState],
    collectionIndex: int,
    nextCollection: CollectionState,
  ): List[CollectionState] =
    CollectionIndexes.foldl([], (acc, i) =>
      acc.append(
        if (i == collectionIndex) {
          nextCollection
        } else {
          collections.nth(i)
        }))

  pure def replaceCollectionLive(
    collections: List[CollectionState],
    collectionIndex: int,
    nextLive: Set[int],
  ): List[CollectionState] =
    replaceCollection(
      collections,
      collectionIndex,
      collectionState(
        nextLive,
        collectionGenerationAt(collections, collectionIndex),
      ),
    )

  pure def replaceCollectionGeneration(
    collections: List[CollectionState],
    collectionIndex: int,
    generation: int,
  ): List[CollectionState] =
    replaceCollection(
      collections,
      collectionIndex,
      collectionState(collectionLiveAt(collections, collectionIndex), generation),
    )

  pure def collectionLivesDisjointFrom(
    collections: List[CollectionState],
    regions: Set[int],
  ): bool =
    collections.indices().forall(i =>
      collectionLiveAt(collections, i).intersect(regions) == Set())

  pure def updateTransactions(
    transactions: List[TxState],
    index: int,
    updatedTx: TxState,
  ): List[TxState] =
    TransactionIndexes.foldl([], (acc, i) =>
      acc.append(if (i == index) { updatedTx } else { transactions.nth(i) }))

  pure def logicalRegionRef(region: int, sequence: int): LogicalRegionRef = {
    region: region,
    sequence: sequence,
  }

  pure def txState(
    phase: TxPhase,
    collectionIndex: int,
    observedGeneration: int,
    cleanupIndex: int,
    allocations: List[int],
    freeIntents: List[LogicalRegionRef],
    rollbackAllocations: List[LogicalRegionRef],
  ): TxState = {
    phase: phase,
    collectionIndex: collectionIndex,
    observedGeneration: observedGeneration,
    cleanupIndex: cleanupIndex,
    allocations: allocations,
    freeIntents: freeIntents,
    rollbackAllocations: rollbackAllocations,
  }

  pure val EmptyTx =
    txState(TxIdle, NoCollection, 0, 0, [], [], [])

  pure val InitialTransactions = [EmptyTx, EmptyTx]

  pure def openTx(collectionIndex: int, observedGeneration: int): TxState =
    txState(TxOpen, collectionIndex, observedGeneration, 0, [], [], [])

  pure def txAddAllocation(tx: TxState, allocation: int): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.allocations.append(allocation),
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txAddFreeIntent(
    tx: TxState,
    freeIntent: LogicalRegionRef,
  ): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.allocations,
      tx.freeIntents.append(freeIntent),
      tx.rollbackAllocations,
    )

  pure def txAddRollbackAllocation(
    tx: TxState,
    rollbackAllocation: LogicalRegionRef,
  ): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations.append(rollbackAllocation),
    )

  pure def txRollbackPreparing(tx: TxState): TxState =
    txState(
      TxRollbackPreparing,
      tx.collectionIndex,
      tx.observedGeneration,
      0,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txCommitted(tx: TxState): TxState =
    txState(
      TxCommittedCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
      0,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txRolledBack(tx: TxState): TxState =
    txState(
      TxRolledBackCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
      0,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txAdvanceCleanup(tx: TxState): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex + 1,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def intListToSet(regions: List[int]): Set[int] =
    regions.foldl(Set(), (acc, region) => acc.union(Set(region)))

  pure def intListEntriesInRegions(regions: List[int]): bool =
    regions.indices().forall(i => regions.nth(i).in(Regions))

  pure def noDuplicates(regions: List[int]): bool =
    regions.indices().forall(i =>
      regions.indices().forall(j =>
        (i == j).or(not(regions.nth(i) == regions.nth(j)))))

  pure def logicalRefRegionSet(refs: List[LogicalRegionRef]): Set[int] =
    refs.foldl(Set(), (acc, entry) => acc.union(Set(entry.region)))

  pure def logicalRefListEntriesInRegions(
    refs: List[LogicalRegionRef],
  ): bool =
    refs.indices().forall(i => refs.nth(i).region.in(Regions))

  pure def logicalRefResolved(ref: LogicalRegionRef): bool =
    0 <= ref.sequence

  pure def logicalRefListSequencesOK(
    refs: List[LogicalRegionRef],
    nextSequence: int,
  ): bool =
    refs.indices().forall(i =>
      logicalRefResolved(refs.nth(i))
      and refs.nth(i).sequence < nextSequence)

  pure def noDuplicateLogicalRefRegions(
    refs: List[LogicalRegionRef],
  ): bool =
    refs.indices().forall(i =>
      refs.indices().forall(j =>
        i == j or not(refs.nth(i).region == refs.nth(j).region)))

  pure def txAllocSet(tx: TxState): Set[int] =
    intListToSet(tx.allocations)

  pure def txFreeIntentSet(tx: TxState): Set[int] =
    logicalRefRegionSet(tx.freeIntents)

  pure def txRollbackAllocSet(tx: TxState): Set[int] =
    logicalRefRegionSet(tx.rollbackAllocations)

  pure def txEntriesOK(tx: TxState, nextSequence: int): bool =
    // Every raw transaction allocation names one modeled region.
    intListEntriesInRegions(tx.allocations)
    // Every retained free/rollback reference names one modeled region.
    and logicalRefListEntriesInRegions(tx.freeIntents)
    and logicalRefListEntriesInRegions(tx.rollbackAllocations)
    // Every retained reference uses an assigned, non-future sequence.
    and logicalRefListSequencesOK(tx.freeIntents, nextSequence)
    and logicalRefListSequencesOK(tx.rollbackAllocations, nextSequence)
    // A transaction may not retain duplicate ownership/cleanup entries.
    and noDuplicates(tx.allocations)
    and noDuplicateLogicalRefRegions(tx.freeIntents)
    and noDuplicateLogicalRefRegions(tx.rollbackAllocations)
    // Rollback records may only describe this transaction's allocations.
    and txRollbackAllocSet(tx).subseteq(txAllocSet(tx))

  pure def txCollectionOK(
    tx: TxState,
    collections: List[CollectionState],
  ): bool =
    // Idle slots have no collection; every non-idle slot names a modeled one.
    if (tx.phase == TxIdle) {
      tx.collectionIndex == NoCollection
    } else {
      tx.collectionIndex.in(collections.indices())
    }

  pure def txGenerationCurrent(
    collections: List[CollectionState],
    tx: TxState,
  ): bool =
    tx.observedGeneration
      == collectionGenerationAt(collections, tx.collectionIndex)

  pure def txIdleStateOK(tx: TxState): bool =
    // An idle slot must not retain provenance or scan state from old work.
    (tx.phase == TxIdle).implies(
      tx.collectionIndex == NoCollection
      and tx.observedGeneration == 0
      and tx.cleanupIndex == 0
      and tx.allocations.length() == 0
      and tx.freeIntents.length() == 0
      and tx.rollbackAllocations.length() == 0)

  pure def txCleanupIndexOK(tx: TxState): bool =
    // The scan cursor is bounded by the list being scanned, and is zero when
    // the phase does not have an active scan.
    if (tx.phase == TxRollbackPreparing) {
      0 <= tx.cleanupIndex
      and tx.cleanupIndex <= tx.allocations.length()
    } else if (tx.phase == TxCommittedCleanup) {
      0 <= tx.cleanupIndex
      and tx.cleanupIndex <= tx.freeIntents.length()
    } else if (tx.phase == TxRolledBackCleanup) {
      0 <= tx.cleanupIndex
      and tx.cleanupIndex <= tx.rollbackAllocations.length()
    } else {
      tx.cleanupIndex == 0
    }

  pure def txRollbackRecordsPhaseOK(tx: TxState): bool =
    // Rollback records exist only after rollback preparation has started.
    (
      tx.phase == TxOpen
      or tx.phase == TxCommittedCleanup
    ).implies(tx.rollbackAllocations.length() == 0)

  pure def logicalRefMatchesHeader(
    headers: List[RegionHeader],
    entry: LogicalRegionRef,
  ): bool =
    entry.region.in(Regions)
    and logicalRefResolved(entry)
    and headers.nth(entry.region).valid
    and headers.nth(entry.region).sequence == entry.sequence

  pure def allLogicalRefsHeaderMatch(
    headers: List[RegionHeader],
    refs: List[LogicalRegionRef],
  ): bool =
    refs.indices().forall(i => logicalRefMatchesHeader(headers, refs.nth(i)))

  pure def allAllocationsHaveRollbackRecords(tx: TxState): bool =
    txAllocSet(tx).subseteq(txRollbackAllocSet(tx))

  pure def logicalCleanupDone(
    activeFree: Set[int],
    headers: List[RegionHeader],
    entry: LogicalRegionRef,
  ): bool =
    entry.region.in(activeFree)
    or not(logicalRefMatchesHeader(headers, entry))

  pure def logicalFreeMayAppend(
    activeFree: Set[int],
    headers: List[RegionHeader],
    entry: LogicalRegionRef,
  ): bool =
    not(entry.region.in(activeFree))
    and logicalRefMatchesHeader(headers, entry)

  pure def pendingLogicalCleanupRegionSet(
    activeFree: Set[int],
    headers: List[RegionHeader],
    refs: List[LogicalRegionRef],
  ): Set[int] =
    refs.foldl(Set(), (acc, entry) =>
      if (logicalCleanupDone(activeFree, headers, entry)) {
        acc
      } else {
        acc.union(Set(entry.region))
      })

  pure def txPendingCommittedCleanupSet(
    activeFree: Set[int],
    headers: List[RegionHeader],
    tx: TxState,
  ): Set[int] =
    if (tx.phase == TxCommittedCleanup) {
      pendingLogicalCleanupRegionSet(activeFree, headers, tx.freeIntents)
    } else {
      Set()
    }

  pure def txOutstandingAllocSet(
    activeFree: Set[int],
    headers: List[RegionHeader],
    tx: TxState,
  ): Set[int] =
    if (tx.phase == TxOpen or tx.phase == TxRollbackPreparing) {
      txAllocSet(tx)
    } else if (tx.phase == TxRolledBackCleanup) {
      pendingLogicalCleanupRegionSet(activeFree, headers, tx.rollbackAllocations)
    } else {
      Set()
    }

  pure def txCurrentRecoverySet(
    activeFree: Set[int],
    headers: List[RegionHeader],
    tx: TxState,
  ): Set[int] =
    txOutstandingAllocSet(activeFree, headers, tx)
      .union(txPendingCommittedCleanupSet(activeFree, headers, tx))

  pure def storageState(
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    regions: List[RegionHeader],
    nextSequence: int,
    collections: List[CollectionState],
    transactions: List[TxState],
  ): StorageState = {
    freeQueue: freeQueue,
    allocationHead: allocationHead,
    readyBoundary: readyBoundary,
    appendTail: appendTail,
    regions: regions,
    nextSequence: nextSequence,
    collections: collections,
    transactions: transactions,
  }

  pure def storageWithTransaction(
    state: StorageState,
    txIndex: int,
    nextTx: TxState,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary,
      state.appendTail,
      state.regions,
      state.nextSequence,
      state.collections,
      updateTransactions(state.transactions, txIndex, nextTx),
    )

  pure def storageWithAllocationHeadAndTransaction(
    state: StorageState,
    allocationHead: int,
    txIndex: int,
    nextTx: TxState,
  ): StorageState =
    storageState(
      state.freeQueue,
      allocationHead,
      state.readyBoundary,
      state.appendTail,
      state.regions,
      state.nextSequence,
      state.collections,
      updateTransactions(state.transactions, txIndex, nextTx),
    )

  pure def storageWithRegionHeaderAndNextSequence(
    state: StorageState,
    region: int,
    header: RegionHeader,
    nextSequence: int,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary,
      state.appendTail,
      replaceRegionHeader(state.regions, region, header),
      nextSequence,
      state.collections,
      state.transactions,
    )

  pure def storageWithCollectionAndTransaction(
    state: StorageState,
    collectionIndex: int,
    nextCollection: CollectionState,
    txIndex: int,
    nextTx: TxState,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary,
      state.appendTail,
      state.regions,
      state.nextSequence,
      replaceCollection(state.collections, collectionIndex, nextCollection),
      updateTransactions(state.transactions, txIndex, nextTx),
    )

  pure def storageWithCollection(
    state: StorageState,
    collectionIndex: int,
    nextCollection: CollectionState,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary,
      state.appendTail,
      state.regions,
      state.nextSequence,
      replaceCollection(state.collections, collectionIndex, nextCollection),
      state.transactions,
    )

  pure def storageWithFreeAppend(
    state: StorageState,
    region: int,
  ): StorageState =
    storageState(
      state.freeQueue.append(region),
      state.allocationHead,
      state.readyBoundary,
      state.appendTail + 1,
      state.regions,
      state.nextSequence,
      state.collections,
      state.transactions,
    )

  pure def storageWithDirectLiveAllocationAndCollection(
    state: StorageState,
    region: int,
    collectionIndex: int,
    nextCollection: CollectionState,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead + 1,
      state.readyBoundary,
      state.appendTail,
      replaceRegionHeader(
        state.regions,
        region,
        validHeader(state.nextSequence),
      ),
      state.nextSequence + 1,
      replaceCollection(state.collections, collectionIndex, nextCollection),
      state.transactions,
    )

  pure def storageWithReadyBoundaryAndInvalidHeader(
    state: StorageState,
    region: int,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary + 1,
      state.appendTail,
      replaceRegionHeader(
        state.regions,
        region,
        invalidHeader(state.regions.nth(region).sequence),
      ),
      state.nextSequence,
      state.collections,
      state.transactions,
    )

  pure val InitialCollections = [
    collectionState(Set(0), 0),
    collectionState(Set(1), 0),
  ]
  pure val InitialFreeQueue = [2, 3, 4, 5]
  pure val InitialRegions = [
    validHeader(0),
    validHeader(1),
    invalidHeader(0),
    invalidHeader(0),
    invalidHeader(0),
    invalidHeader(0),
  ]
  pure val InitialNextSequence = 2
  pure val InitialStorage =
    storageState(
      InitialFreeQueue,
      0,
      InitialFreeQueue.length(),
      InitialFreeQueue.length(),
      InitialRegions,
      InitialNextSequence,
      InitialCollections,
      InitialTransactions,
    )

  // Replayed implementation state. This is the state foreground apply and WAL
  // replay reconstruct: allocator cursors, headers, committed collection
  // state, and retained transaction records.
  var storage: StorageState

  val activeFreeList =
    storage.freeQueue.slice(storage.allocationHead, storage.appendTail)

  val readyFreeList =
    storage.freeQueue.slice(storage.allocationHead, storage.readyBoundary)

  val activeFreeSet =
    intListToSet(activeFreeList)

  val readyFreeSet =
    intListToSet(readyFreeList)

  action init = all {
    storage' = InitialStorage,
  }

  // main_wal begin_transaction(tx, collection, observed_generation)
  action BeginTransaction = {
    nondet txIndex = storage.transactions.indices().oneOf()
    nondet collectionIndex = storage.collections.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx =
      openTx(
        collectionIndex,
        collectionGenerationAt(storage.collections, collectionIndex),
      )
    all {
      currentTx.phase == TxIdle,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // transaction_log allocate_region(tx, region, allocation_head_after)
  action AllocateInTransaction = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val region = storage.freeQueue.nth(storage.allocationHead)
    val nextTx = txAddAllocation(currentTx, region)
    all {
      currentTx.phase == TxOpen,
      storage.allocationHead < storage.readyBoundary,

      storage' =
        storageWithAllocationHeadAndTransaction(
          storage,
          storage.allocationHead + 1,
          txIndex,
          nextTx,
        ),
    }
  }

  // Collection write of the header for one transaction-owned allocation.
  // Durable rollback needs explicit allocation sequence references; commit
  // can publish live ownership without allocation headers.
  action WriteTransactionAllocationHeader = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet allocationIndex = currentTx.allocations.indices().oneOf()
    val region = currentTx.allocations.nth(allocationIndex)
    all {
      currentTx.phase == TxOpen or currentTx.phase == TxRollbackPreparing,
      not(storage.regions.nth(region).valid),

      storage' =
        storageWithRegionHeaderAndNextSequence(
          storage,
          region,
          validHeader(storage.nextSequence),
          storage.nextSequence + 1,
        ),
    }
  }

  // transaction_log free_intent(tx, region, sequence). If the live region does
  // not already have a valid header, staging writes one before recording the
  // retained sequence reference.
  action StageFreeIntent = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet region =
      collectionLiveAt(storage.collections, currentTx.collectionIndex)
        .exclude(txFreeIntentSet(currentTx))
        .oneOf()
    val existingHeader = storage.regions.nth(region)
    val nextStorageWithHeader =
      if (existingHeader.valid) {
        storage
      } else {
        storageWithRegionHeaderAndNextSequence(
          storage,
          region,
          validHeader(storage.nextSequence),
          storage.nextSequence + 1,
        )
      }
    val freeIntent =
      logicalRegionRef(region, nextStorageWithHeader.regions.nth(region).sequence)
    val nextTx = txAddFreeIntent(currentTx, freeIntent)
    all {
      currentTx.phase == TxOpen,

      storage' =
        storageWithTransaction(
          nextStorageWithHeader,
          txIndex,
          nextTx,
        ),
    }
  }

  // main_wal commit_transaction(tx)
  action CommitTransaction = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val allocs = txAllocSet(currentTx)
    val freeIntents = txFreeIntentSet(currentTx)
    val currentLive =
      collectionLiveAt(storage.collections, currentTx.collectionIndex)
    val nextLive =
      currentLive.exclude(freeIntents).union(allocs)
    val nextTx = txCommitted(currentTx)
    val nextGeneration =
      collectionGenerationAt(
        storage.collections,
        currentTx.collectionIndex,
      ) + 1
    val nextCollection =
      collectionState(nextLive, nextGeneration)
    all {
      currentTx.phase == TxOpen,
      currentTx.observedGeneration
        == collectionGenerationAt(
          storage.collections,
          currentTx.collectionIndex,
        ),

      storage' =
        storageWithCollectionAndTransaction(
          storage,
          currentTx.collectionIndex,
          nextCollection,
          txIndex,
          nextTx,
        ),
    }
  }

  // main_wal rollback_decided(tx). This is not the durable rollback marker; it
  // opens the safe window where private allocation headers can be forced.
  action StartRollbackPreparation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRollbackPreparing(currentTx)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // transaction_log rollback_allocation(tx, region, sequence). These records
  // are written while allocations are still private, before the durable main
  // WAL rollback marker makes rollback cleanup recoverable.
  action RecordRollbackAllocation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val region = currentTx.allocations.nth(currentTx.cleanupIndex)
    val rollbackAllocation =
      logicalRegionRef(region, storage.regions.nth(region).sequence)
    val nextTx =
      txAdvanceCleanup(txAddRollbackAllocation(currentTx, rollbackAllocation))
    all {
      currentTx.phase == TxRollbackPreparing,
      currentTx.cleanupIndex < currentTx.allocations.length(),
      storage.regions.nth(region).valid,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // main_wal rollback_transaction(tx), after rollback cleanup is still pending.
  action RollbackTransaction = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRolledBack(currentTx)
    all {
      currentTx.phase == TxRollbackPreparing,
      currentTx.cleanupIndex == currentTx.allocations.length(),

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // main_wal free_region(region, append_tail_after) for a committed free intent.
  action FreeCommittedIntent = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val freeIntent = currentTx.freeIntents.nth(currentTx.cleanupIndex)
    val nextTx = txAdvanceCleanup(currentTx)
    val shouldAppend =
      logicalFreeMayAppend(activeFreeSet, storage.regions, freeIntent)
    all {
      currentTx.phase == TxCommittedCleanup,
      currentTx.cleanupIndex < currentTx.freeIntents.length(),

      storage' =
        if (shouldAppend) {
          storageWithTransaction(
            storageWithFreeAppend(storage, freeIntent.region),
            txIndex,
            nextTx,
          )
        } else {
          storageWithTransaction(storage, txIndex, nextTx)
        },
    }
  }

  // main_wal free_region(region, append_tail_after) for a rolled-back
  // transaction allocation.
  action FreeRolledBackAllocation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val rollbackAllocation =
      currentTx.rollbackAllocations.nth(currentTx.cleanupIndex)
    val nextTx = txAdvanceCleanup(currentTx)
    val shouldAppend =
      logicalFreeMayAppend(activeFreeSet, storage.regions, rollbackAllocation)
    all {
      currentTx.phase == TxRolledBackCleanup,
      currentTx.cleanupIndex < currentTx.rollbackAllocations.length(),

      storage' =
        if (shouldAppend) {
          storageWithTransaction(
            storageWithFreeAppend(storage, rollbackAllocation.region),
            txIndex,
            nextTx,
          )
        } else {
          storageWithTransaction(storage, txIndex, nextTx)
        },
    }
  }

  // main_wal transaction_finished(tx) after committed-free cleanup. This is
  // one of the only actions that clears retained transaction state.
  action FinishCommit = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      currentTx.phase == TxCommittedCleanup,
      currentTx.cleanupIndex == currentTx.freeIntents.length(),

      storage' = storageWithTransaction(storage, txIndex, EmptyTx),
    }
  }

  // main_wal transaction_finished(tx) after rollback cleanup. This is one of
  // the only actions that clears retained transaction state.
  action FinishRollback = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,
      currentTx.cleanupIndex == currentTx.rollbackAllocations.length(),

      storage' = storageWithTransaction(storage, txIndex, EmptyTx),
    }
  }

  // main_wal allocate_region(region, allocation_head_after) for a direct
  // committed allocation outside a transaction.
  action AllocateDirectlyLive = {
    nondet collectionIndex = storage.collections.indices().oneOf()
    val region = storage.freeQueue.nth(storage.allocationHead)
    val nextGeneration =
      collectionGenerationAt(storage.collections, collectionIndex) + 1
    val nextLive =
      collectionLiveAt(storage.collections, collectionIndex).union(Set(region))
    val nextCollection =
      collectionState(nextLive, nextGeneration)
    all {
      storage.allocationHead < storage.readyBoundary,

      storage' =
        storageWithDirectLiveAllocationAndCollection(
          storage,
          region,
          collectionIndex,
          nextCollection,
        ),
    }
  }

  // main_wal free_region(region, sequence, append_tail_after) for a direct
  // committed free outside a transaction. Direct frees are logically
  // authorized too: if the live physical region does not already have a valid
  // header, the free first assigns a sequence and then publishes exactly one
  // physical free entry for that logical region.
  action FreeDirectLive = {
    nondet collectionIndex = storage.collections.indices().oneOf()
    nondet region =
      collectionLiveAt(storage.collections, collectionIndex).oneOf()
    val existingHeader = storage.regions.nth(region)
    val nextStorageWithHeader =
      if (existingHeader.valid) {
        storage
      } else {
        storageWithRegionHeaderAndNextSequence(
          storage,
          region,
          validHeader(storage.nextSequence),
          storage.nextSequence + 1,
        )
      }
    val logicalFree =
      logicalRegionRef(region, nextStorageWithHeader.regions.nth(region).sequence)
    val shouldAppend =
      logicalFreeMayAppend(
        activeFreeSet,
        nextStorageWithHeader.regions,
        logicalFree,
      )
    val nextGeneration =
      collectionGenerationAt(storage.collections, collectionIndex) + 1
    val nextLive =
      collectionLiveAt(storage.collections, collectionIndex).exclude(Set(region))
    val nextCollection =
      collectionState(nextLive, nextGeneration)
    all {
      storage' =
        if (shouldAppend) {
          storageWithCollection(
            storageWithFreeAppend(nextStorageWithHeader, logicalFree.region),
            collectionIndex,
            nextCollection,
          )
        } else {
          storageWithCollection(
            nextStorageWithHeader,
            collectionIndex,
            nextCollection,
          )
        },
    }
  }

  // main_wal erase_free_region_span(1, ready_boundary_after)
  action EraseOneDirty = {
    val region = storage.freeQueue.nth(storage.readyBoundary)
    all {
      storage.readyBoundary < storage.appendTail,

      storage' =
        storageWithReadyBoundaryAndInvalidHeader(storage, region),
    }
  }

  // Unsafe comparison: a transaction-private free intent mutates the global
  // allocator before the transaction commits.
  action UnsafeFreeIntentBeforeCommit = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet freeIntentIndex = currentTx.freeIntents.indices().oneOf()
    val freeIntent = currentTx.freeIntents.nth(freeIntentIndex)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithFreeAppend(storage, freeIntent.region),
    }
  }

  // Unsafe comparison: committed cleanup appends a free record without
  // checking whether the region is already in the active free-space range or
  // still has the expected sequence in its valid header.
  action UnsafeCommittedCleanupWithoutMembershipCheck = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex = currentTx.freeIntents.indices().oneOf()
    val obligation = currentTx.freeIntents.nth(obligationIndex)
    all {
      currentTx.phase == TxCommittedCleanup,

      storage' = storageWithFreeAppend(storage, obligation.region),
    }
  }

  // Unsafe comparison: rollback cleanup appends a free record without checking
  // whether the region is already active-free or still has the expected
  // rollback-allocation sequence.
  action UnsafeRollbackCleanupWithoutMembershipCheck = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex =
      currentTx.rollbackAllocations.indices().oneOf()
    val obligation =
      currentTx.rollbackAllocations.nth(obligationIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,

      storage' = storageWithFreeAppend(storage, obligation.region),
    }
  }

  // Unsafe comparison: a stale transaction commits without the collection
  // generation conflict check.
  action UnsafeCommitWithoutGenerationCheck = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val allocs = txAllocSet(currentTx)
    val freeIntents = txFreeIntentSet(currentTx)
    val currentLive =
      collectionLiveAt(storage.collections, currentTx.collectionIndex)
    val nextLive =
      currentLive.exclude(freeIntents).union(allocs)
    val nextTx = txCommitted(currentTx)
    val nextGeneration =
      collectionGenerationAt(
        storage.collections,
        currentTx.collectionIndex,
      ) + 1
    val nextCollection =
      collectionState(nextLive, nextGeneration)
    all {
      currentTx.phase == TxOpen,

      storage' =
        storageWithCollectionAndTransaction(
          storage,
          currentTx.collectionIndex,
          nextCollection,
          txIndex,
          nextTx,
        ),
    }
  }

  action Stutter = all {
    storage' = storage,
  }

  action step = any {
    BeginTransaction,
    AllocateInTransaction,
    WriteTransactionAllocationHeader,
    StageFreeIntent,
    CommitTransaction,
    StartRollbackPreparation,
    RecordRollbackAllocation,
    RollbackTransaction,
    FreeCommittedIntent,
    FreeRolledBackAllocation,
    FinishCommit,
    FinishRollback,
    AllocateDirectlyLive,
    FreeDirectLive,
    EraseOneDirty,
    Stutter,
  }

  action unsafeStep = any {
    step,
    UnsafeFreeIntentBeforeCommit,
    UnsafeCommittedCleanupWithoutMembershipCheck,
    UnsafeRollbackCleanupWithoutMembershipCheck,
    UnsafeCommitWithoutGenerationCheck,
  }

  // Structural well-formedness for the bounded model state. These checks are
  // not the allocator safety argument by themselves; they make sure every list,
  // cursor, reference, and retained transaction record stays inside the finite
  // domain the rest of the invariants reason about.
  val typeOK =
    // The allocator queue only contains modeled region identifiers.
    storage.freeQueue.indices().forall(i => storage.freeQueue.nth(i).in(Regions))
    // The model has exactly two collection slots; storage.collections.indices()
    // is the source of truth for valid collection indexes.
    and storage.collections.length() == 2
    // Collection live sets cannot mention regions outside the modeled domain.
    and storage.collections.indices().forall(i =>
      collectionLiveAt(storage.collections, i).subseteq(Regions))
    // Collection generations are monotone counters in the modeled nonnegative
    // range.
    and storage.collections.indices().forall(i =>
      0 <= collectionGenerationAt(storage.collections, i))
    // The model always has exactly one header slot per modeled region.
    and storage.regions.length() == 6
    // Header sequence numbers never go negative.
    and storage.regions.indices().forall(i =>
      0 <= storage.regions.nth(i).sequence)
    // Existing headers never use a sequence that has not been allocated yet.
    and storage.regions.indices().forall(i =>
      storage.regions.nth(i).sequence < storage.nextSequence)
    // The model has exactly two transaction slots; storage.transactions.indices()
    // is the source of truth for valid transaction indexes.
    and storage.transactions.length() == 2
    // The next sequence allocator stays in the modeled nonnegative range.
    and 0 <= storage.nextSequence
    // Transaction-owned lists and retained refs are well formed and duplicate-free.
    and storage.transactions.indices().forall(i =>
      txEntriesOK(storage.transactions.nth(i), storage.nextSequence))
    // Each transaction phase has the expected collection binding shape.
    and storage.transactions.indices().forall(i =>
      txCollectionOK(storage.transactions.nth(i), storage.collections))
    // Idle transaction slots are fully empty, not just marked idle.
    and storage.transactions.indices().forall(i =>
      txIdleStateOK(storage.transactions.nth(i)))
    // The cleanup/rollback scan cursor is valid for the current phase.
    and storage.transactions.indices().forall(i =>
      txCleanupIndexOK(storage.transactions.nth(i)))
    // Rollback records cannot appear before a transaction starts rollback.
    and storage.transactions.indices().forall(i =>
      txRollbackRecordsPhaseOK(storage.transactions.nth(i)))

  // Allocator cursors maintain the ready/dirty/active queue partition.
  val cursorOrderOK =
    0 <= storage.allocationHead
    and storage.allocationHead <= storage.readyBoundary
    and storage.readyBoundary <= storage.appendTail
    and storage.appendTail <= storage.freeQueue.length()

  // Committed collection ownership is exclusive across collections. This is a
  // physical-region ownership invariant: a collection live set contains region
  // indexes, not logical refs.
  val collectionOwnershipUnique =
    storage.collections.indices().forall(i =>
      storage.collections.indices().forall(j =>
        i == j
        or collectionLiveAt(storage.collections, i).intersect(
          collectionLiveAt(storage.collections, j)
        ) == Set()))

  // Core invariant: no physical region can be both committed-live and in the
  // active free-space range.
  val noLiveFreeOverlap =
    collectionLivesDisjointFrom(storage.collections, activeFreeSet)

  // Ready free regions have been erased, so their old sequence headers are invalid.
  val readyFreeHeadersErased =
    Regions.forall(region =>
      region.in(readyFreeSet).implies(not(storage.regions.nth(region).valid)))

  // Core invariant: a transaction allocation cannot become collection-live
  // before commit, and rollback cleanup obligations cannot be live after the
  // rollback marker. Raw allocations are physical until rollback preparation
  // records logical cleanup refs for them.
  val noPrematurePublication =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxOpen
        or storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        collectionLivesDisjointFrom(
          storage.collections,
          txAllocSet(storage.transactions.nth(i)),
        )
        and txAllocSet(
          storage.transactions.nth(i)
        ).intersect(activeFreeSet) == Set()))
    and
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        collectionLivesDisjointFrom(
          storage.collections,
          txOutstandingAllocSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          ),
        )))

  // Current recovery ownership is exclusive across transaction slots. Retained
  // cleanup evidence that is already active-free is not part of this set; it is
  // completed evidence kept until transaction_finished.
  val txCurrentRecoverySetsDoNotOverlap =
    storage.transactions.indices().forall(i =>
      storage.transactions.indices().forall(j =>
        i == j or
          txCurrentRecoverySet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          ).intersect(
            txCurrentRecoverySet(
              activeFreeSet,
              storage.regions,
              storage.transactions.nth(j),
            )
          ) == Set()))

  // Core invariant: cleanup refs are phase-safe. Before the terminal marker
  // that authorizes cleanup, current free-intent refs stay out of active free
  // space and rollback-allocation refs still name private allocations. Stale
  // open transactions may retain obsolete free intents, but their generation
  // mismatch blocks commit before those refs can authorize cleanup.
  val cleanupRefsPhaseSafe =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxOpen
        or storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txGenerationCurrent(
          storage.collections,
          storage.transactions.nth(i),
        ).implies(
          pendingLogicalCleanupRegionSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i).freeIntents,
          ).subseteq(
            collectionLiveAt(
              storage.collections,
              storage.transactions.nth(i).collectionIndex,
            )
          )
          and txFreeIntentSet(
            storage.transactions.nth(i)
          ).intersect(activeFreeSet) == Set()
        )))
    and
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txRollbackAllocSet(
          storage.transactions.nth(i)
        ).intersect(activeFreeSet) == Set()
      ))
    // After commit, still-pending free intents are detached from live state
    // until cleanup publishes exactly one free or the logical ref is obsolete.
    and
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxCommittedCleanup
      ).implies(
        collectionLivesDisjointFrom(
          storage.collections,
          txPendingCommittedCleanupSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          ),
        )))
    // After rollback, every allocation has a logical cleanup ref and still-
    // pending rollback obligations are detached from all collection live sets.
    and
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        allAllocationsHaveRollbackRecords(storage.transactions.nth(i))
        and
        collectionLivesDisjointFrom(
          storage.collections,
          txOutstandingAllocSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          ),
        )
      ))

  // Core invariant: no double free. Active free space is a physical-region set
  // represented as a list slice, so duplicate entries would publish the same
  // physical free twice. Cleanup actions also use logicalFreeMayAppend, which
  // refuses to append when the physical region is already active-free.
  val noDoubleFree =
    noDuplicates(activeFreeList)
    and storage.transactions.indices().forall(i =>
      txCurrentRecoverySet(
        activeFreeSet,
        storage.regions,
        storage.transactions.nth(i),
      ).intersect(activeFreeSet) == Set())
    and txCurrentRecoverySetsDoNotOverlap

  // Core invariant: retained logical cleanup refs are well formed, and a ref
  // can be pending cleanup only while it still matches the current valid
  // header. If the physical region is already active-free, the retained ref is
  // completed evidence; if the header changed, the ref is obsolete evidence.
  val logicalCleanupRefsValid =
    storage.transactions.indices().forall(i =>
      txEntriesOK(storage.transactions.nth(i), storage.nextSequence))
    and storage.transactions.indices().forall(i =>
      storage.transactions.nth(i).freeIntents.indices().forall(j =>
        storage.transactions.nth(i).freeIntents.nth(j).region.in(
          txPendingCommittedCleanupSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          )
        ).implies(
          logicalRefMatchesHeader(
            storage.regions,
            storage.transactions.nth(i).freeIntents.nth(j),
          )
        )))
    and storage.transactions.indices().forall(i =>
      storage.transactions.nth(i).rollbackAllocations.indices().forall(j =>
        storage.transactions.nth(i).rollbackAllocations.nth(j).region.in(
          txOutstandingAllocSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          )
        ).implies(
          logicalRefMatchesHeader(
            storage.regions,
            storage.transactions.nth(i).rollbackAllocations.nth(j),
          )
        )))

  // Core invariant: no modeled physical region leaks. Each region is reachable
  // from live state, active free space, or unfinished transaction/recovery
  // state. Completed retained cleanup evidence is not needed for reachability
  // once the physical region is active-free.
  val noLeaks =
    Regions.forall(region =>
      // 1. committed live in some collection
      storage.collections.indices().exists(collectionIndex =>
        region.in(collectionLiveAt(storage.collections, collectionIndex)))
      // 2. allocator-free
      or region.in(activeFreeSet)
      // 3. transaction-private allocation or unfinished cleanup/recovery
      or storage.transactions.indices().exists(txIndex =>
        region.in(
          txCurrentRecoverySet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(txIndex),
          )
        )))

  // Negative reachability helper, not part of safety. Running this as an
  // invariant should fail once two different collection transactions can commit
  // concurrently.
  val twoDifferentCollectionsCommitted =
    storage.transactions.nth(0).phase == TxCommittedCleanup
    and storage.transactions.nth(1).phase == TxCommittedCleanup
    and not(
      storage.transactions.nth(0).collectionIndex
        == storage.transactions.nth(1).collectionIndex)

  val noTwoDifferentCollectionsCommitted =
    not(twoDifferentCollectionsCommitted)

  // Negative reachability helper, not part of safety. Running this as an
  // invariant should fail once two same-collection transactions can stage the
  // same private free intent without relying on global transaction knowledge.
  val twoSameCollectionOpenTransactionsStagedSameFreeIntent =
    storage.transactions.nth(0).phase == TxOpen
    and storage.transactions.nth(1).phase == TxOpen
    and storage.transactions.nth(0).collectionIndex
      == storage.transactions.nth(1).collectionIndex
    and not(
      txFreeIntentSet(storage.transactions.nth(0)).intersect(
        txFreeIntentSet(storage.transactions.nth(1))
      ) == Set())

  val noTwoSameCollectionOpenTransactionsStagedSameFreeIntent =
    not(twoSameCollectionOpenTransactionsStagedSameFreeIntent)

  val safety =
    typeOK
    and cursorOrderOK
    and collectionOwnershipUnique
    and noLiveFreeOverlap
    and readyFreeHeadersErased
    and noPrematurePublication
    and cleanupRefsPhaseSafe
    and noDoubleFree
    and logicalCleanupRefsValid
    and noLeaks
}
