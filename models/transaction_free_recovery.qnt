// Semantic model for allocator WAL commands, transaction-owned allocations,
// transaction-private free intents, and transaction cleanup.
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

  type RegionRef = {
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
    allocations: List[int],
    freeIntents: List[RegionRef],
    rollbackAllocations: List[RegionRef],
  }

  type StorageState = {
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    regions: List[RegionHeader],
    nextSequence: int,
    collectionGenerations: List[int],
    transactions: List[TxState],
  }

  pure val NoCollection = -1
  pure val RegionIndexes = [0, 1, 2, 3, 4, 5]
  pure val TransactionIndexes = [0, 1]
  pure val CollectionIndexes = [0, 1]

  pure val Regions = Set(0, 1, 2, 3, 4, 5)
  pure val TransactionIndexSet = Set(0, 1)
  pure val CollectionIndexSet = Set(0, 1)

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

  pure def replaceCollectionGeneration(
    collectionGenerations: List[int],
    collectionIndex: int,
    generation: int,
  ): List[int] =
    CollectionIndexes.foldl([], (acc, i) =>
      acc.append(
        if (i == collectionIndex) {
          generation
        } else {
          collectionGenerations.nth(i)
        }))

  pure def collectionGenerationAt(
    collectionGenerations: List[int],
    collectionIndex: int,
  ): int =
    if (collectionIndex == 0) {
      collectionGenerations.nth(0)
    } else if (collectionIndex == 1) {
      collectionGenerations.nth(1)
    } else {
      -1
    }

  pure def updateTransactions(
    transactions: List[TxState],
    index: int,
    updatedTx: TxState,
  ): List[TxState] =
    TransactionIndexes.foldl([], (acc, i) =>
      acc.append(if (i == index) { updatedTx } else { transactions.nth(i) }))

  pure def regionRef(region: int, sequence: int): RegionRef = {
    region: region,
    sequence: sequence,
  }

  pure def txState(
    phase: TxPhase,
    collectionIndex: int,
    observedGeneration: int,
    allocations: List[int],
    freeIntents: List[RegionRef],
    rollbackAllocations: List[RegionRef],
  ): TxState = {
    phase: phase,
    collectionIndex: collectionIndex,
    observedGeneration: observedGeneration,
    allocations: allocations,
    freeIntents: freeIntents,
    rollbackAllocations: rollbackAllocations,
  }

  pure val EmptyTx =
    txState(TxIdle, NoCollection, 0, [], [], [])

  pure val InitialTransactions = [EmptyTx, EmptyTx]

  pure def openTx(collectionIndex: int, observedGeneration: int): TxState =
    txState(TxOpen, collectionIndex, observedGeneration, [], [], [])

  pure def txAddAllocation(tx: TxState, allocation: int): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.allocations.append(allocation),
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txAddFreeIntent(tx: TxState, freeIntent: RegionRef): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.allocations,
      tx.freeIntents.append(freeIntent),
      tx.rollbackAllocations,
    )

  pure def txAddRollbackAllocation(
    tx: TxState,
    rollbackAllocation: RegionRef,
  ): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations.append(rollbackAllocation),
    )

  pure def txRollbackPreparing(tx: TxState): TxState =
    txState(
      TxRollbackPreparing,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txCommitted(tx: TxState): TxState =
    txState(
      TxCommittedCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txRolledBack(tx: TxState): TxState =
    txState(
      TxRolledBackCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
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

  pure def refRegionSet(refs: List[RegionRef]): Set[int] =
    refs.foldl(Set(), (acc, entry) => acc.union(Set(entry.region)))

  pure def refListEntriesInRegions(refs: List[RegionRef]): bool =
    refs.indices().forall(i => refs.nth(i).region.in(Regions))

  pure def refListSequencesOK(refs: List[RegionRef], nextSequence: int): bool =
    refs.indices().forall(i =>
      0 <= refs.nth(i).sequence and refs.nth(i).sequence < nextSequence)

  pure def noDuplicateRefRegions(refs: List[RegionRef]): bool =
    refs.indices().forall(i =>
      refs.indices().forall(j =>
        i == j or not(refs.nth(i).region == refs.nth(j).region)))

  pure def txAllocSet(tx: TxState): Set[int] =
    intListToSet(tx.allocations)

  pure def txFreeIntentSet(tx: TxState): Set[int] =
    refRegionSet(tx.freeIntents)

  pure def txRollbackAllocSet(tx: TxState): Set[int] =
    refRegionSet(tx.rollbackAllocations)

  pure def txEntriesOK(tx: TxState, nextSequence: int): bool =
    intListEntriesInRegions(tx.allocations)
    and refListEntriesInRegions(tx.freeIntents)
    and refListEntriesInRegions(tx.rollbackAllocations)
    and refListSequencesOK(tx.freeIntents, nextSequence)
    and refListSequencesOK(tx.rollbackAllocations, nextSequence)
    and noDuplicates(tx.allocations)
    and noDuplicateRefRegions(tx.freeIntents)
    and noDuplicateRefRegions(tx.rollbackAllocations)
    and txRollbackAllocSet(tx).subseteq(txAllocSet(tx))

  pure def txCollectionOK(tx: TxState): bool =
    if (tx.phase == TxIdle) {
      tx.collectionIndex == NoCollection
    } else {
      tx.collectionIndex.in(CollectionIndexSet)
    }

  pure def txIdleStateOK(tx: TxState): bool =
    (tx.phase == TxIdle).implies(
      tx.collectionIndex == NoCollection
      and tx.observedGeneration == 0
      and tx.allocations.length() == 0
      and tx.freeIntents.length() == 0
      and tx.rollbackAllocations.length() == 0)

  pure def txRollbackRecordsPhaseOK(tx: TxState): bool =
    (
      tx.phase == TxOpen
      or tx.phase == TxCommittedCleanup
    ).implies(tx.rollbackAllocations.length() == 0)

  pure def headerMatches(headers: List[RegionHeader], entry: RegionRef): bool =
    entry.region.in(Regions)
    and headers.nth(entry.region).valid
    and headers.nth(entry.region).sequence == entry.sequence

  pure def allRefsHeaderMatch(
    headers: List[RegionHeader],
    refs: List[RegionRef],
  ): bool =
    refs.indices().forall(i => headerMatches(headers, refs.nth(i)))

  pure def allocatedRegionsHaveHeaders(
    headers: List[RegionHeader],
    allocations: List[int],
  ): bool =
    allocations.indices().forall(i => headers.nth(allocations.nth(i)).valid)

  pure def allAllocationsHaveRollbackRecords(tx: TxState): bool =
    txAllocSet(tx).subseteq(txRollbackAllocSet(tx))

  pure def cleanupDone(
    activeFree: Set[int],
    headers: List[RegionHeader],
    entry: RegionRef,
  ): bool =
    entry.region.in(activeFree)
    or not(headerMatches(headers, entry))

  pure def allCleanupDone(
    activeFree: Set[int],
    headers: List[RegionHeader],
    refs: List[RegionRef],
  ): bool =
    refs.indices().forall(i => cleanupDone(activeFree, headers, refs.nth(i)))

  pure def pendingCleanupRegionSet(
    activeFree: Set[int],
    headers: List[RegionHeader],
    refs: List[RegionRef],
  ): Set[int] =
    refs.foldl(Set(), (acc, entry) =>
      if (cleanupDone(activeFree, headers, entry)) {
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
      pendingCleanupRegionSet(activeFree, headers, tx.freeIntents)
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
      pendingCleanupRegionSet(activeFree, headers, tx.rollbackAllocations)
    } else {
      Set()
    }

  pure def storageState(
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    regions: List[RegionHeader],
    nextSequence: int,
    collectionGenerations: List[int],
    transactions: List[TxState],
  ): StorageState = {
    freeQueue: freeQueue,
    allocationHead: allocationHead,
    readyBoundary: readyBoundary,
    appendTail: appendTail,
    regions: regions,
    nextSequence: nextSequence,
    collectionGenerations: collectionGenerations,
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
      state.collectionGenerations,
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
      state.collectionGenerations,
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
      state.collectionGenerations,
      state.transactions,
    )

  pure def storageWithCollectionGenerationAndTransaction(
    state: StorageState,
    collectionIndex: int,
    generation: int,
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
      replaceCollectionGeneration(
        state.collectionGenerations,
        collectionIndex,
        generation,
      ),
      updateTransactions(state.transactions, txIndex, nextTx),
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
      state.collectionGenerations,
      state.transactions,
    )

  pure def storageWithDirectLiveAllocation(
    state: StorageState,
    region: int,
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
      state.collectionGenerations,
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
      state.collectionGenerations,
      state.transactions,
    )

  pure val InitialLive = Set(0, 1)
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
  pure val InitialCollectionGenerations = [0, 0]
  pure val InitialStorage =
    storageState(
      InitialFreeQueue,
      0,
      InitialFreeQueue.length(),
      InitialFreeQueue.length(),
      InitialRegions,
      InitialNextSequence,
      InitialCollectionGenerations,
      InitialTransactions,
    )

  // Replayed implementation state. This is the state foreground apply and WAL
  // replay reconstruct: allocator cursors, headers, collection generations,
  // and retained transaction records.
  var storage: StorageState

  // Semantic/oracle state for all committed collection contents modeled here.
  // Per-collection live membership is intentionally abstracted away in this
  // step; collection-specific conflict behavior is modeled by
  // storage.collectionGenerations.
  var live: Set[int]

  val activeFreeList =
    storage.freeQueue.slice(storage.allocationHead, storage.appendTail)

  val readyFreeList =
    storage.freeQueue.slice(storage.allocationHead, storage.readyBoundary)

  val dirtyFreeList =
    storage.freeQueue.slice(storage.readyBoundary, storage.appendTail)

  val activeFreeSet =
    intListToSet(activeFreeList)

  val readyFreeSet =
    intListToSet(readyFreeList)

  val dirtyFreeSet =
    intListToSet(dirtyFreeList)

  val allTxFreeIntentSet =
    storage.transactions.foldl(Set(), (acc, tx) =>
      acc.union(txFreeIntentSet(tx)))

  val pendingCommittedCleanupSet =
    storage.transactions.foldl(Set(), (acc, tx) =>
      acc.union(
        txPendingCommittedCleanupSet(activeFreeSet, storage.regions, tx)))

  val retainedTransactionAllocSet =
    storage.transactions.foldl(Set(), (acc, tx) =>
      if (tx.phase == TxIdle) {
        acc
      } else {
        acc.union(txAllocSet(tx))
      })

  val retainedTransactionRegionSet =
    retainedTransactionAllocSet.union(pendingCommittedCleanupSet)

  val outstandingAllocSet =
    storage.transactions.foldl(Set(), (acc, tx) =>
      acc.union(txOutstandingAllocSet(activeFreeSet, storage.regions, tx)))

  val accountedRegions =
    live
      .union(activeFreeSet)
      .union(retainedTransactionRegionSet)

  action init = all {
    storage' = InitialStorage,
    live' = InitialLive,
  }

  // main_wal begin_transaction(tx, collection, observed_generation)
  action BeginTransaction = {
    nondet txIndex = TransactionIndexSet.oneOf()
    nondet collectionIndex = CollectionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx =
      openTx(
        collectionIndex,
        collectionGenerationAt(storage.collectionGenerations, collectionIndex),
      )
    all {
      currentTx.phase == TxIdle,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
      live' = live,
    }
  }

  // transaction_log allocate_region(tx, region, allocation_head_after)
  action AllocateInTransaction = {
    nondet txIndex = TransactionIndexSet.oneOf()
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
      live' = live,
    }
  }

  // collection write of the header for one transaction-owned allocation.
  // Commit and durable rollback both require these headers to be present.
  action WriteTransactionAllocationHeader = {
    nondet txIndex = TransactionIndexSet.oneOf()
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
      live' = live,
    }
  }

  // transaction_log free_intent(tx, region, sequence)
  //
  // The model keeps one global live set for allocator ownership. The selected
  // transaction still carries a collection index, so generation conflict checks
  // are collection-specific even though wrong-collection free selection is out
  // of scope for this model step.
  action StageFreeIntent = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet region =
      live
        .exclude(allTxFreeIntentSet)
        .exclude(retainedTransactionAllocSet)
        .oneOf()
    val freeIntent =
      regionRef(region, storage.regions.nth(region).sequence)
    val nextTx = txAddFreeIntent(currentTx, freeIntent)
    all {
      currentTx.phase == TxOpen,
      currentTx.observedGeneration
        == collectionGenerationAt(
          storage.collectionGenerations,
          currentTx.collectionIndex,
        ),
      storage.regions.nth(region).valid,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
      live' = live,
    }
  }

  // main_wal commit_transaction(tx)
  action CommitTransaction = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val allocs = txAllocSet(currentTx)
    val freeIntents = txFreeIntentSet(currentTx)
    val nextTx = txCommitted(currentTx)
    val nextGeneration =
      collectionGenerationAt(
        storage.collectionGenerations,
        currentTx.collectionIndex,
      ) + 1
    all {
      currentTx.phase == TxOpen,
      currentTx.observedGeneration
        == collectionGenerationAt(
          storage.collectionGenerations,
          currentTx.collectionIndex,
        ),
      freeIntents.subseteq(live),
      allocatedRegionsHaveHeaders(storage.regions, currentTx.allocations),
      allRefsHeaderMatch(storage.regions, currentTx.freeIntents),

      storage' =
        storageWithCollectionGenerationAndTransaction(
          storage,
          currentTx.collectionIndex,
          nextGeneration,
          txIndex,
          nextTx,
        ),
      live' = live.exclude(freeIntents).union(allocs),
    }
  }

  // main_wal rollback_decided(tx). This is not the durable rollback marker; it
  // opens the safe window where private allocation headers can be forced.
  action StartRollbackPreparation = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRollbackPreparing(currentTx)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
      live' = live,
    }
  }

  // transaction_log rollback_allocation(tx, region, sequence). These records
  // are written while allocations are still private, before the durable main
  // WAL rollback marker makes rollback cleanup recoverable.
  action RecordRollbackAllocation = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet allocationIndex = currentTx.allocations.indices().oneOf()
    val region = currentTx.allocations.nth(allocationIndex)
    val rollbackAllocation =
      regionRef(region, storage.regions.nth(region).sequence)
    val nextTx =
      txAddRollbackAllocation(currentTx, rollbackAllocation)
    all {
      currentTx.phase == TxRollbackPreparing,
      storage.regions.nth(region).valid,
      not(region.in(txRollbackAllocSet(currentTx))),

      storage' = storageWithTransaction(storage, txIndex, nextTx),
      live' = live,
    }
  }

  // main_wal rollback_transaction(tx), after rollback cleanup is still pending.
  action RollbackTransaction = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRolledBack(currentTx)
    all {
      currentTx.phase == TxRollbackPreparing,
      allAllocationsHaveRollbackRecords(currentTx),
      allRefsHeaderMatch(storage.regions, currentTx.rollbackAllocations),

      storage' = storageWithTransaction(storage, txIndex, nextTx),
      live' = live,
    }
  }

  // main_wal free_region(region, append_tail_after) for a committed free intent.
  action FreeCommittedIntent = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet freeIntentIndex = currentTx.freeIntents.indices().oneOf()
    val freeIntent = currentTx.freeIntents.nth(freeIntentIndex)
    all {
      currentTx.phase == TxCommittedCleanup,
      not(freeIntent.region.in(activeFreeSet)),
      headerMatches(storage.regions, freeIntent),
      not(freeIntent.region.in(live)),

      storage' = storageWithFreeAppend(storage, freeIntent.region),
      live' = live,
    }
  }

  // main_wal free_region(region, append_tail_after) for a rolled-back
  // transaction allocation.
  action FreeRolledBackAllocation = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet rollbackAllocationIndex =
      currentTx.rollbackAllocations.indices().oneOf()
    val rollbackAllocation =
      currentTx.rollbackAllocations.nth(rollbackAllocationIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,
      not(rollbackAllocation.region.in(activeFreeSet)),
      headerMatches(storage.regions, rollbackAllocation),
      not(rollbackAllocation.region.in(live)),

      storage' = storageWithFreeAppend(storage, rollbackAllocation.region),
      live' = live,
    }
  }

  // main_wal transaction_finished(tx). This is the only action that clears
  // retained transaction allocation/free-intent state.
  action FinishTransaction = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      (
        currentTx.phase == TxCommittedCleanup
        and allCleanupDone(
          activeFreeSet,
          storage.regions,
          currentTx.freeIntents,
        )
      )
      or (
        currentTx.phase == TxRolledBackCleanup
        and allCleanupDone(
          activeFreeSet,
          storage.regions,
          currentTx.rollbackAllocations,
        )
      ),

      storage' = storageWithTransaction(storage, txIndex, EmptyTx),
      live' = live,
    }
  }

  // main_wal allocate_region(region, allocation_head_after) for a direct
  // committed allocation outside a transaction.
  action AllocateDirectlyLive = {
    val region = storage.freeQueue.nth(storage.allocationHead)
    all {
      storage.allocationHead < storage.readyBoundary,

      storage' = storageWithDirectLiveAllocation(storage, region),
      live' = live.union(Set(region)),
    }
  }

  // main_wal free_region(region, append_tail_after) for a direct committed
  // free outside a transaction.
  action FreeDirectLive = {
    nondet region = live.oneOf()
    all {
      not(region.in(activeFreeSet)),
      not(region.in(retainedTransactionAllocSet)),
      not(region.in(allTxFreeIntentSet)),

      storage' = storageWithFreeAppend(storage, region),
      live' = live.exclude(Set(region)),
    }
  }

  // main_wal erase_free_region_span(1, ready_boundary_after)
  action EraseOneDirty = {
    val region = storage.freeQueue.nth(storage.readyBoundary)
    all {
      storage.readyBoundary < storage.appendTail,

      storage' =
        storageWithReadyBoundaryAndInvalidHeader(storage, region),
      live' = live,
    }
  }

  // Unsafe comparison: a transaction-private free intent mutates the global
  // allocator before the transaction commits.
  action UnsafeFreeIntentBeforeCommit = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet freeIntentIndex = currentTx.freeIntents.indices().oneOf()
    val freeIntent = currentTx.freeIntents.nth(freeIntentIndex)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithFreeAppend(storage, freeIntent.region),
      live' = live,
    }
  }

  // Unsafe comparison: committed cleanup appends a free record without
  // checking whether the region is already in the active free-space range or
  // still has the expected sequence in its valid header.
  action UnsafeCommittedCleanupWithoutMembershipCheck = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex = currentTx.freeIntents.indices().oneOf()
    val obligation = currentTx.freeIntents.nth(obligationIndex)
    all {
      currentTx.phase == TxCommittedCleanup,

      storage' = storageWithFreeAppend(storage, obligation.region),
      live' = live,
    }
  }

  // Unsafe comparison: rollback cleanup appends a free record without checking
  // whether the region is already active-free or still has the expected
  // rollback-allocation sequence.
  action UnsafeRollbackCleanupWithoutMembershipCheck = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex =
      currentTx.rollbackAllocations.indices().oneOf()
    val obligation =
      currentTx.rollbackAllocations.nth(obligationIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,

      storage' = storageWithFreeAppend(storage, obligation.region),
      live' = live,
    }
  }

  // Unsafe comparison: a stale transaction commits without the collection
  // generation conflict check.
  action UnsafeCommitWithoutGenerationCheck = {
    nondet txIndex = TransactionIndexSet.oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val allocs = txAllocSet(currentTx)
    val freeIntents = txFreeIntentSet(currentTx)
    val nextTx = txCommitted(currentTx)
    val nextGeneration =
      collectionGenerationAt(
        storage.collectionGenerations,
        currentTx.collectionIndex,
      ) + 1
    all {
      currentTx.phase == TxOpen,
      freeIntents.subseteq(live),
      allocatedRegionsHaveHeaders(storage.regions, currentTx.allocations),
      allRefsHeaderMatch(storage.regions, currentTx.freeIntents),

      storage' =
        storageWithCollectionGenerationAndTransaction(
          storage,
          currentTx.collectionIndex,
          nextGeneration,
          txIndex,
          nextTx,
        ),
      live' = live.exclude(freeIntents).union(allocs),
    }
  }

  action Stutter = all {
    storage' = storage,
    live' = live,
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
    FinishTransaction,
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

  val typeOK =
    storage.freeQueue.indices().forall(i => storage.freeQueue.nth(i).in(Regions))
    and live.subseteq(Regions)
    and storage.regions.length() == 6
    and storage.regions.indices().forall(i =>
      0 <= storage.regions.nth(i).sequence)
    and storage.regions.indices().forall(i =>
      storage.regions.nth(i).sequence < storage.nextSequence)
    and storage.transactions.length() == 2
    and storage.collectionGenerations.length() == 2
    and storage.collectionGenerations.indices().forall(i =>
      0 <= storage.collectionGenerations.nth(i))
    and 0 <= storage.nextSequence
    and storage.transactions.indices().forall(i =>
      txEntriesOK(storage.transactions.nth(i), storage.nextSequence))
    and storage.transactions.indices().forall(i =>
      txCollectionOK(storage.transactions.nth(i)))
    and storage.transactions.indices().forall(i =>
      txIdleStateOK(storage.transactions.nth(i)))
    and storage.transactions.indices().forall(i =>
      txRollbackRecordsPhaseOK(storage.transactions.nth(i)))

  val cursorOrderOK =
    0 <= storage.allocationHead
    and storage.allocationHead <= storage.readyBoundary
    and storage.readyBoundary <= storage.appendTail
    and storage.appendTail <= storage.freeQueue.length()

  val activeFreeNoDuplicates =
    noDuplicates(activeFreeList)

  val liveNotFree =
    live.intersect(activeFreeSet) == Set()

  val liveHeadersValid =
    Regions.forall(region =>
      region.in(live).implies(storage.regions.nth(region).valid))

  val readyFreeHeadersErased =
    Regions.forall(region =>
      region.in(readyFreeSet).implies(not(storage.regions.nth(region).valid)))

  val txOpenAllocationsOwned =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxOpen
        or storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txAllocSet(storage.transactions.nth(i)).intersect(live) == Set()
        and txAllocSet(
          storage.transactions.nth(i)
        ).intersect(activeFreeSet) == Set()))

  val txCommittedAllocationsLive =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxCommittedCleanup
      ).implies(txAllocSet(storage.transactions.nth(i)).subseteq(live)))

  val txOpenAllocationsDoNotOverlap =
    TransactionIndexSet.forall(i =>
      TransactionIndexSet.forall(j =>
        i == j or (
          (
            storage.transactions.nth(i).phase == TxOpen
            or storage.transactions.nth(i).phase == TxRollbackPreparing
            or storage.transactions.nth(i).phase == TxRolledBackCleanup
          )
          and (
            storage.transactions.nth(j).phase == TxOpen
            or storage.transactions.nth(j).phase == TxRollbackPreparing
            or storage.transactions.nth(j).phase == TxRolledBackCleanup
          )
        ).implies(
          txOutstandingAllocSet(
            activeFreeSet,
            storage.regions,
            storage.transactions.nth(i),
          ).intersect(
            txOutstandingAllocSet(
              activeFreeSet,
              storage.regions,
              storage.transactions.nth(j),
            )
          ) == Set())))

  val txOpenFreeIntentsRemainLive =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxOpen
        or storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txFreeIntentSet(storage.transactions.nth(i)).subseteq(live)
        and txFreeIntentSet(
          storage.transactions.nth(i)
        ).intersect(activeFreeSet) == Set()
        and allRefsHeaderMatch(
          storage.regions,
          storage.transactions.nth(i).freeIntents,
        )))

  val committedCleanupIntentsDetached =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxCommittedCleanup
      ).implies(
        txPendingCommittedCleanupSet(
          activeFreeSet,
          storage.regions,
          storage.transactions.nth(i),
        ).intersect(live) == Set()))

  val rollbackAllocationsNotLive =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        txOutstandingAllocSet(
          activeFreeSet,
          storage.regions,
          storage.transactions.nth(i),
        ).intersect(live) == Set()))

  val committedCleanupObligationsProgress =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxCommittedCleanup
      ).implies(
        txPendingCommittedCleanupSet(
          activeFreeSet,
          storage.regions,
          storage.transactions.nth(i),
        ).intersect(live) == Set()
      ))

  val rollbackCleanupObligationsProgress =
    TransactionIndexSet.forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        allAllocationsHaveRollbackRecords(storage.transactions.nth(i))
        and
        txOutstandingAllocSet(
          activeFreeSet,
          storage.regions,
          storage.transactions.nth(i),
        ).intersect(live) == Set()
      ))

  val freeIntentSlotsDoNotOverlap =
    TransactionIndexSet.forall(i =>
      TransactionIndexSet.forall(j =>
        i == j or txFreeIntentSet(
          storage.transactions.nth(i)
        ).intersect(txFreeIntentSet(storage.transactions.nth(j))) == Set()))

  val allRegionsAccountedFor =
    accountedRegions == Regions

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

  val safety =
    typeOK
    and cursorOrderOK
    and activeFreeNoDuplicates
    and liveNotFree
    and liveHeadersValid
    and readyFreeHeadersErased
    and txOpenAllocationsOwned
    and txCommittedAllocationsLive
    and txOpenAllocationsDoNotOverlap
    and txOpenFreeIntentsRemainLive
    and committedCleanupIntentsDetached
    and rollbackAllocationsNotLive
    and committedCleanupObligationsProgress
    and rollbackCleanupObligationsProgress
    and freeIntentSlotsDoNotOverlap
    and allRegionsAccountedFor
}
