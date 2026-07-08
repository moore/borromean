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
  ////// Model vocabulary and bounded domains //////

  type TxPhase =
    | TxIdle
    | TxOpen
    | TxRollbackPreparing
    | TxCommittedCleanup
    | TxRolledBackCleanup

  // Transaction phase shape:
  //
  // TxIdle
  //   -> BeginTransaction -> TxOpen
  // TxOpen
  //   -> CommitTransaction -> TxCommittedCleanup
  //   -> StartRollbackPreparation -> TxRollbackPreparing
  // TxRollbackPreparing
  //   -> RecordRollbackAllocation* -> RollbackTransaction
  //   -> TxRolledBackCleanup
  // TxCommittedCleanup
  //   -> FreeCommittedIntent* -> FinishCommit -> TxIdle
  // TxRolledBackCleanup
  //   -> FreeRolledBackAllocation* -> FinishRollback -> TxIdle
  //
  // CommitTransaction and RollbackTransaction acquire walCleanupOwner.
  // FinishCommit and FinishRollback release it.

  // Vocabulary used throughout the model:
  //
  // - A physical region is a bare region index. Collection live sets and the
  //   unconsumed free-space range own physical regions.
  // - A cleanup free reference is a free-log slot. In the implementation this
  //   is a `(free_log_region, offset)` pair; in this bounded model it is the
  //   integer queue position `cleanupStartTail + cleanupIndex`.
  // - Transaction free intents and rollback-allocation cleanup records retain
  //   physical regions. Cleanup idempotence comes from the ordered free slot,
  //   not from a logical region sequence header.
  type TxState = {
    phase: TxPhase,
    collectionIndex: int,
    observedGeneration: int,
    cleanupIndex: int,
    cleanupStartTail: int,
    allocations: List[int],
    freeIntents: List[int],
    rollbackAllocations: List[int],
  }

  type CollectionState = {
    live: Set[int],
    generation: int,
  }

  type StorageState = {
    freeQueue: List[int],
    // Free-queue partitions:
    // - [0, allocationHead): consumed historical prefix.
    // - [allocationHead, readyBoundary): allocatable ready range.
    // - [readyBoundary, appendTail): dirty appended range, not allocatable yet.
    // - [appendTail, freeQueue.length()): unused tail in the modeled list.
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    // Storage-level main-WAL cleanup lock. When this names a transaction,
    // only that transaction may append cleanup free records or finish; other
    // main-WAL actions wait. Transaction-log-only actions may still proceed.
    walCleanupOwner: int,
    collections: List[CollectionState],
    transactions: List[TxState],
  }

  pure val NoCollection = -1
  pure val NoTransaction = -1

  // Fixed bounded domains used for the model.
  pure val RegionIndexes = [0, 1, 2, 3, 4, 5]
  pure val TransactionIndexes = [0, 1]
  pure val CollectionIndexes = [0, 1]

  pure val Regions = Set(0, 1, 2, 3, 4, 5)

  ////// Model state constructors and transition helpers //////

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

  pure def updateTransactions(
    transactions: List[TxState],
    index: int,
    updatedTx: TxState,
  ): List[TxState] =
    TransactionIndexes.foldl([], (acc, i) =>
      acc.append(if (i == index) { updatedTx } else { transactions.nth(i) }))

  pure def txState(
    phase: TxPhase,
    collectionIndex: int,
    observedGeneration: int,
    cleanupIndex: int,
    cleanupStartTail: int,
    allocations: List[int],
    freeIntents: List[int],
    rollbackAllocations: List[int],
  ): TxState = {
    phase: phase,
    collectionIndex: collectionIndex,
    observedGeneration: observedGeneration,
    cleanupIndex: cleanupIndex,
    cleanupStartTail: cleanupStartTail,
    allocations: allocations,
    freeIntents: freeIntents,
    rollbackAllocations: rollbackAllocations,
  }

  pure val EmptyTx =
    txState(TxIdle, NoCollection, 0, 0, 0, [], [], [])

  pure val InitialTransactions = [EmptyTx, EmptyTx]

  pure def openTx(collectionIndex: int, observedGeneration: int): TxState =
    txState(TxOpen, collectionIndex, observedGeneration, 0, 0, [], [], [])

  pure def txAddAllocation(tx: TxState, allocation: int): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.cleanupStartTail,
      tx.allocations.append(allocation),
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txAddFreeIntent(tx: TxState, freeIntent: int): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.cleanupStartTail,
      tx.allocations,
      tx.freeIntents.append(freeIntent),
      tx.rollbackAllocations,
    )

  pure def txAddRollbackAllocation(
    tx: TxState,
    rollbackAllocation: int,
  ): TxState =
    txState(
      tx.phase,
      tx.collectionIndex,
      tx.observedGeneration,
      tx.cleanupIndex,
      tx.cleanupStartTail,
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
      0,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txCommitted(tx: TxState, cleanupStartTail: int): TxState =
    txState(
      TxCommittedCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
      0,
      cleanupStartTail,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def txRolledBack(tx: TxState, cleanupStartTail: int): TxState =
    txState(
      TxRolledBackCleanup,
      tx.collectionIndex,
      tx.observedGeneration,
      0,
      cleanupStartTail,
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
      tx.cleanupStartTail,
      tx.allocations,
      tx.freeIntents,
      tx.rollbackAllocations,
    )

  pure def intListToSet(regions: List[int]): Set[int] =
    regions.foldl(Set(), (acc, region) => acc.union(Set(region)))

  pure def txAllocSet(tx: TxState): Set[int] =
    intListToSet(tx.allocations)

  pure def txFreeIntentSet(tx: TxState): Set[int] =
    intListToSet(tx.freeIntents)

  pure def txRollbackAllocSet(tx: TxState): Set[int] =
    intListToSet(tx.rollbackAllocations)

  pure def pendingRegionSet(regions: List[int], cleanupIndex: int): Set[int] =
    intListToSet(regions.slice(cleanupIndex, regions.length()))

  pure def txInCleanup(tx: TxState): bool =
    tx.phase == TxCommittedCleanup or tx.phase == TxRolledBackCleanup

  pure def txPendingCommittedCleanupSet(tx: TxState): Set[int] =
    if (tx.phase == TxCommittedCleanup) {
      pendingRegionSet(tx.freeIntents, tx.cleanupIndex)
    } else {
      Set()
    }

  pure def txOutstandingAllocSet(tx: TxState): Set[int] =
    if (tx.phase == TxOpen or tx.phase == TxRollbackPreparing) {
      txAllocSet(tx)
    } else if (tx.phase == TxRolledBackCleanup) {
      pendingRegionSet(tx.rollbackAllocations, tx.cleanupIndex)
    } else {
      Set()
    }

  pure def txCurrentRecoverySet(tx: TxState): Set[int] =
    txOutstandingAllocSet(tx).union(txPendingCommittedCleanupSet(tx))

  pure def walUnlocked(state: StorageState): bool =
    state.walCleanupOwner == NoTransaction

  pure def walOwnedBy(state: StorageState, txIndex: int): bool =
    state.walCleanupOwner == txIndex

  pure def storageState(
    freeQueue: List[int],
    allocationHead: int,
    readyBoundary: int,
    appendTail: int,
    walCleanupOwner: int,
    collections: List[CollectionState],
    transactions: List[TxState],
  ): StorageState = {
    freeQueue: freeQueue,
    allocationHead: allocationHead,
    readyBoundary: readyBoundary,
    appendTail: appendTail,
    walCleanupOwner: walCleanupOwner,
    collections: collections,
    transactions: transactions,
  }

  pure def storageWithWalCleanupOwner(
    state: StorageState,
    walCleanupOwner: int,
  ): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary,
      state.appendTail,
      walCleanupOwner,
      state.collections,
      state.transactions,
    )

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
      state.walCleanupOwner,
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
      state.walCleanupOwner,
      state.collections,
      updateTransactions(state.transactions, txIndex, nextTx),
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
      state.walCleanupOwner,
      replaceCollection(state.collections, collectionIndex, nextCollection),
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
      state.walCleanupOwner,
      state.collections,
      state.transactions,
    )

  pure def storageWithReadyBoundary(state: StorageState): StorageState =
    storageState(
      state.freeQueue,
      state.allocationHead,
      state.readyBoundary + 1,
      state.appendTail,
      state.walCleanupOwner,
      state.collections,
      state.transactions,
    )

  ////// Replayed model state and transition actions //////

  pure val InitialCollections = [
    collectionState(Set(0), 0),
    collectionState(Set(1), 0),
  ]
  pure val InitialFreeQueue = [2, 3, 4, 5]
  pure val InitialStorage =
    storageState(
      InitialFreeQueue,
      0,
      InitialFreeQueue.length(),
      InitialFreeQueue.length(),
      NoTransaction,
      InitialCollections,
      InitialTransactions,
    )

  // Replayed implementation state. This is the state foreground apply and WAL
  // replay reconstruct: allocator cursors, committed collection state, and
  // retained transaction records.
  var storage: StorageState

  // Free entries that have not been consumed by allocation. This includes both
  // ready entries and dirty cleanup appends that cannot be allocated until
  // readyBoundary advances.
  val unconsumedFreeList =
    storage.freeQueue.slice(storage.allocationHead, storage.appendTail)

  val unconsumedFreeSet =
    intListToSet(unconsumedFreeList)

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
      walUnlocked(storage),
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

  // transaction_log free_intent(tx, region). Staging has no allocator effect.
  action StageFreeIntent = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet region =
      collectionLiveAt(storage.collections, currentTx.collectionIndex)
        .exclude(txFreeIntentSet(currentTx))
        .oneOf()
    val nextTx = txAddFreeIntent(currentTx, region)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
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
    val nextTx = txCommitted(currentTx, storage.appendTail)
    val nextGeneration =
      collectionGenerationAt(
        storage.collections,
        currentTx.collectionIndex,
      ) + 1
    val nextCollection =
      collectionState(nextLive, nextGeneration)
    all {
      walUnlocked(storage),
      currentTx.phase == TxOpen,
      currentTx.observedGeneration
        == collectionGenerationAt(
          storage.collections,
          currentTx.collectionIndex,
        ),

      storage' =
        storageWithWalCleanupOwner(
          storageWithCollectionAndTransaction(
            storage,
            currentTx.collectionIndex,
            nextCollection,
            txIndex,
            nextTx,
          ),
          txIndex,
        ),
    }
  }

  // main_wal rollback_decided(tx). This is not the durable rollback marker; it
  // opens the safe window where private allocation cleanup records can be forced.
  action StartRollbackPreparation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRollbackPreparing(currentTx)
    all {
      walUnlocked(storage),
      currentTx.phase == TxOpen,

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // transaction_log rollback_allocation(tx, region). These records are written
  // while allocations are still private, before the durable main-WAL rollback
  // marker makes rollback cleanup recoverable.
  action RecordRollbackAllocation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val region = currentTx.allocations.nth(currentTx.cleanupIndex)
    val nextTx =
      txAdvanceCleanup(txAddRollbackAllocation(currentTx, region))
    all {
      currentTx.phase == TxRollbackPreparing,
      currentTx.cleanupIndex < currentTx.allocations.length(),

      storage' = storageWithTransaction(storage, txIndex, nextTx),
    }
  }

  // main_wal rollback_transaction(tx), after rollback cleanup is still pending.
  action RollbackTransaction = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val nextTx = txRolledBack(currentTx, storage.appendTail)
    all {
      walUnlocked(storage),
      currentTx.phase == TxRollbackPreparing,
      currentTx.cleanupIndex == currentTx.allocations.length(),

      storage' =
        storageWithWalCleanupOwner(
          storageWithTransaction(storage, txIndex, nextTx),
          txIndex,
        ),
    }
  }

  // main_wal free_region(region, free_slot) for a committed free intent.
  // In this model, free_slot is cleanupStartTail + cleanupIndex.
  action FreeCommittedIntent = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val freeSlot = currentTx.cleanupStartTail + currentTx.cleanupIndex
    val freeIntent = currentTx.freeIntents.nth(currentTx.cleanupIndex)
    val nextTx = txAdvanceCleanup(currentTx)
    all {
      currentTx.phase == TxCommittedCleanup,
      walOwnedBy(storage, txIndex),
      currentTx.cleanupIndex < currentTx.freeIntents.length(),
      storage.appendTail == freeSlot,

      storage' =
        storageWithTransaction(
          storageWithFreeAppend(storage, freeIntent),
          txIndex,
          nextTx,
        ),
    }
  }

  // main_wal free_region(region, free_slot) for a rolled-back transaction
  // allocation.
  action FreeRolledBackAllocation = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    val freeSlot = currentTx.cleanupStartTail + currentTx.cleanupIndex
    val rollbackAllocation =
      currentTx.rollbackAllocations.nth(currentTx.cleanupIndex)
    val nextTx = txAdvanceCleanup(currentTx)
    all {
      currentTx.phase == TxRolledBackCleanup,
      walOwnedBy(storage, txIndex),
      currentTx.cleanupIndex < currentTx.rollbackAllocations.length(),
      storage.appendTail == freeSlot,

      storage' =
        storageWithTransaction(
          storageWithFreeAppend(storage, rollbackAllocation),
          txIndex,
          nextTx,
        ),
    }
  }

  // main_wal transaction_finished(tx) after committed-free cleanup. This is
  // one of the only actions that clears retained transaction state.
  action FinishCommit = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      currentTx.phase == TxCommittedCleanup,
      walOwnedBy(storage, txIndex),
      currentTx.cleanupIndex == currentTx.freeIntents.length(),

      storage' =
        storageWithWalCleanupOwner(
          storageWithTransaction(storage, txIndex, EmptyTx),
          NoTransaction,
        ),
    }
  }

  // main_wal transaction_finished(tx) after rollback cleanup. This is one of
  // the only actions that clears retained transaction state.
  action FinishRollback = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,
      walOwnedBy(storage, txIndex),
      currentTx.cleanupIndex == currentTx.rollbackAllocations.length(),

      storage' =
        storageWithWalCleanupOwner(
          storageWithTransaction(storage, txIndex, EmptyTx),
          NoTransaction,
        ),
    }
  }

  // main_wal erase_free_region_span(1, ready_boundary_after)
  action EraseOneDirty = all {
    walUnlocked(storage),
    storage.readyBoundary < storage.appendTail,

    storage' = storageWithReadyBoundary(storage),
  }

  action Stutter = all {
    storage' = storage,
  }

  action step = any {
    BeginTransaction,
    AllocateInTransaction,
    StageFreeIntent,
    CommitTransaction,
    StartRollbackPreparation,
    RecordRollbackAllocation,
    RollbackTransaction,
    FreeCommittedIntent,
    FreeRolledBackAllocation,
    FinishCommit,
    FinishRollback,
    EraseOneDirty,
    Stutter,
  }

  ////// Unsafe comparison actions //////

  // Unsafe comparison: a transaction-private free intent mutates the global
  // allocator before the transaction commits.
  action UnsafeFreeIntentBeforeCommit = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet freeIntentIndex = currentTx.freeIntents.indices().oneOf()
    val freeIntent = currentTx.freeIntents.nth(freeIntentIndex)
    all {
      currentTx.phase == TxOpen,

      storage' = storageWithFreeAppend(storage, freeIntent),
    }
  }

  // Unsafe comparison: committed cleanup appends a free record without using
  // the cleanup slot order or advancing the cleanup cursor.
  action UnsafeCommittedCleanupWithoutSlotOrder = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex = currentTx.freeIntents.indices().oneOf()
    val obligation = currentTx.freeIntents.nth(obligationIndex)
    all {
      currentTx.phase == TxCommittedCleanup,

      storage' = storageWithFreeAppend(storage, obligation),
    }
  }

  // Unsafe comparison: rollback cleanup appends a free record without using
  // the cleanup slot order or advancing the cleanup cursor.
  action UnsafeRollbackCleanupWithoutSlotOrder = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    nondet obligationIndex =
      currentTx.rollbackAllocations.indices().oneOf()
    val obligation =
      currentTx.rollbackAllocations.nth(obligationIndex)
    all {
      currentTx.phase == TxRolledBackCleanup,

      storage' = storageWithFreeAppend(storage, obligation),
    }
  }

  // Unsafe comparison: erase maintenance advances the ready boundary over the
  // cleanup suffix before transaction_finished.
  action UnsafeEraseDuringCleanup = {
    nondet txIndex = storage.transactions.indices().oneOf()
    val currentTx = storage.transactions.nth(txIndex)
    all {
      walOwnedBy(storage, txIndex),
      txInCleanup(currentTx),
      storage.readyBoundary == currentTx.cleanupStartTail,
      storage.readyBoundary < storage.appendTail,

      storage' = storageWithReadyBoundary(storage),
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
    val nextTx = txCommitted(currentTx, storage.appendTail)
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
        storageWithWalCleanupOwner(
          storageWithCollectionAndTransaction(
            storage,
            currentTx.collectionIndex,
            nextCollection,
            txIndex,
            nextTx,
          ),
          txIndex,
        ),
    }
  }

  action unsafeStep = any {
    step,
    UnsafeFreeIntentBeforeCommit,
    UnsafeCommittedCleanupWithoutSlotOrder,
    UnsafeRollbackCleanupWithoutSlotOrder,
    UnsafeEraseDuringCleanup,
    UnsafeCommitWithoutGenerationCheck,
  }

  ////// Invariant support helpers //////

  // These helpers do not define model transitions. They only classify or check
  // replayed state for the invariants below.

  pure def collectionLivesDisjointFrom(
    collections: List[CollectionState],
    regions: Set[int],
  ): bool =
    collections.indices().forall(i =>
      collectionLiveAt(collections, i).intersect(regions) == Set())

  pure def intListEntriesInRegions(regions: List[int]): bool =
    regions.indices().forall(i => regions.nth(i).in(Regions))

  pure def noDuplicates(regions: List[int]): bool =
    regions.indices().forall(i =>
      regions.indices().forall(j =>
        (i == j).or(not(regions.nth(i) == regions.nth(j)))))

  pure def allAllocationsHaveRollbackRecords(tx: TxState): bool =
    txAllocSet(tx).subseteq(txRollbackAllocSet(tx))

  pure def cleanupListLength(tx: TxState): int =
    if (tx.phase == TxCommittedCleanup) {
      tx.freeIntents.length()
    } else if (tx.phase == TxRolledBackCleanup) {
      tx.rollbackAllocations.length()
    } else {
      0
    }

  pure def cleanupListEntryAt(tx: TxState, index: int): int =
    if (tx.phase == TxCommittedCleanup) {
      tx.freeIntents.nth(index)
    } else {
      tx.rollbackAllocations.nth(index)
    }

  pure def cleanupPrefixMatchesQueue(
    freeQueue: List[int],
    tx: TxState,
  ): bool =
    if (txInCleanup(tx)) {
      tx.cleanupStartTail + tx.cleanupIndex <= freeQueue.length()
      and RegionIndexes.indices().forall(offset =>
        if (
          offset < tx.cleanupIndex
          and offset < cleanupListLength(tx)
          and tx.cleanupStartTail + offset < freeQueue.length()
        ) {
          freeQueue.nth(tx.cleanupStartTail + offset)
            == cleanupListEntryAt(tx, offset)
        } else {
          true
        })
    } else {
      true
    }

  pure def txEntriesOK(tx: TxState): bool =
    // Every transaction-owned list names modeled physical regions.
    intListEntriesInRegions(tx.allocations)
    and intListEntriesInRegions(tx.freeIntents)
    and intListEntriesInRegions(tx.rollbackAllocations)
    // A transaction may not retain duplicate ownership/cleanup entries.
    and noDuplicates(tx.allocations)
    and noDuplicates(tx.freeIntents)
    and noDuplicates(tx.rollbackAllocations)
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
      and tx.cleanupStartTail == 0
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

  pure def txCleanupStartTailOK(
    tx: TxState,
    appendTail: int,
    freeQueueLength: int,
  ): bool =
    0 <= tx.cleanupStartTail
    and tx.cleanupStartTail <= freeQueueLength
    and
    if (txInCleanup(tx)) {
      tx.cleanupStartTail <= appendTail
    } else {
      tx.cleanupStartTail == 0
    }

  pure def txRollbackRecordsPhaseOK(tx: TxState): bool =
    // Rollback records exist only after rollback preparation has started.
    (
      tx.phase == TxOpen
      or tx.phase == TxCommittedCleanup
    ).implies(tx.rollbackAllocations.length() == 0)

  ////// Core invariants //////

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
    // The model has exactly two transaction slots; storage.transactions.indices()
    // is the source of truth for valid transaction indexes.
    and storage.transactions.length() == 2
    // The WAL cleanup lock is either empty or names one modeled transaction.
    and (
      storage.walCleanupOwner == NoTransaction
      or storage.walCleanupOwner.in(storage.transactions.indices())
    )
    // Transaction-owned lists and retained refs are well formed and duplicate-free.
    and storage.transactions.indices().forall(i =>
      txEntriesOK(storage.transactions.nth(i)))
    // Each transaction phase has the expected collection binding shape.
    and storage.transactions.indices().forall(i =>
      txCollectionOK(storage.transactions.nth(i), storage.collections))
    // Idle transaction slots are fully empty, not just marked idle.
    and storage.transactions.indices().forall(i =>
      txIdleStateOK(storage.transactions.nth(i)))
    // The cleanup/rollback scan cursor is valid for the current phase.
    and storage.transactions.indices().forall(i =>
      txCleanupIndexOK(storage.transactions.nth(i)))
    // Cleanup start slots are bounded and are zero outside cleanup phases.
    and storage.transactions.indices().forall(i =>
      txCleanupStartTailOK(
        storage.transactions.nth(i),
        storage.appendTail,
        storage.freeQueue.length(),
      ))
    // Rollback records cannot appear before a transaction starts rollback.
    and storage.transactions.indices().forall(i =>
      txRollbackRecordsPhaseOK(storage.transactions.nth(i)))

  // Allocator cursors maintain the consumed/ready/dirty/unused queue partition.
  val cursorOrderOK =
    0 <= storage.allocationHead
    and storage.allocationHead <= storage.readyBoundary
    and storage.readyBoundary <= storage.appendTail
    and storage.appendTail <= storage.freeQueue.length()

  // Committed collection ownership is exclusive across collections. This is a
  // physical-region ownership invariant: a collection live set contains region
  // indexes.
  val collectionOwnershipUnique =
    storage.collections.indices().forall(i =>
      storage.collections.indices().forall(j =>
        i == j
        or collectionLiveAt(storage.collections, i).intersect(
          collectionLiveAt(storage.collections, j)
        ) == Set()))

  // Core invariant: no physical region can be both committed-live and in the
  // unconsumed free-space range.
  val noLiveFreeOverlap =
    collectionLivesDisjointFrom(storage.collections, unconsumedFreeSet)

  // Core invariant: the storage WAL cleanup lock names exactly the transaction
  // currently in the serialized main-WAL cleanup section.
  val walCleanupOwnerMatchesCleanupPhase =
    if (walUnlocked(storage)) {
      storage.transactions.indices().forall(i =>
        not(txInCleanup(storage.transactions.nth(i))))
    } else if (storage.walCleanupOwner.in(storage.transactions.indices())) {
      txInCleanup(storage.transactions.nth(storage.walCleanupOwner))
      and storage.transactions.indices().forall(i =>
        i == storage.walCleanupOwner
        or not(txInCleanup(storage.transactions.nth(i))))
    } else {
      false
    }

  // Core invariant: cleanup free commands are an ordered suffix. The queue
  // slots written since cleanup started are exactly the processed prefix of
  // the retained cleanup list.
  val cleanupTailOrdered =
    if (walUnlocked(storage)) {
      true
    } else if (storage.walCleanupOwner.in(storage.transactions.indices())) {
      txInCleanup(storage.transactions.nth(storage.walCleanupOwner)).implies(
        storage.appendTail
          == storage.transactions.nth(storage.walCleanupOwner).cleanupStartTail
            + storage.transactions.nth(storage.walCleanupOwner).cleanupIndex
        and cleanupPrefixMatchesQueue(
          storage.freeQueue,
          storage.transactions.nth(storage.walCleanupOwner),
        ))
    } else {
      false
    }

  // Core invariant: cleanup frees remain dirty until transaction_finished.
  val cleanupReadyBoundaryFrozen =
    if (walUnlocked(storage)) {
      true
    } else if (storage.walCleanupOwner.in(storage.transactions.indices())) {
      txInCleanup(storage.transactions.nth(storage.walCleanupOwner)).implies(
        storage.readyBoundary
          <= storage.transactions.nth(storage.walCleanupOwner).cleanupStartTail)
    } else {
      false
    }

  // Core invariant: a transaction allocation cannot become collection-live
  // before commit, and rollback cleanup obligations cannot be live after the
  // rollback marker.
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
        ).intersect(unconsumedFreeSet) == Set()))
    and
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        collectionLivesDisjointFrom(
          storage.collections,
          txOutstandingAllocSet(storage.transactions.nth(i)),
        )))

  // Current recovery ownership is exclusive across transaction slots. Retained
  // cleanup refs that have already been written into free slots are not part of
  // this set; they are completed evidence kept until transaction_finished.
  val txCurrentRecoverySetsDoNotOverlap =
    storage.transactions.indices().forall(i =>
      storage.transactions.indices().forall(j =>
        i == j or
          txCurrentRecoverySet(
            storage.transactions.nth(i),
          ).intersect(
            txCurrentRecoverySet(
              storage.transactions.nth(j),
            )
          ) == Set()))

  // Current free-intent refs stay live in the transaction's collection and out
  // of unconsumed free space until commit or rollback preparation changes the
  // transaction phase. Stale transactions may retain old intents; the
  // generation check blocks their commit.
  val openFreeIntentsStayLiveAndNotFree =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxOpen
        or storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txGenerationCurrent(
          storage.collections,
          storage.transactions.nth(i),
        ).implies(
          txFreeIntentSet(
            storage.transactions.nth(i)
          ).subseteq(
            collectionLiveAt(
              storage.collections,
              storage.transactions.nth(i).collectionIndex,
            )
          )
          and txFreeIntentSet(
            storage.transactions.nth(i)
          ).intersect(unconsumedFreeSet) == Set()
        )))

  // Rollback-allocation records written before the durable rollback marker are
  // still private transaction evidence and cannot already be free.
  val rollbackPreparationRefsNotFree =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRollbackPreparing
      ).implies(
        txRollbackAllocSet(
          storage.transactions.nth(i)
        ).intersect(unconsumedFreeSet) == Set()
      ))

  // After commit, still-pending cleanup entries are detached from collection
  // live state until cleanup writes their indexed free slots.
  val committedCleanupPendingRefsNotLive =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxCommittedCleanup
      ).implies(
        collectionLivesDisjointFrom(
          storage.collections,
          txPendingCommittedCleanupSet(storage.transactions.nth(i)),
        )))

  // After rollback, every allocation has an explicit rollback record and
  // still-pending rollback cleanup entries are detached from collection live
  // state.
  val rolledBackCleanupPendingAllocationsNotLive =
    storage.transactions.indices().forall(i =>
      (
        storage.transactions.nth(i).phase == TxRolledBackCleanup
      ).implies(
        allAllocationsHaveRollbackRecords(storage.transactions.nth(i))
        and
        collectionLivesDisjointFrom(
          storage.collections,
          txOutstandingAllocSet(storage.transactions.nth(i)),
        )
      ))

  // Core invariant: cleanup refs are phase-safe. Before the terminal marker
  // that authorizes cleanup, refs stay private. After commit/rollback,
  // still-pending cleanup suffixes are detached until cleanup writes their
  // indexed free slots.
  val cleanupRefsPhaseSafe =
    openFreeIntentsStayLiveAndNotFree
    and rollbackPreparationRefsNotFree
    and committedCleanupPendingRefsNotLive
    and rolledBackCleanupPendingAllocationsNotLive

  // Core invariant: no double free. Unconsumed free space is a physical-region
  // set represented as a list slice, so duplicate entries would publish the
  // same physical free twice.
  val noDoubleFree =
    noDuplicates(unconsumedFreeList)
    and storage.transactions.indices().forall(i =>
      txCurrentRecoverySet(
        storage.transactions.nth(i),
      ).intersect(unconsumedFreeSet) == Set())
    and txCurrentRecoverySetsDoNotOverlap

  // Core invariant: no modeled physical region leaks. Each region is reachable
  // from live state, unconsumed free space, or unfinished transaction/recovery
  // state. Completed cleanup refs are not needed for reachability once their
  // physical region is unconsumed-free.
  val noLeaks =
    Regions.forall(region =>
      // 1. committed live in some collection
      storage.collections.indices().exists(collectionIndex =>
        region.in(collectionLiveAt(storage.collections, collectionIndex)))
      // 2. allocator-free
      or region.in(unconsumedFreeSet)
      // 3. transaction-private allocation or unfinished cleanup/recovery
      or storage.transactions.indices().exists(txIndex =>
        region.in(txCurrentRecoverySet(storage.transactions.nth(txIndex)))))

  ////// Reachability helpers and exported invariants //////

  // Positive reachability helper: serialized cleanup should prevent two
  // different collection transactions from both being in committed cleanup.
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
    and walCleanupOwnerMatchesCleanupPhase
    and cleanupTailOrdered
    and cleanupReadyBoundaryFrozen
    and noPrematurePublication
    and cleanupRefsPhaseSafe
    and noDoubleFree
    and noLeaks
}
