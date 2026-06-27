// Minimal model for transaction-owned allocations, transaction-private free
// intents, and post-commit main-WAL frees.
//
// This deliberately ignores byte layout, WAL region rotation, checksums, and
// collection-specific encoding. It focuses on the core safety question:
// transaction-private free intents must not mutate the global allocator before
// the transaction commits, and recovery must be idempotent across repeated
// crashes.

module transaction_free_recovery {
  // Transaction lifecycle phase:
  //
  // Idle: no transaction is active.
  // Open: a transaction has started, but no durable commit/rollback terminal
  //   marker has been reached. This also covers startup recovery of an
  //   uncommitted transaction; rollbackStarted distinguishes recovery from
  //   normal transaction execution.
  // Committed: the main-WAL commit marker is durable, so the new collection
  //   state is visible, but post-commit free-intent cleanup is not finished.
  type Phase = Idle | Open | Committed

  pure val Regions = Set(0, 1, 2)

  // Regions reachable from the visible committed collection state.
  var live: Set[int]

  // Regions currently present in the global free-space collection. At init,
  // this is the materialized free-space checkpoint, not replayed WAL frees.
  var free: Set[int]

  // Abstract log of main-WAL free_region records appended after init. This is
  // intentionally not the full free-space log/checkpoint.
  var mainWalFrees: List[int]

  // Regions allocated by the open transaction.
  var txAlloc: Set[int]

  // Transaction-private free intents. These are not allocator commands.
  var txFreeIntent: Set[int]

  // Once rollback recovery starts, the transaction cannot later commit.
  var rollbackStarted: bool

  var phase: Phase

  // Initial durable store: region 0 is the committed collection state, and
  // regions 1 and 2 are globally free via the materialized free-space
  // checkpoint. No main-WAL free_region records have been appended yet.
  action init = all {
    live' = Set(0),
    free' = Set(1, 2),
    mainWalFrees' = [],
    txAlloc' = Set(),
    txFreeIntent' = Set(),
    rollbackStarted' = false,
    phase' = Idle,
  }

  // Start one collection transaction. This opens private transaction state but
  // does not change live collection state or the global free-space collection.
  action StartTransaction = all {
    phase == Idle,
    live' = live,
    free' = free,
    mainWalFrees' = mainWalFrees,
    txAlloc' = Set(),
    txFreeIntent' = Set(),
    rollbackStarted' = false,
    phase' = Open,
  }

  // Reserve a globally free region for the open transaction. This models the
  // durable allocation effect: the region leaves the global free set
  // immediately, even though the transaction may later roll back.
  action AllocInTx = {
    nondet r = free.oneOf()
    all {
      phase == Open,
      not(rollbackStarted),
      free' = free.exclude(Set(r)),
      txAlloc' = txAlloc.union(Set(r)),
      live' = live,
      txFreeIntent' = txFreeIntent,
      mainWalFrees' = mainWalFrees,
      rollbackStarted' = rollbackStarted,
      phase' = phase,
    }
  }

  // Record that a currently live region should be freed if this transaction
  // commits. This is only an intent: it does not mutate the global allocator
  // and does not make the region free before commit.
  action StageFreeIntent = {
    nondet r = live.exclude(txFreeIntent).oneOf()
    all {
      phase == Open,
      not(rollbackStarted),
      txFreeIntent' = txFreeIntent.union(Set(r)),
      live' = live,
      free' = free,
      mainWalFrees' = mainWalFrees,
      txAlloc' = txAlloc,
      rollbackStarted' = rollbackStarted,
      phase' = phase,
    }
  }

  // Unsafe design for comparison: make a transaction-private free mutate the
  // global allocator before commit. This action is intentionally not part of
  // step; unsafeStep includes it and should violate noLiveFreeOverlap.
  action UnsafeTxnFree = {
    nondet r = live.oneOf()
    all {
      phase == Open,
      free' = free.union(Set(r)),
      mainWalFrees' = mainWalFrees.append(r),
      live' = live,
      txAlloc' = txAlloc,
      txFreeIntent' = txFreeIntent,
      rollbackStarted' = rollbackStarted,
      phase' = phase,
    }
  }

  // Commit the transaction. This atomically swaps the visible collection
  // state: staged free-intent regions become detached from live state, and
  // transaction allocations become live. The detached regions are still not in
  // the global free-space collection until ApplyCommittedFree runs.
  action Commit = all {
    phase == Open,
    not(rollbackStarted),
    // Commit atomically swaps visible collection state. Free intents become
    // detached only here; they are not allocator-visible yet.
    live' = live.exclude(txFreeIntent).union(txAlloc),
    free' = free,
    mainWalFrees' = mainWalFrees,
    txAlloc' = Set(),
    txFreeIntent' = txFreeIntent,
    rollbackStarted' = false,
    phase' = Committed,
  }

  // Recovery for an uncommitted transaction. Return one transaction-owned
  // allocation to the global free-space collection with a main-WAL free. This
  // action can be repeated after crashes, but it only chooses allocations that
  // are not already free.
  action RecoverRollbackOneAlloc = {
    nondet r = txAlloc.exclude(free).oneOf()
    all {
      phase == Open,
      free' = free.union(Set(r)),
      mainWalFrees' = mainWalFrees.append(r),
      live' = live,
      txAlloc' = txAlloc,
      txFreeIntent' = txFreeIntent,
      rollbackStarted' = true,
      phase' = phase,
    }
  }

  // Finish rollback recovery after every transaction-owned allocation has been
  // returned. Free intents are discarded because the transaction never
  // committed, so old committed regions remained live throughout rollback.
  // Once rollback is complete, the store has no active transaction again.
  action FinishRollback = all {
    phase == Open,
    txAlloc.subseteq(free),
    live' = live,
    free' = free,
    mainWalFrees' = mainWalFrees,
    txAlloc' = Set(),
    txFreeIntent' = Set(),
    rollbackStarted' = false,
    phase' = Idle,
  }

  // Post-commit cleanup. Convert one committed free intent into a main-WAL
  // free_region effect. This can be repeated after crashes and only appends
  // missing frees.
  action ApplyCommittedFree = {
    nondet r = txFreeIntent.exclude(free).oneOf()
    all {
      phase == Committed,
      free' = free.union(Set(r)),
      mainWalFrees' = mainWalFrees.append(r),
      live' = live,
      txAlloc' = txAlloc,
      txFreeIntent' = txFreeIntent,
      rollbackStarted' = rollbackStarted,
      phase' = phase,
    }
  }

  // Mark the committed transaction complete after every committed free intent
  // has been converted into a global free-space entry. Once finish is durable,
  // the store has no active transaction again.
  action FinishCommitted = all {
    phase == Committed,
    txFreeIntent.subseteq(free),
    live' = live,
    free' = free,
    mainWalFrees' = mainWalFrees,
    txAlloc' = Set(),
    txFreeIntent' = Set(),
    rollbackStarted' = false,
    phase' = Idle,
  }

  // No-op step used to let simulations continue after a terminal state.
  action Stutter = all {
    live' = live,
    free' = free,
    mainWalFrees' = mainWalFrees,
    txAlloc' = txAlloc,
    txFreeIntent' = txFreeIntent,
    rollbackStarted' = rollbackStarted,
    phase' = phase,
  }

  // Safe transition relation for the proposed two-stage-free design.
  action step = any {
    StartTransaction,
    AllocInTx,
    StageFreeIntent,
    Commit,
    RecoverRollbackOneAlloc,
    FinishRollback,
    ApplyCommittedFree,
    FinishCommitted,
    Stutter,
  }

  // Comparison transition relation: includes UnsafeTxnFree so the checker can
  // demonstrate why allocator-effective transaction frees are unsafe.
  action unsafeStep = any {
    step,
    UnsafeTxnFree,
  }

  val typeOK =
    live.subseteq(Regions)
    and free.subseteq(Regions)
    and txAlloc.subseteq(Regions)
    and txFreeIntent.subseteq(Regions)
    and mainWalFrees.indices().forall(i => mainWalFrees.nth(i).in(Regions))

  val noLiveFreeOverlap = live.intersect(free) == Set()

  // Active duplicate free-space entries are abstracted away here by modeling
  // free space as a set. The historical mainWalFrees may contain the same
  // physical region more than once across allocation/free cycles.

  val intentsHaveNoAllocatorEffectBeforeCommit =
    (phase == Open).implies(txFreeIntent.intersect(free) == Set())

  val idleHasNoTransactionScratch =
    (phase == Idle).implies(txAlloc == Set() and txFreeIntent == Set())

  val safety =
    typeOK
    and noLiveFreeOverlap
    and intentsHaveNoAllocatorEffectBeforeCommit
    and idleHasNoTransactionScratch
}
