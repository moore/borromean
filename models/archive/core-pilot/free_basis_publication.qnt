// Archived core-pilot model. It is not current design authority.
module free_basis_publication {
  var liveRoot: int
  var replacementAllocated: bool
  var replacementDurable: bool
  var oldReclaimed: bool

  action init = all {
    liveRoot' = 0,
    replacementAllocated' = false,
    replacementDurable' = false,
    oldReclaimed' = false,
  }
  action AllocateReplacement = all {
    not(replacementAllocated),
    liveRoot' = liveRoot,
    replacementAllocated' = true,
    replacementDurable' = false,
    oldReclaimed' = oldReclaimed,
  }
  action WriteAndSyncReplacement = all {
    replacementAllocated,
    liveRoot' = liveRoot,
    replacementAllocated' = replacementAllocated,
    replacementDurable' = true,
    oldReclaimed' = oldReclaimed,
  }
  action PublishReplacement = all {
    replacementDurable,
    liveRoot' = 1,
    replacementAllocated' = replacementAllocated,
    replacementDurable' = replacementDurable,
    oldReclaimed' = oldReclaimed,
  }
  action ReclaimOld = all {
    liveRoot == 1,
    liveRoot' = liveRoot,
    replacementAllocated' = replacementAllocated,
    replacementDurable' = replacementDurable,
    oldReclaimed' = true,
  }
  action Crash = all {
    liveRoot' = liveRoot,
    replacementAllocated' = replacementAllocated,
    replacementDurable' = replacementDurable,
    oldReclaimed' = oldReclaimed,
  }
  action step = any { AllocateReplacement, WriteAndSyncReplacement, PublishReplacement, ReclaimOld, Crash }

  val safety =
    (liveRoot == 1).implies(replacementDurable)
    and oldReclaimed.implies(liveRoot == 1)
}
