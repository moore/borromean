// Archived core-pilot model. It is not current design authority.
module wal_rotation {
  var tailRegion: int
  var targetReserved: bool
  var targetPrepared: bool
  var linkDurable: bool

  action init = all { tailRegion' = 0, targetReserved' = false, targetPrepared' = false, linkDurable' = false }
  action ReserveTarget = all {
    not(targetReserved),
    tailRegion' = tailRegion,
    targetReserved' = true,
    targetPrepared' = false,
    linkDurable' = false,
  }
  action PrepareAndSync = all {
    targetReserved,
    tailRegion' = tailRegion,
    targetReserved' = targetReserved,
    targetPrepared' = true,
    linkDurable' = linkDurable,
  }
  action PublishLink = all {
    targetPrepared,
    tailRegion' = tailRegion,
    targetReserved' = targetReserved,
    targetPrepared' = targetPrepared,
    linkDurable' = true,
  }
  action SwitchTail = all {
    linkDurable,
    tailRegion' = 1,
    targetReserved' = targetReserved,
    targetPrepared' = targetPrepared,
    linkDurable' = linkDurable,
  }
  action Crash = all {
    tailRegion' = if (linkDurable) 1 else 0,
    targetReserved' = targetReserved,
    targetPrepared' = targetPrepared,
    linkDurable' = linkDurable,
  }
  action step = any { ReserveTarget, PrepareAndSync, PublishLink, SwitchTail, Crash }

  val safety =
    linkDurable.implies(targetPrepared)
    and (tailRegion == 1).implies(linkDurable and targetPrepared)
}
