// Archived core-pilot model. It is not current design authority.
module reclaim_reuse {
  type Phase = Published | Dirty | PhysicallyErased | Ready
  var phase: Phase
  var eraseObserved: bool

  action init = all { phase' = Published, eraseObserved' = false }
  action Release = all { phase == Published, phase' = Dirty, eraseObserved' = false }
  action Erase = all { phase == Dirty, phase' = PhysicallyErased, eraseObserved' = true }
  action PublishReady = all { phase == PhysicallyErased, phase' = Ready, eraseObserved' = eraseObserved }
  action Allocate = all { phase == Ready, phase' = Published, eraseObserved' = false }
  action Crash = all { phase' = phase, eraseObserved' = eraseObserved }
  action step = any { Release, Erase, PublishReady, Allocate, Crash }

  val safety = (phase == Ready).implies(eraseObserved)
}
