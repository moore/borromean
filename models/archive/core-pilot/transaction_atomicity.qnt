// Archived core-pilot model. It is not current design authority.
module transaction_atomicity {
  type Phase = Idle | Open | Committed | RolledBack
  var phase: Phase
  var committedA: int
  var committedB: int
  var privateA: int
  var privateB: int

  action init = all { phase' = Idle, committedA' = 0, committedB' = 0, privateA' = 0, privateB' = 0 }
  action Begin = all {
    phase == Idle,
    phase' = Open,
    committedA' = committedA,
    committedB' = committedB,
    privateA' = committedA,
    privateB' = committedB,
  }
  action WriteBoth = all {
    phase == Open,
    phase' = phase,
    committedA' = committedA,
    committedB' = committedB,
    privateA' = 1,
    privateB' = 1,
  }
  action Commit = all {
    phase == Open,
    phase' = Committed,
    committedA' = privateA,
    committedB' = privateB,
    privateA' = privateA,
    privateB' = privateB,
  }
  action Rollback = all {
    phase == Open,
    phase' = RolledBack,
    committedA' = committedA,
    committedB' = committedB,
    privateA' = privateA,
    privateB' = privateB,
  }
  action Crash = all {
    phase' = phase,
    committedA' = committedA,
    committedB' = committedB,
    privateA' = privateA,
    privateB' = privateB,
  }
  action step = any { Begin, WriteBoth, Commit, Rollback, Crash }

  val safety = committedA == committedB
}
