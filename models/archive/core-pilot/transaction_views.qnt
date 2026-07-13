// Archived core-pilot model. It is not current design authority.
module transaction_views {
  var open: bool
  var committed: int
  var privateValue: int

  action init = all { open' = false, committed' = 0, privateValue' = 0 }
  action Begin = all { not(open), open' = true, committed' = committed, privateValue' = committed }
  action PrivateWrite = all { open, open' = open, committed' = committed, privateValue' = committed + 1 }
  action Commit = all { open, open' = false, committed' = privateValue, privateValue' = privateValue }
  action Rollback = all { open, open' = false, committed' = committed, privateValue' = committed }
  action Crash = all { open' = false, committed' = committed, privateValue' = committed }
  action step = any { Begin, PrivateWrite, Commit, Rollback, Crash }

  val ordinaryView = committed
  val transactionView = if (open) privateValue else committed
  val safety = ordinaryView == committed and (not(open) or transactionView == privateValue)
}
