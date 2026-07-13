// Archived core-pilot model. It is not current design authority.
module writer_exclusion {
  var enrolled: bool
  var enrolledWritesAfterLock: int
  var unrelatedWrites: int
  var committedReads: int

  action init = all { enrolled' = false, enrolledWritesAfterLock' = 0, unrelatedWrites' = 0, committedReads' = 0 }
  action Enroll = all {
    not(enrolled),
    enrolled' = true,
    enrolledWritesAfterLock' = enrolledWritesAfterLock,
    unrelatedWrites' = unrelatedWrites,
    committedReads' = committedReads,
  }
  action ReadEnrolled = all {
    enrolled,
    enrolled' = enrolled,
    enrolledWritesAfterLock' = enrolledWritesAfterLock,
    unrelatedWrites' = unrelatedWrites,
    committedReads' = committedReads + 1,
  }
  action RejectEnrolledWrite = all {
    enrolled,
    enrolled' = enrolled,
    enrolledWritesAfterLock' = enrolledWritesAfterLock,
    unrelatedWrites' = unrelatedWrites,
    committedReads' = committedReads,
  }
  action WriteUnrelated = all {
    enrolled,
    enrolled' = enrolled,
    enrolledWritesAfterLock' = enrolledWritesAfterLock,
    unrelatedWrites' = unrelatedWrites + 1,
    committedReads' = committedReads,
  }
  action Finish = all {
    enrolled,
    enrolled' = false,
    enrolledWritesAfterLock' = enrolledWritesAfterLock,
    unrelatedWrites' = unrelatedWrites,
    committedReads' = committedReads,
  }
  action step = any { Enroll, ReadEnrolled, RejectEnrolledWrite, WriteUnrelated, Finish }

  val safety = enrolledWritesAfterLock == 0
}
