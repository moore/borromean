// Archived core-pilot model. It is not current design authority.
module fifo_wear {
  var nextRegion: int
  var count0: int
  var count1: int
  var count2: int

  action init = all { nextRegion' = 0, count0' = 0, count1' = 0, count2' = 0 }

  action Allocate0 = all {
    nextRegion == 0,
    nextRegion' = 1,
    count0' = count0 + 1,
    count1' = count1,
    count2' = count2,
  }
  action Allocate1 = all {
    nextRegion == 1,
    nextRegion' = 2,
    count0' = count0,
    count1' = count1 + 1,
    count2' = count2,
  }
  action Allocate2 = all {
    nextRegion == 2,
    nextRegion' = 0,
    count0' = count0,
    count1' = count1,
    count2' = count2 + 1,
  }
  action Crash = all { nextRegion' = nextRegion, count0' = count0, count1' = count1, count2' = count2 }
  action step = any { Allocate0, Allocate1, Allocate2, Crash }

  val safety =
    nextRegion >= 0 and nextRegion <= 2
    and count0 - count1 <= 1 and count1 - count0 <= 1
    and count0 - count2 <= 1 and count2 - count0 <= 1
    and count1 - count2 <= 1 and count2 - count1 <= 1
}
