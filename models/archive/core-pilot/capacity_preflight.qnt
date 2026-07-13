// Archived core-pilot model. It is not current design authority.
module capacity_preflight {
  var capacity: int
  var durableEvents: int
  var rejected: bool
  var eventsAtReject: int

  action init = all { capacity' = 2, durableEvents' = 0, rejected' = false, eventsAtReject' = 0 }
  action DurableMutation = all {
    capacity > 0,
    capacity' = capacity - 1,
    durableEvents' = durableEvents + 1,
    rejected' = false,
    eventsAtReject' = eventsAtReject,
  }
  action RejectForPressure = all {
    capacity == 0,
    capacity' = capacity,
    durableEvents' = durableEvents,
    rejected' = true,
    eventsAtReject' = durableEvents,
  }
  action RefillByMaintenance = all {
    capacity == 0,
    capacity' = 1,
    durableEvents' = durableEvents,
    rejected' = false,
    eventsAtReject' = eventsAtReject,
  }
  action step = any { DurableMutation, RejectForPressure, RefillByMaintenance }

  val safety = capacity >= 0 and rejected.implies(durableEvents == eventsAtReject)
}
