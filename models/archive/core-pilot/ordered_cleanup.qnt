// Archived core-pilot model. It is not current design authority.
module ordered_cleanup {
  var cursor: int
  var freed0: bool
  var freed1: bool
  var freed2: bool

  action init = all { cursor' = 0, freed0' = false, freed1' = false, freed2' = false }
  action Free0 = all { cursor == 0, cursor' = 1, freed0' = true, freed1' = freed1, freed2' = freed2 }
  action Free1 = all { cursor == 1, cursor' = 2, freed0' = freed0, freed1' = true, freed2' = freed2 }
  action Free2 = all { cursor == 2, cursor' = 3, freed0' = freed0, freed1' = freed1, freed2' = true }
  action Crash = all { cursor' = cursor, freed0' = freed0, freed1' = freed1, freed2' = freed2 }
  action step = any { Free0, Free1, Free2, Crash }

  val safety =
    cursor >= 0 and cursor <= 3
    and (cursor >= 1) == freed0
    and (cursor >= 2) == freed1
    and (cursor >= 3) == freed2
}
