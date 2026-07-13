// Archived core-pilot model. It is not current design authority.
module core_composition {
  type RegionPhase = Ready | Reserved | Published | Dirty
  var region: RegionPhase
  var transactionOpen: bool
  var committedGeneration: int
  var privateGeneration: int
  var durablePublications: int

  action init = all {
    region' = Ready,
    transactionOpen' = false,
    committedGeneration' = 0,
    privateGeneration' = 0,
    durablePublications' = 0,
  }
  action Reserve = all {
    region == Ready,
    region' = Reserved,
    transactionOpen' = transactionOpen,
    committedGeneration' = committedGeneration,
    privateGeneration' = privateGeneration,
    durablePublications' = durablePublications,
  }
  action BeginAndWrite = all {
    region == Reserved,
    not(transactionOpen),
    region' = region,
    transactionOpen' = true,
    committedGeneration' = committedGeneration,
    privateGeneration' = committedGeneration + 1,
    durablePublications' = durablePublications,
  }
  action CommitAndPublish = all {
    transactionOpen,
    region == Reserved,
    region' = Published,
    transactionOpen' = false,
    committedGeneration' = privateGeneration,
    privateGeneration' = privateGeneration,
    durablePublications' = durablePublications + 1,
  }
  action Release = all {
    region == Published,
    region' = Dirty,
    transactionOpen' = transactionOpen,
    committedGeneration' = committedGeneration,
    privateGeneration' = privateGeneration,
    durablePublications' = durablePublications,
  }
  action Crash = all {
    region' = region,
    transactionOpen' = false,
    committedGeneration' = committedGeneration,
    privateGeneration' = committedGeneration,
    durablePublications' = durablePublications,
  }
  action step = any { Reserve, BeginAndWrite, CommitAndPublish, Release, Crash }

  val safety =
    committedGeneration <= durablePublications
    and (region == Published).implies(durablePublications > 0)
}
