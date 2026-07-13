// Archived core-pilot model. It is not current design authority.
// Abstract region-ownership machine for spec/archive/core-pilot/01-ownership.md.
//
// The pure values and reducers in this module are the specification boundary.
// The executable actions at the end explore a bounded set of valid and invalid
// requests. Concrete storage models import only the pure interface through a
// separate refinement module; this module has no mechanical dependencies.

module region_ownership {
  pure val NoIdentity = -1
  pure val RegionIndexes = [0, 1, 2, 3, 4, 5]
  pure val Regions = Set(0, 1, 2, 3, 4, 5)
  pure val PermanentRegions = Set(0)
  pure val CollectionOwnerIds = Set(0, 1)
  pure val TransactionOwnerIds = Set(0, 1)
  pure val MaxVerificationDepth = 8

  type SystemPurpose =
    | MainWalPurpose
    | TransactionLogPurpose(int)
    | FreeBasisPurpose
    | BootstrapPurpose

  // System purposes and collection-data purposes are different variants, so
  // collection type identifiers cannot alias a system purpose.
  type Purpose =
    | SystemPurposeOf(SystemPurpose)
    | CollectionDataPurpose(int)
    | InvalidPurpose

  type CollectionOwnerData = {
    collection: int,
    collectionType: int,
  }

  type Owner =
    | MainWalOwner
    | TransactionOwner(int)
    | FreeBasisOwner
    | BootstrapOwner
    | CollectionOwner(CollectionOwnerData)
    | InvalidOwner

  type Reservation = {
    region: int,
    purpose: Purpose,
    operation: int,
  }

  type Publication = {
    reservation: Reservation,
    owner: Owner,
  }

  type Release = {
    region: int,
    owner: Owner,
  }

  // The payload of Reserved is the linear publication authority. Published
  // contains only the retained owner; the construction identity is consumed.
  type RegionState =
    | ErasedPrepared
    | Reserved(Reservation)
    | Published(Owner)
    | Dirty

  type OwnershipEvent =
    | ReserveFact(Reservation)
    | PublishFact(Publication)
    | ReleaseFact(Release)
    | ReadinessFact(int)

  type OwnershipError =
    | OwnershipOk
    | RegionOutOfRange
    | InvalidSourceState
    | ReservationMismatch
    | ReservationTokenMissing
    | PurposeOwnerMismatch
    | PublishedOwnerMismatch
    | PermanentRegion
    | PurposeInvalid

  type OwnershipState = {
    regions: List[RegionState],
    // A set makes exact token clones unrepresentable. The invariant below also
    // requires at most one distinct reservation token for each region.
    tokens: Set[Reservation],
  }

  type ApplyResult = {
    state: OwnershipState,
    error: OwnershipError,
  }

  pure def ownershipState(
    regions: List[RegionState],
    tokens: Set[Reservation],
  ): OwnershipState = {
    regions: regions,
    tokens: tokens,
  }

  pure def result(
    state: OwnershipState,
    error: OwnershipError,
  ): ApplyResult = {
    state: state,
    error: error,
  }

  pure def reservation(
    region: int,
    purpose: Purpose,
    operation: int,
  ): Reservation = {
    region: region,
    purpose: purpose,
    operation: operation,
  }

  pure def collectionOwner(
    collection: int,
    collectionType: int,
  ): Owner =
    CollectionOwner({ collection: collection, collectionType: collectionType })

  pure def validRegion(region: int): bool =
    region.in(Regions)

  pure def tokenPresent(
    tokens: Set[Reservation],
    requested: Reservation,
  ): bool =
    requested.in(tokens)

  pure def removeToken(
    tokens: Set[Reservation],
    requested: Reservation,
  ): Set[Reservation] =
    tokens.exclude(Set(requested))

  pure def replaceRegion(
    regions: List[RegionState],
    region: int,
    nextRegion: RegionState,
  ): List[RegionState] =
    RegionIndexes.foldl([], (updated, i) =>
      updated.append(
        if (i == region) { nextRegion } else { regions.nth(i) }))

  pure def validPurpose(purpose: Purpose): bool =
    match purpose {
      SystemPurposeOf(systemPurpose) =>
        match systemPurpose {
          MainWalPurpose => true
          | TransactionLogPurpose(transaction) =>
              transaction.in(TransactionOwnerIds)
          | FreeBasisPurpose => true
          | BootstrapPurpose => true
        }
      | CollectionDataPurpose(_) => true
      | InvalidPurpose => false
    }

  pure def validOwner(owner: Owner): bool =
    match owner {
      MainWalOwner => true
      | TransactionOwner(transaction) =>
          transaction.in(TransactionOwnerIds)
      | FreeBasisOwner => true
      | BootstrapOwner => true
      | CollectionOwner(data) => data.collection.in(CollectionOwnerIds)
      | InvalidOwner => false
    }

  pure def purposeOwnerCompatible(purpose: Purpose, owner: Owner): bool =
    if (not(validPurpose(purpose)) or not(validOwner(owner))) {
      false
    } else {
      match purpose {
        SystemPurposeOf(systemPurpose) =>
          match systemPurpose {
            MainWalPurpose => owner == MainWalOwner
            | TransactionLogPurpose(transaction) =>
                owner == TransactionOwner(transaction)
            | FreeBasisPurpose => owner == FreeBasisOwner
            | BootstrapPurpose => owner == BootstrapOwner
          }
        | CollectionDataPurpose(collectionType) =>
            match owner {
              CollectionOwner(data) => data.collectionType == collectionType
              | _ => false
            }
        | InvalidPurpose => false
      }
    }

  pure def applyReserve(
    state: OwnershipState,
    requested: Reservation,
  ): ApplyResult =
    if (not(validRegion(requested.region))) {
      result(state, RegionOutOfRange)
    } else if (requested.region.in(PermanentRegions)) {
      result(state, PermanentRegion)
    } else if (not(validPurpose(requested.purpose))) {
      result(state, PurposeInvalid)
    } else if (
      state.tokens.exists(token => token.region == requested.region)
    ) {
      result(state, ReservationTokenMissing)
    } else {
      match state.regions.nth(requested.region) {
        ErasedPrepared =>
          result(
            ownershipState(
              replaceRegion(
                state.regions,
                requested.region,
                Reserved(requested),
              ),
              state.tokens.union(Set(requested)),
            ),
            OwnershipOk,
          )
        | _ => result(state, InvalidSourceState)
      }
    }

  pure def applyPublish(
    state: OwnershipState,
    publication: Publication,
  ): ApplyResult = {
    val requested = publication.reservation
    if (not(validRegion(requested.region))) {
      result(state, RegionOutOfRange)
    } else {
      match state.regions.nth(requested.region) {
        Reserved(actual) =>
          if (actual != requested) {
            result(state, ReservationMismatch)
          } else if (not(tokenPresent(state.tokens, requested))) {
            result(state, ReservationTokenMissing)
          } else if (
            not(purposeOwnerCompatible(requested.purpose, publication.owner))
          ) {
            result(state, PurposeOwnerMismatch)
          } else {
            result(
              ownershipState(
                replaceRegion(
                  state.regions,
                  requested.region,
                  Published(publication.owner),
                ),
                removeToken(state.tokens, requested),
              ),
              OwnershipOk,
            )
          }
        | _ => result(state, InvalidSourceState)
      }
    }
  }

  pure def applyRelease(
    state: OwnershipState,
    release: Release,
  ): ApplyResult =
    if (not(validRegion(release.region))) {
      result(state, RegionOutOfRange)
    } else if (release.region.in(PermanentRegions)) {
      result(state, PermanentRegion)
    } else {
      match state.regions.nth(release.region) {
        Published(actualOwner) =>
          if (actualOwner == release.owner) {
            result(
              ownershipState(
                replaceRegion(state.regions, release.region, Dirty),
                state.tokens,
              ),
              OwnershipOk,
            )
          } else {
            result(state, PublishedOwnerMismatch)
          }
        | _ => result(state, InvalidSourceState)
      }
    }

  pure def applyReadiness(
    state: OwnershipState,
    region: int,
  ): ApplyResult =
    if (not(validRegion(region))) {
      result(state, RegionOutOfRange)
    } else if (region.in(PermanentRegions)) {
      result(state, PermanentRegion)
    } else {
      match state.regions.nth(region) {
        Dirty =>
          result(
            ownershipState(
              replaceRegion(state.regions, region, ErasedPrepared),
              state.tokens,
            ),
            OwnershipOk,
          )
        | _ => result(state, InvalidSourceState)
      }
    }

  pure def applyEvent(
    state: OwnershipState,
    event: OwnershipEvent,
  ): ApplyResult =
    match event {
      ReserveFact(requested) => applyReserve(state, requested)
      | PublishFact(publication) => applyPublish(state, publication)
      | ReleaseFact(release) => applyRelease(state, release)
      | ReadinessFact(region) => applyReadiness(state, region)
    }

  // A WAL record may publish several ownership changes atomically. Validation
  // is modeled as an ordered fold; mechanical preflight guarantees a durable
  // batch contains no failing suffix.
  pure def applyBatch(
    state: OwnershipState,
    events: List[OwnershipEvent],
  ): ApplyResult =
    events.foldl(result(state, OwnershipOk), (acc, event) =>
      if (acc.error == OwnershipOk) {
        applyEvent(acc.state, event)
      } else {
        acc
      })

  pure def replay(events: List[OwnershipEvent]): ApplyResult =
    applyBatch(InitialOwnership, events)

  pure val InitialOwnership =
    ownershipState(
      [
        Published(BootstrapOwner),
        Published(MainWalOwner),
        Published(CollectionOwner({ collection: 0, collectionType: 7 })),
        Published(CollectionOwner({ collection: 1, collectionType: 8 })),
        ErasedPrepared,
        ErasedPrepared,
      ],
      Set(),
    )

  pure def tokenCorrespondsToReservedState(state: OwnershipState): bool =
    state.tokens.forall(token =>
      validRegion(token.region)
      and state.regions.nth(token.region) == Reserved(token))
    and Regions.forall(region =>
      match state.regions.nth(region) {
        Reserved(actual) =>
          tokenPresent(state.tokens, actual) and actual.region == region
        | _ => state.tokens.forall(token => token.region != region)
      })

  pure def lifecyclePayloadsValid(state: OwnershipState): bool =
    Regions.forall(region =>
      match state.regions.nth(region) {
        Reserved(actual) => validPurpose(actual.purpose)
        | Published(owner) => validOwner(owner)
        | _ => true
      })

  pure def ownershipInvariant(state: OwnershipState): bool =
    state.regions.length() == RegionIndexes.length()
    and state.regions.nth(0) == Published(BootstrapOwner)
    and tokenCorrespondsToReservedState(state)
    and lifecyclePayloadsValid(state)
    and state.tokens.forall(left =>
      state.tokens.forall(right =>
        (left.region == right.region).implies(left == right)))

  pure def successfulBatchStep(
    before: OwnershipState,
    events: List[OwnershipEvent],
    after: OwnershipState,
  ): bool = {
    val applied = applyBatch(before, events)
    applied.error == OwnershipOk and applied.state == after
  }

  ////// Independently executable bounded abstract model //////

  pure val ReserveData4 =
    ReserveFact(reservation(4, CollectionDataPurpose(7), 100))
  pure val PublishData4 =
    PublishFact({
      reservation: reservation(4, CollectionDataPurpose(7), 100),
      owner: collectionOwner(0, 7),
    })
  pure val MismatchedPublish4 =
    PublishFact({
      reservation: reservation(4, CollectionDataPurpose(7), 999),
      owner: collectionOwner(0, 7),
    })
  pure val WrongOwnerPublish4 =
    PublishFact({
      reservation: reservation(4, CollectionDataPurpose(7), 100),
      owner: MainWalOwner,
    })
  pure val InvalidCollectionOwnerPublish4 =
    PublishFact({
      reservation: reservation(4, CollectionDataPurpose(7), 100),
      owner: collectionOwner(2, 7),
    })
  pure val InvalidPurposeReserve4 =
    ReserveFact(reservation(4, InvalidPurpose, 100))
  pure val ReleaseData4 =
    ReleaseFact({ region: 4, owner: collectionOwner(0, 7) })

  var ownership: OwnershipState
  var previousOwnership: OwnershipState
  var lastEvent: OwnershipEvent
  var lastError: OwnershipError
  var acceptedEvents: List[OwnershipEvent]
  var verificationDepth: int

  action init = all {
    ownership' = InitialOwnership,
    previousOwnership' = InitialOwnership,
    lastEvent' = ReadinessFact(NoIdentity),
    lastError' = OwnershipOk,
    acceptedEvents' = [],
    verificationDepth' = 0,
  }

  action ApplyCandidate(event: OwnershipEvent): bool = {
    val applied = applyEvent(ownership, event)
    all {
      verificationDepth < MaxVerificationDepth,
      previousOwnership' = ownership,
      ownership' = applied.state,
      lastEvent' = event,
      lastError' = applied.error,
      acceptedEvents' =
        if (applied.error == OwnershipOk) {
          acceptedEvents.append(event)
        } else {
          acceptedEvents
        },
      verificationDepth' = verificationDepth + 1,
    }
  }

  action Stutter = all {
    verificationDepth == MaxVerificationDepth,
    ownership' = ownership,
    previousOwnership' = previousOwnership,
    lastEvent' = lastEvent,
    lastError' = lastError,
    acceptedEvents' = acceptedEvents,
    verificationDepth' = verificationDepth,
  }

  action step = any {
    ApplyCandidate(ReserveData4),
    ApplyCandidate(PublishData4),
    ApplyCandidate(MismatchedPublish4),
    ApplyCandidate(WrongOwnerPublish4),
    ApplyCandidate(InvalidCollectionOwnerPublish4),
    ApplyCandidate(InvalidPurposeReserve4),
    ApplyCandidate(ReleaseData4),
    ApplyCandidate(ReadinessFact(4)),
    Stutter,
  }

  val lastStepMatchesReducer =
    verificationDepth == 0
    or {
      val applied = applyEvent(previousOwnership, lastEvent)
      applied.error == lastError and applied.state == ownership
    }

  val typedRejectionLeavesStateUnchanged =
    verificationDepth == 0
    or lastError == OwnershipOk
    or ownership == previousOwnership

  val coreOwn001ExactlyOneLifecycleState =
    ownership.regions.length() == RegionIndexes.length()

  val coreOwn002OwnerOnlyPublished =
    Regions.forall(region =>
      match ownership.regions.nth(region) {
        Published(owner) => validOwner(owner)
        | _ => true
      })

  val coreOwn003ReservationIdentityRetained =
    tokenCorrespondsToReservedState(ownership)

  val coreOwn004PurposeNamespacesDistinct =
    SystemPurposeOf(MainWalPurpose) != CollectionDataPurpose(0)

  val coreOwn005ReservationSourceChecked =
    verificationDepth == 0
    or match lastEvent {
      ReserveFact(requested) =>
        if (lastError == OwnershipOk) {
          validRegion(requested.region)
          and previousOwnership.regions.nth(requested.region) == ErasedPrepared
          and ownership.regions.nth(requested.region) == Reserved(requested)
          and requested.in(ownership.tokens)
        } else {
          ownership == previousOwnership
          and applyReserve(previousOwnership, requested).error == lastError
        }
      | _ => true
    }

  val coreOwn006PublicationAfterDurability =
    verificationDepth == 0
    or match lastEvent {
      PublishFact(publication) => {
        val requested = publication.reservation
        if (lastError == OwnershipOk) {
          validRegion(requested.region)
          and previousOwnership.regions.nth(requested.region)
            == Reserved(requested)
          and requested.in(previousOwnership.tokens)
          and purposeOwnerCompatible(requested.purpose, publication.owner)
          and ownership.regions.nth(requested.region)
            == Published(publication.owner)
          and not(requested.in(ownership.tokens))
        } else {
          ownership == previousOwnership
        }
      }
      | _ => true
    }

  val coreOwn007ReleaseProducesDirty =
    verificationDepth == 0
    or match lastEvent {
      ReleaseFact(release) =>
        if (lastError == OwnershipOk) {
          validRegion(release.region)
          and previousOwnership.regions.nth(release.region)
            == Published(release.owner)
          and ownership.regions.nth(release.region) == Dirty
        } else {
          ownership == previousOwnership
        }
      | _ => true
    }

  val coreOwn008ReadinessRequiresErase =
    verificationDepth == 0
    or match lastEvent {
      ReadinessFact(region) =>
        if (lastError == OwnershipOk) {
          validRegion(region)
          and previousOwnership.regions.nth(region) == Dirty
          and ownership.regions.nth(region) == ErasedPrepared
        } else {
          ownership == previousOwnership
        }
      | _ => true
    }

  val coreOwn009ForegroundReplayEquivalent = {
    val replayed = replay(acceptedEvents)
    replayed.error == OwnershipOk
    and replayed.state == ownership
    and lastStepMatchesReducer
    and typedRejectionLeavesStateUnchanged
  }

  val coreOwn010ReservationTokenLinear =
    tokenCorrespondsToReservedState(ownership)
    and ownership.tokens.forall(left =>
      ownership.tokens.forall(right =>
        (left.region == right.region).implies(left == right)))

  val coreOwn011PublicationIdentityChecked =
    purposeOwnerCompatible(
      SystemPurposeOf(TransactionLogPurpose(0)),
      TransactionOwner(0),
    )
    and not(
      purposeOwnerCompatible(
        SystemPurposeOf(TransactionLogPurpose(0)),
        TransactionOwner(1),
      )
    )
    and not(purposeOwnerCompatible(CollectionDataPurpose(7), collectionOwner(2, 7)))
    and (
      verificationDepth == 0
      or match lastEvent {
        PublishFact(publication) => {
          val applied = applyPublish(previousOwnership, publication)
          applied.error == lastError
          and applied.state == ownership
          and (lastError != OwnershipOk).implies(
            ownership == previousOwnership
          )
        }
        | _ => true
      }
    )

  val verificationDepthBounded =
    verificationDepth >= 0 and verificationDepth <= MaxVerificationDepth

  val safety =
    coreOwn001ExactlyOneLifecycleState
    and coreOwn002OwnerOnlyPublished
    and coreOwn003ReservationIdentityRetained
    and coreOwn004PurposeNamespacesDistinct
    and coreOwn005ReservationSourceChecked
    and coreOwn006PublicationAfterDurability
    and coreOwn007ReleaseProducesDirty
    and coreOwn008ReadinessRequiresErase
    and coreOwn009ForegroundReplayEquivalent
    and coreOwn010ReservationTokenLinear
    and coreOwn011PublicationIdentityChecked
    and verificationDepthBounded
    and ownershipInvariant(ownership)
}
