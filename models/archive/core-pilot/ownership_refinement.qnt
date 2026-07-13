// Archived core-pilot model. It is not current design authority.
// Forward-refinement bridge from storage_mechanical to region_ownership.
//
// Neither model depends on the other. This proof module imports both, maps the
// recoverable mechanical image to one abstract ownership state, and records the
// pre-step projection so a state invariant can check every mechanical step.

module ownership_refinement {
  import region_ownership as Abstract from "./region_ownership"
  import storage_mechanical as Mechanical from "./storage_mechanical"
  import region_ownership.Purpose from "./region_ownership"
  import region_ownership.Owner from "./region_ownership"
  import region_ownership.Reservation from "./region_ownership"
  import region_ownership.OwnershipEvent from "./region_ownership"
  import region_ownership.RegionState from "./region_ownership"
  import region_ownership.OwnershipState from "./region_ownership"
  import region_ownership.ApplyResult from "./region_ownership"
  import region_ownership.OwnershipError from "./region_ownership"
  import storage_mechanical.PurposeKind from "./storage_mechanical"
  import storage_mechanical.OwnerKind from "./storage_mechanical"
  import storage_mechanical.Token from "./storage_mechanical"
  import storage_mechanical.MechanicalEvent from "./storage_mechanical"
  import storage_mechanical.DurableCommand from "./storage_mechanical"
  import storage_mechanical.RuntimeState from "./storage_mechanical"
  import storage_mechanical.MechanicalState from "./storage_mechanical"
  import storage_mechanical.MechanicalError from "./storage_mechanical"
  import storage_mechanical.CommandKind from "./storage_mechanical"

  // Quint 0.32's TLC adapter ignores --max-steps. Each executable refinement
  // wrapper below carries explicit model fuel so TLC explores a finite prefix.
  pure val ReservationPublicationVerificationBound = 13
  pure val ReleaseReuseVerificationBound = 8
  pure val TransactionVerificationBound = 21
  pure val CrashReplayVerificationBound = 10
  pure val FullRefinementVerificationBound = 6
  pure val MaxRefinementVerificationBound = 21

  pure def abstractPurpose(
    purposeKind: PurposeKind,
    purposeIdentity: int,
  ): Purpose =
    if (purposeKind == Mechanical::MNoPurpose) {
      Abstract::InvalidPurpose
    } else if (purposeKind == Mechanical::MMainWal) {
      Abstract::SystemPurposeOf(Abstract::MainWalPurpose)
    } else if (purposeKind == Mechanical::MTransactionLog) {
      Abstract::SystemPurposeOf(
        Abstract::TransactionLogPurpose(purposeIdentity)
      )
    } else if (purposeKind == Mechanical::MFreeBasis) {
      Abstract::SystemPurposeOf(Abstract::FreeBasisPurpose)
    } else if (purposeKind == Mechanical::MBootstrap) {
      Abstract::SystemPurposeOf(Abstract::BootstrapPurpose)
    } else if (purposeKind == Mechanical::MCollectionData) {
      Abstract::CollectionDataPurpose(purposeIdentity)
    } else {
      Abstract::InvalidPurpose
    }

  pure def abstractOwner(
    ownerKind: OwnerKind,
    ownerIdentity: int,
    collectionType: int,
  ): Owner =
    if (ownerKind == Mechanical::MNoOwner) {
      Abstract::InvalidOwner
    } else if (ownerKind == Mechanical::MMainWalOwner) {
      Abstract::MainWalOwner
    } else if (ownerKind == Mechanical::MTransactionOwner) {
      Abstract::TransactionOwner(ownerIdentity)
    } else if (ownerKind == Mechanical::MFreeBasisOwner) {
      Abstract::FreeBasisOwner
    } else if (ownerKind == Mechanical::MBootstrapOwner) {
      Abstract::BootstrapOwner
    } else if (ownerKind == Mechanical::MCollectionOwner) {
      Abstract::collectionOwner(ownerIdentity, collectionType)
    } else {
      Abstract::InvalidOwner
    }

  pure def abstractReservation(
    region: int,
    purposeKind: PurposeKind,
    purposeIdentity: int,
    operation: int,
  ): Reservation =
    Abstract::reservation(
      region,
      abstractPurpose(purposeKind, purposeIdentity),
      operation,
    )

  pure def abstractToken(
    token: Token,
  ): Reservation =
    abstractReservation(
      token.region,
      token.purposeKind,
      token.purposeIdentity,
      token.operation,
    )

  pure def abstractEvent(
    mechanicalEvent: MechanicalEvent,
  ): OwnershipEvent = {
    val reservation =
      abstractReservation(
        mechanicalEvent.region,
        mechanicalEvent.purposeKind,
        mechanicalEvent.purposeIdentity,
        mechanicalEvent.operation,
      )
    val owner =
      abstractOwner(
        mechanicalEvent.ownerKind,
        mechanicalEvent.ownerIdentity,
        mechanicalEvent.collectionType,
      )
    if (mechanicalEvent.kind == Mechanical::MReserveFact) {
      Abstract::ReserveFact(reservation)
    } else if (mechanicalEvent.kind == Mechanical::MPublishFact) {
      Abstract::PublishFact({ reservation: reservation, owner: owner })
    } else if (mechanicalEvent.kind == Mechanical::MReleaseFact) {
      Abstract::ReleaseFact({ region: mechanicalEvent.region, owner: owner })
    } else {
      Abstract::ReadinessFact(mechanicalEvent.region)
    }
  }

  pure def appendAbstractEvents(
    existing: List[OwnershipEvent],
    mechanicalEvents: List[MechanicalEvent],
  ): List[OwnershipEvent] =
    mechanicalEvents.foldl(existing, (events, nextEvent) =>
      events.append(abstractEvent(nextEvent)))

  pure def abstractEvents(
    commands: List[DurableCommand],
  ): List[OwnershipEvent] =
    commands.foldl([], (events, command) =>
      appendAbstractEvents(events, command.events))

  pure def abstractRegion(
    runtime: RuntimeState,
    regionIndex: int,
  ): RegionState = {
    val current = runtime.regions.nth(regionIndex)
    val reservation =
      abstractReservation(
        regionIndex,
        current.purposeKind,
        current.purposeIdentity,
        current.operation,
      )
    val owner =
      abstractOwner(
        current.ownerKind,
        current.ownerIdentity,
        current.collectionType,
      )
    if (current.phase == Mechanical::MErasedPrepared) {
      Abstract::ErasedPrepared
    } else if (current.phase == Mechanical::MReserved) {
      Abstract::Reserved(reservation)
    } else if (current.phase == Mechanical::MPublished) {
      Abstract::Published(owner)
    } else {
      Abstract::Dirty
    }
  }

  pure def abstractRuntime(
    runtime: RuntimeState,
  ): OwnershipState = {
    val regions =
      Mechanical::RegionIndexes.foldl([], (mapped, regionIndex) =>
        mapped.append(abstractRegion(runtime, regionIndex)))
    val tokens =
      Mechanical::RegionIndexes.foldl(Set(), (mapped, regionIndex) => {
        val current = runtime.regions.nth(regionIndex)
        val mechanicalToken =
          Mechanical::token(
            regionIndex,
            current.purposeKind,
            current.purposeIdentity,
            current.operation,
          )
        if (mechanicalToken.in(runtime.tokens)) {
          mapped.union(Set(abstractToken(mechanicalToken)))
        } else {
          mapped
        }
      })
    Abstract::ownershipState(regions, tokens)
  }

  pure def abstractDurable(
    state: MechanicalState,
  ): ApplyResult =
    Abstract::replay(abstractEvents(state.durableCommands))

  pure def abstractError(
    error: MechanicalError,
  ): OwnershipError =
    if (error == Mechanical::MRegionOutOfRange) {
      Abstract::RegionOutOfRange
    } else if (error == Mechanical::MInvalidSourceState) {
      Abstract::InvalidSourceState
    } else if (error == Mechanical::MReservationMismatch) {
      Abstract::ReservationMismatch
    } else if (error == Mechanical::MReservationTokenMissing) {
      Abstract::ReservationTokenMissing
    } else if (error == Mechanical::MPurposeOwnerMismatch) {
      Abstract::PurposeOwnerMismatch
    } else if (error == Mechanical::MPublishedOwnerMismatch) {
      Abstract::PublishedOwnerMismatch
    } else if (error == Mechanical::MPermanentRegion) {
      Abstract::PermanentRegion
    } else if (error == Mechanical::MPurposeInvalid) {
      Abstract::PurposeInvalid
    } else {
      Abstract::OwnershipOk
    }

  pure def commandKindForEvent(
    mechanicalEvent: MechanicalEvent,
  ): CommandKind =
    if (mechanicalEvent.kind == Mechanical::MReserveFact) {
      Mechanical::MReserveCommand
    } else if (mechanicalEvent.kind == Mechanical::MPublishFact) {
      Mechanical::MPublishCommand
    } else if (mechanicalEvent.kind == Mechanical::MReleaseFact) {
      Mechanical::MReleaseCommand
    } else {
      Mechanical::MReadinessCommand
    }

  // State carried only by the proof. previousAbstract is the durable abstract
  // projection before the most recent mechanical step.
  var previousAbstract: OwnershipState
  var previousDurableLength: int

  action init = all {
    Mechanical::init,
    previousAbstract' = Abstract::InitialOwnership,
    previousDurableLength' = 0,
  }

  action unboundedStep = all {
    previousAbstract' = abstractDurable(Mechanical::mechanical).state,
    previousDurableLength' = Mechanical::mechanical.durableCommands.length(),
    Mechanical::unboundedStep,
  }

  action captureBeforeStep = all {
    previousAbstract' = abstractDurable(Mechanical::mechanical).state,
    previousDurableLength' = Mechanical::mechanical.durableCommands.length(),
  }

  action unboundedReservationPublicationStep = all {
    captureBeforeStep,
    any {
      Mechanical::IssueCollectionAllocation,
      Mechanical::SyncEvents,
      Mechanical::ApplyDurableEvents,
      Mechanical::IssueContent,
      Mechanical::SyncContent,
      Mechanical::IssuePublication,
      Mechanical::IssuePublicationPair,
      Mechanical::RejectMismatchedPublication,
      Mechanical::RejectIncompatibleOwnerPublication,
      Mechanical::RejectOutOfRangeCollectionOwner,
      Mechanical::RejectInvalidPurposeReservation,
      Mechanical::Stutter,
    },
  }

  action unboundedReleaseReuseStep = all {
    captureBeforeStep,
    any {
      Mechanical::IssueRelease,
      Mechanical::SyncEvents,
      Mechanical::ApplyDurableEvents,
      Mechanical::IssueErase,
      Mechanical::CompleteErase,
      Mechanical::IssueReadiness,
      Mechanical::Stutter,
    },
  }

  action unboundedTransactionStep = all {
    captureBeforeStep,
    any {
      Mechanical::BeginTransaction,
      Mechanical::EnrollCollection,
      Mechanical::StageFreeIntent,
      Mechanical::SealPrivateSuffix,
      Mechanical::IssueTransactionAllocation,
      Mechanical::SyncEvents,
      Mechanical::ApplyDurableEvents,
      Mechanical::IssueContent,
      Mechanical::SyncContent,
      Mechanical::IssuePublication,
      Mechanical::IssueCommit,
      Mechanical::IssueRollback,
      Mechanical::IssueCleanup,
      Mechanical::IssueFinish,
      Mechanical::RejectWrongTransactionOwnerPublication,
      Mechanical::Stutter,
    },
  }

  action unboundedCrashReplayStep = all {
    captureBeforeStep,
    any {
      Mechanical::IssueCollectionAllocation,
      Mechanical::SyncEvents,
      Mechanical::ApplyDurableEvents,
      Mechanical::IssueContent,
      Mechanical::SyncContent,
      Mechanical::IssuePublication,
      Mechanical::IssueRelease,
      Mechanical::IssueErase,
      Mechanical::CompleteErase,
      Mechanical::IssueReadiness,
      Mechanical::CrashDropsUnsynced,
      Mechanical::CrashKeepsCompleteUnsynced,
      Mechanical::CrashKeepsTornUnsynced,
      Mechanical::Stutter,
    },
  }

  action refinementStutter = all {
    previousAbstract' = previousAbstract,
    previousDurableLength' = previousDurableLength,
    Mechanical::mechanical' = Mechanical::mechanical,
    Mechanical::verificationDepth' = Mechanical::verificationDepth,
  }

  action reservationPublicationStep =
    if (
      Mechanical::verificationDepth
        < ReservationPublicationVerificationBound
    ) {
      all {
        unboundedReservationPublicationStep,
        Mechanical::verificationDepth' = Mechanical::verificationDepth + 1,
      }
    } else {
      refinementStutter
    }

  action releaseReuseStep =
    if (Mechanical::verificationDepth < ReleaseReuseVerificationBound) {
      all {
        unboundedReleaseReuseStep,
        Mechanical::verificationDepth' = Mechanical::verificationDepth + 1,
      }
    } else {
      refinementStutter
    }

  action transactionStep =
    if (Mechanical::verificationDepth < TransactionVerificationBound) {
      all {
        unboundedTransactionStep,
        Mechanical::verificationDepth' = Mechanical::verificationDepth + 1,
      }
    } else {
      refinementStutter
    }

  action crashReplayStep =
    if (Mechanical::verificationDepth < CrashReplayVerificationBound) {
      all {
        unboundedCrashReplayStep,
        Mechanical::verificationDepth' = Mechanical::verificationDepth + 1,
      }
    } else {
      refinementStutter
    }

  action step =
    if (Mechanical::verificationDepth < FullRefinementVerificationBound) {
      all {
        unboundedStep,
        Mechanical::verificationDepth' = Mechanical::verificationDepth + 1,
      }
    } else {
      refinementStutter
    }

  val initialStateRefines =
    abstractDurable(Mechanical::mechanical).error == Abstract::OwnershipOk
    and (Mechanical::mechanical.durableCommands.length() == 0).implies(
      abstractDurable(Mechanical::mechanical).state
        == Abstract::InitialOwnership
    )

  val mechanicalStepRefines = {
    val newCommands =
      Mechanical::mechanical.durableCommands.slice(
        previousDurableLength,
        Mechanical::mechanical.durableCommands.length()
      )
    val applied =
      Abstract::applyBatch(previousAbstract, abstractEvents(newCommands))
    applied.error == Abstract::OwnershipOk
    and applied.state == abstractDurable(Mechanical::mechanical).state
  }

  val runtimeAgreesWithDurableProjection = {
    val runtimeState = abstractRuntime(Mechanical::mechanical.runtime)
    val durableState = abstractDurable(Mechanical::mechanical)
    val pending =
      Abstract::applyBatch(
        runtimeState,
        appendAbstractEvents([], Mechanical::mechanical.pendingCommand.events),
      )
    durableState.error == Abstract::OwnershipOk
    and if (Mechanical::mechanical.phase == Mechanical::MEventsDurable) {
      pending.error == Abstract::OwnershipOk
      and pending.state == durableState.state
    } else {
      runtimeState == durableState.state
    }
  }

  val rejectedEventMatchesAbstractError = {
    val attemptedEvent = Mechanical::mechanical.lastAttemptedEvent
    val attempted =
      Abstract::applyEvent(
        abstractRuntime(Mechanical::mechanical.runtime),
        abstractEvent(attemptedEvent),
      )
    val attemptedCommand =
      Mechanical::command(
        commandKindForEvent(attemptedEvent),
        attemptedEvent.transaction,
        [attemptedEvent],
      )
    val mechanicalReplay =
      Mechanical::replay(
        Mechanical::mechanical.durableCommands.append(attemptedCommand)
      )
    val abstractReplay =
      Abstract::replay(
        abstractEvents(Mechanical::mechanical.durableCommands)
          .append(abstractEvent(attemptedEvent))
      )
    if (
      Mechanical::mechanical.lastError == Mechanical::MOk
      or attemptedEvent.region == Mechanical::NoneId
    ) {
      true
    } else {
      attempted.state == abstractRuntime(Mechanical::mechanical.runtime)
      and attempted.error == abstractError(Mechanical::mechanical.lastError)
      and mechanicalReplay.error == Mechanical::mechanical.lastError
      and Mechanical::sameOwnership(
        mechanicalReplay.runtime,
        Mechanical::mechanical.runtime,
      )
      and abstractReplay.error == attempted.error
      and abstractReplay.state
        == abstractDurable(Mechanical::mechanical).state
    }
  }

  val coreOwn001ExactlyOneLifecycleState =
    abstractDurable(Mechanical::mechanical).state.regions.length()
      == Abstract::RegionIndexes.length()

  val coreOwn002OwnerOnlyPublished =
    Abstract::ownershipInvariant(abstractDurable(Mechanical::mechanical).state)

  val coreOwn003ReservationIdentityRetained =
    Abstract::tokenCorrespondsToReservedState(
      abstractDurable(Mechanical::mechanical).state,
    )

  val coreOwn004PurposeNamespacesDistinct =
    Abstract::SystemPurposeOf(Abstract::MainWalPurpose)
      != Abstract::CollectionDataPurpose(0)

  val coreOwn005ReservationSourceChecked =
    mechanicalStepRefines

  val coreOwn006PublicationAfterDurability =
    mechanicalStepRefines
    and Mechanical::everyDurablePublicationHasContent(Mechanical::mechanical)
    and runtimeAgreesWithDurableProjection

  val coreOwn007ReleaseProducesDirty =
    mechanicalStepRefines

  val coreOwn008ReadinessRequiresErase =
    mechanicalStepRefines
    and Mechanical::everyDurableReadinessHasErase(Mechanical::mechanical)

  val coreOwn009ForegroundReplayEquivalent =
    runtimeAgreesWithDurableProjection
    and rejectedEventMatchesAbstractError

  val coreOwn010ReservationTokenLinear =
    Abstract::tokenCorrespondsToReservedState(
      abstractRuntime(Mechanical::mechanical.runtime),
    )

  val coreOwn011PublicationIdentityChecked =
    mechanicalStepRefines
    and rejectedEventMatchesAbstractError

  val safety =
    initialStateRefines
    and Mechanical::mechanicalWellFormed
    and coreOwn001ExactlyOneLifecycleState
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
    and Mechanical::verificationDepth >= 0
    and Mechanical::verificationDepth <= MaxRefinementVerificationBound
}
