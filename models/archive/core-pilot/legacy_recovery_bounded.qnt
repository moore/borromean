// Archived core-pilot model. It is not current design authority.
// Bounded executable wrapper for the unchanged legacy recovery model.
//
// Quint 0.32 does not forward --max-steps to TLC. This module preserves the
// legacy init, transition relation, and safety predicate exactly while adding
// finite verification fuel around them.

module legacy_recovery_bounded {
  import transaction_free_recovery as Legacy from "../../transaction_free_recovery"

  pure val VerificationBound = 8

  var verificationDepth: int

  action init = all {
    Legacy::init,
    verificationDepth' = 0,
  }

  action step =
    if (verificationDepth < VerificationBound) {
      all {
        Legacy::step,
        verificationDepth' = verificationDepth + 1,
      }
    } else {
      all {
        Legacy::Stutter,
        verificationDepth' = verificationDepth,
      }
    }

  val safety =
    Legacy::safety
    and verificationDepth >= 0
    and verificationDepth <= VerificationBound
}
