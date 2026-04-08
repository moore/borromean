# Implementation Policy

## Purpose

This specification defines repository traceability policy and
documentation-format rules for the borromean implementation
specifications and their verification harnesses.
[spec/ring.md](ring.md) and
[spec/implementation.md](implementation.md) remain the source of truth
for storage semantics and executable implementation constraints.

## Requirements Format

This policy specification keeps normative requirements adjacent to the
text that motivates them.

1. `RING-IMPL-FORMAT-001` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST start with a stable identifier such as `RING-IMPL-CORE-001`.
2. `RING-IMPL-FORMAT-002` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST use explicit RFC-2119 normative language.

These identifiers are intended to be Duvet traceability targets for
implementation-architecture decisions and repository policy
constraints that are not themselves on-disk format requirements.

## Verification Strategy

Requirement traceability should stay mechanically simple. If a single
test body tries to verify many independent requirements at once, the
trace becomes noisy and failure diagnosis gets worse. A tighter rule is
better: one requirement, one dedicated test function, with any shared
setup moved into helpers.

This does not forbid helper code, fixtures, macros, or shared property
generators. It only constrains how normative requirements are claimed
by top-level tests.

Verification structure should also preserve a clean separation between
functional code and test code. Requirement-specific tests are part of
the verification surface, not part of the production implementation
module they exercise.

### Verification Requirements

1. `RING-IMPL-TEST-001` Every normative requirement in
[spec/ring.md](ring.md) or [spec/implementation.md](implementation.md)
MUST have at least one dedicated automated test function or dedicated
compile-time test case whose primary purpose is to verify that single
requirement.
2. `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
claim to verify multiple normative requirement identifiers.
3. `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
macros, and data generators MAY be reused across requirement-specific
tests, but the final traced test entry point MUST remain specific to
one requirement identifier.
4. `RING-IMPL-TEST-004` When a requirement is verified by a
compile-fail, compile-pass, or other non-runtime harness, that harness
entry MUST still be dedicated to a single requirement identifier.
5. `RING-IMPL-TEST-005` Automated test functions and compile-time test
harness entries MUST be defined only in dedicated test modules or
files rather than inside the functional implementation module they
exercise.
