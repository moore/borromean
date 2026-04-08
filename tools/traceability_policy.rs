//= spec/implementation-policy.md#requirements-format
//# `RING-IMPL-FORMAT-001` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST start with a stable identifier such as `RING-IMPL-CORE-001`.
const REQUIREMENT_STABLE_ID_POLICY: () = ();
// `src/bin/traceability_audit.rs` enforces this against
// `spec/implementation.md` and `spec/implementation-policy.md`.

//= spec/implementation-policy.md#requirements-format
//# `RING-IMPL-FORMAT-002` Each normative requirement in [spec/implementation.md](implementation.md) or this specification MUST use explicit RFC-2119 normative language.
const REQUIREMENT_NORMATIVE_LANGUAGE_POLICY: () = ();
// `src/bin/traceability_audit.rs` enforces these format checks against
// `spec/implementation.md` and `spec/implementation-policy.md`.

//= spec/implementation-policy.md#verification-requirements
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or [spec/implementation.md](implementation.md)
//# MUST have at least one dedicated automated test function or dedicated
//# compile-time test case whose primary purpose is to verify that single
//# requirement.
const TEST_COVERAGE_POLICY: () = ();
// `scripts/verify.sh` runs the test-backed Duvet pass with
// `--require-tests true`.

//= spec/implementation-policy.md#verification-requirements
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
//# claim to verify multiple normative requirement identifiers.
const SINGLE_REQUIREMENT_TEST_POLICY: () = ();

//= spec/implementation-policy.md#verification-requirements
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
//# macros, and data generators MAY be reused across requirement-specific
//# tests, but the final traced test entry point MUST remain specific to
//# one requirement identifier.
const UNTRACED_HELPER_POLICY: () = ();

//= spec/implementation-policy.md#verification-requirements
//# `RING-IMPL-TEST-004` When a requirement is verified by a
//# compile-fail, compile-pass, or other non-runtime harness, that harness
//# entry MUST still be dedicated to a single requirement identifier.
const SINGLE_REQUIREMENT_HARNESS_POLICY: () = ();

//= spec/implementation-policy.md#verification-requirements
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test
//# harness entries MUST be defined only in dedicated test modules or
//# files rather than inside the functional implementation module they
//# exercise.
const DEDICATED_TEST_MODULE_POLICY: () = ();
// `src/bin/traceability_audit.rs` enforces `RING-IMPL-TEST-002`
// through `RING-IMPL-TEST-005`, and `scripts/verify.sh` runs that
// audit as part of repository verification.
