use super::*;

//= spec/implementation.md#requirements-format
//# Each normative requirement starts with a stable
//# identifier such as `RING-IMPL-CORE-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
//= spec/implementation.md#requirements-format
//= type=test
//# Each normative requirement starts with a stable
//# identifier such as `RING-IMPL-CORE-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
#[test]
fn implementation_spec_requirements_use_stable_identifiers_and_normative_language() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert_spec_requirement_format(&repo_root.join("spec/implementation.md"), "RING-IMPL-");
}

//= spec/ring.md#requirements-format
//# Each normative requirement starts with a stable
//# identifier such as `RING-WAL-ENC-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
//= spec/ring.md#requirements-format
//= type=test
//# Each normative requirement starts with a stable
//# identifier such as `RING-WAL-ENC-001` and uses explicit normative
//# language such as `MUST`, `MUST NOT`, `SHOULD`, or `MAY`.
#[test]
fn ring_spec_requirements_use_stable_identifiers_and_normative_language() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert_spec_requirement_format(&repo_root.join("spec/ring.md"), "RING-");
}
