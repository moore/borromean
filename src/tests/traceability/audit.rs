use super::*;

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or this specification MUST have at least one
//# dedicated automated test function or dedicated compile-time test case
//# whose primary purpose is to verify that single requirement.
//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or this specification MUST have at least one
//# dedicated automated test function or dedicated compile-time test case
//# whose primary purpose is to verify that single requirement.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-001` Every normative requirement in
//# [spec/ring.md](ring.md) or this specification MUST have at least one
//# dedicated automated test function or dedicated compile-time test case
//# whose primary purpose is to verify that single requirement.
#[test]
fn every_normative_requirement_has_a_dedicated_test_or_harness_entry() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut traced_files = Vec::new();
    collect_rust_sources(&repo_root.join("src"), &mut traced_files);
    let mut covered = BTreeSet::new();

    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            collect_rust_sources(&root, &mut traced_files);
        }
    }

    for path in traced_files {
        let source = fs::read_to_string(&path).unwrap();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                covered.insert(id);
            }
        }
    }

    let mut missing = Vec::new();
    for spec in ["spec/ring.md", "spec/implementation.md"] {
        for id in collect_normative_requirement_ids(&repo_root.join(spec)) {
            if !covered.contains(&id) {
                missing.push(id);
            }
        }
    }

    assert!(
        missing.is_empty(),
        "normative requirements without dedicated test or harness coverage: {missing:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test
//# harness entries MUST be defined only in dedicated test modules or
//# files rather than inside the functional implementation module they
//# exercise.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-005` Automated test functions and compile-time test
//# harness entries MUST be defined only in dedicated test modules or
//# files rather than inside the functional implementation module they
//# exercise.
#[test]
fn automated_tests_live_only_in_dedicated_test_modules() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_sources(&src_root, &mut files);

    let mut offenders = Vec::new();
    for path in files {
        if is_dedicated_test_file(&path) {
            continue;
        }

        let source = fs::read_to_string(&path).unwrap();
        if source.contains("mod tests {") || source.contains("#[test]") {
            offenders.push(path.strip_prefix(&src_root).unwrap().display().to_string());
        }
    }

    assert!(
        offenders.is_empty(),
        "non-test source files still contain inline test bodies: {offenders:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
//# claim to verify multiple normative requirement identifiers.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-002` A top-level automated test function MUST NOT
//# claim to verify multiple normative requirement identifiers.
#[test]
fn top_level_automated_tests_claim_at_most_one_requirement_identifier() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root) {
        for entry in parse_test_entries(&path) {
            if entry.requirement_ids.len() > 1 {
                offenders.push(format!("{} -> {:?}", entry.location, entry.requirement_ids));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "top-level test functions still claim multiple requirements: {offenders:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
//# macros, and data generators MAY be reused across requirement-specific
//# tests, but the final traced test entry point MUST remain specific to
//# one requirement identifier.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-003` Shared setup, fixtures, helper functions,
//# macros, and data generators MAY be reused across requirement-specific
//# tests, but the final traced test entry point MUST remain specific to
//# one requirement identifier.
#[test]
fn shared_test_helpers_remain_untraced() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    for path in collect_dedicated_test_files(&src_root) {
        for helper in parse_traced_helpers(&path) {
            offenders.push(format!(
                "{} -> {:?}",
                helper.location, helper.requirement_ids
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "shared helpers or fixtures still carry traced requirement ids: {offenders:?}"
    );
}

//= spec/implementation.md#verification-requirements
//# `RING-IMPL-TEST-004` When a requirement is verified by a
//# compile-fail, compile-pass, or other non-runtime harness, that harness
//# entry MUST still be dedicated to a single requirement identifier.
//= spec/implementation.md#verification-requirements
//= type=test
//# `RING-IMPL-TEST-004` When a requirement is verified by a
//# compile-fail, compile-pass, or other non-runtime harness, that harness
//# entry MUST still be dedicated to a single requirement identifier.
#[test]
fn non_runtime_harness_entries_claim_at_most_one_requirement_when_present() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut harness_files = Vec::new();
    for relative in ["tests", "ui", "compile"] {
        let root = repo_root.join(relative);
        if root.exists() {
            collect_rust_sources(&root, &mut harness_files);
        }
    }

    let mut offenders = Vec::new();
    for path in harness_files {
        let source = fs::read_to_string(&path).unwrap();
        let mut ids = BTreeSet::new();
        for line in source.lines() {
            if !line.trim().starts_with("//#") {
                continue;
            }
            for id in extract_requirement_ids(line) {
                ids.insert(id);
            }
        }
        if ids.len() > 1 {
            offenders.push(format!("{} -> {:?}", path.display(), ids));
        }
    }

    assert!(
        offenders.is_empty(),
        "non-runtime harness entries still claim multiple requirements: {offenders:?}"
    );
}
