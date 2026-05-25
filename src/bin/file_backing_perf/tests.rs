use super::*;

#[test]
fn sync_audit_records_unexpected_hot_write_violation() {
    let mut collector = SyncAuditCollector::default();
    collector.observe(
        7,
        WorkloadOp::Set,
        SyncAuditDelta {
            wal_records: 1,
            wal_syncs: 2,
            io_region_writes: 1,
            io_syncs: 2,
            ..SyncAuditDelta::default()
        },
    );

    assert_eq!(collector.write_operations, 1);
    assert_eq!(collector.non_hot_write_exceptions, 0);
    assert_eq!(collector.first_violations.len(), 1);
    assert!(!collector.first_violations[0].expected_exception);
    assert!(collector.first_violations[0]
        .reasons
        .iter()
        .any(|reason| reason.contains("wal_syncs=2")));
}

#[test]
fn sync_audit_marks_frontier_flush_as_expected_exception() {
    let mut collector = SyncAuditCollector::default();
    collector.observe(
        11,
        WorkloadOp::Set,
        SyncAuditDelta {
            wal_records: 3,
            wal_syncs: 3,
            io_region_writes: 5,
            io_syncs: 3,
            flushes: 1,
            overflow_flushes: 1,
            ..SyncAuditDelta::default()
        },
    );

    assert_eq!(collector.write_operations, 1);
    assert_eq!(collector.non_hot_write_exceptions, 1);
    assert_eq!(collector.first_violations.len(), 1);
    assert!(collector.first_violations[0].expected_exception);
}

#[test]
fn sync_audit_marks_wal_rotation_as_expected_exception() {
    let mut collector = SyncAuditCollector::default();
    collector.observe(
        13,
        WorkloadOp::Set,
        SyncAuditDelta {
            wal_records: 2,
            wal_syncs: 2,
            io_region_writes: 3,
            io_syncs: 2,
            wal_rotations_attempted: 1,
            wal_rotations_completed: 1,
            wal_rotation_required: 1,
            ..SyncAuditDelta::default()
        },
    );

    assert_eq!(collector.write_operations, 1);
    assert_eq!(collector.non_hot_write_exceptions, 1);
    assert_eq!(collector.first_violations.len(), 1);
    assert!(collector.first_violations[0].expected_exception);
}
