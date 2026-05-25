//! Optional compile-time performance metrics for storage hot paths.

use serde::Serialize;
use std::time::Instant;

/// Performance counters and accumulated timings for a [`Storage`](crate::Storage) instance.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StoragePerfMetrics {
    pub map_reads: u64,
    pub map_sets: u64,
    pub map_deletes: u64,
    pub frontier_cache_hits: u64,
    pub frontier_cache_misses: u64,
    pub frontier_reloads: u64,
    pub update_encodes: u64,
    pub encoded_update_bytes: u64,
    pub frontier_checkpoints: u64,
    pub frontier_applies: u64,
    pub overflow_flushes: u64,
    pub wal_records: u64,
    pub wal_update_records: u64,
    pub wal_bytes: u64,
    pub wal_rotations_attempted: u64,
    pub wal_rotations_completed: u64,
    pub runtime_reopens: u64,
    pub wal_rotation_remaining_bytes_total: u64,
    pub wal_rotation_remaining_bytes_min: u64,
    pub wal_rotation_reserve_bytes_total: u64,
    pub wal_rotation_alloc_begin_bytes_total: u64,
    pub wal_rotation_link_bytes_total: u64,
    pub wal_syncs: u64,
    pub wal_replay_reads: u64,
    pub wal_replay_read_bytes: u64,
    pub frontier_open_wal_scans: u64,
    pub wal_head_reclaim_copied_records: u64,
    pub compaction_checks: u64,
    pub compactions_run: u64,
    pub flushes: u64,
    pub reclaim_starts: u64,
    pub reclaim_ends: u64,
    pub committed_run_segments_checked: u64,
    pub committed_run_bounds_reads: u64,
    pub committed_run_snapshot_ref_reads: u64,
    pub committed_run_entry_reads: u64,
    pub committed_run_full_region_reads: u64,
    pub buffer_too_small_errors: u64,
    pub wal_rotation_required: u64,
    pub append_failures: u64,
    pub map_read_lookup_nanos: u128,
    pub update_encode_nanos: u128,
    pub frontier_checkpoint_nanos: u128,
    pub frontier_apply_nanos: u128,
    pub overflow_flush_nanos: u128,
    pub wal_encode_nanos: u128,
    pub wal_write_nanos: u128,
    pub wal_sync_nanos: u128,
    pub full_write_path_nanos: u128,
    pub compaction_check_nanos: u128,
    pub compaction_nanos: u128,
    pub flush_nanos: u128,
    pub reclaim_nanos: u128,
    pub wal_rotation_nanos: u128,
    pub mmap_flush_nanos: u128,
    pub file_sync_nanos: u128,
    pub dirty_sync_bytes: u64,
    pub dirty_sync_regions: u64,
    pub dirty_sync_metadata_regions: u64,
}

impl StoragePerfMetrics {
    pub(crate) fn increment(&mut self, field: StoragePerfCounter) {
        self.add(field, 1);
    }

    pub(crate) fn add(&mut self, field: StoragePerfCounter, value: u64) {
        match field {
            StoragePerfCounter::MapReads => self.map_reads = self.map_reads.saturating_add(value),
            StoragePerfCounter::MapSets => self.map_sets = self.map_sets.saturating_add(value),
            StoragePerfCounter::MapDeletes => {
                self.map_deletes = self.map_deletes.saturating_add(value);
            }
            StoragePerfCounter::FrontierCacheHits => {
                self.frontier_cache_hits = self.frontier_cache_hits.saturating_add(value);
            }
            StoragePerfCounter::FrontierCacheMisses => {
                self.frontier_cache_misses = self.frontier_cache_misses.saturating_add(value);
            }
            StoragePerfCounter::FrontierReloads => {
                self.frontier_reloads = self.frontier_reloads.saturating_add(value);
            }
            StoragePerfCounter::UpdateEncodes => {
                self.update_encodes = self.update_encodes.saturating_add(value);
            }
            StoragePerfCounter::EncodedUpdateBytes => {
                self.encoded_update_bytes = self.encoded_update_bytes.saturating_add(value);
            }
            StoragePerfCounter::FrontierCheckpoints => {
                self.frontier_checkpoints = self.frontier_checkpoints.saturating_add(value);
            }
            StoragePerfCounter::FrontierApplies => {
                self.frontier_applies = self.frontier_applies.saturating_add(value);
            }
            StoragePerfCounter::OverflowFlushes => {
                self.overflow_flushes = self.overflow_flushes.saturating_add(value);
            }
            StoragePerfCounter::WalRecords => {
                self.wal_records = self.wal_records.saturating_add(value);
            }
            StoragePerfCounter::WalUpdateRecords => {
                self.wal_update_records = self.wal_update_records.saturating_add(value);
            }
            StoragePerfCounter::WalBytes => self.wal_bytes = self.wal_bytes.saturating_add(value),
            StoragePerfCounter::WalRotationsAttempted => {
                self.wal_rotations_attempted = self.wal_rotations_attempted.saturating_add(value);
            }
            StoragePerfCounter::WalRotationsCompleted => {
                self.wal_rotations_completed = self.wal_rotations_completed.saturating_add(value);
            }
            StoragePerfCounter::RuntimeReopens => {
                self.runtime_reopens = self.runtime_reopens.saturating_add(value);
            }
            StoragePerfCounter::WalSyncs => self.wal_syncs = self.wal_syncs.saturating_add(value),
            StoragePerfCounter::WalReplayReads => {
                self.wal_replay_reads = self.wal_replay_reads.saturating_add(value);
            }
            StoragePerfCounter::WalReplayReadBytes => {
                self.wal_replay_read_bytes = self.wal_replay_read_bytes.saturating_add(value);
            }
            StoragePerfCounter::FrontierOpenWalScans => {
                self.frontier_open_wal_scans = self.frontier_open_wal_scans.saturating_add(value);
            }
            StoragePerfCounter::WalHeadReclaimCopiedRecords => {
                self.wal_head_reclaim_copied_records =
                    self.wal_head_reclaim_copied_records.saturating_add(value);
            }
            StoragePerfCounter::CompactionChecks => {
                self.compaction_checks = self.compaction_checks.saturating_add(value);
            }
            StoragePerfCounter::CompactionsRun => {
                self.compactions_run = self.compactions_run.saturating_add(value);
            }
            StoragePerfCounter::Flushes => self.flushes = self.flushes.saturating_add(value),
            StoragePerfCounter::ReclaimStarts => {
                self.reclaim_starts = self.reclaim_starts.saturating_add(value);
            }
            StoragePerfCounter::ReclaimEnds => {
                self.reclaim_ends = self.reclaim_ends.saturating_add(value);
            }
            StoragePerfCounter::CommittedRunSegmentsChecked => {
                self.committed_run_segments_checked =
                    self.committed_run_segments_checked.saturating_add(value);
            }
            StoragePerfCounter::CommittedRunBoundsReads => {
                self.committed_run_bounds_reads =
                    self.committed_run_bounds_reads.saturating_add(value);
            }
            StoragePerfCounter::CommittedRunSnapshotRefReads => {
                self.committed_run_snapshot_ref_reads =
                    self.committed_run_snapshot_ref_reads.saturating_add(value);
            }
            StoragePerfCounter::CommittedRunEntryReads => {
                self.committed_run_entry_reads =
                    self.committed_run_entry_reads.saturating_add(value);
            }
            StoragePerfCounter::BufferTooSmallErrors => {
                self.buffer_too_small_errors = self.buffer_too_small_errors.saturating_add(value);
            }
            StoragePerfCounter::WalRotationRequired => {
                self.wal_rotation_required = self.wal_rotation_required.saturating_add(value);
            }
            StoragePerfCounter::AppendFailures => {
                self.append_failures = self.append_failures.saturating_add(value);
            }
        }
    }

    pub(crate) fn add_nanos(&mut self, field: StoragePerfTimer, nanos: u128) {
        match field {
            StoragePerfTimer::MapReadLookup => {
                self.map_read_lookup_nanos = self.map_read_lookup_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::UpdateEncode => {
                self.update_encode_nanos = self.update_encode_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::FrontierCheckpoint => {
                self.frontier_checkpoint_nanos =
                    self.frontier_checkpoint_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::FrontierApply => {
                self.frontier_apply_nanos = self.frontier_apply_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::OverflowFlush => {
                self.overflow_flush_nanos = self.overflow_flush_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::WalEncode => {
                self.wal_encode_nanos = self.wal_encode_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::WalWrite => {
                self.wal_write_nanos = self.wal_write_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::WalSync => {
                self.wal_sync_nanos = self.wal_sync_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::FullWritePath => {
                self.full_write_path_nanos = self.full_write_path_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::CompactionCheck => {
                self.compaction_check_nanos = self.compaction_check_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::Compaction => {
                self.compaction_nanos = self.compaction_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::Flush => {
                self.flush_nanos = self.flush_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::Reclaim => {
                self.reclaim_nanos = self.reclaim_nanos.saturating_add(nanos);
            }
            StoragePerfTimer::WalRotation => {
                self.wal_rotation_nanos = self.wal_rotation_nanos.saturating_add(nanos);
            }
        }
    }

    pub(crate) fn observe_wal_rotation_window(
        &mut self,
        remaining_bytes: u64,
        alloc_begin_bytes: u64,
        link_bytes: u64,
        reserve_bytes: u64,
    ) {
        self.wal_rotation_remaining_bytes_total = self
            .wal_rotation_remaining_bytes_total
            .saturating_add(remaining_bytes);
        self.wal_rotation_remaining_bytes_min = if self.wal_rotation_remaining_bytes_min == 0 {
            remaining_bytes
        } else {
            self.wal_rotation_remaining_bytes_min.min(remaining_bytes)
        };
        self.wal_rotation_alloc_begin_bytes_total = self
            .wal_rotation_alloc_begin_bytes_total
            .saturating_add(alloc_begin_bytes);
        self.wal_rotation_link_bytes_total = self
            .wal_rotation_link_bytes_total
            .saturating_add(link_bytes);
        self.wal_rotation_reserve_bytes_total = self
            .wal_rotation_reserve_bytes_total
            .saturating_add(reserve_bytes);
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StoragePerfCounter {
    MapReads,
    MapSets,
    MapDeletes,
    FrontierCacheHits,
    FrontierCacheMisses,
    FrontierReloads,
    UpdateEncodes,
    EncodedUpdateBytes,
    FrontierCheckpoints,
    FrontierApplies,
    OverflowFlushes,
    WalRecords,
    WalUpdateRecords,
    WalBytes,
    WalRotationsAttempted,
    WalRotationsCompleted,
    RuntimeReopens,
    WalSyncs,
    WalReplayReads,
    WalReplayReadBytes,
    FrontierOpenWalScans,
    WalHeadReclaimCopiedRecords,
    CompactionChecks,
    CompactionsRun,
    Flushes,
    ReclaimStarts,
    ReclaimEnds,
    CommittedRunSegmentsChecked,
    CommittedRunBoundsReads,
    CommittedRunSnapshotRefReads,
    CommittedRunEntryReads,
    BufferTooSmallErrors,
    WalRotationRequired,
    AppendFailures,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StoragePerfTimer {
    MapReadLookup,
    UpdateEncode,
    FrontierCheckpoint,
    FrontierApply,
    OverflowFlush,
    WalEncode,
    WalWrite,
    WalSync,
    FullWritePath,
    CompactionCheck,
    Compaction,
    Flush,
    Reclaim,
    WalRotation,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StoragePerfTimerGuard {
    start: Instant,
}

impl StoragePerfTimerGuard {
    pub(crate) fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub(crate) fn elapsed_nanos(self) -> u128 {
        self.start.elapsed().as_nanos()
    }
}
