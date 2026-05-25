use std::cell::RefCell;
use std::env;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::rc::Rc;
use std::time::{Duration, Instant};

use borromean::{
    AllocationPolicy, CollectionId, FileBacking, FileBackingFileSyncKind, FileBackingOptions,
    FlashIo, FreePointerFooter, Header, LsmMap, MadvisePolicy, MockError, MockFormatError, Storage,
    StorageFormatConfig, StorageFormatError, StorageIoError, StorageMetadata, StoragePerfMetrics,
    WalRegionPrologue, WAL_V1_FORMAT,
};
use heapless::Vec as HeaplessVec;
use redb::{Builder as RedbBuilder, Database, Durability, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_PATH: &str = "perf/file_backing.toml";
const MAX_COLLECTIONS: usize = 64;
const MAX_PENDING_RECLAIMS: usize = 64;
const MAX_INDEXES: usize = 128;
const MAX_RUNS: usize = 128;
const REDB_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("kv");

type PerfResult<T> = Result<T, String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PerfConfig {
    geometry: GeometryConfig,
    #[serde(default)]
    comparison: ComparisonConfig,
    #[serde(default)]
    backing: BackingConfig,
    #[serde(default)]
    redb: RedbConfig,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    preload: PreloadConfig,
    #[serde(default)]
    workload: WorkloadConfig,
    #[serde(default)]
    maintenance: MaintenanceConfig,
    #[serde(default)]
    output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ComparisonConfig {
    #[serde(default = "default_comparison_engines")]
    engines: Vec<EngineKind>,
}

impl Default for ComparisonConfig {
    fn default() -> Self {
        Self {
            engines: default_comparison_engines(),
        }
    }
}

fn default_comparison_engines() -> Vec<EngineKind> {
    vec![EngineKind::Borromean]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EngineKind {
    Borromean,
    #[serde(rename = "borromean-memory")]
    BorromeanMemory,
    Redb,
}

impl EngineKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Borromean => "borromean",
            Self::BorromeanMemory => "borromean-memory",
            Self::Redb => "redb",
        }
    }

    fn comparison_label(self) -> &'static str {
        match self {
            Self::Borromean => "borromean-file",
            Self::BorromeanMemory => "borromean-memory",
            Self::Redb => "redb",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GeometryConfig {
    region_size: usize,
    #[serde(default)]
    region_count: Option<usize>,
    #[serde(default)]
    db_size_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackingConfig {
    path: PathBuf,
    allocation_policy: ConfigAllocationPolicy,
    madvise_policy: ConfigMadvisePolicy,
    erased_byte: u8,
    sync_on_create: bool,
    remove_existing: bool,
    remove_after: bool,
}

impl Default for BackingConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("target/perf/file_backing.db"),
            allocation_policy: ConfigAllocationPolicy::FallbackOnUnsupported,
            madvise_policy: ConfigMadvisePolicy::Normal,
            erased_byte: 0xff,
            sync_on_create: false,
            remove_existing: true,
            remove_after: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RedbConfig {
    path: PathBuf,
    remove_existing: bool,
    remove_after: bool,
    #[serde(default)]
    cache_size_bytes: Option<usize>,
    compact_interval: u64,
    compact_after_workload: bool,
    durability: RedbDurability,
}

impl Default for RedbConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("target/perf/redb.db"),
            remove_existing: true,
            remove_after: false,
            cache_size_bytes: None,
            compact_interval: 0,
            compact_after_workload: false,
            durability: RedbDurability::Immediate,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RedbDurability {
    None,
    #[default]
    Immediate,
}

impl From<RedbDurability> for Durability {
    fn from(durability: RedbDurability) -> Self {
        match durability {
            RedbDurability::None => Self::None,
            RedbDurability::Immediate => Self::Immediate,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ConfigAllocationPolicy {
    Strict,
    #[default]
    FallbackOnUnsupported,
}

impl From<ConfigAllocationPolicy> for AllocationPolicy {
    fn from(policy: ConfigAllocationPolicy) -> Self {
        match policy {
            ConfigAllocationPolicy::Strict => Self::Strict,
            ConfigAllocationPolicy::FallbackOnUnsupported => Self::FallbackOnUnsupported,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ConfigMadvisePolicy {
    #[default]
    Normal,
    Random,
    Sequential,
    WillNeed,
}

impl From<ConfigMadvisePolicy> for MadvisePolicy {
    fn from(policy: ConfigMadvisePolicy) -> Self {
        match policy {
            ConfigMadvisePolicy::Normal => Self::Normal,
            ConfigMadvisePolicy::Random => Self::Random,
            ConfigMadvisePolicy::Sequential => Self::Sequential,
            ConfigMadvisePolicy::WillNeed => Self::WillNeed,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageConfig {
    min_free_regions: u32,
    wal_write_granule: u32,
    wal_record_magic: u8,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            min_free_regions: 2,
            wal_write_granule: 8,
            wal_record_magic: 0xa5,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreloadConfig {
    #[serde(default)]
    count: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadConfig {
    map_count: usize,
    operation_count: u64,
    warmup_count: u64,
    key_space: u64,
    seed: u64,
    read_ratio: u32,
    set_ratio: u32,
    delete_ratio: u32,
    #[serde(default = "default_value_size_bytes")]
    value_size_bytes: usize,
    #[serde(default)]
    key_mode: WorkloadKeyMode,
    compact_on_signal: bool,
    #[serde(default)]
    compact_interval: u64,
    #[serde(default)]
    compaction_region_target: Option<usize>,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            map_count: 1,
            operation_count: 100,
            warmup_count: 10,
            key_space: 100_000,
            seed: 0x0123_4567_89ab_cdef,
            read_ratio: 50,
            set_ratio: 45,
            delete_ratio: 5,
            value_size_bytes: default_value_size_bytes(),
            key_mode: WorkloadKeyMode::default(),
            compact_on_signal: true,
            compact_interval: 0,
            compaction_region_target: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum WorkloadKeyMode {
    #[default]
    Random,
    Preloaded,
    Missing,
    SingleKey,
    InsertRange,
}

fn default_value_size_bytes() -> usize {
    8
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceConfig {
    #[serde(default)]
    optimize_after_workload: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputConfig {
    json_path: Option<PathBuf>,
    latency_sample_interval: u64,
    #[serde(default = "default_progress_interval")]
    progress_interval: u64,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            json_path: Some(PathBuf::from("target/perf/file_backing.json")),
            latency_sample_interval: 100,
            progress_interval: default_progress_interval(),
        }
    }
}

fn default_progress_interval() -> u64 {
    1000
}

#[derive(Debug, Clone, Copy, Serialize)]
struct EffectiveGeometry {
    region_size: usize,
    region_count: usize,
    file_len_bytes: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
struct WorkloadCounters {
    reads: u64,
    sets: u64,
    set_inserts_expected: u64,
    set_updates_expected: u64,
    deletes: u64,
    compactions: u64,
    hits: u64,
    misses: u64,
    read_hits_expected: u64,
    read_misses_expected: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct WorkloadTimings {
    operation_generation_nanos: u128,
    reads_nanos: u128,
    writes_nanos: u128,
    compactions_nanos: u128,
}

#[derive(Debug, Default, Clone, Serialize)]
struct PerfDiagnostics {
    operation_generation_nanos: u128,
    read_lookup_nanos: u128,
    write_apply_nanos: u128,
    transaction_begin_nanos: u128,
    table_open_nanos: u128,
    commit_nanos: u128,
    commit_count: u64,
    transaction_count: u64,
    borromean_io: Option<IoDiagnostics>,
    memory: MemoryDiagnostics,
    redb_cache_size_bytes: Option<usize>,
    redb_stats: Option<RedbStorageStats>,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct IoDiagnostics {
    metadata_reads: u64,
    metadata_writes: u64,
    region_reads: u64,
    region_writes: u64,
    region_erases: u64,
    syncs: u64,
    bytes_read: u64,
    bytes_written: u64,
    bytes_erased: u64,
    read_region_nanos: u128,
    write_region_nanos: u128,
    erase_region_nanos: u128,
    sync_nanos: u128,
    mmap_flush_nanos: u128,
    file_sync_nanos: u128,
    dirty_sync_bytes: u64,
    dirty_sync_regions: u64,
    dirty_sync_metadata_regions: u64,
    exact_dirty_range_bytes: u64,
    aligned_dirty_bytes: u64,
    requested_mmap_flush_bytes: u64,
    flush_overreach_bytes: u64,
    last_file_sync_kind: Option<SyncFileKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SyncFileKind {
    SyncAll,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct MemoryDiagnostics {
    rss_start_bytes: Option<u64>,
    rss_peak_bytes: Option<u64>,
    rss_end_bytes: Option<u64>,
    rss_delta_bytes: Option<i64>,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct RedbStorageStats {
    tree_height: u32,
    allocated_pages: u64,
    leaf_pages: u64,
    branch_pages: u64,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
    page_size: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
struct OperationLatencySummaries {
    reads: Option<LatencySummary>,
    sets: Option<LatencySummary>,
    deletes: Option<LatencySummary>,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct MaintenanceReport {
    ran: bool,
    nanos: u128,
    compactions: u64,
    file_len_after_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct LatencySummary {
    samples: usize,
    min_nanos: u128,
    p50_nanos: u128,
    p95_nanos: u128,
    p99_nanos: u128,
    max_nanos: u128,
}

#[derive(Debug, Clone, Serialize)]
struct PerfReport {
    config: PerfConfig,
    geometry: EffectiveGeometry,
    engine_reports: Vec<EngineReport>,
    comparison_reports: Vec<EngineComparisonReport>,
}

#[derive(Debug, Clone, Serialize)]
struct EngineComparisonReport {
    left_engine: EngineKind,
    right_engine: EngineKind,
    throughput_ratio: Option<f64>,
    p50_latency_ratio: Option<f64>,
    p95_latency_ratio: Option<f64>,
    p99_latency_ratio: Option<f64>,
    sync_time_ratio: Option<f64>,
    compaction_time_ratio: Option<f64>,
    average_read_time_ratio: Option<f64>,
    average_write_time_ratio: Option<f64>,
    logical_size_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct EngineReport {
    engine: EngineKind,
    path: PathBuf,
    create_format_nanos: u128,
    setup_nanos: u128,
    preload_nanos: u128,
    preload_counters: WorkloadCounters,
    warmup_nanos: u128,
    workload_nanos: u128,
    operations_per_second: f64,
    counters: WorkloadCounters,
    workload_timings: WorkloadTimings,
    sampled_latency: Option<LatencySummary>,
    sampled_latency_by_op: OperationLatencySummaries,
    diagnostics: PerfDiagnostics,
    borromean_core_metrics: Option<StoragePerfMetrics>,
    sync_audit: Option<SyncAuditReport>,
    maintenance: MaintenanceReport,
    file_len_bytes: u64,
    logical_len_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct SyncAuditReport {
    write_operations: u64,
    wal_records: u64,
    wal_bytes: u64,
    wal_syncs: u64,
    io_region_writes: u64,
    io_syncs: u64,
    metadata_writes_during_workload: u64,
    region_erases: u64,
    flushes: u64,
    compactions: u64,
    dirty_regions_synced: u64,
    exact_dirty_range_bytes: u64,
    aligned_dirty_bytes: u64,
    requested_mmap_flush_bytes: u64,
    flush_overreach_bytes: u64,
    wal_syncs_per_write: Option<f64>,
    io_syncs_per_write: Option<f64>,
    wal_records_per_write: Option<f64>,
    io_region_writes_per_write: Option<f64>,
    dirty_regions_per_sync: Option<f64>,
    non_hot_write_exceptions: u64,
    first_violations: Vec<SyncAuditViolation>,
}

#[derive(Debug, Clone, Serialize)]
struct SyncAuditViolation {
    operation_index: u64,
    operation: WorkloadOp,
    expected_exception: bool,
    reasons: Vec<String>,
    delta: SyncAuditDelta,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct SyncAuditDelta {
    wal_records: u64,
    wal_syncs: u64,
    io_region_writes: u64,
    io_syncs: u64,
    metadata_writes: u64,
    region_erases: u64,
    flushes: u64,
    compactions: u64,
    overflow_flushes: u64,
    wal_rotations_attempted: u64,
    wal_rotations_completed: u64,
    wal_rotation_required: u64,
    reclaim_starts: u64,
    reclaim_ends: u64,
}

#[derive(Default)]
struct SyncAuditCollector {
    write_operations: u64,
    non_hot_write_exceptions: u64,
    first_violations: Vec<SyncAuditViolation>,
}

#[derive(Clone, Copy)]
struct SyncAuditSnapshot {
    metrics: StoragePerfMetrics,
    io: IoDiagnostics,
}

const MAX_SYNC_AUDIT_VIOLATIONS: usize = 8;

impl SyncAuditCollector {
    fn observe(&mut self, operation_index: u64, operation: WorkloadOp, delta: SyncAuditDelta) {
        if !operation.is_write() {
            return;
        }
        self.write_operations = self.write_operations.saturating_add(1);

        let expected_exception = delta.has_expected_exception();
        if expected_exception {
            self.non_hot_write_exceptions = self.non_hot_write_exceptions.saturating_add(1);
        }

        let reasons = delta.violation_reasons();
        if reasons.is_empty() || self.first_violations.len() >= MAX_SYNC_AUDIT_VIOLATIONS {
            return;
        }

        self.first_violations.push(SyncAuditViolation {
            operation_index,
            operation,
            expected_exception,
            reasons,
            delta,
        });
    }

    fn finish(
        self,
        metrics: StoragePerfMetrics,
        io: IoDiagnostics,
        counters: &WorkloadCounters,
    ) -> SyncAuditReport {
        let write_operations = counters.sets.saturating_add(counters.deletes);
        SyncAuditReport {
            write_operations,
            wal_records: metrics.wal_records,
            wal_bytes: metrics.wal_bytes,
            wal_syncs: metrics.wal_syncs,
            io_region_writes: io.region_writes,
            io_syncs: io.syncs,
            metadata_writes_during_workload: io.metadata_writes,
            region_erases: io.region_erases,
            flushes: metrics.flushes,
            compactions: metrics.compactions_run,
            dirty_regions_synced: io.dirty_sync_regions,
            exact_dirty_range_bytes: io.exact_dirty_range_bytes,
            aligned_dirty_bytes: io.aligned_dirty_bytes,
            requested_mmap_flush_bytes: io.requested_mmap_flush_bytes,
            flush_overreach_bytes: io.flush_overreach_bytes,
            wal_syncs_per_write: ratio_u64(metrics.wal_syncs, write_operations),
            io_syncs_per_write: ratio_u64(io.syncs, write_operations),
            wal_records_per_write: ratio_u64(metrics.wal_records, write_operations),
            io_region_writes_per_write: ratio_u64(io.region_writes, write_operations),
            dirty_regions_per_sync: ratio_u64(io.dirty_sync_regions, io.syncs),
            non_hot_write_exceptions: self.non_hot_write_exceptions,
            first_violations: self.first_violations,
        }
    }
}

impl SyncAuditSnapshot {
    fn delta_since(self, before: Self) -> SyncAuditDelta {
        SyncAuditDelta {
            wal_records: self
                .metrics
                .wal_records
                .saturating_sub(before.metrics.wal_records),
            wal_syncs: self
                .metrics
                .wal_syncs
                .saturating_sub(before.metrics.wal_syncs),
            io_region_writes: self
                .io
                .region_writes
                .saturating_sub(before.io.region_writes),
            io_syncs: self.io.syncs.saturating_sub(before.io.syncs),
            metadata_writes: self
                .io
                .metadata_writes
                .saturating_sub(before.io.metadata_writes),
            region_erases: self
                .io
                .region_erases
                .saturating_sub(before.io.region_erases),
            flushes: self.metrics.flushes.saturating_sub(before.metrics.flushes),
            compactions: self
                .metrics
                .compactions_run
                .saturating_sub(before.metrics.compactions_run),
            overflow_flushes: self
                .metrics
                .overflow_flushes
                .saturating_sub(before.metrics.overflow_flushes),
            wal_rotations_attempted: self
                .metrics
                .wal_rotations_attempted
                .saturating_sub(before.metrics.wal_rotations_attempted),
            wal_rotations_completed: self
                .metrics
                .wal_rotations_completed
                .saturating_sub(before.metrics.wal_rotations_completed),
            wal_rotation_required: self
                .metrics
                .wal_rotation_required
                .saturating_sub(before.metrics.wal_rotation_required),
            reclaim_starts: self
                .metrics
                .reclaim_starts
                .saturating_sub(before.metrics.reclaim_starts),
            reclaim_ends: self
                .metrics
                .reclaim_ends
                .saturating_sub(before.metrics.reclaim_ends),
        }
    }
}

impl SyncAuditDelta {
    fn has_expected_exception(self) -> bool {
        self.overflow_flushes != 0
            || self.flushes != 0
            || self.compactions != 0
            || self.wal_rotations_attempted != 0
            || self.wal_rotations_completed != 0
            || self.wal_rotation_required != 0
            || self.reclaim_starts != 0
            || self.reclaim_ends != 0
    }

    fn violation_reasons(self) -> Vec<String> {
        let mut reasons = Vec::new();
        if self.wal_syncs != 1 {
            reasons.push(format!("wal_syncs={} expected=1", self.wal_syncs));
        }
        if self.io_syncs != 1 {
            reasons.push(format!("io_syncs={} expected=1", self.io_syncs));
        }
        if self.wal_records != 1 {
            reasons.push(format!("wal_records={} expected=1", self.wal_records));
        }
        if self.io_region_writes != 1 {
            reasons.push(format!(
                "io_region_writes={} expected=1",
                self.io_region_writes
            ));
        }
        if self.metadata_writes != 0 {
            reasons.push(format!(
                "metadata_writes={} expected=0",
                self.metadata_writes
            ));
        }
        if self.region_erases != 0 {
            reasons.push(format!("region_erases={} expected=0", self.region_erases));
        }
        if self.flushes != 0 {
            reasons.push(format!("flushes={} expected=0", self.flushes));
        }
        if self.compactions != 0 {
            reasons.push(format!("compactions={} expected=0", self.compactions));
        }
        if self.wal_rotations_attempted != 0 {
            reasons.push(format!(
                "wal_rotations_attempted={} expected=0",
                self.wal_rotations_attempted
            ));
        }
        reasons
    }
}

#[derive(Debug, Clone)]
struct WorkloadStep<const VALUE_BYTES: usize> {
    map_index: usize,
    key: u64,
    operation: WorkloadOp,
    value: Option<HeaplessVec<u8, VALUE_BYTES>>,
    expected_presence: ExpectedPresence,
}

#[derive(Debug, Clone, Copy)]
struct ExecutedOperation {
    operation: WorkloadOp,
}

#[derive(Default)]
struct OperationLatencySamples {
    reads: Vec<u128>,
    sets: Vec<u128>,
    deletes: Vec<u128>,
}

struct MemoryTracker {
    start: Option<u64>,
    peak: Option<u64>,
}

impl MemoryTracker {
    fn start() -> Self {
        let start = current_rss_bytes();
        Self { start, peak: start }
    }

    fn sample(&mut self) {
        if let Some(current) = current_rss_bytes() {
            self.peak = Some(self.peak.map_or(current, |peak| peak.max(current)));
        }
    }

    fn finish(mut self) -> MemoryDiagnostics {
        self.sample();
        let end = current_rss_bytes();
        let peak = match (self.peak, end) {
            (Some(peak), Some(end)) => Some(peak.max(end)),
            (Some(peak), None) => Some(peak),
            (None, Some(end)) => Some(end),
            (None, None) => None,
        };
        let rss_delta_bytes = match (self.start, end) {
            (Some(start), Some(end)) => Some(end as i64 - start as i64),
            _ => None,
        };
        MemoryDiagnostics {
            rss_start_bytes: self.start,
            rss_peak_bytes: peak,
            rss_end_bytes: end,
            rss_delta_bytes,
        }
    }
}

struct SyncDetails {
    mmap_flush_nanos: u128,
    file_sync_nanos: u128,
    exact_dirty_range_bytes: u64,
    aligned_dirty_bytes: u64,
    requested_mmap_flush_bytes: u64,
    flush_overreach_bytes: u64,
    file_sync_kind: Option<SyncFileKind>,
}

trait PerfBacking: FlashIo {
    fn sync_for_perf(&mut self) -> Result<SyncDetails, StorageIoError>;
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> PerfBacking
    for FileBacking<REGION_SIZE, REGION_COUNT>
{
    fn sync_for_perf(&mut self) -> Result<SyncDetails, StorageIoError> {
        let report = self.sync_with_report().map_err(StorageIoError::from)?;
        Ok(SyncDetails {
            mmap_flush_nanos: report.mmap_flush_nanos,
            file_sync_nanos: report.file_sync_nanos,
            exact_dirty_range_bytes: report.dirty_range_bytes as u64,
            aligned_dirty_bytes: report.aligned_dirty_bytes as u64,
            requested_mmap_flush_bytes: report.requested_mmap_flush_bytes as u64,
            flush_overreach_bytes: report.flush_overreach_bytes as u64,
            file_sync_kind: Some(match report.file_sync_kind {
                FileBackingFileSyncKind::SyncAll => SyncFileKind::SyncAll,
            }),
        })
    }
}

struct MemoryBacking<const REGION_SIZE: usize, const REGION_COUNT: usize> {
    storage: Vec<u8>,
    erased_byte: u8,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> MemoryBacking<REGION_SIZE, REGION_COUNT> {
    fn new(erased_byte: u8) -> PerfResult<Self> {
        let len = Self::logical_len_bytes()
            .ok_or_else(|| "memory backing length overflowed".to_owned())?;
        Ok(Self {
            storage: vec![erased_byte; len],
            erased_byte,
        })
    }

    fn logical_len_bytes() -> Option<usize> {
        REGION_SIZE.checked_mul(REGION_COUNT.checked_add(1)?)
    }

    fn metadata_range(&self) -> Result<std::ops::Range<usize>, MockError> {
        checked_memory_range(0, REGION_SIZE, self.storage.len())
    }

    fn region_range(
        &self,
        region_index: u32,
        offset: usize,
        len: usize,
    ) -> Result<std::ops::Range<usize>, MockError> {
        let index = usize::try_from(region_index)
            .map_err(|_| MockError::InvalidRegionIndex(region_index))?;
        if index >= REGION_COUNT {
            return Err(MockError::InvalidRegionIndex(region_index));
        }
        let region_offset = checked_memory_range(offset, len, REGION_SIZE)?;
        let region_start = REGION_SIZE
            .checked_mul(index.checked_add(1).ok_or(MockError::OutOfBounds)?)
            .ok_or(MockError::OutOfBounds)?;
        let start = region_start
            .checked_add(region_offset.start)
            .ok_or(MockError::OutOfBounds)?;
        let end = region_start
            .checked_add(region_offset.end)
            .ok_or(MockError::OutOfBounds)?;
        Ok(start..end)
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> FlashIo
    for MemoryBacking<REGION_SIZE, REGION_COUNT>
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        let range = self.metadata_range().map_err(StorageIoError::from)?;
        let metadata_region = &self.storage[range];
        if metadata_region.iter().all(|byte| *byte == self.erased_byte) {
            return Ok(None);
        }
        StorageMetadata::decode(metadata_region)
            .map(Some)
            .map_err(|_| StorageIoError::from(MockError::OutOfBounds))
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        let range = self.metadata_range().map_err(StorageIoError::from)?;
        let metadata_region = &mut self.storage[range];
        metadata_region.fill(self.erased_byte);
        metadata
            .encode_into(metadata_region)
            .map(|_| ())
            .map_err(|_| StorageIoError::from(MockError::OutOfBounds))
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), StorageIoError> {
        let range = self
            .region_range(region_index, offset, buffer.len())
            .map_err(StorageIoError::from)?;
        buffer.copy_from_slice(&self.storage[range]);
        Ok(())
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        let range = self
            .region_range(region_index, offset, data.len())
            .map_err(StorageIoError::from)?;
        self.storage[range].copy_from_slice(data);
        Ok(())
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        let range = self
            .region_range(region_index, 0, REGION_SIZE)
            .map_err(StorageIoError::from)?;
        self.storage[range].fill(self.erased_byte);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        Ok(())
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        let region_size =
            u32::try_from(REGION_SIZE).map_err(|_| MockFormatError::RegionSizeTooLarge)?;
        let region_count =
            u32::try_from(REGION_COUNT).map_err(|_| MockFormatError::RegionCountTooLarge)?;

        if region_count < 2 + min_free_regions {
            return Err(StorageFormatError::from(
                MockFormatError::InsufficientRegions {
                    region_count,
                    min_free_regions,
                },
            ));
        }

        let metadata = StorageMetadata::new(
            region_size,
            region_count,
            min_free_regions,
            wal_write_granule,
            self.erased_byte,
            wal_record_magic,
        )
        .map_err(|error| StorageFormatError::from(MockFormatError::from(error)))?;

        self.write_metadata(metadata).map_err(|error| match error {
            StorageIoError::Mock(error) => StorageFormatError::from(MockFormatError::from(error)),
            StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
        })?;

        for region_index in 0..region_count {
            self.erase_region(region_index)
                .map_err(|error| match error {
                    StorageIoError::Mock(error) => {
                        StorageFormatError::from(MockFormatError::from(error))
                    }
                    StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
                })?;
        }

        let header = Header {
            sequence: 0,
            collection_id: CollectionId::new(0),
            collection_format: WAL_V1_FORMAT,
        };
        let mut header_bytes = [0u8; Header::ENCODED_LEN];
        header
            .encode_into(&mut header_bytes)
            .map_err(|error| StorageFormatError::from(MockFormatError::from(error)))?;
        self.write_region(0, 0, &header_bytes)
            .map_err(|error| match error {
                StorageIoError::Mock(error) => {
                    StorageFormatError::from(MockFormatError::from(error))
                }
                StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
            })?;

        let prologue = WalRegionPrologue {
            wal_head_region_index: 0,
        };
        let mut prologue_bytes = [0u8; WalRegionPrologue::ENCODED_LEN];
        prologue
            .encode_into(&mut prologue_bytes, region_count)
            .map_err(|error| StorageFormatError::from(MockFormatError::from(error)))?;
        self.write_region(0, Header::ENCODED_LEN, &prologue_bytes)
            .map_err(|error| match error {
                StorageIoError::Mock(error) => {
                    StorageFormatError::from(MockFormatError::from(error))
                }
                StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
            })?;

        let footer_offset = REGION_SIZE
            .checked_sub(FreePointerFooter::ENCODED_LEN)
            .ok_or_else(|| {
                StorageFormatError::from(MockFormatError::from(MockError::OutOfBounds))
            })?;
        for region_index in 1..region_count {
            let next_tail = if region_index + 1 < region_count {
                Some(region_index + 1)
            } else {
                None
            };
            let footer = FreePointerFooter { next_tail };
            let mut footer_bytes = [0u8; FreePointerFooter::ENCODED_LEN];
            footer
                .encode_into(&mut footer_bytes, self.erased_byte)
                .map_err(|error| StorageFormatError::from(MockFormatError::from(error)))?;
            self.write_region(region_index, footer_offset, &footer_bytes)
                .map_err(|error| match error {
                    StorageIoError::Mock(error) => {
                        StorageFormatError::from(MockFormatError::from(error))
                    }
                    StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
                })?;
        }

        self.sync().map_err(|error| match error {
            StorageIoError::Mock(error) => StorageFormatError::from(MockFormatError::from(error)),
            StorageIoError::FileBacking(_) => unreachable_storage_format_error(),
        })?;
        Ok(metadata)
    }
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> PerfBacking
    for MemoryBacking<REGION_SIZE, REGION_COUNT>
{
    fn sync_for_perf(&mut self) -> Result<SyncDetails, StorageIoError> {
        self.sync()?;
        Ok(SyncDetails {
            mmap_flush_nanos: 0,
            file_sync_nanos: 0,
            exact_dirty_range_bytes: 0,
            aligned_dirty_bytes: 0,
            requested_mmap_flush_bytes: 0,
            flush_overreach_bytes: 0,
            file_sync_kind: None,
        })
    }
}

fn checked_memory_range(
    offset: usize,
    len: usize,
    total_len: usize,
) -> Result<std::ops::Range<usize>, MockError> {
    let end = offset.checked_add(len).ok_or(MockError::OutOfBounds)?;
    if end > total_len {
        return Err(MockError::OutOfBounds);
    }
    Ok(offset..end)
}

fn unreachable_storage_format_error() -> StorageFormatError {
    StorageFormatError::from(MockFormatError::from(MockError::OutOfBounds))
}

#[derive(Clone)]
struct IoDiagnosticsHandle<const REGION_SIZE: usize, const REGION_COUNT: usize> {
    state: Rc<RefCell<IoDiagnosticsState>>,
}

struct IoDiagnosticsState {
    diagnostics: IoDiagnostics,
    metadata_dirty: bool,
    dirty_regions: Vec<bool>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize>
    IoDiagnosticsHandle<REGION_SIZE, REGION_COUNT>
{
    fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(IoDiagnosticsState {
                diagnostics: IoDiagnostics::default(),
                metadata_dirty: false,
                dirty_regions: vec![false; REGION_COUNT],
            })),
        }
    }

    fn diagnostics(&self) -> IoDiagnostics {
        self.state.borrow().diagnostics
    }

    fn reset(&self) {
        let mut state = self.state.borrow_mut();
        state.diagnostics = IoDiagnostics::default();
        state.clear_dirty_state();
    }

    fn update(&self, f: impl FnOnce(&mut IoDiagnosticsState)) {
        f(&mut self.state.borrow_mut());
    }

    fn dirty_state(&self) -> (u64, u64, u64) {
        self.state.borrow().dirty_state::<REGION_SIZE>()
    }
}

impl IoDiagnosticsState {
    fn mark_region_dirty(&mut self, region_index: u32) {
        let Ok(index) = usize::try_from(region_index) else {
            return;
        };
        if let Some(dirty) = self.dirty_regions.get_mut(index) {
            *dirty = true;
        }
    }

    fn dirty_state<const REGION_SIZE: usize>(&self) -> (u64, u64, u64) {
        let dirty_regions = self.dirty_regions.iter().filter(|dirty| **dirty).count() as u64;
        let dirty_metadata_regions = u64::from(self.metadata_dirty);
        let dirty_bytes = dirty_regions
            .saturating_mul(REGION_SIZE as u64)
            .saturating_add(dirty_metadata_regions.saturating_mul(REGION_SIZE as u64));
        (dirty_regions, dirty_metadata_regions, dirty_bytes)
    }

    fn clear_dirty_state(&mut self) {
        self.metadata_dirty = false;
        for dirty in &mut self.dirty_regions {
            *dirty = false;
        }
    }
}

struct InstrumentedBacking<IO, const REGION_SIZE: usize, const REGION_COUNT: usize> {
    inner: IO,
    diagnostics: IoDiagnosticsHandle<REGION_SIZE, REGION_COUNT>,
}

impl<IO, const REGION_SIZE: usize, const REGION_COUNT: usize>
    InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>
{
    fn new(inner: IO) -> Self {
        Self {
            inner,
            diagnostics: IoDiagnosticsHandle::new(),
        }
    }

    fn diagnostics_handle(&self) -> IoDiagnosticsHandle<REGION_SIZE, REGION_COUNT> {
        self.diagnostics.clone()
    }
}

fn merge_io_metrics_into_core(metrics: &mut StoragePerfMetrics, io: IoDiagnostics) {
    metrics.mmap_flush_nanos = metrics.mmap_flush_nanos.saturating_add(io.mmap_flush_nanos);
    metrics.file_sync_nanos = metrics.file_sync_nanos.saturating_add(io.file_sync_nanos);
    metrics.dirty_sync_bytes = metrics.dirty_sync_bytes.saturating_add(io.dirty_sync_bytes);
    metrics.dirty_sync_regions = metrics
        .dirty_sync_regions
        .saturating_add(io.dirty_sync_regions);
    metrics.dirty_sync_metadata_regions = metrics
        .dirty_sync_metadata_regions
        .saturating_add(io.dirty_sync_metadata_regions);
}

fn capture_sync_audit_snapshot<IO, const REGION_SIZE: usize, const REGION_COUNT: usize>(
    storage: &Storage<
        '_,
        InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    diagnostics_handle: &IoDiagnosticsHandle<REGION_SIZE, REGION_COUNT>,
) -> SyncAuditSnapshot
where
    IO: PerfBacking,
{
    SyncAuditSnapshot {
        metrics: storage.perf_metrics(),
        io: diagnostics_handle.diagnostics(),
    }
}

impl<IO, const REGION_SIZE: usize, const REGION_COUNT: usize> FlashIo
    for InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>
where
    IO: PerfBacking,
{
    fn read_metadata(&mut self) -> Result<Option<StorageMetadata>, StorageIoError> {
        self.diagnostics.update(|state| {
            state.diagnostics.metadata_reads = state.diagnostics.metadata_reads.saturating_add(1);
        });
        self.inner.read_metadata().map_err(StorageIoError::from)
    }

    fn write_metadata(&mut self, metadata: StorageMetadata) -> Result<(), StorageIoError> {
        let result = self.inner.write_metadata(metadata);
        self.diagnostics.update(|state| {
            state.diagnostics.metadata_writes = state.diagnostics.metadata_writes.saturating_add(1);
            if result.is_ok() {
                state.metadata_dirty = true;
            }
        });
        result
    }

    fn read_region(
        &mut self,
        region_index: u32,
        offset: usize,
        buffer: &mut [u8],
    ) -> Result<(), StorageIoError> {
        let start = Instant::now();
        let result = self.inner.read_region(region_index, offset, buffer);
        let elapsed = start.elapsed().as_nanos();
        self.diagnostics.update(|state| {
            state.diagnostics.region_reads = state.diagnostics.region_reads.saturating_add(1);
            state.diagnostics.bytes_read = state
                .diagnostics
                .bytes_read
                .saturating_add(buffer.len() as u64);
            state.diagnostics.read_region_nanos =
                state.diagnostics.read_region_nanos.saturating_add(elapsed);
        });
        result
    }

    fn write_region(
        &mut self,
        region_index: u32,
        offset: usize,
        data: &[u8],
    ) -> Result<(), StorageIoError> {
        let start = Instant::now();
        let result = self.inner.write_region(region_index, offset, data);
        let elapsed = start.elapsed().as_nanos();
        self.diagnostics.update(|state| {
            state.diagnostics.region_writes = state.diagnostics.region_writes.saturating_add(1);
            state.diagnostics.bytes_written = state
                .diagnostics
                .bytes_written
                .saturating_add(data.len() as u64);
            state.diagnostics.write_region_nanos =
                state.diagnostics.write_region_nanos.saturating_add(elapsed);
            if result.is_ok() {
                state.mark_region_dirty(region_index);
            }
        });
        result
    }

    fn erase_region(&mut self, region_index: u32) -> Result<(), StorageIoError> {
        let start = Instant::now();
        let result = self.inner.erase_region(region_index);
        let elapsed = start.elapsed().as_nanos();
        self.diagnostics.update(|state| {
            state.diagnostics.region_erases = state.diagnostics.region_erases.saturating_add(1);
            state.diagnostics.bytes_erased = state
                .diagnostics
                .bytes_erased
                .saturating_add(REGION_SIZE as u64);
            state.diagnostics.erase_region_nanos =
                state.diagnostics.erase_region_nanos.saturating_add(elapsed);
            if result.is_ok() {
                state.mark_region_dirty(region_index);
            }
        });
        result
    }

    fn sync(&mut self) -> Result<(), StorageIoError> {
        let start = Instant::now();
        let (dirty_regions, dirty_metadata_regions, dirty_bytes) = self.diagnostics.dirty_state();
        let result = self.inner.sync_for_perf();
        let sync_nanos = start.elapsed().as_nanos();
        match result {
            Ok(details) => {
                self.diagnostics.update(|state| {
                    state.diagnostics.syncs = state.diagnostics.syncs.saturating_add(1);
                    state.diagnostics.sync_nanos =
                        state.diagnostics.sync_nanos.saturating_add(sync_nanos);
                    state.diagnostics.mmap_flush_nanos = state
                        .diagnostics
                        .mmap_flush_nanos
                        .saturating_add(details.mmap_flush_nanos);
                    state.diagnostics.file_sync_nanos = state
                        .diagnostics
                        .file_sync_nanos
                        .saturating_add(details.file_sync_nanos);
                    state.diagnostics.exact_dirty_range_bytes = state
                        .diagnostics
                        .exact_dirty_range_bytes
                        .saturating_add(details.exact_dirty_range_bytes);
                    state.diagnostics.aligned_dirty_bytes = state
                        .diagnostics
                        .aligned_dirty_bytes
                        .saturating_add(details.aligned_dirty_bytes);
                    state.diagnostics.requested_mmap_flush_bytes = state
                        .diagnostics
                        .requested_mmap_flush_bytes
                        .saturating_add(details.requested_mmap_flush_bytes);
                    state.diagnostics.flush_overreach_bytes = state
                        .diagnostics
                        .flush_overreach_bytes
                        .saturating_add(details.flush_overreach_bytes);
                    state.diagnostics.last_file_sync_kind = details.file_sync_kind;
                    state.diagnostics.dirty_sync_regions = state
                        .diagnostics
                        .dirty_sync_regions
                        .saturating_add(dirty_regions);
                    state.diagnostics.dirty_sync_metadata_regions = state
                        .diagnostics
                        .dirty_sync_metadata_regions
                        .saturating_add(dirty_metadata_regions);
                    state.diagnostics.dirty_sync_bytes = state
                        .diagnostics
                        .dirty_sync_bytes
                        .saturating_add(dirty_bytes);
                    state.clear_dirty_state();
                });
                Ok(())
            }
            Err(error) => {
                self.diagnostics.update(|state| {
                    state.diagnostics.syncs = state.diagnostics.syncs.saturating_add(1);
                    state.diagnostics.sync_nanos =
                        state.diagnostics.sync_nanos.saturating_add(sync_nanos);
                });
                Err(error)
            }
        }
    }

    fn format_empty_store(
        &mut self,
        min_free_regions: u32,
        wal_write_granule: u32,
        wal_record_magic: u8,
    ) -> Result<StorageMetadata, StorageFormatError> {
        self.inner
            .format_empty_store(min_free_regions, wal_write_granule, wal_record_magic)
    }
}

struct ProgressReporter {
    interval: u64,
    total: u64,
    started_at: Instant,
    last_reported: u64,
}

impl ProgressReporter {
    fn new(interval: u64, total: u64) -> Self {
        Self {
            interval,
            total,
            started_at: Instant::now(),
            last_reported: 0,
        }
    }

    fn maybe_report(
        &mut self,
        phase: &str,
        completed: u64,
        counters: Option<&WorkloadCounters>,
        latency_sample_count: usize,
    ) {
        if self.interval == 0 || completed == 0 {
            return;
        }
        if completed >= self.total {
            return;
        }
        if completed.saturating_sub(self.last_reported) < self.interval {
            return;
        }
        self.last_reported = completed;
        self.report(phase, completed, counters, latency_sample_count);
    }

    fn report(
        &self,
        phase: &str,
        completed: u64,
        counters: Option<&WorkloadCounters>,
        latency_sample_count: usize,
    ) {
        let elapsed = self.started_at.elapsed();
        let elapsed_seconds = elapsed.as_secs_f64();
        let rate = if elapsed_seconds == 0.0 {
            0.0
        } else {
            completed as f64 / elapsed_seconds
        };
        let percent = if self.total == 0 {
            100.0
        } else {
            (completed as f64 / self.total as f64) * 100.0
        };
        let remaining = self.total.saturating_sub(completed);
        let eta = estimate_remaining(rate, remaining);

        eprint!(
            "[file-backing-perf] {phase}: {completed}/{} ops ({percent:.1}%) elapsed={} eta={} rate={rate:.2} ops/s",
            self.total,
            format_duration(elapsed),
            eta.map_or_else(|| "unknown".to_owned(), format_duration)
        );
        if let Some(counters) = counters {
            eprint!(
                " reads={} sets={} deletes={} compactions={} misses={} latency_samples={}",
                counters.reads,
                counters.sets,
                counters.deletes,
                counters.compactions,
                counters.misses,
                latency_sample_count
            );
        }
        eprintln!();
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
enum WorkloadOp {
    Read,
    Set,
    Delete,
}

impl WorkloadOp {
    fn is_write(self) -> bool {
        matches!(self, Self::Set | Self::Delete)
    }
}

#[derive(Debug, Clone, Copy)]
enum ExpectedPresence {
    Unknown,
    Present,
    Missing,
}

#[derive(Debug, Clone, Copy)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn next_bounded(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        self.next_u64() % bound
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("[file-backing-perf] {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> PerfResult<()> {
    let config_path = parse_config_path()?;
    let config = read_config(&config_path)?;
    let region_count = config.geometry.effective_region_count()?;
    validate_config(&config, region_count)?;
    let report = dispatch_geometry(&config, region_count)?;
    print_report(&report);
    write_json_report(&report)?;
    Ok(())
}

fn parse_config_path() -> PerfResult<PathBuf> {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next(), args.next()) {
        (None, None, None) => Ok(PathBuf::from(DEFAULT_CONFIG_PATH)),
        (Some("--config"), Some(path), None) => Ok(PathBuf::from(path)),
        (Some("-h" | "--help"), None, None) => {
            println!("usage: file_backing_perf [--config <path>]");
            std::process::exit(0);
        }
        _ => Err("usage: file_backing_perf [--config <path>]".to_owned()),
    }
}

fn read_config(path: &Path) -> PerfResult<PerfConfig> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

fn validate_config(config: &PerfConfig, region_count: usize) -> PerfResult<()> {
    if config.comparison.engines.is_empty() {
        return Err("comparison.engines must contain at least one engine".to_owned());
    }
    if has_duplicate_engines(&config.comparison.engines) {
        return Err("comparison.engines must not contain duplicates".to_owned());
    }
    if config.workload.map_count == 0 {
        return Err("workload.map_count must be greater than zero".to_owned());
    }
    if config.workload.map_count > MAX_COLLECTIONS {
        return Err(format!(
            "workload.map_count={} exceeds MAX_COLLECTIONS={MAX_COLLECTIONS}",
            config.workload.map_count
        ));
    }
    if config.workload.key_space == 0 {
        return Err("workload.key_space must be greater than zero".to_owned());
    }
    if config.preload.count > config.workload.key_space {
        return Err("preload.count must not exceed workload.key_space".to_owned());
    }
    if matches!(
        config.workload.key_mode,
        WorkloadKeyMode::Preloaded | WorkloadKeyMode::SingleKey
    ) && config.preload.count == 0
    {
        return Err("workload.key_mode requires preload.count > 0".to_owned());
    }
    if matches!(config.workload.key_mode, WorkloadKeyMode::Missing)
        && config.preload.count >= config.workload.key_space
    {
        return Err(
            "workload.key_mode = \"missing\" requires preload.count < workload.key_space"
                .to_owned(),
        );
    }
    if matches!(config.workload.key_mode, WorkloadKeyMode::InsertRange)
        && config.preload.count >= config.workload.key_space
    {
        return Err(
            "workload.key_mode = \"insert-range\" requires preload.count < workload.key_space"
                .to_owned(),
        );
    }
    let ratio_total = ratio_total(config.workload)?;
    if ratio_total == 0 {
        return Err("at least one workload ratio must be nonzero".to_owned());
    }
    if region_count == 0 {
        return Err("geometry must provide at least one data region".to_owned());
    }
    match config.workload.value_size_bytes {
        8 | 64 | 256 | 1024 => {}
        other => {
            return Err(format!(
                "unsupported workload.value_size_bytes={other}; supported sizes are 8, 64, 256, 1024"
            ));
        }
    }
    if config.comparison.engines.contains(&EngineKind::Redb) {
        validate_redb_key_space(config.workload)?;
    }
    Ok(())
}

fn has_duplicate_engines(engines: &[EngineKind]) -> bool {
    engines
        .iter()
        .enumerate()
        .any(|(index, engine)| engines[index + 1..].contains(engine))
}

fn validate_redb_key_space(workload: WorkloadConfig) -> PerfResult<()> {
    let map_count = u64::try_from(workload.map_count)
        .map_err(|_| "workload.map_count does not fit in u64".to_owned())?;
    map_count.checked_mul(workload.key_space).ok_or_else(|| {
        "workload.map_count * workload.key_space must fit in u64 for redb comparison".to_owned()
    })?;
    Ok(())
}

fn ratio_total(workload: WorkloadConfig) -> PerfResult<u32> {
    workload
        .read_ratio
        .checked_add(workload.set_ratio)
        .and_then(|sum| sum.checked_add(workload.delete_ratio))
        .ok_or_else(|| "workload ratios overflowed u32".to_owned())
}

fn dispatch_geometry(config: &PerfConfig, region_count: usize) -> PerfResult<PerfReport> {
    match config.workload.value_size_bytes {
        8 => dispatch_geometry_for_value::<8>(config, region_count),
        64 => dispatch_geometry_for_value::<64>(config, region_count),
        256 => dispatch_geometry_for_value::<256>(config, region_count),
        1024 => dispatch_geometry_for_value::<1024>(config, region_count),
        other => Err(format!(
            "unsupported workload.value_size_bytes={other}; supported sizes are 8, 64, 256, 1024"
        )),
    }
}

fn dispatch_geometry_for_value<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    region_count: usize,
) -> PerfResult<PerfReport>
where
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    macro_rules! dispatch_count {
        ($region_size:literal, $region_count:expr, $config:expr) => {
            match $region_count {
                64 => run_geometry::<$region_size, 64, VALUE_BYTES>($config),
                256 => run_geometry::<$region_size, 256, VALUE_BYTES>($config),
                1024 => run_geometry::<$region_size, 1024, VALUE_BYTES>($config),
                1599 => run_geometry::<$region_size, 1599, VALUE_BYTES>($config),
                4096 => run_geometry::<$region_size, 4096, VALUE_BYTES>($config),
                other => Err(format!(
                    "unsupported region_count={other}; supported counts are 64, 256, 1024, 1599, 4096"
                )),
            }
        };
    }

    match config.geometry.region_size {
        4096 => dispatch_count!(4096, region_count, config),
        8192 => dispatch_count!(8192, region_count, config),
        16_384 => dispatch_count!(16_384, region_count, config),
        65_536 => dispatch_count!(65_536, region_count, config),
        other => Err(format!(
            "unsupported region_size={other}; supported sizes are 4096, 8192, 16384, 65536"
        )),
    }
}

fn run_geometry<const REGION_SIZE: usize, const REGION_COUNT: usize, const VALUE_BYTES: usize>(
    config: &PerfConfig,
) -> PerfResult<PerfReport>
where
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let geometry = EffectiveGeometry {
        region_size: REGION_SIZE,
        region_count: REGION_COUNT,
        file_len_bytes: REGION_SIZE
            .checked_mul(REGION_COUNT + 1)
            .ok_or_else(|| "file length overflowed".to_owned())?,
    };
    let mut engine_reports = Vec::with_capacity(config.comparison.engines.len());
    for engine in &config.comparison.engines {
        let engine_report = match engine {
            EngineKind::Borromean => {
                run_borromean_engine::<REGION_SIZE, REGION_COUNT, VALUE_BYTES>(config)?
            }
            EngineKind::BorromeanMemory => {
                run_borromean_memory_engine::<REGION_SIZE, REGION_COUNT, VALUE_BYTES>(config)?
            }
            EngineKind::Redb => run_redb_engine::<VALUE_BYTES>(config, geometry.file_len_bytes)?,
        };
        engine_reports.push(engine_report);
    }
    let comparison_reports = build_comparison_reports(&engine_reports);

    Ok(PerfReport {
        config: config.clone(),
        geometry,
        engine_reports,
        comparison_reports,
    })
}

fn run_borromean_engine<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
) -> PerfResult<EngineReport>
where
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let mut memory = MemoryTracker::start();
    prepare_db_path(&config.backing)?;
    let options = file_backing_options(&config.backing);

    eprintln!(
        "[file-backing-perf] preparing borromean {} with region_size={} region_count={} value_size={} preload={} operations={}",
        config.backing.path.display(),
        REGION_SIZE,
        REGION_COUNT,
        VALUE_BYTES,
        config.preload.count,
        config.workload.operation_count
    );
    let create_format_start = Instant::now();
    let backing =
        FileBacking::<REGION_SIZE, REGION_COUNT>::create_new(&config.backing.path, options)
            .map_err(|error| format!("failed to create file backing: {error:?}"))?;
    let mut backing = InstrumentedBacking::new(backing);
    let diagnostics_handle = backing.diagnostics_handle();
    let mut storage =
        Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>::format(
            &mut backing,
            StorageFormatConfig::new(
                config.storage.min_free_regions,
                config.storage.wal_write_granule,
                config.storage.wal_record_magic,
            ),
        )
        .map_err(|error| format!("failed to format storage: {error:?}"))?;
    let create_format = create_format_start.elapsed();
    eprintln!(
        "[file-backing-perf] borromean create+format complete in {}",
        format_duration(create_format)
    );

    let map_setup_start = Instant::now();
    let mut maps = Vec::with_capacity(config.workload.map_count);
    for _ in 0..config.workload.map_count {
        let mut map =
            LsmMap::<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>::new(&mut storage)
                .map_err(|error| format!("failed to create map: {error:?}"))?;
        if let Some(target) = config.workload.compaction_region_target {
            map = map
                .with_compaction_region_target(target)
                .map_err(|error| format!("failed to set compaction target: {error:?}"))?;
        }
        maps.push(map);
    }
    let map_setup = map_setup_start.elapsed();
    eprintln!(
        "[file-backing-perf] borromean map setup complete in {}",
        format_duration(map_setup)
    );
    memory.sample();

    let preload_start = Instant::now();
    let mut preload_counters = WorkloadCounters::default();
    let mut preload_timings = WorkloadTimings::default();
    run_borromean_preload::<_, REGION_SIZE, REGION_COUNT, VALUE_BYTES>(
        config,
        "borromean preload",
        &mut maps,
        &mut storage,
        &mut preload_counters,
        &mut preload_timings,
    )?;
    let preload = preload_start.elapsed();
    if config.preload.count != 0 {
        eprintln!(
            "[file-backing-perf] borromean preload complete in {}",
            format_duration(preload)
        );
    }
    memory.sample();

    let mut rng = XorShift64::new(config.workload.seed);
    let warmup_start = Instant::now();
    let mut warmup_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.warmup_count,
    );
    for index in 0..config.workload.warmup_count {
        execute_one_borromean_operation(
            config,
            &mut rng,
            &mut maps,
            &mut storage,
            None,
            None,
            Some(index),
        )
        .map_err(|error| format!("warmup operation {index} failed: {error}"))?;
        warmup_progress.maybe_report("borromean warmup", index + 1, None, 0);
    }
    let warmup = warmup_start.elapsed();
    if config.workload.warmup_count != 0 {
        warmup_progress.report(
            "borromean warmup complete",
            config.workload.warmup_count,
            None,
            0,
        );
    }

    storage.reset_perf_metrics();
    diagnostics_handle.reset();
    let mut counters = WorkloadCounters::default();
    let mut workload_timings = WorkloadTimings::default();
    let mut latency_samples = Vec::new();
    let mut latency_samples_by_op = OperationLatencySamples::default();
    let mut sync_audit = SyncAuditCollector::default();
    let workload_start = Instant::now();
    let mut workload_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.operation_count,
    );
    for index in 0..config.workload.operation_count {
        let sample = should_sample_latency(config.output.latency_sample_interval, index);
        let operation_start = sample.then(Instant::now);
        let audit_before = capture_sync_audit_snapshot(&storage, &diagnostics_handle);
        let executed = execute_one_borromean_operation(
            config,
            &mut rng,
            &mut maps,
            &mut storage,
            Some(&mut counters),
            Some(&mut workload_timings),
            Some(index),
        )
        .map_err(|error| format!("workload operation {index} failed: {error}"))?;
        let audit_after = capture_sync_audit_snapshot(&storage, &diagnostics_handle);
        sync_audit.observe(
            index,
            executed.operation,
            audit_after.delta_since(audit_before),
        );
        if let Some(start) = operation_start {
            let elapsed = start.elapsed().as_nanos();
            latency_samples.push(elapsed);
            push_latency_sample(&mut latency_samples_by_op, executed.operation, elapsed);
        }
        workload_progress.maybe_report(
            "borromean workload",
            index + 1,
            Some(&counters),
            latency_samples.len(),
        );
        maybe_sample_memory(&mut memory, config.output.progress_interval, index);
    }
    let workload = workload_start.elapsed();
    workload_progress.report(
        "borromean workload complete",
        config.workload.operation_count,
        Some(&counters),
        latency_samples.len(),
    );

    let mut borromean_core_metrics = storage.take_perf_metrics();
    let io_diagnostics = diagnostics_handle.diagnostics();
    merge_io_metrics_into_core(&mut borromean_core_metrics, io_diagnostics);
    let sync_audit = sync_audit.finish(borromean_core_metrics, io_diagnostics, &counters);
    let maintenance = run_borromean_post_workload_maintenance::<
        _,
        REGION_SIZE,
        REGION_COUNT,
        VALUE_BYTES,
    >(config, &mut maps, &mut storage, Some(&config.backing.path))?;

    drop(maps);
    drop(storage);
    let file_len_bytes = current_file_len(&config.backing.path)?;
    drop(backing);
    if config.backing.remove_after {
        remove_file_if_present(&config.backing.path)?;
    }
    let memory = memory.finish();
    let diagnostics = PerfDiagnostics {
        operation_generation_nanos: workload_timings.operation_generation_nanos,
        read_lookup_nanos: workload_timings.reads_nanos,
        write_apply_nanos: workload_timings.writes_nanos,
        borromean_io: Some(io_diagnostics),
        memory,
        ..PerfDiagnostics::default()
    };

    Ok(EngineReport {
        engine: EngineKind::Borromean,
        path: config.backing.path.clone(),
        create_format_nanos: create_format.as_nanos(),
        setup_nanos: map_setup.as_nanos(),
        preload_nanos: preload.as_nanos(),
        preload_counters,
        warmup_nanos: warmup.as_nanos(),
        workload_nanos: workload.as_nanos(),
        operations_per_second: operations_per_second(config.workload.operation_count, workload),
        counters,
        workload_timings,
        sampled_latency: summarize_latency(latency_samples),
        sampled_latency_by_op: summarize_operation_latency(latency_samples_by_op),
        diagnostics,
        borromean_core_metrics: Some(borromean_core_metrics),
        sync_audit: Some(sync_audit),
        maintenance,
        file_len_bytes,
        logical_len_bytes: file_len_bytes,
    })
}

fn run_borromean_memory_engine<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
) -> PerfResult<EngineReport>
where
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let mut memory = MemoryTracker::start();
    let logical_len_bytes = MemoryBacking::<REGION_SIZE, REGION_COUNT>::logical_len_bytes()
        .ok_or_else(|| "memory backing length overflowed".to_owned())?
        as u64;

    eprintln!(
        "[file-backing-perf] preparing borromean-memory with region_size={} region_count={} value_size={} preload={} operations={}",
        REGION_SIZE,
        REGION_COUNT,
        VALUE_BYTES,
        config.preload.count,
        config.workload.operation_count
    );
    let create_format_start = Instant::now();
    let backing = MemoryBacking::<REGION_SIZE, REGION_COUNT>::new(config.backing.erased_byte)?;
    let mut backing = InstrumentedBacking::new(backing);
    let diagnostics_handle = backing.diagnostics_handle();
    let mut storage =
        Storage::<_, REGION_SIZE, REGION_COUNT, MAX_COLLECTIONS, MAX_PENDING_RECLAIMS>::format(
            &mut backing,
            StorageFormatConfig::new(
                config.storage.min_free_regions,
                config.storage.wal_write_granule,
                config.storage.wal_record_magic,
            ),
        )
        .map_err(|error| format!("failed to format memory storage: {error:?}"))?;
    let create_format = create_format_start.elapsed();
    eprintln!(
        "[file-backing-perf] borromean-memory create+format complete in {}",
        format_duration(create_format)
    );

    let map_setup_start = Instant::now();
    let mut maps = Vec::with_capacity(config.workload.map_count);
    for _ in 0..config.workload.map_count {
        let mut map =
            LsmMap::<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>::new(&mut storage)
                .map_err(|error| format!("failed to create memory map: {error:?}"))?;
        if let Some(target) = config.workload.compaction_region_target {
            map = map
                .with_compaction_region_target(target)
                .map_err(|error| format!("failed to set compaction target: {error:?}"))?;
        }
        maps.push(map);
    }
    let map_setup = map_setup_start.elapsed();
    eprintln!(
        "[file-backing-perf] borromean-memory map setup complete in {}",
        format_duration(map_setup)
    );
    memory.sample();

    let preload_start = Instant::now();
    let mut preload_counters = WorkloadCounters::default();
    let mut preload_timings = WorkloadTimings::default();
    run_borromean_preload::<_, REGION_SIZE, REGION_COUNT, VALUE_BYTES>(
        config,
        "borromean-memory preload",
        &mut maps,
        &mut storage,
        &mut preload_counters,
        &mut preload_timings,
    )?;
    let preload = preload_start.elapsed();
    if config.preload.count != 0 {
        eprintln!(
            "[file-backing-perf] borromean-memory preload complete in {}",
            format_duration(preload)
        );
    }
    memory.sample();

    let mut rng = XorShift64::new(config.workload.seed);
    let warmup_start = Instant::now();
    let mut warmup_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.warmup_count,
    );
    for index in 0..config.workload.warmup_count {
        execute_one_borromean_operation(
            config,
            &mut rng,
            &mut maps,
            &mut storage,
            None,
            None,
            Some(index),
        )
        .map_err(|error| format!("warmup operation {index} failed: {error}"))?;
        warmup_progress.maybe_report("borromean-memory warmup", index + 1, None, 0);
    }
    let warmup = warmup_start.elapsed();
    if config.workload.warmup_count != 0 {
        warmup_progress.report(
            "borromean-memory warmup complete",
            config.workload.warmup_count,
            None,
            0,
        );
    }

    storage.reset_perf_metrics();
    diagnostics_handle.reset();
    let mut counters = WorkloadCounters::default();
    let mut workload_timings = WorkloadTimings::default();
    let mut latency_samples = Vec::new();
    let mut latency_samples_by_op = OperationLatencySamples::default();
    let mut sync_audit = SyncAuditCollector::default();
    let workload_start = Instant::now();
    let mut workload_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.operation_count,
    );
    for index in 0..config.workload.operation_count {
        let sample = should_sample_latency(config.output.latency_sample_interval, index);
        let operation_start = sample.then(Instant::now);
        let audit_before = capture_sync_audit_snapshot(&storage, &diagnostics_handle);
        let executed = execute_one_borromean_operation(
            config,
            &mut rng,
            &mut maps,
            &mut storage,
            Some(&mut counters),
            Some(&mut workload_timings),
            Some(index),
        )
        .map_err(|error| format!("workload operation {index} failed: {error}"))?;
        let audit_after = capture_sync_audit_snapshot(&storage, &diagnostics_handle);
        sync_audit.observe(
            index,
            executed.operation,
            audit_after.delta_since(audit_before),
        );
        if let Some(start) = operation_start {
            let elapsed = start.elapsed().as_nanos();
            latency_samples.push(elapsed);
            push_latency_sample(&mut latency_samples_by_op, executed.operation, elapsed);
        }
        workload_progress.maybe_report(
            "borromean-memory workload",
            index + 1,
            Some(&counters),
            latency_samples.len(),
        );
        maybe_sample_memory(&mut memory, config.output.progress_interval, index);
    }
    let workload = workload_start.elapsed();
    workload_progress.report(
        "borromean-memory workload complete",
        config.workload.operation_count,
        Some(&counters),
        latency_samples.len(),
    );

    let mut borromean_core_metrics = storage.take_perf_metrics();
    let io_diagnostics = diagnostics_handle.diagnostics();
    merge_io_metrics_into_core(&mut borromean_core_metrics, io_diagnostics);
    let sync_audit = sync_audit.finish(borromean_core_metrics, io_diagnostics, &counters);
    let maintenance = run_borromean_post_workload_maintenance::<
        _,
        REGION_SIZE,
        REGION_COUNT,
        VALUE_BYTES,
    >(config, &mut maps, &mut storage, None)?;

    drop(maps);
    drop(storage);
    drop(backing);
    let memory = memory.finish();
    let diagnostics = PerfDiagnostics {
        operation_generation_nanos: workload_timings.operation_generation_nanos,
        read_lookup_nanos: workload_timings.reads_nanos,
        write_apply_nanos: workload_timings.writes_nanos,
        borromean_io: Some(io_diagnostics),
        memory,
        ..PerfDiagnostics::default()
    };

    Ok(EngineReport {
        engine: EngineKind::BorromeanMemory,
        path: PathBuf::from("<memory>"),
        create_format_nanos: create_format.as_nanos(),
        setup_nanos: map_setup.as_nanos(),
        preload_nanos: preload.as_nanos(),
        preload_counters,
        warmup_nanos: warmup.as_nanos(),
        workload_nanos: workload.as_nanos(),
        operations_per_second: operations_per_second(config.workload.operation_count, workload),
        counters,
        workload_timings,
        sampled_latency: summarize_latency(latency_samples),
        sampled_latency_by_op: summarize_operation_latency(latency_samples_by_op),
        diagnostics,
        borromean_core_metrics: Some(borromean_core_metrics),
        sync_audit: Some(sync_audit),
        maintenance,
        file_len_bytes: 0,
        logical_len_bytes,
    })
}

fn run_redb_engine<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    borromean_file_len_bytes: usize,
) -> PerfResult<EngineReport> {
    let mut memory = MemoryTracker::start();
    prepare_path(&config.redb.path, config.redb.remove_existing)?;
    let cache_size_bytes = config
        .redb
        .cache_size_bytes
        .unwrap_or(borromean_file_len_bytes);

    eprintln!(
        "[file-backing-perf] preparing redb {} with value_size={} cache_size={} preload={} operations={}",
        config.redb.path.display(),
        VALUE_BYTES,
        cache_size_bytes,
        config.preload.count,
        config.workload.operation_count
    );
    let create_format_start = Instant::now();
    let mut db = RedbBuilder::new()
        .set_cache_size(cache_size_bytes)
        .create(&config.redb.path)
        .map_err(|error| format!("failed to create redb database: {error}"))?;
    let create_format = create_format_start.elapsed();
    eprintln!(
        "[file-backing-perf] redb create complete in {}",
        format_duration(create_format)
    );

    let setup_start = Instant::now();
    let mut write_txn = db
        .begin_write()
        .map_err(|error| format!("failed to begin redb setup transaction: {error}"))?;
    write_txn
        .set_durability(config.redb.durability.into())
        .map_err(|error| format!("failed to set redb setup durability: {error}"))?;
    {
        let _table = write_txn
            .open_table(REDB_TABLE)
            .map_err(|error| format!("failed to open redb table during setup: {error}"))?;
    }
    write_txn
        .commit()
        .map_err(|error| format!("failed to commit redb setup transaction: {error}"))?;
    let setup = setup_start.elapsed();
    eprintln!(
        "[file-backing-perf] redb table setup complete in {}",
        format_duration(setup)
    );
    memory.sample();

    let mut diagnostics = PerfDiagnostics {
        redb_cache_size_bytes: Some(cache_size_bytes),
        ..PerfDiagnostics::default()
    };

    let preload_start = Instant::now();
    let mut preload_counters = WorkloadCounters::default();
    let mut preload_timings = WorkloadTimings::default();
    run_redb_preload::<VALUE_BYTES>(
        config,
        &mut db,
        &mut preload_counters,
        &mut preload_timings,
        &mut diagnostics,
    )?;
    let preload = preload_start.elapsed();
    if config.preload.count != 0 {
        eprintln!(
            "[file-backing-perf] redb preload complete in {}",
            format_duration(preload)
        );
    }
    memory.sample();

    let mut rng = XorShift64::new(config.workload.seed);
    let warmup_start = Instant::now();
    let mut warmup_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.warmup_count,
    );
    for index in 0..config.workload.warmup_count {
        execute_one_redb_operation::<VALUE_BYTES>(
            config,
            &mut rng,
            &mut db,
            None,
            None,
            Some(&mut diagnostics),
            Some(index),
        )
        .map_err(|error| format!("warmup operation {index} failed: {error}"))?;
        warmup_progress.maybe_report("redb warmup", index + 1, None, 0);
    }
    let warmup = warmup_start.elapsed();
    if config.workload.warmup_count != 0 {
        warmup_progress.report(
            "redb warmup complete",
            config.workload.warmup_count,
            None,
            0,
        );
    }

    let mut counters = WorkloadCounters::default();
    let mut workload_timings = WorkloadTimings::default();
    let mut latency_samples = Vec::new();
    let mut latency_samples_by_op = OperationLatencySamples::default();
    let workload_start = Instant::now();
    let mut workload_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.operation_count,
    );
    for index in 0..config.workload.operation_count {
        let sample = should_sample_latency(config.output.latency_sample_interval, index);
        let operation_start = sample.then(Instant::now);
        let executed = execute_one_redb_operation::<VALUE_BYTES>(
            config,
            &mut rng,
            &mut db,
            Some(&mut counters),
            Some(&mut workload_timings),
            Some(&mut diagnostics),
            Some(index),
        )
        .map_err(|error| format!("workload operation {index} failed: {error}"))?;
        if let Some(start) = operation_start {
            let elapsed = start.elapsed().as_nanos();
            latency_samples.push(elapsed);
            push_latency_sample(&mut latency_samples_by_op, executed.operation, elapsed);
        }
        workload_progress.maybe_report(
            "redb workload",
            index + 1,
            Some(&counters),
            latency_samples.len(),
        );
        maybe_sample_memory(&mut memory, config.output.progress_interval, index);
    }
    if config.redb.compact_after_workload {
        compact_redb(&mut db, Some(&mut counters), Some(&mut workload_timings))?;
    }
    let workload = workload_start.elapsed();
    workload_progress.report(
        "redb workload complete",
        config.workload.operation_count,
        Some(&counters),
        latency_samples.len(),
    );

    let maintenance = run_redb_post_workload_maintenance(config, &mut db, &config.redb.path)?;
    diagnostics.redb_stats = collect_redb_stats(&db)?;
    let file_len_bytes = current_file_len(&config.redb.path)?;
    drop(db);
    if config.redb.remove_after {
        remove_file_if_present(&config.redb.path)?;
    }
    diagnostics.operation_generation_nanos = workload_timings.operation_generation_nanos;
    diagnostics.memory = memory.finish();

    Ok(EngineReport {
        engine: EngineKind::Redb,
        path: config.redb.path.clone(),
        create_format_nanos: create_format.as_nanos(),
        setup_nanos: setup.as_nanos(),
        preload_nanos: preload.as_nanos(),
        preload_counters,
        warmup_nanos: warmup.as_nanos(),
        workload_nanos: workload.as_nanos(),
        operations_per_second: operations_per_second(config.workload.operation_count, workload),
        counters,
        workload_timings,
        sampled_latency: summarize_latency(latency_samples),
        sampled_latency_by_op: summarize_operation_latency(latency_samples_by_op),
        diagnostics,
        borromean_core_metrics: None,
        sync_audit: None,
        maintenance,
        file_len_bytes,
        logical_len_bytes: file_len_bytes,
    })
}

fn prepare_db_path(backing: &BackingConfig) -> PerfResult<()> {
    prepare_path(&backing.path, backing.remove_existing)
}

fn prepare_path(path: &Path, remove_existing: bool) -> PerfResult<()> {
    if remove_existing {
        remove_file_if_present(path)?;
    }
    if let Some(parent) = non_empty_parent(path) {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    Ok(())
}

fn current_file_len(path: &Path) -> PerfResult<u64> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("failed to stat {}: {error}", path.display()))
}

fn remove_file_if_present(path: &Path) -> PerfResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("failed to remove {}: {error}", path.display())),
    }
}

fn non_empty_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

fn file_backing_options(config: &BackingConfig) -> FileBackingOptions {
    let mut options = FileBackingOptions::new(config.erased_byte);
    options.allocation_policy = config.allocation_policy.into();
    options.madvise_policy = config.madvise_policy.into();
    options.sync_on_create = config.sync_on_create;
    options
}

fn run_borromean_preload<
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
    progress_label: &str,
    maps: &mut [LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>],
    storage: &mut Storage<
        '_,
        InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    counters: &mut WorkloadCounters,
    timings: &mut WorkloadTimings,
) -> PerfResult<()>
where
    IO: PerfBacking,
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    if config.preload.count == 0 {
        return Ok(());
    }
    let mut rng = XorShift64::new(config.workload.seed ^ 0xa076_1d64_78bd_642f);
    let mut operation_index = 0;
    let preload_total = config
        .preload
        .count
        .checked_mul(
            u64::try_from(maps.len()).map_err(|_| "map count does not fit in u64".to_owned())?,
        )
        .ok_or_else(|| "preload operation count overflowed".to_owned())?;
    let mut preload_progress =
        ProgressReporter::new(config.output.progress_interval, preload_total);
    for map in maps.iter_mut() {
        for key in 0..config.preload.count {
            counters.sets = counters.sets.saturating_add(1);
            counters.set_inserts_expected = counters.set_inserts_expected.saturating_add(1);
            let value = next_value::<VALUE_BYTES>(&mut rng);
            let write_start = Instant::now();
            let compact_needed = map
                .set(storage, key, value)
                .map_err(|error| format!("preload set failed: {error:?}"))?;
            timings.writes_nanos = timings
                .writes_nanos
                .saturating_add(write_start.elapsed().as_nanos());
            maybe_compact(
                config,
                map,
                storage,
                Some(counters),
                Some(timings),
                compact_needed,
                Some(operation_index),
            )?;
            operation_index += 1;
            preload_progress.maybe_report(progress_label, operation_index, Some(counters), 0);
        }
    }
    preload_progress.report(
        &format!("{progress_label} complete"),
        preload_total,
        Some(counters),
        0,
    );
    Ok(())
}

fn run_redb_preload<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    db: &mut Database,
    counters: &mut WorkloadCounters,
    timings: &mut WorkloadTimings,
    diagnostics: &mut PerfDiagnostics,
) -> PerfResult<()> {
    if config.preload.count == 0 {
        return Ok(());
    }
    let mut rng = XorShift64::new(config.workload.seed ^ 0xa076_1d64_78bd_642f);
    let preload_total = config
        .preload
        .count
        .checked_mul(
            u64::try_from(config.workload.map_count)
                .map_err(|_| "map count does not fit in u64".to_owned())?,
        )
        .ok_or_else(|| "preload operation count overflowed".to_owned())?;
    let mut operation_index = 0u64;
    let mut preload_progress =
        ProgressReporter::new(config.output.progress_interval, preload_total);
    for map_index in 0..config.workload.map_count {
        for key in 0..config.preload.count {
            counters.sets = counters.sets.saturating_add(1);
            counters.set_inserts_expected = counters.set_inserts_expected.saturating_add(1);
            let value = next_value::<VALUE_BYTES>(&mut rng);
            let redb_key = encode_redb_key(config.workload, map_index, key)?;

            let write_start = Instant::now();
            let begin_start = Instant::now();
            let mut write_txn = db
                .begin_write()
                .map_err(|error| format!("redb preload write transaction failed: {error}"))?;
            diagnostics.transaction_count = diagnostics.transaction_count.saturating_add(1);
            diagnostics.transaction_begin_nanos = diagnostics
                .transaction_begin_nanos
                .saturating_add(begin_start.elapsed().as_nanos());
            write_txn
                .set_durability(config.redb.durability.into())
                .map_err(|error| format!("redb preload set durability failed: {error}"))?;
            {
                let table_open_start = Instant::now();
                let mut table = write_txn
                    .open_table(REDB_TABLE)
                    .map_err(|error| format!("redb preload open table failed: {error}"))?;
                diagnostics.table_open_nanos = diagnostics
                    .table_open_nanos
                    .saturating_add(table_open_start.elapsed().as_nanos());
                let apply_start = Instant::now();
                table
                    .insert(&redb_key, value.as_slice())
                    .map_err(|error| format!("redb preload insert failed: {error}"))?;
                diagnostics.write_apply_nanos = diagnostics
                    .write_apply_nanos
                    .saturating_add(apply_start.elapsed().as_nanos());
            }
            let commit_start = Instant::now();
            write_txn
                .commit()
                .map_err(|error| format!("redb preload commit failed: {error}"))?;
            diagnostics.commit_count = diagnostics.commit_count.saturating_add(1);
            diagnostics.commit_nanos = diagnostics
                .commit_nanos
                .saturating_add(commit_start.elapsed().as_nanos());
            timings.writes_nanos = timings
                .writes_nanos
                .saturating_add(write_start.elapsed().as_nanos());
            operation_index = operation_index.saturating_add(1);
            preload_progress.maybe_report("redb preload", operation_index, Some(counters), 0);
        }
    }
    preload_progress.report("redb preload complete", preload_total, Some(counters), 0);
    Ok(())
}

fn execute_one_borromean_operation<
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
    rng: &mut XorShift64,
    maps: &mut [LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>],
    storage: &mut Storage<
        '_,
        InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    operation_index: Option<u64>,
) -> PerfResult<ExecutedOperation>
where
    IO: PerfBacking,
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let generation_start = Instant::now();
    let step = next_workload_step::<VALUE_BYTES>(config, rng)?;
    let mut counters = counters;
    let mut timings = timings;
    if let Some(timings) = timings.as_deref_mut() {
        timings.operation_generation_nanos = timings
            .operation_generation_nanos
            .saturating_add(generation_start.elapsed().as_nanos());
    }

    match step.operation {
        WorkloadOp::Read => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.reads += 1;
                count_expected_read(counters, step.expected_presence);
            }
            let read_start = Instant::now();
            let result = maps[step.map_index]
                .get(storage, &step.key, |_, value| value.clone())
                .map_err(|error| format!("read failed: {error:?}"))?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.reads_nanos = timings
                    .reads_nanos
                    .saturating_add(read_start.elapsed().as_nanos());
            }
            if result.is_none() {
                if let Some(counters) = counters.as_deref_mut() {
                    counters.misses += 1;
                }
            } else if let Some(counters) = counters.as_deref_mut() {
                counters.hits += 1;
            }
        }
        WorkloadOp::Set => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.sets += 1;
                count_expected_set(counters, step.expected_presence);
            }
            let value = step
                .value
                .ok_or_else(|| "set operation missing generated value".to_owned())?;
            let write_start = Instant::now();
            let compact_needed = maps[step.map_index]
                .set(storage, step.key, value)
                .map_err(|error| format!("set failed: {error:?}"))?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.writes_nanos = timings
                    .writes_nanos
                    .saturating_add(write_start.elapsed().as_nanos());
            }
            maybe_compact(
                config,
                &mut maps[step.map_index],
                storage,
                counters,
                timings,
                compact_needed,
                operation_index,
            )?;
        }
        WorkloadOp::Delete => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.deletes += 1;
            }
            let write_start = Instant::now();
            let compact_needed = maps[step.map_index]
                .delete(storage, step.key)
                .map_err(|error| format!("delete failed: {error:?}"))?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.writes_nanos = timings
                    .writes_nanos
                    .saturating_add(write_start.elapsed().as_nanos());
            }
            maybe_compact(
                config,
                &mut maps[step.map_index],
                storage,
                counters,
                timings,
                compact_needed,
                operation_index,
            )?;
        }
    }

    Ok(ExecutedOperation {
        operation: step.operation,
    })
}

fn execute_one_redb_operation<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    rng: &mut XorShift64,
    db: &mut Database,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    diagnostics: Option<&mut PerfDiagnostics>,
    operation_index: Option<u64>,
) -> PerfResult<ExecutedOperation> {
    let generation_start = Instant::now();
    let step = next_workload_step::<VALUE_BYTES>(config, rng)?;
    let generation_nanos = generation_start.elapsed().as_nanos();
    let redb_key = encode_redb_key(config.workload, step.map_index, step.key)?;
    let mut counters = counters;
    let mut timings = timings;
    let mut diagnostics = diagnostics;
    if let Some(timings) = timings.as_deref_mut() {
        timings.operation_generation_nanos = timings
            .operation_generation_nanos
            .saturating_add(generation_nanos);
    }
    if let Some(diagnostics) = diagnostics.as_deref_mut() {
        diagnostics.operation_generation_nanos = diagnostics
            .operation_generation_nanos
            .saturating_add(generation_nanos);
    }

    match step.operation {
        WorkloadOp::Read => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.reads += 1;
                count_expected_read(counters, step.expected_presence);
            }
            let read_start = Instant::now();
            let begin_start = Instant::now();
            let read_txn = db
                .begin_read()
                .map_err(|error| format!("redb read transaction failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.transaction_count = diagnostics.transaction_count.saturating_add(1);
                diagnostics.transaction_begin_nanos = diagnostics
                    .transaction_begin_nanos
                    .saturating_add(begin_start.elapsed().as_nanos());
            }
            let table_open_start = Instant::now();
            let table = read_txn
                .open_table(REDB_TABLE)
                .map_err(|error| format!("redb open table for read failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.table_open_nanos = diagnostics
                    .table_open_nanos
                    .saturating_add(table_open_start.elapsed().as_nanos());
            }
            let lookup_start = Instant::now();
            let result = table
                .get(&redb_key)
                .map_err(|error| format!("redb get failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.read_lookup_nanos = diagnostics
                    .read_lookup_nanos
                    .saturating_add(lookup_start.elapsed().as_nanos());
            }
            if let Some(timings) = timings.as_deref_mut() {
                timings.reads_nanos = timings
                    .reads_nanos
                    .saturating_add(read_start.elapsed().as_nanos());
            }
            if result.is_none() {
                if let Some(counters) = counters.as_deref_mut() {
                    counters.misses += 1;
                }
            } else if let Some(counters) = counters.as_deref_mut() {
                counters.hits += 1;
            }
        }
        WorkloadOp::Set => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.sets += 1;
                count_expected_set(counters, step.expected_presence);
            }
            let value = step
                .value
                .ok_or_else(|| "set operation missing generated value".to_owned())?;
            let write_start = Instant::now();
            let begin_start = Instant::now();
            let mut write_txn = db
                .begin_write()
                .map_err(|error| format!("redb write transaction failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.transaction_count = diagnostics.transaction_count.saturating_add(1);
                diagnostics.transaction_begin_nanos = diagnostics
                    .transaction_begin_nanos
                    .saturating_add(begin_start.elapsed().as_nanos());
            }
            write_txn
                .set_durability(config.redb.durability.into())
                .map_err(|error| format!("redb set durability failed: {error}"))?;
            {
                let table_open_start = Instant::now();
                let mut table = write_txn
                    .open_table(REDB_TABLE)
                    .map_err(|error| format!("redb open table for set failed: {error}"))?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.table_open_nanos = diagnostics
                        .table_open_nanos
                        .saturating_add(table_open_start.elapsed().as_nanos());
                }
                let apply_start = Instant::now();
                table
                    .insert(&redb_key, value.as_slice())
                    .map_err(|error| format!("redb insert failed: {error}"))?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.write_apply_nanos = diagnostics
                        .write_apply_nanos
                        .saturating_add(apply_start.elapsed().as_nanos());
                }
            }
            let commit_start = Instant::now();
            write_txn
                .commit()
                .map_err(|error| format!("redb commit after set failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.commit_count = diagnostics.commit_count.saturating_add(1);
                diagnostics.commit_nanos = diagnostics
                    .commit_nanos
                    .saturating_add(commit_start.elapsed().as_nanos());
            }
            if let Some(timings) = timings.as_deref_mut() {
                timings.writes_nanos = timings
                    .writes_nanos
                    .saturating_add(write_start.elapsed().as_nanos());
            }
        }
        WorkloadOp::Delete => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.deletes += 1;
            }
            let write_start = Instant::now();
            let begin_start = Instant::now();
            let mut write_txn = db
                .begin_write()
                .map_err(|error| format!("redb write transaction failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.transaction_count = diagnostics.transaction_count.saturating_add(1);
                diagnostics.transaction_begin_nanos = diagnostics
                    .transaction_begin_nanos
                    .saturating_add(begin_start.elapsed().as_nanos());
            }
            write_txn
                .set_durability(config.redb.durability.into())
                .map_err(|error| format!("redb set durability failed: {error}"))?;
            {
                let table_open_start = Instant::now();
                let mut table = write_txn
                    .open_table(REDB_TABLE)
                    .map_err(|error| format!("redb open table for delete failed: {error}"))?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.table_open_nanos = diagnostics
                        .table_open_nanos
                        .saturating_add(table_open_start.elapsed().as_nanos());
                }
                let apply_start = Instant::now();
                table
                    .remove(&redb_key)
                    .map_err(|error| format!("redb remove failed: {error}"))?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.write_apply_nanos = diagnostics
                        .write_apply_nanos
                        .saturating_add(apply_start.elapsed().as_nanos());
                }
            }
            let commit_start = Instant::now();
            write_txn
                .commit()
                .map_err(|error| format!("redb commit after delete failed: {error}"))?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.commit_count = diagnostics.commit_count.saturating_add(1);
                diagnostics.commit_nanos = diagnostics
                    .commit_nanos
                    .saturating_add(commit_start.elapsed().as_nanos());
            }
            if let Some(timings) = timings.as_deref_mut() {
                timings.writes_nanos = timings
                    .writes_nanos
                    .saturating_add(write_start.elapsed().as_nanos());
            }
        }
    }

    maybe_compact_redb(config, db, counters, timings, operation_index)?;

    Ok(ExecutedOperation {
        operation: step.operation,
    })
}

fn next_workload_step<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    rng: &mut XorShift64,
) -> PerfResult<WorkloadStep<VALUE_BYTES>> {
    let workload = config.workload;
    let map_bound = u64::try_from(workload.map_count)
        .map_err(|_| "workload.map_count does not fit in u64".to_owned())?;
    let map_index = usize::try_from(rng.next_bounded(map_bound))
        .map_err(|_| "map index conversion failed".to_owned())?;
    let (key, expected_presence) = next_key(config, rng)?;
    let operation = next_operation(workload, rng)?;
    let value = match operation {
        WorkloadOp::Set => Some(next_value::<VALUE_BYTES>(rng)),
        WorkloadOp::Read | WorkloadOp::Delete => None,
    };
    Ok(WorkloadStep {
        map_index,
        key,
        operation,
        value,
        expected_presence,
    })
}

fn next_key(config: &PerfConfig, rng: &mut XorShift64) -> PerfResult<(u64, ExpectedPresence)> {
    match config.workload.key_mode {
        WorkloadKeyMode::Random => Ok((
            rng.next_bounded(config.workload.key_space),
            ExpectedPresence::Unknown,
        )),
        WorkloadKeyMode::Preloaded => Ok((
            rng.next_bounded(config.preload.count),
            ExpectedPresence::Present,
        )),
        WorkloadKeyMode::Missing => {
            let missing_space = config
                .workload
                .key_space
                .saturating_sub(config.preload.count);
            Ok((
                config
                    .preload
                    .count
                    .checked_add(rng.next_bounded(missing_space))
                    .ok_or_else(|| "missing key generation overflowed".to_owned())?,
                ExpectedPresence::Missing,
            ))
        }
        WorkloadKeyMode::SingleKey => Ok((0, ExpectedPresence::Present)),
        WorkloadKeyMode::InsertRange => {
            let insert_space = config
                .workload
                .key_space
                .saturating_sub(config.preload.count);
            Ok((
                config
                    .preload
                    .count
                    .checked_add(rng.next_bounded(insert_space))
                    .ok_or_else(|| "insert-range key generation overflowed".to_owned())?,
                ExpectedPresence::Missing,
            ))
        }
    }
}

fn count_expected_read(counters: &mut WorkloadCounters, expected_presence: ExpectedPresence) {
    match expected_presence {
        ExpectedPresence::Present => {
            counters.read_hits_expected = counters.read_hits_expected.saturating_add(1);
        }
        ExpectedPresence::Missing => {
            counters.read_misses_expected = counters.read_misses_expected.saturating_add(1);
        }
        ExpectedPresence::Unknown => {}
    }
}

fn count_expected_set(counters: &mut WorkloadCounters, expected_presence: ExpectedPresence) {
    match expected_presence {
        ExpectedPresence::Present => {
            counters.set_updates_expected = counters.set_updates_expected.saturating_add(1);
        }
        ExpectedPresence::Missing => {
            counters.set_inserts_expected = counters.set_inserts_expected.saturating_add(1);
        }
        ExpectedPresence::Unknown => {}
    }
}

fn encode_redb_key(workload: WorkloadConfig, map_index: usize, key: u64) -> PerfResult<u64> {
    let map_index =
        u64::try_from(map_index).map_err(|_| "map index does not fit in u64".to_owned())?;
    map_index
        .checked_mul(workload.key_space)
        .and_then(|base| base.checked_add(key))
        .ok_or_else(|| "encoded redb key overflowed u64".to_owned())
}

fn next_operation(config: WorkloadConfig, rng: &mut XorShift64) -> PerfResult<WorkloadOp> {
    let total = ratio_total(config)?;
    let choice = u32::try_from(rng.next_bounded(u64::from(total)))
        .map_err(|_| "operation choice conversion failed".to_owned())?;
    if choice < config.read_ratio {
        return Ok(WorkloadOp::Read);
    }
    if choice < config.read_ratio + config.set_ratio {
        return Ok(WorkloadOp::Set);
    }
    Ok(WorkloadOp::Delete)
}

fn next_value<const VALUE_BYTES: usize>(rng: &mut XorShift64) -> HeaplessVec<u8, VALUE_BYTES> {
    let mut value = HeaplessVec::new();
    while value.len() < VALUE_BYTES {
        let bytes = rng.next_u64().to_le_bytes();
        let remaining = VALUE_BYTES - value.len();
        let chunk_len = remaining.min(bytes.len());
        for byte in &bytes[..chunk_len] {
            let _ = value.push(*byte);
        }
    }
    value
}

fn maybe_compact<
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
    map: &mut LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>,
    storage: &mut Storage<
        '_,
        InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    compact_needed: bool,
    operation_index: Option<u64>,
) -> PerfResult<()>
where
    IO: PerfBacking,
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let interval_compact = config.workload.compact_interval != 0
        && operation_index
            .is_some_and(|index| (index + 1).is_multiple_of(config.workload.compact_interval));
    if !config.workload.compact_on_signal || !(compact_needed || interval_compact) {
        return Ok(());
    }
    let compaction_start = Instant::now();
    let did_compact = map
        .compact_and_report(storage)
        .map_err(|error| format!("compaction failed: {error:?}"))?;
    if did_compact {
        if let Some(counters) = counters {
            counters.compactions += 1;
        }
    }
    if did_compact {
        if let Some(timings) = timings {
            timings.compactions_nanos = timings
                .compactions_nanos
                .saturating_add(compaction_start.elapsed().as_nanos());
        }
    }
    Ok(())
}

fn maybe_compact_redb(
    config: &PerfConfig,
    db: &mut Database,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    operation_index: Option<u64>,
) -> PerfResult<()> {
    let interval_compact = config.redb.compact_interval != 0
        && operation_index
            .is_some_and(|index| (index + 1).is_multiple_of(config.redb.compact_interval));
    if !interval_compact {
        return Ok(());
    }
    compact_redb(db, counters, timings)
}

fn compact_redb(
    db: &mut Database,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
) -> PerfResult<()> {
    let compaction_start = Instant::now();
    let did_compact = db
        .compact()
        .map_err(|error| format!("redb compaction failed: {error}"))?;
    if did_compact {
        if let Some(counters) = counters {
            counters.compactions += 1;
        }
        if let Some(timings) = timings {
            timings.compactions_nanos = timings
                .compactions_nanos
                .saturating_add(compaction_start.elapsed().as_nanos());
        }
    }
    Ok(())
}

fn run_borromean_post_workload_maintenance<
    IO,
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
    maps: &mut [LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>],
    storage: &mut Storage<
        '_,
        InstrumentedBacking<IO, REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    path: Option<&Path>,
) -> PerfResult<MaintenanceReport>
where
    IO: PerfBacking,
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    if !config.maintenance.optimize_after_workload {
        return Ok(MaintenanceReport::default());
    }
    let start = Instant::now();
    let mut compactions = 0u64;
    for map in maps.iter_mut() {
        loop {
            let did_compact = map
                .compact_and_report(storage)
                .map_err(|error| format!("post-workload compaction failed: {error:?}"))?;
            if !did_compact {
                break;
            }
            compactions = compactions.saturating_add(1);
        }
    }
    Ok(MaintenanceReport {
        ran: true,
        nanos: start.elapsed().as_nanos(),
        compactions,
        file_len_after_bytes: path.and_then(|path| current_file_len(path).ok()),
    })
}

fn run_redb_post_workload_maintenance(
    config: &PerfConfig,
    db: &mut Database,
    path: &Path,
) -> PerfResult<MaintenanceReport> {
    if !config.maintenance.optimize_after_workload {
        return Ok(MaintenanceReport::default());
    }
    let start = Instant::now();
    let did_compact = db
        .compact()
        .map_err(|error| format!("redb post-workload compaction failed: {error}"))?;
    Ok(MaintenanceReport {
        ran: true,
        nanos: start.elapsed().as_nanos(),
        compactions: if did_compact { 1 } else { 0 },
        file_len_after_bytes: current_file_len(path).ok(),
    })
}

fn collect_redb_stats(db: &Database) -> PerfResult<Option<RedbStorageStats>> {
    let txn = db
        .begin_write()
        .map_err(|error| format!("failed to begin redb stats transaction: {error}"))?;
    let stats = txn
        .stats()
        .map_err(|error| format!("failed to collect redb stats: {error}"))?;
    let report = RedbStorageStats {
        tree_height: stats.tree_height(),
        allocated_pages: stats.allocated_pages(),
        leaf_pages: stats.leaf_pages(),
        branch_pages: stats.branch_pages(),
        stored_bytes: stats.stored_bytes(),
        metadata_bytes: stats.metadata_bytes(),
        fragmented_bytes: stats.fragmented_bytes(),
        page_size: stats.page_size(),
    };
    txn.abort()
        .map_err(|error| format!("failed to abort redb stats transaction: {error}"))?;
    Ok(Some(report))
}

fn should_sample_latency(sample_interval: u64, operation_index: u64) -> bool {
    sample_interval != 0 && operation_index.is_multiple_of(sample_interval)
}

fn maybe_sample_memory(memory: &mut MemoryTracker, interval: u64, operation_index: u64) {
    if interval != 0 && operation_index.is_multiple_of(interval) {
        memory.sample();
    }
}

fn push_latency_sample(
    samples: &mut OperationLatencySamples,
    operation: WorkloadOp,
    elapsed_nanos: u128,
) {
    match operation {
        WorkloadOp::Read => samples.reads.push(elapsed_nanos),
        WorkloadOp::Set => samples.sets.push(elapsed_nanos),
        WorkloadOp::Delete => samples.deletes.push(elapsed_nanos),
    }
}

fn summarize_operation_latency(samples: OperationLatencySamples) -> OperationLatencySummaries {
    OperationLatencySummaries {
        reads: summarize_latency(samples.reads),
        sets: summarize_latency(samples.sets),
        deletes: summarize_latency(samples.deletes),
    }
}

fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let value_kib = parts.next()?.parse::<u64>().ok()?;
        return value_kib.checked_mul(1024);
    }
    None
}

fn operations_per_second(operation_count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return 0.0;
    }
    operation_count as f64 / seconds
}

fn estimate_remaining(rate: f64, remaining: u64) -> Option<Duration> {
    if remaining == 0 {
        return Some(Duration::ZERO);
    }
    if rate <= 0.0 || !rate.is_finite() {
        return None;
    }
    Some(Duration::from_secs_f64(remaining as f64 / rate))
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    if hours == 0 {
        format!("{minutes:02}:{seconds:02}")
    } else {
        format!("{hours}:{minutes:02}:{seconds:02}")
    }
}

fn summarize_latency(mut samples: Vec<u128>) -> Option<LatencySummary> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    let min_nanos = samples[0];
    let max_nanos = samples[samples.len() - 1];
    Some(LatencySummary {
        samples: samples.len(),
        min_nanos,
        p50_nanos: percentile(&samples, 50),
        p95_nanos: percentile(&samples, 95),
        p99_nanos: percentile(&samples, 99),
        max_nanos,
    })
}

fn percentile(sorted_samples: &[u128], percentile: usize) -> u128 {
    let max_index = sorted_samples.len() - 1;
    let index = max_index.saturating_mul(percentile).div_ceil(100);
    sorted_samples[index.min(max_index)]
}

fn print_report(report: &PerfReport) {
    println!("Storage perf");
    println!(
        "geometry: region_size={} region_count={} borromean_file_len={} bytes",
        report.geometry.region_size, report.geometry.region_count, report.geometry.file_len_bytes
    );
    println!(
        "workload: maps={} preload={} warmup={} operations={} key_space={} key_mode={:?} value_size={} compact_interval={}",
        report.config.workload.map_count,
        report.config.preload.count,
        report.config.workload.warmup_count,
        report.config.workload.operation_count,
        report.config.workload.key_space,
        report.config.workload.key_mode,
        report.config.workload.value_size_bytes,
        report.config.workload.compact_interval
    );
    println!(
        "engines: {}",
        report
            .engine_reports
            .iter()
            .map(|engine| engine.engine.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    for engine in &report.engine_reports {
        print_engine_report(engine);
    }
    print_comparison(report);
}

fn print_engine_report(report: &EngineReport) {
    println!();
    println!("{}:", report.engine.as_str());
    println!("  db: {}", report.path.display());
    println!(
        "  create+format: {:.3} ms",
        nanos_to_millis(report.create_format_nanos)
    );
    println!("  setup: {:.3} ms", nanos_to_millis(report.setup_nanos));
    println!("  preload: {:.3} ms", nanos_to_millis(report.preload_nanos));
    println!("  warmup: {:.3} ms", nanos_to_millis(report.warmup_nanos));
    println!(
        "  workload: {:.3} ms",
        nanos_to_millis(report.workload_nanos)
    );
    println!("  throughput: {:.2} ops/s", report.operations_per_second);
    if report.engine == EngineKind::BorromeanMemory {
        println!("  logical size: {} bytes", report.logical_len_bytes);
    } else {
        println!("  file size: {} bytes", report.file_len_bytes);
    }
    if report.preload_counters.sets != 0 {
        print_counters("  preload counts", &report.preload_counters);
    }
    print_counters("  counts", &report.counters);
    print_workload_timing_split(report);
    if let Some(latency) = &report.sampled_latency {
        println!(
            "  sampled latency: samples={} min={} p50={} p95={} p99={} max={}",
            latency.samples,
            format_nanos(latency.min_nanos),
            format_nanos(latency.p50_nanos),
            format_nanos(latency.p95_nanos),
            format_nanos(latency.p99_nanos),
            format_nanos(latency.max_nanos)
        );
    }
    print_operation_latency(
        "  sampled read latency",
        &report.sampled_latency_by_op.reads,
    );
    print_operation_latency("  sampled set latency", &report.sampled_latency_by_op.sets);
    print_operation_latency(
        "  sampled delete latency",
        &report.sampled_latency_by_op.deletes,
    );
    print_diagnostics(report);
    if report.maintenance.ran {
        println!(
            "  post-workload maintenance: {} compactions in {} file_len_after={}",
            report.maintenance.compactions,
            format_nanos(report.maintenance.nanos),
            report
                .maintenance
                .file_len_after_bytes
                .map_or_else(|| "unknown".to_owned(), |bytes| bytes.to_string())
        );
    }
}

fn print_counters(label: &str, counters: &WorkloadCounters) {
    println!(
        "{label}: reads={} hits={} misses={} expected_read_hits={} expected_read_misses={} sets={} expected_inserts={} expected_updates={} deletes={} compactions={}",
        counters.reads,
        counters.hits,
        counters.misses,
        counters.read_hits_expected,
        counters.read_misses_expected,
        counters.sets,
        counters.set_inserts_expected,
        counters.set_updates_expected,
        counters.deletes,
        counters.compactions
    );
}

fn print_operation_latency(label: &str, latency: &Option<LatencySummary>) {
    if let Some(latency) = latency {
        println!(
            "{label}: samples={} min={} p50={} p95={} p99={} max={}",
            latency.samples,
            format_nanos(latency.min_nanos),
            format_nanos(latency.p50_nanos),
            format_nanos(latency.p95_nanos),
            format_nanos(latency.p99_nanos),
            format_nanos(latency.max_nanos)
        );
    }
}

fn print_diagnostics(report: &EngineReport) {
    let diagnostics = &report.diagnostics;
    println!(
        "  diagnostics: op_gen={} read_lookup={} write_apply={} tx_begin={} table_open={} commit={} commits={} transactions={}",
        format_nanos(diagnostics.operation_generation_nanos),
        format_nanos(diagnostics.read_lookup_nanos),
        format_nanos(diagnostics.write_apply_nanos),
        format_nanos(diagnostics.transaction_begin_nanos),
        format_nanos(diagnostics.table_open_nanos),
        format_nanos(diagnostics.commit_nanos),
        diagnostics.commit_count,
        diagnostics.transaction_count
    );
    println!(
        "  memory: rss_start={} rss_peak={} rss_end={} rss_delta={}",
        format_optional_bytes(diagnostics.memory.rss_start_bytes),
        format_optional_bytes(diagnostics.memory.rss_peak_bytes),
        format_optional_bytes(diagnostics.memory.rss_end_bytes),
        diagnostics
            .memory
            .rss_delta_bytes
            .map_or_else(|| "unknown".to_owned(), |bytes| bytes.to_string())
    );
    if let Some(io) = diagnostics.borromean_io {
        println!(
            "  io: metadata_reads={} metadata_writes={} region_reads={} region_writes={} region_erases={} syncs={} bytes_read={} bytes_written={} bytes_erased={} dirty_sync_bytes={} dirty_sync_regions={} dirty_sync_metadata_regions={} exact_dirty_range_bytes={} aligned_dirty_bytes={} requested_mmap_flush_bytes={} flush_overreach_bytes={} file_sync_kind={} read_time={} write_time={} erase_time={} sync_time={} mmap_flush={} file_sync={}",
            io.metadata_reads,
            io.metadata_writes,
            io.region_reads,
            io.region_writes,
            io.region_erases,
            io.syncs,
            io.bytes_read,
            io.bytes_written,
            io.bytes_erased,
            io.dirty_sync_bytes,
            io.dirty_sync_regions,
            io.dirty_sync_metadata_regions,
            io.exact_dirty_range_bytes,
            io.aligned_dirty_bytes,
            io.requested_mmap_flush_bytes,
            io.flush_overreach_bytes,
            format_sync_file_kind(io.last_file_sync_kind),
            format_nanos(io.read_region_nanos),
            format_nanos(io.write_region_nanos),
            format_nanos(io.erase_region_nanos),
            format_nanos(io.sync_nanos),
            format_nanos(io.mmap_flush_nanos),
            format_nanos(io.file_sync_nanos)
        );
    }
    if let Some(metrics) = report.borromean_core_metrics {
        print_borromean_core_metrics(&metrics);
    }
    if let Some(audit) = &report.sync_audit {
        print_sync_audit(audit);
    }
    if let Some(cache_size) = diagnostics.redb_cache_size_bytes {
        println!("  redb cache_size: {cache_size} bytes");
    }
    if let Some(stats) = diagnostics.redb_stats {
        println!(
            "  redb stats: tree_height={} allocated_pages={} leaf_pages={} branch_pages={} stored_bytes={} metadata_bytes={} fragmented_bytes={} page_size={}",
            stats.tree_height,
            stats.allocated_pages,
            stats.leaf_pages,
            stats.branch_pages,
            stats.stored_bytes,
            stats.metadata_bytes,
            stats.fragmented_bytes,
            stats.page_size
        );
    }
}

fn print_sync_audit(audit: &SyncAuditReport) {
    println!(
        "  sync audit: writes={} wal_syncs/write={} io_syncs/write={} wal_records/write={} io_region_writes/write={} dirty_regions/sync={} metadata_writes={} non_hot_exceptions={}",
        audit.write_operations,
        format_optional_ratio_value(audit.wal_syncs_per_write),
        format_optional_ratio_value(audit.io_syncs_per_write),
        format_optional_ratio_value(audit.wal_records_per_write),
        format_optional_ratio_value(audit.io_region_writes_per_write),
        format_optional_ratio_value(audit.dirty_regions_per_sync),
        audit.metadata_writes_during_workload,
        audit.non_hot_write_exceptions
    );
    println!(
        "  sync audit bytes: wal_bytes={} exact_dirty_range_bytes={} aligned_dirty_bytes={} requested_mmap_flush_bytes={} flush_overreach_bytes={}",
        audit.wal_bytes,
        audit.exact_dirty_range_bytes,
        audit.aligned_dirty_bytes,
        audit.requested_mmap_flush_bytes,
        audit.flush_overreach_bytes
    );
    for violation in &audit.first_violations {
        println!(
            "  sync audit violation: op={} kind={:?} expected_exception={} reasons={} delta_wal_syncs={} delta_io_syncs={} delta_wal_records={} delta_region_writes={} delta_flushes={} delta_compactions={} delta_rotations={}",
            violation.operation_index,
            violation.operation,
            violation.expected_exception,
            violation.reasons.join("; "),
            violation.delta.wal_syncs,
            violation.delta.io_syncs,
            violation.delta.wal_records,
            violation.delta.io_region_writes,
            violation.delta.flushes,
            violation.delta.compactions,
            violation.delta.wal_rotations_attempted
        );
    }
}

fn print_borromean_core_metrics(metrics: &StoragePerfMetrics) {
    let write_ops = metrics.map_sets.saturating_add(metrics.map_deletes);
    let map_ops = metrics
        .map_reads
        .saturating_add(metrics.map_sets)
        .saturating_add(metrics.map_deletes);
    let wal_bytes_per_op = if map_ops == 0 {
        0.0
    } else {
        metrics.wal_bytes as f64 / map_ops as f64
    };

    println!(
        "  borromean core: reads={} sets={} deletes={} cache_hits={} cache_misses={} reloads={}",
        metrics.map_reads,
        metrics.map_sets,
        metrics.map_deletes,
        metrics.frontier_cache_hits,
        metrics.frontier_cache_misses,
        metrics.frontier_reloads
    );
    println!(
        "  borromean wal: records={} update_records={} bytes={} bytes/op={:.1} syncs={} avg_sync={} wal_encode={} wal_write={} wal_sync={} mmap_flush={} file_sync={}",
        metrics.wal_records,
        metrics.wal_update_records,
        metrics.wal_bytes,
        wal_bytes_per_op,
        metrics.wal_syncs,
        format_average_nanos(metrics.wal_sync_nanos, metrics.wal_syncs),
        format_nanos(metrics.wal_encode_nanos),
        format_nanos(metrics.wal_write_nanos),
        format_nanos(metrics.wal_sync_nanos),
        format_nanos(metrics.mmap_flush_nanos),
        format_nanos(metrics.file_sync_nanos)
    );
    println!(
        "  borromean write path: full={} avg_write={} update_encode={} frontier_checkpoint={} frontier_apply={} overflow_flushes={} overflow_flush_time={}",
        format_nanos(metrics.full_write_path_nanos),
        format_average_nanos(metrics.full_write_path_nanos, write_ops),
        format_nanos(metrics.update_encode_nanos),
        format_nanos(metrics.frontier_checkpoint_nanos),
        format_nanos(metrics.frontier_apply_nanos),
        metrics.overflow_flushes,
        format_nanos(metrics.overflow_flush_nanos)
    );
    println!(
        "  borromean maintenance core: compaction_checks={} compactions={} compaction_check_time={} compaction_time={} flushes={} reclaim_begin={} reclaim_end={} dirty_sync_bytes={} dirty_sync_regions={} dirty_sync_metadata_regions={} buffer_too_small={} wal_rotation_required={} append_failures={}",
        metrics.compaction_checks,
        metrics.compactions_run,
        format_nanos(metrics.compaction_check_nanos),
        format_nanos(metrics.compaction_nanos),
        metrics.flushes,
        metrics.reclaim_starts,
        metrics.reclaim_ends,
        metrics.dirty_sync_bytes,
        metrics.dirty_sync_regions,
        metrics.dirty_sync_metadata_regions,
        metrics.buffer_too_small_errors,
        metrics.wal_rotation_required,
        metrics.append_failures
    );
}

fn print_workload_timing_split(report: &EngineReport) {
    let timings = &report.workload_timings;
    let measured_nanos = timings
        .operation_generation_nanos
        .saturating_add(timings.reads_nanos)
        .saturating_add(timings.writes_nanos)
        .saturating_add(timings.compactions_nanos);
    let other_nanos = report.workload_nanos.saturating_sub(measured_nanos);
    println!(
        "  time split: op_gen={} ({:.1}%) reads={} ({:.1}%) writes={} ({:.1}%) compactions={} ({:.1}%) other={} ({:.1}%)",
        format_nanos(timings.operation_generation_nanos),
        percent_of(timings.operation_generation_nanos, report.workload_nanos),
        format_nanos(timings.reads_nanos),
        percent_of(timings.reads_nanos, report.workload_nanos),
        format_nanos(timings.writes_nanos),
        percent_of(timings.writes_nanos, report.workload_nanos),
        format_nanos(timings.compactions_nanos),
        percent_of(timings.compactions_nanos, report.workload_nanos),
        format_nanos(other_nanos),
        percent_of(other_nanos, report.workload_nanos)
    );

    let write_count = report.counters.sets.saturating_add(report.counters.deletes);
    println!(
        "  average timing: read={} write={} compaction={}",
        format_average_nanos(timings.reads_nanos, report.counters.reads),
        format_average_nanos(timings.writes_nanos, write_count),
        format_average_nanos(timings.compactions_nanos, report.counters.compactions)
    );
}

fn build_comparison_reports(engine_reports: &[EngineReport]) -> Vec<EngineComparisonReport> {
    let mut comparisons = Vec::new();
    push_comparison_report(
        engine_reports,
        &mut comparisons,
        EngineKind::Borromean,
        EngineKind::BorromeanMemory,
    );
    push_comparison_report(
        engine_reports,
        &mut comparisons,
        EngineKind::BorromeanMemory,
        EngineKind::Redb,
    );
    push_comparison_report(
        engine_reports,
        &mut comparisons,
        EngineKind::Borromean,
        EngineKind::Redb,
    );
    comparisons
}

fn push_comparison_report(
    engine_reports: &[EngineReport],
    comparisons: &mut Vec<EngineComparisonReport>,
    left_engine: EngineKind,
    right_engine: EngineKind,
) {
    let Some(left) = find_engine_report_in(engine_reports, left_engine) else {
        return;
    };
    let Some(right) = find_engine_report_in(engine_reports, right_engine) else {
        return;
    };
    comparisons.push(EngineComparisonReport {
        left_engine,
        right_engine,
        throughput_ratio: ratio_f64(left.operations_per_second, right.operations_per_second),
        p50_latency_ratio: latency_ratio(
            left.sampled_latency.as_ref(),
            right.sampled_latency.as_ref(),
            |latency| latency.p50_nanos,
        ),
        p95_latency_ratio: latency_ratio(
            left.sampled_latency.as_ref(),
            right.sampled_latency.as_ref(),
            |latency| latency.p95_nanos,
        ),
        p99_latency_ratio: latency_ratio(
            left.sampled_latency.as_ref(),
            right.sampled_latency.as_ref(),
            |latency| latency.p99_nanos,
        ),
        sync_time_ratio: ratio_u128(sync_nanos(left), sync_nanos(right)),
        compaction_time_ratio: ratio_u128(
            left.workload_timings.compactions_nanos,
            right.workload_timings.compactions_nanos,
        ),
        average_read_time_ratio: ratio_u128(
            average_nanos(left.workload_timings.reads_nanos, left.counters.reads),
            average_nanos(right.workload_timings.reads_nanos, right.counters.reads),
        ),
        average_write_time_ratio: ratio_u128(
            average_nanos(left.workload_timings.writes_nanos, write_count(left)),
            average_nanos(right.workload_timings.writes_nanos, write_count(right)),
        ),
        logical_size_ratio: ratio_f64(
            left.logical_len_bytes as f64,
            right.logical_len_bytes as f64,
        ),
    });
}

fn print_comparison(report: &PerfReport) {
    for comparison in &report.comparison_reports {
        println!();
        println!(
            "comparison {}/{}:",
            comparison.left_engine.comparison_label(),
            comparison.right_engine.comparison_label()
        );
        print_optional_ratio("  throughput", comparison.throughput_ratio, "x");
        print_optional_ratio("  p50 latency", comparison.p50_latency_ratio, "x");
        print_optional_ratio("  p95 latency", comparison.p95_latency_ratio, "x");
        print_optional_ratio("  p99 latency", comparison.p99_latency_ratio, "x");
        print_optional_ratio("  sync time", comparison.sync_time_ratio, "x");
        print_optional_ratio("  compaction time", comparison.compaction_time_ratio, "x");
        print_optional_ratio(
            "  average read time",
            comparison.average_read_time_ratio,
            "x",
        );
        print_optional_ratio(
            "  average write time",
            comparison.average_write_time_ratio,
            "x",
        );
        print_optional_ratio("  logical size", comparison.logical_size_ratio, "x");
    }
}

fn find_engine_report_in(
    engine_reports: &[EngineReport],
    engine: EngineKind,
) -> Option<&EngineReport> {
    engine_reports
        .iter()
        .find(|candidate| candidate.engine == engine)
}

fn print_optional_ratio(label: &str, ratio: Option<f64>, suffix: &str) {
    match ratio {
        Some(ratio) if ratio.is_finite() => println!("{label}: {ratio:.3}{suffix}"),
        _ => println!("{label}: n/a"),
    }
}

fn format_optional_ratio_value(ratio: Option<f64>) -> String {
    match ratio {
        Some(ratio) if ratio.is_finite() => format!("{ratio:.3}"),
        _ => "n/a".to_owned(),
    }
}

fn format_sync_file_kind(kind: Option<SyncFileKind>) -> &'static str {
    match kind {
        Some(SyncFileKind::SyncAll) => "sync_all",
        None => "none",
    }
}

fn ratio_u64(numerator: u64, denominator: u64) -> Option<f64> {
    if denominator == 0 {
        return None;
    }
    Some(numerator as f64 / denominator as f64)
}

fn ratio_f64(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator == 0.0 {
        return None;
    }
    let ratio = numerator / denominator;
    ratio.is_finite().then_some(ratio)
}

fn ratio_u128(numerator: u128, denominator: u128) -> Option<f64> {
    if denominator == 0 {
        return None;
    }
    Some(numerator as f64 / denominator as f64)
}

fn latency_ratio(
    left: Option<&LatencySummary>,
    right: Option<&LatencySummary>,
    accessor: fn(&LatencySummary) -> u128,
) -> Option<f64> {
    ratio_u128(accessor(left?), accessor(right?))
}

fn sync_nanos(report: &EngineReport) -> u128 {
    report
        .diagnostics
        .borromean_io
        .map_or(report.diagnostics.commit_nanos, |io| io.sync_nanos)
}

fn write_count(report: &EngineReport) -> u64 {
    report.counters.sets.saturating_add(report.counters.deletes)
}

fn average_nanos(total_nanos: u128, count: u64) -> u128 {
    if count == 0 {
        return 0;
    }
    total_nanos / u128::from(count)
}

fn nanos_to_millis(nanos: u128) -> f64 {
    nanos as f64 / 1_000_000.0
}

fn percent_of(part: u128, total: u128) -> f64 {
    if total == 0 {
        return 0.0;
    }
    (part as f64 / total as f64) * 100.0
}

fn format_average_nanos(total_nanos: u128, count: u64) -> String {
    if count == 0 {
        return "n/a".to_owned();
    }
    format_nanos(total_nanos / u128::from(count))
}

fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map_or_else(|| "unknown".to_owned(), |bytes| bytes.to_string())
}

fn format_nanos(nanos: u128) -> String {
    const NANOS_PER_MICRO: u128 = 1_000;
    const NANOS_PER_MILLI: u128 = 1_000_000;
    const NANOS_PER_SECOND: u128 = 1_000_000_000;

    if nanos < NANOS_PER_MICRO {
        return format!("{nanos} ns");
    }
    if nanos < NANOS_PER_MILLI {
        return format!("{:.3} us", nanos as f64 / NANOS_PER_MICRO as f64);
    }
    if nanos < NANOS_PER_SECOND {
        return format!("{:.3} ms", nanos as f64 / NANOS_PER_MILLI as f64);
    }
    format!("{:.3} s", nanos as f64 / NANOS_PER_SECOND as f64)
}

#[cfg(test)]
mod tests {
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
}

fn write_json_report(report: &PerfReport) -> PerfResult<()> {
    let Some(path) = &report.config.output.json_path else {
        return Ok(());
    };
    if let Some(parent) = non_empty_parent(path) {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|error| format!("failed to encode JSON report: {error}"))?;
    fs::write(path, json)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    println!("  json: {}", path.display());
    Ok(())
}

impl GeometryConfig {
    fn effective_region_count(self) -> PerfResult<usize> {
        match (self.region_count, self.db_size_bytes) {
            (Some(region_count), None) => Ok(region_count),
            (None, Some(db_size_bytes)) => {
                let total_region_slots = db_size_bytes
                    .checked_div(self.region_size)
                    .ok_or_else(|| "geometry.region_size must be greater than zero".to_owned())?;
                if db_size_bytes % self.region_size != 0 {
                    return Err(format!(
                        "geometry.db_size_bytes={db_size_bytes} is not a multiple of region_size={}",
                        self.region_size
                    ));
                }
                total_region_slots.checked_sub(1).ok_or_else(|| {
                    "geometry.db_size_bytes must include metadata plus at least one data region"
                        .to_owned()
                })
            }
            (Some(_), Some(_)) => Err(
                "set either geometry.region_count or geometry.db_size_bytes, not both".to_owned(),
            ),
            (None, None) => Err("set geometry.region_count or geometry.db_size_bytes".to_owned()),
        }
    }
}
