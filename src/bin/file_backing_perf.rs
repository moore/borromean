use std::env;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use borromean::{
    AllocationPolicy, FileBacking, FileBackingOptions, LsmMap, MadvisePolicy, Storage,
    StorageFormatConfig,
};
use heapless::Vec as HeaplessVec;
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
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
    workload: WorkloadConfig,
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
    Redb,
}

impl EngineKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Borromean => "borromean",
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
            compact_on_signal: true,
            compact_interval: 0,
            compaction_region_target: None,
        }
    }
}

fn default_value_size_bytes() -> usize {
    8
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
    deletes: u64,
    compactions: u64,
    misses: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct WorkloadTimings {
    reads_nanos: u128,
    writes_nanos: u128,
    compactions_nanos: u128,
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
}

#[derive(Debug, Clone, Serialize)]
struct EngineReport {
    engine: EngineKind,
    path: PathBuf,
    create_format_nanos: u128,
    setup_nanos: u128,
    warmup_nanos: u128,
    workload_nanos: u128,
    operations_per_second: f64,
    counters: WorkloadCounters,
    workload_timings: WorkloadTimings,
    sampled_latency: Option<LatencySummary>,
    file_len_bytes: u64,
}

#[derive(Debug, Clone)]
struct WorkloadStep<const VALUE_BYTES: usize> {
    map_index: usize,
    key: u64,
    operation: WorkloadOp,
    value: Option<HeaplessVec<u8, VALUE_BYTES>>,
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

#[derive(Debug, Clone, Copy)]
enum WorkloadOp {
    Read,
    Set,
    Delete,
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
            EngineKind::Redb => run_redb_engine::<VALUE_BYTES>(config)?,
        };
        engine_reports.push(engine_report);
    }

    Ok(PerfReport {
        config: config.clone(),
        geometry,
        engine_reports,
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
    prepare_db_path(&config.backing)?;
    let options = file_backing_options(&config.backing);

    eprintln!(
        "[file-backing-perf] preparing borromean {} with region_size={} region_count={} value_size={} operations={}",
        config.backing.path.display(),
        REGION_SIZE,
        REGION_COUNT,
        VALUE_BYTES,
        config.workload.operation_count
    );
    let create_format_start = Instant::now();
    let mut backing =
        FileBacking::<REGION_SIZE, REGION_COUNT>::create_new(&config.backing.path, options)
            .map_err(|error| format!("failed to create file backing: {error:?}"))?;
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

    let mut counters = WorkloadCounters::default();
    let mut workload_timings = WorkloadTimings::default();
    let mut latency_samples = Vec::new();
    let workload_start = Instant::now();
    let mut workload_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.operation_count,
    );
    for index in 0..config.workload.operation_count {
        let sample = should_sample_latency(config.output.latency_sample_interval, index);
        let operation_start = sample.then(Instant::now);
        execute_one_borromean_operation(
            config,
            &mut rng,
            &mut maps,
            &mut storage,
            Some(&mut counters),
            Some(&mut workload_timings),
            Some(index),
        )
        .map_err(|error| format!("workload operation {index} failed: {error}"))?;
        if let Some(start) = operation_start {
            latency_samples.push(start.elapsed().as_nanos());
        }
        workload_progress.maybe_report(
            "borromean workload",
            index + 1,
            Some(&counters),
            latency_samples.len(),
        );
    }
    let workload = workload_start.elapsed();
    workload_progress.report(
        "borromean workload complete",
        config.workload.operation_count,
        Some(&counters),
        latency_samples.len(),
    );

    drop(maps);
    drop(storage);
    drop(backing);
    let file_len_bytes = current_file_len(&config.backing.path)?;
    if config.backing.remove_after {
        remove_file_if_present(&config.backing.path)?;
    }

    Ok(EngineReport {
        engine: EngineKind::Borromean,
        path: config.backing.path.clone(),
        create_format_nanos: create_format.as_nanos(),
        setup_nanos: map_setup.as_nanos(),
        warmup_nanos: warmup.as_nanos(),
        workload_nanos: workload.as_nanos(),
        operations_per_second: operations_per_second(config.workload.operation_count, workload),
        counters,
        workload_timings,
        sampled_latency: summarize_latency(latency_samples),
        file_len_bytes,
    })
}

fn run_redb_engine<const VALUE_BYTES: usize>(config: &PerfConfig) -> PerfResult<EngineReport> {
    prepare_path(&config.redb.path, config.redb.remove_existing)?;

    eprintln!(
        "[file-backing-perf] preparing redb {} with value_size={} operations={}",
        config.redb.path.display(),
        VALUE_BYTES,
        config.workload.operation_count
    );
    let create_format_start = Instant::now();
    let mut db = Database::create(&config.redb.path)
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
    let workload_start = Instant::now();
    let mut workload_progress = ProgressReporter::new(
        config.output.progress_interval,
        config.workload.operation_count,
    );
    for index in 0..config.workload.operation_count {
        let sample = should_sample_latency(config.output.latency_sample_interval, index);
        let operation_start = sample.then(Instant::now);
        execute_one_redb_operation::<VALUE_BYTES>(
            config,
            &mut rng,
            &mut db,
            Some(&mut counters),
            Some(&mut workload_timings),
            Some(index),
        )
        .map_err(|error| format!("workload operation {index} failed: {error}"))?;
        if let Some(start) = operation_start {
            latency_samples.push(start.elapsed().as_nanos());
        }
        workload_progress.maybe_report(
            "redb workload",
            index + 1,
            Some(&counters),
            latency_samples.len(),
        );
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

    let file_len_bytes = current_file_len(&config.redb.path)?;
    drop(db);
    if config.redb.remove_after {
        remove_file_if_present(&config.redb.path)?;
    }

    Ok(EngineReport {
        engine: EngineKind::Redb,
        path: config.redb.path.clone(),
        create_format_nanos: create_format.as_nanos(),
        setup_nanos: setup.as_nanos(),
        warmup_nanos: warmup.as_nanos(),
        workload_nanos: workload.as_nanos(),
        operations_per_second: operations_per_second(config.workload.operation_count, workload),
        counters,
        workload_timings,
        sampled_latency: summarize_latency(latency_samples),
        file_len_bytes,
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

fn execute_one_borromean_operation<
    const REGION_SIZE: usize,
    const REGION_COUNT: usize,
    const VALUE_BYTES: usize,
>(
    config: &PerfConfig,
    rng: &mut XorShift64,
    maps: &mut [LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>],
    storage: &mut Storage<
        '_,
        FileBacking<REGION_SIZE, REGION_COUNT>,
        REGION_SIZE,
        REGION_COUNT,
        MAX_COLLECTIONS,
        MAX_PENDING_RECLAIMS,
    >,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    operation_index: Option<u64>,
) -> PerfResult<()>
where
    HeaplessVec<u8, VALUE_BYTES>: Debug + Serialize + for<'de> Deserialize<'de>,
{
    let step = next_workload_step::<VALUE_BYTES>(config.workload, rng)?;
    let mut counters = counters;
    let mut timings = timings;

    match step.operation {
        WorkloadOp::Read => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.reads += 1;
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
            }
        }
        WorkloadOp::Set => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.sets += 1;
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

    Ok(())
}

fn execute_one_redb_operation<const VALUE_BYTES: usize>(
    config: &PerfConfig,
    rng: &mut XorShift64,
    db: &mut Database,
    counters: Option<&mut WorkloadCounters>,
    timings: Option<&mut WorkloadTimings>,
    operation_index: Option<u64>,
) -> PerfResult<()> {
    let step = next_workload_step::<VALUE_BYTES>(config.workload, rng)?;
    let redb_key = encode_redb_key(config.workload, step.map_index, step.key)?;
    let mut counters = counters;
    let mut timings = timings;

    match step.operation {
        WorkloadOp::Read => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.reads += 1;
            }
            let read_start = Instant::now();
            let read_txn = db
                .begin_read()
                .map_err(|error| format!("redb read transaction failed: {error}"))?;
            let table = read_txn
                .open_table(REDB_TABLE)
                .map_err(|error| format!("redb open table for read failed: {error}"))?;
            let result = table
                .get(&redb_key)
                .map_err(|error| format!("redb get failed: {error}"))?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.reads_nanos = timings
                    .reads_nanos
                    .saturating_add(read_start.elapsed().as_nanos());
            }
            if result.is_none() {
                if let Some(counters) = counters.as_deref_mut() {
                    counters.misses += 1;
                }
            }
        }
        WorkloadOp::Set => {
            if let Some(counters) = counters.as_deref_mut() {
                counters.sets += 1;
            }
            let value = step
                .value
                .ok_or_else(|| "set operation missing generated value".to_owned())?;
            let write_start = Instant::now();
            let mut write_txn = db
                .begin_write()
                .map_err(|error| format!("redb write transaction failed: {error}"))?;
            write_txn
                .set_durability(config.redb.durability.into())
                .map_err(|error| format!("redb set durability failed: {error}"))?;
            {
                let mut table = write_txn
                    .open_table(REDB_TABLE)
                    .map_err(|error| format!("redb open table for set failed: {error}"))?;
                table
                    .insert(&redb_key, value.as_slice())
                    .map_err(|error| format!("redb insert failed: {error}"))?;
            }
            write_txn
                .commit()
                .map_err(|error| format!("redb commit after set failed: {error}"))?;
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
            let mut write_txn = db
                .begin_write()
                .map_err(|error| format!("redb write transaction failed: {error}"))?;
            write_txn
                .set_durability(config.redb.durability.into())
                .map_err(|error| format!("redb set durability failed: {error}"))?;
            {
                let mut table = write_txn
                    .open_table(REDB_TABLE)
                    .map_err(|error| format!("redb open table for delete failed: {error}"))?;
                table
                    .remove(&redb_key)
                    .map_err(|error| format!("redb remove failed: {error}"))?;
            }
            write_txn
                .commit()
                .map_err(|error| format!("redb commit after delete failed: {error}"))?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.writes_nanos = timings
                    .writes_nanos
                    .saturating_add(write_start.elapsed().as_nanos());
            }
        }
    }

    maybe_compact_redb(config, db, counters, timings, operation_index)?;

    Ok(())
}

fn next_workload_step<const VALUE_BYTES: usize>(
    workload: WorkloadConfig,
    rng: &mut XorShift64,
) -> PerfResult<WorkloadStep<VALUE_BYTES>> {
    let map_bound = u64::try_from(workload.map_count)
        .map_err(|_| "workload.map_count does not fit in u64".to_owned())?;
    let map_index = usize::try_from(rng.next_bounded(map_bound))
        .map_err(|_| "map index conversion failed".to_owned())?;
    let key = rng.next_bounded(workload.key_space);
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
    })
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

fn maybe_compact<const REGION_SIZE: usize, const REGION_COUNT: usize, const VALUE_BYTES: usize>(
    config: &PerfConfig,
    map: &mut LsmMap<u64, HeaplessVec<u8, VALUE_BYTES>, MAX_INDEXES, MAX_RUNS>,
    storage: &mut Storage<
        '_,
        FileBacking<REGION_SIZE, REGION_COUNT>,
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

fn should_sample_latency(sample_interval: u64, operation_index: u64) -> bool {
    sample_interval != 0 && operation_index.is_multiple_of(sample_interval)
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
        "workload: maps={} warmup={} operations={} key_space={} value_size={} compact_interval={}",
        report.config.workload.map_count,
        report.config.workload.warmup_count,
        report.config.workload.operation_count,
        report.config.workload.key_space,
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
    println!("  warmup: {:.3} ms", nanos_to_millis(report.warmup_nanos));
    println!(
        "  workload: {:.3} ms",
        nanos_to_millis(report.workload_nanos)
    );
    println!("  throughput: {:.2} ops/s", report.operations_per_second);
    println!("  file size: {} bytes", report.file_len_bytes);
    println!(
        "  counts: reads={} sets={} deletes={} compactions={} misses={}",
        report.counters.reads,
        report.counters.sets,
        report.counters.deletes,
        report.counters.compactions,
        report.counters.misses
    );
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
}

fn print_workload_timing_split(report: &EngineReport) {
    let timings = &report.workload_timings;
    let measured_nanos = timings
        .reads_nanos
        .saturating_add(timings.writes_nanos)
        .saturating_add(timings.compactions_nanos);
    let other_nanos = report.workload_nanos.saturating_sub(measured_nanos);
    println!(
        "  time split: reads={} ({:.1}%) writes={} ({:.1}%) compactions={} ({:.1}%) other={} ({:.1}%)",
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
        "  average timing: write={} compaction={}",
        format_average_nanos(timings.writes_nanos, write_count),
        format_average_nanos(timings.compactions_nanos, report.counters.compactions)
    );
}

fn print_comparison(report: &PerfReport) {
    let Some(borromean) = find_engine_report(report, EngineKind::Borromean) else {
        return;
    };
    let Some(redb) = find_engine_report(report, EngineKind::Redb) else {
        return;
    };

    println!();
    println!("comparison redb/borromean:");
    print_ratio(
        "  throughput",
        redb.operations_per_second,
        borromean.operations_per_second,
        "x",
    );
    if let (Some(redb_latency), Some(borromean_latency)) =
        (&redb.sampled_latency, &borromean.sampled_latency)
    {
        print_ratio_u128(
            "  p50 latency",
            redb_latency.p50_nanos,
            borromean_latency.p50_nanos,
        );
        print_ratio_u128(
            "  p95 latency",
            redb_latency.p95_nanos,
            borromean_latency.p95_nanos,
        );
        print_ratio_u128(
            "  p99 latency",
            redb_latency.p99_nanos,
            borromean_latency.p99_nanos,
        );
    }
    print_ratio(
        "  file size",
        redb.file_len_bytes as f64,
        borromean.file_len_bytes as f64,
        "x",
    );
}

fn find_engine_report(report: &PerfReport, engine: EngineKind) -> Option<&EngineReport> {
    report
        .engine_reports
        .iter()
        .find(|candidate| candidate.engine == engine)
}

fn print_ratio(label: &str, numerator: f64, denominator: f64, suffix: &str) {
    if denominator == 0.0 {
        println!("{label}: n/a");
        return;
    }
    println!("{label}: {:.3}{suffix}", numerator / denominator);
}

fn print_ratio_u128(label: &str, numerator: u128, denominator: u128) {
    if denominator == 0 {
        println!("{label}: n/a");
        return;
    }
    println!("{label}: {:.3}x", numerator as f64 / denominator as f64);
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
