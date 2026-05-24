use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use borromean::{AllocationPolicy, FileBacking, FileBackingOptions, MadvisePolicy};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

const REGION_SIZE: usize = 65_536;
const REGION_COUNT: usize = 64;
const IO_SIZE: usize = 4_096;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(0);

struct BenchDb<const REGION_SIZE: usize, const REGION_COUNT: usize> {
    path: PathBuf,
    backing: FileBacking<REGION_SIZE, REGION_COUNT>,
}

impl<const REGION_SIZE: usize, const REGION_COUNT: usize> Drop
    for BenchDb<REGION_SIZE, REGION_COUNT>
{
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20)
}

fn file_backing_options() -> FileBackingOptions {
    let mut options = FileBackingOptions::new(0xff);
    options.allocation_policy = AllocationPolicy::FallbackOnUnsupported;
    options.madvise_policy = MadvisePolicy::Normal;
    options.sync_on_create = false;
    options
}

fn next_path(label: &str) -> PathBuf {
    let id = NEXT_DB_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("target/criterion/file_backing_mmap")
        .join(format!("{label}-{}-{id}.db", process::id()))
}

fn prepare_path(path: &Path) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create criterion DB directory");
    }
    let _ = fs::remove_file(path);
}

fn create_db_at_path<const REGION_SIZE: usize, const REGION_COUNT: usize>(
    path: PathBuf,
) -> BenchDb<REGION_SIZE, REGION_COUNT> {
    prepare_path(&path);
    let backing =
        FileBacking::<REGION_SIZE, REGION_COUNT>::create_new(&path, file_backing_options())
            .expect("create FileBacking benchmark DB");
    BenchDb { path, backing }
}

fn create_db<const REGION_SIZE: usize, const REGION_COUNT: usize>(
    label: &str,
) -> BenchDb<REGION_SIZE, REGION_COUNT> {
    create_db_at_path(next_path(label))
}

fn bench_raw_region_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_backing_mmap/raw_region_io");
    group.throughput(Throughput::Bytes(IO_SIZE as u64));

    {
        let mut db = create_db::<REGION_SIZE, REGION_COUNT>("read-region-4k");
        let payload = [0x5au8; IO_SIZE];
        db.backing
            .write_region(1, 0, &payload)
            .expect("seed read benchmark region");
        let mut buffer = [0u8; IO_SIZE];
        group.bench_function("read_region_4k", |bencher| {
            bencher.iter(|| {
                db.backing
                    .read_region(black_box(1), black_box(0), black_box(&mut buffer))
                    .expect("read benchmark region");
                black_box(buffer[0]);
            });
        });
    }

    {
        let mut db = create_db::<REGION_SIZE, REGION_COUNT>("write-region-4k");
        let payload = [0xa5u8; IO_SIZE];
        group.bench_function("write_region_4k", |bencher| {
            bencher.iter(|| {
                db.backing
                    .write_region(black_box(1), black_box(0), black_box(&payload))
                    .expect("write benchmark region");
            });
        });
    }

    group.finish();
}

fn bench_region_erase(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_backing_mmap/region_erase");
    group.throughput(Throughput::Bytes(REGION_SIZE as u64));

    let mut db = create_db::<REGION_SIZE, REGION_COUNT>("erase-region");
    group.bench_function("erase_region_64k", |bencher| {
        bencher.iter(|| {
            db.backing
                .erase_region(black_box(1))
                .expect("erase benchmark region");
        });
    });

    group.finish();
}

fn bench_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_backing_mmap/lifecycle");
    group.throughput(Throughput::Bytes((REGION_SIZE * (REGION_COUNT + 1)) as u64));

    group.bench_function("create_new_4mib", |bencher| {
        bencher.iter_batched(
            || next_path("create-new"),
            |path| {
                let db = create_db_at_path::<REGION_SIZE, REGION_COUNT>(path);
                black_box(db.backing.geometry());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("format_empty_store_4mib", |bencher| {
        bencher.iter_batched(
            || create_db::<REGION_SIZE, REGION_COUNT>("format-empty-store"),
            |mut db| {
                let metadata = db
                    .backing
                    .format_empty_store(2, 8, 0xa5)
                    .expect("format FileBacking benchmark DB");
                black_box(metadata);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group! {
    name = file_backing_mmap;
    config = criterion_config();
    targets = bench_raw_region_io, bench_region_erase, bench_lifecycle
}
criterion_main!(file_backing_mmap);
