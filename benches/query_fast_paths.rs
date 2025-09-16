use cass::storage::{Storage, local::LocalStorage};
use cass::{Database, SqlEngine};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn setup(wal: &str) -> (Runtime, tempfile::TempDir, Arc<Database>, Arc<SqlEngine>) {
    let rt = Runtime::new().unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn Storage> = Arc::new(LocalStorage::new(tempdir.path()));
    let db = Arc::new(rt.block_on(Database::new(storage, wal)).unwrap());
    let engine = Arc::new(SqlEngine::new());
    (rt, tempdir, db, engine)
}

fn bench_query_fast_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_fast_paths");

    group.bench_function("insert_fast_path", |b| {
        let (rt, _tempdir, db, engine) = setup("fast_insert.wal");
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "CREATE TABLE fast_insert (id TEXT, val TEXT, PRIMARY KEY(id))",
                    )
                    .await
                    .unwrap();
            });
        }
        let sql = "INSERT INTO fast_insert (id, val) VALUES ('id', 'value')";
        b.iter(|| {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                let out = engine.execute(db.as_ref(), sql).await.unwrap();
                black_box(out);
            });
        });
    });

    group.bench_function("insert_full_parser", |b| {
        let (rt, _tempdir, db, engine) = setup("slow_insert.wal");
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "CREATE TABLE slow_insert (id TEXT, val TEXT, PRIMARY KEY(id))",
                    )
                    .await
                    .unwrap();
            });
        }
        let sql = "INSERT INTO slow_insert (id, val) VALUES ('id', 'value') /* force slow path */";
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine.execute(db.as_ref(), sql).await.unwrap();
            });
        }
        b.iter(|| {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                let out = engine.execute(db.as_ref(), sql).await.unwrap();
                black_box(out);
            });
        });
    });

    group.bench_function("select_fast_path", |b| {
        let (rt, _tempdir, db, engine) = setup("fast_select.wal");
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "CREATE TABLE fast_select (id TEXT, val TEXT, PRIMARY KEY(id))",
                    )
                    .await
                    .unwrap();
            });
        }
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "INSERT INTO fast_select (id, val) VALUES ('id', 'value')",
                    )
                    .await
                    .unwrap();
            });
        }
        let sql = "SELECT val FROM fast_select WHERE id = 'id'";
        b.iter(|| {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                let out = engine.execute(db.as_ref(), sql).await.unwrap();
                black_box(out);
            });
        });
    });

    group.bench_function("select_full_parser", |b| {
        let (rt, _tempdir, db, engine) = setup("slow_select.wal");
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "CREATE TABLE slow_select (id TEXT, val TEXT, PRIMARY KEY(id))",
                    )
                    .await
                    .unwrap();
            });
        }
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine
                    .execute(
                        db.as_ref(),
                        "INSERT INTO slow_select (id, val) VALUES ('id', 'value')",
                    )
                    .await
                    .unwrap();
            });
        }
        let sql = "SELECT val FROM slow_select WHERE id = 'id' /* force slow path */";
        {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                engine.execute(db.as_ref(), sql).await.unwrap();
            });
        }
        b.iter(|| {
            let engine = engine.clone();
            let db = db.clone();
            rt.block_on(async move {
                let out = engine.execute(db.as_ref(), sql).await.unwrap();
                black_box(out);
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_query_fast_paths);
criterion_main!(benches);
