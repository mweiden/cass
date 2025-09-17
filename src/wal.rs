use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    future::Future,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::{
    runtime::Builder,
    sync::{Mutex, Notify},
};

use crate::storage::{Storage, StorageError};

/// Configuration options for the write-ahead log.
#[derive(Clone, Copy)]
pub struct WalOptions {
    /// When operating in periodic commitlog mode, controls how frequently the
    /// WAL is synced to durable storage. Defaults to 10 seconds, mirroring
    /// Cassandra's `commitlog_sync_period_in_ms` value.
    pub commitlog_sync_period: Duration,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            commitlog_sync_period: Duration::from_millis(10_000),
        }
    }
}

/// Basic write-ahead log persisted via the configured storage backend.
pub struct Wal {
    inner: Arc<WalInner>,
}

struct WalInner {
    path: String,
    storage: Arc<dyn Storage>,
    state: Mutex<WalState>,
    flush_lock: Mutex<()>,
    shutdown: AtomicBool,
    flush_interval: Duration,
    notify: Notify,
    flush_task: StdMutex<Option<thread::JoinHandle<()>>>,
}

struct WalState {
    data: Vec<u8>,
    flushed: usize,
}

fn parse_entries(data: &[u8]) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut res = Vec::new();
    for line in data.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Some(pos) = line.iter().position(|b| *b == b'\t') {
            let key = std::str::from_utf8(&line[..pos])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                .to_string();
            let val = STANDARD
                .decode(&line[pos + 1..])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            res.push((key, val));
        }
    }
    Ok(res)
}

fn map_err(e: StorageError) -> std::io::Error {
    match e {
        StorageError::Io(e) => e,
        StorageError::Unimplemented => {
            std::io::Error::new(std::io::ErrorKind::Other, "unimplemented")
        }
    }
}

fn block_on_future<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime for WAL drop")
            .block_on(future)
    })
    .join()
    .expect("blocking WAL drop thread panicked")
}

impl WalInner {
    async fn flush_pending(&self) -> std::io::Result<()> {
        let _flush_guard = self.flush_lock.lock().await;
        let (to_flush, new_flushed) = {
            let state = self.state.lock().await;
            let end = state.data.len();
            if state.flushed >= end {
                return Ok(());
            }
            let buf = state.data[state.flushed..end].to_vec();
            (buf, end)
        };

        if to_flush.is_empty() {
            return Ok(());
        }

        self.storage
            .append(&self.path, &to_flush)
            .await
            .map_err(map_err)?;

        let mut state = self.state.lock().await;
        state.flushed = state.data.len().min(new_flushed);
        Ok(())
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn set_flush_task(&self, handle: thread::JoinHandle<()>) {
        let mut guard = self.flush_task.lock().unwrap();
        *guard = Some(handle);
    }

    fn take_flush_task(&self) -> Option<thread::JoinHandle<()>> {
        self.flush_task.lock().unwrap().take()
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let inner = self.inner.clone();

        inner.shutdown.store(true, Ordering::Release);

        inner.notify.notify_waiters();

        if let Some(task) = inner.take_flush_task() {
            if let Err(err) = task.join() {
                eprintln!("wal flush worker panicked: {err:?}");
            }
        }

        let flush_inner = inner.clone();
        if let Err(err) = block_on_future(async move { flush_inner.flush_pending().await }) {
            eprintln!("wal flush error: {err}");
        }
    }
}

impl Wal {
    /// Create or open a log at `path`, returning the WAL instance and any
    /// existing entries to be replayed into the memtable.
    pub async fn new(
        storage: Arc<dyn Storage>,
        path: impl Into<String>,
    ) -> std::io::Result<(Self, Vec<(String, Vec<u8>)>)> {
        Self::new_with_options(storage, path, WalOptions::default()).await
    }

    /// Create or open a log with custom configuration options.
    pub async fn new_with_options(
        storage: Arc<dyn Storage>,
        path: impl Into<String>,
        options: WalOptions,
    ) -> std::io::Result<(Self, Vec<(String, Vec<u8>)>)> {
        let path = path.into();
        let buf = storage.get(&path).await.unwrap_or_default();
        let entries = parse_entries(&buf)?;
        let initial_len = buf.len();
        let inner = Arc::new(WalInner {
            path,
            storage,
            state: Mutex::new(WalState {
                data: buf,
                flushed: initial_len,
            }),
            flush_lock: Mutex::new(()),
            shutdown: AtomicBool::new(false),
            flush_interval: options.commitlog_sync_period,
            notify: Notify::new(),
            flush_task: StdMutex::new(None),
        });

        if !inner.flush_interval.is_zero() {
            let worker_inner = inner.clone();
            let handle = thread::spawn(move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build WAL worker runtime");

                runtime.block_on(async move {
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(worker_inner.flush_interval) => {},
                            _ = worker_inner.notify.notified() => {},
                        }

                        if let Err(err) = worker_inner.flush_pending().await {
                            eprintln!("wal flush error: {err}");
                        }

                        if worker_inner.shutdown_requested() {
                            if let Err(err) = worker_inner.flush_pending().await {
                                eprintln!("wal flush error: {err}");
                            }
                            break;
                        }
                    }
                });
            });
            inner.set_flush_task(handle);
        }

        Ok((Self { inner }, entries))
    }

    /// Append a line of `data` to the log and persist it through the storage
    /// backend.
    pub async fn append(&self, data: &[u8]) -> std::io::Result<()> {
        {
            let mut state = self.inner.state.lock().await;
            state.data.extend_from_slice(data);
            state.data.push(b'\n');
        }

        if self.inner.flush_interval.is_zero() {
            self.inner.flush_pending().await?;
        }

        Ok(())
    }

    /// Manually flush the WAL, syncing any pending data to the storage backend.
    pub async fn flush(&self) -> std::io::Result<()> {
        self.inner.flush_pending().await
    }

    /// Remove all data from the log by truncating the underlying storage
    /// object.
    pub async fn clear(&self) -> std::io::Result<()> {
        self.flush().await?;
        {
            let mut state = self.inner.state.lock().await;
            state.data.clear();
            state.flushed = 0;
        }
        self.inner
            .storage
            .put(&self.inner.path, Vec::new())
            .await
            .map_err(map_err)?;
        Ok(())
    }
}
