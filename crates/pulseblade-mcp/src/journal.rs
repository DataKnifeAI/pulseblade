//! Hub database handle. The listener stays up when the pool is closed and
//! serves the last successful summary snapshot with `stale: true`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pulseblade_store::{Store, StoreError};
use schemars::JsonSchema;
use serde::Serialize;

use crate::query::{Detail, QueryError, Snapshot};

/// Live tool output, or the cached summary when the database is down.
///
/// Untagged so a live body keeps its shape. A stale body is the summary
/// snapshot (`stale: true`, `as_of_seq` from the last successful read).
#[derive(Debug, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum LiveOrStale<T> {
    Stale(Snapshot),
    Live(T),
}

pub enum Read<T> {
    Live(T),
    Stale(Snapshot),
}

#[derive(Debug)]
pub enum WriteError {
    Unavailable,
    Store(StoreError),
}

/// Shared by the HTTP API, MCP, and the hub push path.
pub struct Journal {
    slot: Mutex<Option<Arc<Store>>>,
    cache: Mutex<Option<Snapshot>>,
    path: PathBuf,
    up: AtomicBool,
}

impl Journal {
    /// Store that stays open. Nodes use this. There is no cache file.
    pub fn always(store: Arc<Store>) -> Arc<Self> {
        Arc::new(Self {
            slot: Mutex::new(Some(store)),
            cache: Mutex::new(None),
            path: PathBuf::new(),
            up: AtomicBool::new(true),
        })
    }

    /// Hub journal. An empty `path` keeps the cache in memory only.
    /// Starts unavailable until [`install`](Self::install).
    pub fn recover(path: impl Into<PathBuf>) -> Arc<Self> {
        let path = path.into();
        let cache = load_cache(&path);
        Arc::new(Self {
            slot: Mutex::new(None),
            cache: Mutex::new(cache),
            path,
            up: AtomicBool::new(false),
        })
    }

    pub fn is_up(&self) -> bool {
        self.up.load(Ordering::SeqCst)
    }

    pub fn store(&self) -> Option<Arc<Store>> {
        if !self.is_up() {
            return None;
        }
        self.slot.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn install(&self, store: Arc<Store>) {
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(store);
        if !self.up.swap(true, Ordering::SeqCst) {
            tracing::info!("hub database is available");
        }
    }

    /// Drop the pool. [`Store`]'s drop hops off a tokio runtime.
    pub fn mark_down(&self) {
        let was = self.up.swap(false, Ordering::SeqCst);
        let old = self.slot.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(old);
        if was {
            tracing::warn!("hub database unavailable; serving cached snapshot");
        }
    }

    /// Record a summary only after it was read successfully.
    pub fn remember(&self, snap: &Snapshot) {
        if snap.stale || snap.unchanged || snap.detail != Some(Detail::Summary) {
            return;
        }
        let mut cached = snap.clone();
        cached.stale = false;
        *self.cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(cached.clone());
        write_cache(&self.path, &cached);
    }

    /// Cached summary, marked stale. Empty when nothing has been read yet.
    pub fn stale(&self) -> Snapshot {
        let mut snap = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_default();
        snap.stale = true;
        snap.unchanged = false;
        snap
    }

    pub fn read<T, F>(&self, f: F) -> Result<Read<T>, QueryError>
    where
        F: FnOnce(&Store) -> Result<T, QueryError>,
    {
        let Some(store) = self.store() else {
            return Ok(Read::Stale(self.stale()));
        };
        match f(&store) {
            Ok(v) => Ok(Read::Live(v)),
            Err(QueryError::Store(err)) if is_down(&err) => {
                self.mark_down();
                Ok(Read::Stale(self.stale()))
            }
            Err(err) => Err(err),
        }
    }

    /// Run `f` against the live store. Does not refresh the cache.
    pub fn try_store<T, F>(&self, f: F) -> Result<T, WriteError>
    where
        F: FnOnce(&Store) -> Result<T, StoreError>,
    {
        let Some(store) = self.store() else {
            return Err(WriteError::Unavailable);
        };
        match f(&store) {
            Ok(v) => Ok(v),
            Err(err) if is_down(&err) => {
                self.mark_down();
                Err(WriteError::Unavailable)
            }
            Err(err) => Err(WriteError::Store(err)),
        }
    }
}

pub fn is_down(err: &StoreError) -> bool {
    matches!(err, StoreError::Database(_))
}

fn load_cache(path: &Path) -> Option<Snapshot> {
    if path.as_os_str().is_empty() || !path.is_file() {
        return None;
    }
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "snapshot cache was not readable");
            return None;
        }
    };
    match serde_json::from_str::<Snapshot>(&text) {
        Ok(mut snap) => {
            snap.stale = false;
            Some(snap)
        }
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "snapshot cache was not readable");
            None
        }
    }
}

fn write_cache(path: &Path, snap: &Snapshot) {
    if path.as_os_str().is_empty() {
        return;
    }
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            if let Err(err) = std::fs::create_dir_all(dir) {
                tracing::warn!(
                    path = %dir.display(),
                    error = %err,
                    "snapshot cache directory was not created"
                );
                return;
            }
        }
    }
    let text = match serde_json::to_string(snap) {
        Ok(text) => text,
        Err(err) => {
            tracing::warn!(error = %err, "snapshot cache was not encoded");
            return;
        }
    };
    let tmp = path.with_extension("tmp");
    if let Err(err) = std::fs::write(&tmp, text) {
        tracing::warn!(path = %tmp.display(), error = %err, "snapshot cache was not written");
        return;
    }
    if let Err(err) = std::fs::rename(&tmp, path) {
        tracing::warn!(path = %path.display(), error = %err, "snapshot cache was not written");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{snapshot, SnapshotParams};
    use pulseblade_core::{Health, Observation, Resource, ResourceKind};

    #[test]
    fn cache_file_round_trips_and_serves_stale() {
        let dir = std::env::temp_dir().join(format!(
            "pulseblade-cache-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.json");
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                chrono::Utc::now(),
                &Observation {
                    resources: vec![Resource::new("host:h", ResourceKind::Host, "h")
                        .health(Health::Ok)
                        .label("host", "h")],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let snap = snapshot(
            &store,
            SnapshotParams {
                detail: Some(Detail::Summary),
                ..Default::default()
            },
        )
        .unwrap();
        let journal = Journal::recover(&path);
        journal.remember(&snap);
        assert!(path.is_file());
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.contains("\"stale\":true"));

        let restarted = Journal::recover(&path);
        assert!(!restarted.is_up());
        let stale = restarted.stale();
        assert!(stale.stale);
        assert_eq!(stale.as_of_seq, snap.as_of_seq);
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            restarted.try_store(|s| s.current_seq()),
            Err(WriteError::Unavailable)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
