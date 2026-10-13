//! One execution API over the node SQLite file and the hub Postgres pool.
//!
//! SQL is written with `?` / `?1` placeholders. SQLite receives them unchanged.
//! Postgres renumbers them to `$1`. Values stay as integers, floats, and text so
//! both engines share the statements the hub and the node already use.

use std::sync::{Mutex, MutexGuard};

use postgres::types::ToSql as PgToSql;
use postgres::NoTls;
use postgres_native_tls::MakeTlsConnector;
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{params_from_iter, Connection, ToSql};

use crate::{Store, StoreError};

pub(crate) enum Backend {
    Sqlite {
        conn: Mutex<Connection>,
        path: Option<std::path::PathBuf>,
    },
    Postgres {
        /// `None` after [`Drop`] has moved the pool onto a non-runtime thread.
        pool: Option<PgPool>,
    },
}

impl Drop for Backend {
    fn drop(&mut self) {
        let Backend::Postgres { pool } = self else {
            return;
        };
        let Some(pool) = pool.take() else {
            return;
        };
        // Idle clients live in the pool. Dropping them here would start a
        // runtime on whatever thread is dropping the store.
        off_runtime(move || drop(pool));
    }
}

pub(crate) type PgPool = r2d2::Pool<PgManager>;

#[derive(Clone)]
enum PgTls {
    Off,
    On(MakeTlsConnector),
}

#[derive(Clone)]
pub(crate) struct PgManager {
    config: postgres::Config,
    tls: PgTls,
}

impl r2d2::ManageConnection for PgManager {
    type Connection = postgres::Client;
    type Error = postgres::Error;

    fn connect(&self) -> Result<postgres::Client, postgres::Error> {
        match &self.tls {
            PgTls::Off => self.config.connect(NoTls),
            PgTls::On(tls) => self.config.connect(tls.clone()),
        }
    }

    fn is_valid(&self, client: &mut postgres::Client) -> Result<(), postgres::Error> {
        client.execute("SELECT 1", &[]).map(|_| ())
    }

    fn has_broken(&self, client: &mut postgres::Client) -> bool {
        client.is_closed()
    }
}

#[derive(Clone)]
pub(crate) enum SqlVal {
    NullInt,
    NullText,
    Int(i64),
    Float(f64),
    Text(String),
}

impl SqlVal {
    pub(crate) fn text(s: impl Into<String>) -> Self {
        Self::Text(s.into())
    }

    pub(crate) fn opt_text(s: Option<String>) -> Self {
        match s {
            Some(s) => Self::Text(s),
            None => Self::NullText,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn opt_int(n: Option<i64>) -> Self {
        match n {
            Some(n) => Self::Int(n),
            None => Self::NullInt,
        }
    }
}

impl ToSql for SqlVal {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Self::NullInt | Self::NullText => Ok(ToSqlOutput::Owned(Value::Null)),
            Self::Int(n) => rusqlite::ToSql::to_sql(n),
            Self::Float(n) => rusqlite::ToSql::to_sql(n),
            Self::Text(s) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes()))),
        }
    }
}

pub(crate) trait SqlRow {
    fn int(&self, idx: usize) -> crate::Result<i64>;
    fn float(&self, idx: usize) -> crate::Result<f64>;
    fn text(&self, idx: usize) -> crate::Result<String>;
    fn opt_text(&self, idx: usize) -> crate::Result<Option<String>>;
    fn opt_int(&self, idx: usize) -> crate::Result<Option<i64>>;
}

/// Object-safe statement runner. Generic helpers live on [`SqlExt`].
pub(crate) trait Sql {
    fn execute(&mut self, sql: &str, args: &[SqlVal]) -> crate::Result<usize>;
    fn query_raw(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        f: &mut dyn FnMut(&dyn SqlRow) -> crate::Result<()>,
    ) -> crate::Result<()>;
    fn last_id(&mut self) -> crate::Result<i64>;
    fn batch(&mut self, sql: &str) -> crate::Result<()>;
    fn postgres(&self) -> bool;
}

pub(crate) trait SqlExt: Sql {
    fn query<T>(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        mut map: impl FnMut(&dyn SqlRow) -> crate::Result<T>,
    ) -> crate::Result<Vec<T>> {
        let mut out = Vec::new();
        self.query_raw(sql, args, &mut |row| {
            out.push(map(row)?);
            Ok(())
        })?;
        Ok(out)
    }

    fn query_opt<T>(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        map: impl FnMut(&dyn SqlRow) -> crate::Result<T>,
    ) -> crate::Result<Option<T>> {
        Ok(self.query(sql, args, map)?.into_iter().next())
    }

    fn query_one<T>(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        map: impl FnMut(&dyn SqlRow) -> crate::Result<T>,
    ) -> crate::Result<T> {
        self.query_opt(sql, args, map)?
            .ok_or_else(|| StoreError::Corrupt("query returned no rows".into()))
    }
}

impl<T: Sql + ?Sized> SqlExt for T {}

pub(crate) struct SqliteSql<'a> {
    pub conn: &'a Connection,
}

impl Sql for SqliteSql<'_> {
    fn execute(&mut self, sql: &str, args: &[SqlVal]) -> crate::Result<usize> {
        Ok(self.conn.execute(sql, params_from_iter(args.iter()))?)
    }

    fn query_raw(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        f: &mut dyn FnMut(&dyn SqlRow) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(args.iter()))?;
        while let Some(row) = rows.next()? {
            f(&SqliteRow { row })?;
        }
        Ok(())
    }

    fn last_id(&mut self) -> crate::Result<i64> {
        Ok(self.conn.last_insert_rowid())
    }

    fn batch(&mut self, sql: &str) -> crate::Result<()> {
        Ok(self.conn.execute_batch(sql)?)
    }

    fn postgres(&self) -> bool {
        false
    }
}

struct SqliteRow<'a> {
    row: &'a rusqlite::Row<'a>,
}

impl SqlRow for SqliteRow<'_> {
    fn int(&self, idx: usize) -> crate::Result<i64> {
        Ok(self.row.get(idx)?)
    }
    fn float(&self, idx: usize) -> crate::Result<f64> {
        Ok(self.row.get(idx)?)
    }
    fn text(&self, idx: usize) -> crate::Result<String> {
        Ok(self.row.get(idx)?)
    }
    fn opt_text(&self, idx: usize) -> crate::Result<Option<String>> {
        Ok(self.row.get(idx)?)
    }
    fn opt_int(&self, idx: usize) -> crate::Result<Option<i64>> {
        Ok(self.row.get(idx)?)
    }
}

struct PgSql<'a> {
    client: &'a mut postgres::Client,
}

impl Sql for PgSql<'_> {
    fn execute(&mut self, sql: &str, args: &[SqlVal]) -> crate::Result<usize> {
        let sql = pg_sql(sql);
        let bound = pg_args(args);
        let refs = pg_refs(&bound);
        let n = self.client.execute(&sql, &refs).map_err(pg_err)?;
        Ok(usize::try_from(n).unwrap_or(usize::MAX))
    }

    fn query_raw(
        &mut self,
        sql: &str,
        args: &[SqlVal],
        f: &mut dyn FnMut(&dyn SqlRow) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let sql = pg_sql(sql);
        let bound = pg_args(args);
        let refs = pg_refs(&bound);
        let rows = self.client.query(&sql, &refs).map_err(pg_err)?;
        for row in &rows {
            f(&PgRow { row })?;
        }
        Ok(())
    }

    fn last_id(&mut self) -> crate::Result<i64> {
        let row = self
            .client
            .query_one("SELECT lastval()", &[])
            .map_err(pg_err)?;
        row.try_get(0).map_err(pg_err)
    }

    fn batch(&mut self, sql: &str) -> crate::Result<()> {
        self.client.batch_execute(sql).map_err(pg_err)
    }

    fn postgres(&self) -> bool {
        true
    }
}

struct PgRow<'a> {
    row: &'a postgres::Row,
}

impl SqlRow for PgRow<'_> {
    fn int(&self, idx: usize) -> crate::Result<i64> {
        self.row
            .try_get(idx)
            .or_else(|_| self.row.try_get::<_, i32>(idx).map(i64::from))
            .map_err(pg_err)
    }
    fn float(&self, idx: usize) -> crate::Result<f64> {
        self.row.try_get(idx).map_err(pg_err)
    }
    fn text(&self, idx: usize) -> crate::Result<String> {
        self.row.try_get(idx).map_err(pg_err)
    }
    fn opt_text(&self, idx: usize) -> crate::Result<Option<String>> {
        self.row.try_get(idx).map_err(pg_err)
    }
    fn opt_int(&self, idx: usize) -> crate::Result<Option<i64>> {
        match self.row.try_get::<_, Option<i64>>(idx) {
            Ok(v) => Ok(v),
            Err(_) => self
                .row
                .try_get::<_, Option<i32>>(idx)
                .map(|v| v.map(i64::from))
                .map_err(pg_err),
        }
    }
}

fn pg_err(err: postgres::Error) -> StoreError {
    StoreError::Database(err.to_string())
}

fn pg_args(args: &[SqlVal]) -> Vec<Box<dyn PgToSql + Sync>> {
    args.iter()
        .map(|v| match v {
            SqlVal::Int(n) => Box::new(*n) as Box<dyn PgToSql + Sync>,
            SqlVal::Float(n) => Box::new(*n),
            SqlVal::Text(s) => Box::new(s.clone()),
            SqlVal::NullInt => Box::new(Option::<i64>::None),
            SqlVal::NullText => Box::new(Option::<String>::None),
        })
        .collect()
}

fn pg_refs(bound: &[Box<dyn PgToSql + Sync>]) -> Vec<&(dyn PgToSql + Sync)> {
    bound.iter().map(|b| b.as_ref()).collect()
}

/// `?1` becomes `$1`. A bare `?` is numbered in the order it appears.
pub(crate) fn pg_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut bare = 0u32;
    while i < bytes.len() {
        if bytes[i] == b'?' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            out.push('$');
            if start == i {
                bare += 1;
                out.push_str(&bare.to_string());
            } else {
                out.push_str(&sql[start..i]);
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// `prefix` plus `(?1,?2),(?3,?4)` for `rows` of `cols` values.
pub(crate) fn values_sql(prefix: &str, rows: usize, cols: usize) -> String {
    let mut sql = String::from(prefix);
    let mut n = 1usize;
    for r in 0..rows {
        if r > 0 {
            sql.push(',');
        }
        sql.push('(');
        for c in 0..cols {
            if c > 0 {
                sql.push(',');
            }
            sql.push('?');
            sql.push_str(&n.to_string());
            n += 1;
        }
        sql.push(')');
    }
    sql
}

impl Store {
    pub(crate) fn sqlite_conn(&self) -> MutexGuard<'_, Connection> {
        match &self.backend {
            Backend::Sqlite { conn, .. } => conn.lock().unwrap_or_else(|e| e.into_inner()),
            Backend::Postgres { .. } => {
                panic!("node SQLite connection used on the postgres hub store")
            }
        }
    }

    pub(crate) fn with_sql<T: Send>(
        &self,
        f: impl FnOnce(&mut dyn Sql) -> crate::Result<T> + Send,
    ) -> crate::Result<T> {
        match &self.backend {
            Backend::Sqlite { conn, .. } => {
                let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
                let mut session = SqliteSql { conn: &guard };
                f(&mut session)
            }
            Backend::Postgres { pool } => {
                let pool = pg_pool(pool)?;
                with_pg(pool, |client| {
                    let mut session = PgSql { client };
                    f(&mut session)
                })
            }
        }
    }

    pub(crate) fn transaction<T: Send>(
        &self,
        f: impl FnOnce(&mut dyn Sql) -> crate::Result<T> + Send,
    ) -> crate::Result<T> {
        match &self.backend {
            Backend::Sqlite { conn, .. } => {
                let mut guard = conn.lock().unwrap_or_else(|e| e.into_inner());
                let tx = guard.transaction()?;
                let result = {
                    let mut session = SqliteSql { conn: &tx };
                    f(&mut session)
                };
                match result {
                    Ok(v) => {
                        tx.commit()?;
                        Ok(v)
                    }
                    Err(e) => Err(e),
                }
            }
            Backend::Postgres { pool } => {
                let pool = pg_pool(pool)?;
                with_pg(pool, |client| {
                    let mut session = PgSql { client };
                    session.execute("BEGIN", &[])?;
                    let result = f(&mut session);
                    match &result {
                        Ok(_) => {
                            session.execute("COMMIT", &[])?;
                        }
                        Err(_) => {
                            let _ = session.execute("ROLLBACK", &[]);
                        }
                    }
                    result
                })
            }
        }
    }

    /// Hub store. `ca_pem` is the server CA when the URL requires TLS.
    ///
    /// Safe to call from a tokio runtime. The blocking client is opened on
    /// another thread.
    pub fn open_postgres(url: &str, ca_pem: Option<&str>) -> crate::Result<Self> {
        off_runtime(|| Self::open_postgres_blocking(url, ca_pem))
    }

    fn open_postgres_blocking(url: &str, ca_pem: Option<&str>) -> crate::Result<Self> {
        let mut config: postgres::Config = url
            .parse()
            .map_err(|e: postgres::Error| StoreError::Database(e.to_string()))?;
        config.application_name("pulseblade");
        let tls = if let Some(pem) = ca_pem.filter(|s| !s.trim().is_empty()) {
            let cert = native_tls::Certificate::from_pem(pem.as_bytes())
                .map_err(|e| StoreError::Database(e.to_string()))?;
            let mut builder = native_tls::TlsConnector::builder();
            builder.add_root_certificate(cert);
            let tls = builder
                .build()
                .map_err(|e| StoreError::Database(e.to_string()))?;
            PgTls::On(MakeTlsConnector::new(tls))
        } else {
            PgTls::Off
        };
        let pool = r2d2::Pool::builder()
            .max_size(8)
            .build(PgManager { config, tls })
            .map_err(|e| StoreError::Database(e.to_string()))?;
        let store = Self {
            backend: Backend::Postgres { pool: Some(pool) },
        };
        store.with_sql(|sql| sql.batch(PG_SCHEMA))?;
        Ok(store)
    }
}

fn pg_pool(pool: &Option<PgPool>) -> crate::Result<&PgPool> {
    pool.as_ref()
        .ok_or_else(|| StoreError::Database("postgres pool is closed".into()))
}

/// `postgres` 0.19's blocking `Client` owns a current-thread runtime and
/// starts it for every call, including `Drop`. That panics with "Cannot
/// start a runtime from within a runtime" on a tokio worker.
/// r2d2's pool threads have no runtime handle and run the client directly.
/// A handle is set on workers, which panic, and also on `spawn_blocking`
/// threads, which do not. Hopping whenever a handle is present covers both.
fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(f)
                .join()
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
        })
    } else {
        f()
    }
}

fn with_pg<T: Send>(
    pool: &PgPool,
    f: impl FnOnce(&mut postgres::Client) -> crate::Result<T> + Send,
) -> crate::Result<T> {
    off_runtime(move || {
        let mut conn = pool
            .get()
            .map_err(|e| StoreError::Database(e.to_string()))?;
        let result = f(&mut conn);
        // Return or discard the client before leaving this thread.
        drop(conn);
        result
    })
}

const PG_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS resources (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    name       TEXT NOT NULL,
    parent     TEXT,
    health     TEXT NOT NULL,
    labels     TEXT NOT NULL,
    attrs      TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen BIGINT NOT NULL,
    last_seen  BIGINT NOT NULL,
    present    BIGINT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS resources_source ON resources(source);
CREATE INDEX IF NOT EXISTS resources_parent ON resources(parent);

CREATE TABLE IF NOT EXISTS changes (
    seq         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ts          BIGINT NOT NULL,
    resource_id TEXT NOT NULL,
    kind        TEXT NOT NULL,
    field       TEXT,
    before      TEXT,
    after       TEXT
);
CREATE INDEX IF NOT EXISTS changes_resource ON changes(resource_id, seq);
CREATE INDEX IF NOT EXISTS changes_ts ON changes(ts);

CREATE TABLE IF NOT EXISTS checkpoints (
    name TEXT PRIMARY KEY,
    seq  BIGINT NOT NULL,
    ts   BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          BIGINT NOT NULL,
    value       DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS samples_series ON samples(resource_id, metric, ts);

CREATE TABLE IF NOT EXISTS latest (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          BIGINT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (resource_id, metric)
);

CREATE TABLE IF NOT EXISTS collector_runs (
    id             BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source         TEXT NOT NULL,
    ts             BIGINT NOT NULL,
    duration_ms    BIGINT NOT NULL,
    ok             BIGINT NOT NULL,
    error          TEXT,
    resource_count BIGINT NOT NULL,
    sample_count   BIGINT NOT NULL,
    change_count   BIGINT NOT NULL,
    interval_secs  BIGINT
);
CREATE INDEX IF NOT EXISTS collector_runs_source ON collector_runs(source, id);

CREATE TABLE IF NOT EXISTS logs (
    ts          BIGINT NOT NULL,
    resource_id TEXT NOT NULL,
    message     TEXT NOT NULL,
    PRIMARY KEY (resource_id, ts, message)
);
CREATE INDEX IF NOT EXISTS logs_ts ON logs(ts);

CREATE TABLE IF NOT EXISTS notes (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    resource_id TEXT NOT NULL,
    author      TEXT NOT NULL,
    text        TEXT NOT NULL,
    created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS notes_resource ON notes(resource_id, created_at);

CREATE TABLE IF NOT EXISTS findings (
    id          TEXT PRIMARY KEY,
    dedupe_key  TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    rule        TEXT NOT NULL,
    severity    TEXT NOT NULL,
    status      TEXT NOT NULL,
    summary     TEXT NOT NULL,
    opened_at   BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL,
    context     TEXT NOT NULL,
    logs        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS findings_status ON findings(status, opened_at);
CREATE INDEX IF NOT EXISTS findings_key ON findings(dedupe_key);
CREATE INDEX IF NOT EXISTS findings_resource ON findings(resource_id);

CREATE TABLE IF NOT EXISTS action_runs (
    id               TEXT PRIMARY KEY,
    action           TEXT NOT NULL,
    resource_id      TEXT NOT NULL,
    params           TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    tier             TEXT NOT NULL,
    status           TEXT NOT NULL,
    dry_run          BIGINT NOT NULL,
    output           TEXT,
    created_at       BIGINT NOT NULL,
    updated_at       BIGINT NOT NULL,
    expires_at       BIGINT
);
CREATE INDEX IF NOT EXISTS action_runs_key ON action_runs(idempotency_key, status);
CREATE INDEX IF NOT EXISTS action_runs_resource ON action_runs(resource_id, created_at);

CREATE TABLE IF NOT EXISTS audit (
    seq       BIGINT PRIMARY KEY,
    ts        BIGINT NOT NULL,
    run_id    TEXT NOT NULL,
    event     TEXT NOT NULL,
    payload   TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pilot_runs (
    id         TEXT PRIMARY KEY,
    ts         BIGINT NOT NULL,
    trigger    TEXT NOT NULL,
    tier       TEXT NOT NULL,
    endpoint   TEXT NOT NULL,
    tool_calls BIGINT NOT NULL,
    tokens     BIGINT NOT NULL,
    conclusion TEXT NOT NULL,
    proposed   TEXT NOT NULL,
    status     TEXT NOT NULL,
    detail     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS node_cursors (
    node_id       TEXT PRIMARY KEY,
    seq           BIGINT NOT NULL,
    sample_ms     BIGINT NOT NULL,
    last_push_ms  BIGINT NOT NULL DEFAULT 0,
    interval_secs BIGINT NOT NULL DEFAULT 15,
    shutting_down BIGINT NOT NULL DEFAULT 0,
    bucket_ms     BIGINT NOT NULL DEFAULT 0,
    event_id      BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS route_rpc (
    id         TEXT PRIMARY KEY,
    node_id    TEXT NOT NULL,
    op         TEXT NOT NULL,
    body       TEXT NOT NULL,
    status     TEXT NOT NULL,
    result     TEXT,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS route_rpc_node ON route_rpc(node_id, status);

CREATE TABLE IF NOT EXISTS route_plans (
    plan_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_results (
    id      TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    body    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sample_buckets (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    bucket_ms   BIGINT NOT NULL,
    n           BIGINT NOT NULL,
    vmin        DOUBLE PRECISION NOT NULL,
    vmax        DOUBLE PRECISION NOT NULL,
    vsum        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (resource_id, metric, bucket_ms)
);

CREATE TABLE IF NOT EXISTS events (
    id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ts          BIGINT NOT NULL,
    resource_id TEXT NOT NULL,
    kind        TEXT NOT NULL,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_res ON events(resource_id, ts);
CREATE INDEX IF NOT EXISTS events_ts ON events(ts);
CREATE UNIQUE INDEX IF NOT EXISTS events_dedupe ON events(resource_id, ts, kind, message);
"#;

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use super::{pg_sql, SqlExt};
    use crate::Store;

    #[test]
    fn numbered_and_bare_placeholders_become_postgres_parameters() {
        assert_eq!(pg_sql("SELECT ?1, ?2"), "SELECT $1, $2");
        assert_eq!(pg_sql("IN (?, ?, ?)"), "IN ($1, $2, $3)");
    }

    /// Opens the blocking client from a tokio runtime. `Client::drop` panics
    /// there unless the checkout runs on another thread.
    #[tokio::test]
    async fn open_postgres_from_tokio_runtime_does_not_panic() {
        let Some(pg) = disposable_postgres() else {
            eprintln!("skipping postgres runtime test: docker unavailable");
            return;
        };
        assert!(
            tokio::runtime::Handle::try_current().is_ok(),
            "the open must run on a tokio runtime thread"
        );
        let store = open_when_ready(&pg.url);
        let one = store
            .with_sql(|sql| sql.query_one("SELECT 1::bigint", &[], |row| row.int(0)))
            .expect("select 1");
        assert_eq!(one, 1);
        drop(store);
    }

    struct DisposablePg {
        url: String,
        container: Option<String>,
    }

    impl Drop for DisposablePg {
        fn drop(&mut self) {
            let Some(name) = self.container.take() else {
                return;
            };
            let _ = Command::new("docker")
                .args(["rm", "-f", &name])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }

    fn disposable_postgres() -> Option<DisposablePg> {
        if let Ok(url) = std::env::var("PULSEBLADE_TEST_PG") {
            let url = url.trim();
            if !url.is_empty() {
                return Some(DisposablePg {
                    url: url.to_string(),
                    container: None,
                });
            }
        }
        if !docker_available() {
            return None;
        }
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("bind a free local port")
            .local_addr()
            .expect("read the free port")
            .port();
        let name = format!(
            "pb-pg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-e",
                "POSTGRES_USER=pulseblade",
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "POSTGRES_DB=pulseblade",
                "-p",
                &format!("127.0.0.1:{port}:5432"),
                "postgres:16-alpine",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .expect("run docker");
        if !output.status.success() {
            let _ = Command::new("docker")
                .args(["rm", "-f", &name])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            panic!(
                "docker run failed ({}): {}",
                output.status,
                redact_process(&output.stderr)
            );
        }
        let pg = DisposablePg {
            url: format!("postgres://pulseblade@127.0.0.1:{port}/pulseblade"),
            container: Some(name.clone()),
        };
        if !wait_ready(&name, port) {
            panic!("postgres container did not become ready");
        }
        Some(pg)
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .arg("info")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn wait_ready(name: &str, port: u16) -> bool {
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        for _ in 0..60 {
            let ready = Command::new("docker")
                .args([
                    "exec",
                    name,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "pulseblade",
                    "-d",
                    "pulseblade",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            let published =
                std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok();
            if ready && published {
                return true;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        false
    }

    fn open_when_ready(url: &str) -> Store {
        let mut last = String::from("not attempted");
        for _ in 0..20 {
            match Store::open_postgres(url, None) {
                Ok(store) => return store,
                Err(err) => {
                    last = redact(&err.to_string());
                    std::thread::sleep(Duration::from_millis(250));
                }
            }
        }
        panic!("postgres did not accept a connection: {last}");
    }

    fn redact(text: &str) -> String {
        let mut out = String::new();
        for line in text.lines() {
            if line.contains("postgres://") || line.to_ascii_lowercase().contains("password") {
                continue;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(line);
        }
        if out.is_empty() {
            "connection failed".into()
        } else {
            out
        }
    }

    fn redact_process(stderr: &[u8]) -> String {
        let clipped: String = String::from_utf8_lossy(stderr)
            .chars()
            .take(2_000)
            .collect();
        redact(&clipped)
    }
}
