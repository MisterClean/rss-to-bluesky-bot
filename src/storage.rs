//! Versioned SQLite observations and an immutable publication outbox.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use atrium_api::types::string::{Did, Tid};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde::Serialize;
use serde_json::Value;

use crate::model::{Article, FeedValidators, Receipt};
use crate::{Error, Result};

const SCHEMA_VERSION: i64 = 1;
const APPLICATION_ID: i64 = 0x52534242;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT UNIQUE,
    published_date TEXT
);
CREATE TABLE legacy_title_aliases (
    normalized_title TEXT PRIMARY KEY,
    legacy_post_id INTEGER NOT NULL REFERENCES posts(id)
);
CREATE TABLE metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    profile TEXT NOT NULL,
    account_did TEXT,
    initialized_at INTEGER NOT NULL,
    last_worker_started_at INTEGER,
    last_worker_success_at INTEGER
);
CREATE TABLE feeds (
    feed_id TEXT PRIMARY KEY,
    baseline_at INTEGER NOT NULL,
    last_scan_at INTEGER NOT NULL,
    etag TEXT,
    last_modified TEXT
);
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
    article_json TEXT NOT NULL,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    historical_title INTEGER NOT NULL CHECK (historical_title IN (0, 1))
);
CREATE TABLE item_aliases (
    kind TEXT NOT NULL CHECK (kind IN ('guid', 'url')),
    scope TEXT NOT NULL,
    value TEXT NOT NULL,
    item_id INTEGER NOT NULL REFERENCES items(id),
    PRIMARY KEY (kind, scope, value)
);
CREATE TABLE deliveries (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL UNIQUE REFERENCES items(id),
    state TEXT NOT NULL CHECK (state IN ('queued', 'preparing', 'prepared', 'uncertain', 'held', 'sent')),
    account_did TEXT,
    rkey TEXT UNIQUE,
    record_json TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at INTEGER NOT NULL,
    last_error TEXT,
    retry_allowed INTEGER NOT NULL DEFAULT 1 CHECK (retry_allowed IN (0, 1)),
    created_at INTEGER NOT NULL,
    sent_at INTEGER,
    uri TEXT,
    cid TEXT,
    CHECK (state NOT IN ('preparing', 'prepared', 'uncertain', 'sent') OR (rkey IS NOT NULL AND account_did IS NOT NULL)),
    CHECK (state NOT IN ('prepared', 'uncertain', 'sent') OR record_json IS NOT NULL),
    CHECK (state != 'sent' OR (sent_at IS NOT NULL AND uri IS NOT NULL AND cid IS NOT NULL))
);
CREATE INDEX deliveries_due ON deliveries(state, next_attempt_at, id);
CREATE INDEX aliases_item ON item_aliases(item_id);
";

/// A complete validated scan, including currently ineligible observations.
#[derive(Debug, Clone)]
pub struct FeedSnapshot {
    pub feed_id: String,
    pub articles: Vec<Article>,
    pub eligible_ids: Vec<String>,
    pub validators: FeedValidators,
}

/// Bounds for an explicit, local-only queue operation.
#[derive(Debug, Clone)]
pub struct BackfillSelection {
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub item_ids: Vec<String>,
    pub max_items: usize,
}

/// Changes committed together for one or more scans.
#[derive(Debug, Default, Serialize)]
pub struct IngestReport {
    pub observed: usize,
    pub queued: usize,
    pub historical_matches: usize,
    pub baselined_feeds: usize,
}

/// Persistent source health without exposing source URLs.
#[derive(Debug, Serialize)]
pub struct FeedStatus {
    pub feed_id: String,
    pub baseline_at: i64,
    pub last_scan_at: i64,
}

/// A bounded, source-independent queue detail.
#[derive(Debug, Serialize)]
pub struct DeliveryStatus {
    pub id: i64,
    pub feed_id: String,
    pub state: String,
    pub rkey: Option<String>,
    pub attempts: u32,
    pub next_attempt_at: i64,
    pub last_error: Option<String>,
}

/// Machine-readable state that can be obtained with a read-only connection.
#[derive(Debug, Serialize)]
pub struct StatusReport {
    pub schema_version: i64,
    pub profile: String,
    pub account_did: Option<String>,
    pub legacy_posts: usize,
    pub observed_items: usize,
    pub queue: BTreeMap<String, usize>,
    pub due: usize,
    pub oldest_pending_at: Option<i64>,
    pub last_worker_started_at: Option<i64>,
    pub last_worker_success_at: Option<i64>,
    pub feeds: Vec<FeedStatus>,
    pub deliveries: Vec<DeliveryStatus>,
}

/// One durable delivery loaded independently from the current feed.
#[derive(Debug)]
pub(crate) struct Delivery {
    pub id: i64,
    pub article: Article,
    pub state: String,
    pub account_did: Option<String>,
    pub rkey: Option<String>,
    pub record: Option<Value>,
    pub attempts: u32,
    pub retry_allowed: bool,
}

/// An initialized, profile-bound database. CLI writers also take an OS lock.
pub struct Store {
    connection: Connection,
    read_only: bool,
}

impl Store {
    /// Create new state and silently remember every item in the initial scans.
    pub fn initialize(
        path: &Path,
        profile: &str,
        expected_did: Option<&str>,
        snapshots: &[FeedSnapshot],
    ) -> Result<Self> {
        validate_initial_inputs(profile, expected_did, snapshots)?;
        let created = !path.exists();
        if created {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            drop(options.open(path)?);
        }
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure(&connection, false)?;
        if user_version(&connection)? != 0
            || application_id(&connection)? != 0
            || !tables(&connection)?.is_empty()
        {
            return Err(Error::State(
                "initialization requires a new empty database".into(),
            ));
        }
        private_file(path)?;
        let mut store = Self {
            connection,
            read_only: false,
        };
        if let Err(error) = store.install(profile, expected_did, snapshots, false) {
            drop(store);
            if created {
                std::fs::remove_file(path)?;
            }
            return Err(error);
        }
        Ok(store)
    }

    /// Add our versioned schema to the recognized legacy title table in place.
    /// IDs and published_date values remain opaque and untouched.
    pub fn migrate_legacy(
        path: &Path,
        profile: &str,
        expected_did: Option<&str>,
        snapshots: &[FeedSnapshot],
    ) -> Result<Self> {
        validate_initial_inputs(profile, expected_did, snapshots)?;
        let connection = existing_connection(path, false)?;
        if user_version(&connection)? != 0
            || application_id(&connection)? != 0
            || tables(&connection)? != ["posts"]
        {
            return Err(Error::State(
                "migration requires the recognized legacy-only database".into(),
            ));
        }
        validate_columns(&connection, "posts", &["id", "title", "published_date"])?;
        validate_legacy(&connection)?;
        let mut store = Self {
            connection,
            read_only: false,
        };
        store.install(profile, expected_did, snapshots, true)?;
        Ok(store)
    }

    /// Open existing initialized state; missing state never creates a database.
    pub fn open(path: &Path, profile: &str, expected_did: Option<&str>) -> Result<Self> {
        Self::open_mode(path, profile, expected_did, false)
    }

    /// Open initialized state without writes or automatic migrations.
    pub fn open_read_only(path: &Path, profile: &str, expected_did: Option<&str>) -> Result<Self> {
        Self::open_mode(path, profile, expected_did, true)
    }

    fn open_mode(
        path: &Path,
        profile: &str,
        expected_did: Option<&str>,
        read_only: bool,
    ) -> Result<Self> {
        let connection = existing_connection(path, read_only)?;
        validate_schema(&connection)?;
        let store = Self {
            connection,
            read_only,
        };
        let stored_profile: String = store.connection.query_row(
            "SELECT profile FROM metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if stored_profile != profile {
            return Err(Error::State(
                "database belongs to a different profile".into(),
            ));
        }
        if let Some(expected) = expected_did {
            validate_did(expected)?;
            if store
                .account_did()?
                .as_deref()
                .is_some_and(|did| did != expected)
            {
                return Err(Error::Account(
                    "configured DID differs from persistent account".into(),
                ));
            }
        }
        Ok(store)
    }

    fn install(
        &mut self,
        profile: &str,
        did: Option<&str>,
        scans: &[FeedSnapshot],
        migration: bool,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let transaction = self.connection.transaction()?;
        transaction.execute_batch(SCHEMA)?;
        install_legacy_title_aliases(&transaction)?;
        transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.execute("INSERT INTO metadata (singleton, profile, account_did, initialized_at) VALUES (1, ?1, ?2, ?3)", params![profile, did, now])?;
        for scan in scans {
            insert_feed(&transaction, scan, now)?;
            scan_items(
                &transaction,
                scan,
                now,
                migration,
                false,
                None,
                &mut IngestReport::default(),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Explicitly baseline newly added feeds without publishing existing items.
    pub fn baseline(&mut self, snapshots: &[FeedSnapshot]) -> Result<IngestReport> {
        self.require_writable()?;
        validate_snapshots(snapshots)?;
        let now = Utc::now().timestamp();
        let transaction = self.connection.transaction()?;
        let mut report = IngestReport::default();
        for scan in snapshots {
            if !feed_exists(&transaction, &scan.feed_id)? {
                insert_feed(&transaction, scan, now)?;
                scan_items(&transaction, scan, now, false, false, None, &mut report)?;
                report.baselined_feeds += 1;
            }
        }
        transaction.commit()?;
        Ok(report)
    }

    /// Atomically observe a complete scan and queue only newly seen eligible items.
    pub fn ingest(&mut self, snapshot: &FeedSnapshot) -> Result<IngestReport> {
        self.require_writable()?;
        validate_snapshots(std::slice::from_ref(snapshot))?;
        let now = Utc::now().timestamp();
        let transaction = self.connection.transaction()?;
        require_feed(&transaction, &snapshot.feed_id)?;
        let mut report = IngestReport::default();
        scan_items(&transaction, snapshot, now, true, false, None, &mut report)?;
        update_validators(&transaction, snapshot, now)?;
        transaction.commit()?;
        Ok(report)
    }

    /// Queue an explicitly bounded selection; no publisher or credentials are used.
    pub fn backfill(
        &mut self,
        snapshot: &FeedSnapshot,
        selection: &BackfillSelection,
    ) -> Result<IngestReport> {
        self.require_writable()?;
        validate_snapshots(std::slice::from_ref(snapshot))?;
        validate_selection(selection)?;
        let eligible: HashSet<&str> = snapshot.eligible_ids.iter().map(String::as_str).collect();
        let selected_ids: HashSet<&str> = selection.item_ids.iter().map(String::as_str).collect();
        let mut articles: Vec<&Article> = snapshot
            .articles
            .iter()
            .filter(|article| {
                eligible.contains(article.id.as_str())
                    && (selected_ids.is_empty()
                        || selected_ids.contains(article.id.as_str())
                        || selected_ids.contains(article.url.as_str()))
                    && selection
                        .since
                        .is_none_or(|since| article.published_at.is_some_and(|date| date >= since))
                    && selection
                        .until
                        .is_none_or(|until| article.published_at.is_some_and(|date| date <= until))
            })
            .collect();
        articles.sort_by(|a, b| {
            a.published_at
                .cmp(&b.published_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        let bounded = FeedSnapshot {
            feed_id: snapshot.feed_id.clone(),
            articles: articles.into_iter().cloned().collect(),
            eligible_ids: snapshot.eligible_ids.clone(),
            validators: snapshot.validators.clone(),
        };
        let now = Utc::now().timestamp();
        let transaction = self.connection.transaction()?;
        require_feed(&transaction, &snapshot.feed_id)?;
        let mut report = IngestReport::default();
        scan_items(
            &transaction,
            &bounded,
            now,
            true,
            true,
            Some(selection.max_items),
            &mut report,
        )?;
        transaction.commit()?;
        Ok(report)
    }

    /// Conditional-request metadata is available only for a baselined feed.
    pub fn validators(&self, feed_id: &str) -> Result<Option<FeedValidators>> {
        Ok(self
            .connection
            .query_row(
                "SELECT etag, last_modified FROM feeds WHERE feed_id = ?1",
                [feed_id],
                |row| {
                    Ok(FeedValidators {
                        etag: row.get(0)?,
                        last_modified: row.get(1)?,
                    })
                },
            )
            .optional()?)
    }

    /// Whether an explicit successful baseline exists for this configured ID.
    pub fn is_baselined(&self, feed_id: &str) -> Result<bool> {
        feed_exists(&self.connection, feed_id)
    }

    /// Record a successful conditional 304 check without replacing validators.
    pub fn mark_feed_checked(&mut self, feed_id: &str) -> Result<()> {
        self.require_writable()?;
        let transaction = self.connection.transaction()?;
        require_feed(&transaction, feed_id)?;
        transaction.execute(
            "UPDATE feeds SET last_scan_at = ?2 WHERE feed_id = ?1",
            params![feed_id, Utc::now().timestamp()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// The stable account namespace; handles never participate in this value.
    pub fn account_did(&self) -> Result<Option<String>> {
        Ok(self.connection.query_row(
            "SELECT account_did FROM metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?)
    }

    /// Pin a verified account on first use, or require the already pinned DID.
    pub fn verify_or_bind_did(&mut self, did: &str) -> Result<()> {
        self.require_writable()?;
        validate_did(did)?;
        let transaction = self.connection.transaction()?;
        let stored: Option<String> = transaction.query_row(
            "SELECT account_did FROM metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if stored.as_deref().is_some_and(|value| value != did) {
            return Err(Error::Account(
                "authenticated DID differs from persistent account".into(),
            ));
        }
        transaction.execute(
            "UPDATE metadata SET account_did = ?1 WHERE singleton = 1 AND account_did IS NULL",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// The number of recoverable deliveries eligible at a given time.
    pub fn due_count(&self, now: DateTime<Utc>) -> Result<usize> {
        count(
            &self.connection,
            "SELECT COUNT(*) FROM deliveries WHERE state IN ('queued', 'preparing', 'prepared', 'uncertain') AND next_attempt_at <= ?1",
            [now.timestamp()],
        )
    }

    /// Read source health, queue counts, and at most 100 pending details.
    pub fn status(&self) -> Result<StatusReport> {
        let (profile, account_did, last_worker_started_at, last_worker_success_at) = self.connection.query_row("SELECT profile, account_did, last_worker_started_at, last_worker_success_at FROM metadata WHERE singleton = 1", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?;
        let mut statement = self
            .connection
            .prepare("SELECT state, COUNT(*) FROM deliveries GROUP BY state")?;
        let queue = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .map(|entry| {
                let (state, amount) = entry?;
                Ok((state, checked_count(amount)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut statement = self
            .connection
            .prepare("SELECT feed_id, baseline_at, last_scan_at FROM feeds ORDER BY feed_id")?;
        let feeds = statement
            .query_map([], |row| {
                Ok(FeedStatus {
                    feed_id: row.get(0)?,
                    baseline_at: row.get(1)?,
                    last_scan_at: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut statement = self.connection.prepare("SELECT d.id, i.feed_id, d.state, d.rkey, d.attempts, d.next_attempt_at, d.last_error FROM deliveries d JOIN items i ON i.id = d.item_id WHERE d.state != 'sent' ORDER BY d.id LIMIT 100")?;
        let deliveries = statement
            .query_map([], |row| {
                Ok(DeliveryStatus {
                    id: row.get(0)?,
                    feed_id: row.get(1)?,
                    state: row.get(2)?,
                    rkey: row.get(3)?,
                    attempts: row.get(4)?,
                    next_attempt_at: row.get(5)?,
                    last_error: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(StatusReport {
            schema_version: SCHEMA_VERSION,
            profile,
            account_did,
            legacy_posts: count(&self.connection, "SELECT COUNT(*) FROM posts", [])?,
            observed_items: count(&self.connection, "SELECT COUNT(*) FROM items", [])?,
            queue,
            due: self.due_count(Utc::now())?,
            oldest_pending_at: self.connection.query_row(
                "SELECT MIN(created_at) FROM deliveries WHERE state != 'sent'",
                [],
                |row| row.get(0),
            )?,
            last_worker_started_at,
            last_worker_success_at,
            feeds,
            deliveries,
        })
    }

    pub(crate) fn due(&self, now: DateTime<Utc>, limit: usize) -> Result<Vec<Delivery>> {
        let limit = i64::try_from(limit)
            .map_err(|_| Error::State("delivery limit exceeds SQLite range".into()))?;
        let mut statement = self.connection.prepare("SELECT d.id, i.article_json, d.state, d.account_did, d.rkey, d.record_json, d.attempts, d.retry_allowed FROM deliveries d JOIN items i ON i.id = d.item_id WHERE d.state IN ('queued', 'preparing', 'prepared', 'uncertain') AND d.next_attempt_at <= ?1 ORDER BY d.id LIMIT ?2")?;
        let rows = statement.query_map(params![now.timestamp(), limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, u32>(6)?,
                row.get::<_, bool>(7)?,
            ))
        })?;
        rows.map(|row| {
            let (id, article, state, account_did, rkey, record, attempts, retry_allowed) = row?;
            Ok(Delivery {
                id,
                article: serde_json::from_str(&article)?,
                state,
                account_did,
                rkey,
                record: record
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
                attempts,
                retry_allowed,
            })
        })
        .collect()
    }

    pub(crate) fn worker_started(&mut self, now: DateTime<Utc>) -> Result<()> {
        self.require_writable()?;
        self.connection.execute(
            "UPDATE metadata SET last_worker_started_at = ?1 WHERE singleton = 1",
            [now.timestamp()],
        )?;
        Ok(())
    }

    pub(crate) fn worker_finished(&mut self, now: DateTime<Utc>) -> Result<()> {
        self.connection.execute(
            "UPDATE metadata SET last_worker_success_at = ?1 WHERE singleton = 1",
            [now.timestamp()],
        )?;
        Ok(())
    }

    pub(crate) fn begin_attempt(&mut self, id: i64) -> Result<()> {
        self.transition("UPDATE deliveries SET attempts = attempts + 1 WHERE id = ?1 AND state IN ('queued', 'preparing', 'prepared', 'uncertain')", [id])
    }

    pub(crate) fn preparing(&mut self, id: i64, rkey: &str, did: &str) -> Result<()> {
        Tid::from_str(rkey)
            .map_err(|_| Error::Protocol("publisher returned an invalid TID".into()))?;
        self.require_writable()?;
        let transaction = self.connection.transaction()?;
        let bound: Option<String> = transaction.query_row(
            "SELECT account_did FROM metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if bound.as_deref() != Some(did) {
            return Err(Error::Account(
                "delivery account differs from persistent account".into(),
            ));
        }
        let changed = transaction.execute("UPDATE deliveries SET state = 'preparing', rkey = ?2, account_did = ?3 WHERE id = ?1 AND state = 'queued' AND rkey IS NULL", params![id, rkey, did])?;
        require_transition(changed)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn prepared(&mut self, id: i64, record: &Value) -> Result<()> {
        if !record.is_object() {
            return Err(Error::Protocol(
                "prepared record must be a JSON object".into(),
            ));
        }
        let record = serde_json::to_string(record)?;
        self.transition("UPDATE deliveries SET state = 'prepared', record_json = ?2, last_error = NULL, retry_allowed = 1 WHERE id = ?1 AND state = 'preparing' AND record_json IS NULL", params![id, record])
    }

    pub(crate) fn uncertain(&mut self, id: i64) -> Result<()> {
        self.transition("UPDATE deliveries SET state = 'uncertain' WHERE id = ?1 AND state IN ('prepared', 'uncertain')", [id])
    }

    pub(crate) fn defer(
        &mut self,
        id: i64,
        next: DateTime<Utc>,
        reason: &str,
        retry_allowed: bool,
    ) -> Result<()> {
        self.transition("UPDATE deliveries SET next_attempt_at = ?2, last_error = ?3, retry_allowed = ?4 WHERE id = ?1 AND state IN ('queued', 'preparing', 'prepared', 'uncertain')", params![id, next.timestamp(), reason, retry_allowed])
    }

    pub(crate) fn hold(&mut self, id: i64, reason: &str) -> Result<()> {
        self.transition("UPDATE deliveries SET state = 'held', last_error = ?2, retry_allowed = 0 WHERE id = ?1 AND state IN ('queued', 'preparing', 'prepared', 'uncertain')", params![id, reason])
    }

    pub(crate) fn sent(&mut self, id: i64, receipt: &Receipt, now: DateTime<Utc>) -> Result<()> {
        if receipt.uri.is_empty() || receipt.cid.is_empty() {
            return Err(Error::Protocol("remote receipt is incomplete".into()));
        }
        self.transition("UPDATE deliveries SET state = 'sent', uri = ?2, cid = ?3, sent_at = ?4, last_error = NULL WHERE id = ?1 AND state IN ('prepared', 'uncertain')", params![id, receipt.uri, receipt.cid, now.timestamp()])
    }

    fn transition<P: rusqlite::Params>(&mut self, sql: &str, params: P) -> Result<()> {
        self.require_writable()?;
        let transaction = self.connection.transaction()?;
        require_transition(transaction.execute(sql, params)?)?;
        transaction.commit()?;
        Ok(())
    }

    fn require_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::State(
                "read-only status connection cannot modify state".into(),
            ));
        }
        Ok(())
    }
}

fn private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn existing_connection(path: &Path, read_only: bool) -> Result<Connection> {
    if !path.is_file() {
        return Err(Error::State(
            "database is missing; initialize it explicitly".into(),
        ));
    }
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let connection = Connection::open_with_flags(path, flags)?;
    configure(&connection, read_only)?;
    Ok(connection)
}

fn configure(connection: &Connection, read_only: bool) -> Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    if !read_only {
        // FULL commits make a confirmed delivery durable before media removal.
        connection.pragma_update(None, "synchronous", "FULL")?;
    }
    Ok(())
}

fn user_version(connection: &Connection) -> Result<i64> {
    Ok(connection.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn application_id(connection: &Connection) -> Result<i64> {
    Ok(connection.pragma_query_value(None, "application_id", |row| row.get(0))?)
}

fn tables(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection.prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")?;
    Ok(statement
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn validate_columns(connection: &Connection, table: &str, expected: &[&str]) -> Result<()> {
    // Table names only come from constants owned by the schema, never configuration.
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if names
        .iter()
        .map(String::as_str)
        .ne(expected.iter().copied())
    {
        return Err(Error::State(format!("unexpected columns in {table}")));
    }
    Ok(())
}

fn validate_legacy(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(posts)")?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let expected = [
        ("id", "INTEGER", 1),
        ("title", "TEXT", 0),
        ("published_date", "TEXT", 0),
    ];
    if columns
        .iter()
        .map(|(name, kind, primary)| (name.as_str(), kind.as_str(), *primary))
        .ne(expected)
    {
        return Err(Error::State("unrecognized legacy posts schema".into()));
    }
    let title_unique: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_index_list('posts') AS indexes WHERE indexes.\"unique\" = 1 AND indexes.partial = 0 AND (SELECT COUNT(*) FROM pragma_index_info(indexes.name)) = 1 AND (SELECT name FROM pragma_index_info(indexes.name) LIMIT 1) = 'title'",
        [], |row| row.get(0),
    )?;
    if title_unique == 0 {
        return Err(Error::State(
            "legacy posts title uniqueness is missing".into(),
        ));
    }
    Ok(())
}

fn validate_schema(connection: &Connection) -> Result<()> {
    if user_version(connection)? != SCHEMA_VERSION || application_id(connection)? != APPLICATION_ID
    {
        return Err(Error::State(
            "unknown or uninitialized database schema".into(),
        ));
    }
    if tables(connection)?
        != [
            "deliveries",
            "feeds",
            "item_aliases",
            "items",
            "legacy_title_aliases",
            "metadata",
            "posts",
        ]
    {
        return Err(Error::State(
            "database tables do not match the initialized schema".into(),
        ));
    }
    for (table, columns) in [
        ("posts", &["id", "title", "published_date"][..]),
        (
            "legacy_title_aliases",
            &["normalized_title", "legacy_post_id"],
        ),
        (
            "metadata",
            &[
                "singleton",
                "profile",
                "account_did",
                "initialized_at",
                "last_worker_started_at",
                "last_worker_success_at",
            ],
        ),
        (
            "feeds",
            &[
                "feed_id",
                "baseline_at",
                "last_scan_at",
                "etag",
                "last_modified",
            ],
        ),
        (
            "items",
            &[
                "id",
                "feed_id",
                "article_json",
                "first_seen_at",
                "last_seen_at",
                "historical_title",
            ],
        ),
        ("item_aliases", &["kind", "scope", "value", "item_id"]),
        (
            "deliveries",
            &[
                "id",
                "item_id",
                "state",
                "account_did",
                "rkey",
                "record_json",
                "attempts",
                "next_attempt_at",
                "last_error",
                "retry_allowed",
                "created_at",
                "sent_at",
                "uri",
                "cid",
            ],
        ),
    ] {
        validate_columns(connection, table, columns)?;
    }
    if count(
        connection,
        "SELECT COUNT(*) FROM metadata WHERE singleton = 1",
        [],
    )? != 1
    {
        return Err(Error::State("initialization metadata is missing".into()));
    }
    let violations: Option<String> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()?;
    if violations.is_some() {
        return Err(Error::State(
            "database contains broken state references".into(),
        ));
    }
    Ok(())
}

fn validate_initial_inputs(
    profile: &str,
    did: Option<&str>,
    snapshots: &[FeedSnapshot],
) -> Result<()> {
    if profile.trim().is_empty() || snapshots.is_empty() {
        return Err(Error::State(
            "initialization requires a profile and validated feed scans".into(),
        ));
    }
    if let Some(did) = did {
        validate_did(did)?;
    }
    validate_snapshots(snapshots)
}

fn validate_did(did: &str) -> Result<()> {
    Did::from_str(did).map_err(|_| Error::Account("invalid account DID".into()))?;
    Ok(())
}

fn validate_snapshots(snapshots: &[FeedSnapshot]) -> Result<()> {
    let mut feeds = HashSet::new();
    for scan in snapshots {
        if scan.feed_id.trim().is_empty() || !feeds.insert(scan.feed_id.as_str()) {
            return Err(Error::Feed("missing or repeated feed identity".into()));
        }
        let mut ids = HashSet::new();
        let mut urls = HashSet::new();
        let mut aliases = HashSet::new();
        for article in &scan.articles {
            if article.feed_id != scan.feed_id
                || article.id.trim().is_empty()
                || article.title.trim().is_empty()
                || !is_http_url(&article.url)
                || !ids.insert(article.id.as_str())
                || !urls.insert(article.url.as_str())
                || article.aliases.iter().any(|alias| alias.trim().is_empty())
                || source_aliases(article)
                    .into_iter()
                    .any(|alias| !aliases.insert(alias))
            {
                return Err(Error::Feed(
                    "scan contains invalid or duplicate article identities".into(),
                ));
            }
        }
        if scan
            .eligible_ids
            .iter()
            .any(|id| !ids.contains(id.as_str()))
        {
            return Err(Error::Feed(
                "eligibility references an item absent from its validated scan".into(),
            ));
        }
    }
    Ok(())
}

fn validate_selection(selection: &BackfillSelection) -> Result<()> {
    if selection.max_items == 0
        || (selection.since.is_none() && selection.until.is_none() && selection.item_ids.is_empty())
        || matches!((selection.since, selection.until), (Some(since), Some(until)) if since > until)
    {
        return Err(Error::Config(
            "backfill requires a positive limit and valid date or item bounds".into(),
        ));
    }
    Ok(())
}

fn count<P: rusqlite::Params>(connection: &Connection, sql: &str, params: P) -> Result<usize> {
    checked_count(connection.query_row(sql, params, |row| row.get(0))?)
}

fn checked_count(amount: i64) -> Result<usize> {
    usize::try_from(amount)
        .map_err(|_| Error::State("database count exceeds supported range".into()))
}

fn require_transition(changed: usize) -> Result<()> {
    if changed != 1 {
        return Err(Error::State(
            "delivery transition was not applicable".into(),
        ));
    }
    Ok(())
}

fn feed_exists(connection: &Connection, id: &str) -> Result<bool> {
    Ok(connection
        .query_row("SELECT 1 FROM feeds WHERE feed_id = ?1", [id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn require_feed(connection: &Connection, id: &str) -> Result<()> {
    if !feed_exists(connection, id)? {
        return Err(Error::State(
            "feed has no baseline; initialize added feeds explicitly".into(),
        ));
    }
    Ok(())
}

fn insert_feed(transaction: &Transaction<'_>, scan: &FeedSnapshot, now: i64) -> Result<()> {
    transaction.execute("INSERT INTO feeds (feed_id, baseline_at, last_scan_at, etag, last_modified) VALUES (?1, ?2, ?2, ?3, ?4)", params![scan.feed_id, now, scan.validators.etag, scan.validators.last_modified])?;
    Ok(())
}

fn update_validators(transaction: &Transaction<'_>, scan: &FeedSnapshot, now: i64) -> Result<()> {
    transaction.execute(
        "UPDATE feeds SET last_scan_at = ?2, etag = ?3, last_modified = ?4 WHERE feed_id = ?1",
        params![
            scan.feed_id,
            now,
            scan.validators.etag,
            scan.validators.last_modified
        ],
    )?;
    Ok(())
}

fn scan_items(
    transaction: &Transaction<'_>,
    scan: &FeedSnapshot,
    now: i64,
    queue_new: bool,
    backfill: bool,
    queue_limit: Option<usize>,
    report: &mut IngestReport,
) -> Result<()> {
    let eligible: HashSet<&str> = scan.eligible_ids.iter().map(String::as_str).collect();
    for article in &scan.articles {
        if queue_limit.is_some_and(|limit| report.queued >= limit) {
            break;
        }
        let aliases = source_aliases(article);
        let mut existing = None;
        for &(kind, scope, value) in &aliases {
            let matched: Option<i64> = transaction.query_row(
                "SELECT item_id FROM item_aliases WHERE kind = ?1 AND scope = ?2 AND value = ?3",
                params![kind, scope, value], |row| row.get(0),
            ).optional()?;
            if let Some(item_id) = matched {
                if existing.is_some_and(|other| other != item_id) {
                    return Err(Error::Feed(
                        "source aliases refer to conflicting observed items".into(),
                    ));
                }
                existing = Some(item_id);
            }
        }
        let new_in_feed = if let Some(item_id) = existing {
            transaction.query_row(
                "SELECT 1 FROM item_aliases WHERE kind = 'guid' AND scope = ?1 AND item_id = ?2 LIMIT 1",
                params![scan.feed_id, item_id], |_| Ok(()),
            ).optional()?.is_none()
        } else {
            true
        };
        let exact_historical = transaction
            .query_row(
                "SELECT 1 FROM posts WHERE title = ?1",
                [&article.title],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let mut historical = exact_historical;
        for key in crate::feed::legacy_title_keys(&article.title) {
            if transaction
                .query_row(
                    "SELECT 1 FROM legacy_title_aliases WHERE normalized_title = ?1",
                    [&key],
                    |_| Ok(()),
                )
                .optional()?
                .is_some()
            {
                historical = true;
                break;
            }
        }
        let (item_id, legacy_match) = if let Some(item_id) = existing {
            // Only unqueued evidence may evolve. Pending work must survive source edits.
            transaction.execute(
                "UPDATE items SET last_seen_at = ?2, historical_title = historical_title OR ?3, article_json = CASE WHEN EXISTS (SELECT 1 FROM deliveries WHERE item_id = ?1) THEN article_json ELSE ?4 END WHERE id = ?1",
                params![item_id, now, historical, serde_json::to_string(article)?],
            )?;
            let legacy: bool = transaction.query_row(
                "SELECT historical_title FROM items WHERE id = ?1",
                [item_id],
                |row| row.get(0),
            )?;
            (item_id, legacy || historical)
        } else {
            transaction.execute("INSERT INTO items (feed_id, article_json, first_seen_at, last_seen_at, historical_title) VALUES (?1, ?2, ?3, ?3, ?4)", params![scan.feed_id, serde_json::to_string(article)?, now, historical])?;
            report.observed += 1;
            (transaction.last_insert_rowid(), historical)
        };
        for (kind, scope, value) in aliases {
            transaction.execute("INSERT OR IGNORE INTO item_aliases (kind, scope, value, item_id) VALUES (?1, ?2, ?3, ?4)", params![kind, scope, value, item_id])?;
        }
        if legacy_match {
            report.historical_matches += 1;
        }
        if queue_new
            && (new_in_feed || backfill)
            && eligible.contains(article.id.as_str())
            && !legacy_match
        {
            report.queued += transaction.execute("INSERT OR IGNORE INTO deliveries (item_id, state, next_attempt_at, created_at) VALUES (?1, 'queued', ?2, ?2)", params![item_id, now])?;
        }
    }
    Ok(())
}

fn install_legacy_title_aliases(transaction: &Transaction<'_>) -> Result<()> {
    let mut select =
        transaction.prepare("SELECT id, title FROM posts WHERE title IS NOT NULL ORDER BY id")?;
    let titles = select.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut insert = transaction.prepare("INSERT OR IGNORE INTO legacy_title_aliases (normalized_title, legacy_post_id) VALUES (?1, ?2)")?;
    for title in titles {
        let (id, title) = title?;
        for key in crate::feed::legacy_title_keys(&title) {
            insert.execute(params![key, id])?;
        }
    }
    Ok(())
}

fn is_http_url(value: &str) -> bool {
    url::Url::parse(value)
        .is_ok_and(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
}

fn source_aliases(article: &Article) -> Vec<(&str, &str, &str)> {
    let mut aliases = vec![
        ("guid", article.feed_id.as_str(), article.id.as_str()),
        ("url", "", article.url.as_str()),
    ];
    for alias in std::iter::once(&article.id).chain(&article.aliases) {
        let key = if is_http_url(alias) {
            ("url", "", alias.as_str())
        } else {
            ("guid", article.feed_id.as_str(), alias.as_str())
        };
        if !aliases.contains(&key) {
            aliases.push(key);
        }
    }
    aliases
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub fn article(id: &str, url: &str, title: &str) -> Article {
        Article {
            feed_id: "news".into(),
            id: id.into(),
            url: url.into(),
            title: title.into(),
            aliases: Vec::new(),
            summary: "A source-backed summary".into(),
            published_at: Some(Utc::now()),
            image_urls: Vec::new(),
            image_alt: None,
            feed_title: "News".into(),
        }
    }

    pub fn snapshot(articles: Vec<Article>) -> FeedSnapshot {
        FeedSnapshot {
            feed_id: "news".into(),
            eligible_ids: articles.iter().map(|article| article.id.clone()).collect(),
            articles,
            validators: FeedValidators::default(),
        }
    }

    pub fn store() -> Result<(tempfile::TempDir, Store)> {
        let directory = tempfile::tempdir()?;
        let store = Store::initialize(
            &directory.path().join("state.sqlite"),
            "test",
            None,
            &[snapshot(Vec::new())],
        )?;
        Ok((directory, store))
    }

    pub fn enqueue(store: &mut Store, id: &str) -> Result<()> {
        store.ingest(&snapshot(vec![article(
            id,
            &format!("https://news.example/{id}"),
            "Same title",
        )]))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn silent_baseline_does_not_queue_but_new_items_do() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let first = article("old", "https://news.example/old", "First");
        let scan = snapshot(vec![first.clone()]);
        let mut store = Store::initialize(
            &directory.path().join("state.sqlite"),
            "test",
            None,
            std::slice::from_ref(&scan),
        )?;
        assert_eq!(store.due_count(Utc::now())?, 0);
        assert_eq!(store.ingest(&scan)?.queued, 0);
        let second = article("new", "https://news.example/new", "Second");
        assert_eq!(store.ingest(&snapshot(vec![first, second]))?.queued, 1);
        Ok(())
    }

    #[test]
    fn malformed_initial_scan_cannot_create_state() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state.sqlite");
        let invalid = article("id", "not a URL", "Title");
        assert!(Store::initialize(&path, "test", None, &[snapshot(vec![invalid])]).is_err());
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn initial_cross_feed_alias_conflict_rolls_back_and_removes_new_database() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state.sqlite");
        let primary = snapshot(vec![
            article("one", "https://news.example/one", "One"),
            article("two", "https://news.example/two", "Two"),
        ]);
        let mut conflict = article("other", "https://news.example/two", "Conflicting source");
        conflict.feed_id = "other-feed".into();
        conflict.aliases.push("https://news.example/one".into());
        let secondary = FeedSnapshot {
            feed_id: "other-feed".into(),
            eligible_ids: vec![conflict.id.clone()],
            articles: vec![conflict],
            validators: FeedValidators::default(),
        };
        assert!(Store::initialize(&path, "test", None, &[primary, secondary]).is_err());
        assert!(!path.exists());
        Ok(())
    }

    #[test]
    fn added_feed_requires_explicit_silent_baseline() -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut item = article("id", "https://news.example/added", "Added");
        item.feed_id = "added".into();
        let mut scan = snapshot(vec![item]);
        scan.feed_id = "added".into();
        assert!(store.ingest(&scan).is_err());
        assert_eq!(
            store.baseline(std::slice::from_ref(&scan))?.baselined_feeds,
            1
        );
        assert_eq!(store.ingest(&scan)?.queued, 0);
        Ok(())
    }

    #[test]
    fn guid_and_url_changes_keep_all_previously_seen_aliases() -> Result<()> {
        let (_directory, mut store) = store()?;
        enqueue(&mut store, "original")?;
        assert_eq!(
            store
                .ingest(&snapshot(vec![article(
                    "rotated",
                    "https://news.example/original",
                    "Edited"
                )]))?
                .queued,
            0
        );
        assert_eq!(
            store
                .ingest(&snapshot(vec![article(
                    "rotated",
                    "https://news.example/rotated",
                    "Edited again"
                )]))?
                .queued,
            0
        );
        assert_eq!(
            store
                .ingest(&snapshot(vec![article(
                    "original",
                    "https://news.example/original",
                    "Original restored"
                )]))?
                .queued,
            0
        );
        assert_eq!(store.status()?.observed_items, 1);
        Ok(())
    }

    #[test]
    fn original_guid_alias_survives_canonical_guid_and_link_changes() -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut first = article(
            "https://news.example/guid",
            "https://news.example/link",
            "Original",
        );
        first.aliases = vec![
            "https://news.example/guid#raw-guid".into(),
            "raw-source-guid".into(),
        ];
        store.ingest(&snapshot(vec![first]))?;
        let mut changed = article("new-guid", "https://news.example/new-link", "Changed");
        changed.aliases = vec!["raw-source-guid".into()];
        assert_eq!(store.ingest(&snapshot(vec![changed]))?.queued, 0);
        let mut changed_again =
            article("another-guid", "https://news.example/another-link", "Again");
        changed_again.aliases = vec!["https://news.example/guid#raw-guid".into()];
        assert_eq!(store.ingest(&snapshot(vec![changed_again]))?.queued, 0);
        assert_eq!(store.status()?.observed_items, 1);
        Ok(())
    }

    #[test]
    fn backfill_uses_current_unqueued_content_and_preserves_queued_evidence() -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut scan = snapshot(vec![article(
            "one",
            "https://news.example/one",
            "Initial title",
        )]);
        scan.eligible_ids.clear();
        store.ingest(&scan)?;
        scan.articles[0].title = "Current title".into();
        scan.eligible_ids.push("one".into());
        let selection = BackfillSelection {
            since: None,
            until: None,
            item_ids: vec!["one".into()],
            max_items: 1,
        };
        store.backfill(&scan, &selection)?;
        scan.articles[0].title = "Edited after queueing".into();
        store.ingest(&scan)?;
        assert_eq!(store.due(Utc::now(), 1)?[0].article.title, "Current title");
        Ok(())
    }

    #[test]
    fn same_title_with_distinct_ids_and_urls_is_distinct_work() -> Result<()> {
        let (_directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        enqueue(&mut store, "two")?;
        assert_eq!(store.due_count(Utc::now())?, 2);
        Ok(())
    }

    #[test]
    fn global_url_alias_prevents_duplicate_across_feed_ids() -> Result<()> {
        let (_directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut second = article("other-guid", "https://news.example/one", "Same item");
        second.feed_id = "other-feed".into();
        let scan = FeedSnapshot {
            feed_id: second.feed_id.clone(),
            eligible_ids: vec![second.id.clone()],
            articles: vec![second],
            validators: FeedValidators::default(),
        };
        store.baseline(&[scan])?;
        assert_eq!(store.status()?.observed_items, 1);
        Ok(())
    }

    #[test]
    fn first_eligible_observation_in_another_feed_queues_a_shared_ineligible_item() -> Result<()> {
        let (_directory, mut store) = store()?;
        let added = FeedSnapshot {
            feed_id: "other-feed".into(),
            articles: Vec::new(),
            eligible_ids: Vec::new(),
            validators: FeedValidators::default(),
        };
        store.baseline(&[added])?;
        let mut first = snapshot(vec![article(
            "guid-a",
            "https://news.example/shared",
            "Ineligible A",
        )]);
        first.eligible_ids.clear();
        assert_eq!(store.ingest(&first)?.queued, 0);
        let mut second_article = article("guid-b", "https://news.example/shared", "Eligible B");
        second_article.feed_id = "other-feed".into();
        let mut second = FeedSnapshot {
            feed_id: "other-feed".into(),
            eligible_ids: vec![second_article.id.clone()],
            articles: vec![second_article],
            validators: FeedValidators::default(),
        };
        assert_eq!(store.ingest(&second)?.queued, 1);
        assert_eq!(store.due(Utc::now(), 1)?[0].article.feed_id, "other-feed");
        assert_eq!(store.ingest(&second)?.queued, 0);
        second.articles[0].id = "rotated-guid-b".into();
        second.eligible_ids = vec!["rotated-guid-b".into()];
        assert_eq!(store.ingest(&second)?.queued, 0);
        first.eligible_ids.push("guid-a".into());
        assert_eq!(store.ingest(&first)?.queued, 0);
        assert_eq!(store.due_count(Utc::now())?, 1);
        Ok(())
    }

    #[test]
    fn changed_guid_and_filters_in_one_observed_feed_do_not_backfill_an_ineligible_item()
    -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut scan = snapshot(vec![article(
            "original",
            "https://news.example/observed",
            "Observed",
        )]);
        scan.eligible_ids.clear();
        store.ingest(&scan)?;
        scan.articles[0].id = "rotated".into();
        scan.eligible_ids.push("rotated".into());
        assert_eq!(store.ingest(&scan)?.queued, 0);
        assert_eq!(store.due_count(Utc::now())?, 0);
        Ok(())
    }

    #[test]
    fn alias_conflict_rolls_back_the_whole_ingestion_and_validators() -> Result<()> {
        let (_directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        enqueue(&mut store, "two")?;
        let mut scan = snapshot(vec![
            article("new", "https://news.example/new", "New"),
            article("one", "https://news.example/two", "Conflict"),
        ]);
        scan.validators.etag = Some("must-not-commit".into());
        assert!(store.ingest(&scan).is_err());
        assert_eq!(store.status()?.observed_items, 2);
        assert_eq!(store.validators("news")?.and_then(|v| v.etag), None);
        Ok(())
    }

    #[test]
    fn filter_changes_require_explicit_bounded_backfill() -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut scan = snapshot(vec![article("old", "https://news.example/old", "Old")]);
        scan.eligible_ids.clear();
        store.ingest(&scan)?;
        scan.eligible_ids.push("old".into());
        assert_eq!(store.ingest(&scan)?.queued, 0);
        let unbounded = BackfillSelection {
            since: None,
            until: None,
            item_ids: Vec::new(),
            max_items: 1,
        };
        assert!(store.backfill(&scan, &unbounded).is_err());
        let bounded = BackfillSelection {
            item_ids: vec!["old".into()],
            ..unbounded
        };
        assert_eq!(store.backfill(&scan, &bounded)?.queued, 1);
        assert_eq!(store.backfill(&scan, &bounded)?.queued, 0);
        Ok(())
    }

    #[test]
    fn backfill_enforces_date_selection_and_item_cap() -> Result<()> {
        let (_directory, mut store) = store()?;
        let now = Utc::now();
        let mut older = article("old", "https://news.example/old", "Older");
        older.published_at = Some(now - chrono::Duration::days(2));
        let mut recent = article("recent", "https://news.example/recent", "Recent");
        recent.published_at = Some(now);
        let mut later = article("later", "https://news.example/later", "Later");
        later.published_at = Some(now + chrono::Duration::hours(1));
        let mut undated = article("undated", "https://news.example/undated", "Undated");
        undated.published_at = None;
        let mut scan = snapshot(vec![older, recent, later, undated]);
        let ids = std::mem::take(&mut scan.eligible_ids);
        store.ingest(&scan)?;
        scan.eligible_ids = ids;
        let selection = BackfillSelection {
            since: Some(now - chrono::Duration::hours(1)),
            until: Some(now + chrono::Duration::hours(2)),
            item_ids: Vec::new(),
            max_items: 1,
        };
        assert_eq!(store.backfill(&scan, &selection)?.queued, 1);
        let queued = store.due(Utc::now(), 10)?;
        assert_eq!(queued[0].article.id, "recent");
        Ok(())
    }

    #[test]
    fn repeated_bounded_backfill_fills_cap_after_queued_and_historical_rows() -> Result<()> {
        let (_directory, mut store) = store()?;
        store.connection.execute(
            "INSERT INTO posts (title, published_date) VALUES ('Historic', 'opaque legacy date')",
            [],
        )?;
        let now = Utc::now();
        let mut historic = article("historic", "https://news.example/historic", "Historic");
        historic.published_at = Some(now - chrono::Duration::days(3));
        let mut one = article("one", "https://news.example/one", "One");
        one.published_at = Some(now - chrono::Duration::days(2));
        let mut two = article("two", "https://news.example/two", "Two");
        two.published_at = Some(now - chrono::Duration::days(1));
        let mut scan = snapshot(vec![historic, one, two]);
        let eligible = std::mem::take(&mut scan.eligible_ids);
        store.ingest(&scan)?;
        scan.eligible_ids = eligible;
        let selection = BackfillSelection {
            since: Some(now - chrono::Duration::days(4)),
            until: None,
            item_ids: Vec::new(),
            max_items: 1,
        };
        assert_eq!(store.backfill(&scan, &selection)?.queued, 1);
        assert_eq!(store.backfill(&scan, &selection)?.queued, 1);
        assert_eq!(store.backfill(&scan, &selection)?.queued, 0);
        assert_eq!(store.due_count(Utc::now())?, 2);
        Ok(())
    }

    #[test]
    fn legacy_migration_preserves_opaque_ids_and_dates_and_suppresses_titles() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("legacy.sqlite");
        let connection = Connection::open(&path)?;
        connection.execute_batch("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT UNIQUE, published_date TEXT); INSERT INTO posts VALUES (7, 'Historic', 'Wed, 02 Oct 2024 15:00:00 GMT'); INSERT INTO posts VALUES (9001, 'Recovery', '2026-09-20T01:02:03.456+00:00');")?;
        let before = legacy_rows(&connection)?;
        drop(connection);
        let scan = snapshot(vec![
            article("historic", "https://news.example/historic", "Historic"),
            article("current", "https://news.example/current", "Current"),
        ]);
        let mut store = Store::migrate_legacy(&path, "test", None, &[scan])?;
        assert_eq!(legacy_rows(&store.connection)?, before);
        assert_eq!(store.due_count(Utc::now())?, 1);
        assert_eq!(
            store
                .ingest(&snapshot(vec![article(
                    "different-guid",
                    "https://news.example/different",
                    "Historic"
                )]))?
                .queued,
            0
        );
        assert_eq!(store.status()?.legacy_posts, 2);
        Ok(())
    }

    #[test]
    fn canonical_legacy_titles_suppress_whitespace_entities_and_markup_without_changing_rows()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("legacy.sqlite");
        let connection = Connection::open(&path)?;
        connection.execute_batch("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT UNIQUE, published_date TEXT);")?;
        for (id, title, date) in [
            (7, "A\u{a0}B", "opaque source date"),
            (9, "A  B", "opaque recovery date"),
            (12, "C &amp; D", "mixed date string"),
            (15, "<b>E</b>&nbsp;F", "preserved markup date"),
        ] {
            connection.execute(
                "INSERT INTO posts (id, title, published_date) VALUES (?1, ?2, ?3)",
                params![id, title, date],
            )?;
        }
        let before = legacy_rows(&connection)?;
        drop(connection);
        let rss = br#"<rss version="2.0"><channel><title>News</title><link>https://news.example/</link><description>News</description>
            <item><guid>space</guid><title>A&#160;B</title><link>https://news.example/space</link></item>
            <item><guid>entity</guid><title>C &amp; D</title><link>https://news.example/entity</link></item>
            <item><guid>markup</guid><title>E F</title><link>https://news.example/markup</link></item>
        </channel></rss>"#;
        let config = crate::config::FeedConfig {
            id: "news".into(),
            url: "https://news.example/feed".into(),
            ..Default::default()
        };
        let scan = snapshot(crate::feed::parse(rss, &config)?);
        let store = Store::migrate_legacy(&path, "test", None, &[scan])?;
        assert_eq!(store.due_count(Utc::now())?, 0);
        assert_eq!(legacy_rows(&store.connection)?, before);
        drop(store);
        let mut reopened = Store::open(&path, "test", None)?;
        assert_eq!(
            reopened
                .ingest(&snapshot(vec![article(
                    "another-space",
                    "https://news.example/another-space",
                    "A B"
                )]))?
                .queued,
            0
        );
        // Whitespace normalization is reserved for imported history. Ordinary
        // new observations with matching text keep independent source identity.
        assert_eq!(
            reopened
                .ingest(&snapshot(vec![article(
                    "new-one",
                    "https://news.example/new-one",
                    "New title"
                )]))?
                .queued,
            1
        );
        assert_eq!(
            reopened
                .ingest(&snapshot(vec![article(
                    "new-two",
                    "https://news.example/new-two",
                    "New  title"
                )]))?
                .queued,
            1
        );
        assert_eq!(
            reopened
                .ingest(&snapshot(vec![article(
                    "case-distinct",
                    "https://news.example/case",
                    "a b"
                )]))?
                .queued,
            1
        );
        Ok(())
    }

    fn legacy_rows(connection: &Connection) -> Result<Vec<(i64, String, String)>> {
        let mut statement =
            connection.prepare("SELECT id, title, published_date FROM posts ORDER BY id")?;
        Ok(statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    #[test]
    fn read_only_status_preserves_file_and_rejects_mutations() -> Result<()> {
        let (directory, store) = store()?;
        drop(store);
        let path = directory.path().join("state.sqlite");
        let before = std::fs::read(&path)?;
        let mut read_only = Store::open_read_only(&path, "test", None)?;
        assert_eq!(read_only.status()?.profile, "test");
        assert!(read_only.verify_or_bind_did("did:plc:test").is_err());
        drop(read_only);
        assert_eq!(std::fs::read(&path)?, before);
        Ok(())
    }

    #[test]
    fn missing_unknown_and_profile_mismatched_state_fail_closed() -> Result<()> {
        let (directory, store) = store()?;
        let missing = directory.path().join("missing.sqlite");
        assert!(Store::open(&missing, "test", None).is_err());
        assert!(!missing.exists());
        let path = directory.path().join("state.sqlite");
        assert!(Store::open(&path, "different", None).is_err());
        store.connection.pragma_update(None, "user_version", 99)?;
        assert!(Store::open(&path, "test", None).is_err());
        Ok(())
    }

    #[test]
    fn missing_initialized_table_and_unknown_legacy_schema_are_rejected() -> Result<()> {
        let (directory, store) = store()?;
        store.connection.execute("DROP TABLE item_aliases", [])?;
        assert!(Store::open(&directory.path().join("state.sqlite"), "test", None).is_err());
        let legacy_path = directory.path().join("unknown.sqlite");
        let legacy = Connection::open(&legacy_path)?;
        legacy.execute(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, sent_at TEXT)",
            [],
        )?;
        assert!(
            Store::migrate_legacy(&legacy_path, "test", None, &[snapshot(Vec::new())]).is_err()
        );
        assert_eq!(tables(&legacy)?, ["posts"]);
        Ok(())
    }

    #[test]
    fn conditional_check_preserves_existing_validators() -> Result<()> {
        let (_directory, mut store) = store()?;
        let mut scan = snapshot(Vec::new());
        scan.validators.etag = Some("source-version".into());
        store.ingest(&scan)?;
        store.mark_feed_checked("news")?;
        assert_eq!(
            store
                .validators("news")?
                .and_then(|validators| validators.etag),
            Some("source-version".into())
        );
        assert!(store.mark_feed_checked("unbaselined").is_err());
        Ok(())
    }

    #[test]
    fn account_handle_changes_keep_did_but_wrong_did_fails() -> Result<()> {
        let (directory, mut store) = store()?;
        store.verify_or_bind_did("did:plc:stable")?;
        store.verify_or_bind_did("did:plc:stable")?;
        assert!(store.verify_or_bind_did("did:plc:other").is_err());
        assert!(
            Store::open(
                &directory.path().join("state.sqlite"),
                "test",
                Some("did:plc:other")
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn failed_scan_never_discards_work_that_disappeared_from_feed() -> Result<()> {
        let (_directory, mut store) = store()?;
        enqueue(&mut store, "pending")?;
        let invalid = snapshot(vec![article("invalid", "bad URL", "Invalid")]);
        assert!(store.ingest(&invalid).is_err());
        store.ingest(&snapshot(Vec::new()))?;
        assert_eq!(store.due_count(Utc::now())?, 1);
        Ok(())
    }
}
