//! Bounded delivery and same-key recovery over a persisted immutable outbox.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::config::Config;
use crate::model::{Publisher, Reconciliation, RunReport};
use crate::storage::{Delivery, Store};
use crate::{Error, Result};

const BASE_RETRY_SECONDS: u64 = 30;
const MAX_BACKOFF_SECONDS: u64 = 6 * 60 * 60;

/// Process at most the configured cap, retaining every delayed or uncertain item.
/// The caller holds the process lock; SQLite commits still guard each transition.
pub async fn deliver(
    config: &Config,
    store: &mut Store,
    publisher: &mut impl Publisher,
) -> Result<RunReport> {
    deliver_with_clock(config, store, publisher, Utc::now).await
}

#[cfg(test)]
async fn deliver_at(
    config: &Config,
    store: &mut Store,
    publisher: &mut impl Publisher,
    now: DateTime<Utc>,
) -> Result<RunReport> {
    deliver_with_clock(config, store, publisher, || now).await
}

async fn deliver_with_clock(
    config: &Config,
    store: &mut Store,
    publisher: &mut impl Publisher,
    mut clock: impl FnMut() -> DateTime<Utc>,
) -> Result<RunReport> {
    let now = clock();
    if config
        .bluesky
        .expected_did
        .as_deref()
        .is_some_and(|did| did != publisher.account_did())
    {
        return Err(Error::Account(
            "publisher DID differs from configured account".into(),
        ));
    }
    store.verify_or_bind_did(publisher.account_did())?;
    store.worker_started(now)?;
    let mut report = RunReport::default();
    let deliveries = store.due(now, config.posting.max_posts_per_run)?;
    let mut previous_write = false;
    for delivery in deliveries {
        verify_account(store, publisher, &delivery)?;
        store.begin_attempt(delivery.id)?;
        report.attempted += 1;
        let attempts = delivery.attempts.saturating_add(1);
        let recovering = matches!(delivery.state.as_str(), "prepared" | "uncertain");

        let rkey = if let Some(rkey) = delivery.rkey.as_deref() {
            rkey.to_owned()
        } else {
            match publisher.allocate_rkey() {
                Ok(rkey) => {
                    store.preparing(delivery.id, &rkey, publisher.account_did())?;
                    rkey
                }
                Err(error) => {
                    fail_preparation(store, &delivery, &error, clock(), attempts, &mut report)?;
                    continue;
                }
            }
        };

        let record = if let Some(record) = delivery.record.as_ref() {
            record.clone()
        } else {
            match publisher.prepare(&rkey, &delivery.article).await {
                Ok(record) => {
                    if let Err(error) = verify_account(store, publisher, &delivery) {
                        store.hold(delivery.id, "account_mismatch_before_record")?;
                        return Err(error);
                    }
                    store.prepared(delivery.id, &record)?;
                    record
                }
                Err(error) => {
                    if let Err(account_error) = verify_account(store, publisher, &delivery) {
                        store.hold(delivery.id, "account_mismatch_before_record")?;
                        return Err(account_error);
                    }
                    fail_preparation(store, &delivery, &error, clock(), attempts, &mut report)?;
                    continue;
                }
            }
        };

        if recovering {
            // A crash between freezing and sending is indistinguishable from a
            // lost response. Every recovered frozen record is reconciled first.
            store.uncertain(delivery.id)?;
            let result = publisher.reconcile(&rkey, &record).await;
            let completed_at = clock();
            if let Err(error) = verify_account(store, publisher, &delivery) {
                store.defer(
                    delivery.id,
                    retry_at(completed_at, attempts, &error)?,
                    "account_mismatch_uncertain",
                    false,
                )?;
                return Err(error);
            }
            match result {
                Ok(Reconciliation::Matching(receipt)) => {
                    store.sent(delivery.id, &receipt, completed_at)?;
                    publisher.cleanup(&rkey)?;
                    report.sent += 1;
                    report.reconciled += 1;
                    continue;
                }
                Ok(Reconciliation::Conflict) => {
                    store.hold(delivery.id, "remote_record_conflict")?;
                    report.held += 1;
                    continue;
                }
                Ok(Reconciliation::Absent) => {
                    if !delivery.retry_allowed {
                        store.hold(delivery.id, "permanent_write_failure_confirmed_absent")?;
                        report.held += 1;
                        continue;
                    }
                }
                Err(error) => {
                    // A failed read cannot establish absence or permit a rewrite.
                    store.defer(
                        delivery.id,
                        retry_at(completed_at, attempts, &error)?,
                        error_category(&error),
                        delivery.retry_allowed,
                    )?;
                    report.deferred += 1;
                    if matches!(
                        error,
                        Error::Account(_)
                            | Error::Http {
                                status: 401 | 403,
                                ..
                            }
                    ) {
                        return Err(error);
                    }
                    continue;
                }
            }
        }

        if previous_write && config.posting.pacing_seconds > 0 {
            tokio::time::sleep(Duration::from_secs(config.posting.pacing_seconds)).await;
        }
        verify_account(store, publisher, &delivery)?;
        // Commit uncertainty before the network call so abrupt termination can
        // never leave a supposedly unwritten payload eligible for blind retry.
        store.uncertain(delivery.id)?;
        previous_write = true;
        let result = publisher.publish(&rkey, &record).await;
        let completed_at = clock();
        if let Err(error) = verify_account(store, publisher, &delivery) {
            store.defer(
                delivery.id,
                retry_at(completed_at, attempts, &error)?,
                "account_mismatch_uncertain",
                false,
            )?;
            return Err(error);
        }
        match result {
            Ok(receipt) => {
                store.sent(delivery.id, &receipt, completed_at)?;
                // Cleanup failure leaves the durable receipt intact; a later run
                // must never recreate this post to recover a cache directory.
                publisher.cleanup(&rkey)?;
                report.sent += 1;
            }
            Err(error) => {
                store.defer(
                    delivery.id,
                    retry_at(completed_at, attempts, &error)?,
                    error_category(&error),
                    is_retryable(&error),
                )?;
                report.deferred += 1;
                if matches!(
                    error,
                    Error::Account(_)
                        | Error::Http {
                            status: 401 | 403,
                            ..
                        }
                ) {
                    return Err(error);
                }
            }
        }
    }
    if report.deferred == 0 && report.held == 0 {
        store.worker_finished(clock())?;
    }
    Ok(report)
}

fn verify_account(
    store: &mut Store,
    publisher: &impl Publisher,
    delivery: &Delivery,
) -> Result<()> {
    if delivery
        .account_did
        .as_deref()
        .is_some_and(|did| did != publisher.account_did())
    {
        return Err(Error::Account(
            "delivery belongs to a different account DID".into(),
        ));
    }
    store.verify_or_bind_did(publisher.account_did())
}

fn fail_preparation(
    store: &mut Store,
    delivery: &Delivery,
    error: &Error,
    now: DateTime<Utc>,
    attempts: u32,
    report: &mut RunReport,
) -> Result<()> {
    if matches!(error, Error::Account(_)) {
        store.defer(
            delivery.id,
            retry_at(now, attempts, error)?,
            "account_validation",
            true,
        )?;
        return Err(Error::Account(
            "publisher rejected the authenticated account during preparation".into(),
        ));
    }
    if let Error::Http {
        status,
        retry_after,
    } = error
        && matches!(*status, 401 | 403)
    {
        store.defer(
            delivery.id,
            retry_at(now, attempts, error)?,
            "http_authentication",
            true,
        )?;
        return Err(Error::Http {
            status: *status,
            retry_after: *retry_after,
        });
    }
    if is_retryable(error) {
        store.defer(
            delivery.id,
            retry_at(now, attempts, error)?,
            error_category(error),
            true,
        )?;
        report.deferred += 1;
    } else {
        store.hold(delivery.id, error_category(error))?;
        report.held += 1;
    }
    Ok(())
}

fn is_retryable(error: &Error) -> bool {
    match error {
        Error::Http { status, .. } => {
            matches!(*status, 401 | 403 | 408 | 409 | 425 | 429 | 500..=599)
        }
        Error::Transport(_) | Error::Media(_) | Error::Io(_) | Error::Account(_) => true,
        _ => false,
    }
}

fn retry_at(now: DateTime<Utc>, attempts: u32, error: &Error) -> Result<DateTime<Utc>> {
    let exponent = attempts.saturating_sub(1).min(16);
    let backoff = BASE_RETRY_SECONDS
        .saturating_mul(1_u64 << exponent)
        .min(MAX_BACKOFF_SECONDS);
    let server_delay = match error {
        Error::Http {
            retry_after: Some(delay),
            ..
        } => *delay,
        _ => 0,
    };
    // Bound the exponential component, while honoring a longer server hint.
    let seconds = i64::try_from(backoff.max(server_delay))
        .map_err(|_| Error::Protocol("retry hint exceeds supported timestamp range".into()))?;
    chrono::Duration::try_seconds(seconds)
        .and_then(|delay| now.checked_add_signed(delay))
        .ok_or_else(|| Error::Protocol("retry hint exceeds supported timestamp range".into()))
}

fn error_category(error: &Error) -> &'static str {
    match error {
        Error::Config(_) => "configuration",
        Error::Feed(_) => "feed_validation",
        Error::Media(_) => "media_preparation",
        Error::Http { .. } => "http",
        Error::Transport(_) => "transport",
        Error::Protocol(_) => "protocol_validation",
        Error::Account(_) => "account_validation",
        Error::Conflict(_) => "remote_record_conflict",
        Error::State(_) => "state_validation",
        Error::Database(_) => "database",
        Error::Io(_) => "file_operation",
        Error::Json(_) => "json_validation",
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::{HashMap, VecDeque};
    use std::path::PathBuf;
    use std::rc::Rc;

    use rusqlite::{Connection, params};
    use serde_json::{Value, json};

    use super::*;
    use crate::model::{Article, Receipt};
    use crate::storage::test_support::{enqueue, snapshot, store};

    enum Write {
        Success,
        CommittedResponseLost,
        Fail(Error),
        RefreshToWrongAccount,
    }

    struct MockPublisher {
        did: String,
        allocated: Cell<u8>,
        prepared: usize,
        writes: VecDeque<Write>,
        attempts: Vec<(String, Value)>,
        remote: HashMap<String, Value>,
        reconciles: usize,
        cleaned: usize,
        prepare_error: Option<Error>,
        reconcile_error: Option<Error>,
        preparation_clock: Option<(Rc<Cell<DateTime<Utc>>>, chrono::Duration)>,
        database: PathBuf,
    }

    impl MockPublisher {
        fn new(database: PathBuf) -> Self {
            Self {
                did: "did:plc:stable".into(),
                allocated: Cell::new(0),
                prepared: 0,
                writes: VecDeque::new(),
                attempts: Vec::new(),
                remote: HashMap::new(),
                reconciles: 0,
                cleaned: 0,
                prepare_error: None,
                reconcile_error: None,
                preparation_clock: None,
                database,
            }
        }

        fn receipt(&self, rkey: &str) -> Receipt {
            Receipt {
                uri: format!("at://{}/app.bsky.feed.post/{rkey}", self.did),
                cid: "bafy-fixture".into(),
            }
        }
    }

    impl Publisher for MockPublisher {
        fn account_did(&self) -> &str {
            &self.did
        }

        fn allocate_rkey(&self) -> Result<String> {
            let number = self.allocated.get();
            self.allocated.set(number.saturating_add(1));
            Ok(format!(
                "3lq2gjk7x4c2{}",
                char::from(b'b'.saturating_add(number))
            ))
        }

        async fn prepare(&mut self, _rkey: &str, article: &Article) -> Result<Value> {
            self.prepared += 1;
            if let Some((clock, elapsed)) = &self.preparation_clock {
                clock.set(clock.get() + *elapsed);
            }
            if let Some(error) = self.prepare_error.take() {
                return Err(error);
            }
            Ok(json!({
                "$type":"app.bsky.feed.post", "text":article.title,
                "createdAt":"2026-10-02T12:00:00.000Z",
                "facets":[{"index":{"byteStart":0,"byteEnd":1},"features":[{"$type":"app.bsky.richtext.facet#link","uri":article.url}]}],
                "embed":{"$type":"app.bsky.embed.images","images":[{"alt":"Exact prepared image","image":{"$type":"blob","ref":{"$link":"bafy-image"},"mimeType":"image/png","size":42},"aspectRatio":{"width":4,"height":3}}]}
            }))
        }

        async fn reconcile(&mut self, rkey: &str, record: &Value) -> Result<Reconciliation> {
            self.reconciles += 1;
            if let Some(error) = self.reconcile_error.take() {
                return Err(error);
            }
            Ok(match self.remote.get(rkey) {
                Some(remote) if remote == record => Reconciliation::Matching(self.receipt(rkey)),
                Some(_) => Reconciliation::Conflict,
                None => Reconciliation::Absent,
            })
        }

        async fn publish(&mut self, rkey: &str, record: &Value) -> Result<Receipt> {
            self.attempts.push((rkey.to_owned(), record.clone()));
            match self.writes.pop_front().unwrap_or(Write::Success) {
                Write::Success => {
                    self.remote.insert(rkey.to_owned(), record.clone());
                    Ok(self.receipt(rkey))
                }
                Write::CommittedResponseLost => {
                    self.remote.insert(rkey.to_owned(), record.clone());
                    Err(Error::Transport("lost response".into()))
                }
                Write::Fail(error) => Err(error),
                Write::RefreshToWrongAccount => {
                    self.remote.insert(rkey.to_owned(), record.clone());
                    self.did = "did:plc:wrong".into();
                    Ok(self.receipt(rkey))
                }
            }
        }

        fn cleanup(&mut self, rkey: &str) -> Result<()> {
            let connection = Connection::open(&self.database)?;
            let state: String = connection.query_row(
                "SELECT state FROM deliveries WHERE rkey = ?1",
                [rkey],
                |row| row.get(0),
            )?;
            if state != "sent" {
                return Err(Error::State("cleanup before durable receipt".into()));
            }
            self.cleaned += 1;
            Ok(())
        }
    }

    fn config(cap: usize) -> Config {
        let mut config = Config::default();
        config.posting.max_posts_per_run = cap;
        config.posting.pacing_seconds = 0;
        config
    }

    #[tokio::test]
    async fn cap_leaves_backlog_queued_even_after_feed_items_disappear() -> Result<()> {
        let (directory, mut store) = store()?;
        for id in ["one", "two", "three"] {
            enqueue(&mut store, id)?;
        }
        store.ingest(&snapshot(Vec::new()))?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        let report = deliver(&config(1), &mut store, &mut publisher).await?;
        assert_eq!(report.sent, 1);
        assert_eq!(store.due_count(Utc::now())?, 2);
        assert_eq!(publisher.cleaned, 1);
        Ok(())
    }

    #[tokio::test]
    async fn committed_write_with_lost_response_is_reconciled_without_republish() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let path = directory.path().join("state.sqlite");
        let mut publisher = MockPublisher::new(path.clone());
        publisher.writes.push_back(Write::CommittedResponseLost);
        let now = Utc::now();
        let first = deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(first.deferred, 1);
        assert_eq!(store.status()?.queue.get("uncertain"), Some(&1));
        drop(store);
        let mut reopened = Store::open(&path, "test", Some("did:plc:stable"))?;
        let recovered = deliver_at(
            &config(1),
            &mut reopened,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(recovered.reconciled, 1);
        assert_eq!(publisher.attempts.len(), 1);
        assert_eq!(publisher.prepared, 1);
        assert_eq!(publisher.cleaned, 1);
        Ok(())
    }

    #[tokio::test]
    async fn absent_uncertain_write_retries_exact_tid_and_full_record() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher
            .writes
            .push_back(Write::Fail(Error::Transport("network".into())));
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(publisher.attempts.len(), 2);
        assert_eq!(publisher.attempts[0], publisher.attempts[1]);
        assert_eq!(publisher.reconciles, 1);
        assert_eq!(publisher.prepared, 1);
        Ok(())
    }

    #[tokio::test]
    async fn full_record_conflicts_are_held_even_when_text_matches() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::CommittedResponseLost);
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        for record in publisher.remote.values_mut() {
            record["embed"]["images"][0]["alt"] = json!("Different remote alt");
        }
        let report = deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(report.held, 1);
        assert_eq!(publisher.attempts.len(), 1);
        assert_eq!(store.status()?.queue.get("held"), Some(&1));
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_read_failure_keeps_uncertainty_without_write() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::CommittedResponseLost);
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        publisher.reconcile_error = Some(Error::Transport("read unavailable".into()));
        let report = deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(report.deferred, 1);
        assert_eq!(publisher.attempts.len(), 1);
        assert_eq!(store.status()?.queue.get("uncertain"), Some(&1));
        Ok(())
    }

    #[tokio::test]
    async fn crash_after_freezing_record_reconciles_before_first_write() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let now = Utc::now();
        let delivery = store.due(now, 1)?.remove(0);
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        store.verify_or_bind_did(publisher.account_did())?;
        let rkey = publisher.allocate_rkey()?;
        store.preparing(delivery.id, &rkey, publisher.account_did())?;
        let record = publisher.prepare(&rkey, &delivery.article).await?;
        store.prepared(delivery.id, &record)?;
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(publisher.reconciles, 1);
        assert_eq!(publisher.attempts[0], (rkey, record));
        assert_eq!(publisher.prepared, 1);
        Ok(())
    }

    #[tokio::test]
    async fn preparation_failure_keeps_allocated_tid_and_defers_required_media() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.prepare_error = Some(Error::Media("required image unavailable".into()));
        let now = Utc::now();
        let first = deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(first.deferred, 1);
        let first_rkey = store.status()?.deliveries.remove(0).rkey;
        deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(
            first_rkey.as_deref(),
            Some(publisher.attempts[0].0.as_str())
        );
        assert_eq!(publisher.allocated.get(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn preparation_authentication_failure_pauses_pass_and_resumes_after_recovery()
    -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        enqueue(&mut store, "two")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.prepare_error = Some(Error::Account("private session requires recovery".into()));
        let now = Utc::now();
        assert!(matches!(
            deliver_at(&config(2), &mut store, &mut publisher, now).await,
            Err(Error::Account(_))
        ));
        let status = store.status()?;
        assert_eq!(status.queue.get("preparing"), Some(&1));
        assert_eq!(status.queue.get("queued"), Some(&1));
        assert_eq!(status.last_worker_success_at, None);
        assert!(status.last_worker_started_at.is_some());
        assert_eq!(publisher.prepared, 1);
        let recovered = deliver_at(
            &config(2),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(recovered.sent, 2);
        assert!(store.status()?.last_worker_success_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_authentication_failure_pauses_without_processing_next_item()
    -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        enqueue(&mut store, "two")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::CommittedResponseLost);
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(store.status()?.last_worker_success_at, None);
        publisher.reconcile_error = Some(Error::Http {
            status: 401,
            retry_after: None,
        });
        assert!(matches!(
            deliver_at(
                &config(2),
                &mut store,
                &mut publisher,
                now + chrono::Duration::seconds(31)
            )
            .await,
            Err(Error::Http { status: 401, .. })
        ));
        assert_eq!(publisher.attempts.len(), 1);
        assert_eq!(store.status()?.queue.get("queued"), Some(&1));
        let recovered = deliver_at(
            &config(2),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(92),
        )
        .await?;
        assert_eq!(recovered.reconciled, 1);
        assert_eq!(recovered.sent, 2);
        Ok(())
    }

    #[tokio::test]
    async fn publication_authentication_failure_preserves_frozen_record_for_session_recovery()
    -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::Fail(Error::Account(
            "session refresh unknown".into(),
        )));
        let now = Utc::now();
        assert!(matches!(
            deliver_at(&config(1), &mut store, &mut publisher, now).await,
            Err(Error::Account(_))
        ));
        assert_eq!(store.status()?.queue.get("uncertain"), Some(&1));
        let recovered = deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(recovered.sent, 1);
        assert_eq!(publisher.attempts[0], publisher.attempts[1]);
        Ok(())
    }

    #[tokio::test]
    async fn rotated_session_account_mismatch_preserves_uncertain_payload() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::RefreshToWrongAccount);
        assert!(matches!(
            deliver(&config(1), &mut store, &mut publisher).await,
            Err(Error::Account(_))
        ));
        assert_eq!(store.status()?.queue.get("uncertain"), Some(&1));
        assert_eq!(store.account_did()?.as_deref(), Some("did:plc:stable"));
        assert_eq!(publisher.cleaned, 0);
        Ok(())
    }

    #[tokio::test]
    async fn permanent_write_error_is_reconciled_before_being_held() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::Fail(Error::Http {
            status: 400,
            retry_after: None,
        }));
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(store.status()?.queue.get("uncertain"), Some(&1));
        let report = deliver_at(
            &config(1),
            &mut store,
            &mut publisher,
            now + chrono::Duration::seconds(31),
        )
        .await?;
        assert_eq!(report.held, 1);
        assert_eq!(publisher.reconciles, 1);
        assert_eq!(publisher.attempts.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn retry_after_and_backoff_delay_work_without_discarding_it() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.writes.push_back(Write::Fail(Error::Http {
            status: 429,
            retry_after: Some(120),
        }));
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        assert_eq!(store.due_count(now + chrono::Duration::seconds(119))?, 0);
        assert_eq!(store.due_count(now + chrono::Duration::seconds(120))?, 1);
        assert_eq!(
            retry_at(now, 30, &Error::Transport("network".into()))?
                .signed_duration_since(now)
                .num_seconds(),
            MAX_BACKOFF_SECONDS as i64
        );
        assert!(
            retry_at(
                now,
                1,
                &Error::Http {
                    status: 429,
                    retry_after: Some(u64::MAX)
                }
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn retry_after_starts_after_slow_preparation_and_receipt_uses_current_time() -> Result<()>
    {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let now = Utc::now();
        let clock = Rc::new(Cell::new(now));
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.preparation_clock = Some((Rc::clone(&clock), chrono::Duration::minutes(5)));
        publisher.writes.push_back(Write::Fail(Error::Http {
            status: 429,
            retry_after: Some(120),
        }));
        deliver_with_clock(&config(1), &mut store, &mut publisher, || clock.get()).await?;
        let failure_at = now + chrono::Duration::minutes(5);
        let retry_at = failure_at + chrono::Duration::seconds(120);
        assert_eq!(
            store.status()?.deliveries[0].next_attempt_at,
            retry_at.timestamp()
        );
        assert_eq!(store.due_count(retry_at - chrono::Duration::seconds(1))?, 0);
        clock.set(retry_at);
        deliver_with_clock(&config(1), &mut store, &mut publisher, || clock.get()).await?;
        let connection = Connection::open(directory.path().join("state.sqlite"))?;
        let sent_at: i64 =
            connection.query_row("SELECT sent_at FROM deliveries", [], |row| row.get(0))?;
        assert_eq!(sent_at, retry_at.timestamp());
        assert_eq!(
            store.status()?.last_worker_success_at,
            Some(retry_at.timestamp())
        );
        assert_eq!(publisher.prepared, 1);
        Ok(())
    }

    #[tokio::test]
    async fn due_selection_stays_at_pass_cutoff_while_receipt_time_advances() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        enqueue(&mut store, "two")?;
        let now = Utc::now();
        let pending = store.due(now, 2)?;
        store.defer(
            pending[1].id,
            now + chrono::Duration::minutes(2),
            "scheduled",
            true,
        )?;
        let clock = Rc::new(Cell::new(now));
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher.preparation_clock = Some((Rc::clone(&clock), chrono::Duration::minutes(5)));
        let report =
            deliver_with_clock(&config(2), &mut store, &mut publisher, || clock.get()).await?;
        assert_eq!(report.sent, 1);
        assert_eq!(store.due_count(clock.get())?, 1);
        let connection = Connection::open(directory.path().join("state.sqlite"))?;
        let sent_at: i64 = connection.query_row(
            "SELECT sent_at FROM deliveries WHERE state = 'sent'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(sent_at, (now + chrono::Duration::minutes(5)).timestamp());
        assert_eq!(
            store.status()?.last_worker_success_at,
            Some(clock.get().timestamp())
        );
        Ok(())
    }

    #[tokio::test]
    async fn frozen_record_is_never_changed_after_a_write_attempt() -> Result<()> {
        let (directory, mut store) = store()?;
        enqueue(&mut store, "one")?;
        let mut publisher = MockPublisher::new(directory.path().join("state.sqlite"));
        publisher
            .writes
            .push_back(Write::Fail(Error::Transport("network".into())));
        let now = Utc::now();
        deliver_at(&config(1), &mut store, &mut publisher, now).await?;
        let id = store.status()?.deliveries.remove(0).id;
        assert!(
            store
                .prepared(id, &json!({"text":"fallback text only"}))
                .is_err()
        );
        let connection = Connection::open(directory.path().join("state.sqlite"))?;
        let stored: String = connection.query_row(
            "SELECT record_json FROM deliveries WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )?;
        assert_eq!(
            serde_json::from_str::<Value>(&stored)?,
            publisher.attempts[0].1
        );
        Ok(())
    }
}
