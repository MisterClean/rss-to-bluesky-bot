//! Shared source and publication models.

use crate::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;

/// An item normalized from any supported feed format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Article {
    pub feed_id: String,
    pub id: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub url: String,
    pub title: String,
    pub summary: String,
    pub published_at: Option<DateTime<Utc>>,
    pub image_urls: Vec<String>,
    pub image_alt: Option<String>,
    pub feed_title: String,
}

/// Conditional-request validators stored only after a valid feed scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedValidators {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// The outcome of a bounded conditional feed request.
#[derive(Debug)]
pub enum FeedFetch {
    Modified {
        articles: Vec<Article>,
        validators: FeedValidators,
    },
    NotModified,
}

/// A confirmed remote publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub uri: String,
    pub cid: String,
}

/// A reconciliation result from the authoritative account PDS.
#[derive(Debug, PartialEq, Eq)]
pub enum Reconciliation {
    Absent,
    Matching(Receipt),
    Conflict,
}

/// A publisher boundary that supports crash-safe, immutable-record recovery.
///
/// Futures need not be Send: the worker processes one delivery at a time on a
/// current-thread runtime. Tests can provide a deterministic remote service.
pub trait Publisher {
    fn account_did(&self) -> &str;
    fn allocate_rkey(&self) -> Result<String>;
    fn prepare(&mut self, rkey: &str, article: &Article) -> impl Future<Output = Result<Value>>;
    fn reconcile(
        &mut self,
        rkey: &str,
        record: &Value,
    ) -> impl Future<Output = Result<Reconciliation>>;
    fn publish(&mut self, rkey: &str, record: &Value) -> impl Future<Output = Result<Receipt>>;
    fn cleanup(&mut self, _rkey: &str) -> Result<()> {
        Ok(())
    }
}

/// Counts describing a bounded publication pass.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RunReport {
    pub attempted: usize,
    pub sent: usize,
    pub reconciled: usize,
    pub deferred: usize,
    pub held: usize,
}
