//! Typed configuration with private paths anchored to the configuration file.

use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{Error, Result};

/// One independent account, state directory, and collection of feeds.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub profile: String,
    pub state: StateConfig,
    pub bluesky: BlueskyConfig,
    pub posting: PostingConfig,
    pub media: MediaConfig,
    pub http: HttpConfig,
    pub feeds: Vec<FeedConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            profile: "default".into(),
            state: StateConfig::default(),
            bluesky: BlueskyConfig::default(),
            posting: PostingConfig::default(),
            media: MediaConfig::default(),
            http: HttpConfig::default(),
            feeds: Vec::new(),
        }
    }
}

/// Persistent files; relative paths are resolved by [`Config::load`].
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateConfig {
    pub database: PathBuf,
    pub session: PathBuf,
    pub media_dir: PathBuf,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            database: "state/history.sqlite3".into(),
            session: "state/session.json".into(),
            media_dir: "state/media".into(),
        }
    }
}

/// Public account settings. Credentials are read only by the publishing command.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlueskyConfig {
    pub service: String,
    pub identifier_env: String,
    pub password_env: String,
    pub expected_did: Option<String>,
}

impl Default for BlueskyConfig {
    fn default() -> Self {
        Self {
            service: "https://bsky.social".into(),
            identifier_env: "BLUESKY_USERNAME".into(),
            password_env: "BLUESKY_PASSWORD".into(),
            expected_did: None,
        }
    }
}

/// Formatting, per-run queue cap, and spacing between deliveries.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PostingConfig {
    pub template: String,
    pub max_posts_per_run: usize,
    pub pacing_seconds: u64,
}

impl Default for PostingConfig {
    fn default() -> Self {
        Self {
            template: "{title}\n\nRead more: {link}".into(),
            max_posts_per_run: 10,
            pacing_seconds: 5,
        }
    }
}

/// Sequential image preparation limits and optional article-page discovery.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MediaConfig {
    pub enabled: bool,
    pub required: bool,
    pub max_images: usize,
    pub max_dimension: u32,
    pub max_upload_bytes: usize,
    pub max_download_bytes: usize,
    pub max_source_pixels: u64,
    pub discover_from_article: bool,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            required: false,
            max_images: 4,
            max_dimension: 4_000,
            max_upload_bytes: 2_000_000,
            max_download_bytes: 16 * 1024 * 1024,
            max_source_pixels: 24_000_000,
            discover_from_article: true,
        }
    }
}

/// Total request timeout and limits on decompressed source response bodies.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    pub timeout_seconds: u64,
    pub max_feed_bytes: usize,
    pub max_article_bytes: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
            max_feed_bytes: 4 * 1024 * 1024,
            max_article_bytes: 2 * 1024 * 1024,
        }
    }
}

/// A stable identity and source URL, with independently evaluated eligibility.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeedConfig {
    pub id: String,
    pub url: String,
    pub min_date: Option<NaiveDate>,
    pub include_keywords: Vec<String>,
    pub exclude_keywords: Vec<String>,
}

impl Config {
    /// Read and validate TOML without reading account credentials or creating files.
    pub fn load(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::Config("configuration exceeds 1 MiB".into()));
        }
        let source = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Config("configuration must be UTF-8".into()))?;
        // TOML diagnostics can quote secret-bearing source lines, so do not expose them.
        let mut config: Self = toml::from_str(source).map_err(|error: toml::de::Error| {
            let location = error
                .span()
                .and_then(|span| source.get(..span.start))
                .map(|prefix| {
                    format!(
                        " near line {}",
                        prefix.bytes().filter(|byte| *byte == b'\n').count() + 1
                    )
                })
                .unwrap_or_default();
            Error::Config(format!("unable to parse TOML configuration{location}"))
        })?;
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let base = absolute
            .parent()
            .ok_or_else(|| Error::Config("configuration has no parent directory".into()))?;
        for state_path in [
            &mut config.state.database,
            &mut config.state.session,
            &mut config.state.media_dir,
        ] {
            if state_path.is_relative() {
                *state_path = base.join(&*state_path);
            }
        }
        config.validate()?;
        Ok(config)
    }

    /// Reject invalid IDs, unsafe URLs, unsupported templates, and unbounded limits.
    pub fn validate(&self) -> Result<()> {
        if !valid_id(&self.profile) {
            return invalid("profile must contain 1–128 ASCII letters, digits, '-' or '_'");
        }
        if self.feeds.is_empty() {
            return invalid("at least one feed is required");
        }
        let mut ids = HashSet::new();
        for feed in &self.feeds {
            if !valid_id(&feed.id) || !ids.insert(&feed.id) {
                return invalid("feed IDs must be valid and unique");
            }
            web_url(&feed.url).map_err(|_| {
                Error::Config("feed URL must be an HTTP(S) URL without credentials".into())
            })?;
            if feed
                .include_keywords
                .iter()
                .chain(&feed.exclude_keywords)
                .any(|keyword| keyword.trim().is_empty() || keyword.chars().any(char::is_control))
            {
                return invalid("feed keywords must be nonempty and contain no control characters");
            }
        }
        let service = web_url(&self.bluesky.service).map_err(|_| {
            Error::Config("Bluesky service must be an HTTP(S) URL without credentials".into())
        })?;
        if service.query().is_some() || service.fragment().is_some() {
            return invalid("Bluesky service must not contain a query or fragment");
        }
        if !valid_env_name(&self.bluesky.identifier_env)
            || !valid_env_name(&self.bluesky.password_env)
            || self.bluesky.identifier_env == self.bluesky.password_env
        {
            return invalid("credential environment variable names must be valid and distinct");
        }
        if self.bluesky.expected_did.as_ref().is_some_and(|did| {
            !did.starts_with("did:") || did.len() < 8 || did.chars().any(char::is_whitespace)
        }) {
            return invalid("expected_did must be a DID without whitespace");
        }
        if [
            &self.state.database,
            &self.state.session,
            &self.state.media_dir,
        ]
        .iter()
        .any(|path| path.as_os_str().is_empty())
            || self.state.database == self.state.session
            || self.state.database == self.state.media_dir
            || self.state.session == self.state.media_dir
        {
            return invalid("state paths must be nonempty and distinct");
        }
        crate::text::validate_template(&self.posting.template)?;
        if !(1..=1_000).contains(&self.posting.max_posts_per_run) {
            return invalid("max_posts_per_run must be between 1 and 1000");
        }
        if self.posting.pacing_seconds > 86_400 {
            return invalid("pacing_seconds must not exceed 86400");
        }
        if self.media.required && !self.media.enabled {
            return invalid("required media must be enabled");
        }
        if !(1..=4).contains(&self.media.max_images)
            || !(1..=4_000).contains(&self.media.max_dimension)
            || !(1..=2_000_000).contains(&self.media.max_upload_bytes)
        {
            return invalid(
                "media limits must allow 1–4 images, 1–4000 pixels, and 1–2000000 upload bytes",
            );
        }
        if !(1..=100_000_000).contains(&self.media.max_source_pixels) {
            return invalid("max_source_pixels must be between 1 and 100000000");
        }
        for bytes in [
            self.media.max_download_bytes,
            self.http.max_feed_bytes,
            self.http.max_article_bytes,
        ] {
            if !(1..=256 * 1024 * 1024).contains(&bytes) {
                return invalid("source body limits must be between 1 byte and 256 MiB");
            }
        }
        if !(1..=600).contains(&self.http.timeout_seconds) {
            return invalid("timeout_seconds must be between 1 and 600");
        }
        Ok(())
    }
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(Error::Config(message.into()))
}

fn valid_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn web_url(value: &str) -> Result<Url> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(Error::Config("invalid HTTP(S) URL".into()));
    }
    let url = Url::parse(value).map_err(|_| Error::Config("invalid HTTP(S) URL".into()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::Config("invalid HTTP(S) URL".into()));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            feeds: vec![FeedConfig {
                id: "science".into(),
                url: "https://example.org/feed.xml".into(),
                ..FeedConfig::default()
            }],
            ..Config::default()
        }
    }

    #[test]
    fn load_resolves_paths_next_to_config_and_uses_generic_defaults() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bot.toml");
        std::fs::write(
            &path,
            "[[feeds]]\nid = 'science'\nurl = 'https://example.org/rss'\nmin_date = '2026-01-01'\n",
        )?;
        let config = Config::load(&path)?;
        assert_eq!(
            config.state.database,
            directory.path().join("state/history.sqlite3")
        );
        assert_eq!(config.posting.template, "{title}\n\nRead more: {link}");
        assert_eq!(
            config.feeds[0].min_date,
            NaiveDate::from_ymd_opt(2026, 1, 1)
        );
        Ok(())
    }

    #[test]
    fn validate_rejects_duplicate_feed_ids() {
        let mut config = valid_config();
        config.feeds.push(config.feeds[0].clone());
        assert!(matches!(config.validate(), Err(Error::Config(_))));
    }

    #[test]
    fn validate_rejects_protocol_limit_violations() {
        let mut config = valid_config();
        config.media.max_upload_bytes = 2_000_001;
        assert!(config.validate().is_err());
        config.media.max_upload_bytes = 2_000_000;
        config.media.max_images = 5;
        assert!(config.validate().is_err());
        config.media.max_images = 4;
        config.http.timeout_seconds = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn malformed_toml_does_not_echo_private_values() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bot.toml");
        std::fs::write(&path, "url = https://private.example/secret-token")?;
        let error = Config::load(&path)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(!error.contains("secret-token"));
        Ok(())
    }

    #[test]
    fn unknown_settings_and_embedded_credentials_are_rejected() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bot.toml");
        std::fs::write(&path, "[http]\ntimeout_second = 20\n")?;
        assert!(Config::load(&path).is_err());
        let mut config = valid_config();
        config.feeds[0].url = "https://user:private@example.org/rss".into();
        assert!(config.validate().is_err());
        Ok(())
    }
}
