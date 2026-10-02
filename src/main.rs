//! Scheduler-neutral commands for operating a single configured bot.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use rss_to_bluesky_bot::{
    bluesky::{Bluesky, Credentials},
    config::Config,
    feed, http, media,
    model::{FeedFetch, RunReport},
    storage::{BackfillSelection, FeedSnapshot, Store},
    text, worker,
};
use serde_json::json;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Parser)]
#[command(
    version,
    about = "Publish RSS and Atom articles to Bluesky with durable delivery"
)]
struct Cli {
    /// TOML configuration; relative state paths resolve beside this file.
    #[arg(long, default_value = "bot.toml", global = true)]
    config: PathBuf,
    /// Private dotenv credentials. Used only by run when work is due.
    #[arg(long, global = true)]
    env_file: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate configuration without networking or reading credentials.
    CheckConfig,
    /// Silently baseline current articles; never authenticates or publishes.
    Init {
        /// Explicitly baseline only newly added feeds in existing state.
        #[arg(long)]
        add_feeds: bool,
        /// Permit a valid feed with zero entries during initialization.
        #[arg(long)]
        allow_empty: bool,
    },
    /// Render current feed articles without state changes or social credentials.
    Preview {
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,
        /// Write processed image previews to this explicit directory.
        #[arg(long)]
        media_dir: Option<PathBuf>,
    },
    /// Ingest feeds, process a bounded queue and exit; may publish.
    Run,
    /// Read baseline, account and queue status without altering state.
    Status,
    /// Queue selected current feed items; never authenticates or publishes.
    Backfill {
        #[arg(long)]
        feed: String,
        /// Inclusive UTC date or RFC3339 timestamp.
        #[arg(long)]
        since: Option<String>,
        /// Inclusive UTC date or RFC3339 timestamp.
        #[arg(long)]
        until: Option<String>,
        /// Select exact normalized entry IDs; repeat for multiple items.
        #[arg(long)]
        item: Vec<String>,
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=1000))]
        max_items: u32,
    },
    /// Back up and add Rust state to a recognized legacy title-history DB.
    MigrateLegacy {
        /// New backup file; defaults to a dated file beside the database.
        #[arg(long)]
        backup: Option<PathBuf>,
        #[arg(long)]
        allow_empty: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    match cli.command {
        Command::CheckConfig => print_json(&json!({"valid":true, "profile":config.profile,
            "feeds":config.feeds.iter().map(|f| &f.id).collect::<Vec<_>>()}))?,
        Command::Status => {
            let store = Store::open_read_only(
                &config.state.database,
                &config.profile,
                config.bluesky.expected_did.as_deref(),
            )?;
            print_json(&store.status()?)?;
        }
        Command::Preview { limit, media_dir } => {
            let client = http::client(&config.http)?;
            let mut output = Vec::new();
            for source in &config.feeds {
                if output.len() >= limit as usize {
                    break;
                }
                let fetch = feed::fetch(&client, source, &config.http, None).await?;
                if let FeedFetch::Modified { articles, .. } = fetch {
                    for article in articles.iter().filter(|a| feed::eligible(a, source)) {
                        if output.len() >= limit as usize {
                            break;
                        }
                        let mut preview = json!({"feed":article.feed_id,"id":article.id,
                            "source_url":article.url,"record":text::render(article,&config.posting.template)?});
                        if let Some(directory) = media_dir.as_ref() {
                            match media::prepare_images(
                                &client,
                                article,
                                &config.media,
                                &config.http,
                            )
                            .await
                            {
                                Ok(images) => {
                                    fs::create_dir_all(directory)?;
                                    let mut paths = Vec::new();
                                    for (index, image) in images.iter().enumerate() {
                                        let extension = if image.mime == "image/png" {
                                            "png"
                                        } else {
                                            "jpg"
                                        };
                                        let path = directory.join(format!(
                                            "{}-{}-{index}.{extension}",
                                            article.feed_id,
                                            output.len()
                                        ));
                                        let mut file = OpenOptions::new().write(true).create_new(true).open(&path)
                                            .context("Preview output already exists or cannot be written")?;
                                        std::io::Write::write_all(&mut file, &image.bytes)?;
                                        paths.push(json!({"path":path,"width":image.width,
                                            "height":image.height,"bytes":image.bytes.len(),"mime":image.mime}));
                                    }
                                    preview["media"] = json!(paths);
                                }
                                Err(error) => {
                                    preview["media_error"] = json!(error.to_string());
                                }
                            }
                        }
                        output.push(preview);
                    }
                }
            }
            print_json(&output)?;
        }
        Command::Init {
            add_feeds,
            allow_empty,
        } => {
            let _lock = state_lock(&config.state.database, !add_feeds)?;
            let client = http::client(&config.http)?;
            if add_feeds {
                let mut store = Store::open(
                    &config.state.database,
                    &config.profile,
                    config.bluesky.expected_did.as_deref(),
                )?;
                let ids = config
                    .feeds
                    .iter()
                    .filter_map(|f| match store.is_baselined(&f.id) {
                        Ok(false) => Some(Ok(f.id.clone())),
                        Ok(true) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect::<rss_to_bluesky_bot::Result<Vec<_>>>()?;
                let snapshots = snapshots(&config, &client, Some(&ids), allow_empty).await?;
                print_json(&store.baseline(&snapshots)?)?;
            } else {
                if config.state.database.exists() {
                    bail!("State already exists; use init --add-feeds for new feeds");
                }
                let snapshots = snapshots(&config, &client, None, allow_empty).await?;
                let store = Store::initialize(
                    &config.state.database,
                    &config.profile,
                    config.bluesky.expected_did.as_deref(),
                    &snapshots,
                )?;
                print_json(&store.status()?)?;
            }
        }
        Command::MigrateLegacy {
            backup,
            allow_empty,
        } => {
            let _lock = state_lock(&config.state.database, false)?;
            let client = http::client(&config.http)?;
            let snapshots = snapshots(&config, &client, None, allow_empty).await?;
            let backup = backup.unwrap_or_else(|| {
                config.state.database.with_extension(format!(
                    "pre-rust-{}.sqlite3",
                    Utc::now().format("%Y%m%dT%H%M%S%.fZ")
                ))
            });
            consistent_backup(&config.state.database, &backup)?;
            let store = Store::migrate_legacy(
                &config.state.database,
                &config.profile,
                config.bluesky.expected_did.as_deref(),
                &snapshots,
            )?;
            print_json(&json!({"backup":backup,"status":store.status()?}))?;
        }
        Command::Backfill {
            feed: feed_id,
            since,
            until,
            item,
            max_items,
        } => {
            let selection = BackfillSelection {
                since: since.as_deref().map(|s| boundary(s, false)).transpose()?,
                until: until.as_deref().map(|s| boundary(s, true)).transpose()?,
                item_ids: item,
                max_items: max_items as usize,
            };
            if selection.since.is_none()
                && selection.until.is_none()
                && selection.item_ids.is_empty()
            {
                bail!(
                    "Backfill needs --since, --until or --item; publication requires a separate run"
                );
            }
            let source = config
                .feeds
                .iter()
                .find(|f| f.id == feed_id)
                .context("Unknown configured feed ID")?;
            let _lock = state_lock(&config.state.database, false)?;
            let mut store = Store::open(
                &config.state.database,
                &config.profile,
                config.bluesky.expected_did.as_deref(),
            )?;
            let client = http::client(&config.http)?;
            let fetch = feed::fetch(&client, source, &config.http, None).await?;
            if let FeedFetch::Modified {
                articles,
                validators,
            } = fetch
            {
                let snapshot = snapshot(source, articles, validators);
                print_json(&store.backfill(&snapshot, &selection)?)?;
            } else {
                bail!("Unconditional feed request returned not-modified");
            }
        }
        Command::Run => {
            let _lock = state_lock(&config.state.database, false)?;
            let mut store = Store::open(
                &config.state.database,
                &config.profile,
                config.bluesky.expected_did.as_deref(),
            )?;
            for source in &config.feeds {
                if !store.is_baselined(&source.id)? {
                    bail!("Feed {} needs init --add-feeds before run", source.id);
                }
            }
            let client = http::client(&config.http)?;
            let mut scans = Vec::new();
            let mut source_errors = Vec::new();
            for source in &config.feeds {
                let validators = store.validators(&source.id)?;
                match feed::fetch(&client, source, &config.http, validators.as_ref()).await {
                    Ok(FeedFetch::Modified {
                        articles,
                        validators,
                    }) => {
                        let report = store.ingest(&snapshot(source, articles, validators))?;
                        scans.push(json!({"feed":source.id,"result":report}));
                    }
                    Ok(FeedFetch::NotModified) => {
                        store.mark_feed_checked(&source.id)?;
                        scans.push(json!({"feed":source.id,"unchanged":true}));
                    }
                    Err(error) => {
                        source_errors.push(json!({"feed":source.id,"error":error.to_string()}))
                    }
                }
            }
            let delivery = if store.due_count(Utc::now())? > 0 {
                let credentials = credentials(&config, cli.env_file.as_deref())?;
                let expected = store.account_did()?;
                let mut publisher = Bluesky::connect_with_credentials(
                    &config,
                    client,
                    expected.as_deref(),
                    credentials,
                )
                .await?;
                worker::deliver(&config, &mut store, &mut publisher).await?
            } else {
                RunReport::default()
            };
            print_json(&json!({"scans":scans,"source_errors":source_errors,"delivery":delivery}))?;
            if !source_errors.is_empty() {
                bail!("One or more source scans failed; queued delivery history was retained");
            }
            if delivery.held > 0 || delivery.deferred > 0 {
                bail!("Deliveries need retry or operator attention; inspect status");
            }
        }
    }
    Ok(())
}

fn snapshot(
    source: &rss_to_bluesky_bot::config::FeedConfig,
    articles: Vec<rss_to_bluesky_bot::model::Article>,
    validators: rss_to_bluesky_bot::model::FeedValidators,
) -> FeedSnapshot {
    let eligible_ids = articles
        .iter()
        .filter(|a| feed::eligible(a, source))
        .map(|a| a.id.clone())
        .collect();
    FeedSnapshot {
        feed_id: source.id.clone(),
        articles,
        eligible_ids,
        validators,
    }
}

async fn snapshots(
    config: &Config,
    client: &reqwest::Client,
    only: Option<&[String]>,
    allow_empty: bool,
) -> Result<Vec<FeedSnapshot>> {
    let mut output = Vec::new();
    for source in &config.feeds {
        if only.is_some_and(|ids| !ids.contains(&source.id)) {
            continue;
        }
        match feed::fetch(client, source, &config.http, None).await? {
            FeedFetch::Modified {
                articles,
                validators,
            } => {
                if articles.is_empty() && !allow_empty {
                    bail!(
                        "Feed {} has no entries; use --allow-empty only for an intentionally empty feed",
                        source.id
                    );
                }
                output.push(snapshot(source, articles, validators));
            }
            FeedFetch::NotModified => bail!("Unconditional feed request returned not-modified"),
        }
    }
    Ok(output)
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn state_lock(database: &Path, initializing: bool) -> Result<File> {
    if !initializing && !database.is_file() {
        bail!("Database is missing; initialize or migrate the intended state explicitly");
    }
    let parent = database
        .parent()
        .context("Database needs a parent directory")?;
    if initializing {
        private_directory(parent)?;
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options.open(database.with_extension("lock"))?;
    FileExt::try_lock_exclusive(&lock)
        .context("Another worker or migration owns this database lock")?;
    Ok(lock)
}

fn credentials(config: &Config, env_file: Option<&Path>) -> Result<Credentials> {
    let mut values = HashMap::new();
    if let Some(path) = env_file {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
                bail!("Credential file must have private permissions (chmod 600)");
            }
        }
        for item in dotenvy::from_path_iter(path).context("Cannot open private credential file")? {
            let (key, value) = item.map_err(|_| {
                anyhow::anyhow!("Private credential file contains an invalid dotenv line")
            })?;
            values.insert(key, value);
        }
    }
    let resolve = |key: &str| {
        values
            .remove(key)
            .or_else(|| std::env::var(key).ok())
            .filter(|v| !v.trim().is_empty())
            .with_context(|| format!("Required credential variable {key} is missing"))
    };
    let mut resolve = resolve;
    Ok(Credentials {
        identifier: resolve(&config.bluesky.identifier_env)?,
        password: resolve(&config.bluesky.password_env)?,
    })
}

fn boundary(input: &str, end: bool) -> Result<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(input) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .context("Use a UTC YYYY-MM-DD date or RFC3339 timestamp")?;
    let time = if end {
        date.and_hms_nano_opt(23, 59, 59, 999_999_999)
    } else {
        date.and_hms_opt(0, 0, 0)
    }
    .context("Invalid date boundary")?;
    Ok(time.and_utc())
}

fn consistent_backup(source: &Path, destination: &Path) -> Result<()> {
    if source == destination {
        bail!("Backup must be a new separate file");
    }
    if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
        private_directory(parent)?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    drop(
        options
            .open(destination)
            .context("Backup path already exists or cannot be created")?,
    );
    let source_db =
        rusqlite::Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut destination_db = rusqlite::Connection::open(destination)?;
    {
        let backup = rusqlite::backup::Backup::new(&source_db, &mut destination_db)?;
        backup.run_to_completion(128, Duration::from_millis(10), None)?;
    }
    let integrity: String = destination_db.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    if integrity != "ok" {
        bail!("Backup integrity check failed; migration was not attempted");
    }
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_boundaries_are_inclusive_utc() -> Result<()> {
        assert_eq!(
            boundary("2026-10-02", false)?.to_rfc3339(),
            "2026-10-02T00:00:00+00:00"
        );
        assert_eq!(
            boundary("2026-10-02", true)?.to_rfc3339(),
            "2026-10-02T23:59:59.999999999+00:00"
        );
        assert_eq!(
            boundary("2026-10-02T01:00:00+01:00", false)?,
            boundary("2026-10-02", false)?
        );
        assert!(boundary("not-a-date", false).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn new_state_directories_and_explicit_credentials_are_private() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir()?;
        let state = directory.path().join("private/state");
        private_directory(&state)?;
        assert_eq!(fs::metadata(&state)?.permissions().mode() & 0o777, 0o700);
        let file = state.join("credentials.env");
        fs::write(
            &file,
            "BLUESKY_USERNAME=test.bsky.social\nBLUESKY_PASSWORD=test-password\n",
        )?;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600))?;
        let values = credentials(&Config::default(), Some(&file))?;
        assert_eq!(values.identifier, "test.bsky.social");
        assert_eq!(values.password, "test-password");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644))?;
        assert!(credentials(&Config::default(), Some(&file)).is_err());
        Ok(())
    }
}
