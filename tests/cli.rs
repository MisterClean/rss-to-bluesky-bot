//! Exercise the public commands with disposable state and a local RSS server.

use fs2::FileExt;
use rusqlite::Connection;
use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const RSS: &str = r#"<?xml version="1.0"?><rss version="2.0"><channel>
<title>Test News</title><link>https://example.org</link><description>News</description>
<item><guid>first</guid><title>First article</title><link>https://example.org/first</link><pubDate>Thu, 01 Oct 2026 12:00:00 +0000</pubDate></item>
<item><guid>second</guid><title>Second article</title><link>https://example.org/second</link><pubDate>Fri, 02 Oct 2026 12:00:00 +0000</pubDate></item>
</channel></rss>"#;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn config(directory: &Path, url: &str) -> TestResult<std::path::PathBuf> {
    let file = directory.join("bot.toml");
    fs::write(
        &file,
        format!(
            "profile = 'test'\n[http]\ntimeout_seconds = 2\n[media]\nenabled = false\n[[feeds]]\nid = 'news'\nurl = '{url}'\n"
        ),
    )?;
    Ok(file)
}

async fn cli(config: &Path, args: &[&str]) -> TestResult<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rss-to-bluesky-bot"));
    command
        .arg("--config")
        .arg(config)
        .args(args)
        .env_remove("BLUESKY_USERNAME")
        .env_remove("BLUESKY_PASSWORD");
    Ok(tokio::task::spawn_blocking(move || command.output()).await??)
}

fn success(output: &Output) -> TestResult<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

async fn source(body: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rss"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body.to_owned()))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn offline_validation_and_preview_never_read_credentials_or_create_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    let checked = cli(&file, &["--env-file", "/does-not-exist", "check-config"]).await?;
    assert_eq!(success(&checked)?["valid"], true);
    let preview = cli(
        &file,
        &["--env-file", "/does-not-exist", "preview", "--limit", "1"],
    )
    .await?;
    let json = success(&preview)?;
    assert_eq!(json.as_array().map(Vec::len), Some(1));
    assert!(
        json[0]["record"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("https://example.org/"))
    );
    assert!(!directory.path().join("state").exists());
    Ok(())
}

#[tokio::test]
async fn preview_stops_before_unneeded_later_sources() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    let mut contents = fs::read_to_string(&file)?;
    contents.push_str(&format!(
        "\n[[feeds]]\nid = 'broken'\nurl = '{}/missing'\n",
        server.uri()
    ));
    fs::write(&file, contents)?;
    let preview = success(&cli(&file, &["preview", "--limit", "1"]).await?)?;
    assert_eq!(preview.as_array().map(Vec::len), Some(1));
    Ok(())
}

#[tokio::test]
async fn baseline_noop_run_and_read_only_status_need_no_authentication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    success(&cli(&file, &["--env-file", "/does-not-exist", "init"]).await?)?;
    let database = directory.path().join("state/history.sqlite3");
    let before = fs::read(&database)?;
    let status = success(&cli(&file, &["status"]).await?)?;
    assert_eq!(status["observed_items"], 2);
    assert_eq!(status["due"], 0);
    assert_eq!(before, fs::read(&database)?);
    let run = success(&cli(&file, &["--env-file", "/does-not-exist", "run"]).await?)?;
    assert_eq!(run["delivery"]["attempted"], 0);
    assert!(!directory.path().join("state/session.json").exists());
    Ok(())
}

#[tokio::test]
async fn missing_state_and_lock_overlap_fail_before_authentication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    let missing = cli(&file, &["run"]).await?;
    assert!(!missing.status.success());
    assert!(!directory.path().join("state").exists());
    success(&cli(&file, &["init"]).await?)?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join("state/history.lock"))?;
    FileExt::try_lock_exclusive(&lock)?;
    let overlap = cli(&file, &["run"]).await?;
    assert!(!overlap.status.success());
    assert!(String::from_utf8_lossy(&overlap.stderr).contains("database lock"));
    Ok(())
}

#[tokio::test]
async fn backfill_is_explicit_bounded_and_does_not_publish() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    success(&cli(&file, &["init"]).await?)?;
    assert!(
        !cli(&file, &["backfill", "--feed", "news"])
            .await?
            .status
            .success()
    );
    success(
        &cli(
            &file,
            &[
                "--env-file",
                "/does-not-exist",
                "backfill",
                "--feed",
                "news",
                "--since",
                "2026-10-01",
                "--until",
                "2026-10-02",
                "--max-items",
                "1",
            ],
        )
        .await?,
    )?;
    let status = success(&cli(&file, &["status"]).await?)?;
    assert_eq!(status["due"], 1);
    assert!(!directory.path().join("state/session.json").exists());
    // Missing credentials cannot discard already queued work.
    assert!(!cli(&file, &["run"]).await?.status.success());
    assert_eq!(success(&cli(&file, &["status"]).await?)?["due"], 1);
    Ok(())
}

#[tokio::test]
async fn malformed_initial_feed_never_creates_a_baseline() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source("<rss><channel><item><title>truncated").await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    assert!(!cli(&file, &["init"]).await?.status.success());
    assert!(!directory.path().join("state/history.sqlite3").exists());
    Ok(())
}

#[tokio::test]
async fn new_feed_needs_explicit_baseline() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    success(&cli(&file, &["init"]).await?)?;
    let mut contents = fs::read_to_string(&file)?;
    contents.push_str(&format!(
        "\n[[feeds]]\nid = 'other'\nurl = '{}/rss'\n",
        server.uri()
    ));
    fs::write(&file, contents)?;
    let run = cli(&file, &["run"]).await?;
    assert!(!run.status.success());
    assert!(String::from_utf8_lossy(&run.stderr).contains("init --add-feeds"));
    success(&cli(&file, &["init", "--add-feeds"]).await?)?;
    assert_eq!(success(&cli(&file, &["status"]).await?)?["due"], 0);
    Ok(())
}

#[tokio::test]
async fn legacy_cli_backup_preserves_rows_and_queues_unposted_items() -> TestResult {
    let directory = tempfile::tempdir()?;
    let server = source(RSS).await;
    let file = config(directory.path(), &format!("{}/rss", server.uri()))?;
    fs::create_dir(directory.path().join("state"))?;
    let database = directory.path().join("state/history.sqlite3");
    let connection = Connection::open(&database)?;
    connection.execute_batch("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT UNIQUE, published_date TEXT); INSERT INTO posts VALUES (7, 'First article', 'opaque legacy date');")?;
    drop(connection);
    let backup = directory.path().join("backup.sqlite3");
    let backup_argument = backup.to_str().ok_or("temporary path is not UTF-8")?;
    success(
        &cli(
            &file,
            &[
                "--env-file",
                "/does-not-exist",
                "migrate-legacy",
                "--backup",
                backup_argument,
            ],
        )
        .await?,
    )?;
    let status = success(&cli(&file, &["status"]).await?)?;
    assert_eq!(status["legacy_posts"], 1);
    assert_eq!(status["due"], 1);
    for path in [&database, &backup] {
        let connection = Connection::open(path)?;
        let row =
            connection.query_row("SELECT id, title, published_date FROM posts", [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
        assert_eq!(
            row,
            (7, "First article".into(), "opaque legacy date".into())
        );
    }
    assert!(!directory.path().join("state/session.json").exists());
    Ok(())
}
