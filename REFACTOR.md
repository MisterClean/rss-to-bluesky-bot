# Rust RSS-to-Bluesky refactor

This is the durable implementation contract for the refactor. Update the completion and verification notes as work progresses. The public application is `rss-to-bluesky-bot`; Chicago YIMBY is its first example configuration.

## Scope and authorization

- Implement on `codex/rust-rss-bluesky-refactor`, commit and push to the public repository, and prepare a reviewable PR.
- The user now authorizes renaming the repository and project to `rss-to-bluesky-bot`, merging PR #4, migrating the existing production history and deploying the Rust worker. Verify a consistent production database copy and stop all previous publishers before enabling the replacement schedule.
- Preserve the Chicago YIMBY profile/feed IDs and account DID through the rename and cutover. Keep the established Bluesky account identity; live test posts and account changes remain outside this authorization.
- Keep credentials, sessions, databases, production snapshots, SSH information, and machine-specific audit details out of Git. The existing tracked 13-row database is not production input and must be untracked, without deleting the local file.
- User explicitly permits replacing legacy behavior. Preserve historical suppression and reliable new-article delivery; replace Python loops, fuzzy title checks, and the old image quality limits.

## Product contract

- A native Rust one-shot CLI, one account and one or more RSS/Atom feeds per configuration. Independent configurations have independent state and can share the executable.
- Typed TOML configuration; secrets are environment variables or an explicit private env file. Support the existing `BLUESKY_USERNAME` and `BLUESKY_PASSWORD` names.
- Commands: `check-config`, `init`, `preview`, `run`, `status`, `backfill`, and `migrate-legacy`.
- `init` silently baselines current validated feeds. `run` requires an existing initialized database. Each subsequently added feed needs explicit baselining. Explicit backfill queues selected items and never publishes by itself.
- Preview fetches/renders source content but never loads social credentials, authenticates, creates state, or publishes. Status opens state read-only.
- Stable configured feed IDs and entry GUID/URL identity. Preserve aliases when RSS GUIDs/URLs are available; same-title/different-item entries are distinct. Preserve the legacy title table and suppress its historical matches.
- Atomic ingestion and durable delivery queue. A cap leaves work queued. Feed failures cannot create a baseline or discard queued work. Preserve pending items after they disappear from the feed.
- Persist a valid TID and frozen full JSON record before record publication. Every uncertain attempt reconciles the same key on the authoritative PDS; compare the complete expected record and hold conflicts. Never replace an uncertain image post with a new text post.
- Pin account DID in persistent state; verify session/config against it. Handle changes do not reset history. Atomic private session storage includes refreshed credentials.
- Optional-media fallback is allowed before record publication. Required-media errors defer delivery. Store exact prepared image bytes until delivery is confirmed so unreferenced blobs can be reuploaded without changing the frozen record.

## Image and protocol contract

Verified against official upstream source on 2026-10-02:

- Standard image embeds: 1–4 images, each at most **2,000,000 bytes**, with alt text and aspect ratio.
- Application output target: longest edge at most **4,000 pixels**, preserving aspect ratio, no upscaling. The dimension target comes from Bluesky's client, whereas the byte limit is a Lexicon constraint.
- Preserve valid supported originals when possible. Otherwise normalize orientation and use adaptive high-quality encoding/downscaling; remove the old 1024px / quality-65 defaults.
- Bound downloaded/decompressed bytes, source pixels and decoder allocations; work sequentially. A 4000-square decoded image has a substantial transient footprint; measure full-resolution conversion rather than claiming a tiny memory budget from an idle run.
- Use integer Lanczos convolution for resizing, consume source buffers and borrow RGB/RGBA data during encoding. A measured floating-point resize path exceeded the host's practical budget and was replaced before acceptance.
- Text: at most 300 graphemes / 3,000 UTF-8 bytes. Facet offsets are UTF-8 byte positions. Preserve the article link when shortening text.
- Current ATrium Rust bindings, minimal features, and one current-thread Tokio runtime. Reuse Reqwest transport for source fetching and XRPC where possible. Pin Cargo.lock and verify used endpoints against current Lexicons. No Node/Python packages required at runtime.

Sources: [image Lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/embed/images.json), [client constants](https://github.com/bluesky-social/social-app/blob/main/src/lib/constants.ts), [post Lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/feed/post.json), [ATrium](https://docs.rs/atrium-api/latest/atrium_api/).

## Implementation ownership and interfaces

All agents share this checkout. Do not change another owner's files without coordinating. Do not commit independently.

Root owns Cargo manifests/lock, `model.rs`, `error.rs`, `lib.rs`, CLI `main.rs`, public documentation, examples, CI, deployment examples, and integration verification.

Feed/config agent owns `config.rs`, `feed.rs`, `http.rs`, `text.rs` and their unit tests. Config types are specified below. Expose:

- `Config::load(path: &Path) -> Result<Config>`, `Config::validate() -> Result<()>`; resolve state paths relative to config file.
- `http::client(settings: &HttpConfig) -> Result<reqwest::Client>` and `http::get_bytes(client, url, limit) -> async Result<Vec<u8>>` with strict decompressed-body limits and redacted errors.
- `feed::parse(bytes: &[u8], feed: &FeedConfig) -> Result<Vec<Article>>`.
- `feed::eligible(article: &Article, feed: &FeedConfig) -> bool` applies date/keyword filters separately. Parsing returns all normalized entries, including ineligible ones, so filter changes do not manufacture historical new items.
- `feed::fetch(client: &Client, feed: &FeedConfig, http: &HttpConfig, validators: Option<&FeedValidators>) -> async Result<FeedFetch>`.
- `text::render(article: &Article, template: &str) -> Result<serde_json::Value>`; outputs post text/facets/createdAt, no embed. Validate a single `{link}` template placeholder. Supported other placeholders: `{title}`, `{summary}`, `{feed_title}`, `{published}`.

State/worker agent owns `storage.rs`, `worker.rs`, state/delivery tests. Expose a `Store` with initialize/open/read-only, feed validators/baseline state, ingestion/backfill/migration, DID binding, pending selection, preparing/prepared/uncertain/held/sent transitions and status. Preserve legacy `posts` rows/IDs/date strings. Agree exact CLI-facing methods with root early. `worker::deliver(config: &Config, store: &mut Store, publisher: &mut impl Publisher) -> async Result<RunReport>` handles bounded queue processing/recovery; it must persist preparation before send and mark all record-write errors uncertain, then reconcile before another write. Test with a mock Publisher.

`FeedSnapshot { feed_id, articles, eligible_ids, validators }` carries all normalized articles and a list of eligible IDs. Silent initialization remembers all observed entries; ordinary ingest queues only newly observed eligible entries. `migrate_legacy` queues eligible entries absent from legacy history, preserving historical suppression rather than silently dropping unposted work. CLI defaults reject empty initial snapshots unless `--allow-empty` is explicit. `init --add-feeds` explicitly baselines newly configured feeds; normal runs reject unbaselined feeds.

Bluesky/media agent owns `bluesky.rs`, `media.rs`, protocol/media tests. Implement `model::Publisher` and expose `Bluesky::connect(config: &Config, client: reqwest::Client, expected_did: Option<&str>) -> async Result<Bluesky>`. `media` owns discovery, bounded download, decoding/encoding, exact-byte cache and aspect-ratio/alt preparation. Cache under `<media_dir>/<rkey>/`, remove only that directory after confirmed delivery. Reupload frozen bytes when needed and verify returned blob reference matches. Do not log auth bodies, tokens, passwords, private feed URLs, or arbitrary API response bodies.

Config shape (owner can add documented optional settings):

```text
Config { profile: String, state: StateConfig, bluesky: BlueskyConfig,
         posting: PostingConfig, media: MediaConfig, http: HttpConfig, feeds: Vec<FeedConfig> }
StateConfig { database: PathBuf, session: PathBuf, media_dir: PathBuf }
BlueskyConfig { service: String, identifier_env: String, password_env: String, expected_did: Option<String> }
PostingConfig { template: String, max_posts_per_run: usize, pacing_seconds: u64 }
MediaConfig { enabled: bool, required: bool, max_images: usize, max_dimension: u32,
              max_upload_bytes: usize, max_download_bytes: usize, max_source_pixels: u64,
              discover_from_article: bool }
HttpConfig { timeout_seconds: u64, max_feed_bytes: usize, max_article_bytes: usize }
FeedConfig { id: String, url: String, min_date: Option<chrono::NaiveDate>,
             include_keywords: Vec<String>, exclude_keywords: Vec<String> }
```

Public structs/types and trait signatures are in `model.rs`. Errors are in `error.rs`. Use typed Result errors; no unwrap/expect outside tests, no unsafe code, narrow dependencies, meaningful behavior tests. Root will run final fmt, strict Clippy, tests and release build after integrating owners' work.

## YIMBY migration and hosting

- Carry over existing private credentials, feed URL, cutoff and posting preferences through the example profile and private environment file at deployment time; never copy real secrets into this checkout.
- The prior audit found 1,329 production title-history rows with mixed source/recovery dates and no remote receipts. Preserve opaque dates and IDs; do not infer sent times, failures or new-item cursors from them. Validate on a consistent disposable copy before production use.
- Historical suppression uses indexed whitespace/HTML-compatible title aliases built from the original legacy rows. Preserve the rows verbatim; retain exact case and punctuation, and use item identity for nonlegacy articles.
- Actual current publisher ownership was unresolved in the read-only audit. Confirm a sole publisher before enabling the new schedule.
- Generic hosting docs include scheduled one-shot examples. YIMBY's Petit job should dispatch a dedicated systemd oneshot with `systemctl start --wait`, preserving the shared scheduler and history. Use independent credentials, state, lock, permissions, limits and logs. Avoid inheriting another bot's env.
- Build off-host, keep immutable code separate from persistent state, back up consistently before migration, and roll back code without restoring stale posting history.

## Acceptance and progress

- [x] New branch and durable refactor contract.
- [x] Reusable feed/config/text pipeline and strict config validation.
- [x] Durable SQLite history, baseline/backfill/migration and recoverable publishing.
- [x] Current ATProto client, private sessions and quality-first bounded media.
- [x] CLI and examples, public README, portable scheduling and Petit integration examples.
- [x] Behavioral/CLI/protocol/media tests, formatting, Clippy, release build and native Linux memory measurements.
- [x] Public-data read-only preview and disposable legacy migration tests; no live social writes.
- [x] Secret/artifact review, commit, push, [PR #4](https://github.com/MisterClean/rss-to-bluesky-bot/pull/4) and attach it to this chat.
- [x] Rename package, executable, repository references and deployment examples to `rss-to-bluesky-bot`, preserving configured publication identity.
- [ ] Merge PR #4 after verification of the renamed project.
- [ ] Verify a production history copy, migrate during the authorized cutover, deploy and observe the dedicated Rust worker.

Update this section with exact commands, measured results and remaining limitations as work completes. Production migration and deployment are now in scope; the checkpoint below records the earlier implementation-only verification.

### Project rename verification

- `cargo check --all-targets --all-features` regenerated Cargo.lock with the renamed package. `cargo fmt --all -- --check`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, `cargo test --locked --all-targets` and `git diff --check` passed; all **115 tests** remained green.
- Package/crate names, CLI examples, the HTTP user-agent, CI artifact names and deployment paths now use `rss-to-bluesky-bot` / `rss_to_bluesky_bot`. The Petit sample dispatches the renamed service from `/`; the worker retains its private state directory.
- Chicago YIMBY's configured profile/feed IDs and pinned DID are unchanged. This rename did not change application behavior or the state schema.

### Historical implementation verification checkpoint

- CLI integration tests use local feed servers and disposable databases. They cover missing state, process overlap, silent baseline, read-only status, preview without credentials, explicit backfill, added feeds and consistent legacy backups.
- Local verification: `cargo test --locked --all-targets` passed **115 tests** (105 library, 2 CLI helper, 8 CLI integration); `cargo fmt --all -- --check`, strict all-target Clippy and the release build passed.
- Protocol tests use mock PDS endpoints only. Review found and fixed legacy title normalization and response-time retry scheduling gaps; regression tests accompany both fixes.
- Multiple feeds queue a shared URL when its first observation in another feed is eligible, while scoped observation aliases prevent filter/GUID changes from silently backfilling old items. An existing delivery remains unique across all feeds.
- Read-only public YIMBY preview rendered current source titles, intact links and a valid article image. It did not create posting state or load social credentials.
- The final public preview selected the original 1049×788 article image rather than the RSS thumbnail, producing a 609,308-byte JPEG without upscaling. Article metadata and responsive candidates are considered even with a one-image cap.
- A **synthetic** 1,329-row disposable legacy fixture modeled ID gaps, mixed opaque date formats and max ID 1,332. CLI migration and its consistent backup preserved every row exactly, passed SQLite integrity checks and created no authentication state.
- Initial native 4000-square noisy-image conversion peaked at 475,414,528 bytes; integer resizing reduced the corresponding macOS sample to 204,111,872 bytes. Native Linux peaked at **135,864 KiB (132.7 MiB)** for that fixture and **167,544 KiB (163.6 MiB)** for a 6000×4000 fixture, completing in 2.23s and 1.99s respectively. Both also passed inside independent 256 MiB/no-swap cgroups.
- Native Ubuntu 24.04 and macOS CI passed all 115 tests, formatting, strict Clippy and release builds on implementation commit `7603576`: [verified run](https://github.com/MisterClean/rss-to-bluesky-bot/actions/runs/37027547119). The Ubuntu X64 executable measured **12,443,736 bytes**; its compressed workflow artifact was about 5 MiB. Offline native macOS configuration validation peaked at 8 MiB.
- Run `cargo build --locked --release --example media-bench`, then generate fixtures in a separate process (`media-bench generate PATH WIDTH HEIGHT`) and measure `media-bench process PATH` with `/usr/bin/time -v` on Linux. The benchmark uses the same processing function and conservatively includes an extra source copy; live publishing consumes downloaded bytes directly. The generated fixtures stress compression and therefore require dimension reduction to meet the upload limit. Results are measured examples, not an allocator-level maximum for every codec/source.
- Fresh production-copy verification remains a cutover prerequisite: follow-up SSH attempts timed out or stalled during this implementation session. No remote file, process, database, credentials or schedule was changed. Synthetic legacy fixtures validate compatibility, but do not replace inspection of the actual database at cutover.
