# RSS to Bluesky Bot

A native Rust application that posts new articles from RSS or Atom feeds to a
Bluesky account. Configure your feeds and account, initialize the history, and
schedule the executable. Each invocation scans sources, delivers a bounded batch
and exits, so a small server does not need a resident interpreter or PM2 process.

This project began as the [Chicago YIMBY bot](https://bsky.app/profile/chicagoyimby.bsky.social).
The application is now reusable: one configuration can follow several feeds for
one account, and several independent configurations can share the same executable.
The [YIMBY example](examples/yimby.toml) contains that bot's public parameters.

## Features

- RSS and Atom, conditional HTTP requests, date and keyword filters, configurable
  post templates, and clickable article links with Unicode-safe facets.
- SQLite history and a durable delivery queue. A batch limit or disappearing feed
  entry does not discard pending work.
- Silent first-run baselines, explicit backfill, account DID pinning and legacy
  title-history migration.
- Up to four images per post, up to **2,000,000 bytes each** and a **4,000-pixel
  longest edge**, with alt text and aspect ratio. Images are processed sequentially.
- Persistent private ATProto sessions, refreshed tokens, and reconciliation of
  uncertain writes against the account's authoritative PDS.
- One native executable with bundled SQLite. No Python, Node, external database
  server, image CLI or web service is required at runtime.

The byte limit follows the current [Bluesky image Lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/embed/images.json).
The dimension target follows [Bluesky's client settings](https://github.com/bluesky-social/social-app/blob/main/src/lib/constants.ts).

## Quick start

Build with Rust **1.94 or newer**. CI also produces native Ubuntu 24.04 and macOS
executables with checksums in its workflow artifacts.

```sh
cargo build --locked --release
cp bot.example.toml bot.toml
cp .env.example .env
chmod 600 .env
```

Edit `bot.toml` to set your feed URL and profile name. Edit `.env` to set your bot's
handle and a [Bluesky app password](https://bsky.app/settings/app-passwords).
Secrets belong in the environment or a private dotenv file, never in TOML or Git.

```sh
target/release/rss-bluesky-bot --config bot.toml check-config
target/release/rss-bluesky-bot --config bot.toml preview --limit 3
target/release/rss-bluesky-bot --config bot.toml init
target/release/rss-bluesky-bot --config bot.toml status
target/release/rss-bluesky-bot --config bot.toml --env-file .env run
```

**`init` remembers the feed's current entries without posting them.** Subsequent
`run` invocations queue and publish newly observed eligible entries. To publish
selected existing entries, explicitly use `backfill` and review `status` before
running the publisher. A missing database is an error; `run` never recreates lost
history automatically.

Schedule `run` every ten minutes using Petit, cron or a systemd timer. The included
[Petit and systemd examples](deploy/README.md) use the existing shared scheduler
and a dedicated worker service with independent credentials and memory limits.

## Configuration

[bot.example.toml](bot.example.toml) shows every configurable setting. Relative
state paths resolve beside the configuration file, independent of the current
working directory. Give each account/profile its own database, session and media
directory. Keep the configured profile and feed IDs stable across restarts.

```toml
profile = "local-news"

[posting]
template = "{title}\n\nRead more: {link}"
max_posts_per_run = 5
pacing_seconds = 5

[[feeds]]
id = "news"
url = "https://example.org/feed.xml"
min_date = "2026-01-01"
include_keywords = ["housing", "transit"]
exclude_keywords = ["sponsored"]

[[feeds]]
id = "other-news"
url = "https://example.net/atom.xml"
```

`min_date` is inclusive and compares publication dates in UTC; Atom's updated date
is used when its publication date is absent. Undated entries are excluded when a
minimum date is set. Keywords match title and summary without
case sensitivity. Any include keyword qualifies, and any exclude keyword rejects.
Omit filters for unrestricted new articles. Changing filters does not queue
previously observed entries; select them through `backfill` instead.

Templates support `{title}`, `{summary}`, `{feed_title}`, `{published}` (YYYY-MM-DD)
and exactly one `{link}`. The article URL stays intact when text is shortened to
Bluesky's 300-grapheme / 3,000-byte post limits. A URL/template combination that
cannot fit is rejected. Use `preview` to inspect the resulting JSON.

After adding a feed to an existing configuration, run `init --add-feeds`. This
silently baselines only the new feeds. Normal `run` refuses unbaselined feeds.
Renaming a feed ID creates a new configured source, so use a new state/profile
when intentionally replacing its identity.

By default, credentials use `BLUESKY_USERNAME` and `BLUESKY_PASSWORD`. An explicit
`--env-file` overrides matching process environment variables and is read only
when `run` has due work. Optional `bluesky.expected_did` pins the account before
login; otherwise the first publishing pass binds the DID to the database. A
handle change preserves history, while a different account is rejected.

## Commands

All commands accept `--config PATH`. They emit JSON to stdout and errors to stderr.

| Command | Behavior |
| --- | --- |
| `check-config` | Validate TOML offline. |
| `preview --limit 3` | Fetch and render eligible articles without creating state or reading social credentials. |
| `preview --media-dir /tmp/rss-preview` | Also download and prepare images into an explicit output directory. |
| `init` | Create new state and silently baseline all current entries. |
| `init --add-feeds` | Explicitly baseline newly configured feeds in existing state. |
| `status` | Read account, source checks, queue counts and pending details without changing the database. |
| `backfill --feed news --since 2026-01-01 --until 2026-01-31 --max-items 10` | Queue up to ten eligible current feed entries in an inclusive UTC range. Does not publish. |
| `backfill --feed news --item ENTRY_ID` | Select a normalized entry ID from `preview`; repeat `--item` to select several. |
| `run` | Scan sources, publish/reconcile at most the configured batch cap, then exit. |
| `migrate-legacy --backup /private/pre-rust.sqlite3` | Back up and migrate a recognized legacy `posts` database while preserving its rows. |

Initialization/migration reject empty feeds unless `--allow-empty` is explicitly
supplied. Backfill needs a date or item bound and operates on entries still
available in the source feed; it does not crawl archives. The batch cap leaves
remaining work queued for future invocations.

## Images

When enabled, article discovery prefers the article's social image metadata and
largest responsive images, with feed media as a fallback. Valid JPEG/PNG originals
that already fit are
preserved where possible. Other images are oriented, resized without upscaling,
and encoded at the highest quality that fits the configured byte limit; dimensions
may be reduced further when compression alone cannot meet the limit. GIF/WebP
sources become a static image. This application does not publish video or animation.

The defaults permit 24 million source pixels, a 16 MiB download and a 4,000-pixel
longest edge. Codec preflight, allocation limits and bounded encoding constrain
memory, but library allocation budgets are best effort; full-resolution images
still need a meaningful transient memory allowance. Reduce the source pixel or
output dimension limits when needed, and measure with your feeds. A small idle
footprint does not predict the peak during image conversion.

Native Ubuntu 24.04 verification measured **133 MiB** peak RSS for a noisy
4,000×4,000 JPEG conversion and **164 MiB** for a 6,000×4,000 source. Both completed
under a 256 MiB cgroup cap. These are stress fixtures, not universal codec memory
guarantees. The standalone Linux executable is about **12.4 MB**. See
[REFACTOR.md](REFACTOR.md) for reproducible measurements and validation details.

Feed and article bodies default to 4 MiB and 2 MiB respectively. Both encoded and
decoded gzip bodies are bounded, and the configured HTTP deadline covers transport
and source decompression.

Optional image failures may produce a text-only record **before publication**.
Set `media.required = true` to defer instead. Once a record is frozen, retries
retain its text, timestamp, image references and key. Prepared image bytes are
cached until the delivery is confirmed so retries can restore expired unreferenced
blobs without substituting a different post.

## Delivery and recovery

Entries are identified by feed-scoped GUIDs and canonical URL aliases, with URLs
deduplicated across configured feeds. Different entries with the same title remain
distinct; migrated legacy titles remain suppressed because the old database has
no GUIDs or remote receipts.

Before writing a post, the worker durably saves its account DID, valid ATProto TID
and complete JSON record. A failed or interrupted record write becomes uncertain.
The next pass reads that same key from the PDS: an identical record confirms
delivery, an absent record permits the same create-only write, and a different
record is held for review. This reduces duplicate delivery after lost responses;
external publishers, state loss and manual account edits still require operator
judgment.

`status` includes pending, prepared, uncertain, held and sent counts, retry timing,
source checks and the last delivery pass. Retryable errors persist a delayed retry
with backoff and `Retry-After`; each invocation stays bounded. Permanent conflicts
are held rather than overwritten. A nonzero `run` exit can mean a source failure,
deferred work or an item needing attention, even if other deliveries succeeded.

SQLite and a process lock prevent concurrent mutation of the same database. Keep
the database, session and media cache on durable storage and back up SQLite
consistently. Private session files are written atomically with mode `0600`; corrupt
or uncertain authentication state stops password fallback. Stop the scheduler,
preserve/rename the private session file with mode `0600`, then reconnect with an
app password for the same pinned DID. Do not force `pending_auth` to false or edit
tokens after an unknown refresh. Preserve the delivery database and media cache
throughout recovery. See [hosting and rollback](deploy/README.md).

## Migrating the original Python bot

1. Confirm which process actually owns posting and stop it during an authorized
   cutover. Use the real production history, not the former checked-in sample DB.
2. Copy [examples/yimby.toml](examples/yimby.toml) to a private configuration.
   Set its database path to a consistent **disposable copy** of the old `posts.db`.
   Carry the existing credentials through a private env file. The old
   `RSS_FEED_URL` moves into `feeds.url` in TOML.
3. Run `preview`, then `migrate-legacy`. It creates a consistent backup, retains
   the entire `posts(id, title, published_date)` table and adds the new state tables.
   Legacy date strings remain opaque; they are not treated as posting timestamps.
4. Inspect `status`. Current eligible entries whose titles are absent from legacy
   history are queued by migration. Migration itself never logs in or posts.
5. Repeat the validated procedure on production state during the cutover, then
   enable the dedicated Petit worker. Keep the prior publisher stopped.

Legacy Python configuration, fuzzy-title heuristics and the continuous loop are
replaced by typed TOML, stable item identity and scheduled Rust passes. Rolling
back executable code must preserve current delivery history; the old Python app
does not understand the Rust outbox.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release
```

Tests use disposable databases and mock HTTP/ATProto responses. They do not log in
to a real account or publish live posts. [REFACTOR.md](REFACTOR.md) records the design
contract and verification results. Source modules separate configuration, feed
normalization, text, media, ATProto, storage and delivery, keeping product choices
independent of any particular publisher or feed.

Licensed under [MIT](LICENSE).
