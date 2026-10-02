# Hosting with Petit and systemd

The executable runs one bounded publishing pass and exits. Schedule it every ten
minutes with the existing Petit scheduler. A dedicated systemd oneshot keeps its
credentials, writable state and memory budget separate from the scheduler and
other bots. PM2 and a resident Python interpreter are unnecessary.

These are installation examples, not an automatic production deployment. Build
off-host. Before enabling a replacement job, locate and stop the previous
publisher everywhere it runs; two implementations sharing an account do not
share this application's lock.

## Install a profile

1. Create an unprivileged `rss-bot` system account and a private
   `/var/lib/rss-to-bluesky-bot` directory owned by it. Put the native executable in
   a versioned `/opt/rss-to-bluesky-bot/releases/<version>/` directory and point
   `/opt/rss-to-bluesky-bot/current` at that release. Keep code read-only for the worker.
2. Copy your TOML configuration to `/etc/rss-to-bluesky-bot.toml`. Set all three state
   paths to absolute paths beneath `/var/lib/rss-to-bluesky-bot`. Store app credentials
   in `/etc/rss-to-bluesky-bot.env`, mode `0600`, owned by root; systemd reads it before
   dropping privileges. `BLUESKY_USERNAME` and `BLUESKY_PASSWORD` are the defaults.
3. Install `rss-to-bluesky-bot.service` in `/etc/systemd/system/`, adjust its paths and
   limits, then run `systemctl daemon-reload`. Run `check-config` and `preview`
   as the worker user. Initialize a fresh bot with `init`; for existing title
   history, follow the migration procedure in the main README instead.
4. Install `petit-rss-to-bluesky.yaml` in your existing Petit's jobs directory. Its
   six-field cron schedules on second zero every ten minutes. The scheduler must
   be allowed to start this specific systemd service. Preserve its existing jobs,
   credentials, history database, global concurrency and memory limits. Reload
   the job directory using your installed Petit's supported procedure.
   The job uses `/` as its working directory so the scheduler does not need access
   to the worker's private state directory.
5. After reviewing baseline/queue status, run `systemctl start --wait
   rss-to-bluesky-bot.service` for the first authorized publishing pass. Inspect
   `journalctl -u rss-to-bluesky-bot.service` and the CLI's `status` output.

The sample worker has a **256 MiB** hard memory cap and a **192 MiB** soft threshold.
These are starting budgets to validate against your sources, not a guarantee for
every permitted source image. The default source pixel budget is 24 million;
encoding and codec buffers add to decoded memory. Reduce `max_source_pixels`,
`max_dimension` or `max_images` for a smaller machine, and measure a real image run
before enabling the schedule. OOM termination preserves the database and frozen
records for reconciliation on the next pass. The separate service prevents worker
image memory from consuming the shared Petit cgroup's budget.

## Chicago YIMBY migration

Use `examples/yimby.toml` as public parameters and carry the current app credentials
through the private environment file. Preserve the production `posts.db` file;
the old repository's sample database is not production history. The migration
adds tables and retains every legacy row, ID and date string. Preview and migrate
a consistent disposable copy first, then migrate the real database only during an
authorized cutover with all previous publishers stopped.

If state remains in a pre-existing directory, change the worker's user/permissions
and `ReadWritePaths` to that exact directory. Do not grant write access to the
entire application checkout. The worker's separate service owns these permissions;
the Petit scheduler does not need write access to the bot's database or sessions.

## Backups, health and rollback

- Back up SQLite consistently with its backup API or the `sqlite3 .backup` command.
  Copying an open database alone can lose committed WAL contents. Preserve the
  database, private session and prepared-media directory as one operational state
  set; treat session files as credentials.
- `status` exposes queue counts, due items, account DID, source check timestamps
  and the last delivery pass. Monitor both scheduler failures and queue age/held
  deliveries; a successful process exit with no due work does not establish that
  every historical delivery succeeded.
- Code rollback changes the `current` symlink. Preserve current posting state;
  restoring an old database after new posts can create duplicates. The former
  Python app does not understand the Rust outbox, so reverting to it needs a
  separate state-aware recovery review, not merely a code switch.
- Keep a single job per account/profile. Application file locking rejects overlap
  for the same database; distinct copies of state do not coordinate each other.
