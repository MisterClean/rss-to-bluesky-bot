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
4. Stage `petit-rss-to-bluesky.yaml` outside the daemon's jobs directory, or set
   `enabled: false` in the staged copy. Its six-field cron schedules on second
   zero every ten minutes. Validate and list the staged directory offline:

   ```sh
   pt validate /path/to/staged-jobs
   pt list /path/to/staged-jobs
   ```

   `enabled: false` does not block `pt trigger`; triggering executes the job and
   must not be used for validation. Keep the job staged or disabled until the
   migration, queue review and first manual publishing pass are complete.
   The job uses `/` as its working directory so the scheduler does not need access
   to the worker's private state directory.
5. Grant the scheduler permission to start the worker as described below. After
   reviewing baseline/queue status, run `sudo -u ubuntu systemctl --no-ask-password
   start --wait rss-to-bluesky-bot.service` for the first authorized publishing
   pass. Replace `ubuntu` with the actual scheduler user when different. Inspect
   `journalctl -u rss-to-bluesky-bot.service` and the CLI's `status` output before
   activating the Petit schedule.

## Allow the scheduler to start the worker

For a scheduler running as `ubuntu`, install this root-owned, mode `0644` rule at
`/etc/polkit-1/rules.d/50-rss-to-bluesky-bot.rules`. Match the actual scheduler user
if different. The rule grants only starting this specific worker service:

```js
polkit.addRule(function(action, subject) {
    if (action.id === "org.freedesktop.systemd1.manage-units" &&
        subject.user === "ubuntu" &&
        action.lookup("unit") === "rss-to-bluesky-bot.service" &&
        action.lookup("verb") === "start") {
        return polkit.Result.YES;
    }
});
```

Keep the worker unit, executable and credential file owned by root. The scheduler
starts the unit; systemd supplies the worker's private environment and runs it as
`rss-bot`. The manual pass above also verifies this permission without an
interactive authentication prompt.

## Activate the Petit schedule

Petit **0.2.0** loads job YAML once when its daemon starts; editing files or running
`pt validate` / `pt list` does not reload that running daemon. See the
[pinned startup implementation](https://github.com/PedramNavid/petit/blob/170dee43be847b29bc065f4265a4b3e16d4fb7fd/src/main.rs#L445-L477).
Installing the worker unit and running `systemctl daemon-reload` are separate from
loading the Petit job.

Inspect the existing shared Petit service and its wrapper scripts before a
restart, including any startup catch-up or recovery hooks. Wait for active jobs
to finish, preserve the same scheduler database, all existing job IDs, credentials,
concurrency and memory limits, then place the enabled job in its configured jobs
directory. Validate and list that complete directory again before restarting the
existing daemon. For an installation whose unit is named `petit.service`:

```sh
sudo systemctl restart petit.service
sudo journalctl -u petit.service --since '5 minutes ago' --no-pager
```

Use the actual unit name for your installation. Confirm the daemon loaded the new
job and retained its existing jobs, then observe the next scheduled worker pass
in both the Petit history and worker journal. Keep the job ID stable on future
updates so scheduler history remains associated with the same job.

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
