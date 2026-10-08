# TODO

Brainstorming / backlog. Not prioritized.

---

## Monitoring follow-ups

**Links detected / new URLs per minute**
Currently not included into the stats because they are a pain to get as is (would require some very expensive reads into the raw-data table). Clean fix is a schema change to include a `created_at` timestamp to record when a link was discovered similar to how the scraped/minute stat works.

**S3 object count vs. `done` rows**
Show the S3 object count and flag a mismatch with `done` rows. Needs read-only S3 credentials for the monitor.

**Status count cost on a large table**
`DataEndpoint._db_stats` counts `RawData` by status every poll (10s). On a large crawl this is too expensive so there should be a way to lower the interval and adapt to the table size.

**Enforce read-only on Redis**
Redis access is read-only only by convention (`XINFO`). A Redis ACL user limited to `XINFO`/`PING` would enforce it.

**Container health and uptime**
Show the health/uptime of the other containers on the dashboard (currently only the monitor has a healthcheck).

---

## Features

**Crawl depth limit**
Specify a maximum crawl depth (possibly as a Crawler subclass, implementation still open). Important for testing on larger websites where we don't want to scrape everything.

**Rework the whitelisting**
Make sure it's possible to whitelist a domain without entering a seed website. The crawl might only start on a single seed website but should still be allowed to extend onto different domains as needed.

Also the way whitelisting and fetching of robots.txt is handled right now is very fragile and more like a workaround. Instead of passively reading it on startup some service should actively communicate the current state with the crawlers.

**Start/Stop management**
Maybe pass the docker compose start stop signals to not actually end the main.py but rather interface with the run / stop methods? check pros and cons here.

---

## Reliability & Data Safety

**Rows orphaned as `queued` when the dispatcher's Redis push fails**
`QueueManager._process_batch` commits the new `RawData` rows, then pushes them onto `crawl_stream`. If that push fails, the batch stays unacked and `ReclaimManager` replays it later, but by then the rows already exist, so the upsert reports `is_new = false` and nothing gets pushed. Those URLs stay `queued` in the DB and are never crawled.

**`crawler`/`dispatcher` have no restart policy or healthcheck**
The two services that do the actual work have no health check or restart policy in `docker-compose.yaml`. If either process dies from an unhandled exception, the crawl silently stops with nothing to bring it back or flag it unhealthy. Add `restart: always` and a basic healthcheck (a process-liveness check, or a small HTTP endpoint like the monitor's `/health`).

**`db` can fail its healthcheck on first start**
On a fresh `db-data` volume, Postgres's init took over 30s, longer than `start_period: 30s` + 5 retries allow. The first `docker compose up -d` after `down -v` then fails with "dependency db failed to start"; a second `up` works. Raise `start_period` (e.g. 90s).

**Deduplicate Code**
There are a bunch of functions that are needed by both the crawler and dispatcher such as `_canonicalize` but that are currently just copy-paste. If one changes on accident the services don't fit together anymore and deduplication etc. breaks. Not good.

---

## Scalability

**Multiple crawler containers**
The Redis Stream consumer group architecture already supports multiple consumers. `docker compose up --scale crawler=N` should work in principle but has not been tested. Verify that nothing breaks at N > 1.

---

## Testing

**Multi-container crawl**
Test with `--scale crawler=2` or more to verify consumer group isolation works correctly and URLs are not double-processed.

**Failure injection**
Test crawler crash mid-scrape, dispatcher crash mid-batch, Redis restart, PostgreSQL restart. Verify the system recovers without data loss once the stale entry reclaim mechanism is in place.

**Integration test suite**
No automated tests exist.
At minimum add some tests for the fragile parts of the crawler and dispatcher (`_canonicalize`, `_is_article_url`, `_parse_wiki_layout`, `robots.txt` status handling and `_evaluate_completion`).

Next step would be to test the `xmax = 0` upsert, the `XAUTOCLAIM` replay and the stale sweep.

Finally there could be an entire integration test with a small sample website spin up, failure injection etc. This is also where some CI/CD would most pay off.
