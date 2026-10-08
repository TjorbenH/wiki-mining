# wiki-mining

A distributed web crawler that scrapes HTML, stores it in object storage, and builds a link graph across pages. Designed for wiki-style sites where crawling should stay within a restricted domain range. The crawler is really only suited for static webpages as it can only capture the raw HTML. Any dynamic pages haven't been tested or even considered yet.

## Architecture

Two Python services coordinate over Redis and persist to PostgreSQL and SeaweedFS:

- **Dispatcher** deduplicates discovered URLs against the DB and queues new ones for crawling
- **Crawler** fetches pages, saves raw HTML to SeaweedFS (S3-compatible object storage), and pushes outgoing links back to the dispatcher

Redis carries two channels, both Redis Streams with consumer groups: `crawl_stream` (dispatcher → crawler) and `unprocessed_links` (crawler → dispatcher).

`seed.py` is used to enter starting URLs into the system and also to whitelist the needed domains after fetching and evaluating their `robots.txt`. It's important to run this script only when the crawlers are not running (e.g. on first start of a crawl) because they rely on the whitelist and only load it on startup.

Interfaces into the system:
- Admin interfaces: **Adminer** at `localhost:8080` (PostgreSQL)
- **SeaweedFS web UI** at `localhost:9333`
- Monitoring frontend: **Monitor** at `localhost:5000` (`MONITOR_PORT`)

## Disclaimer
This crawler is a passion project and more of a technical challenge than actual software.

**Before pointing this at any site, check that site's Terms of Service, `robots.txt`, and any other policies.** I'm not liable for how anyone uses this project, including scraping a site in a way that violates rules.

`seed.py` fetches and honours each domain's `robots.txt` on a best-effort basis. This is not a substitute for reading a site's actual Terms of Service. All in all the scraper is probably not suited for any commercial websites.

For testing the project in a safe environment I recommend practice websites such as `books.toscrape.com` and others listed in this wonderful [article](https://www.scrapingbee.com/blog/scraper-sites/).

The scraper has been deployed on a real website and scraped 18k pages successfully.

### License
This project runs under the [GNU GPLv3 License](LICENSE).

## Usage

Create a `.env` file in the project root (it is gitignored) with the following variables:

```env
DB_HOST=db
DB_PORT=5432
DB_NAME=scraped_data
DB_USER=
DB_PASSWORD=
# read-only role for the monitoring interface
MONITOR_DB_USER=monitor
MONITOR_DB_PASSWORD=
# port of the monitoring web interface (host and container side, bound to localhost)
MONITOR_PORT=5000

S3_ACCESS_KEY=
S3_SECRET_KEY=
S3_BUCKET=raw-html

REDIS_HOST=queue
REDIS_CRAWL_STREAM=crawl_stream
REDIS_CRAWL_GROUP=crawlers
REDIS_DISPATCHER_STREAM=unprocessed_links
REDIS_DISPATCHER_GROUP=dispatchers

# --- TUNABLES ---

# crawler tunables (preset is a very polite crawl around 50 requests/minute)
CONCURRENCY_LIMIT=2
RATE_LIMIT=2.0
# identity sent with page fetches and used to evaluate robots.txt (seed.py and crawler both use this)
# example: wiki-mining-bot/1.0 (+mailto: <email>)
CRAWLER_USER_AGENT=*

# dispatcher tunables
# retries per URL before it's given up on
RECLAIM_RETRY_MAX_ATTEMPTS=5

# LOG_LEVEL is shared across the entire system (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### Quick start (run.sh)

`run.sh` wraps the manual steps below into one command:

```bash
./run.sh --help
./run.sh --log-level DEBUG --logs --seed http://books.toscrape.com
```

- `-l, --log-level LEVEL` override `LOG_LEVEL` for this run only, without editing `.env`
- `-s, --seed URL [URL ...]` seed one or more starting URLs, then start the crawler once seeding succeeds. Without `--seed`, `run.sh` brings up everything except the crawler (see Manual Start below for starting it yourself)
- `--logs` / `--no-logs` toggle the on-the-fly log capture described below (off by default)

### Monitoring
There is a simple web monitoring console implemented on `localhost:5000` (change it with `MONITOR_PORT` in `.env`) which provides useful information on the state of the crawl as well as completion detection to indicate whether the web-crawling has reached a quiescent state. It is read-only and does not interfere with the actual crawling in any way.

### Manual Start

Start all services except the crawler (it needs a whitelisted domain first):

```bash
docker compose up -d
```

Seed one or more starting URLs (bare hostnames are accepted) and whitelist their domains after fetching and evaluating each one's `robots.txt`:

```bash
docker compose exec dispatcher python seed.py http://books.toscrape.com
```

Then start the crawler:

```bash
docker compose up -d crawler
```

To actively log during the session (optional but recommended):
```bash
mkdir -p logs
ts=$(date +%Y%m%d-%H%M%S)
docker compose logs -f --no-color dispatcher > "logs/dispatcher-$ts.log" &
docker compose logs -f --no-color crawler   > "logs/crawler-$ts.log"   &
```
Stop them once the run is done with `kill %1 %2`.

After any code change:

```bash
docker compose --profile crawler down [-v --remove-orphans]
docker compose --profile crawler build --no-cache
docker compose up -d
docker compose up -d crawler   # once you've (re)seeded, if needed
```

> **Note:** because `crawler` is separated into its own compose profile, it's excluded from `down`, `build`, and `config` unless you pass `--profile crawler`.

Control individual services:

```bash
docker compose start crawler
docker compose stop crawler
docker compose start dispatcher
docker compose stop dispatcher
```

Follow logs:

```bash
docker compose logs -f
docker compose logs -f crawler
```

### Backup and restore

`scripts/backup.sh` saves a crawl to `~/wiki-mining-backup-<date>-<time>/` or a specified directory. Each backup holds a `pg_dump`, every HTML object as a plain file, raw snapshots of all three volumes, the row counts at backup time, `logs/` and SHA-256 checksums. The crawler and dispatcher are stopped while the data is exported.

```bash
./scripts/backup.sh                    # backup folder, including .env
./scripts/backup.sh --archive --no-env # one .tar.gz + .sha256 to transfer, without .env
```

`scripts/restore.sh` verifies a backup folder or `.tar.gz` and then **replaces** the stack's database, S3 bucket and Redis queue with it. It checks the restored row and object counts against the backup. The crawler is not started.

```bash
./scripts/restore.sh ~/wiki-mining-backup-2026-10-08-153012
./scripts/restore.sh --env wiki-mining-backup-2026-10-08-153012.tar.gz   # also take the backup's .env
```

Either run this script with `--env` (to copy over the .env from the backup) or ensure crucial settings are the same (such as passwords and usernames). Otherwise the restore will not run. See `--help` for details.

## Contributing

A pre-commit hook (`scripts/git-hooks/pre-commit`) checks Python syntax and lints for bugs before every commit. Install it once per clone:

```bash
cp scripts/git-hooks/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit
```

The hook's linter (`ruff`) runs from a project-local virtual environment, not your global Python install:

```bash
python3 -m venv .venv
.venv/bin/pip install ruff==0.16.6
```

## Extending the crawler

`ScraperWorker` can be used directly. By default `_filter_urls` keeps only discovered links on domains whitelisted by `seed.py` and honours each domain's cached `robots.txt`. Subclass it and override either hook for site-specific behavior, then point `crawler/main.py` at your subclass:

- `_process_html(html: str) -> str`: transform HTML before it is saved to S3
- `_filter_urls(urls: set[str]) -> set[str]`: restrict which discovered URLs are forwarded to the dispatcher (call `super()._filter_urls()` to keep the whitelist/robots.txt behavior)

**MediaWikiScraperWorker** is a special subclass for MediaWiki pages. Those tend to have a lot more junk pages (Files, Images etc.) than actual article pages so this class filters them out.

## AI-Usage

Parts of this project (code, commit messages, documentation) were written with [Claude Code](https://claude.com/claude-code).
