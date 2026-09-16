# wiki-mining

A distributed web crawler that scrapes HTML, stores it in object storage, and builds a link graph across pages. Designed for wiki-style sites where crawling should stay within a restricted domain range. The crawler is really only suited for static webpages as it can only capture the raw html. Any dynamic pages haven't been tested or even considered yet.

## Architecture

Two Python services coordinate over Redis and persist to PostgreSQL and MinIO:

- **Dispatcher** deduplicates discovered URLs against the DB and queues new ones for crawling
- **Crawler** fetches pages, saves raw HTML to MinIO, and pushes outgoing links back to the dispatcher

Redis carries two channels, both Redis Streams with consumer groups: `crawl_stream` (dispatcher → crawler) and `unprocessed_links` (crawler → dispatcher).

`seed.py` is used to enter starting URLs into the system and also to whitelist the needed domains after fetching and evaluating their `robots.txt`. It's important to run this script only when the Crawlers are not running (eg. on first start of a crawl) because they rely on the whitelist and only load it on startup.

Admin interfaces: **Adminer** at `localhost:8080` (PostgreSQL), **MinIO console** at `localhost:9001`.

## Disclaimer 
This crawler is a passion project and more of a technical challenge than actual software. 

**Before pointing this at any site, check that site's Terms of Service, `robots.txt`, and any other policies.** I'm not liable for how anyone uses this project, including scraping a site in a way that violates rules.

`seed.py` fetches and honours each domain's `robots.txt` on a best-effort basis. This is not a substitute for reading a site's actual Terms of Service. All in all the scraper is probably not suited for any commercial websites.

For testing the project in a safe environment I recommend practice websites such as `books.toscrape.com` and others listed in this wonderful [article](https://www.scrapingbee.com/blog/scraper-sites/).

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

MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
MINIO_BUCKET=raw-html

REDIS_HOST=queue
REDIS_PORT=6379
REDIS_CRAWL_STREAM=crawl_stream
REDIS_CRAWL_GROUP=crawlers
REDIS_DISPATCHER_STREAM=unprocessed_links
REDIS_DISPATCHER_GROUP=dispatchers

# --- TUNABLES ---

# crawler tunables
CONCURRENCY_LIMIT=4
RATE_LIMIT=0.5
# identity sent with page fetches and used to evaluate robots.txt (seed.py and crawler both use this)
CRAWLER_USER_AGENT=*

# dispatcher tunables
BATCH_SIZE=50

# dispatcher reclaim tunables
RECLAIM_PENDING_MIN_IDLE_MS=60000 # how long a url has to be pending to be reclaimed
RECLAIM_PENDING_POLL_INTERVAL=30
RECLAIM_PENDING_BATCH_SIZE=200
RECLAIM_RETRY_BATCH_SIZE=100
RECLAIM_RETRY_MAX_ATTEMPTS=5
RECLAIM_RETRY_BASE_DELAY_SECONDS=60
RECLAIM_RETRY_POLL_INTERVAL=60

# LOG_LEVEL is shared by crawler, dispatcher, and seed.py (DEBUG, INFO, WARNING, ERROR)
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

> **Note:** because `crawler` is seperated into its own compose profile, it's excluded from `down`, `build`, and `config` unless you pass `--profile crawler`.

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

`ScraperWorker` (`crawler/main.py`) is used directly. By default `_filter_urls` keeps only discovered links on domains whitelisted by `seed.py` and honours each domain's cached `robots.txt`. Subclass it and override either hook for site-specific behavior, then point `crawler/main.py` at your subclass:

- `_process_html(html: str) -> str`: transform HTML before it is saved to MinIO
- `_filter_urls(urls: set[str]) -> set[str]`: restrict which discovered URLs are forwarded to the dispatcher (call `super()._filter_urls()` to keep the whitelist/robots.txt behavior)

## AI-Usage

Parts of this project (code, commit messages, documentation) were written with [Claude Code](https://claude.com/claude-code).
