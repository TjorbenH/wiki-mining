# wiki-mining

A distributed web crawler that scrapes HTML, stores it in object storage, and builds a link graph across pages. Designed for wiki-style sites where crawling should stay within a single domain.

## Architecture

Two Python services coordinate over Redis and persist to PostgreSQL and MinIO:

- **Dispatcher** deduplicates discovered URLs against the DB and queues new ones for crawling
- **Crawler** fetches pages, saves raw HTML to MinIO, and pushes outgoing links back to the dispatcher

Redis carries two channels: `crawl_stream` (a Redis Stream with consumer groups, crawler → dispatcher → crawler) and `unprocessed_links` (a plain list, crawler → dispatcher).

Admin interfaces: **Adminer** at `localhost:8080` (PostgreSQL), **MinIO console** at `localhost:9001`.

## Usage

Create a `.env` file in the project root (it is gitignored) with the following variables:

```env
DB_HOST=db
DB_PORT=5432
DB_NAME=scraped_data
DB_USER=
DB_PASSWORD=

REDIS_HOST=queue
REDIS_PORT=6379
REDIS_CRAWL_STREAM=crawl_stream
REDIS_CRAWL_GROUP=crawlers
REDIS_LINKS_QUEUE=unprocessed_links

MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
MINIO_BUCKET=raw-html
```

### Quick start (run.sh)

`run.sh` wraps the manual steps below into one command:

```bash
./run.sh --help
./run.sh --log-level DEBUG --logs --seed http://books.toscrape.com
```

- `-l, --log-level LEVEL` override `LOG_LEVEL` for this run only, without editing `.env`
- `-s, --seed URL [URL ...]` seed one or more starting URLs once the stack is up
- `--logs` / `--no-logs` toggle the on-the-fly log capture described below (off by default)

Note: on a first-ever startup (empty volumes), `docker compose up -d` can take a while. `run.sh`'s `--seed` retry loop only waits ~10 seconds for the dispatcher and might time out on first startup. If that happens, just re-run `./run.sh --seed <url>` once `docker compose ps` shows everything healthy.

### Manual Start

Start all services with:

```bash
docker compose up -d
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
docker compose down [-v --remove-orphans]
docker compose build --no-cache
docker compose up -d
```

Seed one or more starting URLs (bare hostnames are accepted):

```bash
docker compose exec dispatcher python seed.py http://books.toscrape.com
```

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

Subclass `ScraperWorker` and override either or both hooks, then point `crawler/main.py` at your subclass:

- `_process_html(html: str) -> str`: transform HTML before it is saved to MinIO
- `_filter_urls(urls: set[str]) -> set[str]`: restrict which discovered URLs are forwarded to the dispatcher

`WikiScraper` is the current implementation; it filters crawling to a single domain.

## License

[GNU GPLv3](LICENSE)
