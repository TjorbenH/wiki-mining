# wiki-mining

A distributed web crawler that scrapes HTML, stores it in object storage, and builds a link graph across pages. Designed for wiki-style sites where crawling should stay within a single domain.

## Architecture

Two Python services coordinate over Redis and persist to PostgreSQL and MinIO:

- **Dispatcher** deduplicates discovered URLs against the DB and queues new ones for crawling
- **Crawler** fetches pages, saves raw HTML to MinIO, and pushes outgoing links back to the dispatcher

Redis carries two channels: `crawl_stream` (a Redis Stream with consumer groups, crawler → dispatcher → crawler) and `unprocessed_links` (a plain list, crawler → dispatcher).

Admin interfaces: **Adminer** at `localhost:8080` (PostgreSQL), **MinIO console** at `localhost:9001`.

## Setup

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

Then start all services:

```bash
docker compose up -d
```

After any code change:

```bash
docker compose down -v --remove-orphans
docker compose build --no-cache
docker compose up -d
```

## Usage

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

## Extending the crawler

Subclass `ScraperWorker` and override either or both hooks, then point `crawler/main.py` at your subclass:

- `_process_html(html: str) -> str`: transform HTML before it is saved to MinIO
- `_filter_urls(urls: set[str]) -> set[str]`: restrict which discovered URLs are forwarded to the dispatcher

`WikiScraper` is the current implementation; it filters crawling to a single domain.
