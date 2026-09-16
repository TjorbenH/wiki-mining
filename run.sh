#!/usr/bin/env bash
# Convenience wrapper around docker compose for common dev/test workflows.
# Usage: ./run.sh [options]
set -euo pipefail

# Resolve the repo root regardless of the caller's working directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_CAPTURE=false
SEED_URLS=()

usage() {
    cat << 'EOF'
Usage: ./run.sh [options]

Starts the crawler stack via docker compose, with a few convenience flags. The
crawler service is NOT started by 'docker compose up -d' (it needs at least one
domain whitelisted first via seed.py) - it only starts if --seed is given here,
right after seeding succeeds. Without --seed, everything else comes up and you
seed/start the crawler yourself when ready.

Options:
  -l, --log-level LEVEL   Override LOG_LEVEL for dispatcher/crawler (DEBUG, INFO,
                          WARNING, ERROR). Default: whatever is set in .env.
  -s, --seed URL [...]    Seed one or more starting URLs, then start the crawler.
                          Accepts multiple URLs; must be the last flag given.
      --logs              Enable on-the-fly log capture to logs/<service>-<ts>.log
                          (background 'docker compose logs -f', split per service).
      --no-logs           Disable log capture (default).
  -h, --help              Show this help and exit.

Examples:
  ./run.sh --log-level DEBUG --logs --seed http://books.toscrape.com
  ./run.sh                                    # bring up everything but the crawler
  docker compose exec dispatcher python seed.py http://books.toscrape.com
  docker compose up -d crawler
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -s|--seed)
            shift
            while [[ $# -gt 0 && "$1" != -* ]]; do
                SEED_URLS+=("$1")
                shift
            done
            ;;
        --logs)
            LOG_CAPTURE=true
            shift
            ;;
        --no-logs)
            LOG_CAPTURE=false
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

echo "==> Starting services..."
if [[ -n "${LOG_LEVEL:-}" ]]; then
    echo "    LOG_LEVEL override: $LOG_LEVEL"
    export LOG_LEVEL
fi
docker compose up -d

if $LOG_CAPTURE; then
    mkdir -p logs
    ts=$(date +%Y%m%d-%H%M%S)
    docker compose logs -f --no-color dispatcher > "logs/dispatcher-$ts.log" &
    dispatcher_pid=$!
    echo "$dispatcher_pid" > "logs/.run-$ts.pids"
    echo "==> Logging dispatcher to logs/dispatcher-$ts.log"
fi

if [[ ${#SEED_URLS[@]} -gt 0 ]]; then
    echo "==> Seeding ${#SEED_URLS[@]} URL(s)..."
    seeded=false
    for i in $(seq 1 10); do
        if docker compose exec -T dispatcher python seed.py "${SEED_URLS[@]}"; then
            seeded=true
            break
        fi
        echo "    dispatcher not ready yet, retrying ($i/10)..."
        sleep 1
    done
    if ! $seeded; then
        echo "==> ERROR: failed to seed URLs after 10 attempts." >&2
        exit 1
    fi

    echo "==> Seeding done, starting crawler..."
    docker compose up -d crawler

    if $LOG_CAPTURE; then
        docker compose logs -f --no-color crawler > "logs/crawler-$ts.log" &
        crawler_pid=$!
        echo "$dispatcher_pid $crawler_pid" > "logs/.run-$ts.pids"
        echo "==> Logging crawler to logs/crawler-$ts.log"
        echo "    Stop logging with: kill $dispatcher_pid $crawler_pid  (also saved in logs/.run-$ts.pids)"
    fi
else
    echo "==> No --seed given: crawler NOT started (it needs at least one whitelisted domain first)."
    echo "    Seed a domain, then start it:"
    echo "      docker compose exec dispatcher python seed.py <url1> <url2> ..."
    echo "      docker compose up -d crawler"
fi

echo "==> Done. 'docker compose logs -f' to watch, 'docker compose down' to stop."
