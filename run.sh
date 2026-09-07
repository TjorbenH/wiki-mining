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

Starts the crawler stack via docker compose, with a few convenience flags.

Options:
  -l, --log-level LEVEL   Override LOG_LEVEL for dispatcher/crawler (DEBUG, INFO,
                          WARNING, ERROR). Default: whatever is set in .env.
  -s, --seed URL [...]    Seed one or more starting URLs after startup. Accepts
                          multiple URLs; must be the last flag given.
      --logs              Enable on-the-fly log capture to logs/<service>-<ts>.log
                          (background 'docker compose logs -f', split per service).
      --no-logs           Disable log capture (default).
  -h, --help              Show this help and exit.

Examples:
  ./run.sh --log-level DEBUG --logs
  ./run.sh --seed http://books.toscrape.com
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
    docker compose logs -f --no-color crawler > "logs/crawler-$ts.log" &
    crawler_pid=$!
    echo "$dispatcher_pid $crawler_pid" > "logs/.run-$ts.pids"
    echo "==> Logging to logs/dispatcher-$ts.log and logs/crawler-$ts.log"
    echo "    Stop with: kill $dispatcher_pid $crawler_pid  (also saved in logs/.run-$ts.pids)"
fi

sleep 5

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
fi

echo "==> Done. 'docker compose logs -f' to watch, 'docker compose down' to stop."
