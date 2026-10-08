# Shared helpers for backup.sh and restore.sh - sourced, not executed.
# shellcheck shell=bash

RCLONE_IMAGE="rclone/rclone:1.68.2"
ALPINE_IMAGE="alpine:3.20"

# Named volumes from docker-compose.yaml that a backup snapshots.
VOLUMES=(db-data seaweedfs-data queue-data)

# .env keys the restored data depends on
DATA_ENV_KEYS=(DB_NAME DB_USER DB_PASSWORD MONITOR_DB_USER MONITOR_DB_PASSWORD S3_BUCKET
               REDIS_CRAWL_STREAM REDIS_CRAWL_GROUP REDIS_DISPATCHER_STREAM REDIS_DISPATCHER_GROUP)

# SeaweedFS runs in no-auth mode, so any access keys are accepted
S3_FLAGS=(--s3-provider Other --s3-endpoint http://seaweedfs:8333 --s3-access-key-id x --s3-secret-access-key x)

# Snapshot of the crawl state written to final-counts.txt by backup.sh and compared again by restore.sh.
COUNTS_SQL="SELECT scraping_status, count(*) FROM RawData GROUP BY 1;
SELECT count(*) AS links FROM Links;
SELECT count(DISTINCT storage_key) AS html_objects FROM RawData WHERE scraping_status='done';"

info() { echo "==> $*"; }
warn() { echo "==> WARNING: $*" >&2; }
die()  { echo "==> ERROR: $*" >&2; exit 1; }

# Reads KEY from an env file without sourcing it
env_get() {
    local key=$1 file=${2:-.env}
    sed -n "s/^${key}=//p" "$file" | tail -n 1 | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/"
}

# Compose project name
compose_project() {
    local name
    name=$(docker compose config 2> /dev/null | sed -n 's/^name: //p')
    [[ -n $name ]] || die "'docker compose config' failed - is docker running?"
    echo "$name"
}

# Sets PROJECT and the DB/bucket names from .env
load_config() {
    PROJECT=$(compose_project)
    DB_USER=$(env_get DB_USER)
    DB_NAME=$(env_get DB_NAME)
    S3_BUCKET=$(env_get S3_BUCKET)
}

volume_name()   { echo "${PROJECT}_$1"; }
volume_exists() { docker volume inspect "$(volume_name "$1")" > /dev/null 2>&1; }

db_counts() { docker compose exec -T db psql -U "$DB_USER" "$DB_NAME" -c "$COUNTS_SQL"; }
db_scalar() { docker compose exec -T db psql -U "$DB_USER" -d "$DB_NAME" -tAc "$1"; }

# Distinct storage keys of scraped pages: how many objects the bucket should hold
expected_html_objects() {
    db_scalar "SELECT count(DISTINCT storage_key) FROM RawData WHERE scraping_status='done';"
}

s3_object_count() {
    docker run --rm --network "${PROJECT}_default" "$RCLONE_IMAGE" \
        lsf -R --files-only ":s3:$S3_BUCKET" "${S3_FLAGS[@]}" 2> /dev/null | wc -l
}
