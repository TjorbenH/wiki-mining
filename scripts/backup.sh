#!/usr/bin/env bash
# Backs up the crawl data: a portable export (pg_dump + every HTML object as a plain file) plus raw snapshots of all volumes, with SHA-256 checksums. Restore with scripts/restore.sh.
# Usage: ./scripts/backup.sh [options] [DEST_DIR]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/backup-common.sh
source "$SCRIPT_DIR/backup-common.sh"
cd "$SCRIPT_DIR/.."

COPY_ENV=true
ARCHIVE=false
DEST_DIR="$HOME"

usage() {
    cat << 'EOF'
Usage: ./scripts/backup.sh [options] [DEST_DIR]

Backs up the crawl into DEST_DIR/wiki-mining-backup-<date>-<time>/ (DEST_DIR defaults to $HOME):

  scraped_data.dump    pg_dump of the database (custom format, load with pg_restore)
  html/                every object in the S3 bucket as a plain file, named by RawData.storage_key
  <volume>.tar.gz      raw snapshots of the db-data, seaweedfs-data and queue-data volumes
  final-counts.txt     row counts at backup time, checked again by restore.sh
  .env, logs/, git-commit.txt, SHA256SUMS

The crawler and dispatcher are stopped during the export, the whole stack during the volume
snapshots. Whatever was running beforehand is started again afterwards, also if the backup fails.

Options:
      --env         Include .env in the backup (default).
      --no-env      Leave .env out.
      --folder      Keep the backup as a folder (default). For a backup that stays on this machine.
      --archive     Pack it into one .tar.gz plus a .sha256 file and remove the folder.
                    For a backup you're going to transfer somewhere else.
  -h, --help        Show this help and exit.

Examples:
  ./scripts/backup.sh                             # folder in $HOME, including .env
  ./scripts/backup.sh --archive --no-env /mnt/backups
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)     COPY_ENV=true;  shift ;;
        --no-env)  COPY_ENV=false; shift ;;
        --folder)  ARCHIVE=false;  shift ;;
        --archive) ARCHIVE=true;   shift ;;
        -h|--help) usage; exit 0 ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            DEST_DIR="$1"
            shift
            ;;
    esac
done

[[ -f .env ]] || die "no .env in $PWD"
load_config
for v in db-data seaweedfs-data; do
    volume_exists "$v" || die "volume $(volume_name "$v") not found - nothing to back up"
done

[[ -d $DEST_DIR && -w $DEST_DIR ]] || die "$DEST_DIR is not a writable directory"
DEST_DIR=$(cd "$DEST_DIR" && pwd)
NAME="wiki-mining-backup-$(date +%F-%H%M%S)"
OUT="$DEST_DIR/$NAME"
[[ ! -e $OUT && ! -e $OUT.tar.gz ]] || die "$OUT already exists"

volume_kb() { docker run --rm -v "$(volume_name "$1"):/v:ro" "$ALPINE_IMAGE" du -sk /v | cut -f1; }
db_kb=$(volume_kb db-data)
s3_kb=$(volume_kb seaweedfs-data)
need_kb=$(( 2 * db_kb + 2 * s3_kb ))
if $ARCHIVE; then need_kb=$(( 2 * need_kb )); fi
avail_kb=$(df -Pk "$DEST_DIR" | awk 'NR == 2 { print $4 }')
(( avail_kb > need_kb )) \
    || die "not enough space in $DEST_DIR: need about $(( need_kb / 1024 )) MiB, $(( avail_kb / 1024 )) MiB free"
info "Backing up compose project '$PROJECT' to $OUT ($(( avail_kb / 1024 )) MiB free, needs about $(( need_kb / 1024 )) MiB)"

# --- stop writers, remembering what to start again ---

# grep drops the blank line compose prints when nothing is running
mapfile -t RUNNING < <(docker compose --profile crawler ps --services --status running | grep .)
restart_services() {
    if (( ${#RUNNING[@]} )); then
        info "Starting the services that were running before: ${RUNNING[*]}"
        docker compose --profile crawler start "${RUNNING[@]}"
    fi
}
trap restart_services EXIT

info "Stopping crawler and dispatcher so nothing writes during the export..."
docker compose --profile crawler stop crawler dispatcher
docker compose up -d --wait db seaweedfs

# --- portable export ---

mkdir -p "$OUT/html"

info "Recording row counts in final-counts.txt..."
db_counts | tee "$OUT/final-counts.txt"

info "Dumping the database to scraped_data.dump..."
docker compose exec -T db pg_dump -U "$DB_USER" -Fc "$DB_NAME" > "$OUT/scraped_data.dump"
docker compose exec -T db pg_restore --list < "$OUT/scraped_data.dump" > /dev/null \
    || die "pg_restore can't read the dump"

info "Exporting bucket '$S3_BUCKET' to html/..."
docker run --rm --network "${PROJECT}_default" --user "$(id -u):$(id -g)" -v "$OUT/html:/out:z" "$RCLONE_IMAGE" \
    copy ":s3:$S3_BUCKET" /out "${S3_FLAGS[@]}" --stats 15s --stats-one-line --stats-log-level NOTICE

expected=$(expected_html_objects)
got=$(find "$OUT/html" -type f | wc -l)
(( got >= expected )) || die "html/ has $got files but the database has $expected scraped pages"
if (( got > expected )); then
    # an upload whose mark_done never landed (crawler died in between) leaves an object without a 'done' row
    warn "html/ has $got files, $(( got - expected )) more than the $expected scraped pages in the database (harmless)"
else
    info "html/ has all $got scraped pages"
fi

# --- raw volume snapshots ---

info "Stopping the whole stack for consistent volume snapshots..."
docker compose --profile crawler stop
for v in "${VOLUMES[@]}"; do
    if ! volume_exists "$v"; then
        warn "volume $(volume_name "$v") not found, skipping it"
        continue
    fi
    info "Snapshotting $(volume_name "$v") to $v.tar.gz..."
    # streamed over stdout instead of a bind mount: the file belongs to the caller and SELinux stays out of it
    docker run --rm -v "$(volume_name "$v"):/v:ro" "$ALPINE_IMAGE" tar czf - -C /v . > "$OUT/$v.tar.gz"
done

# all data is captured, so the stack doesn't need to wait for checksums and packing
restart_services
trap - EXIT

# --- metadata + checksums ---

if $COPY_ENV; then
    cp .env "$OUT/.env"
fi
if [[ -d logs ]]; then
    cp -r logs "$OUT/logs"
fi
if commit=$(git rev-parse HEAD 2> /dev/null); then
    [[ -z $(git status --porcelain --untracked-files=no) ]] || commit+=" (plus uncommitted changes)"
else
    commit="unknown"
fi
echo "$commit" > "$OUT/git-commit.txt"

info "Writing SHA256SUMS..."
(cd "$OUT" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS)

# --- optional packing ---

if $ARCHIVE; then
    info "Packing into $NAME.tar.gz..."
    tar czf "$OUT.tar.gz" -C "$DEST_DIR" "$NAME"
    (cd "$DEST_DIR" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
    rm -rf "$OUT"
    info "Done: $OUT.tar.gz ($(du -sh "$OUT.tar.gz" | cut -f1)) + $NAME.tar.gz.sha256"
    echo "    Fetch both with:  rsync -avP --partial '$(id -un)@$(hostname):$OUT.tar.gz*' ."
    echo "    Restore with:     ./scripts/restore.sh $NAME.tar.gz"
else
    info "Done: $OUT ($(du -sh "$OUT" | cut -f1))"
    echo "    Restore with:     ./scripts/restore.sh $OUT"
fi
