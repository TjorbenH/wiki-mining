#!/usr/bin/env bash
# Verifies a backup made by scripts/backup.sh (folder or .tar.gz) and loads it into this machine's stack, replacing the current database, S3 bucket and Redis queue.
# Usage: ./scripts/restore.sh [options] BACKUP
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/backup-common.sh
source "$SCRIPT_DIR/backup-common.sh"
cd "$SCRIPT_DIR/.."

IMPORT_ENV=false
ASSUME_YES=false
SOURCE=""

usage() {
    cat << 'EOF'
Usage: ./scripts/restore.sh [options] BACKUP

BACKUP is a backup folder or a .tar.gz made by scripts/backup.sh (a .tar.gz needs its .sha256 file
next to it). The backup is verified first. Then all containers and volumes of this compose project
are DELETED and replaced by the backup's volume snapshots, and the restored row and object counts
are checked against the backup. The crawler is not started.

Without --env, your .env must match the backup's in the settings the data depends on (DB names and
passwords, S3 bucket, Redis stream/group names). The restore refuses to run otherwise.

Options:
      --env         Replace .env with the backup's (your current one is saved as .env.bak-<date>).
      --no-env      Keep your current .env (default).
  -y, --yes         Don't ask for confirmation before deleting the current data.
  -h, --help        Show this help and exit.

Examples:
  ./scripts/restore.sh ~/wiki-mining-backup-2026-10-08
  ./scripts/restore.sh --env ~/wiki-mining-backup-2026-10-08-153012.tar.gz
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --env)     IMPORT_ENV=true;  shift ;;
        --no-env)  IMPORT_ENV=false; shift ;;
        -y|--yes)  ASSUME_YES=true;  shift ;;
        -h|--help) usage; exit 0 ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            [[ -z $SOURCE ]] || die "only one BACKUP allowed"
            SOURCE="$1"
            shift
            ;;
    esac
done
[[ -n $SOURCE ]] || { usage >&2; exit 1; }
[[ -e $SOURCE ]] || die "$SOURCE not found"
SOURCE=$(realpath "$SOURCE")

STAGE=""
cleanup() {
    if [[ -n $STAGE ]]; then
        chmod -R u+w "$STAGE"   # a backup folder made read-only before packing would block rm
        rm -rf "$STAGE"
    fi
}
trap cleanup EXIT

# --- verify: nothing is touched before this passes ---

if [[ -d $SOURCE ]]; then
    BACKUP=$SOURCE
    [[ -f $BACKUP/SHA256SUMS ]] || die "no SHA256SUMS in $BACKUP - not a backup folder?"
    info "Verifying every file against SHA256SUMS (reads all of html/ too)..."
    (cd "$BACKUP" && sha256sum -c --quiet SHA256SUMS) || die "checksum mismatch - the backup is damaged"
elif [[ $SOURCE == *.tar.gz ]]; then
    [[ -f $SOURCE.sha256 ]] || die "$SOURCE.sha256 not found - can't verify the archive"
    info "Verifying the archive against $(basename "$SOURCE").sha256..."
    (cd "$(dirname "$SOURCE")" && sha256sum -c --quiet "$(basename "$SOURCE").sha256") \
        || die "checksum mismatch - the archive is damaged"

    # next to the archive rather than /tmp
    stage_parent=$(dirname "$SOURCE")
    [[ -w $stage_parent ]] || stage_parent=${TMPDIR:-/var/tmp}
    STAGE=$(mktemp -d -p "$stage_parent" .restore-XXXXXX)
    info "Unpacking everything except html/ to $STAGE..."
    tar xzf "$SOURCE" -C "$STAGE" --exclude='*/html/*' --exclude='*/html'
    BACKUP=$(find "$STAGE" -mindepth 1 -maxdepth 1 -type d)
    [[ -n $BACKUP && $(wc -l <<< "$BACKUP") -eq 1 && -f $BACKUP/SHA256SUMS ]] \
        || die "the archive doesn't hold a single backup folder with SHA256SUMS"
    (cd "$BACKUP" && sha256sum -c --quiet --ignore-missing SHA256SUMS) || die "checksum mismatch inside the archive"
else
    die "$SOURCE is neither a backup folder nor a .tar.gz"
fi
info "Backup verified"

for v in db-data seaweedfs-data; do
    [[ -f $BACKUP/$v.tar.gz ]] || die "$v.tar.gz missing from the backup"
done
if [[ ! -f $BACKUP/queue-data.tar.gz ]]; then
    warn "no queue-data.tar.gz in this backup (made by hand?) - Redis starts with an empty queue"
fi

if $IMPORT_ENV; then
    [[ -f $BACKUP/.env ]] || die "--env given, but the backup has no .env (made with --no-env?)"
elif [[ ! -f .env ]]; then
    die "no .env in $PWD - rerun with --env to use the backup's"
elif [[ -f $BACKUP/.env ]]; then
    mismatched=()
    for key in "${DATA_ENV_KEYS[@]}"; do
        [[ $(env_get "$key") == "$(env_get "$key" "$BACKUP/.env")" ]] || mismatched+=("$key")
    done
    (( ${#mismatched[@]} == 0 )) \
        || die "your .env differs from the backup's in: ${mismatched[*]}. The restored data depends on these" \
               "(the DB passwords live inside the db volume) - rerun with --env, or align .env by hand."
else
    warn "the backup has no .env, so I can't check that your DB_*, S3_BUCKET and REDIS_* settings match the data"
fi

# --- confirm, then replace ---

project=$(compose_project)
echo
echo "This DELETES all containers and volumes of compose project '$project' (database, S3 bucket,"
echo "Redis queue) and replaces them with the backup from $SOURCE."
if ! $ASSUME_YES; then
    read -r -p "Type 'yes' to continue: " answer || true
    [[ ${answer:-} == yes ]] || die "aborted, nothing was changed"
fi

if $IMPORT_ENV; then
    if [[ -f .env ]]; then
        bak=".env.bak-$(date +%F-%H%M%S)"
        cp .env "$bak"
        info "Saved your current .env as $bak"
    fi
    cp "$BACKUP/.env" .env
    chmod u+w .env   # cp keeps the mode of a backup made read-only
fi
load_config

info "Removing the current stack and its volumes..."
docker compose --profile crawler down -v --remove-orphans
info "Creating fresh containers and empty volumes..."
docker compose create

for v in "${VOLUMES[@]}"; do
    [[ -f $BACKUP/$v.tar.gz ]] || continue
    info "Loading $v.tar.gz into $(volume_name "$v")..."
    docker run --rm -i -v "$(volume_name "$v"):/v" "$ALPINE_IMAGE" tar xzf - -C /v < "$BACKUP/$v.tar.gz"
done

# --- check the restored data before anything else starts working on it ---

info "Starting db and seaweedfs to check the restored data..."
docker compose up -d --wait db seaweedfs

ok=true
if [[ -f $BACKUP/final-counts.txt ]]; then
    # sorted: GROUP BY doesn't guarantee row order
    if counts_diff=$(diff <(sort "$BACKUP/final-counts.txt") <(db_counts | sort)); then
        info "Row counts match final-counts.txt:"
        cat "$BACKUP/final-counts.txt"
    else
        warn "row counts differ from final-counts.txt (< backup, > restored):"
        echo "$counts_diff" >&2
        ok=false
    fi
else
    warn "no final-counts.txt in the backup, skipping the row count check"
fi

expected=$(expected_html_objects)
got=$(s3_object_count) || die "couldn't list bucket '$S3_BUCKET'"
if (( got >= expected )); then
    info "Bucket '$S3_BUCKET' has $got objects for $expected scraped pages"
else
    warn "bucket '$S3_BUCKET' has only $got objects for $expected scraped pages"
    ok=false
fi

$ok || die "the restored data doesn't match the backup. Only db and seaweedfs are running, so you can inspect it."

info "Starting the remaining services (not the crawler)..."
docker compose up -d
info "Restore complete. If the backup was taken mid-crawl, resume it with: docker compose up -d crawler"
