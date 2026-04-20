#!/usr/bin/env bash
#
# cleanup_scraped_data.sh
#
# Deletes scraped data ONLY after verifying it has been safely archived.
# Safety guarantees:
#   1. Checkpoint + manifest must exist and be non-empty
#   2. Archive file referenced by checkpoint must exist with matching SHA256
#   3. Every file to be deleted must have a matching SHA256 in the manifest
#   4. No file newer than the checkpoint's latest_data_mtime is ever deleted
#      (protects data created after the archive was made)
#   5. Runs in dry-run mode by default — use --execute to actually delete
#
# Usage:
#   ./scripts/cleanup_scraped_data.sh [--data-dir DIR] [--archive-dir DIR] [--execute]
#
# Without --execute, prints what would be deleted without deleting anything.
# With --execute, performs the deletion after all safety checks pass.
#
# Requirements: sha256sum, jq (for reading checkpoint JSON)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${REPO_ROOT}/core/scraper_data_mainnet"
ARCHIVE_DIR="${REPO_ROOT}/archives"
DRY_RUN=true
FORCE=false

# ── Parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir)    DATA_DIR="$2";    shift 2 ;;
        --archive-dir) ARCHIVE_DIR="$2"; shift 2 ;;
        --execute)     DRY_RUN=false;    shift ;;
        --force)       FORCE=true;       shift ;;
        -h|--help)
            echo "Usage: $0 [--data-dir DIR] [--archive-dir DIR] [--execute] [--force]"
            echo ""
            echo "  --execute   Actually delete files (default: dry-run)"
            echo "  --force     Skip archive existence check (use after moving archive to cold storage)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Resolve to absolute paths ────────────────────────────────────────────────
DATA_DIR="$(cd "$DATA_DIR" && pwd)"
if [[ ! -d "$ARCHIVE_DIR" ]]; then
    echo "ERROR: archive directory not found: $ARCHIVE_DIR"
    echo "       Run archive_scraped_data.sh first."
    exit 1
fi
ARCHIVE_DIR="$(cd "$ARCHIVE_DIR" && pwd)"

CHECKPOINT_FILE="${ARCHIVE_DIR}/archive.checkpoint.json"
MANIFEST_FILE="${ARCHIVE_DIR}/archive.manifest.tsv"

# ── Preflight checks ─────────────────────────────────────────────────────────
if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required (https://jqlang.github.io/jq/)"
    exit 1
fi

# Warn if scraper appears to be running (new files being created)
if pgrep -f "MainnetOfferScraper\|bisq_scraper\|runScraper" >/dev/null 2>&1; then
    echo "WARNING: It looks like the scraper is currently running."
    echo "  Files being written right now will be skipped (they're newer than the archive),"
    echo "  but you should stop the scraper first to avoid a race condition."
    if [[ "$DRY_RUN" == "false" ]]; then
        echo "  Press Enter to continue anyway, or Ctrl+C to abort..."
        read -r
    fi
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "ERROR: data directory not found: $DATA_DIR"
    exit 1
fi

if [[ ! -f "$CHECKPOINT_FILE" ]]; then
    echo "ERROR: checkpoint file not found: $CHECKPOINT_FILE"
    echo "       Run archive_scraped_data.sh first to create an archive."
    exit 1
fi

if [[ ! -f "$MANIFEST_FILE" ]]; then
    echo "ERROR: manifest file not found: $MANIFEST_FILE"
    echo "       The manifest should have been created alongside the checkpoint."
    exit 1
fi

if [[ ! -s "$MANIFEST_FILE" ]]; then
    echo "ERROR: manifest file is empty: $MANIFEST_FILE"
    exit 1
fi

# ── Parse checkpoint ─────────────────────────────────────────────────────────
echo "Reading checkpoint: $CHECKPOINT_FILE"
ARCHIVE_NAME=$(jq -r '.archive_file' "$CHECKPOINT_FILE")
ARCHIVE_SHA256=$(jq -r '.archive_sha256' "$CHECKPOINT_FILE")
LATEST_MTIME=$(jq -r '.latest_data_mtime_epoch' "$CHECKPOINT_FILE")
LATEST_MTIME_ISO=$(jq -r '.latest_data_mtime_iso' "$CHECKPOINT_FILE")
MANIFESTED_TOTAL=$(jq -r '.total_files' "$CHECKPOINT_FILE")
LAST_ARCHIVED=$(jq -r '.last_archived_timestamp' "$CHECKPOINT_FILE")

if [[ "$ARCHIVE_NAME" == "null" || -z "$ARCHIVE_NAME" ]]; then
    echo "ERROR: checkpoint has no archive reference (was the data empty when archived?)"
    exit 1
fi

if [[ "$LATEST_MTIME" == "null" || "$LATEST_MTIME" == "0" ]]; then
    echo "ERROR: checkpoint has no valid mtime"
    exit 1
fi

echo "  Archive:          $ARCHIVE_NAME"
echo "  Last archived:    $LAST_ARCHIVED"
echo "  Latest data time: $LATEST_MTIME_ISO (epoch: $LATEST_MTIME)"
echo "  Manifested files: $MANIFESTED_TOTAL"

# ── Verify archive exists and has correct checksum ───────────────────────────
ARCHIVE_PATH="${ARCHIVE_DIR}/${ARCHIVE_NAME}"

if [[ "$FORCE" == "true" ]]; then
    echo "WARNING: --force mode — skipping archive existence and checksum verification."
else
    if [[ ! -f "$ARCHIVE_PATH" ]]; then
        echo "ERROR: archive file not found: $ARCHIVE_PATH"
        echo "       Cannot verify data is safely archived."
        echo "       If the archive was moved to cold storage, use --force."
        exit 1
    fi

    echo "Verifying archive checksum..."
    CURRENT_SHA256=$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')

    if [[ "$CURRENT_SHA256" != "$ARCHIVE_SHA256" ]]; then
        echo "ERROR: archive checksum mismatch!"
        echo "       Expected: $ARCHIVE_SHA256"
        echo "       Got:      $CURRENT_SHA256"
        echo "       The archive may have been corrupted. Do NOT delete data."
        exit 1
    fi
    echo "  Archive checksum OK."
fi

# ── Build deletion plan ─────────────────────────────────────────────────────
echo ""
echo "Scanning data directory for files eligible for deletion..."

# Build a lookup: relative_path -> expected_sha256 from the manifest
declare -A MANIFEST_HASHES
declare -A MANIFEST_MTIMES
while IFS=$'\t' read -r HASH MTIME SIZE REL_PATH; do
    # Strip leading ./ if present
    CLEAN_PATH="${REL_PATH#./}"
    MANIFEST_HASHES["$CLEAN_PATH"]="$HASH"
    MANIFEST_MTIMES["$CLEAN_PATH"]="$MTIME"
done < "$MANIFEST_FILE"

echo "  Manifest contains ${#MANIFEST_HASHES[@]} file entries."

# Scan data directory and classify each file
SAFE_TO_DELETE=0
SKIP_NEWER=0
SKIP_NOT_IN_MANIFEST=0
SKIP_HASH_MISMATCH=0
SKIP_PERMISSION=0

declare -a DELETE_LIST    # files confirmed safe to delete
declare -a SKIP_LIST_NEW  # files newer than checkpoint (not safe)

(cd "$DATA_DIR"
    while IFS= read -r -d '' file; do
        # Get relative path
        REL="${file#./}"

        # Edge case: file is newer than the latest mtime in the checkpoint
        FILE_MTIME=$(stat -c %Y "$file")
        if [[ "$FILE_MTIME" -gt "$LATEST_MTIME" ]]; then
            SKIP_NEWER=$((SKIP_NEWER + 1))
            SKIP_LIST_NEW+=("$REL")
            continue
        fi

        # Edge case: file not in the manifest at all
        if [[ -z "${MANIFEST_HASHES[$REL]+x}" ]]; then
            SKIP_NOT_IN_MANIFEST=$((SKIP_NOT_IN_MANIFEST + 1))
            continue
        fi

        # Edge case: file is not readable
        if [[ ! -r "$file" ]]; then
            SKIP_PERMISSION=$((SKIP_PERMISSION + 1))
            continue
        fi

        # Verify SHA256 matches the manifest
        FILE_HASH=$(sha256sum "$file" | awk '{print $1}')
        EXPECTED_HASH="${MANIFEST_HASHES[$REL]}"

        if [[ "$FILE_HASH" != "$EXPECTED_HASH" ]]; then
            SKIP_HASH_MISMATCH=$((SKIP_HASH_MISMATCH + 1))
            echo "  WARNING: hash mismatch for '$REL'"
            echo "    expected: $EXPECTED_HASH"
            echo "    current:  $FILE_HASH"
            echo "    File has been modified since archiving — skipping."
            continue
        fi

        # All checks passed — safe to delete
        DELETE_LIST+=("$REL")
        SAFE_TO_DELETE=$((SAFE_TO_DELETE + 1))

    done < <(find . -type f -print0)
)

# ── Print summary ────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  DELETION PLAN SUMMARY"
echo "═══════════════════════════════════════════════════════════"
echo "  Files safe to delete:       $SAFE_TO_DELETE"
echo "  Skipped (newer than archive): $SKIP_NEWER"
echo "  Skipped (not in manifest):  $SKIP_NOT_IN_MANIFEST"
echo "  Skipped (hash mismatch):    $SKIP_HASH_MISMATCH"
echo "  Skipped (permission denied): $SKIP_PERMISSION"
echo "═══════════════════════════════════════════════════════════"

if [[ "$SKIP_NEWER" -gt 0 ]]; then
    echo ""
    echo "  Files newer than the archive (will NOT be deleted):"
    for f in "${SKIP_LIST_NEW[@]}"; do
        echo "    - $f"
    done
fi

if [[ "$SKIP_HASH_MISMATCH" -gt 0 ]]; then
    echo ""
    echo "  WARNING: Some files have been modified since archiving."
    echo "  Re-run archive_scraped_data.sh to capture the latest data first."
fi

if [[ "$SAFE_TO_DELETE" -eq 0 ]]; then
    echo ""
    echo "No files are eligible for deletion."
    if [[ "$SKIP_NEWER" -gt 0 ]]; then
        echo "All files are newer than the last archive. Re-archive first."
    fi
    exit 0
fi

# ── Compute space to be freed ────────────────────────────────────────────────
FREED_BYTES=0
for f in "${DELETE_LIST[@]}"; do
    SIZE=$(stat -c %s "${DATA_DIR}/${f}")
    FREED_BYTES=$((FREED_BYTES + SIZE))
done
echo ""
echo "  Space to be freed: $(numfmt --to=iec "$FREED_BYTES")"

# ── Dry-run: just report and exit ────────────────────────────────────────────
if [[ "$DRY_RUN" == "true" ]]; then
    echo ""
    echo "  *** DRY RUN — no files will be deleted. ***"
    echo "  Run with --execute to perform the deletion."
    echo ""
    echo "  First 20 files that would be deleted:"
    for f in "${DELETE_LIST[@]:0:20}"; do
        echo "    - $f"
    done
    if [[ "${#DELETE_LIST[@]}" -gt 20 ]]; then
        echo "    ... and $((${#DELETE_LIST[@]} - 20)) more"
    fi
    exit 0
fi

# ── Execute: delete files ────────────────────────────────────────────────────
echo ""
echo "Are you sure you want to delete $SAFE_TO_DELETE files? (Ctrl+C to abort)"
sleep 3

DELETED=0
FAILED=0
for f in "${DELETE_LIST[@]}"; do
    FULL_PATH="${DATA_DIR}/${f}"
    if rm -f "$FULL_PATH"; then
        DELETED=$((DELETED + 1))
    else
        echo "  ERROR: failed to delete '$f'"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "Deletion complete."
echo "  Deleted: $DELETED"
echo "  Failed:  $FAILED"

# ── Remove empty directories left behind ─────────────────────────────────────
echo "Removing empty directories..."
(cd "$DATA_DIR"
    find . -type d -empty -not -path "." -delete 2>/dev/null || true
)
echo "Done."
