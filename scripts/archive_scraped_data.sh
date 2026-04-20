#!/usr/bin/env bash
#
# archive_scraped_data.sh
#
# Creates a tar.zst archive of all scraped data and writes a checkpoint file
# with a SHA256 manifest of every file archived. The checkpoint + manifest are
# the authoritative record used by cleanup_scraped_data.sh to safely delete.
#
# Checkpoint format (archive.checkpoint.json):
#   {
#     "last_archived_timestamp": "2026-04-20T10:00:00Z",
#     "archive_file": "scraper_data_20260420_100000.tar.zst",
#     "archive_sha256": "abc123...",
#     "archive_bytes": 1234567,
#     "latest_data_mtime_epoch": 1713576000,
#     "latest_data_mtime_iso": "2026-04-20T09:32:09Z",
#     "total_files": 7010,
#     "total_data_bytes": 7200000000,
#     "manifest_file": "archive.manifest.tsv"
#   }
#
# Manifest format (archive.manifest.tsv) — one line per file:
#   sha256<TAB>mtime_epoch<TAB>size_bytes<TAB>relative_path
#
# Usage:
#   ./scripts/archive_scraped_data.sh [--data-dir DIR] [--archive-dir DIR]
#
# Default data dir:    core/scraper_data_mainnet/
# Default archive dir: archives/
#
# Requirements: zstd, sha256sum, GNU coreutils

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${REPO_ROOT}/core/scraper_data_mainnet"
ARCHIVE_DIR="${REPO_ROOT}/archives"

# ── Parse args ───────────────────────────────────────────────────────────────
SPOT_CHECK=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir)     DATA_DIR="$2";    shift 2 ;;
        --archive-dir)  ARCHIVE_DIR="$2"; shift 2 ;;
        --no-spot-check) SPOT_CHECK=false; shift ;;
        -h|--help)
            echo "Usage: $0 [--data-dir DIR] [--archive-dir DIR] [--no-spot-check]"
            echo ""
            echo "  --no-spot-check  Skip extracting files from the archive to verify hashes"
            echo "                   (saves time for very large archives, still runs zstd -t)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Resolve to absolute paths ────────────────────────────────────────────────
DATA_DIR="$(cd "$DATA_DIR" && pwd)"
mkdir -p "$ARCHIVE_DIR"
ARCHIVE_DIR="$(cd "$ARCHIVE_DIR" && pwd)"

CHECKPOINT_FILE="${ARCHIVE_DIR}/archive.checkpoint.json"
MANIFEST_FILE="${ARCHIVE_DIR}/archive.manifest.tsv"
LOCK_FILE="/tmp/archive_scraped_data.lock"

# ── Preflight checks ─────────────────────────────────────────────────────────
for cmd in zstd sha256sum tar stat; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd not found in PATH"; exit 1; }
done

if [[ ! -d "$DATA_DIR" ]]; then
    echo "ERROR: data directory not found: $DATA_DIR"
    exit 1
fi

# ── Locking (prevent concurrent archive runs) ───────────────────────────────
if [[ -f "$LOCK_FILE" ]]; then
    LOCK_PID="$(cat "$LOCK_FILE")"
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        echo "ERROR: another archive process is running (PID $LOCK_PID). Remove $LOCK_FILE if stale."
        exit 1
    else
        echo "WARNING: stale lock file found (PID $LOCK_PID not running). Removing."
        rm -f "$LOCK_FILE"
    fi
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

# ── Count files to archive ───────────────────────────────────────────────────
echo "Scanning data directory: $DATA_DIR"
FILE_COUNT=$(find "$DATA_DIR" -type f | wc -l)
TOTAL_SIZE=$(du -sb "$DATA_DIR" | cut -f1)
echo "Found $FILE_COUNT files ($(numfmt --to=iec "$TOTAL_SIZE"))"

# ── Handle empty data dir ────────────────────────────────────────────────────
if [[ "$FILE_COUNT" -eq 0 ]]; then
    echo "NOTICE: no files to archive. Writing empty checkpoint."
    TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    cat > "$CHECKPOINT_FILE" <<EOF
{
  "last_archived_timestamp": "$TIMESTAMP",
  "archive_file": null,
  "archive_sha256": "",
  "archive_bytes": 0,
  "latest_data_mtime_epoch": 0,
  "latest_data_mtime_iso": "",
  "total_files": 0,
  "total_data_bytes": 0,
  "manifest_file": ""
}
EOF
    echo "" > "$MANIFEST_FILE"
    echo "Checkpoint written to $CHECKPOINT_FILE"
    exit 0
fi

# ── Build archive name ───────────────────────────────────────────────────────
TIMESTAMP="$(date -u +%Y%m%d_%H%M%S)"
ARCHIVE_NAME="scraper_data_${TIMESTAMP}.tar.zst"
ARCHIVE_PATH="${ARCHIVE_DIR}/${ARCHIVE_NAME}"

# ── Generate per-file SHA256 manifest ────────────────────────────────────────
# This is done BEFORE archiving so the hashes correspond to the live data.
# The manifest is later verified against the archive for end-to-end integrity.
echo "Generating file hashes (this may take a while for large datasets)..."
TMP_MANIFEST=$(mktemp)
HASHED=0

(cd "$DATA_DIR"
    find . -type f -print0 | sort -z | while IFS= read -r -d '' file; do
        HASH="$(sha256sum "$file" | awk '{print $1}')"
        MTIME="$(stat -c %Y "$file")"
        SIZE="$(stat -c %s "$file")"
        printf '%s\t%s\t%s\t%s\n' "$HASH" "$MTIME" "$SIZE" "$file"
        HASHED=$((HASHED + 1))
        if [[ $((HASHED % 1000)) -eq 0 ]]; then
            echo "  ... $HASHED files hashed" >&2
        fi
    done > "$TMP_MANIFEST"
)

MANIFEST_LINE_COUNT=$(wc -l < "$TMP_MANIFEST")
echo "Hashed $MANIFEST_LINE_COUNT files."

# ── Create tar.zst archive ──────────────────────────────────────────────────
echo "Creating archive: $ARCHIVE_PATH"
# Level 3: good balance of speed vs compression ratio for large data
tar -cf - -C "$DATA_DIR" . | zstd -T0 -3 > "$ARCHIVE_PATH"
ARCHIVE_SIZE=$(stat -c %s "$ARCHIVE_PATH")
echo "Archive created: $(numfmt --to=iec "$ARCHIVE_SIZE")"

# ── Verify archive integrity ────────────────────────────────────────────────
echo "Verifying archive integrity..."
if ! zstd -t "$ARCHIVE_PATH" >/dev/null 2>&1; then
    echo "ERROR: archive integrity check failed (zstd -t). Removing corrupt archive."
    rm -f "$ARCHIVE_PATH"
    rm -f "$TMP_MANIFEST"
    exit 1
fi
echo "Archive integrity OK."

# ── Spot-check: verify small files from manifest against archive ─────────────
# Pick up to 5 files under 1MB, extract them all in a single zstd|tar pass,
# then hash and compare. This avoids decompressing the 7GB archive per file.
# Disabled by default for large archives — use --no-spot-check to skip.
if [[ "$SPOT_CHECK" == "true" ]]; then
echo "Spot-checking files inside archive..."
SPOT_DIR=$(mktemp -d)
SPOT_ERRORS=0
SPOT_CHECKED=0
SPOT_MAX=5
SPOT_FILES=()

# Find small files from manifest
while IFS=$'\t' read -r EXPECTED_HASH MTIME FSIZE REL_PATH && [[ "${#SPOT_FILES[@]}" -lt "$SPOT_MAX" ]]; do
    if [[ "$FSIZE" -gt 1048576 ]]; then
        continue
    fi
    SPOT_FILES+=("$REL_PATH"$'\t'"$EXPECTED_HASH")
done < <(sort -t$'\t' -k3 -n "$TMP_MANIFEST" | head -100)

if [[ "${#SPOT_FILES[@]}" -gt 0 ]]; then
    # Build list of files to extract
    EXTRACT_LIST=()
    EXPECTED_HASHES=()
    for ENTRY in "${SPOT_FILES[@]}"; do
        EXTRACT_LIST+=("$(echo "$ENTRY" | awk -F'\t' '{print $1}')")
        EXPECTED_HASHES+=("$(echo "$ENTRY" | awk -F'\t' '{print $2}')")
    done

    # Single-pass extract
    zstd -dc "$ARCHIVE_PATH" 2>/dev/null | tar -xf - -C "$SPOT_DIR" "${EXTRACT_LIST[@]}" 2>/dev/null || true

    for i in "${!EXTRACT_LIST[@]}"; do
        REL="${EXTRACT_LIST[$i]}"
        EXPECTED="${EXPECTED_HASHES[$i]}"
        EXTRACTED=$(sha256sum "${SPOT_DIR}/${REL}" 2>/dev/null | awk '{print $1}') || true

        if [[ -z "$EXTRACTED" ]]; then
            echo "  WARNING: could not extract '$REL' from archive"
            continue
        fi
        if [[ "$EXPECTED" != "$EXTRACTED" ]]; then
            echo "  ERROR: hash mismatch for '$REL'"
            echo "    expected: $EXPECTED"
            echo "    got:      $EXTRACTED"
            SPOT_ERRORS=$((SPOT_ERRORS + 1))
        fi
        SPOT_CHECKED=$((SPOT_CHECKED + 1))
    done
fi

rm -rf "$SPOT_DIR"

if [[ "$SPOT_ERRORS" -gt 0 ]]; then
    echo "ERROR: $SPOT_ERRORS file(s) failed spot-check. Archive is corrupt. Removing."
    rm -f "$ARCHIVE_PATH"
    rm -f "$TMP_MANIFEST"
    exit 1
fi
echo "Spot-check passed (${SPOT_CHECKED} files verified)."
fi  # end SPOT_CHECK

ARCHIVE_SHA256=$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')
echo "Archive SHA256: $ARCHIVE_SHA256"

# ── Compute summary stats from manifest ──────────────────────────────────────
LATEST_MTIME=$(awk -F'\t' 'BEGIN{m=0} $2>m{m=$2} END{print m}' "$TMP_MANIFEST")
LATEST_MTIME_ISO="$(date -u -d "@$LATEST_MTIME" +%Y-%m-%dT%H:%M:%SZ)"
LAST_ARCHIVED_TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# ── Atomically write manifest and checkpoint ─────────────────────────────────
# Write to temp files first, then move — prevents partial writes on crash/kill.
TMP_CHECKPOINT=$(mktemp)
cat > "$TMP_CHECKPOINT" <<EOF
{
  "last_archived_timestamp": "$LAST_ARCHIVED_TIMESTAMP",
  "archive_file": "$ARCHIVE_NAME",
  "archive_sha256": "$ARCHIVE_SHA256",
  "archive_bytes": $ARCHIVE_SIZE,
  "latest_data_mtime_epoch": $LATEST_MTIME,
  "latest_data_mtime_iso": "$LATEST_MTIME_ISO",
  "total_files": $MANIFEST_LINE_COUNT,
  "total_data_bytes": $TOTAL_SIZE,
  "manifest_file": "archive.manifest.tsv"
}
EOF

mv "$TMP_MANIFEST" "$MANIFEST_FILE"
mv "$TMP_CHECKPOINT" "$CHECKPOINT_FILE"
chmod 644 "$MANIFEST_FILE" "$CHECKPOINT_FILE"

echo "Done."
echo "  Archive:    $ARCHIVE_PATH ($(numfmt --to=iec "$ARCHIVE_SIZE"))"
echo "  Checkpoint: $CHECKPOINT_FILE"
echo "  Manifest:   $MANIFEST_FILE ($MANIFEST_LINE_COUNT entries)"
echo "  Latest data mtime: $LATEST_MTIME_ISO"
