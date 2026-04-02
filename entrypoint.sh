#!/bin/bash
# entrypoint.sh
#
# Runs transfer_to_massive.py once per day at 02:00 UTC against the newest
# .sql.gz dump in /data/db.
#
# Required environment variables:
#   FTP_PASSWORD   — MASSIVE FTP password
#
# Optional environment variables:
#   FTP_USER       — MASSIVE FTP username (default: gonzolabucsd)

set -euo pipefail

FTP_USER="${FTP_USER:-gonzolabucsd}"

if [ -z "${FTP_PASSWORD:-}" ]; then
    echo "ERROR: FTP_PASSWORD environment variable is required." >&2
    exit 1
fi

echo "Ardia → MASSIVE transfer daemon started. Will run daily at 02:00 UTC."

LAST_PROCESSED=""

while true; do
    # Seconds until next 02:00 UTC
    NOW=$(date -u +%s)
    NEXT_2AM=$(date -u -d "tomorrow 02:00" +%s)
    SLEEP_SECS=$(( NEXT_2AM - NOW ))

    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Sleeping ${SLEEP_SECS}s until next run at $(date -u -d "@${NEXT_2AM}" '+%Y-%m-%dT%H:%M:%SZ')."
    sleep "$SLEEP_SECS"

    # Find the newest dump
    NEWEST=$(ls -t /data/db/*.sql.gz 2>/dev/null | head -1 || true)

    if [ -z "$NEWEST" ]; then
        echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] No dump found in /data/db — skipping." >&2
        continue
    fi

    if [ "$NEWEST" = "$LAST_PROCESSED" ]; then
        echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Dump unchanged ($NEWEST) — skipping."
        continue
    fi

    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Running transfer using dump: $NEWEST"

    python3 /app/transfer_to_massive.py \
        --dump         "$NEWEST" \
        --raw-dir      /data/raw \
        --ftp-user     "$FTP_USER" \
        --ftp-password "$FTP_PASSWORD" \
        --remote-base  /ardia_raw \
        --log          /data/output/transfer_log.csv \
    && { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Transfer complete."; LAST_PROCESSED="$NEWEST"; } \
    || echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Transfer exited with errors — check log above."
done
