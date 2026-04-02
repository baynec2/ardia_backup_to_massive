FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary is only needed for --db-host (live DB) mode
RUN pip install --no-cache-dir psycopg2-binary

COPY resolve_filenames.py transfer_to_massive.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Mount points for data (callers bind-mount these at runtime):
#   /data/db       — drop .sql.gz dumps here; a new dump triggers a transfer run
#   /data/raw      — UUID .raw files (backup-raw-data layout)
#   /data/output   — transfer_log.csv and mapping CSVs land here
VOLUME ["/data/db", "/data/raw", "/data/output"]

# Required: FTP_PASSWORD
# Optional: FTP_USER (default: gonzolabucsd)
ENV FTP_USER=gonzolabucsd

ENTRYPOINT ["/app/entrypoint.sh"]
