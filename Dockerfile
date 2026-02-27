FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary is only needed for --db-host (live DB) mode
RUN pip install --no-cache-dir psycopg2-binary

COPY resolve_filenames.py transfer_to_massive.py ./

# Mount points for data (callers bind-mount these at runtime):
#   /data/db       — put your .sql.gz dump here
#   /data/raw      — put your UUID .raw files here (backup-raw-data layout)
#   /data/output   — CSV outputs and transfer_log.csv land here
VOLUME ["/data/db", "/data/raw", "/data/output"]

ENTRYPOINT ["python3"]
