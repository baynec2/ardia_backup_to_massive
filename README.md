# Ardia → MASSIVE Transfer

Automates the transfer of mass spectrometry `.raw` files from the Ardia server (Thermo Fisher Astral) to [MassIVE](https://massive.ucsd.edu) via FTP.

Files on the Ardia are stored with hashed (UUID) filenames. This pipeline resolves them to human-readable paths using the Ardia PostgreSQL database, then uploads each file to MassIVE preserving the original folder structure.

## How it works

The Ardia database contains three tables needed to map UUID filenames to human-readable paths:

| Table | Key columns |
|---|---|
| `standard_sequence."RawData"` | `StoragePath` (UUID on disk), `Name` (human-readable), `InjectionVersionId` |
| `standard_sequence."InjectionVersion"` | `Id`, `InjectionId` |
| `audit.audit_log` | `subject_id`, `subject_name`, `subject_path` (full destination path) |

**Join chain:** `RawData.StoragePath` → UUID stem → `InjectionVersion` → `audit_log.subject_path` + `.raw` = destination

## Prerequisites

- Python 3.10+
- `psycopg2-binary` — only needed for live database mode

```bash
pip install psycopg2-binary
```

Or use Docker (see below) — no local install required.

## Usage

### Step 1 — Resolve filenames

Produces a `mapping.csv` with columns: `uuid`, `raw_data_name`, `injection_id`, `destination`, `resolved`, `local_path`.

```bash
# From a database dump (offline / dev):
python3 resolve_filenames.py \
  --dump backup-db/<dump>.sql.gz \
  --raw-dir backup-raw-data/<timestamp> \
  -o mapping.csv

# From a live PostgreSQL database:
python3 resolve_filenames.py \
  --db-host <host> --db-name <name> --db-user <user> \
  --raw-dir <nas-mount-path> \
  -o mapping.csv
```

### Step 2 — Transfer to MassIVE

```bash
# From a pre-built mapping CSV:
python3 transfer_to_massive.py \
  --mapping-csv mapping.csv \
  --ftp-user gonzolabucsd \
  --remote-base /ardia_raw

# Build mapping and transfer in one step:
python3 transfer_to_massive.py \
  --dump backup-db/<dump>.sql.gz \
  --raw-dir <nas-mount-path> \
  --ftp-user gonzolabucsd \
  --remote-base /ardia_raw \
  --delete    # remove local file after confirmed upload

# Dry run (no actual upload):
python3 transfer_to_massive.py --mapping-csv mapping.csv --dry-run ...
```

`transfer_log.csv` is appended after each run; files already marked `success` are skipped automatically.

## Docker

```bash
docker build -t ardia-transfer .

# Resolve
docker run --rm \
  -v "$(pwd)/backup-db:/data/db" \
  -v "$(pwd)/backup-raw-data/<timestamp>:/data/raw" \
  -v "$(pwd):/data/output" \
  ardia-transfer resolve_filenames.py \
    --dump /data/db/<dump>.sql.gz \
    --raw-dir /data/raw \
    -o /data/output/mapping.csv

# Transfer
docker run --rm \
  -v "$(pwd)/backup-raw-data/<timestamp>:/data/raw" \
  -v "$(pwd):/data/output" \
  ardia-transfer transfer_to_massive.py \
    --mapping-csv /data/output/mapping.csv \
    --ftp-user gonzolabucsd \
    --remote-base /ardia_raw \
    --log /data/output/transfer_log.csv
```

## Repository structure

```
.
├── resolve_filenames.py    # Step 1: UUID → human-readable path mapping
├── transfer_to_massive.py  # Step 2: FTP upload to MassIVE
├── Dockerfile
├── backup-db/              # Place .sql.gz dump here (not committed)
├── backup-raw-data/        # Place UUID .raw files here (not committed)
└── infra-minio-backup/     # Future: MinIO backup infrastructure
```
