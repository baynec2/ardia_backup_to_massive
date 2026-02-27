# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project automates the transfer of mass spectrometry `.raw` files from the Ardia server (Thermo Fisher Astral) to [MASSIVE](https://massive.ucsd.edu) (gonzolabucsd account) via FTP. Files on the Ardia are stored with hashed filenames that must be resolved to human-readable paths using the PostgreSQL database before transfer.

## Repository Structure

- `backup-db/` — PostgreSQL dump (`.sql.gz`) exported from the Ardia. Contains the mapping data.
- `backup-raw-data/` — Sample `.raw` files organized by backup timestamp → `YYYY/MM/DD/<uuid>.raw`. The UUID filenames are the hashed names used on the Ardia.
- `infra-minio-backup/` — (Currently empty) Intended for MinIO backup infrastructure.

## Key Database Schema

The PostgreSQL dump contains three critical tables for resolving hashed filenames to human-readable paths:

| Table | Key Columns | Purpose |
|---|---|---|
| `standard_sequence."RawData"` | `StoragePath`, `Name`, `InjectionVersionId` | `StoragePath` contains the on-disk UUID path (e.g. `2025/02/14/{uuid}.raw`); `Name` is the human-readable filename |
| `standard_sequence."InjectionVersion"` | `Id`, `InjectionId` | Bridges `RawData.InjectionVersionId` → `InjectionId` used in the audit log |
| `audit.audit_log` | `subject_id`, `subject_name`, `subject_path`, `subject_type` | Rows where `subject_type = 'Raw File'`: `subject_id` = `InjectionId`, `subject_path` = full destination path (append `.raw`) |

**Join chain:**
```
RawData.StoragePath          → extract UUID stem (the on-disk filename)
RawData.InjectionVersionId   → InjectionVersion.Id
InjectionVersion.InjectionId → audit_log.subject_id  (subject_type = 'Raw File',
                                                       subject_name = RawData.Name without .raw)
audit_log.subject_path + ".raw"  → human-readable destination path
```

> Note: `RawData."Id"` is an internal DB primary key — it is **not** the UUID used on disk. The on-disk UUID comes from `RawData."StoragePath"`.

## Workflow

The intended pipeline once files land on the NAS:
1. Query the DB to resolve each hashed `.raw` filename to its human-readable destination path
2. FTP transfer files to MASSIVE (gonzolabucsd account) preserving folder structure
3. Delete file from NAS after confirmed transfer

## Scripts

### `resolve_filenames.py`

Resolves all UUID filenames to human-readable MASSIVE paths. Outputs a CSV with columns: `uuid`, `raw_data_name`, `injection_id`, `destination`, `resolved`, and optionally `local_path`.

```bash
# From dump (offline / dev):
python3 resolve_filenames.py \
  --dump backup-db/pg-backup-2025-09-18-195011UTC.sql.gz \
  --raw-dir backup-raw-data/2025-09-18-195011UTC \
  -o mapping.csv

# From live database:
python3 resolve_filenames.py --db-host <host> --db-name <name> --db-user <user>
```

### `transfer_to_massive.py`

Uploads resolved files to MASSIVE via FTP (`massive-ftp.ucsd.edu`, port 21, plain FTP). Reads from a mapping CSV or builds the mapping on the fly. Appends results to `transfer_log.csv` and skips files already marked as success in a previous run.

```bash
# From a pre-built mapping CSV:
python3 transfer_to_massive.py \
  --mapping-csv mapping.csv \
  --ftp-user gonzolabucsd \
  --remote-base /ardia_raw

# Build mapping and transfer in one step:
python3 transfer_to_massive.py \
  --dump backup-db/pg-backup-2025-09-18-195011UTC.sql.gz \
  --raw-dir <nas-mount-path> \
  --ftp-user gonzolabucsd \
  --remote-base /ardia_raw \
  --delete          # delete local file after confirmed upload
```

`--dry-run` prints what would be transferred without connecting to FTP.

## Working with the Database Dump

To load the dump into a local Postgres instance for ad-hoc queries:

```bash
gunzip -k backup-db/pg-backup-2025-09-18-195011UTC.sql.gz
psql -U <user> -d <dbname> < backup-db/pg-backup-2025-09-18-195011UTC.sql
```

## Target File Structure on MASSIVE

Scripts should replicate the Ardia's logical folder structure (human-readable) on MASSIVE. The `destination` column in `resolve_filenames.py` output gives the full target path (e.g. `/Ardia_Test_Folder/Ardia_Test_Sequence/HEK_ardia_test.raw`).
