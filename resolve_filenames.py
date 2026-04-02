#!/usr/bin/env python3
"""
resolve_filenames.py

Resolves hashed UUID filenames on the Ardia NAS to their human-readable
destination paths using the Ardia PostgreSQL database.

Join chain:
  RawData.StoragePath  (contains the UUID used on disk, e.g. 2025/02/14/{uuid}.raw)
  RawData.InjectionVersionId  → InjectionVersion.Id
  InjectionVersion.InjectionId → audit_log.subject_id (subject_type='Raw File')
  audit_log.subject_path + '.raw'  → human-readable destination path

Usage:
  # From SQL dump:
  python3 resolve_filenames.py --dump backup-db/pg-backup-2025-09-18-195011UTC.sql.gz

  # Scan a local raw-data directory and match files:
  python3 resolve_filenames.py --dump backup-db/pg-backup-2025-09-18-195011UTC.sql.gz \\
      --raw-dir backup-raw-data/2025-09-18-195011UTC

  # From a live PostgreSQL database:
  python3 resolve_filenames.py --db-host <host> --db-name <name> --db-user <user>

  # Save output to a CSV file:
  python3 resolve_filenames.py --dump ... -o mapping.csv
"""

import argparse
import csv
import gzip
import os
import sys
from pathlib import Path


# Candidate timestamp column names to probe on audit_log (in priority order).
_TS_CANDIDATES = ["occurred_at", "created_at", "logged_at", "timestamp", "event_time"]


# ---------------------------------------------------------------------------
# Dump parser
# ---------------------------------------------------------------------------

def _parse_copy_block(lines):
    """Parse a COPY ... FROM stdin block into a list of row dicts."""
    header = lines[0]
    col_str = header[header.index("(") + 1 : header.rindex(")")]
    columns = [c.strip().strip('"') for c in col_str.split(",")]

    rows = []
    for line in lines[1:]:
        if line == "\\.":
            break
        values = line.split("\t")
        row = {col: (None if values[i] == r"\N" else values[i])
               for i, col in enumerate(columns) if i < len(values)}
        rows.append(row)
    return rows


def load_from_dump(dump_path):
    """Return (raw_data, injection_versions, audit_log_rows) parsed from a pg dump."""
    targets = {
        'COPY standard_sequence."RawData"': "RawData",
        'COPY standard_sequence."InjectionVersion"': "InjectionVersion",
        "COPY audit.audit_log ": "audit_log",
    }
    tables = {"RawData": [], "InjectionVersion": [], "audit_log": []}

    open_fn = gzip.open if str(dump_path).endswith(".gz") else open
    current_key = None
    current_lines = []

    with open_fn(dump_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            matched = next((k for k in targets if line.startswith(k)), None)
            if matched:
                current_key = targets[matched]
                current_lines = [line]
            elif current_key:
                current_lines.append(line)
                if line == "\\.":
                    tables[current_key] = _parse_copy_block(current_lines)
                    current_key = None
                    current_lines = []

    return tables


# ---------------------------------------------------------------------------
# Live DB loader
# ---------------------------------------------------------------------------

def load_from_db(host, port, dbname, user, password):
    """Return the same table dict by querying a live PostgreSQL database."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        sys.exit("psycopg2 not found — install with: pip install psycopg2-binary")

    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                             user=user, password=password)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT "Id", "Name", "StoragePath", "InjectionVersionId"
        FROM standard_sequence."RawData"
        WHERE "StoragePath" IS NOT NULL AND "StoragePath" != 'N/A'
    """)
    raw_data = [dict(r) for r in cur.fetchall()]

    cur.execute('SELECT "Id", "InjectionId" FROM standard_sequence."InjectionVersion"')
    injection_versions = [dict(r) for r in cur.fetchall()]

    # Probe for a timestamp column so _pick_latest_audit_row can sort authoritatively.
    ts_candidates_sql = ", ".join(f"'{c}'" for c in _TS_CANDIDATES)
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'audit'
          AND table_name   = 'audit_log'
          AND column_name  IN ({ts_candidates_sql})
        ORDER BY ordinal_position
        LIMIT 1
    """)
    ts_row = cur.fetchone()
    ts_col = ts_row[0] if ts_row else None

    if ts_col:
        cur.execute(f"""
            SELECT subject_id, subject_name, subject_path, {ts_col}
            FROM audit.audit_log
            WHERE subject_type = 'Raw File' AND subject_path IS NOT NULL
            ORDER BY {ts_col} ASC
        """)
    else:
        cur.execute("""
            SELECT subject_id, subject_name, subject_path
            FROM audit.audit_log
            WHERE subject_type = 'Raw File' AND subject_path IS NOT NULL
        """)
    audit_log = [dict(r) for r in cur.fetchall()]

    conn.close()
    return {"RawData": raw_data, "InjectionVersion": injection_versions,
            "audit_log": audit_log}


# ---------------------------------------------------------------------------
# Audit-row helpers
# ---------------------------------------------------------------------------

def _pick_latest_audit_row(rows):
    """
    Given a list of audit_log row dicts that share the same (subject_id, subject_name),
    return the single row representing the file's current location.

    Strategy:
      1. Find the first timestamp column present and non-null in any row.
      2. If found, return the row with the maximum value for that column
         (string comparison is safe for ISO-8601 timestamps).
      3. Fallback: return the last row in the list (dump/query order is
         append-order for an audit log, so last == most recent move).
    """
    if len(rows) == 1:
        return rows[0]

    for ts_col in _TS_CANDIDATES:
        if any(r.get(ts_col) for r in rows):
            return max(rows, key=lambda r: r.get(ts_col) or "")

    return rows[-1]


# ---------------------------------------------------------------------------
# Mapping logic
# ---------------------------------------------------------------------------

def build_mapping(tables):
    """
    Return a list of dicts, one per RawData row that has a StoragePath UUID:
      uuid            - the UUID filename on disk (without .raw)
      raw_data_name   - RawData.Name (the original human-readable name)
      injection_id    - InjectionVersion.InjectionId
      destination     - resolved human-readable path on MASSIVE (with .raw)
      resolved        - True if a destination was found
    """
    # InjectionVersion.Id → InjectionId
    iv_to_injection = {
        row["Id"]: row["InjectionId"]
        for row in tables["InjectionVersion"]
    }

    # (injection_id, subject_name) → subject_path  — from audit_log Raw File rows
    # subject_path already ends with the filename, so we append .raw for the destination.
    #
    # Phase 1: group all rows by key (preserving encounter order so that
    #          last-in-sequence == most-recent-move when no timestamp exists).
    audit_candidates = {}
    for row in tables["audit_log"]:
        subject_type = row.get("subject_type")   # None when loaded from live DB (already filtered)
        if subject_type and subject_type != "Raw File":
            continue
        path = row.get("subject_path")
        if not path:
            continue
        key = (row["subject_id"], row["subject_name"])
        audit_candidates.setdefault(key, []).append(row)

    # Phase 2: for each key pick the authoritative (latest) row; warn on duplicates.
    audit_index = {}
    for key, candidates in audit_candidates.items():
        best = _pick_latest_audit_row(candidates)
        audit_index[key] = best["subject_path"]
        if len(candidates) > 1:
            ts_used = next(
                (c for c in _TS_CANDIDATES if best.get(c)), None
            )
            print(
                f"WARNING: {len(candidates)} audit entries for subject_id={key[0]!r} "
                f"name={key[1]!r}; using path '{best['subject_path']}' "
                f"({'timestamp column: ' + ts_used if ts_used else 'last-in-sequence fallback'})",
                file=sys.stderr,
            )

    results = []
    for row in tables["RawData"]:
        storage_path = row.get("StoragePath") or ""
        if not storage_path or storage_path == "N/A":
            continue  # not yet uploaded to NAS

        # Extract UUID from path like "2025/02/14/{uuid}.raw"
        uuid = Path(storage_path).stem

        raw_name = row.get("Name") or ""
        name_stem = raw_name.removesuffix(".raw")  # strip extension for audit lookup

        iv_id = row.get("InjectionVersionId")
        inj_id = iv_to_injection.get(iv_id) if iv_id else None

        destination = None
        if inj_id:
            audit_path = audit_index.get((inj_id, name_stem))
            if audit_path:
                destination = audit_path + ".raw"

        results.append({
            "uuid": uuid,
            "raw_data_name": raw_name,
            "injection_id": inj_id or "",
            "destination": destination or "",
            "resolved": destination is not None,
        })

    return results


# ---------------------------------------------------------------------------
# Disk scanner
# ---------------------------------------------------------------------------

def find_raw_files(raw_dir):
    """Walk raw_dir and return a dict mapping UUID stem → absolute file path."""
    files = {}
    for root, _, filenames in os.walk(raw_dir):
        for fname in filenames:
            if fname.lower().endswith(".raw"):
                files[Path(fname).stem] = os.path.join(root, fname)
    return files


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Resolve Ardia UUID filenames to human-readable MASSIVE paths",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage:")[1] if "Usage:" in __doc__ else "",
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dump", metavar="FILE",
                        help="Path to .sql or .sql.gz PostgreSQL dump")
    source.add_argument("--db-host", metavar="HOST",
                        help="PostgreSQL host for live DB connection")

    parser.add_argument("--db-port", default=5432, type=int, metavar="PORT")
    parser.add_argument("--db-name", default="ardia", metavar="NAME")
    parser.add_argument("--db-user", default="postgres", metavar="USER")
    parser.add_argument("--db-password", default="", metavar="PASS")

    parser.add_argument("--raw-dir", metavar="DIR",
                        help="Local directory of UUID .raw files to cross-reference")
    parser.add_argument("-o", "--output", metavar="FILE",
                        help="Write CSV output to FILE instead of stdout")
    parser.add_argument("--unresolved-only", action="store_true",
                        help="Print only rows that could not be resolved")

    args = parser.parse_args()

    # Load data
    if args.dump:
        print(f"Loading from dump: {args.dump}", file=sys.stderr)
        tables = load_from_dump(args.dump)
    else:
        print(f"Connecting to {args.db_host}:{args.db_port}/{args.db_name}", file=sys.stderr)
        tables = load_from_db(args.db_host, args.db_port, args.db_name,
                               args.db_user, args.db_password)

    print(
        f"Loaded {len(tables['RawData'])} RawData | "
        f"{len(tables['InjectionVersion'])} InjectionVersion | "
        f"{len(tables['audit_log'])} audit_log rows",
        file=sys.stderr,
    )

    mapping = build_mapping(tables)

    # Optionally cross-reference with files on disk
    raw_files = {}
    if args.raw_dir:
        raw_files = find_raw_files(args.raw_dir)
        print(f"Found {len(raw_files)} .raw files in {args.raw_dir}", file=sys.stderr)

    resolved = sum(1 for m in mapping if m["resolved"])
    print(f"Resolved {resolved}/{len(mapping)} entries", file=sys.stderr)

    # Write CSV
    fieldnames = ["uuid", "raw_data_name", "injection_id", "destination", "resolved"]
    if args.raw_dir:
        fieldnames.append("local_path")

    out = open(args.output, "w", newline="") if args.output else sys.stdout
    try:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        for row in mapping:
            if args.unresolved_only and row["resolved"]:
                continue
            if args.raw_dir:
                row["local_path"] = raw_files.get(row["uuid"], "")
            writer.writerow(row)
    finally:
        if args.output:
            out.close()


if __name__ == "__main__":
    main()
