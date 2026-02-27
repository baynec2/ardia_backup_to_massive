#!/usr/bin/env python3
"""
transfer_to_massive.py

Transfers Ardia .raw files to the gonzolabucsd MASSIVE account via FTP,
placing each file at its human-readable destination path.

Reads the file mapping produced by resolve_filenames.py (--mapping-csv), or
builds it on the fly from the database (--dump / --db-host) together with a
local raw-data directory (--raw-dir).

After each successful upload the local file is optionally deleted (--delete).

Usage:
  python3 transfer_to_massive.py \\
      --mapping-csv mapping.csv \\
      --ftp-user gonzolabucsd \\
      --ftp-password <pass> \\
      --remote-base /ardia_raw

  # Build mapping on the fly and transfer in one step:
  python3 transfer_to_massive.py \\
      --dump backup-db/pg-backup-2025-09-18-195011UTC.sql.gz \\
      --raw-dir backup-raw-data/2025-09-18-195011UTC \\
      --ftp-user gonzolabucsd \\
      --ftp-password <pass>

  # Dry run (shows what would be transferred, no actual upload):
  python3 transfer_to_massive.py --mapping-csv mapping.csv --dry-run ...
"""

import argparse
import csv
import ftplib
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


FTP_HOST = "massive-ftp.ucsd.edu"
FTP_PORT = 21
TRANSFER_LOG = "transfer_log.csv"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB read chunks
CONNECT_TIMEOUT = 30           # seconds


# ---------------------------------------------------------------------------
# Mapping helpers (re-use resolve_filenames logic without importing it)
# ---------------------------------------------------------------------------

def load_mapping_csv(csv_path):
    """Load a mapping CSV produced by resolve_filenames.py."""
    rows = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def build_mapping_from_db(dump=None, db_host=None, db_port=5432,
                           db_name="ardia", db_user="postgres",
                           db_password="", raw_dir=None):
    """Import resolve_filenames and build the mapping in-process."""
    script_dir = Path(__file__).parent
    sys.path.insert(0, str(script_dir))
    try:
        import resolve_filenames as rf
    except ImportError:
        sys.exit("resolve_filenames.py must be in the same directory as this script.")

    if dump:
        print(f"Loading from dump: {dump}", file=sys.stderr)
        tables = rf.load_from_dump(dump)
    else:
        print(f"Connecting to {db_host}:{db_port}/{db_name}", file=sys.stderr)
        tables = rf.load_from_db(db_host, db_port, db_name, db_user, db_password)

    mapping = rf.build_mapping(tables)

    raw_files = rf.find_raw_files(raw_dir) if raw_dir else {}
    for row in mapping:
        row["local_path"] = raw_files.get(row["uuid"], "")
    return mapping


# ---------------------------------------------------------------------------
# Transfer log
# ---------------------------------------------------------------------------

LOG_FIELDS = ["timestamp", "uuid", "local_path", "remote_path", "status",
              "bytes_transferred", "error"]


def open_log(log_path):
    """Open the transfer log CSV, writing the header only if the file is new."""
    is_new = not Path(log_path).exists()
    f = open(log_path, "a", newline="")
    writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
    if is_new:
        writer.writeheader()
    return f, writer


def load_completed(log_path):
    """Return the set of remote_paths that previously succeeded."""
    completed = set()
    if not Path(log_path).exists():
        return completed
    with open(log_path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("status") == "success":
                completed.add(row["remote_path"])
    return completed


# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------

def ftp_connect(user, password):
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=CONNECT_TIMEOUT)
    ftp.login(user, password)
    ftp.set_pasv(True)
    return ftp


def ftp_makedirs(ftp, remote_dir):
    """Recursively create remote_dir on the FTP server if it doesn't exist."""
    parts = PurePosixPath(remote_dir).parts
    path = "/"
    for part in parts:
        if part == "/":
            continue
        path = str(PurePosixPath(path) / part)
        try:
            ftp.cwd(path)
        except ftplib.error_perm:
            ftp.mkd(path)
            ftp.cwd(path)


def remote_file_size(ftp, remote_path):
    """Return the size of a remote file, or None if it doesn't exist."""
    try:
        return ftp.size(remote_path)
    except ftplib.error_perm:
        return None


def upload_file(ftp, local_path, remote_path, dry_run=False):
    """
    Upload local_path to remote_path.
    Returns bytes transferred on success, raises on failure.
    """
    if dry_run:
        size = Path(local_path).stat().st_size
        print(f"  [dry-run] would upload {local_path} → {remote_path} ({_fmt_size(size)})")
        return size

    remote_dir = str(PurePosixPath(remote_path).parent)
    ftp_makedirs(ftp, remote_dir)

    local_size = Path(local_path).stat().st_size
    transferred = [0]
    start = time.monotonic()

    def progress(block):
        transferred[0] += len(block)
        elapsed = time.monotonic() - start
        rate = transferred[0] / elapsed if elapsed > 0 else 0
        pct = 100 * transferred[0] / local_size if local_size else 0
        print(
            f"\r  {pct:5.1f}%  {_fmt_size(transferred[0])}/{_fmt_size(local_size)}"
            f"  {_fmt_size(rate)}/s    ",
            end="", flush=True,
        )

    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {remote_path}", f,
                       blocksize=CHUNK_SIZE, callback=progress)
    print()  # newline after progress line

    return transferred[0]


def _fmt_size(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ---------------------------------------------------------------------------
# Main transfer loop
# ---------------------------------------------------------------------------

def run_transfers(mapping, ftp_user, ftp_password, remote_base,
                  dry_run, delete_after, log_path):
    completed = load_completed(log_path)
    log_file, log_writer = open_log(log_path)

    # Filter to transferable rows
    transferable = [
        r for r in mapping
        if r.get("local_path") and r.get("destination") and str(r.get("resolved")) != "False"
    ]
    skippable = [r for r in transferable
                 if str(PurePosixPath(remote_base) / r["destination"].lstrip("/")) in completed]

    print(f"\n{len(transferable)} files ready to transfer  "
          f"({len(skippable)} already completed, "
          f"{len(transferable) - len(skippable)} remaining)")

    if not transferable:
        print("Nothing to transfer.")
        log_file.close()
        return

    ftp = None
    if not dry_run:
        print(f"\nConnecting to {FTP_HOST}:{FTP_PORT} as {ftp_user}…")
        ftp = ftp_connect(ftp_user, ftp_password)
        print("Connected.\n")

    ok = skip = fail = 0

    for i, row in enumerate(transferable, 1):
        local_path = row["local_path"]
        destination = row["destination"].lstrip("/")
        remote_path = str(PurePosixPath(remote_base) / destination)

        print(f"[{i}/{len(transferable)}] {Path(local_path).name}  →  {remote_path}")

        if remote_path in completed:
            print("  already transferred, skipping.")
            skip += 1
            continue

        log_row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uuid": row.get("uuid", ""),
            "local_path": local_path,
            "remote_path": remote_path,
            "status": "",
            "bytes_transferred": 0,
            "error": "",
        }

        try:
            bytes_sent = upload_file(ftp, local_path, remote_path, dry_run=dry_run)

            if not dry_run:
                # Verify: remote size must match local size
                local_size = Path(local_path).stat().st_size
                remote_size = remote_file_size(ftp, remote_path)
                if remote_size is not None and remote_size != local_size:
                    raise RuntimeError(
                        f"Size mismatch: local={local_size}, remote={remote_size}"
                    )

            log_row.update(status="success", bytes_transferred=bytes_sent)
            log_writer.writerow(log_row)
            log_file.flush()
            ok += 1

            if delete_after and not dry_run:
                os.remove(local_path)
                print(f"  deleted local file.")

        except Exception as exc:
            print(f"  FAILED: {exc}")
            log_row.update(status="failed", error=str(exc))
            log_writer.writerow(log_row)
            log_file.flush()
            fail += 1

            # Reconnect on FTP errors so subsequent files can still be attempted
            if ftp and isinstance(exc, ftplib.Error):
                try:
                    ftp.quit()
                except Exception:
                    pass
                print("  Reconnecting to FTP…")
                ftp = ftp_connect(ftp_user, ftp_password)

    if ftp:
        try:
            ftp.quit()
        except Exception:
            pass

    log_file.close()
    print(f"\nDone. success={ok}  skipped={skip}  failed={fail}")
    print(f"Log written to: {log_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Transfer Ardia .raw files to MASSIVE via FTP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage:")[1] if "Usage:" in __doc__ else "",
    )

    # Mapping source
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--mapping-csv", metavar="FILE",
                        help="Mapping CSV from resolve_filenames.py")
    source.add_argument("--dump", metavar="FILE",
                        help="Build mapping from a .sql/.sql.gz dump")
    source.add_argument("--db-host", metavar="HOST",
                        help="Build mapping from a live PostgreSQL host")

    # DB options (used with --dump or --db-host)
    parser.add_argument("--raw-dir", metavar="DIR",
                        help="Local raw-data directory (required with --dump/--db-host)")
    parser.add_argument("--db-port", default=5432, type=int, metavar="PORT")
    parser.add_argument("--db-name", default="ardia", metavar="NAME")
    parser.add_argument("--db-user", default="postgres", metavar="USER")
    parser.add_argument("--db-password", default="", metavar="PASS")

    # FTP options
    parser.add_argument("--ftp-user", required=True, metavar="USER",
                        help="MASSIVE FTP username (e.g. gonzolabucsd)")
    parser.add_argument("--ftp-password", metavar="PASS", default=None,
                        help="MASSIVE FTP password (prompted if omitted)")
    parser.add_argument("--remote-base", default="/",
                        help="Remote base directory on MASSIVE (default: /)")

    # Behaviour
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be transferred without uploading")
    parser.add_argument("--delete", action="store_true",
                        help="Delete each local file after a successful upload")
    parser.add_argument("--log", default=TRANSFER_LOG, metavar="FILE",
                        help=f"Transfer log CSV (default: {TRANSFER_LOG})")

    args = parser.parse_args()

    # Password prompt
    ftp_password = args.ftp_password
    if not ftp_password and not args.dry_run:
        import getpass
        ftp_password = getpass.getpass(f"MASSIVE FTP password for {args.ftp_user}: ")

    # Build/load mapping
    if args.mapping_csv:
        print(f"Loading mapping from: {args.mapping_csv}", file=sys.stderr)
        mapping = load_mapping_csv(args.mapping_csv)
    elif args.dump:
        if not args.raw_dir:
            parser.error("--raw-dir is required when using --dump")
        mapping = build_mapping_from_db(dump=args.dump, raw_dir=args.raw_dir)
    else:
        if not args.raw_dir:
            parser.error("--raw-dir is required when using --db-host")
        mapping = build_mapping_from_db(
            db_host=args.db_host, db_port=args.db_port,
            db_name=args.db_name, db_user=args.db_user,
            db_password=args.db_password, raw_dir=args.raw_dir,
        )

    run_transfers(
        mapping=mapping,
        ftp_user=args.ftp_user,
        ftp_password=ftp_password,
        remote_base=args.remote_base,
        dry_run=args.dry_run,
        delete_after=args.delete,
        log_path=args.log,
    )


if __name__ == "__main__":
    main()
