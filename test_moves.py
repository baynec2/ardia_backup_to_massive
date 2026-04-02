#!/usr/bin/env python3
"""
test_moves.py

Self-contained tests for the file-move handling logic in resolve_filenames.py
and transfer_to_massive.py. No FTP connection, dump file, or external dependencies
required — everything runs locally with temp files.

Run with:
    python3 test_moves.py
"""

import csv
import os
import sys
import tempfile
import traceback
from pathlib import Path

import resolve_filenames as rf
import transfer_to_massive as tm

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

_results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    print(f"  [{status}] {name}" + (f": {detail}" if detail else ""))
    _results.append(condition)


# ---------------------------------------------------------------------------
# 1. _pick_latest_audit_row
# ---------------------------------------------------------------------------

def test_pick_latest_audit_row():
    print("\n1. _pick_latest_audit_row")

    # Single row — should always return it
    rows = [{"subject_path": "/a", "subject_id": "1", "subject_name": "f"}]
    check("single row returned as-is", rf._pick_latest_audit_row(rows)["subject_path"] == "/a")

    # Multiple rows, no timestamp — should return last
    rows = [
        {"subject_path": "/old", "subject_id": "1", "subject_name": "f"},
        {"subject_path": "/new", "subject_id": "1", "subject_name": "f"},
    ]
    check("no timestamp → last row wins", rf._pick_latest_audit_row(rows)["subject_path"] == "/new")

    # Multiple rows with ISO-8601 timestamp — should return the one with max value
    rows = [
        {"subject_path": "/new", "subject_id": "1", "subject_name": "f",
         "created_at": "2025-06-01T10:00:00"},
        {"subject_path": "/old", "subject_id": "1", "subject_name": "f",
         "created_at": "2025-01-01T10:00:00"},
    ]
    check("timestamp present → max timestamp wins",
          rf._pick_latest_audit_row(rows)["subject_path"] == "/new")

    # Some rows missing the timestamp value (None) — should still pick the max non-null
    rows = [
        {"subject_path": "/maybe", "subject_id": "1", "subject_name": "f",
         "occurred_at": None},
        {"subject_path": "/latest", "subject_id": "1", "subject_name": "f",
         "occurred_at": "2025-09-01T00:00:00"},
        {"subject_path": "/earlier", "subject_id": "1", "subject_name": "f",
         "occurred_at": "2025-03-01T00:00:00"},
    ]
    check("null timestamp rows ignored, max non-null wins",
          rf._pick_latest_audit_row(rows)["subject_path"] == "/latest")


# ---------------------------------------------------------------------------
# 2. build_mapping with duplicate audit entries
# ---------------------------------------------------------------------------

def test_build_mapping_move():
    print("\n2. build_mapping — file moved (duplicate audit entries)")

    tables = {
        "RawData": [
            {
                "Id": "db-id-1",
                "Name": "sample.raw",
                "StoragePath": "2025/09/18/uuid-abc.raw",
                "InjectionVersionId": "iv-1",
            }
        ],
        "InjectionVersion": [
            {"Id": "iv-1", "InjectionId": "inj-1"}
        ],
        "audit_log": [
            # Original location
            {
                "subject_id": "inj-1",
                "subject_name": "sample",
                "subject_path": "/OldProject/OldSequence/sample",
                "subject_type": "Raw File",
                "created_at": "2025-01-10T08:00:00",
            },
            # Moved to new location
            {
                "subject_id": "inj-1",
                "subject_name": "sample",
                "subject_path": "/NewProject/NewSequence/sample",
                "subject_type": "Raw File",
                "created_at": "2025-06-15T12:00:00",
            },
        ],
    }

    import io
    import contextlib
    stderr_buf = io.StringIO()
    with contextlib.redirect_stderr(stderr_buf):
        mapping = rf.build_mapping(tables)

    check("exactly one mapping row produced", len(mapping) == 1)
    row = mapping[0]
    check("uuid extracted correctly", row["uuid"] == "uuid-abc")
    check("destination points to NEW location",
          row["destination"] == "/NewProject/NewSequence/sample.raw",
          row["destination"])
    check("resolved=True", row["resolved"] is True)

    warning = stderr_buf.getvalue()
    check("WARNING emitted for duplicate audit entries", "WARNING" in warning, warning.strip())


# ---------------------------------------------------------------------------
# 3. build_mapping — no timestamp, last-in-sequence wins
# ---------------------------------------------------------------------------

def test_build_mapping_no_timestamp():
    print("\n3. build_mapping — duplicate audit entries, no timestamp column")

    tables = {
        "RawData": [
            {
                "Id": "db-id-2",
                "Name": "run.raw",
                "StoragePath": "2025/09/18/uuid-xyz.raw",
                "InjectionVersionId": "iv-2",
            }
        ],
        "InjectionVersion": [{"Id": "iv-2", "InjectionId": "inj-2"}],
        "audit_log": [
            {
                "subject_id": "inj-2", "subject_name": "run",
                "subject_path": "/ProjA/run", "subject_type": "Raw File",
            },
            {
                "subject_id": "inj-2", "subject_name": "run",
                "subject_path": "/ProjB/run", "subject_type": "Raw File",
            },
        ],
    }

    import io, contextlib
    with contextlib.redirect_stderr(io.StringIO()):
        mapping = rf.build_mapping(tables)

    check("last-in-sequence chosen when no timestamp",
          mapping[0]["destination"] == "/ProjB/run.raw",
          mapping[0]["destination"])


# ---------------------------------------------------------------------------
# 4. load_completed — old log format (missing new columns)
# ---------------------------------------------------------------------------

def test_load_completed_old_format():
    print("\n4. load_completed — backward compatibility with old log format")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        old_fields = ["timestamp", "uuid", "local_path", "remote_path",
                      "status", "bytes_transferred", "error"]
        writer = csv.DictWriter(f, fieldnames=old_fields)
        writer.writeheader()
        writer.writerow({
            "timestamp": "2025-01-01T00:00:00+00:00",
            "uuid": "uuid-old",
            "local_path": "/data/uuid-old.raw",
            "remote_path": "/ardia_raw/OldProject/file.raw",
            "status": "success",
            "bytes_transferred": 1024,
            "error": "",
        })
        log_path = f.name

    try:
        completed = tm.load_completed(log_path)
        check("old log loads without error", True)
        check("uuid present in completed dict", "uuid-old" in completed)
        check("remote_path populated", completed["uuid-old"]["remote_path"] == "/ardia_raw/OldProject/file.raw")
        check("previous_remote_path defaults to empty string",
              completed["uuid-old"]["previous_remote_path"] == "")
    except Exception as exc:
        check("old log loads without error", False, str(exc))
    finally:
        os.unlink(log_path)


# ---------------------------------------------------------------------------
# 5. load_completed — new log format, move re-upload overwrites earlier entry
# ---------------------------------------------------------------------------

def test_load_completed_move_overwrite():
    print("\n5. load_completed — re-upload after move overwrites earlier success row")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=tm.LOG_FIELDS)
        writer.writeheader()
        # First success: original path
        writer.writerow({
            "timestamp": "2025-01-01T00:00:00+00:00",
            "uuid": "uuid-moved",
            "local_path": "/data/uuid-moved.raw",
            "remote_path": "/ardia_raw/Old/file.raw",
            "previous_remote_path": "",
            "old_path_deleted": "n/a",
            "status": "success",
            "bytes_transferred": 512,
            "error": "",
        })
        # Second success: after move
        writer.writerow({
            "timestamp": "2025-06-01T00:00:00+00:00",
            "uuid": "uuid-moved",
            "local_path": "/data/uuid-moved.raw",
            "remote_path": "/ardia_raw/New/file.raw",
            "previous_remote_path": "/ardia_raw/Old/file.raw",
            "old_path_deleted": "yes",
            "status": "success",
            "bytes_transferred": 512,
            "error": "",
        })
        log_path = f.name

    try:
        completed = tm.load_completed(log_path)
        check("only one entry per uuid", len(completed) == 1)
        check("latest remote_path wins",
              completed["uuid-moved"]["remote_path"] == "/ardia_raw/New/file.raw",
              completed["uuid-moved"]["remote_path"])
    finally:
        os.unlink(log_path)


# ---------------------------------------------------------------------------
# 6. run_transfers dry-run — Case B (move detected)
# ---------------------------------------------------------------------------

def test_run_transfers_move_dryrun():
    print("\n6. run_transfers (dry-run) — Case B: file moved, re-upload + old delete announced")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a real local .raw file
        raw_file = Path(tmpdir) / "uuid-b.raw"
        raw_file.write_bytes(b"fake raw data")

        # Mapping: uuid-b now resolves to new path
        mapping = [
            {
                "uuid": "uuid-b",
                "raw_data_name": "sample.raw",
                "injection_id": "inj-b",
                "destination": "/NewProject/sample.raw",
                "resolved": True,
                "local_path": str(raw_file),
            }
        ]

        # Transfer log: uuid-b was previously uploaded to old path
        log_path = Path(tmpdir) / "transfer_log.csv"
        with open(log_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=tm.LOG_FIELDS)
            writer.writeheader()
            writer.writerow({
                "timestamp": "2025-01-01T00:00:00+00:00",
                "uuid": "uuid-b",
                "local_path": str(raw_file),
                "remote_path": "/ardia_raw/OldProject/sample.raw",
                "previous_remote_path": "",
                "old_path_deleted": "n/a",
                "status": "success",
                "bytes_transferred": 13,
                "error": "",
            })

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            tm.run_transfers(
                mapping=mapping,
                ftp_user="testuser",
                ftp_password="testpass",
                remote_base="/ardia_raw",
                dry_run=True,
                delete_after=False,
                log_path=str(log_path),
            )
        output = buf.getvalue()

        check("MOVED message printed", "MOVED" in output, output.strip())
        check("new remote path mentioned", "/NewProject/sample.raw" in output)
        check("old path mentioned for deletion", "OldProject" in output)
        check("dry-run delete announced", "would delete old MASSIVE copy" in output)


# ---------------------------------------------------------------------------
# 7. run_transfers dry-run — Case A (exact match skip)
# ---------------------------------------------------------------------------

def test_run_transfers_skip_dryrun():
    print("\n7. run_transfers (dry-run) — Case A: already at correct path, skipped")

    with tempfile.TemporaryDirectory() as tmpdir:
        raw_file = Path(tmpdir) / "uuid-a.raw"
        raw_file.write_bytes(b"fake raw data")

        mapping = [
            {
                "uuid": "uuid-a",
                "raw_data_name": "file.raw",
                "injection_id": "inj-a",
                "destination": "/Project/file.raw",
                "resolved": True,
                "local_path": str(raw_file),
            }
        ]

        log_path = Path(tmpdir) / "transfer_log.csv"
        with open(log_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=tm.LOG_FIELDS)
            writer.writeheader()
            writer.writerow({
                "timestamp": "2025-01-01T00:00:00+00:00",
                "uuid": "uuid-a",
                "local_path": str(raw_file),
                "remote_path": "/ardia_raw/Project/file.raw",
                "previous_remote_path": "",
                "old_path_deleted": "n/a",
                "status": "success",
                "bytes_transferred": 13,
                "error": "",
            })

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            tm.run_transfers(
                mapping=mapping,
                ftp_user="testuser",
                ftp_password="testpass",
                remote_base="/ardia_raw",
                dry_run=True,
                delete_after=False,
                log_path=str(log_path),
            )
        output = buf.getvalue()

        check("skip message printed", "already transferred, skipping" in output)
        check("summary shows skipped=1", "skipped=1" in output)


# ---------------------------------------------------------------------------
# 8. run_transfers — missing_after_move
# ---------------------------------------------------------------------------

def test_run_transfers_missing_after_move():
    print("\n8. run_transfers — missing_after_move: local file deleted, destination changed")

    with tempfile.TemporaryDirectory() as tmpdir:
        missing_path = Path(tmpdir) / "uuid-gone.raw"
        # Intentionally do NOT create this file

        mapping = [
            {
                "uuid": "uuid-gone",
                "raw_data_name": "gone.raw",
                "injection_id": "inj-gone",
                "destination": "/NewProject/gone.raw",
                "resolved": True,
                "local_path": str(missing_path),
            }
        ]

        # Log shows it was previously uploaded to old path (and deleted locally)
        log_path = Path(tmpdir) / "transfer_log.csv"
        with open(log_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=tm.LOG_FIELDS)
            writer.writeheader()
            writer.writerow({
                "timestamp": "2025-01-01T00:00:00+00:00",
                "uuid": "uuid-gone",
                "local_path": str(missing_path),
                "remote_path": "/ardia_raw/OldProject/gone.raw",
                "previous_remote_path": "",
                "old_path_deleted": "n/a",
                "status": "success",
                "bytes_transferred": 0,
                "error": "",
            })

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            tm.run_transfers(
                mapping=mapping,
                ftp_user="testuser",
                ftp_password="testpass",
                remote_base="/ardia_raw",
                dry_run=True,
                delete_after=False,
                log_path=str(log_path),
            )
        output = buf.getvalue()

        # Check the log for missing_after_move status
        completed_after = tm.load_completed(str(log_path))
        new_rows = []
        with open(log_path, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "missing_after_move":
                    new_rows.append(row)

        check("WARNING printed", "WARNING" in output, output.strip())
        check("missing_after_move logged", len(new_rows) == 1)
        check("summary shows failed=1", "failed=1" in output)
        # The original success row remains in the log, so the UUID is still in
        # completed — but pointing to the OLD path, not the new one. This means
        # the next run will detect Case B again and re-warn until manually resolved.
        check("uuid still points to OLD path (not new)",
              completed_after.get("uuid-gone", {}).get("remote_path") == "/ardia_raw/OldProject/gone.raw")


# ---------------------------------------------------------------------------
# 9. run_transfers — missing local file, no prior upload (plain missing)
# ---------------------------------------------------------------------------

def test_run_transfers_missing_no_prior():
    print("\n9. run_transfers — missing local file with no prior upload (status=missing)")

    with tempfile.TemporaryDirectory() as tmpdir:
        missing_path = Path(tmpdir) / "uuid-nofile.raw"

        mapping = [
            {
                "uuid": "uuid-nofile",
                "raw_data_name": "nofile.raw",
                "injection_id": "inj-nofile",
                "destination": "/Project/nofile.raw",
                "resolved": True,
                "local_path": str(missing_path),
            }
        ]

        log_path = Path(tmpdir) / "transfer_log.csv"

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            tm.run_transfers(
                mapping=mapping,
                ftp_user="testuser",
                ftp_password="testpass",
                remote_base="/ardia_raw",
                dry_run=True,
                delete_after=False,
                log_path=str(log_path),
            )
        output = buf.getvalue()

        new_rows = []
        with open(log_path, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("status") == "missing":
                    new_rows.append(row)

        check("WARNING printed", "WARNING" in output)
        check("status=missing logged", len(new_rows) == 1)
        check("summary shows failed=1", "failed=1" in output)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

TESTS = [
    test_pick_latest_audit_row,
    test_build_mapping_move,
    test_build_mapping_no_timestamp,
    test_load_completed_old_format,
    test_load_completed_move_overwrite,
    test_run_transfers_move_dryrun,
    test_run_transfers_skip_dryrun,
    test_run_transfers_missing_after_move,
    test_run_transfers_missing_no_prior,
]

if __name__ == "__main__":
    for test_fn in TESTS:
        try:
            test_fn()
        except Exception:
            print(f"  [{FAIL}] {test_fn.__name__} raised an unexpected exception:")
            traceback.print_exc()
            _results.append(False)

    total = len(_results)
    passed = sum(_results)
    failed = total - passed
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} passed" + (f"  ({failed} failed)" if failed else ""))
    sys.exit(0 if failed == 0 else 1)
