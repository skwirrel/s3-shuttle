#!/usr/bin/env python3
"""S3 Shuttle — relays files between local and remote S3 buckets.

See docs/specification.md for the full technical specification.
"""

import argparse
import fcntl
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------
EXIT_OK = 0
EXIT_PARTIAL_FAILURE = 1
EXIT_FATAL = 2

# ---------------------------------------------------------------------------
# Emoji map (used only in --debug mode)
# ---------------------------------------------------------------------------
EMOJI = {
    "start": "\U0001f3c1",      # 🏁
    "lock": "\U0001f512",       # 🔒
    "push": "\u2b06\ufe0f",    # ⬆️
    "pull": "\u2b07\ufe0f",    # ⬇️
    "head": "\U0001f50d",       # 🔍
    "prune": "\U0001f5d1\ufe0f",  # 🗑️
    "skip": "\u23ed\ufe0f",    # ⏭️
    "ok": "\u2705",             # ✅
    "warn": "\u26a0\ufe0f",    # ⚠️
    "error": "\u274c",          # ❌
    "dry": "\U0001f9ea",        # 🧪
}

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------
CONFIG_DEFAULTS = {
    "bucket_config_path": "./buckets.json",
    "database_path": "./shuttle_state.db",
    "lock_file_path": "./shuttle.lock",
    "log_level": "INFO",
    "verification_interval_hours": 24,
    "mass_delete_threshold_percent": 20,
    "assume_role_session_name": "s3-shuttle",
}

DIRECTORY_DEFAULTS = {
    "incoming": "incoming/",
    "completed": "completed/",
    "errors": "errors/",
}


# ===================================================================
# Configuration loading
# ===================================================================

def load_config(path):
    """Load the general config file and return the merged settings dict.

    Relative paths inside the config (bucket_config_path, database_path,
    lock_file_path, log_file) are resolved against the config file's own
    directory.
    """
    try:
        with open(path) as f:
            raw = json.load(f)
    except FileNotFoundError:
        print(f"Fatal: config file not found: {path}", file=sys.stderr)
        sys.exit(EXIT_FATAL)
    except json.JSONDecodeError as exc:
        print(f"Fatal: invalid JSON in config file {path}: {exc}", file=sys.stderr)
        sys.exit(EXIT_FATAL)

    config = {**CONFIG_DEFAULTS, **raw}
    config_dir = os.path.dirname(os.path.abspath(path))

    for key in ("bucket_config_path", "database_path", "lock_file_path"):
        config[key] = _resolve(config[key], config_dir)
    if "log_file" in config and config["log_file"]:
        config["log_file"] = _resolve(config["log_file"], config_dir)

    return config


def load_bucket_config(path):
    """Load and validate the bucket-pair config file."""
    try:
        with open(path) as f:
            raw = json.load(f)
    except FileNotFoundError:
        print(f"Fatal: bucket config file not found: {path}", file=sys.stderr)
        sys.exit(EXIT_FATAL)
    except json.JSONDecodeError as exc:
        print(f"Fatal: invalid JSON in bucket config file {path}: {exc}", file=sys.stderr)
        sys.exit(EXIT_FATAL)

    pairs = raw.get("bucket_pairs")
    if not isinstance(pairs, list) or len(pairs) == 0:
        print("Fatal: bucket config must contain a non-empty 'bucket_pairs' list", file=sys.stderr)
        sys.exit(EXIT_FATAL)

    labels_seen = set()
    for i, pair in enumerate(pairs):
        tag = f"bucket_pairs[{i}]"

        # Required top-level fields
        for field in ("label", "local", "remote"):
            if field not in pair:
                print(f"Fatal: {tag} missing required field '{field}'", file=sys.stderr)
                sys.exit(EXIT_FATAL)

        label = pair["label"]
        if label in labels_seen:
            print(f"Fatal: duplicate pair label '{label}'", file=sys.stderr)
            sys.exit(EXIT_FATAL)
        labels_seen.add(label)

        # Defaults
        pair.setdefault("enabled", True)
        pair.setdefault("provider_retention_days", 0)

        # Directories — apply defaults and ensure trailing slashes
        dirs = pair.get("directories", {})
        for dname, default in DIRECTORY_DEFAULTS.items():
            val = dirs.get(dname, default)
            if not val.endswith("/"):
                val += "/"
            dirs[dname] = val
        pair["directories"] = dirs

        # Validate local fields
        local = pair["local"]
        for field in ("bucket", "region", "access_key_id", "secret_access_key"):
            if field not in local:
                print(f"Fatal: {tag}.local missing required field '{field}'", file=sys.stderr)
                sys.exit(EXIT_FATAL)

        # Validate remote fields
        remote = pair["remote"]
        for field in ("assume_role_access_key_id", "assume_role_secret_access_key",
                       "external_id", "role_arn", "bucket", "region"):
            if field not in remote:
                print(f"Fatal: {tag}.remote missing required field '{field}'", file=sys.stderr)
                sys.exit(EXIT_FATAL)

    return pairs


def _resolve(path, base_dir):
    """Resolve a potentially relative path against a base directory."""
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(base_dir, path))


# ===================================================================
# Logging setup
# ===================================================================

class EmojiFormatter(logging.Formatter):
    """Prepends an emoji to each log record based on a custom `emoji` attribute."""

    def format(self, record):
        prefix = getattr(record, "emoji", "")
        if prefix:
            prefix = prefix + " "
        original = super().format(record)
        return f"{prefix}{original}"


def setup_logging(config, args):
    """Configure the root logger per spec §11."""
    logger = logging.getLogger()
    logger.handlers.clear()

    if args.log_level:
        level = getattr(logging, args.log_level.upper(), logging.INFO)
    else:
        level = getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO)

    logger.setLevel(level)

    fmt_str = "%(asctime)s %(levelname)-7s %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%SZ"

    if args.debug:
        # Debug mode: stdout only, emoji-decorated
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(EmojiFormatter(fmt_str, datefmt=datefmt))
        handler.formatter.converter = lambda *a: datetime.now(timezone.utc).timetuple()
        logger.addHandler(handler)
    else:
        plain_fmt = logging.Formatter(fmt_str, datefmt=datefmt)
        plain_fmt.converter = lambda *a: datetime.now(timezone.utc).timetuple()

        # File handler if log_file is configured
        log_file = config.get("log_file")
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(plain_fmt)
            logger.addHandler(file_handler)

        # Stdout handler only if --log-level was explicitly passed
        if args.log_level:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setFormatter(plain_fmt)
            logger.addHandler(stdout_handler)


def _log(level, msg, emoji_key=None):
    """Convenience wrapper that attaches an emoji key for the EmojiFormatter."""
    logging.log(level, msg, extra={"emoji": EMOJI.get(emoji_key, "")})


# ===================================================================
# Lock acquisition
# ===================================================================

def acquire_lock(lock_path):
    """Try to acquire an exclusive, non-blocking lock. Returns the file object or None."""
    try:
        lock_file = open(lock_path, "w")
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except OSError:
        return None


# ===================================================================
# Verification database (SQLite)
# ===================================================================

class VerificationDB:
    """SQLite wrapper for per-file verification timestamps."""

    def __init__(self, db_path):
        parent = os.path.dirname(os.path.abspath(db_path))
        if not os.path.isdir(parent):
            print(
                f"Fatal: parent directory for database does not exist: {parent}",
                file=sys.stderr,
            )
            sys.exit(EXIT_FATAL)

        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS file_verification (
                pair_label    TEXT NOT NULL,
                directory     TEXT NOT NULL,
                object_key    TEXT NOT NULL,
                last_verified TEXT NOT NULL,
                PRIMARY KEY (pair_label, directory, object_key)
            )"""
        )
        self.conn.commit()

    def upsert(self, pair_label, directory, object_key, last_verified):
        """Insert or update the verification timestamp for a file."""
        self.conn.execute(
            """INSERT INTO file_verification (pair_label, directory, object_key, last_verified)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(pair_label, directory, object_key)
               DO UPDATE SET last_verified = excluded.last_verified""",
            (pair_label, directory, object_key, last_verified),
        )
        self.conn.commit()

    def get_last_verified(self, pair_label, directory, object_key):
        """Return the last_verified ISO string, or None if no row exists."""
        row = self.conn.execute(
            """SELECT last_verified FROM file_verification
               WHERE pair_label = ? AND directory = ? AND object_key = ?""",
            (pair_label, directory, object_key),
        ).fetchone()
        return row[0] if row else None

    def delete(self, pair_label, directory, object_key):
        """Delete the verification row for a file."""
        self.conn.execute(
            """DELETE FROM file_verification
               WHERE pair_label = ? AND directory = ? AND object_key = ?""",
            (pair_label, directory, object_key),
        )
        self.conn.commit()

    def cleanup_orphans(self, pair_label, directory, live_keys):
        """Remove rows whose object_key is not in the given set of live keys."""
        cursor = self.conn.execute(
            """SELECT object_key FROM file_verification
               WHERE pair_label = ? AND directory = ?""",
            (pair_label, directory),
        )
        for (key,) in cursor.fetchall():
            if key not in live_keys:
                self.conn.execute(
                    """DELETE FROM file_verification
                       WHERE pair_label = ? AND directory = ? AND object_key = ?""",
                    (pair_label, directory, key),
                )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ===================================================================
# S3 helpers
# ===================================================================

def build_local_s3_client(local_cfg):
    """Build an S3 client from local IAM credentials."""
    return boto3.client(
        "s3",
        aws_access_key_id=local_cfg["access_key_id"],
        aws_secret_access_key=local_cfg["secret_access_key"],
        region_name=local_cfg["region"],
    )


def assume_role_and_build_remote_client(remote_cfg, session_name):
    """Call AssumeRole and return an S3 client using the temporary credentials."""
    sts = boto3.client(
        "sts",
        aws_access_key_id=remote_cfg["assume_role_access_key_id"],
        aws_secret_access_key=remote_cfg["assume_role_secret_access_key"],
        region_name=remote_cfg["region"],
    )
    resp = sts.assume_role(
        RoleArn=remote_cfg["role_arn"],
        RoleSessionName=session_name,
        ExternalId=remote_cfg["external_id"],
    )
    creds = resp["Credentials"]
    return boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=remote_cfg["region"],
    )


def remote_head_status(s3, bucket, key):
    """HEAD an object and return the HTTP status code (200, 404, 403, etc.)."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return 200
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"]


def list_objects_paginated(s3, bucket, prefix):
    """List all objects under a prefix using the paginator. Returns a list of dicts.

    Excludes the prefix-only key (e.g. 'incoming/') which S3 returns as a
    zero-byte folder marker rather than a real file.
    """
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"] != prefix:
                objects.append(obj)
    return objects


def normalize_etag(etag):
    """Strip surrounding quotes from an ETag for comparison."""
    if etag:
        return etag.strip('"')
    return etag


# ===================================================================
# Process incoming/ — push local → remote (§7.1)
# ===================================================================

def process_incoming(pair, local_s3, remote_s3, config, dry_run):
    """Push files from local incoming/ to remote incoming/.

    Returns a counts dict: pushed, skipped, failed.
    """
    label = pair["label"]
    prefix = pair["directories"]["incoming"]
    local_bucket = pair["local"]["bucket"]
    remote_bucket = pair["remote"]["bucket"]

    counts = {"pushed": 0, "skipped": 0, "failed": 0}

    _log(logging.INFO, f"[{label}] listing local {prefix}", "start")
    local_objects = list_objects_paginated(local_s3, local_bucket, prefix)

    if not local_objects:
        _log(logging.INFO, f"[{label}] no files in local {prefix}", "skip")
        return counts

    _log(logging.INFO, f"[{label}] found {len(local_objects)} file(s) in local {prefix}", "start")

    for obj in local_objects:
        key = obj["Key"]
        try:
            # Read the file from local
            _log(logging.INFO, f"[{label}] pushing: {key}", "push")
            get_resp = local_s3.get_object(Bucket=local_bucket, Key=key)
            body = get_resp["Body"].read()
            source_size = get_resp["ContentLength"]
            source_etag = normalize_etag(get_resp["ETag"])

            # Put to remote
            if dry_run:
                _log(logging.INFO, f"[{label}] DRY-RUN would push: {key}", "dry")
                counts["pushed"] += 1
                continue

            _log(logging.DEBUG, f"[{label}] putting to remote: {key}", "push")
            remote_s3.put_object(Bucket=remote_bucket, Key=key, Body=body)

            # Verify
            _log(logging.DEBUG, f"[{label}] verifying upload: {key}", "head")
            head = remote_s3.head_object(Bucket=remote_bucket, Key=key)
            remote_size = head["ContentLength"]
            remote_etag = normalize_etag(head["ETag"])

            if remote_size == source_size and remote_etag == source_etag:
                _log(logging.INFO, f"[{label}] verified upload: {key}", "ok")
                local_s3.delete_object(Bucket=local_bucket, Key=key)
                _log(logging.INFO, f"[{label}] pushed and removed: {key}", "push")
                counts["pushed"] += 1
            else:
                _log(
                    logging.ERROR,
                    f"[{label}] verification mismatch: {key} "
                    f"(local size={source_size} etag={source_etag}, "
                    f"remote size={remote_size} etag={remote_etag}) "
                    "— left in place for retry",
                    "error",
                )
                counts["failed"] += 1

        except Exception as e:
            _log(logging.ERROR, f"[{label}] push failed: {key}: {e} — left in place for retry", "error")
            counts["failed"] += 1

    # Ensure the folder marker still exists after deleting files
    if counts["pushed"] > 0 and not dry_run:
        try:
            local_s3.put_object(Bucket=local_bucket, Key=prefix, Body=b"")
        except Exception as e:
            _log(logging.WARNING, f"[{label}] could not restore folder marker {prefix}: {e}", "warn")

    return counts


# ===================================================================
# Process completed/ and errors/ — pull + prune (§7.2)
# ===================================================================

def process_completed_or_errors(pair, local_s3, remote_s3, config, db, directory_name, dry_run):
    """Pull new files from remote and prune local files confirmed gone.

    Returns a counts dict: copied, pruned, skipped, failed.
    """
    label = pair["label"]
    prefix = pair["directories"][directory_name]
    local_bucket = pair["local"]["bucket"]
    remote_bucket = pair["remote"]["bucket"]
    retention_days = pair.get("provider_retention_days", 0)
    verification_hours = config["verification_interval_hours"]
    mass_threshold = config["mass_delete_threshold_percent"]

    counts = {"copied": 0, "pruned": 0, "skipped": 0, "failed": 0}
    now_utc = datetime.now(timezone.utc)

    # --- Remote listing ---
    remote_keys = set()
    try:
        _log(logging.INFO, f"[{label}] listing remote {prefix}", "start")
        remote_objects = list_objects_paginated(remote_s3, remote_bucket, prefix)
        remote_keys = {obj["Key"] for obj in remote_objects}
        _log(logging.INFO, f"[{label}] found {len(remote_keys)} file(s) in remote {prefix}", "start")
    except Exception as e:
        _log(
            logging.ERROR,
            f"[{label}] failed to list remote {prefix}: {e} — proceeding with empty remote set",
            "error",
        )

    # --- Local listing ---
    _log(logging.INFO, f"[{label}] listing local {prefix}", "start")
    local_objects = list_objects_paginated(local_s3, local_bucket, prefix)
    local_map = {obj["Key"]: obj["LastModified"] for obj in local_objects}
    local_keys = set(local_map.keys())
    _log(logging.INFO, f"[{label}] found {len(local_keys)} file(s) in local {prefix}", "start")

    # --- Copy-down phase ---
    new_keys = remote_keys - local_keys
    if new_keys:
        _log(logging.INFO, f"[{label}] {len(new_keys)} new file(s) to copy down from {prefix}", "pull")
    for key in sorted(new_keys):
        try:
            _log(logging.INFO, f"[{label}] copying down: {key}", "pull")

            if dry_run:
                _log(logging.INFO, f"[{label}] DRY-RUN would copy down: {key}", "dry")
                counts["copied"] += 1
                continue

            get_resp = remote_s3.get_object(Bucket=remote_bucket, Key=key)
            body = get_resp["Body"].read()
            source_size = get_resp["ContentLength"]
            source_etag = normalize_etag(get_resp["ETag"])

            local_s3.put_object(Bucket=local_bucket, Key=key, Body=body)

            # Verify
            head = local_s3.head_object(Bucket=local_bucket, Key=key)
            local_size = head["ContentLength"]
            local_etag = normalize_etag(head["ETag"])

            if local_size == source_size and local_etag == source_etag:
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                db.upsert(label, prefix, key, now_str)
                _log(logging.INFO, f"[{label}] copied down: {key}", "pull")
                counts["copied"] += 1
            else:
                _log(
                    logging.ERROR,
                    f"[{label}] copy-down verification mismatch: {key} — file kept",
                    "error",
                )
                counts["failed"] += 1

        except Exception as e:
            _log(logging.ERROR, f"[{label}] copy-down failed: {key}: {e}", "error")
            counts["failed"] += 1

    # --- Bulk verification refresh ---
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for key in remote_keys:
        db.upsert(label, prefix, key, now_str)

    # --- Prune phase ---
    delete_candidates = []
    for key in sorted(local_keys):
        last_modified = local_map[key]
        # Ensure last_modified is timezone-aware
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        age = now_utc - last_modified

        # Retention gate
        if retention_days > 0 and age < timedelta(days=retention_days):
            _log(logging.INFO, f"[{label}] skipped (younger than {retention_days}d retention): {key}", "skip")
            counts["skipped"] += 1
            continue

        # Verification interval gate
        last_verified = db.get_last_verified(label, prefix, key)
        if last_verified is not None:
            try:
                lv_dt = datetime.strptime(last_verified, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                if (now_utc - lv_dt) < timedelta(hours=verification_hours):
                    _log(logging.INFO, f"[{label}] skipped (verified {last_verified}): {key}", "skip")
                    counts["skipped"] += 1
                    continue
            except ValueError:
                pass  # Malformed timestamp — proceed with HEAD check

        # HEAD check
        _log(logging.INFO, f"[{label}] HEAD check against remote: {key}", "head")
        status = remote_head_status(remote_s3, remote_bucket, key)

        if status == 404:
            _log(logging.INFO, f"[{label}] confirmed gone from remote (404): {key}", "prune")
            delete_candidates.append(key)
        elif status == 200:
            now_str2 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            db.upsert(label, prefix, key, now_str2)
            _log(logging.INFO, f"[{label}] still present remotely (200): {key}", "ok")
            counts["skipped"] += 1
        else:
            _log(logging.WARNING, f"[{label}] inconclusive HEAD (status {status}) for {key} — keeping", "warn")
            counts["skipped"] += 1

    # --- Mass-delete safety valve ---
    if delete_candidates and local_keys:
        delete_pct = len(delete_candidates) / len(local_keys) * 100
        if delete_pct > mass_threshold:
            _log(
                logging.ERROR,
                f"[{label}] mass-delete guard tripped: would delete "
                f"{len(delete_candidates)} of {len(local_keys)} in {prefix} "
                f"({delete_pct:.1f}% > {mass_threshold}%) — prune skipped",
                "error",
            )
            counts["failed"] += len(delete_candidates)
            delete_candidates = []

    # Execute deletes
    for key in delete_candidates:
        try:
            if dry_run:
                _log(logging.INFO, f"[{label}] DRY-RUN would prune: {key}", "dry")
                counts["pruned"] += 1
                continue

            local_s3.delete_object(Bucket=local_bucket, Key=key)
            db.delete(label, prefix, key)
            _log(logging.INFO, f"[{label}] pruned local (confirmed gone from remote): {key}", "prune")
            counts["pruned"] += 1
        except Exception as e:
            _log(logging.ERROR, f"[{label}] prune failed: {key}: {e}", "error")
            counts["failed"] += 1

    # Optional: clean up orphaned DB rows
    db.cleanup_orphans(label, prefix, local_keys)

    return counts


# ===================================================================
# Main orchestrator
# ===================================================================

def main():
    args = parse_args()

    # Load config
    config = load_config(args.config)
    pairs = load_bucket_config(config["bucket_config_path"])

    # Setup logging
    setup_logging(config, args)

    _log(logging.INFO, "S3 Shuttle starting", "start")

    # Acquire lock
    lock = acquire_lock(config["lock_file_path"])
    if lock is None:
        _log(logging.INFO, "Another run is already in progress (lock held) — exiting", "lock")
        return EXIT_OK

    _log(logging.DEBUG, f"Lock acquired: {config['lock_file_path']}", "lock")

    # Open database
    db = VerificationDB(config["database_path"])

    dry_run = args.dry_run
    if dry_run:
        _log(logging.INFO, "DRY-RUN mode: no mutating operations will be performed", "dry")

    # Filter pairs if --pair specified
    selected_labels = set(args.pair) if args.pair else None
    if selected_labels:
        unknown = selected_labels - {p["label"] for p in pairs}
        if unknown:
            _log(logging.ERROR, f"Unknown pair labels: {', '.join(sorted(unknown))}", "error")
            db.close()
            lock.close()
            return EXIT_FATAL

    # Process pairs
    run_had_failures = False
    all_summaries = []

    for pair in pairs:
        label = pair["label"]

        if not pair.get("enabled", True):
            _log(logging.DEBUG, f"[{label}] pair disabled — skipping", "skip")
            continue

        if selected_labels and label not in selected_labels:
            _log(logging.DEBUG, f"[{label}] not in --pair selection — skipping", "skip")
            continue

        _log(logging.INFO, f"[{label}] processing pair", "start")

        try:
            local_s3 = build_local_s3_client(pair["local"])
            remote_s3 = assume_role_and_build_remote_client(
                pair["remote"], config["assume_role_session_name"]
            )
        except Exception as e:
            _log(logging.ERROR, f"[{label}] failed to initialise clients: {e} — skipping pair", "error")
            run_had_failures = True
            continue

        pair_summary = {}

        # Process incoming/
        incoming_counts = process_incoming(pair, local_s3, remote_s3, config, dry_run)
        pair_summary["incoming"] = incoming_counts

        # Process completed/
        completed_counts = process_completed_or_errors(
            pair, local_s3, remote_s3, config, db, "completed", dry_run
        )
        pair_summary["completed"] = completed_counts

        # Process errors/
        errors_counts = process_completed_or_errors(
            pair, local_s3, remote_s3, config, db, "errors", dry_run
        )
        pair_summary["errors"] = errors_counts

        # Check for failures
        for dir_name, cnts in pair_summary.items():
            if cnts.get("failed", 0) > 0:
                run_had_failures = True

        all_summaries.append((label, pair_summary))

        _log(logging.INFO, f"[{label}] pair done", "start")

    # --- Run summary ---
    _log(logging.INFO, "--- Run Summary ---", "start")
    for label, summary in all_summaries:
        for dir_name in ("incoming", "completed", "errors"):
            cnts = summary.get(dir_name, {})
            parts = []
            for metric in ("pushed", "copied", "pruned", "skipped", "failed"):
                val = cnts.get(metric, 0)
                if val > 0:
                    parts.append(f"{metric}={val}")
            if parts:
                _log(logging.INFO, f"  [{label}] {dir_name}: {', '.join(parts)}", "start")
            else:
                _log(logging.INFO, f"  [{label}] {dir_name}: no activity", "start")

    if run_had_failures:
        _log(logging.INFO, "Run completed with errors (exit 1)", "error")
    else:
        _log(logging.INFO, "Run completed successfully (exit 0)", "ok")

    db.close()
    lock.close()

    return EXIT_PARTIAL_FAILURE if run_had_failures else EXIT_OK


# ===================================================================
# CLI entry point
# ===================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="S3 Shuttle — relay files between local and remote S3 buckets."
    )
    parser.add_argument(
        "--config",
        default="./config.json",
        help="Path to the general config file (default: ./config.json).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform no mutating operations. Log what would happen.",
    )
    parser.add_argument(
        "--pair",
        action="append",
        metavar="LABEL",
        help="Process only the pair with this label (repeatable).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log_level from config.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Interactive debug mode: stdout only, emoji output. Respects --log-level.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
