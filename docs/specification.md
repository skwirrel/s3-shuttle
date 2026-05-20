# S3 Bucket Relay — Technical Specification

**Version:** 1.2
**Status:** Draft for implementation
**Target language:** Python 3.11+
**Intended audience:** Implementing programmer or coding agent

> **Changes since 1.1:** Added a `--debug` flag (forces DEBUG level, sends output to
> stdout only, decorates lines with emoji for interactive use). Specified that the SQLite
> state database is created automatically on first run.
>
> **Changes since 1.0:** Configuration split into two files (general settings vs.
> bucket definitions). Added a per-pair `provider_retention_days` gate so files are not
> prune-checked until they are old enough to have possibly been pruned remotely.

---

## 1. Overview

A scheduled Python tool that relays files between **local** AWS S3 buckets (accessed
with ordinary IAM access keys) and **remote** third-party S3 buckets (accessed via
STS `AssumeRole` with an External ID).

It exists because a third-party data provider wants files delivered into a bucket they
own, but the local IT team will only expose buckets *they* own, reachable with plain API
keys. This tool bridges that gap.

It is explicitly a **stopgap**. It should be simple, observable, and easy to retire once
the provider integration is handled natively (e.g. an AWS Lambda in the IT team's own
account).

---

## 2. Goals and Non-Goals

### Goals

- Relay files between multiple, independently configured local/remote bucket pairs.
- Handle three directories per pair, each with its own direction and rules.
- Be safe: never delete data on the strength of an ambiguous or failed API response.
- Be observable: verbose, level-controlled logging so an operator can see exactly what
  happened on any run.
- Be configurable entirely from JSON files: a general settings file plus a separate
  bucket-pair file. No admin panel, no database of config.

### Non-Goals (out of scope)

- A web admin panel or UI.
- A Cloudflare Worker implementation (this may follow later; see §16).
- Real-time, event-driven processing via SNS/SQS. This tool **polls** on a schedule.
- Optimising for very large objects. Files are expected to be small XML documents.

---

## 3. High-Level Architecture

The tool is a single Python program. Each invocation:

1. Loads and validates the general config file, then the bucket config file it points to.
2. Acquires an exclusive lock (so overlapping scheduled runs cannot collide).
3. Iterates every **enabled** bucket pair.
4. For each pair, processes three directories in turn: `incoming/`, `completed/`,
   `errors/`.
5. Writes a run summary to the log and exits with a status code reflecting success or
   failure.

Scheduling is **external** (cron, systemd timer, Windows Task Scheduler). The tool runs,
does one pass, and exits. A run every 1–5 minutes is expected.

A small **local SQLite database** holds per-file verification state. This is the tool's
only persistent state and is not part of configuration.

---

## 4. Technology Stack

| Concern              | Choice                                  |
|----------------------|-----------------------------------------|
| Language             | Python 3.11+                            |
| AWS access           | `boto3` (handles S3, STS, SigV4, pagination natively) |
| Local state          | `sqlite3` (Python standard library)     |
| Config               | `json` (standard library)               |
| Logging              | `logging` (standard library)            |
| Locking              | `fcntl.flock` on a lock file (POSIX), or equivalent |

Keep external dependencies to **`boto3` only**. Everything else is standard library.

Using `boto3` removes the manual request-signing, XML parsing, and pagination work that a
Cloudflare Worker implementation would have required. It also handles multipart transfers
transparently, so object size is not a practical concern.

---

## 5. Configuration

Configuration is split across **two JSON files**:

- A **general config file** (default `./config.json`, overridable with `--config`)
  holding tool-wide settings. It contains **no credentials**.
- A **bucket config file**, whose path is given by the `bucket_config_path` setting in
  the general config. It holds the bucket-pair definitions, **including credentials**.

Splitting them keeps the tool's behavioural knobs separate from the credential-bearing
data, lets the two files carry different filesystem permissions and version-control
treatment, and makes the bucket pairs easy to manage on their own.

### 5.1 General config file (`config.json`)

```json
{
  "bucket_config_path": "./buckets.json",
  "database_path": "./relay_state.db",
  "lock_file_path": "./relay.lock",
  "log_level": "INFO",
  "log_file": "./relay.log",
  "verification_interval_hours": 24,
  "mass_delete_threshold_percent": 20,
  "assume_role_session_name": "bucket-relay"
}
```

| Setting | Meaning |
|---|---|
| `bucket_config_path` | Path to the bucket config file (§5.2). A relative path is resolved against the general config file's own directory. |
| `database_path` | Location of the SQLite state database (§8). |
| `lock_file_path` | Lock file used to prevent overlapping runs (§12). |
| `log_level` | Default log level: `DEBUG`, `INFO`, `WARNING`, or `ERROR`. |
| `log_file` | Optional path for a file log, in addition to stdout. Omit for stdout only. |
| `verification_interval_hours` | Re-verification window for files past the retention age (§7.2, §9). |
| `mass_delete_threshold_percent` | Mass-delete safety valve threshold (§9 rule 5). |
| `assume_role_session_name` | `RoleSessionName` passed to `AssumeRole`. |

### 5.2 Bucket config file

Path is given by `bucket_config_path` in the general config. It contains the list of
bucket pairs.

```json
{
  "bucket_pairs": [
    {
      "label": "BMT TEP XML Ingestion (staging)",
      "enabled": true,
      "provider_retention_days": 30,
      "directories": {
        "incoming": "incoming/",
        "completed": "completed/",
        "errors": "errors/"
      },
      "local": {
        "bucket": "REPLACE_WITH_LOCAL_BUCKET_NAME",
        "region": "eu-west-2",
        "access_key_id": "REPLACE_WITH_LOCAL_ACCESS_KEY_ID",
        "secret_access_key": "REPLACE_WITH_LOCAL_SECRET_ACCESS_KEY"
      },
      "remote": {
        "assume_role_access_key_id": "REPLACE_WITH_IAM_USER_ACCESS_KEY_ID",
        "assume_role_secret_access_key": "REPLACE_WITH_IAM_USER_SECRET_ACCESS_KEY",
        "external_id": "tep-xml-upload-bmt",
        "role_arn": "arn:aws:iam::060011828759:role/tep-xml-ingestion-bmt-upload",
        "bucket": "tep-xml-ingestion-staging-bmt",
        "region": "eu-west-2"
      }
    }
  ]
}
```

### 5.3 Field notes

- **`bucket_pairs`** is a list; the tool supports many pairs, each fully independent.
- **`label`** is a human-readable identifier, used in logging and as part of the database
  key. It must be unique across pairs.
- **`enabled`** — if `false`, the pair is skipped entirely. Lets an operator pause a pair
  without deleting its config.
- **`provider_retention_days`** — the **minimum** time the remote provider guarantees to
  keep a file in `completed/` and `errors/` before it may be pruned. The tool will not
  prune-check a local file until it has reached this age, because a younger file cannot
  yet have been removed on the remote side (see §7.2). Set this to the provider's
  *guaranteed minimum* retention, not their typical or maximum figure. If omitted or set
  to `0`, no retention gate is applied and every file is checked, subject only to the
  verification interval. It is a per-pair value because each pair may talk to a different
  provider with a different retention policy. It does not apply to `incoming/`.
- **`directories`** — prefix names are configurable but default to `incoming/`,
  `completed/`, `errors/`. Always store them with a trailing slash.
- **`remote.assume_role_access_key_id` / `assume_role_secret_access_key`** — these are
  **not** credentials for the remote bucket. They belong to an IAM user that holds
  `sts:AssumeRole` permission for `remote.role_arn`. The tool calls `AssumeRole` with
  these and the `external_id` to obtain temporary credentials for the remote bucket.
  Label this clearly in any documentation so nobody hunts for direct remote-bucket keys
  that do not exist.
- The example above uses the real BMT **staging** identifiers (`role_arn`, `external_id`,
  `bucket`). They are infrastructure identifiers, not secrets, and are safe to keep in the
  spec for testing. The access keys are placeholders and must be supplied at deployment.
- The real bucket name is `tep-xml-ingestion-staging-bmt` with no spaces. Any spaces in
  source material are copy-paste artefacts; S3 bucket names cannot contain spaces.

---

## 6. Directory Model & Sync Semantics

Each bucket pair has three directories. They behave differently and must not be confused.

| Directory    | Direction        | Delete permission                         | Behaviour |
|--------------|------------------|--------------------------------------------|-----------|
| `incoming/`  | local → remote   | Delete on **local** (yes)                  | Push: copy local files up to remote, then delete the local copy once the upload is confirmed. The local bucket acts as the outbound queue. |
| `completed/` | remote → local   | **No delete on remote**; delete on local   | Pull: mirror remote files down to local. Prune local files once they no longer exist on the remote. |
| `errors/`    | remote → local   | **No delete on remote**; delete on local   | Identical behaviour to `completed/`. |

The provider has confirmed they will prune `completed/` and `errors/` on their side. The
tool mirrors that pruning down to the local bucket so the two sides stay consistent.

---

## 7. Detailed Sync Logic

For each pair, obtain credentials once per run:

1. Build a local S3 client from `local` credentials and region.
2. Call `sts.assume_role(RoleArn, RoleSessionName, ExternalId)` using the
   `assume_role_*` credentials.
3. Build a remote S3 client from the returned **temporary** credentials and remote
   region.

Do **not** call `AssumeRole` more than once per pair per run. Reuse the temporary
credentials across every operation for that pair.

### 7.1 `incoming/` — push (local → remote)

```
list local bucket under incoming/ (paginated)
for each object key:
    try:
        body          = local_s3.get_object(local_bucket, key)
        source_size   = body content length
        source_etag   = local object ETag
        remote_s3.put_object(remote_bucket, key, body)

        # verify before any delete
        head = remote_s3.head_object(remote_bucket, key)
        if head.ContentLength == source_size and head.ETag == source_etag:
            local_s3.delete_object(local_bucket, key)        # confirmed: safe to delete
            log INFO  "pushed and removed: <key>"
        else:
            log ERROR "verification mismatch: <key> — left in place for retry"
    except Exception as e:
        log ERROR "push failed: <key>: <e> — left in place for retry"
        continue   # next file; never abort the whole run for one file
```

Notes:

- The local file is the source of truth and the queue. A file still present in local
  `incoming/` simply means "not yet delivered".
- **Never delete the local file unless the remote upload is positively confirmed.**
- ETag comparison is valid for non-multipart uploads. `incoming/` files are small XML
  documents uploaded in a single PUT, so ETag comparison is correct here. If large files
  ever appear, verification must fall back to size plus an independently computed content
  hash.
- Re-uploading the same key on a later run is idempotent (it overwrites with identical
  content), so retries are safe.
- The retention gate (§7.2) does **not** apply to `incoming/`.

### 7.2 `completed/` and `errors/` — pull and prune (remote → local)

Run this logic once for `completed/` and once for `errors/`.

```
remote_keys = set( list remote bucket under <prefix>, fully paginated )   # see Safety §9
# the local listing must retain each object's LastModified for the age check below
local      = { key: LastModified } from listing local bucket under <prefix>, fully paginated
local_keys = set( local.keys )

# --- copy-down phase ---
for key in (remote_keys - local_keys):
    body = remote_s3.get_object(remote_bucket, key)
    local_s3.put_object(local_bucket, key, body)
    verify size and ETag match
    db.upsert(pair_label, prefix, key, last_verified = now_utc)
    log INFO "copied down: <key>"

# --- bulk verification refresh ---
# every key still present in the remote listing is, by definition, verified present
for key in remote_keys:
    db.upsert(pair_label, prefix, key, last_verified = now_utc)

# --- prune phase ---
delete_candidates = []
for key in local_keys:

    # retention gate: a file younger than the provider's guaranteed retention
    # cannot have been pruned remotely yet, so do not check it at all.
    age = now_utc - local[key]   # LastModified ~= when the relay copied the file down
    if provider_retention_days > 0 and age < provider_retention_days:
        log DEBUG "skip prune-check (younger than provider retention): <key>"
        continue

    # second gate: has this file been confirmed present recently?
    last_verified = db.get(pair_label, prefix, key)
    if last_verified is not None and (now_utc - last_verified) < verification_interval:
        log DEBUG "skip prune-check (verified recently): <key>"
        continue

    # old enough to possibly be gone, and not recently confirmed — HEAD to be sure
    status = remote_head_status(remote_s3, remote_bucket, key)
    if status == 404:
        delete_candidates.append(key)
    elif status == 200:
        db.upsert(pair_label, prefix, key, last_verified = now_utc)
        log DEBUG "still present remotely: <key>"
    else:
        log WARNING "inconclusive HEAD (status <status>) for <key> — keeping"

# --- mass-delete safety valve (see §9 rule 5) ---
if local_keys and (len(delete_candidates) / len(local_keys) * 100) > mass_delete_threshold_percent:
    log ERROR "mass-delete guard tripped: would delete <N> of <M> in <prefix> — prune skipped"
else:
    for key in delete_candidates:
        local_s3.delete_object(local_bucket, key)
        db.delete(pair_label, prefix, key)
        log INFO "pruned local (confirmed gone from remote): <key>"
```

Notes:

- **Retention gate.** A local `completed/` or `errors/` file younger than the pair's
  `provider_retention_days` is skipped before any database lookup or HEAD. The provider
  guarantees not to prune a file before that age, so a younger file must still exist
  remotely and checking it is wasted effort. This is the cheapest and largest filter: in
  normal operation most local files are younger than the retention period and cost
  nothing per run.
- **How age is measured.** From the local object's `LastModified`, which is when the
  relay copied the file down. That is slightly *later* than when the provider created the
  file on their side, so the age is biased very slightly low. That bias is safe: the gate
  will, if anything, hold a file in "skip" a few minutes longer than strictly necessary,
  never release it too early.
- **The remote listing** is used to *discover* files to copy down and to *refresh*
  verification state cheaply. A failed or truncated listing degrades performance only
  (some files just are not refreshed this run); it can never by itself cause a deletion.
- **The verification interval** is the second-line filter, for files that *are* past the
  retention age. A file still present in the remote listing is refreshed by the bulk
  step, so its interval never expires and it is never HEAD-checked. Only a file that is
  both old enough and absent from the listing goes stale, earns a HEAD, and is pruned on
  a definitive `404`.
- **Net effect:** in steady state the tool issues almost no HEAD requests. Young files
  are skipped by the retention gate; old-but-present files are skipped via the listing
  refresh; only old-and-absent files are HEAD-checked, and only a `404` deletes.

---

## 8. State Database (SQLite)

A single local SQLite file at `database_path`. It stores per-file verification
timestamps for `completed/` and `errors/` only. `incoming/` needs no database (the local
bucket itself is the queue).

The database is **created automatically on first run**: the tool opens `database_path`
(which, per `sqlite3.connect`, creates the file if it does not already exist) and then
runs `CREATE TABLE IF NOT EXISTS`. A fresh deployment therefore needs no manual database
setup. The *parent directory* of `database_path` must already exist; if it does not, the
tool exits with a fatal error (code `2`) and a clear message rather than guessing where
to put it.

### 8.1 Schema

```sql
CREATE TABLE IF NOT EXISTS file_verification (
    pair_label    TEXT NOT NULL,
    directory     TEXT NOT NULL,   -- e.g. 'completed/' or 'errors/'
    object_key    TEXT NOT NULL,
    last_verified TEXT NOT NULL,   -- ISO 8601 UTC, e.g. '2026-05-20T14:03:11Z'
    PRIMARY KEY (pair_label, directory, object_key)
);
```

### 8.2 Rules

- **Upsert** `last_verified = now` whenever the file is positively proven to exist
  remotely: it appeared in the remote listing, was just copied down, or returned `200`
  from a HEAD.
- **Delete** the row when the corresponding local file is pruned.
- A local file with no database row is treated as stale (it will be HEAD-checked once it
  is past the retention age). The state self-heals; the database does not need
  pre-seeding.
- Orphaned rows (a row whose local file no longer exists) are harmless. They may
  optionally be reaped by deleting rows whose `object_key` is not in `local_keys`.
- File **age** for the retention gate is taken from the S3 object's `LastModified`, not
  from this database.

The database is also a useful audit trail when debugging. An optional second table
recording per-run summaries may be added but is not required.

---

## 9. Critical Safety Rules

These rules are not optional. They are the difference between a relay and a data-loss
incident.

1. **`incoming/`: confirm before delete.** Never delete a local file until the remote
   `PUT` is confirmed by a HEAD showing matching size and ETag.

2. **`completed/` / `errors/`: delete only on a definitive 404.** A local file is pruned
   **only** when a HEAD against the remote object returns HTTP `404`. Every other outcome
   means *keep the file*: `200`, `403`, any `5xx`, a `429` throttle, a timeout, or a
   network error. Do **not** write the delete condition as `status != 200`; it must be
   `status == 404`.

3. **Never delete based on a listing.** Absence of a key from a `ListObjectsV2` response
   is **not** proof the object is gone. A truncated, errored, or empty listing looks
   identical to "the remote has nothing". The remote listing may drive copy-down and
   verification refresh; it must never drive deletion.

4. **The 403-vs-404 quirk.** If the assumed role lacks `s3:ListBucket` on the remote
   bucket, S3 returns `403` for a missing object instead of `404`, to avoid leaking
   object existence. Under rule 2 that means pruning silently never happens. The assumed
   role's policy **must** grant `s3:ListBucket` on the remote bucket so that genuinely
   missing objects return `404`. (This failure mode is at least safe: it under-prunes, it
   never wrongly deletes.)

5. **Mass-delete safety valve.** If a single run's prune phase would delete more than
   `mass_delete_threshold_percent` of the local files in a directory, abort the prune for
   that directory, delete nothing, and log an `ERROR`. A sudden mass deletion is almost
   always a bug, not a real event.

6. **One file's failure is not the run's failure.** Catch exceptions per file and per
   pair. Log and continue. A single bad object must not prevent other files or other
   pairs from processing.

7. **`AssumeRole` once per pair per run.** Obtain temporary credentials once and reuse
   them; do not re-assume per file.

### Detecting a 404 with boto3

`head_object` on a missing key raises `botocore.exceptions.ClientError`. Inspect the HTTP
status, do not pattern-match on messages:

```python
from botocore.exceptions import ClientError

def remote_head_status(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return 200
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"]   # 404, 403, 5xx, ...
```

---

## 10. Authentication & AWS Permissions

### 10.1 AssumeRole flow

```python
sts = boto3.client(
    "sts",
    aws_access_key_id=remote["assume_role_access_key_id"],
    aws_secret_access_key=remote["assume_role_secret_access_key"],
    region_name=remote["region"],
)
resp = sts.assume_role(
    RoleArn=remote["role_arn"],
    RoleSessionName=settings["assume_role_session_name"],
    ExternalId=remote["external_id"],
)
creds = resp["Credentials"]
remote_s3 = boto3.client(
    "s3",
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
    region_name=remote["region"],
)
```

### 10.2 Permissions required

**Local IAM user** (`local.access_key_id`), on the local bucket:

- `s3:ListBucket` on the bucket
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on objects under all three prefixes
  (`GetObject` also covers `HeadObject`)

**Remote — the AssumeRole IAM user** (`remote.assume_role_*`):

- `sts:AssumeRole` on `remote.role_arn`

**Remote — the assumed role itself** (configured by the provider, documented here for
completeness):

- `s3:PutObject` on `incoming/*`
- `s3:ListBucket` on the bucket (see §9 rule 4 — required for correct `404` behaviour)
- `s3:GetObject` on `completed/*` and `errors/*`

---

## 11. Logging & Observability

Use the standard `logging` module. Logging is a first-class requirement: an operator
should be able to read a run's log and know exactly what happened and why.

### 11.1 Output destinations

- **Normal mode:** log to `log_file` if it is set, and also to stdout. Lines are plain
  text with no emoji, so the file stays easy to grep and parse.
- **`--debug` mode:** log to **stdout only**. The file handler is not attached even if
  `log_file` is set, so an interactive debug session never pollutes the persistent log
  file. Output is decorated with emoji (see §11.3).
- Every line carries an ISO 8601 timestamp, the level, the pair label, and the directory
  where applicable.

### 11.2 Levels

The level is `log_level` from the general config, overridable with `--log-level`.

`--debug` is a convenience composite for interactive use: it forces the level to `DEBUG`
regardless of `log_level` or `--log-level`, switches output to stdout-only, and turns on
emoji decoration. If `--debug` and `--log-level` are both supplied, `--debug` wins.

| Level    | Used for |
|----------|----------|
| `DEBUG`  | Every file considered and every decision: skipped (younger than retention), skipped (verified recently), copied, HEAD result, push/verify outcomes. Every AWS call. |
| `INFO`   | Run start/end, per-pair start/end, per-directory summary counts, each push/copy/prune action. |
| `WARNING`| Retries, inconclusive HEAD results, files left in place for retry, anything unexpected but handled. |
| `ERROR`  | Verification mismatches, the mass-delete guard tripping, per-file or per-pair failures, config problems. |

### 11.3 Emoji decoration (`--debug` only)

In `--debug` mode each line is prefixed with an emoji marking the kind of event, as a
quick visual scan aid. Emoji appear **only** in `--debug` output; the file log and
normal-mode stdout stay plain text so they remain machine-parseable. Suggested mapping,
which the implementer may adjust:

| Emoji | Event |
|-------|-------|
| 🏁 | Run or pair start/end, run summary |
| 🔒 | Lock acquired, or another run already holds the lock |
| ⬆️ | Pushed a file up to the remote (`incoming/`) |
| ⬇️ | Copied a file down from the remote (`completed/`, `errors/`) |
| 🔍 | HEAD check against the remote |
| 🗑️ | Pruned a local file |
| ⏭️ | Skipped a file (younger than retention, or verified recently) |
| ✅ | Verification passed |
| ⚠️ | Warning |
| ❌ | Error |
| 🧪 | A `--dry-run` action that was logged but not actually performed |

### 11.4 Run summary

At the end of every run, emit an `INFO` **run summary**: per pair and directory, counts
of files pushed, copied down, pruned, skipped, and failed.

---

## 12. Error Handling & Retries

- **Per-file isolation.** Wrap each file's processing in try/except. Log and move on.
- **Per-pair isolation.** A pair that fails to initialise (e.g. `AssumeRole` fails) is
  logged as an `ERROR` and skipped; other pairs still run.
- **Implicit retry.** A failed `incoming/` push leaves the local file in place, so the
  next scheduled run retries it automatically. The same is true for a failed copy-down.
- **Exit codes:**
  - `0` — run completed, no errors.
  - `1` — run completed but one or more files or pairs failed (operator should look).
  - `2` — fatal: config invalid, lock could not be acquired, or the run could not start.
- **Concurrency.** Acquire an exclusive lock on `lock_file_path` at startup. If the lock
  is held (a previous run is still going), log an `INFO` and exit `0` without doing
  anything.
- An optional alerting hook (email or webhook) on a non-zero exit may be added later; for
  now, rely on the exit code and log so external monitoring or the cron wrapper can catch
  failures.

---

## 13. Command-Line Interface

```
relay.py [options]

  --config PATH        Path to the GENERAL config file (default: ./config.json).
                       The bucket config file is located via that file's
                       bucket_config_path setting.
  --dry-run            Perform no mutating operations. Log every PUT, DELETE and
                       copy that WOULD happen, but do not execute them. Listing,
                       HEAD and GET (read-only) calls still run normally.
  --pair LABEL         Process only the pair with this label (repeatable).
  --log-level LEVEL    Override log_level (DEBUG|INFO|WARNING|ERROR).
  --debug              Interactive debug mode: force DEBUG level, send all output
                       to stdout only (no log file, even if log_file is set), and
                       decorate lines with emoji. Overrides --log-level. See §11.
```

`--dry-run` is important. The first deployment and any config change should be exercised
with `--dry-run` before the tool is allowed to delete anything.

---

## 14. Security Considerations

- **The bucket config file contains live AWS credentials in plaintext.** It must have
  restrictive filesystem permissions (`chmod 600`) and must never be committed to version
  control (add it to `.gitignore`; commit a `buckets.example.json` with placeholders
  instead).
- **The general config file contains no credentials.** It can be treated less strictly
  and may, if desired, be committed to version control (review it first).
- Consider, as a follow-up, allowing each secret in the bucket config to be supplied via
  an environment variable so the file itself holds references rather than raw secrets.
  Not required for the first version.
- The SQLite database holds object keys and timestamps only. It is not sensitive, but
  keep it with the deployment and out of version control.
- Never log credential values. Logging object keys, sizes, and ETags is fine; logging
  access keys, secret keys, or session tokens is not.

---

## 15. Acceptance Criteria

The implementation is complete when:

1. Configuration is split between a general config file and a separate bucket config
   file, with the general file naming the location of the bucket file.
2. The bucket config file drives multiple independent bucket pairs.
3. `incoming/` files are pushed to the remote and removed locally **only** after a
   confirmed, verified upload.
4. `completed/` and `errors/` files are copied down to the local bucket.
5. Local `completed/` and `errors/` files younger than the pair's
   `provider_retention_days` are not prune-checked at all.
6. Local `completed/` and `errors/` files are pruned **only** on a definitive remote
   `404`, with the mass-delete guard enforced.
7. A failed or truncated remote listing never causes a deletion.
8. The SQLite database correctly suppresses redundant HEAD checks within
   `verification_interval_hours`.
9. The SQLite database is created automatically on first run; no manual setup is
   required.
10. `--dry-run` performs no mutating operations.
11. `--debug` forces DEBUG level, sends output to stdout only, and is the only mode that
    emits emoji.
12. One bad file or one bad pair does not abort the run.
13. Logs at `DEBUG` clearly explain every decision; a run summary is emitted at `INFO`.
14. Exit codes correctly distinguish clean, partial-failure, and fatal outcomes.
15. The tool runs cleanly against the BMT staging bucket pair.

---

## 16. Out of Scope / Future Work

- Cloudflare Worker implementation of the same logic (the original deployment idea; the
  Python version is being built first because it is easier to oversee).
- A web admin panel for managing bucket pairs (replaced for now by the bucket config
  file).
- Real-time, event-driven processing via the provider's SNS topic.
- Alerting integrations (email/webhook) on failure.
- Encryption of secrets at rest within the bucket config file.

This tool is a deliberate stopgap. The intended long-term solution is for the IT team to
run a native integration (e.g. an AWS Lambda triggered by S3 events) in their own
account, removing the need to relay through a third location at all.
