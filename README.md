# S3 Shuttle

A scheduled Python tool that relays files between local and remote S3 buckets, bridging the gap when a third-party provider requires files in a bucket they own but your infrastructure only exposes buckets you control.

Each invocation does one pass and exits. Scheduling is external (cron, systemd timer, etc.) with runs expected every 1-5 minutes.

## How it works

S3 Shuttle processes **bucket pairs**, each with three directories:

| Directory | Direction | Behaviour |
|-----------|-----------|-----------|
| `incoming/` | local -> remote | Push files to the remote bucket, delete locally only after verified upload |
| `completed/` | remote -> local | Mirror remote files down, prune local copies once confirmed gone from remote |
| `errors/` | remote -> local | Same as `completed/` |

Remote buckets are accessed via STS AssumeRole with an External ID. Local buckets use standard IAM access keys.

## Requirements

- Python 3.11+
- `boto3`

```
pip install -r requirements.txt
```

## Configuration

Configuration is split into two JSON files:

1. **General config** (`config.json`) -- tool-wide settings, no credentials
2. **Bucket config** (`buckets.json`) -- bucket pair definitions including credentials

Copy the examples to get started:

```
cp config.example.json config.json
cp buckets.example.json buckets.json
chmod 600 buckets.json
```

Edit `buckets.json` with your actual bucket names, regions, and credentials. See `docs/specification.md` for full field documentation.

## Usage

```
python s3_shuttle.py [options]

  --config PATH        Path to the general config file (default: ./config.json)
  --dry-run            Log what would happen without making any changes
  --pair LABEL         Process only this pair (repeatable)
  --log-level LEVEL    Output to stdout at this level (DEBUG|INFO|WARNING|ERROR)
  --debug              Stdout only with emoji decoration, for interactive use
```

### First run

Always start with `--dry-run` to verify your configuration:

```
python s3_shuttle.py --dry-run --log-level INFO
```

### Logging

By default the tool is silent on stdout. Output destinations:

- **`--log-level`** -- sends output to stdout at the specified level
- **`--debug`** -- stdout with emoji decoration for interactive sessions
- **`log_file`** in config -- writes to a file at the configured `log_level`

With no flags and no `log_file` configured, the tool runs silently (exit code only).

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Run completed successfully, or lock held by another run |
| 1 | Run completed but one or more files or pairs failed |
| 2 | Fatal: invalid config, missing database directory, or similar |

## Safety

- Local `incoming/` files are never deleted until the remote upload is verified by a HEAD check matching size and ETag
- Local `completed/`/`errors/` files are pruned only on a definitive remote 404 -- never on a failed listing or ambiguous response
- A mass-delete safety valve aborts pruning if it would remove more than a configurable percentage of files in a single directory
- One bad file or pair never aborts the entire run

## Project structure

```
s3_shuttle.py          # The tool
config.example.json    # General config template (safe to commit)
buckets.example.json   # Bucket pair template with placeholders (safe to commit)
requirements.txt       # boto3
docs/specification.md  # Full technical specification
```

## License

Copyright 2026 Ben Jefferson. Licensed under the Apache License 2.0 -- see [LICENSE](LICENSE).
