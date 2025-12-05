# Environment variables used by this project

This file documents environment variables that the pipeline reads (mainly from `run_pipeline.py` and helpers in `src/`). It is a concise, accurate reference for local development and for running in AWS Lambda.

## Core notes

- `RUN_MODE` controls behaviour: `local`, `local_s3`, or `remote_s3`. See below for how credentials/S3 are selected.
- `TRANS_CONFIG` may be a JSON string or a path to a JSON file; see `src/utils.get_trans_config`.

## AWS credentials and `AWS_PROFILE`

- When `RUN_MODE=local_s3` the code reads `AWS_PROFILE` from the environment and creates a boto3 `Session` with `boto3.Session(profile_name=aws_profile)` (see `run_pipeline.py`). That Session is used to build an S3 client.
- When `RUN_MODE=remote_s3` the code calls `boto3.client('s3')` which uses the default credential resolution chain (Lambda execution role, environment variables, shared credentials default profile, etc.).
- If you want to force a particular profile in local runs either set `AWS_PROFILE` in `.env` or export it in your shell before running:

```powershell
$env:AWS_PROFILE = 'ctd-pa-discovery'
python .\run_pipeline.py
```

## Main variables (summary)

- `RUN_MODE` (required): `local`, `local_s3`, or `remote_s3`. Default in code: `remote_s3`.
- `AWS_PROFILE` (required for `local_s3`): AWS CLI profile name used to create a `boto3.Session(profile_name=...)`.
- `S3_OUTPUT_DIR`: S3 prefix where outputs are uploaded. Default: `json_outputs`.
- `TRANS_CONFIG`: JSON string or path to transformation configuration. See `src/utils.get_trans_config`.
- `TRANSFER_REGISTER_FILENAME`: Filename for deduplication transfer register (default: `uploaded_records_transfer_register.json`).

## Local / filesystem variables

- `CTD_DATA_INPUT`: Path to triggers/XML folder (used by `src.utils.get_triggers_dir`).
- `CTD_DATA_INTERMEDIATE`: Directory to write intermediate JSON files when running locally.
- `CTD_DATA_OUTPUT`: Local output directory for saved tarballs (if `run_mode == 'local'`).
- `CTD_TRIGGER_JSON`: Path to a local trigger JSON when running locally.

## Behavioural and debug flags

- `TEST_MODE`: when truthy the pipeline will look under `S3_TEST_FOLDER` for inputs/outputs.
- `S3_TEST_FOLDER`: test folder prefix used in `TEST_MODE`.
- `S3_USE_LEVEL_SUBFOLDERS`: when truthy the pipeline groups outputs into subfolders per catalogue level.
- `PROGRESS_VERBOSE`: toggles progress printing.
- `DEBUG_TRANSFORMERS` / `SAVE_INTERMEDIATE_JSON`: when truthy the pipeline writes pre/post transform JSON into `CTD_DATA_INTERMEDIATE` (local only).
- `CTD_LOG_LEVEL`: logging level (e.g., `DEBUG`, `INFO`).

## Replica / digitised handling

- `REPLICA_METADATA_PREFIX`: S3 prefix used to find replica metadata JSON objects (default in code: `metadata`).
- `REPLICA_FILENAME_PREFIX`: prefix used to list replica files.
- `FILTER_REPLICA_METADATA`: when truthy, records that have replica metadata will be treated as digitised and placed into separate `_digitised` tarballs.

## Filtering / exclusions

- `FILTER_IAID`: when set and running in `local` mode, the pipeline filters the XML input to that IAID only (debug/testing).
- `BFI_EXCLUSION_CODE`: when set to a code (e.g., `2870`) the pipeline will record and skip records whose `heldBy[0].xReferenceCode` equals this value; in local modes the excluded JSONs are saved under `CTD_DATA_INTERMEDIATE/bfi_exclusion_json_files/`.

## References (definitive lists / exclusions)

- `VALID_DEPT_CODES_KEY`: S3 key or local path under `data/references` for the definitive department codes JSON (default in code: `references/valid_dept_codes.json`).
- `VALID_DEPT_CODES_BUCKET`: optional bucket override for reference files (defaults to the event bucket when unset).
- `YNAMING_EXCLUSIONS_KEY`: S3 key or local path for Y-naming exclusion patterns (`references/ynaming_exclusions.json` by default).

## Where these are used in the codebase

- `run_pipeline.py` — primary reader of environment variables and orchestrator. It uses `AWS_PROFILE` to build a `boto3.Session` when `RUN_MODE='local_s3'`.
- `src/utils.py` — `get_trans_config()` parses `TRANS_CONFIG`.
- `src/config_loader.py` — `UniversalConfig` loads `.env` for local development when the Lambda environment variables are not present.
- `src/transformers.py` — uses values passed from the pipeline (e.g., definitive refs, exclusions), but does not directly read `AWS_PROFILE`.

## Practical recommendations

- For reliable local S3 operations set `RUN_MODE=local_s3` and set `AWS_PROFILE` in `.env` or in your shell. The code creates a Session with that profile so the chosen credentials are used.
- For CI or Lambda deployments use `RUN_MODE=remote_s3` and rely on the Lambda execution role (no `AWS_PROFILE` required).
- Keep sensitive values out of the repo `.env`; prefer local-only `.env` and a tracked `.env.template` (already present).

If you want, I can also generate a one-line mapping file that shows which files reference each env var (grep-style) so you can verify usages across the repo. Tell me if you want that next.
# Environment Variables Reference

## Core Pipeline Configuration

### `RUN_MODE` (required)
- **Values**: `local`, `local_s3`, `remote_s3`
- **Default**: `remote_s3`
- **Description**: Execution mode for the pipeline
  - `local`: Local filesystem only, no S3
  - `local_s3`: S3 with AWS profile (requires AWS_PROFILE)
  - `remote_s3`: S3 with IAM role (Lambda execution)

### `AWS_PROFILE` (required for local_s3 mode)
- **Example**: `ax-prod`
- **Description**: AWS CLI profile name for S3 access in local_s3 mode

### `S3_OUTPUT_DIR`
- **Default**: `json_outputs`
- **Description**: S3 folder for output tarballs and manifest

### `TRANS_CONFIG` (required)
- **Description**: Path to transformation config JSON file or JSON string
- **Example**: `config/transformation_config.json`

### `TRANSFER_REGISTER_FILENAME`
- **Default**: `uploaded_records_transfer_register.json`
- **Description**: Filename for deduplication transfer register stored in S3

---

## Local Development & Testing

### `FILTER_IAID` (local mode only)
- **Example**: `C12345678`
- **Description**: Filter XML to single IAID for fast testing. Only works in `local` mode.
- **Use case**: Test transformations on one record without processing entire tree

### `SAVE_PRE_TRANSFORM`
- **Values**: `true`, `false`
- **Default**: `false`
- **Description**: Save JSON before any transformations (raw XML→JSON conversion)
- **Output dir**: `PRE_TRANSFORM_DIR` or `data/intermediate/pre_transform/`

### `SAVE_POST_NEWLINE`
- **Values**: `true`, `false`
- **Default**: `false`
- **Description**: Save JSON after newline→`<p>` transformation
- **Output dir**: `POST_NEWLINE_DIR` or `data/intermediate/post_newline/`

### `SAVE_POST_YNAMING`
- **Values**: `true`, `false`
- **Default**: `false`
- **Description**: Save JSON after Y-naming transformation (final output)
- **Output dir**: `POST_YNAMING_DIR` or `data/intermediate/post_ynaming/`

### `PRE_TRANSFORM_DIR`
- **Default**: `data/intermediate/pre_transform`
- **Description**: Custom directory for pre-transformation JSON files

### `POST_NEWLINE_DIR`
- **Default**: `data/intermediate/post_newline`
- **Description**: Custom directory for post-newline JSON files

### `POST_YNAMING_DIR`
- **Default**: `data/intermediate/post_ynaming`
- **Description**: Custom directory for post-ynaming JSON files

---

## Other Configuration

### `SAVE_INTERMEDIATE_JSON`
- **Values**: `true`, `false`
- **Default**: `true`
- **Description**: Save converted XML→JSON files to intermediate directory

### `CTD_DATA_INTERMEDIATE`
- **Default**: `data/intermediate`
- **Description**: Base directory for intermediate files

### `CTD_LOG_LEVEL`
- **Values**: `DEBUG`, `INFO`, `WARNING`, `ERROR`
- **Default**: `DEBUG`
- **Description**: Logging verbosity level

### `USE_LEVEL_SUBFOLDERS`
- **Values**: `true`, `false`
- **Default**: `true`
- **Description**: Organize output by catalogue level (FONDS, SERIES, etc.)

### `MERGE_XML`
- **Values**: `true`, `false`
- **Default**: `false`
- **Description**: Merge multiple XML files before processing

### `CTD_TRIGGER_JSON`
### `ENABLE_REPLICA_METADATA`
- **Values**: `1`, `true`, `y`, `0`, `false`
- **Default**: `0`
- **Description**: When truthy, enables replica metadata enrichment transformer.

### `REPLICA_METADATA_BUCKET`
- **Default**: Uses ingest event bucket if unset
- **Description**: S3 bucket containing per-record replica metadata JSON files named `<IAID>.json`.

### `REPLICA_METADATA_PREFIX`
- **Default**: `replica`
- **Description**: Prefix (folder) inside `REPLICA_METADATA_BUCKET` where metadata JSON files reside.

- **Default**: `trigger.json`
- **Description**: Path to trigger JSON for local execution

---

## Example .env for Local Testing with Single IAID

```bash
# Run mode
RUN_MODE=local
AWS_PROFILE=ax-prod

# Filter to single record for fast testing
FILTER_IAID=C12345678

# Save all transformation stages
SAVE_PRE_TRANSFORM=true
SAVE_POST_NEWLINE=true
SAVE_POST_YNAMING=true

# Use custom directories (optional)
PRE_TRANSFORM_DIR=data/test_outputs/pre_transform
POST_NEWLINE_DIR=data/test_outputs/post_newline
POST_YNAMING_DIR=data/test_outputs/post_ynaming

# Logging
CTD_LOG_LEVEL=DEBUG

# Transformation config
TRANS_CONFIG=config/transformation_config.json
```
