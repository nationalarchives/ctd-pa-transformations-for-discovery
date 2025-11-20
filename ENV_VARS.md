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

---

## Quick Testing Workflow

1. **Find IAID** from XML file you want to test
2. **Set environment variables**:
   ```bash
   RUN_MODE=local
   FILTER_IAID=C12345678
   SAVE_PRE_TRANSFORM=true
   SAVE_POST_NEWLINE=true
   SAVE_POST_YNAMING=true
   ```
3. **Run pipeline**: `python src/run_pipeline.py`
4. **Inspect outputs** in `data/intermediate/` subdirectories

This processes only one record in seconds instead of hours!
