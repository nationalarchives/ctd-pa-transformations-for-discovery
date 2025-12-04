# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-12-04

### Added

- **Y-Naming Exclusions**: Implemented position-aware Y-naming exclusions to prevent incorrect transformations in contextual phrases (e.g., "(their ref: PO.BAT)"). Exclusions are loaded from `references/ynaming_exclusions.json`.
- **BFI Record Handling**: Added functionality to save excluded BFI records to `data/intermediate/bfi_exclusion_json_files` during local runs for easier review.
- **Digitised Record Separation**: Records identified as "digitised" are now grouped and placed into separate tarballs with a `_digitised` suffix, controlled by the `FILTER_REPLICA_METADATA` environment flag.

### Fixed

- **Y-Naming Transformation**: Corrected an issue where Y-naming exclusions would incorrectly prevent transformation of other valid department codes within the same field.
- **IndexError in JSON conversion**: Fixed an `IndexError: list index out of range` when accessing `heldBy` field during BFI record checks.

## [1.0.0] - 2025-12-03

### Added

- **Initial Pipeline**: Established the core pipeline for converting XML records to JSON, applying a series of transformations, and packaging the output.
- **Transformation Engine**:
    - `YNamingTransformer`: Applies "Y" prefix to department codes based on a definitive reference list.
    - `ReplicaDataTransformer`: Attaches replica metadata to records.
    - `NewlineToPTransformer`: Converts newlines to `<p>` tags in specified fields.
- **Deduplication**: Implemented a transfer register (`uploaded_records_transfer_register.json`) to prevent reprocessing of already uploaded records.
- **Deployment and Execution**:
    - Introduced a three-mode execution system (`local`, `local_s3`, `remote_s3`) controlled by the `RUN_MODE` environment variable.
    - Added build and deployment scripts for AWS Lambda, including Lambda layer creation and SSM parameter management.
    - Implemented Lambda timeout monitoring to prevent incomplete runs.
- **Output Management**:
    - Tarball creation is now batched (max 10,000 files per tar) to handle large datasets.
    - Implemented super-tarball functionality to bundle all level-specific tarballs into a single archive for easier download.
    - Outputs are organized into a structured folder hierarchy in S3 based on the input file name.
- **Configuration and Tooling**:
    - Centralized transformation configuration loading from environment variables or a `TRANS_CONFIG` file.
    - Added helper scripts for setting up a Python virtual environment.
    - Integrated detailed logging and progress indicators for better diagnostics.
- **Security**:
    - Added support for GPG signing of commits.
    - Removed `.env` file from version control.

### Changed

- **Configuration**: Moved transformation configuration from AWS SSM Parameter Store to an environment variable-based system for greater flexibility.
- **Creator Information**: Updated `ReplicaDataTransformer` to improve `creatorName` fallback logic.
- **Closure Status**: Enabled "U" status for records held by Parliament.

### Fixed

- **ParentId Performance**: Improved performance related to ParentId processing.
- **S3 Paths**: Corrected S3 input directory and folder path configurations.
- **Build Process**: Fixed issues in build scripts to ensure `src` files are correctly included in the Lambda layer.
- Minor bug fixes related to whitespace, end-of-file issues, and tar file entry sanitization.
