from asyncio.log import logger
import boto3
import sys
import json
import os
import logging
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime
import time
import json, tarfile, io

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from src.config_loader import UniversalConfig
from src.utils import find_key, merge_xml_files, log_timing, _load_json_file    
from src.utils import load_manifest, save_manifest, filter_new_records, update_manifest_with_records
from src.transformers import NewlineToPTransformer, YNamingTransformer, convert_to_json


# load in the environment variables from .env in repo root (done by AWS Lambda automatically)
if not os.getenv("AWS_LAMBDA_FUNCTION_NAME") and not os.getenv("AWS_EXECUTION_ENV"):
    UniversalConfig(env_file=repo_root / ".env")

# Set run mode with validation
# Supported modes:
#   - local: Download/upload to local disk only
#   - local_s3: Download/upload to S3 using AWS profile (local development with S3)
#   - remote_s3: Download/upload to S3 using IAM role (Lambda/AWS execution)
VALID_RUN_MODES = ["local", "local_s3", "remote_s3"]
run_mode = os.getenv("RUN_MODE", "remote_s3").strip().lower()

if run_mode not in VALID_RUN_MODES:
    raise ValueError(
        f"Invalid RUN_MODE '{run_mode}'. Must be one of: {', '.join(VALID_RUN_MODES)}"
    )

# S3 client configuration based on run mode
if run_mode == "local_s3":
    # Local development with S3: requires AWS profile
    aws_profile = os.getenv("AWS_PROFILE")
    if not aws_profile:
        raise ValueError("RUN_MODE='local_s3' requires AWS_PROFILE environment variable")
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')
    logger.info("Using S3 with AWS profile: %s", aws_profile)
elif run_mode == "remote_s3":
    # AWS Lambda/remote execution: uses IAM execution role
    s3 = boto3.client('s3')
    logger.info("Using S3 with IAM execution role")
else:
    # local mode: no S3 client needed
    s3 = None
    logger.info("Running in local mode (no S3)")

# Configure module logger (level can be set with CTD_LOG_LEVEL env var)
_log_level = os.getenv("CTD_LOG_LEVEL", "DEBUG").upper()
_numeric_level = getattr(logging, _log_level, logging.INFO)
logging.basicConfig(level=_numeric_level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Set up file handler if running in local modes
if run_mode in ["local", "local_s3"]:
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(logs_dir / "pipeline.log")
    file_handler.setLevel(_numeric_level)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    logger.addHandler(file_handler)


# define pipeline configuration separately from event
transformations_str = os.environ.get("TRANS_CONFIG")
transformation_config = _load_json_file(transformations_str, logger=logger)

# Manifest configuration
manifest_filename = os.getenv("MANIFEST_FILENAME", "uploaded_records_manifest.json")

def lambda_handler(event, context):
    
    # 1. Get bucket and key from event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    # get the input directory from the event itself
    input_dir = Path(key).resolve().parent
    input_dir.mkdir(parents=True, exist_ok=True)

    # the output dir is set in the env vars
    output_dir = Path(os.environ.get('S3_OUTPUT_DIR', ''))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Validate key exists and is an XML file
    if not key or not key.endswith(".xml"):
        logger.error("Invalid or missing file key in event: key=%s", key)
        return {"status": "error", "message": "Invalid or missing XML file key in event"}
    
    # Load manifest for deduplication (only in S3 modes)
    manifest = None
    if run_mode in ["local_s3", "remote_s3"]:
        if not manifest_filename:
            logger.error("MANIFEST_FILENAME environment variable is not set")
            return {
                "status": "error",
                "message": "MANIFEST_FILENAME environment variable is required"
            }
        try:
            manifest = load_manifest(manifest_filename, s3, bucket, output_dir, logger)
            num_existing = len(manifest.get('records', {}))
            logger.info("Loaded manifest with %d existing records", num_existing)
        except Exception as e:
            logger.exception("FATAL: Failed to load manifest - cannot proceed without deduplication")
            return {
                "status": "error",
                "message": f"Failed to load deduplication manifest: {str(e)}"
            }
    else:
        manifest = None
        logger.info("Running in local mode - skipping manifest/deduplication")

    # we can view any transformation intermediates in this dir (not in AWS Lambda)
    intermediate_dir = Path(os.environ.get('CTD_DATA_INTERMEDIATE', ''))
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    # whether to use subfolders in S3 output
    truthy_chars = ("1", "true", "yes", "y")
    use_level_subfolders = os.getenv("USE_LEVEL_SUBFOLDERS", "true").strip().lower() in truthy_chars
    logger.info("S3 folders - Input: %s, Output: %s, Level subfolders: %s", 
           input_dir, output_dir, use_level_subfolders)

    # Get merge flag from environment
    _merge_env = os.getenv("MERGE_XML")
    if _merge_env is None:
        merge_xml = False
    else:
        merge_xml = str(_merge_env).strip().lower() in truthy_chars

    # 2. Determine source: local file or S3 download
    xml_path_to_convert = None
    tmp_path = None

    # Download from S3 in S3 modes (local_s3 or remote_s3)
    if run_mode in ["local_s3", "remote_s3"]:
        # Download from S3 to a temp file
        tmp_path = input_dir / f"tmp_{Path(key).name}"
        try:
            s3.download_file(Bucket=bucket, Key=key, Filename=str(tmp_path))
            xml_path_to_convert = tmp_path
            logger.info("Downloaded %s from S3 bucket %s", key, bucket)
        except ClientError as e:
            logger.exception("Error downloading %s from S3: %s", key, 
                             e.response.get('Error', {}).get('Code'))
            return {"status": "error", "message": f"Error downloading {key} from S3: "
                    f"{e.response.get('Error', {}).get('Code')}"}
    else:
        # Local mode: file must exist locally
        logger.info("Running in local mode - using local file system")
    if merge_xml:
        logger.info("Merging XML files in %s into one for conversion...", input_dir)
        date = Path().stat().st_mtime
        merged_output_path = input_dir / f"merged_input_{date}.xml"
        merge_xml_files(
            triggers_dir=input_dir,
            output_path=merged_output_path
        )
        xml_path_to_convert = merged_output_path
        logger.info("Finished merging XML files into: %s", merged_output_path)
    else:
        xml_path_to_convert = input_dir / Path(key).name
    
    print(f"input_dir: {input_dir}")
    if not xml_path_to_convert.exists() or not xml_path_to_convert.is_file():
        logger.error("Local XML file not found: %s", xml_path_to_convert)
        return {"status": "error", "message": f"Local XML file not found: "
                f"{xml_path_to_convert}"}
    
    # 3. Convert XML to JSON
    try:
        with log_timing(f"XML to JSON conversion ({xml_path_to_convert.name})", logger):
            records = convert_to_json(xml_path=str(xml_path_to_convert), 
                                      output_dir=str(output_dir))
        logger.info("Converted %d records", len(records))
        
        # Filter out records that have already been uploaded
        if manifest is not None:
            original_count = len(records)
            records = filter_new_records(records, manifest, logger)
            filtered_count = original_count - len(records)
            if filtered_count > 0:
                logger.info("Filtered out %d already-uploaded records, %d new records remaining", 
                           filtered_count, len(records))
        
        for f in output_dir.iterdir():
            logger.debug("  %s", f.name)
    except Exception:
        logger.exception("Error converting XML to JSON: %s", xml_path_to_convert)
        return {"status": "error", "message": f"Error converting XML to JSON for {xml_path_to_convert}"}
    finally:
        # Clean up temp file if we downloaded from S3
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                logger.exception("Failed to remove temp file %s", tmp_path)

    # 4. Load the converted JSON (convert_xml_to_json should have written it)
    converted_xml_to_json_files = records

    # save the converted files to disk to investigation - remove this in the future
    save_intermediate = os.getenv("SAVE_INTERMEDIATE_JSON", "true").strip().lower() in truthy_chars
    if save_intermediate and run_mode in ["local", "local_s3"]:
        for filename, _file in converted_xml_to_json_files.items():
            output_file = intermediate_dir / f"{filename}.json"
            try:
                with output_file.open("w", encoding="utf-8") as fh:
                    json.dump(_file, fh, ensure_ascii=False, indent=2)
            except Exception as exc:
                print(f"Error writing transformed json to {output_file}: {exc}")

    # 5. Apply transformations if we have JSON data
    if converted_xml_to_json_files:
        successfully_transformed_files = []
        # Collect transformed JSONs by level (in memory)
        jsons_by_level = {}  # {level_name: [(filename, json_dict), ...]}

        with log_timing("Applying transformations", logger):
            for filename, _file in converted_xml_to_json_files.items():
                
                # set up transformation config
                record_level_mapping = transformation_config.get("record_level_mapping", {})
                if len(transformation_config) == 0 or len(record_level_mapping) == 0:
                    logger.error("transformation_config: %s", transformation_config)
                    logger.error("record_level_mapping: %s", record_level_mapping)
                    return {"status": "error", "message": "Transformation config or record "
                                                            "level mapping is missing or empty"}

                # do the transformations
                try:
                    # newline to <p> transformation
                    transformed_json = None
                    task = transformation_config['tasks'].get('newline_to_p', {})
                    n = NewlineToPTransformer(target_columns=None, **task.get('params', {}))
                    transformed_json = n.transform(_file)

                    # Y naming transformation
                    task = transformation_config['tasks'].get('y_naming')
                    y = YNamingTransformer(target_columns=None)
                    transformed_json = y.transform(transformed_json)

                    # Save the final transformed JSON
                    # Collect in memory by level (no disk writes except in DEBUG mode)
                    if use_level_subfolders:
                        level = str(next((v for v in find_key(transformed_json, 
                                                              "catalogueLevel")), None))
                        dir_name = record_level_mapping.get(level, "UNKNOWN")
                        # Collect in memory by level
                        if dir_name not in jsons_by_level:
                            jsons_by_level[dir_name] = []
                        jsons_by_level[dir_name].append((filename, transformed_json))
                    else:
                        # No level-based dirs, collect as "root"
                        if "root" not in jsons_by_level:
                            jsons_by_level["root"] = []
                        jsons_by_level["root"].append((filename, transformed_json))
                except Exception:
                    logger.exception("Error applying transformations for file %s",
                                     f"{filename}.json")
                    return {"status": "error", "message": f"Error applying "
                            f"transformations for {filename}.json"}
                successfully_transformed_files.append(filename)

    # Create in-memory tarballs for each level
    if jsons_by_level:
        with log_timing("Creating tarballs", logger):
            logger.info("Creating %d tarball(s) in memory...", len(jsons_by_level))
            tree_name = Path(key).stem
            level_tarballs = {}  # {level_name: tar_bytes}
            for level_name, files in jsons_by_level.items():
                # Define tarball name as <original_filename>_<level_name>.tar.gz
                tarball_name = f"{tree_name}_{level_name}.tar.gz"

                # Build tarball in memory
                buf = io.BytesIO()
                try:
                    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                        for filename, json_data in files:
                            json_bytes = json.dumps(json_data, ensure_ascii=False, 
                                                    indent=2).encode("utf-8")
                            ti = tarfile.TarInfo(name=f"{filename}.json")
                            ti.size = len(json_bytes)
                            ti.mtime = int(time.time())
                            tar.addfile(ti, fileobj=io.BytesIO(json_bytes))
                    
                    buf.seek(0)
                    tar_bytes = buf.getvalue()
                    file_count = len(files)
                    logger.info("Created in-memory tarball: %s (%d files, %d bytes)", 
                                tarball_name, file_count, len(tar_bytes))
                    
                    level_tarballs[level_name] = tar_bytes
                    
                    # Write tar by folder in local mode
                    if run_mode == "local":
                        tarball_path = output_dir / tarball_name
                        with tarball_path.open("wb") as f:
                            f.write(tar_bytes)
                        logger.info("Saved tarball locally: %s", tarball_path)

                except Exception:
                    logger.exception("Error creating tarball for level %s", level_name)
                    return {"status": "error", "message": f"Error creating tarball "
                            f"for level {level_name}"}    
                    
            # Upload to S3 in S3 modes (local_s3 or remote_s3)
            # we need to create a super-tarball containing all level tarballs
            if run_mode in ["local_s3", "remote_s3"]:
                if not bucket:
                    logger.error("No S3 bucket specified for upload")
                    return {"status": "error", "message": "No S3 bucket specified"}
                
                super_tarball_name = f"{tree_name}.tar.gz"
                logger.info("Creating super-tarball: %s with %d level tarballs", 
                            super_tarball_name, len(level_tarballs))
                
                # Create super-tarball containing all level tarballs
                super_buf = io.BytesIO()
                with tarfile.open(fileobj=super_buf, mode="w:gz") as super_tar:
                    for level_name, tar_bytes in level_tarballs.items():
                        level_tarball_name = f"{tree_name}_{level_name}.tar.gz"
                        ti = tarfile.TarInfo(name=level_tarball_name)
                        ti.size = len(tar_bytes)
                        ti.mtime = int(time.time())
                        super_tar.addfile(ti, fileobj=io.BytesIO(tar_bytes))
                        logger.info("Added %s to super-tarball (%d bytes)", 
                                    level_tarball_name, len(tar_bytes))

                super_buf.seek(0)
                super_tar_bytes = super_buf.getvalue()
                logger.info("Created super-tarball: %s (%d bytes)", 
                            super_tarball_name, len(super_tar_bytes))
                
                # Upload to json_outputs folder in S3
                tar_key = f"{output_dir}/{super_tarball_name}"
                try:
                    s3.put_object(Bucket=bucket, Key=tar_key, Body=super_tar_bytes)
                    logger.info("Uploaded tarball to s3://%s/%s", bucket, tar_key)
                    
                    # Update manifest with newly uploaded records
                    if manifest is not None:
                        manifest = update_manifest_with_records(manifest, converted_xml_to_json_files, 
                                                                key, bucket, output_dir, logger)
                        try:
                            save_manifest(manifest_filename, s3, bucket, output_dir, manifest, logger)
                            logger.info("Updated manifest with %d total records", len(manifest.get('records', {})))
                        except Exception:
                            logger.exception("Error saving manifest (non-fatal)")
                            
                except ClientError as e:
                    logger.exception("Error uploading tarball to S3: %s", 
                                    e.response.get('Error', {}).get('Code'))
                    return {"status": "error", 
                            "message": f"Error uploading super-tarball to S3:"
                                f" {e.response.get('Error', {}).get('Code')}"}
                


    if len(successfully_transformed_files) > 0:
        logger.info("Processed %s successfully", key)
        return {"status": "ok", "message": f"Processed {key} successfully."}
    else:
        logger.error("No transformed JSON generated for %s", key)
        return {"status": "error", "message": f"No transformed JSON generated for {key}"}

# Run the handler locally if in local or local_s3 mode
if run_mode in ["local", "local_s3"]:
    if __name__ == "__main__":
        logger.info("Running pipeline in %s mode...", run_mode)
        trigger_path = Path(os.getenv("CTD_TRIGGER_JSON", repo_root / "trigger.json"))
        if not trigger_path.exists() or not trigger_path.is_file():
            logger.error("Local trigger JSON file not found: %s", trigger_path)
            raise SystemExit(1)
        with trigger_path.open("r", encoding="utf-8") as f:
            trigger_json = json.load(f)
        # When running locally, call the handler with trigger_json
        result = lambda_handler(event=trigger_json, context=None)
        logger.info("Result: %s", json.dumps(result, indent=2))