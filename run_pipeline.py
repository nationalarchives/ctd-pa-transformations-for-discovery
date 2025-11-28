import boto3
import sys
import json
import os
import logging
import tempfile
from pathlib import Path
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
import time
import tarfile
import io
from collections import defaultdict

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from src.config_loader import UniversalConfig
from src.utils import find_key, merge_xml_files, log_timing, _load_json_file, filter_xml_by_iaid
from src.utils import load_transfer_register, save_transfer_register, filter_new_records, update_transfer_register_with_records
from src.utils import insert_ordered, progress_context, get_trans_config
from src.transformers import NewlineToPTransformer, YNamingTransformer, ReplicaDataTransformer, convert_to_json


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

# verbose print statements on progress for long-running batches of records
VERBOSE_PROGRESS = os.getenv("PROGRESS_VERBOSE", "0").lower() in ("1","true","y")

if run_mode not in VALID_RUN_MODES:
    raise ValueError(
        f"Invalid RUN_MODE '{run_mode}'. Must be one of: {', '.join(VALID_RUN_MODES)}"
    )

# Configure module logger (level can be set with CTD_LOG_LEVEL env var)
_log_level = os.getenv("CTD_LOG_LEVEL", "DEBUG").upper()
_numeric_level = getattr(logging, _log_level, logging.INFO)
logging.basicConfig(level=_numeric_level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

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

# Set up file handler if running in local modes
if run_mode in ["local", "local_s3"]:
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(logs_dir / "pipeline.log")
    file_handler.setLevel(_numeric_level)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    logger.addHandler(file_handler)


# Load transformation configuration from SSM Parameter Store or environment variable
transformation_config = get_trans_config(logger=logger)

logger.info("Using transformation_config: %s", transformation_config)

# Transfer register configuration (no manifest terminology)
transfer_register_filename = os.getenv("TRANSFER_REGISTER_FILENAME", "uploaded_records_transfer_register.json")

def lambda_handler(event, context):

    # log when process was started
    start_time = datetime.now()
    logger.info("Lambda handler started at %s", start_time.isoformat())
    
    # Log Lambda context information if running in remote_s3 mode
    if run_mode == "remote_s3" and context:
        logger.info("Function: %s, Version: %s, Request ID: %s",
                    context.function_name, context.function_version, context.aws_request_id)
        logger.info("Memory limit: %s MB, Timeout in: %.1f seconds",
                    context.memory_limit_in_mb, context.get_remaining_time_in_millis() / 1000)

    # 1. Get bucket and key from event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    raw_key = key # this is the <foldername>/<filename> in S3

    # Validate key exists and is an XML file
    if not key or not key.endswith(".xml"):
        result = {"status": "error", "message": "Invalid or missing XML file key in event"}
        logger.info("Pipeline result: %s", json.dumps(result))
        return result

    # Check if we're in test mode and apply test folder prefix
    test_mode = os.getenv("TEST_MODE", "false").strip().lower() in ("1", "true", "yes", "y")
    test_folder = os.getenv("S3_TEST_FOLDER", "").strip().strip('/')
    
    if test_mode and test_folder:
        logger.info("TEST_MODE enabled: using test folder '%s'", test_folder)
        # Prepend test folder to input key if not already present
        if not key.startswith(f"{test_folder}/"):
            key = f"{test_folder}/{key}"
            logger.info("Adjusted input key for test mode: %s", key)
    
    # S3 output prefix (key prefix for uploads). Alias output_dir for legacy variable usage below.
    output_prefix = os.environ.get('S3_OUTPUT_DIR', 'json_outputs').strip().strip('/')
    
    # Apply test folder prefix to output if in test mode
    if test_mode and test_folder:
        output_prefix = f"{test_folder}/{output_prefix}"
        logger.info("Adjusted output prefix for test mode: %s", output_prefix)
    
    output_dir = output_prefix  # backward compatibility for existing code paths
    
    # Load transfer register for deduplication (only in S3 modes)
    transfer_register = None
    if run_mode in ["local_s3", "remote_s3"]:
        if not transfer_register_filename:
            result = {
                "status": "error",
                "message": "TRANSFER_REGISTER_FILENAME environment variable is required"
            }
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
        try:
            transfer_register = load_transfer_register(transfer_register_filename, s3, bucket, output_prefix, logger)
            num_existing = len(transfer_register.get('records', {}))
            logger.info("Loaded transfer register with %d existing records", num_existing)
        except Exception as e:
            logger.exception("FATAL: Failed to load transfer register - cannot proceed without deduplication")
            result = {
                "status": "error",
                "message": f"Failed to load deduplication transfer register: {str(e)}"
            }
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
    else:
        transfer_register = None
        logger.info("Running in local mode - skipping transfer register/deduplication")

    # we can view any transformation intermediates in this dir (not in AWS Lambda)
    intermediate_dir = Path(os.environ.get('CTD_DATA_INTERMEDIATE', ''))
    if run_mode == "local" and intermediate_dir:
        intermediate_dir.mkdir(parents=True, exist_ok=True)

    # whether to use subfolders in S3 output
    truthy_chars = ("1", "true", "yes", "y")
    use_level_subfolders = os.getenv("USE_LEVEL_SUBFOLDERS", "true").strip().lower() in truthy_chars
    
    # whether to merge XML files from folder structure before processing
    _merge_env = os.getenv("MERGE_XML", "false")
    merge_xml = _merge_env.strip().lower() in truthy_chars

    # Portable work directory for temp/intermediate files
    work_dir = Path(tempfile.gettempdir())
    work_dir.mkdir(parents=True, exist_ok=True)

    # set other paths to None initially
    xml_path_to_convert = None
    tmp_path = None

    # filter iaid list for replica metadata to those in bucket before applying replica transformer
    replica_metadata_prefix = os.getenv("REPLICA_METADATA_PREFIX", "metadata")
    replica_filename_prefix = os.getenv("REPLICA_FILENAME_PREFIX", "files")
    
    # Apply test folder prefix to replica metadata if in test mode
    replica_metadata_prefix = f"{replica_metadata_prefix}"
    
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': replica_metadata_prefix}
    page_iterator = paginator.paginate(**operation_parameters,
                PaginationConfig={'MaxItems': 5000})
    
    replica_list = []
    for page in page_iterator:
        replica_list.append(page.get('Contents', []))
    replica_metadata_filenames = set([Path(obj['Key']).stem for sublist in replica_list for obj in sublist])

    # list filenames in files folder
    paginator_files = s3.get_paginator('list_objects_v2')
    operation_parameters_files = {'Bucket': bucket,
                                  'Prefix': replica_filename_prefix}
    page_iterator_files = paginator_files.paginate(**operation_parameters_files,
                PaginationConfig={'MaxItems': 5000})
    
    # Build mapping: folder name -> unique list of filenames
    replica_filedata = defaultdict(list)

    for page in page_iterator_files:
        for obj in page.get('Contents', []):
            obj_key = obj['Key']
            parts = obj_key.split('/')
            
            # Only process if it's in the format folder/filename
            if len(parts) == 3 and parts[1]:  # Avoid empty filenames
                folder = parts[1]
                filename = os.path.splitext(parts[2])[0]
                replica_filedata[folder].append(filename)


    # Convert defaultdict to normal dict if needed
    replica_filedata = dict(replica_filedata)
    num_files = sum(len(v) for v in replica_filedata.values())
    logger.debug("replica files: %s", json.dumps(replica_filedata))
    logger.info("Loaded %s replica data files from S3", num_files)

    # Download from S3 in S3 modes (local_s3 or remote_s3)
    if run_mode in ["local_s3", "remote_s3"]:
        tmp_path = work_dir / Path(raw_key).name
        logger.info("Downloading s3://%s/%s -> %s", bucket, raw_key, tmp_path)
        try:
            s3.download_file(bucket, raw_key, str(tmp_path))
        except ClientError as e:
            err = e.response.get('Error', {})
            code = err.get('Code')
            if code in ("404", "NoSuchKey"):
                result = {"status": "error", "message": f"S3 key not found: s3://{bucket}/{raw_key}"}
                logger.info("Pipeline result: %s", json.dumps(result))
                return result
            result = {"status": "error", "message": f"S3 download failed ({code}) for {raw_key}"}
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
        except Exception as e:
            logger.exception("Unexpected S3 download error")
            result = {"status": "error", "message": f"Unexpected download error for {raw_key}: {e}"}
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
            result = {"status": "error", "message": f"Downloaded file missing or empty: {tmp_path}"}
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
        xml_path_to_convert = tmp_path
        logger.info("Downloaded OK (%d bytes)", tmp_path.stat().st_size)
    else:
        # local mode: resolve path relative to repo root if not absolute
        local_candidate = Path(key)
        if not local_candidate.is_absolute():
            local_candidate = repo_root / key
        if not local_candidate.exists():
            result = {"status": "error", "message": f"Local XML file not found: {local_candidate}"}
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
        # use local XML file instead
        xml_path_to_convert = local_candidate
        logger.info("Using local XML file: %s", xml_path_to_convert)

    # merge XML files if requested (local mode only)
    if merge_xml and run_mode == "local":
        merged_output_path = work_dir / f"merged_{int(time.time())}.xml"
        merge_xml_files(triggers_dir=work_dir, output_path=merged_output_path)
        xml_path_to_convert = merged_output_path
        logger.info("Merged XML written to: %s", merged_output_path)
    
    logger.debug("Final xml_path_to_convert: %s (exists=%s)", xml_path_to_convert, xml_path_to_convert.exists())
    
    # IAID filtering (local only)
    filter_iaid = os.getenv("FILTER_IAID")
    if filter_iaid and run_mode == "local":
        logger.info("Filtering XML for IAID=%s", filter_iaid)
        filtered_xml_path = work_dir / f"filtered_{filter_iaid}.xml"
        try:
            xml_path_to_convert = filter_xml_by_iaid(xml_path_to_convert, filter_iaid, filtered_xml_path, logger)
            logger.info("Filtered XML path: %s", xml_path_to_convert)
        except ValueError as e:
            result = {"status": "error", "message": str(e)}
            logger.info("Pipeline result: %s", json.dumps(result))
            return result
    
    # 2. Convert XML to JSON
    try:
        with log_timing(f"XML->JSON ({xml_path_to_convert.name})", logger):
            records = convert_to_json(xml_path=str(xml_path_to_convert), output_dir=str(work_dir),
                                      progress_verbose=VERBOSE_PROGRESS)
        logger.info("Converted %d records", len(records))
        if transfer_register is not None:
            before = len(records)
            records = filter_new_records(records, transfer_register, logger)
            removed = before - len(records)
            if removed:
                logger.info("Dedupe proc removed %d records; %d remain", removed, len(records))
    except Exception:
        logger.exception("Conversion failed")
        result = {"status": "error", "message": f"Conversion failed for {xml_path_to_convert.name}"}
        logger.info("Pipeline result: %s", json.dumps(result))
        return result
    finally:
        if tmp_path and tmp_path.exists() and run_mode in ["local_s3", "remote_s3"]:
            try:
                tmp_path.unlink()
            except Exception:
                logger.warning("Could not remove temp file %s", tmp_path)

    # 3. Load the converted JSON (convert_xml_to_json should have written it)
    converted_xml_to_json_files = records 

    # save the converted files to disk to investigation if option selected
    save_intermediate = os.getenv("DEBUG_TRANSFORMERS", "true").strip().lower() in truthy_chars
    if save_intermediate and run_mode == "local":
        for filename, _file in converted_xml_to_json_files.items():
            output_file = intermediate_dir / f"{filename}.json"
            try:
                with output_file.open("w", encoding="utf-8") as fh:
                    json.dump(_file, fh, ensure_ascii=False, indent=2)
            except Exception as exc:
                print(f"Error writing transformed json to {output_file}: {exc}")

    # 4. Apply transformations if we have JSON data
    if converted_xml_to_json_files:
        
        # keep track of successfully transformed files and closure status
        successfully_transformed_files = []
        open_count = 0
        closure_status_dict = {
            'open': open_count,
            'held_at_parliament': [],
            'closed_TNA': []
        }

        replica_iaids_added = []

        # Collect transformed JSONs by level (in memory)
        jsons_by_level = {}  # {level_name: [(filename, json_dict), ...]}
        replica_filedata_count = 0
        logger.info("Applying transformations to %d JSON files...", len(converted_xml_to_json_files))
        with progress_context(total = len(converted_xml_to_json_files), interval=100, label="Transforming") as tick:
            for i, (filename, _file) in enumerate(converted_xml_to_json_files.items(), start=1): #filename = iaid
                
                # Check timeout in remote_s3 mode periodically
                if run_mode == "remote_s3" and context and i % 100 == 0:
                    remaining_ms = context.get_remaining_time_in_millis()
                    if remaining_ms < 60000:  # Less than 60 seconds remaining
                        logger.warning("Running low on time (%d ms remaining) at record %d/%d",
                                     remaining_ms, i, len(converted_xml_to_json_files))
                        if remaining_ms < 30000:  # Less than 30 seconds - abort
                            result = {"status": "error", "message": f"Lambda timeout approaching - processed {i}/{len(converted_xml_to_json_files)} records"}
                            logger.info("Pipeline result: %s", json.dumps(result))
                            return result

                # set up transformation config
                record_level_mapping = transformation_config.get("record_level_mapping", {})
                if len(transformation_config) == 0 or len(record_level_mapping) == 0:
                    result = {"status": "error", "message": "Transformation config or record level mapping is missing or empty"}
                    logger.info("Pipeline result: %s", json.dumps(result))
                    return result

                # do the transformations
                try:
                    # Save pre-transformation JSON (before any transformers)
                    save_intermediates = os.getenv("SAVE_INTERMEDIATE_JSON", "true").strip().lower() in truthy_chars
                    if save_intermediates and run_mode == "local":
                        pre_transform_dir = intermediate_dir / "pre_transformed"
                        pre_transform_dir.mkdir(parents=True, exist_ok=True)
                        pre_transform_file = pre_transform_dir / f"{filename}.json"
                        with pre_transform_file.open("w", encoding="utf-8") as fh:
                            json.dump(_file, fh, ensure_ascii=False, indent=2)
                        logger.debug("Saved pre-transformed JSON: %s", pre_transform_file)

                    # newline to <p> transformation
                    transformed_json = None
                    task = transformation_config['tasks'].get('newline_to_p', {})
                    npt = NewlineToPTransformer(target_columns=task.get('target_columns'),
                                              **task.get('params', {}))
                    transformed_json = npt.transform(_file)

                    # Y naming transformation
                    task = transformation_config['tasks'].get('y_naming')
                    yt = YNamingTransformer(target_columns=task.get('target_columns'))
                    transformed_json = yt.transform(transformed_json)

                    # Replica data transformation
                    do_replicas = True
                    if do_replicas:

                        # Insert 'replicaID' at position 1 in the record dictionary
                        transformed_json["record"] = insert_ordered(
                                transformed_json["record"], "replicaId", None, 1
                            )

                        # now process replica metadata if available
                        if filename in replica_metadata_filenames:
                            rtd = ReplicaDataTransformer(bucket_name=bucket,
                                                            prefix=replica_metadata_prefix,
                                                            s3_client=s3 if run_mode in ["local_s3", "remote_s3"] else None)
                            transformed_json = rtd.transform(transformed_json)
                            replica_iaids_added.append(filename)

                            # check that files listed in metadata exist in replica filedata in s3
                            if transformed_json["record"]["replica"]:
                                for filedata in transformed_json["record"]["replica"]["files"]:
                                    #print("filedata:", filedata)
                                    #print(f"filename: {filedata['name']}, replica_filedata: {replica_filedata.get(filename, [])}")
                                    if filedata["name"] not in replica_filedata.get(filename, []):
                                        logger.info("File '%s' listed in replica metadata for IAID '%s' not found in S3 '%s/%s/'", 
                                                    filedata["name"], filename, replica_filename_prefix, filename)
                                    else:
                                        replica_filedata_count += 1
                                #raise ValueError(f"File '{filedata_name}' listed in replica metadata for IAID '{filename}' not found in S3 '{replica_filename_prefix}/{filename}/'")
                    
                    # Save post-transformation JSON (after all transformers)
                    if save_intermediates and run_mode == "local":
                        post_transform_dir = intermediate_dir / "post_transformed"
                        post_transform_dir.mkdir(parents=True, exist_ok=True)
                        post_transform_file = post_transform_dir / f"{filename}.json"
                        with post_transform_file.open("w", encoding="utf-8") as fh:
                            json.dump(transformed_json, fh, ensure_ascii=False, indent=2)
                        logger.debug("Saved post-transformed JSON: %s", post_transform_file)

                    # Save the final transformed JSON
                    # Collect in memory by level (no disk writes except in DEBUG mode)
                    if use_level_subfolders:
                        level = str(next((v for v in find_key(transformed_json,
                                                              "catalogueLevel")), None))
                        dir_name = record_level_mapping.get(level, "UNKNOWN").lower().replace(" ", "_")
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
                    result = {"status": "error", "message": f"Error applying transformations for {filename}.json"}
                    logger.info("Pipeline result: %s", json.dumps(result))
                    return result
                successfully_transformed_files.append(filename)

                # build list of counts of closureStatus' if needed
                if transformed_json["record"].get("closureStatus"):
                    # logic is:
                    # if closureType = 'O' then it is open
                    # if closureStatus = 'U' then it is held by Parliament (whether they are open or closed)
                    # if closureStatus = 'D' and closureType = 'U' then it is closed to TNA

                    # get the closure status
                    cl_status = transformed_json["record"]["closureStatus"]
                    # get closure type for additional tna closed check
                    closure_type = transformed_json["record"].get("closureType")
                    is_held_pa = cl_status == 'U'
                    is_tna_closed = cl_status == 'D' and closure_type == 'U'
                    # if Open, increment count instead of appending filename
                    if cl_status == 'O':
                        closure_status_dict['open'] += 1
                    elif is_held_pa:
                        closure_status_dict['held_at_parliament'].append(filename)
                    elif is_tna_closed:
                        closure_status_dict['closed_TNA'].append(filename)
                    else:
                        raise ValueError(f"Unknown closureStatus '{cl_status}' in record {filename}")
                tick(i)

    payload = closure_status_dict
    logger.info("Closure Status Summary: %s", json.dumps(payload, indent=2))

    payload = replica_iaids_added
    logger.info("Replica IAIDs added: %s", json.dumps(payload, indent=2))

    payload = replica_filedata_count
    logger.info("Replica filedata count: %s", json.dumps(payload, indent=2))

    # Create in-memory tarballs for each level
    if jsons_by_level and run_mode in ['local_s3', 'remote_s3']:
        with log_timing("Creating tarballs", logger):
            logger.info("Creating %d tarball(s) in memory...", len(jsons_by_level))
            tree_name = Path(key).stem.lower().replace(" ", "_")
            print(key)
            print("tree_name:", tree_name)
            level_tarballs = {}  # {level_name: tar_bytes}
            # Batch files per level into tarballs of up to 10,000 JSON files each
            BATCH_SIZE = 10000
            for level_name, files in jsons_by_level.items():
                # files is a list of (filename, json_data)
                total_files = len(files)
                logger.info("Level '%s' has %d files; batching into %d-file chunks", level_name, total_files, BATCH_SIZE)

                # Create chunks of files
                chunks = [files[i:i + BATCH_SIZE] for i in range(0, total_files, BATCH_SIZE)]
                cumulative_count = 0

                for chunk_index, chunk in enumerate(chunks, start=1):
                    cumulative_count += len(chunk)
                    # Name tarball with cumulative end count: <tree>_<level>_N.tar.gz where N is cumulative_count
                    tarball_name = f"{tree_name}_{level_name}_{cumulative_count}.tar.gz"

                    buf = io.BytesIO()
                    try:
                        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                            for filename, json_data in chunk:
                                safe_name = f"{Path(filename).name}.json"
                                json_bytes = json.dumps(json_data, ensure_ascii=False,
                                                        indent=2).encode("utf-8")
                                ti = tarfile.TarInfo(name=safe_name)
                                ti.size = len(json_bytes)
                                ti.mtime = int(time.time())
                                tar.addfile(ti, fileobj=io.BytesIO(json_bytes))

                        buf.seek(0)
                        tar_bytes = buf.getvalue()
                        file_count = len(chunk)
                        logger.info("Created in-memory tarball: %s (%d files, %d bytes)",
                                    tarball_name, file_count, len(tar_bytes))

                        # Store tar bytes under a compound key so super-tar builder can include them
                        # Use a level-specific list to preserve ordering
                        if level_name not in level_tarballs:
                            level_tarballs[level_name] = []
                        level_tarballs[level_name].append((tarball_name, tar_bytes))

                        # Write tar by folder in local mode
                        if run_mode == "local":
                            tarball_path = Path(output_dir) / tarball_name
                            with tarball_path.open("wb") as f:
                                f.write(tar_bytes)
                            logger.info("Saved tarball locally: %s", tarball_path)

                    except Exception:
                        logger.exception("Error creating tarball for level %s (chunk %d)", level_name, chunk_index)
                        result = {"status": "error", "message": f"Error creating tarball for level {level_name} chunk {chunk_index}"}
                        logger.info("Pipeline result: %s", json.dumps(result))
                        return result

            # Upload to S3 in S3 modes (local_s3 or remote_s3)
            # we need to create a super-tarball containing all level tarballs
            if run_mode in ["local_s3", "remote_s3"]:
                if not bucket:
                    result = {"status": "error", "message": "No S3 bucket specified"}
                    logger.info("Pipeline result: %s", json.dumps(result))
                    return result

                super_tarball_name = f"{tree_name}.tar.gz"
                logger.info("Creating super-tarball: %s with %d level tarballs",
                            super_tarball_name, len(level_tarballs))

                # Create super-tarball containing all level tarballs
                super_buf = io.BytesIO()
                with tarfile.open(fileobj=super_buf, mode="w:gz") as super_tar:
                    # level_tarballs now maps level_name -> list of (tar_name, tar_bytes)
                    for level_name, tar_entries in level_tarballs.items():
                        for tar_name, tar_bytes in tar_entries:
                            ti = tarfile.TarInfo(name=tar_name)
                            ti.size = len(tar_bytes)
                            ti.mtime = int(time.time())
                            super_tar.addfile(ti, fileobj=io.BytesIO(tar_bytes))
                            logger.info("Added %s to super-tarball (%d bytes)", tar_name, len(tar_bytes))

                super_buf.seek(0)
                super_tar_bytes = super_buf.getvalue()
                logger.info("Created super-tarball: %s (%d bytes)",
                            super_tarball_name, len(super_tar_bytes))

                # Upload to json_outputs folder in S3, creating a subfolder for the supertar
                folder_name = tree_name  # Use tree_name as the folder name
                folder_key = f"{output_prefix}/{folder_name}/"
                
                # Upload the supertar into the folder
                tar_key = f"{folder_key}{super_tarball_name}"
                try:
                    s3.put_object(Bucket=bucket, Key=tar_key, Body=super_tar_bytes)
                    logger.info("Uploaded supertar to s3://%s/%s", bucket, tar_key)
                    
                    # Extract and upload each subtar (level tarball) into the same folder
                    super_buf.seek(0)
                    # Upload each contained tar (we have them in level_tarballs already)
                    for level_name, tar_entries in level_tarballs.items():
                        for tar_name, tar_bytes in tar_entries:
                            subtar_key = f"{folder_key}{tar_name}"
                            s3.put_object(Bucket=bucket, Key=subtar_key, Body=tar_bytes)
                            logger.info("Uploaded subtar to s3://%s/%s", bucket, subtar_key)
                    
                    # Update transfer register with newly uploaded records
                    if transfer_register is not None:
                        transfer_register = update_transfer_register_with_records(transfer_register, converted_xml_to_json_files,
                                                                                   key, bucket, output_prefix, logger)
                        try:
                            save_transfer_register(transfer_register_filename, s3, bucket, output_prefix, transfer_register, logger)
                            logger.info("Updated transfer register with %d total records", len(transfer_register.get('records', {})))
                        except Exception:
                            logger.exception("Error saving transfer register (non-fatal)")
                            
                except ClientError as e:
                    logger.exception("Error uploading tarballs to S3: %s",
                                    e.response.get('Error', {}).get('Code'))
                    result = {"status": "error",
                            "message": f"Error uploading tarballs to S3: {e.response.get('Error', {}).get('Code')}"}
                    logger.info("Pipeline result: %s", json.dumps(result))
                    return result



    if len(successfully_transformed_files) > 0:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        h, rem = divmod(duration, 3600)
        m, s = divmod(rem, 60)
        result = {"status": "ok", "message": f"Processed {len(successfully_transformed_files)} in {key} successfully (Duration: {int(h):02d}:{int(m):02d}:{int(s):02d})"}
        logger.info("Pipeline result: %s", json.dumps(result))
        return result
    else:
        result = {"status": "error", "message": f"No transformed JSON generated for {key}"}
        logger.info("Pipeline result: %s", json.dumps(result))
        return result

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
