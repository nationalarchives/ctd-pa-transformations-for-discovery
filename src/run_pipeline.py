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
from src.utils import find_key
from src.utils import merge_xml_files
from src.utils import log_timing
from src.transformers import NewlineToPTransformer, YNamingTransformer, convert_to_json


# load in the environment variables from .env in repo root (this is done by AWS Lambda automatically)
if not os.getenv("AWS_LAMBDA_FUNCTION_NAME") and not os.getenv("AWS_EXECUTION_ENV"):
    UniversalConfig(env_file=repo_root / ".env")

# set run mode (local vs cloud)
run_mode = os.getenv("RUN_MODE", "cloud").strip().lower()

# S3 client used when running in AWS (or when credentials/profile available)
s3 = boto3.client('s3')

# Configure module logger (level can be set with CTD_LOG_LEVEL env var)
_log_level = os.getenv("CTD_LOG_LEVEL", "DEBUG").upper()
_numeric_level = getattr(logging, _log_level, logging.INFO)
logging.basicConfig(level=_numeric_level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Set up file handler if running locally
if run_mode == "local":
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(logs_dir / "pipeline.log")
    file_handler.setLevel(_numeric_level)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    logger.addHandler(file_handler)

def _resolve_env_path(env_name: str, default_path: Path) -> Path:
    """Resolve an environment variable to an absolute Path.

    If the env var is set and is an absolute path, return it. If it's a
    relative path, resolve it against repo_root. If not set, return the
    default_path (resolved).
    """
    val = os.getenv(env_name)
    if not val:
        return default_path.resolve()
    p = Path(val)
    return p.resolve() if p.is_absolute() else (repo_root / p).resolve()


# Helper to load trigger JSON at module import
# ...existing code...
def _load_json_file(path: Optional[str]) -> dict:
    """Load JSON from a file path or from a JSON string stored in an env var.
    Returns an empty dict on error or when no input provided.
    """
    if not path:
        return {}

    s = str(path).strip()

    # 1) If it looks like an existing file (absolute or relative), load it
    p = Path(s)
    if p.exists():
        try:
            with p.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            logger.exception("Error reading JSON file %s", p)
            return {}

    # 3) Finally, try parsing the string itself as JSON content
    try:
        return json.loads(s)
    except Exception:
        logger.exception("TRANS_CONFIG value is not a valid JSON string or file: %s", s[:200])
        return {}

# define pipeline configuration separately from event
transformations_str = os.environ.get("TRANS_CONFIG")
transformation_config = _load_json_file(transformations_str)

def lambda_handler(event, context):
    
    # 1. Get bucket and key from event
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    # Validate key exists and is an XML file
    if not key or not key.endswith(".xml"):
        logger.error("Invalid or missing file key in event: key=%s", key)
        return {"status": "error", "message": "Invalid or missing XML file key in event"}

    # get the input directory from the event itself
    input_dir = Path(key).resolve().parent
    input_dir.mkdir(parents=True, exist_ok=True)

    # the output dir is set in the env vars
    output_dir = Path(os.environ.get('OUTPUT_DIR', ''))
    output_dir.mkdir(parents=True, exist_ok=True)

    # we can view any transformation intermediates in this dir (not in AWS Lambda)
    intermediate_dir = Path(os.environ.get('CTD_DATA_INTERMEDIATE', ''))
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    # Get merge flag from environment
    _merge_env = os.getenv("MERGE_XML")
    if _merge_env is None:
        merge_xml = False
    else:
        merge_xml = str(_merge_env).strip().lower() in ("1", "true", "yes", "y")

    # 2. Determine source: local file or S3 download
    xml_path_to_convert = None
    tmp_path = None

    # Download from S3 unless in local mode
    if run_mode != "local":
        # Download from S3 to a temp file
        tmp_path = input_dir / f"tmp_{Path(key).name}"
        try:
            s3.download_file(Bucket=bucket, Key=key, Filename=str(tmp_path))
            xml_path_to_convert = tmp_path
        except ClientError as e:
            logger.exception("Error downloading %s from S3: %s", key, e.response.get('Error', {}).get('Code'))
            return {"status": "error", "message": f"Error downloading {key} from S3: {e.response.get('Error', {}).get('Code')}"}
    
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
        return {"status": "error", "message": f"Local XML file not found: {xml_path_to_convert}"}
    
    # 3. Convert XML to JSON
    try:
        with log_timing(f"XML to JSON conversion ({xml_path_to_convert.name})", logger):
            records = convert_to_json(xml_path=str(xml_path_to_convert), output_dir=str(output_dir))
        logger.info("Converted %d records", len(records))
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
    save_intermediate = os.getenv("SAVE_INTERMEDIATE_JSON", "true").strip().lower() in ("1", "true", "yes", "y")
    if save_intermediate and run_mode == "local":
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
                    return {"status": "error", "message": "Transformation config or record level mapping is missing or empty"}

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
                    if transformation_config.get("record_level_dirs"):
                        level = str(next((v for v in find_key(transformed_json, "catalogueLevel")), None))
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
                    logger.exception("Error applying transformations for file %s", f"{filename}.json")
                    return {"status": "error", "message": f"Error applying transformations for {filename}.json"}
                successfully_transformed_files.append(filename)

    # Create in-memory tarballs for each level
    if jsons_by_level:
        with log_timing("Creating tarballs", logger):
            logger.info("Creating %d tarball(s) in memory...", len(jsons_by_level))
            for level_name, files in jsons_by_level.items():
                tarball_name = f"{level_name}_{Path(key).stem}.tar.gz"
                
                # Build tarball in memory
                buf = io.BytesIO()
                try:
                    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                        for filename, json_data in files:
                            json_bytes = json.dumps(json_data, ensure_ascii=False, indent=2).encode("utf-8")
                            ti = tarfile.TarInfo(name=f"{filename}.json")
                            ti.size = len(json_bytes)
                            ti.mtime = int(time.time())
                            tar.addfile(ti, fileobj=io.BytesIO(json_bytes))
                    
                    buf.seek(0)
                    tar_bytes = buf.getvalue()
                    file_count = len(files)
                    logger.info("Created in-memory tarball: %s (%d files, %d bytes)", 
                                tarball_name, file_count, len(tar_bytes))
                    
                    # Write locally if in local mode
                    if run_mode == "local":
                        tarball_path = output_dir / tarball_name
                        with tarball_path.open("wb") as f:
                            f.write(tar_bytes)
                        logger.info("Saved tarball locally: %s", tarball_path)
                    
                    # Upload to S3 (only in cloud mode)
                    if bucket and run_mode != "local":
                        tar_key = f"{Path(key).stem}/{tarball_name}"
                        try:
                            s3.put_object(Bucket=bucket, Key=tar_key, Body=tar_bytes)
                            logger.info("Uploaded tarball to s3://%s/%s", bucket, tar_key)
                        except ClientError as e:
                            logger.exception("Error uploading tarball to S3: %s", 
                                            e.response.get('Error', {}).get('Code'))
                            return {"status": "error", 
                                    "message": f"Error uploading tarball to S3: {e.response.get('Error', {}).get('Code')}"}
                
                except Exception:
                    logger.exception("Error creating tarball for level %s", level_name)
                    return {"status": "error", "message": f"Error creating tarball for {level_name}"}

    if len(successfully_transformed_files) > 0:
        logger.info("Processed %s successfully", key)
        return {"status": "ok", "message": f"Processed {key} successfully."}
    else:
        logger.error("No transformed JSON generated for %s", key)
        return {"status": "error", "message": f"No transformed JSON generated for {key}"}

# Run the handler locally if in local mode
if run_mode == "local":
    if __name__ == "__main__":
        logger.info("Running pipeline locally (not in Lambda)...")
        trigger_path = Path(os.getenv("CTD_TRIGGER_JSON", repo_root / "trigger.json"))
        if not trigger_path.exists() or not trigger_path.is_file():
            logger.error("Local trigger JSON file not found: %s", trigger_path)
            raise SystemExit(1)
        with trigger_path.open("r", encoding="utf-8") as f:
            trigger_json = json.load(f)
        # When running locally, call the handler with trigger_json
        result = lambda_handler(event=trigger_json, context=None)
        logger.info("Result: %s", json.dumps(result, indent=2))