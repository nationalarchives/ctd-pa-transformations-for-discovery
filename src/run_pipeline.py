import boto3
import sys
import json
import os
from pathlib import Path
from botocore.exceptions import ClientError

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from src.utils import find_key
from src.utils import merge_xml_files
from src.transformers import NewlineToPTransformer, YNamingTransformer, convert_to_json

# S3 client used when running in AWS (or when credentials/profile available)
s3 = boto3.client('s3')

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


# Directories and files used by the handler. These can be overridden via env vars.
input_dir = _resolve_env_path("CTD_DATA_INPUT", repo_root / "data" / "triggers")
intermediate_dir = _resolve_env_path("CTD_DATA_INTERMEDIATE", repo_root / "data" / "intermediate")
output_dir = _resolve_env_path("CTD_DATA_OUTPUT", repo_root / "data" / "processed")
json_file_path = _resolve_env_path("CTD_TRIGGER_JSON", repo_root / "trigger.json")

# Ensure directories exist if you write locally
input_dir.mkdir(parents=True, exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)
intermediate_dir.mkdir(parents=True, exist_ok=True)


def _load_json_file(path: Path) -> dict:
    """Safely load JSON from `path`. Returns a dict or empty dict on error.

    This runs at module import so the handler can use the parsed trigger JSON
    for local testing or when no event is provided.
    """
    try:
        if not path.exists():
            print(f"Info: trigger json not found at {path}")
            return {}
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        print(f"Error loading JSON from {path}: {exc}")
        return {}


# Load trigger JSON at module import so it's available to the handler
trigger_json = _load_json_file(json_file_path)


def lambda_handler(event, context):
    # Allow the handler to be called with no event (local testing) by
    # falling back to the parsed trigger JSON we loaded at module import.
    if not event:
        event = trigger_json

    # 1. Get bucket and key from event
    action = event.get('action')
    key = event.get('file')
    merge_xml = event.get('merge_xml')
    bucket = event.get('bucket')  # optional - may be omitted for local runs

    if not action or not key:
        return {"statusCode": 400, "body": "Missing action or file in event"}

    if action != "run_pipeline" or not key.endswith(".xml"):
        return {"statusCode": 400, "body": "Invalid action or file type"}

    # 2. Determine source: local file or S3 download
    xml_path_to_convert = None
    tmp_path = None

    if bucket:
        # Download from S3 to a temp file
        tmp_path = output_dir / f"tmp_{Path(key).name}"
        try:
            s3.download_file(Bucket=bucket, Key=key, Filename=str(tmp_path))
            xml_path_to_convert = tmp_path
        except ClientError as e:
            return {
                "statusCode": 500,
                "body": f"Error downloading {key} from S3: {e.response['Error']['Code']}"
            }
    else:
        # Use local file
        if merge_xml:
            print(f"Merging XML files in {input_dir} into one for conversion...")
            date = Path().stat().st_mtime
            merged_output_path = input_dir / f"merged_input_{date}.xml"
            merge_xml_files(
                triggers_dir=input_dir,
                output_path=merged_output_path
            )
            xml_path_to_convert = merged_output_path
            print(f"Finished merging XML files into: {merged_output_path}")
        else:
            xml_path_to_convert = input_dir / key
        if not xml_path_to_convert.exists() or not xml_path_to_convert.is_file():
            return {
                "statusCode": 404,
                "body": f"Local XML file not found: {xml_path_to_convert}"
            }
        
    # 3. Convert XML to JSON
    try:
        print(f"Converting XML to JSON: {xml_path_to_convert}...")
        records = convert_to_json(xml_path=str(xml_path_to_convert), output_dir=str(output_dir))
        print(f"Files in output_dir after conversion:")
        for f in output_dir.iterdir():
            print(f"  {f.name}")
    except Exception as exc:
        return {
            "statusCode": 500,
            "body": f"Error converting XML to JSON: {exc}"
        }
    finally:
        # Clean up temp file if we downloaded from S3
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()

    # 4. Load the converted JSON (convert_xml_to_json should have written it)
    converted_xml_to_json_files = records

    # save the converted files to disk to investigation - remove this in the future
    """
    for filename, _file in converted_xml_to_json_files.items():
        output_file = intermediate_dir / f"{filename}.json"
        try:
            with output_file.open("w", encoding="utf-8") as fh:
                json.dump(_file, fh, ensure_ascii=False, indent=2)
        except Exception as exc:
            print(f"Error writing transformed json to {output_file}: {exc}")
    """

    tmp_config = {
        "tasks": {
            "newline_to_p": {
                "params": {
                    "match": "\n",
                    "replace": "<p>",
                    },
                "fields": None
            },
            "y_naming": {
                "fields": None
            }
        },
        "record_level_dirs": True
    }

    record_level_mapping = {
        1: '1. FONDS',
        2: '2. SUB-FONDS',
        3: '3. SUB-SUB-FONDS',
        4: '4. SUB-SUB-SUB-FONDS',
        5: '5. SUB-SUB-SUB-SUB-FONDS',
        6: '6. SERIES',
        7: '7. SUB-SERIES',
        8: '8. SUB-SUB-SERIES',
        9: '9. FILE',
        10: '10. ITEM'
    }
    

    # 5. Apply transformations if we have JSON data
    
    import sys
    import time

    total_count = len(converted_xml_to_json_files)
    for i in range(total_count):
        if converted_xml_to_json_files:
            for filename, _file in converted_xml_to_json_files.items():

                # newline to <p> transformation
                task = tmp_config['tasks'].get('newline_to_p')
                n = NewlineToPTransformer(target_columns=None, **task.get('params', {}))
                transformed_json = n.transform(_file)

                # Y naming transformation
                task = tmp_config['tasks'].get('y_naming')
                y = YNamingTransformer(target_columns=None)
                transformed_json = y.transform(transformed_json)

                # Save the final transformed JSON locally
                # save the transformed json into level based directories if needed
                if tmp_config.get("record_level_dirs"):
                    level = next((v for v in find_key(transformed_json, "catalogueLevel")), None)
                    dir_name = record_level_mapping.get(level)
                    target_dir = output_dir / dir_name
                    target_dir.mkdir(parents=True, exist_ok=True)
                    output_file = output_dir / dir_name / f"{filename}.json"
                else:
                    output_file = output_dir / f"{filename}.json"
                try:
                    with output_file.open("w", encoding="utf-8") as fh:
                        json.dump(transformed_json, fh, ensure_ascii=False, indent=2)
                except Exception as exc:
                    print(f"Error writing transformed json to {output_file}: {exc}")

            # 6. Optionally upload to S3 if bucket was provided
            if bucket and transformed_json:
                output_key = f"{Path(key).stem}_transformed.json"
                try:
                    s3.put_object(
                        Bucket=bucket,
                        Key=output_key,
                        Body=json.dumps(transformed_json, ensure_ascii=False, indent=2)
                    )
                except ClientError as e:
                    return {
                        "statusCode": 500,
                        "body": f"Error uploading to S3: {e.response['Error']['Code']}"
                    }

            return {
                "statusCode": 200,
                "body": f"Processed {key} successfully"
            }
        sys.stdout.write(f"\rProcessing {i}/{total_count}")
        sys.stdout.flush()
        time.sleep(0.05)


if __name__ == "__main__":
    print("Running pipeline locally (not in Lambda)...")
    # When running locally, call the handler with trigger_json
    result = lambda_handler(event=trigger_json, context=None)
    print("\nResult:")
    print(json.dumps(result, indent=2))