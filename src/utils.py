"""Utility helpers for the PA discovery transformation pipeline.

Includes:
 - Project root resolution
 - Generic JSON key finder
 - XML merge helpers (merge multiple trigger XML files into one)
"""

from __future__ import annotations

import os
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Iterable, List, Optional, Sequence, Union
from dotenv import load_dotenv
import contextlib
import time
import logging
from datetime import datetime
import json

def set_project_root(marker: str = "README.md") -> str:
    """
    Set the working directory to the project root by searching for a marker file.
    Usage:
        import utils
        utils.set_project_root()
    """
    start_dir = os.getcwd()
    current = start_dir
    while True:
        if os.path.exists(os.path.join(current, marker)):
            os.chdir(current)
            print(f"Set working directory to project root: {current}")
            return current
        parent = os.path.dirname(current)
        if parent == current:
            raise FileNotFoundError(f"Could not find {marker} in any parent directory.")
        current = parent


# Helper to find nested key values
def find_key(obj, target):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == target:
                yield v
            yield from find_key(v, target)
    elif isinstance(obj, list):
        for item in obj:
            yield from find_key(item, target)


@contextlib.contextmanager
def log_timing(operation_name: str, logger: Optional[logging.Logger] = None):
    """Context manager to log start time, end time, and duration of an operation.
    
    Usage:
        with log_timing("XML conversion", logger):
            convert_to_json(...)
    
    Parameters
    ----------
    operation_name : str
        Description of the operation being timed
    logger : logging.Logger | None
        Logger instance to use. If None, uses root logger.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    start = time.perf_counter()
    start_ts = datetime.now().isoformat()
    logger.info("Started %s at %s", operation_name, start_ts)
    
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        hh = int(duration // 3600)
        mm = int(duration // 60)
        ss = duration % 60
        end_ts = datetime.now().isoformat()
        logger.info("Finished %s at %s (duration %dh %dm %.2fs)", operation_name.lower(), end_ts, hh, mm, ss)



# List of XML files to merge
load_dotenv(Path.cwd() / ".env")


# ---------------------------------------------------------------------------
# XML merge helpers
# ---------------------------------------------------------------------------
def get_triggers_dir(env_var: str = "CTD_DATA_INPUT") -> Path:
    """Return the triggers directory path.

    Prefers environment variable (CTD_DATA_INPUT) and falls back to
    <repo-root>/data/triggers where <repo-root> is discovered from the
    current file location.
    """
    env_val = os.getenv(env_var)
    if env_val:
        p = Path(env_val)
        return p if p.is_absolute() else (Path(__file__).resolve().parents[1] / p).resolve()
    return Path(__file__).resolve().parents[1] / "data" / "triggers"


def list_xml_files(
    triggers_dir: Optional[Union[str, Path]] = None,
    *,
    filenames: Optional[Sequence[Union[str, Path]]] = None,
    pattern: str = "*.xml"
) -> List[Path]:
    """List XML files to process.

    If ``filenames`` provided, only those (resolved against ``triggers_dir``
    when relative) are returned. Otherwise all matching ``pattern``.
    Missing files are silently skipped.
    """
    base = Path(triggers_dir) if triggers_dir else get_triggers_dir()
    files: List[Path] = []
    if filenames:
        for name in filenames:
            p = Path(name)
            if not p.is_absolute():
                p = base / p
            if p.exists() and p.is_file():
                files.append(p)
        return files
    # Glob all by pattern
    if base.exists():
        files.extend(sorted(base.glob(pattern)))
    return files


def merge_xml_files(
    triggers_dir: Optional[Union[str, Path]] = None,
    *,
    filenames: Optional[Sequence[Union[str, Path]]] = None,
    root_tag: str = "MergedData",
    child_root_tag: Optional[str] = None,
    output_path: Optional[Union[str, Path]] = None
) -> ET.ElementTree:
    """Merge multiple XML files from the triggers directory into one tree.

    Parameters
    ----------
    triggers_dir : path-like | None
        Directory containing XML files. Falls back to environment or default.
    filenames : sequence[path-like] | None
        Explicit list of filenames to merge. If omitted, all *.xml in dir.
    root_tag : str
        Tag name for new merged root element.
    child_root_tag : str | None
        If provided, only children matching this tag under each source root
        are appended. Otherwise all direct children of each root are appended.
    output_path : path-like | None
        If given, write merged XML to this path (directories auto-created).

    Returns
    -------
    ElementTree
        The merged XML tree in memory.
    """
    files = list_xml_files(triggers_dir, filenames=filenames)
    #files = [f for f in files if "merged" not in str(f).lower() and "tree" not in str(f).lower()]
    files = [f for f in files if any(x in str(f).lower() for x in ['fonds', 'series', 'item', 'file'])]
    merged_root = ET.Element(root_tag)
    print(f"Merging {len(files)} XML files from {triggers_dir or get_triggers_dir()}:")
    for f in files:
        try:
            tree = ET.parse(f)
            src_root = tree.getroot()
        except Exception as exc:
            print(f"Warning: skipping '{f}': {exc}")
            continue
        # Select children to append
        children = list(src_root)
        if child_root_tag:
            children = [c for c in children if c.tag == child_root_tag]
        for child in children:
            # detach & append a shallow copy to avoid cross-tree references
            merged_root.append(child)
    merged_tree = ET.ElementTree(merged_root)
    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        merged_tree.write(out, encoding="utf-8", xml_declaration=True)
    print(f"Merged {len(files)} XML files into root <{root_tag}> with {len(merged_root)} children.")
    return merged_tree


__all__ = [
    "set_project_root",
    "find_key",
    "log_timing",
    "get_triggers_dir",
    "list_xml_files",
    "merge_xml_files",
]

def _load_json_file(path: Optional[str], logger) -> dict:
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
    
# manifest helpers    
def load_manifest(manifest_filename, s3, bucket, s3_output_folder, logger):
    """Load the manifest of uploaded records from S3"""
    manifest_key = f"{s3_output_folder}/{manifest_filename}"
    
    try:
        response = s3.get_object(Bucket=bucket, Key=manifest_key)
        manifest = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Loaded manifest with %d records", len(manifest.get('records', {})))
        return manifest
    except s3.exceptions.NoSuchKey:
        logger.info("Manifest file not found, creating new one")
        return {"last_updated": None, "total_records": 0, "records": {}}
    except Exception as e:
        logger.exception("Error loading manifest: %s", e)
        return {"last_updated": None, "total_records": 0, "records": {}}

def save_manifest(manifest_filename, s3, bucket, output_dir, manifest, logger):
    """Save the manifest of uploaded records to S3 (with timestamped backup of existing)"""
    manifest_key = f"{output_dir}/{manifest_filename}"
    
    try:
        # Check if manifest already exists and create a backup copy
        try:
            s3.head_object(Bucket=bucket, Key=manifest_key)
            # Manifest exists, create timestamped backup
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            base_name = Path(manifest_filename).stem
            ext = Path(manifest_filename).suffix
            backup_filename = f"{base_name}_copy_{timestamp}{ext}"
            backup_key = f"{output_dir}/{backup_filename}"
            
            # Copy existing manifest to backup
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': manifest_key},
                Key=backup_key
            )
            logger.info("Created backup of existing manifest: s3://%s/%s", bucket, backup_key)
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Manifest doesn't exist yet, no backup needed
                logger.info("No existing manifest to backup")
            else:
                raise
        
        # Save the new manifest
        manifest['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        manifest['total_records'] = len(manifest.get('records', {}))
        
        manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)
        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=manifest_json.encode('utf-8'),
            ContentType='application/json'
        )
        logger.info("Saved manifest with %d total records to s3://%s/%s", 
                   manifest['total_records'], bucket, manifest_key)
    except Exception as e:
        logger.exception("Error saving manifest: %s", e)

def filter_new_records(records, manifest, logger):
    """Filter out already-uploaded records using manifest"""
    uploaded_iaids = set(manifest.get('records', {}).keys())
    new_records = {}
    skipped_count = 0
    
    for iaid, record_data in records.items():
        if iaid in uploaded_iaids:
            logger.debug("Skipping already-uploaded record: %s", iaid)
            skipped_count += 1
            continue
        
        new_records[iaid] = record_data
    
    logger.info("Filtered %d new records (skipped %d duplicates)", 
               len(new_records), skipped_count)
    return new_records

def update_manifest_with_records(manifest, records, source_file, bucket, s3_output_folder, logger):
    """Add newly uploaded records to manifest (excluding the deepest/leaf level in this tree)"""
    if 'records' not in manifest:
        manifest['records'] = {}
    
    # Find the deepest level in this tree
    max_level = 0
    for iaid, record_data in records.items():
        catalogue_level = record_data.get('record', {}).get('catalogueLevel', 0)
        if catalogue_level > max_level:
            max_level = catalogue_level
    
    logger.info("Tree max catalogue level: %d - will track all levels except %d", max_level, max_level)
    
    added_count = 0
    skipped_count = 0
    
    for iaid, record_data in records.items():
        catalogue_level = record_data.get('record', {}).get('catalogueLevel', 0)
        
        # Skip the deepest level (leaf records that are unique per tree)
        if catalogue_level == max_level:
            logger.debug("Skipping leaf record %s (level %d)", iaid, catalogue_level)
            skipped_count += 1
            continue
        
        manifest['records'][iaid] = {
            'reference': record_data.get('record', {}).get('citableReference', 'N/A'),
            'uploaded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'uploaded_to': f"s3://{bucket}/{s3_output_folder}",
            'source_file': source_file,
            'QA_status': {
                'checked_complete': False,
                'checked_by': None,
                'check_complete_date': None
            },
            'catalogue_level': catalogue_level
        }
        added_count += 1
    
    logger.info("Added %d records to manifest, skipped %d leaf records", added_count, skipped_count)
    return manifest