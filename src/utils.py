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
from datetime import datetime, timedelta
import json
import time as pytime

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
        hours, rem = divmod(duration, 3600)
        minutes, seconds = divmod(rem, 60)
        end_ts = datetime.now().isoformat()
        logger.info(
            "Finished %s at %s (duration %dh %dm %.2fs)",
            operation_name.lower(),
            end_ts,
            int(hours),
            int(minutes),
            seconds
        )


@contextlib.contextmanager
def progress_context(total: int, interval: int = 500, label: str = "process"):
    """Lightweight progress reporting context.

    Yields a ``tick(done_count)`` function. Call it every *interval* items
    (or always, your choice) to print rate and ETA (HH:MM). No class
    instantiation per iteration; minimal overhead.

    Example:
        with progress_context(total=N, interval=1000, label="xml->json") as tick:
            for i in range(N):
                # ... work ...
                tick(i + 1)
    """
    start = pytime.time()

    def _format_line(done: int, elapsed: float) -> str:
        rate = (done / elapsed) * 60.0 if elapsed > 0 else 0.0
        remaining = total - done
        eta_secs = (remaining * (elapsed / done)) if done > 0 else 0
        eta_str = (datetime.now() + timedelta(seconds=eta_secs)).strftime("%H:%M")
        return (f"[{label}] {done}/{total} ({done/total*100:.0f}%) | "
                f"Rate: {rate:.0f}/min | ETA ~ {eta_str}    ")

    def tick(done: int):
        # Suppress final print; handled once in finally
        if done == total:
            return
        if done % interval == 0:
            elapsed = pytime.time() - start
            print(_format_line(done, elapsed), end='\r')

    try:
        yield tick
    finally:
        elapsed = pytime.time() - start
        # Final line (no carriage return; ends with newline)
        print(_format_line(total, elapsed))
        # Ensure cursor on next line
        print()

# helper to format duration
def _fmt_duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    # use mod to get remainders for formatting (so we don't get more than 60 minutes etc)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{int(h)}h {int(m)}m {s:.1f}s"
    if m:
        return f"{int(m)}m {s:.1f}s"
    return f"{s:.2f}s"

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

def filter_xml_by_iaid(xml_path: Union[str, Path], target_iaid: str, output_path: Union[str, Path], logger) -> Path:
    """Filter XML to only include the record with specified citableReference.

    Parameters
    ----------
    xml_path : path-like
        Path to input XML file
    target_iaid : str
        citableReference to filter for (e.g., "C12345")
    output_path : path-like
        Path where filtered XML will be saved
    logger : logging.Logger
        Logger instance

    Returns
    -------
    Path
        Path to the filtered XML file
    """
    xml_path = Path(xml_path)
    output_path = Path(output_path)

    logger.info("Filtering XML for alternative_number: %s", target_iaid)

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Find all record elements (not InformationObject - that's the root container)
    found_record = None
    for record in root.findall('.//record'):
        # Look for alternative_number with type='CALM RecordID'
        alt_num_elem = record.find("Alternative_number/[alternative_number.type='CALM RecordID']/alternative_number")
        if alt_num_elem is not None and alt_num_elem.text and alt_num_elem.text.strip() == target_iaid:
            found_record = record
            logger.info("Found record with alternative_number %s", target_iaid)
            break

        # Also try without the type filter (in case structure varies)
        if found_record is None:
            for alt_num in record.findall('.//alternative_number'):
                if alt_num.text and alt_num.text.strip() == target_iaid:
                    found_record = record
                    logger.info("Found record with alternative_number %s (fallback search)", target_iaid)
                    break
            if found_record is not None:
                break

    if found_record is None:
        logger.warning("Record with alternative_number %s not found in XML file", target_iaid)
        raise ValueError(f"Record with alternative_number {target_iaid} not found in {xml_path}")

    # Create new XML with just this record
    new_root = ET.Element(root.tag, attrib=root.attrib)
    new_root.append(found_record)
    new_tree = ET.ElementTree(new_root)

    # Save filtered XML
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_tree.write(output_path, encoding='utf-8', xml_declaration=True)
    logger.info("Saved filtered XML to %s", output_path)

    return output_path
    
############################
# transfer register helpers
############################
def load_transfer_register(register_filename, s3, bucket, s3_output_folder, logger):
    """Load the transfer register (previously called manifest) from S3."""
    key = f"{s3_output_folder}/{register_filename}"
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        register = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Loaded transfer register with %d records", len(register.get('records', {})))
        return register
    except s3.exceptions.NoSuchKey:
        logger.info("Transfer register file not found, creating new one")
        return {"last_updated": None, "total_records": 0, "records": {}}
    except Exception as e:
        logger.exception("Error loading transfer register: %s", e)
        return {"last_updated": None, "total_records": 0, "records": {}}

def save_transfer_register(register_filename, s3, bucket, output_dir, register, logger):
    """Save the transfer register to S3 with a timestamped backup of existing (backward compatible with manifest)."""
    key = f"{output_dir}/{register_filename}"
    try:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            base_name = Path(register_filename).stem
            ext = Path(register_filename).suffix
            backup_filename = f"{base_name}_copy_{timestamp}{ext}"
            backup_key = f"{output_dir}/{backup_filename}"
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=backup_key)
            logger.info("Created backup of existing transfer register: s3://%s/%s", bucket, backup_key)
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info("No existing transfer register to backup")
            else:
                raise
        register['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        register['total_records'] = len(register.get('records', {}))
        body = json.dumps(register, indent=2, ensure_ascii=False).encode('utf-8')
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json')
        logger.info("Saved transfer register with %d total records to s3://%s/%s", register['total_records'], bucket, key)
    except Exception as e:
        logger.exception("Error saving transfer register: %s", e)

def filter_new_records(records, transfer_register, logger):
    """Filter out already-uploaded records using transfer register"""
    uploaded_iaids = set(transfer_register.get('records', {}).keys())
    new_records = {}
    skipped_count = 0

    for iaid, record_data in records.items():
        if iaid in uploaded_iaids:
            logger.debug("Skipping already-uploaded record: %s", iaid)
            skipped_count += 1
            continue
        new_records[iaid] = record_data

    logger.info("Filtered %d new records (skipped %d duplicates)", len(new_records), skipped_count)
    return new_records

def update_transfer_register_with_records(transfer_register, records, source_file, bucket, s3_output_folder, logger):
    """Add newly uploaded records to transfer register (excluding the deepest/leaf level)."""
    if 'records' not in transfer_register:
        transfer_register['records'] = {}

    max_level = 0
    for iaid, record_data in records.items():
        catalogue_level = record_data.get('record', {}).get('catalogueLevel', 0)
        if catalogue_level > max_level:
            max_level = catalogue_level

    logger.info("Tree max catalogue level: %d - tracking all except deepest", max_level)

    added_count = 0
    skipped_count = 0
    for iaid, record_data in records.items():
        catalogue_level = record_data.get('record', {}).get('catalogueLevel', 0)
        if catalogue_level == max_level:
            logger.debug("Skipping leaf record %s (level %d)", iaid, catalogue_level)
            skipped_count += 1
            continue
        transfer_register['records'][iaid] = {
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

    logger.info("Added %d records to transfer register, skipped %d leaf records", added_count, skipped_count)
    return transfer_register
