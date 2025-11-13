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