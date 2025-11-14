import pytest

import sys, os

# Add project root to PYTHONPATH so 'src' can be imported when running pytest from repo root.
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.transformers import YNamingTransformer

# Test matrix for _is_reference_like (syntactic detection only)
REFERENCE_TESTS = {
    # Basic valid references
    "ABC/1": True,
    "ABC/123": True,
    "ABC/1/2": True,
    "XYZ-12/ABC-3": False,  # hyphen not allowed in prefix (prefix must be alphabetic only)
    "A1B2C3/456": False, # digits not allowed in prefix (must be alphabetic only)
    "PARL/123": True,  # special-case prefix handled later in naming
    "123/ABC": False,   # prefix must be alphabetic

    # Invalid / rejection cases
    "ABC": False,              # no slash
    "ABC/": False,             # trailing empty token
    "ABC//DEF": False,         # empty middle token
    "///": False,              # empty tokens only
    "A B/1": False,            # space in token
    "ABC/1 DEF": False,        # space in second token
    "ABC/1/ DEF": False,       # space causes empty token
    "APT/": False,             # explicit APT exclusion
    "APT/1": False,            # APT exclusion
    "APT/XYZ": False,          # APT exclusion

    # Length / slash count boundaries
    "A/1": False,               # the prefix should be at least 2 characters
    "AB/1": True,              # minimal tokens
    # More than 9 slashes should fail (construct one)
    "A/1/2/3/4/5/6/7/8/9/10": False,
}

# Embedded reference strings (apply_if_reference should transform embedded tokens)
EMBEDDED_CASES = [
    ("indigo ABC/1 test", "indigo YABC/1 test"),
    ("prefix XYZ-12/ABC-3 suffix", "prefix XYZ-12/ABC-3 suffix"),
    ("mixed APT/1 keep", "mixed APT/1 keep"),  # APT should remain unchanged
    ("already YABC/1 here", "already YABC/1 here"),  # avoid double prefix
    ("PARL/123 in text", "YUKP/123 in text"),  # special case mapping
]


def test_is_reference_like_matrix():
    t = YNamingTransformer()
    for ref, expected in REFERENCE_TESTS.items():
        assert t._is_reference_like(ref) == expected, f"_is_reference_like('{ref}') expected {expected}" 


def test_embedded_reference_transforms():
    t = YNamingTransformer()
    for original, expected in EMBEDDED_CASES:
        transformed = t.apply_if_reference(original)
        assert transformed == expected, f"Embedded transform failed: '{original}' -> '{transformed}', expected '{expected}'"


def test_apply_if_reference_whole_string():
    t = YNamingTransformer()
    assert t.apply_if_reference("ABC/1") == "YABC/1"
    assert t.apply_if_reference("YABC/1") == "YABC/1"  # no double prefix
    assert t.apply_if_reference("PARL/999") == "YUKP/999"  # special case
    assert t.apply_if_reference("APT/1") == "APT/1"  # excluded


def test_non_string_input():
    t = YNamingTransformer()
    assert t.apply_if_reference(123) == 123
    assert t._is_reference_like(123) is False


def test_reference_length_trim():
    t = YNamingTransformer()
    # Prefix longer than 3 chars once Y added should be trimmed to 4 total
    # e.g., 'LONG' -> 'YLON' (Y + first 3 of LONG)
    assert t.apply_if_reference("LONG/1").startswith("YLON/")


def test_whitespace_embedded_cases():
    """Differentiate whole-string syntactic failure from embedded token success.

    The entire string 'ABC/1 DEF' is not reference-like (space in second token) so _is_reference_like returns False.
    However, apply_if_reference should still transform the embedded valid token 'ABC/1'.

    In contrast, 'ABC/1/ DEF' contains 'ABC/1/' which is syntactically invalid (trailing slash => empty token).
    No embedded replacement should occur.
    """
    t = YNamingTransformer()
    # Whole-string classification
    assert t._is_reference_like("ABC/1 DEF") is False
    assert t._is_reference_like("ABC/1/ DEF") is False
    # Embedded transformation behavior
    assert t.apply_if_reference("ABC/1 DEF") == "YABC/1 DEF"
    assert t.apply_if_reference("ABC/1/ DEF") == "ABC/1/ DEF"

