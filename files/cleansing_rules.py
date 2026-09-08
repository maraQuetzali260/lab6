"""
cleansing_rules.py
-------------------
Pure-Python (no Spark/Glue dependency) implementation of the data-quality
rules used by the alumnus ETL pipeline.

Keeping these functions Spark-free means:
  1. They can be unit tested quickly, without spinning up a SparkSession
     or the Glue runtime.
  2. They can be wrapped as PySpark UDFs inside the Glue job
     (see glue_etl_job.py) without duplicating logic.

Business rules implemented
---------------------------
1. AGE        -> must be numeric and 0 < age <= 100
2. JOIN DATE  -> must be a string in strict mm/dd/yy format (no datetime
                 objects, no free text, no other date formats)
3. LEVEL      -> must be exactly one of: ASSOCIATE, BACHELOR, MASTER
                 (case-sensitive; "Master" / "master" / "MASTERS" are invalid)
4. NAME       -> must be present after trimming whitespace
"""

import re
from datetime import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VALID_LEVELS = {"ASSOCIATE", "BACHELOR", "MASTER"}

# Strict mm/dd/yy: MM 01-12, DD 01-31, YY two digits
DATE_PATTERN = re.compile(r"^(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/\d{2}$")

MAX_AGE = 100
MIN_AGE = 0  # exclusive lower bound


# ---------------------------------------------------------------------------
# Field-level validators
# ---------------------------------------------------------------------------
def is_valid_age(age) -> bool:
    """
    Rule: age must not exceed 100.
    Also guards against non-numeric input and non-positive ages, since a
    record with a nonsensical age is not usable downstream either way.
    Accepts int, float or numeric string; rejects None and non-numeric text.
    """
    if age is None:
        return False
    try:
        age_int = int(age)
    except (ValueError, TypeError):
        return False
    return MIN_AGE < age_int <= MAX_AGE


def is_valid_date_format(date_value) -> bool:
    """
    Rule: date_of_joining must follow mm/dd/yy format, and no strings that
    don't conform to that pattern are allowed (i.e. free text is rejected).

    Note: date_value itself must be a `str`. Anything already parsed into a
    datetime/date object is rejected by design -- the business rule is about
    the *format* of the raw string, so we require the raw string form and
    then confirm it maps to a real calendar date.
    """
    if not isinstance(date_value, str):
        return False
    if not DATE_PATTERN.match(date_value):
        return False
    try:
        datetime.strptime(date_value, "%m/%d/%y")
    except ValueError:
        return False
    return True


def is_valid_level(level_value) -> bool:
    """
    Rule: level must be one of ASSOCIATE, BACHELOR, MASTER, and matching is
    uppercase-sensitive. Deliberately does NOT call .upper() to normalize --
    "Bachelor" or "bachelor" must be rejected, not silently corrected.
    """
    if not isinstance(level_value, str):
        return False
    return level_value in VALID_LEVELS


def clean_string_field(value):
    """
    Generic trim/empty-check helper for free-text fields such as name,
    mail_address, living_address. Returns a trimmed string, or None if the
    value is missing/blank.
    """
    if value is None:
        return None
    v = str(value).strip()
    return v if v else None


# ---------------------------------------------------------------------------
# Record-level validation (convenience wrapper, handy for tests/dry-runs)
# ---------------------------------------------------------------------------
def validate_record(record: dict) -> dict:
    """
    Validates a single alumnus record (dict) against all business rules.

    Expected keys: name, age, date_of_joining, level, mail_address,
    living_address (mail/living address are not subject to a specific
    business rule here beyond basic non-empty cleansing).

    Returns:
        {
          "is_valid": bool,
          "rejection_reasons": [str, ...]
        }
    """
    reasons = []

    if not is_valid_age(record.get("age")):
        reasons.append("INVALID_AGE")

    if not is_valid_date_format(record.get("date_of_joining")):
        reasons.append("INVALID_DATE_FORMAT")

    if not is_valid_level(record.get("level")):
        reasons.append("INVALID_LEVEL")

    if not clean_string_field(record.get("name")):
        reasons.append("MISSING_NAME")

    return {"is_valid": len(reasons) == 0, "rejection_reasons": reasons}
