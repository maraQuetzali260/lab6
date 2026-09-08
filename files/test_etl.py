"""
test_etl.py
-----------
Unit tests for the alumnus ETL cleansing rules (cleansing_rules.py).

These tests do NOT require Spark, boto3, or the AWS Glue libraries --
they exercise the pure-Python validators directly, which is what makes
them fast enough to run on every commit / in CI.

Run with:
    pip install pytest --break-system-packages
    pytest test_etl.py -v
"""

import pytest

from cleansing_rules import (
    is_valid_age,
    is_valid_date_format,
    is_valid_level,
    clean_string_field,
    validate_record,
)


# ---------------------------------------------------------------------------
# AGE rule: must not exceed 100
# ---------------------------------------------------------------------------
class TestAgeRule:
    @pytest.mark.parametrize("age", [1, 25, 99, 100])
    def test_valid_ages(self, age):
        assert is_valid_age(age) is True

    def test_age_exactly_100_is_valid(self):
        assert is_valid_age(100) is True

    def test_age_101_exceeds_limit(self):
        assert is_valid_age(101) is False

    def test_age_far_over_limit(self):
        assert is_valid_age(150) is False

    def test_age_zero_is_invalid(self):
        assert is_valid_age(0) is False

    def test_negative_age_is_invalid(self):
        assert is_valid_age(-5) is False

    def test_none_age_is_invalid(self):
        assert is_valid_age(None) is False

    def test_non_numeric_string_age_is_invalid(self):
        assert is_valid_age("twenty") is False

    def test_numeric_string_age_is_valid(self):
        # tolerate ages coming through as strings from CSV sources
        assert is_valid_age("45") is True

    def test_empty_string_age_is_invalid(self):
        assert is_valid_age("") is False


# ---------------------------------------------------------------------------
# DATE rule: must be a string strictly in mm/dd/yy format, no free text
# ---------------------------------------------------------------------------
class TestDateFormatRule:
    @pytest.mark.parametrize(
        "date_str",
        ["01/15/20", "12/31/99", "02/29/20", "07/04/76"],
    )
    def test_valid_dates(self, date_str):
        assert is_valid_date_format(date_str) is True

    def test_none_date_is_invalid(self):
        assert is_valid_date_format(None) is False

    def test_non_string_date_is_invalid(self):
        # a datetime/date object (or int) is rejected -- rule requires the
        # raw string representation, not a parsed object
        import datetime

        assert is_valid_date_format(datetime.date(2020, 1, 15)) is False
        assert is_valid_date_format(20200115) is False

    def test_free_text_string_is_invalid(self):
        assert is_valid_date_format("not a date") is False

    def test_wrong_format_yyyy_mm_dd_is_invalid(self):
        assert is_valid_date_format("2020-01-15") is False

    def test_wrong_format_dd_mm_yy_is_invalid_when_month_out_of_range(self):
        # 31/12/20 -- "31" is not a valid month
        assert is_valid_date_format("31/12/20") is False

    def test_four_digit_year_is_invalid(self):
        assert is_valid_date_format("01/15/2020") is False

    def test_invalid_calendar_date_feb_30(self):
        assert is_valid_date_format("02/30/21") is False

    def test_month_13_is_invalid(self):
        assert is_valid_date_format("13/01/20") is False

    def test_day_32_is_invalid(self):
        assert is_valid_date_format("01/32/20") is False

    def test_single_digit_without_leading_zero_is_invalid(self):
        assert is_valid_date_format("1/5/20") is False

    def test_empty_string_is_invalid(self):
        assert is_valid_date_format("") is False


# ---------------------------------------------------------------------------
# LEVEL rule: must be exactly ASSOCIATE, BACHELOR or MASTER (case sensitive)
# ---------------------------------------------------------------------------
class TestLevelRule:
    @pytest.mark.parametrize("level", ["ASSOCIATE", "BACHELOR", "MASTER"])
    def test_valid_levels(self, level):
        assert is_valid_level(level) is True

    @pytest.mark.parametrize(
        "level", ["Associate", "bachelor", "Master", "master", "MASTERS"]
    )
    def test_case_mismatch_is_invalid(self, level):
        assert is_valid_level(level) is False

    def test_unknown_level_is_invalid(self):
        assert is_valid_level("PHD") is False

    def test_none_level_is_invalid(self):
        assert is_valid_level(None) is False

    def test_empty_string_level_is_invalid(self):
        assert is_valid_level("") is False

    def test_non_string_level_is_invalid(self):
        assert is_valid_level(123) is False

    def test_level_with_trailing_whitespace_is_invalid(self):
        # deliberately strict: " MASTER" / "MASTER " are not silently trimmed
        assert is_valid_level("MASTER ") is False
        assert is_valid_level(" MASTER") is False


# ---------------------------------------------------------------------------
# Free-text field cleansing helper (name / mail_address / living_address)
# ---------------------------------------------------------------------------
class TestCleanStringField:
    def test_trims_whitespace(self):
        assert clean_string_field("  Jane Doe  ") == "Jane Doe"

    def test_none_stays_none(self):
        assert clean_string_field(None) is None

    def test_blank_string_becomes_none(self):
        assert clean_string_field("   ") is None

    def test_non_string_is_stringified(self):
        assert clean_string_field(123) == "123"


# ---------------------------------------------------------------------------
# Full record validation (integration of all rules)
# ---------------------------------------------------------------------------
class TestValidateRecord:
    def test_fully_valid_record(self):
        record = {
            "name": "Jane Doe",
            "age": 34,
            "date_of_joining": "09/01/15",
            "level": "BACHELOR",
            "mail_address": "jane@example.com",
            "living_address": "123 Main St",
        }
        result = validate_record(record)
        assert result["is_valid"] is True
        assert result["rejection_reasons"] == []

    def test_record_with_multiple_violations(self):
        record = {
            "name": "",
            "age": 250,
            "date_of_joining": "2015-09-01",
            "level": "bachelor",
            "mail_address": "jane@example.com",
            "living_address": "123 Main St",
        }
        result = validate_record(record)
        assert result["is_valid"] is False
        assert set(result["rejection_reasons"]) == {
            "INVALID_AGE",
            "INVALID_DATE_FORMAT",
            "INVALID_LEVEL",
            "MISSING_NAME",
        }

    def test_record_with_single_violation_age(self):
        record = {
            "name": "John Smith",
            "age": 101,
            "date_of_joining": "01/01/10",
            "level": "MASTER",
            "mail_address": "john@example.com",
            "living_address": "456 Oak Ave",
        }
        result = validate_record(record)
        assert result["is_valid"] is False
        assert result["rejection_reasons"] == ["INVALID_AGE"]

    def test_missing_fields_entirely(self):
        result = validate_record({})
        assert result["is_valid"] is False
        assert "INVALID_AGE" in result["rejection_reasons"]
        assert "INVALID_DATE_FORMAT" in result["rejection_reasons"]
        assert "INVALID_LEVEL" in result["rejection_reasons"]
        assert "MISSING_NAME" in result["rejection_reasons"]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
