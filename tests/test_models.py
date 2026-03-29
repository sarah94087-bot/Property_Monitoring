"""
Tests for models.py — priority logic, date parsing, Case construction.
Run: pytest tests/
"""

import pytest
from datetime import date, timedelta
from models import (
    parse_date,
    calc_priority,
    check_is_new,
    _status_urgency,
    Case,
)

TODAY = date.today()


# ── parse_date ─────────────────────────────────────────────────────────────────

class TestParseDate:
    def test_mm_dd_yyyy(self):
        assert parse_date("05/11/2005") == date(2005, 5, 11)

    def test_iso_format(self):
        assert parse_date("2005-05-11") == date(2005, 5, 11)

    def test_datetime_with_am_pm(self):
        # Format from detail page: "5/3/2005 12:00:00 AM"
        assert parse_date("5/3/2005 12:00:00 AM") == date(2005, 5, 3)

    def test_datetime_pm(self):
        assert parse_date("11/5/2007 11:08:00 AM") == date(2007, 11, 5)

    def test_empty_string(self):
        assert parse_date("") is None

    def test_none(self):
        assert parse_date(None) is None

    def test_na(self):
        assert parse_date("N/A") is None

    def test_dash(self):
        assert parse_date("-") is None


# ── _status_urgency ────────────────────────────────────────────────────────────

class TestStatusUrgency:
    def test_all_violations_resolved_is_low(self):
        assert _status_urgency("All Violations Resolved Date") == "Low"

    def test_no_violations_is_low(self):
        assert _status_urgency("No Violations Observed") == "Low"

    def test_complaint_closed_is_low(self):
        assert _status_urgency("Complaint Closed") == "Low"

    def test_referred_to_enforcement_is_high(self):
        assert _status_urgency("Referred to Enforcement Section") == "High"

    def test_appeal_received_is_high(self):
        assert _status_urgency("Senior Inspector Appeal Received") == "High"

    def test_citation_issued_is_high(self):
        assert _status_urgency("Citation Issued") == "High"

    def test_neutral_status_returns_none(self):
        assert _status_urgency("Site Visit/Initial Inspection") is None

    def test_empty_status_returns_none(self):
        assert _status_urgency("") is None

    def test_case_insensitive(self):
        assert _status_urgency("ALL VIOLATIONS RESOLVED") == "Low"


# ── calc_priority ──────────────────────────────────────────────────────────────

class TestCalcPriority:
    def test_closed_case_is_always_low(self):
        assert calc_priority(close_date=TODAY) == "Low"

    def test_closed_case_ignores_open_date(self):
        old_date = TODAY - timedelta(days=500)
        assert calc_priority(close_date=TODAY, open_date=old_date) == "Low"

    def test_resolved_status_overrides_to_low(self):
        # Formally open, but last activity says resolved
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=1000),
            current_status="All Violations Resolved Date",
        ) == "Low"

    def test_enforcement_status_overrides_to_high(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=5),  # very recent, would normally be Low
            current_status="Referred to Enforcement Section",
        ) == "High"

    def test_hearing_open_is_high(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=10),
            case_type="Hearing",
        ) == "High"

    def test_training_program_open_is_low(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=10),
            case_type="Property Management Training Program",
        ) == "Low"

    def test_training_program_with_enforcement_is_high(self):
        # Even Training Program becomes High if enforcement is active
        assert calc_priority(
            close_date=None,
            case_type="Property Management Training Program",
            current_status="Referred to Enforcement Section",
        ) == "High"

    def test_open_over_30_days_is_high(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=31),
        ) == "High"

    def test_open_7_to_30_days_is_medium(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=15),
        ) == "Medium"

    def test_open_under_7_days_is_low(self):
        assert calc_priority(
            close_date=None,
            open_date=TODAY - timedelta(days=3),
        ) == "Low"

    def test_no_dates_no_type_is_medium(self):
        assert calc_priority(close_date=None) == "Medium"


# ── check_is_new ───────────────────────────────────────────────────────────────

class TestCheckIsNew:
    def test_opened_today_is_new(self):
        assert check_is_new(open_date=TODAY) is True

    def test_opened_7_days_ago_is_new(self):
        assert check_is_new(open_date=TODAY - timedelta(days=7)) is True

    def test_opened_8_days_ago_is_not_new(self):
        assert check_is_new(open_date=TODAY - timedelta(days=8)) is False

    def test_no_open_date_falls_back_to_close_date(self):
        assert check_is_new(open_date=None, close_date=TODAY - timedelta(days=3)) is True

    def test_old_close_date_is_not_new(self):
        assert check_is_new(open_date=None, close_date=TODAY - timedelta(days=100)) is False

    def test_no_dates_is_not_new(self):
        assert check_is_new(open_date=None, close_date=None) is False


# ── Case.from_dict ─────────────────────────────────────────────────────────────

class TestCaseFromDict:
    def _base(self, **kwargs):
        data = {
            "case_number": "12345",
            "case_type": "Complaint",
            "open_date": None,
            "close_date": None,
            "current_status": "",
            "address": "",
            "inspector": "",
            "council_district": "",
            "activity_count": 0,
        }
        data.update(kwargs)
        return Case.from_dict(data, apn="2654002037")

    def test_closed_case_status(self):
        case = self._base(close_date="05/11/2005")
        assert case.status == "Closed"

    def test_open_case_status(self):
        case = self._base(close_date=None)
        assert case.status == "Open"

    def test_priority_computed_correctly(self):
        case = self._base(
            close_date=None,
            open_date=(TODAY - timedelta(days=60)).isoformat(),
            case_type="Hearing",
        )
        assert case.priority == "High"

    def test_is_new_flag(self):
        case = self._base(open_date=TODAY.isoformat())
        assert case.is_new is True

    def test_apn_stored(self):
        case = self._base()
        assert case.apn == "2654002037"

    def test_to_dict_roundtrip(self):
        case = self._base(
            case_number="99999",
            close_date="2024-01-01",
            current_status="Complaint Closed",
        )
        d = case.to_dict()
        assert d["case_number"] == "99999"
        assert d["status"] == "Closed"
        assert d["priority"] == "Low"
