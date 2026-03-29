from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional


def parse_date(value: Optional[str]) -> Optional[date]:
    if not value or str(value).strip() in ("", "None", "N/A", "-"):
        return None
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",  # detail page: "5/3/2005 12:00:00 AM"
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


HIGH_CASE_TYPES = {"hearing"}
LOW_CASE_TYPES  = {"property management training program", "training program"}

# current_status keywords that indicate the case is effectively resolved
# even if not formally closed in the system
RESOLVED_STATUS_KEYWORDS = {
    "all violations resolved",
    "no violations observed",
    "complaint closed",
    "case closed",
    "violation corrected",
    "resolved",
    "closed",
    "complied",
}

# current_status keywords that signal active enforcement — always High
ENFORCEMENT_STATUS_KEYWORDS = {
    "referred to enforcement",
    "notice of violation",
    "order to comply",
    "citation issued",
    "penalty",
    "lien",
    "hearing scheduled",
    "appeal received",
}


def _status_urgency(current_status: str) -> Optional[str]:
    """
    Infer priority from the last known activity text.
    Returns 'High', 'Low', or None (= no override, fall through to other logic).
    """
    cs = current_status.lower()
    if any(kw in cs for kw in ENFORCEMENT_STATUS_KEYWORDS):
        return "High"
    if any(kw in cs for kw in RESOLVED_STATUS_KEYWORDS):
        return "Low"
    return None


def calc_priority(
    close_date: Optional[date],
    open_date: Optional[date] = None,
    case_type: str = "",
    current_status: str = "",
) -> str:
    # Formally closed → always Low
    if close_date is not None:
        return "Low"

    ct = case_type.lower().strip()

    # Smart override: last activity reveals true state
    status_override = _status_urgency(current_status)
    if status_override == "Low":
        return "Low"   # effectively resolved despite being formally open

    # Hearing → always High regardless of age
    if ct in HIGH_CASE_TYPES:
        return status_override or "High"

    # Training Program → Low unless enforcement action is active
    if ct in LOW_CASE_TYPES:
        return status_override or "Low"

    # Time-based priority for remaining open cases
    if open_date is not None:
        days_open = (date.today() - open_date).days
        if status_override == "High" or days_open > 30:
            return "High"
        if days_open > 7:
            return "Medium"
        return "Low"

    # Fallback
    return status_override or "Medium"


def check_is_new(open_date: Optional[date], close_date: Optional[date] = None) -> bool:
    if open_date is not None:
        return (date.today() - open_date).days <= 7
    if close_date is not None:
        return (date.today() - close_date).days <= 7
    return False


@dataclass
class Case:
    case_number: str
    apn: str
    case_type: str
    status: str           # Open / Closed
    current_status: str   # last activity from detail page
    open_date: Optional[date]
    close_date: Optional[date]
    address: str
    inspector: str
    council_district: str
    activity_count: int
    priority: str
    is_new: bool
    scraped_at: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_dict(cls, data: dict, apn: str) -> "Case":
        open_date = parse_date(data.get("open_date"))
        close_date = parse_date(data.get("close_date"))
        status = "Closed" if close_date else "Open"
        case_type = str(data.get("case_type", "")).strip()
        return cls(
            case_number=str(data.get("case_number", "")).strip(),
            apn=apn,
            case_type=case_type,
            status=status,
            current_status=str(data.get("current_status", "")).strip(),
            open_date=open_date,
            close_date=close_date,
            address=str(data.get("address", "")).strip(),
            inspector=str(data.get("inspector", "")).strip(),
            council_district=str(data.get("council_district", "")).strip(),
            activity_count=int(data.get("activity_count", 0)),
            priority=calc_priority(close_date, open_date, case_type, str(data.get("current_status", ""))),
            is_new=check_is_new(open_date, close_date),
        )

    def to_dict(self) -> dict:
        return {
            "case_number": self.case_number,
            "apn": self.apn,
            "case_type": self.case_type,
            "status": self.status,
            "current_status": self.current_status,
            "open_date": self.open_date.isoformat() if self.open_date else None,
            "close_date": self.close_date.isoformat() if self.close_date else None,
            "address": self.address,
            "inspector": self.inspector,
            "council_district": self.council_district,
            "activity_count": self.activity_count,
            "priority": self.priority,
            "is_new": self.is_new,
            "scraped_at": self.scraped_at.isoformat(),
        }
