from datetime import date, timedelta, datetime
from typing import Optional
import pytz

from config import TIMEZONE

TZ = pytz.timezone(TIMEZONE)


def html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_mention(user_id: Optional[int], username: Optional[str], display_name: str) -> str:
    """Return an HTML inline mention if user_id is known, else @username or plain name."""
    if user_id:
        safe = html_escape(display_name)
        return f'<a href="tg://user?id={user_id}">{safe}</a>'
    if username:
        return f"@{username}"
    return html_escape(display_name)


def get_almaty_now() -> datetime:
    return datetime.now(TZ)


def get_almaty_today() -> date:
    return datetime.now(TZ).date()


def get_current_week_bounds() -> tuple[date, date]:
    """Return (Monday, Sunday) of the current week in Almaty time."""
    today = get_almaty_today()
    week_start = today - timedelta(days=today.weekday())   # Monday
    week_end = week_start + timedelta(days=6)              # Sunday
    return week_start, week_end


def get_prev_week_bounds() -> tuple[date, date]:
    """Return (Monday, Sunday) of the previous week in Almaty time."""
    today = get_almaty_today()
    current_monday = today - timedelta(days=today.weekday())
    prev_monday = current_monday - timedelta(days=7)
    prev_sunday = current_monday - timedelta(days=1)
    return prev_monday, prev_sunday


def make_poll_link(group_id: int, message_id: int) -> Optional[str]:
    """Build a t.me deep link for a supergroup message. Returns None for basic groups."""
    gid_str = str(group_id)
    if gid_str.startswith("-100"):
        channel_id = gid_str[4:]          # strip the -100 prefix
        return f"https://t.me/c/{channel_id}/{message_id}"
    return None


def days_since_join(joined_at_iso: str, week_start: date) -> int:
    """Number of days in the current week that a participant was enrolled (1-7)."""
    try:
        joined = date.fromisoformat(joined_at_iso[:10])
    except (ValueError, TypeError):
        return 7
    effective_start = max(joined, week_start)
    today = get_almaty_today()
    return max(1, (today - effective_start).days + 1)
