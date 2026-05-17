MODULE_MANIFEST = {
    "id": "M002",
    "name": "Calendar",
    "category": "Core",
    "description": "Manage personal and family calendar events, scheduling, and upcoming event lookup.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_calendar_event", "get_calendar"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from engine.storage import load, save, now_iso, today_str, new_id


def add_event(profile_dir, title, start, end=None, notes='', family=False):
    key = 'family_calendar' if family else 'calendar'
    items = load(profile_dir, key)
    event = {'id': new_id(), 'title': title, 'start': start,
             'end': end or start, 'notes': notes, 'created': now_iso()}
    items.append(event)
    save(profile_dir, key, items)
    dest = "family calendar" if family else "your calendar"
    return f"Added '{title}' to {dest} on {start}."


def get_today(profile_dir, family=False):
    key = 'family_calendar' if family else 'calendar'
    today = today_str()
    return [e for e in load(profile_dir, key) if e.get('start', '').startswith(today)]


def get_range(profile_dir, start_date, end_date, family=False):
    key = 'family_calendar' if family else 'calendar'
    return [e for e in load(profile_dir, key)
            if start_date <= e.get('start', '') <= end_date]


def delete_event(profile_dir, event_id, family=False):
    key = 'family_calendar' if family else 'calendar'
    items = load(profile_dir, key)
    items = [e for e in items if e['id'] != event_id]
    save(profile_dir, key, items)
    return "Event removed."


def list_all(profile_dir, family=False):
    key = 'family_calendar' if family else 'calendar'
    return load(profile_dir, key)


def context_summary(profile_dir) -> str:
    today = get_today(profile_dir, family=False)
    family = get_today(profile_dir, family=True)
    parts = []
    if today:
        parts.append("YOUR SCHEDULE TODAY: " + "; ".join(
            f"{e['title']} at {e.get('start','')}" for e in today))
    if family:
        parts.append("FAMILY CALENDAR TODAY: " + "; ".join(
            f"{e['title']} at {e.get('start','')}" for e in family))
    return "\n".join(parts)
