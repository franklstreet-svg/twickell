MODULE_MANIFEST = {
    "id": "M001",
    "name": "Reminders",
    "category": "Core",
    "description": "Set and manage personal reminders with natural language scheduling and recurring options.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["set_reminder", "get_reminders", "complete_reminder"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime, timezone
from engine.storage import load, save, now_iso, today_str, new_id


def add(profile_dir, text, when, recurring=None):
    items = load(profile_dir, 'reminders')
    item = {'id': new_id(), 'text': text, 'when': when,
            'recurring': recurring, 'done': False, 'created': now_iso()}
    items.append(item)
    save(profile_dir, 'reminders', items)
    return f"Reminder set: '{text}' at {when}"


def get_all(profile_dir):
    return load(profile_dir, 'reminders')


def get_due(profile_dir):
    now = datetime.now(timezone.utc).isoformat()
    items = load(profile_dir, 'reminders')
    due = []
    for r in items:
        if not r.get('done') and r.get('when', '9999') <= now:
            due.append(r)
    return due


def complete(profile_dir, reminder_id):
    items = load(profile_dir, 'reminders')
    for r in items:
        if r['id'] == reminder_id:
            r['done'] = True
            r['completed_at'] = now_iso()
    save(profile_dir, 'reminders', items)
    return "Reminder marked done."


def delete(profile_dir, reminder_id):
    items = load(profile_dir, 'reminders')
    items = [r for r in items if r['id'] != reminder_id]
    save(profile_dir, 'reminders', items)
    return "Reminder deleted."


def get_upcoming(profile_dir, limit=5):
    now = datetime.now(timezone.utc).isoformat()
    items = [r for r in load(profile_dir, 'reminders')
             if not r.get('done') and r.get('when', '') >= now]
    items.sort(key=lambda x: x.get('when', ''))
    return items[:limit]


def context_summary(profile_dir) -> str:
    due = get_due(profile_dir)
    upcoming = get_upcoming(profile_dir)
    parts = []
    if due:
        parts.append("REMINDERS DUE NOW: " + "; ".join(r['text'] for r in due))
    if upcoming:
        parts.append("UPCOMING REMINDERS: " + "; ".join(
            f"{r['text']} ({r.get('when','')})" for r in upcoming))
    return "\n".join(parts)
