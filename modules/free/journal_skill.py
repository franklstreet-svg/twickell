MODULE_MANIFEST = {
    "id": "M010",
    "name": "Journal",
    "category": "Wellness",
    "description": "Write and retrieve personal journal entries with date-based organization.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_journal_entry", "get_journal_entries"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def add_entry(profile_dir, content, mood=None, is_dream=False):
    key = 'dream_journal' if is_dream else 'journal'
    items = load(profile_dir, key)
    entry = {'id': new_id(), 'date': today_str(), 'content': content,
             'mood': mood, 'created': now_iso()}
    items.append(entry)
    save(profile_dir, key, items)
    label = "dream" if is_dream else "journal entry"
    return f"Your {label} has been saved."


def get_entries(profile_dir, limit=10, is_dream=False):
    key = 'dream_journal' if is_dream else 'journal'
    items = load(profile_dir, key)
    return sorted(items, key=lambda x: x.get('created', ''), reverse=True)[:limit]


def get_by_date(profile_dir, date_str, is_dream=False):
    key = 'dream_journal' if is_dream else 'journal'
    return [e for e in load(profile_dir, key) if e.get('date') == date_str]


def search(profile_dir, query, is_dream=False):
    key = 'dream_journal' if is_dream else 'journal'
    q = query.lower()
    return [e for e in load(profile_dir, key) if q in e.get('content', '').lower()]


def memory_lane(profile_dir):
    """Find entries from this same date in past years."""
    from datetime import datetime
    today = datetime.now()
    today_mmdd = today.strftime('%m-%d')
    past = []
    for e in load(profile_dir, 'journal'):
        date = e.get('date', '')
        if date and date[5:] == today_mmdd and date[:4] != str(today.year):
            past.append(e)
    return sorted(past, key=lambda x: x.get('date', ''), reverse=True)
