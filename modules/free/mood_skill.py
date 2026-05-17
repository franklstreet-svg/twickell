MODULE_MANIFEST = {
    "id": "M009",
    "name": "Mood Tracker",
    "category": "Wellness",
    "description": "Log daily mood entries and track emotional patterns over time.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["log_mood", "get_mood_history"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


MOODS = ['great', 'good', 'okay', 'low', 'bad', 'anxious', 'sad', 'happy', 'excited', 'tired', 'stressed']


def log_mood(profile_dir, mood, notes=''):
    items = load(profile_dir, 'mood')
    entry = {'id': new_id(), 'date': today_str(), 'mood': mood.lower(),
             'notes': notes, 'created': now_iso()}
    items.append(entry)
    save(profile_dir, 'mood', items)
    return f"Mood logged: {mood}."


def get_recent(profile_dir, days=7):
    items = load(profile_dir, 'mood')
    return sorted(items, key=lambda x: x.get('created', ''), reverse=True)[:days]


def get_today(profile_dir):
    today = today_str()
    return [m for m in load(profile_dir, 'mood') if m.get('date') == today]


def mood_pattern(profile_dir, days=30):
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    recent = [m for m in load(profile_dir, 'mood') if m.get('date', '') >= cutoff]
    if not recent:
        return "No mood data yet."
    counts = {}
    for m in recent:
        mood = m.get('mood', 'unknown')
        counts[mood] = counts.get(mood, 0) + 1
    top = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return f"Most common moods (last {days} days): " + ", ".join(f"{m} ({c}x)" for m, c in top[:3])


def context_summary(profile_dir) -> str:
    today = get_today(profile_dir)
    if today:
        latest = today[-1]
        return f"TODAY'S MOOD: {latest.get('mood', 'unknown')}" + (
            f" — {latest['notes']}" if latest.get('notes') else '')
    return ''
