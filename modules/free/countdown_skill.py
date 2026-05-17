MODULE_MANIFEST = {
    "id": "M028",
    "name": "Countdowns",
    "category": "Lifestyle",
    "description": "Create countdown timers for upcoming events and milestones.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_countdown", "get_countdowns"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from engine.storage import load, save, now_iso, new_id


def add_countdown(profile_dir, name, target_date):
    items = load(profile_dir, 'countdowns')
    item = {'id': new_id(), 'name': name, 'target_date': target_date, 'created': now_iso()}
    items.append(item)
    save(profile_dir, 'countdowns', items)
    days = _days_until(target_date)
    return f"Countdown set: {days} days until {name}."


def get_all(profile_dir):
    items = load(profile_dir, 'countdowns')
    result = []
    for item in items:
        days = _days_until(item.get('target_date', ''))
        if days is not None and days >= 0:
            result.append({**item, 'days_remaining': days})
    return sorted(result, key=lambda x: x['days_remaining'])


def delete(profile_dir, countdown_id):
    items = [c for c in load(profile_dir, 'countdowns') if c['id'] != countdown_id]
    save(profile_dir, 'countdowns', items)
    return "Countdown removed."


def _days_until(date_str: str):
    try:
        target = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        today = datetime.now().date()
        return (target - today).days
    except Exception:
        return None


def context_summary(profile_dir) -> str:
    items = get_all(profile_dir)
    if not items:
        return ''
    soon = [i for i in items if i['days_remaining'] <= 30]
    if soon:
        return "COUNTDOWNS: " + " | ".join(
            f"{i['name']} in {i['days_remaining']} days" for i in soon[:3])
    return ''
