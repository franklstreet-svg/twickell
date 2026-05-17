MODULE_MANIFEST = {
    "id": "M030",
    "name": "Quotes Collection",
    "category": "Lifestyle",
    "description": "Save inspirational quotes, browse by category, and get a daily quote.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["save_quote", "get_quotes", "daily_quote"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def save_quote(profile_dir, text, author='', tags=None, source=''):
    items = load(profile_dir, 'quotes')
    entry = {'id': new_id(), 'text': text, 'author': author,
             'tags': tags or [], 'source': source, 'saved': today_str(), 'created': now_iso()}
    items.append(entry)
    save(profile_dir, 'quotes', items)
    return f"Quote saved" + (f" by {author}" if author else "") + "."


def get_random(profile_dir):
    import random
    items = load(profile_dir, 'quotes')
    if not items:
        return None
    return random.choice(items)


def get_all(profile_dir):
    return load(profile_dir, 'quotes')


def search(profile_dir, query):
    q = query.lower()
    return [q_ for q_ in load(profile_dir, 'quotes')
            if q in q_.get('text', '').lower() or q in q_.get('author', '').lower()
            or any(q in tag.lower() for tag in q_.get('tags', []))]


def daily_quote(profile_dir) -> str:
    from datetime import datetime
    items = load(profile_dir, 'quotes')
    if not items:
        return ''
    idx = datetime.now().timetuple().tm_yday % len(items)
    q = items[idx]
    text = q['text']
    if q.get('author'):
        text += f" — {q['author']}"
    return f"Your quote for today: \"{text}\""
