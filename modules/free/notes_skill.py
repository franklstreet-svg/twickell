MODULE_MANIFEST = {
    "id": "M004",
    "name": "Notes",
    "category": "Core",
    "description": "Take, search, and organize personal notes by category.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_note", "get_notes", "search_notes"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def add(profile_dir, content, title='', tags=None):
    items = load(profile_dir, 'notes')
    item = {'id': new_id(), 'title': title or content[:40],
            'content': content, 'tags': tags or [], 'created': now_iso()}
    items.append(item)
    save(profile_dir, 'notes', items)
    return f"Note saved: '{item['title']}'"


def search(profile_dir, query):
    q = query.lower()
    return [n for n in load(profile_dir, 'notes')
            if q in n.get('content', '').lower() or q in n.get('title', '').lower()
            or any(q in tag.lower() for tag in n.get('tags', []))]


def get_all(profile_dir):
    return load(profile_dir, 'notes')


def delete(profile_dir, note_id):
    items = load(profile_dir, 'notes')
    items = [n for n in items if n['id'] != note_id]
    save(profile_dir, 'notes', items)
    return "Note deleted."


def get_recent(profile_dir, limit=5):
    items = load(profile_dir, 'notes')
    return sorted(items, key=lambda x: x.get('created', ''), reverse=True)[:limit]
