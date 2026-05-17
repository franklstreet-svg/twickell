MODULE_MANIFEST = {
    "id": "M003",
    "name": "To-Do Lists",
    "category": "Core",
    "description": "Create, manage, and complete personal and family to-do lists with priorities.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_todo", "get_todos", "complete_todo"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def add(profile_dir, text, family=False, priority='normal'):
    key = 'family_todo' if family else 'todo'
    items = load(profile_dir, key)
    item = {'id': new_id(), 'text': text, 'done': False,
            'priority': priority, 'created': now_iso()}
    items.append(item)
    save(profile_dir, key, items)
    dest = "family to-do list" if family else "your to-do list"
    return f"Added '{text}' to {dest}."


def get_open(profile_dir, family=False):
    key = 'family_todo' if family else 'todo'
    return [t for t in load(profile_dir, key) if not t.get('done')]


def complete(profile_dir, item_id, family=False):
    key = 'family_todo' if family else 'todo'
    items = load(profile_dir, key)
    for t in items:
        if t['id'] == item_id:
            t['done'] = True
            t['completed_at'] = now_iso()
    save(profile_dir, key, items)
    return "Marked done."


def delete(profile_dir, item_id, family=False):
    key = 'family_todo' if family else 'todo'
    items = load(profile_dir, key)
    items = [t for t in items if t['id'] != item_id]
    save(profile_dir, key, items)
    return "Removed."


def list_all(profile_dir, family=False):
    key = 'family_todo' if family else 'todo'
    return load(profile_dir, key)


def context_summary(profile_dir) -> str:
    personal = get_open(profile_dir, family=False)
    family = get_open(profile_dir, family=True)
    parts = []
    if personal:
        parts.append(f"YOUR TO-DO LIST ({len(personal)} open): " +
                     "; ".join(t['text'] for t in personal[:5]))
    if family:
        parts.append(f"FAMILY TO-DO ({len(family)} open): " +
                     "; ".join(t['text'] for t in family[:5]))
    return "\n".join(parts)
