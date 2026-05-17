MODULE_MANIFEST = {
    "id": "M029",
    "name": "Bucket List",
    "category": "Lifestyle",
    "description": "Track life goals, dream experiences, and personal milestones to accomplish.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_bucket_item", "complete_bucket_item", "get_bucket_list"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def add_item(profile_dir, item, category='life', notes=''):
    items = load(profile_dir, 'bucket_list')
    entry = {'id': new_id(), 'item': item, 'category': category,
             'notes': notes, 'done': False, 'created': now_iso()}
    items.append(entry)
    save(profile_dir, 'bucket_list', items)
    return f"Added to your bucket list: '{item}'"


def complete_item(profile_dir, item_id, notes=''):
    items = load(profile_dir, 'bucket_list')
    for entry in items:
        if entry['id'] == item_id:
            entry['done'] = True
            entry['completed_date'] = today_str()
            if notes:
                entry['completion_notes'] = notes
    save(profile_dir, 'bucket_list', items)
    return "Marked as done — that's huge!"


def get_open(profile_dir, category=None):
    items = [i for i in load(profile_dir, 'bucket_list') if not i.get('done')]
    if category:
        items = [i for i in items if i.get('category', '').lower() == category.lower()]
    return items


def get_completed(profile_dir):
    return [i for i in load(profile_dir, 'bucket_list') if i.get('done')]


def get_all(profile_dir):
    return load(profile_dir, 'bucket_list')
