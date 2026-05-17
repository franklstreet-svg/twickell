MODULE_MANIFEST = {
    "id": "M005",
    "name": "Shopping Lists",
    "category": "Core",
    "description": "Build and manage shopping lists, check off items, and organize by store or category.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_shopping_item", "get_shopping_list", "check_item"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def add_item(profile_dir, item, quantity='1', family=False):
    key = 'family_shopping' if family else 'shopping'
    items = load(profile_dir, key)
    entry = {'id': new_id(), 'item': item, 'quantity': quantity,
             'bought': False, 'added': now_iso()}
    items.append(entry)
    save(profile_dir, key, items)
    dest = "family shopping list" if family else "your shopping list"
    return f"Added {quantity}x {item} to {dest}."


def get_list(profile_dir, family=False):
    key = 'family_shopping' if family else 'shopping'
    return [i for i in load(profile_dir, key) if not i.get('bought')]


def mark_bought(profile_dir, item_id, family=False):
    key = 'family_shopping' if family else 'shopping'
    items = load(profile_dir, key)
    for i in items:
        if i['id'] == item_id:
            i['bought'] = True
    save(profile_dir, key, items)
    return "Marked as bought."


def clear_bought(profile_dir, family=False):
    key = 'family_shopping' if family else 'shopping'
    items = [i for i in load(profile_dir, key) if not i.get('bought')]
    save(profile_dir, key, items)
    return "Cleared bought items."


def context_summary(profile_dir) -> str:
    personal = get_list(profile_dir, family=False)
    family = get_list(profile_dir, family=True)
    parts = []
    if personal:
        parts.append("SHOPPING LIST: " + ", ".join(i['item'] for i in personal))
    if family:
        parts.append("FAMILY SHOPPING LIST: " + ", ".join(i['item'] for i in family))
    return "\n".join(parts)
