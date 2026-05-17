MODULE_MANIFEST = {
    "id": "M024",
    "name": "Home Maintenance",
    "category": "Home",
    "description": "Log home repairs, schedule maintenance tasks, and track warranties and service history.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["log_maintenance", "get_maintenance_log", "schedule_maintenance"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def log_repair(profile_dir, item, description, cost=None, contractor='', date=''):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    data['repairs'].append({
        'id': new_id(),
        'item': item,
        'description': description,
        'cost': cost,
        'contractor': contractor,
        'date': date or today_str()
    })
    save(profile_dir, 'home_maintenance', data)
    cost_str = f" (${cost:.2f})" if cost is not None else ''
    return f"Home repair logged: {item} — {description}{cost_str}."


def add_warranty(profile_dir, item, expires, purchase_date='', notes=''):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    data['warranties'].append({
        'id': new_id(),
        'item': item,
        'expires': expires,
        'purchase_date': purchase_date,
        'notes': notes,
        'added': today_str()
    })
    save(profile_dir, 'home_maintenance', data)
    return f"Warranty saved: {item} — expires {expires}."


def add_contractor(profile_dir, name, trade, phone='', email='', notes=''):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    data['contractors'].append({
        'id': new_id(),
        'name': name,
        'trade': trade,
        'phone': phone,
        'email': email,
        'notes': notes
    })
    save(profile_dir, 'home_maintenance', data)
    return f"Contractor saved: {name} ({trade}){', ' + phone if phone else ''}."


def find_contractor(profile_dir, trade=''):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    contractors = data['contractors']
    if trade:
        contractors = [c for c in contractors if trade.lower() in c.get('trade', '').lower()
                       or trade.lower() in c.get('name', '').lower()]
    return contractors


def get_repair_history(profile_dir, item='', limit=10):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    repairs = data['repairs']
    if item:
        repairs = [r for r in repairs if item.lower() in r.get('item', '').lower()]
    return repairs[-limit:]


def get_warranties(profile_dir):
    data = load(profile_dir, 'home_maintenance', {'repairs': [], 'warranties': [], 'contractors': []})
    return data['warranties']


def context_summary(profile_dir):
    return ''
