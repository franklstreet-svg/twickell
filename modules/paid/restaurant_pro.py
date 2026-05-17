"""Restaurant Pro — Menu, inventory, reservations, and supplier management."""
MODULE_MANIFEST = {
    "id": "I004",
    "name": "Restaurant Pro",
    "category": "Industry",
    "description": "Complete restaurant management. Build your menu, track inventory, manage reservations, and organize suppliers — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_menu_item", "list_menu", "update_menu_item",
              "update_inventory", "get_low_inventory", "add_reservation",
              "list_reservations", "add_supplier"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
from pathlib import Path


# ── Storage helpers ───────────────────────────────────────────────────────────

def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'industry'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename


def _load(path: Path, default=None):
    if path.exists():
        return json.loads(path.read_text())
    return default if default is not None else {}


def _save(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now() -> str:
    return datetime.utcnow().isoformat()


def _uid() -> str:
    return str(uuid.uuid4())[:8]


# ── Public tools ──────────────────────────────────────────────────────────────

def add_menu_item(profile_dir: str, name: str, category: str, price: float,
                  description: str = '', available: bool = True) -> str:
    path = _path(profile_dir, 'restaurant_menu.json')
    menu = _load(path, [])
    item = {
        'id': _uid(),
        'name': name,
        'category': category,
        'price': price,
        'description': description,
        'available': available,
        'created_at': _now()
    }
    menu.append(item)
    _save(path, menu)
    avail = "available" if available else "unavailable"
    return f"Menu item added [id:{item['id']}] — {name} (${price:.2f}) [{category}] — {avail}."


def list_menu(profile_dir: str, category: str = '') -> str:
    path = _path(profile_dir, 'restaurant_menu.json')
    menu = _load(path, [])
    if category:
        menu = [i for i in menu if i.get('category', '').lower() == category.lower()]
    if not menu:
        return f"No menu items found{' in category ' + category if category else ''}."
    # Group by category
    cats = {}
    for item in menu:
        cat = item.get('category', 'Other')
        cats.setdefault(cat, []).append(item)
    lines = ["Menu:"]
    for cat, items in cats.items():
        lines.append(f"  {cat}:")
        for i in items:
            avail = '' if i.get('available', True) else ' [UNAVAIL]'
            lines.append(f"    [{i['id']}] {i['name']} — ${i.get('price',0):.2f}{avail}")
    return '\n'.join(lines)


def update_menu_item(profile_dir: str, item_id: str, price: float = 0,
                     available=None, description: str = '') -> str:
    path = _path(profile_dir, 'restaurant_menu.json')
    menu = _load(path, [])
    item = next((i for i in menu if i['id'] == item_id), None)
    if not item:
        return f"Menu item '{item_id}' not found."
    if price:
        item['price'] = price
    if available is not None:
        item['available'] = available
    if description:
        item['description'] = description
    item['updated_at'] = _now()
    _save(path, menu)
    return f"Menu item {item_id} updated — {item['name']}."


def update_inventory(profile_dir: str, item_name: str, quantity: float,
                     unit: str, reorder_level: float = 0) -> str:
    path = _path(profile_dir, 'restaurant_inventory.json')
    inventory = _load(path, {})
    key = item_name.lower()
    inventory[key] = {
        'name': item_name,
        'quantity': quantity,
        'unit': unit,
        'reorder_level': reorder_level,
        'updated_at': _now()
    }
    _save(path, inventory)
    low = ' — LOW STOCK' if quantity <= reorder_level and reorder_level > 0 else ''
    return f"Inventory updated: {item_name} — {quantity} {unit}{low}."


def get_low_inventory(profile_dir: str) -> str:
    path = _path(profile_dir, 'restaurant_inventory.json')
    inventory = _load(path, {})
    low = [v for v in inventory.values()
           if v.get('reorder_level', 0) > 0 and v.get('quantity', 0) <= v.get('reorder_level', 0)]
    if not low:
        return "All inventory items are above reorder levels."
    lines = [f"Low inventory items ({len(low)}):"]
    for item in low:
        lines.append(f"  • {item['name']}: {item['quantity']} {item['unit']} (reorder at {item['reorder_level']})")
    return '\n'.join(lines)


def add_reservation(profile_dir: str, name: str, date: str, time: str,
                    party_size: int, phone: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'restaurant_reservations.json')
    reservations = _load(path, [])
    res = {
        'id': _uid(),
        'name': name,
        'date': date,
        'time': time,
        'party_size': party_size,
        'phone': phone,
        'notes': notes,
        'created_at': _now()
    }
    reservations.append(res)
    _save(path, reservations)
    return (f"Reservation booked [id:{res['id']}] — {name}, party of {party_size} "
            f"on {date} at {time}" + (f" — {phone}" if phone else "") + ".")


def list_reservations(profile_dir: str, date: str = '') -> str:
    path = _path(profile_dir, 'restaurant_reservations.json')
    reservations = _load(path, [])
    today = _now()[:10]
    if date:
        reservations = [r for r in reservations if r.get('date', '') == date]
    else:
        reservations = [r for r in reservations if r.get('date', '') >= today]
    if not reservations:
        return f"No reservations found{' for ' + date if date else ''}."
    reservations = sorted(reservations, key=lambda r: (r.get('date', ''), r.get('time', '')))
    lines = [f"Reservations ({len(reservations)}):"]
    for r in reservations:
        lines.append(f"  [{r['id']}] {r['date']} {r['time']} — {r['name']}, party of {r.get('party_size',1)}")
    return '\n'.join(lines)


def add_supplier(profile_dir: str, name: str, category: str, contact: str,
                 phone: str = '', email: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'restaurant_suppliers.json')
    suppliers = _load(path, [])
    supplier = {
        'id': _uid(),
        'name': name,
        'category': category,
        'contact': contact,
        'phone': phone,
        'email': email,
        'notes': notes,
        'created_at': _now()
    }
    suppliers.append(supplier)
    _save(path, suppliers)
    return f"Supplier added [id:{supplier['id']}] — {name} ({category}), contact: {contact}."


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    menu = _load(_path(profile_dir, 'restaurant_menu.json'), [])
    if not menu:
        return ''
    today = _now()[:10]
    reservations = _load(_path(profile_dir, 'restaurant_reservations.json'), [])
    today_res = [r for r in reservations if r.get('date', '') == today]
    inventory = _load(_path(profile_dir, 'restaurant_inventory.json'), {})
    low = [v for v in inventory.values()
           if v.get('reorder_level', 0) > 0 and v.get('quantity', 0) <= v.get('reorder_level', 0)]
    parts = [f"RESTAURANT PRO ACTIVE: {len(menu)} menu item(s)"]
    if today_res:
        total_covers = sum(r.get('party_size', 0) for r in today_res)
        parts.append(f"  Today: {len(today_res)} reservation(s), {total_covers} covers")
    if low:
        parts.append(f"  {len(low)} inventory item(s) need reordering")
    parts.append(
        "What I can help you with:\n"
        "  Track: menu items, inventory, reservations, staff schedules, suppliers, waste log\n"
        "  Write: daily specials descriptions, menu item copy, supplier order emails,\n"
        "         staff schedules, task briefings, health inspection prep notes,\n"
        "         customer complaint response letters, catering proposals\n"
        "  Know: food cost analysis, general food safety practices, supplier management basics"
    )
    return '\n'.join(parts)
