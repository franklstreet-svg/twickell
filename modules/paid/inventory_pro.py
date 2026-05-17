"""Inventory Pro — Full-scale inventory management. Configurable for any size operation."""
MODULE_MANIFEST = {
    "id": "I013",
    "name": "Inventory Pro",
    "category": "Business",
    "description": "Scalable inventory management from a single-location shop to a multi-warehouse operation. Track products, locations, stock levels, purchase orders, suppliers, and generate full reports — all configured your way.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["setup_inventory_config", "add_inventory_item", "list_inventory",
              "search_inventory", "update_stock_level", "bulk_update_stock",
              "add_inventory_location", "list_inventory_locations",
              "add_inventory_supplier", "create_purchase_order",
              "receive_purchase_order", "list_purchase_orders",
              "get_low_stock_report", "get_inventory_value_report",
              "get_stock_movement_report", "add_inventory_category"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
from pathlib import Path


# ── Storage helpers ───────────────────────────────────────────────────────────

def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'inventory'
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


# ── Configuration ─────────────────────────────────────────────────────────────

def setup_inventory_config(profile_dir: str, business_name: str,
                            default_location: str = 'Main',
                            currency: str = 'USD',
                            low_stock_default: int = 10,
                            track_by: str = 'sku',
                            notes: str = '') -> str:
    path = _path(profile_dir, 'inv_config.json')
    config = {
        'business_name': business_name,
        'default_location': default_location,
        'currency': currency,
        'low_stock_default': low_stock_default,
        'track_by': track_by,
        'notes': notes,
        'configured': _now()
    }
    _save(path, config)
    # Auto-create default location
    add_inventory_location(profile_dir, default_location, 'Default storage location')
    return (f"Inventory configured for {business_name}.\n"
            f"  Default location: {default_location}\n"
            f"  Currency: {currency}\n"
            f"  Low stock alert threshold: {low_stock_default} units\n"
            f"  Tracking by: {track_by}")


def _config(profile_dir: str) -> dict:
    return _load(_path(profile_dir, 'inv_config.json'), {
        'business_name': 'My Business',
        'default_location': 'Main',
        'currency': 'USD',
        'low_stock_default': 10
    })


# ── Categories ────────────────────────────────────────────────────────────────

def add_inventory_category(profile_dir: str, name: str,
                            parent_category: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'inv_categories.json')
    cats = _load(path, [])
    cat = {
        'id': _uid(), 'name': name,
        'parent': parent_category, 'notes': notes, 'created': _now()
    }
    cats.append(cat)
    _save(path, cats)
    return f"Category added: {name}" + (f" (under {parent_category})" if parent_category else '')


# ── Locations ─────────────────────────────────────────────────────────────────

def add_inventory_location(profile_dir: str, name: str,
                            description: str = '', address: str = '') -> str:
    path = _path(profile_dir, 'inv_locations.json')
    locations = _load(path, [])
    if any(l['name'].lower() == name.lower() for l in locations):
        return f"Location '{name}' already exists."
    location = {
        'id': _uid(), 'name': name,
        'description': description, 'address': address, 'created': _now()
    }
    locations.append(location)
    _save(path, locations)
    return f"Location added: {name}"


def list_inventory_locations(profile_dir: str) -> str:
    locations = _load(_path(profile_dir, 'inv_locations.json'), [])
    if not locations:
        return "No locations set up. Use setup_inventory_config to get started."
    lines = [f"Inventory locations ({len(locations)}):"]
    for l in locations:
        lines.append(f"  {l['name']}" + (f" — {l['description']}" if l.get('description') else ''))
    return '\n'.join(lines)


# ── Items ─────────────────────────────────────────────────────────────────────

def add_inventory_item(profile_dir: str, name: str, sku: str,
                        category: str = '', unit: str = 'each',
                        cost: float = 0, price: float = 0,
                        reorder_level: int = 0,
                        location: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'inv_items.json')
    items = _load(path, [])
    cfg = _config(profile_dir)
    if any(i['sku'].upper() == sku.upper() for i in items):
        return f"SKU '{sku}' already exists. Use update_stock_level to adjust quantity."
    item = {
        'id': _uid(), 'name': name, 'sku': sku.upper(),
        'category': category, 'unit': unit,
        'cost': cost, 'price': price,
        'reorder_level': reorder_level or cfg.get('low_stock_default', 10),
        'location': location or cfg.get('default_location', 'Main'),
        'notes': notes,
        'stock': {},
        'created': _now(), 'updated': _now()
    }
    # Initialize stock at 0 for this location
    item['stock'][item['location']] = 0
    items.append(item)
    _save(path, items)
    return (f"Item added: {name} [{sku.upper()}]\n"
            f"  Category: {category or 'uncategorized'} | Unit: {unit}\n"
            f"  Cost: ${cost:.2f} | Price: ${price:.2f} | Reorder at: {item['reorder_level']}")


def list_inventory(profile_dir: str, category: str = '',
                    location: str = '', low_stock_only: bool = False,
                    limit: int = 50) -> str:
    items = _load(_path(profile_dir, 'inv_items.json'), [])
    if not items:
        return "No inventory items. Add items with add_inventory_item."
    if category:
        items = [i for i in items if category.lower() in i.get('category', '').lower()]
    if location:
        items = [i for i in items if location.lower() in i.get('location', '').lower()]
    if low_stock_only:
        items = [i for i in items
                 if sum(i.get('stock', {}).values()) <= i.get('reorder_level', 10)]
    items = items[:limit]
    lines = [f"Inventory ({len(items)} items" + (' shown' if len(items) == limit else '') + "):"]
    for i in items:
        total_stock = sum(i.get('stock', {}).values())
        low = ' ⚠ LOW' if total_stock <= i.get('reorder_level', 10) else ''
        lines.append(f"  [{i['sku']}] {i['name']} — {total_stock} {i['unit']}{low}")
    return '\n'.join(lines)


def search_inventory(profile_dir: str, query: str) -> str:
    items = _load(_path(profile_dir, 'inv_items.json'), [])
    q = query.lower()
    matches = [i for i in items
               if q in i['name'].lower() or q in i['sku'].lower()
               or q in i.get('category', '').lower() or q in i.get('notes', '').lower()]
    if not matches:
        return f"No items matching '{query}'."
    lines = [f"Search results for '{query}' ({len(matches)} found):"]
    for i in matches:
        total_stock = sum(i.get('stock', {}).values())
        lines.append(f"  [{i['sku']}] {i['name']} — {total_stock} {i['unit']} — {i.get('category', '')} — ${i.get('price', 0):.2f}")
    return '\n'.join(lines)


def update_stock_level(profile_dir: str, sku: str, quantity_change: int,
                        location: str = '', reason: str = '',
                        reference: str = '') -> str:
    path = _path(profile_dir, 'inv_items.json')
    items = _load(path, [])
    item = next((i for i in items if i['sku'].upper() == sku.upper()), None)
    if not item:
        return f"SKU '{sku}' not found."
    cfg = _config(profile_dir)
    loc = location or item.get('location') or cfg.get('default_location', 'Main')
    current = item.get('stock', {}).get(loc, 0)
    new_qty = max(0, current + quantity_change)
    if 'stock' not in item:
        item['stock'] = {}
    item['stock'][loc] = new_qty
    item['updated'] = _now()
    _save(path, items)
    # Log movement
    _log_movement(profile_dir, sku, item['name'], quantity_change, loc, reason, reference)
    direction = '+' if quantity_change >= 0 else ''
    low_warning = f" ⚠ Below reorder level ({item.get('reorder_level', 10)})" if new_qty <= item.get('reorder_level', 10) else ''
    return (f"Stock updated: {item['name']} [{sku.upper()}] at {loc}\n"
            f"  {current} → {new_qty} ({direction}{quantity_change}){low_warning}")


def bulk_update_stock(profile_dir: str, updates: list) -> str:
    """updates: list of {sku, quantity_change, location, reason}"""
    results = []
    for u in updates:
        r = update_stock_level(
            profile_dir,
            u.get('sku', ''),
            u.get('quantity_change', 0),
            u.get('location', ''),
            u.get('reason', 'bulk update')
        )
        results.append(r.split('\n')[0])
    return f"Bulk update complete ({len(updates)} items):\n" + '\n'.join(results)


def _log_movement(profile_dir: str, sku: str, name: str, qty: int,
                   location: str, reason: str, reference: str) -> None:
    path = _path(profile_dir, 'inv_movement.json')
    log = _load(path, [])
    log.append({
        'id': _uid(), 'sku': sku, 'name': name,
        'quantity': qty, 'location': location,
        'reason': reason, 'reference': reference,
        'timestamp': _now()
    })
    # Keep last 5000 movements
    if len(log) > 5000:
        log = log[-5000:]
    _save(path, log)


# ── Suppliers ─────────────────────────────────────────────────────────────────

def add_inventory_supplier(profile_dir: str, name: str, contact: str = '',
                             phone: str = '', email: str = '',
                             lead_days: int = 0, notes: str = '') -> str:
    path = _path(profile_dir, 'inv_suppliers.json')
    suppliers = _load(path, [])
    supplier = {
        'id': _uid(), 'name': name, 'contact': contact,
        'phone': phone, 'email': email,
        'lead_days': lead_days, 'notes': notes, 'created': _now()
    }
    suppliers.append(supplier)
    _save(path, suppliers)
    return f"Supplier added: {name}" + (f" — {lead_days} day lead time" if lead_days else '')


# ── Purchase Orders ───────────────────────────────────────────────────────────

def create_purchase_order(profile_dir: str, supplier_name: str,
                           items: list, notes: str = '') -> str:
    """items: list of {sku, quantity, unit_cost}"""
    path = _path(profile_dir, 'inv_pos.json')
    pos = _load(path, [])
    po_num = f"PO-{len(pos) + 1:05d}"
    total = sum(i.get('quantity', 0) * i.get('unit_cost', 0) for i in items)
    po = {
        'id': _uid(), 'number': po_num,
        'supplier': supplier_name, 'items': items,
        'total': total, 'notes': notes,
        'status': 'ordered', 'created': _now()
    }
    pos.append(po)
    _save(path, pos)
    lines = [f"Purchase order {po_num} created — {supplier_name}"]
    for i in items:
        lines.append(f"  {i.get('sku','?')} × {i.get('quantity',0)} @ ${i.get('unit_cost',0):.2f}")
    lines.append(f"  Total: ${total:.2f} [id:{po['id']}]")
    return '\n'.join(lines)


def receive_purchase_order(profile_dir: str, po_id: str,
                            received_items: list = None) -> str:
    """received_items: list of {sku, quantity} — if None, receives all"""
    path = _path(profile_dir, 'inv_pos.json')
    pos = _load(path, [])
    po = next((p for p in pos if p['id'] == po_id or p['number'] == po_id), None)
    if not po:
        return f"Purchase order '{po_id}' not found."
    items_to_receive = received_items or [{'sku': i['sku'], 'quantity': i['quantity']} for i in po['items']]
    results = []
    for r in items_to_receive:
        result = update_stock_level(
            profile_dir, r['sku'], r['quantity'],
            reason=f"PO received: {po['number']}", reference=po['number']
        )
        results.append(result.split('\n')[0])
    po['status'] = 'received'
    po['received'] = _now()
    _save(path, pos)
    return f"PO {po['number']} received:\n" + '\n'.join(results)


def list_purchase_orders(profile_dir: str, status: str = '') -> str:
    pos = _load(_path(profile_dir, 'inv_pos.json'), [])
    if status:
        pos = [p for p in pos if p['status'] == status]
    if not pos:
        return "No purchase orders." if not status else f"No {status} purchase orders."
    lines = [f"Purchase orders ({len(pos)}):"]
    for p in sorted(pos, key=lambda x: x['created'], reverse=True):
        lines.append(f"  [{p['number']}] {p['supplier']} — ${p['total']:.2f} — {p['status'].upper()} — {p['created'][:10]}")
    return '\n'.join(lines)


# ── Reports ───────────────────────────────────────────────────────────────────

def get_low_stock_report(profile_dir: str, location: str = '') -> str:
    items = _load(_path(profile_dir, 'inv_items.json'), [])
    low = []
    for i in items:
        stock = i.get('stock', {})
        if location:
            total = stock.get(location, 0)
        else:
            total = sum(stock.values()) if stock else 0
        if total <= i.get('reorder_level', 10):
            low.append((i, total))
    if not low:
        return "All items are above reorder levels. ✓"
    lines = [f"⚠ Low stock report — {len(low)} items need attention:"]
    for i, qty in sorted(low, key=lambda x: x[1]):
        lines.append(f"  [{i['sku']}] {i['name']} — {qty} {i['unit']} (reorder at {i.get('reorder_level', 10)})")
    return '\n'.join(lines)


def get_inventory_value_report(profile_dir: str, location: str = '') -> str:
    items = _load(_path(profile_dir, 'inv_items.json'), [])
    cfg = _config(profile_dir)
    total_cost = 0
    total_retail = 0
    lines_data = []
    for i in items:
        stock = i.get('stock', {})
        qty = stock.get(location, 0) if location else sum(stock.values())
        cost_val = qty * i.get('cost', 0)
        retail_val = qty * i.get('price', 0)
        total_cost += cost_val
        total_retail += retail_val
        if qty > 0:
            lines_data.append((i['name'], i['sku'], qty, i['unit'], cost_val, retail_val))
    lines = [f"Inventory Value Report — {cfg.get('business_name', 'My Business')}" +
             (f" @ {location}" if location else '')]
    lines.append(f"  Total cost value:   ${total_cost:,.2f}")
    lines.append(f"  Total retail value: ${total_retail:,.2f}")
    lines.append(f"  Potential margin:   ${total_retail - total_cost:,.2f}")
    lines.append(f"\nTop items by value:")
    for name, sku, qty, unit, cv, rv in sorted(lines_data, key=lambda x: x[4], reverse=True)[:10]:
        lines.append(f"  [{sku}] {name} — {qty} {unit} — cost ${cv:,.2f} / retail ${rv:,.2f}")
    return '\n'.join(lines)


def get_stock_movement_report(profile_dir: str, sku: str = '',
                               days: int = 30) -> str:
    from datetime import timedelta
    log = _load(_path(profile_dir, 'inv_movement.json'), [])
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    log = [m for m in log if m['timestamp'] >= cutoff]
    if sku:
        log = [m for m in log if m['sku'].upper() == sku.upper()]
    if not log:
        return f"No stock movements in the last {days} days."
    received = sum(m['quantity'] for m in log if m['quantity'] > 0)
    sold = sum(m['quantity'] for m in log if m['quantity'] < 0)
    lines = [f"Stock movement — last {days} days" + (f" [{sku.upper()}]" if sku else '')]
    lines.append(f"  Received: +{received} units across {len([m for m in log if m['quantity'] > 0])} transactions")
    lines.append(f"  Removed:  {sold} units across {len([m for m in log if m['quantity'] < 0])} transactions")
    lines.append(f"\nRecent movements:")
    for m in sorted(log, key=lambda x: x['timestamp'], reverse=True)[:15]:
        direction = '+' if m['quantity'] >= 0 else ''
        lines.append(f"  {m['timestamp'][:10]} [{m['sku']}] {m['name']} {direction}{m['quantity']} @ {m['location']}" +
                     (f" — {m['reason']}" if m.get('reason') else ''))
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    items = _load(_path(profile_dir, 'inv_items.json'), [])
    if not items:
        return ''
    low = [i for i in items if sum(i.get('stock', {}).values()) <= i.get('reorder_level', 10)]
    pos = _load(_path(profile_dir, 'inv_pos.json'), [])
    pending_pos = [p for p in pos if p['status'] == 'ordered']
    parts = [f"INVENTORY PRO ACTIVE: {len(items)} SKUs tracked"]
    if low:
        parts.append(f"  {len(low)} item(s) at or below reorder level")
    if pending_pos:
        parts.append(f"  {len(pending_pos)} purchase order(s) pending receipt")
    parts.append(
        "What I can help you with:\n"
        "  Track: SKUs, stock levels across locations, suppliers, purchase orders,\n"
        "         stock movements, inventory value\n"
        "  Write: inventory audit reports, supplier purchase requests,\n"
        "         low-stock alert summaries, supplier negotiation notes,\n"
        "         inventory policy & procedure documents, receiving discrepancy notes,\n"
        "         stock transfer documentation\n"
        "  Know: inventory valuation methods, reorder point basics, shrinkage/loss concepts,\n"
        "        purchase order workflow"
    )
    return '\n'.join(parts)
