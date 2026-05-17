"""Property Management Pro — Tenants, leases, rent tracking, and maintenance requests."""
MODULE_MANIFEST = {
    "id": "I012",
    "name": "Property Management Pro",
    "category": "Industry",
    "description": "Manage rental properties, tenants, leases, rent payments, and maintenance requests — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_property", "list_properties", "add_tenant", "list_tenants",
              "add_lease", "get_lease", "log_rent_payment", "get_rent_status",
              "add_maintenance_request", "list_maintenance_requests", "update_maintenance_request"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime, date
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


# ── Properties ────────────────────────────────────────────────────────────────

def add_property(profile_dir: str, address: str, type: str, bedrooms: int = 0,
                 bathrooms: float = 0, rent: float = 0, notes: str = '') -> str:
    path = _path(profile_dir, 'pm_properties.json')
    props = _load(path, [])
    prop = {
        'id': _uid(), 'address': address, 'type': type,
        'bedrooms': bedrooms, 'bathrooms': bathrooms,
        'rent': rent, 'status': 'vacant', 'notes': notes, 'created': _now()
    }
    props.append(prop)
    _save(path, props)
    return f"Property added: {address} ({type}) — ${rent}/mo [id:{prop['id']}]"


def list_properties(profile_dir: str, status: str = '') -> str:
    props = _load(_path(profile_dir, 'pm_properties.json'), [])
    if status:
        props = [p for p in props if p.get('status', '') == status]
    if not props:
        return "No properties on file." if not status else f"No {status} properties."
    lines = [f"Properties ({len(props)}):"]
    for p in props:
        rent = f"${p['rent']}/mo" if p.get('rent') else 'rent not set'
        lines.append(f"  [{p['id']}] {p['address']} — {p.get('status','unknown')} — {rent}")
    return '\n'.join(lines)


# ── Tenants ───────────────────────────────────────────────────────────────────

def add_tenant(profile_dir: str, name: str, phone: str, email: str = '',
               id_type: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'pm_tenants.json')
    tenants = _load(path, [])
    tenant = {
        'id': _uid(), 'name': name, 'phone': phone, 'email': email,
        'id_type': id_type, 'notes': notes, 'created': _now()
    }
    tenants.append(tenant)
    _save(path, tenants)
    return f"Tenant added: {name} [id:{tenant['id']}]"


def list_tenants(profile_dir: str, search: str = '') -> str:
    tenants = _load(_path(profile_dir, 'pm_tenants.json'), [])
    if search:
        tenants = [t for t in tenants if search.lower() in t['name'].lower()]
    if not tenants:
        return "No tenants on file." if not search else f"No tenants matching '{search}'."
    lines = [f"Tenants ({len(tenants)}):"]
    for t in tenants:
        lines.append(f"  [{t['id']}] {t['name']} — {t['phone']}")
    return '\n'.join(lines)


# ── Leases ────────────────────────────────────────────────────────────────────

def add_lease(profile_dir: str, property_id: str, tenant_name: str,
              start_date: str, end_date: str, monthly_rent: float,
              deposit: float = 0, notes: str = '') -> str:
    path = _path(profile_dir, 'pm_leases.json')
    leases = _load(path, [])
    lease = {
        'id': _uid(), 'property_id': property_id, 'tenant_name': tenant_name,
        'start_date': start_date, 'end_date': end_date,
        'monthly_rent': monthly_rent, 'deposit': deposit,
        'status': 'active', 'notes': notes, 'created': _now()
    }
    leases.append(lease)
    _save(path, leases)
    # Mark property as occupied
    props = _load(_path(profile_dir, 'pm_properties.json'), [])
    for p in props:
        if p['id'] == property_id:
            p['status'] = 'occupied'
            p['current_tenant'] = tenant_name
    _save(_path(profile_dir, 'pm_properties.json'), props)
    return (f"Lease created: {tenant_name} at property {property_id}\n"
            f"  {start_date} → {end_date} | ${monthly_rent}/mo | deposit ${deposit} [id:{lease['id']}]")


def get_lease(profile_dir: str, tenant_name: str = '', property_id: str = '') -> str:
    leases = _load(_path(profile_dir, 'pm_leases.json'), [])
    matches = leases
    if tenant_name:
        matches = [l for l in matches if tenant_name.lower() in l['tenant_name'].lower()]
    if property_id:
        matches = [l for l in matches if l['property_id'] == property_id]
    matches = [l for l in matches if l.get('status') == 'active']
    if not matches:
        return "No active lease found."
    lease = matches[0]
    lines = [
        f"Lease [{lease['id']}]: {lease['tenant_name']}",
        f"  Property: {lease['property_id']}",
        f"  Term: {lease['start_date']} → {lease['end_date']}",
        f"  Rent: ${lease['monthly_rent']}/mo | Deposit: ${lease['deposit']}",
        f"  Status: {lease['status']}"
    ]
    if lease.get('notes'):
        lines.append(f"  Notes: {lease['notes']}")
    return '\n'.join(lines)


# ── Rent tracking ─────────────────────────────────────────────────────────────

def log_rent_payment(profile_dir: str, tenant_name: str, amount: float,
                     payment_date: str, method: str = 'check', notes: str = '') -> str:
    path = _path(profile_dir, 'pm_payments.json')
    payments = _load(path, [])
    payment = {
        'id': _uid(), 'tenant_name': tenant_name, 'amount': amount,
        'payment_date': payment_date, 'method': method,
        'notes': notes, 'recorded': _now()
    }
    payments.append(payment)
    _save(path, payments)
    return f"Payment logged: {tenant_name} paid ${amount:.2f} on {payment_date} via {method}."


def get_rent_status(profile_dir: str, tenant_name: str = '') -> str:
    leases = _load(_path(profile_dir, 'pm_leases.json'), [])
    payments = _load(_path(profile_dir, 'pm_payments.json'), [])
    active = [l for l in leases if l.get('status') == 'active']
    if tenant_name:
        active = [l for l in active if tenant_name.lower() in l['tenant_name'].lower()]
    if not active:
        return "No active leases found."
    today = date.today().isoformat()
    lines = ["Rent status:"]
    for lease in active:
        tn = lease['tenant_name']
        rent = lease['monthly_rent']
        month = today[:7]
        paid_this_month = sum(
            p['amount'] for p in payments
            if p['tenant_name'] == tn and p['payment_date'].startswith(month)
        )
        status = 'PAID' if paid_this_month >= rent else f'OUTSTANDING — ${rent - paid_this_month:.2f} due'
        lines.append(f"  {tn}: ${rent}/mo — {status}")
    return '\n'.join(lines)


# ── Maintenance ───────────────────────────────────────────────────────────────

def add_maintenance_request(profile_dir: str, property_id: str, tenant_name: str,
                             description: str, priority: str = 'normal',
                             category: str = '') -> str:
    path = _path(profile_dir, 'pm_maintenance.json')
    requests = _load(path, [])
    req = {
        'id': _uid(), 'property_id': property_id, 'tenant_name': tenant_name,
        'description': description, 'priority': priority, 'category': category,
        'status': 'open', 'created': _now(), 'updated': _now()
    }
    requests.append(req)
    _save(path, requests)
    return f"Maintenance request logged [{req['id']}]: {description[:60]} — priority: {priority}"


def list_maintenance_requests(profile_dir: str, status: str = 'open',
                               property_id: str = '') -> str:
    requests = _load(_path(profile_dir, 'pm_maintenance.json'), [])
    if status:
        requests = [r for r in requests if r.get('status') == status]
    if property_id:
        requests = [r for r in requests if r.get('property_id') == property_id]
    if not requests:
        return f"No {status} maintenance requests." if status else "No maintenance requests."
    lines = [f"Maintenance requests ({len(requests)}):"]
    for r in sorted(requests, key=lambda x: x['created'], reverse=True):
        lines.append(f"  [{r['id']}] {r['property_id']} — {r['priority'].upper()} — {r['description'][:60]}")
        lines.append(f"        Status: {r['status']} | Tenant: {r['tenant_name']}")
    return '\n'.join(lines)


def update_maintenance_request(profile_dir: str, request_id: str,
                                status: str = '', notes: str = '',
                                vendor: str = '', cost: float = 0) -> str:
    path = _path(profile_dir, 'pm_maintenance.json')
    requests = _load(path, [])
    req = next((r for r in requests if r['id'] == request_id), None)
    if not req:
        return f"Maintenance request '{request_id}' not found."
    if status:
        req['status'] = status
    if notes:
        req['notes'] = notes
    if vendor:
        req['vendor'] = vendor
    if cost:
        req['cost'] = cost
    req['updated'] = _now()
    _save(path, requests)
    return f"Request [{request_id}] updated — status: {req['status']}"


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    props = _load(_path(profile_dir, 'pm_properties.json'), [])
    if not props:
        return ''
    occupied = [p for p in props if p.get('status') == 'occupied']
    vacant = [p for p in props if p.get('status') == 'vacant']
    maintenance = _load(_path(profile_dir, 'pm_maintenance.json'), [])
    open_requests = [r for r in maintenance if r.get('status') == 'open']
    urgent = [r for r in open_requests if r.get('priority') == 'urgent']
    parts = [f"PROPERTY MANAGEMENT ACTIVE: {len(props)} properties — {len(occupied)} occupied, {len(vacant)} vacant"]
    if urgent:
        parts.append(f"  {len(urgent)} URGENT maintenance request(s) open")
    elif open_requests:
        parts.append(f"  {len(open_requests)} maintenance request(s) open")
    parts.append(
        "What I can help you with:\n"
        "  Track: properties, tenants, leases, rent payments, maintenance requests,\n"
        "         vacancy status, vendor contacts\n"
        "  Write: lease agreements (draft), tenant welcome letters,\n"
        "         maintenance request acknowledgments, vendor work orders,\n"
        "         late payment notices, eviction warning letters, lease renewal offers,\n"
        "         move-in/move-out inspection notes, tenant rules & regulations letters\n"
        "  Know: general landlord-tenant practices, lease terms, rent roll reporting"
    )
    return '\n'.join(parts)
