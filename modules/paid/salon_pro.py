"""Salon & Spa Pro — Client management, appointments, services, and visit history."""
MODULE_MANIFEST = {
    "id": "I006",
    "name": "Salon & Spa Pro",
    "category": "Industry",
    "description": "Complete salon and spa management. Track clients, book appointments, manage your service menu, and log visit history with notes — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_salon_client", "list_salon_clients", "add_salon_appointment",
              "list_salon_appointments", "add_service", "log_client_visit",
              "get_client_history"],
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

def add_salon_client(profile_dir: str, name: str, phone: str,
                     email: str = '', birthday: str = '',
                     notes: str = '', allergies: str = '') -> str:
    path = _path(profile_dir, 'salon_clients.json')
    clients = _load(path, [])
    client = {
        'id': _uid(),
        'name': name,
        'phone': phone,
        'email': email,
        'birthday': birthday,
        'notes': notes,
        'allergies': allergies,
        'created_at': _now()
    }
    clients.append(client)
    _save(path, clients)
    return (f"Client added [id:{client['id']}] — {name}, {phone}"
            + (f". Allergies noted." if allergies else "") + ".")


def list_salon_clients(profile_dir: str, search: str = '') -> str:
    path = _path(profile_dir, 'salon_clients.json')
    clients = _load(path, [])
    if search:
        sl = search.lower()
        clients = [c for c in clients if sl in c.get('name', '').lower()
                   or sl in c.get('phone', '').lower()]
    if not clients:
        return "No clients found."
    lines = [f"Clients ({len(clients)}):"]
    for c in clients:
        allergy_flag = ' [ALLERGY]' if c.get('allergies') else ''
        lines.append(f"  [{c['id']}] {c['name']} — {c.get('phone','')}{allergy_flag}")
    return '\n'.join(lines)


def add_salon_appointment(profile_dir: str, client_name: str, date: str,
                          time: str, service: str, staff: str = '',
                          duration_min: int = 60, notes: str = '') -> str:
    path = _path(profile_dir, 'salon_appointments.json')
    appts = _load(path, [])
    appt = {
        'id': _uid(),
        'client_name': client_name,
        'date': date,
        'time': time,
        'service': service,
        'staff': staff,
        'duration_min': duration_min,
        'notes': notes,
        'created_at': _now()
    }
    appts.append(appt)
    _save(path, appts)
    return (f"Appointment booked [id:{appt['id']}] — {client_name}, {service} "
            f"on {date} at {time} ({duration_min} min)"
            + (f" with {staff}" if staff else "") + ".")


def list_salon_appointments(profile_dir: str, date: str = '',
                             staff: str = '') -> str:
    path = _path(profile_dir, 'salon_appointments.json')
    appts = _load(path, [])
    today = _now()[:10]
    if date:
        appts = [a for a in appts if a.get('date', '') == date]
    else:
        appts = [a for a in appts if a.get('date', '') >= today]
    if staff:
        appts = [a for a in appts if a.get('staff', '').lower() == staff.lower()]
    if not appts:
        return "No appointments found."
    appts = sorted(appts, key=lambda a: (a.get('date', ''), a.get('time', '')))
    lines = [f"Appointments ({len(appts)}):"]
    for a in appts:
        staff_str = f" — {a['staff']}" if a.get('staff') else ''
        lines.append(f"  [{a['id']}] {a['date']} {a['time']} — {a['client_name']}: {a['service']}{staff_str}")
    return '\n'.join(lines)


def add_service(profile_dir: str, name: str, category: str, price: float,
                duration_min: int, description: str = '') -> str:
    path = _path(profile_dir, 'salon_services.json')
    services = _load(path, [])
    service = {
        'id': _uid(),
        'name': name,
        'category': category,
        'price': price,
        'duration_min': duration_min,
        'description': description,
        'created_at': _now()
    }
    services.append(service)
    _save(path, services)
    return f"Service added [id:{service['id']}] — {name} ({category}), ${price:.2f}, {duration_min} min."


def log_client_visit(profile_dir: str, client_name: str, date: str,
                     services: list, total: float, staff: str = '',
                     notes: str = '') -> str:
    """services: list of service names or strings"""
    path = _path(profile_dir, 'salon_visits.json')
    visits = _load(path, [])
    visit = {
        'id': _uid(),
        'client_name': client_name,
        'date': date,
        'services': services if isinstance(services, list) else [services],
        'total': total,
        'staff': staff,
        'notes': notes,
        'created_at': _now()
    }
    visits.append(visit)
    _save(path, visits)
    services_str = ', '.join(visit['services']) if visit['services'] else 'services'
    return (f"Visit logged [id:{visit['id']}] — {client_name} on {date}: "
            f"{services_str}, ${total:.2f}" + (f" with {staff}" if staff else "") + ".")


def get_client_history(profile_dir: str, client_name: str) -> str:
    cl = client_name.lower()
    # Client record
    clients = _load(_path(profile_dir, 'salon_clients.json'), [])
    client = next((c for c in clients if c.get('name', '').lower() == cl), None)
    # Visit history
    visits = _load(_path(profile_dir, 'salon_visits.json'), [])
    visits = [v for v in visits if v.get('client_name', '').lower() == cl]
    # Upcoming appointments
    appts = _load(_path(profile_dir, 'salon_appointments.json'), [])
    today = _now()[:10]
    upcoming = [a for a in appts if a.get('client_name', '').lower() == cl
                and a.get('date', '') >= today]

    if not client and not visits:
        return f"No records found for {client_name}."

    lines = [f"Client: {client_name}"]
    if client:
        if client.get('phone'):
            lines.append(f"  Phone: {client['phone']}")
        if client.get('email'):
            lines.append(f"  Email: {client['email']}")
        if client.get('allergies'):
            lines.append(f"  ALLERGIES: {client['allergies']}")
        if client.get('notes'):
            lines.append(f"  Notes: {client['notes']}")
    if upcoming:
        lines.append(f"  Upcoming appointments ({len(upcoming)}):")
        for a in sorted(upcoming, key=lambda x: x.get('date', ''))[:3]:
            lines.append(f"    • {a['date']} {a['time']} — {a['service']}")
    if visits:
        total_spent = sum(v.get('total', 0) for v in visits)
        lines.append(f"  Visit history ({len(visits)} visits, ${total_spent:.2f} total):")
        for v in sorted(visits, key=lambda x: x.get('date', ''), reverse=True)[:5]:
            services_str = ', '.join(v.get('services', []))
            lines.append(f"    • {v['date']} — {services_str}, ${v.get('total',0):.2f}")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    clients = _load(_path(profile_dir, 'salon_clients.json'), [])
    if not clients:
        return ''
    today = _now()[:10]
    appts = _load(_path(profile_dir, 'salon_appointments.json'), [])
    today_appts = [a for a in appts if a.get('date', '') == today]
    parts = [f"SALON PRO ACTIVE: {len(clients)} client(s) on file"]
    if today_appts:
        parts.append(f"  Today: {len(today_appts)} appointment(s)")
    parts.append(
        "What I can help you with:\n"
        "  Track: clients, appointments, service menu, staff schedules, product inventory,\n"
        "         visit history, allergies, birthdays\n"
        "  Write: client consultation notes, appointment reminder messages,\n"
        "         service menu descriptions, promotional offers, client thank-you notes,\n"
        "         allergy waiver notes, staff schedules\n"
        "  Know: service pricing strategies, retail product basics, client retention tips,\n"
        "        revenue reporting"
    )
    return '\n'.join(parts)
