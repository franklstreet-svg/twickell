"""Therapy & Counseling Pro — Client management, session notes, appointments, and treatment plans. HIPAA-conscious: all data stored locally only."""
MODULE_MANIFEST = {
    "id": "I011",
    "name": "Therapy & Counseling Pro",
    "category": "Industry",
    "description": "Practice management for therapists and counselors. Manage clients, session notes, appointments, and treatment plans — stored locally only. HIPAA-conscious design.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_therapy_client", "list_therapy_clients",
              "add_session_note", "get_session_history",
              "add_therapy_appointment", "list_therapy_appointments",
              "add_treatment_plan", "get_treatment_plan"],
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

def add_therapy_client(profile_dir: str, name: str, dob: str = '',
                       phone: str = '', email: str = '', insurance: str = '',
                       presenting_issue: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'therapy_clients.json')
    clients = _load(path, [])
    client = {
        'id': _uid(),
        'name': name,
        'dob': dob,
        'phone': phone,
        'email': email,
        'insurance': insurance,
        'presenting_issue': presenting_issue,
        'notes': notes,
        'created_at': _now()
    }
    clients.append(client)
    _save(path, clients)
    return (f"Client added [id:{client['id']}] — {name}. "
            f"(Data stored locally only.)")


def list_therapy_clients(profile_dir: str, search: str = '') -> str:
    path = _path(profile_dir, 'therapy_clients.json')
    clients = _load(path, [])
    if search:
        sl = search.lower()
        clients = [c for c in clients if sl in c.get('name', '').lower()]
    if not clients:
        return "No clients found."
    lines = [f"Clients ({len(clients)}):"]
    for c in clients:
        lines.append(f"  [{c['id']}] {c['name']}" + (f" — {c['presenting_issue'][:50]}" if c.get('presenting_issue') else ""))
    return '\n'.join(lines)


def add_session_note(profile_dir: str, client_name: str, date: str,
                     duration_min: int, session_type: str, note: str,
                     mood_rating: int = 0, provider: str = '') -> str:
    """session_type: individual/family/group"""
    path = _path(profile_dir, 'therapy_sessions.json')
    sessions = _load(path, [])
    session = {
        'id': _uid(),
        'client_name': client_name,
        'date': date,
        'duration_min': duration_min,
        'session_type': session_type,
        'note': note,
        'mood_rating': mood_rating,
        'provider': provider,
        'created_at': _now()
    }
    sessions.append(session)
    _save(path, sessions)
    return (f"Session note added [id:{session['id']}] — {client_name}, "
            f"{date}, {duration_min} min ({session_type})"
            + (f", mood: {mood_rating}/10" if mood_rating else "")
            + (f" by {provider}" if provider else "")
            + ". (Stored locally only.)")


def get_session_history(profile_dir: str, client_name: str,
                        limit: int = 10) -> str:
    path = _path(profile_dir, 'therapy_sessions.json')
    sessions = _load(path, [])
    cl = client_name.lower()
    sessions = [s for s in sessions if s.get('client_name', '').lower() == cl]
    if not sessions:
        return f"No session history found for {client_name}."
    sessions = sorted(sessions, key=lambda s: s.get('date', ''), reverse=True)[:limit]
    lines = [f"Session history: {client_name} (showing {len(sessions)} most recent)"]
    for s in sessions:
        mood_str = f" | mood: {s['mood_rating']}/10" if s.get('mood_rating') else ''
        lines.append(f"  [{s['id']}] {s['date']} — {s['session_type']}, {s['duration_min']}min{mood_str}")
        lines.append(f"    {s['note'][:150]}{'...' if len(s.get('note','')) > 150 else ''}")
    return '\n'.join(lines)


def add_therapy_appointment(profile_dir: str, client_name: str, date: str,
                             time: str, type: str, provider: str = '',
                             notes: str = '') -> str:
    path = _path(profile_dir, 'therapy_appointments.json')
    appts = _load(path, [])
    appt = {
        'id': _uid(),
        'client_name': client_name,
        'date': date,
        'time': time,
        'type': type,
        'provider': provider,
        'notes': notes,
        'created_at': _now()
    }
    appts.append(appt)
    _save(path, appts)
    return (f"Appointment scheduled [id:{appt['id']}] — {client_name}, "
            f"{type} on {date} at {time}"
            + (f" with {provider}" if provider else "") + ".")


def list_therapy_appointments(profile_dir: str, date: str = '',
                               upcoming_only: bool = True) -> str:
    path = _path(profile_dir, 'therapy_appointments.json')
    appts = _load(path, [])
    today = _now()[:10]
    if date:
        appts = [a for a in appts if a.get('date', '') == date]
    elif upcoming_only:
        appts = [a for a in appts if a.get('date', '') >= today]
    if not appts:
        return "No appointments found."
    appts = sorted(appts, key=lambda a: (a.get('date', ''), a.get('time', '')))
    lines = [f"Appointments ({len(appts)}):"]
    for a in appts:
        provider_str = f" with {a['provider']}" if a.get('provider') else ''
        lines.append(f"  [{a['id']}] {a['date']} {a['time']} — {a['client_name']} ({a.get('type','')}){provider_str}")
    return '\n'.join(lines)


def add_treatment_plan(profile_dir: str, client_name: str, goals: str,
                       interventions: str, review_date: str,
                       provider: str = '') -> str:
    path = _path(profile_dir, 'therapy_plans.json')
    plans = _load(path, [])
    plan = {
        'id': _uid(),
        'client_name': client_name,
        'goals': goals,
        'interventions': interventions,
        'review_date': review_date,
        'provider': provider,
        'created_at': _now(),
        'updated_at': _now()
    }
    # Replace existing plan for this client
    plans = [p for p in plans if p.get('client_name', '').lower() != client_name.lower()]
    plans.append(plan)
    _save(path, plans)
    return (f"Treatment plan saved [id:{plan['id']}] — {client_name}. "
            f"Review date: {review_date}. (Stored locally only.)")


def get_treatment_plan(profile_dir: str, client_name: str) -> str:
    path = _path(profile_dir, 'therapy_plans.json')
    plans = _load(path, [])
    cl = client_name.lower()
    plan = next((p for p in plans if p.get('client_name', '').lower() == cl), None)
    if not plan:
        return f"No treatment plan found for {client_name}."
    lines = [
        f"Treatment Plan: {client_name}",
        f"  Created: {plan.get('created_at','')[:10]}",
        f"  Review date: {plan.get('review_date','')}",
    ]
    if plan.get('provider'):
        lines.append(f"  Provider: {plan['provider']}")
    lines.append(f"  Goals:\n    {plan.get('goals','')}")
    lines.append(f"  Interventions:\n    {plan.get('interventions','')}")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    clients = _load(_path(profile_dir, 'therapy_clients.json'), [])
    if not clients:
        return ''
    today = _now()[:10]
    appts = _load(_path(profile_dir, 'therapy_appointments.json'), [])
    today_appts = [a for a in appts if a.get('date', '') == today]
    upcoming = [a for a in appts if a.get('date', '') > today]
    parts = [
        f"THERAPY PRO ACTIVE: {len(clients)} client(s) on file",
        "  NOTE: All data stored locally only — HIPAA-conscious design."
    ]
    if today_appts:
        parts.append(f"  Today: {len(today_appts)} session(s)")
    elif upcoming:
        next_a = sorted(upcoming, key=lambda a: (a.get('date', ''), a.get('time', '')))[0]
        parts.append(f"  Next session: {next_a['client_name']} on {next_a['date']} at {next_a.get('time','')}")
    parts.append(
        "What I can help you with:\n"
        "  Track: clients, sessions, appointments, treatment plans, mood ratings,\n"
        "         progress notes, session history\n"
        "  Write: session notes (SOAP and DAP format), treatment plan documents,\n"
        "         progress notes, client intake summaries, discharge summaries,\n"
        "         referral letters, insurance billing notes\n"
        "  Know: SOAP/DAP note format, general therapeutic approaches,\n"
        "        insurance billing note conventions, session documentation best practices"
    )
    return '\n'.join(parts)
