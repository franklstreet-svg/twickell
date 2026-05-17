"""Legal Pro — Case management, billing, deadlines, and time tracking for law practices."""
MODULE_MANIFEST = {
    "id": "I001",
    "name": "Legal Pro",
    "category": "Industry",
    "description": "Full case management for law practices. Track cases, deadlines, billable hours, notes, and generate billing summaries — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_legal_case", "list_legal_cases", "update_legal_case",
              "add_case_deadline", "log_billable_time", "get_billing_summary",
              "add_case_note", "get_legal_case"],
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

def add_legal_case(profile_dir: str, client_name: str, case_type: str,
                   description: str, status: str = 'open') -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = {
        'id': _uid(),
        'client_name': client_name,
        'case_type': case_type,
        'description': description,
        'status': status,
        'created_at': _now(),
        'deadlines': [],
        'time_entries': [],
        'notes': []
    }
    cases.append(case)
    _save(path, cases)
    return f"Case added [id:{case['id']}] — {client_name} / {case_type}. Status: {status}."


def list_legal_cases(profile_dir: str, status: str = '') -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    if status:
        cases = [c for c in cases if c.get('status', '').lower() == status.lower()]
    if not cases:
        return f"No cases found{' with status ' + status if status else ''}."
    lines = [f"Cases ({len(cases)}):"]
    for c in cases:
        lines.append(f"  [{c['id']}] {c['client_name']} — {c['case_type']} ({c.get('status','open')})")
    return '\n'.join(lines)


def update_legal_case(profile_dir: str, case_id: str, status: str = '',
                      notes: str = '', description: str = '') -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case '{case_id}' not found."
    if status:
        case['status'] = status
    if description:
        case['description'] = description
    if notes:
        case['notes'].append({'text': notes, 'date': _now()})
    case['updated_at'] = _now()
    _save(path, cases)
    return f"Case {case_id} updated — {case['client_name']} / {case['case_type']}."


def add_case_deadline(profile_dir: str, case_id: str,
                      deadline_date: str, description: str) -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case '{case_id}' not found."
    deadline = {'id': _uid(), 'date': deadline_date, 'description': description, 'added_at': _now()}
    case.setdefault('deadlines', []).append(deadline)
    _save(path, cases)
    return f"Deadline added to case {case_id}: {description} due {deadline_date}."


def log_billable_time(profile_dir: str, case_id: str, hours: float,
                      description: str, date: str = '') -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case '{case_id}' not found."
    entry = {
        'id': _uid(),
        'hours': hours,
        'description': description,
        'date': date or _now()[:10],
        'logged_at': _now()
    }
    case.setdefault('time_entries', []).append(entry)
    _save(path, cases)
    return f"Logged {hours}h on case {case_id} ({case['client_name']}): {description}."


def get_billing_summary(profile_dir: str, case_id: str = '') -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    # Default hourly rate — can be overridden per practice
    HOURLY_RATE = 250.0

    if case_id:
        cases = [c for c in cases if c['id'] == case_id]
        if not cases:
            return f"Case '{case_id}' not found."

    lines = ["Billing Summary:"]
    grand_hours = 0.0
    for c in cases:
        entries = c.get('time_entries', [])
        total_hours = sum(e.get('hours', 0) for e in entries)
        amount = total_hours * HOURLY_RATE
        grand_hours += total_hours
        lines.append(f"  [{c['id']}] {c['client_name']} / {c['case_type']}: "
                     f"{total_hours:.1f}h = ${amount:,.2f}")
    if not case_id and len(cases) > 1:
        lines.append(f"  TOTAL: {grand_hours:.1f}h = ${grand_hours * HOURLY_RATE:,.2f}")
    lines.append(f"  (Rate: ${HOURLY_RATE:.0f}/hr)")
    return '\n'.join(lines)


def add_case_note(profile_dir: str, case_id: str, note: str) -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case '{case_id}' not found."
    case.setdefault('notes', []).append({'text': note, 'date': _now()})
    _save(path, cases)
    return f"Note added to case {case_id} ({case['client_name']})."


def get_legal_case(profile_dir: str, case_id: str) -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case '{case_id}' not found."
    lines = [
        f"Case: {case['client_name']} — {case['case_type']}",
        f"  ID: {case['id']}  Status: {case.get('status','open')}",
        f"  Created: {case.get('created_at','')[:10]}",
        f"  Description: {case.get('description','')}",
    ]
    deadlines = case.get('deadlines', [])
    if deadlines:
        lines.append(f"  Deadlines ({len(deadlines)}):")
        for d in deadlines:
            lines.append(f"    • {d['date']} — {d['description']}")
    time_entries = case.get('time_entries', [])
    if time_entries:
        total_h = sum(e.get('hours', 0) for e in time_entries)
        lines.append(f"  Time Entries ({len(time_entries)}, {total_h:.1f}h total):")
        for e in time_entries[-5:]:
            lines.append(f"    • {e['date']} — {e['hours']}h: {e['description']}")
    notes = case.get('notes', [])
    if notes:
        lines.append(f"  Notes ({len(notes)}):")
        for n in notes[-5:]:
            lines.append(f"    • {n['date'][:10]}: {n['text']}")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    path = _path(profile_dir, 'legal_cases.json')
    cases = _load(path, [])
    if not cases:
        return ''
    open_cases = [c for c in cases if c.get('status') == 'open']
    today = _now()[:10]
    overdue = []
    upcoming = []
    for c in cases:
        for d in c.get('deadlines', []):
            if d['date'] < today:
                overdue.append(f"{c['client_name']}: {d['description']} (was {d['date']})")
            elif d['date'] <= today[:8] + '31':
                upcoming.append(f"{c['client_name']}: {d['description']} due {d['date']}")
    parts = [f"LEGAL PRO ACTIVE: {len(open_cases)} open case(s) of {len(cases)} total"]
    if overdue:
        parts.append(f"  OVERDUE DEADLINES: {'; '.join(overdue[:3])}")
    if upcoming:
        parts.append(f"  Upcoming deadlines: {'; '.join(upcoming[:3])}")
    parts.append(
        "What I can help you with:\n"
        "  Track: cases, clients, billable hours, court deadlines, case notes, conflict checks\n"
        "  Write: legal briefs, motions, pleadings, demand letters, settlement agreements,\n"
        "         client intake letters, retainer agreements, discovery requests,\n"
        "         deposition prep questions, client update letters, case summaries,\n"
        "         billing invoices from logged hours\n"
        "  Know: statute of limitations by case type, general legal concepts,\n"
        "        court procedures, filing requirements"
    )
    return '\n'.join(parts)
