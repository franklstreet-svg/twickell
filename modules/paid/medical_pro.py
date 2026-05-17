"""Medical Pro — Practice management for medical offices. HIPAA-conscious: no actual medical records transmitted, notes stored locally only."""
MODULE_MANIFEST = {
    "id": "I002",
    "name": "Medical Pro",
    "category": "Industry",
    "description": "Practice management for medical offices. Manage patients, appointments, treatment notes, and prescriptions — all stored locally. HIPAA-conscious design.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_patient", "list_patients", "add_medical_appointment",
              "list_medical_appointments", "add_treatment_note", "get_patient_history",
              "add_prescription", "list_prescriptions"],
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

def add_patient(profile_dir: str, name: str, dob: str, phone: str,
                email: str = '', insurance: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'medical_patients.json')
    patients = _load(path, [])
    patient = {
        'id': _uid(),
        'name': name,
        'dob': dob,
        'phone': phone,
        'email': email,
        'insurance': insurance,
        'notes': notes,
        'created_at': _now()
    }
    patients.append(patient)
    _save(path, patients)
    return f"Patient added [id:{patient['id']}] — {name}, DOB {dob}."


def list_patients(profile_dir: str, search: str = '') -> str:
    path = _path(profile_dir, 'medical_patients.json')
    patients = _load(path, [])
    if search:
        sl = search.lower()
        patients = [p for p in patients if sl in p.get('name', '').lower()]
    if not patients:
        return "No patients found."
    lines = [f"Patients ({len(patients)}):"]
    for p in patients:
        lines.append(f"  [{p['id']}] {p['name']} — DOB: {p.get('dob','')}  Phone: {p.get('phone','')}")
    return '\n'.join(lines)


def add_medical_appointment(profile_dir: str, patient_name: str, date: str,
                             time: str, type: str, notes: str = '') -> str:
    path = _path(profile_dir, 'medical_appointments.json')
    appts = _load(path, [])
    appt = {
        'id': _uid(),
        'patient_name': patient_name,
        'date': date,
        'time': time,
        'type': type,
        'notes': notes,
        'created_at': _now()
    }
    appts.append(appt)
    _save(path, appts)
    return f"Appointment scheduled [id:{appt['id']}] — {patient_name} on {date} at {time} ({type})."


def list_medical_appointments(profile_dir: str, date: str = '',
                               upcoming_only: bool = True) -> str:
    path = _path(profile_dir, 'medical_appointments.json')
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
        lines.append(f"  [{a['id']}] {a['date']} {a['time']} — {a['patient_name']} ({a.get('type','')})")
    return '\n'.join(lines)


def add_treatment_note(profile_dir: str, patient_name: str, date: str,
                       note: str, provider: str = '') -> str:
    path = _path(profile_dir, 'medical_notes.json')
    notes_data = _load(path, [])
    record = {
        'id': _uid(),
        'patient_name': patient_name,
        'date': date,
        'note': note,
        'provider': provider,
        'created_at': _now()
    }
    notes_data.append(record)
    _save(path, notes_data)
    return (f"Treatment note added for {patient_name} on {date}"
            + (f" by {provider}" if provider else "") + ". (Stored locally only.)")


def get_patient_history(profile_dir: str, patient_name: str) -> str:
    nl = patient_name.lower()
    # Appointments
    appts = _load(_path(profile_dir, 'medical_appointments.json'), [])
    appts = [a for a in appts if a.get('patient_name', '').lower() == nl]
    # Notes
    notes_data = _load(_path(profile_dir, 'medical_notes.json'), [])
    notes_data = [n for n in notes_data if n.get('patient_name', '').lower() == nl]
    # Prescriptions
    rxs = _load(_path(profile_dir, 'medical_prescriptions.json'), [])
    rxs = [r for r in rxs if r.get('patient_name', '').lower() == nl]

    lines = [f"Patient History: {patient_name}"]
    if appts:
        lines.append(f"  Appointments ({len(appts)}):")
        for a in sorted(appts, key=lambda x: x.get('date', ''), reverse=True)[:10]:
            lines.append(f"    • {a['date']} {a.get('time','')} — {a.get('type','')} [{a['id']}]")
    if notes_data:
        lines.append(f"  Treatment Notes ({len(notes_data)}):")
        for n in sorted(notes_data, key=lambda x: x.get('date', ''), reverse=True)[:5]:
            lines.append(f"    • {n['date']}: {n['note'][:100]}")
    if rxs:
        lines.append(f"  Prescriptions ({len(rxs)}):")
        for r in rxs:
            lines.append(f"    • {r.get('medication','')} {r.get('dosage','')} (by {r.get('prescriber','')}, {r.get('date','')})")
    if not appts and not notes_data and not rxs:
        return f"No history found for {patient_name}."
    return '\n'.join(lines)


def add_prescription(profile_dir: str, patient_name: str, medication: str,
                     dosage: str, prescriber: str, date: str = '') -> str:
    path = _path(profile_dir, 'medical_prescriptions.json')
    rxs = _load(path, [])
    rx = {
        'id': _uid(),
        'patient_name': patient_name,
        'medication': medication,
        'dosage': dosage,
        'prescriber': prescriber,
        'date': date or _now()[:10],
        'created_at': _now()
    }
    rxs.append(rx)
    _save(path, rxs)
    return f"Prescription logged [id:{rx['id']}] — {medication} {dosage} for {patient_name} by {prescriber}."


def list_prescriptions(profile_dir: str, patient_name: str) -> str:
    path = _path(profile_dir, 'medical_prescriptions.json')
    rxs = _load(path, [])
    nl = patient_name.lower()
    rxs = [r for r in rxs if r.get('patient_name', '').lower() == nl]
    if not rxs:
        return f"No prescriptions found for {patient_name}."
    lines = [f"Prescriptions for {patient_name} ({len(rxs)}):"]
    for r in rxs:
        lines.append(f"  [{r['id']}] {r.get('date','')} — {r['medication']} {r['dosage']} (Rx: {r['prescriber']})")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    patients = _load(_path(profile_dir, 'medical_patients.json'), [])
    if not patients:
        return ''
    appts = _load(_path(profile_dir, 'medical_appointments.json'), [])
    today = _now()[:10]
    today_appts = [a for a in appts if a.get('date', '') == today]
    upcoming = [a for a in appts if a.get('date', '') > today]
    parts = [f"MEDICAL PRO ACTIVE: {len(patients)} patient(s) on file"]
    if today_appts:
        parts.append(f"  Today: {len(today_appts)} appointment(s)")
    elif upcoming:
        next_a = sorted(upcoming, key=lambda a: (a.get('date',''), a.get('time','')))[0]
        parts.append(f"  Next appointment: {next_a['patient_name']} on {next_a['date']} at {next_a.get('time','')}")
    parts.append(
        "What I can help you with:\n"
        "  Track: patients, appointments, prescriptions, referrals, immunization schedules,\n"
        "         insurance notes, follow-up alerts\n"
        "  Write: patient SOAP notes, referral letters, prior authorization letters,\n"
        "         patient discharge summaries, prescription notes, patient education materials,\n"
        "         appointment reminder letters, insurance claim notes, billing summaries\n"
        "  Know: general clinical procedures, medication classes, immunization schedules,\n"
        "        SOAP note format, medical terminology"
    )
    return '\n'.join(parts)
