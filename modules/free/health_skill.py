MODULE_MANIFEST = {
    "id": "M013",
    "name": "Health Log",
    "category": "Wellness",
    "description": "Track health metrics, medications, symptoms, appointments, and medical history.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["log_health_metric", "get_health_log", "add_medication"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def log_health(profile_dir, weight=None, sleep_hours=None, water_glasses=None,
               symptoms=None, medications=None, notes=''):
    items = load(profile_dir, 'health')
    entry = {'id': new_id(), 'date': today_str(), 'weight': weight,
             'sleep_hours': sleep_hours, 'water_glasses': water_glasses,
             'symptoms': symptoms or [], 'medications': medications or [],
             'notes': notes, 'created': now_iso()}
    items.append(entry)
    save(profile_dir, 'health', items)
    parts = []
    if weight:
        parts.append(f"weight {weight}lbs")
    if sleep_hours:
        parts.append(f"sleep {sleep_hours}hrs")
    if water_glasses:
        parts.append(f"water {water_glasses} glasses")
    return "Health logged: " + (", ".join(parts) if parts else "entry saved") + "."


def add_medication_reminder(profile_dir, med_name, time_of_day, dose=''):
    data = load(profile_dir, 'medications', default=[])
    med = {'id': new_id(), 'name': med_name, 'time': time_of_day,
           'dose': dose, 'active': True, 'created': now_iso()}
    data.append(med)
    save(profile_dir, 'medications', data)
    return f"Medication reminder set: {med_name} at {time_of_day}."


def get_meds(profile_dir):
    return [m for m in load(profile_dir, 'medications', default=[]) if m.get('active')]


def get_recent(profile_dir, days=7):
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    return [e for e in load(profile_dir, 'health') if e.get('date', '') >= cutoff]


def context_summary(profile_dir) -> str:
    meds = get_meds(profile_dir)
    if meds:
        return "MEDICATIONS: " + ", ".join(f"{m['name']} at {m['time']}" for m in meds)
    return ''
