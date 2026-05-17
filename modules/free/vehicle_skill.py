MODULE_MANIFEST = {
    "id": "M025",
    "name": "Vehicle Tracker",
    "category": "Home",
    "description": "Track vehicle maintenance, mileage, gas logs, and service reminders for all household vehicles.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_vehicle", "log_service", "get_vehicle_log", "set_service_reminder"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def add_vehicle(profile_dir, make, model, year, color='', vin=''):
    items = load(profile_dir, 'vehicles')
    vehicle = {'id': new_id(), 'make': make, 'model': model, 'year': year,
               'color': color, 'vin': vin, 'service_log': [], 'notes': [], 'created': now_iso()}
    items.append(vehicle)
    save(profile_dir, 'vehicles', items)
    return f"Added {year} {make} {model}."


def log_service(profile_dir, vehicle_name, service_type, mileage=None, date=None, cost=None, notes='', next_due_mileage=None):
    items = load(profile_dir, 'vehicles')
    # Find existing vehicle or auto-create one
    match = next((v for v in items if vehicle_name.lower() in
                  f"{v.get('year','')} {v.get('make','')} {v.get('model','')} {v.get('name','')}".lower()), None)
    if not match:
        match = {'id': new_id(), 'name': vehicle_name, 'make': vehicle_name,
                 'model': '', 'year': '', 'color': '', 'vin': '',
                 'service_log': [], 'notes': [], 'created': now_iso()}
        items.append(match)
    match['service_log'].append({
        'id': new_id(), 'service_type': service_type,
        'mileage': mileage, 'date': date or today_str(),
        'cost': cost, 'notes': notes,
        'next_due_mileage': next_due_mileage
    })
    save(profile_dir, 'vehicles', items)
    return f"Service logged for {vehicle_name}: {service_type}" + (f" at {mileage} miles" if mileage else "") + "."


def get_vehicles(profile_dir):
    return load(profile_dir, 'vehicles')


def get_service_history(profile_dir, vehicle_name=None):
    items = load(profile_dir, 'vehicles')
    if vehicle_name:
        items = [v for v in items
                 if vehicle_name.lower() in f"{v.get('year','')} {v.get('make','')} {v.get('model','')}".lower()]
    history = []
    for v in items:
        for s in v.get('service_log', []):
            history.append({**s, 'vehicle': f"{v.get('year','')} {v.get('make','')} {v.get('model','')}".strip()})
    return sorted(history, key=lambda x: x.get('date', ''), reverse=True)
