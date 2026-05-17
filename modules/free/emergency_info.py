MODULE_MANIFEST = {
    "id": "M019",
    "name": "Emergency Info",
    "category": "Family",
    "description": "Store and quickly retrieve emergency contacts, medical info, and household emergency plans.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["save_emergency_info", "get_emergency_info"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def add_emergency_contact(profile_dir, name, relation, phone, notes=''):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    data['contacts'].append({
        'id': new_id(),
        'name': name,
        'relation': relation,
        'phone': phone,
        'notes': notes
    })
    save(profile_dir, 'emergency_info', data)
    return f"Emergency contact saved: {name} ({relation}) — {phone}."


def add_medical_info(profile_dir, person, info_type, details):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    data['medical'].append({
        'id': new_id(),
        'person': person,
        'type': info_type,
        'details': details,
        'date': today_str()
    })
    save(profile_dir, 'emergency_info', data)
    return f"Medical info saved for {person}: [{info_type}] {details}."


def add_insurance(profile_dir, insurance_type, provider, policy_number, phone='', notes=''):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    data['insurance'].append({
        'id': new_id(),
        'type': insurance_type,
        'provider': provider,
        'policy_number': policy_number,
        'phone': phone,
        'notes': notes,
        'added': today_str()
    })
    save(profile_dir, 'emergency_info', data)
    return f"{insurance_type.capitalize()} insurance saved — {provider}, policy #{policy_number}."


def get_emergency_contacts(profile_dir):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    return data['contacts']


def get_medical_info(profile_dir, person=''):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    records = data['medical']
    if person:
        records = [r for r in records if r.get('person', '').lower() == person.lower()]
    return records


def get_insurance(profile_dir, insurance_type=''):
    data = load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})
    records = data['insurance']
    if insurance_type:
        records = [r for r in records if r.get('type', '').lower() == insurance_type.lower()]
    return records


def get_all(profile_dir):
    return load(profile_dir, 'emergency_info', {'contacts': [], 'medical': [], 'insurance': []})


def context_summary(profile_dir):
    return ''
