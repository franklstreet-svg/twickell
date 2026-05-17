MODULE_MANIFEST = {
    "id": "M026",
    "name": "Pet Care",
    "category": "Home",
    "description": "Manage pet profiles, vet appointments, medications, feeding schedules, and health records.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_pet", "log_vet_visit", "get_pet_info", "log_medication"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def add_pet(profile_dir, name, species, breed='', birthdate=''):
    items = load(profile_dir, 'pets')
    pet = {'id': new_id(), 'name': name, 'species': species,
           'breed': breed, 'birthdate': birthdate,
           'vet_visits': [], 'medications': [], 'notes': [], 'created': now_iso()}
    items.append(pet)
    save(profile_dir, 'pets', items)
    return f"Added {name} ({species}) to your pets."


def _get_or_create_pet(items, pet_name):
    for pet in items:
        if pet['name'].lower() == pet_name.lower():
            return pet
    pet = {'id': new_id(), 'name': pet_name, 'species': 'unknown',
           'breed': '', 'birthdate': '', 'vet_visits': [],
           'medications': [], 'notes': [], 'created': now_iso()}
    items.append(pet)
    return pet


def log_vet_visit(profile_dir, pet_name, date, reason='', vet='', notes=''):
    items = load(profile_dir, 'pets')
    pet = _get_or_create_pet(items, pet_name)
    pet['vet_visits'].append({'id': new_id(), 'date': date,
                              'reason': reason, 'vet': vet, 'notes': notes})
    save(profile_dir, 'pets', items)
    return f"Vet visit logged for {pet_name}."


def add_pet_medication(profile_dir, pet_name, med_name, schedule, dose=''):
    items = load(profile_dir, 'pets')
    pet = _get_or_create_pet(items, pet_name)
    pet['medications'].append({'id': new_id(), 'name': med_name,
                               'schedule': schedule, 'dose': dose, 'active': True})
    save(profile_dir, 'pets', items)
    return f"{med_name} added for {pet_name}."


def add_pet_note(profile_dir, pet_name, note):
    items = load(profile_dir, 'pets')
    pet = _get_or_create_pet(items, pet_name)
    pet['notes'].append({'text': note, 'date': today_str()})
    save(profile_dir, 'pets', items)
    return f"Note added for {pet_name}."


def get_pets(profile_dir):
    return load(profile_dir, 'pets')
