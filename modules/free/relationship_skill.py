MODULE_MANIFEST = {
    "id": "M020",
    "name": "People & Relationships",
    "category": "Relationships",
    "description": "Track birthdays, anniversaries, and important details about the people in your life.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_person", "get_person", "get_upcoming_birthdays", "log_interaction"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime, timedelta
from engine.storage import load, save, now_iso, today_str, new_id


def add_person(profile_dir, name, relation='', birthday='', anniversary='', phone='', email='', notes=''):
    items = load(profile_dir, 'relationships')
    person = {'id': new_id(), 'name': name, 'relation': relation,
              'birthday': birthday, 'anniversary': anniversary,
              'phone': phone, 'email': email, 'notes': notes,
              'last_contact': '', 'created': now_iso()}
    items.append(person)
    save(profile_dir, 'relationships', items)
    return f"Added {name} to your people."


def update_person(profile_dir, name, **kwargs):
    items = load(profile_dir, 'relationships')
    for person in items:
        if person['name'].lower() == name.lower():
            person.update({k: v for k, v in kwargs.items() if v is not None})
    save(profile_dir, 'relationships', items)
    return f"Updated {name}."


def log_contact(profile_dir, name, notes=''):
    items = load(profile_dir, 'relationships')
    for person in items:
        if person['name'].lower() == name.lower():
            person['last_contact'] = today_str()
            if notes:
                person.setdefault('contact_log', []).append({'date': today_str(), 'notes': notes})
    save(profile_dir, 'relationships', items)
    return f"Contact with {name} logged."


def get_upcoming_birthdays(profile_dir, days=30):
    today = datetime.now()
    upcoming = []
    for person in load(profile_dir, 'relationships'):
        bday = person.get('birthday', '')
        if not bday:
            continue
        try:
            parts = bday.split('-')
            month, day = int(parts[-2]), int(parts[-1])
            bday_this_year = today.replace(month=month, day=day, hour=0, minute=0, second=0, microsecond=0)
            if bday_this_year < today:
                bday_this_year = bday_this_year.replace(year=today.year + 1)
            delta = (bday_this_year - today).days
            if 0 <= delta <= days:
                upcoming.append({'person': person['name'], 'days_away': delta,
                                 'relation': person.get('relation', ''),
                                 'date': bday_this_year.strftime('%B %d')})
        except Exception:
            pass
    return sorted(upcoming, key=lambda x: x['days_away'])


def get_all(profile_dir):
    return load(profile_dir, 'relationships')


def find_person(profile_dir, name):
    for p in load(profile_dir, 'relationships'):
        if name.lower() in p['name'].lower():
            return p
    return None
