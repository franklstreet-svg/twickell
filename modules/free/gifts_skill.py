MODULE_MANIFEST = {
    "id": "M021",
    "name": "Gift Ideas",
    "category": "Relationships",
    "description": "Save and organize gift ideas for people, track occasions and budgets.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_gift_idea", "get_gift_ideas", "mark_purchased"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def add_idea(profile_dir, person, idea, occasion='', budget=''):
    items = load(profile_dir, 'gifts')
    # Find existing person or create new entry
    for entry in items:
        if entry.get('person', '').lower() == person.lower():
            entry['ideas'].append({'id': new_id(), 'idea': idea, 'occasion': occasion,
                                   'budget': budget, 'purchased': False, 'added': now_iso()})
            save(profile_dir, 'gifts', items)
            return f"Gift idea added for {person}: {idea}."
    items.append({'person': person, 'ideas': [
        {'id': new_id(), 'idea': idea, 'occasion': occasion,
         'budget': budget, 'purchased': False, 'added': now_iso()}
    ]})
    save(profile_dir, 'gifts', items)
    return f"Gift idea saved for {person}: {idea}."


def get_for_person(profile_dir, person):
    for entry in load(profile_dir, 'gifts'):
        if entry.get('person', '').lower() == person.lower():
            return entry.get('ideas', [])
    return []


def mark_purchased(profile_dir, person, idea_id):
    items = load(profile_dir, 'gifts')
    for entry in items:
        if entry.get('person', '').lower() == person.lower():
            for idea in entry.get('ideas', []):
                if idea['id'] == idea_id:
                    idea['purchased'] = True
                    idea['purchased_at'] = now_iso()
    save(profile_dir, 'gifts', items)
    return "Marked as purchased."


def get_all(profile_dir):
    return load(profile_dir, 'gifts')
