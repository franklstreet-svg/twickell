MODULE_MANIFEST = {
    "id": "M018",
    "name": "Bedtime Stories",
    "category": "Family",
    "description": "Generate and store personalized bedtime stories for children.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["generate_story", "get_saved_stories"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def save_story(profile_dir, title, text, characters='', age_range='', tags=None):
    data = load(profile_dir, 'bedtime_stories', {'stories': []})
    data['stories'].append({
        'id': new_id(),
        'title': title,
        'text': text,
        'characters': characters,
        'age_range': age_range,
        'tags': tags or [],
        'date': today_str()
    })
    save(profile_dir, 'bedtime_stories', data)
    return f"Story '{title}' saved to your favorites!"


def get_stories(profile_dir, limit=5):
    data = load(profile_dir, 'bedtime_stories', {'stories': []})
    return data['stories'][-limit:]


def find_story(profile_dir, query):
    data = load(profile_dir, 'bedtime_stories', {'stories': []})
    q = query.lower()
    return [s for s in data['stories']
            if q in s.get('title', '').lower()
            or q in s.get('characters', '').lower()
            or any(q in tag.lower() for tag in s.get('tags', []))]


def story_request_text(child_name='', theme='', characters='', age=''):
    parts = ["Tell me a short, sweet bedtime story"]
    if child_name:
        parts.append(f"for {child_name}")
    if age:
        parts.append(f"(age {age})")
    if characters:
        parts.append(f"starring {characters}")
    if theme:
        parts.append(f"with a {theme} theme")
    parts.append("— keep it warm and calming, about 3-4 short paragraphs, perfect for falling asleep. End peacefully.")
    return ' '.join(parts)
