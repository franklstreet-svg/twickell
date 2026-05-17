MODULE_MANIFEST = {
    "id": "M027",
    "name": "Travel Planner",
    "category": "Lifestyle",
    "description": "Plan trips, track itineraries, save travel notes, and manage packing lists.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["plan_trip", "add_trip_item", "get_trips"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def add_trip(profile_dir, destination, start_date='', end_date='', notes=''):
    items = load(profile_dir, 'travel')
    trip = {'id': new_id(), 'destination': destination,
            'start_date': start_date, 'end_date': end_date,
            'notes': notes, 'packing_list': [], 'itinerary': [], 'created': now_iso()}
    items.append(trip)
    save(profile_dir, 'travel', items)
    return f"Trip to {destination} saved."


def add_packing_item(profile_dir, trip_id, item):
    items = load(profile_dir, 'travel')
    for trip in items:
        if trip['id'] == trip_id:
            trip['packing_list'].append({'item': item, 'packed': False})
    save(profile_dir, 'travel', items)
    return f"Added '{item}' to packing list."


def add_itinerary_item(profile_dir, trip_id, date, activity, notes=''):
    items = load(profile_dir, 'travel')
    for trip in items:
        if trip['id'] == trip_id:
            trip['itinerary'].append({'date': date, 'activity': activity, 'notes': notes})
    save(profile_dir, 'travel', items)
    return f"Added to itinerary: {activity} on {date}."


def mark_packed(profile_dir, trip_id, item_name):
    items = load(profile_dir, 'travel')
    for trip in items:
        if trip['id'] == trip_id:
            for p in trip.get('packing_list', []):
                if item_name.lower() in p['item'].lower():
                    p['packed'] = True
    save(profile_dir, 'travel', items)
    return f"Marked as packed."


def get_trips(profile_dir):
    return sorted(load(profile_dir, 'travel'),
                  key=lambda x: x.get('start_date', ''), reverse=True)


def get_upcoming_trips(profile_dir):
    from engine.storage import today_str
    today = today_str()
    return [t for t in get_trips(profile_dir) if t.get('start_date', '') >= today]
