MODULE_MANIFEST = {
    "id": "M012",
    "name": "Fitness Log",
    "category": "Wellness",
    "description": "Log workouts, track exercise history, and monitor fitness goals and progress.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["log_workout", "get_workouts", "set_fitness_goal"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, today_str, new_id


def log_workout(profile_dir, exercise, duration_minutes, sets=None, reps=None, weight=None, notes=''):
    items = load(profile_dir, 'fitness')
    entry = {'id': new_id(), 'exercise': exercise,
             'duration_minutes': duration_minutes,
             'sets': sets, 'reps': reps, 'weight': weight,
             'notes': notes, 'date': today_str(), 'created': now_iso()}
    items.append(entry)
    save(profile_dir, 'fitness', items)
    parts = [f"Logged: {exercise}"]
    if duration_minutes:
        parts.append(f"{duration_minutes} min")
    if sets and reps:
        parts.append(f"{sets}x{reps}")
        if weight:
            parts.append(f"@ {weight}lbs")
    return " ".join(parts) + "."


def get_recent(profile_dir, days=7):
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    return [e for e in load(profile_dir, 'fitness') if e.get('date', '') >= cutoff]


def get_history(profile_dir, exercise=None):
    items = load(profile_dir, 'fitness')
    if exercise:
        items = [e for e in items if exercise.lower() in e.get('exercise', '').lower()]
    return sorted(items, key=lambda x: x.get('date', ''), reverse=True)


def get_stats(profile_dir, days=30):
    recent = get_recent(profile_dir, days)
    if not recent:
        return f"No workouts logged in the last {days} days."
    total_min = sum(e.get('duration_minutes', 0) or 0 for e in recent)
    types = {}
    for e in recent:
        ex = e.get('exercise', 'unknown')
        types[ex] = types.get(ex, 0) + 1
    top = sorted(types.items(), key=lambda x: x[1], reverse=True)[:3]
    return (f"{len(recent)} workouts in last {days} days, {total_min} total minutes. "
            f"Top activities: {', '.join(f'{e} ({c}x)' for e, c in top)}.")
