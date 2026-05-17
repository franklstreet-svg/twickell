MODULE_MANIFEST = {
    "id": "M011",
    "name": "Habit Tracker",
    "category": "Wellness",
    "description": "Create habits, log completions, and track streaks to build lasting routines.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_habit", "log_habit", "get_habits", "get_habit_streaks"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime, timedelta
from engine.storage import load, save, now_iso, today_str, new_id


def add_habit(profile_dir, name, frequency='daily', goal=''):
    data = load(profile_dir, 'habits', default={'habits': [], 'log': []})
    habit = {'id': new_id(), 'name': name, 'frequency': frequency,
             'goal': goal, 'created': now_iso(), 'active': True}
    data['habits'].append(habit)
    save(profile_dir, 'habits', data)
    return f"Habit '{name}' added ({frequency})."


def log_habit(profile_dir, habit_name, done=True, notes=''):
    data = load(profile_dir, 'habits', default={'habits': [], 'log': []})
    habit_id = None
    for h in data['habits']:
        if habit_name.lower() in h['name'].lower():
            habit_id = h['id']
            break
    entry = {'id': new_id(), 'habit_id': habit_id, 'habit_name': habit_name,
             'done': done, 'notes': notes, 'date': today_str(), 'created': now_iso()}
    data['log'].append(entry)
    save(profile_dir, 'habits', data)
    return f"{'✓' if done else '✗'} Logged {habit_name} for today."


def get_streak(profile_dir, habit_name) -> int:
    data = load(profile_dir, 'habits', default={'habits': [], 'log': []})
    entries = sorted(
        [e for e in data['log'] if habit_name.lower() in e.get('habit_name', '').lower() and e.get('done')],
        key=lambda x: x.get('date', ''), reverse=True
    )
    if not entries:
        return 0
    streak = 0
    check_date = datetime.now().date()
    for entry in entries:
        entry_date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
        if entry_date == check_date:
            streak += 1
            check_date -= timedelta(days=1)
        elif entry_date < check_date:
            break
    return streak


def get_today_habits(profile_dir):
    data = load(profile_dir, 'habits', default={'habits': [], 'log': []})
    today = today_str()
    logged_today = {e.get('habit_name', '').lower() for e in data['log'] if e.get('date') == today}
    return {
        'habits': [h for h in data['habits'] if h.get('active')],
        'logged_today': list(logged_today)
    }


def context_summary(profile_dir) -> str:
    info = get_today_habits(profile_dir)
    if not info['habits']:
        return ''
    not_done = [h['name'] for h in info['habits']
                if h['name'].lower() not in info['logged_today']]
    if not_done:
        return f"HABITS NOT YET DONE TODAY: {', '.join(not_done)}"
    return "ALL HABITS DONE TODAY ✓"
