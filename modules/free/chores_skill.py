MODULE_MANIFEST = {
    "id": "M015",
    "name": "Chore Tracker",
    "category": "Family",
    "description": "Assign, track, and manage household chores for all family members.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_chore", "complete_chore", "get_chores", "assign_chore"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def add_chore(profile_dir, chore, assigned_to='', frequency='weekly', points=0):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    data['chores'].append({
        'id': new_id(),
        'chore': chore,
        'assigned_to': assigned_to,
        'frequency': frequency,
        'points': int(points),
        'active': True,
        'created': today_str()
    })
    save(profile_dir, 'chores', data)
    who = f" for {assigned_to}" if assigned_to else ''
    return f"Chore added{who}: {chore} ({frequency}, {points} pts)."


def complete_chore(profile_dir, chore_id, completed_by=''):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    chore = next((c for c in data['chores'] if c['id'] == chore_id), None)
    if not chore:
        return "Chore not found."
    who = completed_by or chore.get('assigned_to', '')
    data['completions'].append({
        'id': new_id(),
        'chore_id': chore_id,
        'chore': chore['chore'],
        'completed_by': who,
        'date': today_str(),
        'points': chore.get('points', 0)
    })
    save(profile_dir, 'chores', data)
    pts = chore.get('points', 0)
    return f"Done! '{chore['chore']}' marked complete{' by ' + who if who else ''}. +{pts} pts"


def delete_chore(profile_dir, chore_id):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    for c in data['chores']:
        if c['id'] == chore_id:
            c['active'] = False
            save(profile_dir, 'chores', data)
            return f"Chore '{c['chore']}' removed."
    return "Chore not found."


def get_chores(profile_dir, assigned_to=''):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    chores = [c for c in data['chores'] if c.get('active', True)]
    if assigned_to:
        chores = [c for c in chores if c.get('assigned_to', '').lower() == assigned_to.lower()]
    today = today_str()
    done_today = {c['chore_id'] for c in data['completions'] if c['date'] == today}
    for c in chores:
        c['done_today'] = c['id'] in done_today
    return chores


def get_points(profile_dir, person):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    completions = [c for c in data['completions'] if c.get('completed_by', '').lower() == person.lower()]
    total = sum(c.get('points', 0) for c in completions)
    return {'person': person, 'total_points': total, 'completions': len(completions)}


def get_leaderboard(profile_dir):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    scores = {}
    for c in data['completions']:
        who = c.get('completed_by', 'Unknown')
        if who:
            scores[who] = scores.get(who, 0) + c.get('points', 0)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def context_summary(profile_dir):
    data = load(profile_dir, 'chores', {'chores': [], 'completions': []})
    active = [c for c in data['chores'] if c.get('active', True)]
    if not active:
        return ''
    today = today_str()
    done_today = {c['chore_id'] for c in data['completions'] if c['date'] == today}
    pending = [c for c in active if c['id'] not in done_today]
    if not pending:
        return ''
    lines = [f"CHORES PENDING ({len(pending)}):"]
    for c in pending[:5]:
        who = f" → {c['assigned_to']}" if c.get('assigned_to') else ''
        lines.append(f"  • {c['chore']}{who}")
    return '\n'.join(lines)
