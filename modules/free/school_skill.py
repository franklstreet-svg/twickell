MODULE_MANIFEST = {
    "id": "M016",
    "name": "School Tracker",
    "category": "Family",
    "description": "Track assignments, grades, school events, and academic progress for students.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_assignment", "complete_assignment", "get_assignments", "log_grade"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def add_homework(profile_dir, child_name, subject, description, due_date):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    data['homework'].append({
        'id': new_id(),
        'child': child_name,
        'subject': subject,
        'description': description,
        'due_date': due_date,
        'done': False,
        'added': today_str()
    })
    save(profile_dir, 'school', data)
    return f"Homework added for {child_name}: {subject} due {due_date}."


def complete_homework(profile_dir, homework_id):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    for hw in data['homework']:
        if hw['id'] == homework_id:
            hw['done'] = True
            save(profile_dir, 'school', data)
            return f"✓ {hw['child']}'s {hw['subject']} homework marked done!"
    return "Homework not found."


def get_homework(profile_dir, child_name='', include_done=False):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    hw = data['homework']
    if not include_done:
        hw = [h for h in hw if not h.get('done', False)]
    if child_name:
        hw = [h for h in hw if h.get('child', '').lower() == child_name.lower()]
    return sorted(hw, key=lambda h: h.get('due_date', ''))


def save_teacher(profile_dir, child_name, teacher_name, subject, phone='', email='', notes=''):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    data['teachers'].append({
        'id': new_id(),
        'child': child_name,
        'teacher': teacher_name,
        'subject': subject,
        'phone': phone,
        'email': email,
        'notes': notes
    })
    save(profile_dir, 'school', data)
    return f"Saved: {teacher_name} teaches {subject} to {child_name}."


def log_grade(profile_dir, child_name, subject, grade, assignment='', date=''):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    data['grades'].append({
        'id': new_id(),
        'child': child_name,
        'subject': subject,
        'grade': grade,
        'assignment': assignment,
        'date': date or today_str()
    })
    save(profile_dir, 'school', data)
    return f"Grade logged for {child_name}: {subject} — {grade}{' on ' + assignment if assignment else ''}."


def get_teachers(profile_dir, child_name=''):
    data = load(profile_dir, 'school', {'homework': [], 'teachers': [], 'grades': []})
    teachers = data['teachers']
    if child_name:
        teachers = [t for t in teachers if t.get('child', '').lower() == child_name.lower()]
    return teachers


def context_summary(profile_dir):
    open_hw = get_homework(profile_dir)
    if not open_hw:
        return ''
    lines = [f"HOMEWORK DUE ({len(open_hw)}):"]
    for hw in open_hw[:5]:
        lines.append(f"  • {hw['child']}: {hw['subject']} — due {hw['due_date']}")
    return '\n'.join(lines)
