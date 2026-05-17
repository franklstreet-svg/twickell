"""HR Professional — Employee records, onboarding, PTO tracking, and performance reviews."""
MODULE_MANIFEST = {
    "id": "I010",
    "name": "HR Professional",
    "category": "Industry",
    "description": "Human resources management for small businesses. Track employees, onboarding tasks, PTO requests, and schedule performance reviews — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_employee", "list_employees", "get_employee",
              "add_onboarding_task", "get_onboarding_status",
              "log_pto_request", "get_pto_balance",
              "schedule_performance_review", "get_upcoming_reviews"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path


# ── Storage helpers ───────────────────────────────────────────────────────────

def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'industry'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename


def _load(path: Path, default=None):
    if path.exists():
        return json.loads(path.read_text())
    return default if default is not None else {}


def _save(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now() -> str:
    return datetime.utcnow().isoformat()


def _uid() -> str:
    return str(uuid.uuid4())[:8]


DEFAULT_PTO_DAYS = 15


# ── Public tools ──────────────────────────────────────────────────────────────

def add_employee(profile_dir: str, name: str, title: str, department: str,
                 start_date: str, email: str = '', phone: str = '',
                 manager: str = '', status: str = 'active') -> str:
    path = _path(profile_dir, 'hr_employees.json')
    employees = _load(path, [])
    employee = {
        'id': _uid(),
        'name': name,
        'title': title,
        'department': department,
        'start_date': start_date,
        'email': email,
        'phone': phone,
        'manager': manager,
        'status': status,
        'pto_days_allotted': DEFAULT_PTO_DAYS,
        'created_at': _now()
    }
    employees.append(employee)
    _save(path, employees)
    return (f"Employee added [id:{employee['id']}] — {name}, {title} in {department}. "
            f"Start date: {start_date}.")


def list_employees(profile_dir: str, department: str = '',
                   status: str = 'active') -> str:
    path = _path(profile_dir, 'hr_employees.json')
    employees = _load(path, [])
    if status:
        employees = [e for e in employees if e.get('status', 'active') == status]
    if department:
        employees = [e for e in employees if e.get('department', '').lower() == department.lower()]
    if not employees:
        return "No employees found."
    lines = [f"Employees ({len(employees)}):"]
    for e in employees:
        mgr_str = f" → {e['manager']}" if e.get('manager') else ''
        lines.append(f"  [{e['id']}] {e['name']} — {e['title']}, {e['department']}{mgr_str}")
    return '\n'.join(lines)


def get_employee(profile_dir: str, name: str) -> str:
    path = _path(profile_dir, 'hr_employees.json')
    employees = _load(path, [])
    nl = name.lower()
    employee = next((e for e in employees if e.get('name', '').lower() == nl), None)
    if not employee:
        return f"Employee '{name}' not found."
    lines = [
        f"Employee: {employee['name']}",
        f"  Title: {employee['title']}",
        f"  Department: {employee['department']}",
        f"  Start date: {employee.get('start_date','')}",
        f"  Status: {employee.get('status','active')}",
    ]
    if employee.get('manager'):
        lines.append(f"  Manager: {employee['manager']}")
    if employee.get('email'):
        lines.append(f"  Email: {employee['email']}")
    if employee.get('phone'):
        lines.append(f"  Phone: {employee['phone']}")
    return '\n'.join(lines)


def add_onboarding_task(profile_dir: str, employee_name: str, task: str,
                        due_date: str = '', completed: bool = False) -> str:
    path = _path(profile_dir, 'hr_onboarding.json')
    onboarding = _load(path, [])
    item = {
        'id': _uid(),
        'employee_name': employee_name,
        'task': task,
        'due_date': due_date,
        'completed': completed,
        'created_at': _now()
    }
    onboarding.append(item)
    _save(path, onboarding)
    return (f"Onboarding task added for {employee_name}: '{task}'"
            + (f" due {due_date}" if due_date else "")
            + (" [completed]" if completed else "") + ".")


def get_onboarding_status(profile_dir: str, employee_name: str) -> str:
    path = _path(profile_dir, 'hr_onboarding.json')
    onboarding = _load(path, [])
    nl = employee_name.lower()
    tasks = [t for t in onboarding if t.get('employee_name', '').lower() == nl]
    if not tasks:
        return f"No onboarding tasks found for {employee_name}."
    done = [t for t in tasks if t.get('completed')]
    pending = [t for t in tasks if not t.get('completed')]
    lines = [f"Onboarding: {employee_name} — {len(done)}/{len(tasks)} complete"]
    if pending:
        lines.append("  Pending:")
        for t in pending:
            due = f" (due {t['due_date']})" if t.get('due_date') else ''
            lines.append(f"    [ ] {t['task']}{due} [id:{t['id']}]")
    if done:
        lines.append("  Completed:")
        for t in done:
            lines.append(f"    [x] {t['task']}")
    return '\n'.join(lines)


def log_pto_request(profile_dir: str, employee_name: str, start_date: str,
                    end_date: str, type: str, status: str = 'pending',
                    notes: str = '') -> str:
    """type: vacation/sick/personal"""
    path = _path(profile_dir, 'hr_pto.json')
    pto = _load(path, [])
    # Calculate days (simple business day count)
    try:
        sd = datetime.strptime(start_date, '%Y-%m-%d')
        ed = datetime.strptime(end_date, '%Y-%m-%d')
        days = max(1, (ed - sd).days + 1)
    except Exception:
        days = 1
    request = {
        'id': _uid(),
        'employee_name': employee_name,
        'start_date': start_date,
        'end_date': end_date,
        'type': type,
        'days': days,
        'status': status,
        'notes': notes,
        'created_at': _now()
    }
    pto.append(request)
    _save(path, pto)
    return (f"PTO request logged [id:{request['id']}] — {employee_name}: "
            f"{type} {start_date} to {end_date} ({days} day(s)). Status: {status}.")


def get_pto_balance(profile_dir: str, employee_name: str) -> str:
    # Get allotment
    emp_path = _path(profile_dir, 'hr_employees.json')
    employees = _load(emp_path, [])
    nl = employee_name.lower()
    employee = next((e for e in employees if e.get('name', '').lower() == nl), None)
    allotted = employee.get('pto_days_allotted', DEFAULT_PTO_DAYS) if employee else DEFAULT_PTO_DAYS

    # Get approved/used PTO
    pto_path = _path(profile_dir, 'hr_pto.json')
    pto = _load(pto_path, [])
    year = _now()[:4]
    used_requests = [r for r in pto
                     if r.get('employee_name', '').lower() == nl
                     and r.get('status') in ('approved', 'used')
                     and r.get('start_date', '').startswith(year)]
    days_used = sum(r.get('days', 0) for r in used_requests)
    remaining = allotted - days_used

    return (f"PTO Balance: {employee_name} ({year})\n"
            f"  Allotted: {allotted} days\n"
            f"  Used: {days_used} days\n"
            f"  Remaining: {remaining} days")


def schedule_performance_review(profile_dir: str, employee_name: str,
                                 review_date: str, reviewer: str = '',
                                 notes: str = '') -> str:
    path = _path(profile_dir, 'hr_reviews.json')
    reviews = _load(path, [])
    review = {
        'id': _uid(),
        'employee_name': employee_name,
        'review_date': review_date,
        'reviewer': reviewer,
        'notes': notes,
        'status': 'scheduled',
        'created_at': _now()
    }
    reviews.append(review)
    _save(path, reviews)
    return (f"Performance review scheduled [id:{review['id']}] — {employee_name} on {review_date}"
            + (f" with {reviewer}" if reviewer else "") + ".")


def get_upcoming_reviews(profile_dir: str, days: int = 60) -> str:
    path = _path(profile_dir, 'hr_reviews.json')
    reviews = _load(path, [])
    today = _now()[:10]
    cutoff = (datetime.utcnow() + timedelta(days=days)).strftime('%Y-%m-%d')
    upcoming = [r for r in reviews
                if r.get('status') == 'scheduled'
                and today <= r.get('review_date', '') <= cutoff]
    overdue = [r for r in reviews
               if r.get('status') == 'scheduled'
               and r.get('review_date', '') < today]
    lines = []
    if overdue:
        lines.append(f"Overdue reviews ({len(overdue)}):")
        for r in sorted(overdue, key=lambda x: x.get('review_date', '')):
            lines.append(f"  [{r['id']}] {r['employee_name']} — was {r['review_date']}")
    if upcoming:
        lines.append(f"Upcoming reviews in {days} days ({len(upcoming)}):")
        for r in sorted(upcoming, key=lambda x: x.get('review_date', '')):
            reviewer_str = f" with {r['reviewer']}" if r.get('reviewer') else ''
            lines.append(f"  [{r['id']}] {r['review_date']} — {r['employee_name']}{reviewer_str}")
    if not lines:
        return f"No performance reviews scheduled in the next {days} days."
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    employees = _load(_path(profile_dir, 'hr_employees.json'), [])
    if not employees:
        return ''
    active = [e for e in employees if e.get('status') == 'active']
    today = _now()[:10]
    week = (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d')
    reviews = _load(_path(profile_dir, 'hr_reviews.json'), [])
    upcoming_reviews = [r for r in reviews
                        if r.get('status') == 'scheduled'
                        and today <= r.get('review_date', '') <= week]
    # Pending PTO
    pto = _load(_path(profile_dir, 'hr_pto.json'), [])
    pending_pto = [r for r in pto if r.get('status') == 'pending']
    parts = [f"HR PRO ACTIVE: {len(active)} active employee(s)"]
    if upcoming_reviews:
        parts.append(f"  {len(upcoming_reviews)} review(s) this week")
    if pending_pto:
        parts.append(f"  {len(pending_pto)} pending PTO request(s)")
    parts.append(
        "What I can help you with:\n"
        "  Track: employees, onboarding tasks, PTO balances, performance reviews,\n"
        "         compliance reminders, departments\n"
        "  Write: job postings, offer letters, employee handbook sections, policy documents,\n"
        "         termination letters, disciplinary action letters, performance review summaries,\n"
        "         PTO approval/denial letters, onboarding welcome letters\n"
        "  Know: common HR compliance topics, onboarding best practices, org structure basics"
    )
    return '\n'.join(parts)
