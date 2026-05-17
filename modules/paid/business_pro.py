"""Business Pro — CRM, invoicing, expenses, and team tasks for small business owners."""
MODULE_MANIFEST = {
    "id": "B001",
    "name": "Business Pro",
    "category": "Business",
    "description": "Everything a small business owner needs: customer tracking, invoices, quotes, expense logging, and team task management — all managed by Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_business_customer", "list_business_customers", "get_business_customer",
              "add_business_note", "create_invoice", "list_invoices", "mark_invoice_paid",
              "create_quote", "log_expense", "get_expense_summary",
              "add_business_task", "list_business_tasks", "complete_business_task",
              "get_business_dashboard"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
from pathlib import Path


# ── Storage helpers ───────────────────────────────────────────────────────────

def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'business'
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


# ── Customers (CRM) ───────────────────────────────────────────────────────────

def add_business_customer(profile_dir: str, name: str, phone: str = '',
                           email: str = '', company: str = '',
                           source: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'biz_customers.json')
    customers = _load(path, [])
    customer = {
        'id': _uid(), 'name': name, 'phone': phone, 'email': email,
        'company': company, 'source': source, 'notes': notes,
        'status': 'active', 'created': _now()
    }
    customers.append(customer)
    _save(path, customers)
    return f"Customer added: {name}{' (' + company + ')' if company else ''} [id:{customer['id']}]"


def list_business_customers(profile_dir: str, search: str = '') -> str:
    customers = _load(_path(profile_dir, 'biz_customers.json'), [])
    if search:
        s = search.lower()
        customers = [c for c in customers
                     if s in c['name'].lower() or s in c.get('company', '').lower()]
    active = [c for c in customers if c.get('status') == 'active']
    if not active:
        return "No customers on file." if not search else f"No customers matching '{search}'."
    lines = [f"Customers ({len(active)}):"]
    for c in active:
        detail = c['phone'] or c['email'] or ''
        lines.append(f"  [{c['id']}] {c['name']}" +
                     (f" — {c['company']}" if c.get('company') else '') +
                     (f" — {detail}" if detail else ''))
    return '\n'.join(lines)


def get_business_customer(profile_dir: str, name: str) -> str:
    customers = _load(_path(profile_dir, 'biz_customers.json'), [])
    matches = [c for c in customers if name.lower() in c['name'].lower()]
    if not matches:
        return f"No customer found matching '{name}'."
    c = matches[0]
    lines = [f"Customer: {c['name']} [id:{c['id']}]"]
    for field in ['company', 'phone', 'email', 'source', 'notes']:
        if c.get(field):
            lines.append(f"  {field.title()}: {c[field]}")
    # Pull related invoices
    invoices = _load(_path(profile_dir, 'biz_invoices.json'), [])
    cust_invoices = [i for i in invoices if name.lower() in i['customer'].lower()]
    if cust_invoices:
        total_billed = sum(i['amount'] for i in cust_invoices)
        paid = sum(i['amount'] for i in cust_invoices if i['status'] == 'paid')
        lines.append(f"  Invoices: {len(cust_invoices)} — billed ${total_billed:.2f}, paid ${paid:.2f}")
    return '\n'.join(lines)


def add_business_note(profile_dir: str, customer_name: str, note: str) -> str:
    path = _path(profile_dir, 'biz_notes.json')
    notes = _load(path, [])
    notes.append({'id': _uid(), 'customer': customer_name,
                  'note': note, 'created': _now()})
    _save(path, notes)
    return f"Note saved for {customer_name}."


# ── Invoices ──────────────────────────────────────────────────────────────────

def create_invoice(profile_dir: str, customer_name: str, amount: float,
                   description: str, due_date: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'biz_invoices.json')
    invoices = _load(path, [])
    inv_num = f"INV-{len(invoices) + 1:04d}"
    invoice = {
        'id': _uid(), 'number': inv_num, 'customer': customer_name,
        'amount': amount, 'description': description,
        'due_date': due_date, 'notes': notes,
        'status': 'unpaid', 'created': _now()
    }
    invoices.append(invoice)
    _save(path, invoices)
    return (f"Invoice {inv_num} created — {customer_name}: ${amount:.2f}\n"
            f"  {description}" +
            (f"\n  Due: {due_date}" if due_date else '') +
            f"\n  [id:{invoice['id']}]")


def list_invoices(profile_dir: str, status: str = '', customer_name: str = '') -> str:
    invoices = _load(_path(profile_dir, 'biz_invoices.json'), [])
    if status:
        invoices = [i for i in invoices if i['status'] == status]
    if customer_name:
        invoices = [i for i in invoices if customer_name.lower() in i['customer'].lower()]
    if not invoices:
        return "No invoices found."
    total = sum(i['amount'] for i in invoices)
    lines = [f"Invoices ({len(invoices)}) — total ${total:.2f}:"]
    for i in sorted(invoices, key=lambda x: x['created'], reverse=True):
        lines.append(f"  [{i['number']}] {i['customer']} — ${i['amount']:.2f} — {i['status'].upper()}" +
                     (f" — due {i['due_date']}" if i.get('due_date') else ''))
    return '\n'.join(lines)


def mark_invoice_paid(profile_dir: str, invoice_id: str,
                      payment_date: str = '', method: str = '') -> str:
    path = _path(profile_dir, 'biz_invoices.json')
    invoices = _load(path, [])
    inv = next((i for i in invoices
                if i['id'] == invoice_id or i['number'] == invoice_id), None)
    if not inv:
        return f"Invoice '{invoice_id}' not found."
    inv['status'] = 'paid'
    inv['paid_date'] = payment_date or _now()[:10]
    if method:
        inv['payment_method'] = method
    _save(path, invoices)
    return f"Invoice {inv['number']} marked paid — {inv['customer']} ${inv['amount']:.2f}"


def create_quote(profile_dir: str, customer_name: str, amount: float,
                 description: str, valid_days: int = 30, notes: str = '') -> str:
    path = _path(profile_dir, 'biz_quotes.json')
    quotes = _load(path, [])
    q_num = f"QTE-{len(quotes) + 1:04d}"
    quote = {
        'id': _uid(), 'number': q_num, 'customer': customer_name,
        'amount': amount, 'description': description,
        'valid_days': valid_days, 'notes': notes,
        'status': 'sent', 'created': _now()
    }
    quotes.append(quote)
    _save(path, quotes)
    return (f"Quote {q_num} created — {customer_name}: ${amount:.2f}\n"
            f"  {description}\n"
            f"  Valid for {valid_days} days [id:{quote['id']}]")


# ── Expenses ──────────────────────────────────────────────────────────────────

def log_expense(profile_dir: str, amount: float, category: str,
                description: str, date: str = '', vendor: str = '') -> str:
    path = _path(profile_dir, 'biz_expenses.json')
    expenses = _load(path, [])
    expense = {
        'id': _uid(), 'amount': amount, 'category': category,
        'description': description, 'date': date or _now()[:10],
        'vendor': vendor, 'created': _now()
    }
    expenses.append(expense)
    _save(path, expenses)
    return f"Expense logged: ${amount:.2f} — {category} — {description}"


def get_expense_summary(profile_dir: str, month: str = '') -> str:
    expenses = _load(_path(profile_dir, 'biz_expenses.json'), [])
    if month:
        expenses = [e for e in expenses if e['date'].startswith(month)]
    if not expenses:
        return "No expenses recorded." if not month else f"No expenses for {month}."
    total = sum(e['amount'] for e in expenses)
    by_category: dict = {}
    for e in expenses:
        by_category[e['category']] = by_category.get(e['category'], 0) + e['amount']
    lines = [f"Expenses — total ${total:.2f}" + (f" ({month})" if month else '')]
    for cat, amt in sorted(by_category.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"  {cat}: ${amt:.2f}")
    return '\n'.join(lines)


# ── Business tasks ────────────────────────────────────────────────────────────

def add_business_task(profile_dir: str, task: str, due_date: str = '',
                       assigned_to: str = '', priority: str = 'normal') -> str:
    path = _path(profile_dir, 'biz_tasks.json')
    tasks = _load(path, [])
    t = {
        'id': _uid(), 'task': task, 'due_date': due_date,
        'assigned_to': assigned_to, 'priority': priority,
        'status': 'open', 'created': _now()
    }
    tasks.append(t)
    _save(path, tasks)
    return f"Business task added: {task}" + (f" — due {due_date}" if due_date else '') + f" [id:{t['id']}]"


def list_business_tasks(profile_dir: str, assigned_to: str = '',
                         priority: str = '') -> str:
    tasks = _load(_path(profile_dir, 'biz_tasks.json'), [])
    open_tasks = [t for t in tasks if t['status'] == 'open']
    if assigned_to:
        open_tasks = [t for t in open_tasks
                      if assigned_to.lower() in t.get('assigned_to', '').lower()]
    if priority:
        open_tasks = [t for t in open_tasks if t['priority'] == priority]
    if not open_tasks:
        return "No open business tasks."
    lines = [f"Business tasks ({len(open_tasks)} open):"]
    for t in sorted(open_tasks, key=lambda x: (x.get('due_date', ''), x['created'])):
        lines.append(f"  [{t['id']}] {t['task']}" +
                     (f" — due {t['due_date']}" if t.get('due_date') else '') +
                     (f" — {t['assigned_to']}" if t.get('assigned_to') else '') +
                     (f" [{t['priority']}]" if t['priority'] != 'normal' else ''))
    return '\n'.join(lines)


def complete_business_task(profile_dir: str, task_id: str) -> str:
    path = _path(profile_dir, 'biz_tasks.json')
    tasks = _load(path, [])
    t = next((t for t in tasks if t['id'] == task_id), None)
    if not t:
        return f"Task '{task_id}' not found."
    t['status'] = 'done'
    t['completed'] = _now()
    _save(path, tasks)
    return f"Done: {t['task']}"


# ── Dashboard ─────────────────────────────────────────────────────────────────

def get_business_dashboard(profile_dir: str) -> str:
    customers = _load(_path(profile_dir, 'biz_customers.json'), [])
    invoices  = _load(_path(profile_dir, 'biz_invoices.json'), [])
    expenses  = _load(_path(profile_dir, 'biz_expenses.json'), [])
    tasks     = _load(_path(profile_dir, 'biz_tasks.json'), [])

    unpaid = [i for i in invoices if i['status'] == 'unpaid']
    unpaid_total = sum(i['amount'] for i in unpaid)
    open_tasks = [t for t in tasks if t['status'] == 'open']
    month = _now()[:7]
    month_expenses = sum(e['amount'] for e in expenses if e['date'].startswith(month))
    month_revenue  = sum(i['amount'] for i in invoices
                         if i['status'] == 'paid' and i.get('paid_date', '').startswith(month))

    lines = ["Business Dashboard:"]
    lines.append(f"  Customers: {len([c for c in customers if c.get('status') == 'active'])} active")
    lines.append(f"  Unpaid invoices: {len(unpaid)} — ${unpaid_total:.2f} outstanding")
    lines.append(f"  This month: ${month_revenue:.2f} collected, ${month_expenses:.2f} spent")
    lines.append(f"  Open tasks: {len(open_tasks)}")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    invoices = _load(_path(profile_dir, 'biz_invoices.json'), [])
    unpaid = [i for i in invoices if i['status'] == 'unpaid']
    tasks = _load(_path(profile_dir, 'biz_tasks.json'), [])
    open_tasks = [t for t in tasks if t['status'] == 'open']
    urgent = [t for t in open_tasks if t.get('priority') == 'high']
    if not unpaid and not open_tasks:
        return ''
    parts = ["BUSINESS PRO ACTIVE:"]
    if unpaid:
        parts.append(f"  {len(unpaid)} unpaid invoice(s) — ${sum(i['amount'] for i in unpaid):.2f} outstanding")
    if urgent:
        parts.append(f"  {len(urgent)} high-priority task(s) open")
    elif open_tasks:
        parts.append(f"  {len(open_tasks)} open task(s)")
    parts.append(
        "What I can help you with:\n"
        "  Track: invoices, tasks, expenses, contacts, business notes\n"
        "  Write: customer proposals, professional invoices, follow-up emails,\n"
        "         payment reminder letters, business task lists, employee memos,\n"
        "         expense reports, contract summaries\n"
        "  Know: basic business writing conventions, invoice terms, professional email tone"
    )
    return '\n'.join(parts)
