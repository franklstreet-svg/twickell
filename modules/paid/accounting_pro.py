"""Accounting Pro — Client accounting, tax deadlines, transaction logging, and document tracking."""
MODULE_MANIFEST = {
    "id": "I009",
    "name": "Accounting Pro",
    "category": "Industry",
    "description": "Practice management for accountants and bookkeepers. Track clients, tax deadlines, income/expense transactions, and document references — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_accounting_client", "list_accounting_clients",
              "add_tax_deadline", "get_upcoming_tax_deadlines",
              "log_transaction", "get_client_financials",
              "add_document_ref", "get_document_refs"],
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


# ── Public tools ──────────────────────────────────────────────────────────────

def add_accounting_client(profile_dir: str, name: str, business_name: str,
                          phone: str, email: str = '', tax_id: str = '',
                          filing_type: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'accounting_clients.json')
    clients = _load(path, [])
    client = {
        'id': _uid(),
        'name': name,
        'business_name': business_name,
        'phone': phone,
        'email': email,
        'tax_id': tax_id,
        'filing_type': filing_type,
        'notes': notes,
        'created_at': _now()
    }
    clients.append(client)
    _save(path, clients)
    return (f"Client added [id:{client['id']}] — {name} / {business_name}"
            + (f", filing: {filing_type}" if filing_type else "") + ".")


def list_accounting_clients(profile_dir: str, search: str = '') -> str:
    path = _path(profile_dir, 'accounting_clients.json')
    clients = _load(path, [])
    if search:
        sl = search.lower()
        clients = [c for c in clients if sl in c.get('name', '').lower()
                   or sl in c.get('business_name', '').lower()]
    if not clients:
        return "No clients found."
    lines = [f"Clients ({len(clients)}):"]
    for c in clients:
        lines.append(f"  [{c['id']}] {c['name']} / {c.get('business_name','')} "
                     f"— {c.get('filing_type','')}  {c.get('phone','')}")
    return '\n'.join(lines)


def add_tax_deadline(profile_dir: str, client_name: str, deadline_type: str,
                     due_date: str, description: str = '',
                     status: str = 'pending') -> str:
    path = _path(profile_dir, 'accounting_deadlines.json')
    deadlines = _load(path, [])
    dl = {
        'id': _uid(),
        'client_name': client_name,
        'deadline_type': deadline_type,
        'due_date': due_date,
        'description': description,
        'status': status,
        'created_at': _now()
    }
    deadlines.append(dl)
    _save(path, deadlines)
    return (f"Tax deadline added [id:{dl['id']}] — {client_name}: "
            f"{deadline_type} due {due_date}. Status: {status}.")


def get_upcoming_tax_deadlines(profile_dir: str, days: int = 30) -> str:
    path = _path(profile_dir, 'accounting_deadlines.json')
    deadlines = _load(path, [])
    today = _now()[:10]
    cutoff = (datetime.utcnow() + timedelta(days=days)).strftime('%Y-%m-%d')
    upcoming = [d for d in deadlines
                if d.get('status') == 'pending'
                and d.get('due_date', '') >= today
                and d.get('due_date', '') <= cutoff]
    overdue = [d for d in deadlines
               if d.get('status') == 'pending' and d.get('due_date', '') < today]
    lines = []
    if overdue:
        lines.append(f"OVERDUE ({len(overdue)}):")
        for d in sorted(overdue, key=lambda x: x.get('due_date', '')):
            lines.append(f"  [{d['id']}] {d['client_name']}: {d['deadline_type']} — was due {d['due_date']}")
    if upcoming:
        lines.append(f"Due in next {days} days ({len(upcoming)}):")
        for d in sorted(upcoming, key=lambda x: x.get('due_date', '')):
            lines.append(f"  [{d['id']}] {d['due_date']} — {d['client_name']}: {d['deadline_type']}")
    if not lines:
        return f"No pending deadlines in the next {days} days."
    return '\n'.join(lines)


def log_transaction(profile_dir: str, client_name: str, date: str,
                    type: str, amount: float, description: str,
                    category: str = '') -> str:
    """type: income or expense"""
    path = _path(profile_dir, 'accounting_transactions.json')
    txns = _load(path, [])
    txn = {
        'id': _uid(),
        'client_name': client_name,
        'date': date,
        'type': type.lower(),
        'amount': amount,
        'description': description,
        'category': category,
        'created_at': _now()
    }
    txns.append(txn)
    _save(path, txns)
    return f"Transaction logged [id:{txn['id']}] — {client_name}: {type} ${amount:,.2f} on {date} ({description})."


def get_client_financials(profile_dir: str, client_name: str,
                          year: str = '') -> str:
    path = _path(profile_dir, 'accounting_transactions.json')
    txns = _load(path, [])
    cl = client_name.lower()
    txns = [t for t in txns if t.get('client_name', '').lower() == cl]
    if year:
        txns = [t for t in txns if t.get('date', '').startswith(year)]
    if not txns:
        return f"No transactions found for {client_name}{' in ' + year if year else ''}."
    income = sum(t['amount'] for t in txns if t.get('type') == 'income')
    expenses = sum(t['amount'] for t in txns if t.get('type') == 'expense')
    net = income - expenses
    # Group by category
    cats = {}
    for t in txns:
        cat = t.get('category') or t.get('type', 'other')
        cats.setdefault(cat, []).append(t['amount'])
    lines = [
        f"Financials: {client_name}" + (f" ({year})" if year else ""),
        f"  Total income:  ${income:,.2f}",
        f"  Total expenses: ${expenses:,.2f}",
        f"  Net:           ${net:,.2f}",
    ]
    if cats:
        lines.append("  By category:")
        for cat, amounts in cats.items():
            lines.append(f"    {cat}: ${sum(amounts):,.2f} ({len(amounts)} entries)")
    return '\n'.join(lines)


def add_document_ref(profile_dir: str, client_name: str, doc_type: str,
                     filename: str, year: str, notes: str = '') -> str:
    path = _path(profile_dir, 'accounting_documents.json')
    docs = _load(path, [])
    doc = {
        'id': _uid(),
        'client_name': client_name,
        'doc_type': doc_type,
        'filename': filename,
        'year': year,
        'notes': notes,
        'created_at': _now()
    }
    docs.append(doc)
    _save(path, docs)
    return (f"Document reference added [id:{doc['id']}] — {client_name}: "
            f"{doc_type} '{filename}' ({year}). (Local reference only.)")


def get_document_refs(profile_dir: str, client_name: str) -> str:
    path = _path(profile_dir, 'accounting_documents.json')
    docs = _load(path, [])
    cl = client_name.lower()
    docs = [d for d in docs if d.get('client_name', '').lower() == cl]
    if not docs:
        return f"No documents on file for {client_name}."
    lines = [f"Documents on file for {client_name} ({len(docs)}):"]
    for d in sorted(docs, key=lambda x: x.get('year', ''), reverse=True):
        notes_str = f" — {d['notes']}" if d.get('notes') else ''
        lines.append(f"  [{d['id']}] {d.get('year','')} — {d['doc_type']}: {d['filename']}{notes_str}")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    clients = _load(_path(profile_dir, 'accounting_clients.json'), [])
    if not clients:
        return ''
    today = _now()[:10]
    week_out = (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d')
    deadlines = _load(_path(profile_dir, 'accounting_deadlines.json'), [])
    overdue = [d for d in deadlines if d.get('status') == 'pending' and d.get('due_date', '') < today]
    soon = [d for d in deadlines if d.get('status') == 'pending'
            and today <= d.get('due_date', '') <= week_out]
    parts = [f"ACCOUNTING PRO ACTIVE: {len(clients)} client(s) on file"]
    if overdue:
        parts.append(f"  {len(overdue)} OVERDUE deadline(s)")
    if soon:
        parts.append(f"  {len(soon)} deadline(s) due within 7 days")
    parts.append(
        "What I can help you with:\n"
        "  Track: clients, tax deadlines, income/expenses, document references, filing types\n"
        "  Write: client engagement letters, tax organizer cover letters,\n"
        "         financial summary reports, IRS correspondence letters,\n"
        "         extension request letters, client year-end letters, billing statements\n"
        "  Know: common tax deadlines, filing types, P&L basics, general accounting concepts"
    )
    return '\n'.join(parts)
