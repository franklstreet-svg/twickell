"""Contractor Pro — Jobs, estimates, materials, subcontractors, invoicing, and labor tracking."""
MODULE_MANIFEST = {
    "id": "I007",
    "name": "Contractor Pro",
    "category": "Industry",
    "description": "Complete contractor job management. Track jobs, build estimates with markup, log materials and labor hours, manage subcontractors, and create invoices — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_job", "list_jobs", "update_job",
              "add_estimate", "add_materials", "add_subcontractor",
              "create_invoice", "log_job_hours", "get_job_summary"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
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

def add_job(profile_dir: str, client_name: str, address: str,
            description: str, start_date: str = '', end_date: str = '',
            status: str = 'active', notes: str = '') -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = {
        'id': _uid(),
        'client_name': client_name,
        'address': address,
        'description': description,
        'start_date': start_date,
        'end_date': end_date,
        'status': status,
        'notes': notes,
        'estimates': [],
        'materials': [],
        'hours': [],
        'invoices': [],
        'created_at': _now()
    }
    jobs.append(job)
    _save(path, jobs)
    return f"Job added [id:{job['id']}] — {client_name} at {address}. Status: {status}."


def list_jobs(profile_dir: str, status: str = '') -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    if status:
        jobs = [j for j in jobs if j.get('status', '').lower() == status.lower()]
    if not jobs:
        return f"No jobs found{' with status ' + status if status else ''}."
    lines = [f"Jobs ({len(jobs)}):"]
    for j in jobs:
        lines.append(f"  [{j['id']}] {j['client_name']} — {j['address']} [{j.get('status','active')}]")
    return '\n'.join(lines)


def update_job(profile_dir: str, job_id: str, status: str = '',
               notes: str = '', end_date: str = '') -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    if status:
        job['status'] = status
    if notes:
        job['notes'] = (job.get('notes', '') + '\n' + notes).strip()
    if end_date:
        job['end_date'] = end_date
    job['updated_at'] = _now()
    _save(path, jobs)
    return f"Job {job_id} updated — {job['client_name']} at {job['address']}."


def add_estimate(profile_dir: str, job_id: str, labor_cost: float,
                 materials_cost: float, description: str = '',
                 markup_pct: float = 20.0) -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    subtotal = labor_cost + materials_cost
    markup_amount = subtotal * (markup_pct / 100)
    total = subtotal + markup_amount
    estimate = {
        'id': _uid(),
        'labor_cost': labor_cost,
        'materials_cost': materials_cost,
        'markup_pct': markup_pct,
        'markup_amount': markup_amount,
        'subtotal': subtotal,
        'total': total,
        'description': description,
        'created_at': _now()
    }
    job.setdefault('estimates', []).append(estimate)
    _save(path, jobs)
    return (f"Estimate added [id:{estimate['id']}] for job {job_id}:\n"
            f"  Labor: ${labor_cost:,.2f}  Materials: ${materials_cost:,.2f}\n"
            f"  Markup ({markup_pct}%): ${markup_amount:,.2f}\n"
            f"  Total: ${total:,.2f}")


def add_materials(profile_dir: str, job_id: str, items: list) -> str:
    """items: list of {name, qty, unit, cost}"""
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    total_cost = sum(i.get('cost', 0) * i.get('qty', 1) for i in items)
    materials_entry = {
        'id': _uid(),
        'items': items,
        'total_cost': total_cost,
        'added_at': _now()
    }
    job.setdefault('materials', []).append(materials_entry)
    _save(path, jobs)
    return (f"Materials added to job {job_id} ({len(items)} item(s), "
            f"total cost: ${total_cost:,.2f}).")


def add_subcontractor(profile_dir: str, name: str, trade: str, phone: str,
                      email: str = '', rate: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'contractor_subs.json')
    subs = _load(path, [])
    sub = {
        'id': _uid(),
        'name': name,
        'trade': trade,
        'phone': phone,
        'email': email,
        'rate': rate,
        'notes': notes,
        'created_at': _now()
    }
    subs.append(sub)
    _save(path, subs)
    return (f"Subcontractor added [id:{sub['id']}] — {name} ({trade}), {phone}"
            + (f", rate: {rate}" if rate else "") + ".")


def create_invoice(profile_dir: str, job_id: str, due_date: str = '',
                   notes: str = '') -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    # Sum up estimates
    estimate_total = sum(e.get('total', 0) for e in job.get('estimates', []))
    # Sum up materials
    materials_total = sum(m.get('total_cost', 0) for m in job.get('materials', []))
    # Sum up hours (at $75/hr default)
    HOURLY = 75.0
    total_hours = sum(h.get('hours', 0) for h in job.get('hours', []))
    labor_from_hours = total_hours * HOURLY
    # Use estimate if available, otherwise calculate from hours+materials
    if estimate_total > 0:
        invoice_total = estimate_total
    else:
        invoice_total = labor_from_hours + materials_total

    invoice = {
        'id': _uid(),
        'job_id': job_id,
        'client_name': job['client_name'],
        'address': job['address'],
        'estimate_total': estimate_total,
        'materials_total': materials_total,
        'labor_hours': total_hours,
        'invoice_total': invoice_total,
        'due_date': due_date,
        'notes': notes,
        'status': 'unpaid',
        'created_at': _now()
    }
    job.setdefault('invoices', []).append(invoice)
    _save(path, jobs)
    return (f"Invoice created [id:{invoice['id']}] for {job['client_name']}:\n"
            f"  Job: {job['address']}\n"
            f"  Total: ${invoice_total:,.2f}"
            + (f"\n  Due: {due_date}" if due_date else ""))


def log_job_hours(profile_dir: str, job_id: str, date: str, hours: float,
                  worker: str, description: str = '') -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    entry = {
        'id': _uid(),
        'date': date,
        'hours': hours,
        'worker': worker,
        'description': description,
        'logged_at': _now()
    }
    job.setdefault('hours', []).append(entry)
    _save(path, jobs)
    return f"Logged {hours}h for {worker} on job {job_id} ({date})" + (f": {description}" if description else "") + "."


def get_job_summary(profile_dir: str, job_id: str) -> str:
    path = _path(profile_dir, 'contractor_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    lines = [
        f"Job Summary: {job['client_name']}",
        f"  Address: {job['address']}",
        f"  Status: {job.get('status','active')}",
        f"  Description: {job.get('description','')}",
    ]
    if job.get('start_date'):
        lines.append(f"  Start: {job['start_date']}" + (f"  End: {job.get('end_date','TBD')}" if job.get('end_date') else ''))
    estimates = job.get('estimates', [])
    if estimates:
        latest = estimates[-1]
        lines.append(f"  Latest Estimate: ${latest.get('total',0):,.2f} "
                     f"(labor ${latest.get('labor_cost',0):,.2f} + materials ${latest.get('materials_cost',0):,.2f} + {latest.get('markup_pct',20)}% markup)")
    hours = job.get('hours', [])
    if hours:
        total_h = sum(h.get('hours', 0) for h in hours)
        lines.append(f"  Labor Hours: {total_h:.1f}h ({len(hours)} entries)")
    materials = job.get('materials', [])
    if materials:
        mat_total = sum(m.get('total_cost', 0) for m in materials)
        lines.append(f"  Materials: ${mat_total:,.2f} ({len(materials)} order(s))")
    invoices = job.get('invoices', [])
    if invoices:
        latest_inv = invoices[-1]
        lines.append(f"  Invoice: ${latest_inv.get('invoice_total',0):,.2f} [{latest_inv.get('status','unpaid')}]"
                     + (f" due {latest_inv.get('due_date','')}" if latest_inv.get('due_date') else ''))
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    jobs = _load(_path(profile_dir, 'contractor_jobs.json'), [])
    if not jobs:
        return ''
    active = [j for j in jobs if j.get('status') == 'active']
    # Unpaid invoices
    unpaid = 0
    unpaid_total = 0.0
    for j in jobs:
        for inv in j.get('invoices', []):
            if inv.get('status') == 'unpaid':
                unpaid += 1
                unpaid_total += inv.get('invoice_total', 0)
    parts = [f"CONTRACTOR PRO ACTIVE: {len(active)} active job(s) of {len(jobs)} total"]
    if unpaid:
        parts.append(f"  {unpaid} unpaid invoice(s) totaling ${unpaid_total:,.2f}")
    parts.append(
        "What I can help you with:\n"
        "  Track: jobs, materials, labor hours, subcontractors, invoices, job costs\n"
        "  Write: scope of work documents, bid proposals, change orders,\n"
        "         subcontractor agreements, project progress reports, client update letters,\n"
        "         material lists, punch lists, safety incident reports, lien waiver letters\n"
        "  Know: general construction practices, markup calculations, contract basics"
    )
    return '\n'.join(parts)
