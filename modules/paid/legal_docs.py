MODULE_MANIFEST = {
    "id": "I001-docs",
    "name": "Legal Document Generator",
    "category": "Legal Pro",
    "description": "Generate legal documents from case data — demand letters, briefs, intake forms, retainer agreements, motions, discovery requests, client updates.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_demand_letter", "generate_intake_form", "generate_retainer",
              "generate_case_summary", "generate_client_update", "generate_billing_invoice"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from engine.storage import load, save, now_iso, new_id
from modules.paid import legal_pro


def _now_str():
    return datetime.now().strftime('%B %d, %Y')


def generate_demand_letter(profile_dir: str, case_id: str, demand_amount: float = 0.0) -> str:
    cases = load(profile_dir, 'legal_cases')
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case {case_id} not found."
    amount = f"${demand_amount:,.2f}" if demand_amount else "[AMOUNT TO BE DETERMINED]"
    doc = f"""DEMAND LETTER
Date: {_now_str()}

RE: {case['title']}
Case Reference: {case['id']}

To Whom It May Concern:

This letter serves as formal notice that my client demands {amount} in connection with the above-referenced matter. Unless payment is received or satisfactory arrangements are made within thirty (30) days of this letter, my client will pursue all available legal remedies.

Matter Summary:
{case.get('description', '[Description not provided]')}

Status: {case.get('status', 'Active')}

Please govern yourself accordingly.

Sincerely,
[Attorney Name]
[Bar Number]
[Firm Name]
[Contact Information]"""
    _save_doc(profile_dir, 'demand_letter', case_id, doc)
    return doc


def generate_intake_form(profile_dir: str, client_name: str, case_type: str = 'General') -> str:
    doc = f"""CLIENT INTAKE FORM
Date: {_now_str()}

CLIENT INFORMATION
Name: {client_name}
Date of Birth: _______________
Address: _______________
Phone: _______________
Email: _______________
Employer: _______________

MATTER INFORMATION
Case Type: {case_type}
Description of Legal Issue:
_______________________________________________
_______________________________________________

Prior Representation: Yes / No
If yes, attorney name: _______________

CONFLICT CHECK
Adverse Parties: _______________
Related Parties: _______________

RETAINER AGREEMENT
Hourly Rate: $______/hr
Retainer Amount: $______
Payment Method: _______________

Client Signature: _______________ Date: _______________
Attorney Signature: _______________ Date: _______________"""
    _save_doc(profile_dir, 'intake_form', client_name, doc)
    return doc


def generate_retainer(profile_dir: str, client_name: str, hourly_rate: float, retainer_amount: float) -> str:
    doc = f"""RETAINER AGREEMENT
Date: {_now_str()}

This Retainer Agreement is entered into between {client_name} ("Client") and [Attorney/Firm Name] ("Attorney").

SCOPE OF REPRESENTATION
Attorney agrees to represent Client in connection with [Description of Legal Matter].

FEES AND BILLING
Hourly Rate: ${hourly_rate:,.2f} per hour
Initial Retainer: ${retainer_amount:,.2f}
Billing Cycle: Monthly
The retainer will be deposited into the client trust account and applied against fees as they accrue.

RESPONSIBILITIES OF CLIENT
Client agrees to: (1) Provide all relevant information and documents; (2) Maintain communication with Attorney; (3) Replenish retainer when balance falls below ${retainer_amount * 0.25:,.2f}.

TERMINATION
Either party may terminate this agreement with written notice.

CLIENT SIGNATURE: _______________ Date: _______________
ATTORNEY SIGNATURE: _______________ Date: _______________"""
    _save_doc(profile_dir, 'retainer', client_name, doc)
    return doc


def generate_case_summary(profile_dir: str, case_id: str) -> str:
    cases = load(profile_dir, 'legal_cases')
    case = next((c for c in cases if c['id'] == case_id), None)
    if not case:
        return f"Case {case_id} not found."

    notes = [n for n in load(profile_dir, 'legal_case_notes') if n.get('case_id') == case_id]
    deadlines = [d for d in load(profile_dir, 'legal_deadlines') if d.get('case_id') == case_id]
    time_entries = [t for t in load(profile_dir, 'legal_time') if t.get('case_id') == case_id]
    total_hours = sum(t.get('hours', 0) for t in time_entries)

    doc = f"""CASE SUMMARY REPORT
Date: {_now_str()}

CASE: {case['title']}
ID: {case['id']}
Status: {case.get('status', 'Active')}
Opened: {case.get('opened', 'Unknown')}
Description: {case.get('description', 'N/A')}

DEADLINES ({len(deadlines)}):
"""
    for d in deadlines:
        doc += f"  - {d.get('description','?')} — Due: {d.get('due_date','?')} {'[COMPLETED]' if d.get('completed') else ''}\n"

    doc += f"\nNOTES ({len(notes)}):\n"
    for n in notes[-5:]:
        doc += f"  [{n.get('date','?')}] {n.get('note','')}\n"

    doc += f"\nBILLING SUMMARY:\n  Total Hours Logged: {total_hours:.1f} hrs\n"
    _save_doc(profile_dir, 'case_summary', case_id, doc)
    return doc


def generate_client_update(profile_dir: str, case_id: str, update_text: str) -> str:
    cases = load(profile_dir, 'legal_cases')
    case = next((c for c in cases if c['id'] == case_id), cases[0] if cases else None)
    case_title = case['title'] if case else 'Your Matter'
    doc = f"""CLIENT STATUS UPDATE
Date: {_now_str()}

RE: {case_title}

Dear Client,

I am writing to provide you with an update on the status of your matter.

{update_text}

Please do not hesitate to contact our office if you have any questions or concerns. We are committed to keeping you informed throughout this process.

Sincerely,
[Attorney Name]
[Firm Name]
[Phone] | [Email]"""
    _save_doc(profile_dir, 'client_update', case_id, doc)
    return doc


def generate_billing_invoice(profile_dir: str, case_id: str, hourly_rate: float = 350.0) -> str:
    time_entries = [t for t in load(profile_dir, 'legal_time') if t.get('case_id') == case_id]
    cases = load(profile_dir, 'legal_cases')
    case = next((c for c in cases if c['id'] == case_id), None)
    case_title = case['title'] if case else case_id

    lines = [f"""LEGAL BILLING INVOICE
Date: {_now_str()}
Invoice #: INV-{new_id().upper()}

Matter: {case_title}
Case ID: {case_id}

TIME ENTRIES:
{'Date':<12} {'Description':<40} {'Hours':>6} {'Rate':>10} {'Amount':>10}
{'-'*80}"""]

    total = 0.0
    for e in time_entries:
        amount = e.get('hours', 0) * hourly_rate
        total += amount
        lines.append(f"{e.get('date','?'):<12} {e.get('description','')[:38]:<40} {e.get('hours',0):>6.1f} ${hourly_rate:>8,.2f} ${amount:>8,.2f}")

    lines.append(f"\n{'TOTAL DUE:':>70} ${total:>8,.2f}")
    lines.append("\nPayment due within 30 days. Thank you.")
    doc = '\n'.join(lines)
    _save_doc(profile_dir, 'billing_invoice', case_id, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = load(profile_dir, 'legal_documents')
    docs.append({'id': new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': now_iso()})
    save(profile_dir, 'legal_documents', docs)


def get_documents(profile_dir: str, doc_type: str = None) -> list:
    docs = load(profile_dir, 'legal_documents')
    if doc_type:
        docs = [d for d in docs if d.get('type') == doc_type]
    return docs
