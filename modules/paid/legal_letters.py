MODULE_MANIFEST = {
    "id": "I001-letters",
    "name": "Legal Letters Generator",
    "category": "Legal Pro",
    "description": "Generate specialized legal correspondence — cease and desist letters, debt collection letters, litigation hold/spoliation letters, mediation statements, settlement demand letters.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_cease_desist", "generate_collection_letter",
              "generate_litigation_hold", "generate_mediation_statement",
              "generate_settlement_demand", "generate_closing_letter"],
    "min_bridge_version": "1.0.0"
}

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _path(profile_dir, filename):
    p = Path(profile_dir) / 'industry'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename

def _load(path, default=None):
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return default if default is not None else []

def _save(path, data):
    Path(path).write_text(json.dumps(data, indent=2))

def _now_str():
    return datetime.now().strftime('%B %d, %Y')

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _new_id():
    return str(uuid.uuid4())[:8]


def generate_cease_desist(profile_dir: str, client_name: str,
                           recipient: str, violation: str,
                           demands: list = None, deadline_days: int = 10) -> str:
    demands_list = demands or [
        f'Immediately cease and desist all {violation}',
        'Confirm in writing that you have complied with this demand',
        'Preserve all documents and evidence related to this matter',
    ]
    demands_text = '\n'.join(f"  {i+1}. {d}" for i, d in enumerate(demands_list))

    doc = f"""VIA CERTIFIED MAIL — RETURN RECEIPT REQUESTED
AND FIRST CLASS MAIL

{_now_str()}

{recipient}
[Address]
[City, State, ZIP]

Re: CEASE AND DESIST — {violation.upper()}

Dear {recipient}:

This firm represents {client_name} ("Client"). We write to demand that you immediately cease and desist from the following unlawful conduct:

{violation}

FACTUAL BACKGROUND

Our client has become aware that you have engaged in the above-described conduct, which constitutes a violation of [applicable law/rights]. Specifically:

[Describe specific acts, dates, and circumstances with supporting facts.]

This conduct is unlawful and has caused our client significant harm, including [describe harm/damages].

LEGAL BASIS

Your conduct violates [applicable statutes, common law, contractual obligations], including but not limited to:

• [Specific statute or legal basis #1]
• [Specific statute or legal basis #2]
• [Any applicable intellectual property, contract, or tort law]

DEMANDS

You are hereby demanded to, within {deadline_days} days of the date of this letter:

{demands_text}

CONSEQUENCES OF NON-COMPLIANCE

If you fail to comply with these demands within the time specified, our client will have no choice but to pursue all available legal remedies, including but not limited to:

• Filing suit in a court of competent jurisdiction;
• Seeking injunctive relief;
• Seeking compensatory and punitive damages;
• Seeking attorney's fees and costs to the extent permitted by law.

This letter is written without prejudice to our client's rights and remedies, all of which are expressly reserved.

Your immediate attention to this matter is required.

Very truly yours,

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]
[Address]
[Phone] | [Email]
Counsel for {client_name}

cc: {client_name} [file]"""
    _save_doc(profile_dir, 'cease_desist', client_name, doc)
    return doc


def generate_collection_letter(profile_dir: str, creditor: str,
                                debtor: str, amount_owed: float,
                                debt_description: str,
                                letter_number: int = 1) -> str:
    urgency = {
        1: ("NOTICE OF OUTSTANDING BALANCE",
            "We write as a professional courtesy to notify you of the outstanding balance described below. We trust this is an oversight and look forward to your prompt response."),
        2: ("SECOND NOTICE — PAYMENT REQUIRED",
            "Despite our previous correspondence, the balance below remains unpaid. Immediate payment or contact is required to avoid escalation of this matter."),
        3: ("FINAL NOTICE BEFORE LEGAL ACTION",
            "This is your FINAL NOTICE. Despite repeated demands, the balance below remains unpaid. If payment is not received within 10 days, we will proceed with formal legal action without further notice."),
    }.get(min(letter_number, 3), ("NOTICE OF OUTSTANDING BALANCE", ""))

    doc = f"""VIA CERTIFIED MAIL

{_now_str()}

{debtor}
[Address]
[City, State, ZIP]

Re: {urgency[0]}

Dear {debtor}:

This firm represents {creditor} in connection with the collection of monies due and owing.

{urgency[1]}

ACCOUNT SUMMARY

Creditor: {creditor}
Debtor: {debtor}
Description: {debt_description}
Principal Amount: ${amount_owed:,.2f}
Interest Accrued: $[_______________]
Fees: $[_______________]
TOTAL DUE: $[_______________]

PAYMENT INSTRUCTIONS

To resolve this matter, please remit payment in full to:

[Payee Name]
[Mailing Address / Wire Instructions]
[Reference: Account/Matter Number]

DISPUTE RIGHTS (FDCPA Notice, if applicable)

If you believe this debt is not valid, you have the right to dispute it within 30 days of receiving this notice. If you dispute the debt in writing within 30 days, we will cease collection activity until we provide verification. If you do not dispute the debt within 30 days, we will assume the debt is valid.

You have the right to request, in writing, the name and address of the original creditor if different from the current creditor.

THIS IS AN ATTEMPT TO COLLECT A DEBT AND ANY INFORMATION OBTAINED WILL BE USED FOR THAT PURPOSE.

Please contact our office immediately to resolve this matter:

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]
[Phone] | [Email]
Counsel for {creditor}"""
    _save_doc(profile_dir, 'collection_letter', creditor, doc)
    return doc


def generate_litigation_hold(profile_dir: str, case_id: str,
                              recipient_name: str, recipient_title: str,
                              company: str, subject_matter: str) -> str:
    doc = f"""LITIGATION HOLD NOTICE
PRIVILEGED AND CONFIDENTIAL

{_now_str()}

{recipient_name}
{recipient_title}
{company}

Re: LITIGATION HOLD — Preservation of Documents and Electronically Stored Information

Dear {recipient_name}:

This notice is to inform you that {company} anticipates litigation or legal proceedings regarding: {subject_matter} (the "Matter").

IMMEDIATE ACTION REQUIRED

Effective immediately, you are required to preserve all documents and electronically stored information ("ESI") that may be relevant to the Matter. Failure to preserve relevant information may result in sanctions, adverse inference instructions, and other serious legal consequences.

SCOPE OF PRESERVATION

You must immediately preserve ALL documents and ESI related to the Matter, including:

DOCUMENTS TO PRESERVE:
• Emails, text messages, instant messages, and all electronic communications
• Documents, spreadsheets, presentations, and files of any kind
• Voicemails and recorded communications
• Handwritten notes, letters, memoranda, and correspondence
• Contracts, agreements, invoices, and financial records
• Reports, analyses, and studies
• Calendars, meeting minutes, and agendas
• Social media posts and messages
• Databases and application data

SYSTEMS AND LOCATIONS TO PRESERVE:
• Work computers, laptops, tablets, and mobile devices
• Email accounts (work and personal, if used for work matters)
• Cloud storage (Google Drive, Dropbox, OneDrive, etc.)
• Company servers and shared drives
• External hard drives and USB drives
• Text message and social media archives

WHAT YOU MUST DO NOW:

1. STOP deleting any documents, emails, or data that may relate to the Matter.
2. SUSPEND all automatic deletion or overwrite policies for relevant data.
3. PRESERVE all relevant backup media.
4. COLLECT and segregate all hard copy documents related to the Matter.
5. NOTIFY your IT department of this hold immediately.

PROHIBITED ACTIONS:
• Do not delete, destroy, alter, or overwrite any relevant documents or ESI
• Do not run file-cleaning or disk-wiping software on affected systems
• Do not allow automatic email deletion rules to apply to relevant emails

ONGOING OBLIGATION

Your preservation obligations continue until you receive written notice from legal counsel that the litigation hold has been released.

If you have questions about what to preserve or how to comply, contact [Attorney Name] immediately at [phone/email].

Please acknowledge receipt of this notice by signing and returning the attached acknowledgment.

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]

ACKNOWLEDGMENT OF RECEIPT

I, {recipient_name}, acknowledge receipt of this Litigation Hold Notice and understand my obligations to preserve relevant documents and ESI.

Signature: ___________________
Date: ________________________"""
    _save_doc(profile_dir, 'litigation_hold', case_id, doc)
    return doc


def generate_mediation_statement(profile_dir: str, case_id: str,
                                  client_name: str, opposing_party: str,
                                  dispute_summary: str,
                                  settlement_position: str) -> str:
    cases = _load(_path(profile_dir, 'legal_cases.json'))
    case = next((c for c in cases if c['id'] == case_id), None)
    case_type = case.get('case_type', '') if case else ''

    doc = f"""CONFIDENTIAL MEDIATION STATEMENT

Submitted by: {client_name} ("Client")
Opposing Party: {opposing_party}
Case Type: {case_type}
Date: {_now_str()}

SUBMITTED TO MEDIATOR — CONFIDENTIAL

I. INTRODUCTION

This mediation statement is submitted on behalf of {client_name} in connection with the above-referenced dispute. We believe mediation is an appropriate and efficient means to resolve this matter and approach these proceedings in good faith.

II. BACKGROUND AND FACTS

{dispute_summary}

III. LEGAL THEORIES AND CLAIMS

Our client's claims/defenses are supported by:

[Legal Basis 1]: [Description and supporting authority]
[Legal Basis 2]: [Description and supporting authority]
[Legal Basis 3]: [Description and supporting authority]

IV. DAMAGES AND RELIEF SOUGHT

Our client has suffered the following damages:

• Economic damages: $[_______________]
• Non-economic damages: $[_______________]
• Attorney's fees and costs: $[_______________]
• Total claimed: $[_______________]

[Supporting documentation and damage analysis attached as Exhibit A]

V. STRENGTHS AND WEAKNESSES OF THE CASE

Strengths of our position:
• [Key strength #1]
• [Key strength #2]

Areas of risk we acknowledge:
• [Acknowledged weakness/risk #1]

VI. SETTLEMENT POSITION

{settlement_position}

Our client's priority interests in resolving this matter include:
• [Interest #1]
• [Interest #2]

VII. PROPOSED RESOLUTION

We propose the following terms as a basis for settlement:

[Specific settlement terms, payment amounts, non-monetary terms, etc.]

VIII. CONCLUSION

{client_name} is committed to reaching a fair and efficient resolution through mediation. We look forward to productive discussions with the mediator and all parties.

Respectfully submitted,

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]
Attorney for {client_name}

CONFIDENTIAL — FOR MEDIATION PURPOSES ONLY
NOT ADMISSIBLE IN ANY SUBSEQUENT PROCEEDING"""
    _save_doc(profile_dir, 'mediation_statement', case_id, doc)
    return doc


def generate_settlement_demand(profile_dir: str, case_id: str,
                                claimant: str, respondent: str,
                                demand_amount: float,
                                injuries_summary: str,
                                deadline_days: int = 30) -> str:
    cases = _load(_path(profile_dir, 'legal_cases.json'))
    case = next((c for c in cases if c['id'] == case_id), None)
    case_desc = case.get('description', '') if case else ''

    doc = f"""SETTLEMENT DEMAND LETTER
WITHOUT PREJUDICE

{_now_str()}

[Insurance Company / Opposing Counsel]
[Address]

Re: Settlement Demand — {claimant} v. {respondent}
    Your Insured/Client: {respondent}
    Date of Incident: [Date]
    Claim No.: [Claim Number]

Dear Sir or Madam:

This firm represents {claimant} ("Client") in connection with the above-referenced matter. We write to make a formal settlement demand prior to the filing of litigation.

FACTUAL BACKGROUND

{case_desc if case_desc else '[Describe the incident, how it occurred, and who was responsible.]'}

LIABILITY

{respondent} is liable to our client for the following reasons:

1. [Basis for liability — negligence, breach of contract, etc.]
2. [Specific acts or omissions establishing fault]
3. [Any applicable statutes, regulations, or standards violated]

INJURIES AND DAMAGES

As a direct and proximate result of {respondent}'s conduct, our client has suffered:

{injuries_summary}

ITEMIZATION OF DAMAGES

Medical Expenses (past):                    $[_______________]
Medical Expenses (future, estimated):       $[_______________]
Lost Wages/Income:                          $[_______________]
Future Lost Earning Capacity:               $[_______________]
Pain and Suffering:                         $[_______________]
Emotional Distress:                         $[_______________]
Property Damage:                            $[_______________]
Other Damages:                              $[_______________]
                                            ─────────────────
TOTAL DAMAGES:                              $[_______________]

DEMAND

On behalf of {claimant}, we hereby demand the sum of ${demand_amount:,.2f} in full and final settlement of all claims arising from this matter.

This demand is open for acceptance for {deadline_days} days from the date of this letter. If not accepted within this timeframe, we will proceed with formal litigation without further notice and the demand will be withdrawn.

Please respond to this demand in writing. We look forward to resolving this matter efficiently.

Very truly yours,

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]
Attorney for {claimant}

Enclosures: Medical Records | Bills | [Other Supporting Documentation]"""
    _save_doc(profile_dir, 'settlement_demand', case_id, doc)
    return doc


def generate_closing_letter(profile_dir: str, case_id: str,
                             client_name: str, outcome: str) -> str:
    cases = _load(_path(profile_dir, 'legal_cases.json'))
    case = next((c for c in cases if c['id'] == case_id), None)
    case_title = f"{case['client_name']} — {case.get('case_type','')}" if case else 'Your Matter'

    doc = f"""FILE CLOSING LETTER

{_now_str()}

{client_name}
[Address]

Re: Closing of File — {case_title}

Dear {client_name},

This letter confirms that our representation in connection with the above-referenced matter has now concluded.

MATTER OUTCOME

{outcome}

FILE RETENTION

Pursuant to our file retention policy, your file will be retained for [7] years from the date of this letter. After that period, the file may be destroyed without further notice. If you wish to obtain copies of your file prior to that date, please contact our office.

YOUR DOCUMENTS

Enclosed please find [any original documents belonging to you]. We recommend that you retain these records in a safe place.

OUTSTANDING BALANCES

[Your account is paid in full. / Please remit the outstanding balance of $[___] within [30] days.]

FUTURE MATTERS

We hope you have been satisfied with our representation. Should you have future legal needs, we would welcome the opportunity to assist you. Please keep in mind that statutes of limitations may apply to future legal matters, and prompt action is important.

It has been our privilege to represent you in this matter.

Sincerely,

_______________________________
[Attorney Name]
[Bar Number]
[Firm Name]
[Phone] | [Email]"""
    _save_doc(profile_dir, 'closing_letter', case_id, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = _load(_path(profile_dir, 'legal_documents.json'))
    docs.append({'id': _new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': _now_iso()})
    _save(_path(profile_dir, 'legal_documents.json'), docs)
