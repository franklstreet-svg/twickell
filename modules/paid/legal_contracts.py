MODULE_MANIFEST = {
    "id": "I001-contracts",
    "name": "Legal Contract Generator",
    "category": "Legal Pro",
    "description": "Generate contracts and agreements — NDAs, settlement agreements, service agreements, employment contracts, independent contractor agreements, release of claims.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_nda", "generate_settlement_agreement", "generate_service_agreement",
              "generate_employment_contract", "generate_contractor_agreement",
              "generate_release_of_claims"],
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


def generate_nda(profile_dir: str, party_a: str, party_b: str,
                 purpose: str, duration_years: int = 2,
                 mutual: bool = True) -> str:
    nda_type = "MUTUAL NON-DISCLOSURE AGREEMENT" if mutual else "NON-DISCLOSURE AGREEMENT"
    disclosing = f"Either party" if mutual else party_a
    receiving = f"the other party" if mutual else party_b

    doc = f"""{nda_type}

This Non-Disclosure Agreement ("Agreement") is entered into as of {_now_str()} by and between:

Party A: {party_a} ("{"Party A" if not mutual else "Disclosing/Receiving Party"}")
Party B: {party_b} ("{"Party B" if not mutual else "Disclosing/Receiving Party"}")

(collectively, the "Parties")

PURPOSE

The Parties wish to explore a potential business relationship regarding: {purpose} (the "Purpose"). In connection with this Purpose, each Party may disclose confidential information to the other.

1. DEFINITION OF CONFIDENTIAL INFORMATION

"Confidential Information" means any non-public information that {disclosing} designates as confidential or that reasonably should be understood to be confidential given the nature of the information and circumstances of disclosure, including but not limited to: business plans, financial data, technical information, customer lists, trade secrets, and proprietary processes.

2. OBLIGATIONS OF RECEIVING PARTY

{receiving} agrees to:
(a) Hold Confidential Information in strict confidence;
(b) Not disclose Confidential Information to third parties without prior written consent;
(c) Use Confidential Information solely for the Purpose stated herein;
(d) Protect Confidential Information with at least the same degree of care used to protect its own confidential information, but no less than reasonable care;
(e) Limit access to Confidential Information to those employees or representatives with a need to know.

3. EXCLUSIONS

Confidential Information does not include information that:
(a) Is or becomes publicly available through no fault of the receiving party;
(b) Was known to the receiving party prior to disclosure;
(c) Is independently developed by the receiving party without use of Confidential Information;
(d) Is received from a third party without restriction.

4. TERM

This Agreement shall remain in effect for {duration_years} year{"s" if duration_years != 1 else ""} from the date of execution. Obligations regarding trade secrets shall survive indefinitely.

5. RETURN OF INFORMATION

Upon request or termination, the receiving party shall promptly return or destroy all Confidential Information and certify destruction in writing.

6. REMEDIES

The Parties acknowledge that breach of this Agreement may cause irreparable harm and that monetary damages may be inadequate. Either party shall be entitled to seek equitable relief, including injunction, in addition to all other remedies available at law.

7. GOVERNING LAW

This Agreement shall be governed by the laws of [State], without regard to conflict of law principles.

8. ENTIRE AGREEMENT

This Agreement constitutes the entire agreement between the Parties regarding its subject matter and supersedes all prior discussions and agreements.

IN WITNESS WHEREOF, the Parties have executed this Agreement as of the date first written above.

{party_a}                                    {party_b}

Signature: ___________________               Signature: ___________________
Name: _______________________               Name: _______________________
Title: ________________________              Title: ________________________
Date: ________________________              Date: ________________________"""
    _save_doc(profile_dir, 'nda', party_a, doc)
    return doc


def generate_settlement_agreement(profile_dir: str, case_id: str,
                                   plaintiff: str, defendant: str,
                                   settlement_amount: float,
                                   payment_terms: str = 'lump sum within 30 days') -> str:
    cases = _load(_path(profile_dir, 'legal_cases.json'))
    case = next((c for c in cases if c['id'] == case_id), None)
    dispute = case.get('description', '[Description of dispute]') if case else '[Description of dispute]'

    doc = f"""SETTLEMENT AGREEMENT AND RELEASE

This Settlement Agreement and Release ("Agreement") is entered into as of {_now_str()} by and between:

Plaintiff: {plaintiff} ("Releasor")
Defendant: {defendant} ("Releasee")

RECITALS

WHEREAS, a dispute has arisen between the parties regarding: {dispute} (the "Dispute");

WHEREAS, the parties desire to resolve the Dispute without the expense and uncertainty of further litigation;

NOW, THEREFORE, in consideration of the mutual promises and covenants herein, and for other good and valuable consideration, the parties agree as follows:

1. SETTLEMENT PAYMENT

Defendant agrees to pay Plaintiff the total sum of ${settlement_amount:,.2f} (the "Settlement Amount") as follows: {payment_terms}.

2. RELEASE BY PLAINTIFF

In consideration of the Settlement Amount, Plaintiff, on behalf of itself and its heirs, successors, and assigns, hereby fully, finally, and forever releases and discharges Defendant and its officers, directors, employees, agents, successors, and assigns from any and all claims, demands, causes of action, and liabilities, known and unknown, arising out of or related to the Dispute through the date of this Agreement.

3. RELEASE BY DEFENDANT

Defendant hereby fully, finally, and forever releases and discharges Plaintiff from any and all counterclaims, demands, and causes of action arising out of or related to the Dispute.

4. DISMISSAL OF LITIGATION

The parties agree to execute and file a Stipulation of Dismissal with Prejudice within [___] days of receipt of the Settlement Amount.

5. NO ADMISSION OF LIABILITY

This Agreement is entered into solely for the purpose of avoiding the expense and uncertainty of litigation. Nothing herein constitutes an admission of liability or wrongdoing by either party.

6. CONFIDENTIALITY

The parties agree to keep the terms of this Agreement strictly confidential and shall not disclose the terms to any third party without prior written consent of the other, except as required by law.

7. ENTIRE AGREEMENT

This Agreement constitutes the entire agreement between the parties with respect to the Dispute and supersedes all prior negotiations and agreements.

8. GOVERNING LAW

This Agreement shall be governed by the laws of [State].

IN WITNESS WHEREOF, the parties have executed this Agreement as of the date first written above.

PLAINTIFF: {plaintiff}                       DEFENDANT: {defendant}

Signature: ___________________               Signature: ___________________
Name: _______________________               Name: _______________________
Date: ________________________              Date: ________________________

Approved as to form:

________________________________             ________________________________
Attorney for Plaintiff                       Attorney for Defendant"""
    _save_doc(profile_dir, 'settlement_agreement', case_id, doc)
    return doc


def generate_service_agreement(profile_dir: str, client_name: str,
                                service_provider: str, services: str,
                                fee: float, fee_type: str = 'flat fee') -> str:
    doc = f"""PROFESSIONAL SERVICES AGREEMENT

This Professional Services Agreement ("Agreement") is entered into as of {_now_str()} by and between:

Client: {client_name} ("Client")
Service Provider: {service_provider} ("Provider")

1. SERVICES

Provider agrees to provide the following services ("Services"):
{services}

Provider shall perform Services in a professional and workmanlike manner consistent with industry standards.

2. COMPENSATION

Client agrees to pay Provider:
Fee: ${fee:,.2f} ({fee_type})
Payment due: [Net 30 from invoice date / Upon completion / Per schedule attached]
Late payments accrue interest at 1.5% per month.

3. TERM AND TERMINATION

This Agreement commences on {_now_str()} and continues until completion of Services, unless earlier terminated.

Either party may terminate this Agreement with [30] days written notice. Upon termination, Client shall pay for all Services rendered through the termination date.

4. INDEPENDENT CONTRACTOR

Provider is an independent contractor. Nothing herein creates an employer-employee relationship. Provider is responsible for all taxes on compensation received.

5. INTELLECTUAL PROPERTY

All work product, deliverables, and materials created under this Agreement ("Work Product") shall be [Client's exclusive property upon full payment / Provider retains ownership and grants Client a license].

6. CONFIDENTIALITY

Provider agrees to keep all Client information confidential and not to disclose it to third parties without Client's written consent.

7. LIMITATION OF LIABILITY

Provider's total liability shall not exceed the fees paid in the preceding [3] months. Neither party shall be liable for indirect, incidental, or consequential damages.

8. INDEMNIFICATION

Each party shall indemnify and hold harmless the other from claims arising from its own negligence or willful misconduct.

9. DISPUTE RESOLUTION

Any dispute shall first be submitted to mediation. If unresolved, disputes shall be resolved by binding arbitration in [City, State] under [AAA/JAMS] rules.

10. GOVERNING LAW

This Agreement is governed by the laws of [State].

IN WITNESS WHEREOF, the parties have executed this Agreement as of the date written above.

{client_name}                                {service_provider}

Signature: ___________________               Signature: ___________________
Name: _______________________               Name: _______________________
Title: ________________________              Title: ________________________
Date: ________________________              Date: ________________________"""
    _save_doc(profile_dir, 'service_agreement', client_name, doc)
    return doc


def generate_employment_contract(profile_dir: str, employer: str,
                                  employee_name: str, title: str,
                                  salary: float, start_date: str) -> str:
    doc = f"""EMPLOYMENT AGREEMENT

This Employment Agreement ("Agreement") is entered into as of {_now_str()} by and between:

Employer: {employer} ("Company")
Employee: {employee_name} ("Employee")

1. POSITION AND DUTIES

Company hereby employs Employee in the position of {title}. Employee shall perform such duties as are customary for this position and as assigned by Company.

2. START DATE AND AT-WILL EMPLOYMENT

Employee's employment shall commence on {start_date}. Employment is at-will, meaning either party may terminate the employment relationship at any time, with or without cause or notice.

3. COMPENSATION

Base Salary: ${salary:,.2f} per year, payable bi-weekly
Benefits: As set forth in Company's employee benefits plan
Vacation: [PTO policy reference]
This compensation is subject to applicable tax withholdings.

4. WORK SCHEDULE

Employee shall work [full-time / part-time] as needed to fulfill job responsibilities. Standard hours are [Monday–Friday, 9 AM–5 PM] unless otherwise agreed.

5. CONFIDENTIALITY AND NON-DISCLOSURE

Employee agrees to keep all Company information strictly confidential during and after employment. This includes trade secrets, client lists, financial information, and proprietary processes.

6. NON-COMPETE / NON-SOLICITATION

During employment and for [12] months thereafter, Employee agrees not to:
(a) Work for a direct competitor within [geographic area];
(b) Solicit Company's clients or customers;
(c) Solicit or hire Company's employees.

7. INTELLECTUAL PROPERTY

All inventions, discoveries, and work product created by Employee within the scope of employment belong to Company and are considered works made for hire.

8. TERMINATION

Either party may terminate this Agreement at any time. Upon termination, Employee shall return all Company property and confidential information.

9. GOVERNING LAW

This Agreement is governed by the laws of [State]. Any disputes shall be resolved in [City, State].

10. ENTIRE AGREEMENT

This Agreement supersedes all prior agreements between the parties regarding employment.

IN WITNESS WHEREOF, the parties have executed this Agreement as of the date written above.

{employer}                                   Employee

Signature: ___________________               Signature: ___________________
Authorized Signatory                         {employee_name}
Title: ________________________              Date: ________________________
Date: ________________________"""
    _save_doc(profile_dir, 'employment_contract', employee_name, doc)
    return doc


def generate_contractor_agreement(profile_dir: str, client_name: str,
                                   contractor_name: str, scope: str,
                                   rate: float, rate_type: str = 'hourly') -> str:
    doc = f"""INDEPENDENT CONTRACTOR AGREEMENT

This Independent Contractor Agreement ("Agreement") is entered into as of {_now_str()} by and between:

Client: {client_name} ("Client")
Contractor: {contractor_name} ("Contractor")

1. SCOPE OF WORK

Contractor agrees to provide the following services:
{scope}

2. INDEPENDENT CONTRACTOR STATUS

Contractor is an independent contractor and not an employee. Contractor:
(a) Controls the manner and means of performing the work;
(b) Is responsible for all taxes, insurance, and benefits;
(c) May work for other clients simultaneously;
(d) Shall not be entitled to employee benefits.

3. COMPENSATION

Rate: ${rate:,.2f} per {rate_type}
Invoicing: Monthly (or upon milestone completion)
Payment terms: Net 15 from invoice date

4. TERM

This Agreement commences {_now_str()} and continues until completion of the scope of work or until terminated by either party with [14] days written notice.

5. INTELLECTUAL PROPERTY

All deliverables become Client's property upon full payment. Contractor retains ownership of pre-existing tools, frameworks, and methodologies.

6. CONFIDENTIALITY

Contractor shall maintain strict confidentiality of all Client information and shall not use it for any purpose other than performing services under this Agreement.

7. LIMITATION OF LIABILITY

Contractor's liability is limited to the total fees paid in the 3 months preceding the claim.

8. GOVERNING LAW

This Agreement is governed by the laws of [State].

{client_name}                                {contractor_name}

Signature: ___________________               Signature: ___________________
Name: _______________________               Name: _______________________
Date: ________________________              Date: ________________________"""
    _save_doc(profile_dir, 'contractor_agreement', client_name, doc)
    return doc


def generate_release_of_claims(profile_dir: str, releasor: str,
                                releasee: str, consideration: str,
                                claims_description: str) -> str:
    doc = f"""GENERAL RELEASE OF ALL CLAIMS

This General Release ("Release") is entered into as of {_now_str()} by and between:

Releasor: {releasor}
Releasee: {releasee}

RECITALS

In consideration of {consideration}, and other good and valuable consideration, the receipt and sufficiency of which is hereby acknowledged, Releasor agrees as follows:

1. RELEASE

Releasor, on behalf of itself and its heirs, executors, administrators, successors, and assigns, hereby fully and forever releases and discharges Releasee and its officers, directors, employees, agents, successors, assigns, and affiliates from any and all claims, demands, actions, causes of action, suits, debts, liens, liabilities, and obligations of any kind, whether known or unknown, suspected or unsuspected, fixed or contingent, arising from or related to: {claims_description}, through the date of this Release.

2. UNKNOWN CLAIMS

Releasor expressly waives any right or benefit under California Civil Code Section 1542 (or analogous statute), which provides: "A general release does not extend to claims that the creditor or releasing party does not know or suspect to exist in his or her favor at the time of executing the release and that, if known by him or her, would have materially affected his or her settlement with the debtor or released party."

3. NO ADMISSION

This Release does not constitute an admission of liability or wrongdoing by Releasee.

4. REPRESENTATIONS

Releasor represents that it has not assigned or transferred any claims released herein.

5. GOVERNING LAW

This Release is governed by the laws of [State].

RELEASOR ACKNOWLEDGES HAVING READ AND UNDERSTOOD THIS RELEASE AND HAVING HAD OPPORTUNITY TO CONSULT WITH COUNSEL BEFORE SIGNING.

{releasor}

Signature: ___________________
Name: _______________________
Date: ________________________

WITNESS:
Signature: ___________________
Name: _______________________
Date: ________________________"""
    _save_doc(profile_dir, 'release_of_claims', releasor, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = _load(_path(profile_dir, 'legal_documents.json'))
    docs.append({'id': _new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': _now_iso()})
    _save(_path(profile_dir, 'legal_documents.json'), docs)
