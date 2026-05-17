MODULE_MANIFEST = {
    "id": "I001-motions",
    "name": "Legal Motions & Pleadings Generator",
    "category": "Legal Pro",
    "description": "Generate court-ready pleadings — civil complaints, answers, motions to dismiss, motions for summary judgment, discovery requests (interrogatories, RFPs, RFAs).",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_complaint", "generate_answer", "generate_motion_to_dismiss",
              "generate_motion_summary_judgment", "generate_interrogatories",
              "generate_rfp", "generate_notice_of_appearance"],
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

def _get_case(profile_dir, case_id):
    cases = _load(_path(profile_dir, 'legal_cases.json'))
    return next((c for c in cases if c['id'] == case_id), None)


def generate_complaint(profile_dir: str, case_id: str,
                       court: str, plaintiff: str, defendant: str,
                       causes_of_action: list, jury_demand: bool = True) -> str:
    case = _get_case(profile_dir, case_id)
    case_desc = case.get('description', '') if case else ''
    case_no = f"[Case No. _______________]"
    causes = '\n\n'.join(
        f"COUNT {i+1}: {c}\n{case_desc}"
        for i, c in enumerate(causes_of_action)
    ) if causes_of_action else f"COUNT I: [Cause of Action]\n{case_desc}"

    doc = f"""IN THE {court.upper()}

{plaintiff.upper()},
    Plaintiff,

v.                                          {case_no}

{defendant.upper()},
    Defendant.

COMPLAINT{"AND DEMAND FOR JURY TRIAL" if jury_demand else ""}

Plaintiff {plaintiff} ("Plaintiff"), by and through undersigned counsel, hereby files this Complaint against Defendant {defendant} ("Defendant"), and alleges as follows:

PARTIES

1. Plaintiff {plaintiff} is [individual/entity description].

2. Defendant {defendant} is [individual/entity description].

JURISDICTION AND VENUE

3. This Court has jurisdiction pursuant to [jurisdictional basis].

4. Venue is proper in this district pursuant to [venue statute].

FACTUAL ALLEGATIONS

5. {case_desc if case_desc else '[Set forth factual background of the dispute in numbered paragraphs.]'}

6. [Continue factual allegations...]

CAUSES OF ACTION

{causes}

PRAYER FOR RELIEF

WHEREFORE, Plaintiff respectfully requests that this Court enter judgment in Plaintiff's favor and against Defendant as follows:

(a) Compensatory damages in an amount to be proven at trial;
(b) Consequential damages;
(c) Pre- and post-judgment interest;
(d) Attorney's fees and costs to the extent permitted by law;
(e) Such other and further relief as this Court deems just and proper.

{"PLAINTIFF HEREBY DEMANDS A TRIAL BY JURY ON ALL ISSUES SO TRIABLE." if jury_demand else ""}

Respectfully submitted,

Date: {_now_str()}

_______________________________
[Attorney Name], [Bar Number]
[Firm Name]
[Address]
[Phone] | [Email]
Attorney for Plaintiff {plaintiff}"""
    _save_doc(profile_dir, 'complaint', case_id, doc)
    return doc


def generate_answer(profile_dir: str, case_id: str,
                    court: str, plaintiff: str, defendant: str,
                    affirmative_defenses: list = None) -> str:
    case = _get_case(profile_dir, case_id)
    defenses = affirmative_defenses or [
        'Failure to state a claim upon which relief can be granted',
        'Statute of limitations',
        'Waiver and/or estoppel',
        'Contributory/comparative negligence',
        'Failure to mitigate damages',
    ]
    defenses_text = '\n\n'.join(
        f"AFFIRMATIVE DEFENSE NO. {i+1}: {d}\n\nDefendant realleges and incorporates all prior paragraphs. {d}."
        for i, d in enumerate(defenses)
    )

    doc = f"""IN THE {court.upper()}

{plaintiff.upper()},
    Plaintiff,

v.                                          [Case No. _______________]

{defendant.upper()},
    Defendant.

DEFENDANT'S ANSWER TO COMPLAINT

Defendant {defendant} ("Defendant"), by and through undersigned counsel, hereby answers Plaintiff's Complaint as follows:

GENERAL DENIAL

Defendant denies each and every allegation in Plaintiff's Complaint not specifically admitted herein.

RESPONSES TO SPECIFIC ALLEGATIONS

1. [Admit / Deny / Defendant lacks sufficient knowledge or information to form a belief as to the truth of the allegations in Paragraph 1.]

2. [Admit / Deny / Defendant lacks sufficient knowledge...]

[Continue responding to each numbered paragraph in the Complaint...]

AFFIRMATIVE DEFENSES

{defenses_text}

WHEREFORE, Defendant respectfully requests that this Court:

(a) Dismiss Plaintiff's Complaint with prejudice;
(b) Enter judgment in favor of Defendant;
(c) Award Defendant attorney's fees and costs; and
(d) Grant such other relief as the Court deems just and proper.

Date: {_now_str()}

_______________________________
[Attorney Name], [Bar Number]
[Firm Name]
Attorney for Defendant {defendant}"""
    _save_doc(profile_dir, 'answer', case_id, doc)
    return doc


def generate_motion_to_dismiss(profile_dir: str, case_id: str,
                               court: str, plaintiff: str, defendant: str,
                               grounds: str = 'failure to state a claim') -> str:
    case = _get_case(profile_dir, case_id)

    doc = f"""IN THE {court.upper()}

{plaintiff.upper()},
    Plaintiff,

v.                                          [Case No. _______________]

{defendant.upper()},
    Defendant.

DEFENDANT'S MOTION TO DISMISS
PURSUANT TO RULE 12(b)(6)

Defendant {defendant} ("Defendant"), by and through undersigned counsel, respectfully moves this Court to dismiss Plaintiff's Complaint pursuant to Federal Rule of Civil Procedure 12(b)(6) [or applicable state rule] on the grounds that: {grounds}.

MEMORANDUM OF LAW IN SUPPORT

I. INTRODUCTION

Plaintiff {plaintiff} has filed a Complaint that fails as a matter of law. The Complaint should be dismissed because {grounds}.

II. STANDARD OF REVIEW

To survive a motion to dismiss under Rule 12(b)(6), a complaint must contain sufficient factual matter, accepted as true, to "state a claim to relief that is plausible on its face." Bell Atlantic Corp. v. Twombly, 550 U.S. 544, 570 (2007); Ashcroft v. Iqbal, 556 U.S. 662 (2009). Threadbare recitals of the elements of a cause of action, supported by mere conclusory statements, do not suffice.

III. ARGUMENT

A. The Complaint Fails to State a Claim

{case.get('description', '[Argument: Plaintiff has failed to allege sufficient facts to state a plausible claim for relief. Specifically, the Complaint lacks allegations supporting the essential elements of each cause of action.]') if case else '[Legal argument specific to grounds for dismissal]'}

B. [Additional Argument Heading if Applicable]

[Develop additional grounds for dismissal with supporting case law and statutory authority.]

IV. CONCLUSION

For the foregoing reasons, Defendant respectfully requests that this Court grant this Motion to Dismiss and dismiss Plaintiff's Complaint with prejudice, with an award of costs and attorney's fees as permitted by law.

Date: {_now_str()}

Respectfully submitted,

_______________________________
[Attorney Name], [Bar Number]
[Firm Name]
Attorney for Defendant {defendant}"""
    _save_doc(profile_dir, 'motion_to_dismiss', case_id, doc)
    return doc


def generate_motion_summary_judgment(profile_dir: str, case_id: str,
                                     court: str, moving_party: str,
                                     opposing_party: str, argument: str) -> str:
    doc = f"""IN THE {court.upper()}

{moving_party.upper()},
    Plaintiff/Moving Party,

v.                                          [Case No. _______________]

{opposing_party.upper()},
    Defendant/Opposing Party.

MOTION FOR SUMMARY JUDGMENT

{moving_party} ("Moving Party") respectfully moves for summary judgment pursuant to Rule 56 of the Federal Rules of Civil Procedure [or applicable state rule] on the grounds that there is no genuine dispute as to any material fact and Moving Party is entitled to judgment as a matter of law.

MEMORANDUM OF LAW IN SUPPORT

I. INTRODUCTION AND SUMMARY OF ARGUMENT

{argument}

II. STATEMENT OF UNDISPUTED MATERIAL FACTS

1. [State each undisputed material fact with citation to evidence in the record.]
2. [Continue numbered facts...]

III. STANDARD OF REVIEW

Summary judgment is appropriate when "there is no genuine dispute as to any material fact and the movant is entitled to judgment as a matter of law." Fed. R. Civ. P. 56(a). A dispute is "genuine" only if a reasonable jury could return a verdict for the nonmoving party. Anderson v. Liberty Lobby, Inc., 477 U.S. 242, 248 (1986).

IV. ARGUMENT

A. Moving Party Is Entitled to Summary Judgment

{argument}

B. [Additional argument sections with supporting authority...]

V. CONCLUSION

For the foregoing reasons, Moving Party respectfully requests that the Court grant summary judgment in its favor on all claims and defenses.

Date: {_now_str()}

_______________________________
[Attorney Name], [Bar Number]
[Firm Name]
Attorney for {moving_party}"""
    _save_doc(profile_dir, 'motion_summary_judgment', case_id, doc)
    return doc


def generate_interrogatories(profile_dir: str, case_id: str,
                             propounding_party: str, responding_party: str,
                             set_number: int = 1) -> str:
    doc = f"""IN THE [COURT NAME]

[Caption — Plaintiff v. Defendant]                [Case No. _______________]

{propounding_party.upper()}'S INTERROGATORIES TO {responding_party.upper()},
SET {set_number}

Propounding Party: {propounding_party}
Responding Party: {responding_party}
Set Number: {set_number}

INSTRUCTIONS

1. You are required to answer each interrogatory separately and fully in writing, under oath, within 30 days of service.
2. If you object to any interrogatory, state the specific grounds for objection.
3. If you cannot answer fully, answer to the extent possible, specifying your inability and the reasons therefor.
4. These interrogatories are deemed continuing and require supplemental responses if additional information becomes available.

DEFINITIONS

"You" and "Your" refer to {responding_party} and any agents, representatives, or persons acting on your behalf.
"Document" means any writing, recording, or tangible thing as defined by Federal Rule of Evidence 1001.
"Communication" means any oral, written, or electronic exchange of information.
"Identify" when referring to a person means to state full name, current address, telephone number, and relationship to the parties.

INTERROGATORIES

INTERROGATORY NO. 1:
Identify all persons with knowledge of any facts relevant to the claims or defenses in this action.

INTERROGATORY NO. 2:
Identify all documents you relied upon or consulted in responding to these interrogatories.

INTERROGATORY NO. 3:
Describe in detail the facts and circumstances giving rise to your claims/defenses in this matter.

INTERROGATORY NO. 4:
Identify all persons you expect to call as witnesses at trial and state the subject matter of their anticipated testimony.

INTERROGATORY NO. 5:
Identify all expert witnesses you intend to call and describe the subject of their expected testimony and opinions.

INTERROGATORY NO. 6:
Describe any and all damages you claim to have suffered as a result of the events alleged in this action, including the method and basis for calculating such damages.

INTERROGATORY NO. 7:
Identify all communications between you and [opposing party] from [date range] to present that relate to the subject matter of this litigation.

INTERROGATORY NO. 8:
Have you or anyone acting on your behalf conducted any surveillance, investigation, or background check related to this matter? If so, describe in detail.

[Add additional interrogatories specific to the case...]

Date: {_now_str()}

_______________________________
[Attorney Name], [Bar Number]
Attorney for {propounding_party}"""
    _save_doc(profile_dir, 'interrogatories', case_id, doc)
    return doc


def generate_rfp(profile_dir: str, case_id: str,
                 propounding_party: str, responding_party: str,
                 set_number: int = 1) -> str:
    doc = f"""IN THE [COURT NAME]

[Caption]                                         [Case No. _______________]

{propounding_party.upper()}'S REQUESTS FOR PRODUCTION OF DOCUMENTS
TO {responding_party.upper()}, SET {set_number}

INSTRUCTIONS

You are required to produce the documents requested below within 30 days of service. Documents shall be produced as kept in the usual course of business or organized and labeled to correspond to the categories in the request. If you claim privilege or protection for any document, provide a privilege log.

DEFINITIONS

"Document" includes all writings, drawings, graphs, charts, photographs, sound recordings, images, and other data compilations in any medium.
"Electronically Stored Information" (ESI) includes email, text messages, databases, spreadsheets, and any electronically maintained data.

REQUESTS FOR PRODUCTION

REQUEST NO. 1:
All documents relating to or referenced in your responses to Interrogatories in this action.

REQUEST NO. 2:
All communications between you and [opposing party] concerning the subject matter of this litigation.

REQUEST NO. 3:
All contracts, agreements, or written understandings between you and [opposing party].

REQUEST NO. 4:
All financial records, invoices, statements, or documents relating to any damages claimed or at issue in this action.

REQUEST NO. 5:
All documents reviewed, relied upon, or consulted in connection with the preparation of your pleadings and discovery responses.

REQUEST NO. 6:
All documents, reports, or communications prepared by any expert witness you intend to call at trial.

REQUEST NO. 7:
All photographs, videos, or other recordings of any persons, locations, or events at issue in this litigation.

REQUEST NO. 8:
All insurance policies, declarations pages, or coverage documents potentially applicable to any claim or defense in this action.

REQUEST NO. 9:
All documents relating to any prior disputes, claims, or litigation between you and [opposing party].

REQUEST NO. 10:
All text messages, emails, or social media communications relevant to the facts alleged in the Complaint.

[Add additional requests specific to case type...]

Date: {_now_str()}

_______________________________
[Attorney Name], [Bar Number]
Attorney for {propounding_party}"""
    _save_doc(profile_dir, 'rfp', case_id, doc)
    return doc


def generate_notice_of_appearance(profile_dir: str, case_id: str,
                                   court: str, party_represented: str,
                                   attorney_name: str = '[Attorney Name]') -> str:
    doc = f"""IN THE {court.upper()}

[Caption]                                         [Case No. _______________]

NOTICE OF APPEARANCE

Please take notice that {attorney_name}, of [Firm Name], hereby appears as counsel of record for {party_represented} in the above-captioned matter. All future pleadings, notices, orders, and correspondence should be directed to:

{attorney_name}
[Firm Name]
[Address]
[City, State, ZIP]
Phone: [Phone Number]
Fax: [Fax Number]
Email: [Email Address]
Bar No.: [Bar Number]

Date: {_now_str()}

_______________________________
{attorney_name}
[Bar Number]
[Firm Name]
Attorney for {party_represented}"""
    _save_doc(profile_dir, 'notice_of_appearance', case_id, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = _load(_path(profile_dir, 'legal_documents.json'))
    docs.append({'id': _new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': _now_iso()})
    _save(_path(profile_dir, 'legal_documents.json'), docs)
