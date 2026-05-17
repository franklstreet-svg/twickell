MODULE_MANIFEST = {
    "id": "I011-notes",
    "name": "Therapy Document Generator",
    "category": "Therapy & Counseling Pro",
    "description": "Generate clinical documentation — SOAP/DAP session notes, treatment plans, progress notes, discharge summaries, referral letters, insurance billing notes.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_session_note", "generate_treatment_plan_doc", "generate_progress_note",
              "generate_discharge_note", "generate_referral", "generate_billing_note"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from engine.storage import load, save, now_iso, new_id


def _now_str():
    return datetime.now().strftime('%B %d, %Y')


def generate_session_note(profile_dir: str, client_id: str,
                          subjective: str, objective: str,
                          assessment: str, plan: str,
                          note_format: str = 'SOAP') -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    if note_format.upper() == 'DAP':
        doc = f"""SESSION NOTE (DAP Format)
Date: {_now_str()}
Client: {name} | ID: {client_id}
Session #: [Session Number]
Duration: [Minutes] minutes
Modality: Individual / Group / Family (circle)

D — DATA (What the client reported and observed behaviors)
{subjective}

Behavioral observations: {objective}

A — ASSESSMENT (Clinician's interpretation)
{assessment}

P — PLAN (Next steps and interventions)
{plan}

Clinician Signature: _______________ Date: _______________
License: _______________ | Credential: _______________"""
    else:
        doc = f"""SESSION NOTE (SOAP Format)
Date: {_now_str()}
Client: {name} | ID: {client_id}
Session #: [Session Number]
Duration: [Minutes] minutes

S — SUBJECTIVE (Client's self-report)
{subjective}

O — OBJECTIVE (Clinician's observations)
{objective}

A — ASSESSMENT (Clinical interpretation)
{assessment}

P — PLAN (Interventions and next session goals)
{plan}

Clinician Signature: _______________ Date: _______________
License: _______________ | Credential: _______________"""

    _save_doc(profile_dir, 'session_note', client_id, doc)
    return doc


def generate_treatment_plan_doc(profile_dir: str, client_id: str,
                                presenting_problem: str, goals: list,
                                interventions: str, frequency: str = 'Weekly') -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    goals_text = '\n'.join(f"  Goal {i+1}: {g}" for i, g in enumerate(goals)) if goals else '  [Goals to be determined]'

    doc = f"""TREATMENT PLAN
Date: {_now_str()}
Client: {name} | ID: {client_id}
Clinician: [Clinician Name] | License: _______________

PRESENTING PROBLEM
{presenting_problem}

DIAGNOSIS
Axis I: _______________
DSM-5 Code: _______________

TREATMENT GOALS & OBJECTIVES
{goals_text}

INTERVENTIONS & MODALITIES
{interventions}

SESSION FREQUENCY: {frequency}
ESTIMATED TREATMENT DURATION: [Duration]

STRENGTHS & RESOURCES
Client strengths identified: _______________

SAFETY PLAN
Suicidal ideation: Yes / No
Homicidal ideation: Yes / No
Safety plan in place: Yes / No

Client Signature: _______________ Date: _______________
Clinician Signature: _______________ Date: _______________
Next Review Date: _______________"""
    _save_doc(profile_dir, 'treatment_plan', client_id, doc)
    return doc


def generate_progress_note(profile_dir: str, client_id: str,
                           progress: str, barriers: str, next_steps: str) -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    doc = f"""PROGRESS NOTE
Date: {_now_str()}
Client: {name} | ID: {client_id}

PROGRESS TOWARD TREATMENT GOALS
{progress}

BARRIERS TO PROGRESS
{barriers}

NEXT STEPS & INTERVENTIONS
{next_steps}

RISK ASSESSMENT
Current suicidal/homicidal ideation: None / Passive / Active (circle)
Safety plan reviewed: Yes / No

Clinician: _______________ Date: _______________"""
    _save_doc(profile_dir, 'progress_note', client_id, doc)
    return doc


def generate_discharge_note(profile_dir: str, client_id: str,
                            reason: str, summary: str, aftercare: str) -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    sessions = load(profile_dir, 'therapy_sessions')
    client_sessions = [s for s in sessions if s.get('client_id') == client_id]

    doc = f"""DISCHARGE SUMMARY
Date: {_now_str()}
Client: {name} | ID: {client_id}
Total Sessions: {len(client_sessions)}

REASON FOR DISCHARGE
{reason}

TREATMENT SUMMARY & PROGRESS
{summary}

GOALS ACHIEVED
[List goals met during treatment]

AFTERCARE PLAN & RECOMMENDATIONS
{aftercare}

REFERRALS PROVIDED
[List any referrals to other providers]

Client Signature: _______________ Date: _______________
Clinician Signature: _______________ Date: _______________"""
    _save_doc(profile_dir, 'discharge_note', client_id, doc)
    return doc


def generate_referral(profile_dir: str, client_id: str,
                      referred_to: str, reason: str) -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    doc = f"""REFERRAL LETTER
Date: {_now_str()}

To: {referred_to}

RE: Referral for {name}

I am referring the above-named client to your practice for {reason}.

This client has been seen at our practice for [Duration]. They present with [Brief clinical summary]. I believe specialized services from your practice would be beneficial at this time.

Please feel free to contact me with any questions. I look forward to coordinating care.

With permission, relevant clinical records can be forwarded upon receipt of a signed release.

Sincerely,
[Clinician Name]
[License] | [Credential]
[Practice Name]
[Phone] | [Email]"""
    _save_doc(profile_dir, 'referral', client_id, doc)
    return doc


def generate_billing_note(profile_dir: str, client_id: str,
                          cpt_code: str, dx_code: str,
                          session_date: str, duration_mins: int = 50) -> str:
    clients = load(profile_dir, 'therapy_clients')
    client = next((c for c in clients if c['id'] == client_id), None)
    name = client['name'] if client else client_id

    doc = f"""BILLING / SUPERBILL
Date of Service: {session_date}
Generated: {_now_str()}
Client: {name} | ID: {client_id}

BILLING CODES
CPT Code: {cpt_code}
ICD-10 Diagnosis: {dx_code}
Session Duration: {duration_mins} minutes
Place of Service: Office (11) / Telehealth (02)

PROVIDER INFORMATION
Provider Name: _______________
NPI: _______________
Tax ID: _______________
License: _______________

INSURANCE
Insurance Company: _______________
Member ID: _______________
Group #: _______________

Fee Charged: $_______________
Amount Collected: $_______________
Balance Due: $_______________

Provider Signature: _______________ Date: _______________"""
    _save_doc(profile_dir, 'billing_note', client_id, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = load(profile_dir, 'therapy_documents')
    docs.append({'id': new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': now_iso()})
    save(profile_dir, 'therapy_documents', docs)


def get_documents(profile_dir: str, doc_type: str = None) -> list:
    docs = load(profile_dir, 'therapy_documents')
    if doc_type:
        docs = [d for d in docs if d.get('type') == doc_type]
    return docs
