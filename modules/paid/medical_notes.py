MODULE_MANIFEST = {
    "id": "I002-notes",
    "name": "Medical Document Generator",
    "category": "Medical Pro",
    "description": "Generate clinical documents — SOAP notes, prior authorization letters, discharge summaries, prescription notes, patient education materials, referral letters.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_soap_note", "generate_prior_auth", "generate_discharge_summary",
              "generate_referral_letter", "generate_patient_summary", "generate_rx_note"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from engine.storage import load, save, now_iso, new_id


def _now_str():
    return datetime.now().strftime('%B %d, %Y')


def generate_soap_note(profile_dir: str, patient_id: str,
                       subjective: str, objective: str,
                       assessment: str, plan: str) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    pt_name = pt['name'] if pt else patient_id

    doc = f"""SOAP NOTE
Date: {_now_str()}
Patient: {pt_name}
Patient ID: {patient_id}
Provider: [Provider Name] | [NPI]

S — SUBJECTIVE
{subjective}

O — OBJECTIVE
{objective}

A — ASSESSMENT
{assessment}

P — PLAN
{plan}

Provider Signature: _______________ Date: _______________
[Credentials] | [License #]"""
    _save_doc(profile_dir, 'soap_note', patient_id, doc)
    return doc


def generate_prior_auth(profile_dir: str, patient_id: str,
                        procedure: str, diagnosis: str, insurance: str) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    pt_name = pt['name'] if pt else patient_id
    dob = pt.get('dob', '[DOB]') if pt else '[DOB]'

    doc = f"""PRIOR AUTHORIZATION REQUEST
Date: {_now_str()}
To: {insurance} — Authorization Department

PATIENT INFORMATION
Name: {pt_name}
DOB: {dob}
Member ID: {pt.get('insurance_id', '[Member ID]') if pt else '[Member ID]'}
Group #: [Group Number]

REQUESTED SERVICE
Procedure/Service: {procedure}
CPT Code(s): [Enter CPT Code]
ICD-10 Diagnosis: {diagnosis}
Expected Service Date: [Date]
Treating Provider: [Provider Name] | NPI: [NPI]
Facility: [Facility Name]

CLINICAL JUSTIFICATION
This procedure is medically necessary for the treatment of {diagnosis}. The patient has failed conservative treatment including [prior treatments]. The requested procedure is the appropriate standard of care per [clinical guidelines].

Supporting documentation attached: Yes / No

Provider Signature: _______________ Date: _______________
Phone: [Phone] | Fax: [Fax]"""
    _save_doc(profile_dir, 'prior_auth', patient_id, doc)
    return doc


def generate_discharge_summary(profile_dir: str, patient_id: str,
                               admission_date: str, diagnosis: str,
                               treatment: str, follow_up: str) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    pt_name = pt['name'] if pt else patient_id

    doc = f"""DISCHARGE SUMMARY
Date of Discharge: {_now_str()}
Patient: {pt_name} | ID: {patient_id}
Date of Admission: {admission_date}

PRINCIPAL DIAGNOSIS
{diagnosis}

HOSPITAL COURSE & TREATMENT
{treatment}

DISCHARGE CONDITION
Stable / Good / Fair / Guarded (circle one)

DISCHARGE INSTRUCTIONS
- Follow up with your primary care provider within 7 days
- {follow_up}
- Return to ED if: fever >101°F, worsening symptoms, or new concerns

MEDICATIONS AT DISCHARGE
[List medications, doses, frequency]

FOLLOW-UP APPOINTMENTS
{follow_up}

Attending Physician: _______________ Date: _______________
[Credentials] | [License #]"""
    _save_doc(profile_dir, 'discharge_summary', patient_id, doc)
    return doc


def generate_referral_letter(profile_dir: str, patient_id: str,
                             referred_to: str, specialty: str, reason: str) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    pt_name = pt['name'] if pt else patient_id

    doc = f"""REFERRAL LETTER
Date: {_now_str()}

To: {referred_to}
Specialty: {specialty}

RE: Patient Referral — {pt_name}

Dear {referred_to},

I am referring {pt_name} to your practice for evaluation and management of the following:

REASON FOR REFERRAL
{reason}

RELEVANT HISTORY
{pt.get('history', '[No prior history on file]') if pt else '[No prior history on file]'}

Please feel free to contact our office with any questions. We appreciate your assistance in the care of this patient and look forward to your consultation report.

Sincerely,
[Referring Provider Name]
[Practice Name]
[Phone] | [Fax] | [NPI]"""
    _save_doc(profile_dir, 'referral_letter', patient_id, doc)
    return doc


def generate_patient_summary(profile_dir: str, patient_id: str) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    if not pt:
        return f"Patient {patient_id} not found."

    appts = [a for a in load(profile_dir, 'medical_appointments') if a.get('patient_id') == patient_id]
    rxs   = [r for r in load(profile_dir, 'medical_prescriptions') if r.get('patient_id') == patient_id]
    notes = [n for n in load(profile_dir, 'medical_treatment_notes') if n.get('patient_id') == patient_id]

    doc = f"""PATIENT SUMMARY
Generated: {_now_str()}
Patient: {pt['name']} | ID: {pt['id']}
DOB: {pt.get('dob','N/A')} | Phone: {pt.get('phone','N/A')}

APPOINTMENTS ({len(appts)}):
"""
    for a in appts[-5:]:
        doc += f"  {a.get('date','?')} — {a.get('reason','?')} [{a.get('status','scheduled')}]\n"

    doc += f"\nACTIVE PRESCRIPTIONS ({len(rxs)}):\n"
    for r in rxs:
        if not r.get('discontinued'):
            doc += f"  {r.get('medication','?')} {r.get('dosage','?')} — {r.get('frequency','?')}\n"

    doc += f"\nRECENT NOTES ({min(3,len(notes))}):\n"
    for n in notes[-3:]:
        doc += f"  [{n.get('date','?')}] {n.get('note','')[:100]}\n"

    _save_doc(profile_dir, 'patient_summary', patient_id, doc)
    return doc


def generate_rx_note(profile_dir: str, patient_id: str,
                     medication: str, dosage: str, frequency: str,
                     indication: str, refills: int = 0) -> str:
    patients = load(profile_dir, 'medical_patients')
    pt = next((p for p in patients if p['id'] == patient_id), None)
    pt_name = pt['name'] if pt else patient_id

    doc = f"""PRESCRIPTION NOTE
Date: {_now_str()}
Patient: {pt_name} | ID: {patient_id}

MEDICATION: {medication}
DOSAGE: {dosage}
FREQUENCY: {frequency}
INDICATION: {indication}
REFILLS: {refills}
DAW: No substitution required unless generic available

Prescriber: _______________
DEA #: _______________
NPI: _______________
License #: _______________
Signature: _______________ Date: _______________"""
    _save_doc(profile_dir, 'rx_note', patient_id, doc)
    return doc


def _save_doc(profile_dir, doc_type, ref_id, content):
    docs = load(profile_dir, 'medical_documents')
    docs.append({'id': new_id(), 'type': doc_type, 'ref': ref_id,
                 'content': content, 'created': now_iso()})
    save(profile_dir, 'medical_documents', docs)


def get_documents(profile_dir: str, doc_type: str = None) -> list:
    docs = load(profile_dir, 'medical_documents')
    if doc_type:
        docs = [d for d in docs if d.get('type') == doc_type]
    return docs
