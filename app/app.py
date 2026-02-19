from __future__ import annotations

import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import stripe
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pypdf import PdfReader

APP_ROOT = Path(__file__).resolve().parents[1]  # TWICKELL_ROOT
UPLOADS_DIR = APP_ROOT / "uploads"
JOBS_DIR = APP_ROOT / "jobs"

# Twickell-owned deterministic bridge handoff queue
BRIDGE_QUEUE_DIR = APP_ROOT / "bridge_queue"
BRIDGE_REQ_DIR = BRIDGE_QUEUE_DIR / "requests"
BRIDGE_RES_DIR = BRIDGE_QUEUE_DIR / "results"

# One-time free sample ledger per email (until real auth exists)
LEDGER_DIR = APP_ROOT / "ledger"
FREE_SAMPLE_LEDGER = LEDGER_DIR / "free_sample_by_email.json"

MAX_PAGES = 200
FREE_SAMPLE_MAX_PAGES = 5

PRICE_BLOCK_PAGES = 25
PRICE_PER_BLOCK = 25

DEFAULT_RETENTION_DAYS = 7
# 0 = delete 1 hour after done
ALLOWED_RETENTION_DAYS = {0, 7, 30}

CAP_REJECT_MSG = (
    "Upload rejected: hard cap is 200 pages per upload. "
    "Please split your PDF into 200-page batches and upload each batch separately."
)


def now_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_job_id() -> str:
    return "job_" + uuid.uuid4().hex[:16]


def ensure_dirs() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    BRIDGE_REQ_DIR.mkdir(parents=True, exist_ok=True)
    BRIDGE_RES_DIR.mkdir(parents=True, exist_ok=True)
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)


def calc_base_amount(pages: int) -> int:
    # Base ladder ALWAYS starts at 1–25 = $25, 26–50 = $50, ...
    blocks = int(math.ceil(pages / PRICE_BLOCK_PAGES))
    return blocks * PRICE_PER_BLOCK


def count_pdf_pages(pdf_path: Path) -> int:
    reader = PdfReader(str(pdf_path))
    return len(reader.pages)


def write_job(job: Dict[str, Any]) -> None:
    ensure_dirs()
    (JOBS_DIR / f"{job['id']}.json").write_text(json.dumps(job, indent=2), encoding="utf-8")


def read_job(job_id: str) -> Dict[str, Any]:
    p = JOBS_DIR / f"{job_id}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="job_not_found")
    return json.loads(p.read_text(encoding="utf-8"))


def update_job(job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    job = read_job(job_id)
    job.update(patch)
    job["updated_at"] = now_ts()
    write_job(job)
    return job


def read_ledger() -> Dict[str, Any]:
    ensure_dirs()
    if not FREE_SAMPLE_LEDGER.exists():
        return {"version": 1, "used": {}}
    try:
        return json.loads(FREE_SAMPLE_LEDGER.read_text(encoding="utf-8"))
    except Exception:
        # fail closed: no free samples if ledger is corrupted
        return {"version": 1, "used": {"__CORRUPTED__": now_ts()}}


def write_ledger(obj: Dict[str, Any]) -> None:
    ensure_dirs()
    FREE_SAMPLE_LEDGER.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def email_can_use_free_sample(email_norm: str) -> bool:
    led = read_ledger()
    used = led.get("used", {})
    return email_norm not in used


def mark_email_used_free_sample(email_norm: str, job_id: str) -> None:
    led = read_ledger()
    used = led.setdefault("used", {})
    used[email_norm] = {"job_id": job_id, "ts": now_ts()}
    write_ledger(led)


def write_bridge_request(job: Dict[str, Any]) -> Path:
    ensure_dirs()
    req = {
        "kind": "twickell_pdf_to_xlsx",
        "job_id": job["id"],
        "created_at": now_ts(),
        "input_pdf": job["paths"]["input_pdf"],
        "output_dir": str((APP_ROOT / "outputs" / job["id"]).resolve()),
        "notes": {
            "pages": job.get("pages"),
            "free_sample_applied": job.get("free_sample_applied", False),
            "retention_days": job.get("retention_days", DEFAULT_RETENTION_DAYS),
        },
    }
    req_path = BRIDGE_REQ_DIR / f"{job['id']}.json"
    req_path.write_text(json.dumps(req, indent=2), encoding="utf-8")
    return req_path


# ---- Stripe config ----
load_dotenv()
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8111")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

app = FastAPI(title="Twickell PDF → XLSX Service")


class CreateCheckoutRequest(BaseModel):
    job_id: str


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "twickell", "ts": now_ts()}


@app.get("/bridge/health")
def bridge_health() -> Dict[str, Any]:
    ensure_dirs()
    req_count = len(list(BRIDGE_REQ_DIR.glob("*.json")))
    res_count = len(list(BRIDGE_RES_DIR.glob("*.json")))
    return {
        "ok": True,
        "queue": {
            "requests_dir": str(BRIDGE_REQ_DIR),
            "results_dir": str(BRIDGE_RES_DIR),
            "requests_count": req_count,
            "results_count": res_count,
        },
        "ts": now_ts(),
    }


@app.post("/quote")
async def quote(payload: Dict[str, Any]) -> Dict[str, Any]:
    pages = int(payload.get("pages", 0))
    if pages <= 0:
        raise HTTPException(status_code=400, detail="pages_required")
    if pages > MAX_PAGES:
        return {"ok": False, "reject": True, "reason": CAP_REJECT_MSG}

    base_amount = calc_base_amount(pages)
    return {
        "ok": True,
        "pages": pages,
        "base_amount_due": base_amount,
        "pricing": {"block_pages": PRICE_BLOCK_PAGES, "price_per_block": PRICE_PER_BLOCK},
        "free_sample": f"One-time promo per email for <= {FREE_SAMPLE_MAX_PAGES} pages",
        "retention_options_days": sorted(list(ALLOWED_RETENTION_DAYS)),
        "default_retention_days": DEFAULT_RETENTION_DAYS,
        "retention_note": "0 means delete 1 hour after job is done",
    }


@app.post("/upload")
async def upload_pdf(
    file: UploadFile = File(...),
    email: str = Form(...),
    retention_days: Optional[int] = Form(DEFAULT_RETENTION_DAYS),
) -> Dict[str, Any]:
    """
    Multipart upload:
      - requires email (identity for one-time free sample until real login exists)
      - retention_days: 0(delete 1 hour after done), 7(default), 30
    """
    ensure_dirs()

    email_norm = normalize_email(email)
    if not email_norm or "@" not in email_norm:
        raise HTTPException(status_code=400, detail="valid_email_required")

    try:
        retention_days_i = int(retention_days) if retention_days is not None else DEFAULT_RETENTION_DAYS
    except Exception:
        retention_days_i = DEFAULT_RETENTION_DAYS

    if retention_days_i not in ALLOWED_RETENTION_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"retention_days_must_be_one_of_{sorted(list(ALLOWED_RETENTION_DAYS))}",
        )

    filename = (file.filename or "").lower()
    if not filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="only_pdf_allowed")

    job_id = new_job_id()
    job_upload_dir = UPLOADS_DIR / job_id
    job_upload_dir.mkdir(parents=True, exist_ok=True)
    input_pdf_path = job_upload_dir / "input.pdf"

    # Save upload to disk
    try:
        with input_pdf_path.open("wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"upload_save_failed: {e!s}")

    # Count pages
    try:
        pages = count_pdf_pages(input_pdf_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid_pdf: {e!s}")

    # Enforce hard cap
    if pages > MAX_PAGES:
        try:
            input_pdf_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=413, detail=CAP_REJECT_MSG)

    # Base amount ALWAYS uses 1–25=$25 ladder
    base_amount = calc_base_amount(pages)

    # One-time free sample override (<=5 pages) IF email has not used it before
    free_sample_eligible = (pages <= FREE_SAMPLE_MAX_PAGES) and email_can_use_free_sample(email_norm)
    free_sample_applied = bool(free_sample_eligible)

    amount_due = 0 if free_sample_applied else base_amount

    status = "uploaded" if amount_due == 0 else "awaiting_payment"
    paid = True if amount_due == 0 else False

    job = {
        "id": job_id,
        "created_at": now_ts(),
        "updated_at": now_ts(),
        "status": status,
        "email": email_norm,
        "pages": pages,
        "base_amount_due": base_amount,
        "amount_due": amount_due,
        "paid": paid,
        "free_sample_applied": free_sample_applied,
        "retention_days": retention_days_i,
        # worker sets done_at when finished
        "done_at": None,
        "paths": {
            "input_pdf": str(input_pdf_path),
            "upload_dir": str(job_upload_dir),
        },
    }
    write_job(job)

    if free_sample_applied:
        mark_email_used_free_sample(email_norm, job_id)

    return {
        "ok": True,
        "job_id": job_id,
        "pages": pages,
        "email": email_norm,
        "base_amount_due": base_amount,
        "free_sample_applied": free_sample_applied,
        "amount_due": amount_due,
        "retention_days": retention_days_i,
        "retention_note": ("delete 1 hour after done" if retention_days_i == 0 else f"keep for {retention_days_i} days"),
        "next": {"mark_paid": f"/jobs/{job_id}/mark_paid", "start": f"/jobs/{job_id}/start"},
    }


# ---- Payments (Stripe Checkout) ----
@app.post("/payments/create_session")
async def payments_create_session(req: CreateCheckoutRequest) -> Dict[str, Any]:
    if not stripe.api_key:
        raise HTTPException(status_code=500, detail="stripe_not_configured")

    job = read_job(req.job_id)

    # If free sample / already paid, no checkout needed
    if int(job.get("amount_due", 0)) <= 0:
        return {"ok": True, "job_id": req.job_id, "note": "no_payment_required"}

    if job.get("paid") is True:
        return {"ok": True, "job_id": req.job_id, "note": "already_paid"}

    amount_due = int(job.get("amount_due", 0))
    if amount_due <= 0:
        raise HTTPException(status_code=400, detail="invalid_amount_due")

    # Stripe amount in cents
    unit_amount = int(amount_due * 100)

    session = stripe.checkout.Session.create(
        mode="payment",
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": "Twickell PDF → XLSX Conversion"},
                    "unit_amount": unit_amount,
                },
                "quantity": 1,
            }
        ],
        success_url=f"{PUBLIC_BASE_URL}/payments/success?job_id={req.job_id}",
        cancel_url=f"{PUBLIC_BASE_URL}/payments/cancel?job_id={req.job_id}",
        metadata={"job_id": req.job_id},
    )

    # Store the session id on the job for traceability
    update_job(req.job_id, {"stripe_session_id": session.id})

    return {"ok": True, "job_id": req.job_id, "checkout_url": session.url, "session_id": session.id}


@app.post("/payments/webhook")
async def payments_webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="stripe_webhook_secret_not_set")

    try:
        event = stripe.Webhook.construct_event(payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_webhook_signature")

    # We care about successful checkout completion
    if event.get("type") == "checkout.session.completed":
        session = event["data"]["object"]
        job_id = None

        # Prefer metadata
        md = session.get("metadata") or {}
        job_id = md.get("job_id")

        if job_id:
            job = read_job(job_id)
            if job.get("paid") is not True:
                update_job(job_id, {"paid": True, "status": "paid", "stripe_paid_at": now_ts()})

            # Auto-start: move to queued and write bridge request (idempotent)
            job = read_job(job_id)
            if str(job.get("status")) in {"paid", "uploaded"} and not str(job.get("status")) == "queued":
                job = update_job(job_id, {"status": "queued"})
                req_path = write_bridge_request(job)
                update_job(job_id, {"paths": {**job.get("paths", {}), "bridge_request": str(req_path)}})

    return {"ok": True}


@app.get("/payments/success")
async def payments_success(job_id: str) -> Dict[str, Any]:
    # For now, return JSON; your website can redirect here and show a nicer page later.
    job = read_job(job_id)
    return {"ok": True, "job_id": job_id, "status": job.get("status"), "paid": bool(job.get("paid"))}


@app.get("/payments/cancel")
async def payments_cancel(job_id: str) -> Dict[str, Any]:
    job = read_job(job_id)
    return {"ok": True, "job_id": job_id, "status": job.get("status"), "paid": bool(job.get("paid")), "note": "payment_canceled"}


@app.get("/jobs/{job_id}")
async def jobs_get(job_id: str) -> Dict[str, Any]:
    job = read_job(job_id)
    paths = job.get("paths") or {}

    def is_file(s: str) -> bool:
        try:
            pp = Path(str(s).strip())
            return pp.exists() and pp.is_file()
        except Exception:
            return False

    output_xlsx = paths.get("output_xlsx") or str((APP_ROOT / "outputs" / job_id / "output.xlsx").resolve())
    bundle_zip = paths.get("bundle_zip") or str((APP_ROOT / "outputs" / job_id / "bundle.zip").resolve())

    job_view = dict(job)
    job_view["output_ready"] = is_file(output_xlsx)
    job_view["bundle_ready"] = is_file(bundle_zip)
    job_view.setdefault("paths", {})
    job_view["paths"] = {**paths, "output_xlsx": str(output_xlsx), "bundle_zip": str(bundle_zip)}

    return {"ok": True, "job": job_view}


@app.post("/jobs/{job_id}/mark_paid")
async def mark_paid(job_id: str) -> Dict[str, Any]:
    job = read_job(job_id)

    if job.get("paid") is True:
        if str(job.get("status")) == "awaiting_payment":
            job = update_job(job_id, {"status": "paid"})
        return {"ok": True, "job": job, "note": "already_paid"}

    job = update_job(job_id, {"paid": True, "status": "paid"})
    return {"ok": True, "job": job}


@app.post("/jobs/{job_id}/start")
async def start(job_id: str) -> Dict[str, Any]:
    job = read_job(job_id)

    if not job.get("paid"):
        raise HTTPException(status_code=402, detail="payment_required")

    if str(job.get("status")) == "queued":
        return {"ok": True, "job": job, "note": "already_queued"}

    if str(job.get("status")) not in {"uploaded", "paid"}:
        raise HTTPException(status_code=409, detail={"error": "invalid_status_for_start", "status": job.get("status")})

    job = update_job(job_id, {"status": "queued"})
    req_path = write_bridge_request(job)
    job = update_job(job_id, {"paths": {**job.get("paths", {}), "bridge_request": str(req_path)}})

    return {"ok": True, "job": job, "queued_request": str(req_path)}


@app.get("/jobs/{job_id}/download")
async def download(job_id: str):
    job = read_job(job_id)
    paths = job.get("paths") or {}

    def as_file(path_str: str):
        if not isinstance(path_str, str):
            return None
        s = path_str.strip()
        if not s:
            return None
        pp = Path(s)
        return pp if pp.exists() and pp.is_file() else None

    # ZIP first (job path OR default location)
    zip_p = as_file(paths.get("bundle_zip", ""))
    if zip_p is None:
        zip_p = as_file(str((APP_ROOT / "outputs" / job_id / "bundle.zip").resolve()))
    if zip_p is not None:
        return FileResponse(path=str(zip_p), filename=f"{job_id}.zip", media_type="application/zip")

    # XLSX fallback (job path OR default location)
    xlsx_p = as_file(paths.get("output_xlsx", ""))
    if xlsx_p is None:
        xlsx_p = as_file(str((APP_ROOT / "outputs" / job_id / "output.xlsx").resolve()))
    if xlsx_p is not None:
        return FileResponse(
            path=str(xlsx_p),
            filename=f"{job_id}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    raise HTTPException(status_code=404, detail="output_not_ready")
