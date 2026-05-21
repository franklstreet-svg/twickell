"""
Orbi Bridge routes — embedded as a Flask Blueprint inside the twickell.com app.

Provides B2B product coordination: per-customer data store, API key issuance,
embed code generation, owner dashboard, Stripe checkout, tier usage tracking,
welcome email, heartbeat monitor, self-heal flags.

Originally a standalone service on port 5080; merged into twickell.com so the
whole B2B platform deploys as one HuggingFace Space.

Usage:
    from bridge_routes import register_bridge_routes
    register_bridge_routes(app)

Endpoints
---------
GET  /health                                              service status
POST /api/heartbeat                                       product checks in (body: {product, customer_id, port, version})
GET  /api/products                                        list known products + last-seen
GET  /api/products/stale?seconds=120                      products that haven't pinged in N seconds

GET  /api/customer/<id>/profile                           shared business profile
POST /api/customer/<id>/profile                           merge updates into shared profile
GET  /api/customer/<id>/learned                           all owner-confirmed Q&A
POST /api/customer/<id>/learned                           add an owner-confirmed answer
GET  /api/customer/<id>/pending                           unknown questions waiting for owner
POST /api/customer/<id>/pending                           record a new unknown question
POST /api/customer/<id>/answer                            owner answers a pending question (moves it to learned)
GET  /api/customer/<id>/leads                             all leads
POST /api/customer/<id>/leads                             append a new lead
GET  /api/customer/<id>/notifications                     all owner alerts
POST /api/customer/<id>/notifications                     add a new alert

POST /api/products/<product_key>/restart                  flag a product for restart (read by its watchdog)
GET  /api/products/<product_key>/restart-pending          watchdog polls this to know if it should restart
"""

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, Flask, request, jsonify
from flask_cors import CORS


# ── Setup ──────────────────────────────────────────────────────────────────

HERE = Path(__file__).resolve().parent
# DATA_DIR is env-configurable so HF Spaces can point it at /data (persistent mount)
DATA_DIR = Path(os.environ.get('ORBI_DATA_DIR', str(HERE / 'data')))
DATA_DIR.mkdir(parents=True, exist_ok=True)
CUSTOMERS_DIR = DATA_DIR / 'customers'
CUSTOMERS_DIR.mkdir(parents=True, exist_ok=True)
PRODUCTS_FILE = DATA_DIR / 'products.json'
RESTART_FLAGS_FILE = DATA_DIR / 'restart_flags.json'

log = logging.getLogger(__name__)

# Routes register on this Blueprint; the host Flask app gets them via register_bridge_routes()
bp = Blueprint('bridge', __name__)

_lock = threading.RLock()  # reentrant so helpers can re-enter while holding the lock


def register_bridge_routes(host_app: Flask):
    """Attach the Bridge blueprint to the twickell Flask app and enable CORS
    on the Bridge endpoints so the widget on third-party customer sites can
    call /chat from any origin."""
    CORS(host_app, resources={
        r"/api/wc/*":           {"origins": "*"},
        r"/api/dashboard/*":    {"origins": "*"},
        r"/api/customer/*":     {"origins": "*"},
        r"/api/usage/*":        {"origins": "*"},
        r"/api/heartbeat":      {"origins": "*"},
        r"/api/products/*":     {"origins": "*"},
        r"/api/lookup-api-key/*": {"origins": "*"},
        r"/widget/*":           {"origins": "*"},
        r"/chat":               {"origins": "*"},
    })
    host_app.register_blueprint(bp)


# ── Atomic file helpers ────────────────────────────────────────────────────

def _atomic_write(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    tmp.replace(path)


def _read(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def _safe_id(s: str) -> str:
    return ''.join(c for c in (s or 'unknown')
                   if c.isalnum() or c in ('_', '-')) or 'unknown'


def _cust_dir(customer_id: str) -> Path:
    d = CUSTOMERS_DIR / _safe_id(customer_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Health & products ──────────────────────────────────────────────────────

# STATIC_DIR holds dashboard.html + the widget JS. In the merged twickell deploy,
# these files live in twickell_deploy/website/ (and the widget under website/widget/).
STATIC_DIR = Path(os.environ.get('ORBI_STATIC_DIR', str(HERE / 'website')))


@bp.get('/health')
def health():
    return {'ok': True, 'service': 'orbi_bridge', 'version': '1.0.0',
            'port': int(os.environ.get('ORBI_BRIDGE_PORT', '5080'))}


# /widget/<path> is handled by twickell's static_files catchall (serves from website/widget/)
# so no Blueprint route needed here.


@bp.post('/api/heartbeat')
def heartbeat():
    """A product (Receptionist or WebController) checks in.
    Body: {product, customer_id, port, version, state_checksum}"""
    data = request.get_json(silent=True) or {}
    product = (data.get('product') or '').strip()
    customer_id = (data.get('customer_id') or 'default').strip()
    port = data.get('port')
    version = data.get('version', '')
    state_checksum = data.get('state_checksum', '')

    if not product:
        return jsonify({'ok': False, 'error': 'product required'}), 400

    key = f"{_safe_id(customer_id)}::{_safe_id(product)}"
    with _lock:
        products = _read(PRODUCTS_FILE, {})
        existing = products.get(key, {})
        existing.update({
            'product': product,
            'customer_id': customer_id,
            'port': port,
            'version': version,
            'state_checksum': state_checksum,
            'last_seen': _now_iso(),
            'last_seen_count': (existing.get('last_seen_count') or 0) + 1,
        })
        if 'first_seen' not in existing:
            existing['first_seen'] = _now_iso()
        products[key] = existing
        _atomic_write(PRODUCTS_FILE, products)

    return jsonify({'ok': True, 'key': key, 'received_at': _now_iso()})


@bp.get('/api/products')
def list_products():
    with _lock:
        products = _read(PRODUCTS_FILE, {})
    return jsonify({'ok': True, 'count': len(products), 'products': products})


@bp.get('/api/products/stale')
def stale_products():
    """Products that haven't sent a heartbeat in `seconds` (default 120)."""
    threshold_seconds = int(request.args.get('seconds', '120'))
    now = datetime.now(timezone.utc)
    stale = []
    with _lock:
        products = _read(PRODUCTS_FILE, {})
        for key, p in products.items():
            try:
                last = datetime.fromisoformat(p['last_seen'].replace('Z', '+00:00'))
                age = (now - last).total_seconds()
                if age >= threshold_seconds:
                    stale.append({**p, 'key': key, 'seconds_since_seen': int(age)})
            except Exception:
                continue
    return jsonify({'ok': True, 'count': len(stale), 'stale': stale,
                    'threshold_seconds': threshold_seconds})


# ── Customer data: profile ─────────────────────────────────────────────────

@bp.get('/api/customer/<customer_id>/profile')
def get_profile(customer_id):
    profile = _read(_cust_dir(customer_id) / 'business_profile.json', {})
    return jsonify({'ok': True, 'customer_id': customer_id, 'profile': profile})


@bp.post('/api/customer/<customer_id>/profile')
def update_profile(customer_id):
    updates = request.get_json(silent=True) or {}
    with _lock:
        path = _cust_dir(customer_id) / 'business_profile.json'
        current = _read(path, {})
        for k, v in updates.items():
            if v not in (None, '', []):
                current[k] = v
        current['updated_at'] = _now_iso()
        _atomic_write(path, current)
    return jsonify({'ok': True, 'customer_id': customer_id, 'profile': current})


# ── Customer data: learned answers + pending unknowns ─────────────────────

@bp.get('/api/customer/<customer_id>/learned')
def get_learned(customer_id):
    items = _read(_cust_dir(customer_id) / 'learned_answers.json', [])
    answered = [i for i in items if i.get('verified') and i.get('answer')]
    return jsonify({'ok': True, 'customer_id': customer_id,
                    'count': len(answered), 'answered': answered})


@bp.get('/api/customer/<customer_id>/pending')
def get_pending(customer_id):
    items = _read(_cust_dir(customer_id) / 'learned_answers.json', [])
    pending = [i for i in items if not i.get('verified') or not i.get('answer')]
    return jsonify({'ok': True, 'customer_id': customer_id,
                    'count': len(pending), 'pending': pending})


@bp.post('/api/customer/<customer_id>/pending')
def add_pending(customer_id):
    """A product captures a question Orbi couldn't answer."""
    data = request.get_json(silent=True) or {}
    question = (data.get('question') or '').strip()
    if not question:
        return jsonify({'ok': False, 'error': 'question required'}), 400
    import re
    norm = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', question.lower())).strip()
    with _lock:
        path = _cust_dir(customer_id) / 'learned_answers.json'
        items = _read(path, [])
        # Dedupe by normalized question
        for it in items:
            if it.get('question_normalized') == norm:
                it['asked_count'] = (it.get('asked_count') or 0) + 1
                it['last_asked'] = _now_iso()
                _atomic_write(path, items)
                return jsonify({'ok': True, 'entry': it, 'new': False})
        entry = {
            'id': str(uuid.uuid4())[:8],
            'question': question,
            'question_normalized': norm,
            'answer': '',
            'answered_by': '',
            'verified': False,
            'asked_count': 1,
            'first_asked': _now_iso(),
            'last_asked': _now_iso(),
            'answered_at': '',
            'asked_via_product': data.get('product') or '',
            'session_id': data.get('session_id') or '',
        }
        items.append(entry)
        _atomic_write(path, items)
    return jsonify({'ok': True, 'entry': entry, 'new': True})


@bp.post('/api/customer/<customer_id>/answer')
def answer_pending(customer_id):
    """Owner provides the answer to a pending question.
    Either product can be where the answer came in — both will see it next time."""
    data = request.get_json(silent=True) or {}
    entry_id = (data.get('entry_id') or '').strip()
    answer = (data.get('answer') or '').strip()
    if not entry_id or not answer:
        return jsonify({'ok': False, 'error': 'entry_id and answer required'}), 400
    with _lock:
        path = _cust_dir(customer_id) / 'learned_answers.json'
        items = _read(path, [])
        for it in items:
            if it.get('id') == entry_id:
                it['answer'] = answer
                it['answered_by'] = data.get('answered_by', 'owner')
                it['verified'] = True
                it['answered_at'] = _now_iso()
                it['answered_via_product'] = data.get('product') or ''
                _atomic_write(path, items)
                return jsonify({'ok': True, 'entry': it})
    return jsonify({'ok': False, 'error': 'entry not found'}), 404


# ── Customer data: leads ───────────────────────────────────────────────────

@bp.get('/api/customer/<customer_id>/leads')
def get_leads(customer_id):
    leads = _read(_cust_dir(customer_id) / 'leads.json', [])
    return jsonify({'ok': True, 'customer_id': customer_id,
                    'count': len(leads), 'leads': list(reversed(leads))[:200]})


@bp.post('/api/customer/<customer_id>/leads')
def add_lead(customer_id):
    data = request.get_json(silent=True) or {}
    phone = (data.get('phone') or '').strip()
    email = (data.get('email') or '').strip().lower()
    name = (data.get('name') or '').strip()
    if not (phone or email or name):
        return jsonify({'ok': False, 'error': 'at least one of name/phone/email required'}), 400
    with _lock:
        path = _cust_dir(customer_id) / 'leads.json'
        leads = _read(path, [])
        for prior in leads:
            if phone and prior.get('phone') == phone: return jsonify({'ok': True, 'new': False})
            if email and (prior.get('email') or '').lower() == email: return jsonify({'ok': True, 'new': False})
        lead = {
            'name': name, 'phone': phone, 'email': email,
            'source': data.get('source', ''),
            'session_id': data.get('session_id', ''),
            'captured_via_product': data.get('product', ''),
            'timestamp': _now_iso(),
        }
        for k, v in data.items():
            if k not in lead and v:
                lead[k] = v
        leads.append(lead)
        _atomic_write(path, leads)
    return jsonify({'ok': True, 'new': True, 'lead': lead})


# ── Customer data: owner notifications ─────────────────────────────────────

@bp.get('/api/customer/<customer_id>/notifications')
def get_notifications(customer_id):
    items = _read(_cust_dir(customer_id) / 'notifications.json', [])
    return jsonify({'ok': True, 'customer_id': customer_id,
                    'count': len(items), 'notifications': list(reversed(items))[:100]})


@bp.post('/api/customer/<customer_id>/notifications')
def add_notification(customer_id):
    data = request.get_json(silent=True) or {}
    kind = (data.get('kind') or 'owner_review').strip()
    summary = (data.get('summary') or '').strip()
    if not summary:
        return jsonify({'ok': False, 'error': 'summary required'}), 400
    with _lock:
        path = _cust_dir(customer_id) / 'notifications.json'
        items = _read(path, [])
        entry = {
            'id': str(uuid.uuid4())[:8],
            'kind': kind,
            'urgency': data.get('urgency', 'normal'),
            'summary': summary,
            'caller_name': data.get('caller_name', ''),
            'caller_phone': data.get('caller_phone', ''),
            'recommended_action': data.get('recommended_action', ''),
            'session_id': data.get('session_id', ''),
            'from_product': data.get('product', ''),
            'created_at': _now_iso(),
            'delivered_at': '',
            'acknowledged_at': '',
        }
        items.append(entry)
        _atomic_write(path, items)
    return jsonify({'ok': True, 'notification': entry})


# ── API keys + embed code (Website Controller / Receptionist provisioning) ─

import secrets

PRODUCT_PREFIXES = {
    'website_controller': 'sk_wc_',
    'receptionist': 'sk_rc_',
}


def _api_keys_path(customer_id: str) -> Path:
    return _cust_dir(customer_id) / 'api_keys.json'


def _owner_path(customer_id: str) -> Path:
    return _cust_dir(customer_id) / 'owner.json'


OWNER_TOKEN_INDEX = DATA_DIR / 'owner_token_index.json'


def _index_owner_token(token: str, customer_id: str):
    with _lock:
        idx = _read(OWNER_TOKEN_INDEX, {})
        idx[token] = customer_id
        _atomic_write(OWNER_TOKEN_INDEX, idx)


def _lookup_owner_token(token: str) -> str:
    """token → customer_id, or '' if invalid. Fast lookup via index."""
    if not token:
        return ''
    idx = _read(OWNER_TOKEN_INDEX, {})
    return idx.get(token, '')


def _get_or_create_owner_token(customer_id: str, owner_email: str = '') -> dict:
    """Issues a long-lived owner token used to authenticate the dashboard.
    Single token per customer (not per product) — one owner manages all products."""
    with _lock:
        path = _owner_path(customer_id)
        rec = _read(path, {})
        if not rec.get('owner_token'):
            rec = {
                'customer_id': customer_id,
                'owner_token': secrets.token_urlsafe(32),
                'owner_email': owner_email or rec.get('owner_email', ''),
                'created_at': _now_iso(),
                'last_login_at': '',
            }
            _atomic_write(path, rec)
        elif owner_email and not rec.get('owner_email'):
            rec['owner_email'] = owner_email
            _atomic_write(path, rec)
        # Always ensure the index has the current token (idempotent — handles
        # records written by older Bridge versions before the index existed)
        _index_owner_token(rec['owner_token'], customer_id)
        return rec


def _generate_api_key(product: str) -> str:
    prefix = PRODUCT_PREFIXES.get(product, 'sk_xx_')
    return prefix + secrets.token_urlsafe(24)


def _get_or_create_api_key(customer_id: str, product: str) -> dict:
    """Returns existing API key entry for (customer, product) or creates one.
    Entry shape: {api_key, product, customer_id, created_at, revoked, revoked_at}
    """
    if product not in PRODUCT_PREFIXES:
        raise ValueError(f'unknown product: {product}')
    with _lock:
        path = _api_keys_path(customer_id)
        keys = _read(path, [])
        # Find active (non-revoked) key for this product
        for entry in keys:
            if entry.get('product') == product and not entry.get('revoked'):
                return entry
        # None active — issue one
        entry = {
            'api_key': _generate_api_key(product),
            'product': product,
            'customer_id': customer_id,
            'created_at': _now_iso(),
            'revoked': False,
            'revoked_at': '',
        }
        keys.append(entry)
        _atomic_write(path, keys)
        return entry


def _brain_url() -> str:
    # In the merged twickell deploy, /chat lives on twickell.com itself.
    return os.environ.get('ORBI_BRAIN_URL', 'https://twickell.com').strip().rstrip('/')


def _widget_url() -> str:
    return os.environ.get('ORBI_WIDGET_URL', 'https://twickell.com/widget').strip().rstrip('/')


def _dashboard_base() -> str:
    # .strip() handles any stray whitespace/newlines the env var was pasted with
    return os.environ.get('ORBI_DASHBOARD_URL', 'https://twickell.com').strip().rstrip('/')


def _dashboard_url(owner_token: str) -> str:
    return f'{_dashboard_base()}/dashboard?token={owner_token}'


def _build_embed_snippet(customer_id: str, api_key: str, product: str,
                         business_profile: dict) -> str:
    """Produces the <script> block the customer pastes on their site.
    Pre-fills greeting using business name when available."""
    business_name = (business_profile or {}).get('name', '').strip() or 'us'
    safe_name = business_name.replace('"', '\\"')
    greeting = f"Hi! I'm Orby — I help {safe_name}. What can I do for you?"
    return (
        f'<!-- Orbi AI Website Controller — paste anywhere in your <body> -->\n'
        f'<script>\n'
        f'  window.ORBY_CONFIG = {{\n'
        f'    apiUrl: "{_brain_url()}/chat",\n'
        f'    customerId: "{customer_id}",\n'
        f'    apiKey: "{api_key}",\n'
        f'    deployment: "{product}",\n'
        f'    greeting: "{greeting}"\n'
        f'  }};\n'
        f'</script>\n'
        f'<script src="{_widget_url()}/aurora-widget.js" async></script>\n'
    )


@bp.post('/api/customer/<customer_id>/create-instance')
def create_instance(customer_id):
    """Provision a new product instance for an owner.
    Body: {product: 'website_controller' | 'receptionist', tier?: 'starter'|'growth'|'pro'|'enterprise'}
    Returns: api key + ready-to-paste embed code (for website_controller).
    Called by Stripe webhook handler after successful purchase, or manually by admin.
    """
    data = request.get_json(silent=True) or {}
    product = (data.get('product') or '').strip()
    tier = (data.get('tier') or 'starter').strip()
    if product not in PRODUCT_PREFIXES:
        return jsonify({'ok': False, 'error': f'product must be one of {list(PRODUCT_PREFIXES)}'}), 400
    # Issue/reuse API key
    key_entry = _get_or_create_api_key(customer_id, product)
    # Load business profile if any (for greeting personalization)
    biz = _read(_cust_dir(customer_id) / 'business_profile.json', {})
    # Persist tier on the instance record
    with _lock:
        inst_path = _cust_dir(customer_id) / 'instances.json'
        instances = _read(inst_path, [])
        # Update existing or append
        found = False
        for inst in instances:
            if inst.get('product') == product:
                inst['tier'] = tier
                inst['updated_at'] = _now_iso()
                found = True
                break
        if not found:
            instances.append({
                'product': product,
                'tier': tier,
                'created_at': _now_iso(),
                'updated_at': _now_iso(),
                'monthly_usage': 0,
                'usage_period_start': _now_iso(),
            })
        _atomic_write(inst_path, instances)
    # Issue/reuse owner token so the dashboard URL can be emailed in provisioning
    owner_email = (data.get('owner_email') or '').strip()
    owner_rec = _get_or_create_owner_token(customer_id, owner_email)
    response = {
        'ok': True,
        'customer_id': customer_id,
        'product': product,
        'tier': tier,
        'api_key': key_entry['api_key'],
        'brain_url': _brain_url(),
        'owner_token': owner_rec['owner_token'],
        'dashboard_url': _dashboard_url(owner_rec['owner_token']),
    }
    if product == 'website_controller':
        response['embed_code'] = _build_embed_snippet(customer_id, key_entry['api_key'], product, biz)
        response['widget_url'] = _widget_url() + '/aurora-widget.js'
    return jsonify(response)


@bp.get('/api/customer/<customer_id>/embed-code')
def get_embed_code(customer_id):
    """Returns the embed code for an existing customer's product instance.
    Query: ?product=website_controller (default)
    Used by owner dashboard ('show me my embed code') and provisioning email.
    """
    product = (request.args.get('product') or 'website_controller').strip()
    if product not in PRODUCT_PREFIXES:
        return jsonify({'ok': False, 'error': f'product must be one of {list(PRODUCT_PREFIXES)}'}), 400
    key_entry = _get_or_create_api_key(customer_id, product)
    biz = _read(_cust_dir(customer_id) / 'business_profile.json', {})
    payload = {
        'ok': True,
        'customer_id': customer_id,
        'product': product,
        'api_key': key_entry['api_key'],
        'brain_url': _brain_url(),
        'widget_url': _widget_url() + '/aurora-widget.js',
    }
    if product == 'website_controller':
        payload['embed_code'] = _build_embed_snippet(customer_id, key_entry['api_key'], product, biz)
    return jsonify(payload)


@bp.get('/api/customer/<customer_id>/api-keys')
def list_api_keys(customer_id):
    """List all API keys for a customer (admin/dashboard view).
    Hides nothing — these are the customer's own keys."""
    keys = _read(_api_keys_path(customer_id), [])
    return jsonify({'ok': True, 'customer_id': customer_id, 'count': len(keys), 'api_keys': keys})


@bp.post('/api/customer/<customer_id>/api-keys/<api_key>/revoke')
def revoke_api_key(customer_id, api_key):
    """Mark a key revoked. Brain will refuse requests using it.
    Next request from the customer will get a fresh key via create-instance."""
    with _lock:
        path = _api_keys_path(customer_id)
        keys = _read(path, [])
        hit = None
        for entry in keys:
            if entry.get('api_key') == api_key:
                entry['revoked'] = True
                entry['revoked_at'] = _now_iso()
                hit = entry
                break
        if not hit:
            return jsonify({'ok': False, 'error': 'api_key not found'}), 404
        _atomic_write(path, keys)
    return jsonify({'ok': True, 'revoked': hit})


# ── Owner Dashboard ────────────────────────────────────────────────────────

def _require_owner(request_obj):
    """Returns (customer_id, error_response_or_None).
    On success: (customer_id, None). On failure: (None, (jsonify, status))."""
    token = (request_obj.args.get('token') or
             (request_obj.get_json(silent=True) or {}).get('token') or '').strip()
    if not token:
        return None, (jsonify({'ok': False, 'error': 'owner token required'}), 401)
    customer_id = _lookup_owner_token(token)
    if not customer_id:
        return None, (jsonify({'ok': False, 'error': 'invalid owner token'}), 401)
    return customer_id, None


@bp.get('/dashboard')
def dashboard_page():
    """Serves the owner dashboard HTML. Token validation happens via the JS calls."""
    from flask import send_from_directory
    return send_from_directory(STATIC_DIR, 'dashboard.html')


# /dashboard/static/<path> intentionally removed — dashboard.html inlines all CSS/JS.


@bp.get('/api/dashboard/data')
def dashboard_data():
    """One-shot fetch for the dashboard. Returns everything the dashboard needs."""
    customer_id, err = _require_owner(request)
    if err:
        return err
    # Update last_login_at
    with _lock:
        rec = _read(_owner_path(customer_id), {})
        rec['last_login_at'] = _now_iso()
        _atomic_write(_owner_path(customer_id), rec)
    cdir = _cust_dir(customer_id)
    profile = _read(cdir / 'business_profile.json', {})
    # Q&A live in one file — split by verified+answer to get pending vs learned
    qa_items = _read(cdir / 'learned_answers.json', [])
    pending = [it for it in qa_items if not (it.get('verified') and it.get('answer'))]
    learned = [it for it in qa_items if it.get('verified') and it.get('answer')]
    # Normalize pending entries for the dashboard UI (which expects question/created_at/id/times_asked)
    pending_view = [{
        'id': it.get('id', ''),
        'question': it.get('question', ''),
        'created_at': it.get('first_asked', ''),
        'times_asked': it.get('asked_count', 1),
    } for it in pending]
    leads = _read(cdir / 'leads.json', [])
    notifications = _read(cdir / 'notifications.json', [])
    instances = _read(cdir / 'instances.json', [])
    api_keys = _read(_api_keys_path(customer_id), [])
    # Build embed code for any website_controller instance
    embed_code = ''
    wc_key = next((k for k in api_keys
                   if k.get('product') == 'website_controller' and not k.get('revoked')), None)
    if wc_key:
        embed_code = _build_embed_snippet(customer_id, wc_key['api_key'], 'website_controller', profile)
    # Founding member status — shown in the dashboard as a badge
    founding_status = None
    for inst in instances:
        if inst.get('founding_member'):
            founding_status = {
                'product': inst.get('product'),
                'number': inst.get('founding_member_number'),
                'tier': inst.get('tier'),
                'locked_monthly_cents': inst.get('locked_in_monthly_cents'),
            }
            break
    return jsonify({
        'ok': True,
        'customer_id': customer_id,
        'owner_email': rec.get('owner_email', ''),
        'profile': profile,
        'pending': list(reversed(pending_view))[:100],
        'pending_count': len(pending_view),
        'learned': list(reversed(learned))[:100],
        'leads': list(reversed(leads))[:100],
        'notifications': list(reversed(notifications))[:100],
        'instances': instances,
        'embed_code': embed_code,
        'founding_member': founding_status,
    })


@bp.post('/api/dashboard/answer-pending')
def dashboard_answer_pending():
    """Owner answers a pending question. Marks the entry verified with the answer
    in the single learned_answers.json file (same model used by the Brain)."""
    customer_id, err = _require_owner(request)
    if err:
        return err
    data = request.get_json(silent=True) or {}
    entry_id = (data.get('entry_id') or '').strip()
    answer = (data.get('answer') or '').strip()
    if not entry_id or not answer:
        return jsonify({'ok': False, 'error': 'entry_id and answer required'}), 400
    cdir = _cust_dir(customer_id)
    with _lock:
        path = cdir / 'learned_answers.json'
        items = _read(path, [])
        for it in items:
            if it.get('id') == entry_id:
                it['answer'] = answer
                it['answered_by'] = 'owner_dashboard'
                it['verified'] = True
                it['answered_at'] = _now_iso()
                _atomic_write(path, items)
                return jsonify({'ok': True, 'entry': it})
    return jsonify({'ok': False, 'error': 'pending entry not found'}), 404


@bp.post('/api/dashboard/update-profile')
def dashboard_update_profile():
    """Owner edits business profile. Merges updates into existing profile."""
    customer_id, err = _require_owner(request)
    if err:
        return err
    data = request.get_json(silent=True) or {}
    updates = data.get('updates') or {}
    if not isinstance(updates, dict):
        return jsonify({'ok': False, 'error': 'updates must be an object'}), 400
    cdir = _cust_dir(customer_id)
    with _lock:
        path = cdir / 'business_profile.json'
        profile = _read(path, {})
        for k, v in updates.items():
            if v is None or v == '':
                continue
            profile[k] = v
        profile['updated_at'] = _now_iso()
        _atomic_write(path, profile)
    return jsonify({'ok': True, 'profile': profile})


@bp.post('/api/dashboard/mark-notification')
def dashboard_mark_notification():
    """Owner acknowledges a notification (mark read)."""
    customer_id, err = _require_owner(request)
    if err:
        return err
    data = request.get_json(silent=True) or {}
    notif_id = (data.get('id') or '').strip()
    if not notif_id:
        return jsonify({'ok': False, 'error': 'notification id required'}), 400
    cdir = _cust_dir(customer_id)
    with _lock:
        path = cdir / 'notifications.json'
        items = _read(path, [])
        hit = None
        for n in items:
            if n.get('id') == notif_id:
                n['acknowledged_at'] = _now_iso()
                hit = n
                break
        if not hit:
            return jsonify({'ok': False, 'error': 'notification not found'}), 404
        _atomic_write(path, items)
    return jsonify({'ok': True, 'notification': hit})


# Reverse-lookup helper used by brain auth: api_key → customer_id+product
@bp.get('/api/lookup-api-key/<api_key>')
def lookup_api_key(api_key):
    """Used by the brain to validate an incoming API key.
    Scans all customers; for hundreds of customers an index file would be faster,
    but at our scale (first 50 customers) a scan is fine."""
    for cust_dir in CUSTOMERS_DIR.iterdir():
        if not cust_dir.is_dir():
            continue
        keys = _read(cust_dir / 'api_keys.json', [])
        for entry in keys:
            if entry.get('api_key') == api_key:
                return jsonify({
                    'ok': True,
                    'valid': not entry.get('revoked'),
                    'customer_id': entry.get('customer_id'),
                    'product': entry.get('product'),
                    'revoked': entry.get('revoked', False),
                })
    return jsonify({'ok': True, 'valid': False, 'reason': 'not_found'})


# ── Tier usage tracking ────────────────────────────────────────────────────

# Limits per product/tier — must match WC_PRICING and the published Receptionist tiers
USAGE_LIMITS = {
    'website_controller': {'starter': 500, 'growth': 2500, 'pro': 10000},
    'receptionist':       {'starter': 300, 'growth': 1000, 'pro': 3000},
}


def _current_instance(customer_id: str, product: str) -> tuple:
    """Returns (instances_list, instance_dict_or_None) for the active product instance."""
    path = _cust_dir(customer_id) / 'instances.json'
    items = _read(path, [])
    for inst in items:
        if inst.get('product') == product:
            return items, inst
    return items, None


def _reset_period_if_due(inst: dict) -> bool:
    """Resets monthly_usage to 0 if usage_period_start is >30 days old.
    Returns True if a reset happened."""
    start = inst.get('usage_period_start') or ''
    if not start:
        inst['usage_period_start'] = _now_iso()
        return True
    try:
        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
        if (datetime.now(timezone.utc) - start_dt).days >= 30:
            inst['monthly_usage'] = 0
            inst['usage_period_start'] = _now_iso()
            inst['warning_80_sent'] = False
            inst['warning_100_sent'] = False
            return True
    except Exception:
        pass
    return False


@bp.post('/api/usage/<customer_id>/chat-started')
def usage_chat_started(customer_id):
    """Brain calls this when a new chat session begins for a customer.
    Body: {product: 'website_controller'|'receptionist'}
    Returns current usage state. Triggers warning emails at 80% and 100%."""
    data = request.get_json(silent=True) or {}
    product = (data.get('product') or 'website_controller').strip()
    if product not in USAGE_LIMITS:
        return jsonify({'ok': False, 'error': f'unknown product {product}'}), 400
    with _lock:
        path = _cust_dir(customer_id) / 'instances.json'
        items = _read(path, [])
        inst = next((i for i in items if i.get('product') == product), None)
        if not inst:
            return jsonify({'ok': False, 'error': 'no active instance for this product'}), 404
        _reset_period_if_due(inst)
        inst['monthly_usage'] = (inst.get('monthly_usage') or 0) + 1
        tier = inst.get('tier', 'starter')
        limit = USAGE_LIMITS[product].get(tier, USAGE_LIMITS[product]['starter'])
        usage = inst['monthly_usage']
        pct = (usage / limit) * 100 if limit > 0 else 0
        # Threshold flags (idempotent — fire-once per billing cycle)
        crossed_80 = pct >= 80 and not inst.get('warning_80_sent')
        crossed_100 = pct >= 100 and not inst.get('warning_100_sent')
        if crossed_80:
            inst['warning_80_sent'] = True
        if crossed_100:
            inst['warning_100_sent'] = True
        _atomic_write(path, items)

    # Send warning emails outside the lock
    if crossed_80 or crossed_100:
        try:
            owner_rec = _read(_owner_path(customer_id), {})
            owner_email = owner_rec.get('owner_email', '')
            if owner_email:
                if crossed_100:
                    subj = f"You've reached your {product.replace('_', ' ').title()} tier limit"
                    body = (f"You've hit your monthly limit of {limit} for the {tier.title()} tier "
                            f"on your {product.replace('_', ' ').title()}.\n\n"
                            f"Orby will keep working, but to make sure she stays fast and reliable "
                            f"we recommend upgrading to the next tier. Reply to this email or visit "
                            f"your dashboard to upgrade.\n\nDashboard: "
                            f"{_dashboard_url(owner_rec.get('owner_token', ''))}\n\n— Orby AI")
                else:
                    subj = f"You're at 80% of your {product.replace('_', ' ').title()} tier"
                    body = (f"Heads up — you've used {usage} of {limit} this month on the {tier.title()} "
                            f"tier. You're growing! Let us know if you'd like to upgrade to the next "
                            f"tier so you don't hit the cap.\n\nDashboard: "
                            f"{_dashboard_url(owner_rec.get('owner_token', ''))}\n\n— Orby AI")
                send_email(owner_email, subj, body)
        except Exception as e:
            log.warning('usage warning email failed for %s: %s', customer_id, e)

    return jsonify({
        'ok': True,
        'customer_id': customer_id,
        'product': product,
        'tier': tier,
        'usage': usage,
        'limit': limit,
        'percent': round(pct, 1),
        'over_limit': pct >= 100,
    })


@bp.get('/api/usage/<customer_id>')
def usage_get(customer_id):
    """Read-only usage status across all products for a customer."""
    items = _read(_cust_dir(customer_id) / 'instances.json', [])
    out = []
    for inst in items:
        product = inst.get('product', '')
        tier = inst.get('tier', 'starter')
        limit = USAGE_LIMITS.get(product, {}).get(tier, 0)
        usage = inst.get('monthly_usage', 0)
        out.append({
            'product': product,
            'tier': tier,
            'usage': usage,
            'limit': limit,
            'percent': round((usage / limit) * 100, 1) if limit > 0 else 0,
            'usage_period_start': inst.get('usage_period_start', ''),
        })
    return jsonify({'ok': True, 'customer_id': customer_id, 'usage': out})


# ── Email send (Gmail SMTP) ────────────────────────────────────────────────

def _email_config() -> dict:
    return {
        'host': os.environ.get('ORBI_EMAIL_HOST', 'smtp.gmail.com'),
        'port': int(os.environ.get('ORBI_EMAIL_PORT', '465')),
        'user': os.environ.get('ORBI_EMAIL', ''),
        'password': os.environ.get('ORBI_EMAIL_PASSWORD', ''),
        'from_name': os.environ.get('ORBI_EMAIL_FROM_NAME', 'Orby AI'),
        'dry_run': os.environ.get('ORBI_EMAIL_DRY_RUN', '').strip() in ('1', 'true', 'yes'),
    }


def send_email(to_email: str, subject: str, text_body: str, html_body: str = '') -> dict:
    """Sends an email via Gmail SMTP using ORBI_EMAIL credentials.
    If ORBI_EMAIL_DRY_RUN is set, just logs the message and returns ok=True without sending."""
    cfg = _email_config()
    if not to_email or not cfg['user']:
        return {'ok': False, 'error': 'missing to_email or ORBI_EMAIL'}
    if cfg['dry_run'] or not cfg['password']:
        log.info('[EMAIL DRY-RUN] To: %s | Subject: %s | First 200 chars: %s',
                 to_email, subject, text_body[:200].replace('\n', ' '))
        return {'ok': True, 'dry_run': True}
    try:
        import smtplib
        import ssl
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        msg = MIMEMultipart('alternative')
        msg['From'] = f"{cfg['from_name']} <{cfg['user']}>"
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg['host'], cfg['port'], context=ctx, timeout=20) as server:
            server.login(cfg['user'], cfg['password'])
            server.sendmail(cfg['user'], [to_email], msg.as_string())
        log.info('Email sent: to=%s subject=%s', to_email, subject)
        return {'ok': True}
    except Exception as e:
        log.warning('Email send failed: %s', e)
        return {'ok': False, 'error': str(e)}


def _welcome_email_body(delivery: dict) -> tuple:
    """Returns (text_body, html_body) for the post-purchase welcome email."""
    biz = delivery.get('business_name', '') or 'your business'
    dashboard_url = delivery.get('dashboard_url', '')
    embed_code = delivery.get('embed_code', '')
    tier_label = (delivery.get('tier') or 'Starter').title()
    text = f"""Welcome to Orby!

Thanks for buying the AI Website Controller — {tier_label} tier — for {biz}.
Your Orby is provisioned and ready.

▸ Your Owner Dashboard:
{dashboard_url}

This is where you answer Orby's questions, see leads she captures, and edit
your business profile. Bookmark this link — it's how you stay in control.

▸ Your Embed Code:

Paste this anywhere in your website's <body> tag. Once it's live, Orby will
appear as a floating chat in the bottom-right of every page. Customers can
talk to her 24/7.

{embed_code}

▸ Quick install help:

- WordPress: Appearance → Theme Editor → footer.php, before </body>
- Shopify: Online Store → Themes → Actions → Edit code → theme.liquid
- Squarespace: Settings → Advanced → Code Injection → Footer
- Wix: Settings → Custom Code → Add Custom Code → choose "Body — end"

Reload your site and Orby should appear within 5 seconds.

▸ Need help?

Reply to this email or write to support@twickell.com — we read every message.

Welcome aboard.

— The Orbi AI team
"""
    html = f"""<html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0f1e;color:#e8eaf0;margin:0;padding:32px 16px;">
<div style="max-width:600px;margin:0 auto;background:#111827;border:1px solid rgba(212,160,23,0.3);border-radius:12px;padding:32px;">
<h1 style="background:linear-gradient(135deg,#d4a017,#f0c040);-webkit-background-clip:text;background-clip:text;color:transparent;margin:0 0 16px;">Welcome to Orby</h1>
<p style="color:#e8eaf0;line-height:1.6;">Thanks for buying the <strong>AI Website Controller — {tier_label}</strong> tier for <strong>{biz}</strong>. Your Orby is provisioned and ready.</p>

<h3 style="color:#f0c040;margin-top:32px;">Your Owner Dashboard</h3>
<p style="color:#e8eaf0;line-height:1.6;">Answer Orby's questions, see captured leads, edit your business profile:</p>
<p><a href="{dashboard_url}" style="background:linear-gradient(135deg,#d4a017,#f0c040);color:#0a0f1e;padding:12px 24px;border-radius:8px;font-weight:700;text-decoration:none;display:inline-block;">Open Dashboard →</a></p>
<p style="color:#8892a4;font-size:12px;">Bookmark this link — it's how you stay in control.</p>

<h3 style="color:#f0c040;margin-top:32px;">Your Embed Code</h3>
<p style="color:#e8eaf0;line-height:1.6;">Paste this anywhere in your website's <code>&lt;body&gt;</code> tag:</p>
<pre style="background:#0a0f1e;border:1px solid rgba(212,160,23,0.3);padding:14px;border-radius:8px;overflow-x:auto;font-size:11px;color:#e8eaf0;white-space:pre-wrap;">{embed_code}</pre>

<h3 style="color:#f0c040;margin-top:32px;">Quick install help</h3>
<ul style="color:#e8eaf0;line-height:1.8;">
<li><strong>WordPress:</strong> Appearance → Theme Editor → footer.php, before <code>&lt;/body&gt;</code></li>
<li><strong>Shopify:</strong> Online Store → Themes → Actions → Edit code → theme.liquid</li>
<li><strong>Squarespace:</strong> Settings → Advanced → Code Injection → Footer</li>
<li><strong>Wix:</strong> Settings → Custom Code → Add Custom Code → choose "Body — end"</li>
</ul>
<p style="color:#e8eaf0;">Reload your site and Orby should appear within 5 seconds.</p>

<hr style="border:none;border-top:1px solid rgba(212,160,23,0.2);margin:32px 0;">
<p style="color:#8892a4;font-size:13px;">Need help? Reply to this email or write to support@twickell.com — we read every message.</p>
<p style="color:#8892a4;font-size:13px;">— The Orbi AI team</p>
</div></body></html>"""
    return text, html


def send_welcome_email(delivery: dict) -> dict:
    """Sends the welcome email containing embed code + dashboard link."""
    to_email = delivery.get('owner_email', '')
    if not to_email:
        return {'ok': False, 'error': 'no owner_email in delivery'}
    text, html = _welcome_email_body(delivery)
    biz = delivery.get('business_name', '') or 'your business'
    subject = f"Your Orby is ready — {biz}"
    return send_email(to_email, subject, text, html)


@bp.post('/api/dashboard/resend-welcome')
def resend_welcome_email():
    """Owner-facing: resend the welcome email (with embed code) to themselves.
    Useful if they lost the original email."""
    customer_id, err = _require_owner(request)
    if err:
        return err
    delivery = _read(_cust_dir(customer_id) / 'delivery.json', None)
    if not delivery:
        # Rebuild delivery on the fly if not saved
        owner_rec = _read(_owner_path(customer_id), {})
        biz = _read(_cust_dir(customer_id) / 'business_profile.json', {})
        api_keys = _read(_api_keys_path(customer_id), [])
        wc_key = next((k for k in api_keys
                       if k.get('product') == 'website_controller' and not k.get('revoked')), None)
        if not wc_key:
            return jsonify({'ok': False, 'error': 'no Website Controller instance found'}), 404
        delivery = {
            'customer_id': customer_id,
            'owner_email': owner_rec.get('owner_email', ''),
            'business_name': biz.get('name', ''),
            'business_website': biz.get('website', ''),
            'tier': 'starter',  # fallback
            'api_key': wc_key['api_key'],
            'embed_code': _build_embed_snippet(customer_id, wc_key['api_key'], 'website_controller', biz),
            'dashboard_url': _dashboard_url(owner_rec.get('owner_token', '')),
        }
    result = send_welcome_email(delivery)
    return jsonify({'ok': result.get('ok', False), 'dry_run': result.get('dry_run', False),
                    'error': result.get('error', '')})


# ── Stripe checkout (Website Controller B2B) ───────────────────────────────

# Pricing — locked per memory/project_b2b_pricing.md (2026-05-21)
WC_PRICING = {
    'starter':    {'monthly': 9900,  'label': 'AI Website Controller — Starter (up to 500 chats/mo)'},
    'growth':     {'monthly': 19900, 'label': 'AI Website Controller — Growth (up to 2,500 chats/mo)'},
    'pro':        {'monthly': 34900, 'label': 'AI Website Controller — Pro (up to 10,000 chats/mo)'},
}
WC_SETUP_FEE_CENTS = 29900  # $299 per product, all tiers
FOUNDING_MEMBER_CAP = 1000  # first N paying customers per product get 50% off setup


def _count_founding_members(product: str) -> int:
    """Scan all customer instances and return how many are already flagged
    as founding members for this product. Used to determine whether the next
    purchase still qualifies (cap is 50 per product)."""
    count = 0
    if not CUSTOMERS_DIR.exists():
        return 0
    for cdir in CUSTOMERS_DIR.iterdir():
        if not cdir.is_dir():
            continue
        for inst in _read(cdir / 'instances.json', []):
            if inst.get('product') == product and inst.get('founding_member'):
                count += 1
    return count


def _safe_customer_id(business_name: str, email: str) -> str:
    """Build a sane customer_id slug from business name + email."""
    import re
    base = re.sub(r'[^a-z0-9]+', '_', (business_name or email or 'customer').lower()).strip('_')
    return base[:40] or 'customer'


@bp.post('/api/wc/checkout')
def wc_checkout():
    """Customer clicks 'Buy Website Controller — Starter' on the marketing site.
    Mirrors twickell's existing purchase flow: legal acceptance must come first.

    Body: {acceptance_id, tier?, owner_email?, business_name?, business_website?, success_url?, cancel_url?}
    The acceptance_id is required and must match a record saved by /api/legal_accept.
    Tier/email/business fields fall back to the values stored in the legal record.
    Returns: {url} to redirect to Stripe-hosted checkout."""
    stripe_key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not stripe_key:
        return jsonify({'ok': False, 'error': 'Stripe not configured on the Bridge.'}), 503
    data = request.get_json(silent=True) or {}

    # Legal acceptance gate — match the consumer purchase flow on twickell.com
    acceptance_id = (data.get('acceptance_id') or '').strip()
    legal_dir = Path('/tmp/orby_legal')
    legal_record = {}
    if acceptance_id:
        legal_file = legal_dir / f'{acceptance_id}.json'
        if legal_file.exists():
            try:
                legal_record = json.loads(legal_file.read_text(encoding='utf-8'))
            except Exception:
                legal_record = {}
    if not acceptance_id or not legal_record.get('accepted'):
        return jsonify({'ok': False, 'error': 'Legal acceptance required before payment. Please accept the terms first.'}), 403

    # Prefer fields from the legal record (the contract source of truth); fall
    # back to request body where the visitor may want to override (e.g. fixing a typo).
    tier = (data.get('tier') or legal_record.get('tier') or 'starter').strip().lower()
    owner_email = (data.get('owner_email') or legal_record.get('email') or '').strip()
    business_name = (data.get('business_name') or legal_record.get('business_name') or '').strip()
    business_website = (data.get('business_website') or legal_record.get('business_website') or '').strip()
    if tier not in WC_PRICING:
        return jsonify({'ok': False, 'error': f'tier must be one of {list(WC_PRICING)}'}), 400
    if not owner_email:
        return jsonify({'ok': False, 'error': 'owner_email required'}), 400
    try:
        import stripe as _stripe
        _stripe.api_key = stripe_key
    except ImportError:
        return jsonify({'ok': False, 'error': 'stripe library not installed'}), 500

    customer_id = _safe_customer_id(business_name, owner_email)
    pricing = WC_PRICING[tier]

    # Founding member check — first 1000 paying customers per product get 50% off
    # the one-time setup fee ($149.50 instead of $299). Decided at checkout time
    # so the discount is reflected on the Stripe invoice the customer signs for.
    founding_count = _count_founding_members('website_controller')
    is_founding_member = founding_count < FOUNDING_MEMBER_CAP
    setup_fee_cents = WC_SETUP_FEE_CENTS // 2 if is_founding_member else WC_SETUP_FEE_CENTS
    setup_label = ('AI Website Controller — Founding Member Setup (50% off, #'
                   + str(founding_count + 1) + ' of ' + str(FOUNDING_MEMBER_CAP) + ')') if is_founding_member else \
                  'AI Website Controller — One-Time Setup Fee'

    line_items = [
        {
            'price_data': {
                'currency': 'usd',
                'product_data': {'name': pricing['label']},
                'unit_amount': pricing['monthly'],
                'recurring': {'interval': 'month'},
            },
            'quantity': 1,
        },
        {
            'price_data': {
                'currency': 'usd',
                'product_data': {'name': setup_label},
                'unit_amount': setup_fee_cents,
            },
            'quantity': 1,
        },
    ]
    base_url = request.host_url.rstrip('/')
    success_url = (data.get('success_url') or f'{base_url}/wc/success?session_id={{CHECKOUT_SESSION_ID}}')
    cancel_url = (data.get('cancel_url') or f'{base_url}/wc/cancel')
    try:
        session = _stripe.checkout.Session.create(
            mode='subscription',
            customer_email=owner_email,
            line_items=line_items,
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                'product': 'website_controller',
                'tier': tier,
                'customer_id': customer_id,
                'owner_email': owner_email,
                'business_name': business_name,
                'business_website': business_website,
                'acceptance_id': acceptance_id,
                'founding_member': 'true' if is_founding_member else 'false',
                'founding_member_number': str(founding_count + 1) if is_founding_member else '',
                'setup_fee_paid_cents': str(setup_fee_cents),
            },
            subscription_data={'metadata': {'customer_id': customer_id, 'tier': tier}},
        )
        log.info('Stripe WC checkout created: session=%s tier=%s customer=%s', session.id, tier, customer_id)
        return jsonify({'ok': True, 'url': session.url, 'session_id': session.id, 'customer_id': customer_id})
    except Exception as e:
        log.error('Stripe WC checkout error: %s', e)
        return jsonify({'ok': False, 'error': 'Payment system error. Please try again.'}), 502


@bp.post('/api/wc/webhook')
def wc_webhook():
    """Stripe webhook for B2B Website Controller purchases.
    On checkout.session.completed → provision the customer (folder, API key, embed code).
    On invoice.payment_failed → flag account, notify owner.

    NOTE: this endpoint reads STRIPE_WC_WEBHOOK_SECRET (its own signing secret),
    NOT the consumer STRIPE_WEBHOOK_SECRET used by /stripe_webhook. The two webhooks
    have distinct signing secrets so they can co-exist in Stripe."""
    stripe_key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not stripe_key:
        return jsonify({'error': 'not configured'}), 500
    payload = request.get_data()
    sig = request.headers.get('Stripe-Signature', '')
    # Use the B2B-specific signing secret. Falls back to the consumer one for backward
    # compatibility during the cutover period if only one is set.
    webhook_secret = (os.environ.get('STRIPE_WC_WEBHOOK_SECRET', '')
                      or os.environ.get('STRIPE_WEBHOOK_SECRET', ''))
    try:
        import stripe as _stripe
        _stripe.api_key = stripe_key
        if webhook_secret:
            event = _stripe.Webhook.construct_event(payload, sig, webhook_secret)
        else:
            event = _stripe.Event.construct_from(json.loads(payload), _stripe.api_key)
    except Exception as e:
        log.warning('WC webhook error: %s', e)
        return jsonify({'error': str(e)}), 400

    event_type = event.get('type', '')
    if event_type == 'checkout.session.completed':
        s = event['data']['object']
        meta = s.get('metadata') or {}
        cust = s.get('customer_details') or {}
        customer_id = meta.get('customer_id') or _safe_customer_id(
            meta.get('business_name', ''), cust.get('email', '')
        )
        tier = meta.get('tier', 'starter')
        owner_email = cust.get('email', '') or meta.get('owner_email', '')
        business_name = meta.get('business_name', '')
        business_website = meta.get('business_website', '')
        log.info('WC PAYMENT CONFIRMED: customer=%s tier=%s email=%s', customer_id, tier, owner_email)

        # Persist initial profile so the embed code's greeting uses the business name
        if business_name or business_website:
            cdir = _cust_dir(customer_id)
            profile_path = cdir / 'business_profile.json'
            profile = _read(profile_path, {})
            if business_name and not profile.get('name'):
                profile['name'] = business_name
            if business_website and not profile.get('website'):
                profile['website'] = business_website
            profile['created_at'] = profile.get('created_at') or _now_iso()
            profile['updated_at'] = _now_iso()
            _atomic_write(profile_path, profile)

        # Provision: API key + owner token + instance record
        key_entry = _get_or_create_api_key(customer_id, 'website_controller')
        owner_rec = _get_or_create_owner_token(customer_id, owner_email)
        # Founding member status — read from the metadata we set at checkout time
        # (so the post-payment record matches the discount the customer actually paid).
        founding_member = (meta.get('founding_member') == 'true')
        try:
            founding_number = int(meta.get('founding_member_number') or 0) or None
        except Exception:
            founding_number = None
        try:
            setup_fee_paid_cents = int(meta.get('setup_fee_paid_cents') or 0) or WC_SETUP_FEE_CENTS
        except Exception:
            setup_fee_paid_cents = WC_SETUP_FEE_CENTS
        with _lock:
            inst_path = _cust_dir(customer_id) / 'instances.json'
            instances = _read(inst_path, [])
            found = False
            for inst in instances:
                if inst.get('product') == 'website_controller':
                    inst['tier'] = tier
                    inst['stripe_subscription_id'] = s.get('subscription', '')
                    inst['updated_at'] = _now_iso()
                    found = True
                    break
            if not found:
                instances.append({
                    'product': 'website_controller',
                    'tier': tier,
                    'stripe_subscription_id': s.get('subscription', ''),
                    'stripe_checkout_session_id': s.get('id', ''),
                    'created_at': _now_iso(),
                    'updated_at': _now_iso(),
                    'monthly_usage': 0,
                    'usage_period_start': _now_iso(),
                    'founding_member': founding_member,
                    'founding_member_number': founding_number,
                    'setup_fee_paid_cents': setup_fee_paid_cents,
                })
            _atomic_write(inst_path, instances)
        if founding_member:
            log.info('FOUNDING MEMBER #%s: customer=%s tier=%s paid_setup=$%.2f (50%% off)',
                     founding_number, customer_id, tier, setup_fee_paid_cents / 100.0)

        # Build the welcome payload (next task will email this — for now we log+save)
        biz = _read(_cust_dir(customer_id) / 'business_profile.json', {})
        embed_code = _build_embed_snippet(customer_id, key_entry['api_key'], 'website_controller', biz)
        delivery = {
            'customer_id': customer_id,
            'owner_email': owner_email,
            'business_name': business_name,
            'business_website': business_website,
            'tier': tier,
            'api_key': key_entry['api_key'],
            'embed_code': embed_code,
            'dashboard_url': _dashboard_url(owner_rec['owner_token']),
            'stripe_session_id': s.get('id', ''),
            'created_at': _now_iso(),
        }
        delivery_path = _cust_dir(customer_id) / 'delivery.json'
        _atomic_write(delivery_path, delivery)
        log.info('WC PROVISIONED: customer=%s dashboard=%s', customer_id, delivery['dashboard_url'])

        # Run the scraper in the background to enrich the business profile
        # (best-effort — failure shouldn't break the provisioning)
        if business_website:
            def _scrape_in_bg(cid, url):
                try:
                    # Scraper is shipped with the twickell deploy at modules/business/scraper/
                    from modules.business.scraper.site_scraper import SiteScraper
                    result = SiteScraper(max_pages=15).scrape(url)
                    if not result.get('ok', True):
                        log.warning('Scrape failed for %s: %s', cid, result.get('error'))
                        return
                    scraped_profile = result.get('business_profile', {})
                    if scraped_profile:
                        with _lock:
                            p = cdir / 'business_profile.json'
                            cur = _read(p, {})
                            # Only fill in fields that aren't already set
                            for k, v in scraped_profile.items():
                                if k not in cur or cur[k] in (None, '', [], {}):
                                    cur[k] = v
                            cur['updated_at'] = _now_iso()
                            _atomic_write(p, cur)
                        log.info('Scrape enriched profile for %s', cid)
                except Exception as e:
                    log.warning('Background scrape failed for %s: %s', cid, e)
            threading.Thread(target=_scrape_in_bg, args=(customer_id, business_website),
                             daemon=True, name=f'scrape_{customer_id}').start()

        # Send welcome email (with embed code + dashboard link)
        try:
            email_result = send_welcome_email(delivery)
            if not email_result.get('ok'):
                log.warning('Welcome email failed for %s: %s', customer_id, email_result.get('error'))
        except Exception as e:
            log.warning('Welcome email exception for %s: %s', customer_id, e)

    elif event_type == 'invoice.payment_failed':
        s = event['data']['object']
        meta = s.get('subscription_details', {}).get('metadata', {}) if isinstance(s.get('subscription_details'), dict) else {}
        customer_id = meta.get('customer_id', '')
        if customer_id:
            log.warning('WC payment failed: customer=%s', customer_id)
            # Drop a notification for the owner dashboard
            with _lock:
                notif_path = _cust_dir(customer_id) / 'notifications.json'
                items = _read(notif_path, [])
                items.append({
                    'id': str(uuid.uuid4())[:8],
                    'kind': 'billing_issue',
                    'urgency': 'high',
                    'summary': 'Your latest payment failed. Update your card to keep Orby running.',
                    'recommended_action': 'Open Stripe customer portal and update payment method.',
                    'created_at': _now_iso(),
                    'delivered_at': '',
                    'acknowledged_at': '',
                })
                _atomic_write(notif_path, items)

    return jsonify({'received': True, 'type': event_type})


@bp.get('/wc/success')
def wc_success_page():
    """Customer lands here after Stripe payment. Show the embed code + dashboard link.
    The actual provisioning happens via the webhook, but this page can poll for it."""
    session_id = (request.args.get('session_id') or '').strip()
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Welcome to Orby</title>'
        '<style>body{font-family:-apple-system,sans-serif;background:#0a0f1e;color:#e8eaf0;'
        'margin:0;padding:0;display:flex;align-items:center;justify-content:center;min-height:100vh;}'
        '.box{max-width:640px;padding:48px;background:#111827;border:1px solid rgba(212,160,23,0.3);'
        'border-radius:16px;text-align:center;}'
        'h1{background:linear-gradient(135deg,#d4a017,#f0c040);-webkit-background-clip:text;'
        'background-clip:text;color:transparent;margin:0 0 16px;}'
        '.muted{color:#8892a4;line-height:1.6;}'
        '</style></head><body><div class="box">'
        '<h1>Welcome to Orby</h1>'
        '<p class="muted">Your payment was received. Within the next minute we\'ll email you your '
        'embed code and your owner dashboard link. The email goes to the address you used at checkout.</p>'
        '<p class="muted" style="margin-top:24px;font-size:13px;">Reference: ' + session_id + '</p>'
        '</div></body></html>'
    )


@bp.get('/wc/cancel')
def wc_cancel_page():
    return ('<!DOCTYPE html><html><body style="font-family:sans-serif;text-align:center;padding:60px;">'
            '<h1>Checkout cancelled</h1>'
            '<p>No charge was made. <a href="/">Return to the home page</a> if you want to try again.</p>'
            '</body></html>')


@bp.get('/api/wc/delivery/<customer_id>')
def wc_get_delivery(customer_id):
    """Internal admin endpoint — fetch the delivery payload for an existing customer.
    Used by the email-sending background job or admin dashboard."""
    delivery = _read(_cust_dir(customer_id) / 'delivery.json', None)
    if not delivery:
        return jsonify({'ok': False, 'error': 'no delivery for this customer'}), 404
    return jsonify({'ok': True, 'delivery': delivery})


# ── Self-heal: restart flags ───────────────────────────────────────────────

@bp.post('/api/products/<product_key>/restart')
def flag_restart(product_key):
    """Mark a product for restart. Its watchdog polls /restart-pending."""
    data = request.get_json(silent=True) or {}
    reason = data.get('reason', 'manual')
    with _lock:
        flags = _read(RESTART_FLAGS_FILE, {})
        flags[product_key] = {
            'flagged_at': _now_iso(),
            'reason': reason,
            'consumed': False,
        }
        _atomic_write(RESTART_FLAGS_FILE, flags)
    return jsonify({'ok': True, 'product_key': product_key, 'reason': reason})


@bp.get('/api/products/<product_key>/restart-pending')
def check_restart(product_key):
    """Watchdog polls this. If a flag is set and not yet consumed, returns it
    and marks it consumed (so the watchdog only restarts once per flag)."""
    with _lock:
        flags = _read(RESTART_FLAGS_FILE, {})
        flag = flags.get(product_key)
        if flag and not flag.get('consumed'):
            flag['consumed'] = True
            flag['consumed_at'] = _now_iso()
            flags[product_key] = flag
            _atomic_write(RESTART_FLAGS_FILE, flags)
            return jsonify({'restart': True, 'flag': flag})
    return jsonify({'restart': False})


# Module-only — no standalone server. Use register_bridge_routes(app) from the
# host Flask application (see twickell_deploy/app.py for the call site).
