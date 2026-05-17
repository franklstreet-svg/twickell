"""Trade Specialties — Plumbing, electrical, HVAC, flooring, and roofing job management with trade code references."""
MODULE_MANIFEST = {
    "id": "I008",
    "name": "Trade Specialties",
    "category": "Industry",
    "description": "Job management for trade contractors — plumbing, electrical, HVAC, flooring, and roofing. Includes built-in code and specification references for each trade.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_trade_job", "list_trade_jobs", "add_trade_note",
              "get_common_codes", "add_material_order", "get_trade_summary"],
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


TRADE_CODES = {
    'plumbing': """PLUMBING QUICK REFERENCE:
Pipe Sizes (nominal): 1/2", 3/4", 1", 1-1/4", 1-1/2", 2", 2-1/2", 3", 4", 6"
Common DWV Fittings: 1/4 bend (90°), 1/8 bend (45°), wye, sanitary tee, P-trap, cleanout
Fixture Units (DFU): toilet=3, lavatory=1, bathtub=2, shower=2, kitchen sink=2, dishwasher=2, washing machine=3
Minimum Drain Slopes: 3" pipe = 1/8" per foot; 2" or less = 1/4" per foot
Water Supply Pressure: Residential max 80 PSI; PRV required >80 PSI
Hot Water Temp: Min 120°F at heater; scalding risk above 120°F for residences
Vent Requirements: Each fixture needs vent within 5 ft of trap (varies by code)
Common Codes: IPC (International Plumbing Code), UPC (Uniform Plumbing Code)
PEX: A/B/C types; expansion (A), crimp (B/C); min 1/2" supply""",

    'electrical': """ELECTRICAL QUICK REFERENCE:
AWG Wire Sizes (copper): 14 AWG=15A, 12 AWG=20A, 10 AWG=30A, 8 AWG=40A, 6 AWG=55A, 4 AWG=70A, 2 AWG=95A
Breaker Ratings: 15A (lighting/general), 20A (kitchen/bath), 30A (dryer/water heater), 40A (range), 50A (EV charger)
NEC Key References:
  210.8 — GFCI protection required (bathrooms, garages, outdoors, kitchens, crawlspaces)
  210.11 — Branch circuit requirements
  210.52 — Receptacle spacing (max 12 ft apart, within 6 ft of doorways in living areas)
  230.79 — Service entrance rating (min 100A residential)
  250.50 — Grounding electrode requirements
  410.16 — Luminaires in closets
Conduit: EMT (dry indoor), RMC (outdoor/wet), PVC (direct bury), FMC (flex)
AFCI: Required for bedrooms, living rooms, hallways (most habitable spaces)
GFCI: Required within 6 ft of water source
Panel: Neutral bar isolated from ground bar in sub-panels""",

    'hvac': """HVAC QUICK REFERENCE:
Refrigerants: R-410A (most common residential, phasing out), R-32 (newer, lower GWP), R-22 (discontinued, illegal to produce), R-454B (R-410A replacement)
SEER Ratings: Min 14 SEER (North), 15 SEER (South); high-efficiency = 18-25 SEER; SEER2 new standard
BTU Sizing Rule of Thumb: 1 ton = 12,000 BTU; ~400-600 sq ft per ton (varies by climate/insulation)
Duct Sizing: Supply trunk typically 14"-20"; branch ducts 6"-10"; return slightly larger than supply
Static Pressure: Residential systems typically 0.5" w.c. total; external static 0.1-0.2" per 100 ft
Filter MERV Ratings: MERV 8 = standard; MERV 11 = better allergen capture; MERV 13 = HEPA-like
Refrigerant Handling: EPA 608 certification required; Section 608 prohibits venting
Airflow: 400 CFM per ton; check with anemometer or flow hood
Superheat: 10-18°F for fixed orifice; Subcooling: 10-18°F for TXV systems
Freon Phase-Out: R-22 no new production since 2020; R-410A production limits starting 2025""",

    'flooring': """FLOORING QUICK REFERENCE:
Subfloor Requirements:
  Hardwood: Max deflection L/360; min 3/4" plywood or OSB; moisture <12%
  Tile: Min 1-1/4" combined; max deflection L/360 (ceramic), L/480 (large format)
  LVP/LVT: Flatness within 3/16" per 10 ft; fill low spots; can float over most surfaces
  Carpet: 7/16" pad max for berber; cushion improves R-value and comfort
Expansion Gaps:
  Hardwood: 1/2" at all walls; T-molding at doorways and transitions >20 ft
  LVP: 1/4" perimeter; use expansion clips if bridging rooms
  Tile: 1/8"-3/16" grout joints; movement joints at walls and every 8-12 ft
Moisture Testing: ASTM F2170 (in-situ RH probe); acceptable <75-80% RH for most products
Concrete Flatness: F-number system; FF25 typical residential; use self-leveler for low spots
Radiant Heat: LVP max 85°F surface; hardwood max 80°F; confirm with manufacturer
Transition Types: T-molding (same height), reducer (height change), end cap (to carpet/threshold)""",

    'roofing': """ROOFING QUICK REFERENCE:
Pitch Guidelines:
  Low slope: <2:12 — requires modified bitumen, TPO, EPDM, or built-up
  Conventional: 2:12 to 4:12 — requires special underlayment; check local code
  Steep slope: 4:12+ — standard shingles acceptable
  High pitch: 12:12+ — use starter strips, hand-nail in high-wind areas
Underlayment Specs:
  #15 felt — light duty, temp protection
  #30 felt — standard residential; 2-layer on pitches <4:12
  Synthetic — tear-resistant, moisture-resistant; 10-20x stronger than felt
  Self-adhering (ice & water shield) — required at eaves (2 ft inside heated envelope min), valleys, penetrations
Shingle Exposure: Standard 3-tab = 5"; architectural = 5-5/8" typical; verify with manufacturer
Flashing Rules:
  Step flashing at walls/dormers: 4" x 4" min; overlap each course
  Chimney: Cricket required if >30" wide; lead or aluminum flashing with counter-flashing
  Valley: Open metal min 24" wide; closed-cut or woven with min 36" felt
  Pipe boots: Neoprene; replace at re-roof; check for cracks > 5-7 years
Fasteners: Min 4 nails per 3-tab; 6 in high-wind zones (>90 mph); corrosion-resistant
Ice Dam Prevention: Ventilate ridge + soffit; air seal attic floor; add ice & water shield 2 ft past exterior wall
Load: Dead load ~3 psf for asphalt shingles; check structural for re-roof over existing"""
}


# ── Public tools ──────────────────────────────────────────────────────────────

def add_trade_job(profile_dir: str, trade: str, client_name: str,
                  address: str, description: str, status: str = 'active',
                  notes: str = '') -> str:
    path = _path(profile_dir, 'trade_jobs.json')
    jobs = _load(path, [])
    job = {
        'id': _uid(),
        'trade': trade.lower(),
        'client_name': client_name,
        'address': address,
        'description': description,
        'status': status,
        'notes': notes,
        'trade_notes': [],
        'material_orders': [],
        'created_at': _now()
    }
    jobs.append(job)
    _save(path, jobs)
    return f"Trade job added [id:{job['id']}] — {trade.title()}: {client_name} at {address}. Status: {status}."


def list_trade_jobs(profile_dir: str, trade: str = '') -> str:
    path = _path(profile_dir, 'trade_jobs.json')
    jobs = _load(path, [])
    if trade:
        jobs = [j for j in jobs if j.get('trade', '').lower() == trade.lower()]
    if not jobs:
        return f"No trade jobs found{' for ' + trade if trade else ''}."
    lines = [f"Trade jobs ({len(jobs)}):"]
    for j in jobs:
        lines.append(f"  [{j['id']}] [{j.get('trade','').upper()}] {j['client_name']} — {j['address']} [{j.get('status','active')}]")
    return '\n'.join(lines)


def add_trade_note(profile_dir: str, job_id: str, note: str,
                   code: str = '') -> str:
    path = _path(profile_dir, 'trade_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    entry = {'note': note, 'code': code, 'date': _now()}
    job.setdefault('trade_notes', []).append(entry)
    _save(path, jobs)
    return (f"Note added to job {job_id} ({job['client_name']})"
            + (f" [code: {code}]" if code else "") + ".")


def get_common_codes(profile_dir: str, trade: str) -> str:
    trade_key = trade.lower()
    if trade_key in TRADE_CODES:
        return TRADE_CODES[trade_key]
    available = ', '.join(TRADE_CODES.keys())
    return f"No codes available for '{trade}'. Available trades: {available}."


def add_material_order(profile_dir: str, job_id: str, supplier: str,
                       items: list, order_date: str = '',
                       status: str = 'ordered') -> str:
    """items: list of strings or dicts describing materials"""
    path = _path(profile_dir, 'trade_jobs.json')
    jobs = _load(path, [])
    job = next((j for j in jobs if j['id'] == job_id), None)
    if not job:
        return f"Job '{job_id}' not found."
    order = {
        'id': _uid(),
        'supplier': supplier,
        'items': items,
        'order_date': order_date or _now()[:10],
        'status': status,
        'created_at': _now()
    }
    job.setdefault('material_orders', []).append(order)
    _save(path, jobs)
    item_count = len(items)
    return (f"Material order added [id:{order['id']}] for job {job_id} — "
            f"{item_count} item(s) from {supplier}, status: {status}.")


def get_trade_summary(profile_dir: str, trade: str) -> str:
    path = _path(profile_dir, 'trade_jobs.json')
    jobs = _load(path, [])
    trade_jobs = [j for j in jobs if j.get('trade', '').lower() == trade.lower()]
    if not trade_jobs:
        return f"No {trade} jobs found."
    active = [j for j in trade_jobs if j.get('status') == 'active']
    # Pending orders across all trade jobs
    pending_orders = []
    for j in trade_jobs:
        for o in j.get('material_orders', []):
            if o.get('status') in ('ordered', 'pending'):
                pending_orders.append(f"{j['client_name']}: {o['supplier']} ({o.get('order_date','')})")
    # Recent activity
    recent = sorted(trade_jobs, key=lambda j: j.get('created_at', ''), reverse=True)[:3]
    lines = [
        f"{trade.title()} Summary:",
        f"  Total jobs: {len(trade_jobs)}",
        f"  Active: {len(active)}",
        f"  Completed: {len([j for j in trade_jobs if j.get('status') == 'completed'])}",
    ]
    if pending_orders:
        lines.append(f"  Pending material orders ({len(pending_orders)}):")
        for o in pending_orders[:5]:
            lines.append(f"    • {o}")
    if recent:
        lines.append(f"  Recent jobs:")
        for j in recent:
            lines.append(f"    • [{j['id']}] {j['client_name']} — {j['address']} [{j.get('status','active')}]")
    return '\n'.join(lines)


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    jobs = _load(_path(profile_dir, 'trade_jobs.json'), [])
    if not jobs:
        return ''
    active = [j for j in jobs if j.get('status') == 'active']
    trades = {}
    for j in active:
        t = j.get('trade', 'other')
        trades[t] = trades.get(t, 0) + 1
    trade_str = ', '.join(f"{t}: {n}" for t, n in trades.items())
    parts = [f"TRADE SPECIALTIES ACTIVE: {len(active)} active job(s) — {trade_str}"]
    parts.append(
        "What I can help you with:\n"
        "  Track: jobs, materials, permits, equipment/tools, inspection notes\n"
        "  Write: job quotes/estimates, material lists, inspection prep notes,\n"
        "         permit application notes, code violation remediation plans,\n"
        "         customer progress update letters, warranty letters\n"
        "  Know: trade-specific code references, material specs, permit basics,\n"
        "        inspection checklist standards per trade"
    )
    return '\n'.join(parts)
