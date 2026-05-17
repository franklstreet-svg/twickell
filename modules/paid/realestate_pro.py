"""Real Estate Pro — Listings, clients, showings, offers, and commission calculations."""
MODULE_MANIFEST = {
    "id": "I003",
    "name": "Real Estate Pro",
    "category": "Industry",
    "description": "Full real estate workflow management. Track listings, buyers and sellers, showings, offers, and calculate commissions — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_listing", "list_listings", "update_listing",
              "add_re_client", "list_re_clients", "add_showing",
              "list_showings", "add_offer", "calculate_commission"],
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

def add_listing(profile_dir: str, address: str, price: float, bedrooms: int,
                bathrooms: float, sqft: int, status: str = 'active',
                notes: str = '') -> str:
    path = _path(profile_dir, 're_listings.json')
    listings = _load(path, [])
    listing = {
        'id': _uid(),
        'address': address,
        'price': price,
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'sqft': sqft,
        'status': status,
        'notes': notes,
        'created_at': _now()
    }
    listings.append(listing)
    _save(path, listings)
    return (f"Listing added [id:{listing['id']}] — {address}, "
            f"${price:,.0f}, {bedrooms}bd/{bathrooms}ba, {sqft:,} sqft. Status: {status}.")


def list_listings(profile_dir: str, status: str = '') -> str:
    path = _path(profile_dir, 're_listings.json')
    listings = _load(path, [])
    if status:
        listings = [l for l in listings if l.get('status', '').lower() == status.lower()]
    if not listings:
        return f"No listings found{' with status ' + status if status else ''}."
    lines = [f"Listings ({len(listings)}):"]
    for l in listings:
        lines.append(f"  [{l['id']}] {l['address']} — ${l.get('price',0):,.0f} "
                     f"({l.get('bedrooms',0)}bd/{l.get('bathrooms',0)}ba) [{l.get('status','active')}]")
    return '\n'.join(lines)


def update_listing(profile_dir: str, listing_id: str, status: str = '',
                   price: float = 0, notes: str = '') -> str:
    path = _path(profile_dir, 're_listings.json')
    listings = _load(path, [])
    listing = next((l for l in listings if l['id'] == listing_id), None)
    if not listing:
        return f"Listing '{listing_id}' not found."
    if status:
        listing['status'] = status
    if price:
        listing['price'] = price
    if notes:
        listing['notes'] = (listing.get('notes', '') + '\n' + notes).strip()
    listing['updated_at'] = _now()
    _save(path, listings)
    return f"Listing {listing_id} updated — {listing['address']}."


def add_re_client(profile_dir: str, name: str, type: str, phone: str,
                  email: str = '', budget: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 're_clients.json')
    clients = _load(path, [])
    client = {
        'id': _uid(),
        'name': name,
        'type': type.lower(),  # buyer or seller
        'phone': phone,
        'email': email,
        'budget': budget,
        'notes': notes,
        'created_at': _now()
    }
    clients.append(client)
    _save(path, clients)
    return f"Client added [id:{client['id']}] — {name} ({type})" + (f", budget: {budget}" if budget else "") + "."


def list_re_clients(profile_dir: str, type: str = '') -> str:
    path = _path(profile_dir, 're_clients.json')
    clients = _load(path, [])
    if type:
        clients = [c for c in clients if c.get('type', '').lower() == type.lower()]
    if not clients:
        return f"No clients found{' of type ' + type if type else ''}."
    lines = [f"Clients ({len(clients)}):"]
    for c in clients:
        budget_str = f", budget: {c['budget']}" if c.get('budget') else ''
        lines.append(f"  [{c['id']}] {c['name']} ({c.get('type','')}) — {c.get('phone','')}{budget_str}")
    return '\n'.join(lines)


def add_showing(profile_dir: str, listing_id: str, client_name: str,
                date: str, time: str, notes: str = '') -> str:
    path = _path(profile_dir, 're_showings.json')
    showings = _load(path, [])
    # Verify listing exists
    listings = _load(_path(profile_dir, 're_listings.json'), [])
    listing = next((l for l in listings if l['id'] == listing_id), None)
    address = listing['address'] if listing else listing_id
    showing = {
        'id': _uid(),
        'listing_id': listing_id,
        'address': address,
        'client_name': client_name,
        'date': date,
        'time': time,
        'notes': notes,
        'created_at': _now()
    }
    showings.append(showing)
    _save(path, showings)
    return f"Showing scheduled [id:{showing['id']}] — {client_name} at {address} on {date} at {time}."


def list_showings(profile_dir: str, upcoming_only: bool = True) -> str:
    path = _path(profile_dir, 're_showings.json')
    showings = _load(path, [])
    today = _now()[:10]
    if upcoming_only:
        showings = [s for s in showings if s.get('date', '') >= today]
    if not showings:
        return "No showings found."
    showings = sorted(showings, key=lambda s: (s.get('date', ''), s.get('time', '')))
    lines = [f"Showings ({len(showings)}):"]
    for s in showings:
        lines.append(f"  [{s['id']}] {s['date']} {s['time']} — {s['client_name']} @ {s.get('address', s['listing_id'])}")
    return '\n'.join(lines)


def add_offer(profile_dir: str, listing_id: str, client_name: str,
              amount: float, date: str, status: str = 'pending',
              notes: str = '') -> str:
    path = _path(profile_dir, 're_offers.json')
    offers = _load(path, [])
    listings = _load(_path(profile_dir, 're_listings.json'), [])
    listing = next((l for l in listings if l['id'] == listing_id), None)
    address = listing['address'] if listing else listing_id
    offer = {
        'id': _uid(),
        'listing_id': listing_id,
        'address': address,
        'client_name': client_name,
        'amount': amount,
        'date': date,
        'status': status,
        'notes': notes,
        'created_at': _now()
    }
    offers.append(offer)
    _save(path, offers)
    return (f"Offer recorded [id:{offer['id']}] — {client_name} offered ${amount:,.0f} "
            f"on {address} ({status}).")


def calculate_commission(profile_dir: str, sale_price: float,
                         commission_rate: float = 6.0) -> str:
    total = sale_price * (commission_rate / 100)
    agent_split = total / 2
    broker_split = total / 2
    return (f"Commission on ${sale_price:,.0f} sale at {commission_rate}%:\n"
            f"  Total commission: ${total:,.2f}\n"
            f"  Agent split (50%): ${agent_split:,.2f}\n"
            f"  Broker split (50%): ${broker_split:,.2f}")


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    listings = _load(_path(profile_dir, 're_listings.json'), [])
    if not listings:
        return ''
    active = [l for l in listings if l.get('status') == 'active']
    pending = [l for l in listings if l.get('status') == 'pending']
    today = _now()[:10]
    showings = _load(_path(profile_dir, 're_showings.json'), [])
    today_showings = [s for s in showings if s.get('date', '') == today]
    parts = [f"REAL ESTATE PRO ACTIVE: {len(active)} active listing(s), {len(pending)} pending"]
    if today_showings:
        parts.append(f"  {len(today_showings)} showing(s) today")
    parts.append(
        "What I can help you with:\n"
        "  Track: listings, buyers/sellers, showings, offers, commissions, transaction timelines\n"
        "  Write: property listing descriptions, buyer/seller cover letters, offer letters,\n"
        "         follow-up emails, client update letters, market analysis summaries,\n"
        "         contract summaries, showing feedback letters, referral thank-you letters\n"
        "  Know: commission calculations, general real estate transaction process,\n"
        "        offer/counteroffer conventions"
    )
    return '\n'.join(parts)
