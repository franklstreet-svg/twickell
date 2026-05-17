"""Product development tracker — ideas, features, roadmaps, notes, launch checklists."""
MODULE_MANIFEST = {
    "id": "P001",
    "name": "Product Development",
    "category": "Productivity",
    "description": "Track products, features, roadmaps, ideas, launch checklists, and product notes. Full product management suite for entrepreneurs and builders.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["create_product", "get_products", "update_product_status", "add_feature",
              "get_features", "update_feature_status", "save_product_idea", "get_product_ideas",
              "add_product_note", "get_product_dashboard", "add_launch_checklist",
              "get_launch_checklist", "complete_checklist_item"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
from pathlib import Path


def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'skills'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename


def _load(path: Path, default) -> list | dict:
    if path.exists():
        return json.loads(path.read_text())
    return default


def _save(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now() -> str:
    return datetime.utcnow().isoformat()


def _uid() -> str:
    return str(uuid.uuid4())[:8]


# ── PRODUCTS ──────────────────────────────────────────────────────────────────

def add_product(profile_dir: str, name: str, description: str = '',
                status: str = 'in_development', target_audience: str = '',
                revenue_model: str = '', industry_tags: str = '',
                linked_modules: str = '') -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    existing = next((p for p in products if p['name'].lower() == name.lower()), None)
    if existing:
        return f"'{name}' already exists (id: {existing['id']}). Use add_product_note to add notes to it."
    product = {
        'id': _uid(),
        'name': name,
        'description': description,
        'status': status,
        'target_audience': target_audience,
        'revenue_model': revenue_model,
        'industry_tags': [t.strip() for t in industry_tags.split(',') if t.strip()] if industry_tags else [],
        'linked_modules': [m.strip() for m in linked_modules.split(',') if m.strip()] if linked_modules else [],
        'created': _now(),
        'notes': []
    }
    products.append(product)
    _save(path, products)
    return f"Product '{name}' added (id: {product['id']}, status: {status})."


def update_product(profile_dir: str, product_id: str,
                   description: str = '', target_audience: str = '',
                   revenue_model: str = '', industry_tags: str = '',
                   linked_modules: str = '') -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    p = _find_product(products, product_id)
    if not p:
        return f"Product '{product_id}' not found."
    changed = []
    if description:
        p['description'] = description
        changed.append('description')
    if target_audience:
        p['target_audience'] = target_audience
        changed.append('target_audience')
    if revenue_model:
        p['revenue_model'] = revenue_model
        changed.append('revenue_model')
    if industry_tags:
        p['industry_tags'] = [t.strip() for t in industry_tags.split(',') if t.strip()]
        changed.append('industry_tags')
    if linked_modules:
        p['linked_modules'] = [m.strip() for m in linked_modules.split(',') if m.strip()]
        changed.append('linked_modules')
    _save(path, products)
    return f"'{p['name']}' updated: {', '.join(changed)}."


def get_products(profile_dir: str, status_filter: str = '') -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    if status_filter:
        products = [p for p in products if p['status'] == status_filter]
    if not products:
        return "No products found." if not status_filter else f"No products with status '{status_filter}'."
    lines = []
    for p in products:
        lines.append(f"[{p['id']}] {p['name']} — {p['status'].replace('_',' ').upper()}")
        if p.get('description'):
            lines.append(f"    {p['description']}")
        if p.get('target_audience'):
            lines.append(f"    Audience: {p['target_audience']}")
        if p.get('revenue_model'):
            lines.append(f"    Revenue: {p['revenue_model']}")
        if p.get('industry_tags'):
            lines.append(f"    Industries: {', '.join(p['industry_tags'])}")
        if p.get('linked_modules'):
            lines.append(f"    Modules: {', '.join(p['linked_modules'])}")
        note_count = len(p.get('notes', []))
        if note_count:
            lines.append(f"    {note_count} note(s)")
    return '\n'.join(lines)


def get_product_dashboard(profile_dir: str) -> str:
    """Status dashboard — all products at a glance with feature counts and checklist progress."""
    products_path   = _path(profile_dir, 'products.json')
    features_path   = _path(profile_dir, 'features.json')
    checklists_path = _path(profile_dir, 'launch_checklists.json')
    ideas_path      = _path(profile_dir, 'product_ideas.json')

    products   = _load(products_path, [])
    features   = _load(features_path, [])
    checklists = _load(checklists_path, [])
    ideas      = _load(ideas_path, [])

    if not products:
        return "No products tracked yet. Use add_product to get started."

    STATUS_ICON = {
        'in_development': '🔨',
        'launched': '✓',
        'idea': '💡',
        'paused': '⏸',
    }

    lines = ['══════════ PRODUCT DASHBOARD ══════════']
    for p in products:
        icon  = STATUS_ICON.get(p['status'], '?')
        lines.append(f"\n{icon}  {p['name'].upper()}  [{p['status'].replace('_',' ')}]")
        if p.get('description'):
            lines.append(f"     {p['description']}")
        if p.get('target_audience'):
            lines.append(f"     → Audience: {p['target_audience']}")
        if p.get('revenue_model'):
            lines.append(f"     → Revenue:  {p['revenue_model']}")
        if p.get('industry_tags'):
            lines.append(f"     → Industry: {', '.join(p['industry_tags'])}")
        if p.get('linked_modules'):
            lines.append(f"     → Modules:  {', '.join(p['linked_modules'])}")

        feats = [f for f in features if p['name'].lower() in f['product'].lower()]
        if feats:
            built   = sum(1 for f in feats if f['status'] == 'built')
            in_prog = sum(1 for f in feats if f['status'] == 'in_progress')
            planned = sum(1 for f in feats if f['status'] == 'planned')
            free_c  = sum(1 for f in feats if f['tier'] == 'free')
            paid_c  = sum(1 for f in feats if f['tier'] == 'paid')
            lines.append(f"     → Features: {built} built · {in_prog} in progress · {planned} planned  |  {free_c} free · {paid_c} paid")
            in_progress_names = [f['name'] for f in feats if f['status'] == 'in_progress']
            if in_progress_names:
                lines.append(f"     → In progress: {', '.join(in_progress_names)}")

        prod_checks = [c for c in checklists if p['name'].lower() in c.get('product', '').lower()]
        for c in prod_checks:
            done  = sum(1 for i in c.get('items', []) if i.get('done'))
            total = len(c.get('items', []))
            pct   = int(done / total * 100) if total else 0
            bar   = '█' * (pct // 10) + '░' * (10 - pct // 10)
            lines.append(f"     → Launch '{c['name']}': [{bar}] {done}/{total}")

        prod_ideas = [i for i in ideas
                      if p['name'].lower() in i.get('product', '').lower()
                      and i['status'] == 'new']
        if prod_ideas:
            lines.append(f"     → {len(prod_ideas)} unreviewed idea(s)")

    lines.append('\n═══════════════════════════════════════')
    return '\n'.join(lines)


def update_product_status(profile_dir: str, product_id: str, status: str) -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    p = _find_product(products, product_id)
    if not p:
        return f"Product '{product_id}' not found."
    old = p['status']
    p['status'] = status
    _save(path, products)
    return f"'{p['name']}' status updated: {old} → {status}."


def add_product_note(profile_dir: str, product_id: str, content: str) -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    p = _find_product(products, product_id)
    if not p:
        return f"Product '{product_id}' not found. Use get_products to see product IDs."
    note = {'id': _uid(), 'content': content, 'date': _now()}
    p.setdefault('notes', []).append(note)
    _save(path, products)
    return f"Note saved to '{p['name']}'."


def get_product_notes(profile_dir: str, product_id: str) -> str:
    path = _path(profile_dir, 'products.json')
    products = _load(path, [])
    p = _find_product(products, product_id)
    if not p:
        return f"Product '{product_id}' not found."
    notes = p.get('notes', [])
    if not notes:
        return f"No notes for '{p['name']}' yet."
    lines = [f"Notes for '{p['name']}':"]
    for n in notes[-20:]:
        date = n['date'][:10]
        lines.append(f"  [{date}] {n['content']}")
    return '\n'.join(lines)


def _find_product(products: list, identifier: str):
    return (
        next((p for p in products if p['id'] == identifier), None)
        or next((p for p in products if p['name'].lower() == identifier.lower()), None)
        or next((p for p in products if identifier.lower() in p['name'].lower()), None)
    )


# ── IDEAS ─────────────────────────────────────────────────────────────────────

def save_product_idea(profile_dir: str, idea: str, product: str = '') -> str:
    path = _path(profile_dir, 'product_ideas.json')
    ideas = _load(path, [])
    entry = {
        'id': _uid(),
        'idea': idea,
        'product': product,
        'captured': _now(),
        'status': 'new'
    }
    ideas.append(entry)
    _save(path, ideas)
    tag = f" (for '{product}')" if product else ""
    return f"Idea captured{tag}: \"{idea}\""


def get_product_ideas(profile_dir: str, product: str = '',
                      status_filter: str = '') -> str:
    path = _path(profile_dir, 'product_ideas.json')
    ideas = _load(path, [])
    if product:
        ideas = [i for i in ideas if product.lower() in i.get('product', '').lower()]
    if status_filter:
        ideas = [i for i in ideas if i['status'] == status_filter]
    if not ideas:
        return "No ideas found."
    lines = []
    for i in ideas[-30:]:
        date = i['captured'][:10]
        tag = f" [{i['product']}]" if i.get('product') else ""
        status = f" ({i['status']})" if i['status'] != 'new' else ""
        lines.append(f"  [{i['id']}]{tag} {i['idea']}{status} — {date}")
    return f"{len(ideas)} idea(s):\n" + '\n'.join(lines)


def update_idea_status(profile_dir: str, idea_id: str, status: str) -> str:
    path = _path(profile_dir, 'product_ideas.json')
    ideas = _load(path, [])
    idea = next((i for i in ideas if i['id'] == idea_id), None)
    if not idea:
        return f"Idea '{idea_id}' not found."
    idea['status'] = status
    _save(path, ideas)
    return f"Idea marked as '{status}'."


# ── FEATURES ──────────────────────────────────────────────────────────────────

def add_feature(profile_dir: str, product: str, name: str,
                description: str = '', tier: str = 'free',
                status: str = 'planned', priority: str = 'normal') -> str:
    path = _path(profile_dir, 'features.json')
    features = _load(path, [])
    feature = {
        'id': _uid(),
        'product': product,
        'name': name,
        'description': description,
        'tier': tier,
        'status': status,
        'priority': priority,
        'created': _now(),
        'updated': _now()
    }
    features.append(feature)
    _save(path, features)
    return f"Feature '{name}' added to '{product}' [{tier}, {status}]."


def update_feature_status(profile_dir: str, feature_id: str, status: str) -> str:
    path = _path(profile_dir, 'features.json')
    features = _load(path, [])
    feat = next((f for f in features
                 if f['id'] == feature_id
                 or f['name'].lower() == feature_id.lower()
                 or feature_id.lower() in f['name'].lower()), None)
    if not feat:
        return f"Feature '{feature_id}' not found."
    old = feat['status']
    feat['status'] = status
    feat['updated'] = _now()
    _save(path, features)
    return f"'{feat['name']}' status: {old} → {status}."


def get_features(profile_dir: str, product: str = '',
                 status_filter: str = '', tier_filter: str = '') -> str:
    path = _path(profile_dir, 'features.json')
    features = _load(path, [])
    if product:
        features = [f for f in features if product.lower() in f['product'].lower()]
    if status_filter:
        features = [f for f in features if f['status'] == status_filter]
    if tier_filter:
        features = [f for f in features if f['tier'] == tier_filter]
    if not features:
        return "No features found."

    STATUS_ORDER = ['built', 'in_progress', 'planned', 'backlog']
    features.sort(key=lambda f: (STATUS_ORDER.index(f['status'])
                                 if f['status'] in STATUS_ORDER else 99,
                                 f['product']))

    lines = []
    current_product = None
    for f in features:
        if f['product'] != current_product:
            current_product = f['product']
            lines.append(f"\n{current_product}:")
        icon = {'built': '✓', 'in_progress': '→', 'planned': '○', 'backlog': '·'}.get(f['status'], '?')
        tier_tag = '[PAID]' if f['tier'] == 'paid' else '[free]'
        pri_tag = ' !' if f['priority'] == 'high' else ''
        lines.append(f"  {icon} {tier_tag} [{f['id']}] {f['name']}{pri_tag}")
        if f.get('description'):
            lines.append(f"      {f['description']}")
    return '\n'.join(lines).strip()


# ── ROADMAP ───────────────────────────────────────────────────────────────────

def get_roadmap(profile_dir: str, product: str = '') -> str:
    """Full picture: product status, features by tier and status, checklist summary."""
    products_path = _path(profile_dir, 'products.json')
    features_path = _path(profile_dir, 'features.json')
    checklists_path = _path(profile_dir, 'launch_checklists.json')

    all_products = _load(products_path, [])
    all_features = _load(features_path, [])
    all_checklists = _load(checklists_path, [])

    if product:
        all_products = [p for p in all_products
                        if product.lower() in p['name'].lower()]

    if not all_products:
        return "No products found. Add one with add_product."

    lines = ['── PRODUCT ROADMAP ──────────────────────────────']
    for prod in all_products:
        lines.append(f"\n{prod['name'].upper()} — {prod['status'].replace('_',' ')}")
        if prod.get('description'):
            lines.append(f"  {prod['description']}")

        feats = [f for f in all_features if prod['name'].lower() in f['product'].lower()]
        if feats:
            built     = [f for f in feats if f['status'] == 'built']
            in_prog   = [f for f in feats if f['status'] == 'in_progress']
            planned   = [f for f in feats if f['status'] == 'planned']
            backlog   = [f for f in feats if f['status'] == 'backlog']
            free_feats = [f for f in feats if f['tier'] == 'free']
            paid_feats = [f for f in feats if f['tier'] == 'paid']
            lines.append(f"  Features: {len(built)} built · {len(in_prog)} in progress · {len(planned)} planned · {len(backlog)} backlog")
            lines.append(f"  Tier split: {len(free_feats)} free · {len(paid_feats)} paid")
            if in_prog:
                lines.append("  In progress: " + ", ".join(f['name'] for f in in_prog))
            if planned:
                hi = [f for f in planned if f['priority'] == 'high']
                if hi:
                    lines.append("  High priority next: " + ", ".join(f['name'] for f in hi))

        checks = [c for c in all_checklists if prod['name'].lower() in c.get('product', '').lower()]
        for c in checks:
            total = len(c.get('items', []))
            done  = sum(1 for i in c.get('items', []) if i.get('done'))
            lines.append(f"  Launch checklist '{c['name']}': {done}/{total} complete")

    return '\n'.join(lines)


# ── LAUNCH CHECKLISTS ─────────────────────────────────────────────────────────

def add_launch_checklist(profile_dir: str, product: str, name: str,
                         items: list | None = None) -> str:
    path = _path(profile_dir, 'launch_checklists.json')
    checklists = _load(path, [])
    checklist = {
        'id': _uid(),
        'product': product,
        'name': name,
        'created': _now(),
        'items': []
    }
    if items:
        for item_text in items:
            checklist['items'].append({'id': _uid(), 'task': item_text, 'done': False})
    checklists.append(checklist)
    _save(path, checklists)
    count = len(checklist['items'])
    return f"Launch checklist '{name}' created for '{product}'" + (f" with {count} items." if count else ".")


def add_checklist_item(profile_dir: str, checklist_id: str, task: str) -> str:
    path = _path(profile_dir, 'launch_checklists.json')
    checklists = _load(path, [])
    cl = _find_checklist(checklists, checklist_id)
    if not cl:
        return f"Checklist '{checklist_id}' not found."
    cl.setdefault('items', []).append({'id': _uid(), 'task': task, 'done': False})
    _save(path, checklists)
    return f"Added to '{cl['name']}': {task}"


def complete_checklist_item(profile_dir: str, checklist_id: str, item_id: str) -> str:
    path = _path(profile_dir, 'launch_checklists.json')
    checklists = _load(path, [])
    cl = _find_checklist(checklists, checklist_id)
    if not cl:
        return f"Checklist '{checklist_id}' not found."
    item = next((i for i in cl.get('items', [])
                 if i['id'] == item_id
                 or item_id.lower() in i['task'].lower()), None)
    if not item:
        return f"Item '{item_id}' not found in checklist."
    item['done'] = True
    _save(path, checklists)
    done  = sum(1 for i in cl['items'] if i['done'])
    total = len(cl['items'])
    return f"✓ '{item['task']}' done. Checklist: {done}/{total} complete."


def get_launch_checklist(profile_dir: str, checklist_id: str) -> str:
    path = _path(profile_dir, 'launch_checklists.json')
    checklists = _load(path, [])
    if not checklist_id:
        if not checklists:
            return "No launch checklists yet."
        lines = ["Launch checklists:"]
        for c in checklists:
            done  = sum(1 for i in c.get('items', []) if i.get('done'))
            total = len(c.get('items', []))
            lines.append(f"  [{c['id']}] {c['name']} ({c['product']}) — {done}/{total}")
        return '\n'.join(lines)

    cl = _find_checklist(checklists, checklist_id)
    if not cl:
        return f"Checklist '{checklist_id}' not found."
    done  = sum(1 for i in cl.get('items', []) if i.get('done'))
    total = len(cl.get('items', []))
    lines = [f"'{cl['name']}' for {cl['product']} — {done}/{total} complete"]
    for item in cl.get('items', []):
        mark = '✓' if item['done'] else '○'
        lines.append(f"  {mark} [{item['id']}] {item['task']}")
    return '\n'.join(lines)


def _find_checklist(checklists: list, identifier: str):
    return (
        next((c for c in checklists if c['id'] == identifier), None)
        or next((c for c in checklists if c['name'].lower() == identifier.lower()), None)
        or next((c for c in checklists if identifier.lower() in c['name'].lower()), None)
        or next((c for c in checklists if identifier.lower() in c.get('product', '').lower()), None)
    )


# ── CONTEXT SUMMARY ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    """Brief snapshot for the session context — active products and open ideas."""
    products_path = _path(profile_dir, 'products.json')
    ideas_path    = _path(profile_dir, 'product_ideas.json')

    products = _load(products_path, [])
    ideas    = _load(ideas_path, [])

    active  = [p for p in products if p['status'] in ('in_development', 'launched')]
    new_ideas = [i for i in ideas if i['status'] == 'new']

    if not active and not new_ideas:
        return ''

    parts = []
    if active:
        names = ', '.join(p['name'] for p in active)
        parts.append(f"ACTIVE PRODUCTS: {names}")
    if new_ideas:
        parts.append(f"UNREVIEWED IDEAS: {len(new_ideas)} waiting — use get_product_ideas to review")

    return '\n'.join(parts)
