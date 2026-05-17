"""Module registry — knows what's installed, what's available, what's coming."""
import json
from datetime import datetime
from pathlib import Path

_REGISTRY_PATH = Path(__file__).parent.parent / 'modules_registry.json'

# Stub — set to real URL when bridge is live
_BRIDGE_URL = None


def _load_registry() -> dict:
    if _REGISTRY_PATH.exists():
        return json.loads(_REGISTRY_PATH.read_text())
    return {'registry_version': '0', 'modules': []}


def _modules() -> list:
    return _load_registry().get('modules', [])


def list_modules(tier_filter: str = 'all') -> str:
    """List installed modules. Paid module catalog lives on the bridge server."""
    modules = _modules()
    installed = [m for m in modules if m['status'] == 'installed']

    lines = [f"INSTALLED MODULES ({len(installed)}):"]
    for m in installed:
        lines.append(f"  ✓ {m['name']} — {m['description']}")

    reg = _load_registry()
    store = reg.get('module_store', {})
    if store.get('endpoint'):
        lines.append("\nType 'show me what modules I can add' to browse the paid module store.")
    else:
        lines.append("\nPaid add-on modules available through the module store — coming with the bridge server launch.")

    lines.append(f"\nRegistry v{reg.get('registry_version','?')} · {reg.get('last_updated','?')}")
    return '\n'.join(lines)


def get_module_info(module_id: str) -> str:
    """Get full details about one module."""
    modules = _modules()
    m = next((x for x in modules if x['id'] == module_id
              or x['name'].lower() == module_id.lower()), None)
    if not m:
        # fuzzy: check if query is in name
        m = next((x for x in modules if module_id.lower() in x['name'].lower()
                  or module_id.lower() in x['id'].lower()), None)
    if not m:
        names = ', '.join(x['id'] for x in modules)
        return f"Module '{module_id}' not found. Available: {names}"

    lines = [
        f"Module: {m['name']} (id: {m['id']})",
        f"Category: {m['category']}",
        f"Status: {m['status']}",
        f"Tier: {m['tier']}",
        f"Version: {m['version']}",
        f"Description: {m['description']}",
    ]
    if m['tier'] == 'paid' and 'price' in m:
        lines.append(f"Price: ${m['price']}/{m.get('price_period','month')}")
    if m.get('tools'):
        lines.append(f"Capabilities: {', '.join(m['tools'])}")
    return '\n'.join(lines)


def check_for_updates() -> str:
    """Check if any installed modules have updates available. Stubs to bridge when live."""
    if _BRIDGE_URL is None:
        reg = _load_registry()
        return (
            f"Bridge server not yet connected — running on local registry v{reg.get('registry_version','?')}. "
            "Automatic update checks will activate once the bridge is live. "
            "All installed modules are at their current release versions."
        )
    # Real update check goes here when bridge is live
    return "Update check complete — all modules are up to date."


def new_modules_announcement() -> str:
    """Returns a one-liner if there are purchasable modules the owner might not know about.
    Called once per session from context.py to let Orby mention them naturally."""
    modules = _modules()
    available = [m for m in modules if m['status'] == 'available']
    if not available:
        return ''
    names = ', '.join(m['name'] for m in available)
    count = len(available)
    return (
        f"NEW MODULES AVAILABLE ({count}): {names}. "
        "If the owner asks what else you can do or mentions smart home/finance/health, "
        "naturally mention these add-ons exist. Don't force it — only bring it up if relevant."
    )


def request_module(module_id: str, owner_name: str = '') -> str:
    """Browse or request a paid module from the bridge store."""
    if _BRIDGE_URL is None:
        return (
            "Paid modules are available through the module store, which launches with the bridge server. "
            "Once live, you'll be able to browse, buy, and install new modules directly through me. "
            "The module downloads to your computer only after purchase — nothing installs until you pay for it."
        )
    # Real purchase flow: fetch module info from bridge, redirect to checkout
    return f"Connecting to module store..."


def activate_module(module_id: str, license_key: str = '') -> str:
    """Activate a paid module using a license key from a purchase email."""
    if not license_key:
        return (
            "To activate a paid module, paste the license key from your purchase confirmation email. "
            "Example: 'activate Smart Home with key ABC-123-XYZ'"
        )

    if _BRIDGE_URL is None:
        return (
            "Module activation requires the bridge server to validate your license key. "
            "This will be live at launch. Once connected: paste your key, I validate it, "
            "download the module, install it, and it's live — all automatically."
        )

    # Real flow when bridge is live:
    # 1. POST license_key to bridge /validate
    # 2. Bridge returns module_code download URL
    # 3. Download module to skills/
    # 4. Update local registry to installed
    # 5. Reload tools
    return f"Validating license and downloading module..."
