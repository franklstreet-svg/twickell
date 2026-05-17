MODULE_MANIFEST = {
    "id": "M017",
    "name": "Allowance Manager",
    "category": "Family",
    "description": "Track allowances, payments, chores completed, and savings goals for kids.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["set_allowance", "log_chore_complete", "get_allowance_balance"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str


def set_allowance(profile_dir, child_name, weekly_amount, day_of_week='Saturday'):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    existing = next((a for a in data['allowances'] if a['child'].lower() == child_name.lower()), None)
    if existing:
        existing['weekly_amount'] = weekly_amount
        existing['day_of_week'] = day_of_week
    else:
        data['allowances'].append({
            'id': new_id(),
            'child': child_name,
            'weekly_amount': weekly_amount,
            'day_of_week': day_of_week,
            'balance': 0.0
        })
    save(profile_dir, 'allowance', data)
    return f"{child_name}'s allowance set to ${weekly_amount:.2f}/week (paid on {day_of_week})."


def pay_allowance(profile_dir, child_name, amount=None, notes=''):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    child = next((a for a in data['allowances'] if a['child'].lower() == child_name.lower()), None)
    if not child:
        return f"No allowance set up for {child_name}. Use set_allowance first."
    pay = amount if amount is not None else child['weekly_amount']
    child['balance'] = round(child.get('balance', 0) + pay, 2)
    data['transactions'].append({
        'id': new_id(),
        'child': child_name,
        'type': 'income',
        'amount': pay,
        'notes': notes or 'weekly allowance',
        'date': today_str(),
        'balance_after': child['balance']
    })
    save(profile_dir, 'allowance', data)
    return f"Paid {child_name} ${pay:.2f}. New balance: ${child['balance']:.2f}."


def spend_allowance(profile_dir, child_name, amount, description):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    child = next((a for a in data['allowances'] if a['child'].lower() == child_name.lower()), None)
    if not child:
        return f"No allowance set up for {child_name}."
    if child.get('balance', 0) < amount:
        return f"{child_name} only has ${child.get('balance', 0):.2f} — not enough for ${amount:.2f}."
    child['balance'] = round(child['balance'] - amount, 2)
    data['transactions'].append({
        'id': new_id(),
        'child': child_name,
        'type': 'expense',
        'amount': amount,
        'notes': description,
        'date': today_str(),
        'balance_after': child['balance']
    })
    save(profile_dir, 'allowance', data)
    return f"{child_name} spent ${amount:.2f} on {description}. Remaining: ${child['balance']:.2f}."


def get_balance(profile_dir, child_name=''):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    children = data['allowances']
    if child_name:
        children = [c for c in children if c['child'].lower() == child_name.lower()]
    return children


def get_transactions(profile_dir, child_name, limit=10):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    txns = [t for t in data['transactions'] if t['child'].lower() == child_name.lower()]
    return txns[-limit:]


def context_summary(profile_dir):
    data = load(profile_dir, 'allowance', {'allowances': [], 'transactions': []})
    if not data['allowances']:
        return ''
    lines = []
    for child in data['allowances']:
        lines.append(f"  {child['child']}: ${child.get('balance', 0):.2f}")
    if lines:
        return "ALLOWANCE BALANCES:\n" + '\n'.join(lines)
    return ''
