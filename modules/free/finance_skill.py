MODULE_MANIFEST = {
    "id": "M008",
    "name": "Finance Tracker",
    "category": "Finance",
    "description": "Track income, expenses, budgets, and financial goals across accounts.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["add_transaction", "get_transactions", "set_budget", "get_budget_summary"],
    "min_bridge_version": "1.0.0"
}

from decimal import Decimal, ROUND_HALF_UP
from engine.storage import load, save, now_iso, today_str, new_id


def _money(value) -> str:
    return f"${Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"


def add_account(profile_dir, name, account_type, balance, limit=None):
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    existing = [a for a in data['accounts'] if a['name'].lower() != name.lower()]
    account = {'id': new_id(), 'name': name, 'type': account_type,
               'balance': float(balance), 'limit': float(limit) if limit else None}
    existing.append(account)
    data['accounts'] = existing
    save(profile_dir, 'finance', data)
    limit_str = f" (limit {_money(limit)})" if limit else ""
    return f"Account '{name}' saved — balance {_money(balance)}{limit_str}."


def log_expense(profile_dir, amount, description, store='', payment_method='', category='general'):
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    tx = {'id': new_id(), 'type': 'expense', 'amount': float(amount),
          'description': description, 'store': store,
          'payment_method': payment_method, 'category': category,
          'date': today_str(), 'created': now_iso()}
    data['transactions'].append(tx)

    # Update account balance if payment method matches an account
    if payment_method:
        for acct in data['accounts']:
            if payment_method.lower() in acct['name'].lower():
                if acct['type'] in ('checking', 'savings', 'debit'):
                    acct['balance'] = round(acct['balance'] - float(amount), 2)
                elif acct['type'] == 'credit':
                    acct['balance'] = round(acct['balance'] + float(amount), 2)

    save(profile_dir, 'finance', data)
    return f"Logged {_money(amount)} expense — {description}" + (f" at {store}" if store else "") + (f" on {payment_method}" if payment_method else "") + "."


def log_income(profile_dir, amount, source='', notes=''):
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    entry = {'id': new_id(), 'amount': float(amount), 'source': source,
             'notes': notes, 'date': today_str(), 'created': now_iso()}
    data['income'].append(entry)
    save(profile_dir, 'finance', data)
    return f"Income logged: {_money(amount)}" + (f" from {source}" if source else "") + "."


def update_balance(profile_dir, account_name, new_balance):
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    for acct in data['accounts']:
        if account_name.lower() in acct['name'].lower():
            acct['balance'] = float(new_balance)
    save(profile_dir, 'finance', data)
    return f"Balance updated: {account_name} → {_money(new_balance)}."


def get_summary(profile_dir) -> dict:
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    accounts = data.get('accounts', [])
    transactions = data.get('transactions', [])
    income_log = data.get('income', [])

    from datetime import datetime
    this_month = datetime.now().strftime('%Y-%m')
    month_txs = [t for t in transactions if t.get('date', '').startswith(this_month)]
    month_income = [i for i in income_log if i.get('date', '').startswith(this_month)]

    total_spent = sum(t['amount'] for t in month_txs if t['type'] == 'expense')
    total_income = sum(i['amount'] for i in month_income)

    categories = {}
    for t in month_txs:
        cat = t.get('category', 'general')
        categories[cat] = categories.get(cat, 0) + t['amount']

    by_payment = {}
    for t in month_txs:
        pm = t.get('payment_method', 'unknown')
        by_payment[pm] = by_payment.get(pm, 0) + t['amount']

    return {
        'accounts': accounts,
        'month_spent': total_spent,
        'month_income': total_income,
        'net': total_income - total_spent,
        'categories': categories,
        'by_payment': by_payment
    }


def get_alerts(profile_dir) -> list:
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    alerts = []
    for acct in data.get('accounts', []):
        if acct.get('type') == 'credit' and acct.get('limit'):
            pct = (acct['balance'] / acct['limit']) * 100
            if pct >= 80:
                alerts.append(f"{acct['name']} is at {pct:.0f}% of its limit ({_money(acct['balance'])} of {_money(acct['limit'])})")
        if acct.get('type') in ('checking', 'savings') and acct.get('balance', 999) < 200:
            alerts.append(f"{acct['name']} is running low — {_money(acct['balance'])} remaining")
    return alerts


def context_summary(profile_dir) -> str:
    data = load(profile_dir, 'finance', default={'accounts': [], 'transactions': [], 'income': []})
    accounts = data.get('accounts', [])
    if not accounts:
        return ''
    lines = ['ACCOUNT BALANCES: ' + ' | '.join(
        f"{a['name']} {_money(a['balance'])}" + (f"/{_money(a['limit'])}" if a.get('limit') else '')
        for a in accounts
    )]
    alerts = get_alerts(profile_dir)
    if alerts:
        lines.append('FINANCE ALERTS: ' + '; '.join(alerts))
    return "\n".join(lines)
