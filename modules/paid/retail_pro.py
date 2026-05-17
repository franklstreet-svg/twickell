"""Retail Pro — Products, inventory, sales, customers, and suppliers for retail businesses."""
MODULE_MANIFEST = {
    "id": "I005",
    "name": "Retail Pro",
    "category": "Industry",
    "description": "Complete retail management. Track products and inventory, record sales, manage customers, get sales reports, and organize suppliers — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["add_product", "list_products", "update_stock",
              "get_low_stock", "add_retail_customer", "record_sale",
              "get_sales_report", "add_retail_supplier"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime, timedelta
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

def add_product(profile_dir: str, name: str, sku: str, price: float,
                category: str, stock: int = 0, reorder_level: int = 5,
                notes: str = '') -> str:
    path = _path(profile_dir, 'retail_products.json')
    products = _load(path, [])
    if any(p.get('sku') == sku for p in products):
        return f"A product with SKU '{sku}' already exists."
    product = {
        'id': _uid(),
        'name': name,
        'sku': sku,
        'price': price,
        'category': category,
        'stock': stock,
        'reorder_level': reorder_level,
        'notes': notes,
        'created_at': _now()
    }
    products.append(product)
    _save(path, products)
    return f"Product added [id:{product['id']}] — {name} (SKU: {sku}), ${price:.2f}, stock: {stock}."


def list_products(profile_dir: str, category: str = '',
                  low_stock_only: bool = False) -> str:
    path = _path(profile_dir, 'retail_products.json')
    products = _load(path, [])
    if category:
        products = [p for p in products if p.get('category', '').lower() == category.lower()]
    if low_stock_only:
        products = [p for p in products if p.get('stock', 0) <= p.get('reorder_level', 5)]
    if not products:
        return "No products found."
    lines = [f"Products ({len(products)}):"]
    for p in products:
        low = ' [LOW]' if p.get('stock', 0) <= p.get('reorder_level', 5) else ''
        lines.append(f"  [{p['id']}] {p['name']} (SKU:{p.get('sku','')}) — ${p.get('price',0):.2f} — stock:{p.get('stock',0)}{low}")
    return '\n'.join(lines)


def update_stock(profile_dir: str, sku: str, quantity_change: int,
                 reason: str = '') -> str:
    path = _path(profile_dir, 'retail_products.json')
    products = _load(path, [])
    product = next((p for p in products if p.get('sku') == sku), None)
    if not product:
        return f"Product with SKU '{sku}' not found."
    old_stock = product.get('stock', 0)
    product['stock'] = old_stock + quantity_change
    product['updated_at'] = _now()
    _save(path, products)
    direction = "+" if quantity_change >= 0 else ""
    return (f"Stock updated: {product['name']} — {old_stock} → {product['stock']} "
            f"({direction}{quantity_change})" + (f". Reason: {reason}" if reason else "") + ".")


def get_low_stock(profile_dir: str) -> str:
    path = _path(profile_dir, 'retail_products.json')
    products = _load(path, [])
    low = [p for p in products if p.get('stock', 0) <= p.get('reorder_level', 5)]
    if not low:
        return "All products are above reorder levels."
    lines = [f"Low stock products ({len(low)}):"]
    for p in low:
        lines.append(f"  • {p['name']} (SKU:{p.get('sku','')}) — stock:{p.get('stock',0)} / reorder at:{p.get('reorder_level',5)}")
    return '\n'.join(lines)


def add_retail_customer(profile_dir: str, name: str, email: str = '',
                        phone: str = '', notes: str = '') -> str:
    path = _path(profile_dir, 'retail_customers.json')
    customers = _load(path, [])
    customer = {
        'id': _uid(),
        'name': name,
        'email': email,
        'phone': phone,
        'notes': notes,
        'created_at': _now()
    }
    customers.append(customer)
    _save(path, customers)
    return f"Customer added [id:{customer['id']}] — {name}" + (f" ({email})" if email else "") + "."


def record_sale(profile_dir: str, items: list, customer_name: str = '',
                notes: str = '') -> str:
    """items: list of {sku, qty, price}"""
    path = _path(profile_dir, 'retail_sales.json')
    sales = _load(path, [])
    # Calculate total
    total = sum(i.get('price', 0) * i.get('qty', 1) for i in items)
    sale = {
        'id': _uid(),
        'items': items,
        'total': total,
        'customer_name': customer_name,
        'notes': notes,
        'date': _now()[:10],
        'created_at': _now()
    }
    sales.append(sale)
    _save(path, sales)
    # Update stock
    products_path = _path(profile_dir, 'retail_products.json')
    products = _load(products_path, [])
    for item in items:
        sku = item.get('sku', '')
        qty = item.get('qty', 1)
        product = next((p for p in products if p.get('sku') == sku), None)
        if product:
            product['stock'] = product.get('stock', 0) - qty
    _save(products_path, products)
    item_count = sum(i.get('qty', 1) for i in items)
    return (f"Sale recorded [id:{sale['id']}] — {item_count} item(s), total: ${total:.2f}"
            + (f" for {customer_name}" if customer_name else "") + ".")


def get_sales_report(profile_dir: str, days: int = 30) -> str:
    path = _path(profile_dir, 'retail_sales.json')
    sales = _load(path, [])
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    recent = [s for s in sales if s.get('date', '') >= cutoff]
    if not recent:
        return f"No sales in the last {days} days."
    total_revenue = sum(s.get('total', 0) for s in recent)
    # Top products
    sku_totals = {}
    for s in recent:
        for item in s.get('items', []):
            sku = item.get('sku', 'unknown')
            sku_totals[sku] = sku_totals.get(sku, 0) + item.get('qty', 1)
    top = sorted(sku_totals.items(), key=lambda x: x[1], reverse=True)[:5]
    lines = [
        f"Sales Report (last {days} days):",
        f"  Transactions: {len(recent)}",
        f"  Total revenue: ${total_revenue:,.2f}",
        f"  Avg transaction: ${total_revenue/len(recent):.2f}",
    ]
    if top:
        lines.append("  Top products by units sold:")
        for sku, qty in top:
            lines.append(f"    • SKU:{sku} — {qty} units")
    return '\n'.join(lines)


def add_retail_supplier(profile_dir: str, name: str, category: str,
                        contact: str, phone: str = '', email: str = '') -> str:
    path = _path(profile_dir, 'retail_suppliers.json')
    suppliers = _load(path, [])
    supplier = {
        'id': _uid(),
        'name': name,
        'category': category,
        'contact': contact,
        'phone': phone,
        'email': email,
        'created_at': _now()
    }
    suppliers.append(supplier)
    _save(path, suppliers)
    return f"Supplier added [id:{supplier['id']}] — {name} ({category}), contact: {contact}."


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    products = _load(_path(profile_dir, 'retail_products.json'), [])
    if not products:
        return ''
    low = [p for p in products if p.get('stock', 0) <= p.get('reorder_level', 5)]
    sales = _load(_path(profile_dir, 'retail_sales.json'), [])
    today = _now()[:10]
    today_sales = [s for s in sales if s.get('date', '') == today]
    today_revenue = sum(s.get('total', 0) for s in today_sales)
    parts = [f"RETAIL PRO ACTIVE: {len(products)} product(s) tracked"]
    if today_sales:
        parts.append(f"  Today: {len(today_sales)} sale(s), ${today_revenue:.2f}")
    if low:
        parts.append(f"  {len(low)} product(s) low on stock")
    parts.append(
        "What I can help you with:\n"
        "  Track: products/SKUs, stock levels, sales, customers, suppliers\n"
        "  Write: product descriptions, promotional copy, supplier negotiation emails,\n"
        "         customer service response letters, inventory audit reports,\n"
        "         return policy letters, staff task lists\n"
        "  Know: retail pricing strategies, inventory management basics, sales reporting"
    )
    return '\n'.join(parts)
