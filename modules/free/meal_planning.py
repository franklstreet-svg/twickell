MODULE_MANIFEST = {
    "id": "M022",
    "name": "Meal Planning",
    "category": "Home",
    "description": "Plan weekly meals, build grocery lists from meal plans, and track nutrition goals.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["plan_meal", "get_meal_plan", "generate_grocery_list"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, new_id, today_str
from datetime import datetime, timedelta


def plan_meal(profile_dir, day, meal_type, dish, servings='', notes=''):
    data = load(profile_dir, 'meal_plan', {'meals': []})
    entry = {
        'id': new_id(),
        'day': day,
        'meal_type': meal_type.lower(),
        'dish': dish,
        'servings': servings,
        'notes': notes,
        'added': today_str()
    }
    data['meals'].append(entry)
    save(profile_dir, 'meal_plan', data)
    return f"{meal_type.capitalize()} planned for {day}: {dish}."


def get_week_plan(profile_dir):
    data = load(profile_dir, 'meal_plan', {'meals': []})
    today = datetime.now()
    week_days = [(today + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    meals = [m for m in data['meals'] if m['day'] in week_days]
    return meals


def get_day_plan(profile_dir, day=''):
    data = load(profile_dir, 'meal_plan', {'meals': []})
    day = day or today_str()
    return [m for m in data['meals'] if m['day'] == day]


def delete_meal(profile_dir, meal_id):
    data = load(profile_dir, 'meal_plan', {'meals': []})
    before = len(data['meals'])
    data['meals'] = [m for m in data['meals'] if m['id'] != meal_id]
    if len(data['meals']) < before:
        save(profile_dir, 'meal_plan', data)
        return "Meal removed from plan."
    return "Meal not found."


def context_summary(profile_dir):
    meals = get_day_plan(profile_dir)
    if not meals:
        return ''
    order = {'breakfast': 0, 'lunch': 1, 'dinner': 2, 'snack': 3}
    meals.sort(key=lambda m: order.get(m['meal_type'], 9))
    lines = ["TODAY'S MEALS:"]
    for m in meals:
        lines.append(f"  {m['meal_type'].capitalize()}: {m['dish']}")
    return '\n'.join(lines)
