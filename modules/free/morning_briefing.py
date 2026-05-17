MODULE_MANIFEST = {
    "id": "M006",
    "name": "Morning Briefing",
    "category": "Core",
    "description": "Generate a personalized daily briefing with agenda, tasks, weather, and reminders.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["get_morning_briefing"],
    "min_bridge_version": "1.0.0"
}

from datetime import datetime
from modules.free import reminders, calendar_skill, todo_skill, mood_skill, finance_skill, shopping_skill
from engine.storage import load


def build(profile_dir, owner_name='') -> str:
    now = datetime.now()
    greeting = _time_greeting(now.hour)
    name_part = f", {owner_name}" if owner_name else ""

    sections = [f"Good {greeting}{name_part}."]

    # Date
    sections.append(f"Today is {now.strftime('%A, %B %d, %Y')}.")

    # Due reminders
    due = reminders.get_due(profile_dir)
    if due:
        sections.append("Reminders due: " + ", ".join(r['text'] for r in due))

    # Today's schedule
    today_events = calendar_skill.get_today(profile_dir, family=False)
    family_events = calendar_skill.get_today(profile_dir, family=True)
    if today_events:
        sections.append("Your schedule: " + " | ".join(
            f"{e['title']} at {e.get('start','')}" for e in today_events))
    if family_events:
        sections.append("Family calendar: " + " | ".join(
            f"{e['title']} at {e.get('start','')}" for e in family_events))

    # Open to-dos
    todos = todo_skill.get_open(profile_dir)
    if todos:
        top = todos[:3]
        sections.append(f"To-do ({len(todos)} open): " + ", ".join(t['text'] for t in top))

    # Finance alerts
    alerts = finance_skill.get_alerts(profile_dir)
    if alerts:
        sections.append("Finance heads up: " + "; ".join(alerts))

    # Shopping list
    shopping = shopping_skill.get_list(profile_dir)
    if shopping:
        sections.append("Shopping list: " + ", ".join(i['item'] for i in shopping[:5]))

    # Upcoming birthdays from relationships
    upcoming_bdays = _upcoming_birthdays(profile_dir)
    if upcoming_bdays:
        sections.append("Upcoming birthdays: " + "; ".join(upcoming_bdays))

    # Memory lane
    from .journal_skill import memory_lane
    past_entries = memory_lane(profile_dir)
    if past_entries:
        entry = past_entries[0]
        sections.append(f"On this day in {entry['date'][:4]}, you wrote: \"{entry['content'][:80]}...\"")

    # Fun fact placeholder (rotates)
    fact = _daily_fact(now)
    if fact:
        sections.append(fact)

    return " ".join(sections)


def _time_greeting(hour: int) -> str:
    if hour < 12:
        return "morning"
    elif hour < 17:
        return "afternoon"
    else:
        return "evening"


def _upcoming_birthdays(profile_dir, days=7):
    from datetime import datetime, timedelta
    today = datetime.now()
    upcoming = []
    people = load(profile_dir, 'relationships')
    for person in people:
        bday = person.get('birthday', '')
        if not bday:
            continue
        try:
            # Birthday stored as MM-DD or YYYY-MM-DD
            parts = bday.split('-')
            month = int(parts[-2])
            day = int(parts[-1])
            this_year_bday = today.replace(month=month, day=day, hour=0, minute=0, second=0)
            if this_year_bday < today:
                this_year_bday = this_year_bday.replace(year=today.year + 1)
            delta = (this_year_bday - today).days
            if 0 <= delta <= days:
                upcoming.append(f"{person['name']}'s birthday in {delta} days" if delta > 0
                                else f"Today is {person['name']}'s birthday!")
        except Exception:
            pass
    return upcoming


def _daily_fact(now: datetime) -> str:
    facts = [
        "Fun fact: Honey never spoils — archaeologists have found 3,000-year-old honey in Egyptian tombs.",
        "Fun fact: A group of flamingos is called a flamboyance.",
        "Fun fact: Octopuses have three hearts and blue blood.",
        "Fun fact: The shortest war in history lasted 38 minutes.",
        "Fun fact: A day on Venus is longer than a year on Venus.",
        "Fun fact: Bananas are technically berries but strawberries aren't.",
        "Fun fact: The Eiffel Tower grows about 6 inches taller in summer due to heat expansion.",
    ]
    return facts[now.timetuple().tm_yday % len(facts)]
