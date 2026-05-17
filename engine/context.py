"""Builds skill context to inject into the system prompt before each chat."""
from datetime import datetime
from modules.free import (reminders, calendar_skill, todo_skill, mood_skill,
               finance_skill, shopping_skill, countdown_skill,
               habit_tracker, health_skill, relationship_skill, quotes_skill,
               family_msg, meal_planning, chores_skill, school_skill,
               allowance_skill)
from modules.paid import product_dev, deep_memory, social_media
from modules.paid import image_studio, creator_3d, video_studio
from engine import wellbeing, module_manager


def build(profile_dir: str, owner_name: str = '') -> str:
    parts = []
    today = datetime.now()
    parts.append(f"TODAY: {today.strftime('%A, %B %d, %Y')} {today.strftime('%I:%M %p')}")

    _add(parts, reminders.context_summary(profile_dir))
    _add(parts, calendar_skill.context_summary(profile_dir))
    _add(parts, todo_skill.context_summary(profile_dir))
    _add(parts, mood_skill.context_summary(profile_dir))
    _add(parts, finance_skill.context_summary(profile_dir))
    _add(parts, shopping_skill.context_summary(profile_dir))
    _add(parts, habit_tracker.context_summary(profile_dir))
    _add(parts, health_skill.context_summary(profile_dir))
    _add(parts, countdown_skill.context_summary(profile_dir))
    _add(parts, family_msg.context_summary(profile_dir, owner_name))
    _add(parts, meal_planning.context_summary(profile_dir))
    _add(parts, chores_skill.context_summary(profile_dir))
    _add(parts, school_skill.context_summary(profile_dir))
    _add(parts, allowance_skill.context_summary(profile_dir))

    # Upcoming birthdays
    upcoming = relationship_skill.get_upcoming_birthdays(profile_dir, days=7)
    if upcoming:
        parts.append("UPCOMING BIRTHDAYS: " + "; ".join(
            f"{p['name']} ({p['date']}, {p['days_away']} days)" for p in upcoming))

    # Daily quote if user has saved quotes
    try:
        quote = quotes_skill.daily_quote(profile_dir)
        if quote:
            parts.append(quote)
    except Exception:
        pass

    # Product dev context — active products and unreviewed ideas
    try:
        _add(parts, product_dev.context_summary(profile_dir))
    except Exception:
        pass

    # Deep memory context — recent notes and last session
    try:
        _add(parts, deep_memory.context_summary(profile_dir))
    except Exception:
        pass

    # Social media status
    try:
        _add(parts, social_media.context_summary(profile_dir))
    except Exception:
        pass

    # Creative studio context
    try:
        _add(parts, image_studio.context_summary(profile_dir))
    except Exception:
        pass
    try:
        _add(parts, creator_3d.context_summary(profile_dir))
    except Exception:
        pass
    try:
        _add(parts, video_studio.context_summary(profile_dir))
    except Exception:
        pass

    # Announce purchasable modules so Orby can mention them naturally
    try:
        announcement = module_manager.new_modules_announcement()
        if announcement:
            parts.append(announcement)
    except Exception:
        pass

    return "\n".join(parts)


def check_wellbeing(message: str, profile_dir: str) -> dict:
    return wellbeing.check_message(message)


def _add(parts: list, text: str):
    if text and text.strip():
        parts.append(text.strip())
