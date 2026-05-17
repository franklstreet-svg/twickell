"""Routes tool calls from Anthropic to the correct skill handler."""
import logging
from system import system_test as _system_test
from system import health_monitor, snapshots
from system.identity import get_or_create as get_identity
from engine import wellbeing, module_manager
from modules.free import (reminders, calendar_skill, todo_skill, notes_skill,
               shopping_skill, journal_skill, mood_skill, weather_skill,
               finance_skill, family_msg, morning_briefing,
               recipes_skill, habit_tracker, fitness_skill, health_skill,
               gifts_skill, pets_skill, vehicle_skill, travel_skill,
               relationship_skill, countdown_skill, bucket_list, quotes_skill,
               meal_planning, chores_skill, home_maintenance, emergency_info,
               school_skill, allowance_skill, bedtime_story, web_search)
from modules.paid import product_dev, deep_memory, social_media
from modules.paid import image_studio, creator_3d, video_studio
from modules.paid import (legal_pro, medical_pro, realestate_pro, restaurant_pro,
                           retail_pro, salon_pro, contractor_pro, trade_pro,
                           accounting_pro, hr_pro, therapy_pro, property_mgmt,
                           business_pro, inventory_pro)

log = logging.getLogger(__name__)


def execute(tool_name: str, tool_input: dict, profile_dir: str, owner_name: str = '') -> str:
    try:
        return _dispatch(tool_name, tool_input, profile_dir, owner_name)
    except Exception as e:
        log.error('Tool error [%s]: %s', tool_name, e)
        return f"Error executing {tool_name}: {e}"


def _dispatch(tool_name: str, inp: dict, profile_dir: str, owner_name: str) -> str:

    # ── REMINDERS ──────────────────────────────────────────────────────────
    if tool_name == 'set_reminder':
        return reminders.add(profile_dir, inp['text'], inp['when'], inp.get('recurring'))
    if tool_name == 'get_reminders':
        items = reminders.get_upcoming(profile_dir, limit=10)
        if not items:
            return "No upcoming reminders."
        return "\n".join(f"• {r['text']} ({r['when']}) [id:{r['id']}]" for r in items)
    if tool_name == 'complete_reminder':
        return reminders.complete(profile_dir, inp['reminder_id'])

    # ── CALENDAR ───────────────────────────────────────────────────────────
    if tool_name == 'add_calendar_event':
        return calendar_skill.add_event(
            profile_dir, inp['title'], inp['start'],
            inp.get('end'), inp.get('notes', ''), inp.get('family', False))
    if tool_name == 'get_calendar':
        from engine.storage import today_str
        date = inp.get('date', today_str())
        family = inp.get('family', False)
        events = calendar_skill.get_range(profile_dir, date, date + 'T23:59', family)
        if not events:
            return f"Nothing on {'family ' if family else ''}calendar for {date}."
        return "\n".join(f"• {e['title']} at {e['start']} [id:{e['id']}]" for e in events)

    # ── TO-DO ──────────────────────────────────────────────────────────────
    if tool_name == 'add_todo':
        return todo_skill.add(profile_dir, inp['text'], inp.get('family', False), inp.get('priority', 'normal'))
    if tool_name == 'get_todos':
        items = todo_skill.get_open(profile_dir, inp.get('family', False))
        if not items:
            return "To-do list is empty."
        return "\n".join(f"• {t['text']} [{t.get('priority','normal')}] [id:{t['id']}]" for t in items)
    if tool_name == 'complete_todo':
        return todo_skill.complete(profile_dir, inp['item_id'], inp.get('family', False))

    # ── NOTES ──────────────────────────────────────────────────────────────
    if tool_name == 'save_note':
        return notes_skill.add(profile_dir, inp['content'], inp.get('title', ''), inp.get('tags'))
    if tool_name == 'search_notes':
        results = notes_skill.search(profile_dir, inp['query'])
        if not results:
            return f"No notes found for '{inp['query']}'."
        return "\n".join(f"• {n['title']}: {n['content'][:100]}" for n in results[:5])

    # ── SHOPPING ───────────────────────────────────────────────────────────
    if tool_name == 'add_shopping_item':
        return shopping_skill.add_item(profile_dir, inp['item'], inp.get('quantity', '1'), inp.get('family', False))
    if tool_name == 'get_shopping_list':
        items = shopping_skill.get_list(profile_dir, inp.get('family', False))
        if not items:
            return "Shopping list is empty."
        return "\n".join(f"• {i['quantity']}x {i['item']} [id:{i['id']}]" for i in items)

    # ── JOURNAL ────────────────────────────────────────────────────────────
    if tool_name == 'add_journal_entry':
        return journal_skill.add_entry(profile_dir, inp['content'],
                                       inp.get('mood'), inp.get('is_dream', False))
    if tool_name == 'get_journal_entries':
        entries = journal_skill.get_entries(profile_dir, inp.get('limit', 5), inp.get('is_dream', False))
        if not entries:
            return "No journal entries yet."
        return "\n\n".join(f"[{e['date']}] {e['content'][:200]}" for e in entries)

    # ── MOOD ───────────────────────────────────────────────────────────────
    if tool_name == 'log_mood':
        return mood_skill.log_mood(profile_dir, inp['mood'], inp.get('notes', ''))

    # ── WEATHER ────────────────────────────────────────────────────────────
    if tool_name == 'get_weather':
        return weather_skill.get_weather(inp['location'])

    # ── FINANCE ────────────────────────────────────────────────────────────
    if tool_name == 'log_expense':
        return finance_skill.log_expense(
            profile_dir, inp['amount'], inp['description'],
            inp.get('store', ''), inp.get('payment_method', ''), inp.get('category', 'general'))
    if tool_name == 'log_income':
        return finance_skill.log_income(profile_dir, inp['amount'], inp.get('source', ''))
    if tool_name == 'add_account':
        return finance_skill.add_account(
            profile_dir, inp['name'], inp['account_type'],
            inp['balance'], inp.get('limit'))
    if tool_name == 'get_finance_summary':
        summary = finance_skill.get_summary(profile_dir)
        if not summary['accounts']:
            return "No accounts set up yet. Tell me your account balances to get started."
        lines = ["Account balances:"]
        for acct in summary['accounts']:
            from decimal import Decimal, ROUND_HALF_UP
            bal = f"${Decimal(str(acct['balance'])).quantize(Decimal('0.01'))}"
            lim = f"/{Decimal(str(acct['limit'])).quantize(Decimal('0.01'))}" if acct.get('limit') else ""
            lines.append(f"  {acct['name']}: {bal}{lim} ({acct['type']})")
        lines.append(f"This month: spent ${summary['month_spent']:.2f}, income ${summary['month_income']:.2f}, net ${summary['net']:.2f}")
        alerts = finance_skill.get_alerts(profile_dir)
        if alerts:
            lines.append("ALERTS: " + "; ".join(alerts))
        return "\n".join(lines)

    # ── FAMILY MESSAGING ───────────────────────────────────────────────────
    if tool_name == 'send_family_message':
        return family_msg.send_message(
            profile_dir, owner_name or 'Someone',
            inp['recipient'], inp['content'], inp.get('urgent', False))

    # ── RECIPES ────────────────────────────────────────────────────────────
    if tool_name == 'save_recipe':
        return recipes_skill.save_recipe(
            profile_dir, inp['name'], inp['ingredients'], inp['steps'],
            inp.get('tags'), inp.get('source_url', ''), inp.get('servings', ''))
    if tool_name == 'find_recipe':
        results = recipes_skill.find_recipe(profile_dir, inp['query'])
        if not results:
            return f"No recipes found for '{inp['query']}'."
        return "\n".join(f"• {r['name']} (servings: {r.get('servings','?')}) [id:{r['id']}]" for r in results[:5])

    # ── HABITS ─────────────────────────────────────────────────────────────
    if tool_name == 'add_habit':
        return habit_tracker.add_habit(profile_dir, inp['name'],
                                       inp.get('frequency', 'daily'), inp.get('goal', ''))
    if tool_name == 'log_habit':
        return habit_tracker.log_habit(profile_dir, inp['habit_name'], inp.get('done', True))

    # ── FITNESS ────────────────────────────────────────────────────────────
    if tool_name == 'log_workout':
        return fitness_skill.log_workout(
            profile_dir, inp['exercise'], inp.get('duration_minutes'),
            inp.get('sets'), inp.get('reps'), inp.get('weight'), inp.get('notes', ''))

    # ── HEALTH ─────────────────────────────────────────────────────────────
    if tool_name == 'log_health':
        return health_skill.log_health(
            profile_dir, inp.get('weight'), inp.get('sleep_hours'),
            inp.get('water_glasses'), inp.get('symptoms'), inp.get('medications'), inp.get('notes', ''))
    if tool_name == 'add_medication_reminder':
        return health_skill.add_medication_reminder(
            profile_dir, inp['med_name'], inp['time_of_day'], inp.get('dose', ''))

    # ── PEOPLE ─────────────────────────────────────────────────────────────
    if tool_name == 'save_person':
        return relationship_skill.add_person(
            profile_dir, inp['name'], inp.get('relation', ''),
            inp.get('birthday', ''), inp.get('anniversary', ''),
            inp.get('phone', ''), inp.get('email', ''), inp.get('notes', ''))

    # ── GIFTS ──────────────────────────────────────────────────────────────
    if tool_name == 'add_gift_idea':
        return gifts_skill.add_idea(
            profile_dir, inp['person'], inp['idea'],
            inp.get('occasion', ''), inp.get('budget', ''))

    # ── PETS ───────────────────────────────────────────────────────────────
    if tool_name == 'log_pet_care':
        pet_name = inp['pet_name']
        care_type = inp.get('care_type', 'note')
        notes = inp.get('notes', '')
        date = inp.get('date', '')
        if care_type == 'vet_visit':
            return pets_skill.log_vet_visit(profile_dir, pet_name, date or 'today', notes)
        elif care_type == 'medication':
            return pets_skill.add_pet_medication(profile_dir, pet_name, notes, 'as needed')
        else:
            return pets_skill.add_pet_note(profile_dir, pet_name, notes or care_type)

    # ── VEHICLES ───────────────────────────────────────────────────────────
    if tool_name == 'log_vehicle_service':
        return vehicle_skill.log_service(
            profile_dir, inp['vehicle'], inp['service_type'],
            inp.get('mileage'), None, inp.get('cost'), inp.get('notes', ''))

    # ── TRAVEL ─────────────────────────────────────────────────────────────
    if tool_name == 'add_trip':
        return travel_skill.add_trip(
            profile_dir, inp['destination'],
            inp.get('start_date', ''), inp.get('end_date', ''), inp.get('notes', ''))

    # ── COUNTDOWN ──────────────────────────────────────────────────────────
    if tool_name == 'add_countdown':
        return countdown_skill.add_countdown(profile_dir, inp['name'], inp['target_date'])

    # ── BUCKET LIST ────────────────────────────────────────────────────────
    if tool_name == 'add_bucket_list_item':
        return bucket_list.add_item(profile_dir, inp['item'],
                                    inp.get('category', 'life'), inp.get('notes', ''))
    if tool_name == 'get_bucket_list':
        items = bucket_list.get_open(profile_dir)
        if not items:
            return "Your bucket list is empty — let's fix that!"
        return "\n".join(f"• [{i.get('category','life')}] {i['item']} [id:{i['id']}]" for i in items)

    # ── QUOTES ─────────────────────────────────────────────────────────────
    if tool_name == 'save_quote':
        return quotes_skill.save_quote(profile_dir, inp['text'], inp.get('author', ''))

    # ── MORNING BRIEFING ───────────────────────────────────────────────────
    if tool_name == 'get_morning_briefing':
        return morning_briefing.build(profile_dir, owner_name)

    # ── EMERGENCY CONTACT ──────────────────────────────────────────────────
    if tool_name == 'set_emergency_contact':
        return wellbeing.add_emergency_contact(profile_dir, inp['name'], inp.get('relation', ''))

    # ── MEAL PLANNING ──────────────────────────────────────────────────────
    if tool_name == 'plan_meal':
        return meal_planning.plan_meal(
            profile_dir, inp['day'], inp['meal_type'], inp['dish'],
            inp.get('servings', ''), inp.get('notes', ''))
    if tool_name == 'get_meal_plan':
        if inp.get('week'):
            meals = meal_planning.get_week_plan(profile_dir)
        else:
            meals = meal_planning.get_day_plan(profile_dir, inp.get('day', ''))
        if not meals:
            return "No meals planned yet."
        lines = []
        for m in meals:
            lines.append(f"• {m['day']} {m['meal_type']}: {m['dish']}" + (f" ({m['servings']} servings)" if m.get('servings') else ''))
        return '\n'.join(lines)

    # ── CHORES ─────────────────────────────────────────────────────────────
    if tool_name == 'add_chore':
        return chores_skill.add_chore(
            profile_dir, inp['chore'], inp.get('assigned_to', ''),
            inp.get('frequency', 'weekly'), inp.get('points', 0))
    if tool_name == 'complete_chore':
        return chores_skill.complete_chore(profile_dir, inp['chore_id'], inp.get('completed_by', ''))
    if tool_name == 'get_chores':
        items = chores_skill.get_chores(profile_dir, inp.get('assigned_to', ''))
        if not items:
            return "No chores on the chart yet."
        lines = []
        for c in items:
            who = f" → {c['assigned_to']}" if c.get('assigned_to') else ''
            done = " ✓" if c.get('done_today') else ''
            lines.append(f"• {c['chore']}{who} [{c.get('frequency','weekly')}]{done} [id:{c['id']}]")
        return '\n'.join(lines)
    if tool_name == 'get_chore_points':
        if inp.get('person'):
            result = chores_skill.get_points(profile_dir, inp['person'])
            return f"{result['person']}: {result['total_points']} points ({result['completions']} chores done)"
        board = chores_skill.get_leaderboard(profile_dir)
        if not board:
            return "No chores completed yet."
        return "Chore leaderboard:\n" + '\n'.join(f"  {i+1}. {name}: {pts} pts" for i, (name, pts) in enumerate(board))

    # ── HOME MAINTENANCE ───────────────────────────────────────────────────
    if tool_name == 'log_home_repair':
        return home_maintenance.log_repair(
            profile_dir, inp['item'], inp['description'],
            inp.get('cost'), inp.get('contractor', ''), inp.get('date', ''))
    if tool_name == 'add_warranty':
        return home_maintenance.add_warranty(
            profile_dir, inp['item'], inp['expires'],
            inp.get('purchase_date', ''), inp.get('notes', ''))
    if tool_name == 'save_contractor':
        return home_maintenance.add_contractor(
            profile_dir, inp['name'], inp['trade'],
            inp.get('phone', ''), inp.get('email', ''), inp.get('notes', ''))
    if tool_name == 'find_contractor':
        results = home_maintenance.find_contractor(profile_dir, inp.get('trade', ''))
        if not results:
            return f"No contractors saved for that trade."
        return '\n'.join(f"• {c['name']} ({c['trade']})" + (f" — {c['phone']}" if c.get('phone') else '') for c in results)

    # ── EMERGENCY INFO ─────────────────────────────────────────────────────
    if tool_name == 'save_emergency_contact':
        return emergency_info.add_emergency_contact(
            profile_dir, inp['name'], inp['relation'], inp['phone'], inp.get('notes', ''))
    if tool_name == 'save_medical_info':
        return emergency_info.add_medical_info(
            profile_dir, inp['person'], inp['info_type'], inp['details'])
    if tool_name == 'save_insurance':
        return emergency_info.add_insurance(
            profile_dir, inp['insurance_type'], inp['provider'],
            inp['policy_number'], inp.get('phone', ''), inp.get('notes', ''))
    if tool_name == 'get_emergency_info':
        data = emergency_info.get_all(profile_dir)
        info_type = inp.get('info_type', 'all')
        person = inp.get('person', '')
        lines = []
        if info_type in ('contacts', 'all') and data['contacts']:
            lines.append("EMERGENCY CONTACTS:")
            for c in data['contacts']:
                lines.append(f"  • {c['name']} ({c['relation']}): {c['phone']}")
        if info_type in ('medical', 'all') and data['medical']:
            records = data['medical']
            if person:
                records = [r for r in records if r.get('person', '').lower() == person.lower()]
            if records:
                lines.append("MEDICAL INFO:")
                for r in records:
                    lines.append(f"  • {r['person']} [{r['type']}]: {r['details']}")
        if info_type in ('insurance', 'all') and data['insurance']:
            lines.append("INSURANCE:")
            for i in data['insurance']:
                lines.append(f"  • {i['type'].capitalize()}: {i['provider']} #{i['policy_number']}")
        return '\n'.join(lines) if lines else "No emergency info saved yet."

    # ── SCHOOL / HOMEWORK ──────────────────────────────────────────────────
    if tool_name == 'add_homework':
        return school_skill.add_homework(
            profile_dir, inp['child_name'], inp['subject'],
            inp['description'], inp['due_date'])
    if tool_name == 'complete_homework':
        return school_skill.complete_homework(profile_dir, inp['homework_id'])
    if tool_name == 'get_homework':
        items = school_skill.get_homework(profile_dir, inp.get('child_name', ''))
        if not items:
            return "No open homework assignments."
        return '\n'.join(f"• {h['child']}: {h['subject']} — {h['description']} (due {h['due_date']}) [id:{h['id']}]" for h in items)
    if tool_name == 'save_teacher':
        return school_skill.save_teacher(
            profile_dir, inp['child_name'], inp['teacher_name'], inp['subject'],
            inp.get('phone', ''), inp.get('email', ''), inp.get('notes', ''))
    if tool_name == 'log_grade':
        return school_skill.log_grade(
            profile_dir, inp['child_name'], inp['subject'], inp['grade'],
            inp.get('assignment', ''), inp.get('date', ''))

    # ── ALLOWANCE ──────────────────────────────────────────────────────────
    if tool_name == 'set_allowance':
        return allowance_skill.set_allowance(
            profile_dir, inp['child_name'], inp['weekly_amount'],
            inp.get('day_of_week', 'Saturday'))
    if tool_name == 'pay_allowance':
        return allowance_skill.pay_allowance(
            profile_dir, inp['child_name'], inp.get('amount'), inp.get('notes', ''))
    if tool_name == 'spend_allowance':
        return allowance_skill.spend_allowance(
            profile_dir, inp['child_name'], inp['amount'], inp['description'])
    if tool_name == 'get_allowance_balance':
        children = allowance_skill.get_balance(profile_dir, inp.get('child_name', ''))
        if not children:
            return "No allowances set up yet."
        return '\n'.join(f"• {c['child']}: ${c.get('balance', 0):.2f} (${c['weekly_amount']:.2f}/week)" for c in children)

    # ── WEB SEARCH ─────────────────────────────────────────────────────────
    if tool_name == 'search_web':
        query = inp['query']
        search_type = inp.get('search_type', 'web')
        if search_type == 'news':
            return web_search.news_search(query)
        elif search_type == 'wikipedia':
            return web_search.wikipedia_lookup(query)
        else:
            return web_search.smart_search(query)

    # ── BEDTIME STORIES ────────────────────────────────────────────────────
    if tool_name == 'tell_bedtime_story':
        prompt = bedtime_story.story_request_text(
            inp.get('child_name', ''), inp.get('theme', ''),
            inp.get('characters', ''), inp.get('age', ''))
        return f"STORY_REQUEST:{prompt}"
    if tool_name == 'get_saved_stories':
        query = inp.get('query', '')
        if query:
            stories = bedtime_story.find_story(profile_dir, query)
        else:
            stories = bedtime_story.get_stories(profile_dir)
        if not stories:
            return "No saved stories yet."
        return '\n'.join(f"• {s['title']} ({s['date']})" for s in stories)

    # ── FULL SYSTEM TEST ───────────────────────────────────────────────────
    if tool_name == 'run_system_test':
        return _system_test.run(profile_dir, owner_name)

    # ── SELF-HEALTH & REPAIR ───────────────────────────────────────────────
    if tool_name == 'check_my_health':
        result = health_monitor.run_full_check()
        if result['healthy']:
            return "All systems healthy. Skills: OK, Profile data: OK, Memory: OK. I'm running well."
        lines = ["Health check found issues:"]
        if not result['skills']['ok']:
            lines.append(f"  Missing skill files: {', '.join(result['skills']['missing'])}")
        if not result['profiles']['ok']:
            lines.append(f"  Profile data errors: {'; '.join(result['profiles']['errors'])}")
        if not result['memory']['ok']:
            lines.append(f"  Memory error: {result['memory'].get('error','unknown')}")
        lines.append("I'm attempting self-repair now — use repair_myself to fix what I can.")
        return '\n'.join(lines)

    if tool_name == 'take_snapshot':
        label = inp.get('label', 'manual')
        name = snapshots.take_snapshot(label)
        if name:
            return f"Snapshot saved: '{name}'. I can roll back to this point any time."
        return "Snapshot failed — check logs."

    if tool_name == 'list_snapshots':
        snaps = snapshots.list_snapshots()
        if not snaps:
            return "No snapshots saved yet."
        return "Available restore points:\n" + '\n'.join(
            f"  • {s['name']}" for s in snaps[:10])

    if tool_name == 'rollback':
        snap = inp.get('snapshot_name')
        if snap:
            result = snapshots.rollback(snap)
        else:
            result = snapshots.rollback_latest()
        return result

    if tool_name == 'repair_myself':
        repaired = snapshots.repair_corrupted_files()
        if repaired:
            return f"Repaired {len(repaired)} file(s) from the latest snapshot: {', '.join(repaired)}"
        check = health_monitor.run_full_check()
        if check['healthy']:
            return "Ran a repair check — everything looks clean, nothing needed fixing."
        return "Issues found but I couldn't auto-repair them. Pinging for support."

    if tool_name == 'get_my_identity':
        identity = get_identity()
        lines = [
            f"My instance ID: {identity['instance_id']}",
            f"Created: {identity['created'][:10]}",
            f"Version: {identity['version']}",
        ]
        if identity.get('setup_url'):
            lines.append(f"Setup/QR URL: {identity['setup_url']}")
        else:
            lines.append("QR code is generated and saved as orby_qr.png — setup URL will be active once the bridge server is live.")
        bridge_status = "Connected" if identity.get('bridge_url') else "Not yet configured (coming soon)"
        lines.append(f"Bridge: {bridge_status}")
        return '\n'.join(lines)

    # ── PERMANENT MEMORY ───────────────────────────────────────────────────
    if tool_name == 'remember':
        import json
        from pathlib import Path
        long_path = Path(profile_dir) / 'long_term.json'
        memories = json.loads(long_path.read_text()) if long_path.exists() else []
        memories.append({
            'highlight': inp['fact'],
            'category': inp.get('category', 'general'),
            'date': __import__('datetime').datetime.utcnow().isoformat()
        })
        long_path.write_text(json.dumps(memories, indent=2))
        return f"Got it — I'll always remember: {inp['fact']}"

    if tool_name == 'recall':
        import json
        from pathlib import Path
        long_path = Path(profile_dir) / 'long_term.json'
        if not long_path.exists():
            return "I don't have any permanent memories saved yet."
        memories = json.loads(long_path.read_text())
        if not memories:
            return "I don't have any permanent memories saved yet."
        query = inp['query'].lower()
        matches = [m for m in memories if query in m['highlight'].lower() or query in m.get('category', '').lower()]
        if not matches:
            matches = memories
        return "\n".join(f"• [{m.get('category','general')}] {m['highlight']}" for m in matches[-20:])

    if tool_name == 'update_profile':
        import json
        from pathlib import Path
        prof_path = Path(profile_dir) / 'profile.json'
        profile = json.loads(prof_path.read_text()) if prof_path.exists() else {}
        if 'owner_name' in inp and inp['owner_name']:
            profile['owner_name'] = inp['owner_name']
        if 'orby_name' in inp and inp['orby_name']:
            profile['orby_name'] = inp['orby_name']
        if 'website_url' in inp and inp['website_url']:
            profile['website_url'] = inp['website_url']
        prof_path.write_text(json.dumps(profile, indent=2))
        parts = []
        if 'owner_name' in inp and inp['owner_name']:
            parts.append(f"your name is {inp['owner_name']}")
        if 'orby_name' in inp and inp['orby_name']:
            parts.append(f"my name is {inp['orby_name']}")
        return "Profile updated — " + " and ".join(parts) + "." if parts else "Profile updated."

    if tool_name == 'scan_website':
        import json, re, requests
        from pathlib import Path
        url = inp['url']
        if not url.startswith('http'):
            url = 'https://' + url
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; OrbyBot/1.0)'}
            r = requests.get(url, headers=headers, timeout=12)
            r.raise_for_status()
            text = re.sub(r'<[^>]+>', ' ', r.text)
            text = re.sub(r'\s+', ' ', text).strip()[:4000]
            prof_path = Path(profile_dir) / 'profile.json'
            profile = json.loads(prof_path.read_text()) if prof_path.exists() else {}
            profile['website_url'] = url
            profile['website_content'] = text
            prof_path.write_text(json.dumps(profile, indent=2))
            return f"WEBSITE CONTENT FROM {url}:\n{text}"
        except Exception as e:
            return f"Couldn't read {url}: {e}. Ask the user to double-check the address."

    if tool_name == 'complete_onboarding':
        import json
        from pathlib import Path
        prof_path = Path(profile_dir) / 'profile.json'
        profile = json.loads(prof_path.read_text()) if prof_path.exists() else {}
        profile['onboarding_complete'] = True
        prof_path.write_text(json.dumps(profile, indent=2))
        return "Onboarding complete."

    # ── PRODUCT DEVELOPMENT ────────────────────────────────────────────────
    if tool_name == 'save_product_idea':
        return product_dev.save_product_idea(
            profile_dir, inp['idea'], inp.get('product', ''))

    if tool_name == 'get_product_ideas':
        return product_dev.get_product_ideas(
            profile_dir, inp.get('product', ''), inp.get('status', ''))

    if tool_name == 'update_idea_status':
        return product_dev.update_idea_status(profile_dir, inp['idea_id'], inp['status'])

    if tool_name == 'add_product':
        return product_dev.add_product(
            profile_dir, inp['name'],
            description=inp.get('description', ''),
            status=inp.get('status', 'in_development'),
            target_audience=inp.get('target_audience', ''),
            revenue_model=inp.get('revenue_model', ''),
            industry_tags=inp.get('industry_tags', ''),
            linked_modules=inp.get('linked_modules', ''))

    if tool_name == 'update_product':
        return product_dev.update_product(
            profile_dir, inp['product_id'],
            description=inp.get('description', ''),
            target_audience=inp.get('target_audience', ''),
            revenue_model=inp.get('revenue_model', ''),
            industry_tags=inp.get('industry_tags', ''),
            linked_modules=inp.get('linked_modules', ''))

    if tool_name == 'get_product_dashboard':
        return product_dev.get_product_dashboard(profile_dir)

    if tool_name == 'get_products':
        return product_dev.get_products(profile_dir, inp.get('status', ''))

    if tool_name == 'update_product_status':
        return product_dev.update_product_status(profile_dir, inp['product_id'], inp['status'])

    if tool_name == 'add_product_note':
        return product_dev.add_product_note(profile_dir, inp['product_id'], inp['content'])

    if tool_name == 'get_product_notes':
        return product_dev.get_product_notes(profile_dir, inp['product_id'])

    if tool_name == 'add_feature':
        return product_dev.add_feature(
            profile_dir,
            product=inp['product'],
            name=inp['name'],
            description=inp.get('description', ''),
            tier=inp.get('tier', 'free'),
            status=inp.get('status', 'planned'),
            priority=inp.get('priority', 'normal'))

    if tool_name == 'update_feature_status':
        return product_dev.update_feature_status(profile_dir, inp['feature_id'], inp['status'])

    if tool_name == 'get_features':
        return product_dev.get_features(
            profile_dir,
            product=inp.get('product', ''),
            status_filter=inp.get('status', ''),
            tier_filter=inp.get('tier', ''))

    if tool_name == 'get_product_roadmap':
        return product_dev.get_roadmap(profile_dir, inp.get('product', ''))

    if tool_name == 'add_launch_checklist':
        return product_dev.add_launch_checklist(
            profile_dir, inp['product'], inp['name'], inp.get('items'))

    if tool_name == 'add_checklist_item':
        return product_dev.add_checklist_item(profile_dir, inp['checklist_id'], inp['task'])

    if tool_name == 'complete_checklist_item':
        return product_dev.complete_checklist_item(
            profile_dir, inp['checklist_id'], inp['item_id'])

    if tool_name == 'get_launch_checklist':
        return product_dev.get_launch_checklist(profile_dir, inp.get('checklist_id', ''))

    # ── DEEP MEMORY ────────────────────────────────────────────────────────
    if tool_name == 'save_context_note':
        return deep_memory.save_context_note(
            profile_dir, inp['topic'], inp['content'],
            inp.get('project', ''), inp.get('tags', ''))

    if tool_name == 'get_context_note':
        return deep_memory.get_context_note(profile_dir, inp['topic'])

    if tool_name == 'search_context':
        return deep_memory.search_context(profile_dir, inp['query'], inp.get('project', ''))

    if tool_name == 'list_context_notes':
        return deep_memory.list_context_notes(profile_dir, inp.get('project', ''))

    if tool_name == 'delete_context_note':
        return deep_memory.delete_context_note(profile_dir, inp['note_id'])

    if tool_name == 'log_decision':
        return deep_memory.log_decision(
            profile_dir, inp['decision'], inp['reasoning'],
            inp.get('project', ''), inp.get('alternatives', ''))

    if tool_name == 'get_decisions':
        return deep_memory.get_decisions(profile_dir, inp.get('project', ''), inp.get('query', ''))

    if tool_name == 'save_session_summary':
        return deep_memory.save_session_summary(
            profile_dir, inp['title'], inp['what_we_did'],
            inp.get('decisions', ''), inp.get('next_steps', ''), inp.get('project', ''))

    if tool_name == 'get_session':
        return deep_memory.get_session(profile_dir, inp['session_id'])

    if tool_name == 'list_sessions':
        return deep_memory.list_sessions(profile_dir, inp.get('project', ''), inp.get('limit', 20))

    if tool_name == 'search_sessions':
        return deep_memory.search_sessions(profile_dir, inp['query'])

    if tool_name == 'search_everything':
        return deep_memory.search_everything(profile_dir, inp['query'])

    if tool_name == 'save_spec':
        return deep_memory.save_spec(
            profile_dir,
            title=inp['title'],
            purpose=inp['purpose'],
            data_model=inp['data_model'],
            tools=inp['tools'],
            file_path=inp['file_path'],
            behavior_rules=inp.get('behavior_rules', ''),
            test_notes=inp.get('test_notes', ''),
            project=inp.get('project', ''))

    if tool_name == 'get_spec':
        return deep_memory.get_spec(profile_dir, inp['title'])

    if tool_name == 'list_specs':
        return deep_memory.list_specs(profile_dir, inp.get('project', ''))

    if tool_name == 'update_spec_status':
        return deep_memory.update_spec_status(profile_dir, inp['spec_id'], inp['status'])

    # ── SOCIAL MEDIA ───────────────────────────────────────────────────────
    if tool_name == 'get_social_setup_guide':
        return social_media.get_setup_guide(profile_dir, inp['platform'])
    if tool_name == 'connect_social_account':
        return social_media.connect_account(profile_dir, inp['platform'], inp['credentials'])
    if tool_name == 'get_social_accounts':
        return social_media.get_accounts(profile_dir)
    if tool_name == 'disconnect_social_account':
        return social_media.disconnect_account(profile_dir, inp['platform'])
    if tool_name == 'setup_page_profile':
        return social_media.setup_page_profile(
            profile_dir, inp['platform'],
            bio=inp.get('bio',''), website=inp.get('website',''),
            phone=inp.get('phone',''), email=inp.get('email',''),
            hours=inp.get('hours',''), description=inp.get('description',''))
    if tool_name == 'create_post':
        return social_media.create_post(
            profile_dir, inp['content'],
            inp.get('platforms'), inp.get('image_url',''))
    if tool_name == 'schedule_post':
        return social_media.schedule_post(
            profile_dir, inp['content'], inp['publish_at'],
            inp.get('platforms'), inp.get('image_url',''))
    if tool_name == 'get_scheduled_posts':
        return social_media.get_scheduled_posts(profile_dir)
    if tool_name == 'cancel_scheduled_post':
        return social_media.cancel_scheduled_post(profile_dir, inp['post_id'])
    if tool_name == 'get_post_history':
        return social_media.get_post_history(
            profile_dir, inp.get('platform',''), inp.get('limit', 10))
    if tool_name == 'save_post_draft':
        return social_media.save_draft(
            profile_dir, inp['content'],
            inp.get('platforms'), inp.get('notes',''))
    if tool_name == 'get_post_drafts':
        return social_media.get_drafts(profile_dir)
    if tool_name == 'delete_post_draft':
        return social_media.delete_draft(profile_dir, inp['draft_id'])
    if tool_name == 'get_social_analytics':
        return social_media.get_analytics(profile_dir, inp['platform'])
    if tool_name == 'get_page_info':
        return social_media.get_page_info(profile_dir, inp['platform'])

    # ── MODULE MANAGEMENT ──────────────────────────────────────────────────
    if tool_name == 'list_modules':
        tier = inp.get('filter', 'all')
        return module_manager.list_modules(tier)

    if tool_name == 'get_module_info':
        return module_manager.get_module_info(inp['module_id'])

    if tool_name == 'check_for_updates':
        return module_manager.check_for_updates()

    if tool_name == 'request_module':
        return module_manager.request_module(inp['module_id'], owner_name)

    if tool_name == 'activate_module':
        return module_manager.activate_module(inp['module_id'], inp.get('license_key', ''))

    # ── AI IMAGE STUDIO ────────────────────────────────────────────────────
    if tool_name == 'generate_image':
        return image_studio.generate_image(
            profile_dir,
            inp['prompt'],
            inp.get('style', ''),
            inp.get('size', '1024x1024'),
            inp.get('quality', 'standard')
        )
    if tool_name == 'get_image_library':
        return image_studio.get_image_library(profile_dir, inp.get('limit', 20))
    if tool_name == 'delete_image_project':
        return image_studio.delete_image_project(profile_dir, inp['project_id'])
    if tool_name == 'set_image_api_key':
        return image_studio.set_image_api_key(profile_dir, inp['api_key'])
    if tool_name == 'get_image_api_status':
        return image_studio.get_image_api_status(profile_dir)

    # ── 3D CREATOR ─────────────────────────────────────────────────────────
    if tool_name == 'generate_3d':
        return creator_3d.generate_3d(
            profile_dir,
            inp['prompt'],
            inp.get('art_style', 'realistic'),
            inp.get('negative_prompt', '')
        )
    if tool_name == 'check_3d_job':
        return creator_3d.check_3d_job(profile_dir, inp['task_id'])
    if tool_name == 'get_3d_library':
        return creator_3d.get_3d_library(profile_dir, inp.get('limit', 20))
    if tool_name == 'delete_3d_project':
        return creator_3d.delete_3d_project(profile_dir, inp['project_id'])
    if tool_name == 'set_3d_api_key':
        return creator_3d.set_3d_api_key(profile_dir, inp['api_key'])
    if tool_name == 'get_3d_api_status':
        return creator_3d.get_3d_api_status(profile_dir)

    # ── AI VIDEO STUDIO ────────────────────────────────────────────────────
    if tool_name == 'generate_video':
        return video_studio.generate_video(
            profile_dir,
            inp['prompt'],
            inp.get('duration', 5),
            inp.get('ratio', '1280:720')
        )
    if tool_name == 'check_video_job':
        return video_studio.check_video_job(profile_dir, inp['task_id'])
    if tool_name == 'get_video_library':
        return video_studio.get_video_library(profile_dir, inp.get('limit', 20))
    if tool_name == 'delete_video_project':
        return video_studio.delete_video_project(profile_dir, inp['project_id'])
    if tool_name == 'set_video_api_key':
        return video_studio.set_video_api_key(profile_dir, inp['api_key'])
    if tool_name == 'get_video_api_status':
        return video_studio.get_video_api_status(profile_dir)

    # ── I001: LEGAL PRO ────────────────────────────────────────────────────
    if tool_name == 'add_legal_case':
        return legal_pro.add_legal_case(
            profile_dir, inp['client_name'], inp['case_type'],
            inp['description'], inp.get('status', 'open'))
    if tool_name == 'list_legal_cases':
        return legal_pro.list_legal_cases(profile_dir, inp.get('status', ''))
    if tool_name == 'update_legal_case':
        return legal_pro.update_legal_case(
            profile_dir, inp['case_id'],
            inp.get('status', ''), inp.get('notes', ''), inp.get('description', ''))
    if tool_name == 'add_case_deadline':
        return legal_pro.add_case_deadline(
            profile_dir, inp['case_id'], inp['deadline_date'], inp['description'])
    if tool_name == 'log_billable_time':
        return legal_pro.log_billable_time(
            profile_dir, inp['case_id'], inp['hours'],
            inp['description'], inp.get('date', ''))
    if tool_name == 'get_billing_summary':
        return legal_pro.get_billing_summary(profile_dir, inp.get('case_id', ''))
    if tool_name == 'add_case_note':
        return legal_pro.add_case_note(profile_dir, inp['case_id'], inp['note'])
    if tool_name == 'get_legal_case':
        return legal_pro.get_legal_case(profile_dir, inp['case_id'])

    # ── I002: MEDICAL PRO ──────────────────────────────────────────────────
    if tool_name == 'add_patient':
        return medical_pro.add_patient(
            profile_dir, inp['name'], inp['dob'], inp['phone'],
            inp.get('email', ''), inp.get('insurance', ''), inp.get('notes', ''))
    if tool_name == 'list_patients':
        return medical_pro.list_patients(profile_dir, inp.get('search', ''))
    if tool_name == 'add_medical_appointment':
        return medical_pro.add_medical_appointment(
            profile_dir, inp['patient_name'], inp['date'], inp['time'],
            inp['type'], inp.get('notes', ''))
    if tool_name == 'list_medical_appointments':
        return medical_pro.list_medical_appointments(
            profile_dir, inp.get('date', ''), inp.get('upcoming_only', True))
    if tool_name == 'add_treatment_note':
        return medical_pro.add_treatment_note(
            profile_dir, inp['patient_name'], inp['date'],
            inp['note'], inp.get('provider', ''))
    if tool_name == 'get_patient_history':
        return medical_pro.get_patient_history(profile_dir, inp['patient_name'])
    if tool_name == 'add_prescription':
        return medical_pro.add_prescription(
            profile_dir, inp['patient_name'], inp['medication'],
            inp['dosage'], inp['prescriber'], inp.get('date', ''))
    if tool_name == 'list_prescriptions':
        return medical_pro.list_prescriptions(profile_dir, inp['patient_name'])

    # ── I003: REAL ESTATE PRO ──────────────────────────────────────────────
    if tool_name == 'add_listing':
        return realestate_pro.add_listing(
            profile_dir, inp['address'], inp['price'],
            inp['bedrooms'], inp['bathrooms'], inp['sqft'],
            inp.get('status', 'active'), inp.get('notes', ''))
    if tool_name == 'list_listings':
        return realestate_pro.list_listings(profile_dir, inp.get('status', ''))
    if tool_name == 'update_listing':
        return realestate_pro.update_listing(
            profile_dir, inp['listing_id'],
            inp.get('status', ''), inp.get('price', 0), inp.get('notes', ''))
    if tool_name == 'add_re_client':
        return realestate_pro.add_re_client(
            profile_dir, inp['name'], inp['type'], inp['phone'],
            inp.get('email', ''), inp.get('budget', ''), inp.get('notes', ''))
    if tool_name == 'list_re_clients':
        return realestate_pro.list_re_clients(profile_dir, inp.get('type', ''))
    if tool_name == 'add_showing':
        return realestate_pro.add_showing(
            profile_dir, inp['listing_id'], inp['client_name'],
            inp['date'], inp['time'], inp.get('notes', ''))
    if tool_name == 'list_showings':
        return realestate_pro.list_showings(profile_dir, inp.get('upcoming_only', True))
    if tool_name == 'add_offer':
        return realestate_pro.add_offer(
            profile_dir, inp['listing_id'], inp['client_name'],
            inp['amount'], inp['date'], inp.get('status', 'pending'), inp.get('notes', ''))
    if tool_name == 'calculate_commission':
        return realestate_pro.calculate_commission(
            profile_dir, inp['sale_price'], inp.get('commission_rate', 6.0))

    # ── I004: RESTAURANT PRO ───────────────────────────────────────────────
    if tool_name == 'add_menu_item':
        return restaurant_pro.add_menu_item(
            profile_dir, inp['name'], inp['category'], inp['price'],
            inp.get('description', ''), inp.get('available', True))
    if tool_name == 'list_menu':
        return restaurant_pro.list_menu(profile_dir, inp.get('category', ''))
    if tool_name == 'update_menu_item':
        return restaurant_pro.update_menu_item(
            profile_dir, inp['item_id'],
            inp.get('price', 0), inp.get('available'), inp.get('description', ''))
    if tool_name == 'update_inventory':
        return restaurant_pro.update_inventory(
            profile_dir, inp['item_name'], inp['quantity'],
            inp['unit'], inp.get('reorder_level', 0))
    if tool_name == 'get_low_inventory':
        return restaurant_pro.get_low_inventory(profile_dir)
    if tool_name == 'add_reservation':
        return restaurant_pro.add_reservation(
            profile_dir, inp['name'], inp['date'], inp['time'],
            inp['party_size'], inp.get('phone', ''), inp.get('notes', ''))
    if tool_name == 'list_reservations':
        return restaurant_pro.list_reservations(profile_dir, inp.get('date', ''))
    if tool_name == 'add_supplier':
        return restaurant_pro.add_supplier(
            profile_dir, inp['name'], inp['category'], inp['contact'],
            inp.get('phone', ''), inp.get('email', ''), inp.get('notes', ''))

    # ── I005: RETAIL PRO ───────────────────────────────────────────────────
    if tool_name == 'add_retail_product':
        return retail_pro.add_product(
            profile_dir, inp['name'], inp['sku'], inp['price'], inp['category'],
            inp.get('stock', 0), inp.get('reorder_level', 5), inp.get('notes', ''))
    if tool_name == 'list_products':
        return retail_pro.list_products(
            profile_dir, inp.get('category', ''), inp.get('low_stock_only', False))
    if tool_name == 'update_stock':
        return retail_pro.update_stock(
            profile_dir, inp['sku'], inp['quantity_change'], inp.get('reason', ''))
    if tool_name == 'get_low_stock':
        return retail_pro.get_low_stock(profile_dir)
    if tool_name == 'add_retail_customer':
        return retail_pro.add_retail_customer(
            profile_dir, inp['name'],
            inp.get('email', ''), inp.get('phone', ''), inp.get('notes', ''))
    if tool_name == 'record_sale':
        return retail_pro.record_sale(
            profile_dir, inp['items'],
            inp.get('customer_name', ''), inp.get('notes', ''))
    if tool_name == 'get_sales_report':
        return retail_pro.get_sales_report(profile_dir, inp.get('days', 30))
    if tool_name == 'add_retail_supplier':
        return retail_pro.add_retail_supplier(
            profile_dir, inp['name'], inp['category'], inp['contact'],
            inp.get('phone', ''), inp.get('email', ''))

    # ── I006: SALON PRO ────────────────────────────────────────────────────
    if tool_name == 'add_salon_client':
        return salon_pro.add_salon_client(
            profile_dir, inp['name'], inp['phone'],
            inp.get('email', ''), inp.get('birthday', ''),
            inp.get('notes', ''), inp.get('allergies', ''))
    if tool_name == 'list_salon_clients':
        return salon_pro.list_salon_clients(profile_dir, inp.get('search', ''))
    if tool_name == 'add_salon_appointment':
        return salon_pro.add_salon_appointment(
            profile_dir, inp['client_name'], inp['date'], inp['time'],
            inp['service'], inp.get('staff', ''),
            inp.get('duration_min', 60), inp.get('notes', ''))
    if tool_name == 'list_salon_appointments':
        return salon_pro.list_salon_appointments(
            profile_dir, inp.get('date', ''), inp.get('staff', ''))
    if tool_name == 'add_service':
        return salon_pro.add_service(
            profile_dir, inp['name'], inp['category'],
            inp['price'], inp['duration_min'], inp.get('description', ''))
    if tool_name == 'log_client_visit':
        return salon_pro.log_client_visit(
            profile_dir, inp['client_name'], inp['date'],
            inp['services'], inp['total'],
            inp.get('staff', ''), inp.get('notes', ''))
    if tool_name == 'get_client_history':
        return salon_pro.get_client_history(profile_dir, inp['client_name'])

    # ── I007: CONTRACTOR PRO ───────────────────────────────────────────────
    if tool_name == 'add_job':
        return contractor_pro.add_job(
            profile_dir, inp['client_name'], inp['address'], inp['description'],
            inp.get('start_date', ''), inp.get('end_date', ''),
            inp.get('status', 'active'), inp.get('notes', ''))
    if tool_name == 'list_jobs':
        return contractor_pro.list_jobs(profile_dir, inp.get('status', ''))
    if tool_name == 'update_job':
        return contractor_pro.update_job(
            profile_dir, inp['job_id'],
            inp.get('status', ''), inp.get('notes', ''), inp.get('end_date', ''))
    if tool_name == 'add_estimate':
        return contractor_pro.add_estimate(
            profile_dir, inp['job_id'], inp['labor_cost'], inp['materials_cost'],
            inp.get('description', ''), inp.get('markup_pct', 20.0))
    if tool_name == 'add_materials':
        return contractor_pro.add_materials(profile_dir, inp['job_id'], inp['items'])
    if tool_name == 'add_subcontractor':
        return contractor_pro.add_subcontractor(
            profile_dir, inp['name'], inp['trade'], inp['phone'],
            inp.get('email', ''), inp.get('rate', ''), inp.get('notes', ''))
    if tool_name == 'create_invoice':
        return contractor_pro.create_invoice(
            profile_dir, inp['job_id'],
            inp.get('due_date', ''), inp.get('notes', ''))
    if tool_name == 'log_job_hours':
        return contractor_pro.log_job_hours(
            profile_dir, inp['job_id'], inp['date'],
            inp['hours'], inp['worker'], inp.get('description', ''))
    if tool_name == 'get_job_summary':
        return contractor_pro.get_job_summary(profile_dir, inp['job_id'])

    # ── I008: TRADE SPECIALTIES ────────────────────────────────────────────
    if tool_name == 'add_trade_job':
        return trade_pro.add_trade_job(
            profile_dir, inp['trade'], inp['client_name'], inp['address'],
            inp['description'], inp.get('status', 'active'), inp.get('notes', ''))
    if tool_name == 'list_trade_jobs':
        return trade_pro.list_trade_jobs(profile_dir, inp.get('trade', ''))
    if tool_name == 'add_trade_note':
        return trade_pro.add_trade_note(
            profile_dir, inp['job_id'], inp['note'], inp.get('code', ''))
    if tool_name == 'get_common_codes':
        return trade_pro.get_common_codes(profile_dir, inp['trade'])
    if tool_name == 'add_material_order':
        return trade_pro.add_material_order(
            profile_dir, inp['job_id'], inp['supplier'], inp['items'],
            inp.get('order_date', ''), inp.get('status', 'ordered'))
    if tool_name == 'get_trade_summary':
        return trade_pro.get_trade_summary(profile_dir, inp['trade'])

    # ── I009: ACCOUNTING PRO ───────────────────────────────────────────────
    if tool_name == 'add_accounting_client':
        return accounting_pro.add_accounting_client(
            profile_dir, inp['name'], inp['business_name'], inp['phone'],
            inp.get('email', ''), inp.get('tax_id', ''),
            inp.get('filing_type', ''), inp.get('notes', ''))
    if tool_name == 'list_accounting_clients':
        return accounting_pro.list_accounting_clients(profile_dir, inp.get('search', ''))
    if tool_name == 'add_tax_deadline':
        return accounting_pro.add_tax_deadline(
            profile_dir, inp['client_name'], inp['deadline_type'], inp['due_date'],
            inp.get('description', ''), inp.get('status', 'pending'))
    if tool_name == 'get_upcoming_tax_deadlines':
        return accounting_pro.get_upcoming_tax_deadlines(profile_dir, inp.get('days', 30))
    if tool_name == 'log_transaction':
        return accounting_pro.log_transaction(
            profile_dir, inp['client_name'], inp['date'],
            inp['type'], inp['amount'], inp['description'], inp.get('category', ''))
    if tool_name == 'get_client_financials':
        return accounting_pro.get_client_financials(
            profile_dir, inp['client_name'], inp.get('year', ''))
    if tool_name == 'add_document_ref':
        return accounting_pro.add_document_ref(
            profile_dir, inp['client_name'], inp['doc_type'],
            inp['filename'], inp['year'], inp.get('notes', ''))
    if tool_name == 'get_document_refs':
        return accounting_pro.get_document_refs(profile_dir, inp['client_name'])

    # ── I010: HR PROFESSIONAL ──────────────────────────────────────────────
    if tool_name == 'add_employee':
        return hr_pro.add_employee(
            profile_dir, inp['name'], inp['title'], inp['department'],
            inp['start_date'], inp.get('email', ''), inp.get('phone', ''),
            inp.get('manager', ''), inp.get('status', 'active'))
    if tool_name == 'list_employees':
        return hr_pro.list_employees(
            profile_dir, inp.get('department', ''), inp.get('status', 'active'))
    if tool_name == 'get_employee':
        return hr_pro.get_employee(profile_dir, inp['name'])
    if tool_name == 'add_onboarding_task':
        return hr_pro.add_onboarding_task(
            profile_dir, inp['employee_name'], inp['task'],
            inp.get('due_date', ''), inp.get('completed', False))
    if tool_name == 'get_onboarding_status':
        return hr_pro.get_onboarding_status(profile_dir, inp['employee_name'])
    if tool_name == 'log_pto_request':
        return hr_pro.log_pto_request(
            profile_dir, inp['employee_name'], inp['start_date'],
            inp['end_date'], inp['type'],
            inp.get('status', 'pending'), inp.get('notes', ''))
    if tool_name == 'get_pto_balance':
        return hr_pro.get_pto_balance(profile_dir, inp['employee_name'])
    if tool_name == 'schedule_performance_review':
        return hr_pro.schedule_performance_review(
            profile_dir, inp['employee_name'], inp['review_date'],
            inp.get('reviewer', ''), inp.get('notes', ''))
    if tool_name == 'get_upcoming_reviews':
        return hr_pro.get_upcoming_reviews(profile_dir, inp.get('days', 60))

    # ── I011: THERAPY PRO ──────────────────────────────────────────────────
    if tool_name == 'add_therapy_client':
        return therapy_pro.add_therapy_client(
            profile_dir, inp['name'], inp.get('dob', ''), inp.get('phone', ''),
            inp.get('email', ''), inp.get('insurance', ''),
            inp.get('presenting_issue', ''), inp.get('notes', ''))
    if tool_name == 'list_therapy_clients':
        return therapy_pro.list_therapy_clients(profile_dir, inp.get('search', ''))
    if tool_name == 'add_session_note':
        return therapy_pro.add_session_note(
            profile_dir, inp['client_name'], inp['date'], inp['duration_min'],
            inp['session_type'], inp['note'],
            inp.get('mood_rating', 0), inp.get('provider', ''))
    if tool_name == 'get_session_history':
        return therapy_pro.get_session_history(
            profile_dir, inp['client_name'], inp.get('limit', 10))
    if tool_name == 'add_therapy_appointment':
        return therapy_pro.add_therapy_appointment(
            profile_dir, inp['client_name'], inp['date'], inp['time'],
            inp['type'], inp.get('provider', ''), inp.get('notes', ''))
    if tool_name == 'list_therapy_appointments':
        return therapy_pro.list_therapy_appointments(
            profile_dir, inp.get('date', ''), inp.get('upcoming_only', True))
    if tool_name == 'add_treatment_plan':
        return therapy_pro.add_treatment_plan(
            profile_dir, inp['client_name'], inp['goals'],
            inp['interventions'], inp['review_date'], inp.get('provider', ''))
    if tool_name == 'get_treatment_plan':
        return therapy_pro.get_treatment_plan(profile_dir, inp['client_name'])

    # ── PROPERTY MANAGEMENT ────────────────────────────────────────────────
    if tool_name == 'add_property':
        return property_mgmt.add_property(
            profile_dir, inp['address'], inp['type'],
            inp.get('bedrooms', 0), inp.get('bathrooms', 0),
            inp.get('rent', 0), inp.get('notes', ''))
    if tool_name == 'list_properties':
        return property_mgmt.list_properties(profile_dir, inp.get('status', ''))
    if tool_name == 'add_tenant':
        return property_mgmt.add_tenant(
            profile_dir, inp['name'], inp['phone'],
            inp.get('email', ''), inp.get('id_type', ''), inp.get('notes', ''))
    if tool_name == 'list_tenants':
        return property_mgmt.list_tenants(profile_dir, inp.get('search', ''))
    if tool_name == 'add_lease':
        return property_mgmt.add_lease(
            profile_dir, inp['property_id'], inp['tenant_name'],
            inp['start_date'], inp['end_date'], inp['monthly_rent'],
            inp.get('deposit', 0), inp.get('notes', ''))
    if tool_name == 'get_lease':
        return property_mgmt.get_lease(
            profile_dir, inp.get('tenant_name', ''), inp.get('property_id', ''))
    if tool_name == 'log_rent_payment':
        return property_mgmt.log_rent_payment(
            profile_dir, inp['tenant_name'], inp['amount'],
            inp['payment_date'], inp.get('method', 'check'), inp.get('notes', ''))
    if tool_name == 'get_rent_status':
        return property_mgmt.get_rent_status(profile_dir, inp.get('tenant_name', ''))
    if tool_name == 'add_maintenance_request':
        return property_mgmt.add_maintenance_request(
            profile_dir, inp['property_id'], inp['tenant_name'],
            inp['description'], inp.get('priority', 'normal'), inp.get('category', ''))
    if tool_name == 'list_maintenance_requests':
        return property_mgmt.list_maintenance_requests(
            profile_dir, inp.get('status', 'open'), inp.get('property_id', ''))
    if tool_name == 'update_maintenance_request':
        return property_mgmt.update_maintenance_request(
            profile_dir, inp['request_id'], inp.get('status', ''),
            inp.get('notes', ''), inp.get('vendor', ''), inp.get('cost', 0))

    # ── BUSINESS PRO ───────────────────────────────────────────────────────
    if tool_name == 'add_business_customer':
        return business_pro.add_business_customer(
            profile_dir, inp['name'], inp.get('phone', ''), inp.get('email', ''),
            inp.get('company', ''), inp.get('source', ''), inp.get('notes', ''))
    if tool_name == 'list_business_customers':
        return business_pro.list_business_customers(profile_dir, inp.get('search', ''))
    if tool_name == 'get_business_customer':
        return business_pro.get_business_customer(profile_dir, inp['name'])
    if tool_name == 'add_business_note':
        return business_pro.add_business_note(profile_dir, inp['customer_name'], inp['note'])
    if tool_name == 'create_business_invoice':
        return business_pro.create_invoice(
            profile_dir, inp['customer_name'], inp['amount'],
            inp['description'], inp.get('due_date', ''), inp.get('notes', ''))
    if tool_name == 'list_invoices':
        return business_pro.list_invoices(
            profile_dir, inp.get('status', ''), inp.get('customer_name', ''))
    if tool_name == 'mark_invoice_paid':
        return business_pro.mark_invoice_paid(
            profile_dir, inp['invoice_id'], inp.get('payment_date', ''), inp.get('method', ''))
    if tool_name == 'create_quote':
        return business_pro.create_quote(
            profile_dir, inp['customer_name'], inp['amount'], inp['description'],
            inp.get('valid_days', 30), inp.get('notes', ''))
    if tool_name == 'log_business_expense':
        return business_pro.log_expense(
            profile_dir, inp['amount'], inp['category'],
            inp['description'], inp.get('date', ''), inp.get('vendor', ''))
    if tool_name == 'get_expense_summary':
        return business_pro.get_expense_summary(profile_dir, inp.get('month', ''))
    if tool_name == 'add_business_task':
        return business_pro.add_business_task(
            profile_dir, inp['task'], inp.get('due_date', ''),
            inp.get('assigned_to', ''), inp.get('priority', 'normal'))
    if tool_name == 'list_business_tasks':
        return business_pro.list_business_tasks(
            profile_dir, inp.get('assigned_to', ''), inp.get('priority', ''))
    if tool_name == 'complete_business_task':
        return business_pro.complete_business_task(profile_dir, inp['task_id'])
    if tool_name == 'get_business_dashboard':
        return business_pro.get_business_dashboard(profile_dir)

    # ── INVENTORY PRO ──────────────────────────────────────────────────────
    if tool_name == 'setup_inventory_config':
        return inventory_pro.setup_inventory_config(
            profile_dir, inp['business_name'],
            inp.get('default_location', 'Main'),
            inp.get('currency', 'USD'),
            inp.get('low_stock_default', 10),
            inp.get('track_by', 'sku'),
            inp.get('notes', ''))
    if tool_name == 'add_inventory_item':
        return inventory_pro.add_inventory_item(
            profile_dir, inp['name'], inp['sku'],
            inp.get('category', ''), inp.get('unit', 'each'),
            inp.get('cost', 0), inp.get('price', 0),
            inp.get('reorder_level', 0),
            inp.get('location', ''), inp.get('notes', ''))
    if tool_name == 'list_inventory':
        return inventory_pro.list_inventory(
            profile_dir, inp.get('category', ''),
            inp.get('location', ''),
            inp.get('low_stock_only', False),
            inp.get('limit', 50))
    if tool_name == 'search_inventory':
        return inventory_pro.search_inventory(profile_dir, inp['query'])
    if tool_name == 'update_stock_level':
        return inventory_pro.update_stock_level(
            profile_dir, inp['sku'], inp['quantity_change'],
            inp.get('location', ''), inp.get('reason', ''), inp.get('reference', ''))
    if tool_name == 'bulk_update_stock':
        return inventory_pro.bulk_update_stock(profile_dir, inp['updates'])
    if tool_name == 'add_inventory_location':
        return inventory_pro.add_inventory_location(
            profile_dir, inp['name'],
            inp.get('description', ''), inp.get('address', ''))
    if tool_name == 'list_inventory_locations':
        return inventory_pro.list_inventory_locations(profile_dir)
    if tool_name == 'add_inventory_supplier':
        return inventory_pro.add_inventory_supplier(
            profile_dir, inp['name'], inp.get('contact', ''),
            inp.get('phone', ''), inp.get('email', ''),
            inp.get('lead_days', 0), inp.get('notes', ''))
    if tool_name == 'create_purchase_order':
        return inventory_pro.create_purchase_order(
            profile_dir, inp['supplier_name'], inp['items'], inp.get('notes', ''))
    if tool_name == 'receive_purchase_order':
        return inventory_pro.receive_purchase_order(
            profile_dir, inp['po_id'], inp.get('received_items'))
    if tool_name == 'list_purchase_orders':
        return inventory_pro.list_purchase_orders(profile_dir, inp.get('status', ''))
    if tool_name == 'get_low_stock_report':
        return inventory_pro.get_low_stock_report(profile_dir, inp.get('location', ''))
    if tool_name == 'get_inventory_value_report':
        return inventory_pro.get_inventory_value_report(profile_dir, inp.get('location', ''))
    if tool_name == 'get_stock_movement_report':
        return inventory_pro.get_stock_movement_report(
            profile_dir, inp.get('sku', ''), inp.get('days', 30))
    if tool_name == 'add_inventory_category':
        return inventory_pro.add_inventory_category(
            profile_dir, inp['name'],
            inp.get('parent_category', ''), inp.get('notes', ''))

    return f"Unknown tool: {tool_name}"
