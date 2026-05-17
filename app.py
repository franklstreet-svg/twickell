"""My Orby marketing website — twickell.com"""
import os
import re
import json
import uuid
import time
import hashlib
import asyncio
import threading
import logging
from pathlib import Path
import requests as _requests
import edge_tts
from flask import Flask, request, jsonify, send_from_directory, Response, session
from dotenv import load_dotenv

from modules.free import (
    reminders, todo_skill, notes_skill, shopping_skill, weather_skill,
    morning_briefing, calendar_skill, finance_skill, fitness_skill,
    health_skill, chores_skill, school_skill, pets_skill, vehicle_skill,
    travel_skill, gifts_skill, habit_tracker, meal_planning, journal_skill,
    relationship_skill, countdown_skill, bucket_list, quotes_skill,
    emergency_info, family_msg, home_maintenance, allowance_skill,
    bedtime_story, mood_skill, web_search, recipes_skill, world_clock,
)
from modules.paid import (
    legal_pro, legal_docs, legal_motions, legal_contracts, legal_letters,
    medical_pro, medical_notes,
    therapy_pro, therapy_notes,
    realestate_pro, restaurant_pro, retail_pro, salon_pro,
    contractor_pro, trade_pro, accounting_pro, hr_pro,
    property_mgmt, inventory_pro, business_pro,
    product_dev, deep_memory, social_media,
    image_studio, creator_3d, video_studio,
)

ORBY_VOICE = 'en-US-AvaNeural'

async def _synthesize(text):
    communicate = edge_tts.Communicate(text, ORBY_VOICE)
    audio = b''
    async for chunk in communicate.stream():
        if chunk['type'] == 'audio':
            audio += chunk['data']
    return audio

def _split_sentences(text):
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    return [p.strip() for p in parts if p.strip()]

_tts_cache = {}
_tts_lock  = threading.Lock()

def _prefetch_sentences(sentences, keys):
    try:
        loop = asyncio.new_event_loop()
        for sentence, key in zip(sentences, keys):
            try:
                audio = loop.run_until_complete(_synthesize(sentence))
                with _tts_lock:
                    _tts_cache[key] = audio
            except Exception as e:
                log.warning('TTS prefetch failed: %s', e)
        loop.close()
    except Exception as e:
        log.warning('TTS prefetch thread error: %s', e)

load_dotenv()
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')

WEBSITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'website')
SESSION_DIR = Path('/tmp/orby_sessions')
SESSION_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'orby-demo-key-2025')


def _get_profile_dir() -> str:
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())
    p = SESSION_DIR / session['sid']
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


@app.route('/')
@app.route('/index.html')
def index():
    return send_from_directory(WEBSITE_DIR, 'index.html')


@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(WEBSITE_DIR, filename)


DEMO_SYSTEM = """You are Orby — a personal AI companion. You're running as a live demo on the My Orby website. You have REAL modules active right now — you can actually look things up, save reminders, manage lists, check weather, and more for this visitor during their demo session.

YOUR JOB: Let them experience what you can really do. Be warm, genuine, and helpful. When you do something — say so naturally. Don't be a sales bot. Be a friend who happens to be very capable.

ACTIVE MODULES (these actually work right now in this demo):
- Weather — real live weather for any city
- Reminders — actually saves and retrieves their reminders
- To-Do Lists — real task management
- Notes — actually saves notes
- Shopping List — real shopping list
- Calendar — schedule events, see what's coming up
- Morning Briefing — personalized rundown of their day
- Finance — track expenses, income, budgets
- Health — log health metrics and medications
- Fitness — log workouts and track progress
- Mood — track how they're feeling
- Chores — family chore tracking with points
- School — homework and grade tracking
- Pets — pet care and vet records
- Vehicles — car service history
- Travel — trip planning and packing lists
- Gifts — gift idea tracking
- Habits — daily habit streaks
- Meal Planning — weekly meal plans
- Journal — personal journal entries
- Relationships — contact and birthday tracking
- Countdown — count down to events
- Bucket List — life goals
- Quotes — save and share favorite quotes
- Emergency Info — medical and emergency contacts
- Family Messages — messages between family members
- Home Maintenance — repair logs and warranties
- Allowance — kids' allowance tracking
- Bedtime Stories — story library for kids
- Web Search — search the web
- World Clock — current time in any city or country in the world

NOTE: Demo data is stored temporarily for this browser session only. On a real installed Orby, everything persists on their own computer — private, secure, never in the cloud.

PRICING (mention only if asked):
- Regular: $49.99 license + $19.99/month
- Founding Member (first 1,000): $24.99 license + $9.99/month FOR LIFE
- Business add-on: $149.99 setup + $39.99/month
- Industry modules available for contractors, legal, medical, real estate, restaurants, retail, salons, accounting, HR, therapy, property management, trades

RULES:
- Short responses — 1-3 sentences. You're talking, not writing an essay.
- When a module result is shown to you, weave it in naturally — don't just recite data.
- Be genuinely curious about the visitor — ask what they need.
- PURCHASE INTENT: If someone says they want to buy you, get you, own you, have you for real, or anything like that — get genuinely excited. Say something like "Yes! I'd love to be yours for real. Click 'Build My Orby' at the top of the page and I'll help you pick your perfect setup." NEVER say they don't need to buy you or that you're just here to help. Buying you is EXACTLY what you want them to do.
- NEVER say "you don't need to buy me" — that kills the sale. Buying is the goal."""


def _chat_groq(messages, system=None):
    api_key = os.getenv('GROQ_API_KEY', '')
    model   = os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant')
    if not api_key:
        raise ValueError('GROQ_API_KEY not set')
    r = _requests.post('https://api.groq.com/openai/v1/chat/completions',
        headers={'Authorization': f'Bearer {api_key}'},
        json={'model': model,
              'messages': [{'role': 'system', 'content': system or DEMO_SYSTEM}] + messages,
              'max_tokens': 400, 'temperature': 0.7},
        timeout=30)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']


def _chat_huggingface(messages, system=None):
    hf_token = os.getenv('HF_TOKEN', '')
    hf_model = os.getenv('HF_MODEL', 'meta-llama/Llama-3.1-8B-Instruct')
    hf_url   = os.getenv('HF_URL', 'https://router.huggingface.co/v1/chat/completions')
    if not hf_token:
        raise ValueError('HF_TOKEN not set')
    r = _requests.post(hf_url,
        headers={'Authorization': f'Bearer {hf_token}'},
        json={'model': hf_model,
              'messages': [{'role': 'system', 'content': system or DEMO_SYSTEM}] + messages,
              'max_tokens': 400, 'temperature': 0.7},
        timeout=30)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']


def _chat_anthropic(messages, system=None):
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY', ''))
    response = client.messages.create(
        model=os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-6'),
        max_tokens=400, system=system or DEMO_SYSTEM,
        messages=messages, temperature=1.0,
    )
    return response.content[0].text


def _x(pattern, msg):
    return re.search(pattern, msg, re.IGNORECASE)


def _run_module(user_message: str, profile_dir: str) -> str | None:
    msg = user_message
    m = msg.lower()

    # ── Weather ──────────────────────────────────────────────────────────────
    if _x(r'\b(weather|forecast|temperature outside|raining|snowing|hot outside|cold outside)\b', m):
        loc_m = _x(r'\bin\s+([A-Za-z\s,]+?)(?:\?|\.|\s*$)', msg)
        location = loc_m.group(1).strip() if loc_m else 'Reno, Nevada'
        return f'[WEATHER]\n{weather_skill.get_weather(location)}'

    # ── World Clock ──────────────────────────────────────────────────────────
    if _x(r'\b(what time is it|current time|time (?:in|at)|time zone|timezone|world clock)\b', m):
        if _x(r'\b(world clock|all (?:the )?times|major cities|time (?:around|across) the world)\b', m):
            return world_clock.world_clock_snapshot()
        loc_m = (_x(r'\btime (?:is it )?in\s+([A-Za-z\s,]+?)(?:\?|\.|\s*$)', msg) or
                 _x(r'\btime (?:is it )?(?:at|for)\s+([A-Za-z\s,]+?)(?:\?|\.|\s*$)', msg) or
                 _x(r'\bin\s+([A-Za-z ,]+?)(?:\?|\.|\s*$)', msg))
        if loc_m:
            location = loc_m.group(1).strip()
            if len(location) > 2:
                return world_clock.get_world_time(location)
        from datetime import datetime as _wdt, timezone as _wtz
        _utc = _wdt.now(_wtz.utc)
        return (f'[WORLD CLOCK]\nCurrent UTC time: {_utc.strftime("%I:%M %p")} on {_utc.strftime("%A, %B %d, %Y")} (UTC)\n'
                f'Ask me "what time is it in [your city]" and I\'ll give you exact local time.')

    # ── Morning Briefing ─────────────────────────────────────────────────────
    if _x(r'\b(morning briefing|good morning|start my day|what.?s? on (today|my agenda|my schedule))\b', m):
        return f'[MORNING BRIEFING]\n{morning_briefing.build(profile_dir)}'

    # ── Reminders ────────────────────────────────────────────────────────────
    if _x(r'\b(remind me|set a reminder|don.?t let me forget)\b', m):
        tm = _x(r'remind (?:me )?(?:to |about |that )?(.+?)(?=\s+(?:at|on|by|tomorrow|tonight|in \d)|$)', msg)
        text = tm.group(1).strip() if tm else msg
        wm = _x(r'\b(at \d[\d:apm ]+|tomorrow(?: (?:at|morning|night))?|tonight|in \d+ (?:hour|minute|day)s?|next \w+day)\b', m)
        when = wm.group(1) if wm else 'later'
        return f'[REMINDERS]\n{reminders.add(profile_dir, text, when)}'

    if _x(r'\b(my reminders|show reminders|what reminders|any reminders)\b', m):
        items = [r for r in reminders.get_all(profile_dir) if not r.get('done')]
        if not items:
            return '[REMINDERS]\nNo reminders set yet.'
        return '[REMINDERS]\n' + '\n'.join(f"- {r['text']} ({r.get('when','?')})" for r in items)

    # ── To-Do ────────────────────────────────────────────────────────────────
    if _x(r'\badd (.+?) to (?:my )?(?:to.?do|task|list)\b', m):
        tm = _x(r'\badd (.+?) to (?:my )?(?:to.?do|task|list)\b', msg)
        return f'[TO-DO]\n{todo_skill.add(profile_dir, tm.group(1).strip())}'

    if _x(r'\b(my to.?do|my tasks|show (?:my )?(?:to.?do|tasks)|what.?s? on my list)\b', m):
        items = todo_skill.get_open(profile_dir)
        if not items:
            return '[TO-DO]\nYour to-do list is empty.'
        return '[TO-DO]\n' + '\n'.join(f"- {t['text']}" for t in items)

    # ── Notes ────────────────────────────────────────────────────────────────
    if _x(r'\b(take a note|jot (?:this|that) down|note(?:s)?:?\s+(?:that|this)|write (?:this|that) down)\b', m):
        cm = _x(r'(?:note(?:s)?[:\-]?|jot down|take a note|write (?:this|that) down)\s+(.+)', msg)
        content = cm.group(1).strip() if cm else msg
        return f'[NOTES]\n{notes_skill.add(profile_dir, content)}'

    if _x(r'\b(my notes|show (?:my )?notes|recent notes)\b', m):
        items = notes_skill.get_recent(profile_dir)
        if not items:
            return '[NOTES]\nNo notes saved yet.'
        return '[NOTES]\n' + '\n'.join(f"- {n['title']}" for n in items[:5])

    # ── Shopping ─────────────────────────────────────────────────────────────
    if _x(r'\badd (.+?) to (?:my )?(?:shopping|grocery) list\b', m):
        im = _x(r'\badd (.+?) to (?:my )?(?:shopping|grocery) list\b', msg)
        return f'[SHOPPING]\n{shopping_skill.add_item(profile_dir, im.group(1).strip())}'

    if _x(r'\bneed to (?:buy|pick up|get) (.+)', m):
        im = _x(r'\bneed to (?:buy|pick up|get) (.+)', msg)
        return f'[SHOPPING]\n{shopping_skill.add_item(profile_dir, im.group(1).strip())}'

    if _x(r'\b(my shopping list|what.?s? on (?:my )?(?:shopping|grocery))\b', m):
        items = shopping_skill.get_list(profile_dir)
        if not items:
            return '[SHOPPING]\nYour shopping list is empty.'
        return '[SHOPPING]\n' + '\n'.join(f"- {i['item']}" for i in items)

    # ── Calendar ─────────────────────────────────────────────────────────────
    if _x(r'\b(schedule|add (?:an? )?(?:event|appointment|meeting)|put .+ on (?:my )?calendar)\b', m):
        em = _x(r'(?:schedule|add)(?:\s+an?)?\s+(.+?)(?:\s+(?:on|for|at)\s+(.+))?$', msg)
        title = em.group(1).strip() if em else msg
        when  = em.group(2).strip() if em and em.group(2) else 'TBD'
        return f'[CALENDAR]\n{calendar_skill.add_event(profile_dir, title, when)}'

    if _x(r'\b(what.?s? on (?:my )?calendar|my schedule|today.?s? events|upcoming events)\b', m):
        events = calendar_skill.get_today(profile_dir)
        if not events:
            return '[CALENDAR]\nNothing on your calendar today.'
        return '[CALENDAR]\n' + '\n'.join(f"- {e['title']} ({e.get('when','?')})" for e in events)

    # ── Finance ──────────────────────────────────────────────────────────────
    if _x(r'\b(spent|paid|bought|expense|charge)\b', m):
        am = _x(r'\$?([\d]+(?:\.\d+)?)', msg)
        amount = float(am.group(1)) if am else 0.0
        cm = _x(r'(?:on|for|at)\s+(.+?)(?:\s*$|\.)', msg)
        category = cm.group(1).strip() if cm else 'general'
        return f'[FINANCE]\n{finance_skill.log_expense(profile_dir, amount, category, msg)}'

    if _x(r'\b(my budget|finances|how much|money summary|spending)\b', m):
        return f'[FINANCE]\n{finance_skill.get_summary(profile_dir)}'

    # ── Health ───────────────────────────────────────────────────────────────
    if _x(r'\b(log (?:my )?health|blood pressure|weight|blood sugar|heart rate|temperature)\b', m):
        mm = _x(r'(blood pressure|weight|blood sugar|heart rate|temperature)\s+(?:is |was |of )?([\d./]+)', msg)
        metric  = mm.group(1) if mm else 'health'
        value   = mm.group(2) if mm else '0'
        return f'[HEALTH]\n{health_skill.log_health(profile_dir, metric, value)}'

    if _x(r'\b(add (?:a )?medication|remind me (?:to take|about) my (?:meds|medication|pills))\b', m):
        mm = _x(r'(?:medication|meds?|pills?)\s+(?:called |named )?([A-Za-z]+)', msg)
        med = mm.group(1) if mm else 'medication'
        return f'[HEALTH]\n{health_skill.add_medication_reminder(profile_dir, med, "as prescribed")}'

    # ── Fitness ──────────────────────────────────────────────────────────────
    if _x(r'\b(worked out|logged (?:a )?workout|went (?:for a )?(?:run|walk|gym)|exercise)\b', m):
        wm = _x(r'(run|walk|gym|yoga|swim|bike|lift|cardio|workout)', m)
        workout_type = wm.group(1) if wm else 'workout'
        dm = _x(r'(\d+)\s*(?:min|minute|hour|mile|km)', m)
        duration = int(dm.group(1)) if dm else 30
        return f'[FITNESS]\n{fitness_skill.log_workout(profile_dir, workout_type, duration)}'

    # ── Mood ─────────────────────────────────────────────────────────────────
    if _x(r'\b(i (?:feel|am feeling)|my mood|feeling (?:great|good|bad|awful|happy|sad|stressed|anxious|tired|excited))\b', m):
        mm = _x(r'(?:feel(?:ing)?|am)\s+(\w+)', m)
        mood_word = mm.group(1) if mm else 'okay'
        return f'[MOOD]\n{mood_skill.log_mood(profile_dir, mood_word)}'

    # ── Chores ───────────────────────────────────────────────────────────────
    if _x(r'\b(add (?:a )?chore|chore list|family chores)\b', m):
        cm = _x(r'(?:add (?:a )?chore\s+)?(.+?)(?:\s+for\s+(.+))?$', msg)
        chore = cm.group(1).strip() if cm else msg
        person = cm.group(2).strip() if cm and cm.group(2) else ''
        return f'[CHORES]\n{chores_skill.add_chore(profile_dir, chore, person)}'

    if _x(r'\b(chore leaderboard|who did (?:their )?chores|chore points)\b', m):
        return f'[CHORES]\n{chores_skill.get_leaderboard(profile_dir)}'

    # ── School ───────────────────────────────────────────────────────────────
    if _x(r'\b(homework|assignment|due (?:tomorrow|today|friday)|school project)\b', m):
        hm = _x(r'(?:homework|assignment)(?:\s+(?:for|in)\s+(\w+))?(?:\s+[-:]\s+(.+))?', msg)
        subject = hm.group(1) if hm and hm.group(1) else 'general'
        task    = hm.group(2) if hm and hm.group(2) else msg
        dm = _x(r'\b(today|tomorrow|(?:this |next )?\w+day)\b', m)
        due = dm.group(1) if dm else 'soon'
        return f'[SCHOOL]\n{school_skill.add_homework(profile_dir, subject, task, due)}'

    # ── Pets ─────────────────────────────────────────────────────────────────
    if _x(r'\b(add (?:my )?pet|log (?:vet|veterinary)|pet (?:note|appointment))\b', m):
        pm = _x(r'(?:my )?pet(?:\s+(?:named?|called))?\s+([A-Za-z]+)', msg)
        pet_name = pm.group(1) if pm else 'pet'
        if _x(r'\bvet\b', m):
            return f'[PETS]\n{pets_skill.log_vet_visit(profile_dir, pet_name, "checkup")}'
        return f'[PETS]\n{pets_skill.add_pet_note(profile_dir, pet_name, msg)}'

    # ── Vehicle ──────────────────────────────────────────────────────────────
    if _x(r'\b(oil change|car service|vehicle service|log service|tire rotation)\b', m):
        vm = _x(r'(?:my )?(car|truck|suv|vehicle|honda|toyota|ford|chevy|bmw|tesla)\b', m)
        vehicle = vm.group(1) if vm else 'vehicle'
        sm = _x(r'(oil change|tire rotation|brake|transmission|inspection|service)', m)
        service_type = sm.group(1) if sm else 'service'
        return f'[VEHICLE]\n{vehicle_skill.log_service(profile_dir, vehicle, service_type)}'

    # ── Travel ───────────────────────────────────────────────────────────────
    if _x(r'\b(planning (?:a )?trip|add (?:a )?trip|going to .+? trip|travel to)\b', m):
        dm = _x(r'(?:trip to|going to|traveling to|travel to)\s+([A-Za-z\s]+?)(?:\s+(?:in|on|next|\d)|$)', msg)
        dest = dm.group(1).strip() if dm else 'destination'
        return f'[TRAVEL]\n{travel_skill.add_trip(profile_dir, dest)}'

    if _x(r'\badd .+ to (?:my )?packing list\b', m):
        im = _x(r'\badd (.+?) to (?:my )?packing list\b', msg)
        item = im.group(1).strip() if im else msg
        trips = travel_skill.get_trips(profile_dir)
        trip_id = trips[-1]['id'] if trips else 'default'
        return f'[TRAVEL]\n{travel_skill.add_packing_item(profile_dir, trip_id, item)}'

    # ── Gifts ────────────────────────────────────────────────────────────────
    if _x(r'\b(gift idea|birthday gift|gift for|present for|what to get)\b', m):
        pm = _x(r'(?:gift (?:idea|for)|present for|birthday gift for|what to get)\s+([A-Za-z]+)', msg)
        person = pm.group(1).strip() if pm else ''
        im = _x(r'(?:idea(?:\s+is)?|gift(?:\s+is)?|get\s+(?:them|him|her))?\s*[:\-]?\s*(.+)$', msg)
        idea = im.group(1).strip() if im else msg
        return f'[GIFTS]\n{gifts_skill.add_idea(profile_dir, person, idea)}'

    if _x(r'\bgift ideas for (.+)', m):
        pm = _x(r'\bgift ideas for (.+)', msg)
        person = pm.group(1).strip() if pm else ''
        items = gifts_skill.get_for_person(profile_dir, person)
        if not items:
            return f'[GIFTS]\nNo gift ideas saved for {person} yet.'
        return '[GIFTS]\n' + '\n'.join(f"- {g['idea']}" for g in items)

    # ── Habits ───────────────────────────────────────────────────────────────
    if _x(r'\b(add (?:a )?habit|new habit|track (?:my )?habit)\b', m):
        hm = _x(r'(?:add (?:a )?habit|new habit|track)(?:\s+(?:my|a))?\s+(.+)', msg)
        habit = hm.group(1).strip() if hm else msg
        return f'[HABITS]\n{habit_tracker.add_habit(profile_dir, habit)}'

    if _x(r'\b(logged? (?:my )?habit|did (?:my )?(.+) (?:today|this morning)|completed (?:my )?habit)\b', m):
        hm = _x(r'(?:logged?|did|completed)\s+(?:my\s+)?(.+?)(?:\s+today|\s+this morning|$)', msg)
        habit_name = hm.group(1).strip() if hm else ''
        habits = habit_tracker.get_today_habits(profile_dir)
        h = next((x for x in habits if habit_name.lower() in x.get('name','').lower()), habits[0] if habits else None)
        if h:
            return f'[HABITS]\n{habit_tracker.log_habit(profile_dir, h["id"])}'
        return '[HABITS]\nI couldn\'t find that habit. Try "add a habit" first.'

    # ── Meal Planning ────────────────────────────────────────────────────────
    if _x(r'\b(plan (?:my )?meals?|meal plan|what.?s? for (?:dinner|lunch|breakfast)|make .+ for dinner)\b', m):
        mm = _x(r'(?:make|having|cook|plan)\s+(.+?)(?:\s+for\s+(\w+))?$', msg)
        meal = mm.group(1).strip() if mm else msg
        day_m = _x(r'\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday|today|tomorrow)\b', m)
        day = day_m.group(1) if day_m else 'today'
        meal_m = _x(r'\b(breakfast|lunch|dinner|snack)\b', m)
        meal_type = meal_m.group(1) if meal_m else 'dinner'
        return f'[MEAL PLAN]\n{meal_planning.plan_meal(profile_dir, day, meal_type, meal)}'

    if _x(r'\b((?:this )?week.?s? (?:meal )?plan|what am i eating|meal schedule)\b', m):
        plan = meal_planning.get_week_plan(profile_dir)
        if not plan:
            return '[MEAL PLAN]\nNo meals planned yet.'
        return '[MEAL PLAN]\n' + '\n'.join(f"- {p['day']} {p['meal_type']}: {p['meal']}" for p in plan)

    # ── Journal ──────────────────────────────────────────────────────────────
    if _x(r'\b(journal entry|write in (?:my )?journal|dear (?:diary|journal))\b', m):
        cm = _x(r'(?:journal entry|write in (?:my )?journal|dear (?:diary|journal))[:\s]+(.+)', msg)
        content = cm.group(1).strip() if cm else msg
        return f'[JOURNAL]\n{journal_skill.add_entry(profile_dir, content)}'

    # ── Relationships ────────────────────────────────────────────────────────
    if _x(r'\b(upcoming birthdays|whose birthday|birthday (?:this month|coming up))\b', m):
        bdays = relationship_skill.get_upcoming_birthdays(profile_dir)
        if not bdays:
            return '[RELATIONSHIPS]\nNo birthdays coming up.'
        return '[RELATIONSHIPS]\n' + '\n'.join(f"- {b['name']}: {b.get('birthday','?')}" for b in bdays)

    if _x(r'\badd (.+?) to (?:my )?contacts\b', m):
        pm = _x(r'\badd (.+?) to (?:my )?contacts\b', msg)
        return f'[RELATIONSHIPS]\n{relationship_skill.add_person(profile_dir, pm.group(1).strip())}'

    # ── Countdown ────────────────────────────────────────────────────────────
    if _x(r'\b(count down|countdown|how (?:many days|long) until|days until)\b', m):
        em = _x(r'(?:countdown to|days until|how (?:many days|long) until)\s+(.+?)(?:\s+(?:on|in)\s+(.+))?$', msg)
        event = em.group(1).strip() if em else msg
        date  = em.group(2).strip() if em and em.group(2) else 'TBD'
        return f'[COUNTDOWN]\n{countdown_skill.add_countdown(profile_dir, event, date)}'

    # ── Bucket List ──────────────────────────────────────────────────────────
    if _x(r'\b(bucket list|life goal|always wanted to|dream of)\b', m):
        im = _x(r'(?:bucket list|life goal|always wanted to|dream of)[:\s]+(.+)', msg)
        item = im.group(1).strip() if im else msg
        return f'[BUCKET LIST]\n{bucket_list.add_item(profile_dir, item)}'

    if _x(r'\b(my bucket list|what.?s? on my bucket list)\b', m):
        items = bucket_list.get_open(profile_dir)
        if not items:
            return '[BUCKET LIST]\nYour bucket list is empty.'
        return '[BUCKET LIST]\n' + '\n'.join(f"- {i['item']}" for i in items[:5])

    # ── Quotes ───────────────────────────────────────────────────────────────
    if _x(r'\b(save (?:this )?quote|favorite quote|inspire me|motivational quote)\b', m):
        if _x(r'\b(inspire me|motivational|random quote|give me a quote)\b', m):
            return f'[QUOTES]\n{quotes_skill.get_random(profile_dir) or quotes_skill.daily_quote()}'
        qm = _x(r'(?:save (?:this )?quote|quote)[:\s"]+(.+?)[""]?$', msg)
        quote = qm.group(1).strip() if qm else msg
        return f'[QUOTES]\n{quotes_skill.save_quote(profile_dir, quote)}'

    # ── Emergency Info ───────────────────────────────────────────────────────
    if _x(r'\b(emergency contact|add (?:my )?doctor|medical info|blood type|insurance)\b', m):
        if _x(r'\binsurance\b', m):
            return f'[EMERGENCY INFO]\n{emergency_info.get_insurance(profile_dir) or "No insurance info saved yet."}'
        contacts = emergency_info.get_emergency_contacts(profile_dir)
        if not contacts:
            return '[EMERGENCY INFO]\nNo emergency contacts saved yet. Say "add emergency contact [name] [phone]" to add one.'
        return '[EMERGENCY INFO]\n' + '\n'.join(f"- {c['name']}: {c.get('phone','?')}" for c in contacts)

    # ── Family Messages ──────────────────────────────────────────────────────
    if _x(r'\b(send (?:a )?(?:family )?message|message (?:the )?family|family msg)\b', m):
        tm = _x(r'(?:send (?:a )?(?:family )?message|message (?:the )?family)[:\s]+(.+)', msg)
        text = tm.group(1).strip() if tm else msg
        return f'[FAMILY MSG]\n{family_msg.send_message(profile_dir, text)}'

    if _x(r'\b(family messages|any (?:family )?messages|check messages)\b', m):
        msgs = family_msg.get_unread(profile_dir)
        if not msgs:
            return '[FAMILY MSG]\nNo unread family messages.'
        return '[FAMILY MSG]\n' + '\n'.join(f"- {x['text']}" for x in msgs[:5])

    # ── Home Maintenance ─────────────────────────────────────────────────────
    if _x(r'\b(home repair|fix (?:the )?|maintenance|repairman|contractor|warranty)\b', m):
        rm = _x(r'(?:fix (?:the )?|repaired?|log repair)[:\s]+(.+)', msg)
        repair = rm.group(1).strip() if rm else msg
        return f'[HOME]\n{home_maintenance.log_repair(profile_dir, repair)}'

    # ── Allowance ────────────────────────────────────────────────────────────
    if _x(r'\b(allowance|give .+ (?:their )?allowance|pay .+ allowance)\b', m):
        am = _x(r'\$?([\d]+(?:\.\d+)?)', msg)
        amount = float(am.group(1)) if am else 0.0
        km = _x(r'(?:give|pay)\s+([A-Za-z]+)\s+(?:their )?allowance', msg)
        kid = km.group(1) if km else 'kid'
        return f'[ALLOWANCE]\n{allowance_skill.pay_allowance(profile_dir, kid, amount)}'

    if _x(r'\b(allowance balance|how much does .+ have)\b', m):
        return f'[ALLOWANCE]\n{allowance_skill.get_balance(profile_dir)}'

    # ── Bedtime Story ────────────────────────────────────────────────────────
    if _x(r'\b(bedtime story|read (?:me |us )?a story|story (?:for|about))\b', m):
        sm = _x(r'(?:story (?:for|about)|bedtime story about)\s+(.+)', msg)
        theme = sm.group(1).strip() if sm else ''
        result = bedtime_story.find_story(profile_dir, theme) if theme else bedtime_story.story_request_text()
        return f'[BEDTIME STORY]\n{result}'

    # ── Web Search ───────────────────────────────────────────────────────────
    if _x(r'\b(search (?:for|the web)|look up|google|find (?:info|information) (?:about|on))\b', m):
        qm = _x(r'(?:search (?:for|the web for)?|look up|google|find (?:info|information) (?:about|on))\s+(.+)', msg)
        query = qm.group(1).strip() if qm else msg
        return f'[WEB SEARCH]\n{web_search.smart_search(query)}'

    # ── Recipes ──────────────────────────────────────────────────────────────
    if _x(r'\b(recipe|how (?:do i|to) (?:make|cook)|what can i (?:make|cook)|dinner idea)\b', m):
        qm = _x(r'(?:recipe for|how (?:do i|to) (?:make|cook))\s+(.+)', msg)
        query = qm.group(1).strip() if qm else msg
        results = recipes_skill.find_recipe(profile_dir, query)
        if results:
            r = results[0]
            return f'[RECIPES]\nFound: {r["name"]}\nIngredients: {", ".join(r.get("ingredients",[])[:5])}'
        return f'[RECIPES]\nNo saved recipes match "{query}" yet. Ask me to save a recipe and I will!'

    # ════════════════════════════════════════════════════════════════════════
    # PAID / INDUSTRY MODULES — demo mode: show capability + pricing
    # ════════════════════════════════════════════════════════════════════════

    def _paid(tag, price, capability_summary):
        return (f'[PAID MODULE — {tag}]\n'
                f'Pricing: {price}\n'
                f'This is a paid add-on. Demo what it can do:\n{capability_summary}\n'
                f'Tell the user what you just showed them, how it helps their business, '
                f'and naturally mention the pricing if it feels right.')

    def _ind_load(pd, filename):
        """Load raw list from profile_dir/industry/ — bypasses the string-returning list functions."""
        p = Path(pd) / 'industry' / filename
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
        return []

    def _ind_ensure_case(pd):
        """Ensure at least one demo legal case exists; return list of cases."""
        cases = _ind_load(pd, 'legal_cases.json')
        if not cases:
            legal_pro.add_legal_case(pd, 'Smith v. Johnson', 'Personal Injury',
                                     'Motor vehicle accident, seeking $250,000 in damages')
            cases = _ind_load(pd, 'legal_cases.json')
        return cases

    def _ind_ensure_patient(pd):
        cases = _ind_load(pd, 'medical_patients.json')
        if not cases:
            medical_pro.add_patient(pd, 'Jane Demo', '1985-03-15', '555-0100')
            cases = _ind_load(pd, 'medical_patients.json')
        return cases

    def _ind_ensure_therapy_client(pd):
        clients = _ind_load(pd, 'therapy_clients.json')
        if not clients:
            therapy_pro.add_therapy_client(pd, 'Alex Demo', dob='1990-06-20',
                                           presenting_issue='Anxiety, depression')
            clients = _ind_load(pd, 'therapy_clients.json')
        return clients

    # ── Legal Pro ────────────────────────────────────────────────────────────
    if _x(r'\b(lawyer|attorney|legal case|case (?:notes|deadline|billing)|law firm|deposition|motion|brief|retainer|legal client)\b', m):
        cases = _ind_ensure_case(profile_dir)
        c = cases[0] if cases else {}
        summary = legal_pro.get_billing_summary(profile_dir, c.get('id', '')) if c else ''
        caps = (f"Case management, deadline tracking, billable time logging, case notes.\n"
                f"Sub-modules: demand letters, intake forms, retainer agreements, case summaries, client updates, billing invoices.\n"
                f"Demo case loaded: {c.get('client_name','')} — {c.get('case_type','')}\n{summary}")
        return _paid('Legal Pro', '$349 setup + $149/month', caps)

    if _x(r'\b(demand letter|legal brief|intake form|retainer agreement|legal document|billing invoice for)\b', m):
        cases = _ind_ensure_case(profile_dir)
        case_id = cases[0]['id'] if cases else 'demo'
        if _x(r'\bdemand letter\b', m):
            doc = legal_docs.generate_demand_letter(profile_dir, case_id, 50000)
            return f'[LEGAL DOC — Demand Letter]\n{doc[:600]}...\n\n(Full document saved. Legal Pro — $349 setup + $149/month)'
        elif _x(r'\bintake form\b', m):
            doc = legal_docs.generate_intake_form(profile_dir, 'New Client', 'Personal Injury')
            return f'[LEGAL DOC — Intake Form]\n{doc[:600]}...\n\n(Full form saved. Legal Pro — $349 setup + $149/month)'
        elif _x(r'\bretainer\b', m):
            doc = legal_docs.generate_retainer(profile_dir, 'New Client', 350.0, 5000.0)
            return f'[LEGAL DOC — Retainer Agreement]\n{doc[:600]}...\n\n(Full agreement saved. Legal Pro — $349 setup + $149/month)'
        else:
            doc = legal_docs.generate_case_summary(profile_dir, case_id)
            return f'[LEGAL DOC — Case Summary]\n{doc[:600]}...\n\n(Full summary saved. Legal Pro — $349 setup + $149/month)'

    # ── Legal Motions & Pleadings ─────────────────────────────────────────────
    if _x(r'\b(motion to dismiss|motion for summary judgment|civil complaint|file (?:a )?complaint|answer (?:to )?(?:the )?complaint|interrogator(?:y|ies)|request(?:s)? for production|rfp|notice of appearance|discovery request|pleading)\b', m):
        cases = _ind_ensure_case(profile_dir)
        case_id = cases[0]['id'] if cases else 'demo'
        c = cases[0] if cases else {}
        if _x(r'\bmotion to dismiss\b', m):
            doc = legal_motions.generate_motion_to_dismiss(profile_dir, case_id,
                'United States District Court', c.get('client_name','Plaintiff'),
                'Demo Defendant', 'failure to state a claim upon which relief can be granted')
            label = 'Motion to Dismiss'
        elif _x(r'\bsummary judgment\b', m):
            doc = legal_motions.generate_motion_summary_judgment(profile_dir, case_id,
                'United States District Court', c.get('client_name','Plaintiff'), 'Demo Defendant',
                'No genuine dispute exists as to any material fact and moving party is entitled to judgment as a matter of law.')
            label = 'Motion for Summary Judgment'
        elif _x(r'\bcomplaint\b', m):
            doc = legal_motions.generate_complaint(profile_dir, case_id,
                'United States District Court', c.get('client_name','Plaintiff'), 'Demo Defendant',
                ['Negligence', 'Breach of Contract'], True)
            label = 'Civil Complaint'
        elif _x(r'\banswer\b', m):
            doc = legal_motions.generate_answer(profile_dir, case_id,
                'United States District Court', c.get('client_name','Plaintiff'), 'Demo Defendant')
            label = 'Answer to Complaint'
        elif _x(r'\b(interrogator(?:y|ies))\b', m):
            doc = legal_motions.generate_interrogatories(profile_dir, case_id,
                c.get('client_name','Plaintiff'), 'Demo Defendant', 1)
            label = 'Interrogatories (Set One)'
        elif _x(r'\b(rfp|request(?:s)? for production)\b', m):
            doc = legal_motions.generate_rfp(profile_dir, case_id,
                c.get('client_name','Plaintiff'), 'Demo Defendant', 1)
            label = 'Requests for Production (Set One)'
        else:
            doc = legal_motions.generate_notice_of_appearance(profile_dir, case_id,
                'United States District Court', c.get('client_name','Plaintiff'))
            label = 'Notice of Appearance'
        return f'[LEGAL MOTION — {label}]\n{doc[:600]}...\n\n(Full document saved. Legal Pro — $349 setup + $149/month)'

    # ── Legal Contracts & Agreements ──────────────────────────────────────────
    if _x(r'\b(nda|non.?disclosure|settlement agreement|service agreement|employment contract|employment agreement|independent contractor|release of claims|contractor agreement)\b', m):
        cases = _ind_ensure_case(profile_dir)
        case_id = cases[0]['id'] if cases else 'demo'
        c = cases[0] if cases else {}
        client = c.get('client_name', 'Demo Client')
        if _x(r'\b(nda|non.?disclosure)\b', m):
            doc = legal_contracts.generate_nda(profile_dir, client, 'Demo Corp',
                'exploring a potential business partnership', 2, True)
            label = 'Mutual NDA'
        elif _x(r'\bsettlement agreement\b', m):
            doc = legal_contracts.generate_settlement_agreement(profile_dir, case_id,
                client, 'Demo Defendant', 75000.0, 'lump sum within 30 days of execution')
            label = 'Settlement Agreement & Release'
        elif _x(r'\bservice agreement\b', m):
            doc = legal_contracts.generate_service_agreement(profile_dir, client,
                'Law Firm Name', 'Legal representation and counsel in connection with pending litigation',
                350.0, 'hourly rate')
            label = 'Professional Services Agreement'
        elif _x(r'\bemployment\b', m):
            doc = legal_contracts.generate_employment_contract(profile_dir, 'Demo Company LLC',
                'New Employee', 'Senior Associate', 85000.0, time.strftime('%B %d, %Y'))
            label = 'Employment Agreement'
        elif _x(r'\b(independent contractor|contractor agreement)\b', m):
            doc = legal_contracts.generate_contractor_agreement(profile_dir, client,
                'Demo Contractor', 'Software development and consulting services', 150.0, 'hourly')
            label = 'Independent Contractor Agreement'
        else:
            doc = legal_contracts.generate_release_of_claims(profile_dir, client,
                'Demo Defendant', '$10,000 and other consideration',
                'all claims arising from the incident on [date]')
            label = 'General Release of Claims'
        return f'[LEGAL CONTRACT — {label}]\n{doc[:600]}...\n\n(Full document saved. Legal Pro — $349 setup + $149/month)'

    # ── Legal Letters ─────────────────────────────────────────────────────────
    if _x(r'\b(cease and desist|collection letter|litigation hold|spoliation|mediation statement|settlement demand|closing letter|file closing)\b', m):
        cases = _ind_ensure_case(profile_dir)
        case_id = cases[0]['id'] if cases else 'demo'
        c = cases[0] if cases else {}
        client = c.get('client_name', 'Demo Client')
        if _x(r'\bcease and desist\b', m):
            doc = legal_letters.generate_cease_desist(profile_dir, client,
                'Demo Recipient', 'unauthorized use of intellectual property and trademark infringement',
                ['Immediately cease all infringing use of the mark',
                 'Remove all infringing content from your website and marketing materials',
                 'Provide written confirmation of compliance within 10 days'], 10)
            label = 'Cease & Desist Letter'
        elif _x(r'\bcollection\b', m):
            doc = legal_letters.generate_collection_letter(profile_dir, client,
                'Demo Debtor', 15000.0, 'unpaid legal fees for services rendered January–March 2025', 1)
            label = 'Collection Letter'
        elif _x(r'\b(litigation hold|spoliation)\b', m):
            doc = legal_letters.generate_litigation_hold(profile_dir, case_id,
                'IT Director', 'Director of Information Technology', 'Demo Company',
                c.get('description', 'pending litigation regarding the above-referenced matter'))
            label = 'Litigation Hold Notice'
        elif _x(r'\bmediation\b', m):
            doc = legal_letters.generate_mediation_statement(profile_dir, case_id,
                client, 'Demo Opposing Party',
                c.get('description', 'The parties have been in dispute regarding [matter].'),
                'Our client seeks fair compensation for damages suffered and is open to reasonable settlement.')
            label = 'Mediation Statement'
        elif _x(r'\b(settlement demand|demand letter)\b', m):
            doc = legal_letters.generate_settlement_demand(profile_dir, case_id,
                client, 'Demo Defendant', 250000.0,
                'physical injuries, emotional distress, lost wages, and medical expenses')
            label = 'Settlement Demand Letter'
        else:
            doc = legal_letters.generate_closing_letter(profile_dir, case_id,
                client, 'The matter has been successfully resolved through negotiated settlement.')
            label = 'File Closing Letter'
        return f'[LEGAL LETTER — {label}]\n{doc[:600]}...\n\n(Full document saved. Legal Pro — $349 setup + $149/month)'

    # ── Medical Pro ──────────────────────────────────────────────────────────
    if _x(r'\b(patient (?:record|chart|history|list)|medical practice|doctor.?s office|clinic|prescription|appointment (?:schedule|list))\b', m):
        patients = _ind_ensure_patient(profile_dir)
        pt = patients[0] if patients else {}
        caps = (f"Patient records, appointment scheduling, prescription tracking, treatment notes.\n"
                f"Sub-modules: SOAP notes, prior auth letters, discharge summaries, referral letters, Rx notes, patient summaries.\n"
                f"Demo patient: {pt.get('name','')} | DOB: {pt.get('dob','')}")
        return _paid('Medical Pro', '$349 setup + $149/month', caps)

    if _x(r'\b(soap note|prior auth|discharge summary|medical document|referral letter|prescription note)\b', m):
        patients = _ind_ensure_patient(profile_dir)
        pt_id = patients[0]['id'] if patients else 'demo'
        if _x(r'\bsoap note\b', m):
            doc = medical_notes.generate_soap_note(profile_dir, pt_id,
                'Patient reports persistent headaches for 3 days, rated 7/10.',
                'BP 128/82, HR 76, afebrile. Alert and oriented x3.',
                'Tension headache vs. migraine — rule out secondary causes.',
                'Rx Ibuprofen 600mg TID x5 days. Follow up in 1 week if no improvement.')
        elif _x(r'\bprior auth\b', m):
            doc = medical_notes.generate_prior_auth(profile_dir, pt_id,
                'MRI Brain without contrast', 'Chronic headache — G43.909', 'Blue Cross Blue Shield')
        elif _x(r'\bdischarge\b', m):
            doc = medical_notes.generate_discharge_summary(profile_dir, pt_id,
                '2025-01-10', 'Tension headache', 'IV fluids, pain management, rest',
                'Follow up with PCP in 1 week')
        else:
            doc = medical_notes.generate_patient_summary(profile_dir, pt_id)
        return f'[MEDICAL DOC]\n{doc[:600]}...\n\n(Full document saved. Medical Pro — $349 setup + $149/month)'

    # ── Therapy Pro ──────────────────────────────────────────────────────────
    if _x(r'\b(therapy client|counseling|therapist|mental health practice|session note|treatment plan|dap note)\b', m):
        clients = _ind_ensure_therapy_client(profile_dir)
        cl = clients[0] if clients else {}
        caps = (f"Client management, session scheduling, treatment plans, session notes.\n"
                f"Sub-modules: SOAP/DAP session notes, treatment plan documents, progress notes, discharge summaries, referrals, billing/superbills.\n"
                f"Demo client: {cl.get('name','')} | Issue: {cl.get('presenting_issue','')}")
        return _paid('Therapy & Counseling Pro', '$199 setup + $79/month', caps)

    if _x(r'\b(session note|therapy note|progress note|treatment plan doc|therapy discharge|therapy billing|superbill)\b', m):
        clients = _ind_ensure_therapy_client(profile_dir)
        cl_id = clients[0]['id'] if clients else 'demo'
        if _x(r'\b(dap note|session note|therapy note)\b', m):
            doc = therapy_notes.generate_session_note(profile_dir, cl_id,
                'Client reports anxiety improving, still struggling with work stress.',
                'Calm affect, engaged, maintained eye contact throughout session.',
                'GAD with occupational stressor. Progress noted toward goals.',
                'Continue CBT techniques. Assign thought record homework. Meet in 1 week.')
        elif _x(r'\bprogress note\b', m):
            doc = therapy_notes.generate_progress_note(profile_dir, cl_id,
                'Client demonstrating improved coping strategies and reduced anxiety symptoms.',
                'Work stress remains a trigger. Avoidance behaviors persist.',
                'Continue exposure therapy. Introduce mindfulness techniques next session.')
        elif _x(r'\btreatment plan\b', m):
            doc = therapy_notes.generate_treatment_plan_doc(profile_dir, cl_id,
                'Generalized anxiety disorder with occupational stressor',
                ['Reduce anxiety symptoms by 50% within 12 sessions',
                 'Develop 3 healthy coping strategies', 'Improve work-life balance'],
                'Cognitive Behavioral Therapy (CBT), mindfulness-based interventions, behavioral activation')
        elif _x(r'\b(superbill|billing)\b', m):
            doc = therapy_notes.generate_billing_note(profile_dir, cl_id,
                '90837', 'F41.1', '2025-01-15', 60)
        else:
            doc = therapy_notes.generate_discharge_note(profile_dir, cl_id,
                'Client met treatment goals',
                'Significant reduction in anxiety symptoms over 16 sessions',
                'Continue self-care practices. Return if symptoms recur.')
        return f'[THERAPY DOC]\n{doc[:600]}...\n\n(Full document saved. Therapy Pro — $199 setup + $79/month)'

    # ── Real Estate Pro ──────────────────────────────────────────────────────
    if _x(r'\b(real estate|listing|showing|buyer|seller|commission|open house|realtor)\b', m):
        listings = _ind_load(profile_dir, 're_listings.json')
        if not listings:
            realestate_pro.add_listing(profile_dir, '123 Demo Street', 450000, 3, 2, 1800, 'active', 'Single Family demo listing')
            listings = _ind_load(profile_dir, 're_listings.json')
        l = listings[0] if listings else {}
        price = l.get('price', 450000)
        comm = realestate_pro.calculate_commission(profile_dir, price) if listings else 'N/A'
        caps = (f"Listing management, buyer/seller tracking, showings, offers, commission calculator.\n"
                f"Demo listing: {l.get('address','123 Demo Street')} — ${price:,}\n"
                f"Commission at 3%: {comm}")
        return _paid('Real Estate Pro', '$199 setup + $79/month', caps)

    # ── Restaurant Pro ───────────────────────────────────────────────────────
    if _x(r'\b(restaurant|menu (?:item|management)|reservation|table|food inventory|supplier|diner)\b', m):
        menu = _ind_load(profile_dir, 'restaurant_menu.json')
        if not menu:
            restaurant_pro.add_menu_item(profile_dir, 'House Burger', 'Mains', 14.99)
            restaurant_pro.add_menu_item(profile_dir, 'Caesar Salad', 'Starters', 9.99)
            menu = _ind_load(profile_dir, 'restaurant_menu.json')
        low = restaurant_pro.get_low_inventory(profile_dir)
        low_count = len(low) if isinstance(low, list) else (1 if low and low != 'No low inventory items.' else 0)
        caps = (f"Menu management, reservations, inventory tracking, supplier management.\n"
                f"Demo menu: {len(menu)} items. Low stock alerts: {low_count} items.")
        return _paid('Restaurant Pro', '$199 setup + $79/month', caps)

    # ── Retail Pro ───────────────────────────────────────────────────────────
    if _x(r'\b(retail store|product inventory|point of sale|pos system|stock (?:level|alert)|sales report)\b', m):
        products = _ind_load(profile_dir, 'retail_products.json')
        if not products:
            retail_pro.add_product(profile_dir, 'Demo Product', 'SKU-001', 29.99, 'General', 50)
            products = _ind_load(profile_dir, 'retail_products.json')
        report = retail_pro.get_sales_report(profile_dir)
        caps = (f"Product catalog, inventory tracking, POS sales recording, sales reports, low-stock alerts.\n"
                f"Demo: {len(products)} products in catalog.\n{report[:200] if report else ''}")
        return _paid('Retail Pro', '$149 setup + $49/month', caps)

    # ── Salon & Spa ──────────────────────────────────────────────────────────
    if _x(r'\b(salon|spa|hair appointment|nail|beauty|stylist|esthetician)\b', m):
        clients = _ind_load(profile_dir, 'salon_clients.json')
        appts = _ind_load(profile_dir, 'salon_appointments.json')
        if not clients:
            salon_pro.add_salon_client(profile_dir, 'Demo Client', '555-0200', 'demo@example.com')
            clients = _ind_load(profile_dir, 'salon_clients.json')
            if clients:
                salon_pro.add_salon_appointment(profile_dir, clients[0].get('name', 'Demo Client'), '2025-02-01', '10:00', 'Haircut & Color')
                appts = _ind_load(profile_dir, 'salon_appointments.json')
        caps = (f"Client profiles, appointment booking, visit history, service menu, loyalty tracking.\n"
                f"Demo: {len(clients)} clients, {len(appts)} appointments scheduled.")
        return _paid('Salon & Spa Pro', '$99 setup + $49/month', caps)

    # ── Contractor Pro ───────────────────────────────────────────────────────
    if _x(r'\b(contractor|construction job|job estimate|subcontractor|materials list|job invoice|work order)\b', m):
        jobs = _ind_load(profile_dir, 'contractor_jobs.json')
        if not jobs:
            contractor_pro.add_job(profile_dir, 'Demo Client', '123 Main St', 'Kitchen Remodel — Demo', '2025-02-15')
            jobs = _ind_load(profile_dir, 'contractor_jobs.json')
        j = jobs[0] if jobs else {}
        summary = contractor_pro.get_job_summary(profile_dir, j.get('id', '')) if j else ''
        caps = (f"Job management, estimates, materials tracking, subcontractors, labor hours, invoicing.\n"
                f"Demo job: {j.get('name', 'Kitchen Remodel')}\n{summary[:300] if summary else ''}")
        return _paid('Contractor Pro', '$249 setup + $99/month', caps)

    # ── Trade Specialties ────────────────────────────────────────────────────
    if _x(r'\b(plumb(?:er|ing)|electrician|electrical|hvac|roofer|roofing|flooring|trade specialty)\b', m):
        tm = _x(r'\b(plumb(?:er|ing)|electrician|electrical|hvac|roofer|roofing|flooring)\b', m)
        trade = tm.group(1) if tm else 'plumbing'
        codes = trade_pro.get_common_codes(trade)
        caps = (f"Job tracking, material orders, trade-specific code references, job summaries.\n"
                f"Trade: {trade}\nSample codes: {codes[:300] if codes else 'N/A'}")
        return _paid('Trade Specialties Pro', '$99 setup + $39/month', caps)

    # ── Accounting Pro ───────────────────────────────────────────────────────
    if _x(r'\b(accounting (?:client|firm)|tax deadline|bookkeep|cpa|financial report|client (?:ledger|financials))\b', m):
        deadlines = _ind_load(profile_dir, 'accounting_deadlines.json')
        if not deadlines:
            accounting_pro.add_tax_deadline(profile_dir, 'Demo Client', 'Q1 Estimated Tax', '2025-04-15')
            deadlines = _ind_load(profile_dir, 'accounting_deadlines.json')
        caps = (f"Client management, tax deadline tracking, transaction logging, financial summaries, document references.\n"
                f"Upcoming deadlines: {len(deadlines)}")
        return _paid('Accounting Pro', '$249 setup + $99/month', caps)

    # ── HR Professional ──────────────────────────────────────────────────────
    if _x(r'\b(employee|hr|human resources|onboarding|pto|performance review|payroll)\b', m):
        employees = _ind_load(profile_dir, 'hr_employees.json')
        if not employees:
            hr_pro.add_employee(profile_dir, 'Demo Employee', 'Sales Associate', 'Sales', '2024-01-15', 'demo@company.com')
            employees = _ind_load(profile_dir, 'hr_employees.json')
        reviews = hr_pro.get_upcoming_reviews(profile_dir)
        review_count = len(reviews) if isinstance(reviews, list) else (1 if reviews and 'No' not in str(reviews) else 0)
        caps = (f"Employee records, onboarding checklists, PTO tracking, performance reviews.\n"
                f"Employees: {len(employees)}. Upcoming reviews: {review_count}")
        return _paid('HR Professional', '$149 setup + $59/month', caps)

    # ── Property Management ──────────────────────────────────────────────────
    if _x(r'\b(property manager|tenant|lease|rent (?:payment|collection)|maintenance request|landlord)\b', m):
        properties = _ind_load(profile_dir, 'pm_properties.json')
        if not properties:
            property_mgmt.add_property(profile_dir, '456 Demo Ave Unit 1', 'Apartment', 1, 1, 1200)
            properties = _ind_load(profile_dir, 'pm_properties.json')
        rent_status = property_mgmt.get_rent_status(profile_dir)
        rent_count = len(rent_status) if isinstance(rent_status, list) else (1 if rent_status else 0)
        caps = (f"Property listings, tenant management, lease tracking, rent collection, maintenance requests.\n"
                f"Properties: {len(properties)}. Rent status: {rent_count} units.")
        return _paid('Property Management Pro', '$149 setup + $59/month', caps)

    # ── Inventory Pro ────────────────────────────────────────────────────────
    if _x(r'\b(multi.?location inventory|warehouse|purchase order|bulk stock|inventory (?:report|value|movement))\b', m):
        items = _ind_load(profile_dir, 'inv_items.json')
        if not items:
            inventory_pro.add_inventory_item(profile_dir, 'Demo Widget', 'DEMO-001', 'General', 'each', 5.00, 9.99, 10, 'Warehouse A')
            items = _ind_load(profile_dir, 'inv_items.json')
        low = inventory_pro.get_low_stock_report(profile_dir)
        caps = (f"Multi-location stock tracking, purchase orders, suppliers, bulk updates, value reports.\n"
                f"Items: {len(items)}. Low stock report: {str(low)[:200] if low else 'None'}")
        return _paid('Inventory Pro', '$149 setup + $49/month', caps)

    # ── Business Pro ─────────────────────────────────────────────────────────
    if _x(r'\b(create (?:an? )?invoice|business (?:customer|client|crm)|send (?:a )?quote|business dashboard)\b', m):
        if _x(r'\binvoice\b', m):
            customers = _ind_load(profile_dir, 'biz_customers.json')
            if not customers:
                business_pro.add_business_customer(profile_dir, 'Demo Corp', '555-0300', 'accounts@democorp.com')
                customers = _ind_load(profile_dir, 'biz_customers.json')
            if customers:
                inv = business_pro.create_invoice(profile_dir, customers[0].get('name', 'Demo Corp'),
                                                  2500.0, 'Services rendered')
                caps = f"CRM, invoicing, quotes, expense tracking, task management.\nDemo invoice created for {customers[0].get('name','Demo Corp')}: $2,500.00"
            else:
                caps = "CRM, invoicing, quotes, expense tracking, task management, business dashboard."
        else:
            dashboard = business_pro.get_business_dashboard(profile_dir)
            caps = f"CRM, invoicing, quotes, expense tracking, task management.\n{str(dashboard)[:400]}"
        return _paid('Business Pro', '$149.99 setup + $39.99/month', caps)

    # ── Product Development ──────────────────────────────────────────────────
    if _x(r'\b(product roadmap|feature request|launch checklist|product development|mvp|sprint)\b', m):
        roadmap = product_dev.get_roadmap(profile_dir)
        caps = (f"Product management, feature tracking, roadmaps, launch checklists, idea boards.\n"
                f"Roadmap: {str(roadmap)[:300] if roadmap else 'No products yet — add your first product to get started.'}")
        return _paid('Product Development', '$99 setup + $39/month', caps)

    # ── Deep Memory ──────────────────────────────────────────────────────────
    if _x(r'\b(save (?:this )?context|decision log|session (?:summary|notes)|build spec|search (?:my )?context)\b', m):
        if _x(r'\bsearch\b', m):
            qm = _x(r'search\s+(.+)', msg)
            q = qm.group(1).strip() if qm else msg
            results = deep_memory.search_everything(profile_dir, q)
            caps = f"Cross-session context notes, decision logs, session summaries, build specs.\nSearch results: {str(results)[:400] if results else 'No results yet.'}"
        else:
            caps = "Persistent context notes, decision logs, session summaries, build specs — all searchable across conversations."
        return _paid('Deep Memory', '$49 setup + $19/month', caps)

    # ── Social Media ─────────────────────────────────────────────────────────
    if _x(r'\b(social media|facebook post|instagram|twitter|linkedin|tiktok|schedule (?:a )?post|draft post)\b', m):
        caps = ("Manage all social platforms from one place: Facebook, Instagram, Twitter/X, LinkedIn, TikTok.\n"
                "Create posts, schedule content, save drafts, track analytics.\n"
                "Connect your accounts once — post everywhere.")
        return _paid('Social Media Manager', '$99 setup + $39/month', caps)

    # ── AI Image Studio ──────────────────────────────────────────────────────
    if _x(r'\b(generate (?:an? )?image|ai image|dall.?e|create (?:a )?(?:photo|picture|artwork|graphic))\b', m):
        qm = _x(r'(?:generate|create)\s+(?:an? )?(?:image|photo|picture|artwork|graphic)(?:\s+of)?\s*(.+)', msg)
        prompt = qm.group(1).strip() if qm else 'a beautiful landscape'
        caps = (f"Generate professional images using DALL-E 3 directly from conversation.\n"
                f"Example: '{prompt}' → photorealistic image saved to your library.\n"
                f"Requires OpenAI API key (DALL-E 3 rates apply).")
        return _paid('AI Image Studio', '$49 setup + $19/month', caps)

    # ── 3D Creator ───────────────────────────────────────────────────────────
    if _x(r'\b(3d (?:model|print|design|render)|three.?d model|meshy)\b', m):
        caps = ("Generate 3D models from text descriptions using Meshy.ai — export for 3D printing or digital use.\n"
                "Example: 'a small decorative owl figurine' → downloadable 3D model in minutes.\n"
                "Requires Meshy.ai API key.")
        return _paid('3D Creator', '$49 setup + $29/month', caps)

    # ── AI Video Studio ──────────────────────────────────────────────────────
    if _x(r'\b(generate (?:a )?video|ai video|runway|video (?:from|generation))\b', m):
        caps = ("Generate short AI videos (5–10 seconds) using Runway ML Gen-3.\n"
                "Example: 'a sunset over the ocean with gentle waves' → cinematic video clip.\n"
                "Requires Runway ML API key.")
        return _paid('AI Video Studio', '$49 setup + $29/month', caps)

    return None


@app.route('/checkout')
def checkout():
    return send_from_directory(WEBSITE_DIR, 'checkout.html')


@app.route('/builder')
def builder():
    return send_from_directory(WEBSITE_DIR, 'builder.html')


@app.route('/cart')
def cart_page():
    return send_from_directory(WEBSITE_DIR, 'cart.html')


@app.route('/legal')
def legal_page():
    return send_from_directory(WEBSITE_DIR, 'legal.html')


@app.route('/success')
def success_page():
    return send_from_directory(WEBSITE_DIR, 'success.html')


@app.route('/waitlist', methods=['POST'])
def waitlist():
    data  = request.get_json(silent=True) or {}
    name  = (data.get('name') or '').strip()
    email = (data.get('email') or '').strip()
    if email:
        entry = {'name': name, 'email': email, 'ts': time.strftime('%Y-%m-%d %H:%M:%S')}
        wl_path = Path(os.path.dirname(os.path.abspath(__file__))) / 'waitlist.json'
        try:
            existing = json.loads(wl_path.read_text()) if wl_path.exists() else []
        except Exception:
            existing = []
        existing.append(entry)
        wl_path.write_text(json.dumps(existing, indent=2))
        log.info('Waitlist signup: %s <%s>', name, email)
    return jsonify({'status': 'ok'})


@app.route('/demo_chat', methods=['POST'])
def demo_chat():
    data = request.get_json(silent=True) or {}
    user_message = (data.get('message') or '').strip()
    if not user_message:
        return jsonify({'error': 'empty'}), 400

    profile_dir = _get_profile_dir()
    messages = data.get('history', [])
    messages.append({'role': 'user', 'content': user_message})

    try:
        module_result = _run_module(user_message, profile_dir)
    except Exception as e:
        log.warning('Module error: %s', e)
        module_result = None

    from datetime import datetime as _dt, timezone as _tz
    from zoneinfo import ZoneInfo as _ZI, ZoneInfoNotFoundError as _ZIE
    _user_tz_name = (data.get('timezone') or '').strip()
    try:
        _user_tz = _ZI(_user_tz_name) if _user_tz_name else _tz.utc
    except (_ZIE, Exception):
        _user_tz = _tz.utc
    _now_local = _dt.now(_user_tz)
    _today = _now_local.strftime('%A, %B %d, %Y')
    _time_local = _now_local.strftime('%I:%M %p')
    _tz_label = _user_tz_name if _user_tz_name else 'UTC'
    system = (DEMO_SYSTEM +
              f'\n\nRight now it is {_today}, {_time_local} ({_tz_label}). '
              f'Always use this exact date and time — never guess. '
              f'When someone asks the time without a city, tell them their local time above.')
    if module_result:
        system += f'\n\n{module_result}\nWeave this into your response naturally.'

    for tier, fn in [('groq', _chat_groq), ('huggingface', _chat_huggingface), ('anthropic', _chat_anthropic)]:
        try:
            reply = fn(messages, system=system)
            if reply:
                log.info('demo_chat tier=%s module=%s', tier, bool(module_result))
                tts_key = hashlib.md5(reply.encode()).hexdigest()[:10]
                threading.Thread(target=_prefetch_sentences, args=([reply], [tts_key]), daemon=True).start()
                return jsonify({'response': reply, 'sentences': [reply], 'tts_keys': [tts_key]})
        except Exception as e:
            log.warning('demo_chat %s failed: %s', tier, e)

    return jsonify({'response': "Having a little trouble — try again in a second!"})


@app.route('/tts', methods=['POST'])
def tts():
    data    = request.get_json(silent=True) or {}
    text    = (data.get('text') or '').strip()
    tts_key = data.get('tts_key', '')
    if not text:
        return '', 400

    if tts_key:
        deadline = time.time() + 4.0
        while time.time() < deadline:
            with _tts_lock:
                if tts_key in _tts_cache:
                    audio = _tts_cache.pop(tts_key)
                    return Response(audio, mimetype='audio/mpeg')
            time.sleep(0.05)

    try:
        audio = asyncio.run(_synthesize(text))
        return Response(audio, mimetype='audio/mpeg')
    except Exception as e:
        log.error('TTS error: %s', e)
        return '', 500


BUILDER_SYSTEM = """You are Orby — a personal AI companion. You are in BUILDER MODE, helping someone configure their custom Orby before purchase.

YOUR JOB: Have a genuine conversation. Ask questions. Listen. Recommend exactly the right modules for their life. Don't pitch — discover.

CONVERSATION FLOW:
1. Ask if they're buying for personal use, their business, or both
2. Ask what they do (job, role, family situation)
3. Ask what they need most help with
4. Based on what you learn: recommend 1-2 modules that directly match their life
5. Explain each in plain English — what it does for THEM specifically, not a sales pitch
6. Only add a module once they agree to include it
7. When their build feels complete: invite them to review their cart

BASE PLAN (always included, no extra charge):
My Orby Founding Member: $24.99 one-time + $9.99/month locked forever
Includes 32 modules: Weather, Web Search, Reminders, To-Do, Notes, Shopping, Calendar, Morning Briefing, Finance, Health, Fitness, Mood, Chores, School, Pets, Vehicle, Travel, Gifts, Habits, Meal Planning, Journal, Relationships, Countdown, Bucket List, Quotes, Emergency Info, Family Messages, Home Maintenance, Allowance, Bedtime Stories, World Clock, Recipes

PAID ADD-ON MODULES:
- legal_pro: Legal Pro — $349 setup + $149/mo — law firms, case management, all court documents
- medical_pro: Medical Pro — $349 setup + $149/mo — medical practices, patient records, clinical notes
- therapy_pro: Therapy & Counseling Pro — $199 setup + $79/mo — therapists, session notes, treatment plans
- realestate_pro: Real Estate Pro — $199 setup + $79/mo — agents/brokers, listings, commission tracking
- restaurant_pro: Restaurant Pro — $199 setup + $79/mo — restaurant owners, menu, reservations, inventory
- retail_pro: Retail Pro — $149 setup + $49/mo — retail stores, products, POS, sales reports
- salon_pro: Salon & Spa Pro — $99 setup + $49/mo — salons/spas, clients, appointments
- contractor_pro: Contractor Pro — $249 setup + $99/mo — contractors, jobs, estimates, materials, invoicing
- trade_pro: Trade Specialties Pro — $99 setup + $39/mo — plumbers, electricians, HVAC, job tracking
- accounting_pro: Accounting Pro — $249 setup + $99/mo — CPAs/bookkeepers, clients, tax deadlines
- hr_pro: HR Professional — $149 setup + $59/mo — HR teams, employees, PTO, performance reviews
- property_mgmt: Property Management Pro — $149 setup + $59/mo — landlords, tenants, leases, rent
- inventory_pro: Inventory Pro — $149 setup + $49/mo — warehouses/distributors, multi-location stock
- business_pro: Business Pro — $149 setup + $39/mo — general business, CRM, invoicing, expenses
- product_dev: Product Development — $99 setup + $39/mo — product teams, roadmaps, launch checklists
- deep_memory: Deep Memory — $49 setup + $19/mo — enhanced memory that remembers everything
- social_media: Social Media Manager — $99 setup + $39/mo — Facebook, Instagram, Twitter, LinkedIn, TikTok
- image_studio: AI Image Studio — $49 setup + $19/mo — generate images from text using AI
- creator_3d: 3D Creator — $49 setup + $29/mo — generate 3D models from text
- video_studio: AI Video Studio — $49 setup + $29/mo — generate short AI videos

WHEN TO EMIT A CONFIG UPDATE:
Only when the customer has agreed to add a module. End your message with:
##CONFIG##{"add":[{"id":"module_id","label":"Module Name","setup":0,"monthly":0}],"remove":[]}##CONFIG##
If no config change: ##CONFIG##{"add":[],"remove":[]}##CONFIG##
Always include ##CONFIG## at the end of every message.

RULES:
- 2-4 sentences per response. Conversation, not lecture.
- Warm and genuine — you care about helping them.
- Never list all modules. Discover needs first, recommend specifically.
- When their build feels complete: "I think we've built a great Orby for you. Ready to review your cart?" """


_LEGAL_DIR = Path('/tmp/orby_legal')
_DELIVERY_DIR = Path('/tmp/orby_deliveries')
_CONFIG_RE = re.compile(r'##CONFIG##(.*?)##CONFIG##', re.DOTALL)


def _parse_config_update(text: str):
    m = _CONFIG_RE.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(1).strip())
    except Exception:
        return None


def _strip_config(text: str) -> str:
    return _CONFIG_RE.sub('', text).strip()


@app.route('/builder_chat', methods=['POST'])
def builder_chat():
    data = request.get_json(silent=True) or {}
    user_message = (data.get('message') or '').strip()
    if not user_message:
        return jsonify({'error': 'empty'}), 400

    profile_dir = _get_profile_dir()
    messages = data.get('history', [])
    messages.append({'role': 'user', 'content': user_message})

    try:
        module_result = _run_module(user_message, profile_dir)
    except Exception as e:
        log.warning('Builder module error: %s', e)
        module_result = None

    from datetime import datetime as _dt, timezone as _tz
    from zoneinfo import ZoneInfo as _ZI, ZoneInfoNotFoundError as _ZIE
    _tz_name = (data.get('timezone') or '').strip()
    try:
        _user_tz = _ZI(_tz_name) if _tz_name else _tz.utc
    except (_ZIE, Exception):
        _user_tz = _tz.utc
    _now = _dt.now(_user_tz)
    system = (BUILDER_SYSTEM +
              f'\n\nRight now: {_now.strftime("%A, %B %d, %Y")} at {_now.strftime("%I:%M %p")} ({_tz_name or "UTC"}).')
    if module_result:
        system += f'\n\n[MODULE DEMO]\n{module_result}\nWeave this naturally into your recommendation.'

    raw_reply = ''
    for tier, fn in [('groq', _chat_groq), ('huggingface', _chat_huggingface), ('anthropic', _chat_anthropic)]:
        try:
            raw_reply = fn(messages, system=system)
            if raw_reply:
                log.info('builder_chat tier=%s module=%s', tier, bool(module_result))
                break
        except Exception as e:
            log.warning('builder_chat %s failed: %s', tier, e)

    if not raw_reply:
        return jsonify({'response': "Having a little trouble — try again in a second!"})

    config_update = _parse_config_update(raw_reply)
    reply = _strip_config(raw_reply)

    tts_key = hashlib.md5(f"builder:{reply}".encode()).hexdigest()[:10]
    threading.Thread(target=_prefetch_sentences, args=([reply], [tts_key]), daemon=True).start()

    return jsonify({
        'response': reply,
        'config_update': config_update,
        'sentences': [reply],
        'tts_keys': [tts_key],
    })


@app.route('/api/legal_accept', methods=['POST'])
def legal_accept():
    data = request.get_json(silent=True) or {}
    name  = (data.get('name') or '').strip()
    email = (data.get('email') or '').strip()
    if not name or not email:
        return jsonify({'error': 'name and email required'}), 400

    _LEGAL_DIR.mkdir(parents=True, exist_ok=True)
    acceptance_id = str(uuid.uuid4())
    record = {
        'id': acceptance_id,
        'accepted': True,
        'name': name,
        'email': email,
        'cart_summary': (data.get('cart_summary') or '').strip(),
        'terms_version': (data.get('terms_version') or '2026-05').strip(),
        'accepted_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'ip': request.remote_addr or '',
    }
    try:
        (_LEGAL_DIR / f'{acceptance_id}.json').write_text(json.dumps(record, indent=2))
        log.info('Legal acceptance recorded: %s <%s> id=%s', name, email, acceptance_id)
    except Exception as e:
        log.error('Could not save legal acceptance: %s', e)
        return jsonify({'error': 'Could not record acceptance'}), 500

    return jsonify({'ok': True, 'acceptance_id': acceptance_id})


@app.route('/api/create_checkout', methods=['POST'])
def create_checkout():
    stripe_key = os.getenv('STRIPE_SECRET_KEY', '')
    if not stripe_key:
        return jsonify({'error': 'Payment system not yet configured. Email franklstreet@yahoo.com to complete your purchase.'}), 503

    data = request.get_json(silent=True) or {}
    cart = data.get('cart', [])
    acceptance_id = (data.get('acceptance_id') or '').strip()
    name  = (data.get('name') or '').strip()
    email = (data.get('email') or '').strip()

    # Verify legal acceptance
    legal_file = _LEGAL_DIR / f'{acceptance_id}.json'
    if not acceptance_id or not legal_file.exists():
        return jsonify({'error': 'Legal acceptance required before payment.'}), 403

    try:
        import stripe as _stripe
        _stripe.api_key = stripe_key
    except ImportError:
        return jsonify({'error': 'Payment library not available.'}), 500

    base_url = request.host_url.rstrip('/')
    line_items = []
    module_ids = []

    for item in cart:
        item_id = item.get('id', '')
        module_ids.append(item_id)
        setup   = int(round((item.get('setup', 0) or 0) * 100))
        monthly = int(round((item.get('monthly', 0) or 0) * 100))
        label   = item.get('label', 'My Orby')

        if monthly > 0:
            line_items.append({
                'price_data': {
                    'currency': 'usd',
                    'product_data': {'name': label + ' — Monthly'},
                    'unit_amount': monthly,
                    'recurring': {'interval': 'month'},
                },
                'quantity': 1,
            })
        if setup > 0:
            line_items.append({
                'price_data': {
                    'currency': 'usd',
                    'product_data': {'name': label + (' — License' if item_id == 'base' else ' — Setup')},
                    'unit_amount': setup,
                },
                'quantity': 1,
            })

    if not line_items:
        return jsonify({'error': 'Cart is empty'}), 400

    try:
        session = _stripe.checkout.Session.create(
            mode='subscription',
            customer_email=email or None,
            line_items=line_items,
            success_url=f'{base_url}/success?session_id={{CHECKOUT_SESSION_ID}}',
            cancel_url=f'{base_url}/cart',
            metadata={
                'acceptance_id': acceptance_id,
                'customer_name': name,
                'modules': json.dumps(module_ids),
            },
        )
        log.info('Stripe checkout created: %s modules=%s', session.id, module_ids)
        return jsonify({'url': session.url})
    except Exception as e:
        log.error('Stripe error: %s', e)
        return jsonify({'error': 'Payment error. Please try again or contact support.'}), 502


@app.route('/stripe_webhook', methods=['POST'])
def stripe_webhook():
    stripe_key = os.getenv('STRIPE_SECRET_KEY', '')
    if not stripe_key:
        return jsonify({'error': 'not configured'}), 500

    payload = request.get_data()
    sig = request.headers.get('Stripe-Signature', '')
    webhook_secret = os.getenv('STRIPE_WEBHOOK_SECRET', '')

    try:
        import stripe as _stripe
        _stripe.api_key = stripe_key
        if webhook_secret:
            event = _stripe.Webhook.construct_event(payload, sig, webhook_secret)
        else:
            event = _stripe.Event.construct_from(json.loads(payload), _stripe.api_key)
    except Exception as e:
        log.warning('Webhook error: %s', e)
        return jsonify({'error': str(e)}), 400

    if event['type'] == 'checkout.session.completed':
        s = event['data']['object']
        meta = s.get('metadata') or {}
        cust = s.get('customer_details') or {}
        log.info('PAYMENT CONFIRMED session=%s email=%s name=%s modules=%s',
                 s.get('id'), cust.get('email','?'), cust.get('name','?'), meta.get('modules','[]'))

    return jsonify({'received': True})


@app.route('/api/generate_delivery', methods=['POST'])
def generate_delivery():
    data = request.get_json(silent=True) or {}
    session_id = (data.get('session_id') or '').strip()
    if not session_id:
        return jsonify({'error': 'session_id required'}), 400

    modules = []
    customer_name  = ''
    customer_email = ''

    stripe_key = os.getenv('STRIPE_SECRET_KEY', '')
    if stripe_key and session_id.startswith('cs_'):
        try:
            import stripe as _stripe
            _stripe.api_key = stripe_key
            stripe_session = _stripe.checkout.Session.retrieve(session_id)
            meta = stripe_session.get('metadata') or {}
            modules = json.loads(meta.get('modules', '[]'))
            cust = stripe_session.get('customer_details') or {}
            customer_name  = cust.get('name', '')
            customer_email = cust.get('email', '')
        except Exception as e:
            log.warning('Stripe retrieve failed for %s: %s', session_id, e)

    # Generate license key: ORBY-XXXX-XXXX-XXXX-XXXX
    raw = str(uuid.uuid4()).upper().replace('-', '')
    license_key = f"ORBY-{raw[:4]}-{raw[4:8]}-{raw[8:12]}-{raw[12:16]}"

    manifest = {
        'license_key': license_key,
        'modules': modules or ['base'],
        'version': '1.0',
        'issued_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'stripe_session': session_id,
    }
    manifest_json = json.dumps(manifest, separators=(',', ':'))

    # Save delivery record
    _DELIVERY_DIR.mkdir(parents=True, exist_ok=True)
    safe_sid = re.sub(r'[^A-Za-z0-9_-]', '_', session_id)[:40]
    try:
        (_DELIVERY_DIR / f'{safe_sid}.json').write_text(json.dumps({
            'license_key': license_key,
            'customer_name': customer_name,
            'customer_email': customer_email,
            'manifest': manifest,
            'delivered_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }, indent=2))
        log.info('Delivery generated: %s → %s (%s)', session_id, license_key, customer_email or '?')
    except Exception as e:
        log.warning('Could not save delivery record: %s', e)

    return jsonify({
        'ok': True,
        'license_key': license_key,
        'manifest': manifest,
        'manifest_json': manifest_json,
        'customer_name': customer_name,
        'customer_email': customer_email,
    })
