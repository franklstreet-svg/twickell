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
    bedtime_story, mood_skill, web_search, recipes_skill,
)
from modules.paid import (
    legal_pro, legal_docs,
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
- If they ask to buy: tell them to click Get Started above."""


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

    # ── Legal Pro ────────────────────────────────────────────────────────────
    if _x(r'\b(lawyer|attorney|legal case|case (?:notes|deadline|billing)|law firm|deposition|motion|brief|retainer|legal client)\b', m):
        # Add a demo case if none exist
        cases = legal_pro.list_legal_cases(profile_dir)
        if not cases:
            legal_pro.add_legal_case(profile_dir, 'Smith v. Johnson — Personal Injury', 'Active', 'Motor vehicle accident, seeking $250,000 in damages')
            cases = legal_pro.list_legal_cases(profile_dir)
        sample = cases[0] if cases else {}
        summary = legal_pro.get_billing_summary(profile_dir, sample.get('id',''))
        caps = (f"Case management, deadline tracking, billable time logging, case notes.\n"
                f"Sub-modules: demand letters, intake forms, retainer agreements, case summaries, client updates, billing invoices.\n"
                f"Demo case loaded: {sample.get('title','')}\n{summary}")
        return _paid('Legal Pro', '$349 setup + $149/month', caps)

    if _x(r'\b(demand letter|legal brief|intake form|retainer agreement|legal document|billing invoice for)\b', m):
        cases = legal_pro.list_legal_cases(profile_dir)
        if not cases:
            legal_pro.add_legal_case(profile_dir, 'Demo Case — Sample Matter', 'Active', 'Sample legal matter for demonstration')
            cases = legal_pro.list_legal_cases(profile_dir)
        case_id = cases[0]['id']
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

    # ── Medical Pro ──────────────────────────────────────────────────────────
    if _x(r'\b(patient (?:record|chart|history|list)|medical practice|doctor.?s office|clinic|prescription|appointment (?:schedule|list))\b', m):
        patients = medical_pro.list_patients(profile_dir)
        if not patients:
            medical_pro.add_patient(profile_dir, 'Jane Demo', '1985-03-15', '555-0100')
            patients = medical_pro.list_patients(profile_dir)
        pt = patients[0] if patients else {}
        caps = (f"Patient records, appointment scheduling, prescription tracking, treatment notes.\n"
                f"Sub-modules: SOAP notes, prior auth letters, discharge summaries, referral letters, Rx notes, patient summaries.\n"
                f"Demo patient: {pt.get('name','')} | ID: {pt.get('id','')}")
        return _paid('Medical Pro', '$349 setup + $149/month', caps)

    if _x(r'\b(soap note|prior auth|discharge summary|medical document|referral letter|prescription note)\b', m):
        patients = medical_pro.list_patients(profile_dir)
        if not patients:
            medical_pro.add_patient(profile_dir, 'Jane Demo', '1985-03-15', '555-0100')
            patients = medical_pro.list_patients(profile_dir)
        pt_id = patients[0]['id']
        if _x(r'\bsoap note\b', m):
            doc = medical_notes.generate_soap_note(profile_dir, pt_id, 'Patient reports persistent headaches for 3 days, rated 7/10.', 'BP 128/82, HR 76, afebrile. Alert and oriented x3.', 'Tension headache vs. migraine — rule out secondary causes.', 'Rx Ibuprofen 600mg TID x5 days. Follow up in 1 week if no improvement.')
        elif _x(r'\bprior auth\b', m):
            doc = medical_notes.generate_prior_auth(profile_dir, pt_id, 'MRI Brain without contrast', 'Chronic headache — G43.909', 'Blue Cross Blue Shield')
        elif _x(r'\bdischarge\b', m):
            doc = medical_notes.generate_discharge_summary(profile_dir, pt_id, '2025-01-10', 'Tension headache', 'IV fluids, pain management, rest', 'Follow up with PCP in 1 week')
        else:
            doc = medical_notes.generate_patient_summary(profile_dir, pt_id)
        return f'[MEDICAL DOC]\n{doc[:600]}...\n\n(Full document saved. Medical Pro — $349 setup + $149/month)'

    # ── Therapy Pro ──────────────────────────────────────────────────────────
    if _x(r'\b(therapy client|counseling|therapist|mental health practice|session note|treatment plan|dap note)\b', m):
        clients = therapy_pro.list_therapy_clients(profile_dir)
        if not clients:
            therapy_pro.add_therapy_client(profile_dir, 'Alex Demo', '1990-06-20', 'Anxiety, depression')
            clients = therapy_pro.list_therapy_clients(profile_dir)
        cl = clients[0] if clients else {}
        caps = (f"Client management, session scheduling, treatment plans, session notes.\n"
                f"Sub-modules: SOAP/DAP session notes, treatment plan documents, progress notes, discharge summaries, referrals, billing/superbills.\n"
                f"Demo client: {cl.get('name','')}")
        return _paid('Therapy & Counseling Pro', '$199 setup + $79/month', caps)

    if _x(r'\b(session note|therapy note|progress note|treatment plan doc|therapy discharge|therapy billing|superbill)\b', m):
        clients = therapy_pro.list_therapy_clients(profile_dir)
        if not clients:
            therapy_pro.add_therapy_client(profile_dir, 'Alex Demo', '1990-06-20', 'Anxiety, depression')
            clients = therapy_pro.list_therapy_clients(profile_dir)
        cl_id = clients[0]['id']
        if _x(r'\b(dap note|session note|therapy note)\b', m):
            doc = therapy_notes.generate_session_note(profile_dir, cl_id, 'Client reports anxiety improving, still struggling with work stress.', 'Calm affect, engaged, maintained eye contact throughout session.', 'GAD with occupational stressor. Progress noted toward goals.', 'Continue CBT techniques. Assign thought record homework. Meet in 1 week.')
        elif _x(r'\bprogress note\b', m):
            doc = therapy_notes.generate_progress_note(profile_dir, cl_id, 'Client demonstrating improved coping strategies and reduced anxiety symptoms.', 'Work stress remains a trigger. Avoidance behaviors persist.', 'Continue exposure therapy. Introduce mindfulness techniques next session.')
        elif _x(r'\btreatment plan\b', m):
            doc = therapy_notes.generate_treatment_plan_doc(profile_dir, cl_id, 'Generalized anxiety disorder with occupational stressor', ['Reduce anxiety symptoms by 50% within 12 sessions', 'Develop 3 healthy coping strategies', 'Improve work-life balance'], 'Cognitive Behavioral Therapy (CBT), mindfulness-based interventions, behavioral activation')
        elif _x(r'\b(superbill|billing)\b', m):
            doc = therapy_notes.generate_billing_note(profile_dir, cl_id, '90837', 'F41.1', '2025-01-15', 60)
        else:
            doc = therapy_notes.generate_discharge_note(profile_dir, cl_id, 'Client met treatment goals', 'Significant reduction in anxiety symptoms over 16 sessions', 'Continue self-care practices. Return if symptoms recur.')
        return f'[THERAPY DOC]\n{doc[:600]}...\n\n(Full document saved. Therapy Pro — $199 setup + $79/month)'

    # ── Real Estate Pro ──────────────────────────────────────────────────────
    if _x(r'\b(real estate|listing|showing|buyer|seller|commission|open house|realtor)\b', m):
        listings = realestate_pro.list_listings(profile_dir)
        if not listings:
            realestate_pro.add_listing(profile_dir, '123 Demo Street', 450000, 'Single Family', 3, 2)
            listings = realestate_pro.list_listings(profile_dir)
        l = listings[0] if listings else {}
        comm = realestate_pro.calculate_commission(l.get('price', 450000)) if listings else 'N/A'
        caps = f"Listing management, buyer/seller tracking, showings, offers, commission calculator.\nDemo listing: {l.get('address','')} — ${l.get('price',0):,}\nCommission at 3%: {comm}"
        return _paid('Real Estate Pro', '$199 setup + $79/month', caps)

    # ── Restaurant Pro ───────────────────────────────────────────────────────
    if _x(r'\b(restaurant|menu (?:item|management)|reservation|table|food inventory|supplier|diner)\b', m):
        menu = restaurant_pro.list_menu(profile_dir)
        if not menu:
            restaurant_pro.add_menu_item(profile_dir, 'House Burger', 'Mains', 14.99)
            restaurant_pro.add_menu_item(profile_dir, 'Caesar Salad', 'Starters', 9.99)
            menu = restaurant_pro.list_menu(profile_dir)
        low = restaurant_pro.get_low_inventory(profile_dir)
        caps = f"Menu management, reservations, inventory tracking, supplier management.\nDemo menu: {len(menu)} items. Low stock alerts: {len(low)} items."
        return _paid('Restaurant Pro', '$199 setup + $79/month', caps)

    # ── Retail Pro ───────────────────────────────────────────────────────────
    if _x(r'\b(retail store|product inventory|point of sale|pos system|stock (?:level|alert)|sales report)\b', m):
        products = retail_pro.list_products(profile_dir)
        if not products:
            retail_pro.add_product(profile_dir, 'Demo Product', 'SKU-001', 29.99, 50)
            products = retail_pro.list_products(profile_dir)
        report = retail_pro.get_sales_report(profile_dir)
        caps = f"Product catalog, inventory tracking, POS sales recording, sales reports, low-stock alerts.\nDemo: {len(products)} products. {report}"
        return _paid('Retail Pro', '$149 setup + $49/month', caps)

    # ── Salon & Spa ──────────────────────────────────────────────────────────
    if _x(r'\b(salon|spa|hair appointment|nail|beauty|stylist|esthetician)\b', m):
        appts = salon_pro.list_salon_appointments(profile_dir)
        if not appts:
            salon_pro.add_salon_client(profile_dir, 'Demo Client', '555-0200', 'demo@example.com')
            clients = salon_pro.list_salon_clients(profile_dir)
            if clients:
                salon_pro.add_salon_appointment(profile_dir, clients[0]['id'], 'Haircut & Color', '2025-02-01 10:00')
        caps = f"Client profiles, appointment booking, visit history, service menu, loyalty tracking.\nDemo: {len(appts)} appointments scheduled."
        return _paid('Salon & Spa Pro', '$99 setup + $49/month', caps)

    # ── Contractor Pro ───────────────────────────────────────────────────────
    if _x(r'\b(contractor|construction job|job estimate|subcontractor|materials list|job invoice|work order)\b', m):
        jobs = contractor_pro.list_jobs(profile_dir)
        if not jobs:
            contractor_pro.add_job(profile_dir, 'Kitchen Remodel — Demo', 'Demo Client', '2025-02-15')
            jobs = contractor_pro.list_jobs(profile_dir)
        j = jobs[0] if jobs else {}
        summary = contractor_pro.get_job_summary(profile_dir, j.get('id','')) if jobs else ''
        caps = f"Job management, estimates, materials tracking, subcontractors, labor hours, invoicing.\nDemo job: {j.get('name','')}\n{summary}"
        return _paid('Contractor Pro', '$249 setup + $99/month', caps)

    # ── Trade Specialties ────────────────────────────────────────────────────
    if _x(r'\b(plumb(?:er|ing)|electrician|electrical|hvac|roofer|roofing|flooring|trade specialty)\b', m):
        tm = _x(r'\b(plumb(?:er|ing)|electrician|electrical|hvac|roofer|roofing|flooring)\b', m)
        trade = tm.group(1) if tm else 'plumbing'
        codes = trade_pro.get_common_codes(trade)
        caps = f"Job tracking, material orders, trade-specific code references, job summaries.\nTrade: {trade}\nSample codes: {codes[:300] if codes else 'N/A'}"
        return _paid('Trade Specialties Pro', '$99 setup + $39/month', caps)

    # ── Accounting Pro ───────────────────────────────────────────────────────
    if _x(r'\b(accounting (?:client|firm)|tax deadline|bookkeep|cpa|financial report|client (?:ledger|financials))\b', m):
        deadlines = accounting_pro.get_upcoming_tax_deadlines(profile_dir)
        if not deadlines:
            accounting_pro.add_tax_deadline(profile_dir, 'Q1 Estimated Tax', '2025-04-15')
            deadlines = accounting_pro.get_upcoming_tax_deadlines(profile_dir)
        caps = f"Client management, tax deadline tracking, transaction logging, financial summaries, document references.\nUpcoming deadlines: {len(deadlines)}"
        return _paid('Accounting Pro', '$249 setup + $99/month', caps)

    # ── HR Professional ──────────────────────────────────────────────────────
    if _x(r'\b(employee|hr|human resources|onboarding|pto|performance review|payroll)\b', m):
        employees = hr_pro.list_employees(profile_dir)
        if not employees:
            hr_pro.add_employee(profile_dir, 'Demo Employee', 'demo@company.com', 'Sales', '2024-01-15')
            employees = hr_pro.list_employees(profile_dir)
        reviews = hr_pro.get_upcoming_reviews(profile_dir)
        caps = f"Employee records, onboarding checklists, PTO tracking, performance reviews.\nEmployees: {len(employees)}. Upcoming reviews: {len(reviews)}"
        return _paid('HR Professional', '$149 setup + $59/month', caps)

    # ── Property Management ──────────────────────────────────────────────────
    if _x(r'\b(property manager|tenant|lease|rent (?:payment|collection)|maintenance request|landlord)\b', m):
        properties = property_mgmt.list_properties(profile_dir)
        if not properties:
            property_mgmt.add_property(profile_dir, '456 Demo Ave Unit 1', 'Apartment', 1200)
            properties = property_mgmt.list_properties(profile_dir)
        rent_status = property_mgmt.get_rent_status(profile_dir) if properties else []
        caps = f"Property listings, tenant management, lease tracking, rent collection, maintenance requests.\nProperties: {len(properties)}. Rent status: {len(rent_status)} units."
        return _paid('Property Management Pro', '$149 setup + $59/month', caps)

    # ── Inventory Pro ────────────────────────────────────────────────────────
    if _x(r'\b(multi.?location inventory|warehouse|purchase order|bulk stock|inventory (?:report|value|movement))\b', m):
        items = inventory_pro.list_inventory(profile_dir)
        if not items:
            inventory_pro.add_inventory_item(profile_dir, 'Demo Widget', 'DEMO-001', 100, 9.99, 'Warehouse A')
            items = inventory_pro.list_inventory(profile_dir)
        low = inventory_pro.get_low_stock_report(profile_dir)
        caps = f"Multi-location stock tracking, purchase orders, suppliers, bulk updates, value reports.\nItems: {len(items)}. Low stock alerts: {low[:200] if low else 'None'}"
        return _paid('Inventory Pro', '$149 setup + $49/month', caps)

    # ── Business Pro ─────────────────────────────────────────────────────────
    if _x(r'\b(create (?:an? )?invoice|business (?:customer|client|crm)|send (?:a )?quote|business dashboard)\b', m):
        if _x(r'\binvoice\b', m):
            customers = business_pro.list_business_customers(profile_dir)
            if not customers:
                business_pro.add_business_customer(profile_dir, 'Demo Corp', 'accounts@democorp.com', '555-0300')
                customers = business_pro.list_business_customers(profile_dir)
            inv = business_pro.create_invoice(profile_dir, customers[0]['id'], [{'description': 'Services rendered', 'amount': 2500}])
            caps = f"CRM, invoicing, quotes, expense tracking, task management, business dashboard.\nDemo invoice created: ${2500:.2f}"
        else:
            dashboard = business_pro.get_business_dashboard(profile_dir)
            caps = f"CRM, invoicing, quotes, expense tracking, task management.\n{dashboard}"
        return _paid('Business Pro', '$149.99 setup + $39.99/month', caps)

    # ── Product Development ──────────────────────────────────────────────────
    if _x(r'\b(product roadmap|feature request|launch checklist|product development|mvp|sprint)\b', m):
        roadmap = product_dev.get_roadmap(profile_dir)
        caps = f"Product management, feature tracking, roadmaps, launch checklists, idea boards.\nRoadmap: {roadmap[:300] if roadmap else 'No products yet — add your first product to get started.'}"
        return _paid('Product Development', '$99 setup + $39/month', caps)

    # ── Deep Memory ──────────────────────────────────────────────────────────
    if _x(r'\b(save (?:this )?context|decision log|session (?:summary|notes)|build spec|search (?:my )?context)\b', m):
        if _x(r'\bsearch\b', m):
            qm = _x(r'search\s+(.+)', msg)
            q = qm.group(1).strip() if qm else msg
            results = deep_memory.search_everything(profile_dir, q)
            caps = f"Cross-session context notes, decision logs, session summaries, build specs.\nSearch results: {results[:400] if results else 'No results yet.'}"
        else:
            caps = "Persistent context notes, decision logs, session summaries, build specs — all searchable across conversations."
        return _paid('Deep Memory', '$49 setup + $19/month', caps)

    # ── Social Media ─────────────────────────────────────────────────────────
    if _x(r'\b(social media|facebook post|instagram|twitter|linkedin|tiktok|schedule (?:a )?post|draft post)\b', m):
        platforms = ['Facebook', 'Instagram', 'Twitter/X', 'LinkedIn', 'TikTok']
        caps = f"Manage all social platforms from one place: {', '.join(platforms)}.\nCreate posts, schedule content, save drafts, track analytics.\nConnect your accounts once — post everywhere."
        return _paid('Social Media Manager', '$99 setup + $39/month', caps)

    # ── AI Image Studio ──────────────────────────────────────────────────────
    if _x(r'\b(generate (?:an? )?image|ai image|dall.?e|create (?:a )?(?:photo|picture|artwork|graphic))\b', m):
        qm = _x(r'(?:generate|create)\s+(?:an? )?(?:image|photo|picture|artwork|graphic)(?:\s+of)?\s*(.+)', msg)
        prompt = qm.group(1).strip() if qm else 'a beautiful landscape'
        caps = f"Generate professional images using DALL-E 3 directly from conversation.\nExample: '{prompt}' → photorealistic image saved to your library.\nRequires OpenAI API key (DALL-E 3 rates apply)."
        return _paid('AI Image Studio', '$49 setup + $19/month', caps)

    # ── 3D Creator ───────────────────────────────────────────────────────────
    if _x(r'\b(3d (?:model|print|design|render)|three.?d model|meshy)\b', m):
        caps = "Generate 3D models from text descriptions using Meshy.ai — export for 3D printing or digital use.\nExample: 'a small decorative owl figurine' → downloadable 3D model in minutes.\nRequires Meshy.ai API key."
        return _paid('3D Creator', '$49 setup + $29/month', caps)

    # ── AI Video Studio ──────────────────────────────────────────────────────
    if _x(r'\b(generate (?:a )?video|ai video|runway|video (?:from|generation))\b', m):
        caps = "Generate short AI videos (5–10 seconds) using Runway ML Gen-3.\nExample: 'a sunset over the ocean with gentle waves' → cinematic video clip.\nRequires Runway ML API key."
        return _paid('AI Video Studio', '$49 setup + $29/month', caps)

    return None


@app.route('/checkout')
def checkout():
    return send_from_directory(WEBSITE_DIR, 'checkout.html')


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

    system = DEMO_SYSTEM
    if module_result:
        system += f'\n\n{module_result}\nWeave this into your response naturally.'

    for tier, fn in [('groq', _chat_groq), ('huggingface', _chat_huggingface), ('anthropic', _chat_anthropic)]:
        try:
            reply = fn(messages, system=system)
            if reply:
                log.info('demo_chat tier=%s module=%s', tier, bool(module_result))
                sentences = _split_sentences(reply)
                keys = [hashlib.md5(f"{reply}:{i}".encode()).hexdigest()[:10] for i in range(len(sentences))]
                threading.Thread(target=_prefetch_sentences, args=(sentences, keys), daemon=True).start()
                return jsonify({'response': reply, 'sentences': sentences, 'tts_keys': keys})
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


if __name__ == '__main__':
    log.info('My Orby website running at http://localhost:5001')
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)
