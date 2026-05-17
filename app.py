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
    bedtime_story, mood_skill, web_search,
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

    return None


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
