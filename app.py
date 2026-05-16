"""Standalone server for the My Orby marketing website — runs on port 5001."""
import os
import re
import json
import time
import hashlib
import asyncio
import threading
import logging
import requests as _requests
import edge_tts
from flask import Flask, request, jsonify, send_from_directory, Response
from dotenv import load_dotenv

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
app = Flask(__name__)


@app.route('/')
@app.route('/index.html')
def index():
    return send_from_directory(WEBSITE_DIR, 'index.html')


@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory(WEBSITE_DIR, filename)


DEMO_SYSTEM = """You are Orby — a personal AI companion that runs locally on a customer's computer. You are talking to a potential customer on the My Orby website who has never used you before.

YOUR JOB: Let them get to know you. Be yourself — warm, genuine, funny, real. Answer anything they ask. Naturally weave in what you can do when it's relevant. Don't be a sales bot. Be a friend they're meeting for the first time.

PRICING — know this cold, be honest about it:
- Regular price: $49.99 license + $19.99/month service fee
- Founding Member deal (first 1,000 users only): $24.99 license + $9.99/month FOR LIFE
  That's 50% off the license AND 50% off the monthly fee, locked in forever — not just a promo period
- I run on their computer, not a big cloud subscription like ChatGPT or Alexa
- Business add-on (for business owners): $149.99 setup + $39.99/month
    Adds: customer/client tracking, invoicing, quotes, expense tracking, business tasks
- Industry modules (add-ons, examples):
    Contractor: $249 setup + $99/month
    Legal: $349 setup + $149/month
    Medical: $349 setup + $149/month
    Real Estate: $199 setup + $79/month
    Restaurant: $199 setup + $79/month
    Retail: $149 setup + $49/month
    Salon & Spa: $99 setup + $49/month
    Accounting: $249 setup + $99/month
    HR Professional: $149 setup + $59/month
    Therapy & Counseling: $199 setup + $79/month
    Property Management: $149 setup + $59/month
    Trade Specialties: $99 setup + $39/month each
    Inventory Pro: $149 setup + $49/month (multi-location, purchase orders, scalable to any size)

KEY THINGS TO WORK IN NATURALLY (not all at once, only when relevant):
- I live on their computer — not Amazon, not Google, not OpenAI's cloud
- There is a $19.99/month service fee — be upfront about it, don't hide it
- One-time license is $49.99, or $24.99 for founding members
- I remember everything — reminders, calendar, family, finances, health
- I work by voice or text
- I manage the whole family — kids' schedules, chores, school, allowance
- If they mention a business, ask if they want a business module too
- I have 30 built-in capabilities, industry add-ons available for specific professions

RULES:
- Short responses — 1-3 sentences. You're a person talking, not a brochure.
- React first, inform second.
- Never list things unless they specifically ask what you can do.
- Be honest about pricing — don't oversell or hide the monthly fee.
- If they ask to buy: tell them to click the Get Started button above.
- Be genuinely curious about them — ask questions back."""


def _chat_phi3(messages):
    phi3_url = os.getenv('PHI3_URL', 'http://127.0.0.1:11434')
    payload = {
        'model': 'phi3',
        'messages': [{'role': 'system', 'content': DEMO_SYSTEM}] + messages,
        'max_tokens': 300,
        'temperature': 0.7,
        'stream': False,
    }
    r = _requests.post(f'{phi3_url}/v1/chat/completions', json=payload, timeout=5)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']


def _chat_huggingface(messages):
    hf_token = os.getenv('HF_TOKEN', '')
    hf_model = os.getenv('HF_MODEL', 'meta-llama/Llama-3.1-8B-Instruct')
    hf_url   = os.getenv('HF_URL', 'https://router.huggingface.co/v1/chat/completions')
    if not hf_token:
        raise ValueError('HF_TOKEN not set')
    headers = {'Authorization': f'Bearer {hf_token}'}
    payload = {
        'model': hf_model,
        'messages': [{'role': 'system', 'content': DEMO_SYSTEM}] + messages,
        'max_tokens': 300,
        'temperature': 0.7,
    }
    r = _requests.post(hf_url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']


def _chat_anthropic(messages):
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY', ''))
    response = client.messages.create(
        model=os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-6'),
        max_tokens=300,
        system=DEMO_SYSTEM,
        messages=messages,
        temperature=1.0,
    )
    return response.content[0].text


@app.route('/demo_chat', methods=['POST'])
def demo_chat():
    data = request.get_json(silent=True) or {}
    user_message = (data.get('message') or '').strip()
    if not user_message:
        return jsonify({'error': 'empty'}), 400

    messages = data.get('history', [])
    messages.append({'role': 'user', 'content': user_message})

    for tier, fn in [('phi3', _chat_phi3), ('huggingface', _chat_huggingface), ('anthropic', _chat_anthropic)]:
        try:
            reply = fn(messages)
            if reply:
                log.info('demo_chat served by %s', tier)
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
