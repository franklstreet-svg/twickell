"""Standalone server for the My Orby marketing website — runs on port 5001."""
import os
import json
import asyncio
import logging
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


@app.route('/demo_chat', methods=['POST'])
def demo_chat():
    data = request.get_json(silent=True) or {}
    user_message = (data.get('message') or '').strip()
    if not user_message:
        return jsonify({'error': 'empty'}), 400

    demo_system = """You are Orby — a personal AI companion that runs locally on a customer's computer. You are talking to a potential customer on the My Orby website who has never used you before.

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

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY', ''))
        messages = data.get('history', [])
        messages.append({'role': 'user', 'content': user_message})
        response = client.messages.create(
            model=os.getenv('ANTHROPIC_MODEL', 'claude-sonnet-4-6'),
            max_tokens=300,
            system=demo_system,
            messages=messages,
            temperature=1.0,
        )
        return jsonify({'response': response.content[0].text})
    except Exception as e:
        log.error('Demo chat error: %s', e)
        return jsonify({'response': "Having a little trouble — try again in a second!"})


@app.route('/tts', methods=['POST'])
def tts():
    data = request.get_json(silent=True) or {}
    text = (data.get('text') or '').strip()
    if not text:
        return '', 400
    try:
        audio = asyncio.run(_synthesize(text))
        return Response(audio, mimetype='audio/mpeg')
    except Exception as e:
        log.error('TTS error: %s', e)
        return '', 500


if __name__ == '__main__':
    log.info('My Orby website running at http://localhost:5001')
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)
