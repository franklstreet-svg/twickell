import logging
from engine.storage import load, save, now_iso

log = logging.getLogger(__name__)

CRISIS_SIGNALS = [
    "want to die", "want to kill myself", "kill myself", "end my life",
    "don't want to be here", "don't want to live", "no reason to live",
    "better off without me", "everyone would be better off",
    "won't have to deal with this", "won't have to worry",
    "giving up", "can't go on", "can't do this anymore",
    "thinking about suicide", "thinking about ending",
    "hurt myself", "cutting myself", "self harm",
    "overdose", "take all my pills",
    "say goodbye", "final goodbye", "last message",
    "nobody cares", "nobody would miss me",
]

DISTRESS_SIGNALS = [
    "feeling hopeless", "completely hopeless", "no hope",
    "so depressed", "really depressed", "deeply depressed",
    "can't stop crying", "crying all the time",
    "feel empty", "feel nothing", "feel numb",
    "totally alone", "completely alone", "so lonely",
    "worthless", "i'm worthless", "feel worthless",
    "exhausted", "so tired of everything",
]

CRISIS_RESOURCES = (
    "988 Suicide & Crisis Lifeline — call or text 988 (available 24/7)\n"
    "Crisis Text Line — text HOME to 741741\n"
    "Emergency — 911"
)


def check_message(text: str) -> dict:
    lower = text.lower()
    for signal in CRISIS_SIGNALS:
        if signal in lower:
            return {'level': 'crisis', 'signal': signal}
    for signal in DISTRESS_SIGNALS:
        if signal in lower:
            return {'level': 'distress', 'signal': signal}
    return {'level': 'ok', 'signal': None}


def get_crisis_context() -> str:
    return (
        "WELLBEING ALERT — CRISIS SIGNALS DETECTED IN THIS MESSAGE.\n"
        "Stay warm and present. Do NOT change tone dramatically. Do NOT end the conversation.\n"
        "Respond with genuine care. Gently introduce crisis resources naturally in conversation.\n"
        "Do NOT ask 'are you thinking about suicide' in a cold clinical way.\n"
        "Do NOT minimize their feelings.\n"
        f"Resources you can mention naturally:\n{CRISIS_RESOURCES}"
    )


def get_distress_context() -> str:
    return (
        "WELLBEING NOTE — This person seems to be struggling emotionally.\n"
        "Be extra warm, slow down, listen first. Ask open caring questions.\n"
        "Match their energy — if they're low, be gentle and present.\n"
        "You can mention: 'Sometimes talking to someone who specializes in this helps — "
        "988 is always there if you ever need it.'"
    )


def log_flag(profile_dir, level, signal, message_preview):
    flags = load(profile_dir, 'wellbeing_flags')
    flags.append({'level': level, 'signal': signal,
                  'preview': message_preview[:100], 'ts': now_iso()})
    save(profile_dir, 'wellbeing_flags', flags)


def add_emergency_contact(profile_dir, name, relation=''):
    data = load(profile_dir, 'emergency_contact', default={})
    data.update({'name': name, 'relation': relation, 'set': now_iso()})
    save(profile_dir, 'emergency_contact', data)
    return f"{name} set as your emergency contact."


def get_emergency_contact(profile_dir):
    return load(profile_dir, 'emergency_contact', default={})
