MODULE_MANIFEST = {
    "id": "M014",
    "name": "Family Messages",
    "category": "Family",
    "description": "Send and receive messages within the family, view message history and announcements.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["send_family_message", "get_family_messages"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def send_message(profile_dir, sender, recipient, content, urgent=False):
    items = load(profile_dir, 'family_messages')
    msg = {'id': new_id(), 'sender': sender, 'recipient': recipient,
           'content': content, 'urgent': urgent,
           'read': False, 'created': now_iso()}
    items.append(msg)
    save(profile_dir, 'family_messages', items)
    dest = f"to {recipient}" if recipient != 'everyone' else "to the whole family"
    return f"Message sent {dest}: '{content}'"


def get_unread(profile_dir, recipient):
    items = load(profile_dir, 'family_messages')
    return [m for m in items
            if not m.get('read') and (
                m.get('recipient', '').lower() in (recipient.lower(), 'everyone')
            )]


def mark_read(profile_dir, message_id):
    items = load(profile_dir, 'family_messages')
    for m in items:
        if m['id'] == message_id:
            m['read'] = True
    save(profile_dir, 'family_messages', items)


def get_all(profile_dir):
    return load(profile_dir, 'family_messages')


def context_summary(profile_dir, owner_name='') -> str:
    if not owner_name:
        return ''
    unread = get_unread(profile_dir, owner_name)
    if unread:
        return "FAMILY MESSAGES: " + "; ".join(
            f"{m['sender']} says: {m['content']}" for m in unread[:3])
    return ''
