"""Deep Memory — full-detail context notes, decision log, session summaries.
For everything too important and too detailed to fit in a one-line memory."""
MODULE_MANIFEST = {
    "id": "P002",
    "name": "Deep Memory",
    "category": "Productivity",
    "description": "Persistent long-term memory for Orby. Save full-detail context notes, decisions, session summaries, and build specs that survive across every conversation.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["save_context_note", "get_context_note", "search_context", "list_context_notes",
              "delete_context_note", "log_decision", "get_decisions", "save_session_summary",
              "get_session", "list_sessions", "search_sessions", "save_spec", "get_spec",
              "list_specs", "update_spec_status", "search_everything"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
from datetime import datetime
from pathlib import Path


def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'skills'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename


def _load(path: Path, default) -> list:
    if path.exists():
        return json.loads(path.read_text())
    return default


def _save(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now() -> str:
    return datetime.utcnow().isoformat()


def _uid() -> str:
    return str(uuid.uuid4())[:8]


def _date_str() -> str:
    return datetime.utcnow().strftime('%Y-%m-%d')


# ── CONTEXT NOTES ─────────────────────────────────────────────────────────────
# For anything with detail — full specs, background, how something works, why
# something was built a certain way. No truncation. Stored exactly as given.

def save_context_note(profile_dir: str, topic: str, content: str,
                      project: str = '', tags: str = '') -> str:
    path = _path(profile_dir, 'context_notes.json')
    notes = _load(path, [])

    # Update existing note on same topic if found
    existing = next((n for n in notes
                     if n['topic'].lower() == topic.lower()
                     and n.get('project', '').lower() == project.lower()), None)
    if existing:
        existing['content'] = content
        existing['updated'] = _now()
        existing['tags'] = [t.strip() for t in tags.split(',') if t.strip()] if tags else existing.get('tags', [])
        _save(path, notes)
        return f"Context note '{topic}' updated ({len(content)} chars saved)."

    note = {
        'id': _uid(),
        'topic': topic,
        'project': project,
        'content': content,
        'tags': [t.strip() for t in tags.split(',') if t.strip()] if tags else [],
        'created': _now(),
        'updated': _now()
    }
    notes.append(note)
    _save(path, notes)
    return f"Context note saved: '{topic}' ({len(content)} chars). Retrieve anytime with get_context_note."


def get_context_note(profile_dir: str, topic: str) -> str:
    path = _path(profile_dir, 'context_notes.json')
    notes = _load(path, [])

    # Exact match first, then fuzzy
    match = (
        next((n for n in notes if n['topic'].lower() == topic.lower()), None)
        or next((n for n in notes if topic.lower() in n['topic'].lower()), None)
        or next((n for n in notes if n['id'] == topic), None)
    )
    if not match:
        # Try full-text search across topics
        candidates = [n for n in notes if topic.lower() in n['content'].lower()]
        if candidates:
            lines = [f"No note titled '{topic}'. Found {len(candidates)} note(s) mentioning it:"]
            for n in candidates[:5]:
                lines.append(f"  [{n['id']}] {n['topic']} ({n['updated'][:10]})")
            return '\n'.join(lines)
        return f"No context note found for '{topic}'. Use search_context to search by keyword."

    lines = [
        f"CONTEXT: {match['topic']}",
        f"Project: {match['project']}" if match.get('project') else '',
        f"Updated: {match['updated'][:10]}",
        '',
        match['content']
    ]
    return '\n'.join(l for l in lines if l != '')


def search_context(profile_dir: str, query: str, project: str = '') -> str:
    path = _path(profile_dir, 'context_notes.json')
    notes = _load(path, [])
    q = query.lower()
    results = [
        n for n in notes
        if q in n['topic'].lower()
        or q in n['content'].lower()
        or q in ' '.join(n.get('tags', [])).lower()
        or q in n.get('project', '').lower()
    ]
    if project:
        results = [n for n in results if project.lower() in n.get('project', '').lower()]
    if not results:
        return f"Nothing found for '{query}'."
    lines = [f"Found {len(results)} note(s) matching '{query}':"]
    for n in results[-20:]:
        proj_tag = f" [{n['project']}]" if n.get('project') else ''
        preview = n['content'][:120].replace('\n', ' ')
        lines.append(f"\n  [{n['id']}]{proj_tag} {n['topic']} ({n['updated'][:10]})")
        lines.append(f"    {preview}...")
    return '\n'.join(lines)


def list_context_notes(profile_dir: str, project: str = '') -> str:
    path = _path(profile_dir, 'context_notes.json')
    notes = _load(path, [])
    if project:
        notes = [n for n in notes if project.lower() in n.get('project', '').lower()]
    if not notes:
        return "No context notes saved yet."
    notes_sorted = sorted(notes, key=lambda n: n['updated'], reverse=True)
    lines = [f"Context notes ({len(notes_sorted)}):"]
    for n in notes_sorted:
        proj_tag = f" [{n['project']}]" if n.get('project') else ''
        size = len(n['content'])
        lines.append(f"  [{n['id']}]{proj_tag} {n['topic']} — {size} chars · {n['updated'][:10]}")
    return '\n'.join(lines)


def delete_context_note(profile_dir: str, note_id: str) -> str:
    path = _path(profile_dir, 'context_notes.json')
    notes = _load(path, [])
    before = len(notes)
    notes = [n for n in notes
             if n['id'] != note_id and n['topic'].lower() != note_id.lower()]
    if len(notes) == before:
        return f"Note '{note_id}' not found."
    _save(path, notes)
    return f"Context note deleted."


# ── DECISION LOG ──────────────────────────────────────────────────────────────
# For decisions — what was decided, why, what alternatives were considered.
# This is the answer to "why did we do it that way?"

def log_decision(profile_dir: str, decision: str, reasoning: str,
                 project: str = '', alternatives: str = '') -> str:
    path = _path(profile_dir, 'decisions.json')
    decisions = _load(path, [])
    entry = {
        'id': _uid(),
        'decision': decision,
        'reasoning': reasoning,
        'alternatives': alternatives,
        'project': project,
        'date': _now()
    }
    decisions.append(entry)
    _save(path, decisions)
    proj_tag = f" for '{project}'" if project else ''
    return f"Decision logged{proj_tag}: \"{decision[:80]}{'...' if len(decision) > 80 else ''}\""


def get_decisions(profile_dir: str, project: str = '', query: str = '') -> str:
    path = _path(profile_dir, 'decisions.json')
    decisions = _load(path, [])
    results = decisions
    if project:
        results = [d for d in results if project.lower() in d.get('project', '').lower()]
    if query:
        q = query.lower()
        results = [d for d in results
                   if q in d['decision'].lower()
                   or q in d['reasoning'].lower()
                   or q in d.get('alternatives', '').lower()]
    if not results:
        return "No decisions found."
    lines = [f"Decisions ({len(results)}):"]
    for d in results[-20:]:
        proj_tag = f" [{d['project']}]" if d.get('project') else ''
        lines.append(f"\n[{d['id']}]{proj_tag} {d['date'][:10]}")
        lines.append(f"  DECIDED: {d['decision']}")
        lines.append(f"  WHY: {d['reasoning']}")
        if d.get('alternatives'):
            lines.append(f"  ALTERNATIVES CONSIDERED: {d['alternatives']}")
    return '\n'.join(lines)


# ── SESSION SUMMARIES ─────────────────────────────────────────────────────────
# Complete record of a work session — what was discussed, what was built,
# decisions made, next steps. The thing Frank had to copy a whole conversation for.

def save_session_summary(profile_dir: str, title: str, what_we_did: str,
                         decisions: str = '', next_steps: str = '',
                         project: str = '') -> str:
    path = _path(profile_dir, 'sessions.json')
    sessions = _load(path, [])
    session = {
        'id': _uid(),
        'title': title,
        'project': project,
        'what_we_did': what_we_did,
        'decisions': decisions,
        'next_steps': next_steps,
        'date': _now()
    }
    sessions.append(session)
    _save(path, sessions)
    return (
        f"Session '{title}' saved ({_date_str()}). "
        f"Content: {len(what_we_did)} chars. "
        "Retrieve anytime with get_session or search_sessions."
    )


def get_session(profile_dir: str, session_id: str) -> str:
    path = _path(profile_dir, 'sessions.json')
    sessions = _load(path, [])
    match = (
        next((s for s in sessions if s['id'] == session_id), None)
        or next((s for s in sessions if session_id.lower() in s['title'].lower()), None)
    )
    if not match:
        return f"Session '{session_id}' not found. Use list_sessions to see all sessions."
    lines = [
        f"SESSION: {match['title']}",
        f"Date: {match['date'][:10]}",
        f"Project: {match['project']}" if match.get('project') else '',
        '',
        '── WHAT WE DID ──',
        match['what_we_did'],
    ]
    if match.get('decisions'):
        lines += ['', '── DECISIONS MADE ──', match['decisions']]
    if match.get('next_steps'):
        lines += ['', '── NEXT STEPS ──', match['next_steps']]
    return '\n'.join(l for l in lines if l is not None)


def list_sessions(profile_dir: str, project: str = '', limit: int = 20) -> str:
    path = _path(profile_dir, 'sessions.json')
    sessions = _load(path, [])
    if project:
        sessions = [s for s in sessions if project.lower() in s.get('project', '').lower()]
    if not sessions:
        return "No sessions saved yet."
    sessions_sorted = sorted(sessions, key=lambda s: s['date'], reverse=True)
    lines = [f"Sessions ({len(sessions_sorted)}):"]
    for s in sessions_sorted[:limit]:
        proj_tag = f" [{s['project']}]" if s.get('project') else ''
        size = len(s['what_we_did'])
        lines.append(f"  [{s['id']}]{proj_tag} {s['title']} — {s['date'][:10]} · {size} chars")
    return '\n'.join(lines)


def search_sessions(profile_dir: str, query: str) -> str:
    path = _path(profile_dir, 'sessions.json')
    sessions = _load(path, [])
    q = query.lower()
    results = [
        s for s in sessions
        if q in s['title'].lower()
        or q in s['what_we_did'].lower()
        or q in s.get('decisions', '').lower()
        or q in s.get('next_steps', '').lower()
        or q in s.get('project', '').lower()
    ]
    if not results:
        return f"No sessions found mentioning '{query}'."
    lines = [f"Found {len(results)} session(s) mentioning '{query}':"]
    for s in results[-10:]:
        proj_tag = f" [{s['project']}]" if s.get('project') else ''
        preview = s['what_we_did'][:150].replace('\n', ' ')
        lines.append(f"\n  [{s['id']}]{proj_tag} {s['title']} — {s['date'][:10]}")
        lines.append(f"    {preview}...")
    return '\n'.join(lines)


# ── BUILD SPECS ───────────────────────────────────────────────────────────────
# Structured specs written by Orby that Claude Code can read and build from directly.
# Format is designed to give a developer everything needed without back-and-forth.
#
# File location Claude Code reads: profiles/default/skills/specs.json
# Tell Frank: "just say 'build the [spec name] spec' and I'll find it myself"

def save_spec(profile_dir: str, title: str, purpose: str,
              data_model: str, tools: str, file_path: str,
              behavior_rules: str = '', test_notes: str = '',
              project: str = '') -> str:
    """Write a build-ready spec. Each field is a full prose or structured section."""
    path = _path(profile_dir, 'specs.json')
    specs = _load(path, [])

    existing = next((s for s in specs if s['title'].lower() == title.lower()), None)
    if existing:
        existing.update({
            'purpose': purpose,
            'data_model': data_model,
            'tools': tools,
            'file_path': file_path,
            'behavior_rules': behavior_rules,
            'test_notes': test_notes,
            'project': project,
            'status': 'ready_to_build',
            'updated': _now()
        })
        _save(path, specs)
        return f"Spec '{title}' updated — status: ready_to_build. Tell Frank: say 'build the {title} spec' to Claude Code."

    spec = {
        'id': _uid(),
        'title': title,
        'project': project,
        'purpose': purpose,
        'data_model': data_model,
        'tools': tools,
        'file_path': file_path,
        'behavior_rules': behavior_rules,
        'test_notes': test_notes,
        'status': 'ready_to_build',
        'created': _now(),
        'updated': _now()
    }
    specs.append(spec)
    _save(path, specs)
    return (
        f"Spec '{title}' saved — status: ready_to_build.\n"
        f"Claude Code can build this directly. Just say: 'build the {title} spec'"
    )


def get_spec(profile_dir: str, title: str) -> str:
    path = _path(profile_dir, 'specs.json')
    specs = _load(path, [])
    match = (
        next((s for s in specs if s['title'].lower() == title.lower()), None)
        or next((s for s in specs if title.lower() in s['title'].lower()), None)
        or next((s for s in specs if s['id'] == title), None)
    )
    if not match:
        return f"Spec '{title}' not found. Use list_specs to see all specs."

    lines = [
        f"══ SPEC: {match['title']} ══',",
        f"Status:  {match['status']}",
        f"Project: {match['project']}" if match.get('project') else '',
        f"File:    {match['file_path']}",
        f"Updated: {match['updated'][:10]}",
        '',
        '── PURPOSE ──',
        match['purpose'],
        '',
        '── DATA MODEL ──',
        match['data_model'],
        '',
        '── TOOLS TO BUILD ──',
        match['tools'],
    ]
    if match.get('behavior_rules'):
        lines += ['', '── BEHAVIOR RULES ──', match['behavior_rules']]
    if match.get('test_notes'):
        lines += ['', '── HOW TO TEST ──', match['test_notes']]
    return '\n'.join(l for l in lines if l is not None)


def list_specs(profile_dir: str, project: str = '') -> str:
    path = _path(profile_dir, 'specs.json')
    specs = _load(path, [])
    if project:
        specs = [s for s in specs if project.lower() in s.get('project', '').lower()]
    if not specs:
        return "No specs saved yet. Ask Orby to write one."
    specs_sorted = sorted(specs, key=lambda s: s['updated'], reverse=True)
    lines = [f"Specs ({len(specs_sorted)}):"]
    STATUS_ICON = {'ready_to_build': '✓', 'draft': '○', 'built': '✓✓', 'in_progress': '→'}
    for s in specs_sorted:
        icon = STATUS_ICON.get(s['status'], '?')
        proj_tag = f" [{s['project']}]" if s.get('project') else ''
        lines.append(f"  {icon} [{s['id']}]{proj_tag} {s['title']} — {s['status']} · {s['updated'][:10]}")
    return '\n'.join(lines)


def update_spec_status(profile_dir: str, spec_id: str, status: str) -> str:
    path = _path(profile_dir, 'specs.json')
    specs = _load(path, [])
    match = (
        next((s for s in specs if s['id'] == spec_id), None)
        or next((s for s in specs if s['title'].lower() == spec_id.lower()), None)
        or next((s for s in specs if spec_id.lower() in s['title'].lower()), None)
    )
    if not match:
        return f"Spec '{spec_id}' not found."
    old = match['status']
    match['status'] = status
    match['updated'] = _now()
    _save(path, specs)
    return f"Spec '{match['title']}' status: {old} → {status}."


# ── GLOBAL SEARCH ─────────────────────────────────────────────────────────────

def search_everything(profile_dir: str, query: str) -> str:
    """Search across context notes, decisions, session summaries, and specs in one call."""
    q = query.lower()
    results = []

    # Context notes
    notes_path = _path(profile_dir, 'context_notes.json')
    notes = _load(notes_path, [])
    for n in notes:
        if q in n['topic'].lower() or q in n['content'].lower():
            results.append(('NOTE', n['topic'], n['content'][:120], n['updated'][:10]))

    # Decisions
    dec_path = _path(profile_dir, 'decisions.json')
    decisions = _load(dec_path, [])
    for d in decisions:
        if q in d['decision'].lower() or q in d['reasoning'].lower():
            results.append(('DECISION', d['decision'][:80], d['reasoning'][:120], d['date'][:10]))

    # Sessions
    sess_path = _path(profile_dir, 'sessions.json')
    sessions = _load(sess_path, [])
    for s in sessions:
        if q in s['title'].lower() or q in s['what_we_did'].lower():
            results.append(('SESSION', s['title'], s['what_we_did'][:120], s['date'][:10]))

    # Specs
    specs_path = _path(profile_dir, 'specs.json')
    specs = _load(specs_path, [])
    for s in specs:
        if q in s['title'].lower() or q in s['purpose'].lower() or q in s.get('tools', '').lower():
            results.append(('SPEC', s['title'], s['purpose'][:120], s['updated'][:10]))

    if not results:
        return f"Nothing found for '{query}' across notes, decisions, sessions, or specs."

    lines = [f"Found {len(results)} result(s) for '{query}':"]
    for type_, title, preview, date in results[-20:]:
        lines.append(f"\n  [{type_}] {title} — {date}")
        lines.append(f"    {preview.replace(chr(10), ' ')}...")
    return '\n'.join(lines)


# ── CONTEXT SUMMARY ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    """Brief snapshot for the session context."""
    notes_path = _path(profile_dir, 'context_notes.json')
    sess_path  = _path(profile_dir, 'sessions.json')
    specs_path = _path(profile_dir, 'specs.json')

    notes    = _load(notes_path, [])
    sessions = _load(sess_path, [])
    specs    = _load(specs_path, [])

    parts = []
    if notes:
        recent = sorted(notes, key=lambda n: n['updated'], reverse=True)[:3]
        topics = ', '.join(n['topic'] for n in recent)
        parts.append(f"DEEP MEMORY: {len(notes)} context note(s) — most recent: {topics}")
    if sessions:
        last = sorted(sessions, key=lambda s: s['date'], reverse=True)[0]
        parts.append(f"LAST SESSION: '{last['title']}' on {last['date'][:10]}")
    ready = [s for s in specs if s['status'] == 'ready_to_build']
    if ready:
        names = ', '.join(s['title'] for s in ready)
        parts.append(f"SPECS READY TO BUILD ({len(ready)}): {names} — Claude Code can build these directly")

    return '\n'.join(parts)
