import json
import os
from pathlib import Path
from datetime import datetime, timezone
import uuid


def skill_path(profile_dir: str, skill_name: str) -> Path:
    p = Path(profile_dir) / 'skills'
    p.mkdir(parents=True, exist_ok=True)
    return p / f'{skill_name}.json'


def load(profile_dir: str, skill_name: str, default=None):
    path = skill_path(profile_dir, skill_name)
    if not path.exists():
        return default if default is not None else []
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default if default is not None else []


def save(profile_dir: str, skill_name: str, data):
    path = skill_path(profile_dir, skill_name)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
    os.replace(tmp, path)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def new_id() -> str:
    return str(uuid.uuid4())[:8]
