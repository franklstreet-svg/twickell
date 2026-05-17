"""3D Creator — generate 3D models and rendered images from text using Meshy.ai."""
import json
import uuid
import requests
from pathlib import Path
from datetime import datetime

MODULE_MANIFEST = {
    "id": "P005",
    "name": "3D Creator",
    "category": "Creative",
    "description": "Generate 3D models and photorealistic 3D renders from text descriptions using Meshy.ai. Saves preview renders locally with downloadable model files.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_3d", "get_3d_library", "check_3d_job", "delete_3d_project", "set_3d_api_key", "get_3d_api_status"],
    "min_bridge_version": "1.0.0"
}


def set_3d_api_key(profile_dir, api_key):
    """Save the Meshy.ai API key to 3d_config.json."""
    config_path = Path(profile_dir) / 'creative' / '3d_config.json'
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"meshy_api_key": api_key}, indent=2))
    masked = api_key[:7] + '...' if len(api_key) > 7 else api_key
    return f"Meshy.ai API key saved ({masked}). You're ready to generate 3D models!"


def _get_api_key(profile_dir):
    """Load and return the Meshy.ai API key, or None if not set."""
    config_path = Path(profile_dir) / 'creative' / '3d_config.json'
    try:
        if config_path.exists():
            data = json.loads(config_path.read_text())
            return data.get('meshy_api_key') or None
    except Exception:
        pass
    return None


def get_3d_api_status(profile_dir):
    """Return status of the Meshy.ai API key configuration."""
    key = _get_api_key(profile_dir)
    if key:
        masked = key[:7] + '...'
        return f"Meshy.ai API key configured ({masked}). Ready to generate 3D models."
    return (
        "No Meshy.ai API key set yet. To generate 3D models, you need a Meshy.ai API key "
        "(free tier available at meshy.ai). Tell Orby: 'Set my 3D API key to your-key-here'"
    )


def _load_library(profile_dir):
    """Load the 3D library JSON."""
    lib_path = Path(profile_dir) / 'creative' / '3d' / 'library.json'
    try:
        if lib_path.exists():
            return json.loads(lib_path.read_text())
    except Exception:
        pass
    return []


def _save_library(profile_dir, library):
    """Save the 3D library JSON."""
    lib_path = Path(profile_dir) / 'creative' / '3d' / 'library.json'
    lib_path.parent.mkdir(parents=True, exist_ok=True)
    lib_path.write_text(json.dumps(library, indent=2))


def generate_3d(profile_dir, prompt, art_style='realistic', negative_prompt=''):
    """Generate a 3D model via Meshy.ai and save a pending job entry."""
    key = _get_api_key(profile_dir)
    if not key:
        return get_3d_api_status(profile_dir)

    valid_styles = ['realistic', 'cartoon', 'low-poly', 'sculpture', 'pbr']
    if art_style not in valid_styles:
        art_style = 'realistic'

    headers = {
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json'
    }
    body = {
        'mode': 'preview',
        'prompt': prompt,
        'art_style': art_style,
        'negative_prompt': negative_prompt
    }

    try:
        resp = requests.post(
            'https://api.meshy.ai/v2/text-to-3d',
            headers=headers,
            json=body,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        task_id = data.get('result') or data.get('task_id') or data.get('id', '')
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'unknown'
        if status_code == 401:
            return "Invalid Meshy.ai API key — please check your key."
        return f"Meshy.ai API error ({status_code}): {e}"
    except Exception as e:
        return f"3D generation error: {e}"

    # Save pending job to library
    entry = {
        'id': str(uuid.uuid4()),
        'task_id': task_id,
        'prompt': prompt,
        'art_style': art_style,
        'status': 'processing',
        'created_at': datetime.utcnow().isoformat(),
        'file_path': None
    }

    library = _load_library(profile_dir)
    library.append(entry)
    _save_library(profile_dir, library)

    return (
        f"3D model generation started!\n"
        f"Job ID: {task_id}\n"
        f"This takes 1-3 minutes. Ask me 'check my 3D job {task_id}' to see if it's ready."
    )


def check_3d_job(profile_dir, task_id):
    """Check the status of a 3D generation job."""
    key = _get_api_key(profile_dir)
    if not key:
        return get_3d_api_status(profile_dir)

    headers = {
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json'
    }

    try:
        resp = requests.get(
            f'https://api.meshy.ai/v2/text-to-3d/{task_id}',
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"Error checking 3D job: {e}"

    status = data.get('status', '').upper()

    if status == 'SUCCEEDED':
        # Download the thumbnail preview
        thumbnail_url = data.get('thumbnail_url', '')
        saved_path = None
        if thumbnail_url:
            try:
                img_resp = requests.get(thumbnail_url, timeout=60)
                img_resp.raise_for_status()
                preview_dir = Path(profile_dir) / 'creative' / '3d'
                preview_dir.mkdir(parents=True, exist_ok=True)
                preview_file = preview_dir / f'{task_id}_preview.png'
                preview_file.write_bytes(img_resp.content)
                saved_path = str(Path('creative') / '3d' / f'{task_id}_preview.png')
            except Exception:
                pass

        # Update library entry
        library = _load_library(profile_dir)
        for entry in library:
            if entry.get('task_id') == task_id:
                entry['status'] = 'completed'
                entry['file_path'] = saved_path
                break
        _save_library(profile_dir, library)

        model_urls = data.get('model_urls', {})
        model_info = ', '.join(f"{k}: {v}" for k, v in model_urls.items()) if model_urls else 'See Meshy dashboard'
        return f"Your 3D model is ready! Preview saved. Model files: {model_info}"

    elif status in ('FAILED', 'EXPIRED'):
        # Update library entry
        library = _load_library(profile_dir)
        for entry in library:
            if entry.get('task_id') == task_id:
                entry['status'] = status.lower()
                break
        _save_library(profile_dir, library)
        return "3D generation failed. Try a different prompt or style."

    else:
        return "Still processing... check back in a minute."


def get_3d_library(profile_dir, limit=20):
    """Return a formatted list of recent 3D projects."""
    library = _load_library(profile_dir)
    if not library:
        return "No 3D models yet. Describe an object or scene and I'll generate it!"

    recent = library[-limit:][::-1]
    lines = []
    for entry in recent:
        prompt_preview = entry.get('prompt', '')[:50]
        art_style = entry.get('art_style', '')
        status = entry.get('status', 'unknown')
        created = entry.get('created_at', '')[:10]
        entry_id = entry.get('id', '')
        lines.append(f"• [{entry_id}] {prompt_preview} ({art_style}, {status}) — {created}")

    return "\n".join(lines)


def delete_3d_project(profile_dir, project_id):
    """Delete a 3D project by ID."""
    library = _load_library(profile_dir)
    entry = next((e for e in library if e.get('id') == project_id), None)
    if not entry:
        return f"No 3D project found with ID: {project_id}"

    # Delete preview file if it exists
    file_path_rel = entry.get('file_path')
    if file_path_rel:
        file_path = Path(profile_dir) / file_path_rel
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            pass

    library = [e for e in library if e.get('id') != project_id]
    _save_library(profile_dir, library)
    return f"3D project deleted: {entry.get('prompt', '')[:50]}"


def context_summary(profile_dir):
    """Return a brief context summary for the system prompt."""
    library = _load_library(profile_dir)
    completed = [e for e in library if e.get('status') == 'completed']
    count = len(completed)
    if count > 0:
        return f"3D CREATOR: {count} model(s) in library."
    return ''
