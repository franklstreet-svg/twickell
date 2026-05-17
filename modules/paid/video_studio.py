"""AI Video Studio — generate short video clips from text or images using Runway ML."""
import json
import uuid
import requests
from pathlib import Path
from datetime import datetime

MODULE_MANIFEST = {
    "id": "P006",
    "name": "AI Video Studio",
    "category": "Creative",
    "description": "Generate short video clips and animations from text descriptions or images using Runway ML Gen-3. Perfect for social media, website backgrounds, and short films.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_video", "check_video_job", "get_video_library", "delete_video_project", "set_video_api_key", "get_video_api_status"],
    "min_bridge_version": "1.0.0"
}


def set_video_api_key(profile_dir, api_key):
    """Save the Runway ML API key to video_config.json."""
    config_path = Path(profile_dir) / 'creative' / 'video_config.json'
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"runway_api_key": api_key}, indent=2))
    masked = api_key[:7] + '...' if len(api_key) > 7 else api_key
    return f"Runway ML API key saved ({masked}). You're ready to generate videos!"


def _get_api_key(profile_dir):
    """Load and return the Runway ML API key, or None if not set."""
    config_path = Path(profile_dir) / 'creative' / 'video_config.json'
    try:
        if config_path.exists():
            data = json.loads(config_path.read_text())
            return data.get('runway_api_key') or None
    except Exception:
        pass
    return None


def get_video_api_status(profile_dir):
    """Return status of the Runway ML API key configuration."""
    key = _get_api_key(profile_dir)
    if key:
        masked = key[:7] + '...'
        return f"Runway ML API key configured ({masked}). Ready to generate videos."
    return (
        "No Runway ML API key set yet. To generate videos, you need a Runway ML API key "
        "(get one at runwayml.com). Tell Orby: 'Set my video API key to your-key-here'"
    )


def _load_library(profile_dir):
    """Load the video library JSON."""
    lib_path = Path(profile_dir) / 'creative' / 'videos' / 'library.json'
    try:
        if lib_path.exists():
            return json.loads(lib_path.read_text())
    except Exception:
        pass
    return []


def _save_library(profile_dir, library):
    """Save the video library JSON."""
    lib_path = Path(profile_dir) / 'creative' / 'videos' / 'library.json'
    lib_path.parent.mkdir(parents=True, exist_ok=True)
    lib_path.write_text(json.dumps(library, indent=2))


def generate_video(profile_dir, prompt, duration=5, ratio='1280:720'):
    """Generate a video clip via Runway ML Gen-3 and save a pending job entry."""
    key = _get_api_key(profile_dir)
    if not key:
        return get_video_api_status(profile_dir)

    # Validate duration
    if duration not in (5, 10):
        duration = 5

    # Validate ratio
    valid_ratios = ['1280:720', '720:1280', '1104:832', '832:1104', '960:960']
    if ratio not in valid_ratios:
        ratio = '1280:720'

    headers = {
        'Authorization': f'Bearer {key}',
        'X-Runway-Version': '2024-11-06',
        'Content-Type': 'application/json'
    }
    body = {
        'promptText': prompt,
        'model': 'gen3a_turbo',
        'duration': duration,
        'ratio': ratio
    }

    try:
        resp = requests.post(
            'https://api.dev.runwayml.com/v1/text_to_video',
            headers=headers,
            json=body,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        task_id = data.get('id', '')
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'unknown'
        if status_code == 401:
            return "Invalid Runway ML API key — please check your key."
        return f"Runway ML API error ({status_code}): {e}"
    except Exception as e:
        return f"Video generation error: {e}"

    # Save pending job to library
    entry = {
        'id': str(uuid.uuid4()),
        'task_id': task_id,
        'prompt': prompt,
        'duration': duration,
        'ratio': ratio,
        'status': 'processing',
        'created_at': datetime.utcnow().isoformat(),
        'file_path': None
    }

    library = _load_library(profile_dir)
    library.append(entry)
    _save_library(profile_dir, library)

    return (
        f"Video generation started!\n"
        f"Job ID: {task_id}\n"
        f"Duration: {duration}s. This takes 2-4 minutes. "
        f"Ask me 'check my video job {task_id}' to see when it's ready."
    )


def check_video_job(profile_dir, task_id):
    """Check the status of a video generation job."""
    key = _get_api_key(profile_dir)
    if not key:
        return get_video_api_status(profile_dir)

    headers = {
        'Authorization': f'Bearer {key}',
        'X-Runway-Version': '2024-11-06',
        'Content-Type': 'application/json'
    }

    try:
        resp = requests.get(
            f'https://api.dev.runwayml.com/v1/tasks/{task_id}',
            headers=headers,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"Error checking video job: {e}"

    status = data.get('status', '').upper()

    if status == 'SUCCEEDED':
        output = data.get('output', [])
        video_url = output[0] if output else None

        saved_filename = None
        if video_url:
            try:
                vid_resp = requests.get(video_url, timeout=120)
                vid_resp.raise_for_status()
                videos_dir = Path(profile_dir) / 'creative' / 'videos'
                videos_dir.mkdir(parents=True, exist_ok=True)
                vid_file = videos_dir / f'{task_id}.mp4'
                vid_file.write_bytes(vid_resp.content)
                saved_filename = f'{task_id}.mp4'
                saved_path = str(Path('creative') / 'videos' / saved_filename)
            except Exception as e:
                return f"Video ready but could not be downloaded: {e}"

            # Update library entry
            library = _load_library(profile_dir)
            for entry in library:
                if entry.get('task_id') == task_id:
                    entry['status'] = 'completed'
                    entry['file_path'] = saved_path
                    break
            _save_library(profile_dir, library)

            return f"Your video is ready! Saved to: {saved_filename}"
        else:
            return "Video succeeded but no output URL found. Check your Runway dashboard."

    elif status == 'FAILED':
        failure_reason = data.get('failure', data.get('failureCode', 'Unknown reason'))

        # Update library entry
        library = _load_library(profile_dir)
        for entry in library:
            if entry.get('task_id') == task_id:
                entry['status'] = 'failed'
                break
        _save_library(profile_dir, library)

        return f"Video generation failed: {failure_reason}"

    else:
        return "Still generating... check back in a couple minutes."


def get_video_library(profile_dir, limit=20):
    """Return a formatted list of recent video projects."""
    library = _load_library(profile_dir)
    if not library:
        return "No videos generated yet. Describe a scene and I'll create a video clip!"

    recent = library[-limit:][::-1]
    lines = []
    for entry in recent:
        prompt_preview = entry.get('prompt', '')[:50]
        duration = entry.get('duration', '')
        ratio = entry.get('ratio', '')
        status = entry.get('status', 'unknown')
        created = entry.get('created_at', '')[:10]
        entry_id = entry.get('id', '')
        lines.append(f"• [{entry_id}] {prompt_preview} ({duration}s, {ratio}, {status}) — {created}")

    return "\n".join(lines)


def delete_video_project(profile_dir, project_id):
    """Delete a video project by ID."""
    library = _load_library(profile_dir)
    entry = next((e for e in library if e.get('id') == project_id), None)
    if not entry:
        return f"No video project found with ID: {project_id}"

    # Delete video file if it exists
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
    return f"Video project deleted: {entry.get('prompt', '')[:50]}"


def context_summary(profile_dir):
    """Return a brief context summary for the system prompt."""
    library = _load_library(profile_dir)
    completed = [e for e in library if e.get('status') == 'completed']
    count = len(completed)
    if count > 0:
        return f"VIDEO STUDIO: {count} video(s) in library."
    return ''
