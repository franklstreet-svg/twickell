"""AI Image Studio — generate flat 2D images, illustrations, and artwork using DALL-E 3."""
import json
import uuid
import requests
import os
from pathlib import Path
from datetime import datetime

MODULE_MANIFEST = {
    "id": "P004",
    "name": "AI Image Studio",
    "category": "Creative",
    "description": "Generate professional images, illustrations, logos, and artwork from text descriptions using DALL-E 3. Saves images locally and keeps a searchable project library.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["generate_image", "get_image_library", "delete_image_project", "set_image_api_key", "get_image_api_status"],
    "min_bridge_version": "1.0.0"
}


def set_image_api_key(profile_dir, api_key):
    """Save the OpenAI API key to image_config.json."""
    config_path = Path(profile_dir) / 'creative' / 'image_config.json'
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"openai_api_key": api_key}, indent=2))
    masked = api_key[:7] + '...' if len(api_key) > 7 else api_key
    return f"OpenAI API key saved ({masked}). You're ready to generate images!"


def _get_api_key(profile_dir):
    """Load and return the OpenAI API key, or None if not set."""
    config_path = Path(profile_dir) / 'creative' / 'image_config.json'
    try:
        if config_path.exists():
            data = json.loads(config_path.read_text())
            return data.get('openai_api_key') or None
    except Exception:
        pass
    return None


def get_image_api_status(profile_dir):
    """Return status of the OpenAI API key configuration."""
    key = _get_api_key(profile_dir)
    if key:
        masked = key[:7] + '...'
        return f"OpenAI API key is configured ({masked}). Ready to generate images."
    return (
        "No OpenAI API key set yet. To generate images, you need an OpenAI API key. "
        "Tell Orby: 'Set my image API key to sk-...'"
    )


def _load_library(profile_dir):
    """Load the image library JSON."""
    lib_path = Path(profile_dir) / 'creative' / 'images' / 'library.json'
    try:
        if lib_path.exists():
            return json.loads(lib_path.read_text())
    except Exception:
        pass
    return []


def _save_library(profile_dir, library):
    """Save the image library JSON."""
    lib_path = Path(profile_dir) / 'creative' / 'images' / 'library.json'
    lib_path.parent.mkdir(parents=True, exist_ok=True)
    lib_path.write_text(json.dumps(library, indent=2))


def generate_image(profile_dir, prompt, style='', size='1024x1024', quality='standard'):
    """Generate an image using DALL-E 3 and save it locally."""
    key = _get_api_key(profile_dir)
    if not key:
        return get_image_api_status(profile_dir)

    # Validate size
    valid_sizes = ['1024x1024', '1792x1024', '1024x1792']
    if size not in valid_sizes:
        size = '1024x1024'

    # Validate quality
    if quality not in ('standard', 'hd'):
        quality = 'standard'

    # Build full prompt
    full_prompt = prompt
    if style:
        full_prompt = f"{prompt}, {style} style"

    try:
        import openai
        client = openai.OpenAI(api_key=key)
        response = client.images.generate(
            model="dall-e-3",
            prompt=full_prompt,
            size=size,
            quality=quality,
            n=1
        )
    except Exception as e:
        err = str(e)
        if 'AuthenticationError' in type(e).__name__ or 'authentication' in err.lower() or 'invalid_api_key' in err.lower():
            return "Invalid API key — check your OpenAI API key."
        if 'RateLimitError' in type(e).__name__ or 'rate_limit' in err.lower():
            return "Rate limit hit — try again in a moment."
        return f"Image generation error: {err}"

    image_url = response.data[0].url
    revised_prompt = response.data[0].revised_prompt or full_prompt

    # Download the image
    try:
        img_response = requests.get(image_url, timeout=60)
        img_response.raise_for_status()
    except Exception as e:
        return f"Image generated but could not be downloaded: {e}"

    # Build filename
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    safe_name = ''.join(c if c.isalnum() else '_' for c in prompt[:30])
    safe_name = safe_name.strip('_')
    filename = f"{timestamp}_{safe_name}.png"

    images_dir = Path(profile_dir) / 'creative' / 'images'
    images_dir.mkdir(parents=True, exist_ok=True)
    file_path = images_dir / filename

    file_path.write_bytes(img_response.content)

    # Save to library
    entry = {
        "id": str(uuid.uuid4()),
        "prompt": prompt,
        "revised_prompt": revised_prompt,
        "style": style,
        "size": size,
        "quality": quality,
        "file_path": str(Path('creative') / 'images' / filename),
        "created_at": datetime.utcnow().isoformat()
    }

    library = _load_library(profile_dir)
    library.append(entry)
    _save_library(profile_dir, library)

    return (
        f"Image created and saved!\n"
        f"File: {filename}\n"
        f"Prompt used: {revised_prompt[:100]}..."
    )


def get_image_library(profile_dir, limit=20):
    """Return a formatted list of recent images."""
    library = _load_library(profile_dir)
    if not library:
        return "No images generated yet. Describe what you want and I'll create it!"

    recent = library[-limit:][::-1]
    lines = []
    for entry in recent:
        prompt_preview = entry.get('prompt', '')[:50]
        size = entry.get('size', '')
        quality = entry.get('quality', '')
        created = entry.get('created_at', '')[:10]
        entry_id = entry.get('id', '')
        lines.append(f"• [{entry_id}] {prompt_preview} ({size}, {quality}) — {created}")

    return "\n".join(lines)


def delete_image_project(profile_dir, project_id):
    """Delete an image project by ID."""
    library = _load_library(profile_dir)
    entry = next((e for e in library if e.get('id') == project_id), None)
    if not entry:
        return f"No image project found with ID: {project_id}"

    # Delete file if it exists
    file_path = Path(profile_dir) / entry.get('file_path', '')
    try:
        if file_path.exists():
            file_path.unlink()
    except Exception:
        pass

    library = [e for e in library if e.get('id') != project_id]
    _save_library(profile_dir, library)
    return f"Image project deleted: {entry.get('prompt', '')[:50]}"


def context_summary(profile_dir):
    """Return a brief context summary for the system prompt."""
    library = _load_library(profile_dir)
    count = len(library)
    if count > 0:
        last_prompt = library[-1].get('prompt', '')
        return f"IMAGE STUDIO: {count} image(s) in library. Most recent: '{last_prompt[:40]}...'"
    return ''
