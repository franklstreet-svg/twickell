"""Social Media Manager — Facebook, Instagram, Twitter/X, LinkedIn.
Manages connected accounts, builds profiles, posts content, schedules posts, pulls analytics."""
MODULE_MANIFEST = {
    "id": "P003",
    "name": "Social Media Manager",
    "category": "Marketing",
    "description": "Connect and manage Facebook, Instagram, Twitter/X, and LinkedIn. Create and schedule posts, build page profiles, track analytics, and manage drafts — all from Orby.",
    "version": "1.0.0",
    "tier": "paid",
    "tools": ["connect_social_account", "get_social_accounts", "disconnect_social_account",
              "get_social_setup_guide", "setup_page_profile", "create_post", "schedule_post",
              "get_scheduled_posts", "cancel_scheduled_post", "get_post_history",
              "save_post_draft", "get_post_drafts", "delete_post_draft",
              "get_social_analytics", "get_page_info"],
    "min_bridge_version": "1.0.0"
}
import json
import uuid
import requests
from datetime import datetime, timezone
from pathlib import Path


# ── Storage helpers ───────────────────────────────────────────────────────────

def _path(profile_dir: str, filename: str) -> Path:
    p = Path(profile_dir) / 'skills'
    p.mkdir(parents=True, exist_ok=True)
    return p / filename


def _load(path: Path, default=None):
    if path.exists():
        return json.loads(path.read_text())
    return default if default is not None else {}


def _save(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2))


def _now() -> str:
    return datetime.utcnow().isoformat()


def _uid() -> str:
    return str(uuid.uuid4())[:8]


PLATFORMS = ['facebook', 'instagram', 'twitter', 'linkedin', 'tiktok']

SETUP_GUIDES = {
    'facebook': """HOW TO CONNECT FACEBOOK TO ORBY:

1. Go to developers.facebook.com and log in with your Facebook account
2. Click "My Apps" → "Create App" → choose "Business" → give it any name (e.g. "Orby Manager")
3. In your app dashboard, go to "Tools" → "Graph API Explorer"
4. Click "Generate Access Token" — select your Page from the dropdown
5. Under permissions, add: pages_manage_posts, pages_read_engagement, pages_manage_metadata, pages_show_list
6. Click "Generate Access Token" and copy the token
7. Also copy your Page ID — found in your Facebook Page settings under "About"
8. Tell Orby: "Connect my Facebook with page ID [your page ID] and token [your token]"

Note: This token lasts 60 days. To get a permanent token:
- In Graph API Explorer, click the blue "i" next to your token → "Open in Access Token Tool"
- Click "Extend Access Token" for a long-lived token""",

    'instagram': """HOW TO CONNECT INSTAGRAM TO ORBY:

Instagram uses the same app as Facebook. You need:
- A Facebook Page connected to your Instagram Business/Creator account
- The same Facebook Page Access Token (from Facebook setup above)
- Your Instagram User ID

To get your Instagram User ID:
1. In Graph API Explorer, make sure your Page token is selected
2. Enter this in the query field: /me/accounts
3. Find your Instagram account ID in the results
4. Tell Orby: "Connect my Instagram with user ID [your IG ID] and token [same Facebook token]"

Note: Instagram REQUIRES a Business or Creator account (not personal).
To convert: Instagram app → Settings → Account → Switch to Professional Account""",

    'twitter': """HOW TO CONNECT TWITTER/X TO ORBY:

1. Go to developer.twitter.com and log in
2. Apply for a developer account (usually approved in minutes)
3. Create a new Project and App
4. Go to your App settings → "Keys and Tokens"
5. Under "Authentication Tokens" generate:
   - API Key (copy it)
   - API Key Secret (copy it)
   - Access Token (copy it)
   - Access Token Secret (copy it)
6. Make sure your app has Read AND Write permissions
   (Settings → App Settings → User Authentication Settings → App permissions → Read and Write)
7. Tell Orby: "Connect my Twitter with these credentials: [paste all 4 keys]"

Note: Twitter/X API Basic tier required for posting ($100/month)""",

    'linkedin': """HOW TO CONNECT LINKEDIN TO ORBY:

1. Go to linkedin.com/developers and log in
2. Click "Create App" — fill in app name, your LinkedIn Page, and a logo
3. Under "Products" request access to "Share on LinkedIn" and "Sign In with LinkedIn"
4. Go to "Auth" tab and add a Redirect URL: http://localhost:5000/oauth/linkedin
5. Note your Client ID and Client Secret
6. To get an access token, go to the OAuth 2.0 tools section and generate one with scopes:
   w_member_social, r_liteprofile, r_emailaddress
7. Also get your Person URN from: api.linkedin.com/v2/me (run this in their API tester)
8. Tell Orby: "Connect my LinkedIn with token [token] and person URN [urn]"

Note: LinkedIn access tokens expire in 60 days — you'll need to refresh them""",

    'tiktok': """HOW TO CONNECT TIKTOK TO ORBY:

1. Go to developers.tiktok.com and log in
2. Apply for a developer account
3. Create an app and request "Content Posting API" access (requires approval — can take days)
4. Once approved, generate an access token through their OAuth flow
5. Tell Orby: "Connect my TikTok with token [your token]"

Note: TikTok's Content Posting API requires separate approval and review"""
}


# ── Account management ────────────────────────────────────────────────────────

def _accounts(profile_dir: str) -> dict:
    return _load(_path(profile_dir, 'social_accounts.json'), {})


def _save_accounts(profile_dir: str, accounts: dict) -> None:
    _save(_path(profile_dir, 'social_accounts.json'), accounts)


def connect_account(profile_dir: str, platform: str, credentials: dict) -> str:
    platform = platform.lower()
    if platform not in PLATFORMS:
        return f"Unknown platform '{platform}'. Supported: {', '.join(PLATFORMS)}"
    accounts = _accounts(profile_dir)
    accounts[platform] = {**credentials, 'connected_at': _now()}
    _save_accounts(profile_dir, accounts)
    return f"{platform.title()} connected. Orby can now post, manage, and analyze your {platform.title()} presence."


def get_accounts(profile_dir: str) -> str:
    accounts = _accounts(profile_dir)
    if not accounts:
        return "No social accounts connected yet. Ask Orby how to connect Facebook, Instagram, Twitter, or LinkedIn."
    lines = ["Connected social accounts:"]
    for platform, creds in accounts.items():
        connected = creds.get('connected_at', '')[:10]
        lines.append(f"  ✓ {platform.title()} — connected {connected}")
    return '\n'.join(lines)


def disconnect_account(profile_dir: str, platform: str) -> str:
    accounts = _accounts(profile_dir)
    if platform.lower() not in accounts:
        return f"{platform.title()} is not connected."
    del accounts[platform.lower()]
    _save_accounts(profile_dir, accounts)
    return f"{platform.title()} disconnected."


def get_setup_guide(profile_dir: str, platform: str) -> str:
    platform = platform.lower()
    if platform in SETUP_GUIDES:
        return SETUP_GUIDES[platform]
    return (f"Setup guides available for: {', '.join(SETUP_GUIDES.keys())}. "
            f"Ask for a specific platform, e.g. 'how do I connect Facebook?'")


# ── Platform API calls ────────────────────────────────────────────────────────

def _fb_post(creds: dict, message: str, image_url: str = '') -> dict:
    page_id    = creds.get('page_id', '')
    token      = creds.get('access_token', '')
    if not page_id or not token:
        return {'ok': False, 'error': 'Missing page_id or access_token'}
    url = f"https://graph.facebook.com/v19.0/{page_id}/feed"
    params = {'message': message, 'access_token': token}
    if image_url:
        url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['url'] = image_url
    try:
        r = requests.post(url, data=params, timeout=15)
        data = r.json()
        if 'id' in data:
            return {'ok': True, 'post_id': data['id'],
                    'url': f"https://facebook.com/{data['id']}"}
        return {'ok': False, 'error': data.get('error', {}).get('message', str(data))}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _fb_update_profile(creds: dict, updates: dict) -> dict:
    page_id = creds.get('page_id', '')
    token   = creds.get('access_token', '')
    if not page_id or not token:
        return {'ok': False, 'error': 'Missing page_id or access_token'}
    # Filter to fields Facebook allows updating
    allowed = {'about', 'description', 'website', 'phone', 'emails',
               'location', 'hours', 'name'}
    payload = {k: v for k, v in updates.items() if k in allowed and v}
    payload['access_token'] = token
    try:
        r = requests.post(f"https://graph.facebook.com/v19.0/{page_id}",
                          data=payload, timeout=15)
        data = r.json()
        if data.get('success') or data.get('id'):
            return {'ok': True}
        return {'ok': False, 'error': data.get('error', {}).get('message', str(data))}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _fb_get_info(creds: dict) -> dict:
    page_id = creds.get('page_id', '')
    token   = creds.get('access_token', '')
    fields  = 'name,about,description,website,phone,fan_count,picture,cover'
    try:
        r = requests.get(
            f"https://graph.facebook.com/v19.0/{page_id}",
            params={'fields': fields, 'access_token': token}, timeout=10)
        return r.json()
    except Exception as e:
        return {'error': str(e)}


def _fb_analytics(creds: dict) -> dict:
    page_id = creds.get('page_id', '')
    token   = creds.get('access_token', '')
    metrics = 'page_impressions,page_engaged_users,page_fans,page_views_total'
    try:
        r = requests.get(
            f"https://graph.facebook.com/v19.0/{page_id}/insights",
            params={'metric': metrics, 'period': 'day',
                    'access_token': token}, timeout=10)
        return r.json()
    except Exception as e:
        return {'error': str(e)}


def _ig_post(creds: dict, caption: str, image_url: str = '') -> dict:
    ig_user_id = creds.get('ig_user_id', '')
    token      = creds.get('access_token', '')
    if not ig_user_id or not token:
        return {'ok': False, 'error': 'Missing ig_user_id or access_token'}
    if not image_url:
        return {'ok': False,
                'error': 'Instagram requires an image URL. Text-only posts are not supported.'}
    try:
        # Step 1: create media container
        r = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
            data={'image_url': image_url, 'caption': caption,
                  'access_token': token}, timeout=15)
        data = r.json()
        container_id = data.get('id')
        if not container_id:
            return {'ok': False,
                    'error': data.get('error', {}).get('message', str(data))}
        # Step 2: publish
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
            data={'creation_id': container_id, 'access_token': token},
            timeout=15)
        data2 = r2.json()
        if 'id' in data2:
            return {'ok': True, 'post_id': data2['id']}
        return {'ok': False,
                'error': data2.get('error', {}).get('message', str(data2))}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _tw_post(creds: dict, text: str) -> dict:
    import hmac, hashlib, time, urllib.parse, base64, os
    api_key       = creds.get('api_key', '')
    api_secret    = creds.get('api_secret', '')
    access_token  = creds.get('access_token', '')
    token_secret  = creds.get('access_token_secret', '')
    if not all([api_key, api_secret, access_token, token_secret]):
        return {'ok': False, 'error': 'Missing Twitter credentials. Need: api_key, api_secret, access_token, access_token_secret'}

    url    = 'https://api.twitter.com/2/tweets'
    nonce  = base64.b64encode(os.urandom(16)).decode().rstrip('=')
    ts     = str(int(time.time()))

    oauth_params = {
        'oauth_consumer_key': api_key,
        'oauth_nonce': nonce,
        'oauth_signature_method': 'HMAC-SHA1',
        'oauth_timestamp': ts,
        'oauth_token': access_token,
        'oauth_version': '1.0',
    }

    # Build signature
    param_str = '&'.join(f"{urllib.parse.quote(k,'')}"
                         f"={urllib.parse.quote(v,'')}"
                         for k, v in sorted(oauth_params.items()))
    base_str = ('POST&' + urllib.parse.quote(url, '') + '&'
                + urllib.parse.quote(param_str, ''))
    sign_key = urllib.parse.quote(api_secret, '') + '&' + urllib.parse.quote(token_secret, '')
    sig      = base64.b64encode(
        hmac.new(sign_key.encode(), base_str.encode(), hashlib.sha1).digest()
    ).decode()

    oauth_params['oauth_signature'] = sig
    auth_header = 'OAuth ' + ', '.join(
        f'{urllib.parse.quote(k,"")}="{urllib.parse.quote(v,"")}"'
        for k, v in sorted(oauth_params.items()))

    try:
        r = requests.post(url,
                          headers={'Authorization': auth_header,
                                   'Content-Type': 'application/json'},
                          json={'text': text}, timeout=15)
        data = r.json()
        if data.get('data', {}).get('id'):
            tweet_id = data['data']['id']
            username = creds.get('username', 'me')
            return {'ok': True, 'tweet_id': tweet_id,
                    'url': f"https://twitter.com/{username}/status/{tweet_id}"}
        return {'ok': False, 'error': data.get('detail') or data.get('title') or str(data)}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _li_post(creds: dict, text: str, image_url: str = '') -> dict:
    token      = creds.get('access_token', '')
    person_urn = creds.get('person_urn', '')
    if not token or not person_urn:
        return {'ok': False, 'error': 'Missing LinkedIn access_token or person_urn'}
    payload = {
        'author': person_urn,
        'lifecycleState': 'PUBLISHED',
        'specificContent': {
            'com.linkedin.ugc.ShareContent': {
                'shareCommentary': {'text': text},
                'shareMediaCategory': 'NONE'
            }
        },
        'visibility': {'com.linkedin.ugc.MemberNetworkVisibility': 'PUBLIC'}
    }
    try:
        r = requests.post(
            'https://api.linkedin.com/v2/ugcPosts',
            headers={'Authorization': f'Bearer {token}',
                     'Content-Type': 'application/json',
                     'X-Restli-Protocol-Version': '2.0.0'},
            json=payload, timeout=15)
        if r.status_code in (200, 201):
            return {'ok': True, 'post_id': r.headers.get('X-RestLi-Id', 'created')}
        data = r.json()
        return {'ok': False, 'error': data.get('message', str(data))}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


# ── Public tools ──────────────────────────────────────────────────────────────

def setup_page_profile(profile_dir: str, platform: str,
                       bio: str = '', website: str = '', phone: str = '',
                       email: str = '', hours: str = '',
                       description: str = '') -> str:
    platform = platform.lower()
    accounts = _accounts(profile_dir)
    if platform not in accounts:
        return (f"{platform.title()} is not connected. "
                f"Ask for the setup guide: 'how do I connect {platform}?'")
    creds = accounts[platform]
    updates = {k: v for k, v in {
        'about': bio, 'website': website, 'phone': phone,
        'description': description, 'hours': hours
    }.items() if v}

    if platform == 'facebook':
        result = _fb_update_profile(creds, updates)
        if result['ok']:
            fields = ', '.join(updates.keys())
            return f"Facebook page updated — set: {fields}."
        return f"Facebook update failed: {result['error']}"

    return f"Profile setup for {platform.title()} — saved locally. Full API profile update coming with next update."


def create_post(profile_dir: str, content: str,
                platforms: list = None, image_url: str = '') -> str:
    accounts = _accounts(profile_dir)
    if not accounts:
        return "No social accounts connected. Connect Facebook, Instagram, or Twitter first."

    targets = platforms if platforms else list(accounts.keys())
    targets = [p.lower() for p in targets if p.lower() in accounts]
    if not targets:
        return f"None of those platforms are connected. Connected: {', '.join(accounts.keys())}"

    results = []
    history_path = _path(profile_dir, 'social_posts.json')
    history = _load(history_path, [])

    for platform in targets:
        creds = accounts[platform]
        if platform == 'facebook':
            r = _fb_post(creds, content, image_url)
        elif platform == 'instagram':
            r = _ig_post(creds, content, image_url)
        elif platform == 'twitter':
            r = _tw_post(creds, content)
        elif platform == 'linkedin':
            r = _li_post(creds, content, image_url)
        else:
            r = {'ok': False, 'error': 'posting not yet supported for this platform'}

        if r['ok']:
            results.append(f"  ✓ {platform.title()}" +
                           (f" → {r.get('url', '')}" if r.get('url') else ''))
            history.append({'id': _uid(), 'platform': platform, 'content': content,
                            'image_url': image_url, 'posted_at': _now(),
                            'post_id': r.get('post_id', ''), 'url': r.get('url', '')})
        else:
            results.append(f"  ✗ {platform.title()}: {r['error']}")

    _save(history_path, history)
    return "Posted:\n" + '\n'.join(results)


def schedule_post(profile_dir: str, content: str, publish_at: str,
                  platforms: list = None, image_url: str = '') -> str:
    accounts = _accounts(profile_dir)
    targets = platforms if platforms else list(accounts.keys())
    targets = [p.lower() for p in targets if p.lower() in accounts]
    if not targets:
        return "No connected platforms to schedule for."

    path = _path(profile_dir, 'social_scheduled.json')
    scheduled = _load(path, [])
    post = {
        'id': _uid(),
        'content': content,
        'platforms': targets,
        'image_url': image_url,
        'publish_at': publish_at,
        'created_at': _now(),
        'status': 'scheduled'
    }
    scheduled.append(post)
    _save(path, scheduled)

    try:
        dt = datetime.fromisoformat(publish_at.replace('Z', '+00:00'))
        friendly = dt.strftime('%B %d at %I:%M %p')
    except Exception:
        friendly = publish_at

    return (f"Scheduled for {friendly} on {', '.join(p.title() for p in targets)}.\n"
            f"Post ID: {post['id']}\n"
            f"Preview: {content[:100]}{'...' if len(content) > 100 else ''}")


def get_scheduled_posts(profile_dir: str) -> str:
    path = _path(profile_dir, 'social_scheduled.json')
    scheduled = _load(path, [])
    pending = [p for p in scheduled if p['status'] == 'scheduled']
    if not pending:
        return "No scheduled posts."
    lines = [f"Scheduled posts ({len(pending)}):"]
    for p in sorted(pending, key=lambda x: x['publish_at']):
        platforms = ', '.join(pt.title() for pt in p['platforms'])
        lines.append(f"\n  [{p['id']}] {p['publish_at'][:16]} → {platforms}")
        lines.append(f"    {p['content'][:100]}{'...' if len(p['content']) > 100 else ''}")
    return '\n'.join(lines)


def cancel_scheduled_post(profile_dir: str, post_id: str) -> str:
    path = _path(profile_dir, 'social_scheduled.json')
    scheduled = _load(path, [])
    post = next((p for p in scheduled if p['id'] == post_id), None)
    if not post:
        return f"Scheduled post '{post_id}' not found."
    post['status'] = 'cancelled'
    _save(path, scheduled)
    return f"Cancelled: '{post['content'][:60]}...'"


def check_and_publish_scheduled(profile_dir: str) -> list:
    """Called by health_monitor every 5 min — publishes due posts. Returns list of actions."""
    path = _path(profile_dir, 'social_scheduled.json')
    scheduled = _load(path, [])
    now = datetime.utcnow().isoformat()
    published = []
    for post in scheduled:
        if post['status'] == 'scheduled' and post['publish_at'] <= now:
            result = create_post(profile_dir, post['content'],
                                 post['platforms'], post.get('image_url', ''))
            post['status'] = 'published'
            post['published_at'] = now
            published.append(f"Auto-published: {post['content'][:50]}")
    if published:
        _save(path, scheduled)
    return published


def get_post_history(profile_dir: str, platform: str = '', limit: int = 10) -> str:
    path = _path(profile_dir, 'social_posts.json')
    history = _load(path, [])
    if platform:
        history = [p for p in history if p['platform'] == platform.lower()]
    if not history:
        return "No post history yet."
    recent = sorted(history, key=lambda p: p['posted_at'], reverse=True)[:limit]
    lines = [f"Post history ({len(history)} total):"]
    for p in recent:
        lines.append(f"\n  [{p['platform'].title()}] {p['posted_at'][:10]}")
        lines.append(f"    {p['content'][:100]}{'...' if len(p['content']) > 100 else ''}")
        if p.get('url'):
            lines.append(f"    {p['url']}")
    return '\n'.join(lines)


def save_draft(profile_dir: str, content: str,
               platforms: list = None, notes: str = '') -> str:
    path = _path(profile_dir, 'social_drafts.json')
    drafts = _load(path, [])
    draft = {
        'id': _uid(),
        'content': content,
        'platforms': platforms or [],
        'notes': notes,
        'created_at': _now()
    }
    drafts.append(draft)
    _save(path, drafts)
    return f"Draft saved (id: {draft['id']}): \"{content[:80]}{'...' if len(content) > 80 else ''}\""


def get_drafts(profile_dir: str) -> str:
    path = _path(profile_dir, 'social_drafts.json')
    drafts = _load(path, [])
    if not drafts:
        return "No drafts saved."
    lines = [f"Drafts ({len(drafts)}):"]
    for d in drafts:
        platforms = ', '.join(d['platforms']) if d['platforms'] else 'no platform set'
        lines.append(f"\n  [{d['id']}] {d['created_at'][:10]} → {platforms}")
        lines.append(f"    {d['content'][:120]}{'...' if len(d['content']) > 120 else ''}")
        if d.get('notes'):
            lines.append(f"    Note: {d['notes']}")
    return '\n'.join(lines)


def delete_draft(profile_dir: str, draft_id: str) -> str:
    path = _path(profile_dir, 'social_drafts.json')
    drafts = _load(path, [])
    before = len(drafts)
    drafts = [d for d in drafts if d['id'] != draft_id]
    if len(drafts) == before:
        return f"Draft '{draft_id}' not found."
    _save(path, drafts)
    return "Draft deleted."


def get_analytics(profile_dir: str, platform: str) -> str:
    platform = platform.lower()
    accounts = _accounts(profile_dir)
    if platform not in accounts:
        return f"{platform.title()} is not connected."
    creds = accounts[platform]
    if platform == 'facebook':
        data = _fb_analytics(creds)
        if 'error' in data:
            return f"Couldn't pull analytics: {data['error']}"
        info = _fb_get_info(creds)
        lines = [f"Facebook analytics for {info.get('name', 'your page')}:"]
        fans = info.get('fan_count', 'unknown')
        lines.append(f"  Followers: {fans:,}" if isinstance(fans, int) else f"  Followers: {fans}")
        if 'data' in data:
            for item in data['data']:
                name = item.get('name', '').replace('page_', '').replace('_', ' ').title()
                value = item.get('values', [{}])[-1].get('value', 0)
                lines.append(f"  {name}: {value:,}" if isinstance(value, int) else f"  {name}: {value}")
        return '\n'.join(lines)
    return f"Analytics for {platform.title()} — connect the account first and use the platform's native dashboard for now."


def get_page_info(profile_dir: str, platform: str) -> str:
    platform = platform.lower()
    accounts = _accounts(profile_dir)
    if platform not in accounts:
        return f"{platform.title()} is not connected."
    creds = accounts[platform]
    if platform == 'facebook':
        data = _fb_get_info(creds)
        if 'error' in data:
            return f"Couldn't fetch page info: {data['error']}"
        lines = [f"Facebook Page: {data.get('name', 'unknown')}"]
        for field in ['about', 'description', 'website', 'phone']:
            if data.get(field):
                lines.append(f"  {field.title()}: {data[field]}")
        if data.get('fan_count'):
            lines.append(f"  Followers: {data['fan_count']:,}")
        return '\n'.join(lines)
    return f"{platform.title()} page info — coming in next update."


# ── Context summary ───────────────────────────────────────────────────────────

def context_summary(profile_dir: str) -> str:
    accounts = _accounts(profile_dir)
    if not accounts:
        return ''
    path = _path(profile_dir, 'social_scheduled.json')
    scheduled = _load(path, [])
    now = datetime.utcnow().isoformat()
    pending = [p for p in scheduled if p['status'] == 'scheduled']
    overdue = [p for p in pending if p['publish_at'] <= now]

    parts = [f"SOCIAL MEDIA: {len(accounts)} account(s) connected — {', '.join(p.title() for p in accounts)}"]
    if overdue:
        parts.append(f"  {len(overdue)} post(s) due to publish NOW — call check_scheduled_posts")
    elif pending:
        next_post = sorted(pending, key=lambda p: p['publish_at'])[0]
        parts.append(f"  Next scheduled post: {next_post['publish_at'][:16]}")
    return '\n'.join(parts)
