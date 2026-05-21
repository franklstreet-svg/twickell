/* Orbi AI — Website Controller widget
 * Drop this on any customer site via the embed snippet.
 * Reads window.ORBY_CONFIG; sends chat to the brain authenticated by API key. */
(function () {
  'use strict';

  var CONF       = window.ORBY_CONFIG || {};
  var API_URL    = String(CONF.apiUrl || '').replace(/\/$/, '');
  var CUSTOMER   = String(CONF.customerId || '').trim();
  var API_KEY    = String(CONF.apiKey || '').trim();
  var DEPLOYMENT = String(CONF.deployment || 'website_controller').trim();
  var GREETING   = CONF.greeting || "Hi! I'm Orby. How can I help?";
  var THEME      = CONF.theme || {};
  var ACCENT     = THEME.accent || '#d4a017';
  var ACCENT2    = THEME.accent2 || '#f0c040';
  var BG         = THEME.bg || '#0a0f1e';
  var TEXT       = THEME.text || '#e8eaf0';
  var POSITION   = (CONF.position || 'bottom-right').toLowerCase();

  if (!API_URL || !CUSTOMER || !API_KEY) {
    console.error('[Orbi] ORBY_CONFIG missing apiUrl, customerId, or apiKey — widget will not load.');
    return;
  }

  // ── Session + message storage (per browser tab; cleared on hard refresh) ──
  var LS_SESSION  = 'orbi_b2b_session_' + CUSTOMER;
  var LS_MESSAGES = 'orbi_b2b_messages_' + CUSTOMER;

  // Hard refresh = clear chat history (matches consumer widget behavior).
  try {
    var nav = window.performance && window.performance.getEntriesByType
      ? window.performance.getEntriesByType('navigation') : [];
    if (nav.length && nav[0].type === 'reload') {
      localStorage.removeItem(LS_MESSAGES);
      localStorage.removeItem(LS_SESSION);
    }
  } catch (e) {}

  var _session = (function () {
    try {
      var s = localStorage.getItem(LS_SESSION);
      if (s && /^[a-zA-Z0-9_\-]{1,64}$/.test(s)) return s;
    } catch (e) {}
    var id = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    try { localStorage.setItem(LS_SESSION, id); } catch (e) {}
    return id;
  })();

  function _loadMsgs() {
    try { return JSON.parse(localStorage.getItem(LS_MESSAGES) || '[]'); } catch (e) { return []; }
  }
  function _saveMsgs(arr) {
    try { localStorage.setItem(LS_MESSAGES, JSON.stringify(arr.slice(-60))); } catch (e) {}
  }

  // ── Widget DOM ────────────────────────────────────────────────────────────
  var POS = POSITION === 'bottom-left'
    ? 'left:24px;bottom:24px'
    : 'right:24px;bottom:24px';

  function _injectStyles() {
    var css = ''
      + '.orbi-widget-root *{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}'
      + '.orbi-toggle{background:linear-gradient(135deg,' + ACCENT + ',' + ACCENT2 + ');color:' + BG + ';padding:13px 22px;border-radius:999px;cursor:pointer;font-weight:800;font-size:15px;box-shadow:0 8px 24px rgba(0,0,0,0.25);border:none;display:flex;align-items:center;gap:8px;}'
      + '.orbi-toggle:hover{transform:translateY(-1px);}'
      + '.orbi-window{display:none;width:360px;max-width:calc(100vw - 48px);height:520px;max-height:70vh;background:' + BG + ';color:' + TEXT + ';border-radius:16px;overflow:hidden;box-shadow:0 20px 48px rgba(0,0,0,0.4);margin-bottom:12px;flex-direction:column;border:1px solid rgba(212,160,23,0.3);}'
      + '.orbi-header{background:linear-gradient(135deg,' + ACCENT + ',' + ACCENT2 + ');color:' + BG + ';padding:14px 16px;font-weight:800;display:flex;justify-content:space-between;align-items:center;}'
      + '.orbi-close{background:rgba(10,15,30,0.2);border:none;color:' + BG + ';width:28px;height:28px;border-radius:50%;cursor:pointer;font-size:18px;line-height:1;}'
      + '.orbi-body{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:10px;background:' + BG + ';}'
      + '.orbi-msg{padding:10px 14px;border-radius:14px;max-width:80%;line-height:1.4;font-size:14px;word-wrap:break-word;}'
      + '.orbi-msg.user{align-self:flex-end;background:rgba(212,160,23,0.18);color:' + TEXT + ';}'
      + '.orbi-msg.bot{align-self:flex-start;background:rgba(232,234,240,0.08);color:' + TEXT + ';}'
      + '.orbi-msg.typing{align-self:flex-start;color:rgba(232,234,240,0.5);font-style:italic;}'
      + '.orbi-input-row{display:flex;gap:8px;padding:12px;background:rgba(0,0,0,0.2);border-top:1px solid rgba(212,160,23,0.2);}'
      + '.orbi-input{flex:1;background:rgba(232,234,240,0.06);border:1px solid rgba(212,160,23,0.3);color:' + TEXT + ';padding:10px 12px;border-radius:10px;font-size:14px;outline:none;}'
      + '.orbi-input:focus{border-color:' + ACCENT + ';}'
      + '.orbi-send{background:linear-gradient(135deg,' + ACCENT + ',' + ACCENT2 + ');color:' + BG + ';border:none;padding:0 16px;border-radius:10px;cursor:pointer;font-weight:700;font-size:14px;}'
      + '.orbi-send:disabled{opacity:0.4;cursor:not-allowed;}'
      + '.orbi-window.open{display:flex;}'
      + '@media(max-width:480px){.orbi-window{width:calc(100vw - 32px);height:80vh;}}';
    var style = document.createElement('style');
    style.type = 'text/css';
    style.appendChild(document.createTextNode(css));
    document.head.appendChild(style);
  }

  var root, wrap, toggle, win, body, input, send;

  function _build() {
    _injectStyles();
    root = document.createElement('div');
    root.className = 'orbi-widget-root';
    root.style.cssText = 'position:fixed;' + POS + ';z-index:2147483647;';
    wrap = document.createElement('div');
    wrap.style.cssText = 'display:flex;flex-direction:column;align-items:flex-end;';
    win = document.createElement('div');
    win.className = 'orbi-window';
    win.innerHTML = ''
      + '<div class="orbi-header"><span>Orby</span><button class="orbi-close" type="button" aria-label="Close">×</button></div>'
      + '<div class="orbi-body"></div>'
      + '<div class="orbi-input-row">'
      +   '<input class="orbi-input" type="text" placeholder="Type a message…" maxlength="1000" />'
      +   '<button class="orbi-send" type="button">Send</button>'
      + '</div>';
    toggle = document.createElement('button');
    toggle.className = 'orbi-toggle';
    toggle.type = 'button';
    toggle.innerHTML = '<span>💬</span><span>Chat with Orby</span>';
    wrap.appendChild(win);
    wrap.appendChild(toggle);
    root.appendChild(wrap);
    document.body.appendChild(root);

    body = win.querySelector('.orbi-body');
    input = win.querySelector('.orbi-input');
    send = win.querySelector('.orbi-send');
    var closeBtn = win.querySelector('.orbi-close');

    toggle.addEventListener('click', _toggle);
    closeBtn.addEventListener('click', _toggle);
    send.addEventListener('click', _onSend);
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); _onSend(); }
    });
  }

  function _toggle() {
    var open = win.classList.toggle('open');
    if (open) {
      _renderHistory();
      input.focus();
    }
  }

  function _renderHistory() {
    body.innerHTML = '';
    var msgs = _loadMsgs();
    if (msgs.length === 0) {
      _addBot(GREETING, false);
      _push({ role: 'bot', text: GREETING });
    } else {
      msgs.forEach(function (m) {
        if (m.role === 'user') _addUser(m.text);
        else _addBot(m.text, false);
      });
    }
    body.scrollTop = body.scrollHeight;
  }

  function _push(entry) {
    var arr = _loadMsgs();
    arr.push(entry);
    _saveMsgs(arr);
  }

  function _addUser(text) {
    var div = document.createElement('div');
    div.className = 'orbi-msg user';
    div.textContent = text;
    body.appendChild(div);
    body.scrollTop = body.scrollHeight;
  }

  function _addBot(text, persist) {
    var div = document.createElement('div');
    div.className = 'orbi-msg bot';
    div.textContent = text;
    body.appendChild(div);
    body.scrollTop = body.scrollHeight;
    if (persist) _push({ role: 'bot', text: text });
  }

  function _addTyping() {
    var div = document.createElement('div');
    div.className = 'orbi-msg typing';
    div.id = 'orbi-typing';
    div.textContent = 'Orby is typing…';
    body.appendChild(div);
    body.scrollTop = body.scrollHeight;
    return div;
  }

  function _removeTyping() {
    var t = document.getElementById('orbi-typing');
    if (t) t.remove();
  }

  async function _onSend() {
    var text = (input.value || '').trim();
    if (!text) return;
    input.value = '';
    send.disabled = true;
    _addUser(text);
    _push({ role: 'user', text: text });
    var typing = _addTyping();
    try {
      var resp = await fetch(API_URL + '/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Orbi-API-Key': API_KEY
        },
        body: JSON.stringify({
          customer_id: CUSTOMER,
          deployment: DEPLOYMENT,
          session_id: _session,
          message: text,
          api_key: API_KEY,
          page_url: String(window.location.href || '')
        })
      });
      var data = {};
      try { data = await resp.json(); } catch (e) {}
      _removeTyping();
      if (!resp.ok || !data || data.ok === false) {
        var err = (data && (data.error || data.message)) || 'Something went wrong. Please try again.';
        _addBot(err, true);
      } else {
        var reply = data.reply || data.message || 'Sorry, I didn’t catch that.';
        _addBot(reply, true);
      }
    } catch (e) {
      _removeTyping();
      _addBot('I couldn’t reach the server. Please try again in a moment.', true);
    } finally {
      send.disabled = false;
      input.focus();
    }
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _build);
  } else {
    _build();
  }
})();
