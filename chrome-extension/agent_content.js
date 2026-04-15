// Agent content script: performs actions in the page and extracts page state for the background agent.
(function(){
  const isVisible = (el) => !!(el && el.offsetParent !== null && window.getComputedStyle(el).visibility !== 'hidden' && window.getComputedStyle(el).display !== 'none');
  const dispatchInputEvents = (el) => {
    try { el.dispatchEvent(new Event('focus', { bubbles: true })); } catch(e){}
    try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch(e){}
    try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch(e){}
  };
  const setNativeValue = (el, value) => {
    try {
      const tag = el && el.tagName ? el.tagName.toLowerCase() : '';
      const proto = (tag === 'textarea') ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
      const desc = proto ? Object.getOwnPropertyDescriptor(proto, 'value') : null;
      const setter = desc && desc.set;
      if (setter) setter.call(el, value);
      else el.value = value;
    } catch (e) { try { el.value = value; } catch(_) { el.textContent = value; } }
  };

  async function performAction(action) {
    try {
      if (!action || !action.type) return { status: 'error', error: 'bad_action' };
      if (action.type === 'open') {
        if (action.url) window.location.href = action.url;
        return { status: 'success' };
      }
      if (action.type === 'click') {
        const el = document.querySelector(action.selector || action.sel || 'button, a');
        if (!el) return { status: 'error', error: 'no_element' };
        try { el.scrollIntoView({ block: 'center' }); } catch(e){}
        try { el.click(); } catch(e){ try { el.dispatchEvent(new MouseEvent('click', { bubbles:true })); } catch(_){} }
        return { status: 'success' };
      }
      if (action.type === 'type') {
        const sel = action.selector || action.sel || 'input, textarea, [contenteditable="true"]';
        const el = document.querySelector(sel);
        if (!el) return { status: 'error', error: 'no_editable' };
        if (!isVisible(el)) return { status: 'error', error: 'not_visible' };
        try {
          el.focus();
        } catch(e){}
        setNativeValue(el, action.text || action.value || '');
        dispatchInputEvents(el);
        try { el.setSelectionRange && el.setSelectionRange(String(action.text||'').length, String(action.text||'').length); } catch(e){}
        return { status: 'success' };
      }
      if (action.type === 'extractDOM') {
        const title = document.title || '';
        const url = location.href;
        const body = (document.body && document.body.innerText) ? document.body.innerText.slice(0, 20000) : '';
        const links = Array.from(document.querySelectorAll('a')).slice(0,50).map(a => ({ href: a.href, text: (a.innerText||'').slice(0,200) }));
        const inputs = Array.from(document.querySelectorAll('input,textarea,[contenteditable]')).map(i => ({ selector: i.tagName.toLowerCase() + (i.name ? `[name="${i.name}"]` : ''), value: i.value || i.textContent || '' })).slice(0,50);
        return { status: 'success', title, url, body, links, inputs };
      }
      return { status: 'error', error: 'unknown_action' };
    } catch (e) {
      return { status: 'error', error: e && e.message ? e.message : String(e) };
    }
  }

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (!msg || !msg.type) return false;
    if (msg.type === 'agent-perform') {
      performAction(msg.action).then(res => sendResponse(res)).catch(err => sendResponse({ status: 'error', error: String(err) }));
      return true; // indicate async response
    }
    return false;
  });

  // expose to window for debugging
  try { window.__proactive_agent = { performAction, extract: () => performAction({ type: 'extractDOM' }) }; } catch(e){}
})();
