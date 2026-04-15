/**
 * Proactive Browser Agent - Extension Background Worker
 * This script implements a granular Action/Observation protocol
 * for autonomous web interaction.
 */

const NATIVE_HOST_NAME = "com.proactive.browser_agent";
const MAX_SCREENSHOT_DATA_URL_LEN = 650000;
let nativePort = null;
let presenceTimer = null;
let reconnectInterval = null;
let relaySocket = null;
let relayReconnectTimer = null;
let nativeForbiddenUntil = 0;
let relayBackoffMs = 3000;
let relayFailureCount = 0;
let relayPausedUntil = 0;
let lastRelayWarnAt = 0;
const RELAY_BACKOFF_MAX_MS = 120000;
const RELAY_SOFT_PAUSE_MS = 5 * 60 * 1000;

function isRestrictedUrl(url) {
  const lower = String(url || '').toLowerCase();
  return (
    lower.startsWith('chrome://') ||
    lower.startsWith('chrome-extension://') ||
    lower.startsWith('edge://') ||
    lower.startsWith('about:') ||
    lower.startsWith('brave://')
  );
}

function handleHostMessage(msg) {
  console.log("Received action from host:", msg);
  if (msg.type === "EXECUTE_ACTION") {
    handleExecuteAction(msg);
  } else if (msg.type === "execute-task") {
    handleExecuteTask(msg);
  } else if (msg.type === "run-diagnostic") {
    handleRunDiagnostic(msg);
  } else if (msg.type === "APP_BRIDGE_PING") {
    sendToHost({ type: "APP_BRIDGE_PONG", timestamp: Date.now(), extensionId: chrome.runtime.id });
  }
}

function connectToNativeHost() {
  if (Date.now() < nativeForbiddenUntil) return;
  if (nativePort) return;
  console.log("Connecting to native host:", NATIVE_HOST_NAME);
  try {
    nativePort = chrome.runtime.connectNative(NATIVE_HOST_NAME);
    nativeForbiddenUntil = 0;
    startPresenceHeartbeat();

    nativePort.onMessage.addListener((msg) => {
      handleHostMessage(msg);
    });

    nativePort.onDisconnect.addListener(() => {
      const err = chrome.runtime.lastError?.message || '';
      console.warn("Disconnected from native host:", err);
      stopPresenceHeartbeat();
      nativePort = null;
      if (/forbidden/i.test(err)) {
        // Chrome rejected allowed_origins for current extension id.
        nativeForbiddenUntil = Date.now() + (10 * 60 * 1000);
      }
      connectToRelaySocket();
      setTimeout(connectToNativeHost, 5000);
    });
  } catch (e) {
    console.error("Failed to connect to native host:", e);
    connectToRelaySocket();
    setTimeout(connectToNativeHost, 10000);
  }
}

function connectToRelaySocket() {
  if (Date.now() < relayPausedUntil) return;
  if (relaySocket && relaySocket.readyState === WebSocket.OPEN) return;
  if (nativePort) return; // Prefer native host when available.

  try {
    relaySocket = new WebSocket('ws://127.0.0.1:3003');
    relaySocket.onopen = () => {
      console.log("Connected to desktop relay WebSocket fallback");
      relayFailureCount = 0;
      relayBackoffMs = 3000;
      relayPausedUntil = 0;
      startPresenceHeartbeat();
      sendToHost({
        type: "EXTENSION_BRIDGE_STATUS",
        status: "connected",
        timestamp: Date.now(),
        transport: "ws-fallback",
        extensionId: chrome.runtime.id
      });
    };

    relaySocket.onmessage = (event) => {
      try {
        const msg = JSON.parse(String(event.data || '{}'));
        handleHostMessage(msg);
      } catch (e) {
        console.warn("Failed to parse relay message:", e?.message || e);
      }
    };

    relaySocket.onclose = () => {
      if (!nativePort) stopPresenceHeartbeat();
      relaySocket = null;
      relayFailureCount += 1;
      relayBackoffMs = Math.min(RELAY_BACKOFF_MAX_MS, Math.round(relayBackoffMs * 1.6));
      if (relayFailureCount >= 10) {
        relayPausedUntil = Date.now() + RELAY_SOFT_PAUSE_MS;
        if ((Date.now() - lastRelayWarnAt) > 60000) {
          console.warn(`Relay fallback paused for ${Math.round(RELAY_SOFT_PAUSE_MS / 60000)} minutes after repeated connection failures.`);
          lastRelayWarnAt = Date.now();
        }
      }
      scheduleRelayReconnect();
    };

    // Browser logs its own detailed websocket errors; avoid adding noisy duplicate logs here.
    relaySocket.onerror = () => {};
  } catch (e) {
    console.warn("Relay socket connect failed:", e?.message || e);
    relayFailureCount += 1;
    relayBackoffMs = Math.min(RELAY_BACKOFF_MAX_MS, Math.round(relayBackoffMs * 1.6));
    scheduleRelayReconnect();
  }
}

function scheduleRelayReconnect() {
  if (relayReconnectTimer) return;
  const delay = Date.now() < relayPausedUntil
    ? Math.max(3000, relayPausedUntil - Date.now())
    : relayBackoffMs;
  relayReconnectTimer = setTimeout(() => {
    relayReconnectTimer = null;
    if (!nativePort) connectToRelaySocket();
  }, delay);
}

// Initial connection
connectToNativeHost();
ensureReconnectLoop();

chrome.runtime.onInstalled.addListener(() => {
  ensureReconnectLoop();
  connectToNativeHost();
  connectToRelaySocket();
  try { chrome.alarms.create('proactive_native_reconnect', { periodInMinutes: 1 }); } catch (_) {}
});

chrome.runtime.onStartup?.addListener(() => {
  ensureReconnectLoop();
  connectToNativeHost();
  connectToRelaySocket();
  try { chrome.alarms.create('proactive_native_reconnect', { periodInMinutes: 1 }); } catch (_) {}
});

try {
  chrome.alarms.create('proactive_native_reconnect', { periodInMinutes: 1 });
} catch (_) {}

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm?.name !== 'proactive_native_reconnect') return;
  if (!nativePort) {
    connectToNativeHost();
    connectToRelaySocket();
  }
});

function sendToHost(payload) {
  if (nativePort) {
    nativePort.postMessage(payload);
  } else if (relaySocket && relaySocket.readyState === WebSocket.OPEN) {
    relaySocket.send(JSON.stringify(payload));
  } else {
    console.warn("Cannot send to host - not connected:", payload.type);
  }
}

function startPresenceHeartbeat() {
  stopPresenceHeartbeat();
  const transport = nativePort ? 'native-messaging' : (relaySocket && relaySocket.readyState === WebSocket.OPEN ? 'ws-fallback' : 'unknown');
  sendToHost({
    type: "EXTENSION_BRIDGE_STATUS",
    status: "connected",
    timestamp: Date.now(),
    extensionId: chrome.runtime.id,
    transport
  });
  presenceTimer = setInterval(() => {
    const activeTransport = nativePort ? 'native-messaging' : (relaySocket && relaySocket.readyState === WebSocket.OPEN ? 'ws-fallback' : 'unknown');
    sendToHost({
      type: "EXTENSION_BRIDGE_STATUS",
      status: "connected",
      timestamp: Date.now(),
      extensionId: chrome.runtime.id,
      transport: activeTransport
    });
  }, 15000);
}

function stopPresenceHeartbeat() {
  if (presenceTimer) {
    clearInterval(presenceTimer);
    presenceTimer = null;
  }
}

function ensureReconnectLoop() {
  if (reconnectInterval) return;
  reconnectInterval = setInterval(() => {
    if (!nativePort) {
      connectToNativeHost();
      connectToRelaySocket();
    } else if (relaySocket && relaySocket.readyState === WebSocket.OPEN) {
      // Native is healthy; allow fallback socket to close naturally.
      try { relaySocket.close(); } catch (_) {}
    }
  }, 5000);
}

/**
 * Main action router
 */
async function handleExecuteAction(msg) {
  const task_id = msg?.task_id || msg?.taskId;
  const step_id = msg?.step_id ?? msg?.stepId;
  const action = msg?.action || {};

  try {
    // 1. Resolve target tab
    let tab = await getActiveTab();
    if (!tab && action.kind !== 'NAVIGATE') {
      throw new Error("No active tab found and action is not NAVIGATE");
    }
    if (tab && isRestrictedUrl(tab.url) && action.kind !== 'NAVIGATE') {
      throw new Error("Active tab is a restricted browser page. Navigate to a normal website first.");
    }

    let actionResult = {};

    // 2. Execute specific action kind
    switch (action.kind) {
      case "NAVIGATE":
        tab = await navigateTo(action.url);
        actionResult = { kind: "NAVIGATE", url: action.url };
        break;

      case "TYPE":
        await typeInTab(tab.id, action.selector, action.text);
        actionResult = { kind: "TYPE", selector: action.selector };
        break;

      case "CLICK":
        await clickInTab(tab.id, action.selector);
        actionResult = { kind: "CLICK", selector: action.selector };
        break;

      case "DEEP_CLICK":
        {
          const deep = await deepClickInTab(tab.id, action);
          actionResult = { kind: "DEEP_CLICK", ...deep };
        }
        break;

      case "WAIT_VISIBLE":
        await waitForVisible(tab.id, action.selector, action.timeout || 5000);
        actionResult = { kind: "WAIT_VISIBLE", selector: action.selector };
        break;

      case "SCROLL":
        {
          const scrolled = await scrollInTab(tab.id, action);
          actionResult = { kind: "SCROLL", ...scrolled };
        }
        break;

      case "KEY_PRESS":
        {
          const pressed = await keyPressInTab(tab.id, action.key || "Enter");
          actionResult = { kind: "KEY_PRESS", ...pressed };
        }
        break;

      case "FOCUS_TAB":
        await chrome.tabs.update(action.tab_id || tab.id, { active: true });
        actionResult = { kind: "FOCUS_TAB" };
        break;

      case "READ_PAGE_STATE":
        // This is handled by the observation step below
        actionResult = { kind: "READ_PAGE_STATE" };
        break;

      default:
        throw new Error(`Unsupported action kind: ${action.kind}`);
    }

    // 3. Collect observation and send success result
    const observation = await readPageState(tab.id, { includeScreenshot: true, windowId: tab.windowId });
    sendToHost({
      type: "ACTION_RESULT",
      task_id,
      taskId: task_id,
      step_id,
      status: "success",
      action: actionResult,
      observation
    });

  } catch (err) {
    console.error("Action failed:", err);
    // Even on error, try to provide some observation of where we are
    let obs = {};
    try {
      const tab = await getActiveTab();
      if (tab) obs = await readPageState(tab.id, { includeScreenshot: true, windowId: tab.windowId });
    } catch (e) {}

    sendToHost({
      type: "ACTION_RESULT",
      task_id,
      taskId: task_id,
      step_id,
      status: "error",
      error_type: "execution_error",
      message: err.message,
      observation: obs
    });
  }
}

async function handleRunDiagnostic(msg) {
  const requestId = msg.requestId;
  try {
    const tab = await getActiveTab();
    sendToHost({
      type: "run-diagnostic-result",
      requestId,
      result: {
        status: "ok",
        transport: "native-messaging",
        activeTab: tab ? { id: tab.id, url: tab.url || "", title: tab.title || "" } : null,
        timestamp: Date.now()
      }
    });
  } catch (err) {
    sendToHost({
      type: "run-diagnostic-result",
      requestId,
      result: {
        status: "error",
        error: err.message,
        transport: "native-messaging",
        timestamp: Date.now()
      }
    });
  }
}

async function handleExecuteTask(msg) {
  const task = msg?.task || {};
  const taskId = task.id;
  try {
    let tab = await getActiveTab();
    if (task.url) {
      tab = await navigateTo(task.url);
    }
    if (!tab) throw new Error("No active tab available");

    // If draft text exists, type it into the first editable field.
    if (task.draft && String(task.draft).trim()) {
      const selector = await findFirstEditableSelector(tab.id);
      if (selector) {
        await typeInTab(tab.id, selector, String(task.draft));
      }
    }

    const observation = await readPageState(tab.id, { includeScreenshot: true, windowId: tab.windowId });
    sendToHost({
      type: "task-result",
      taskId,
      status: "success",
      result: {
        url: observation?.url || tab.url || "",
        title: observation?.title || tab.title || ""
      }
    });
  } catch (err) {
    sendToHost({
      type: "task-result",
      taskId,
      status: "error",
      error: err.message || String(err)
    });
  }
}

/**
 * DOM Helpers using chrome.scripting
 */

async function navigateTo(url) {
  return new Promise((resolve, reject) => {
    chrome.tabs.create({ url }, (tab) => {
      if (chrome.runtime.lastError) return reject(chrome.runtime.lastError);
      moveTabToAnchorGroup(tab.id, tab.windowId).catch((e) => {
        console.warn('Failed to move tab into Weave group:', e?.message || e);
      });
      
      const listener = (tabId, changeInfo) => {
        if (tabId === tab.id && changeInfo.status === 'complete') {
          chrome.tabs.onUpdated.removeListener(listener);
          resolve(tab);
        }
      };
      chrome.tabs.onUpdated.addListener(listener);
      
      // Fallback timeout for navigation
      setTimeout(() => {
        chrome.tabs.onUpdated.removeListener(listener);
        resolve(tab); 
      }, 10000);
    });
  });
}

async function moveTabToAnchorGroup(tabId, windowId) {
  if (!tabId) return;
  try {
    const existingGroups = await chrome.tabGroups.query({ title: "Weave", windowId });
    if (existingGroups.length > 0) {
      await chrome.tabs.group({ tabIds: tabId, groupId: existingGroups[0].id });
      return;
    }
    const groupId = await chrome.tabs.group({ tabIds: tabId });
    await chrome.tabGroups.update(groupId, { title: "Weave", color: "blue", collapsed: false });
  } catch (e) {
    // Non-fatal; tab grouping is an enhancement.
    console.warn('moveTabToAnchorGroup failed:', e?.message || e);
  }
}

async function typeInTab(tabId, selector, text) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (sel, txt) => {
      const el = document.querySelector(sel);
      if (!el) return { ok: false, error: "selector_not_found" };
      el.focus();
      // Try setting value directly (works for most, but React might need more)
      const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
      const nativeTextAreaValueSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value").set;
      
      if (el.tagName === 'TEXTAREA' && nativeTextAreaValueSetter) {
        nativeTextAreaValueSetter.call(el, txt);
      } else if (nativeInputValueSetter) {
        nativeInputValueSetter.call(el, txt);
      } else {
        el.value = txt;
      }
      
      el.dispatchEvent(new Event("input", { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      return { ok: true };
    },
    args: [selector, text],
  });
  if (!result || !result.ok) throw new Error(result?.error || "Typing failed");
}

async function clickInTab(tabId, selector) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (sel) => {
      const clickNode = (node) => {
        if (!node) return false;
        try { node.scrollIntoView({ block: 'center', inline: 'center' }); } catch (_) {}
        try { node.focus?.(); } catch (_) {}
        const events = ['pointerdown', 'mousedown', 'mouseup', 'click'];
        for (const type of events) {
          try { node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window })); } catch (_) {}
        }
        try { node.click?.(); } catch (_) {}
        return true;
      };

      let el = null;
      if (sel) {
        try { el = document.querySelector(sel); } catch (_) {}
      }
      if (!el) {
        const fallback = Array.from(document.querySelectorAll('button, a[href], [role="button"], [onclick]'))
          .find((node) => {
            const style = window.getComputedStyle(node);
            return style.display !== 'none' && style.visibility !== 'hidden';
          });
        el = fallback || null;
      }

      if (!el) return { ok: false, error: "selector_not_found" };
      clickNode(el);
      return { ok: true };
    },
    args: [selector],
  });
  if (!result || !result.ok) throw new Error(result?.error || "Click failed");
}

async function deepClickInTab(tabId, action) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (input) => {
      const selector = input?.selector || '';
      const text = String(input?.text || '').trim().toLowerCase();
      const index = Math.max(0, Number(input?.index || 0));
      const exact = Boolean(input?.exactText);

      const isVisible = (el) => {
        if (!el) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none' && style.visibility !== 'hidden' && (el.offsetWidth > 0 || el.offsetHeight > 0);
      };
      const clickNode = (node) => {
        try { node.scrollIntoView({ block: 'center', inline: 'center' }); } catch (_) {}
        try { node.focus?.(); } catch (_) {}
        ['pointerdown', 'mousedown', 'mouseup', 'click'].forEach((type) => {
          try { node.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window })); } catch (_) {}
        });
        try { node.click?.(); } catch (_) {}
      };
      const matchesText = (el) => {
        if (!text) return true;
        const hay = `${el.innerText || ''} ${el.getAttribute('aria-label') || ''} ${el.getAttribute('title') || ''}`.toLowerCase().trim();
        return exact ? hay === text : hay.includes(text);
      };

      let candidates = [];
      if (selector) {
        try { candidates = Array.from(document.querySelectorAll(selector)); } catch (_) {}
      }
      if (!candidates.length) {
        candidates = Array.from(document.querySelectorAll('a[href], button, [role="button"], [onclick], [data-testid], [aria-label]'));
      }
      candidates = candidates.filter((el) => isVisible(el) && matchesText(el));
      if (!candidates.length) return { ok: false, error: 'no_click_candidate' };
      const target = candidates[Math.min(index, candidates.length - 1)];
      clickNode(target);
      return {
        ok: true,
        usedSelector: selector || null,
        matchedText: (target.innerText || target.getAttribute('aria-label') || '').trim().slice(0, 120),
        index: Math.min(index, candidates.length - 1)
      };
    },
    args: [action || {}]
  });
  if (!result || !result.ok) throw new Error(result?.error || 'Deep click failed');
  return result;
}

async function scrollInTab(tabId, action) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (input) => {
      const amount = Number(input?.amount || input?.pixels || 700);
      const direction = String(input?.direction || 'down').toLowerCase();
      const selector = input?.selector || null;
      const behavior = input?.smooth ? 'smooth' : 'auto';
      const sign = direction === 'up' ? -1 : 1;
      const delta = Math.abs(amount) * sign;

      if (selector) {
        const el = document.querySelector(selector);
        if (!el) return { ok: false, error: 'selector_not_found' };
        el.scrollBy({ top: delta, behavior });
        return { ok: true, y: el.scrollTop, maxY: el.scrollHeight - el.clientHeight };
      }

      window.scrollBy({ top: delta, behavior });
      return {
        ok: true,
        y: window.scrollY,
        maxY: Math.max(0, (document.documentElement.scrollHeight || document.body.scrollHeight || 0) - window.innerHeight)
      };
    },
    args: [action || {}]
  });
  if (!result || !result.ok) throw new Error(result?.error || 'Scroll failed');
  return result;
}

async function keyPressInTab(tabId, key) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: (k) => {
      const key = String(k || 'Enter');
      const target = document.activeElement || document.body;
      try {
        target.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true }));
        target.dispatchEvent(new KeyboardEvent('keypress', { key, bubbles: true, cancelable: true }));
        target.dispatchEvent(new KeyboardEvent('keyup', { key, bubbles: true, cancelable: true }));
      } catch (_) {}
      if (key === 'PageDown') window.scrollBy({ top: Math.round(window.innerHeight * 0.85), behavior: 'auto' });
      if (key === 'PageUp') window.scrollBy({ top: -Math.round(window.innerHeight * 0.85), behavior: 'auto' });
      if (key === 'End') window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'auto' });
      if (key === 'Home') window.scrollTo({ top: 0, behavior: 'auto' });
      return { ok: true, key };
    },
    args: [key]
  });
  if (!result || !result.ok) throw new Error(result?.error || 'Key press failed');
  return result;
}

async function waitForVisible(tabId, selector, timeoutMs) {
  await chrome.scripting.executeScript({
    target: { tabId },
    func: (sel, timeout) => {
      return new Promise((resolve, reject) => {
        const start = Date.now();
        const poll = () => {
          const el = document.querySelector(sel);
          if (el && (el.offsetWidth > 0 || el.offsetHeight > 0)) {
            resolve(true);
          } else if (Date.now() - start > timeout) {
            reject(new Error("Timeout waiting for element: " + sel));
          } else {
            requestAnimationFrame(poll);
          }
        };
        poll();
      });
    },
    args: [selector, timeoutMs],
  });
}

/**
 * Observation / State Reading
 */
async function captureActionScreenshot(windowId) {
  try {
    const original = await chrome.tabs.captureVisibleTab(windowId, { format: 'jpeg', quality: 45 });
    if (!original) return null;
    if (original.length <= MAX_SCREENSHOT_DATA_URL_LEN) {
      return { data_url: original, mime_type: 'image/jpeg', truncated: false, captured_at: Date.now() };
    }

    if (typeof OffscreenCanvas === 'undefined' || typeof createImageBitmap === 'undefined') {
      return { data_url: '', mime_type: 'image/jpeg', truncated: true, captured_at: Date.now() };
    }

    const sourceBlob = await fetch(original).then((r) => r.blob());
    const bitmap = await createImageBitmap(sourceBlob);
    let scale = Math.min(1, 900 / Math.max(bitmap.width || 1, 1));
    let quality = 0.4;
    let compressed = '';

    for (let i = 0; i < 5; i += 1) {
      const width = Math.max(320, Math.round((bitmap.width || 320) * scale));
      const height = Math.max(200, Math.round((bitmap.height || 200) * scale));
      const canvas = new OffscreenCanvas(width, height);
      const ctx = canvas.getContext('2d', { alpha: false });
      ctx.drawImage(bitmap, 0, 0, width, height);
      const blob = await canvas.convertToBlob({ type: 'image/jpeg', quality });
      const bytes = new Uint8Array(await blob.arrayBuffer());
      let binary = '';
      const chunk = 0x8000;
      for (let k = 0; k < bytes.length; k += chunk) {
        binary += String.fromCharCode(...bytes.subarray(k, k + chunk));
      }
      compressed = `data:image/jpeg;base64,${btoa(binary)}`;
      if (compressed.length <= MAX_SCREENSHOT_DATA_URL_LEN) {
        return { data_url: compressed, mime_type: 'image/jpeg', truncated: false, captured_at: Date.now() };
      }
      scale *= 0.82;
      quality = Math.max(0.2, quality * 0.82);
    }

    return { data_url: '', mime_type: 'image/jpeg', truncated: true, captured_at: Date.now() };
  } catch (e) {
    return { data_url: '', mime_type: 'image/jpeg', error: e?.message || String(e), truncated: true, captured_at: Date.now() };
  }
}

async function readPageState(tabId, options = {}) {
  let result = null;
  try {
    const rows = await chrome.scripting.executeScript({
      target: { tabId },
      func: () => {
        const cssEscape = (value) => {
          const raw = String(value || '');
          if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(raw);
          return raw.replace(/[^a-zA-Z0-9_-]/g, '\\$&');
        };

        const nthPathSelector = (el, maxDepth = 4) => {
          if (!el || !el.tagName) return '';
          const parts = [];
          let node = el;
          let depth = 0;
          while (node && node.nodeType === 1 && node !== document.body && depth < maxDepth) {
            const tag = node.tagName.toLowerCase();
            let idx = 1;
            let sib = node;
            while (sib.previousElementSibling) {
              sib = sib.previousElementSibling;
              if (sib.tagName === node.tagName) idx += 1;
            }
            parts.unshift(`${tag}:nth-of-type(${idx})`);
            node = node.parentElement;
            depth += 1;
          }
          return parts.join(' > ');
        };

        const buildSelector = (el) => {
          if (!el || !el.tagName) return '';
          if (el.id) return `#${cssEscape(el.id)}`;
          const tag = el.tagName.toLowerCase();
          if (el.getAttribute('data-testid')) return `${tag}[data-testid="${cssEscape(el.getAttribute('data-testid'))}"]`;
          if (el.name) return `${tag}[name="${cssEscape(el.name)}"]`;
          if (el.getAttribute('aria-label')) return `${tag}[aria-label="${cssEscape(el.getAttribute('aria-label'))}"]`;
          const classes = Array.from(el.classList || []).filter(Boolean).slice(0, 2);
          if (classes.length) return `${tag}.${classes.map(cssEscape).join('.')}`;
          return nthPathSelector(el) || tag;
        };

        const getInteractiveElements = () => {
          const selectors = [
            'button', 'a[href]', 'input', 'textarea', '[role="button"]', 
            '[role="link"]', '[role="textbox"]', '[contenteditable="true"]'
          ];
          return Array.from(document.querySelectorAll(selectors.join(',')))
            .filter(el => {
              const style = window.getComputedStyle(el);
              return style.display !== 'none' && style.visibility !== 'hidden';
            })
            .map(el => ({
              tag: el.tagName.toLowerCase(),
              text: el.innerText?.trim().slice(0, 50) || el.placeholder || el.getAttribute('aria-label') || '',
              selector: buildSelector(el),
              type: el.type || el.getAttribute('role') || el.tagName.toLowerCase() || 'element',
              name: el.name || '',
              role: el.getAttribute('role') || '',
              placeholder: el.placeholder || '',
              ariaLabel: el.getAttribute('aria-label') || '',
              href: el.href || ''
            }));
        };

        return {
          url: document.location.href,
          title: document.title,
          interactive_elements: getInteractiveElements().slice(0, 50),
          inner_text_sample: document.body.innerText?.slice(0, 1000) || ""
        };
      },
    });
    result = rows?.[0]?.result || null;
  } catch (e) {
    try {
      const tab = await chrome.tabs.get(tabId);
      result = {
        url: tab?.url || '',
        title: tab?.title || '',
        interactive_elements: [],
        inner_text_sample: isRestrictedUrl(tab?.url) ? 'Restricted browser page. Navigate to a normal website to continue.' : ''
      };
    } catch (_) {
      result = { url: '', title: '', interactive_elements: [], inner_text_sample: '' };
    }
  }

  const observation = result || { url: "", title: "", interactive_elements: [] };
  if (options.includeScreenshot) {
    observation.screenshot = await captureActionScreenshot(options.windowId);
  }
  return observation;
}

async function getActiveTab() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  return tabs[0];
}

async function findFirstEditableSelector(tabId) {
  const [{ result }] = await chrome.scripting.executeScript({
    target: { tabId },
    func: () => {
      const cssEscape = (value) => {
        const raw = String(value || '');
        if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(raw);
        return raw.replace(/[^a-zA-Z0-9_-]/g, '\\$&');
      };
      const first = document.querySelector(
        'textarea, input[type="text"], input[type="search"], input:not([type]), [contenteditable="true"]'
      );
      if (!first) return '';
      if (first.id) return `#${cssEscape(first.id)}`;
      if (first.name) return `${first.tagName.toLowerCase()}[name="${cssEscape(first.name)}"]`;
      return first.tagName.toLowerCase();
    }
  });
  return result || '';
}
