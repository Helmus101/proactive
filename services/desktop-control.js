const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { execFile } = require('child_process');
const path = require('path');
const Store = require('electron-store').default || require('electron-store');
const db = require('./db');

let app;
try {
  app = require('electron').app;
} catch (e) {
  app = null;
}

const SCREENSHOT_COOLDOWN_MS = 2500;
const BURST_COOLDOWN_MS = 6000;
const WINDOW_CLIP_COOLDOWN_MS = 12000;
const BURST_FRAME_INTERVAL_MS = 420;
const WINDOW_CLIP_FRAME_INTERVAL_MS = 700;
const MAX_SCREENSHOT_BASE64 = 450000;
let lastScreenshotAt = 0;
let lastBurstAt = 0;
let lastWindowClipAt = 0;

let store;
try {
  store = new Store();
} catch (e) {
  // Store not available (e.g., in non-Electron context); initialize on-demand later
  store = null;
}

// Get screenshots directory path
function getScreenshotsDir() {
  const screenshotRoot = process.env.PROACTIVE_SCREENSHOTS_DIR || path.join(os.homedir(), '.proactive');
  const screenshotsDir = path.join(screenshotRoot, 'screenshots');
  if (!fs.existsSync(screenshotsDir)) {
    try {
      fs.mkdirSync(screenshotsDir, { recursive: true });
    } catch (e) {
      // Silently fail if we can't create the directory
    }
  }
  return screenshotsDir;
}

function readJsonFile(filePath) {
  return fs.promises.readFile(filePath, 'utf8').then((raw) => JSON.parse(raw));
}

function runCommand(cmd, args = [], options = {}) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, {
      timeout: options.timeout || 15000,
      env: {
        ...process.env,
        ...(options.env || {})
      }
    }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error((stderr || error.message || '').toString().trim() || 'Command failed'));
        return;
      }
      resolve((stdout || '').toString());
    });
  });
}

// Check if the screen is on/awake on macOS
async function isScreenOn() {
  if (process.platform !== 'darwin') return true; // Assume screen is on for non-macOS
  try {
    // Quick check: use pmset to see if display is in sleep/off state
    // This is much faster than ioreg
    const output = await runCommand('pmset', ['-g'], { timeout: 3000 });
    // If we get output without timeout, system is responsive (screen likely on)
    return true;
  } catch (error) {
    // If command fails or times out, assume screen is on (fail-safe)
    // This prevents screenshots from being blocked by screen detection
    return true;
  }
}

// Save OCR/extracted text to electron-store and database memory
async function saveOCRResults(ocrText, metadata = {}) {
  if (!store) {
    try {
      store = new Store();
    } catch (e) {
      // Still not available; silently fail
    }
  }

  try {
    // Save to electron-store
    const ocrHistory = store ? (store.get('ocr_history') || []) : [];
    const record = {
      timestamp: new Date().toISOString(),
      text: String(ocrText || ''), // Save full OCR text
      app: metadata.app || '',
      window_title: metadata.window_title || '',
      text_length: String(ocrText || '').length,
      screenshot_file: metadata.screenshot_file || '',
      ...metadata
    };
    ocrHistory.push(record);
    // Keep last 100 OCR results
    if (ocrHistory.length > 100) {
      ocrHistory.shift();
    }
    if (store) {
      store.set('ocr_history', ocrHistory);
    }

    // Also save to database memory as a screen_ocr event
    if (db) {
      try {
        const eventId = `ocr-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;
        const eventMetadata = {
          app: metadata.app || '',
          window_title: metadata.window_title || '',
          screenshot_file: metadata.screenshot_file || '',
          vision_mode: metadata.vision_mode || 'ax_only',
          screenshot_present: metadata.screenshot_present || false
        };
        await db.runQuery(
          `INSERT OR IGNORE INTO events (id, type, timestamp, source, text, metadata) VALUES (?, ?, ?, ?, ?, ?)`,
          [eventId, 'screen_ocr', new Date().toISOString(), 'desktop_vision', String(ocrText || ''), JSON.stringify(eventMetadata)]
        ).catch(() => {}); // Silently fail if DB write fails
      } catch (e) {
        // Silently fail
      }
    }
  } catch (error) {
    // Silently fail if store/db write fails
  }
}

// Run OCR on a screenshot image using the Swift script
async function runOCR(imagePath) {
  return new Promise((resolve, reject) => {
    const scriptPath = path.join(__dirname, '..', 'ocr_vision.swift');
    execFile('swift', [scriptPath, imagePath], { timeout: 30000 }, (error, stdout, stderr) => {
      if (error) {
        console.warn('[OCR] Swift OCR failed:', error.message);
        resolve(null); // Return null on failure, don't reject
        return;
      }
      try {
        const result = JSON.parse(stdout);
        resolve(result);
      } catch (parseError) {
        console.warn('[OCR] Failed to parse OCR result:', parseError.message);
        resolve(null);
      }
    });
  });
}

function appleScriptString(value) {
  return `"${String(value || '')
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\r/g, ' ')
    .replace(/\n/g, '\\n')}"`;
}

function parseLines(raw = '') {
  return String(raw || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

function normalizeKey(key) {
  const value = String(key || '').toLowerCase().trim();
  const keyCodes = {
    return: 36,
    enter: 36,
    tab: 48,
    space: 49,
    escape: 53,
    esc: 53,
    left: 123,
    right: 124,
    down: 125,
    up: 126,
    pagedown: 121,
    page_down: 121,
    pageup: 116,
    page_up: 116
  };
  return { key: value, keyCode: keyCodes[value] || null };
}

function modifiersClause(modifiers = []) {
  const normalized = (Array.isArray(modifiers) ? modifiers : [])
    .map((item) => String(item || '').toLowerCase().trim())
    .filter(Boolean)
    .map((item) => {
      if (item === 'cmd' || item === 'command') return 'command down';
      if (item === 'ctrl' || item === 'control') return 'control down';
      if (item === 'shift') return 'shift down';
      if (item === 'option' || item === 'alt') return 'option down';
      return null;
    })
    .filter(Boolean);
  return normalized.length ? ` using {${normalized.join(', ')}}` : '';
}

async function runOsascript(lines = [], { language = 'AppleScript', timeout = 20000 } = {}) {
  const args = [];
  if (language && language !== 'AppleScript') {
    args.push('-l', language);
  }
  for (const line of lines) {
    args.push('-e', line);
  }
  return runCommand('/usr/bin/osascript', args, { timeout });
}

async function checkAccessibilityPermission() {
  try {
    const output = await runOsascript([
      'tell application "System Events"',
      'return UI elements enabled',
      'end tell'
    ], { timeout: 5000 });
    return {
      trusted: /true/i.test(String(output || '').trim()),
      status: /true/i.test(String(output || '').trim()) ? 'trusted' : 'not_trusted'
    };
  } catch (error) {
    return {
      trusted: false,
      status: 'not_trusted',
      error: error.message
    };
  }
}

async function runSwiftAxOperator(mode, payload = null, timeout = 20000) {
  const scriptPath = path.join(__dirname, '..', 'ax_operator.swift');
  const swiftEnv = {
    CLANG_MODULE_CACHE_PATH: '/tmp/swift-module-cache',
    SWIFT_MODULECACHE_PATH: '/tmp/swift-module-cache'
  };
  if (mode === 'snapshot') {
    const stdout = await runCommand('/usr/bin/xcrun', ['swift', scriptPath, 'snapshot'], { timeout, env: swiftEnv });
    return JSON.parse(stdout.toString());
  }

  const filePath = path.join(os.tmpdir(), `weave-ax-action-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.json`);
  await fs.promises.writeFile(filePath, JSON.stringify(payload || {}), 'utf8');
  try {
    const stdout = await runCommand('/usr/bin/xcrun', ['swift', scriptPath, 'action', filePath], { timeout, env: swiftEnv });
    return JSON.parse(stdout.toString());
  } finally {
    fs.promises.unlink(filePath).catch(() => {});
  }
}

async function getFrontmostWindowContext() {
  if (process.platform !== 'darwin') {
    return {
      appName: '',
      windowTitle: '',
      extractedText: '',
      windowId: null,
      bounds: null,
      status: 'unsupported_platform'
    };
  }

  try {
    const parsed = await runSwiftAxOperator('snapshot', null, 20000);
    return {
      appName: parsed.frontmost_app || '',
      windowTitle: parsed.window_title || '',
      extractedText: parsed.text_sample || '',
      windowId: parsed.window_id || null,
      bounds: parsed.bounds || null,
      axTree: parsed.ax_tree || null,
      focusedElementId: parsed.focused_element_id || null,
      status: parsed.status || 'empty'
    };
  } catch (error) {
    return {
      appName: '',
      windowTitle: '',
      extractedText: '',
      windowId: null,
      bounds: null,
      status: 'unavailable',
      error: error.message
    };
  }
}

function flattenAxTree(node, out = [], depth = 0) {
  if (!node || typeof node !== 'object') return out;
  out.push({
    id: node.id || null,
    index: out.length,
    depth,
    role: node.role || '',
    name: node.title || '',
    description: node.description || '',
    value: node.value || '',
    identifier: node.identifier || '',
    frame: node.frame || null,
    enabled: node.enabled !== false,
    focused: Boolean(node.focused),
    selected: Boolean(node.selected),
    actions: Array.isArray(node.actions) ? node.actions : [],
    hint: `${out.length + 1}. ${node.role || 'element'} ${node.title || node.description || node.value || node.identifier || ''}`.trim()
  });
  if (Array.isArray(node.children)) {
    node.children.forEach((child) => flattenAxTree(child, out, depth + 1));
  }
  return out;
}

async function getVisibleElements() {
  try {
    const snapshot = await runSwiftAxOperator('snapshot', null, 20000);
    return flattenAxTree(snapshot.ax_tree || null).slice(0, 120);
  } catch (_) {
    return [];
  }
}

function classifySurface({ appName = '', windowTitle = '', extractedText = '', visibleElements = [] } = {}) {
  const app = String(appName || '').toLowerCase();
  const title = String(windowTitle || '').toLowerCase();
  const text = String(extractedText || '').toLowerCase();
  const hay = `${app} ${title} ${text}`;
  const hasCompose = visibleElements.some((item) => /compose|reply|subject|to:/i.test(`${item.name || ''} ${item.description || ''} ${item.value || ''}`));
  const hasCalendar = /calendar|meeting|agenda|invite|event/.test(hay);
  const hasFinder = /finder/.test(app);
  const hasBrowser = /chrome|safari|arc|firefox/.test(app);
  const hasEditor = /cursor|code|xcode|terminal/.test(app) || /manifest\.json|function|const |class /.test(text);
  const hasDialog = visibleElements.some((item) => /sheet|dialog|alert|window/i.test(item.role || '')) || /allow|cancel|ok|continue/.test(title);

  if (/system settings|privacy & security|accessibility/.test(hay)) return 'settings_pane';
  if (hasDialog) return 'modal_dialog';
  if (hasFinder) return 'finder_window';
  if (hasCalendar && /compose|new event|details/.test(hay)) return 'calendar_editor';
  if (hasCalendar) return 'calendar_view';
  if (/mail|gmail|outlook/.test(hay) && hasCompose) return 'email_composer';
  if (/mail|gmail|outlook|inbox|thread/.test(hay)) return 'email_view';
  if (hasEditor) return 'editor_view';
  if (hasBrowser && /google/.test(hay) && /search|feeling lucky/.test(hay) && !/result/.test(hay)) return 'search_home';
  if (hasBrowser && /google/.test(hay) && (/result|all images news maps shopping/.test(hay) || visibleElements.some((item) => /link|row/.test(String(item.role || '').toLowerCase())))) return 'search_results';
  if (hasBrowser && /sign in|captcha|verify|consent|choose an account/.test(hay)) return 'auth_interstitial';
  if (hasBrowser) return 'browser_page';
  return 'generic_app';
}

function deriveInteractiveCandidates(visibleElements = [], surfaceType = 'generic_app') {
  const roleWeight = (role = '') => {
    const value = String(role || '').toLowerCase();
    if (/text field|text area|search field/.test(value)) return 120;
    if (/button/.test(value)) return 90;
    if (/link/.test(value)) return surfaceType === 'search_results' ? 140 : 80;
    if (/tab|row|menu item/.test(value)) return 70;
    return 10;
  };
  const candidates = (Array.isArray(visibleElements) ? visibleElements : [])
    .map((item) => {
      const hay = `${item.name || ''} ${item.description || ''} ${item.value || ''}`.trim();
      const role = String(item.role || '').toLowerCase();
      let group = 'generic';
      let boost = 0;
      if (/text field|text area|search/.test(role) || /search|query|ask/.test(hay.toLowerCase())) {
        group = 'search_field';
        boost += 80;
      } else if (surfaceType === 'search_results' && (/link|row/.test(role) || /result|article/.test(hay.toLowerCase()))) {
        group = 'result_link';
        boost += 110;
      } else if (/button/.test(role)) {
        group = 'primary_button';
        boost += /search|next|continue|open|go/.test(hay.toLowerCase()) ? 70 : 30;
      } else if (/dialog|sheet|window/.test(role)) {
        group = 'dialog';
      } else if (/tab/.test(role)) {
        group = 'tab';
      }

      return {
        id: item.id || null,
        index: item.index,
        role: item.role,
        name: item.name,
        description: item.description,
        value: item.value,
        identifier: item.identifier,
        frame: item.frame || null,
        enabled: item.enabled,
        hint: item.hint,
        group,
        score: roleWeight(item.role) + boost + (item.enabled === false ? -500 : 0)
      };
    })
    .filter((item) => item.enabled !== false && (item.name || item.description || item.value))
    .sort((a, b) => b.score - a.score)
    .slice(0, 20);

  return candidates;
}

async function captureWindowScreenshot(bounds = null) {
  if (process.platform !== 'darwin') {
    return { present: false, error: 'unsupported_platform' };
  }

  const screenshotsDir = getScreenshotsDir();
  const fileName = `screen-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.jpg`;
  const filePath = path.join(screenshotsDir, fileName);
  const args = ['-x', '-t', 'jpg'];
  if (bounds && Number.isFinite(bounds.x) && Number.isFinite(bounds.y) && Number.isFinite(bounds.width) && Number.isFinite(bounds.height)) {
    args.push('-R', `${Math.round(bounds.x)},${Math.round(bounds.y)},${Math.max(1, Math.round(bounds.width))},${Math.max(1, Math.round(bounds.height))}`);
  }
  args.push(filePath);

  try {
    await runCommand('/usr/sbin/screencapture', args, { timeout: 15000 });
    const buffer = await fs.promises.readFile(filePath);
    const base64 = buffer.toString('base64');
    const truncated = base64.length > MAX_SCREENSHOT_BASE64;
    return {
      present: true,
      data_url: `data:image/jpeg;base64,${truncated ? base64.slice(0, MAX_SCREENSHOT_BASE64) : base64}`,
      captured_at: new Date().toISOString(),
      file_path: filePath,
      file_name: fileName,
      truncated
    };
  } catch (error) {
    return {
      present: false,
      error: error.message,
      captured_at: new Date().toISOString()
    };
  }
}

function summarizeFrameContext({ frontmost = {}, visibleElements = [], surfaceType = 'generic_app', interactiveCandidates = [] } = {}) {
  return {
    captured_at: new Date().toISOString(),
    app: frontmost.appName || '',
    title: frontmost.windowTitle || '',
    surface_type: surfaceType,
    text_sample: String(frontmost.extractedText || '').slice(0, 240),
    candidate_count: interactiveCandidates.length,
    top_candidates: interactiveCandidates.slice(0, 4).map((item) => ({
      index: item.index,
      role: item.role,
      name: item.name || item.description || item.value || '',
      group: item.group || 'generic'
    }))
  };
}

function compareFrameSummaries(frames = []) {
  const first = frames[0] || null;
  const last = frames[frames.length - 1] || null;
  if (!first || !last) {
    return {
      visual_change_summary: 'no temporal visual bundle available',
      target_confidence: 0.2
    };
  }

  if (first.title && last.title && first.title !== last.title) {
    return {
      visual_change_summary: `window title shifted from "${first.title}" to "${last.title}"`,
      target_confidence: 0.82
    };
  }
  if (first.surface_type !== last.surface_type) {
    return {
      visual_change_summary: `surface changed from ${first.surface_type} to ${last.surface_type}`,
      target_confidence: 0.76
    };
  }
  if (first.text_sample !== last.text_sample && last.text_sample) {
    return {
      visual_change_summary: 'visible window content changed across frames',
      target_confidence: 0.68
    };
  }
  if (last.candidate_count > first.candidate_count) {
    return {
      visual_change_summary: 'more interactive targets became visible over time',
      target_confidence: 0.62
    };
  }
  return {
    visual_change_summary: 'temporal frames show limited visible change',
    target_confidence: 0.38
  };
}

async function captureTemporalVisualBundle({
  mode = 'burst',
  bounds = null
} = {}) {
  if (process.platform !== 'darwin') {
    return {
      mode: 'ax_only',
      window_clip_used: false,
      frames: [],
      frame_summaries: [],
      visual_change_summary: 'temporal capture unsupported on this platform',
      target_confidence: 0.2,
      error: 'unsupported_platform'
    };
  }

  const isClip = mode === 'window_clip';
  const frameCount = isClip ? 3 : 2;
  const intervalMs = isClip ? WINDOW_CLIP_FRAME_INTERVAL_MS : BURST_FRAME_INTERVAL_MS;
  const frameSummaries = [];
  const frames = [];

  for (let index = 0; index < frameCount; index += 1) {
    const [frontmost, visibleElements, screenshot] = await Promise.all([
      getFrontmostWindowContext(),
      getVisibleElements(),
      captureWindowScreenshot(bounds)
    ]);
    const surfaceType = classifySurface({
      appName: frontmost.appName,
      windowTitle: frontmost.windowTitle,
      extractedText: frontmost.extractedText,
      visibleElements
    });
    const interactiveCandidates = deriveInteractiveCandidates(visibleElements, surfaceType);
    const summary = summarizeFrameContext({
      frontmost,
      visibleElements,
      surfaceType,
      interactiveCandidates
    });
    frameSummaries.push(summary);
    frames.push({
      ...summary,
      image: screenshot.present ? {
        data_url: screenshot.data_url,
        captured_at: screenshot.captured_at,
        truncated: screenshot.truncated
      } : null
    });
    if (index < frameCount - 1) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }

  const compared = compareFrameSummaries(frameSummaries);
  if (isClip) lastWindowClipAt = Date.now();
  else lastBurstAt = Date.now();

  return {
    mode,
    window_clip_used: isClip,
    frames,
    frame_summaries: frameSummaries,
    visual_change_summary: compared.visual_change_summary,
    target_confidence: compared.target_confidence
  };
}

async function shouldCaptureScreenshot({
  permissionTrusted = false,
  frontmost = {},
  visibleElements = [],
  interactiveCandidates = [],
  surfaceType = 'generic_app',
  options = {}
} = {}) {
  if (!permissionTrusted) return { capture: false, reason: 'permission_not_granted' };
  if (options.includeScreenshot === false) return { capture: false, reason: 'disabled_by_caller' };
  // Check if screen is on; if off, skip screenshot capture
  const screenIsOn = await isScreenOn();
  if (!screenIsOn) return { capture: false, reason: 'screen_off' };
  if (Date.now() - lastScreenshotAt < SCREENSHOT_COOLDOWN_MS && !options.forceScreenshot) {
    return { capture: false, reason: 'cooldown_active' };
  }

  // If explicitly requested to capture, always do it
  if (options.forceScreenshot) return { capture: true, reason: 'forced' };

  // Otherwise, be more aggressive: try to capture frequently
  // (changed from conservative ax_sufficient default)
  const textSample = String(frontmost?.extractedText || '').trim();
  const weakLabels = interactiveCandidates.length > 0
    && interactiveCandidates.filter((item) => !(item.name || item.description || item.value)).length >= Math.ceil(interactiveCandidates.length / 2);
  const ambiguousSurface = ['browser_page', 'generic_app', 'auth_interstitial'].includes(surfaceType);
  const hardSurface = ['search_results', 'settings_pane', 'modal_dialog'].includes(surfaceType);
  const noElements = !visibleElements.length || !interactiveCandidates.length;
  const noUsefulText = textSample.length < 60;
  const lastActionNoEffect = Boolean(options.lastActionNoEffect);

  if (lastActionNoEffect) return { capture: true, reason: 'last_action_no_effect' };
  if (hardSurface && (weakLabels || noElements)) return { capture: true, reason: 'hard_surface_disambiguation' };
  if (ambiguousSurface && (weakLabels || noUsefulText || noElements)) return { capture: true, reason: 'ambiguous_surface' };
  
  // NEW: Default to capturing unless ax_tree is very strong
  // This ensures screenshots are taken regularly for monitoring
  if (!interactiveCandidates.length || !textSample.length) {
    return { capture: true, reason: 'sparse_ax_data' };
  }
  
  return { capture: true, reason: 'periodic_capture' };
}

async function chooseVisionMode({
  permissionTrusted = false,
  frontmost = {},
  visibleElements = [],
  interactiveCandidates = [],
  surfaceType = 'generic_app',
  options = {}
} = {}) {
  if (!permissionTrusted) return { mode: 'ax_only', reason: 'permission_not_granted' };
  // If screen is off, use ax_only mode (no visual capture)
  const screenIsOn = await isScreenOn();
  if (!screenIsOn) return { mode: 'ax_only', reason: 'screen_off' };
  if (options.visionRequest === 'ax_only') return { mode: 'ax_only', reason: 'planner_requested_ax_only' };
  if (options.visionRequest === 'browser_vlm') return { mode: 'browser_vlm', reason: 'planner_requested_browser_vlm' };
  const weakLabels = interactiveCandidates.length > 0
    && interactiveCandidates.filter((item) => !(item.name || item.description || item.value)).length >= Math.ceil(interactiveCandidates.length / 2);
  const complexSurface = ['search_results', 'browser_page', 'auth_interstitial', 'modal_dialog'].includes(surfaceType);
  const noUsefulText = String(frontmost?.extractedText || '').trim().length < 60;
  const noElements = !visibleElements.length || !interactiveCandidates.length;
  const lastActionNoEffect = Boolean(options.lastActionNoEffect);
  const stale = Boolean(options.staleObservation);

  if ((options.forceWindowClip || options.visionRequest === 'window_clip') && (Date.now() - lastWindowClipAt >= WINDOW_CLIP_COOLDOWN_MS || options.forceWindowClip)) {
    return { mode: 'window_clip', reason: options.visionRequest === 'window_clip' ? 'planner_requested_window_clip' : 'forced_window_clip' };
  }
  if ((complexSurface && noElements && /chrome|safari|arc|firefox/i.test(String(frontmost?.appName || ''))) && (Date.now() - lastBurstAt >= BURST_COOLDOWN_MS)) {
    return { mode: 'browser_vlm', reason: 'browser_ax_blind_spot' };
  }
  if ((lastActionNoEffect && stale) && complexSurface && (Date.now() - lastWindowClipAt >= WINDOW_CLIP_COOLDOWN_MS)) {
    return { mode: 'window_clip', reason: 'temporal_disambiguation_needed' };
  }
  if ((options.forceScreenshot || options.visionRequest === 'burst' || (lastActionNoEffect && (weakLabels || complexSurface)) || (complexSurface && noElements))
    && (Date.now() - lastBurstAt >= BURST_COOLDOWN_MS || options.forceScreenshot || options.visionRequest === 'burst')) {
    return { mode: 'burst', reason: options.visionRequest === 'burst' ? 'planner_requested_burst' : 'visual_fallback_needed' };
  }
  return { mode: 'ax_only', reason: 'ax_sufficient' };
}

async function observeDesktopState(options = {}) {
  console.log('[observeDesktopState] Starting observation, options:', JSON.stringify(options));
  const [permission, frontmost, visibleElements] = await Promise.all([
    checkAccessibilityPermission(),
    getFrontmostWindowContext(),
    getVisibleElements()
  ]);
  console.log('[observeDesktopState] Got permission, frontmost, visibleElements');

  const focusedElement = visibleElements.find((item) => /text field|text area|text/i.test(String(item.role || '').toLowerCase())) || null;
  const surfaceType = classifySurface({
    appName: frontmost.appName,
    windowTitle: frontmost.windowTitle,
    extractedText: frontmost.extractedText,
    visibleElements
  });
  const interactiveCandidates = deriveInteractiveCandidates(visibleElements, surfaceType);
  const visionDecision = await chooseVisionMode({
    permissionTrusted: permission.trusted,
    frontmost,
    visibleElements,
    interactiveCandidates,
    surfaceType,
    options
  });
  console.log('[observeDesktopState] Vision decision:', visionDecision);
  const screenshotDecision = await shouldCaptureScreenshot({
    permissionTrusted: permission.trusted,
    frontmost,
    visibleElements,
    interactiveCandidates,
    surfaceType,
    options
  });
  console.log('[observeDesktopState] Screenshot decision:', screenshotDecision);
  const visualBundle = visionDecision.mode === 'ax_only'
    ? null
    : await captureTemporalVisualBundle({
        mode: visionDecision.mode === 'browser_vlm' ? 'burst' : visionDecision.mode,
        bounds: frontmost.bounds || null
      }).catch((error) => ({
        mode: visionDecision.mode,
        window_clip_used: visionDecision.mode === 'window_clip',
        frames: [],
        frame_summaries: [],
        visual_change_summary: error.message || 'temporal visual capture failed',
        target_confidence: 0.2,
        error: error.message
      }));
  const screenshot = screenshotDecision.capture
    ? (console.log('[observeDesktopState] Capturing screenshot:', screenshotDecision.reason), await captureWindowScreenshot(frontmost.bounds || null))
    : ((visualBundle?.frames?.length && visualBundle.frames[visualBundle.frames.length - 1]?.image)
      ? (console.log('[observeDesktopState] Using visual bundle frame'), {
          present: true,
          data_url: visualBundle.frames[visualBundle.frames.length - 1].image.data_url,
          captured_at: visualBundle.frames[visualBundle.frames.length - 1].image.captured_at,
          truncated: Boolean(visualBundle.frames[visualBundle.frames.length - 1].image.truncated)
        })
      : (console.log('[observeDesktopState] No screenshot:', screenshotDecision.reason), { present: false, skipped: true, reason: screenshotDecision.reason, captured_at: new Date().toISOString() }));
  if (screenshot.present) lastScreenshotAt = Date.now();

  // Get full OCR text from screenshot if available
  let ocrText = frontmost.extractedText || '';
  if (screenshot.present && screenshot.file_path) {
    console.log('[OCR] Running full OCR on screenshot:', screenshot.file_path);
    const ocrResult = await runOCR(screenshot.file_path);
    if (ocrResult && ocrResult.text) {
      ocrText = ocrResult.text;
      console.log('[OCR] Full OCR extracted:', ocrText.length, 'characters');
    } else {
      console.log('[OCR] OCR failed or no text, using AX text');
    }
  }

  const browserLike = /chrome|safari|arc|firefox/.test(String(frontmost.appName || '').toLowerCase());
  const surfaceDriver = browserLike ? 'live_browser_ax' : 'native_ax';
  const screenshotSummary = {
    present: Boolean(screenshot.present),
    captured_at: screenshot.captured_at || null,
    truncated: Boolean(screenshot.truncated),
    decision_reason: screenshotDecision.reason,
    visual_change_summary: visualBundle?.visual_change_summary || '',
    frame_count: Array.isArray(visualBundle?.frames) ? visualBundle.frames.length : 0
  };

  // Save OCR/extracted text to settings and database for future reference
  if (ocrText) {
    await saveOCRResults(ocrText, {
      app: frontmost.appName,
      window_title: frontmost.windowTitle,
      screenshot_present: Boolean(screenshot.present),
      screenshot_file: screenshot.file_name || '',
      vision_mode: visualBundle?.mode || visionDecision.mode || 'ax_only'
    }).catch(() => {}); // Silently fail if OCR save fails
  }

  return {
    surface_driver: surfaceDriver,
    ax_tree: frontmost.axTree || null,
    frontmost_app: frontmost.appName || '',
    window_title: frontmost.windowTitle || '',
    window_id: frontmost.windowId || null,
    bounds: frontmost.bounds || null,
    focused_element_id: frontmost.focusedElementId || null,
    focused_element: focusedElement,
    visible_elements: visibleElements.slice(0, 60),
    interactive_candidates: interactiveCandidates,
    text_sample: String(frontmost.extractedText || '').slice(0, 2000),
    selection: null,
    permission_state: permission,
    status: frontmost.status || 'unknown',
    surface_type: surfaceType,
    perception_mode: visionDecision.mode === 'browser_vlm'
      ? 'browser_vlm'
      : ((frontmost.axTree && interactiveCandidates.length) ? 'ax_tree' : 'ax_only'),
    vision_mode: visualBundle?.mode || visionDecision.mode || 'ax_only',
    window_clip_used: Boolean(visualBundle?.window_clip_used),
    frame_summaries: visualBundle?.frame_summaries || [],
    visual_change_summary: visualBundle?.visual_change_summary || '',
    target_confidence: visualBundle?.target_confidence ?? (screenshot.present ? 0.55 : 0.35),
    screenshot,
    screenshot_summary: screenshotSummary,
    vision_used: Boolean(screenshot.present) || Boolean(visualBundle),
    observation_budget: {
      include_screenshot: screenshotDecision.capture,
      vision_mode: visualBundle?.mode || visionDecision.mode || 'ax_only',
      vision_reason: visionDecision.reason,
      decision_reason: screenshotDecision.reason,
      cooldown_ms: SCREENSHOT_COOLDOWN_MS,
      screenshot_present: Boolean(screenshot.present)
    }
  };
}

async function activateApp(appName) {
  try {
    const result = await runSwiftAxOperator('action', { kind: 'ACTIVATE_APP', app: appName }, 12000);
    if (result?.status === 'success') return { status: 'success' };
  } catch (_) {}
  await runOsascript([`tell application ${appleScriptString(appName)} to activate`], { timeout: 10000 });
  return { status: 'success' };
}

async function launchAppInBackground(appName) {
  const safeName = String(appName || '').trim();
  if (!safeName) throw new Error('Missing app name');
  await runOsascript([`tell application ${appleScriptString(safeName)} to launch`], { timeout: 10000 });
  return { status: 'success' };
}

async function openUrl(url, appName = null, options = {}) {
  const args = [];
  if (options && options.background) {
    args.push('-g');
  }
  if (appName) {
    args.push('-a', appName);
  }
  args.push(url);
  await runCommand('/usr/bin/open', args, { timeout: 10000 });
  return { status: 'success' };
}

async function openAccessibilitySettings() {
  const urls = [
    'x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility',
    'x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_Accessibility'
  ];

  let lastError = null;
  for (const url of urls) {
    try {
      await runCommand('/usr/bin/open', [url], { timeout: 10000 });
      return { status: 'success', url };
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(lastError?.message || 'Unable to open Accessibility settings');
}

async function openScreenRecordingSettings() {
  const urls = [
    'x-apple.systempreferences:com.apple.preference.security?Privacy_ScreenCapture',
    'x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_ScreenCapture'
  ];

  let lastError = null;
  for (const url of urls) {
    try {
      await runCommand('/usr/bin/open', [url], { timeout: 10000 });
      return { status: 'success', url };
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(lastError?.message || 'Unable to open Screen Recording settings');
}

async function keyPress(key, modifiers = []) {
  try {
    const result = await runSwiftAxOperator('action', { kind: 'KEY_PRESS', key, modifiers }, 12000);
    if (result?.status === 'success') return { status: 'success' };
  } catch (_) {}
  const { keyCode } = normalizeKey(key);
  const usingClause = modifiersClause(modifiers);
  await runOsascript([
    'tell application "System Events"',
    keyCode != null ? `key code ${keyCode}${usingClause}` : `keystroke ${appleScriptString(key)}${usingClause}`,
    'end tell'
  ], { timeout: 10000 });
  return { status: 'success' };
}

function scoreElementMatch(element = {}, { label = '', role = '', targetIndex = null } = {}) {
  if (targetIndex != null && Number(element.index) === Number(targetIndex)) return 10_000;
  const hay = `${element.name || ''} ${element.description || ''} ${element.value || ''}`.toLowerCase();
  const roleValue = String(element.role || '').toLowerCase();
  const labelValue = String(label || '').trim().toLowerCase();
  let score = 0;
  if (labelValue) {
    if (hay === labelValue) score += 250;
    if ((element.name || '').toLowerCase() === labelValue) score += 220;
    if (hay.includes(labelValue)) score += 120;
    const terms = labelValue.split(/\s+/).filter(Boolean);
    score += terms.reduce((sum, term) => sum + (hay.includes(term) ? 15 : 0), 0);
  }
  if (role && roleValue.includes(String(role).toLowerCase())) score += 70;
  if (/button|link|tab|menu|row|group|checkbox/.test(roleValue)) score += 20;
  if (element.enabled === false) score -= 500;
  return score;
}

async function clickByLabel(label, role = '', targetIndex = null, targetId = null) {
  if (targetId) {
    try {
      const result = await runSwiftAxOperator('action', { kind: 'PRESS_AX', target_id: targetId }, 15000);
      if (result?.status === 'success') return { status: 'success' };
    } catch (_) {}
  }
  const labelText = String(label || '').trim();
  const roleText = String(role || '').trim().toLowerCase();
  if (!labelText && targetIndex == null) throw new Error('Missing AX label');
  const visibleElements = await getVisibleElements();
  const ranked = visibleElements
    .map((element) => ({ element, score: scoreElementMatch(element, { label: labelText, role: roleText, targetIndex }) }))
    .sort((a, b) => b.score - a.score);
  const best = ranked[0]?.element || null;
  if (!best || ranked[0].score <= 0) {
    throw new Error('AX element not found');
  }
  const bestLabel = best.name || best.description || best.value || labelText;
  const bestRole = best.role || roleText;
  await runOsascript([
    `set labelText to ${appleScriptString(String(bestLabel || '').toLowerCase())}`,
    `set roleText to ${appleScriptString(String(bestRole || '').toLowerCase())}`,
    'tell application "System Events"',
    'tell (first process whose frontmost is true)',
    'set frontWin to front window',
    'repeat with e in (entire contents of frontWin)',
    'set roleName to ""',
    'set elemName to ""',
    'set elemDesc to ""',
    'set elemValue to ""',
    'try set roleName to (role of e as text) end try',
    'try set elemName to (name of e as text) end try',
    'try set elemDesc to (description of e as text) end try',
    'try set elemValue to (value of e as text) end try',
    'set hay to (elemName & " " & elemDesc & " " & elemValue)',
    'if ((labelText is "") or ((hay as text)' + ' contains labelText)) and ((roleText is "") or ((roleName as text) contains roleText)) then',
    'try',
    'perform action "AXPress" of e',
    'return "pressed"',
    'end try',
    'try',
    'click e',
    'return "clicked"',
    'end try',
    'end if',
    'end repeat',
    'end tell',
    'end tell',
    'error "AX element not found"'
  ], { timeout: 20000 });
  return { status: 'success' };
}

async function focusWindowByTitle(title) {
  const windowTitle = String(title || '').trim();
  if (!windowTitle) throw new Error('Missing window title');
  try {
    const result = await runSwiftAxOperator('action', { kind: 'FOCUS_WINDOW', title: windowTitle }, 15000);
    if (result?.status === 'success') return { status: 'success' };
  } catch (_) {}
  await runOsascript([
    `set targetTitle to ${appleScriptString(windowTitle.toLowerCase())}`,
    'tell application "System Events"',
    'tell (first process whose frontmost is true)',
    'repeat with w in windows',
    'set windowName to ""',
    'try set windowName to name of w as text end try',
    'if (windowName as text) contains targetTitle then',
    'try perform action "AXRaise" of w end try',
    'set value of attribute "AXMain" of w to true',
    'return "focused"',
    'end if',
    'end repeat',
    'end tell',
    'end tell',
    'error "Window not found"'
  ], { timeout: 15000 });
  return { status: 'success' };
}

async function setValueOrType({ label = '', text = '', replace = false }) {
  const labelText = String(label || '').trim();
  const valueText = String(text || '');
  const visibleElements = await getVisibleElements();
  const ranked = visibleElements
    .map((element) => ({ element, score: scoreElementMatch(element, { label: labelText, role: 'text', targetIndex: null }) }))
    .sort((a, b) => b.score - a.score);
  const bestId = ranked[0]?.score > 0 ? ranked[0].element?.id : null;
  if (bestId) {
    try {
      const result = await runSwiftAxOperator('action', { kind: 'SET_AX_VALUE', target_id: bestId, text: valueText }, 15000);
      if (result?.status === 'success') return { status: 'success' };
    } catch (_) {}
  }
  if (labelText) {
    try {
      await runOsascript([
        `set labelText to ${appleScriptString(labelText.toLowerCase())}`,
        `set valueText to ${appleScriptString(valueText)}`,
        'tell application "System Events"',
        'tell (first process whose frontmost is true)',
        'set frontWin to front window',
        'repeat with e in (entire contents of frontWin)',
        'set roleName to ""',
        'set elemName to ""',
        'set elemDesc to ""',
        'try set roleName to (role of e as text) end try',
        'try set elemName to (name of e as text) end try',
        'try set elemDesc to (description of e as text) end try',
        'if ((roleName as text) contains "text") and (((elemName as text) contains labelText) or ((elemDesc as text) contains labelText)) then',
        'try',
        'set value of e to valueText',
        'return "set"',
        'end try',
        'try',
        'perform action "AXPress" of e',
        'return "pressed"',
        'end try',
        'end if',
        'end repeat',
        'end tell',
        'end tell',
        'error "Text field not found"'
      ], { timeout: 20000 });
    } catch (_) {
      // fall through to keyboard typing after focus attempts fail
    }
  }

  if (replace) {
    await keyPress('a', ['command']);
  }
  await runOsascript([
    'tell application "System Events"',
    `keystroke ${appleScriptString(valueText)}`,
    'end tell'
  ], { timeout: 20000 });
  return { status: 'success' };
}

async function waitForElement(label, role = '', timeoutMs = 15000, observeOptions = {}) {
  if (!label && !role) {
    await new Promise((resolve) => setTimeout(resolve, Math.max(250, timeoutMs)));
    return { status: 'success', observation: await observeDesktopState(observeOptions) };
  }
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const observation = await observeDesktopState(observeOptions);
    const found = (observation.visible_elements || []).find((item) => {
      const hay = `${item.name || ''} ${item.description || ''} ${item.value || ''}`.toLowerCase();
      const matchesLabel = !label || hay.includes(String(label).toLowerCase());
      const matchesRole = !role || String(item.role || '').toLowerCase().includes(String(role).toLowerCase());
      return matchesLabel && matchesRole;
    });
    if (found) return { status: 'success', observation };
    await new Promise((resolve) => setTimeout(resolve, 600));
  }
  throw new Error('Timed out waiting for AX element');
}

async function scrollWindow(direction = 'down', amount = 1) {
  const normalizedDirection = String(direction || 'down').toLowerCase();
  const repetitions = Math.max(1, Math.min(6, Number(amount) || 1));
  const key = normalizedDirection === 'up' ? 'pageup' : 'pagedown';
  for (let index = 0; index < repetitions; index += 1) {
    await keyPress(key);
    await new Promise((resolve) => setTimeout(resolve, 350));
  }
  return { status: 'success' };
}

async function executeDesktopAction(action = {}, context = {}) {
  const kind = String(action.kind || '').toUpperCase();
  if (!kind) throw new Error('Missing desktop action kind');

  if (kind === 'READ_UI_STATE') {
    return { status: 'success', observation: await observeDesktopState(context.observeOptions || {}) };
  }
  if (kind === 'DONE') {
    return { status: 'success', result: action.message || 'done' };
  }
  if (kind === 'ACTIVATE_APP') {
    return activateApp(action.app || action.appName || context.defaultApp || 'Finder');
  }
  if (kind === 'LAUNCH_APP_BACKGROUND') {
    return launchAppInBackground(action.app || action.appName || context.defaultApp || 'Finder');
  }
  if (kind === 'OPEN_URL') {
    return openUrl(action.url, action.app || context.defaultBrowser || null, {
      background: Boolean(action.background || context.background)
    });
  }
  if (kind === 'FOCUS_WINDOW') {
    return focusWindowByTitle(action.title || action.window_title || '');
  }
  if (kind === 'FOCUS_AX') {
    try {
      const result = await runSwiftAxOperator('action', { kind: 'FOCUS_AX', target_id: action.target_id || null }, 15000);
      if (result?.status === 'success') return { status: 'success' };
    } catch (error) {
      throw new Error(error.message || 'FOCUS_AX failed');
    }
  }
  if (kind === 'CLICK_AX' || kind === 'PRESS_AX') {
    return clickByLabel(action.label || action.text || action.name || '', action.role || '', action.target_index ?? action.index ?? null, action.target_id || null);
  }
  if (kind === 'CLICK_POINT') {
    try {
      const result = await runSwiftAxOperator('action', { kind: 'CLICK_POINT', target_point: action.target_point || null }, 15000);
      if (result?.status === 'success') return { status: 'success' };
    } catch (error) {
      throw new Error(error.message || 'CLICK_POINT failed');
    }
  }
  if (kind === 'TYPE_TEXT') {
    return setValueOrType({ label: action.label || '', text: action.text || '', replace: false });
  }
  if (kind === 'SET_VALUE') {
    return setValueOrType({ label: action.label || '', text: action.text || '', replace: true });
  }
  if (kind === 'SET_AX_VALUE') {
    try {
      const result = await runSwiftAxOperator('action', { kind: 'SET_AX_VALUE', target_id: action.target_id || null, text: action.text || '' }, 15000);
      if (result?.status === 'success') return { status: 'success' };
    } catch (error) {
      throw new Error(error.message || 'SET_AX_VALUE failed');
    }
  }
  if (kind === 'KEY_PRESS') {
    return keyPress(action.key || '', action.modifiers || []);
  }
  if (kind === 'SCROLL_AX') {
    return scrollWindow(action.direction || 'down', action.amount || action.steps || 1);
  }
  if (kind === 'WAIT_FOR_AX') {
    return waitForElement(action.label || action.text || '', action.role || '', Number(action.timeout_ms || 15000), context.observeOptions || {});
  }
  throw new Error(`Unsupported desktop action: ${kind}`);
}

module.exports = {
  checkAccessibilityPermission,
  observeDesktopState,
  executeDesktopAction,
  openAccessibilitySettings,
  openScreenRecordingSettings,
  saveOCRResults,
  getScreenshotsDir,
  getOCRHistory: () => {
    if (!store) {
      try {
        store = new Store();
      } catch (e) {
        return [];
      }
    }
    return store.get('ocr_history') || [];
  }
};
