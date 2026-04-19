const crypto = require('crypto');
const db = require('./db');
const { generateEmbedding } = require('./embedding-engine');
const { upsertRetrievalDoc } = require('./agent/graph-store');
const cognitiveRouter = require('./agent/cognitive-router');
let eventsDateColumnReady = false;
let eventEnvelopeColumnsReady = false;
let textChunksColumnsReady = false;

function normalizeEventTimestamp(input) {
  if (input === null || input === undefined || input === '') {
    const now = new Date();
    return { iso: now.toISOString(), date: now.toISOString().slice(0, 10) };
  }

  if (typeof input === 'number' && Number.isFinite(input)) {
    if (input <= 0) {
      const now = new Date();
      return { iso: now.toISOString(), date: now.toISOString().slice(0, 10) };
    }
    const ms = input < 1e12 ? input * 1000 : input;
    const parsed = new Date(ms);
    if (!Number.isNaN(parsed.getTime())) {
      const iso = parsed.toISOString();
      return { iso, date: iso.slice(0, 10) };
    }
  }

  const raw = String(input).trim();
  if (!raw || raw === '0') {
    const now = new Date();
    return { iso: now.toISOString(), date: now.toISOString().slice(0, 10) };
  }
  if (/^\d+$/.test(raw)) {
    const num = Number(raw);
    if (!Number.isFinite(num) || num <= 0) {
      const now = new Date();
      return { iso: now.toISOString(), date: now.toISOString().slice(0, 10) };
    }
    const ms = num < 1e12 ? num * 1000 : num;
    const parsed = new Date(ms);
    if (!Number.isNaN(parsed.getTime())) {
      const iso = parsed.toISOString();
      return { iso, date: iso.slice(0, 10) };
    }
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    const iso = parsed.toISOString();
    return { iso, date: iso.slice(0, 10) };
  }

  const now = new Date();
  return { iso: now.toISOString(), date: now.toISOString().slice(0, 10) };
}

function pickOccurredAt(type, metadata = {}, fallback = null) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const lowerType = String(type || '').toLowerCase();
  const candidates = [];

  if (lowerType.includes('calendar')) {
    candidates.push(meta.start_time, meta.start, meta.startTime, meta.dateTime, meta.date);
  } else if (lowerType.includes('email') || lowerType.includes('message')) {
    candidates.push(meta.internalDate, meta.sent_at, meta.received_at, meta.date, meta.timestamp);
  } else if (lowerType.includes('browser') || lowerType.includes('history') || lowerType.includes('visit')) {
    candidates.push(meta.last_visit_time, meta.visit_time, meta.visited_at, meta.timestamp);
  } else if (lowerType.includes('screen') || lowerType.includes('desktop') || lowerType.includes('capture') || lowerType.includes('sensor')) {
    candidates.push(meta.captured_at, meta.timestamp);
  }

  candidates.push(meta.occurred_at, meta.event_time, meta.timestamp, meta.updated, meta.modifiedTime, fallback);

  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined || candidate === '') continue;
    const normalized = normalizeEventTimestamp(candidate);
    if (normalized?.iso) return normalized;
  }

  return normalizeEventTimestamp(fallback);
}

async function ensureEventsDateColumn() {
  if (eventsDateColumnReady) return;
  const cols = await db.allQuery(`PRAGMA table_info(events)`).catch(() => []);
  const hasDate = (cols || []).some((c) => c && c.name === 'date');
  if (!hasDate) {
    await db.runQuery(`ALTER TABLE events ADD COLUMN date TEXT`).catch(() => {});
  }
  await db.runQuery(`
    UPDATE events
    SET date = substr(timestamp, 1, 10)
    WHERE (date IS NULL OR date = '') AND timestamp IS NOT NULL AND length(timestamp) >= 10
  `).catch(() => {});
  eventsDateColumnReady = true;
}

async function ensureEventEnvelopeColumns() {
  if (eventEnvelopeColumnsReady) return;
  const cols = await db.allQuery(`PRAGMA table_info(events)`).catch(() => []);
  const existing = new Set((cols || []).map((c) => c?.name).filter(Boolean));
  const required = [
    ['source_type', 'TEXT'],
    ['source_account', 'TEXT'],
    ['occurred_at', 'TEXT'],
    ['ingested_at', 'TEXT'],
    ['app', 'TEXT'],
    ['window_title', 'TEXT'],
    ['url', 'TEXT'],
    ['domain', 'TEXT'],
    ['participants', "TEXT DEFAULT '[]'"],
    ['title', 'TEXT'],
    ['raw_text', 'TEXT'],
    ['redacted_text', 'TEXT'],
    ['source_ref', 'TEXT'],
    ['observation_time', 'TEXT'],
    ['event_time', 'TEXT']
    ];

  for (const [name, sqlType] of required) {
    if (!existing.has(name)) {
      await db.runQuery(`ALTER TABLE events ADD COLUMN ${name} ${sqlType}`).catch(() => {});
    }
  }

  // Enforce canonical temporal fields for all legacy raw events so filters always work.
  await db.runQuery(`
    UPDATE events
    SET timestamp = COALESCE(NULLIF(timestamp, ''), NULLIF(occurred_at, ''), NULLIF(ingested_at, ''), datetime('now')),
        occurred_at = COALESCE(NULLIF(occurred_at, ''), NULLIF(timestamp, ''), NULLIF(ingested_at, ''), datetime('now')),
        ingested_at = COALESCE(NULLIF(ingested_at, ''), NULLIF(timestamp, ''), datetime('now'))
    WHERE timestamp IS NULL OR timestamp = '' OR occurred_at IS NULL OR occurred_at = '' OR ingested_at IS NULL OR ingested_at = ''
  `).catch(() => {});
  await db.runQuery(`
    UPDATE events
    SET date = substr(COALESCE(NULLIF(occurred_at, ''), NULLIF(timestamp, ''), datetime('now')), 1, 10)
    WHERE date IS NULL OR date = ''
  `).catch(() => {});

  eventEnvelopeColumnsReady = true;
}

async function ensureTextChunksColumns() {
  if (textChunksColumnsReady) return;
  const cols = await db.allQuery(`PRAGMA table_info(text_chunks)`).catch(() => []);
  const existing = new Set((cols || []).map((c) => c?.name).filter(Boolean));
  const required = [
    ['event_id', 'TEXT'],
    ['node_id', 'TEXT'],
    ['embedding', 'TEXT'],
    ['timestamp', 'TEXT'],
    ['date', 'TEXT'],
    ['app', 'TEXT'],
    ['data_source', 'TEXT']
  ];
  for (const [name, sqlType] of required) {
    if (!existing.has(name)) {
      await db.runQuery(`ALTER TABLE text_chunks ADD COLUMN ${name} ${sqlType}`).catch(() => {});
    }
  }
  textChunksColumnsReady = true;
}

/**
 * Lightweight generic entity extractor
 */
function extractEntities(text) {
  if (!text) return [];
  const entities = new Set();
  
  // Match Title-Cased names roughly (e.g., John Martins)
  const nameRegex = /([A-Z][a-z]+ [A-Z][a-z]+)/g;
  let match;
  while ((match = nameRegex.exec(text)) !== null) {
    // Avoid common false positives at start of sentences
    if (!['Hello There', 'Best Regards', 'Thanks So', 'Thank You'].includes(match[1])) {
      entities.add(match[1]);
    }
  }

  // Pre-tagged entities if any (e.g. "project:Math IA")
  const taggedRegex = /(project|person|app|topic):[a-zA-Z0-9 ]+/ig;
  while ((match = taggedRegex.exec(text)) !== null) {
    entities.add(match[0].trim());
  }

  // Extract common domains/apps
  if (text.toLowerCase().includes('mail.google.com')) entities.add('app:Gmail');
  if (text.toLowerCase().includes('docs.google.com')) entities.add('app:Google Docs');

  return Array.from(entities).slice(0, 15); // Cap to avoid massive fanout
}

function redactSensitiveText(input) {
  const original = String(input || '');
  let text = original;
  const redactionPatterns = [
    {
      name: 'credit_card',
      // rough PAN detection
      regex: /\b(?:\d[ -]*?){13,19}\b/g
    },
    {
      name: 'api_key',
      regex: /\b(?:sk|rk|pk|api)[-_][a-zA-Z0-9_-]{16,}\b/g
    },
    {
      name: 'bearer_token',
      regex: /\bBearer\s+[A-Za-z0-9\-._~+/]+=*\b/gi
    },
    {
      name: 'credential_assignment',
      regex: /\b(password|passwd|pwd|secret|token)\s*[:=]\s*["']?[^"'\s]{4,}["']?/gi
    }
  ];

  const applied = [];
  for (const pattern of redactionPatterns) {
    let matched = false;
    text = text.replace(pattern.regex, () => {
      matched = true;
      return `[REDACTED_${pattern.name.toUpperCase()}]`;
    });
    if (matched) applied.push(pattern.name);
  }

  return {
    text,
    redacted: applied.length > 0,
    applied
  };
}

function sanitizeEmailText(text) {
  return String(text || '')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\bhttps?:\/\/[^\s]+(?:open|click|track)[^\s]*/gi, ' ')
    .replace(/(^|\n)--\s*\n[\s\S]*$/m, ' ')
    .replace(/\nOn .+wrote:\n[\s\S]*$/i, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeBrowserText(text, metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const title = meta.title || meta.pageTitle || '';
  const url = meta.url || meta.link || '';
  const domain = (() => {
    try {
      return url ? new URL(url).hostname.replace(/^www\./, '').toLowerCase() : '';
    } catch (_) {
      return '';
    }
  })();
  return [
    title ? `Title: ${title}` : '',
    url ? `URL: ${url}` : '',
    domain ? `Domain: ${domain}` : '',
    String(text || '').trim()
  ].filter(Boolean).join('\n').trim();
}

function sanitizeCalendarText(text, metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  return [
    meta.summary || meta.title ? `Event: ${meta.summary || meta.title}` : '',
    meta.start_time || meta.start ? `Start: ${meta.start_time || meta.start}` : '',
    meta.end_time || meta.end ? `End: ${meta.end_time || meta.end}` : '',
    Array.isArray(meta.attendees) && meta.attendees.length
      ? `Attendees: ${meta.attendees.map((item) => item?.email || item).filter(Boolean).join(', ')}`
      : '',
    String(text || meta.description || '').trim()
  ].filter(Boolean).join('\n').trim();
}

function stripLikelyUiChromeLines(lines = []) {
  return (lines || []).filter((line) => {
    const value = String(line || '').trim();
    if (!value) return false;
    if (value.length <= 2) return false;
    // UI Navigation & Chrome
    if (/^(back|next|open|save|share|search|home|reload|refresh|compose|inbox|sent|drafts|trash|archive|reply|forward|settings|help|menu|close|minimize|maximize|restore|quit|exit)$/i.test(value)) return false;
    if (/^(file|edit|view|history|bookmarks|window|help|tools|run|terminal|debug|options|preferences)$/i.test(value)) return false;
    // Time & Date overlays
    if (/^\d{1,2}:\d{2}(?::\d{2})?\s*(?:am|pm)?$/i.test(value)) return false;
    if (/^(mon|tue|wed|thu|fri|sat|sun)\b/i.test(value) && value.length <= 15) return false;
    if (/^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(value) && value.length <= 15) return false;
    // Keyboard shortcuts & System
    if (/^(ctrl|cmd|alt|shift|meta|esc|tab|space|enter|delete|backspace|insert|home|end|pgup|pgdn|up|down|left|right)\b/i.test(value)) return false;
    // App specific UI noise
    if (/^(unread|starred|important|muted|snoozed|promotions|social|updates|forums)$/i.test(value)) return false;
    if (/^(all mail|spam|bin|drafts|sent|inbox|scheduled)$/i.test(value)) return false;
    
    // Aggressive OCR filtering additions
    if (/^(loading|please wait|sign in|log in|sign up|password|username|email address|forgot password)$/i.test(value)) return false;
    if (/^(\d+ messages?|\d+ notifications?|\d+ unread)$/i.test(value)) return false;
    if (/^(cookies|privacy policy|terms of service|accept|decline|manage cookies)$/i.test(value)) return false;
    if (/^https?:\/\/[^\s]+$/i.test(value)) return false; 

    return true;
  });
}

function inferDesktopContentType(text, metadata = {}) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const hay = `${meta.app || meta.activeApp || ''} ${meta.window_title || meta.activeWindowTitle || ''} ${text || ''}`.toLowerCase();
  if (/\bgmail|inbox|subject:|from:|to:|cc:|sent from my iphone|wrote:/i.test(hay)) return 'email';
  if (/\bslack|discord|teams|message|reply|thread\b/.test(hay)) return 'chat';
  if (/\bdocs\.google|document|comments|suggesting|outline\b/.test(hay)) return 'document';
  if (/\bcursor|vscode|terminal|error:|exception|traceback|manifest\.json|background\.js|function |const |import /i.test(hay)) return 'code';
  if (/\bcalendar|meeting|attendees|agenda|zoom|meet\b/.test(hay)) return 'calendar';
  if (/\bform|submit|required|placeholder|field\b/.test(hay)) return 'form';
  if (/\btable|dashboard|chart|metric|analytics|revenue|conversion\b/.test(hay)) return 'dashboard';
  if (meta.url || /\bhttps?:\/\//i.test(hay)) return 'browser_page';
  return 'general';
}

function inferDesktopActivity(contentType, text, metadata = {}) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const app = String(meta.app || meta.activeApp || '').trim();
  const windowTitle = String(meta.window_title || meta.activeWindowTitle || '').trim();
  const source = String(text || '');
  const lowered = source.toLowerCase();
  const lines = source.split('\n').map((line) => line.trim()).filter(Boolean);
  const shortEvidence = (lines.find((line) => /subject:|from:|to:|error|exception|traceback|question|assignment|deadline|due|reply|draft|send|meeting|agenda/i.test(line)) || lines[0] || '').slice(0, 140);
  const focusLine = (lines.find((line) => {
    const v = String(line || '').trim();
    if (!v || v.length < 14) return false;
    if (/^(app|window|captured text|title|url|domain|surface|activity|signals?|entities?):/i.test(v)) return false;
    if (/^(back|next|open|save|share|search|home|reload|refresh|compose|inbox|sent|drafts|trash|archive|reply|forward)$/i.test(v)) return false;
    return /\b(reply|draft|send|submit|review|fix|debug|meeting|agenda|assignment|deadline|due|report|proposal|ticket|issue|task|document|notes|research|plan|code)\b/i.test(v) || v.split(/\s+/).length >= 4;
  }) || '').slice(0, 150);

  // Enhanced Detection for "Creating" vs "Viewing"
  const creatingSignals = [
    /\b(writing|drafting|composing|coding|editing|designing|creating|building|developing|typing|refactoring|debugging|sketching|drawing|painting|producing|rendering|recording)\b/i,
    /\b(save|commit|push|publish|submit|send|post|create|new|add|insert|update|deployment|deploy|checkout|merge|rebase)\b/i,
    /\b(untitled|new folder|new document|new file|draft|unsaved|modified|\*)\b/i
  ];
  
  const viewingSignals = [
    /\b(viewing|reading|browsing|searching|looking|watching|previewing|read-only)\b/i,
    /\b(details|overview|summary|info|about|help|faq)\b/i
  ];

  const hasCreatingSignal = creatingSignals.some(sig => sig.test(lowered)) || /\b(github.*(compare|pull|new)|compose|drafting|replying|editing)\b/i.test(windowTitle);
  const hasViewingSignal = viewingSignals.some(sig => sig.test(lowered));
  
  const isKnownEditor = (app && /\b(cursor|vscode|intellij|sublime|textedit|notes|notion|google docs|pages|word|figma|canva|linear|github|slack|discord|teams|obsidian|craft|scrivener|overleaf|replit|terminal|iterm|xcode|android studio|unity|blender|photoshop|illustrator|premiere|after effects)\b/i.test(app.toLowerCase()));
  const isReadOnlyWindow = /\b(view|preview|read-only|readonly|history|log|output|logs|terminal output)\b/i.test(windowTitle.toLowerCase());

  let isCreating = false;
  if (isKnownEditor && !isReadOnlyWindow) {
    isCreating = true; // Default to creating for editors unless explicitly read-only
  }
  if (hasCreatingSignal) isCreating = true;
  if (hasViewingSignal && !hasCreatingSignal) isCreating = false;

  const activityLabel = isCreating ? 'creating' : 'viewing';

  // Prefer explicit study signal emitted during capture over guessed text labels.
  if (meta.study_signal) {
    const signalMap = {
      reading: 'reading study content',
      solving: 'solving problems or exercises',
      drafting: 'drafting written work',
      revision: 'reviewing feedback or results',
      distraction: 'browsing non-task content',
      'context-switch': 'switching between study contexts',
      idle: 'screen open with little readable activity'
    };
    const signal = String(meta.study_signal).toLowerCase();
    const label = signalMap[signal] || `study signal: ${meta.study_signal}`;
    // "idle/distraction" are noisy in OCR captures; only trust them when evidence is weak.
    if (!['idle', 'distraction'].includes(signal) || (!focusLine && !shortEvidence)) {
      return {
        summary: shortEvidence ? `${label}${focusLine ? ` (${focusLine})` : ''}` : label,
        confidence: shortEvidence || focusLine ? 'high' : 'medium',
        evidence: [focusLine || shortEvidence].filter(Boolean),
        activity_type: isCreating ? 'creating' : 'viewing'
      };
    }
  }

  if (!source || source.length < 20) {
    return {
      summary: windowTitle ? `${activityLabel} content in ${windowTitle}` : (app ? `${activityLabel} content in ${app}` : 'activity unclear from capture'),
      confidence: 'low',
      evidence: [],
      activity_type: activityLabel
    };
  }

  if (contentType === 'email') {
    if (/\bsubject:|from:|to:|cc:/i.test(source)) {
      return {
        summary: focusLine ? `reviewing email thread: ${focusLine}` : 'reviewing email thread metadata',
        confidence: 'high',
        evidence: [focusLine || shortEvidence].filter(Boolean),
        activity_type: activityLabel
      };
    }
    return {
      summary: focusLine ? `${activityLabel === 'creating' ? 'drafting' : 'reviewing'} email: ${focusLine}` : 'email context visible, exact action unclear',
      confidence: 'medium',
      evidence: [focusLine || shortEvidence].filter(Boolean),
      activity_type: activityLabel
    };
  }

  if (contentType === 'code' && /\berror|exception|undefined|failed|stack trace|traceback\b/.test(lowered)) {
    return {
      summary: focusLine ? `debugging runtime/build issue: ${focusLine}` : 'reviewing runtime or build errors',
      confidence: 'high',
      evidence: [focusLine || shortEvidence].filter(Boolean),
      activity_type: 'creating' // debugging is active
    };
  }

  if (/\b(todo|task|deadline|due|submit|assignment|exam|priority|follow up|required|action item)\b/i.test(source)) {
    return {
      summary: focusLine ? `task-oriented work: ${focusLine}` : 'task-oriented activity visible in capture',
      confidence: 'high',
      evidence: [focusLine || shortEvidence].filter(Boolean),
      activity_type: activityLabel
    };
  }

  if (/\b(reply|draft|send|compose|submit|save|publish|commit|merge|schedule|book|confirm)\b/i.test(source)) {
    return {
      summary: focusLine ? `executing action step: ${focusLine}` : 'action-oriented workflow visible in capture',
      confidence: 'medium',
      evidence: [focusLine || shortEvidence].filter(Boolean),
      activity_type: 'creating'
    };
  }

  if (focusLine) {
    return {
      summary: `${activityLabel === 'creating' ? 'drafting' : 'working on'}: ${focusLine}`,
      confidence: focusLine.length > 28 ? 'high' : 'medium',
      evidence: [focusLine],
      activity_type: activityLabel
    };
  }

  return {
    summary: windowTitle
      ? `${activityLabel} content in ${windowTitle}`
      : (app ? `${activityLabel} content in ${app}` : 'viewing on-screen content'),
    confidence: 'medium',
    evidence: shortEvidence ? [shortEvidence] : [],
    activity_type: activityLabel
  };
}

function extractActionMarkers(text, contentType) {
  const markers = [];
  const source = String(text || '');
  if (/\b(reply|respond|follow up)\b/i.test(source)) markers.push('follow_up_needed');
  if (/\b(schedule|reschedule|book|confirm)\b/i.test(source)) markers.push('scheduling');
  if (/\b(review|comment|approve|feedback)\b/i.test(source)) markers.push('review');
  if (/\bfix|debug|error|issue|exception|failed|crash\b/i.test(source)) markers.push('troubleshooting');
  if (/\bdraft|write|prepare|send\b/i.test(source)) markers.push('drafting');
  if (/\b(todo|task|action item|due|deadline|priority|required)\b/i.test(source)) markers.push('task_execution');
  if (/\bsubmit|assignment|exam|study plan|problem set\b/i.test(source)) markers.push('study_task');
  if (contentType === 'email' && /\b(subject:|from:|to:)\b/i.test(source)) markers.push('thread_context');
  return Array.from(new Set(markers)).slice(0, 6);
}

const CAPTURE_CATEGORY_MAP = {
  email: 'communication',
  chat: 'communication',
  code: 'desktop',
  document: 'desktop',
  browser_page: 'desktop',
  calendar: 'calendar',
  form: 'desktop',
  dashboard: 'desktop',
  terminal: 'desktop',
  general: 'desktop'
};

function interpretDesktopCapture(text, metadata = {}) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const raw = String(text || '').replace(/\r/g, '\n');
  const lines = raw.split('\n').map((line) => line.trim()).filter(Boolean);
  const cleanedLines = stripLikelyUiChromeLines(lines);
  let cleanedText = cleanedLines.join('\n').replace(/\n{3,}/g, '\n\n').trim();

  // When OCR produced nothing (or very little), enrich with window title and app name
  // so that content type and activity inference have something to work with.
  if (cleanedText.length < 30) {
    const windowTitle = String(meta.window_title || meta.activeWindowTitle || '').trim();
    const appName = String(meta.app || meta.activeApp || '').trim();
    const urlHint = String(meta.url || '').trim();
    const fallbackParts = [windowTitle, appName, urlHint].filter(Boolean);
    if (fallbackParts.length) {
      cleanedText = [cleanedText, ...fallbackParts].filter(Boolean).join('\n');
    }
  }

  const contentType = inferDesktopContentType(cleanedText || raw, meta);
  const activity = inferDesktopActivity(contentType, cleanedText || raw, meta);
  const activitySummary = activity.summary || 'activity unclear from capture';
  const actionMarkers = extractActionMarkers(cleanedText || raw, contentType);
  const entityRefs = extractEntities(cleanedText || raw).slice(0, 10);
  const uncertainty = cleanedText.length < 40 ? 'high' : (cleanedText.length < 120 ? 'medium' : 'low');
  const focusSnippet = (cleanedLines.slice(0, 6).join(' ') || raw.replace(/\s+/g, ' ').trim()).slice(0, 420);
  const captureCategory = CAPTURE_CATEGORY_MAP[contentType] || 'desktop';

  const searchText = [
    meta.app || meta.activeApp ? `App: ${meta.app || meta.activeApp}` : '',
    meta.window_title || meta.activeWindowTitle ? `Window: ${meta.window_title || meta.activeWindowTitle}` : '',
    `Activity: ${activitySummary}`,
    contentType ? `Surface: ${contentType}` : '',
    entityRefs.length ? `Entities: ${entityRefs.join(', ')}` : '',
    actionMarkers.length ? `Signals: ${actionMarkers.join(', ')}` : '',
    focusSnippet ? `Content: ${focusSnippet}` : ''
  ].filter(Boolean).join('\n').trim();

  return {
    searchText,
    cleanedText,
    activitySummary,
    activityType: activity.activity_type || 'viewing',
    activityConfidence: activity.confidence || 'low',
    activityEvidence: Array.isArray(activity.evidence) ? activity.evidence.slice(0, 3) : [],
    contentType,
    captureCategory,
    actionMarkers,
    entityRefs,
    uncertainty
  };
}

function sanitizeDesktopText(text, metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const interpretation = interpretDesktopCapture(text, meta);
  return `${interpretation.searchText}\n\nFull OCR:\n${interpretation.cleanedText}`;
}

function sanitizeSourceText({ type, source, text, metadata }) {
  const lower = `${type || ''} ${source || ''}`.toLowerCase();
  if (/\bemail|gmail|message|thread\b/.test(lower)) return sanitizeEmailText(text || metadata?.body || metadata?.snippet || '');
  if (/\bbrowser|history|visit|chrome|safari\b/.test(lower)) return sanitizeBrowserText(text, metadata);
  if (/\bcalendar|meeting|event\b/.test(lower)) return sanitizeCalendarText(text, metadata);
  if (/\bscreen|desktop|capture|sensor\b/.test(lower)) return sanitizeDesktopText(text, metadata);
  return String(text || '').replace(/\s+/g, ' ').trim();
}

function chunkText(text, { size = 700, overlap = 120 } = {}) {
  const source = String(text || '').trim();
  if (!source) return [];
  const chunks = [];
  let start = 0;
  let index = 0;
  const stride = Math.max(1, size - overlap);

  while (start < source.length) {
    const end = Math.min(source.length, start + size);
    const chunk = source.slice(start, end).trim();
    if (chunk) {
      chunks.push({ index, text: chunk });
      index += 1;
    }
    if (end >= source.length) break;
    start += stride;
  }

  return chunks.slice(0, 48);
}

function inferEventApp(source, metadata) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const direct = meta.app || meta.appName || meta.activeApp || meta.application || '';
  if (direct) return String(direct);
  if (source) return String(source);
  return 'Unknown';
}

function inferDataSource(type, metadata) {
  const t = String(type || '').toLowerCase();
  const m = metadata && typeof metadata === 'object' ? metadata : {};
  if (m.data_source && ['raw', 'summaries', 'summary'].includes(String(m.data_source).toLowerCase())) {
    const ds = String(m.data_source).toLowerCase();
    return ds === 'summary' ? 'summaries' : ds;
  }
  if (t.includes('summary')) return 'summaries';
  return 'raw';
}

function isHumanParticipantCandidate(value) {
  const text = String(value || '').trim();
  if (!text) return false;
  if (/^(app|topic|project)\s*:/i.test(text)) return false;
  if (/^(chrome|cursor|gmail|calendar|desktop|drive|google docs)$/i.test(text)) return false;
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) return true;
  if (!/^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$/.test(text)) return false;
  if (/\b(School|Team|Inc|LLC|Ltd|University|College|Studio|Labs|App|Docs)\b/.test(text)) return false;
  return true;
}

function normalizeEventEnvelope({ id, type, timestamp, source, text, metadata = {}, entities = [] }) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const lowerType = String(type || '').toLowerCase();
  const url = meta.url || meta.webViewLink || meta.link || meta.currentUrl || null;
  const domain = url
    ? (() => {
        try {
          return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
        } catch (_) {
          return null;
        }
      })()
    : null;
  const participantCandidates = [];
  if (lowerType.includes('calendar')) {
    participantCandidates.push(meta.organizer?.email || meta.organizer);
    participantCandidates.push(...(Array.isArray(meta.participants) ? meta.participants : []));
    participantCandidates.push(...(Array.isArray(meta.attendees) ? meta.attendees.map((item) => item?.email || item) : []));
  } else if (lowerType.includes('email') || lowerType.includes('message')) {
    participantCandidates.push(meta.from);
    participantCandidates.push(...entities.filter((item) => isHumanParticipantCandidate(item)));
  } else {
    participantCandidates.push(...entities.filter((item) => isHumanParticipantCandidate(item)));
  }

  const participants = Array.from(new Set(
    participantCandidates
      .filter(Boolean)
      .map((item) => String(item).trim())
      .filter((item) => isHumanParticipantCandidate(item))
  )).slice(0, 12);

  const topics = Array.from(new Set([
    meta.subject,
    meta.summary,
    meta.title,
    meta.event_title,
    meta.topic,
    domain,
    inferEventApp(source, meta),
    ...entities.filter((item) => /^topic:/i.test(String(item || '')))
  ].filter(Boolean).map((item) => String(item).trim()))).slice(0, 12);

  const title = String(
    meta.title ||
    meta.subject ||
    meta.summary ||
    meta.event_title ||
    meta.window_title ||
    meta.activeWindowTitle ||
    meta.pageTitle ||
    meta.url ||
    type ||
    'Untitled event'
  ).trim();
  const sourceType = String(meta.source_type || type || 'event').trim();
  const sourceRef = String(
    meta.source_ref ||
    meta.threadId ||
    meta.original_event_id ||
    meta.id ||
    meta.url ||
    id
  ).trim();
  const sourceAccount = String(
    meta.account ||
    meta.email ||
    meta.from ||
    meta.source_account ||
    ''
  ).trim() || null;
  const windowTitle = String(meta.window_title || meta.activeWindowTitle || '').trim() || null;
  const ingestedAt = meta.ingested_at ? normalizeEventTimestamp(meta.ingested_at).iso : new Date().toISOString();
  const rawText = String(text || '').trim();
  const redactedText = String(meta.redacted_text || rawText).trim();
  const occurredAtInfo = pickOccurredAt(type, meta, timestamp);
  const occurredAt = occurredAtInfo.iso;

  const isPassive = lowerType.includes('screen') || lowerType.includes('desktop') || lowerType.includes('capture');

  return {
    id,
    type: sourceType,
    source_type: sourceType,
    source_account: sourceAccount,
    occurred_at: occurredAt,
    occurred_date: occurredAtInfo.date,
    ingested_at: ingestedAt,
    observation_time: ingestedAt,
    event_time: occurredAt,
    passive_observation: isPassive,
    type_group: lowerType.includes('calendar') ? 'calendar'
      : (lowerType.includes('email') || lowerType.includes('message') ? 'communication'
        : (lowerType.includes('browser') || lowerType.includes('screen') ? 'desktop' : 'artifact')),
    source: String(source || meta.source || '').trim() || 'Unknown',
    timestamp: occurredAt,
    app: inferEventApp(source, meta),
    window_title: windowTitle,
    title,
    text: redactedText,
    raw_text: rawText,
    redacted_text: redactedText,
    url,
    domain,
    participants,
    actors: participants,
    topics,
    source_ref: sourceRef,
    identifiers: Array.from(new Set([
      meta.threadId,
      meta.original_event_id,
      meta.id,
      meta.subject,
      domain
    ].filter(Boolean).map((item) => String(item).trim()))).slice(0, 10),
    metadata: meta
  };
}

async function indexEventChunks({
  eventId,
  eventType,
  timestampISO,
  date,
  source,
  text,
  metadata
}) {
  await ensureTextChunksColumns();
  const chunks = chunkText(text);
  if (!chunks.length) return { count: 0 };

  const app = inferEventApp(source, metadata);
  const dataSource = inferDataSource(eventType, metadata);

  await db.runQuery(`DELETE FROM text_chunks WHERE event_id = ?`, [eventId]).catch(() => {});

  for (const chunk of chunks) {
    const chunkId = `chk_${eventId}_${chunk.index}`;
    const embedding = await generateEmbedding(chunk.text, process.env.OPENAI_API_KEY);
    await db.runQuery(
      `INSERT OR REPLACE INTO text_chunks
       (id, event_id, node_id, chunk_index, text, embedding, timestamp, date, app, data_source, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        chunkId,
        eventId,
        null,
        chunk.index,
        chunk.text,
        JSON.stringify(embedding || []),
        timestampISO,
        date,
        app,
        dataSource,
        JSON.stringify({
          source,
          event_type: eventType
        })
      ]
    );

    await db.runQuery(
      `INSERT OR IGNORE INTO edges (from_id, to_id, relation, data) VALUES (?, ?, 'has_chunk', ?)`,
      [eventId, chunkId, JSON.stringify({ data_source: dataSource, app })]
    ).catch(() => {});
  }

  return { count: chunks.length };
}

/**
 * Standardize an external payload into the universal L1 format and store in SQLite
 */
async function ingestRawEvent({ type, timestamp, source, text, metadata }) {
  await ensureEventsDateColumn();
  await ensureEventEnvelopeColumns();
  const normalizedTime = pickOccurredAt(
    type,
    metadata && typeof metadata === 'object' ? metadata : {},
    timestamp ?? metadata?.timestamp ?? metadata?.captured_at ?? metadata?.date ?? null
  );
  const tStr = normalizedTime.iso;
  const dStr = normalizedTime.date;
  const safeMetadataInput = metadata && typeof metadata === 'object' ? metadata : {};
  const rawInputText = String(text || safeMetadataInput.body || safeMetadataInput.snippet || '').trim();
  const desktopInterpretation = /\bscreen|desktop|capture|sensor\b/i.test(`${type || ''} ${source || ''}`)
    ? interpretDesktopCapture(rawInputText, safeMetadataInput)
    : null;
  const sourceText = sanitizeSourceText({ type, source, text, metadata: safeMetadataInput });
  const rawIdBasis = safeMetadataInput.id || safeMetadataInput.source_ref || safeMetadataInput.threadId || safeMetadataInput.original_event_id || safeMetadataInput.url || `${type}|${source}|${tStr}|${sourceText.slice(0, 160)}`;
  const id = `evt_${crypto.createHash('sha1').update(String(rawIdBasis)).digest('hex').slice(0, 24)}`;

  const redaction = redactSensitiveText(sourceText || '');
  const safeText = redaction.text;
  const entities = extractEntities(safeText || '');
  const safeMetadata = {
    ...safeMetadataInput,
    timestamp: (metadata && metadata.timestamp) ? metadata.timestamp : tStr,
    occurred_at: (metadata && metadata.occurred_at) ? metadata.occurred_at : tStr,
    date: (metadata && metadata.date) ? metadata.date : dStr,
    event_date: dStr,
    data_source: inferDataSource(type, metadata),
    redaction: {
      applied: redaction.applied,
      redacted: redaction.redacted
    },
    raw_text: rawInputText,
    redacted_text: safeText,
    ...(desktopInterpretation ? {
      activity_summary: desktopInterpretation.activitySummary,
      activity_type: desktopInterpretation.activityType,
      activity_confidence: desktopInterpretation.activityConfidence,
      activity_evidence: desktopInterpretation.activityEvidence,
      content_type: desktopInterpretation.contentType,
      capture_category: desktopInterpretation.captureCategory,
      action_markers: desktopInterpretation.actionMarkers,
      entity_refs: desktopInterpretation.entityRefs,
      capture_uncertainty: desktopInterpretation.uncertainty,
      cleaned_capture_text: desktopInterpretation.cleanedText
    } : {})
  };
  const envelope = normalizeEventEnvelope({
    id,
    type,
    timestamp: tStr,
    source,
    text: safeText,
    metadata: safeMetadata,
    entities
  });

  try {
    await db.runQuery(
      `INSERT OR REPLACE INTO events
       (id, type, timestamp, date, source, source_type, source_account, occurred_at, ingested_at, observation_time, event_time, app, window_title, url, domain, participants, title, raw_text, redacted_text, source_ref, text, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        envelope.source_type,
        envelope.occurred_at,
        envelope.occurred_date || dStr,
        envelope.source,
        envelope.source_type,
        envelope.source_account,
        envelope.occurred_at,
        envelope.ingested_at,
        envelope.observation_time,
        envelope.event_time,
        envelope.app,
        envelope.window_title,
        envelope.url,
        envelope.domain,
        JSON.stringify(envelope.participants || []),
        envelope.title,
        envelope.raw_text,
        envelope.redacted_text,
        envelope.source_ref,
        safeText || '',
        JSON.stringify(safeMetadata)
      ]
    );

    // Write entity lookups
    for (const ent of entities) {
      await db.runQuery(
        `INSERT OR IGNORE INTO event_entities (event_id, entity) VALUES (?, ?)`, 
        [id, ent]
      );
    }

    await upsertRetrievalDoc({
      docId: `event:${id}`,
      sourceType: 'event',
      eventId: id,
      app: envelope.app,
      timestamp: envelope.occurred_at,
      text: [
        envelope.source_type,
        envelope.source,
        envelope.app,
        envelope.domain,
        envelope.window_title,
        envelope.title,
        envelope.participants.join(' '),
        envelope.topics.join(' '),
        envelope.identifiers.join(' '),
        envelope.text
      ].filter(Boolean).join('\n'),
      metadata: {
        envelope,
        data_source: safeMetadata.data_source
      }
    });

    await indexEventChunks({
      eventId: id,
      eventType: type,
      timestampISO: envelope.occurred_at,
      date: dStr,
      source,
      text: safeText,
      metadata: safeMetadata
    });
    
    // Dispatch to real-time cognitive router for high-priority micro-updates
    cognitiveRouter.dispatch(envelope);

  } catch (e) {
    console.error('[ingestRawEvent] Failed storing event:', id, e);
  }

  return { ...envelope, date: dStr, entities, metadata: safeMetadata };
}

module.exports = {
  extractEntities,
  redactSensitiveText,
  chunkText,
  normalizeEventEnvelope,
  ingestRawEvent
};
