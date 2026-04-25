const ignorePipeError = (err) => {
  const code = String(err?.code || '');
  if (code === 'EPIPE' || code === 'EIO' || code === 'ERR_STREAM_DESTROYED') return true;
  return false;
};

if (process.stdout && typeof process.stdout.on === 'function') {
  process.stdout.on('error', (err) => {
    if (ignorePipeError(err)) return;
    throw err;
  });
}

if (process.stderr && typeof process.stderr.on === 'function') {
  process.stderr.on('error', (err) => {
    if (ignorePipeError(err)) return;
    throw err;
  });
}

const { app, BrowserWindow, ipcMain, session, desktopCapturer, systemPreferences, screen, globalShortcut, powerMonitor, nativeImage } = require('electron');
const crypto = require('crypto');
const path = require('path');
const os = require('os');
const fs = require('fs');
const fsPromises = fs.promises;

async function existsAsync(path) {
  try {
    await fsPromises.access(path);
    return true;
  } catch {
    return false;
  }
}
const { execFile } = require('child_process');
const axios = require('axios');
const Store = require('electron-store');
const sqlite3 = (process.env.NODE_ENV === 'production') ? require('sqlite3') : require('sqlite3').verbose();
const ingestion = require('./services/ingestion');
const { ingestRawEvent } = ingestion;
const express = require('express');
const FormData = require('form-data');
require('dotenv').config(); // Load environment variables
const db = require('./services/db');
const engine = require('./services/agent/intelligence-engine');
const extractor = require('./services/extractors/openLoopExtractor');
const scoring = require('./services/scoring');
const { answerChatQuery } = require('./services/agent/chat-engine');
const { buildRadarState } = require('./services/agent/radar-engine');
const { buildHybridGraphRetrieval } = require('./services/agent/hybrid-graph-retrieval');
const { buildRetrievalThought } = require('./services/agent/retrieval-thought-system');
const { upsertMemoryNode } = require('./services/agent/graph-store');
const { generateEmbedding } = require('./services/embedding-engine');
const { generateTopTodosFromMemoryQuery, generateAndPersistTasksFromLLM } = require('./services/agent/suggestion-engine');
const { getLatestRecursiveImprovementLog } = require('./services/agent/recursive-improvement-engine');
const { getRelationshipContactDetail } = require('./services/relationship-graph');
const { getRelationshipContacts } = require('./services/relationship-graph');
const { syncAppleContactsIntoRelationshipGraph } = require('./services/relationship-graph');
const { syncGoogleContactsIntoRelationshipGraph } = require('./services/relationship-graph');
const { updateRelationshipContactProfile } = require('./services/relationship-graph');
const { resetZeroBaseMemory } = require('./services/agent/zero-base-memory');
const { runDailyInsights } = require('./services/agent/intelligence-engine');
const { runEpisodeJob } = require('./services/agent/intelligence-engine');
const { runHourlySemanticPulse } = require('./services/agent/intelligence-engine');
const { runLivingCoreJob } = require("./services/agent/intelligence-engine");
const { runRecursiveImprovementCycle } = require('./services/agent/recursive-improvement-engine');
const { runRelationshipGraphJob } = require('./services/relationship-graph');
const { runSemanticSummaryWindow } = require('./services/agent/intelligence-engine');
const { runWeeklyInsightJob } = require('./services/agent/intelligence-engine');
// const performanceMonitor = require('./performance-monitor'); // Temporarily disabled to test startup

// ── Summarizer services ──────────────────────────────────────────────────────
const { runInitialSync, searchSummaries } = require('./services/summarizer/initialSync');
const { generateTodaySummaryWithContext } = require('./services/summarizer/aiDailySummary');
const { buildDailySummaries } = require('./services/summarizer/dailySummary');
const { planNextAction, normalizeDesktopGoal } = require('./services/agent/agentPlanner');
const { checkAccessibilityPermission, observeDesktopState, executeDesktopAction, openAccessibilitySettings, openScreenRecordingSettings } = require('./services/desktop-control');
const { ensureManagedBrowser, observeManagedBrowserState, executeManagedBrowserAction, getManagedBrowserStatus } = require('./services/browser-driver');
const {
  buildGlobalGraph,
  detectTasks,
  generateSuggestionFromGraph,
  generateCoreGlobal, callLLM
} = require('./services/agent/intelligence-engine');
const {
  normalizeSuggestion,
  rankAndLimitSuggestions
} = require('./services/agent/intent-first-suggestions');
const { rebuildInvertedIndex } = require('./services/summarizer/indexing');

// Initialize electron-store for data persistence
const store = new Store();
const RELATIONSHIP_FEATURE_ENABLED = false;
const storeDebounceTimers = new Map();
function debouncedStoreSet(key, value, delay = 2000) {
  if (storeDebounceTimers.has(key)) {
    clearTimeout(storeDebounceTimers.get(key));
  }
  const timer = setTimeout(() => {
    try {
      store.set(key, value);
    } catch (e) {
      console.warn(`[Store] Debounced set failed for ${key}:`, e.message);
    }
    storeDebounceTimers.delete(key);
  }, delay);
  storeDebounceTimers.set(key, timer);
}

// ── Global safety net: prevent background job crashes from killing the IPC bridge ──
process.on('uncaughtException', (err) => {
  try { console.error('[Main] Uncaught exception (process kept alive):', err?.message || err, err?.stack || ''); } catch (_) {}
});
process.on('unhandledRejection', (reason) => {
  try { console.error('[Main] Unhandled promise rejection (kept alive):', reason?.message || reason); } catch (_) {}
});

function withTimeout(promise, timeoutMs, label = 'operation') {
  let timer = null;
  return Promise.race([
    Promise.resolve(promise),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    })
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

const DEFAULT_CHAT_TIMEOUT_MS = 600000;
const CHAT_STEP_EMIT_INTERVAL_MS = 250;
const activeChatRequestsBySender = new Map();
const activeChatRequestRegistry = new Map();
const queuedChatPersistenceKeys = new Set();
let chatPersistenceQueue = Promise.resolve();

function makeChatRequestKey(senderId, requestId) {
  return `${senderId}:${requestId}`;
}

function startActiveChatRequest(senderId, requestId) {
  const key = makeChatRequestKey(senderId, requestId);
  appInteractionState.chatActive = true;
  markAppInteraction("chat-start");
  const previousKey = activeChatRequestsBySender.get(senderId);
  if (previousKey && previousKey !== key) {
    const previous = activeChatRequestRegistry.get(previousKey);
    if (previous) previous.cancelled = true;
  }
  const record = {
    senderId,
    requestId,
    key,
    cancelled: false,
    startedAt: Date.now(),
    lastStepEmitAt: 0
  };
  activeChatRequestsBySender.set(senderId, key);
  activeChatRequestRegistry.set(key, record);
  return record;
}

function getActiveChatRequest(senderId, requestId) {
  return activeChatRequestRegistry.get(makeChatRequestKey(senderId, requestId)) || null;
}

function cancelActiveChatRequest(senderId, requestId) {
  const record = getActiveChatRequest(senderId, requestId);
  if (!record) return false;
  record.cancelled = true;
  return true;
}

function finishActiveChatRequest(senderId, requestId) {
  const key = makeChatRequestKey(senderId, requestId);
  appInteractionState.chatActive = true;
  markAppInteraction("chat-start");
  const activeKey = activeChatRequestsBySender.get(senderId);
  if (activeKey === key) activeChatRequestsBySender.delete(senderId);
  activeChatRequestRegistry.delete(key);
  if (activeChatRequestRegistry.size === 0) appInteractionState.chatActive = false;
}

function compactChatStepPayload(data = {}, requestId) {
  const payload = { ...data, requestId };
  if (Array.isArray(payload.preview_items)) payload.preview_items = payload.preview_items.slice(0, 3);
  if (payload.trace && Array.isArray(payload.trace)) delete payload.trace;
  if (payload.stage_trace) delete payload.stage_trace;
  if (payload.thinking_trace) delete payload.thinking_trace;
  return payload;
}

function enqueueChatPersistence(key, task) {
  if (!key || typeof task !== 'function' || queuedChatPersistenceKeys.has(key)) return;
  queuedChatPersistenceKeys.add(key);
  chatPersistenceQueue = chatPersistenceQueue
    .then(async () => {
      try {
        await task();
      } catch (error) {
        console.warn('[chat-memory] queued persistence failed:', error?.message || error);
      } finally {
        queuedChatPersistenceKeys.delete(key);
      }
    })
    .catch((error) => {
      queuedChatPersistenceKeys.delete(key);
      console.warn('[chat-memory] queue failure:', error?.message || error);
    });
}

const KNOWN_BROWSER_APPS = new Set([
  'google chrome',
  'chrome',
  'safari',
  'arc',
  'brave browser',
  'brave',
  'microsoft edge',
  'edge',
  'firefox'
]);

let browserHistoryCache = {
  fetchedAt: 0,
  urls: []
};
let browserHistoryRefreshPromise = null;
let browserHistoryRefreshMeta = {
  lastAttemptAt: 0,
  lastSuccessAt: 0,
  lastError: null,
  lastErrorAt: 0
};

const SCREENSHOT_BROWSER_HISTORY_MAX_AGE_MS = 2 * 60 * 1000;
const BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS = 10 * 60 * 1000;
const BROWSER_HISTORY_REFRESH_MIN_GAP_MS = 5 * 60 * 1000;
const BROWSER_HISTORY_REFRESH_TIMEOUT_MS = 12000;

function updateMemoryGraphHealth(patch = {}) {
  debouncedStoreSet('memoryGraphHealth', {
    ...(store.get('memoryGraphHealth') || {}),
    ...patch
  });
}

function emitMemoryGraphUpdate(payload = {}) {
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('memory-graph-update', {
      ...payload,
      timestamp: payload.timestamp || Date.now()
    });
  }
}

function isBrowserAppName(appName = '') {
  return KNOWN_BROWSER_APPS.has(String(appName || '').trim().toLowerCase());
}

function normalizeBrowserHistoryItem(item = {}) {
  const url = String(item.url || '').trim();
  if (!url) return null;
  const timestamp = Number(item.timestamp || item.last_visit_time || item.captured_at || 0);
  const domain = (() => {
    try {
      return new URL(url).hostname.replace(/^www\./, '').toLowerCase();
    } catch (_) {
      return String(item.domain || '').trim().toLowerCase();
    }
  })();
  return {
    url,
    title: String(item.title || domain || url).trim(),
    domain,
    timestamp: Number.isFinite(timestamp) ? timestamp : 0,
    browser: String(item.browser || item.app || 'Browser').trim(),
    visitCount: Number(item.visitCount || item.visit_count || 1) || 1
  };
}

function flattenStoredBrowserHistory() {
  const rows = [];
  const direct = global.extensionData?.urls;
  if (Array.isArray(direct)) rows.push(...direct);

  const userDataUrls = store.get('userData')?.extensionData?.urls;
  if (Array.isArray(userDataUrls)) rows.push(...userDataUrls);

  const rawStore = store.get('extensionData');
  if (Array.isArray(rawStore?.urls)) {
    rows.push(...rawStore.urls);
  } else if (rawStore && typeof rawStore === 'object') {
    for (const value of Object.values(rawStore)) {
      if (Array.isArray(value?.urls)) rows.push(...value.urls);
    }
  }

  const deduped = [];
  const seen = new Set();
  for (const item of rows) {
    const normalized = normalizeBrowserHistoryItem(item);
    if (!normalized) continue;
    const bucket = Math.floor((normalized.timestamp || 0) / 60000);
    const key = `${normalized.url}|${bucket}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(normalized);
  }
  return deduped.sort((a, b) => b.timestamp - a.timestamp);
}

function updateBrowserHistoryCache(rows = [], source = 'background') {
  const normalized = (rows || []).map(normalizeBrowserHistoryItem).filter(Boolean);
  if (!normalized.length) return browserHistoryCache.urls;
  browserHistoryCache = {
    fetchedAt: Date.now(),
    urls: normalized
  };
  updateMemoryGraphHealth({
    browserHistoryCacheSize: normalized.length,
    browserHistoryCacheSource: source,
    browserHistoryFetchedAt: new Date(browserHistoryCache.fetchedAt).toISOString()
  });
  return normalized;
}

function getCachedBrowserHistory({ maxAgeMs = SCREENSHOT_BROWSER_HISTORY_MAX_AGE_MS } = {}) {
  const now = Date.now();
  if (browserHistoryCache.urls.length && (now - browserHistoryCache.fetchedAt) < maxAgeMs) {
    return browserHistoryCache.urls;
  }

  const stored = flattenStoredBrowserHistory();
  if (stored.length) {
    updateBrowserHistoryCache(stored, 'stored_snapshot');
  }

  return browserHistoryCache.urls;
}

async function refreshBrowserHistory(options = {}) {
  const force = options?.force === true;
  const reason = String(options?.reason || 'background');
  const timeoutMs = Math.max(2000, Number(options?.timeoutMs || BROWSER_HISTORY_REFRESH_TIMEOUT_MS));
  const minGapMs = Math.max(1000, Number(options?.minGapMs || BROWSER_HISTORY_REFRESH_MIN_GAP_MS));
  const maxAgeMs = Math.max(1000, Number(options?.maxAgeMs || BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS));
  const now = Date.now();

  if (!force && browserHistoryCache.urls.length && (now - browserHistoryCache.fetchedAt) < maxAgeMs) {
    console.log(`[BrowserHistory] Serving cached history for ${reason}; age=${Math.round((now - browserHistoryCache.fetchedAt) / 1000)}s`);
    return browserHistoryCache.urls;
  }

  if (!force && browserHistoryRefreshPromise) {
    console.log(`[BrowserHistory] Reusing in-flight refresh for ${reason}`);
    return browserHistoryRefreshPromise;
  }

  if (!force && browserHistoryRefreshMeta.lastAttemptAt && (now - browserHistoryRefreshMeta.lastAttemptAt) < minGapMs) {
    console.log(`[BrowserHistory] Skipping refresh for ${reason}; min gap not reached`);
    return browserHistoryCache.urls;
  }

  browserHistoryRefreshMeta.lastAttemptAt = now;
  updateMemoryGraphHealth({
    browserHistoryRefreshStatus: 'running',
    browserHistoryRefreshReason: reason,
    browserHistoryRefreshStartedAt: new Date(now).toISOString()
  });

  browserHistoryRefreshPromise = (async () => {
    const startedAt = Date.now();
    try {
      const fresh = await withTimeout(getBrowserHistory(), timeoutMs, 'refreshBrowserHistory');
      const normalized = updateBrowserHistoryCache(fresh, reason);
      browserHistoryRefreshMeta.lastSuccessAt = Date.now();
      browserHistoryRefreshMeta.lastError = null;
      browserHistoryRefreshMeta.lastErrorAt = 0;
      const durationMs = Date.now() - startedAt;
      console.log(`[BrowserHistory] Refreshed ${normalized.length} URLs for ${reason} in ${durationMs}ms`);
      updateMemoryGraphHealth({
        browserHistoryRefreshStatus: 'idle',
        browserHistoryRefreshDurationMs: durationMs,
        browserHistoryLastRefreshAt: new Date(browserHistoryRefreshMeta.lastSuccessAt).toISOString()
      });
      emitMemoryGraphUpdate({
        type: 'job_status',
        job: 'browser_history_refresh',
        status: 'completed',
        duration_ms: durationMs,
        count: normalized.length
      });
      return normalized;
    } catch (error) {
      browserHistoryRefreshMeta.lastError = error?.message || String(error);
      browserHistoryRefreshMeta.lastErrorAt = Date.now();
      console.warn(`[BrowserHistory] Failed refresh for ${reason}:`, browserHistoryRefreshMeta.lastError);
      updateMemoryGraphHealth({
        browserHistoryRefreshStatus: 'error',
        browserHistoryRefreshError: browserHistoryRefreshMeta.lastError,
        browserHistoryRefreshErrorAt: new Date(browserHistoryRefreshMeta.lastErrorAt).toISOString()
      });
      emitMemoryGraphUpdate({
        type: 'job_status',
        job: 'browser_history_refresh',
        status: 'error',
        error: browserHistoryRefreshMeta.lastError
      });
      return browserHistoryCache.urls;
    } finally {
      browserHistoryRefreshPromise = null;
    }
  })();

  return browserHistoryRefreshPromise;
}

function scoreHistoryItemForScreenshot(item = {}, screenshotTs, appName = '', windowTitle = '') {
  const titleLower = String(windowTitle || '').toLowerCase();
  const itemTitleLower = String(item.title || '').toLowerCase();
  const domainLower = String(item.domain || '').toLowerCase();
  const browserActive = isBrowserAppName(appName);
  const deltaMs = Math.abs(Number(screenshotTs || 0) - Number(item.timestamp || 0));
  const allowedWindowMs = browserActive ? 15 * 60 * 1000 : 3 * 60 * 1000;
  if (!deltaMs || deltaMs > allowedWindowMs) return 0;

  let score = 1 - Math.min(1, deltaMs / allowedWindowMs);
  if (browserActive) score += 0.35;
  if (domainLower && titleLower.includes(domainLower)) score += 0.45;
  if (itemTitleLower && titleLower.includes(itemTitleLower.slice(0, 60))) score += 0.55;
  if (itemTitleLower && itemTitleLower.split(/\s+/).filter((token) => token.length >= 5).some((token) => titleLower.includes(token))) score += 0.18;
  if (String(item.browser || '').toLowerCase() === String(appName || '').toLowerCase()) score += 0.12;
  return Number(score.toFixed(3));
}

async function findAssociatedUrlsForScreenshot({ timestamp, appName = '', windowTitle = '', limit = 5 } = {}) {
  const history = getCachedBrowserHistory({ maxAgeMs: SCREENSHOT_BROWSER_HISTORY_MAX_AGE_MS });
  if (!Array.isArray(history) || !history.length) return [];

  return history
    .map((item) => ({
      ...item,
      association_score: scoreHistoryItemForScreenshot(item, timestamp, appName, windowTitle)
    }))
    .filter((item) => item.association_score >= 0.2)
    .sort((a, b) => {
      if (b.association_score !== a.association_score) return b.association_score - a.association_score;
      return Math.abs((timestamp || 0) - (a.timestamp || 0)) - Math.abs((timestamp || 0) - (b.timestamp || 0));
    })
    .slice(0, Math.max(1, Number(limit || 5)))
    .map((item) => ({
      url: item.url,
      title: item.title,
      domain: item.domain,
      browser: item.browser,
      timestamp: item.timestamp,
      visitCount: item.visitCount,
      association_score: item.association_score
    }));
}

function maybeHandleLocalChatToolQuery(query = '') {
  const normalized = String(query || '').trim();
  const lower = normalized.toLowerCase();
  const asksTime = /\b(what time is it|what's the time|current time|time right now|time now|local time)\b/.test(lower);
  const asksDate = /\b(what(?:'s| is)? the date|today'?s date|current date|date today|what day is it|which day is it|what day today)\b/.test(lower);
  const asksTimezone = /\b(timezone|time zone)\b/.test(lower);

  if (!asksTime && !asksDate && !asksTimezone) return null;

  const now = new Date();
  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'local time';
  const fullDate = new Intl.DateTimeFormat(undefined, { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }).format(now);
  const clockTime = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit', timeZoneName: 'short' }).format(now);

  const parts = [];
  if (asksDate && asksTime) {
    parts.push(`It is ${fullDate}, ${clockTime}.`);
  } else if (asksDate) {
    parts.push(`Today is ${fullDate}.`);
  } else if (asksTime) {
    parts.push(`It is ${clockTime}.`);
  }
  if (asksTimezone) {
    parts.push(`Your current timezone is ${timezone}.`);
  }

  const toolResult = {
    tool: 'local_datetime',
    timezone,
    iso: now.toISOString(),
    date: fullDate,
    time: clockTime
  };

  return {
    content: parts.join(' ').trim(),
    tool_result: toolResult,
    retrieval: {
      source_mode: 'tool_only',
      usedSources: ['local_datetime'],
      evidence_count: 1,
      tool_result: toolResult
    },
    thinking_trace: {
      thinking_summary: 'Handled locally with the built-in date/time tool.',
      filters: [],
      search_queries: { context: [], messages: [], lexical: [], web: [] },
      results_summary: {
        headline: 'Resolved with a local tool call.',
        details: [parts.join(' ').trim()]
      },
      data_sources: ['Local date/time tool'],
      stage_trace: [
        {
          step: 'local_tool',
          label: 'Local tool',
          status: 'completed',
          detail: `Resolved with ${toolResult.tool}.`
        }
      ],
      reasoning_chain: [
        {
          step: 'tool_router',
          summary: 'The query matched a simple date/time request and was answered locally.'
        }
      ]
    }
  };
}

function getStoredRadarState() {
  const state = store.get('radarState') || null;
  if (state && typeof state === 'object') return sanitizeRadarStateForFeatures(state);
  const suggestions = Array.isArray(store.get('suggestions')) ? store.get('suggestions') : [];
  const persistentTodos = Array.isArray(store.get('persistentTodos')) ? store.get('persistentTodos') : [];
  const centralSignals = suggestions.filter((item) => String(item.signal_type || '').toLowerCase() === 'central');
  const relationshipSignals = RELATIONSHIP_FEATURE_ENABLED
    ? suggestions.filter((item) => String(item.signal_type || '').toLowerCase() === 'relationship' || looksRelationshipSuggestion(item))
    : [];
  const todoSignals = [
    ...suggestions.filter((item) => String(item.signal_type || '').toLowerCase() === 'todo'),
    ...persistentTodos.filter((item) => !item?.completed).map((item) => ({
      ...item,
      signal_type: 'todo',
      category: item.category || 'work'
    }))
  ];
  return {
    generated_at: new Date().toISOString(),
    allSignals: [...centralSignals, ...relationshipSignals, ...todoSignals],
    centralSignals,
    relationshipSignals,
    todoSignals,
    sections: {
      central: { status: 'ready', count: centralSignals.length },
      relationship: { status: 'ready', count: relationshipSignals.length },
      todo: { status: 'ready', count: todoSignals.length }
    }
  };
}

function persistRadarState(radarState = {}) {
  const incomingAllSignals = Array.isArray(radarState.allSignals) ? radarState.allSignals : [];
  const allSignals = RELATIONSHIP_FEATURE_ENABLED
    ? incomingAllSignals
    : incomingAllSignals.filter((item) => String(item?.signal_type || '').toLowerCase() !== 'relationship' && !looksRelationshipSuggestion(item));
  const clean = sanitizeRadarStateForFeatures({
    generated_at: radarState.generated_at || new Date().toISOString(),
    allSignals,
    centralSignals: Array.isArray(radarState.centralSignals) ? radarState.centralSignals : [],
    relationshipSignals: Array.isArray(radarState.relationshipSignals) ? radarState.relationshipSignals : [],
    todoSignals: Array.isArray(radarState.todoSignals) ? radarState.todoSignals : [],
    sections: radarState.sections || {}
  });
  
  // Use debounced store set to avoid blocking the main thread with large JSON writes
  debouncedStoreSet('radarState', clean);
  debouncedStoreSet('suggestions', clean.allSignals.filter((item) => !item?.completed));
  return clean;
}

function sanitizeRadarStateForFeatures(radarState = {}) {
  if (RELATIONSHIP_FEATURE_ENABLED) return radarState;
  const notRelationship = (item) => String(item?.signal_type || '').toLowerCase() !== 'relationship' && !looksRelationshipSuggestion(item);
  const centralSignals = Array.isArray(radarState.centralSignals) ? radarState.centralSignals.filter(notRelationship) : [];
  const todoSignals = Array.isArray(radarState.todoSignals) ? radarState.todoSignals.filter(notRelationship) : [];
  return {
    ...radarState,
    allSignals: Array.isArray(radarState.allSignals) ? radarState.allSignals.filter(notRelationship) : [...centralSignals, ...todoSignals],
    centralSignals,
    relationshipSignals: [],
    todoSignals,
    sections: {
      ...(radarState.sections || {}),
      relationship: { status: 'disabled', count: 0 }
    }
  };
}

async function persistDailyBriefSemanticNode(summary = {}) {
  const date = String(summary?.date || '').slice(0, 10);
  if (!date) return null;
  const id = `sem_daily_brief_${date.replace(/[^0-9]/g, '')}`;
  const now = new Date().toISOString();
  const sourceRefs = Array.from(new Set((summary?.events || []).map((item) => item?.id).filter(Boolean))).slice(0, 96);
  const title = `Daily brief ${date}`;
  const narrative = String(summary?.narrative || '').trim();
  const accomplishments = Array.isArray(summary?.suggestions)
    ? summary.suggestions.map((item) => item?.title || item?.task || item?.description).filter(Boolean).slice(0, 8)
    : [];
  const metadata = {
    date,
    generated_at: summary?.generated_at || now,
    counts: summary?.counts || {},
    accomplishments,
    source_refs: sourceRefs,
    latest_activity_at: summary?.generated_at || now
  };

  await db.runQuery(
    `INSERT OR REPLACE INTO memory_nodes
     (id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date, anchor_at)
     VALUES (?, 'semantic', 'daily_brief', ?, ?, ?, ?, ?, ?, ?, ?,
             COALESCE((SELECT created_at FROM memory_nodes WHERE id = ?), ?), ?,
             COALESCE((SELECT embedding FROM memory_nodes WHERE id = ?), '[]'), ?, ?)`,
    [
      id,
      title,
      narrative || 'Daily work summary',
      [
        title,
        narrative ? `Narrative: ${narrative}` : '',
        accomplishments.length ? `Accomplishments: ${accomplishments.join('; ')}` : '',
        Object.keys(metadata.counts || {}).length ? `Counts: ${Object.entries(metadata.counts).map(([key, value]) => `${key}=${value}`).join(', ')}` : ''
      ].filter(Boolean).join('\n'),
      0.84,
      'active',
      JSON.stringify(sourceRefs),
      JSON.stringify(metadata),
      'daily_brief_v1',
      id,
      now,
      now,
      id,
      date,
      `${date}T20:00:00.000Z`
    ]
  ).catch((error) => {
    console.warn('[daily-brief-node] Failed to persist semantic daily brief:', error?.message || error);
  });

  return { id, title };
}

function uniqById(items = []) {
  const out = [];
  const seen = new Set();
  for (const item of items || []) {
    if (!item) continue;
    const key = String(item.id || `${item.title || ''}|${item.signal_type || ''}|${item.category || ''}`);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}


// Express server for OAuth callback
const oauthApp = express();
let oauthPort = process.env.OAUTH_PORT || 3002; // Moved from 3001 to avoid overlap with WebSocket server

let mainWindow;
let authWindow;
let voiceHudWindow = null;
let pendingAITasks = [];
let sensorCaptureTimer = null;
let periodicScreenshotTimer = null;
let periodicScreenshotWatchdogTimer = null;
let sensorCaptureInProgress = false;
let sensorCaptureStartedAt = 0;
let activeVoiceSession = null;
let activeStudySession = null;
const DEFAULT_VOICE_SHORTCUT = 'CommandOrControl+Shift+Space';
const LEGACY_VOICE_SHORTCUT = 'CommandOrControl+Space';
const PLANNER_STEP_THROTTLE_MS = 700;
const SCREENSHOT_RETENTION_DAYS = 36500;

function inferStudySignal(text = '', event = {}) {
  const haystack = [
    text,
    event.activeApp,
    event.activeWindowTitle,
    event.study_goal,
    event.study_subject
  ].map((value) => String(value || '').toLowerCase()).join(' ');

  if (!haystack.trim()) return null;
  if (/\b(exam|quiz|flashcard|anki|revision|review|practice test|problem set)\b/.test(haystack)) return 'reviewing';
  if (/\b(reading|chapter|article|paper|lecture notes|textbook|research)\b/.test(haystack)) return 'reading';
  if (/\b(homework|assignment|submit|due|canvas|classroom|instructure|exercise)\b/.test(haystack)) return 'task';
  if (/\b(study|course|class|lesson|learn|vocab|thesis)\b/.test(haystack)) return 'study';
  return null;
}

// Memory Graph Processing Timers
let episodeGenerationTimer = null;
let suggestionEngineTimer = null;
let semanticsTimer = null;
let dailyInsightTimer = null;
let weeklyInsightTimer = null;
let livingCoreTimer = null;
let semanticsPulseTimer = null;
let relationshipGraphTimer = null;
let episodeJobLock = false;
let suggestionJobLock = false;
let lastSuggestionLockSkipLogAt = 0;
let suggestionRunQueued = false;
let lastCaptureSuggestionTriggerAt = 0;
const MAX_PRACTICAL_SUGGESTIONS = 7;
const CAPTURE_TRIGGER_MIN_INTERVAL_MS = 60 * 60 * 1000;
// EMERGENCY CPU THROTTLING - Disable most background processes
const EMERGENCY_THROTTLE_ENABLED = true;
const PERIODIC_SCREENSHOT_INTERVAL_MS = EMERGENCY_THROTTLE_ENABLED ? 5 * 60 * 1000 : 45 * 1000; // 5 minutes when throttled
const SUGGESTION_REFRESH_INTERVAL_MINUTES = EMERGENCY_THROTTLE_ENABLED ? 60 : 30; // 1 hour when throttled
const PERIODIC_SCREENSHOT_WAKE_DELAY_MS = 15 * 1000;       // 15s after wake before first capture
const LOW_POWER_OCR_MIN_INTERVAL_MS = 15 * 60 * 1000; // INCREASED from 5 min
const LOW_POWER_HEAVY_JOB_MIN_GAP_MS = 60 * 60 * 1000;
const CAPTURE_STALE_LOCK_MS = 30 * 1000;  // 30s — clear hung OCR/screencapture fast
const APP_ACTIVE_CAPTURE_COOLDOWN_MS = 3 * 60 * 1000;
const SCREENSHOT_WATCHDOG_INTERVAL_MS = 60 * 1000;
const CAPTURE_VISUAL_DIFF_THRESHOLD = Number(process.env.CAPTURE_VISUAL_DIFF_THRESHOLD || 0.15); // INCREASED from 0.05
const STARTUP_HEAVY_JOB_DELAY_MS = Math.max(5 * 60 * 1000, Number(process.env.STARTUP_HEAVY_JOB_DELAY_MS || 10 * 60 * 1000));
const STARTUP_INITIAL_SYNC_DELAY_MS = Math.max(10 * 60 * 1000, Number(process.env.STARTUP_INITIAL_SYNC_DELAY_MS || 20 * 60 * 1000));
const GSUITE_SYNC_INTERVAL_MS = Math.max(5 * 60 * 1000, Number(process.env.GSUITE_SYNC_INTERVAL_MS || 5 * 60 * 1000));
const HEAVY_JOB_RETRY_COOLDOWN_MS = 60 * 1000;
const STARTUP_SOURCE_WARMUP_DELAY_MS = STARTUP_HEAVY_JOB_DELAY_MS;
const STARTUP_MEMORY_GRAPH_DELAY_MS = STARTUP_HEAVY_JOB_DELAY_MS + (90 * 1000);
const STARTUP_AUTOMATION_DELAY_MS = STARTUP_HEAVY_JOB_DELAY_MS + (3 * 60 * 1000);
const STARTUP_RECURSIVE_DELAY_MS = STARTUP_HEAVY_JOB_DELAY_MS + (4 * 60 * 1000);
let lastLowPowerOCRAt = 0;
let lastEpisodeHeavyRunAt = 0;
let lastSuggestionHeavyRunAt = 0;
let lastAcceptedCaptureFingerprint = null;
let screenshotsPausedForDisplayOff = false;
let periodicScreenshotNextDueAt = 0;
let periodicScreenshotRunning = false;
let periodicScreenshotPauseReason = '';
const performanceState = {
  onBattery: false,
  thermalState: 'unknown'
};
const appInteractionState = {
  focused: false,
  minimized: false,
  lastInteractionAt: 0,
  chatActive: false
};
const heavyJobState = {
  activeJob: null,
  startedAt: 0,
  perJobLastSkipAt: {}
};
const pendingHeavyJobs = new Map();
const HEAVY_JOB_QUEUE_ORDER = [
  'gsuite_sync',
  'radar_generation',
  'episode_generation',
  'relationship_graph',
  'relationship_graph_backfill',
  'semantic_window',
  'semantic_pulse',
  'daily_insight',
  'weekly_insight',
  'living_core'
];

function getPerformanceMode() {
  const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
  if (idleTime > 300) return 'deep-idle'; // 5m+
  if (Boolean(performanceState.onBattery) || ['serious', 'critical'].includes(String(performanceState.thermalState || '').toLowerCase())) {
    return 'reduced';
  }
  return 'normal';
}

function isReducedLoadMode() {
  const mode = getPerformanceMode();
  return mode === 'reduced' || mode === 'deep-idle';
}

function getPeriodicScreenshotIntervalMs(mode = getPerformanceMode()) {
  if (mode === 'deep-idle') return 45 * 60 * 1000;   // 45 min when idle - INCREASED
  if (mode === 'reduced')   return 20 * 60 * 1000;    // 20 min on battery/thermal - INCREASED
  return PERIODIC_SCREENSHOT_INTERVAL_MS;             // 45 sec normal - USER REQUESTED
}

function getPeriodicScreenshotWakeDelayMs(mode = getPerformanceMode()) {
  if (mode === 'deep-idle') return 60 * 1000; // 1 min after wake in idle
  if (mode === 'reduced')   return 30 * 1000; // 30s after wake in reduced
  return PERIODIC_SCREENSHOT_WAKE_DELAY_MS;   // 15s after wake in normal
}

function canRunHeavyJob(lastRunAt = 0) {
  const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
  const mode = getPerformanceMode();
  
  // More aggressive throttling in reduced modes
  if (mode === 'deep-idle') {
    return idleTime > 600 && (Date.now() - lastRunAt) > (4 * 60 * 60 * 1000); // 4 hours in deep idle
  }
  if (mode === 'reduced') {
    return idleTime > 180 && (Date.now() - lastRunAt) > (2 * 60 * 60 * 1000); // 2 hours in reduced mode
  }
  
  // Even in normal mode, be more conservative
  return idleTime > 60 && (Date.now() - lastRunAt) > LOW_POWER_HEAVY_JOB_MIN_GAP_MS;
}

function shouldDeferBackgroundWork(label = 'background') {
  const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
  const appBusy = isAppInteractionHot() || (activeChatRequestRegistry && activeChatRequestRegistry.size > 0);
  const mode = getPerformanceMode();
  
  // More aggressive deferral based on performance mode
  let deferThreshold = 30; // default 30 seconds
  if (mode === 'reduced') deferThreshold = 120; // 2 minutes
  if (mode === 'deep-idle') deferThreshold = 300; // 5 minutes
  
  const defer = idleTime < deferThreshold || appBusy || mode === 'reduced';
  
  // Only log sporadically to avoid console thrashing (log at most once per minute per label)
  if (defer && Math.random() < 0.01) console.log(`[${label}] Deferring heavy work; idle=${idleTime}s appBusy=${appBusy} mode=${mode}`);
  return defer;
}

function beginHeavyJob(jobName, options = {}) {
  const name = String(jobName || 'heavy_job');
  const now = Date.now();
  if (heavyJobState.activeJob && heavyJobState.activeJob !== name) {
    const lastSkipAt = Number(heavyJobState.perJobLastSkipAt[name] || 0);
    if ((now - lastSkipAt) > HEAVY_JOB_RETRY_COOLDOWN_MS) {
      console.log(`[${name}] Skipping because ${heavyJobState.activeJob} is already running`);
      heavyJobState.perJobLastSkipAt[name] = now;
    }
    updateMemoryGraphHealth({
      heavyJobActive: heavyJobState.activeJob,
      lastHeavyJobSkipped: name,
      lastHeavyJobSkippedAt: new Date(now).toISOString()
    });
    emitMemoryGraphUpdate({
      type: 'job_status',
      job: name,
      status: 'skipped',
      reason: 'busy',
      blocked_by: heavyJobState.activeJob
    });
    return false;
  }
  heavyJobState.activeJob = name;
  heavyJobState.startedAt = now;
  updateMemoryGraphHealth({
    heavyJobActive: name,
    heavyJobStartedAt: new Date(now).toISOString()
  });
  emitMemoryGraphUpdate({
    type: 'job_status',
    job: name,
    status: 'running',
    source: options?.source || 'background'
  });
  return true;
}

function endHeavyJob(jobName, meta = {}) {
  const name = String(jobName || '');
  const now = Date.now();
  if (heavyJobState.activeJob === name) {
    const durationMs = Math.max(0, now - Number(heavyJobState.startedAt || now));
    heavyJobState.activeJob = null;
    heavyJobState.startedAt = 0;
    updateMemoryGraphHealth({
      heavyJobActive: null,
      lastHeavyJobCompleted: name,
      lastHeavyJobCompletedAt: new Date(now).toISOString(),
      lastHeavyJobDurationMs: durationMs
    });
    emitMemoryGraphUpdate({
      type: 'job_status',
      job: name,
      status: meta?.status || 'completed',
      duration_ms: durationMs,
      error: meta?.error || null
    });
    setTimeout(() => {
      drainPendingHeavyJobs();
    }, 150);
  }
}

function enqueueHeavyJob(jobName, runner, options = {}) {
  const name = String(jobName || 'heavy_job');
  if (pendingHeavyJobs.has(name)) return false;
  pendingHeavyJobs.set(name, {
    runner,
    source: options?.source || 'background',
    queuedAt: Date.now()
  });
  updateMemoryGraphHealth({
    pendingHeavyJobs: Array.from(pendingHeavyJobs.keys()),
    lastQueuedHeavyJob: name,
    lastQueuedHeavyJobAt: new Date().toISOString()
  });
  emitMemoryGraphUpdate({
    type: 'job_status',
    job: name,
    status: 'queued',
    source: options?.source || 'background'
  });
  return true;
}

function drainPendingHeavyJobs() {
  if (activeChatRequestRegistry && activeChatRequestRegistry.size > 0) return false;
  if (heavyJobState.activeJob || !pendingHeavyJobs.size) return false;
  const queue = Array.from(pendingHeavyJobs.entries());
  const ordered = HEAVY_JOB_QUEUE_ORDER.map((name) => queue.find(([key]) => key === name)).filter(Boolean);
  const fallback = queue.filter(([key]) => !HEAVY_JOB_QUEUE_ORDER.includes(key));
  const next = [...ordered, ...fallback][0];
  if (!next) return false;
  const [jobName, job] = next;
  pendingHeavyJobs.delete(jobName);
  updateMemoryGraphHealth({
    pendingHeavyJobs: Array.from(pendingHeavyJobs.keys())
  });
  Promise.resolve()
    .then(() => job.runner())
    .catch((error) => console.warn(`[${jobName}] Queued run failed:`, error?.message || error));
  return true;
}

function looksRelationshipSuggestion(item = {}) {
  const category = String(item.category || item.type || '').toLowerCase();
  const opportunityType = String(item.opportunity_type || '').toLowerCase();
  const displayPerson = String(item.display?.person || item.display?.target || '').trim();
  const haystack = [
    item.title,
    item.reason,
    item.description,
    item.trigger_summary,
    item.display?.headline,
    item.display?.summary,
    item.primary_action?.label,
    opportunityType,
    category
  ].map((value) => String(value || '').toLowerCase()).join(' ');

  if (displayPerson) return true;
  if (['followup', 'relationship', 'relationship_intelligence', 'social'].includes(category)) return true;
  if (/(reconnect|follow up|intro|introduction|meeting prep|brief|stakeholder|warm|cold|relationship|network|reach out|investor|client|partner|contact)/.test(haystack)) return true;
  return ['reconnect_risk', 'timely_follow_up', 'intro_opportunity', 'meeting_prep', 'value_add_share', 'emerging_connection'].includes(opportunityType);
}

function markAppInteraction(reason = 'interaction') {
  appInteractionState.lastInteractionAt = Date.now();
  if (reason) {
    debouncedStoreSet('lastAppInteraction', {
      reason,
      at: new Date(appInteractionState.lastInteractionAt).toISOString()
    });
  }
}

function isAppInteractionHot() {
  if (!mainWindow || mainWindow.isDestroyed()) return false;
  if (appInteractionState.minimized) return false;
  if (!appInteractionState.focused) return false;
  if (appInteractionState.chatActive) return true;
  return (Date.now() - Number(appInteractionState.lastInteractionAt || 0)) < APP_ACTIVE_CAPTURE_COOLDOWN_MS;
}

function updatePerformanceState(next = {}) {
  const oldMode = getPerformanceMode();
  Object.assign(performanceState, next || {});
  const newMode = getPerformanceMode();

  debouncedStoreSet("performanceState", {
    ...performanceState,
    mode: newMode,
    updated_at: new Date().toISOString()
  });

  if (oldMode !== newMode) {
    console.log(`[Performance] Mode changed from ${oldMode} to ${newMode}. Restarting capture timers with updated cadence.`);
    startSensorCaptureLoop(newMode);
    if (periodicScreenshotRunning && !screenshotsPausedForDisplayOff) {
      startPeriodicScreenshotCapture(newMode);
    }
    
    // Notify renderer so it can disable expensive CSS effects like blurs
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('performance-mode-changed', newMode);
    }
  }
}

function hydrateStudySessionFromStore() {
  if (activeStudySession) return activeStudySession;
  const stored = store.get('studySessionState') || null;
  if (stored && stored.status === 'active' && stored.session_id) {
    activeStudySession = {
      status: 'active',
      session_id: stored.session_id,
      goal: stored.goal || '',
      subject: stored.subject || '',
      started_at: stored.started_at || new Date().toISOString(),
      ended_at: null
    };
  } else {
    activeStudySession = {
      status: 'idle',
      session_id: null,
      goal: '',
      subject: '',
      started_at: null,
      ended_at: stored?.ended_at || null
    };
  }
  return activeStudySession;
}

function getStudySessionState() {
  return hydrateStudySessionFromStore();
}

function emitStudySessionUpdate() {
  const payload = getStudySessionState();
  if (mainWindow && mainWindow.webContents) {
    try {
      mainWindow.webContents.send('study-session-update', payload);
    } catch (_) {}
  }
}

function setStudySessionState(nextState = {}) {
  activeStudySession = {
    ...(getStudySessionState() || {}),
    ...(nextState || {})
  };
  debouncedStoreSet('studySessionState', activeStudySession);
  emitStudySessionUpdate();
  return activeStudySession;
}

function startScreenshotCleanupLoop() {
  const runCleanup = () => {
    // Keep the cleanup loop lightweight; we only clear stale temp references now.
    pendingAITasks = [];
    
    // Clean up old screenshot files to prevent disk space bloat
    const screenshotsDir = path.join(app.getPath('userData'), 'screenshots');
    const maxAge = 7 * 24 * 60 * 60 * 1000; // 7 days
    const now = Date.now();
    
    fs.readdir(screenshotsDir, (err, files) => {
      if (err) return;
      
      files.forEach(file => {
        if (file.endsWith('.png')) {
          const filePath = path.join(screenshotsDir, file);
          fs.stat(filePath, (statErr, stats) => {
            if (!statErr && (now - stats.mtime.getTime()) > maxAge) {
              fs.unlink(filePath, () => {}); // Async delete, ignore errors
            }
          });
        }
      });
    });
    
    // Force garbage collection if available
    if (global.gc) {
      global.gc();
    }
    
    // Clear any stale timers
    clearStaleTimers();
  };

  runCleanup();
  if (screenshotCleanupTimer) {
    clearInterval(screenshotCleanupTimer);
    screenshotCleanupTimer = null;
  }
  // Run cleanup every 12 hours instead of 6 hours to reduce disk I/O
  screenshotCleanupTimer = setInterval(runCleanup, 12 * 60 * 60 * 1000);
}

function clearStaleTimers() {
  // Clear any potentially stuck timers to prevent memory leaks
  const now = Date.now();
  const staleThreshold = 30 * 60 * 1000; // 30 minutes
  
  // Check for stale sensor capture
  if (sensorCaptureInProgress && (now - sensorCaptureStartedAt) > CAPTURE_STALE_LOCK_MS) {
    console.warn('[Cleanup] Clearing stale sensor capture lock');
    sensorCaptureInProgress = false;
    sensorCaptureStartedAt = 0;
  }
  
  // Clear pending AI tasks that are too old
  pendingAITasks = pendingAITasks.filter(task => {
    return (now - task.createdAt) < staleThreshold;
  });
}

// ── Graph Helpers ──────────────────────────────────────────────────────────
let memoryGraphCache = null;

function getGraph() {
  if (!memoryGraphCache) {
    // Only load from legacy store if SQLite transition is not complete or for small caches
    const legacyNodes = store.get('graphNodes');
    const legacyEdges = store.get('graphEdges');
    
    // Safety check: if legacy data is massive, it will crash Electron Main process
    // We prefer the SQLite DB for large datasets now.
    if (Array.isArray(legacyNodes) && legacyNodes.length > 2000) {
      console.warn(`[Graph] Legacy store contains ${legacyNodes.length} nodes. Skipping to save memory. Use SQLite DB.`);
      memoryGraphCache = { nodes: [], edges: [] };
    } else {
      memoryGraphCache = {
        nodes: legacyNodes || [],
        edges: legacyEdges || []
      };
    }
  }
  return memoryGraphCache;
}

function saveGraph(nodes, edges) {
  memoryGraphCache = { nodes, edges };
  debouncedStoreSet('graphNodes', nodes, 5000);
  debouncedStoreSet('graphEdges', edges, 5000);
}

function addNode(node) {
  const { nodes, edges } = getGraph();
  const idx = nodes.findIndex(n => n.id === node.id);
  if (idx !== -1) nodes[idx] = node;
  else nodes.push(node);
  saveGraph(nodes, edges);
}

function addEdge(from, to, relation, description) {
  const { nodes, edges } = getGraph();
  const edge = { from, to, relation, description };
  // Basic dedup
  const exists = edges.find(e => e.from === from && e.to === to && e.relation === relation);
  if (!exists) edges.push(edge);
  saveGraph(nodes, edges);
}

function getMemoryAppId(event) {
  switch (event?.type) {
    case 'email':
      return 'Gmail';
    case 'calendar_event':
      return 'Calendar';
    case 'doc':
    case 'spreadsheet':
    case 'slide':
      return 'Drive';
    case 'browser_history':
      return 'History';
    case 'screen_capture':
      return 'Sensors';
    default:
      return 'Misc';
  }
}

function buildRawNodeFromEvent(event) {
  return {
    id: event.id,
    type: 'raw_event',
    data: {
      appId: getMemoryAppId(event),
      event_type: event.type,
      title: event.title || '',
      text: event.text || '',
      date: event.date || '',
      timestamp: event.timestamp || null,
      people: event.people || [],
      metadata: event.metadata || {}
    }
  };
}

async function rebuildLayeredMemoryGraphFromEvents(events, apiKey) {
  const normalizedEvents = Array.isArray(events) ? events.filter(e => e && e.id) : [];
  const nodes = [];
  const edges = [];
  const nodeIds = new Set();
  const edgeIds = new Set();

  const pushNode = (node) => {
    if (!node?.id || nodeIds.has(node.id)) return;
    nodeIds.add(node.id);
    nodes.push(node);
  };
  const pushEdge = (from, to, relation, description) => {
    if (!from || !to || !relation) return;
    const key = `${from}|${to}|${relation}`;
    if (edgeIds.has(key)) return;
    edgeIds.add(key);
    edges.push({ from, to, relation, description });
  };

  const grouped = normalizedEvents.reduce((acc, event) => {
    const appId = getMemoryAppId(event);
    if (!acc[appId]) acc[appId] = [];
    acc[appId].push(event);
    return acc;
  }, {});

  normalizedEvents.forEach(event => pushNode(buildRawNodeFromEvent(event)));

  const currentGlobal = (store.get('proactiveMemory') || {}).core || '';
  const appCores = {};

  for (const [appId, items] of Object.entries(grouped)) {
    if (!items.length) continue;

    // Process app data with intelligence engine
    const appDataMap = { [appId]: { rawItems: items, appName: appId } };
    const graphResult = await engine.buildGlobalGraph({ appDataMap, apiKey, store });

    const appCoreId = `core_${appId.toLowerCase()}`;
    const semanticIdsByEpisode = new Map();
    const semanticIds = [];

    if (graphResult && graphResult.nodes) {
      console.log(`[rebuildLayeredMemoryGraphFromEvents] Built graph for ${appId}: ${graphResult.nodes.length} nodes`);

      // Extract app core from the graph results
      const appCoreNode = graphResult.nodes.find(n => n.type === 'app_core');
      if (appCoreNode) {
        appCores[appId] = appCoreNode.data.narrative || appCoreNode.data.title || `${appId} Core`;

        // Add App-Core Node
        pushNode({ id: appCoreId, type: 'app_core', data: { title: `${appId} Core`, narrative: appCores[appId] } });

        // Add Episode Nodes & collect semantic info
        const episodeNodes = graphResult.nodes.filter(n => n.type === 'episode');
        episodeNodes.forEach(ep => {
          pushNode({ id: ep.id, type: 'episode', data: ep.data });

          // Collect semantic nodes for this episode
          const episodeSemantics = graphResult.nodes.filter(n => n.type === 'semantic' && n.data.episode_id === ep.id);
          const semIds = [];
          episodeSemantics.forEach(sem => {
            pushNode({ id: sem.id, type: 'semantic', data: sem.data });
            semIds.push(sem.id);
            pushEdge(sem.id, ep.id, 'extracted_from', 'Semantic understanding extracted from episode.');
          });

          semanticIdsByEpisode.set(ep.id, semIds);
          semanticIds.push(...semIds);
        });

        // Add Insight Nodes
        const insightNodes = graphResult.nodes.filter(n => n.type === 'insight');
        insightNodes.forEach(ins => {
          pushNode({ id: ins.id, type: 'insight', data: ins.data });
          pushEdge(ins.id, appCoreId, 'shapes_core', 'High-level insight shapes app core understanding.');
        });
      }
    }
  }

  const newGlobal = await generateCoreGlobal(appCores, currentGlobal, apiKey);
  pushNode({
    id: 'global_core',
    type: 'global_core',
    data: {
      title: 'Global Core Memory',
      narrative: newGlobal
    }
  });

  Object.keys(appCores).forEach(appId => {
    pushEdge(`core_${appId.toLowerCase()}`, 'global_core', 'contributes_to_core', 'App core contributes to global core memory.');
  });

  saveGraph(nodes, edges);
  store.set('proactiveMemory', {
    ...(store.get('proactiveMemory') || {}),
    core: newGlobal
  });

  return { nodes, edges, core: newGlobal };
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    }
  });

  mainWindow.loadFile('renderer/index.html');

  appInteractionState.focused = mainWindow.isFocused();
  appInteractionState.minimized = mainWindow.isMinimized();
  if (appInteractionState.focused) markAppInteraction('window-created');
  mainWindow.on('focus', () => {
    appInteractionState.focused = true;
    markAppInteraction('focus');
  });
  mainWindow.on('blur', () => {
    appInteractionState.focused = false;
  });
  mainWindow.on('minimize', () => {
    appInteractionState.minimized = true;
  });
  mainWindow.on('restore', () => {
    appInteractionState.minimized = false;
    markAppInteraction('restore');
  });
  mainWindow.webContents.on('before-input-event', () => {
    markAppInteraction('input');
  });

  if (process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools();
  }
}

function createVoiceHudWindow() {
  if (voiceHudWindow && !voiceHudWindow.isDestroyed()) return voiceHudWindow;
  voiceHudWindow = new BrowserWindow({
    width: 268,
    height: 118,
    show: false,
    frame: false,
    transparent: true,
    resizable: false,
    movable: false,
    minimizable: false,
    maximizable: false,
    closable: false,
    skipTaskbar: true,
    focusable: false,
    alwaysOnTop: true,
    hasShadow: true,
    roundedCorners: true,
    backgroundColor: '#00000000',
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  });
  voiceHudWindow.setVisibleOnAllWorkspaces(true, { visibleOnFullScreen: true });
  voiceHudWindow.setAlwaysOnTop(true, 'screen-saver');
  voiceHudWindow.loadFile('renderer/voice-hud.html');
  voiceHudWindow.on('closed', () => {
    voiceHudWindow = null;
  });
  return voiceHudWindow;
}

function ensureVoiceHudVisible() {
  const win = createVoiceHudWindow();
  if (!win) return null;
  try {
    const cursor = screen.getCursorScreenPoint();
    const display = screen.getDisplayNearestPoint(cursor);
    const bounds = display?.workArea || display?.bounds || { x: 0, y: 0, width: 1440, height: 900 };
    const width = 268;
    const height = 118;
    const x = Math.min(Math.max(bounds.x + 12, cursor.x + 16), bounds.x + bounds.width - width - 12);
    const y = Math.min(Math.max(bounds.y + 12, cursor.y + 18), bounds.y + bounds.height - height - 12);
    win.setBounds({ x: Math.round(x), y: Math.round(y), width, height }, false);
  } catch (_) {}
  if (!win.isVisible()) {
    win.showInactive();
  } else {
    win.showInactive();
  }
  return win;
}

function hideVoiceHudLater(delayMs = 2500) {
  setTimeout(() => {
    if (voiceHudWindow && !voiceHudWindow.isDestroyed() && (!activeVoiceSession || ['completed', 'failed'].includes(activeVoiceSession.status))) {
      voiceHudWindow.hide();
    }
  }, delayMs);
}

function emitPlannerStep(payload = {}) {
  const now = Date.now();
  const throttleKey = `${payload.taskId || 'unknown'}:${payload.phase || 'unknown'}`;
  emitPlannerStep._last = emitPlannerStep._last || new Map();
  const lastAt = emitPlannerStep._last.get(throttleKey) || 0;
  const shouldThrottle = payload.phase === 'thinking' || payload.phase === 'planned' || payload.phase === 'executed';
  if (shouldThrottle && (now - lastAt) < PLANNER_STEP_THROTTLE_MS) return;
  emitPlannerStep._last.set(throttleKey, now);

  if (activeVoiceSession && activeVoiceSession.status === 'acting' && payload && payload.taskId && String(payload.taskId).startsWith('voice_task_')) {
    activeVoiceSession = {
      ...activeVoiceSession,
      agent_stage: payload.stage || activeVoiceSession.agent_stage || '',
      effect_summary: payload.effect_summary || activeVoiceSession.effect_summary || '',
      remaining_gap: payload.remaining_gap || activeVoiceSession.remaining_gap || '',
      failure_reason: payload.failure_reason || null
    };
    emitVoiceSessionUpdate(activeVoiceSession);
  }

  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('planner-step', payload);
  }
  if (voiceHudWindow && !voiceHudWindow.isDestroyed()) {
    voiceHudWindow.webContents.send('planner-step', payload);
  }
}

// OAuth setup with proper environment variables
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || 'your-google-client-id';
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || 'your-google-client-secret';
function getRedirectUri() {
  return `http://localhost:${oauthPort}/oauth2callback`;
}

// Enhanced Google API services
const googleapis = require('googleapis');
const { google } = require('googleapis');

// Browser automation
const puppeteer = require('puppeteer-core');

// The browser-extension/native-host bridge has been removed.
let extensionSocket = null;
let extensionLastSeen = null;
let extensionTransport = 'disabled';

function isExtensionConnected() { return false; }
async function waitForExtensionConnection() { return false; }
function sendTaskToExtension() { throw new Error('Browser extension support has been removed'); }
function sendExtensionRequest() { throw new Error('Browser extension support has been removed'); }
async function sendExtensionRequestWithRetry() { throw new Error('Browser extension support has been removed'); }
function flushPendingAITasks() { pendingAITasks = []; }

// Scheduled tasks
let dailySummaryTimer = null;
let patternUpdateTimer = null;
let minutelySyncTimer = null;
let minutelySyncInterval = null;
let morningBriefTimer = null;
let screenshotCleanupTimer = null;

const MINUTELY_MS = 60 * 1000;
const GOOGLE_SYNC_BASELINE_ISO = '2010-01-01T00:00:00.000Z';
const GOOGLE_SYNC_OVERLAP_MS = 5 * 60 * 1000;
const GOOGLE_SYNC_FUTURE_DRIFT_MS = 5 * 60 * 1000;
const GOOGLE_CONTACTS_SYNC_INTERVAL_MS = 12 * 60 * 60 * 1000;
const DEFAULT_SENSOR_SETTINGS = {
  enabled: true, // Auto-enable for continuous capture
  intervalMinutes: 15, // 15 minutes
  maxEvents: 200 // Increase for more frequent captures
};

function getSensorSettings() {
  const stored = store.get('sensorSettings') || {};
  // Force fixed 15-minute capture interval
  const intervalMinutes = 15;
  const maxEvents = Math.max(50, parseInt(stored.maxEvents, 10) || DEFAULT_SENSOR_SETTINGS.maxEvents);
  return {
    enabled: stored.enabled !== undefined ? Boolean(stored.enabled) : DEFAULT_SENSOR_SETTINGS.enabled,
    intervalMinutes,
    maxEvents
  };
}

function getSensorEvents() {
  const events = store.get('sensorEvents') || [];
  const { maxEvents } = getSensorSettings();
  const limit = Math.max(50, Number(maxEvents || DEFAULT_SENSOR_SETTINGS.maxEvents));
  if (Array.isArray(events) && events.length > limit) {
    const trimmed = events.slice(0, limit);
    debouncedStoreSet('sensorEvents', trimmed);
    return trimmed;
  }
  return events;
}

function pruneOldSensorCaptures(events = []) {
  const list = Array.isArray(events) ? events : [];
  const retentionMs = Math.max(1, Number(SCREENSHOT_RETENTION_DAYS || 36500)) * 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - retentionMs;
  const { maxEvents } = getSensorSettings();
  const limit = Math.max(50, Number(maxEvents || DEFAULT_SENSOR_SETTINGS.maxEvents));

  return list
    .filter((event) => {
      const ts = Number(event?.timestamp) || Date.parse(String(event?.captured_at || event?.time || ''));
      return !Number.isFinite(ts) || ts <= 0 || ts >= cutoff;
    })
    .sort((a, b) => {
      const aTs = Number(a?.timestamp) || Date.parse(String(a?.captured_at || '')) || 0;
      const bTs = Number(b?.timestamp) || Date.parse(String(b?.captured_at || '')) || 0;
      return bTs - aTs;
    })
    .slice(0, limit);
}

function getSensorStatus() {
  const settings = getSensorSettings();
  const events = getSensorEvents();
  let screenPermission = 'unknown';

  try {
    if (process.platform === 'darwin') {
      screenPermission = systemPreferences.getMediaAccessStatus('screen');
    }
  } catch (_) {}

  return {
    ...settings,
    active: Boolean(sensorCaptureTimer && settings.enabled),
    intervalSeconds: Math.round(intervalMinutesToMs(settings.intervalMinutes) / 1000),
    lastCaptureAt: events[0]?.timestamp || null,
    totalCaptures: events.length,
    screenPermission,
    transport: 'apple-vision-frontmost-window',
    study_session: getStudySessionState(),
    performance_mode: isReducedLoadMode() ? 'reduced' : 'normal'
  };
}

function intervalMinutesToMs(minutes) {
  return Math.max(1, Number(minutes || 0)) * 60 * 1000;
}

function sanitizeSuggestionProvider(value) {
  return 'deepseek';
}

function getSuggestionLLMSettings() {
  const stored = store.get('suggestionLLMSettings') || {};
  const provider = 'deepseek';
  const model = String(
    stored.model
    || 'deepseek-chat'
  ).trim();
  const baseUrl = String(
    stored.baseUrl
    || process.env.OLLAMA_BASE_URL
    || 'http://127.0.0.1:11434'
  ).trim();
  const apiKey = String(
    process.env.DEEPSEEK_API_KEY
    || stored.apiKey
    || ''
  ).trim();
  return {
    provider,
    model,
    baseUrl,
    apiKey
  };
}

function getSuggestionLLMConfig() {
  const settings = getSuggestionLLMSettings();
  if (settings.provider === 'deepseek') {
    if (!settings.apiKey) return null;
    return {
      provider: 'deepseek',
      model: settings.model || 'deepseek-chat',
      apiKey: settings.apiKey
    };
  }
  return {
    provider: 'ollama',
    model: settings.model || 'llama3.1:8b',
    baseUrl: settings.baseUrl || 'http://127.0.0.1:11434',
    apiKey: settings.apiKey || null
  };
}

function suggestionQueueKey(item = {}) {
  const compact = (value) => String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const title = compact(item?.title || '');
  const action = compact(item?.primary_action?.label || item?.recommended_action || '');
  const type = compact(item?.type || item?.category || '');
  return `${title}|${action}|${type}`;
}

function hasConcreteSuggestionAction(text = '') {
  const value = String(text || '').trim();
  if (!value) return false;
  if (/\b(open|draft|reply|send|prepare|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share|drill|resume|close|start|run|review)\b/i.test(value)) {
    return !/\b(review|open)\s+(this|it|item|context|memory|something|task)$/i.test(value);
  }
  return false;
}

function isActionableSuggestion(item = {}) {
  if (!item || item.completed) return false;
  const title = String(item.title || '').trim();
  const reason = String(item.reason || item.description || item.body || '').trim();
  const primaryLabel = String(item.primary_action?.label || item.recommended_action || '').trim();
  const actions = Array.isArray(item.suggested_actions) ? item.suggested_actions : [];
  const plan = Array.isArray(item.plan) ? item.plan : [];
  const stepPlan = Array.isArray(item.step_plan) ? item.step_plan : [];
  if (!title || !reason) return false;
  if (/\b(take the next step|keep momentum|be proactive|work on this|handle this|make progress|stay on top)\b/i.test(title)) return false;
  if (!hasConcreteSuggestionAction(title) && !hasConcreteSuggestionAction(primaryLabel) && !actions.some((action) => hasConcreteSuggestionAction(action?.label || action?.payload?.action || ''))) return false;
  if (!primaryLabel && !actions.length && !plan.length && !stepPlan.length) return false;
  return true;
}

function mergeSuggestionQueues(existing = [], incoming = [], limit = MAX_PRACTICAL_SUGGESTIONS) {
  const all = [
    ...(Array.isArray(existing) ? existing : []),
    ...(Array.isArray(incoming) ? incoming : [])
  ].filter(isActionableSuggestion);

  const deduped = [];
  const seen = new Set();
  for (const item of all) {
    const key = suggestionQueueKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }

  const priorityValue = (item) => {
    const map = { high: 3, medium: 2, low: 1 };
    return map[String(item?.priority || 'medium').toLowerCase()] || 2;
  };

  deduped.sort((a, b) => {
    const priorityDelta = priorityValue(b) - priorityValue(a);
    if (priorityDelta !== 0) return priorityDelta;

    const urgencyA = Number(a?.urgency ?? a?.score ?? a?.confidence ?? 0);
    const urgencyB = Number(b?.urgency ?? b?.score ?? b?.confidence ?? 0);
    if (urgencyB !== urgencyA) return urgencyB - urgencyA;

    const scoreA = Number(a?.score ?? a?.confidence ?? 0);
    const scoreB = Number(b?.score ?? b?.confidence ?? 0);
    if (scoreB !== scoreA) return scoreB - scoreA;

    return Number(b?.createdAt || 0) - Number(a?.createdAt || 0);
  });

  return deduped.slice(0, Math.max(1, Math.min(MAX_PRACTICAL_SUGGESTIONS, Number(limit || MAX_PRACTICAL_SUGGESTIONS))));
}

async function ensureSensorStorageDir() {
  const capturesDir = path.join(app.getPath('userData'), 'screenshots');
  if (!(await existsAsync(capturesDir))) {
    await fs.promises.mkdir(capturesDir, { recursive: true });
  }
  return capturesDir;
}

// Content filtering functions
function isSensitiveContent(text, windowTitle, appName) {
  const content = `${text} ${windowTitle} ${appName}`.toLowerCase();
  const hasPhrase = (keyword) => {
    const escaped = String(keyword || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, 'i').test(content);
  };

  // Banking and financial sites
  const bankingKeywords = [
    'bank', 'chase', 'wells fargo', 'bank of america', 'citibank', 'capital one',
    'paypal', 'venmo', 'cash app', 'zelle', 'transfer', 'routing number',
    'account number', 'credit card', 'debit card', 'balance', 'transaction',
    'login', 'signin', 'password', 'security code', 'ssn', 'social security'
  ];

  // Adult content detection intentionally avoids generic one-word matches like
  // "adult" because IDEs/terminals can display filter logs containing
  // "Adult content" and should not be classified as adult content themselves.
  const adultStrongKeywords = [
    'porn', 'xxx', 'nsfw', 'erotic', 'nude', 'naked', 'escort', 'adultfriendfinder',
    'pornhub', 'xvideos', 'xhamster'
  ];
  const adultWeakKeywords = [
    'adult', 'sex', 'hookup', 'dating', 'cam', 'strip'
  ];

  // Password and security sensitive sites
  const passwordKeywords = [
    'password', 'passphrase', 'secret key', 'private key', 'api key',
    'two factor', '2fa', 'authentication', 'security question',
    'reset password', 'change password', 'forgot password'
  ];

  // Check each category
  const isBanking = bankingKeywords.some(keyword => hasPhrase(keyword));
  const adultWeakMatches = adultWeakKeywords.filter(keyword => hasPhrase(keyword));
  const isAdult = adultStrongKeywords.some(keyword => hasPhrase(keyword)) || adultWeakMatches.length >= 2;
  const isPasswordRelated = passwordKeywords.some(keyword => hasPhrase(keyword));

  return {
    isSensitive: isBanking || isAdult || isPasswordRelated,
    category: isBanking ? 'banking' : isAdult ? 'adult' : isPasswordRelated ? 'password' : null,
    reason: isBanking ? 'Banking/Financial content' :
            isAdult ? 'Adult content' :
            isPasswordRelated ? 'Password/Security content' : null
  };
}

function shouldFilterCapture(text, windowTitle, appName, url) {
  // Check URL-based filtering first
  if (url) {
    const urlLower = url.toLowerCase();
    const sensitiveDomains = [
      'chase.com', 'wellsfargo.com', 'bankofamerica.com', 'citibank.com',
      'capitalone.com', 'paypal.com', 'venmo.com', 'cash.app',
      'adultfriendfinder.com', 'pornhub.com', 'xvideos.com', 'xhamster.com'
    ];

    if (sensitiveDomains.some(domain => urlLower.includes(domain))) {
      return { shouldFilter: true, reason: 'Sensitive domain detected', category: 'domain' };
    }
  }

  // Check content-based filtering
  const contentCheck = isSensitiveContent(text, windowTitle, appName);
  if (contentCheck.isSensitive) {
    return { shouldFilter: true, reason: contentCheck.reason, category: contentCheck.category };
  }

  return { shouldFilter: false };
}

async function deleteSensitiveCapture(imagePath, eventId, reason) {
  // Total Data Durability: deletion disabled
  console.log(`[Content Filter] Sensitive content detected but deletion skipped for durability: ${reason}`);
  return {
    deleted: false,
    retained_for_durability: true,
    reason,
    imagePath,
    eventId
  };
}

function runVisionOCR(imagePath) {
  const scriptPath = path.join(__dirname, 'ocr_vision.swift');
  const timeoutMs = Number(process.env.VISION_OCR_TIMEOUT_MS || 12000); // must be < outer 25s cap
  const accuracy = (getPerformanceMode() === 'reduced' || getPerformanceMode() === 'deep-idle') ? 'fast' : 'accurate';
  
  return new Promise((resolve) => {
    execFile('/usr/bin/xcrun', ['swift', scriptPath, imagePath, accuracy], { timeout: timeoutMs }, (error, stdout, stderr) => {
      if (error) {
        resolve({
          text: '',
          lines: [],
          confidence: 0,
          status: 'error',
          error: stderr?.trim() || error.message
        });
        return;
      }

      try {
        const parsed = JSON.parse(stdout.toString());
        resolve({
          text: parsed.text || '',
          lines: parsed.lines || [],
          confidence: parsed.confidence || 0,
          status: parsed.text ? 'complete' : 'no_text'
        });
      } catch (parseError) {
        resolve({
          text: '',
          lines: [],
          confidence: 0,
          status: 'error',
          error: parseError.message
        });
      }
    });
  });
}

function buildPerceptualFingerprintFromPngBuffer(pngBuffer) {
  try {
    if (!pngBuffer || !pngBuffer.length) return null;
    const image = nativeImage.createFromBuffer(pngBuffer);
    if (!image || image.isEmpty()) return null;
    // Lower resolution for faster processing and better fuzzy matching
    const normalized = image.resize({ width: 16, height: 9, quality: 'good' });
    const bitmap = normalized.toBitmap();
    if (!bitmap || !bitmap.length) return null;

    const bins = new Array(64).fill(0);
    const counts = new Array(64).fill(0);
    const len = bitmap.length;
    // Optimized loop: process 4 pixels at a time, use bitwise for gray calculation
    for (let i = 0; i < len; i += 16) {
      const r = bitmap[i];
      const g = bitmap[i + 1];
      const b = bitmap[i + 2];
      // gray = (r*30 + g*59 + b*11) / 100 approx.
      const gray = (r * 30 + g * 59 + b * 11) >> 6;
      const bin = Math.min(63, (i * 64 / len) | 0);
      bins[bin] += gray;
      counts[bin]++;
    }
    return bins.map((sum, i) => counts[i] ? (sum / counts[i]) | 0 : 0);
  } catch (_) {
    return null;
  }
}

function fingerprintDiffPercent(prev, next) {
  if (!Array.isArray(prev) || !Array.isArray(next)) return 1;
  if (prev.length !== next.length || !prev.length) return 1;
  let delta = 0;
  for (let i = 0; i < prev.length; i += 1) {
    delta += Math.abs(Number(prev[i] || 0) - Number(next[i] || 0));
  }
  const maxDelta = prev.length * 255;
  if (!maxDelta) return 0;
  return Math.min(1, Math.max(0, delta / maxDelta));
}

function getFrontmostWindowContext() {
  if (process.platform !== 'darwin') {
    return Promise.resolve({
      appName: '',
      windowTitle: '',
      extractedText: '',
      windowId: null,
      bounds: null,
      status: 'unsupported_platform'
    });
  }

  return new Promise((resolve) => {
    const scriptPath = path.join(__dirname, 'frontmost_window.swift');
    const appName = String(app?.getName?.() || '').trim();
    // Pass current process PID as an optional argument so the swift helper can exclude
    // this process' own windows from the frontmost window search.
    execFile('/usr/bin/xcrun', ['swift', scriptPath, String(process.pid), appName], { timeout: 15000 }, (error, stdout, stderr) => {
      if (error) {
        resolve({
          appName: '',
          windowTitle: '',
          extractedText: '',
          windowId: null,
          bounds: null,
          status: 'unavailable',
          error: stderr?.trim() || error.message
        });
        return;
      }

      try {
        const parsed = JSON.parse(stdout.toString());
        resolve({
          appName: parsed.appName || '',
          windowTitle: parsed.windowTitle || '',
          extractedText: parsed.extractedText || '',
          windowId: parsed.windowId || null,
          bounds: (typeof parsed.x === 'number' && typeof parsed.y === 'number' && typeof parsed.width === 'number' && typeof parsed.height === 'number')
            ? { x: parsed.x, y: parsed.y, width: parsed.width, height: parsed.height }
            : null,
          status: parsed.status || 'empty'
        });
      } catch (parseError) {
        resolve({
          appName: '',
          windowTitle: '',
          extractedText: '',
          windowId: null,
          bounds: null,
          status: 'unavailable',
          error: parseError.message
        });
      }
    });
  });
}

async function captureDesktopSensorSnapshot(reason = 'scheduled') {
  const captureReason = String(reason || 'scheduled');
  
  // Check performance monitor for throttling
  // if (performanceMonitor.shouldThrottleOperation('desktop_capture')) {
  //   const delay = performanceMonitor.getRecommendedDelay('desktop_capture');
  //   if (delay > 0) {
  //     console.log(`[SensorCapture] Throttling capture for ${delay}ms due to system load`);
  //     return {
  //       skipped: true,
  //       reason: 'performance_throttled',
  //       screenshot_present: false,
  //       performance_report: performanceMonitor.getPerformanceReport()
  //     };
  //   }
  // }
  
  if (shouldDeferBackgroundWork("SensorCapture")) return { skipped: true, reason: "active_use" };
  const captureStartedAt = Date.now();
  if (screenshotsPausedForDisplayOff) {
    return {
      skipped: true,
      reason: 'display_off',
      screenshot_present: false
    };
  }
  if (sensorCaptureInProgress && (Date.now() - sensorCaptureStartedAt) > CAPTURE_STALE_LOCK_MS) {
    console.warn('[SensorCapture] Resetting stale capture lock');
    sensorCaptureInProgress = false;
    sensorCaptureStartedAt = 0;
  }
  if (sensorCaptureInProgress) {
    // Avoid overlapping captures
    return {
      skipped: true,
      reason: 'capture_in_progress',
      screenshot_present: false
    };
  }
  sensorCaptureInProgress = true;
  sensorCaptureStartedAt = Date.now();
  let captureLockReleased = false;
  let urlAssociationDurationMs = 0;
  let eventPersistenceDurationMs = 0;
  const timestamp = Date.now();
  const filename = `ocr_capture_${timestamp}_${crypto.randomBytes(4).toString('hex')}.png`;
  const sensorStorageDir = await ensureSensorStorageDir();
  const imagePath = path.join(sensorStorageDir, filename);
  let capturePngBuffer = null;
  try {
    const windowContext = await getFrontmostWindowContext();
    let sourceName = 'Screen';
    let captureMode = 'screen';

  if (process.platform === 'darwin' && windowContext.windowId) {
    try {
      await new Promise((resolve, reject) => {
        execFile('/usr/sbin/screencapture', ['-x', '-l', `${windowContext.windowId}`, imagePath], { timeout: 15000 }, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
      if (await existsAsync(imagePath)) {
        captureMode = 'frontmost-window';
        sourceName = windowContext.windowTitle || windowContext.appName || 'Window';
        capturePngBuffer = await fs.promises.readFile(imagePath).catch(() => null);
      }
    } catch (_) {}
  }
  if (captureMode !== 'frontmost-window' && process.platform === 'darwin' && windowContext.bounds) {
    try {
      const { x, y, width, height } = windowContext.bounds || {};
      if ([x, y, width, height].every((value) => Number.isFinite(value)) && width > 20 && height > 20) {
        await new Promise((resolve, reject) => {
          execFile(
            '/usr/sbin/screencapture',
            ['-x', '-R', `${Math.round(x)},${Math.round(y)},${Math.max(1, Math.round(width))},${Math.max(1, Math.round(height))}`, imagePath],
            { timeout: 15000 },
            (error) => {
              if (error) reject(error);
              else resolve();
            }
          );
        });
        if (await existsAsync(imagePath)) {
          captureMode = 'frontmost-window-bounds';
          sourceName = windowContext.windowTitle || windowContext.appName || 'Window';
          capturePngBuffer = await fs.promises.readFile(imagePath).catch(() => null);
        }
      }
    } catch (_) {}
  }
  // windowContext logged at capture start (verbose; omitted from hot path)

  if (!String(captureMode).startsWith('frontmost-window')) {
    const primary = screen.getPrimaryDisplay();
    const fullSize = primary?.size || { width: 1920, height: 1080 };
    const reducedLoad = isReducedLoadMode();
    
    // Further reduce resolution to minimize GPU load
    const maxWidth = reducedLoad ? 640 : 960; // REDUCED from 960/1440
    const scale = Math.min(1, maxWidth / Math.max(1, Number(fullSize.width || 1920)));
    const thumbWidth = Math.max(480, Math.floor((fullSize.width || 1920) * scale)); // REDUCED from 640
    const thumbHeight = Math.max(270, Math.floor((fullSize.height || 1080) * scale)); // REDUCED from 360

    // Request only screen sources for the fallback to minimize GPU/WindowServer load.
    // Capturing all windows via desktopCapturer just to find a title match is extremely expensive.
    const sources = await desktopCapturer.getSources({
      types: ['screen'],
      thumbnailSize: { width: thumbWidth, height: thumbHeight }
    });

  // Try to find a window source that matches the frontmost window title or app name
    let matchedSource = null;
    try {
      const title = String(windowContext.windowTitle || '').toLowerCase();
      const appName = String(windowContext.appName || '').toLowerCase();
      for (const s of sources) {
        const name = String(s.name || '').toLowerCase();
        const displayId = String(s.display_id || '').toLowerCase();
        if (title && name.includes(title)) { matchedSource = s; break; }
        if (appName && name.includes(appName)) { matchedSource = s; break; }
        // Some sources encode app/window metadata in the name like "Safari - example.com"
        if (title && name.indexOf(title) >= 0) { matchedSource = s; break; }
        if (appName && name.indexOf(appName) >= 0) { matchedSource = s; break; }
      }
    } catch (_) { matchedSource = null; }

    // Prefer matched window source; otherwise fall back to the first screen source
    const screenSource = sources.find((s) => String(s.id || '').toLowerCase().startsWith('screen:')) || sources.find((s) => String(s.id || '').toLowerCase().includes('screen'));
    const source = matchedSource || screenSource || sources[0];

    if (!source) {
      throw new Error('No screen or window source available for capture');
    }

    const pngBuffer = source.thumbnail.toPNG();
    if (!pngBuffer || !pngBuffer.length) {
      throw new Error('Screen capture returned an empty image');
    }
    try {
      await fs.promises.writeFile(imagePath, pngBuffer);
      capturePngBuffer = pngBuffer;
    } catch (writeErr) {
      throw writeErr;
    }
    sourceName = source.name || 'Screen';
  }

  if (!capturePngBuffer && (await existsAsync(imagePath))) {
    capturePngBuffer = await fs.promises.readFile(imagePath).catch(() => null);
  }

  // Use the visual fingerprint as the cheap gate before OCR/embedding work.
  const fingerprint = buildPerceptualFingerprintFromPngBuffer(capturePngBuffer);
  const visualDiffPct = Number((fingerprintDiffPercent(lastAcceptedCaptureFingerprint, fingerprint) * 100).toFixed(2));
  const visualChangeSignificant = !lastAcceptedCaptureFingerprint || !fingerprint || visualDiffPct >= (CAPTURE_VISUAL_DIFF_THRESHOLD * 100);
  const forceCapture = /manual|reset/i.test(String(reason || ''));

  if (fingerprint) {
    lastAcceptedCaptureFingerprint = fingerprint;
  }

  if (!visualChangeSignificant && !forceCapture) {
    return {
      skipped: true,
      reason: 'low_visual_change',
      screenshot_present: Boolean(await existsAsync(imagePath)),
      imagePath: (await existsAsync(imagePath)) ? imagePath : null,
      screenshot_filename: filename,
      activeApp: windowContext.appName || '',
      activeWindowTitle: windowContext.windowTitle || '',
      visual_diff_pct: visualDiffPct,
      visual_diff_threshold_pct: Number((CAPTURE_VISUAL_DIFF_THRESHOLD * 100).toFixed(2))
    };
  }

  const contextSuffix = [windowContext.appName, windowContext.windowTitle].filter(Boolean).join(' - ');
  const studySession = getStudySessionState();
  const inStudySession = studySession?.status === 'active' && Boolean(studySession?.session_id);

  const axText = String(windowContext.extractedText || '').trim();
  const canUseAxOnly = !forceCapture && axText.length >= 80;
  
  // Throttle OCR to prevent CPU overload
  const now = Date.now();
  const ocrThrottled = !forceCapture && (now - lastLowPowerOCRAt) < LOW_POWER_OCR_MIN_INTERVAL_MS;
  
  // Check performance monitor for OCR throttling
  // const performanceThrottled = !canUseAxOnly && !ocrThrottled && performanceMonitor.shouldThrottleOperation('ocr');
  
  let ocrStartTime = Date.now();
  const ocr = canUseAxOnly
    ? {
        text: '',
        lines: [],
        confidence: 0,
        status: 'skipped_ax_text_available'
      }
    : ocrThrottled
    ? {
        text: '',
        lines: [],
        confidence: 0,
        status: 'throttled_for_performance'
      }
    : performanceThrottled
    ? {
        text: '',
        lines: [],
        confidence: 0,
        status: 'performance_throttled'
      }
    : await runVisionOCR(imagePath);
  
  const ocrDuration = Date.now() - ocrStartTime;
  
  // Update last OCR timestamp only when OCR actually runs
  if (!canUseAxOnly && !ocrThrottled && !performanceThrottled) {
    lastLowPowerOCRAt = now;
  }

  const ocrText = String(ocr.text || '').trim();
  const mergedText = [axText, ocrText].filter(Boolean).join('\n').trim();

  if (!canUseAxOnly && ocrDuration > 500) {
    console.log(`[SensorCapture] OCR completed in ${ocrDuration}ms for ${imagePath}`);
  }

  const event = {
    id: `sensor_${timestamp}`,
    type: 'screen_capture',
    title: `Frontmost OCR: ${contextSuffix || sourceName || 'Screen'}`,
    timestamp,
    captured_at: new Date(timestamp).toISOString(),
    captured_at_local: new Date(timestamp).toLocaleString(),
    sourceName,
    captureMode: `${captureMode}-ocr`,
    activeApp: windowContext.appName || '',
    activeWindowTitle: windowContext.windowTitle || '',
    windowId: windowContext.windowId || null,
    windowBounds: windowContext.bounds || null,
    windowContextStatus: windowContext.status || 'unavailable',
    imagePath: imagePath,
    text: mergedText,
    textCaptureSource: axText && ocrText ? 'ax+ocr' : (ocrText ? 'ocr' : (axText ? 'ax' : 'none')),
    ocrLines: ocr.lines || [],
    ocrConfidence: ocr.confidence || 0,
    ocrStatus: ocr.status || (ocrText ? 'complete' : 'no_text'),
    ocrDuration,
    visual_diff_pct: visualDiffPct,
    visual_diff_threshold_pct: Number((CAPTURE_VISUAL_DIFF_THRESHOLD * 100).toFixed(2)),
    visual_change_significant: visualChangeSignificant,
    screenshot_folder: sensorStorageDir,
    screenshot_filename: filename,
    reason,
    study_session_id: inStudySession ? studySession.session_id : null,
    study_goal: inStudySession ? (studySession.goal || '') : '',
    study_subject: inStudySession ? (studySession.subject || '') : ''
  };
  if (windowContext.error) event.windowContextError = windowContext.error;
  if (ocr.error) event.ocrError = ocr.error;
  if (isReducedLoadMode()) lastLowPowerOCRAt = Date.now();

  try {
    const urlAssociationStartedAt = Date.now();
    const associatedUrls = await findAssociatedUrlsForScreenshot({
      timestamp,
      appName: event.activeApp,
      windowTitle: event.activeWindowTitle,
      limit: 5
    });
    urlAssociationDurationMs = Date.now() - urlAssociationStartedAt;
    if (associatedUrls.length) {
      event.associatedUrls = associatedUrls;
      event.primaryUrl = associatedUrls[0].url;
      event.primaryDomain = associatedUrls[0].domain || '';
    }
  } catch (historyError) {
    console.warn('[SensorCapture] Failed to associate browser URLs:', historyError?.message || historyError);
  }

  event.study_signal = inferStudySignal(event.text, event);
  event.study_context = {
    in_session: inStudySession,
    session_id: event.study_session_id,
    goal: event.study_goal,
    subject: event.study_subject
  };

  // Content filtering - check for sensitive content and delete if found
  const filterCheck = shouldFilterCapture(
    event.text,
    event.activeWindowTitle,
    event.activeApp,
    event.primaryUrl || null
  );

  if (filterCheck.shouldFilter) {
    const filterResult = await deleteSensitiveCapture(imagePath, event.id, filterCheck.reason);
    return {
      ...event,
      filtered: true,
      filter_reason: filterCheck.reason,
      sensitive_category: filterCheck.category || null,
      retained_for_durability: Boolean(filterResult?.retained_for_durability),
      screenshot_present: Boolean(await existsAsync(imagePath)),
      imagePath: (await existsAsync(imagePath)) ? imagePath : null
    };
  }

  const existing = pruneOldSensorCaptures(getSensorEvents());
  const { maxEvents } = getSensorSettings();
  const nextEvents = [event, ...existing].slice(0, Math.max(50, Number(maxEvents || DEFAULT_SENSOR_SETTINGS.maxEvents)));

  const persistStartedAt = Date.now();
  debouncedStoreSet('sensorEvents', nextEvents);
  eventPersistenceDurationMs = Date.now() - persistStartedAt;

  if (isPeriodicScreenshot) {
    // Release the capture lock promptly so DB/vector ingestion doesn't block the next capture cycle.
    sensorCaptureInProgress = false;
    captureLockReleased = true;
  }

  // L1 Ingestion - Always save OCR to memory with full metadata
  try {
    
    const ingestPayload = {
      type: 'ScreenCapture',
      timestamp: event.timestamp,
      source: 'Sensors',
      text: [
        event.activeApp ? `App: ${event.activeApp}` : '',
        event.activeWindowTitle ? `Window: ${event.activeWindowTitle}` : '',
        ocrText ? `OCR raw text:\n${ocrText}` : '',
        axText ? `AX raw text:\n${axText}` : '',
        event.text ? `Captured text:\n${event.text}` : ''
      ].filter(Boolean).join('\n'),
      metadata: {
        ...event,
        app: event.activeApp || 'Desktop',
        source_app: event.activeApp || 'Desktop',
        data_source: 'screenshot_ocr',
        window_title: event.activeWindowTitle || '',
        url: event.primaryUrl || null,
        primary_url: event.primaryUrl || null,
        primary_domain: event.primaryDomain || null,
        associated_urls: event.associatedUrls || [],
        browser_context_urls: event.associatedUrls || [],
        context_title: event.activeWindowTitle || event.sourceName || '',
        timestamp: event.captured_at,
        app_id: event.activeApp || 'Desktop',
        raw_ocr_text: ocrText,
        raw_ax_text: axText,
        raw_ocr_lines: event.ocrLines || [],
        raw_ocr_confidence: event.ocrConfidence || 0,
        ocr_text_char_count: ocrText.length,
        ax_text_char_count: axText.length,
        text_capture_source: event.textCaptureSource || 'none',
        study_context: event.study_context,
        study_signal: event.study_signal,
        study_session_id: event.study_session_id,
        study_goal: event.study_goal,
        study_subject: event.study_subject,
        screenshot_folder: event.screenshot_folder,
        screenshot_filename: event.screenshot_filename,
        screenshot_path: event.imagePath,
        capture_reason: event.reason,
        capture_interval_seconds: 30
      }
    };

    await ingestRawEvent(ingestPayload);

    // Log OCR ingestion with metadata
    const ocrSummary = [
      `OCR status: ${event.ocrStatus}`,
      event.ocrConfidence ? `confidence: ${(event.ocrConfidence * 100).toFixed(1)}%` : null,
      event.ocrDuration ? `duration: ${event.ocrDuration}ms` : null,
      `source: ${event.textCaptureSource}`,
      `ocr_chars: ${ocrText.length}`,
      `ax_chars: ${axText.length}`,
      event.text ? `text_chars: ${event.text.length}` : 'no_text'
    ].filter(Boolean).join(', ');

    // OCR ingestion saved (verbose logging omitted from hot path)

    updateMemoryGraphHealth({
      lastDesktopCaptureAt: new Date().toISOString(),
      desktopCaptureStatus: 'idle',
      lastDesktopCaptureSource: event.textCaptureSource || 'none',
      lastDesktopCaptureApp: event.activeApp || '',
      lastOCRStatus: event.ocrStatus,
      lastOCRConfidence: event.ocrConfidence,
      lastCaptureDurationMs: Date.now() - captureStartedAt,
      lastUrlAssociationDurationMs: urlAssociationDurationMs,
      lastEventPersistenceDurationMs: eventPersistenceDurationMs
    });
    // Timing summary omitted from hot path; uncomment for profiling:
    // console.log(`[SensorCaptureTiming] total=${Date.now() - captureStartedAt}ms ocr=${ocrDuration}ms urls=${urlAssociationDurationMs}ms persist=${eventPersistenceDurationMs}ms`);

    // Throttle expensive background suggestion generation to avoid UI slowdown.
    const triggerInterval = isReducedLoadMode()
      ? Math.max(CAPTURE_TRIGGER_MIN_INTERVAL_MS * 2, LOW_POWER_HEAVY_JOB_MIN_GAP_MS)
      : Math.max(CAPTURE_TRIGGER_MIN_INTERVAL_MS, LOW_POWER_HEAVY_JOB_MIN_GAP_MS);
    const activeSuggestionCount = mergeSuggestionQueues(store.get('suggestions') || [], [], MAX_PRACTICAL_SUGGESTIONS).length;
    if (activeSuggestionCount < MAX_PRACTICAL_SUGGESTIONS && (Date.now() - lastCaptureSuggestionTriggerAt) >= triggerInterval) {
      lastCaptureSuggestionTriggerAt = Date.now();
      setTimeout(() => {
        runSuggestionEngineJob().catch(err =>
          console.log('Background suggestion engine trigger failed:', err?.message || err)
        );
      }, 5 * 60 * 1000); // Delay to avoid doing LLM work during active screenshot/OCR cycles.
    }
  } catch (e) {
    console.error('[captureDesktopSensorSnapshot] L1 ingestion failed:', e.message || e);
    console.error('[captureDesktopSensorSnapshot] Failed to ingest OCR capture from:', event.activeApp || 'unknown app');
    updateMemoryGraphHealth({
      desktopCaptureStatus: 'error',
      lastDesktopCaptureError: e?.message || String(e),
      lastDesktopCaptureErrorAt: new Date().toISOString(),
      lastCaptureDurationMs: Date.now() - captureStartedAt
    });
  }

    return event;
  } catch (error) {
    console.error('[captureDesktopSensorSnapshot] capture failed:', error?.message || error);
    throw error;
  } finally {
    sensorCaptureStartedAt = 0;
    if (!captureLockReleased) {
      sensorCaptureInProgress = false;
    }
  }
}

function startSensorCaptureLoop(mode = null) {
  const settings = getSensorSettings();
  if (sensorCaptureTimer) {
    clearInterval(sensorCaptureTimer);
    sensorCaptureTimer = null;
  }
  if (!settings.enabled) return;

  // Startup capture is handled by the periodic screenshot loop (startPeriodicScreenshotCapture).

  const intervalMs = getPeriodicScreenshotIntervalMs(mode || getPerformanceMode());
  sensorCaptureTimer = setInterval(() => {
    // Prevent overlapping captures and add additional throttling
    if (periodicScreenshotRunning || sensorCaptureInProgress || screenshotsPausedForDisplayOff) return;
    
    // Emergency throttling if system is under extreme load
    // const perfReport = performanceMonitor.getPerformanceReport();
    // if (perfReport.cpuUsage > 90 || perfReport.memoryUsage > 90) {
    //   console.log(`[SensorCapture] Emergency throttling due to extreme load: CPU=${perfReport.cpuUsage.toFixed(1)}% Memory=${perfReport.memoryUsage.toFixed(1)}%`);
    //   return;
    // }
    
    // Add extra delay if system is under heavy load
    const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
    if (idleTime < 30 && !shouldDeferBackgroundWork("SensorCapture")) {
      // System is active, delay capture to reduce interference
      setTimeout(() => {
        if (!periodicScreenshotRunning && !sensorCaptureInProgress && !screenshotsPausedForDisplayOff) {
          captureDesktopSensorSnapshot("scheduled").catch((error) => {
            console.error("[SensorCapture] Scheduled capture failed:", error?.message || error);
          });
        }
      }, Math.random() * 30000); // Random delay up to 30s
    } else {
      // Fire-and-forget scheduled capture to keep it in the background
      captureDesktopSensorSnapshot("scheduled").catch((error) => {
        console.error("[SensorCapture] Scheduled capture failed:", error?.message || error);
      });
    }
  }, intervalMs);
}

function clearPeriodicScreenshotTimer() {
  if (periodicScreenshotTimer) {
    clearTimeout(periodicScreenshotTimer);
    periodicScreenshotTimer = null;
  }
}

function startPeriodicScreenshotWatchdog() {
  if (periodicScreenshotWatchdogTimer) return;
  periodicScreenshotWatchdogTimer = setInterval(() => {
    if (!periodicScreenshotRunning || screenshotsPausedForDisplayOff) return;
    if (sensorCaptureInProgress && (Date.now() - sensorCaptureStartedAt) > CAPTURE_STALE_LOCK_MS) {
      console.warn('[Screenshot] Clearing stale capture state from watchdog');
      sensorCaptureInProgress = false;
      sensorCaptureStartedAt = 0;
    }
    if (!periodicScreenshotTimer) {
      const intervalMs = getPeriodicScreenshotIntervalMs();
      periodicScreenshotNextDueAt = Date.now() + intervalMs;
      scheduleNextPeriodicScreenshot(intervalMs);
      console.warn('[Screenshot] Watchdog restored missing periodic screenshot timer');
    }
  }, SCREENSHOT_WATCHDOG_INTERVAL_MS);
}

function scheduleNextPeriodicScreenshot(delayMs) {
  clearPeriodicScreenshotTimer();
  const safeDelayMs = Math.max(1000, Number(delayMs || getPeriodicScreenshotIntervalMs()));
  periodicScreenshotTimer = setTimeout(() => {
    runPeriodicScreenshotTick().catch((error) => {
      console.error("[Screenshot] Periodic screenshot loop failed:", error?.message || error);
      if (!screenshotsPausedForDisplayOff && periodicScreenshotRunning) {
        const intervalMs = getPeriodicScreenshotIntervalMs();
        periodicScreenshotNextDueAt = Date.now() + intervalMs;
        scheduleNextPeriodicScreenshot(intervalMs);
      }
    });
  }, safeDelayMs);
}

async function runPeriodicScreenshotTick() {
  periodicScreenshotTimer = null;

  if (!periodicScreenshotRunning) return;

  if (screenshotsPausedForDisplayOff) {
    console.log('[Screenshot] Paused because display/system is asleep');
    return;
  }
  const now = Date.now();
  const intervalMs = getPeriodicScreenshotIntervalMs();
  if (!periodicScreenshotNextDueAt || periodicScreenshotNextDueAt <= now) {
    const missedIntervals = periodicScreenshotNextDueAt
      ? Math.max(1, Math.ceil((now - periodicScreenshotNextDueAt + 1) / intervalMs))
      : 1;
    periodicScreenshotNextDueAt = (periodicScreenshotNextDueAt || now) + (missedIntervals * intervalMs);
  }

  // Periodic screenshot tick (silent)
  try {
    const result = await withTimeout(
      captureDesktopSensorSnapshot('periodic-screenshot'),
      25000,
      'periodic-screenshot'
    );
    if (result?.imagePath) {
      console.log(result?.filtered ? "[Screenshot] Periodic screenshot filtered:" : "[Screenshot] Periodic screenshot captured:", {
        file: result.screenshot_filename,
        app: result.activeApp || '',
        ocr_status: result.ocrStatus,
        text_source: result.textCaptureSource || 'none',
        filtered: Boolean(result?.filtered),
        reason: result?.filter_reason || null
      });
    } else {
      console.warn("[Screenshot] Periodic screenshot skipped:", result?.reason || "unknown");
    }
  } catch (error) {
    // Force-release the lock if the capture timed out or crashed, so next cycle isn't blocked.
    sensorCaptureInProgress = false;
    sensorCaptureStartedAt = 0;
    const msg = error?.message || String(error || '');
    if (/timed out/i.test(msg)) {
      console.warn('[Screenshot] Periodic screenshot timed out — lock released for next cycle');
    } else {
      console.error('[Screenshot] Periodic screenshot failed:', msg);
    }
  } finally {
    if (!screenshotsPausedForDisplayOff && periodicScreenshotRunning) {
      const nextIntervalMs = getPeriodicScreenshotIntervalMs();
      while (periodicScreenshotNextDueAt <= Date.now()) {
        periodicScreenshotNextDueAt += nextIntervalMs;
      }
      scheduleNextPeriodicScreenshot(periodicScreenshotNextDueAt - Date.now());
    }
  }
}

function pausePeriodicScreenshotCapture(reason = 'display/system asleep') {
  screenshotsPausedForDisplayOff = true;
  periodicScreenshotPauseReason = String(reason || 'paused');
  clearPeriodicScreenshotTimer();
  periodicScreenshotNextDueAt = 0;
  console.log(`[Screenshot] Paused: ${reason}`);
}

function resumePeriodicScreenshotCapture(reason = 'display/system awake') {
  const wasPaused = screenshotsPausedForDisplayOff;
  screenshotsPausedForDisplayOff = false;
  periodicScreenshotPauseReason = '';

  if (!periodicScreenshotRunning) {
    console.log(`[Screenshot] Resume requested but periodic capture is not running: ${reason}`);
    return;
  }

  lastAcceptedCaptureFingerprint = null;
  const wakeDelayMs = getPeriodicScreenshotWakeDelayMs();
  periodicScreenshotNextDueAt = Date.now() + wakeDelayMs;
  console.log(`[Screenshot] Resumed: ${reason}; next capture in ${Math.round(wakeDelayMs / 100) / 10}s`);
  scheduleNextPeriodicScreenshot(wakeDelayMs);

  if (!wasPaused) {
    console.log('[Screenshot] Wake/resume event received while already active; timer refreshed');
  }
}

function startPeriodicScreenshotCapture(mode = null, options = {}) {
  clearPeriodicScreenshotTimer();

  const currentMode = mode || getPerformanceMode();
  const intervalMs = getPeriodicScreenshotIntervalMs(currentMode);
  const initialDelayMs = Math.max(1000, Number(options.initialDelayMs || getPeriodicScreenshotWakeDelayMs(currentMode)));

  periodicScreenshotRunning = true;
  periodicScreenshotNextDueAt = Date.now() + initialDelayMs;

  console.log(`[Screenshot] Starting periodic screenshot capture every ${Math.round(intervalMs / 1000)}s (mode=${currentMode}, first=${Math.round(initialDelayMs / 100) / 10}s)`);

  scheduleNextPeriodicScreenshot(initialDelayMs);
  startPeriodicScreenshotWatchdog();
}

function stopPeriodicScreenshotCapture() {
  periodicScreenshotRunning = false;
  clearPeriodicScreenshotTimer();
  periodicScreenshotNextDueAt = 0;
}

// Schedule daily tasks
function scheduleDailyTasks() {
  const now = new Date();

  if (dailySummaryTimer) clearTimeout(dailySummaryTimer);
  if (patternUpdateTimer) clearTimeout(patternUpdateTimer);
  if (morningBriefTimer) clearTimeout(morningBriefTimer);

  // Schedule daily summary at 4:00 PM
  const summaryTime = new Date();
  summaryTime.setHours(16, 0, 0, 0);
  if (summaryTime <= now) {
    summaryTime.setDate(summaryTime.getDate() + 1);
  }
  const summaryDelay = summaryTime.getTime() - now.getTime();

  dailySummaryTimer = setTimeout(async () => {
    console.log('Running scheduled daily summary generation...');
    try {
      await generateDailySummary();
      console.log('Scheduled daily summary completed');
    } catch (error) {
      console.error('Error in scheduled daily summary:', error);
    }

    // Schedule next day's summary
  }, summaryDelay);

  console.log(`Daily summary scheduled for: ${summaryTime.toLocaleString()}`);

  // Schedule pattern update at 11:59 PM
  const patternTime = new Date();
  patternTime.setHours(23, 59, 0, 0);
  if (patternTime <= now) {
    patternTime.setDate(patternTime.getDate() + 1);
  }
  const patternDelay = patternTime.getTime() - now.getTime();

  patternUpdateTimer = setTimeout(async () => {
    console.log('Running scheduled pattern update...');
    try {
      // Update user patterns based on today's activity
      const userProfile = store.get('userProfile') || {};
      const browsingHistory = await refreshBrowserHistory({
        reason: 'pattern_update',
        maxAgeMs: BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS
      });

      // Analyze patterns and update profile
      await updateUserPatterns(userProfile, browsingHistory);
      store.set('userProfile', userProfile);

      console.log('Scheduled pattern update completed');
    } catch (error) {
      console.error('Error in scheduled pattern update:', error);
    }

    // Schedule next day's pattern update
  }, patternDelay);

  console.log(`Pattern update scheduled for: ${patternTime.toLocaleString()}`);

  // Schedule minutely GSuite sync
  const lastSync = store.get('googleData')?.lastSync;
  const lastSyncMs = parseSyncCursorMs(lastSync);
  const timeSinceSync = lastSyncMs ? (now.getTime() - lastSyncMs) : GSUITE_SYNC_INTERVAL_MS;
  const syncDelay = Math.max(STARTUP_HEAVY_JOB_DELAY_MS, GSUITE_SYNC_INTERVAL_MS - timeSinceSync);

  if (minutelySyncTimer) clearTimeout(minutelySyncTimer);
  minutelySyncTimer = setTimeout(async () => {
    await runMinutelySync();
    startMinutelySyncLoop();
  }, syncDelay);

  console.log(`GSuite sync scheduled for: ${new Date(Date.now() + syncDelay).toLocaleString()}`);
}

async function runMinutelySync() {
  console.log('Running automated minutely GSuite sync...');
  if (shouldDeferBackgroundWork('GSuiteSync')) return;
  if (global.__gsuite_sync_lock) {
    console.log('GSuite sync already running; skipping this cycle');
    return;
  }
  if (!beginHeavyJob('gsuite_sync')) {
    enqueueHeavyJob('gsuite_sync', () => runMinutelySync(), { source: 'scheduler' });
    return;
  }
  global.__gsuite_sync_lock = true;
  try {
    const startedAt = Date.now();
    updateMemoryGraphHealth({
      lastSyncAttemptAt: new Date().toISOString(),
      syncStatus: 'running'
    });
    const existingGoogleData = store.get('googleData') || {};
    const lastContactsSyncAt = Date.parse((store.get('googleSyncHealth') || {}).lastContactsSyncAt || '') || 0;
    const shouldFetchContacts = RELATIONSHIP_FEATURE_ENABLED && (!lastContactsSyncAt || (Date.now() - lastContactsSyncAt) >= GOOGLE_CONTACTS_SYNC_INTERVAL_MS);
    const googleDelta = await getGoogleData({ since: existingGoogleData.lastSync, includeContacts: shouldFetchContacts });
    const syncMeta = googleDelta._meta || {};
    const existingEmailIds = new Set((existingGoogleData.gmail || []).map((m) => m.id));
    const existingEventsById = new Map((existingGoogleData.calendar || []).map((e) => [e.id, e]));
    let newEmailCount = 0;
    let newEventCount = 0;
    let editedEventCount = 0;
    let contactsMerged = 0;

    for (const msg of (googleDelta.gmail || [])) {
      if (!existingEmailIds.has(msg.id)) newEmailCount += 1;
    }
    for (const ev of (googleDelta.calendar || [])) {
      const prior = existingEventsById.get(ev.id);
      if (!prior) {
        newEventCount += 1;
      } else {
        const prevUpdated = prior.updated ? new Date(prior.updated).getTime() : 0;
        const nextUpdated = ev.updated ? new Date(ev.updated).getTime() : 0;
        if (nextUpdated > prevUpdated || String(prior.start_time || prior.start || '') !== String(ev.start_time || ev.start || '')) {
          editedEventCount += 1;
        }
      }
    }

    const shouldAdvanceLastSync = !syncMeta.hardFailure;
    const nextLastSync = shouldAdvanceLastSync ? new Date().toISOString() : (existingGoogleData.lastSync || null);
    const googleData = mergeGoogleData(existingGoogleData, {
      ...googleDelta,
      lastSync: nextLastSync
    });
    store.set('googleData', googleData);
    store.set('googleSyncHealth', {
      lastCheckAt: new Date().toISOString(),
      cursorAdvanced: shouldAdvanceLastSync,
      transport: syncMeta.transport || 'google-api',
      syncMeta,
      checks: {
        newEmails: newEmailCount,
        newEvents: newEventCount,
        editedEvents: editedEventCount
      }
    });
    debouncedStoreSet('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastSyncRunAt: new Date().toISOString(),
      syncStatus: 'idle',
      syncChecks: {
        newEmails: newEmailCount,
        newEvents: newEventCount,
        editedEvents: editedEventCount
      }
    });

    // Incremental L1 Data Ingestion: Pipe directly to SQLite events
    try {
      
      let ingestedCount = 0;

      if (googleDelta.gmail) {
        for (const msg of googleDelta.gmail) {
          await ingestRawEvent({
            type: 'EmailThread',
            timestamp: msg.timestamp,
            source: 'Gmail',
            text: [
              msg.from ? `From: ${msg.from}` : '',
              msg.to ? `To: ${Array.isArray(msg.to) ? msg.to.join(', ') : msg.to}` : '',
              msg.subject ? `Subject: ${msg.subject}` : '',
              msg.snippet || ''
            ].filter(Boolean).join('\n'),
            metadata: {
              ...msg,
              app: 'Gmail'
            }
          });
          ingestedCount++;
        }
      }
      if (googleDelta.drive) {
        for (const doc of googleDelta.drive) {
          await ingestRawEvent({ type: 'Document', timestamp: doc.timestamp, source: 'drive', text: doc.title || doc.name, metadata: doc });
          ingestedCount++;
        }
      }
      if (googleDelta.calendar) {
        for (const cal of googleDelta.calendar) {
          const updatedTs = cal.updated ? new Date(cal.updated).getTime() : 0;
          const stableVersion = updatedTs || (cal.timestamp || new Date(cal.start_time || Date.now()).getTime()) || Date.now();
          await ingestRawEvent({
            type: 'CalendarEvent',
            timestamp: cal.timestamp || cal.start_time,
            source: 'Calendar',
            text: [
              cal.summary ? `Event: ${cal.summary}` : '',
              (cal.start_time || cal.start) ? `Start: ${cal.start_time || cal.start}` : '',
              (cal.end_time || cal.end) ? `End: ${cal.end_time || cal.end}` : '',
              Array.isArray(cal.attendees) && cal.attendees.length
                ? `Attendees: ${cal.attendees.map((a) => a.email || a).join(', ')}`
                : '',
              cal.description || ''
            ].filter(Boolean).join('\n'),
            metadata: {
              ...cal,
              app: 'Calendar',
              original_event_id: cal.id,
              id: `cal_${cal.id || 'evt'}_${stableVersion}`
            }
          });
          ingestedCount++;
        }
      }

      const lastContactsSyncAt = Date.parse((store.get('googleSyncHealth') || {}).lastContactsSyncAt || '') || 0;
      const shouldRefreshContacts = RELATIONSHIP_FEATURE_ENABLED && Array.isArray(googleDelta.contacts) && googleDelta.contacts.length > 0
        && (!lastContactsSyncAt || (Date.now() - lastContactsSyncAt) >= GOOGLE_CONTACTS_SYNC_INTERVAL_MS);
      if (shouldRefreshContacts) {
        const contactMerge = await syncGoogleContactsIntoRelationshipGraph({ contacts: googleDelta.contacts, force: true });
        contactsMerged = Number(contactMerge?.imported || contactMerge?.merged || 0);
        const currentHealth = store.get('googleSyncHealth') || {};
        store.set('googleSyncHealth', {
          ...currentHealth,
          lastContactsSyncAt: new Date().toISOString(),
          contactsMerged
        });
      }

      const browserHistory = await refreshBrowserHistory({
        reason: 'minutely_sync',
        maxAgeMs: BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS
      });
      const lastHistory = store.get('lastHistorySync') || 0;
      const recentHistory = browserHistory.filter(h => (h.timestamp || h.last_visit_time || Date.now()) > lastHistory);

      for (const h of recentHistory) {
        const ts = h.timestamp || h.last_visit_time || Date.now();
        const domain = (() => {
          try { return new URL(h.url || '').hostname; } catch (_) { return ''; }
        })();
        const stableId = Buffer.from(`${h.url || ''}|${ts}`).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 24);
        await ingestRawEvent({
          type: 'BrowserVisit',
          timestamp: ts,
          source: 'Browser',
          text: [
            h.title ? `Title: ${h.title}` : '',
            h.url ? `URL: ${h.url}` : '',
            domain ? `Domain: ${domain}` : ''
          ].filter(Boolean).join('\n'),
          metadata: {
            ...h,
            app: 'Browser',
            id: `hist_${stableId}`,
            captured_at: new Date(ts).toISOString(),
            captured_at_local: new Date(ts).toLocaleString(),
            domain
          }
        });
        ingestedCount++;
      }
      store.set('lastHistorySync', Date.now());

      console.log(`[runMinutelySync] Ingested ${ingestedCount} new raw events into SQLite L1. Checks: emails=${newEmailCount}, new_events=${newEventCount}, edited_events=${editedEventCount}, contacts_merged=${contactsMerged}, cursor_advanced=${shouldAdvanceLastSync}`);
      if (RELATIONSHIP_FEATURE_ENABLED && ingestedCount > 0) {
        enqueueHeavyJob('relationship_graph', () => runRelationshipGraphUpdate({ backfill: false }), { source: 'sync_followup' });
      }
      updateMemoryGraphHealth({
        syncStatus: 'idle',
        lastSyncRunAt: new Date().toISOString(),
        syncDurationMs: Date.now() - startedAt
      });
      store.set('googleSyncHealth', {
        ...(store.get('googleSyncHealth') || {}),
        mode: 'incremental_sync',
        phase: 'Idle',
        lastCheckAt: new Date().toISOString(),
        rawIngested: ingestedCount,
        contactsMerged,
        checks: {
          newEmails: newEmailCount,
          newEvents: newEventCount,
          editedEvents: editedEventCount,
          contactsMerged
        }
      });
    } catch (gErr) {
      console.warn('[runMinutelySync] L1 ingestion failed:', gErr.message || gErr);
      updateMemoryGraphHealth({
        syncStatus: 'error',
        lastSyncError: gErr?.message || String(gErr),
        lastSyncErrorAt: new Date().toISOString()
      });
    }
  } finally {
    global.__gsuite_sync_lock = false;
    endHeavyJob('gsuite_sync');
  }
}

function startMinutelySyncLoop() {
  if (minutelySyncInterval) clearInterval(minutelySyncInterval);
  debouncedStoreSet('memoryGraphHealth', {
    ...(store.get('memoryGraphHealth') || {}),
    minutelySyncLoopActive: true
  });
  minutelySyncInterval = setInterval(() => {
    runMinutelySync().catch((e) => console.warn('Minutely sync interval failed:', e?.message || e));
  }, GSUITE_SYNC_INTERVAL_MS);
}

function startSourceWarmup() {
  if (!heavyJobState.activeJob) {
    refreshBrowserHistory({
      reason: 'startup_warmup',
      maxAgeMs: BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS
    }).catch((e) => console.warn('[Warmup] Browser history warmup failed:', e?.message || e));
  }
  enqueueHeavyJob('gsuite_sync', () => runMinutelySync(), { source: 'startup_warmup' });
  drainPendingHeavyJobs();
}

// ── Memory Graph Processing Functions ───────────────────────────────
async function hasNewNodesSince(jobKey, layers = []) {
  const lastRun = store.get(`lastRunTimestamp:${jobKey}`) || "2000-01-01T00:00:00.000Z";
  const placeholders = layers.map(() => "?").join(",");
  const row = await db.getQuery(`SELECT COUNT(*) as count, MAX(created_at) as max_at FROM memory_nodes WHERE layer IN (${placeholders}) AND created_at > ?`, [...layers, lastRun]);
  return {
    hasNew: row && row.count > 0,
    maxAt: row && row.max_at ? row.max_at : lastRun
  };
}

async function hasNewEventsSince(jobKey) {
  const lastProcessed = store.get(`lastProcessedEventTimestamp:${jobKey}`) || "2000-01-01T00:00:00.000Z";
  const row = await db.getQuery(`SELECT COUNT(*) as count, MAX(timestamp) as max_ts FROM events WHERE timestamp > ?`, [lastProcessed]);
  return {
    hasNew: row && row.count > 0,
    maxTs: row && row.max_ts ? row.max_ts : lastProcessed
  };
}

async function runRelationshipGraphUpdate(options = {}) {
  if (!RELATIONSHIP_FEATURE_ENABLED) {
    return { contacts: 0, skipped: true, disabled: true };
  }
  if (!options?.force && shouldDeferBackgroundWork(options?.backfill ? 'RelationshipGraphBackfill' : 'RelationshipGraph')) {
    return { contacts: 0, deferred: true };
  }
  const jobName = options?.backfill ? 'relationship_graph_backfill' : 'relationship_graph';
  if (!beginHeavyJob(jobName, { source: options?.force ? 'manual' : 'background' })) {
    if (!options?.force) enqueueHeavyJob(jobName, () => runRelationshipGraphUpdate(options), { source: 'scheduler' });
    return { contacts: 0, deferred: true, blocked_by: heavyJobState.activeJob };
  }
  try {
    const startedAt = Date.now();
    const result = await runRelationshipGraphJob(options);
    updateMemoryGraphHealth({
      lastRelationshipGraphRunAt: new Date().toISOString(),
      relationshipContactCount: result.contacts,
      relationshipGraphStatus: 'idle',
      relationshipGraphDurationMs: Date.now() - startedAt
    });
    console.log(`[RelationshipGraph] Updated ${result.contacts} contacts${options.backfill ? ' (backfill)' : ''}`);
    return result;
  } catch (error) {
    updateMemoryGraphHealth({
      relationshipGraphStatus: 'error',
      lastRelationshipGraphError: error?.message || String(error),
      lastRelationshipGraphErrorAt: new Date().toISOString()
    });
    console.warn('[RelationshipGraph] Update failed:', error?.message || error);
    return { contacts: 0, error: error?.message || String(error) };
  } finally {
    endHeavyJob(jobName);
  }
}


async function runEpisodeGeneration() {
  if (shouldDeferBackgroundWork('EpisodeJob')) return;
  if (episodeJobLock) {
    console.log('[EpisodeJob] Already running, skipping this cycle');
    return;
  }
  const check = await hasNewEventsSince('episode');
  if (!check.hasNew) {
    console.log('[EpisodeJob] No new events since last run, skipping');
    return;
  }
  if (!canRunHeavyJob(lastEpisodeHeavyRunAt)) {
    console.log('[EpisodeJob] Skipping to reduce system load / active use');
    return;
  }
  if (!beginHeavyJob('episode_generation')) {
    enqueueHeavyJob('episode_generation', () => runEpisodeGeneration(), { source: 'scheduler' });
    return;
  }
  lastEpisodeHeavyRunAt = Date.now();
  episodeJobLock = true;

  try {
    const startedAt = Date.now();
    console.log('[EpisodeJob] Running 15-minute episode generation...');

    updateMemoryGraphHealth({
      lastEpisodeAttemptAt: new Date().toISOString(),
      episodeStatus: 'running'
    });
    const newEpisodeIds = await runEpisodeJob(process.env.DEEPSEEK_API_KEY || null);
    console.log(`[EpisodeJob] Generated ${newEpisodeIds.length} new episodes`);
    store.set('lastEpisodeRun', new Date().toISOString());
    store.set('lastProcessedEventTimestamp:episode', check.maxTs);
    updateMemoryGraphHealth({
      lastEpisodeRunAt: new Date().toISOString(),
      lastEpisodeCount: newEpisodeIds.length,
      episodeStatus: 'idle',
      episodeDurationMs: Date.now() - startedAt
    });

    // Send status to UI
    emitMemoryGraphUpdate({
      type: 'episodes_generated',
      count: newEpisodeIds.length
    });
  } catch (error) {
    console.error('[EpisodeJob] Error:', error.message || error);
    updateMemoryGraphHealth({
      episodeStatus: 'error',
      lastEpisodeError: error?.message || String(error),
      lastEpisodeErrorAt: new Date().toISOString()
    });
  } finally {
    episodeJobLock = false;
    endHeavyJob('episode_generation');
  }
}

async function runSemanticWindowGeneration() {
  try {
    if (shouldDeferBackgroundWork('SemanticWindow')) return;
    const check = await hasNewEventsSince('semantic');
    if (!check.hasNew) {
      console.log('[SemanticWindow] No new events since last run, skipping');
      return;
    }
    if (!canRunHeavyJob(lastEpisodeHeavyRunAt)) {
      console.log('[SemanticWindow] Skipping to reduce system load');
      return;
    }
    if (!beginHeavyJob('semantic_window')) {
      enqueueHeavyJob('semantic_window', () => runSemanticWindowGeneration(), { source: 'scheduler' });
      return;
    }
    const startedAt = Date.now();
    console.log('[SemanticWindow] Running 15-minute semantic summary...');
    const result = await runSemanticSummaryWindow(15 * 60 * 1000, process.env.DEEPSEEK_API_KEY || null);
    store.set('lastProcessedEventTimestamp:semantic', check.maxTs);
    updateMemoryGraphHealth({
      semanticWindowStatus: 'idle',
      semanticWindowDurationMs: Date.now() - startedAt,
      lastSemanticWindowRunAt: new Date().toISOString()
    });
    const semIds = Array.isArray(result) ? result.filter(Boolean) : (result ? [result] : []);
    if (semIds.length) {
      console.log('[SemanticWindow] Created semantic nodes:', semIds.join(', '));
      for (const id of semIds) {
        emitMemoryGraphUpdate({
          type: 'semantic_window_generated',
          id,
          count: semIds.length
        });
      }
    } else {
      console.log('[SemanticWindow] No events to summarize in this window');
    }
  } catch (e) {
    console.error('[SemanticWindow] Error:', e?.message || e);
    updateMemoryGraphHealth({
      semanticWindowStatus: 'error',
      lastSemanticWindowError: e?.message || String(e),
      lastSemanticWindowErrorAt: new Date().toISOString()
    });
  } finally {
    endHeavyJob('semantic_window');
  }
}

async function runSuggestionEngineJob(options = {}) {
  const force = options?.force === true;
  if (!force && shouldDeferBackgroundWork('SuggestionEngine')) return;
  if (suggestionJobLock) {
    suggestionRunQueued = true;
    const now = Date.now();
    if ((now - lastSuggestionLockSkipLogAt) > (5 * 60 * 1000)) {
      console.log('[SuggestionEngine] Already running; queued one follow-up run');
      lastSuggestionLockSkipLogAt = now;
    }
    return;
  }
  const check = await hasNewEventsSince('suggestion');
  const existingState = getStoredRadarState();
  if (!force && !check.hasNew && Array.isArray(existingState?.allSignals) && existingState.allSignals.length) {
    console.log('[SuggestionEngine] No new events; keeping current radar state');
    return;
  }
  if (!beginHeavyJob('radar_generation', { source: force ? 'manual' : 'background' })) {
    suggestionRunQueued = true;
    if (!force) enqueueHeavyJob('radar_generation', () => runSuggestionEngineJob(options), { source: 'scheduler' });
    return { deferred: true, blocked_by: heavyJobState.activeJob };
  }
  lastSuggestionHeavyRunAt = Date.now();
  suggestionJobLock = true;

  try {
    const startedAt = Date.now();
    const llmConfig = getSuggestionLLMConfig();
    if (!llmConfig) {
      console.warn('[SuggestionEngine] No active LLM configuration; skipping radar generation');
      if (mainWindow && mainWindow.webContents) {
        mainWindow.webContents.send('proactive-suggestions', getStoredRadarState());
      }
      return;
    }

    const episodeCountRow = await db.getQuery(`SELECT COUNT(1) AS count FROM memory_nodes WHERE layer = 'episode'`).catch(() => ({ count: 0 }));
    const episodeCount = Number(episodeCountRow?.count || 0);
    const lastEpisodeRunMs = Date.parse(String(store.get('lastEpisodeRun') || '')) || 0;
    const episodeGraphStale = !lastEpisodeRunMs || (Date.now() - lastEpisodeRunMs) > (45 * 60 * 1000);
    const shouldWarmEpisodes = episodeCount === 0 || (!force && episodeGraphStale);
    if (!episodeJobLock && shouldWarmEpisodes) {
      console.log('[SuggestionEngine] Refreshing episode graph before suggestion generation...');
      await runEpisodeGeneration().catch((err) => {
        console.warn('[SuggestionEngine] Episode refresh failed before suggestion run:', err?.message || err);
      });
    }

    console.log(`[SuggestionEngine] Running radar planner (new_events=${Boolean(check.hasNew)}, provider=${llmConfig.provider}, force=${force})...`);
    const manualTodos = (store.get('persistentTodos') || []).filter((todo) => !todo?.completed);

    updateMemoryGraphHealth({
      lastSuggestionAttemptAt: new Date().toISOString(),
      suggestionStatus: 'running'
    });
    const radarState = await withTimeout(
      buildRadarState({
        llmConfig,
        manualTodos,
        maxCentralSignals: 5,
        maxRelationshipSignals: RELATIONSHIP_FEATURE_ENABLED ? 5 : 0,
        maxTodoSignals: 5,
        existingState
      }),
      45000,
      'buildRadarState'
    );
    persistRadarState(radarState);
    console.log(`[SuggestionEngine] Generated radar state (${radarState.relationshipSignals?.length || 0} relationship, ${radarState.todoSignals?.length || 0} todo)`);
    store.set('lastSuggestionRun', new Date().toISOString());
    store.set('lastProcessedEventTimestamp:suggestion', check.maxTs);
    updateMemoryGraphHealth({
      lastSuggestionRunAt: new Date().toISOString(),
      lastSuggestionCount: radarState.allSignals?.length || 0,
      suggestionStatus: 'idle',
      suggestionDurationMs: Date.now() - startedAt
    });
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('proactive-suggestions', radarState);
    }
    console.log(`[SuggestionEngine] Published ${radarState.allSignals?.length || 0} radar signals`);
    return radarState;
  } catch (error) {
    console.error('[SuggestionEngine] Error:', error.message || error);
    updateMemoryGraphHealth({
      suggestionStatus: 'error',
      lastSuggestionError: error?.message || String(error),
      lastSuggestionErrorAt: new Date().toISOString()
    });
    throw error;
  } finally {
    suggestionJobLock = false;
    endHeavyJob('radar_generation');
    if (suggestionRunQueued) {
      suggestionRunQueued = false;
      setTimeout(() => {
        runSuggestionEngineJob().catch((e) => console.warn('[SuggestionEngine] Queued run failed:', e?.message || e));
      }, 250);
    }
  }
}

async function runWeeklyInsightJobScheduled() {
  try {
    if (shouldDeferBackgroundWork('WeeklyInsight')) return;
    if (!beginHeavyJob('weekly_insight')) {
      enqueueHeavyJob('weekly_insight', () => runWeeklyInsightJobScheduled(), { source: 'scheduler' });
      return;
    }
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[WeeklyInsight] No DeepSeek API key, skipping weekly insights');
      return;
    }
    const check = await hasNewNodesSince('weekly_insight', ['cloud', 'episode', 'semantic']);
    if (!check.hasNew) {
      console.log('[WeeklyInsight] No new source nodes since last run, skipping');
      return;
    }

    console.log('[WeeklyInsight] Running weekly insight generation...');

    await runWeeklyInsightJob(apiKey);
    store.set('lastRunTimestamp:weekly_insight', check.maxAt);
    console.log('[WeeklyInsight] Weekly insights completed');

    // Send status to UI
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', {
        type: 'weekly_insights_completed',
        timestamp: Date.now()
      });
    }
  } catch (error) {
    console.error('[WeeklyInsight] Error:', error.message || error);
  } finally {
    endHeavyJob('weekly_insight');
  }
}

async function runDailyInsightsScheduled() {
  try {
    if (shouldDeferBackgroundWork('DailyInsight')) return;
    if (!beginHeavyJob('daily_insight')) {
      enqueueHeavyJob('daily_insight', () => runDailyInsightsScheduled(), { source: 'scheduler' });
      return;
    }
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[DailyInsight] No DeepSeek API key, skipping daily insights');
      return;
    }
    const check = await hasNewNodesSince('daily_insight', ['cloud', 'episode', 'semantic']);
    if (!check.hasNew) {
      console.log('[DailyInsight] No new source nodes since last run, skipping');
      return;
    }
    console.log('[DailyInsight] Running daily insight generation...');
    const created = await runDailyInsights(apiKey);
    store.set('lastRunTimestamp:daily_insight', check.maxAt);
    console.log('[DailyInsight] Promoted insights:', created.length);
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', { type: 'daily_insights_completed', count: created.length, timestamp: Date.now() });
    }
  } catch (e) {
    console.error('[DailyInsight] Error:', e?.message || e);
  } finally {
    endHeavyJob('daily_insight');
  }
}


async function runHourlySemanticPulseJob() {
  try {
    if (shouldDeferBackgroundWork('SemanticPulse')) return;
    if (!beginHeavyJob('semantic_pulse')) {
      enqueueHeavyJob('semantic_pulse', () => runHourlySemanticPulseJob(), { source: 'scheduler' });
      return;
    }
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[SemanticPulse] No DeepSeek API key, skipping hourly pulse');
      return;
    }
    console.log('[SemanticPulse] Running hourly semantic pulse (deduplication & graduation)...');
    const consolidatedIds = await runHourlySemanticPulse(apiKey);
    console.log(`[SemanticPulse] Consolidated ${consolidatedIds.length} semantic nodes`);

    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', {
        type: 'hourly_pulse_completed',
        count: consolidatedIds.length,
        timestamp: Date.now()
      });
    }
  } catch (e) {
    console.error('[SemanticPulse] Error:', e?.message || e);
  } finally {
    endHeavyJob('semantic_pulse');
  }
}

async function runLivingCoreJobScheduled() {
  try {
    if (shouldDeferBackgroundWork('LivingCore')) return;
    if (!beginHeavyJob('living_core')) {
      enqueueHeavyJob('living_core', () => runLivingCoreJobScheduled(), { source: 'scheduler' });
      return;
    }
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn("[LivingCore] No DeepSeek API key, skipping Living Core synthesis");
      return;
    }
    const check = await hasNewNodesSince('living_core', ['insight']);
    if (!check.hasNew) {
      console.log('[LivingCore] No new source nodes since last run, skipping');
      return;
    }
    console.log("[LivingCore] Running Living Core synthesis job...");
    const created = await runLivingCoreJob(apiKey);
    store.set('lastRunTimestamp:living_core', check.maxAt);
    console.log("[LivingCore] Created core nodes:", created.length);
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send("memory-graph-update", { type: "living_core_completed", count: created.length, timestamp: Date.now() });
    }
  } catch (e) {
    console.error("[LivingCore] Error:", e?.message || e);
  } finally {
    endHeavyJob('living_core');
  }
}

function startMemoryGraphProcessing() {
  console.log('[MemoryGraph] Starting automated processing...');
  debouncedStoreSet('memoryGraphHealth', {
    ...(store.get('memoryGraphHealth') || {}),
    processorStartedAt: new Date().toISOString(),
    processorTimersActive: true
  });

  function scheduleAlignedJobs() {
    // EMERGENCY THROTTLING - Disable most background jobs
    if (EMERGENCY_THROTTLE_ENABLED) {
      console.log('[Emergency] Background jobs disabled due to CPU throttling');
      return;
    }
  
    try {
      const quarterHourMs = 15 * 60 * 1000;
      // clear any previous timers
      if (episodeGenerationTimer) try { clearInterval(episodeGenerationTimer); } catch (_) {}
      if (semanticsTimer) try { clearInterval(semanticsTimer); } catch (_) {}

      const now = Date.now();
      // compute next quarter-hour boundary
      const nextBoundary = Math.ceil(now / quarterHourMs) * quarterHourMs;
      const delay = Math.max(1000, nextBoundary - now);

      // schedule first aligned run, then set repeating intervals
      setTimeout(() => {
        runEpisodeGeneration().catch((e) => console.warn('[MemoryGraph] Aligned episode generation failed:', e?.message || e));
        try { episodeGenerationTimer = setInterval(runEpisodeGeneration, 30 * 60 * 1000); } catch (_) {} // INCREASED from 15min
        // also kick off semantic window at same boundary
        runSemanticWindowGeneration().catch((e) => console.warn('[MemoryGraph] Aligned semantic window failed:', e?.message || e));
        try { semanticsTimer = setInterval(runSemanticWindowGeneration, 30 * 60 * 1000); } catch (_) {} // INCREASED from 15min
      }, delay);
      console.log('[MemoryGraph] Aligned episode (30m) & semantic (30m) scheduling, first run in', Math.round(delay / 1000), 's');
    } catch (e) {
      console.warn('[MemoryGraph] Failed to schedule aligned quarter-hour jobs, falling back to interval timers:', e?.message || e);
      if (episodeGenerationTimer) clearInterval(episodeGenerationTimer);
      episodeGenerationTimer = setInterval(runEpisodeGeneration, 30 * 60 * 1000); // INCREASED from 15min
      if (semanticsTimer) clearInterval(semanticsTimer);
      semanticsTimer = setInterval(runSemanticWindowGeneration, 30 * 60 * 1000); // INCREASED from 15min
    }


    // Hourly semantic pulse aligned to the hour boundary (:00)
    try {
      if (semanticsPulseTimer) try { clearInterval(semanticsPulseTimer); } catch (_) {}
      const hourMs = 60 * 60 * 1000;
      const nextHour = Math.ceil(Date.now() / hourMs) * hourMs;
      const hourDelay = Math.max(1000, nextHour - Date.now());
      setTimeout(() => {
        runHourlySemanticPulseJob().catch((e) => console.warn('[MemoryGraph] Aligned semantic pulse failed:', e?.message || e));
        try { semanticsPulseTimer = setInterval(runHourlySemanticPulseJob, hourMs); } catch (_) {}
      }, hourDelay);
    } catch (e) {
      console.warn('[MemoryGraph] Failed to schedule aligned hourly pulse:', e?.message || e);
    }

  // Suggestion engine hourly; each run only fills missing slots up to 7.
  if (suggestionEngineTimer) clearInterval(suggestionEngineTimer);
  // Aligned relationship suggestion engine
  function scheduleSuggestionEngineJob() {
    // EMERGENCY THROTTLING - Disable suggestion engine
    if (EMERGENCY_THROTTLE_ENABLED) {
      console.log('[Emergency] Suggestion engine disabled due to CPU throttling');
      return;
    }
  
    try {
      if (suggestionEngineTimer) try { clearInterval(suggestionEngineTimer); } catch (_) {}
      const suggestionIntervalMs = SUGGESTION_REFRESH_INTERVAL_MINUTES * 60 * 1000;
      const nextBoundary = Math.ceil(Date.now() / suggestionIntervalMs) * suggestionIntervalMs;
      const suggestionDelay = Math.max(10 * 60 * 1000, nextBoundary - Date.now());
      setTimeout(() => {
        runSuggestionEngineJob().catch((e) => console.warn('[MemoryGraph] Aligned suggestion generation failed:', e?.message || e));
        try { suggestionEngineTimer = setInterval(runSuggestionEngineJob, suggestionIntervalMs); } catch (_) {}
      }, suggestionDelay);
    } catch (e) {
      console.warn('[MemoryGraph] Failed to schedule aligned suggestion job:', e?.message || e);
      suggestionEngineTimer = setInterval(runSuggestionEngineJob, SUGGESTION_REFRESH_INTERVAL_MINUTES * 60 * 1000);
    }
  }
  scheduleSuggestionEngineJob();

  if (relationshipGraphTimer) clearInterval(relationshipGraphTimer);
  if (RELATIONSHIP_FEATURE_ENABLED && !EMERGENCY_THROTTLE_ENABLED) {
    relationshipGraphTimer = setInterval(() => {
      runRelationshipGraphUpdate({ backfill: false }).catch((e) => console.warn('[RelationshipGraph] Scheduled update failed:', e?.message || e));
    }, 12 * 60 * 60 * 1000);
  }

  // Schedule weekly insights for Sunday 11:59 PM
  if (!EMERGENCY_THROTTLE_ENABLED) {
    scheduleWeeklyInsights();
  }

  // Schedule daily insights (every day at 23:00 local time by default)
  if (!EMERGENCY_THROTTLE_ENABLED) {
    scheduleDailyInsights();
    scheduleLivingCore();
  }

  // Avoid heavy graph work during initial UI load; rely on scheduled aligned runs.

}

function scheduleWeeklyInsights() {
  if (weeklyInsightTimer) clearTimeout(weeklyInsightTimer);

  const now = new Date();
  const nextSunday = new Date(now);
  nextSunday.setDate(now.getDate() + ((7 - now.getDay()) % 7 || 7)); // Next Sunday
  nextSunday.setHours(23, 59, 0, 0);

  if (nextSunday <= now) {
    nextSunday.setDate(nextSunday.getDate() + 7); // Next week if already past
  }

  const delay = nextSunday.getTime() - now.getTime();

  weeklyInsightTimer = setTimeout(async () => {
    await runWeeklyInsightJobScheduled();
    // Schedule next week
    scheduleWeeklyInsights();
  }, delay);

  console.log(`[WeeklyInsight] Scheduled for: ${nextSunday.toLocaleString()}`);
}

function scheduleDailyInsights() {
  if (dailyInsightTimer) clearTimeout(dailyInsightTimer);
  const now = new Date();
  const next = new Date(now);
  // schedule for 23:00 local time today or next
  next.setHours(23, 0, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  const delay = next.getTime() - now.getTime();
  dailyInsightTimer = setTimeout(async function tick() {
    await runDailyInsightsScheduled();
    // schedule next day
    dailyInsightTimer = setTimeout(tick, 24 * 60 * 60 * 1000);
  }, delay);
  console.log('[DailyInsight] Scheduled for:', next.toLocaleString());
}

function scheduleLivingCore() {
  if (livingCoreTimer) clearTimeout(livingCoreTimer);
  const now = new Date();
  const next = new Date(now);
  next.setHours(1, 0, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  const delay = next.getTime() - now.getTime();
  livingCoreTimer = setTimeout(async function tick() {
    await runLivingCoreJobScheduled();
    livingCoreTimer = setTimeout(tick, 24 * 60 * 60 * 1000);
  }, delay);
  console.log("[LivingCore] Scheduled for:", next.toLocaleString());
}

function stopMemoryGraphProcessing() {
  if (episodeGenerationTimer) {
    clearInterval(episodeGenerationTimer);
    episodeGenerationTimer = null;
  }
  if (suggestionEngineTimer) {
    clearInterval(suggestionEngineTimer);
    suggestionEngineTimer = null;
  }
  if (weeklyInsightTimer) {
    clearTimeout(weeklyInsightTimer);
    weeklyInsightTimer = null;
  }
  if (livingCoreTimer) {
    clearTimeout(livingCoreTimer);
    livingCoreTimer = null;
  }
  if (semanticsPulseTimer) {
    clearInterval(semanticsPulseTimer);
    semanticsPulseTimer = null;
  }
  console.log('[MemoryGraph] Automated processing stopped');
}

// Update user patterns based on browsing behavior
async function updateUserPatterns(userProfile, browsingHistory) {
  const today = new Date().toDateString();
  const todayHistory = browsingHistory.filter(item =>
    new Date(item.timestamp || item.last_visit_time || Date.now()).toDateString() === today
  );

  // Analyze most visited domains
  const domainCounts = {};
  todayHistory.forEach(item => {
    try {
      const domain = new URL(item.url).hostname;
      domainCounts[domain] = (domainCounts[domain] || 0) + 1;
    } catch (e) {}
  });

  const topDomains = Object.entries(domainCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([domain]) => domain);

  // Update user profile with new patterns
  userProfile.patterns = {
    ...userProfile.patterns,
    topDomains,
    lastUpdated: new Date().toISOString(),
    analysisDate: today
  };

  return userProfile;
}

// Data Models for Proactive AI
const DataModels = {
  // Browser history events
  PageVisit: {
    domain: String,
    path: String,
    page_title: String,
    timestamp: Number,
    duration: Number,
    device_type: String
  },

  // Gmail events
  MessageEvent: {
    from: String,
    to: String,
    cc: String,
    subject: String,
    body_snippet: String,
    timestamp: Number,
    is_action_item: Boolean,
    is_unreplied: Boolean,
    opened_times: Number
  },

  // Calendar events
  CalendarEvent: {
    event_title: String,
    start_time: Number,
    end_time: Number,
    attendees: Array,
    is_recurring: Boolean,
    type: String // "Exam", "Interview", "Team meeting", "Workout"
  },

  // Drive/Docs events
  DocEvent: {
    doc_id: String,
    doc_name: String,
    shared_with: Array,
    last_modified: Number,
    last_opened: Number,
    state: String // "draft", "in-review", "submitted"
  }
};

// Intent clusters for classification
const IntentClusters = {
  JOB_SEARCH: 'job_search',
  THESIS_STUDY: 'thesis_study',
  RELATIONSHIP_NETWORKING: 'relationship_networking',
  PERSONAL_CARE: 'personal_care',
  SIDE_PROJECT: 'side_project',
  PROCRASTINATION_DISTRACTION: 'procrastination_distraction'
};

// Core Proactive AI Algorithm
class ProactiveSuggestionEngine {
  constructor() {
    this.userGoals = store.get('userGoals') || [];
    this.userPreferences = store.get('userPreferences') || {
      intensity: 'medium', // light | medium | heavy
      preferred_time: 'morning', // morning | afternoon | evening
      privacy: {
        can_use_emails: true,
        can_use_browser_history: true
      }
    };
    this.learnedState = store.get('learnedState') || {
      avg_productive_hours: [9, 10, 14, 15],
      cluster_weights: {
        job_search: 0.8,
        thesis_study: 0.7,
        relationship_networking: 0.6,
        side_project: 0.6,
        personal_care: 0.5
      }
    };
    this.patternState = store.get('userPatternState') || {
      productive_vs_distracting: 0.5,
      high_leverage_vs_low_leverage: 0.5,
      weekly_trend: 'stable'
    };
  }

  // ─── Helper models derived from signals ─────────────────────────
  computeFrequentDomains(pageVisits) {
    const map = new Map();
    pageVisits.forEach(v => {
      const key = v.domain || '';
      if (!key) return;
      const cur = map.get(key) || { domain: key, count: 0, last_seen: 0, total_duration: 0 };
      cur.count += 1;
      cur.last_seen = Math.max(cur.last_seen, v.timestamp || 0);
      cur.total_duration += v.duration || 0;
      map.set(key, cur);
    });
    return Array.from(map.values()).map(d => ({
      domain: d.domain,
      count_last_7_days: d.count,
      avg_duration_ms: d.count ? Math.round(d.total_duration / d.count) : 0,
      last_seen: d.last_seen
    }));
  }

  extractRecentlyViewedEntities(pageVisits) {
    const entities = new Map();
    pageVisits.forEach(v => {
      const url = v.url || '';
      const domain = v.domain || this.extractDomain(url);
      const path = v.path || this.extractPath(url);
      let entity_type = null;
      let entity_id = null;
      let raw_name = v.page_title || '';

      if (domain.includes('linkedin.com')) {
        if (path.includes('/in/')) {
          entity_type = 'person';
          entity_id = path.split('/in/')[1]?.split('/')[0] || raw_name;
        } else if (path.includes('/company/')) {
          entity_type = 'company';
          entity_id = path.split('/company/')[1]?.split('/')[0] || raw_name;
        } else if (path.includes('/jobs/')) {
          entity_type = 'job';
          entity_id = path.split('/jobs/')[1]?.split('/')[0] || raw_name;
        }
      } else if (domain.includes('amazon.com') || domain.includes('store')) {
        entity_type = 'product';
        entity_id = raw_name || path;
      } else if (domain.includes('docs.google.com')) {
        entity_type = 'doc';
        entity_id = path;
      } else if (raw_name) {
        entity_type = 'page';
        entity_id = raw_name;
      }

      if (!entity_type || !entity_id) return;
      const key = `${entity_type}:${entity_id}`;
      const cur = entities.get(key) || {
        entity_type,
        entity_id,
        raw_name,
        url,
        last_seen: 0,
        visit_count: 0
      };
      cur.last_seen = Math.max(cur.last_seen, v.timestamp || 0);
      cur.visit_count += 1;
      if (!cur.raw_name && raw_name) cur.raw_name = raw_name;
      if (!cur.url && url) cur.url = url;
      entities.set(key, cur);
    });
    return Array.from(entities.values());
  }

  summarizeDocStates(driveDocs) {
    return (driveDocs || []).map(doc => {
      const name = (doc.doc_name || doc.name || '').toLowerCase();
      let state = 'draft';
      if (name.includes('final') || name.includes('submitted')) state = 'submitted';
      else if (name.includes('review')) state = 'in_review';
      return {
        doc_id: doc.id || doc.doc_id || '',
        doc_name: doc.doc_name || doc.name || '',
        state,
        last_modified: doc.last_modified || doc.modified || doc.modifiedTime || 0,
        shared_with: doc.shared_with || []
      };
    });
  }

  summarizeSensorCaptures(sensorCaptures) {
    return (sensorCaptures || [])
      .filter(capture => (capture.text || '').trim().length >= 20)
      .map(capture => ({
        id: capture.id || '',
        active_app: capture.activeApp || '',
        active_window_title: capture.activeWindowTitle || '',
        text: (capture.text || '').replace(/\s+/g, ' ').trim().slice(0, 300),
        timestamp: capture.timestamp || 0,
        capture_mode: capture.captureMode || 'screen'
      }));
  }

  // Step 1: Extract intent clusters from signals
  extractIntentClusters(signals) {
    const clusters = {};

    // Initialize clusters
    Object.values(IntentClusters).forEach(cluster => {
      clusters[cluster] = { count: 0, examples: [], signals: [] };
    });

    // Process browser history
    signals.browserHistory?.forEach(visit => {
      const cluster = this.inferClusterFromPage(visit);
      if (cluster) {
        clusters[cluster].count++;
        clusters[cluster].examples.push(visit.page_title);
        clusters[cluster].signals.push({ type: 'PageVisit', data: visit });
      }
    });

    // Process Gmail
    signals.gmail?.forEach(email => {
      const cluster = this.inferClusterFromEmail(email);
      if (cluster) {
        clusters[cluster].count++;
        clusters[cluster].examples.push(email.subject);
        clusters[cluster].signals.push({ type: 'MessageEvent', data: email });
      }
    });

    // Process Calendar
    signals.calendar?.forEach(event => {
      const cluster = this.inferClusterFromCalendar(event);
      if (cluster) {
        clusters[cluster].count++;
        clusters[cluster].examples.push(event.event_title);
        clusters[cluster].signals.push({ type: 'CalendarEvent', data: event });
      }
    });

    // Process Drive/Docs
    signals.drive?.forEach(doc => {
      const cluster = this.inferClusterFromDoc(doc);
      if (cluster) {
        clusters[cluster].count++;
        clusters[cluster].examples.push(doc.doc_name);
        clusters[cluster].signals.push({ type: 'DocEvent', data: doc });
      }
    });

    // Process Sensor Captures
    signals.sensorCaptures?.forEach(capture => {
      const cluster = this.inferClusterFromSensor(capture);
      if (cluster) {
        clusters[cluster].count++;
        clusters[cluster].examples.push(capture.active_window_title || capture.active_app || capture.text.slice(0, 80));
        clusters[cluster].signals.push({ type: 'ScreenCapture', data: capture });
      }
    });

    return clusters;
  }

  inferClusterFromPage(visit) {
    const { domain, page_title } = visit;

    // Job search patterns
    if (domain.includes('linkedin.com') && page_title.includes('career')) return IntentClusters.JOB_SEARCH;
    if (domain.includes('linkedin.com') && page_title.includes('job')) return IntentClusters.JOB_SEARCH;
    if (domain.includes('indeed.com') || domain.includes('glassdoor.com')) return IntentClusters.JOB_SEARCH;
    if (domain.includes('careers') || page_title.includes('career')) return IntentClusters.JOB_SEARCH;

    // Thesis/study patterns
    if (domain.includes('university.edu') || domain.includes('edu')) return IntentClusters.THESIS_STUDY;
    if (page_title.includes('exam') || page_title.includes('thesis') || page_title.includes('research')) return IntentClusters.THESIS_STUDY;

    // Relationship/networking patterns
    if (domain.includes('linkedin.com') && page_title.includes('profile')) return IntentClusters.RELATIONSHIP_NETWORKING;
    if (domain.includes('facebook.com') || domain.includes('instagram.com')) return IntentClusters.RELATIONSHIP_NETWORKING;

    // Personal care patterns
    if (domain.includes('gym') || domain.includes('fitness') || page_title.includes('workout')) return IntentClusters.PERSONAL_CARE;
    if (domain.includes('youtube.com') && (page_title.includes('exercise') || page_title.includes('fitness'))) return IntentClusters.PERSONAL_CARE;

    // Side project patterns
    if (domain.includes('github.com') || domain.includes('notion.so') || page_title.includes('project')) return IntentClusters.SIDE_PROJECT;

    // Procrastination/distraction patterns
    if (domain.includes('youtube.com') || domain.includes('reddit.com') || domain.includes('twitter.com')) {
      return IntentClusters.PROCRASTINATION_DISTRACTION;
    }

    return null;
  }

  inferClusterFromEmail(email) {
    const { subject, body_snippet } = email;
    const content = (subject + ' ' + body_snippet).toLowerCase();

    if (content.includes('interview') || content.includes('recruiter') || content.includes('offer')) return IntentClusters.JOB_SEARCH;
    if (content.includes('thesis') || content.includes('research') || content.includes('report')) return IntentClusters.THESIS_STUDY;
    if (content.includes('coffee') || content.includes('meet') || content.includes('catch up')) return IntentClusters.RELATIONSHIP_NETWORKING;
    if (content.includes('project') || content.includes('startup') || content.includes('idea')) return IntentClusters.SIDE_PROJECT;

    return null;
  }

  inferClusterFromCalendar(event) {
    const { event_title } = event;
    const title = event_title.toLowerCase();

    if (title.includes('interview')) return IntentClusters.JOB_SEARCH;
    if (title.includes('exam') || title.includes('study') || title.includes('class')) return IntentClusters.THESIS_STUDY;
    if (title.includes('coffee') || title.includes('meet') || title.includes('call')) return IntentClusters.RELATIONSHIP_NETWORKING;
    if (title.includes('workout') || title.includes('gym') || title.includes('exercise')) return IntentClusters.PERSONAL_CARE;
    if (title.includes('project') || title.includes('startup')) return IntentClusters.SIDE_PROJECT;

    return null;
  }

  inferClusterFromDoc(doc) {
    const { doc_name } = doc;
    const name = doc_name.toLowerCase();

    if (name.includes('resume') || name.includes('cv') || name.includes('cover')) return IntentClusters.JOB_SEARCH;
    if (name.includes('thesis') || name.includes('research') || name.includes('paper')) return IntentClusters.THESIS_STUDY;
    if (name.includes('pitch') || name.includes('proposal') || name.includes('idea')) return IntentClusters.SIDE_PROJECT;

    return null;
  }

  inferClusterFromSensor(capture) {
    const content = `${capture.active_app || ''} ${capture.active_window_title || ''} ${capture.text || ''}`.toLowerCase();
    if (/interview|recruiter|resume|cv|cover letter|application/.test(content)) return IntentClusters.JOB_SEARCH;
    if (/thesis|research|paper|study|lecture|exam/.test(content)) return IntentClusters.THESIS_STUDY;
    if (/github|pull request|notion|project|roadmap|deploy|bug/.test(content)) return IntentClusters.SIDE_PROJECT;
    if (/message|linkedin|email|calendar|meeting|follow up|reply/.test(content)) return IntentClusters.RELATIONSHIP_NETWORKING;
    if (/workout|meal|health|sleep|meditation/.test(content)) return IntentClusters.PERSONAL_CARE;
    if (/youtube|reddit|twitter|x.com|netflix/.test(content)) return IntentClusters.PROCRASTINATION_DISTRACTION;
    return null;
  }

  // Step 2: Generate candidates from signals (Gmail, PageVisits, Calendar, Drive)
  generateCandidateActions(signals) {
    const candidates = [];
    const clusterSignals = this.computeClusterSignals(signals);
    const frequentDomains = this.computeFrequentDomains(signals.browserHistory || []);
    const entities = this.extractRecentlyViewedEntities(signals.browserHistory || []);
    const docSummaries = this.summarizeDocStates(signals.drive || []);
    const sensorSummaries = this.summarizeSensorCaptures(signals.sensorCaptures || []);

    candidates.push(
      ...this.generateFromGmail(signals.gmail || [], signals.calendar || [], clusterSignals),
      ...this.generateFromPageVisits(signals.browserHistory || [], entities, signals.calendar || [], clusterSignals, frequentDomains),
      ...this.generateFromCalendar(signals.calendar || [], docSummaries, clusterSignals),
      ...this.generateFromDocEvents(docSummaries, signals.calendar || [], clusterSignals),
      ...this.generateFromSensorCaptures(sensorSummaries, clusterSignals)
    );

    return candidates;
  }

  computeClusterSignals(signals) {
    const clusters = this.extractIntentClusters(signals);
    const scores = {};
    Object.entries(clusters).forEach(([key, val]) => {
      scores[key] = val.count || 0;
    });
    return scores;
  }

  // ─── Generators ─────────────────────────────────────────────────
  generateFromGmail(messageEvents, calendarEvents, clusterSignals) {
    const candidates = [];
    if (!this.userPreferences?.privacy?.can_use_emails) return candidates;

    messageEvents.forEach(msg => {
      const content = `${msg.subject || ''} ${msg.body_snippet || msg.snippet || ''}`.toLowerCase();
      const isAction = /can you|could you|please|action|required|deadline|due|follow up|reply/i.test(content);
      const isJobRelated = /interview|recruiter|offer|application/i.test(content);
      const isUnreplied = msg.is_replied === false || msg.is_unreplied === true;
      if (!isAction || !isUnreplied) return;

      const urgency = this.computeGmailUrgency(msg);
      const cluster = this.inferClusterFromEmail({
        subject: msg.subject || '',
        body_snippet: msg.body_snippet || msg.snippet || ''
      }) || IntentClusters.RELATIONSHIP_NETWORKING;

      const timeWindow = this.suggestTimeWindow(urgency);
      const subject = msg.subject || 'email';
      const from = msg.from || 'sender';
      const link = msg.id ? `https://mail.google.com/mail/u/0/#inbox/${msg.id}` : null;
      const nameHint = from.split('<')[0]?.trim() || from;

      const template = isJobRelated
        ? `Thanks for reaching out. I'm available and looking forward to next steps. Happy to share any materials you need.`
        : `Thanks for the note. Quick update: I can handle this today and will follow up shortly.`;

      candidates.push({
        id: `ca_gmail_${msg.id || Math.random().toString(36).slice(2, 8)}`,
        type: 'reply_email',
        source_type: 'message',
        source_id: msg.id || '',
        cluster,
        urgency,
        intent_strength: 0.6,
        goal_alignment: this.calculateImportance({ cluster }),
        completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
        suggested_action: {
          title: `Reply to ${nameHint}`,
          description: `You opened this email and haven’t replied.`,
          template,
          link,
          email_to: from,
          email_subject: subject,
          email_thread_id: msg.threadId || null,
          email_message_id: msg.id || null,
          time_window: timeWindow,
          duration_minutes: 10
        },
        context: {
          target_person: { email: from, name_hint: nameHint }
        }
      });
    });

    return candidates;
  }

  generateFromPageVisits(pageVisits, entities, calendarEvents, clusterSignals, frequentDomains) {
    const candidates = [];
    if (!this.userPreferences?.privacy?.can_use_browser_history) return candidates;

    // Frequent domain action
    frequentDomains.forEach(d => {
      if (d.count_last_7_days < 3) return;
      if (!this.isHighLeverageDomain(d.domain)) return;
      const urgency = 6;
      const timeWindow = this.suggestTimeWindow(urgency);
      const cluster = this.inferClusterFromPage({ domain: d.domain, page_title: '' }) || IntentClusters.SIDE_PROJECT;
      candidates.push({
        id: `ca_domain_${d.domain}`,
        type: 'apply_or_reach_out',
        source_type: 'page_visit',
        source_id: d.domain,
        cluster,
        urgency,
        intent_strength: Math.min(1, d.count_last_7_days / 5),
        goal_alignment: this.calculateImportance({ cluster }),
        completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
        suggested_action: {
          title: `Take action on ${d.domain} (visited ${d.count_last_7_days}x)`,
          description: `You visited this page ${d.count_last_7_days} times this week without acting.`,
          template: `Open ${d.domain} and take the next concrete step (apply, message, or purchase).`,
          link: `https://${d.domain}`,
          time_window: timeWindow,
          duration_minutes: 15
        }
      });
    });

    // Product or person entities
    entities.forEach(e => {
      if (e.visit_count < 3) return;
      const urgency = 7;
      const timeWindow = this.suggestTimeWindow(urgency);

      if (e.entity_type === 'product') {
        candidates.push({
          id: `ca_product_${e.entity_id}`,
          type: 'buy_product',
          source_type: 'page_visit',
          source_id: e.entity_id,
          cluster: IntentClusters.RELATIONSHIP_NETWORKING,
          urgency,
          intent_strength: Math.min(1, e.visit_count / 5),
          goal_alignment: this.calculateImportance({ cluster: IntentClusters.RELATIONSHIP_NETWORKING }),
          completion_likelihood: this.learnedState?.cluster_weights?.[IntentClusters.RELATIONSHIP_NETWORKING] || 0.5,
          suggested_action: {
            title: `Decide on “${(e.raw_name || 'this product').slice(0, 60)}”`,
            description: `You viewed this product ${e.visit_count} times.`,
            template: `Purchase it now to avoid forgetting.`,
            link: e.url || null,
            time_window: timeWindow,
            duration_minutes: 5
          },
          context: {
            target_product: { name: e.raw_name || e.entity_id, url: e.url }
          }
        });
      }

      if (e.entity_type === 'person' || e.entity_type === 'company') {
        candidates.push({
          id: `ca_network_${e.entity_id}`,
          type: 'send_message',
          source_type: 'page_visit',
          source_id: e.entity_id,
          cluster: IntentClusters.RELATIONSHIP_NETWORKING,
          urgency: 6,
          intent_strength: Math.min(1, e.visit_count / 5),
          goal_alignment: this.calculateImportance({ cluster: IntentClusters.RELATIONSHIP_NETWORKING }),
          completion_likelihood: this.learnedState?.cluster_weights?.[IntentClusters.RELATIONSHIP_NETWORKING] || 0.5,
          suggested_action: {
            title: `Send a message to ${e.raw_name || e.entity_id}`,
            description: `You revisited this profile ${e.visit_count} times.`,
            template: `Hi ${e.raw_name || ''}, enjoyed reviewing your work — would love to connect.`,
            link: e.url || null,
            time_window: timeWindow,
            duration_minutes: 10
          }
        });
      }
    });

    return candidates;
  }

  generateFromCalendar(calendarEvents, docSummaries, clusterSignals) {
    const candidates = [];
    const now = Date.now();
    (calendarEvents || []).forEach(ev => {
      const start = ev.start_time || ev.start || ev.startTime;
      const startMs = start ? new Date(start).getTime() : 0;
      if (!startMs || startMs < now || startMs > now + 7 * 24 * 60 * 60 * 1000) return;
      const urgency = this.computeEventUrgency({ start_time: startMs });
      const timeWindow = this.suggestTimeWindow(urgency);
      const title = ev.event_title || ev.summary || 'upcoming event';
      const hoursUntil = Math.max(1, Math.round((startMs - now) / (60 * 60 * 1000)));
      const cluster = this.inferClusterFromCalendar({ event_title: title }) || IntentClusters.SIDE_PROJECT;
      candidates.push({
        id: `ca_cal_${ev.id || title}`,
        type: 'prepare',
        source_type: 'calendar_event',
        source_id: ev.id || '',
        cluster,
        urgency,
        intent_strength: 0.7,
        goal_alignment: this.calculateImportance({ cluster }),
        completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
        suggested_action: {
          title: `Prepare for ${title}`,
          description: `Event starts in ~${hoursUntil}h and no prep is logged.`,
          template: `Review notes and prepare key points before the meeting.`,
          link: null,
          time_window: timeWindow,
          duration_minutes: 30
        },
        context: {
          related_calendar_event: { event_id: ev.id || '', event_type: title, start_time: start }
        }
      });
    });
    return candidates;
  }

  generateFromDocEvents(docSummaries, calendarEvents, clusterSignals) {
    const candidates = [];
    const now = Date.now();
    (docSummaries || []).forEach(doc => {
      if (!doc.doc_name) return;
      const lastMod = doc.last_modified ? new Date(doc.last_modified).getTime() : 0;
      if (doc.state !== 'draft' && doc.state !== 'in_review') return;
      if (lastMod && lastMod < now - 7 * 24 * 60 * 60 * 1000) return;
      const urgency = 5;
      const timeWindow = this.suggestTimeWindow(urgency);
      const cluster = this.inferClusterFromDoc({ doc_name: doc.doc_name }) || IntentClusters.SIDE_PROJECT;
      candidates.push({
        id: `ca_doc_${doc.doc_id || doc.doc_name}`,
        type: 'finalize_doc',
        source_type: 'doc',
        source_id: doc.doc_id || '',
        cluster,
        urgency,
        intent_strength: 0.6,
        goal_alignment: this.calculateImportance({ cluster }),
        completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
        suggested_action: {
          title: `Finalize ${doc.doc_name}`,
          description: `You edited this ${doc.state.replace('_', ' ')} doc recently and haven’t shared it.`,
          template: `Review outstanding sections and share the final version.`,
          link: doc.webViewLink || null,
          time_window: timeWindow,
          duration_minutes: 30
        },
        context: {
          target_doc: { doc_id: doc.doc_id || '', doc_name: doc.doc_name, share_with: doc.shared_with || [] }
        }
      });
    });
    return candidates;
  }

  generateFromSensorCaptures(sensorCaptures, clusterSignals) {
    const candidates = [];
    (sensorCaptures || []).slice(0, 12).forEach(capture => {
      const text = capture.text || '';
      const lower = text.toLowerCase();
      const cluster = this.inferClusterFromSensor(capture);
      if (!cluster) return;

      if (/todo|to do|action items?|next steps?|follow up|deadline|due|submit|reply/.test(lower)) {
        const urgency = /deadline|due|submit/.test(lower) ? 8 : 6;
        candidates.push({
          id: `ca_sensor_followup_${capture.id}`,
          type: 'prepare',
          source_type: 'screen_capture',
          source_id: capture.id,
          cluster,
          urgency,
          intent_strength: 0.7,
          goal_alignment: this.calculateImportance({ cluster }),
          completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
          suggested_action: {
            title: `Follow up on ${capture.active_window_title || capture.active_app || 'captured work'}`,
            description: `Recent desktop capture contained action-oriented text.`,
            template: `Open the captured context and complete the next visible action item.`,
            link: null,
            time_window: this.suggestTimeWindow(urgency),
            duration_minutes: 20
          },
          signalData: {
            activeApp: capture.active_app,
            activeWindowTitle: capture.active_window_title,
            excerpt: text.slice(0, 180)
          }
        });
      } else if (/draft|outline|proposal|document|slide|sheet|notes/.test(lower)) {
        const urgency = 5;
        candidates.push({
          id: `ca_sensor_doc_${capture.id}`,
          type: 'finalize_doc',
          source_type: 'screen_capture',
          source_id: capture.id,
          cluster,
          urgency,
          intent_strength: 0.55,
          goal_alignment: this.calculateImportance({ cluster }),
          completion_likelihood: this.learnedState?.cluster_weights?.[cluster] || 0.5,
          suggested_action: {
            title: `Return to ${capture.active_window_title || capture.active_app || 'your draft'}`,
            description: `You were recently working in this window and OCR captured draft-like content.`,
            template: `Re-open the draft and finish one concrete section before switching context.`,
            link: null,
            time_window: this.suggestTimeWindow(urgency),
            duration_minutes: 25
          },
          signalData: {
            activeApp: capture.active_app,
            activeWindowTitle: capture.active_window_title,
            excerpt: text.slice(0, 180)
          }
        });
      }
    });
    return candidates;
  }

  isHighLeverageDomain(domain) {
    const highLeverageDomains = [
      'linkedin.com', 'indeed.com', 'glassdoor.com',
      'amazon.com', 'github.com', 'notion.so'
    ];
    return highLeverageDomains.some(d => domain.includes(d));
  }

  computeGmailUrgency(email) {
    const ts = email.timestamp || email.internalDate || 0;
    const daysSinceReceived = ts ? (Date.now() - ts) / (24 * 60 * 60 * 1000) : 2;
    if (daysSinceReceived < 1) return 8;
    if (daysSinceReceived < 3) return 6;
    return 4;
  }

  computeEventUrgency(event) {
    const hoursUntilEvent = (event.start_time - Date.now()) / (60 * 60 * 1000);
    if (hoursUntilEvent < 24) return 9;
    if (hoursUntilEvent < 72) return 7;
    return 5;
  }

  // Step 3: Score and prioritize
  scoreAndPrioritize(candidates) {
    return candidates
      .map(candidate => ({
        ...candidate,
        score: this.calculateScore(candidate)
      }))
      .sort((a, b) => b.score - a.score);
  }

  calculateScore(candidate) {
    const intentStrength = candidate.intent_strength ?? 0.5;
    const urgencyScore = (candidate.urgency || 0) / 10;
    const goalAlignment = candidate.goal_alignment ?? this.calculateImportance(candidate);
    const completionLikelihood = candidate.completion_likelihood ?? 0.5;
    return (
      0.4 * intentStrength +
      0.3 * urgencyScore +
      0.2 * goalAlignment +
      0.1 * completionLikelihood
    );
  }

  calculateImportance(candidate) {
    // Check alignment with user goals
    const alignedGoal = this.userGoals.find(goal =>
      candidate.cluster && goal.cluster === candidate.cluster
    );

    if (alignedGoal) {
      return alignedGoal.priority === 'high' ? 1 : alignedGoal.priority === 'medium' ? 0.7 : 0.4;
    }

    // Default importance based on cluster
    const clusterImportance = {
      [IntentClusters.JOB_SEARCH]: 0.8,
      [IntentClusters.THESIS_STUDY]: 0.7,
      [IntentClusters.RELATIONSHIP_NETWORKING]: 0.6,
      [IntentClusters.PERSONAL_CARE]: 0.5,
      [IntentClusters.SIDE_PROJECT]: 0.6,
      [IntentClusters.PROCRASTINATION_DISTRACTION]: 0.2
    };
    const learnedWeight = this.learnedState?.cluster_weights?.[candidate.cluster] || 0.5;
    return (clusterImportance[candidate.cluster] || 0.5) * 0.5 + learnedWeight * 0.5;
  }

  // Step 4: Generate specific suggestions
  generateSuggestions(prioritizedCandidates) {
    const maxTasks = this.getMaxTasks();
    const seen = new Set();
    const results = [];
    for (const candidate of prioritizedCandidates) {
      if (!this.isValidCandidate(candidate)) continue;
      const suggestion = this.createSpecificSuggestion(candidate);
      if (!this.isValidSuggestion(suggestion)) continue;
      const key = this.normalizeTitle(suggestion.title);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      results.push(suggestion);
      if (results.length >= maxTasks) break;
    }
    return results;
  }

  getMaxTasks() {
    const intensityMap = {
      light: 3,
      medium: 5,
      heavy: 7
    };
    return intensityMap[this.userPreferences.intensity] || 7;
  }

  createSpecificSuggestion(candidate) {
    const sa = candidate.suggested_action || {};
    const timeWindow = sa.time_window || this.suggestTimeWindow(candidate.urgency || 5);
    const reason = sa.description || this.generateReason(candidate);
    const duration = sa.duration_minutes ? `${sa.duration_minutes} min` : this.estimateDuration(candidate);
    const automatableTypes = new Set(['reply_email', 'apply_or_reach_out', 'send_message', 'buy_product']);
    const isAutomatable = automatableTypes.has(candidate.type);
    const draftText = sa.template || this.generateAIDraft(candidate) || '';
    const automationPlan = isAutomatable
      ? `Open target, type the draft, and submit.`
      : null;
    const shortTitle = sa.title || `${this.generateAction(candidate)} ${this.generateTarget(candidate)}`;
    const actionType = candidate.type === 'reply_email' ? 'send_email' : (isAutomatable ? 'automate' : null);

    return {
      title: shortTitle,
      priority: this.urgencyToPriority(candidate.urgency),
      category: this.getCategory(candidate),
      goal: candidate.cluster,
      duration,
      time_window: timeWindow,
      reason,
      description: sa.description || reason || '',
      assignee: isAutomatable ? 'ai' : 'human',
      xp: this.calculateXP(candidate),
      ai_draft: draftText || null,
      automation_plan: automationPlan,
      url: sa.link || null,
      action_type: actionType,
      email_to: sa.email_to || null,
      email_subject: sa.email_subject || null,
      email_thread_id: sa.email_thread_id || null,
      email_message_id: sa.email_message_id || null,
      signal_data: candidate.signalData || null
    };
  }

  suggestTimeWindow(urgency) {
    const preferredTime = this.userPreferences.preferred_time || 'morning';
    const baseHour = preferredTime === 'morning' ? 9 : preferredTime === 'evening' ? 18 : 14;
    const urgencyScore = (urgency || 0) / 10;
    const startHour = urgencyScore >= 0.8 ? Math.max(baseHour - 1, 8) : baseHour;
    const endHour = startHour + 1;
    const format = (h) => {
      const hour = ((h + 11) % 12) + 1;
      const ampm = h >= 12 ? 'PM' : 'AM';
      return `${hour}:00 ${ampm}`;
    };
    return `today ${format(startHour)} - ${format(endHour)}`;
  }

  generateAction(candidate) {
    const actionMap = {
      reply_email: 'Reply',
      apply_or_reach_out: 'Apply/Reach out',
      prepare: 'Prepare',
      finalize_doc: 'Finalize',
      buy_product: 'Buy',
      send_message: 'Send message'
    };
    return actionMap[candidate.type] || 'Complete';
  }

  generateTarget(candidate) {
    if (candidate.type === 'reply_email') {
      return `email from ${candidate.signalData.from}`;
    }
    if (candidate.type === 'apply_or_reach_out') {
      return candidate.target;
    }
    return candidate.target;
  }

  generateReason(candidate) {
    if (candidate.type === 'reply_email') {
      return `this email looks like an action item and hasn’t been replied to`;
    }
    if (candidate.type === 'apply_or_reach_out') {
      return `you visited this site multiple times without taking action`;
    }
    return 'this needs your attention';
  }

  getCategory(candidate) {
    const u = candidate.urgency || 0;
    if (u >= 8) return 'Must-Do';
    if (u >= 6) return 'Quick';
    return 'Follow-up';
  }

  estimateDuration(candidate) {
    const durationMap = {
      reply_email: '10 min',
      apply_or_reach_out: '15 min',
      prepare: '30 min',
      finalize_doc: '20 min',
      buy_product: '5 min',
      send_message: '10 min'
    };
    return durationMap[candidate.type] || '15 min';
  }

  calculateXP(candidate) {
    const baseXP = 30;
    const u = candidate.urgency || 0;
    const urgencyBonus = u >= 8 ? 20 : u >= 6 ? 10 : 0;
    return baseXP + urgencyBonus;
  }

  generateAIDraft(candidate) {
    if (candidate.type === 'reply_email') {
      const email = candidate.signalData;
      return `Hi ${email.from},\n\nRe: ${email.subject}\n\n[AI will draft response based on context]`;
    }
    if (candidate.type === 'apply_or_reach_out') {
      return `[AI will draft outreach message for ${candidate.target}]`;
    }
    return null;
  }

  urgencyToPriority(urgency) {
    if (urgency >= 8) return 'high';
    if (urgency >= 6) return 'medium';
    return 'low';
  }

  isValidCandidate(candidate) {
    const hasAction = !!(candidate?.suggested_action?.title || candidate?.type);
    const hasEvidence = !!(candidate?.suggested_action?.description);
    const hasTime = !!(candidate?.suggested_action?.time_window);
    const intentStrength = candidate?.intent_strength ?? 0;
    const weakSignal = intentStrength < 0.4;
    return hasAction && hasEvidence && hasTime && !weakSignal;
  }

  isValidSuggestion(suggestion) {
    if (!suggestion?.title || suggestion.title.length < 4) return false;
    if (!suggestion?.description || suggestion.description.length < 6) return false;
    if (!suggestion?.time_window) return false;
    return true;
  }

  normalizeTitle(title) {
    return (title || '').toLowerCase().replace(/[^a-z0-9]/g, '').trim();
  }

  // Main execution method
  async generateProactiveSuggestions() {
    try {
      // Collect signals from last 7 days
      const signals = await this.collectSignals();

      // Step 1: Extract intent clusters
      const clusters = this.extractIntentClusters(signals);

      // Step 2: Generate candidate actions
      const candidates = this.generateCandidateActions(signals);

      // Step 3: Score and prioritize
      const prioritized = this.scoreAndPrioritize(candidates);

      // Step 4: Generate specific suggestions
      const suggestions = this.generateSuggestions(prioritized);

      return {
        clusters,
        suggestions,
        patternState: this.patternState,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('Error generating proactive suggestions:', error);
      return { suggestions: [], error: error.message };
    }
  }

  async collectSignals() {
    const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);

    // Get browser history (filtered to last 7 days)
    const browserHistory = await refreshBrowserHistory({ reason: 'proactive_collect_signals', force: true });
    const recentBrowserHistory = browserHistory.filter(visit =>
      (visit.timestamp || visit.last_visit_time || 0) > sevenDaysAgo
    ).map(visit => ({
      domain: this.extractDomain(visit.url),
      path: this.extractPath(visit.url),
      page_title: visit.title,
      timestamp: visit.timestamp || visit.last_visit_time,
      duration: visit.visit_duration || 0,
      device_type: 'desktop'
    }));

    // Get Gmail data
    const gmailData = await getGoogleData();
    const recentGmail = (gmailData.gmail || []).filter(email => {
      const ts = email.timestamp || email.internalDate || 0;
      return ts > sevenDaysAgo;
    }).map(email => ({
      id: email.id,
      threadId: email.threadId,
      subject: email.subject,
      from: email.from,
      to: email.to,
      body_snippet: email.snippet,
      snippet: email.snippet,
      timestamp: email.timestamp,
      labelIds: email.labelIds || [],
      is_replied: false,
      is_unreplied: true
    }));

    // Get Calendar data
    const calendarData = gmailData.calendar || [];
    const recentCalendar = calendarData.filter(event => {
      const start = event.start_time || event.start;
      const startMs = start ? new Date(start).getTime() : 0;
      return startMs > sevenDaysAgo;
    });

    // Get Drive data
    const driveData = gmailData.drive || [];
    const recentDrive = driveData.filter(doc => {
      const lm = doc.last_modified || doc.modified || doc.modifiedTime || 0;
      return lm ? new Date(lm).getTime() > sevenDaysAgo : false;
    });

    const recentSensorCaptures = getSensorEvents()
      .filter(event => (event.timestamp || 0) > sevenDaysAgo)
      .map(event => ({
        id: event.id,
        activeApp: event.activeApp || '',
        activeWindowTitle: event.activeWindowTitle || '',
        text: (event.text || '').replace(/\s+/g, ' ').trim(),
        ocrStatus: event.ocrStatus || 'unknown',
        timestamp: event.timestamp || 0,
        captureMode: event.captureMode || 'screen'
      }))
      .filter(event => event.text.length >= 20);

    return {
      browserHistory: recentBrowserHistory.map(v => ({ ...v, url: v.url || '' })),
      gmail: recentGmail,
      calendar: recentCalendar,
      drive: recentDrive,
      sensorCaptures: recentSensorCaptures
    };
  }

  extractDomain(url) {
    try {
      return new URL(url).hostname;
    } catch (e) {
      return '';
    }
  }

  extractPath(url) {
    try {
      return new URL(url).pathname;
    } catch (e) {
      return '';
    }
  }
}

// Start OAuth server
oauthApp.get('/oauth2callback', async (req, res) => {
  const { code } = req.query;

  try {
    // Exchange code for tokens
    const response = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        code,
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: getRedirectUri(),
        grant_type: 'authorization_code',
      }),
    });

    const tokens = await response.json();

    // Initialize Google API Auth temporarily to fetch user email
    const auth = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      getRedirectUri()
    );
    auth.setCredentials(tokens);

    // Fetch account email
    const oauth2 = google.oauth2({ version: 'v2', auth });
    let email = 'Unknown Account';
    try {
      const userInfo = await oauth2.userinfo.get();
      email = userInfo.data.email || email;
    } catch(e) { console.warn('Could not fetch user info', e); }

    // Store in array to support MULTIPLE accounts
    const currentAccounts = store.get('googleAccounts') || [];
    // Remove if already exists so we can update tokens
    const filteredAccounts = currentAccounts.filter(acc => acc.email !== email);
    filteredAccounts.push({ email, tokens });
    store.set('googleAccounts', filteredAccounts);

    // Legacy support (to avoid breaking current UI state checks)
    store.set('googleTokens', tokens);

    // Send success response and close auth window
    res.send('<html><body><script>window.close();</script>Authentication successful! You can close this window.</body></html>');

    if (authWindow) {
      authWindow.close();
      authWindow = null;
    }

    // Notify main window
    mainWindow.webContents.send('auth-success');
    // Trigger a full GSuite sync immediately after successful OAuth
    (async () => {
      try {
        console.log('[oauth2callback] Triggering full Google sync after auth');
        await fullGoogleSync({ since: null, forceHistoricalBackfill: true, mode: 'initial_backfill', includeContacts: true });
        if (mainWindow && mainWindow.webContents) mainWindow.webContents.send('gsuite-sync-complete', store.get('googleData'));

        if (mainWindow && mainWindow.webContents) {
          mainWindow.webContents.send('memory-graph-update', {
            type: 'google_sync_completed',
            timestamp: Date.now()
          });
        }
      } catch (e) {
        console.warn('[oauth2callback] Full Google sync failed:', e && e.message ? e.message : e);
      }
    })();
  } catch (error) {
    console.error('OAuth error:', error);
    res.status(500).send('Authentication failed');
  }
});

// Enhanced data collection functions for proactive to-do list
async function fetchGmailData() {
  try {
    const { gmail } = store.get('googleAPIs') || {};
    if (!gmail) return [];

    // Get recent emails with focus on action items
    const response = await gmail.users.messages.list({
      userId: 'me',
      maxResults: 50,
      q: 'is:inbox OR is:important OR (subject:deadline OR subject:urgent OR subject:action OR subject:follow up)'
    });

    const messages = [];
    for (const messageRef of response.data.messages || []) {
      const message = await gmail.users.messages.get({
        userId: 'me',
        id: messageRef.id,
        format: 'full'
      });

      const headers = message.data.payload.headers;
      const emailData = {
        id: message.data.id,
        subject: headers.find(h => h.name === 'Subject')?.value || '',
        from: headers.find(h => h.name === 'From')?.value || '',
        date: headers.find(h => h.name === 'Date')?.value || '',
        to: headers.find(h => h.name === 'To')?.value || '',
        snippet: message.data.snippet,
        threadId: message.data.threadId,
        isUnread: message.data.labelIds?.includes('UNREAD') || false,
        isImportant: message.data.labelIds?.includes('IMPORTANT') || false,
        hasAttachments: message.data.payload.parts?.some(part => part.filename),
        // Extract action items from content
        actionItems: extractActionItems(message.data),
        // Determine if reply is needed
        needsReply: needsReply(message.data),
        // Priority based on content
        priority: extractEmailPriority(emailData)
      };

      messages.push(emailData);
    }

    return messages;
  } catch (error) {
    console.error('Gmail fetch error:', error);
    return [];
  }
}

// Extract action items from email content
function extractActionItems(message) {
  const actions = [];
  const content = getMessageContent(message);

  // Look for action-oriented phrases
  const actionPhrases = [
    /please\s+(.+?)(?:\.|$)/gi,
    /could\s+you\s+(.+?)(?:\.|$)/gi,
    /need\s+to\s+(.+?)(?:\.|$)/gi,
    /deadline:\s*(.+?)(?:\.|$)/gi,
    /due:\s*(.+?)(?:\.|$)/gi,
    /action\s+item:\s*(.+?)(?:\.|$)/gi
  ];

  actionPhrases.forEach(phrase => {
    const matches = content.match(phrase);
    if (matches) {
      actions.push(...matches.map(m => m.trim()));
    }
  });

  return actions;
}

// Get email content
function getMessageContent(message) {
  let content = '';
  if (message.data.payload.parts) {
    message.data.payload.parts.forEach(part => {
      if (part.mimeType === 'text/plain' && part.body.data) {
        content += Buffer.from(part.body.data, 'base64').toString();
      }
    });
  } else if (message.data.payload.body.data) {
    content = Buffer.from(message.data.payload.body.data, 'base64').toString();
  }
  return content;
}

// Check if email needs reply
function needsReply(message) {
  const content = getMessageContent(message);
  const from = message.payload.headers.find(h => h.name === 'From')?.value || '';

  // Check if it's a question
  if (content.includes('?')) return true;

  // Check if it's from a person (not automated)
  if (!from.includes('noreply') && !from.includes('no-reply')) return true;

  // Check for request phrases
  const requestPhrases = ['please', 'could you', 'would you', 'let me know', 'reply'];
  return requestPhrases.some(phrase => content.toLowerCase().includes(phrase));
}

// Enhanced calendar data for proactive scheduling
async function fetchCalendarData() {
  try {
    const { calendar } = store.get('googleAPIs') || {};
    if (!calendar) return [];

    // Get events for next 30 days
    const now = new Date();
    const timeMax = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);

    const response = await calendar.events.list({
      calendarId: 'primary',
      timeMin: now.toISOString(),
      timeMax: timeMax.toISOString(),
      singleEvents: true,
      orderBy: 'startTime'
    });

    const events = response.data.items || [];
    return events.map(event => ({
      id: event.id,
      summary: event.summary,
      description: event.description,
      start: event.start,
      end: event.end,
      location: event.location,
      priority: extractEventPriority(event),
      // Add proactive fields
      hasPreparation: needsPreparation(event),
      suggestedPrepTime: getSuggestedPrepTime(event),
      attendees: event.attendees || []
    }));
  } catch (error) {
    console.error('Calendar fetch error:', error);
    return [];
  }
}

// Check if event needs preparation
function needsPreparation(event) {
  const summary = (event.summary || '').toLowerCase();
  const description = (event.description || '').toLowerCase();

  const prepKeywords = ['interview', 'meeting', 'presentation', 'call', 'discussion', 'review'];
  return prepKeywords.some(keyword =>
    summary.includes(keyword) || description.includes(keyword)
  );
}

// Get suggested preparation time
function getSuggestedPrepTime(event) {
  const summary = (event.summary || '').toLowerCase();

  if (summary.includes('interview')) return 60; // 1 hour
  if (summary.includes('presentation')) return 120; // 2 hours
  if (summary.includes('meeting')) return 30; // 30 minutes
  if (summary.includes('call')) return 15; // 15 minutes

  return 30; // default 30 minutes
}

// Enhanced Drive data for project tracking
async function fetchDriveData() {
  try {
    const { drive } = store.get('googleAPIs') || {};
    if (!drive) return [];

    // Get recent files
    const response = await drive.files.list({
      pageSize: 25,
      fields: 'nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, owners, webViewLink)',
      orderBy: 'modifiedTime desc'
    });

    const files = response.data.files || [];
    return files.map(file => ({
      id: file.id,
      name: file.name,
      mimeType: file.mimeType,
      createdTime: file.createdTime,
      modifiedTime: file.modifiedTime,
      size: file.size,
      owners: file.owners,
      webViewLink: file.webViewLink,
      priority: extractFilePriority(file),
      // Add proactive fields
      isDraft: file.name.toLowerCase().includes('draft'),
      needsReview: needsReview(file),
      isShared: file.owners?.length > 1,
      estimatedTimeToComplete: estimateTimeToComplete(file)
    }));
  } catch (error) {
    console.error('Drive fetch error:', error);
    return [];
  }
}

// Check if file needs review
function needsReview(file) {
  const name = (file.name || '').toLowerCase();
  const reviewKeywords = ['draft', 'review', 'edit', 'update', 'final'];
  return reviewKeywords.some(keyword => name.includes(keyword));
}

// Estimate time to complete file
function estimateTimeToComplete(file) {
  const name = (file.name || '').toLowerCase();
  const mimeType = (file.mimeType || '').toLowerCase();

  if (mimeType.includes('document') || name.includes('doc')) return 30;
  if (mimeType.includes('presentation') || name.includes('slide')) return 60;
  if (mimeType.includes('spreadsheet') || name.includes('sheet')) return 45;
  if (name.includes('report')) return 90;
  if (name.includes('proposal')) return 120;

  return 30; // default 30 minutes
}

// Priority extraction functions
function extractEmailPriority(email) {
  const subject = (email.subject || '').toLowerCase();
  const from = (email.from || '').toLowerCase();

  // High priority indicators
  if (subject.includes('urgent') || subject.includes('asap') ||
      subject.includes('deadline') || subject.includes('important') ||
      from.includes('manager') || from.includes('boss') || from.includes('ceo')) {
    return 'high';
  }

  // Medium priority indicators
  if (subject.includes('meeting') || subject.includes('review') ||
      subject.includes('follow up') || subject.includes('action required')) {
    return 'medium';
  }

  return 'low';
}

function extractEventPriority(event) {
  const summary = (event.summary || '').toLowerCase();
  const description = (event.description || '').toLowerCase();

  // High priority events
  if (summary.includes('deadline') || summary.includes('urgent meeting') ||
      summary.includes('client') || summary.includes('presentation') ||
      description.includes('critical') || description.includes('important')) {
    return 'high';
  }

  // Medium priority events
  if (summary.includes('meeting') || summary.includes('review') ||
      summary.includes('call') || summary.includes('discussion')) {
    return 'medium';
  }

  return 'low';
}

function extractFilePriority(file) {
  const name = (file.name || '').toLowerCase();
  const mimeType = (file.mimeType || '').toLowerCase();

  // High priority files
  if (name.includes('urgent') || name.includes('important') ||
      name.includes('deadline') || name.includes('contract') ||
      mimeType.includes('presentation') || mimeType.includes('spreadsheet')) {
    return 'high';
  }

  // Medium priority files
  if (name.includes('report') || name.includes('proposal') ||
      name.includes('project') || mimeType.includes('document')) {
    return 'medium';
  }

  return 'low';
}

// Handle Chrome extension data with enhanced processing
oauthApp.post('/extension-data', express.json(), async (req, res) => {
  try {
    const extensionData = req.body;

    // Process browsing data for proactive insights
    const processedData = processBrowsingData(extensionData);

    // Store extension data
    const currentData = store.get('extensionData') || {};
    currentData[Date.now()] = processedData;
    store.set('extensionData', currentData);

    // Merge with user data
    const userData = store.get('userData') || {};
    userData.extensionData = processedData;
    store.set('userData', userData);

    // Persist browser history signals into L1 SQLite with deterministic IDs.
    try {
      
      const urls = processedData?.urls || extensionData?.urls || [];
      for (const item of urls) {
        const ts = item.timestamp || item.last_visit_time || Date.now();
        const domain = (() => {
          try { return new URL(item.url || '').hostname; } catch (_) { return ''; }
        })();
        const stableId = Buffer.from(`${item.url || ''}|${ts}`).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 24);
        await ingestRawEvent({
          type: 'BrowserVisit',
          timestamp: ts,
          source: 'Browser',
          text: [
            item.title ? `Title: ${item.title}` : '',
            item.url ? `URL: ${item.url}` : '',
            domain ? `Domain: ${domain}` : ''
          ].filter(Boolean).join('\n'),
          metadata: {
            ...item,
            app: 'Browser',
            id: `hist_${stableId}`,
            domain,
            captured_at: new Date(ts).toISOString(),
            captured_at_local: new Date(ts).toLocaleString()
          }
        });
      }
    } catch (ingestErr) {
      console.warn('[extension-data] browser ingestion failed:', ingestErr?.message || ingestErr);
    }

    // Generate proactive tasks based on new data
    generateProactiveTasksFromData(userData);

    // Notify renderer
    if (mainWindow) {
      mainWindow.webContents.send('extension-data-updated', processedData);
    }

    res.json({ success: true, message: 'Data received successfully' });
  } catch (error) {
    console.error('Extension data error:', error);
    res.status(500).json({ success: false, message: 'Failed to process data' });
  }
});

// Process browsing data for proactive insights
function processBrowsingData(extensionData) {
  const insights = {
    frequentVisits: analyzeFrequentVisits(extensionData),
    abandonedTasks: identifyAbandonedTasks(extensionData),
    researchPatterns: analyzeResearchPatterns(extensionData),
    networkingOpportunities: identifyNetworkingOpportunities(extensionData),
    productivityPatterns: analyzeProductivityPatterns(extensionData)
  };

  return { ...extensionData, insights };
}

// Analyze frequent website visits
function analyzeFrequentVisits(data) {
  const domainCounts = {};
  const recentVisits = data.browsingData?.filter(item =>
    item.type === 'website_visit' &&
    Date.now() - item.timestamp < 7 * 24 * 60 * 60 * 1000 // Last 7 days
  ) || [];

  recentVisits.forEach(visit => {
    const domain = visit.domain;
    domainCounts[domain] = (domainCounts[domain] || 0) + 1;
  });

  return Object.entries(domainCounts)
    .sort(([,a], [,b]) => b - a)
    .slice(0, 10)
    .map(([domain, count]) => ({ domain, count }));
}

// Identify abandoned tasks (visited but not acted upon)
function identifyAbandonedTasks(data) {
  const abandoned = [];

  // Look for job applications started but not completed
  const jobSites = data.browsingData?.filter(item =>
    item.domain?.includes('linkedin') ||
    item.domain?.includes('indeed') ||
    item.domain?.includes('glassdoor')
  ) || [];

  // Group by company
  const companyVisits = {};
  jobSites.forEach(visit => {
    const company = extractCompanyFromUrl(visit.url);
    if (company) {
      if (!companyVisits[company]) companyVisits[company] = [];
      companyVisits[company].push(visit);
    }
  });

  // Identify companies visited multiple times but no application
  Object.entries(companyVisits).forEach(([company, visits]) => {
    if (visits.length >= 3 && visits.length <= 5) {
      abandoned.push({
        type: 'job_application',
        company,
        visitCount: visits.length,
        lastVisit: Math.max(...visits.map(v => v.timestamp)),
        suggestedAction: 'Complete job application'
      });
    }
  });

  return abandoned;
}

// Extract company name from URL
function extractCompanyFromUrl(url) {
  try {
    const urlObj = new URL(url);
    const path = urlObj.pathname.toLowerCase();

    // Look for company patterns in path
    if (path.includes('/company/')) {
      return path.split('/company/')[1]?.split('/')[0];
    }

    return null;
  } catch (e) {
    return null;
  }
}

// Analyze research patterns
function analyzeResearchPatterns(data) {
  const researchTopics = {};
  const researchKeywords = ['research', 'tutorial', 'guide', 'how to', 'learn', 'course'];

  const searchQueries = data.searchQueries || [];
  searchQueries.forEach(query => {
    if (researchKeywords.some(keyword =>
      query.query.toLowerCase().includes(keyword)
    )) {
      const topic = extractTopicFromQuery(query.query);
      if (topic) {
        researchTopics[topic] = (researchTopics[topic] || 0) + 1;
      }
    }
  });

  return Object.entries(researchTopics)
    .sort(([,a], [,b]) => b - a)
    .slice(0, 5)
    .map(([topic, count]) => ({ topic, count }));
}

// Extract topic from search query
function extractTopicFromQuery(query) {
  const words = query.toLowerCase().split(' ');
  const researchWords = words.filter(word =>
    !['how', 'to', 'the', 'a', 'an', 'for', 'and', 'or', 'but'].includes(word)
  );
  return researchWords.slice(0, 3).join(' ');
}

// Identify networking opportunities
function identifyNetworkingOpportunities(data) {
  const opportunities = [];

  // Look for LinkedIn profile visits
  const linkedinVisits = data.browsingData?.filter(item =>
    item.domain === 'linkedin.com' &&
    item.path?.includes('/in/')
  ) || [];

  linkedinVisits.forEach(visit => {
    const profileName = extractProfileName(visit.url);
    if (profileName) {
      opportunities.push({
        type: 'linkedin_connection',
        person: profileName,
        url: visit.url,
        visitCount: this.getVisitCount(visit.url),
        suggestedAction: 'Send connection request'
      });
    }
  });

  return opportunities;
}

// Extract profile name from LinkedIn URL
function extractProfileName(url) {
  try {
    const urlObj = new URL(url);
    const path = urlObj.pathname;
    if (path.includes('/in/')) {
      return path.split('/in/')[1]?.split('/')[0];
    }
    return null;
  } catch (e) {
    return null;
  }
}

// Analyze productivity patterns
function analyzeProductivityPatterns(data) {
  const patterns = {
    peakHours: analyzePeakHours(data),
    averageSessionTime: calculateAverageSessionTime(data),
    distractionSites: identifyDistractionSites(data)
  };

  return patterns;
}

// Analyze peak productivity hours
function analyzePeakHours(data) {
  const hourlyActivity = new Array(24).fill(0);

  data.browsingData?.forEach(item => {
    const hour = new Date(item.timestamp).getHours();
    hourlyActivity[hour]++;
  });

  return hourlyActivity
    .map((count, hour) => ({ hour, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 3);
}

// Calculate average session time
function calculateAverageSessionTime(data) {
  const sessions = [];
  let currentSession = null;

  data.browsingData?.forEach(item => {
    if (item.type === 'website_visit') {
      if (!currentSession) {
        currentSession = { start: item.timestamp, end: item.timestamp };
      } else if (item.timestamp - currentSession.end < 30 * 60 * 1000) { // Within 30 minutes
        currentSession.end = item.timestamp;
      } else {
        sessions.push(currentSession.end - currentSession.start);
        currentSession = { start: item.timestamp, end: item.timestamp };
      }
    }
  });

  if (currentSession) {
    sessions.push(currentSession.end - currentSession.start);
  }

  return sessions.length > 0
    ? Math.round(sessions.reduce((a, b) => a + b, 0) / sessions.length / 1000 / 60) // minutes
    : 0;
}

// Identify distraction sites
function identifyDistractionSites(data) {
  const distractionKeywords = ['youtube', 'twitter', 'instagram', 'facebook', 'reddit', 'tiktok'];

  const distractionVisits = data.browsingData?.filter(item =>
    distractionKeywords.some(keyword => item.domain?.includes(keyword))
  ) || [];

  const domainCounts = {};
  distractionVisits.forEach(visit => {
    const domain = visit.domain;
    domainCounts[domain] = (domainCounts[domain] || 0) + 1;
  });

  return Object.entries(domainCounts)
    .sort(([,a], [,b]) => b - a)
    .slice(0, 5)
    .map(([domain, count]) => ({ domain, count }));
}

// Generate proactive tasks from collected data
async function generateProactiveTasksFromData(userData) {
  try {
    const tasks = [];

    // Tasks from Gmail
    if (userData.googleData?.gmail) {
      userData.googleData.gmail.forEach(email => {
        if (email.needsReply && !email.isUnread) {
          tasks.push({
            title: `Reply to ${email.from.split('<')[0].trim()}`,
            description: email.subject,
            source: 'gmail',
            type: 'communication',
            priority: email.priority,
            estimatedTime: 15,
            actionUrl: `https://mail.google.com/#inbox/${email.id}`,
            reason: 'Email needs reply but not yet answered'
          });
        }

        if (email.actionItems.length > 0) {
          email.actionItems.forEach(action => {
            tasks.push({
              title: `Complete: ${action}`,
              description: `From email: ${email.subject}`,
              source: 'gmail',
              type: 'action_item',
              priority: email.priority,
              estimatedTime: 30,
              actionUrl: `https://mail.google.com/#inbox/${email.id}`,
              reason: 'Action item found in email'
            });
          });
        }
      });
    }

    // Tasks from Calendar
    if (userData.googleData?.calendar) {
      userData.googleData.calendar.forEach(event => {
        if (event.hasPreparation) {
          tasks.push({
            title: `Prepare for: ${event.summary}`,
            description: `Event requires ${event.suggestedPrepTime} minutes preparation`,
            source: 'calendar',
            type: 'preparation',
            priority: 'medium',
            estimatedTime: event.suggestedPrepTime,
            actionUrl: event.htmlLink,
            reason: 'Event requires preparation'
          });
        }
      });
    }

    // Tasks from Drive
    if (userData.googleData?.drive) {
      userData.googleData.drive.forEach(file => {
        if (file.needsReview) {
          tasks.push({
            title: `Review: ${file.name}`,
            description: 'File needs review or finalization',
            source: 'drive',
            type: 'review',
            priority: 'medium',
            estimatedTime: 15,
            actionUrl: file.webViewLink,
            reason: 'File requires review'
          });
        }
      });
    }

    return tasks;
  } catch (error) {
    console.error('Error generating proactive tasks:', error);
    return [];
  }
}

// Get browsing history directly from Chrome, Brave, Arc and Safari
let safariFullDiskAccessWarned = false;

async function getBrowserHistory() {
  const history = [];

  // --- Chromium-based browsers ---
  const chromiumHistory = await getChromiumHistory();
  history.push(...chromiumHistory);

  // --- Safari (macOS only) ---
  if (process.platform === 'darwin') {
    const safariHistory = await getSafariHistory();
    history.push(...safariHistory);
  }

  // Sort newest first, deduplicate by url+timestamp
  history.sort((a, b) => b.timestamp - a.timestamp);

  const seen = new Set();
  const deduped = history.filter(item => {
    const key = `${item.url}|${Math.floor(item.timestamp / 60000)}`; // 1-min bucket
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`Browser history total: ${deduped.length} URLs from ${history.length} raw entries`);
  return deduped.slice(0, 300);
}

// Read a single Chromium history DB file and return entries
async function readChromiumHistoryDB(dbPath, browserName) {

  if (!(await existsAsync(dbPath))) return [];

  // Copy first to avoid the browser's exclusive lock
  const tmpPath = path.join(os.tmpdir(), `chromium_hist_${Date.now()}_${Math.random().toString(36).slice(2)}.db`);
  try {
    await fs.promises.copyFile(dbPath, tmpPath);
  } catch (e) {
    console.warn(`Could not copy ${browserName} DB at ${dbPath}:`, e.message);
    return [];
  }

  try {

    return await new Promise((resolve) => {
      const db = new sqlite3.Database(tmpPath, sqlite3.OPEN_READONLY, (err) => {
        if (err) {
          console.warn(`Could not open ${browserName} DB copy:`, err.message);
          resolve([]);
        }
      });

      // Chromium epoch: microseconds since 1601-01-01
      const EPOCH_OFFSET_US = 11644473600 * 1_000_000;
      const sevenDaysAgo_us = (Date.now() * 1000) - (7 * 24 * 60 * 60 * 1_000_000) + EPOCH_OFFSET_US;

      db.all(
        `SELECT url, title, visit_count, last_visit_time
         FROM urls
         WHERE last_visit_time > ?
         ORDER BY last_visit_time DESC
         LIMIT 500`,
        [sevenDaysAgo_us],
        (err, rows) => {
          db.close(async () => { try { await fs.promises.unlink(tmpPath); } catch (_) {} });
          if (err) {
            console.warn(`${browserName} query error:`, err.message);
            resolve([]);
            return;
          }
          const entries = (rows || []).map(row => ({
            url: row.url,
            title: row.title && row.title.trim() ? row.title : extractDomain(row.url),
            domain: extractDomain(row.url),
            category: categorizeURL(row.url),
            timestamp: (row.last_visit_time - EPOCH_OFFSET_US) / 1000,
            visitCount: row.visit_count || 1,
            browser: browserName
          }));
          if (entries.length > 0) {
            try {
              console.log(`${browserName} (${dbPath}): ${entries.length} URLs`);
            } catch (logErr) {
              if (String(logErr?.code || '') !== 'EIO') throw logErr;
            }
          }
          resolve(entries);
        }
      );
    });
  } catch (sqlErr) {
    try { await fs.promises.unlink(tmpPath); } catch (_) {}
    console.warn(`sqlite3 unavailable for ${browserName}:`, sqlErr.message);
    return [];
  }
}

// Collect Chromium-family history: Chrome, Brave, Arc — all profiles
async function getChromiumHistory() {
  const home = os.homedir();
  const all  = [];

  // Map of browser name → base directory that contains profile folders
  const browserBases = [];

  if (process.platform === 'darwin') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, 'Library/Application Support/Google/Chrome') },
      { name: 'Brave',   base: path.join(home, 'Library/Application Support/BraveSoftware/Brave-Browser') },
      { name: 'Arc',     base: path.join(home, 'Library/Application Support/Arc/User Data') },
      { name: 'Edge',    base: path.join(home, 'Library/Application Support/Microsoft Edge') }
    );
  } else if (process.platform === 'linux') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, '.config/google-chrome') },
      { name: 'Brave',   base: path.join(home, '.config/BraveSoftware/Brave-Browser') }
    );
  } else if (process.platform === 'win32') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, 'AppData/Local/Google/Chrome/User Data') },
      { name: 'Brave',   base: path.join(home, 'AppData/Local/BraveSoftware/Brave-Browser/User Data') },
      { name: 'Edge',    base: path.join(home, 'AppData/Local/Microsoft/Edge/User Data') }
    );
  }

  for (const { name, base } of browserBases) {
    if (!(await existsAsync(base))) continue;

    // Try Default profile + numbered profiles (Profile 1, Profile 2 ...)
    let profileDirs;
    try {
      profileDirs = ['Default', ...(await fs.promises.readdir(base)).filter(d => /^Profile \d+$/.test(d))];
    } catch (_) {
      profileDirs = ['Default'];
    }

    for (const profile of profileDirs) {
      const dbPath = path.join(base, profile, 'History');
      const entries = await readChromiumHistoryDB(dbPath, name);
      all.push(...entries);
    }
  }

  return all;
}

// Get Safari history from SQLite database
async function getSafariHistory() {

  try {
    if (process.platform !== 'darwin') return [];

    const safariHistoryPath = path.join(os.homedir(), 'Library/Safari/History.db');

    if (!(await existsAsync(safariHistoryPath))) {
      console.log('Safari history file not found');
      return [];
    }

    // Safari also locks its DB while open — copy it first
    const tmpPath = path.join(os.tmpdir(), `safari_history_${Date.now()}.db`);
    try {
      await fs.promises.copyFile(safariHistoryPath, tmpPath);
    } catch (e) {
      if (e.code === 'EPERM') {
        if (!safariFullDiskAccessWarned) {
          console.log('Skipping Safari history — Full Disk Access is required for this terminal/app.');
          safariFullDiskAccessWarned = true;
        }
      } else {
        console.warn('Could not copy Safari history DB:', e.message);
      }
      return [];
    }

    try {

      return await new Promise((resolve) => {
        const db = new sqlite3.Database(tmpPath, sqlite3.OPEN_READONLY, (err) => {
          if (err) {
            console.error('Could not open Safari history DB copy:', err.message);
            resolve([]);
            return;
          }
        });

        // Safari timestamps: seconds since Apple epoch (2001-01-01)
        const APPLE_EPOCH_MS = new Date('2001-01-01T00:00:00Z').getTime();
        const sevenDaysAgo_apple = ((Date.now() - APPLE_EPOCH_MS) / 1000) - 7 * 24 * 60 * 60;

        const query = `
          SELECT
            history_items.url,
            history_items.title,
            history_items.visit_count,
            history_visits.visit_time
          FROM history_items
          JOIN history_visits ON history_items.id = history_visits.history_item
          WHERE history_visits.visit_time > ?
          ORDER BY history_visits.visit_time DESC
          LIMIT 200
        `;

        db.all(query, [sevenDaysAgo_apple], (err, rows) => {
          db.close(async () => {
            try { await fs.promises.unlink(tmpPath); } catch (_) {}
          });

          if (err) {
            console.error('Safari history query error:', err.message);
            resolve([]);
            return;
          }

          const history = rows.map(row => ({
            url: row.url,
            title: row.title || 'Untitled',
            domain: extractDomain(row.url),
            category: categorizeURL(row.url),
            // Convert Apple seconds → Unix ms
            timestamp: (row.visit_time * 1000) + APPLE_EPOCH_MS,
            visitCount: row.visit_count || 1,
            browser: 'Safari'
          }));

          console.log(`Safari history: ${history.length} URLs loaded`);
          resolve(history);
        });
      });
    } catch (sqliteError) {
      console.log('sqlite3 not available for Safari:', sqliteError.message);
      try { await fs.promises.unlink(tmpPath); } catch (_) {}
      return [];
    }
  } catch (error) {
    console.error('Error getting Safari history:', error);
    return [];
  }
}

// Extract domain from URL
function extractDomain(url) {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch (error) {
    return 'unknown';
  }
}

// Get productivity score for URL
function getProductivityScore(url) {
  const domain = new URL(url).hostname.toLowerCase();

  // High productivity
  if (domain.includes('github') || domain.includes('stackoverflow') ||
      domain.includes('notion') || domain.includes('figma')) {
    return 0.9;
  }

  // Medium productivity
  if (domain.includes('linkedin') || domain.includes('medium') ||
      domain.includes('slack') || domain.includes('discord')) {
    return 0.6;
  }

  // Low productivity
  if (domain.includes('twitter') || domain.includes('facebook') ||
      domain.includes('youtube') || domain.includes('reddit')) {
    return 0.3;
  }

  return 0.5; // Neutral
}

// Get extension data (now using real browser history)
async function getExtensionData() {
  try {
    console.log('Getting real browser history...');
    const browserHistory = await refreshBrowserHistory({ reason: 'get_extension_data' });

    // Convert browser history to extension data format
    const urls = browserHistory.map(item => ({
      url: item.url,
      title: item.title,
      domain: item.domain,
      category: item.category,
      timestamp: item.timestamp,
      visitCount: item.visitCount,
      productivityScore: item.productivityScore || 0.5,
      synced: true,
      browser: item.browser || 'Chrome'
    }));

    // Calculate stats
    const today = new Date().toDateString();
    const todayUrls = urls.filter(url =>
      new Date(url.timestamp).toDateString() === today
    );

    const stats = {
      totalUrls: urls.length,
      todayUrls: todayUrls.length,
      uniqueDomains: new Set(urls.map(url => url.domain)).size,
      syncedCount: urls.length,
      categories: getCategoryBreakdown(urls),
      productivity: getProductivityStats(urls),
      lastSync: Date.now()
    };

    console.log(`Retrieved ${urls.length} URLs from browser history`);

    // Store for AI processing
    global.extensionData = {
      urls: urls,
      stats: stats,
      lastReceived: Date.now(),
      source: 'direct-browser-access'
    };

    return {
      urls: urls,
      searchQueries: [], // Could be extracted from URLs with search patterns
      stats: stats
    };
  } catch (error) {
    console.error('Error getting extension data:', error);
    return {
      urls: [],
      searchQueries: [],
      stats: { totalUrls: 0, uniqueDomains: 0 }
    };
  }
}

function getCategoryBreakdown(data) {
  const counts = {};
  data.forEach(item => { counts[item.category] = (counts[item.category] || 0) + 1; });
  return counts;
}

function getProductivityStats(data) {
  const sum = data.reduce((a, b) => a + (b.productivityScore || 0.5), 0);
  return { average: sum / (data.length || 1) };
}

function mergeUniqueById(existing, incoming, key = 'id') {
  const map = new Map();
  [...(existing || []), ...(incoming || [])].forEach(item => {
    const itemKey = item?.[key] || JSON.stringify(item);
    if (!itemKey) return;
    map.set(itemKey, { ...(map.get(itemKey) || {}), ...item });
  });
  return Array.from(map.values()).sort((a, b) => {
    const aTs = a.timestamp || a.start_time || a.last_modified || a.modifiedTime || 0;
    const bTs = b.timestamp || b.start_time || b.last_modified || b.modifiedTime || 0;
    return bTs - aTs;
  });
}

function parseSyncCursorMs(value) {
  if (!value && value !== 0) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeSyncCursorMs(since) {
  const baselineMs = Date.parse(GOOGLE_SYNC_BASELINE_ISO);
  const now = Date.now();
  let sinceMs = parseSyncCursorMs(since);
  if (!sinceMs) return null;
  if (sinceMs > now + GOOGLE_SYNC_FUTURE_DRIFT_MS) {
    sinceMs = now - (24 * 60 * 60 * 1000);
  }
  return Math.max(baselineMs, sinceMs);
}

function toValidEventTimestamp(...candidates) {
  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined || candidate === '') continue;
    if (typeof candidate === 'number' && Number.isFinite(candidate) && candidate > 0) return candidate;
    const str = String(candidate || '').trim();
    if (!str || str === '0') continue;
    if (/^\d+$/.test(str)) {
      const n = Number(str);
      if (Number.isFinite(n) && n > 0) return n;
      continue;
    }
    const parsed = new Date(str).getTime();
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return Date.now();
}

function mergeGoogleData(existing = {}, incoming = {}) {
  return {
    gmail: mergeUniqueById(existing.gmail, incoming.gmail, 'id'),
    gmailSent: mergeUniqueById(existing.gmailSent, incoming.gmailSent, 'id'),
    calendar: mergeUniqueById(existing.calendar, incoming.calendar, 'id'),
    contacts: mergeUniqueById(existing.contacts, incoming.contacts, 'id'),
    drive: mergeUniqueById(existing.drive, incoming.drive, 'id'),
    lastSync: incoming.lastSync || existing.lastSync || null
  };
}

function normalizeGoogleContactRecord(person = {}) {
  const names = Array.isArray(person.names) ? person.names : [];
  const primaryName = names.find((item) => item?.displayName) || names[0] || {};
  const emails = (Array.isArray(person.emailAddresses) ? person.emailAddresses : [])
    .map((item) => String(item?.value || '').trim().toLowerCase())
    .filter(Boolean);
  const phones = (Array.isArray(person.phoneNumbers) ? person.phoneNumbers : [])
    .map((item) => String(item?.value || '').trim())
    .filter(Boolean);
  const organizations = Array.isArray(person.organizations) ? person.organizations : [];
  const primaryOrg = organizations.find((item) => item?.current) || organizations[0] || {};
  const urls = (Array.isArray(person.urls) ? person.urls : [])
    .map((item) => String(item?.value || '').trim())
    .filter(Boolean);
  const biographies = (Array.isArray(person.biographies) ? person.biographies : [])
    .map((item) => String(item?.value || '').trim())
    .filter(Boolean);
  const addresses = (Array.isArray(person.addresses) ? person.addresses : []).map((item) => {
    const parts = [
      item?.streetAddress,
      item?.city,
      item?.region,
      item?.postalCode,
      item?.country
    ].filter(Boolean);
    return parts.join(', ');
  }).filter(Boolean);
  const id = String(person.resourceName || person.etag || emails[0] || primaryName.displayName || '').trim();
  if (!id) return null;
  return {
    id,
    resourceName: String(person.resourceName || id),
    etag: String(person.etag || ''),
    name: String(primaryName.displayName || primaryName.unstructuredName || emails[0] || 'Unknown').trim(),
    first_name: String(primaryName.givenName || '').trim(),
    last_name: String(primaryName.familyName || '').trim(),
    company: String(primaryOrg.name || '').trim(),
    role: String(primaryOrg.title || '').trim(),
    emails,
    phones,
    addresses,
    urls,
    notes: biographies.join('\n\n').trim(),
    birthday: (() => {
      const birthday = Array.isArray(person.birthdays) ? person.birthdays[0] : null;
      const date = birthday?.date || {};
      if (!date?.year && !date?.month && !date?.day) return null;
      const year = date.year || 1900;
      const month = String(date.month || 1).padStart(2, '0');
      const day = String(date.day || 1).padStart(2, '0');
      return `${year}-${month}-${day}`;
    })(),
    metadata: {
      google_contact_id: String(person.resourceName || id),
      source: 'google_contacts',
      google_contacts: true
    }
  };
}

async function fetchGoogleContactsForAuth(auth) {
  const peopleApi = google.people({ version: 'v1', auth });
  const contacts = [];
  let pageToken = null;
  do {
    const res = await peopleApi.people.connections.list({
      resourceName: 'people/me',
      pageSize: 1000,
      pageToken: pageToken || undefined,
      personFields: 'names,emailAddresses,phoneNumbers,organizations,biographies,urls,birthdays,addresses'
    });
    const items = Array.isArray(res?.data?.connections) ? res.data.connections : [];
    for (const person of items) {
      const normalized = normalizeGoogleContactRecord(person);
      if (normalized) contacts.push(normalized);
    }
    pageToken = res?.data?.nextPageToken || null;
  } while (pageToken);
  return contacts;
}

async function persistGoogleSyncProjectNote() {
  const now = new Date().toISOString();
  await upsertMemoryNode({
    id: 'project_google_sync_memory',
    layer: 'semantic',
    subtype: 'project_note',
    title: 'Google sync architecture',
    summary: 'Google sync backfills Gmail and Calendar into raw, episodic, and semantic memory.',
    canonicalText: [
      'Google sync architecture',
      'Backfill Gmail and Calendar from 2010.',
      'Use raw ingestion first, then episode generation and semantic summarization.'
    ].join('\n'),
    confidence: 0.86,
    status: 'active',
    sourceRefs: ['google_sync_settings'],
    metadata: {
      project: 'google_sync',
      layer_flow: ['raw', 'episodic', 'semantic'],
      source_systems: ['gmail', 'calendar']
    },
    graphVersion: 'google_sync_project_v1',
    createdAt: now,
    updatedAt: now,
    anchorDate: now.slice(0, 10),
    anchorAt: now
  }).catch(() => {});
}

function getGoogleSyncStatusSnapshot() {
  const accounts = store.get('googleAccounts') || [];
  const health = store.get('googleSyncHealth') || {};
  const googleData = store.get('googleData') || {};
  return {
    connected: accounts.length > 0,
    accounts: accounts.map((item) => ({ email: item.email || 'Unknown Account' })),
    lastSync: googleData.lastSync || null,
    health,
    counts: {
      gmail: Array.isArray(googleData.gmail) ? googleData.gmail.length : 0,
      calendar: Array.isArray(googleData.calendar) ? googleData.calendar.length : 0,
      contacts: RELATIONSHIP_FEATURE_ENABLED && Array.isArray(googleData.contacts) ? googleData.contacts.length : 0
    }
  };
}

async function repairEmailEventTimestamps() {
  const repairedAt = store.get('emailTimestampRepairAt');
  if (repairedAt) return;
  try {
    const rows = await db.allQuery(
      `SELECT id, timestamp, occurred_at, date, metadata, source_type, type
       FROM events
       WHERE LOWER(COALESCE(source_type, type, '')) LIKE '%email%'
          OR LOWER(COALESCE(source_type, type, '')) LIKE '%message%'
       LIMIT 8000`
    ).catch(() => []);

    if (!rows || rows.length === 0) {
      store.set('emailTimestampRepairAt', new Date().toISOString());
      return;
    }

    let updated = 0;
    try {
      await db.runQuery('BEGIN TRANSACTION');
      for (const row of rows) {
        let meta = {};
        try { meta = JSON.parse(row.metadata || '{}'); } catch (_) { meta = {}; }
        const sourceTs = toValidEventTimestamp(meta.sent_at, meta.internalDate, meta.date, meta.received_at, row.occurred_at, row.timestamp);
        if (!Number.isFinite(sourceTs) || sourceTs <= 0) continue;
        const iso = new Date(sourceTs).toISOString();
        const day = iso.slice(0, 10);
        if (String(row.timestamp || '') === iso && String(row.occurred_at || '') === iso && String(row.date || '') === day) continue;
        await db.runQuery(
          `UPDATE events
           SET timestamp = ?, occurred_at = ?, date = ?
           WHERE id = ?`,
          [iso, iso, day, row.id]
        );
        updated += 1;
      }
      await db.runQuery('COMMIT');
    } catch (err) {
      try { await db.runQuery('ROLLBACK'); } catch (_) {}
      throw err;
    }

    store.set('emailTimestampRepairAt', new Date().toISOString());
    store.set('emailTimestampRepairCount', updated);
    if (updated > 0) {
      console.log(`[EmailTimestampRepair] Corrected ${updated} email/message events to source-time timestamps`);
    }
  } catch (error) {
    console.warn('[EmailTimestampRepair] Failed:', error?.message || error);
  }
}



// Get Google data (Gmail, Calendar) using real APIs for ALL accounts
async function getGoogleData({ since, includeContacts = true, includeDrive = false } = {}) {
  try {
    let accounts = store.get('googleAccounts') || [];

    // Legacy fallback migration
    if (accounts.length === 0) {
      const storedTokens = store.get('googleTokens');
      if (storedTokens && storedTokens.access_token) {
        accounts = [{ email: 'Legacy Account', tokens: storedTokens }];
      }
    }

    if (accounts.length === 0) {
      return {
        gmail: [],
        gmailSent: [],
        calendar: [],
        contacts: [],
        drive: [],
        _meta: {
          hardFailure: false,
          sinceInput: since || null,
          sinceEffective: GOOGLE_SYNC_BASELINE_ISO,
          overlapMs: 0,
          accountsTotal: 0,
          accountsWithTokens: 0,
          sources: {
            gmail: { successAccounts: 0, failedAccounts: 0, lastError: null },
            gmailSent: { successAccounts: 0, failedAccounts: 0, lastError: null },
            calendar: { successAccounts: 0, failedAccounts: 0, lastError: null },
            contacts: { successAccounts: 0, failedAccounts: 0, lastError: null },
            drive: { successAccounts: 0, failedAccounts: 0, lastError: null }
          }
        }
      };
    }

    let allGmailInbox = [];
    let allGmailSent = [];
    let allCalItems = [];
    let allContacts = [];
    let allDriveFiles = [];
    const sinceCursorMs = normalizeSyncCursorMs(since);
    const useIncrementalCursor = Boolean(sinceCursorMs);
    const effectiveSinceMs = useIncrementalCursor
      ? Math.max(Date.parse(GOOGLE_SYNC_BASELINE_ISO), sinceCursorMs - GOOGLE_SYNC_OVERLAP_MS)
      : Date.parse(GOOGLE_SYNC_BASELINE_ISO);
    const gmailAfter = useIncrementalCursor
      ? `after:${Math.floor(effectiveSinceMs / 1000)}`
      : `after:${GOOGLE_SYNC_BASELINE_ISO.slice(0, 10).replace(/-/g, '/')}`;
    const calendarUpdatedMin = useIncrementalCursor ? new Date(effectiveSinceMs).toISOString() : null;
    const driveModifiedMin = useIncrementalCursor ? new Date(effectiveSinceMs).toISOString() : null;
    const syncMeta = {
      hardFailure: false,
      sinceInput: since || null,
      sinceEffective: new Date(effectiveSinceMs).toISOString(),
      overlapMs: useIncrementalCursor ? GOOGLE_SYNC_OVERLAP_MS : 0,
      accountsTotal: accounts.length,
      accountsWithTokens: 0,
        sources: {
          gmail: { successAccounts: 0, failedAccounts: 0, lastError: null },
          gmailSent: { successAccounts: 0, failedAccounts: 0, lastError: null },
          calendar: { successAccounts: 0, failedAccounts: 0, lastError: null },
          contacts: { successAccounts: 0, failedAccounts: 0, lastError: null },
          drive: { successAccounts: 0, failedAccounts: 0, lastError: null }
        }
      };

    // Loop through every connected account to aggregate data
    for (const account of accounts) {
      if (!account.tokens) continue;
      syncMeta.accountsWithTokens += 1;
      const auth = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, getRedirectUri());
      auth.setCredentials(account.tokens);

      const gmail    = google.gmail({ version: 'v1', auth });
      const calendar = google.calendar({ version: 'v3', auth });
      const drive    = includeDrive ? google.drive({ version: 'v3', auth }) : null;

      // ---- Gmail: Incoming (all messages since 2020) ----
      try {
        let inboxPageToken = null;
        do {
          const inboxRes = await gmail.users.messages.list({
            userId: 'me',
            maxResults: 500,
            pageToken: inboxPageToken || undefined,
            q: `${gmailAfter} -from:me`
          });
          const refs = inboxRes.data.messages || [];
          for (const ref of refs) {
            const msg = await gmail.users.messages.get({
              userId: 'me',
              id: ref.id,
              format: 'metadata',
              metadataHeaders: ['Subject', 'From', 'To', 'Date']
            });
            const headers = msg.data.payload.headers || [];
            const subject = headers.find(x => x.name === 'Subject')?.value || '';
            const from = headers.find(h => h.name === 'From')?.value || '';
            const to = headers.find(h => h.name === 'To')?.value || '';
            const date = headers.find(h => h.name === 'Date')?.value || '';
            const sentAtMs = toValidEventTimestamp(msg.data.internalDate, date);
            allGmailInbox.push({
              id: msg.data.id,
              threadId: msg.data.threadId,
              subject,
              from,
              to,
              date,
              sent_at: new Date(sentAtMs).toISOString(),
              snippet: msg.data.snippet,
              timestamp: sentAtMs,
              labelIds: msg.data.labelIds || []
            });
          }
          inboxPageToken = inboxRes.data.nextPageToken || null;
        } while (inboxPageToken);
        syncMeta.sources.gmail.successAccounts += 1;
      } catch (e) {
        syncMeta.sources.gmail.failedAccounts += 1;
        syncMeta.sources.gmail.lastError = e?.message || String(e);
        console.warn('[getGoogleData] Gmail inbox sync failed for account:', account.email || 'unknown', e?.message || e);
      }

      // ---- Gmail: Sent (all sent emails since 2020; used for style/profiles) ----
      try {
        let sentPageToken = null;
        do {
          const sentRes = await gmail.users.messages.list({
            userId: 'me',
            maxResults: 500,
            pageToken: sentPageToken || undefined,
            q: `${gmailAfter} from:me`
          });
          const refs = sentRes.data.messages || [];
          for (const ref of refs) {
            const msg = await gmail.users.messages.get({ userId: 'me', id: ref.id, format: 'full' });
            let body = msg.data.snippet || '';
            if (msg.data.payload.parts) {
              const part = msg.data.payload.parts.find(p => p.mimeType === 'text/plain');
              if (part?.body?.data) body = Buffer.from(part.body.data, 'base64').toString('utf8');
            }
            allGmailSent.push({ id: msg.data.id, body: body.slice(0, 500), timestamp: Number(msg.data.internalDate || 0) });
          }
          sentPageToken = sentRes.data.nextPageToken || null;
        } while (sentPageToken);
        syncMeta.sources.gmailSent.successAccounts += 1;
      } catch (e) {
        syncMeta.sources.gmailSent.failedAccounts += 1;
        syncMeta.sources.gmailSent.lastError = e?.message || String(e);
        console.warn('[getGoogleData] Gmail sent sync failed for account:', account.email || 'unknown', e?.message || e);
      }

      // ---- Calendar: New, updated, or full historical fetch ----
      try {
        let calPageToken = null;
        do {
          const calRes = await calendar.events.list({
            calendarId: 'primary',
            // Keep a wide time window so incremental updatedMin can capture edits/new items
            // regardless of event start date.
            timeMin: GOOGLE_SYNC_BASELINE_ISO,
            timeMax: (new Date(Date.now() + 5 * 365 * 24 * 60 * 60 * 1000)).toISOString(),
            updatedMin: calendarUpdatedMin || undefined,
            singleEvents: true,
            orderBy: 'startTime',
            maxResults: 250,
            pageToken: calPageToken || undefined
          });
          const items = (calRes.data.items || []).map(ev => ({
            id: ev.id,
            summary: ev.summary,
            start: ev.start?.dateTime || ev.start?.date,
            end: ev.end?.dateTime || ev.end?.date,
            attendees: ev.attendees || [],
            event_title: ev.summary,
            start_time: ev.start?.dateTime || ev.start?.date,
            end_time: ev.end?.dateTime || ev.end?.date,
            updated: ev.updated
          }));
          allCalItems.push(...items);
          calPageToken = calRes.data.nextPageToken || null;
        } while (calPageToken);
        syncMeta.sources.calendar.successAccounts += 1;
      } catch (e) {
        syncMeta.sources.calendar.failedAccounts += 1;
        syncMeta.sources.calendar.lastError = e?.message || String(e);
        console.warn('[getGoogleData] Calendar sync failed for account:', account.email || 'unknown', e?.message || e);
      }

      if (includeContacts) {
        try {
          const contacts = await fetchGoogleContactsForAuth(auth);
          allContacts.push(...contacts);
          syncMeta.sources.contacts.successAccounts += 1;
        } catch (e) {
          syncMeta.sources.contacts.failedAccounts += 1;
          syncMeta.sources.contacts.lastError = e?.message || String(e);
          console.warn('[getGoogleData] Google Contacts sync failed for account:', account.email || 'unknown', e?.message || e);
        }
      }

      // ---- Drive: New, modified, or full historical fetch ----
      if (includeDrive) try {
        const driveQuery = driveModifiedMin
          ? `trashed = false and modifiedTime > '${driveModifiedMin}'`
          : "trashed = false";
        let drivePageToken = null;
        do {
          const driveRes = await drive.files.list({
            pageSize: 100,
            pageToken: drivePageToken || undefined,
            fields: 'nextPageToken, files(id, name, mimeType, modifiedTime, webViewLink, owners, shared, lastModifyingUser)',
            orderBy: 'modifiedTime desc',
            q: driveQuery
          });
          allDriveFiles.push(...(driveRes.data.files || []).map(f => ({
            id: f.id,
            name: f.name,
            doc_name: f.name,
            mimeType: f.mimeType,
            modified: f.modifiedTime,
            last_modified: f.modifiedTime,
            webViewLink: f.webViewLink || null,
            shared_with: (f.owners || []).map(o => o.emailAddress).filter(Boolean)
          })));
          drivePageToken = driveRes.data.nextPageToken || null;
        } while (drivePageToken);
        syncMeta.sources.drive.successAccounts += 1;
      } catch (e) {
        syncMeta.sources.drive.failedAccounts += 1;
        syncMeta.sources.drive.lastError = e?.message || String(e);
        console.warn('[getGoogleData] Drive sync failed for account:', account.email || 'unknown', e?.message || e);
      }
    }

    const hardFailure =
      syncMeta.accountsWithTokens === 0 ||
      (syncMeta.sources.gmail.failedAccounts > 0 && syncMeta.sources.gmail.successAccounts === 0) ||
      (syncMeta.sources.calendar.failedAccounts > 0 && syncMeta.sources.calendar.successAccounts === 0);
    syncMeta.hardFailure = hardFailure;

    return {
      gmail: allGmailInbox,
      gmailSent: allGmailSent,
      calendar: allCalItems,
      contacts: allContacts,
      drive: allDriveFiles,
      _meta: syncMeta
    };
  } catch (error) {
    console.error('Error getting Google data:', error);
    return {
      gmail: [],
      gmailSent: [],
      calendar: [],
      contacts: [],
      drive: [],
      _meta: {
        hardFailure: true,
        sinceInput: since || null,
        sinceEffective: null,
        overlapMs: 0,
        accountsTotal: 0,
        accountsWithTokens: 0,
        sources: {
          gmail: { successAccounts: 0, failedAccounts: 1, lastError: error?.message || String(error) },
          gmailSent: { successAccounts: 0, failedAccounts: 0, lastError: null },
          calendar: { successAccounts: 0, failedAccounts: 1, lastError: error?.message || String(error) },
          contacts: { successAccounts: 0, failedAccounts: 1, lastError: error?.message || String(error) },
          drive: { successAccounts: 0, failedAccounts: 0, lastError: null }
        }
      }
    };
  }
}

async function generateDailySummary() {
  try {
    console.log('Generating AI daily summary with historical context...');

    // Define today's date for use throughout the function
    const today = new Date().toISOString().slice(0, 10);

    // Initialize the proactive suggestion engine
    const engine = new ProactiveSuggestionEngine();

    // Generate proactive suggestions using the new algorithm
    const proactiveResult = await engine.generateProactiveSuggestions();

    // Get real browser history
    const browserHistory = await refreshBrowserHistory({ reason: 'generate_proactive_todos', force: true });
    console.log(`Retrieved ${browserHistory.length} URLs from browser history`);

    // Get Google data
    const googleData = await getGoogleData();
    const sensorEvents = getSensorEvents();

    // Prepare data for AI analysis
    const urlLines = browserHistory.slice(0, 100).map(item =>
      `${item.url} | ${item.title} | ${new Date(item.timestamp || item.last_visit_time || Date.now()).toLocaleString()}`
    ).join('\n');

    const emailLines = (googleData.gmail || []).slice(0, 20).map(email =>
      `From: ${email.from} | Subject: ${email.subject} | Date: ${new Date(email.timestamp).toLocaleString()}`
    ).join('\n');

    const eventLines = (googleData.calendar || []).slice(0, 10).map(event =>
      `${event.event_title} | ${new Date(event.start_time).toLocaleString()}`
    ).join('\n');

    const driveLines = (googleData.drive || []).slice(0, 10).map(doc =>
      `${doc.doc_name} | Modified: ${new Date(doc.last_modified).toLocaleString()}`
    ).join('\n');

    // Get existing user profile and historical summaries
    const existingProfile = store.get('userProfile') || {};
    const historicalSummaries = store.get('historicalSummaries') || {};
    const sortedPastDates = Object.keys(historicalSummaries).sort((a,b) => b.localeCompare(a));
    const recentSummaries  = sortedPastDates.slice(0, 14).map(d => historicalSummaries[d]);

    const todayEventsUnified = [
      ...(googleData.gmail || []).map(m => ({
        id: `email_${m.id}`,
        date: new Date(m.timestamp).toISOString().slice(0, 10),
        type: 'email',
        title: m.subject,
        text: m.snippet || m.body || '',
        people: [m.from, ...(m.to || [])].filter(Boolean),
        metadata: { email: { from: m.from, to: m.to || [] } },
        timestamp: m.timestamp
      })),
      ...(googleData.drive || []).map(d => ({
        id: `doc_${d.id}`,
        date: new Date(d.last_modified).toISOString().slice(0, 10),
        type: d.mimeType?.includes('spreadsheet') ? 'spreadsheet' : 'doc',
        title: d.name,
        text: '',
        people: [],
        metadata: { doc: { last_modified: d.last_modified, mimeType: d.mimeType } },
        timestamp: d.last_modified
      })),
      ...(googleData.calendar || []).map(c => ({
        id: `cal_${c.id}`,
        date: new Date(c.start_time).toISOString().slice(0, 10),
        type: 'calendar_event',
        title: c.summary || c.title,
        text: c.description || '',
        people: (c.attendees || []).map(a => a.email || a).filter(Boolean),
        metadata: { calendar: { start: c.start_time, end: c.end_time } },
        timestamp: c.start_time
      })),
      ...browserHistory.map(p => {
        let domain = '';
        try {
          domain = new URL(p.url).hostname;
        } catch (_) {}

        return {
          id: `hist_${Math.random()}`,
          date: new Date(p.timestamp || p.last_visit_time).toISOString().slice(0, 10),
          type: 'browser_history',
          title: p.title || p.url,
          text: '',
          people: [],
          metadata: { history: { url: p.url, domain } },
          timestamp: p.timestamp || p.last_visit_time
        };
      }),
      ...sensorEvents.map(event => ({
        id: event.id,
        date: new Date(event.timestamp).toISOString().slice(0, 10),
        type: 'screen_capture',
        title: event.title || 'Desktop capture',
        text: event.text || '',
        people: [],
        metadata: {
          sensor: {
            sourceName: event.sourceName || '',
            imagePath: event.imagePath || '',
            ocrStatus: event.ocrStatus || 'unavailable',
            captureMode: event.captureMode || 'screen',
            activeApp: event.activeApp || '',
            activeWindowTitle: event.activeWindowTitle || '',
            windowId: event.windowId || null,
            windowContextStatus: event.windowContextStatus || 'unavailable'
          }
        },
        timestamp: event.timestamp
      }))
    ];

    // Use the new context-aware AI summary generator
    let todayResult;
    try {
      todayResult = await generateTodaySummaryWithContext({
        todayEvents: { all: todayEventsUnified },
        historicalSummaries: recentSummaries,
        userProfile:  existingProfile,
        futureCal:    googleData.calendar || [],
        apiKey:       process.env.DEEPSEEK_API_KEY
      });
    } catch (aiErr) {
      console.warn('AI summary with context failed, using fallback:', aiErr.message);
      todayResult = {
        narrative:    'AI analysis temporarily unavailable.',
        suggestions:  [],
        patterns:     [],
        preferences:  [],
        intent_clusters: []
      };
    }


    // ── NEW: Hierarchical Graph Memory Build (Section 6) ───────────────────
    const apiKey = process.env.DEEPSEEK_API_KEY;
    const appData = {
      Gmail: todayEventsUnified.filter(item => item.type === 'email'),
      Calendar: todayEventsUnified.filter(item => item.type === 'calendar_event'),
      Drive: todayEventsUnified.filter(item => item.type === 'doc' || item.type === 'spreadsheet'),
      History: todayEventsUnified.filter(item => item.type === 'browser_history'),
      Sensors: todayEventsUnified.filter(item => item.type === 'screen_capture')
    };

    const appCores = {};
    const appItemsForTaskDetection = [];

    for (const [appId, items] of Object.entries(appData)) {
      if (!items.length) continue;

      items.forEach(item => {
        addNode({
          id: item.id,
          type: 'raw_event',
          data: {
            appId,
            event_type: item.type,
            title: item.title || '',
            text: item.text || '',
            timestamp: item.timestamp || null,
            metadata: item.metadata || {}
          }
        });
      });

      // 1. Process app data with intelligence engine
      const appDataMap = { [appId]: { rawItems: items, appName: appId } };
      const graphResult = await engine.buildGlobalGraph({ appDataMap, apiKey, store });

      if (graphResult && graphResult.nodes) {
        console.log(`[generateDailySummary] Built graph for ${appId}: ${graphResult.nodes.length} nodes`);

        // Extract app core from the graph results
        const appCoreNode = graphResult.nodes.find(n => n.type === 'app_core');
        if (appCoreNode) {
          appCores[appId] = appCoreNode.data.narrative || appCoreNode.data.title || `${appId} Core`;

          // Add App-Core Node
          const appCoreId = `core_${appId.toLowerCase()}`;
          addNode({ id: appCoreId, type: 'app_core', data: { title: `${appId} Core`, narrative: appCores[appId] } });
          addEdge('global_core', appCoreId, 'manages', `Centralized control of ${appId} wisdom.`);

          // Add Episode Nodes & Edges from graph results
          const episodeNodes = graphResult.nodes.filter(n => n.type === 'episode');
          episodeNodes.forEach(ep => {
            addNode({ id: ep.id, type: 'episode', data: ep.data });
            addEdge(ep.id, appCoreId, 'informs_core', 'Specific reconstructed experience informs app core.');
          });

          // Add Semantic Nodes & Edges from graph results
          const semanticNodes = graphResult.nodes.filter(n => n.type === 'semantic');
          semanticNodes.forEach(sem => {
            addNode({ id: sem.id, type: 'semantic', data: sem.data });
            // Link semantic to its episode (if available)
            const episodeId = sem.data.episode_id;
            if (episodeId && episodeNodes.find(ep => ep.id === episodeId)) {
              addEdge(sem.id, episodeId, 'extracted_from', 'Semantic understanding extracted from episode.');
            }
          });

          // Add Insight Nodes & Edges from graph results
          const insightNodes = graphResult.nodes.filter(n => n.type === 'insight');
          insightNodes.forEach(ins => {
            addNode({ id: ins.id, type: 'insight', data: ins.data });
            addEdge(ins.id, appCoreId, 'shapes_core', 'High-level insight shapes app core understanding.');
          });
        }
      }

      // Collect for task detection
      appItemsForTaskDetection.push(...items);
    }

    // 2. Build Global Core
    const currentGlobal = (store.get('proactiveMemory') || {}).core || '';

    // Simple global core generation - combine app cores
    const newGlobal = Object.values(appCores).length > 0
      ? `Integrated core memory from ${Object.keys(appCores).join(', ')} apps`
      : 'Building initial core memory...';

    addNode({ id: 'global_core', type: 'global_core', data: { title: 'Global Core Memory', narrative: newGlobal } });
    Object.keys(appCores).forEach(appId => {
      addEdge(`core_${appId.toLowerCase()}`, 'global_core', 'contributes_to_core', 'App core contributes to global core memory.');
    });
    store.set('proactiveMemory', { core: newGlobal });

    // 3. Stage 1: Task Detection
    const detectedTasks = await detectTasks(appItemsForTaskDetection, apiKey);
    const taskNodes = detectedTasks.map(t => {
      const id = `task_${Math.random().toString(36).slice(2, 9)}`;
      const node = { id, type: 'task', data: t };
      addNode(node);
      if (t.source_id) addEdge(t.source_id, id, 'triggers_task', 'Source obligation.');
      return node;
    });

    // 4. Update legacy state for UI backward compatibility
    historicalSummaries[today] = {
      id:              `daily_${today}`,
      date:            today,
      narrative:       todayResult.narrative || '',
      suggestions:     todayResult.suggestions || [],
      events:          todayEventsUnified,
      counts: {
        emails: todayEventsUnified.filter(item => item.type === 'email').length,
        calendar_events: todayEventsUnified.filter(item => item.type === 'calendar_event').length,
        docs: todayEventsUnified.filter(item => item.type === 'doc' || item.type === 'spreadsheet').length,
        page_visits: todayEventsUnified.filter(item => item.type === 'browser_history').length,
        screen_captures: todayEventsUnified.filter(item => item.type === 'screen_capture').length
      },
      generated_at:    new Date().toISOString()
    };
    store.set('historicalSummaries', historicalSummaries);
    await persistDailyBriefSemanticNode(historicalSummaries[today]);

    // Update search index with today's data
    const newIndex = rebuildInvertedIndex(historicalSummaries);
    store.set('searchIndex', newIndex);

    // 5. Stage 2: Suggestion Generation (Walk the Graph)
    const graphSuggestions = [];
    for (const tn of taskNodes) {
      // Build a minimal context subgraph: immediate neighbors and core global
      const { nodes, edges } = getGraph();
      const neighbors = edges
        .filter(e => e.to === tn.id || e.from === tn.id)
        .map(e => nodes.find(n => n.id === (e.to === tn.id ? e.from : e.to)))
        .filter(Boolean);

      const context = {
        task: tn.data,
        neighbors: neighbors.map(n => ({ type: n.type, ...n.data })),
        core: newGlobal.slice(0, 1000) // Keep it manageable
      };

      const suggResult = await generateSuggestionFromGraph(tn.data, context, newGlobal, apiKey);
      if (suggResult) {
        const sugg = {
          title:       suggResult.title,
          priority:    tn.data.priority || 'medium',
          category:    'Proactive',
          goal:        'Personal Prep',
          reason:      suggResult.justification,
          description: suggResult.suggestion,
          substeps:    suggResult.substeps || [],
          evidence_id: tn.data.source_id || null,
          assignee:    'human',
          xp:          tn.data.priority === 'high' ? 50 : 30
        };
        graphSuggestions.push(sugg);
        // Link suggestion back in graph
        const suggId = `sugg_${Math.random().toString(36).slice(2,9)}`;
        addNode({ id: suggId, type: 'suggestion', data: sugg });
        addEdge(tn.id, suggId, 'suggests', 'Specific actionable advice.');
      }
    }

    const allTasks = deduplicateTasks([
      ...graphSuggestions,
      ...(todayResult.suggestions || []).map(s => ({ ...s, assignee: 'human' })),
      ...(proactiveResult.suggestions || [])
    ]);

    // 6. Update user profile
    const updatedProfile = {
      ...existingProfile,
      proactiveMemory: { core: newGlobal }
    };
    store.set('userProfile', updatedProfile);

    // 6b. Refresh single global core memory with detailed user context.
    const detailedCore = buildDetailedGlobalCoreMemory({
      userProfile: updatedProfile,
      dailySummary: { narrative: todayResult.narrative || '' },
      historicalSummaries,
      suggestions: proactiveResult.suggestions || [],
      todos: allTasks
    });
    persistGlobalCoreMemory(detailedCore);

    // ── Build final summary payload ────────────────────────────────────────
    const summaryData = {
      narrative:        todayResult.narrative || '',
      status:           'productive',
      tasks:            allTasks,
      proactiveClusters: proactiveResult.clusters,
      date:             today,
      timestamp:        Date.now(),
      top_people:       todayResult.top_people      || [],
      patterns:         todayResult.patterns        || [],
      preferences:      todayResult.preferences     || [],
      intent_clusters:  todayResult.intent_clusters || [],
      browserStats: {
        totalUrls:        browserHistory.length,
        topDomains:       getTopDomains(browserHistory, 5),
        productivityScore: calculateProductivityScore(browserHistory)
      },
      googleStats: {
        emails:    (googleData.gmail    || []).length,
        events:    (googleData.calendar || []).length,
        documents: (googleData.drive    || []).length
      },
      sensorStats: {
        captures: sensorEvents.length,
        lastCaptureAt: sensorEvents[0]?.timestamp || null
      }
    };

    store.set('dailySummary', summaryData);

    console.log('Daily summary generated successfully with historical context.');
    const aiSuggestionCount = Array.isArray(todayResult?.suggestions) ? todayResult.suggestions.length : 0;
    console.log(`Tasks: ${allTasks.length} | AI suggestions: ${aiSuggestionCount} | Proactive: ${proactiveResult.suggestions?.length || 0}`);

    return summaryData;
  } catch (error) {
    console.error('Error generating daily summary:', error);
    const fallbackSummary = {
      narrative:  'Summary generation encountered an error.',
      status:     'productive',
      tasks:      [],
      date:       new Date().toISOString().slice(0, 10),
      timestamp:  Date.now(),
      error:      error.message
    };
    store.set('dailySummary', fallbackSummary);
    return fallbackSummary;
  }
}


// ── IPC: Initial Historical Sync ─────────────────────────────────────────────
async function processSyncResult(result) {
  // 1. Persist historical summaries
  store.set('historicalSummaries', result.summaries);
  store.set('initialSyncDone', true);

  // 2. Build Inverted Index (Section 3.1)
  const index = rebuildInvertedIndex(result.summaries);
  store.set('searchIndex', index);

  // 3. Intelligence Engine (Section 5)
  const summariesArray = Object.values(result.summaries).sort((a, b) => a.date.localeCompare(b.date));

  // Simple pattern extraction - skip for now to avoid error
  const patterns = { work_hours: [], productivity_peaks: [], recurring_tasks: [] };

  const allEventsFlat = summariesArray.flatMap(s => s.events || []);

  // Simple preferences extraction - skip for now to avoid error
  const deepPrefs = { deep_work_focus: [], productivity_patterns: [], communication_style: [] };

  const rebuiltGraph = await rebuildLayeredMemoryGraphFromEvents(allEventsFlat, process.env.DEEPSEEK_API_KEY);

  // 4. Update User Profile
  const existingProfile = store.get('userProfile') || {};
  store.set('userProfile', {
    ...existingProfile,
    patterns: [...new Set([...(existingProfile.patterns || []), ...result.userPatterns])].slice(0, 40),
    preferences: [...new Set([...(existingProfile.preferences || []), ...result.userPreferences])].slice(0, 40),
    work_style: patterns?.work_style || existingProfile.work_style,
    leisure_style: patterns?.leisure_style || existingProfile.leisure_style,
    planning_style: patterns?.planning_style || existingProfile.planning_style,
    deep_preferences: deepPrefs || existingProfile.deep_preferences,
    top_intent_clusters: result.topIntentClusters || [],
    proactiveMemory: { core: rebuiltGraph.core }
  });

  return Object.keys(result.summaries).length;
}

// ── IPC: Initial Historical Sync ─────────────────────────────────────────────
ipcMain.handle('run-initial-sync', async (event) => {
  try {
    const googleData    = store.get('googleData') || await getGoogleData();
    const browserHistory = await refreshBrowserHistory({ reason: 'manual_initial_sync', force: true });

    const sendProgress = (progress) => {
      if (mainWindow && mainWindow.webContents) {
        mainWindow.webContents.send('initial-sync-progress', progress);
      }
    };

    sendProgress({ phase: 'starting', done: 0, total: 0 });

    const result = await runInitialSync({
      userId:         'local',
      messages:       googleData.gmail     || [],
      docs:           googleData.drive     || [],
      calendarEvents: googleData.calendar  || [],
      pageVisits:     browserHistory,
      apiKey:         process.env.DEEPSEEK_API_KEY,
      onProgress:     sendProgress,
      store
    });

    // Build the hierarchical memory graph from the freshly-synced raw items.
    try {
      const appDataMap = {
        Gmail: { rawItems: googleData.gmail || [], appName: 'Gmail' },
        Drive: { rawItems: googleData.drive || [], appName: 'Drive' },
        Calendar: { rawItems: googleData.calendar || [], appName: 'Calendar' },
        History: { rawItems: browserHistory || [], appName: 'History' }
      };

      const graphResult = await require('./services/agent/intelligence-engine').buildGlobalGraph({ appDataMap, apiKey: process.env.DEEPSEEK_API_KEY, store });
      console.log(`[initialSync] Graph built: nodes=${(graphResult.nodes||[]).length} edges=${(graphResult.edges||[]).length}`);
      // Expose graph info to renderer via progress
      sendProgress({ phase: 'graph_built', done: 0, total: 0, graph: { nodes: (graphResult.nodes||[]).length, edges: (graphResult.edges||[]).length } });
    } catch (gErr) {
      console.warn('[initialSync] Graph build failed:', gErr.message || gErr);
    }

    const daysCount = await processSyncResult(result);
    console.log(`[initialSync] Complete: ${daysCount} days summarised.`);
    sendProgress({ phase: 'complete', done: daysCount, total: daysCount });

    return {
      success:       true,
      daysProcessed: daysCount,
      sortedDates:   result.sortedDates,
      userPatterns:  result.userPatterns,
      userPreferences: result.userPreferences
    };
  } catch (err) {
    console.error('[initialSync] Failed:', err);
    return { success: false, error: err.message };
  }
});

// ── IPC: Get full details for a specific event (evidence backlink) ───────────
ipcMain.handle('get-event-details', async (event, eventId) => {
  try {
    const row = await db.getQuery(`SELECT * FROM events WHERE id = ? OR source_ref = ?`, [eventId, eventId]);
    if (row) {
      try {
        row.metadata = JSON.parse(row.metadata || '{}');
      } catch (_) {}
      return row;
    }
  } catch (err) {
    console.error('[get-event-details] Error:', err);
  }

  const historicalSummaries = store.get('historicalSummaries') || {};
  for (const date in historicalSummaries) {
    const summary = historicalSummaries[date];
    if (summary.events) {
      const found = summary.events.find(e => e.id === eventId || e.source_id === eventId);
      if (found) return found;
    }
  }
  return null;
});

// ── IPC: Search daily summaries by keyword / person name ─────────────────────
ipcMain.handle('search-daily-summaries', (event, query) => {
  const historicalSummaries = store.get('historicalSummaries') || {};
  const searchIndex = store.get('searchIndex') || { people: {}, topics: {} };

  const q = (query || '').toLowerCase().trim();
  if (!q) return [];

  // 1. Check inverted index for exact matches or related dates
  const datesFromPeople = searchIndex.people[q] || [];
  const datesFromTopics = searchIndex.topics[q] || [];
  const indexedDates = [...new Set([...datesFromPeople, ...datesFromTopics])];

  // 2. Perform fallback search on all summaries if index is empty or for fuzzy matching
  return searchSummaries(historicalSummaries, query, 20, indexedDates);
});

// ── IPC: Get historical summaries (sorted, with optional date range) ──────────
ipcMain.handle('get-historical-summaries', (event, { startDate, endDate, limit } = {}) => {
  const historicalSummaries = store.get('historicalSummaries') || {};
  let dates = Object.keys(historicalSummaries).sort((a, b) => b.localeCompare(a));
  if (startDate) dates = dates.filter(d => d >= startDate);
  if (endDate)   dates = dates.filter(d => d <= endDate);
  if (limit)     dates = dates.slice(0, limit);
  return dates.map(d => historicalSummaries[d]);
});

// ── IPC: Get initial sync status ─────────────────────────────────────────────
ipcMain.handle('get-initial-sync-status', () => {
  const done     = store.get('initialSyncDone') || false;
  const summaries = store.get('historicalSummaries') || {};
  const daysCount = Object.keys(summaries).length;
  return { done, daysCount };
});

// Helper functions for task deduplication and analytics
function deduplicateTasks(tasks) {
  const seenTitles = new Set();
  return tasks.filter(task => {
    if (!task || !task.title) return false;

    // Normalize title for strict comparison
    const normalized = task.title.toLowerCase()
      .replace(/[^a-z0-9]/g, '')
      .trim();

    // Skip if we've already seen this exact task
    if (seenTitles.has(normalized)) return false;

    // Fuzzy duplicate check: skip if a very similar title exists
    for (let seen of seenTitles) {
      if (normalized.includes(seen) || seen.includes(normalized)) {
        if (Math.abs(normalized.length - seen.length) < 5) return false;
      }
    }

    seenTitles.add(normalized);
    return true;
  });
}

function normalizeTaskCategory(category) {
  const raw = String(category || '').toLowerCase().trim();
  if (raw.includes('follow')) return 'followup';
  if (raw.includes('creative')) return 'creative';
  if (raw.includes('personal')) return 'personal';
  return 'work';
}

function isGenericTaskTitle(title) {
  return /\b(continue|follow up|return to|work on|resume|finish this|keep working)\b/i.test(String(title || ''));
}

function extractConcreteFocus(text) {
  const cleaned = String(text || '')
    .replace(/\s+/g, ' ')
    .replace(/^[\-\•]\s*/, '')
    .trim();
  if (!cleaned) return '';
  const firstSentence = cleaned.split(/[.!?]/)[0] || cleaned;
  return firstSentence.slice(0, 90).trim();
}

function refineTaskSpecificity(task) {
  const next = { ...task };
  const contextText = [next.description, next.reason, next.ai_draft].filter(Boolean).join(' ');
  const focus = extractConcreteFocus(contextText);
  const baseTitle = String(next.title || '').trim();

  if (!baseTitle || isGenericTaskTitle(baseTitle)) {
    if (/\b(reply|email|message)\b/i.test(contextText)) {
      next.title = `Send the pending reply${focus ? `: ${focus}` : ''}`.slice(0, 120);
    } else if (/\b(homework|assignment|problem set|exercise|chapter|class)\b/i.test(contextText)) {
      next.title = `Finish and submit homework${focus ? `: ${focus}` : ''}`.slice(0, 120);
    } else if (/\b(draft|doc|document|report|slide|proposal)\b/i.test(contextText)) {
      next.title = `Finalize the draft${focus ? `: ${focus}` : ''}`.slice(0, 120);
    } else if (focus) {
      next.title = `Complete: ${focus}`.slice(0, 120);
    }
  }

  return next;
}

function trimToSingleClause(text) {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  const parts = raw.split(/\b(?:and|then|also|plus|while)\b|[;|]/i).map((p) => p.trim()).filter(Boolean);
  return (parts[0] || raw).slice(0, 180);
}

function inferTaskIntent(text) {
  const t = String(text || '').toLowerCase();
  const intents = [];
  if (/\b(reply|email|gmail|follow up|message|inbox|thread)\b/.test(t)) intents.push('communication');
  if (/\b(meeting|calendar|agenda|attendees|event)\b/.test(t)) intents.push('meeting');
  if (/\b(doc|document|proposal|slide|deck|report|draft)\b/.test(t)) intents.push('document');
  if (/\b(code|bug|error|stack|commit|deploy|api|manifest|extension)\b/.test(t)) intents.push('engineering');
  if (/\b(homework|assignment|study|class|lecture|exam)\b/.test(t)) intents.push('study');
  if (/\b(invoice|tax|bank|payment|finance|bill)\b/.test(t)) intents.push('admin');
  if (!intents.length) intents.push('general');
  return intents;
}

function enforceSingleFocusTask(task) {
  const next = { ...task };
  const combined = [next.title, next.description, next.reason, next.ai_draft].filter(Boolean).join(' ');
  const intents = inferTaskIntent(combined);
  const primaryIntent = intents[0];
  const focus = extractConcreteFocus(trimToSingleClause(next.title || next.description || next.reason || ''));

  // Normalize multi-intent tasks to a single objective.
  if (intents.length > 1 || /\b(and|then|also|plus)\b/i.test(String(next.title || ''))) {
    if (primaryIntent === 'communication') {
      next.title = `Reply to one pending thread${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Handle exactly one communication task now${focus ? ` (${focus})` : ''}.`;
    } else if (primaryIntent === 'meeting') {
      next.title = `Prepare one meeting${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Focus on a single meeting prep action${focus ? ` for ${focus}` : ''}.`;
    } else if (primaryIntent === 'document') {
      next.title = `Finalize one document step${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Complete one concrete document action${focus ? ` for ${focus}` : ''}.`;
    } else if (primaryIntent === 'engineering') {
      next.title = `Fix one specific engineering issue${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Work on one bug/implementation step only${focus ? ` (${focus})` : ''}.`;
    } else if (primaryIntent === 'study') {
      next.title = `Complete one study task${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Finish one concrete study assignment step${focus ? ` for ${focus}` : ''}.`;
    } else if (primaryIntent === 'admin') {
      next.title = `Resolve one admin item${focus ? `: ${focus}` : ''}`.slice(0, 120);
      next.description = `Handle one operational/admin task${focus ? ` (${focus})` : ''}.`;
    } else if (focus) {
      next.title = `Complete one specific task: ${focus}`.slice(0, 120);
      next.description = `Focus only on this single task: ${focus}.`;
    }

    next.reason = trimToSingleClause(next.reason || next.description || '');
  }

  next.title = trimToSingleClause(next.title).slice(0, 120);
  next.description = trimToSingleClause(next.description).slice(0, 180);
  return next;
}

function ensureShortPlan(task) {
  const next = { ...task };
  const currentPlan = Array.isArray(next.action_plan) ? next.action_plan : [];
  if (currentPlan.length >= 2 && /\bplan:/i.test(String(next.ai_draft || ''))) return next;

  const targetUrl = next.deeplink || currentPlan[0]?.url || null;
  const shortPlan = [
    targetUrl ? 'Open the exact source context.' : 'Open the source context.',
    'Complete one concrete unfinished step.',
    'Verify completion and log what changed.'
  ];

  const normalizedPlan = currentPlan.length ? currentPlan : [
    ...(targetUrl ? [{ step: 1, action: 'NAVIGATE', url: targetUrl, intent: 'open_source_context' }] : []),
    { step: targetUrl ? 2 : 1, action: 'READ_PAGE_STATE', intent: 'identify_unfinished_step' },
    { step: targetUrl ? 3 : 2, action: 'READ_PAGE_STATE', intent: 'verify_completion' }
  ];

  const planText = `Plan: 1) ${shortPlan[0]} 2) ${shortPlan[1]} 3) ${shortPlan[2]}`;
  if (!String(next.ai_draft || '').includes('Plan:')) {
    next.ai_draft = `${(next.ai_draft || '').trim()}${next.ai_draft ? '\n\n' : ''}${planText}`.trim();
  }
  next.action_plan = normalizedPlan;
  return next;
}

function taskLooksRecentlyCompleted(task) {
  const text = `${task?.title || ''} ${task?.description || ''} ${task?.reason || ''}`.toLowerCase();
  const terms = text
    .split(/[^a-z0-9]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 4)
    .slice(0, 8);
  if (!terms.length) return false;

  const completionRegex = /\b(done|completed|resolved|sent|replied|submitted|finished|closed|merged|deployed)\b/i;
  const recentWindowMs = 36 * 60 * 60 * 1000;
  const since = Date.now() - recentWindowMs;

  const completedLog = store.get('completedTasksLog') || [];
  for (const entry of completedLog) {
    const ts = parseTs(entry.completedAt || entry.timestamp || 0);
    if (!ts || ts < since) continue;
    const hay = `${entry.title || ''} ${entry.reason || ''}`.toLowerCase();
    if (!completionRegex.test(`completed ${hay}`)) continue;
    const overlap = terms.filter((t) => hay.includes(t)).length;
    if (overlap >= 2) return true;
  }

  return false;
}

function diversifyTasksByCategory(tasks, maxTotal = 16, maxPerCategory = 4) {
  const priorityWeight = { high: 3, medium: 2, low: 1 };
  const sorted = [...(tasks || [])].sort((a, b) => {
    const pa = priorityWeight[String(a?.priority || 'medium').toLowerCase()] || 0;
    const pb = priorityWeight[String(b?.priority || 'medium').toLowerCase()] || 0;
    if (pb !== pa) return pb - pa;
    return Number(b?.createdAt || 0) - Number(a?.createdAt || 0);
  });

  const byCat = new Map();
  const output = [];
  for (const task of sorted) {
    if (output.length >= maxTotal) break;
    const category = normalizeTaskCategory(task?.category);
    const used = byCat.get(category) || 0;
    if (used >= maxPerCategory) continue;
    byCat.set(category, used + 1);
    output.push({ ...task, category });
  }
  return output;
}

function buildHomeworkRecoveryTasks(limit = 4) {
  const studyPattern = /\b(homework|assignment|problem set|exercise|chapter|submit|deadline|due|lecture|classroom|canvas|study)\b/i;
  const sensors = (getSensorEvents() || []).slice(0, 120);
  const candidates = sensors.filter((ev) => {
    const text = `${ev.activeWindowTitle || ''} ${ev.text || ''} ${ev.activeApp || ''}`;
    return studyPattern.test(text);
  });
  if (!candidates.length) return [];

  const recentUrls = (((store.get('extensionData') || {}).urls) || [])
    .slice()
    .sort((a, b) => Number(b?.timestamp || 0) - Number(a?.timestamp || 0));

  const studyUrl = recentUrls.find((u) => /(classroom\.google\.com|canvas|instructure|docs\.google\.com|notion\.so)/i.test(String(u?.url || '')))?.url || null;
  const seen = new Set();
  const tasks = [];
  for (const ev of candidates) {
    if (tasks.length >= limit) break;
    const focus = extractConcreteFocus(ev.activeWindowTitle || ev.text || 'unfinished homework');
    const key = focus.toLowerCase();
    if (!focus || seen.has(key)) continue;
    seen.add(key);
    tasks.push({
      id: `homework_${ev.id || Date.now()}_${tasks.length}`,
      title: `Finish and submit: ${focus}`.slice(0, 120),
      priority: /\bdue|deadline|submit\b/i.test(String(ev.text || '')) ? 'high' : 'medium',
      description: `Detected unfinished study work from recent activity: ${focus}.`,
      reason: 'You started this academic task but no completion signal was captured.',
      category: 'work',
      assignee: 'ai',
      ai_draft: `Plan: 1) Reopen ${focus}. 2) Complete the missing section or answers. 3) Submit and confirm status.`,
      deeplink: studyUrl,
      action_plan: studyUrl
        ? [
            { step: 1, action: 'NAVIGATE', url: studyUrl, intent: 'open_study_workspace' },
            { step: 2, action: 'READ_PAGE_STATE', intent: 'locate_unfinished_item' },
            { step: 3, action: 'READ_PAGE_STATE', intent: 'confirm_submission' }
          ]
        : [],
      source: 'homework-recovery',
      completed: false,
      createdAt: Date.now()
    });
  }
  return tasks;
}

async function buildCurrentWorkContinuationTasks(limit = 6) {
  const sensors = (getSensorEvents() || []).slice(0, 100);
  const urls = ((((store.get('extensionData') || {}).urls) || []).slice())
    .sort((a, b) => Number(b?.timestamp || 0) - Number(a?.timestamp || 0))
    .slice(0, 120);
  const rawEvents = await db.allQuery(
    `SELECT id, type, text, timestamp, metadata, source FROM events ORDER BY timestamp DESC LIMIT 120`
  ).catch(() => []);

  const actionPattern = /\b(opened|edited|drafted|reviewed|searched|replied|wrote|updated|clicked)\b/gi;
  const incompletePattern = /\b(todo|to do|next step|follow up|unfinished|draft|pending|fix|revise|submit)\b/i;

  const tasks = [];
  const seen = new Set();
  for (const sensor of sensors) {
    if (tasks.length >= limit) break;
    const focus = extractConcreteFocus(sensor.activeWindowTitle || sensor.title || sensor.text || '');
    if (!focus) continue;
    const key = focus.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    const sensorText = String(sensor.text || '');
    const isIncomplete = incompletePattern.test(sensorText) || incompletePattern.test(focus);
    if (!isIncomplete) continue;

    const relatedUrls = urls
      .filter((u) => String(u?.title || '').toLowerCase().includes(key.slice(0, 30)) || String(u?.url || '').toLowerCase().includes(key.split(' ')[0] || ''))
      .slice(0, 2);
    const relatedRaw = rawEvents
      .filter((e) => String(e?.text || '').toLowerCase().includes(key.slice(0, 24)))
      .slice(0, 3);

    const takenActions = Array.from(new Set([
      ...(sensorText.match(actionPattern) || []).map((a) => a.toLowerCase()),
      ...relatedRaw.flatMap((e) => (String(e?.text || '').match(actionPattern) || []).map((a) => a.toLowerCase()))
    ])).slice(0, 4);

    const deeplink = relatedUrls[0]?.url || null;
    const reason = `Actions already taken: ${takenActions.length ? takenActions.join(', ') : 'context opened and reviewed'}. Continue from the unfinished state now.`;
    tasks.push({
      id: `cont_${sensor.id || Date.now()}_${tasks.length}`,
      title: `Continue and finish: ${focus}`.slice(0, 120),
      priority: /deadline|due|submit|asap/i.test(sensorText) ? 'high' : 'medium',
      description: `Current work detected in ${sensor.activeApp || 'desktop'}: ${focus}.`,
      reason,
      category: /essay|assignment|homework|study|class/i.test(`${focus} ${sensorText}`) ? 'work' : 'followup',
      assignee: 'ai',
      ai_draft: `Plan: 1) Reopen the active context. 2) Complete the unfinished step you already started. 3) Confirm completion and capture the final state.`,
      deeplink,
      action_plan: [
        ...(deeplink ? [{ step: 1, action: 'NAVIGATE', url: deeplink, intent: 'reopen_work_context' }] : []),
        { step: deeplink ? 2 : 1, action: 'READ_PAGE_STATE', intent: 'identify_incomplete_action' },
        { step: deeplink ? 3 : 2, action: 'READ_PAGE_STATE', intent: 'complete_and_verify' }
      ],
      source: 'live-work-monitor',
      completed: false,
      createdAt: Date.now()
    });
  }
  return tasks;
}

function buildTaskMemoryEvidenceIndex(limit = 120) {
  const sensorEvents = (getSensorEvents() || []).slice(0, limit);
  return sensorEvents.map((event) => {
    const summary = `${event.activeApp || ''} ${event.activeWindowTitle || ''} ${event.text || ''}`.replace(/\s+/g, ' ').trim();
    return {
      text: summary,
      app: event.activeApp || 'desktop',
      ts: parseTs(event.timestamp || 0)
    };
  }).filter((item) => item.text.length >= 12);
}

function pickTaskEvidence(task, memoryIndex = []) {
  const taskText = `${task?.title || ''} ${task?.description || ''} ${task?.reason || ''}`.toLowerCase();
  const terms = Array.from(new Set(taskText.split(/[^a-z0-9]+/).filter((t) => t.length >= 5))).slice(0, 10);
  if (!terms.length) return null;
  let best = null;
  let score = 0;
  for (const item of memoryIndex) {
    const hay = String(item.text || '').toLowerCase();
    const overlap = terms.filter((term) => hay.includes(term)).length;
    if (overlap > score) {
      score = overlap;
      best = item;
    }
  }
  if (!best || score < 2) return null;
  const snippet = extractConcreteFocus(best.text).slice(0, 120);
  if (!snippet) return null;
  return {
    app: best.app,
    snippet
  };
}

function ensureTaskFormatting(task) {
  const next = { ...task };
  let title = String(next.title || '').replace(/\s+/g, ' ').trim();
  if (!/^(reply|send|finish|complete|prepare|review|fix|submit|draft|schedule|confirm|update|finalize|continue|ship|close|resolve)\b/i.test(title)) {
    title = `Complete: ${title || 'the next concrete step'}`;
  }
  next.title = trimToSingleClause(title, 120).replace(/[.]+$/, '');

  const desc = trimToSingleClause(next.description || next.reason || '', 180);
  next.description = desc || `Execute one concrete step for ${next.title.toLowerCase()}.`;

  let reason = trimToSingleClause(next.reason || next.description || '', 200);
  if (!/\bbecause\b/i.test(reason)) reason = `Because recent memory shows this is still open: ${reason || next.title}.`;
  next.reason = reason;

  const currentPlan = Array.isArray(next.plan) ? next.plan : [];
  next.plan = currentPlan.length ? currentPlan.slice(0, 4).map((step) => trimToSingleClause(step, 110)).filter(Boolean) : [
    'Open the exact source context',
    'Complete one unfinished step only',
    'Confirm completion before switching'
  ];
  next.step_plan = Array.isArray(next.step_plan) && next.step_plan.length ? next.step_plan.slice(0, 4) : next.plan;
  return next;
}

function groundTaskWithMemory(task, memoryIndex = []) {
  const next = { ...task };
  const evidence = pickTaskEvidence(next, memoryIndex);
  if (!evidence) return next;
  const evidenceReason = `Because in recent ${evidence.app} activity you were working on "${evidence.snippet}" and no completion signal was captured.`;
  next.reason = evidenceReason;
  if (!next.trigger_summary) next.trigger_summary = evidenceReason;
  return next;
}

function finalizeProactiveTasks(tasks, maxTotal = MAX_PRACTICAL_SUGGESTIONS) {
  const memoryIndex = buildTaskMemoryEvidenceIndex(140);
  const normalized = (tasks || [])
    .filter(Boolean)
    .map((task) => ({
      ...task,
      category: normalizeTaskCategory(task.category)
    }))
    .map(refineTaskSpecificity)
    .map(enforceSingleFocusTask)
    .map(ensureShortPlan)
    .map((task) => groundTaskWithMemory(task, memoryIndex))
    .map(ensureTaskFormatting)
    .filter((task) => !taskLooksRecentlyCompleted(task))
    .map((task) => normalizeSuggestion(task, { now: Date.now() }));
  return rankAndLimitSuggestions(normalized, { maxTotal, maxPerCategory: 2, maxFollowups: 1, now: Date.now() });
}

function getTopDomains(browserHistory, limit = 5) {
  const domainCounts = {};
  browserHistory.forEach(item => {
    try {
      const domain = new URL(item.url).hostname;
      domainCounts[domain] = (domainCounts[domain] || 0) + 1;
    } catch (e) {}
  });

  return Object.entries(domainCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([domain, count]) => ({ domain, count }));
}

function calculateProductivityScore(browserHistory) {
  const productiveDomains = ['github.com', 'notion.so', 'docs.google.com', 'linkedin.com'];
  const distractingDomains = ['youtube.com', 'reddit.com', 'twitter.com', 'facebook.com'];

  let productive = 0;
  let distracting = 0;

  browserHistory.forEach(item => {
    try {
      const domain = new URL(item.url).hostname;
      if (productiveDomains.some(d => domain.includes(d))) {
        productive++;
      } else if (distractingDomains.some(d => domain.includes(d))) {
        distracting++;
      }
    } catch (e) {}
  });

  const total = productive + distracting;
  return total > 0 ? Math.round((productive / total) * 100) : 50;
}

// Get daily summary for IPC
ipcMain.handle('get-daily-summary', async () => {
  return await generateDailySummary();
});

function getDateKey(ts = Date.now()) {
  const d = new Date(ts);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function getDayWindow(ts = Date.now()) {
  const start = new Date(ts);
  start.setHours(0, 0, 0, 0);
  const end = new Date(start);
  end.setDate(end.getDate() + 1);
  return { start, end };
}

function getMorningBriefsStore() {
  return (store.get('morningBriefs') || []).slice(0, 60);
}

function selectQuoteForDate(dateKey) {
  const quotes = [
    'Small progress compounds into major outcomes.',
    'Discipline is choosing what matters most now.',
    'Focus is saying no to almost everything else.',
    'Consistency beats intensity when repeated daily.',
    'Clarity first, then speed.',
    'What you finish today reduces tomorrow’s stress.',
    'High-quality work starts with one committed block.',
    'Momentum grows when the next action is obvious.',
    'Preparation is a competitive advantage.',
    'Lead with action, not urgency.'
  ];
  const n = (String(dateKey || '').replace(/\D/g, '').split('').reduce((a, b) => a + Number(b || 0), 0) || 0);
  return quotes[n % quotes.length];
}

function getFocusVideoPicks(dateKey) {
  const pools = [
    [
      { title: 'Lofi Girl — lofi hip hop radio', url: 'https://www.youtube.com/watch?v=jfKfPfyJRdk' },
      { title: 'Chillhop Music — beats to study', url: 'https://www.youtube.com/watch?v=5yx6BWlEVcY' }
    ],
    [
      { title: 'Relaxing Jazz Piano Radio', url: 'https://www.youtube.com/watch?v=Dx5qFachd3A' },
      { title: 'Deep Focus — Instrumental Study Mix', url: 'https://www.youtube.com/watch?v=lFcSrYw-ARY' }
    ],
    [
      { title: 'Cafe Music BGM channel — Work Jazz', url: 'https://www.youtube.com/watch?v=6uddGul0oAc' },
      { title: 'Ambient Study Music To Concentrate', url: 'https://www.youtube.com/watch?v=WPni755-Krg' }
    ]
  ];
  const n = (String(dateKey || '').replace(/\D/g, '').split('').reduce((a, b) => a + Number(b || 0), 0) || 0);
  return pools[n % pools.length];
}

function toPriorityCandidate(item, source) {
  if (!item) return null;
  const title = String(item.title || item.summary || '').trim();
  if (!title) return null;
  const description = String(item.description || item.reason || '').trim();
  const priority = String(item.priority || 'medium').toLowerCase();
  return { title, description, priority, source, due: item.due_date || item.start_time || item.start || null };
}

function toTaskForPriority(priority, idx) {
  const fallback = [
    'Block 45 focused minutes and complete the first meaningful chunk.',
    'Send one concrete follow-up that unblocks progress.',
    'Prepare key notes before your next scheduled commitment.'
  ];
  if (!priority) return fallback[idx] || fallback[0];
  return priority.description || `Execute the next concrete step for "${priority.title}".`;
}

function safeParseJson(value, fallback = {}) {
  if (!value) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function isGenericActivityLabel(label = '') {
  return /\b(off-task browsing|low activity|activity unclear|viewing content in|viewing on-screen content|exact action unclear)\b/i.test(String(label || ''));
}

function deriveConcreteActivityFromEvent(row = {}) {
  const metadata = safeParseJson(row.metadata, {});
  const activitySummary = String(metadata.activity_summary || '').trim();
  const evidence = Array.isArray(metadata.activity_evidence) ? metadata.activity_evidence.filter(Boolean) : [];
  const cleaned = String(metadata.cleaned_capture_text || row.text || '').replace(/\s+/g, ' ').trim();
  const app = String(row.app || metadata.app || '').trim();
  const windowTitle = String(row.window_title || metadata.window_title || '').trim();

  if (activitySummary && !isGenericActivityLabel(activitySummary)) {
    return activitySummary;
  }

  const focus = cleaned
    .split(/[\n.]/)
    .map((line) => line.trim())
    .find((line) => line.length >= 16 && /\b(reply|draft|send|review|fix|debug|meeting|agenda|assignment|deadline|submit|document|report|proposal|task|research|ticket|issue)\b/i.test(line));
  if (focus) return `working on ${focus.slice(0, 130)}`;

  if (evidence.length) return evidence[0].slice(0, 130);
  if (windowTitle) return `working in ${windowTitle.slice(0, 90)}`;
  if (app) return `working in ${app.slice(0, 40)}`;
  return '';
}

async function loadMemoryTaskCandidatesForBrief(todayStartMs, limit = 24) {
  const rows = await db.allQuery(
    `SELECT id, title, summary, confidence, status, metadata, updated_at
     FROM memory_nodes
     WHERE layer = 'semantic' AND subtype = 'task' AND status NOT IN ('done', 'archived')
     ORDER BY datetime(updated_at) DESC
     LIMIT ?`,
    [limit]
  ).catch(() => []);

  return (rows || []).map((row) => {
    const metadata = safeParseJson(row.metadata, {});
    return {
      id: row.id,
      title: String(row.title || '').trim(),
      description: String(row.summary || metadata.reason || '').trim(),
      priority: Number(row.confidence || 0) >= 0.72 ? 'high' : 'medium',
      due: metadata.due_date || metadata.deadline || null,
      source: 'memory_task',
      updatedAt: parseTs(row.updated_at || 0),
      status: String(row.status || '').toLowerCase()
    };
  }).filter((item) => item.title);
}

async function loadMemoryCompletedTasksForBrief(startMs, endMs, limit = 16) {
  const startIso = new Date(startMs).toISOString();
  const endIso = new Date(endMs).toISOString();
  const rows = await db.allQuery(
    `SELECT title, summary
     FROM memory_nodes
     WHERE layer = 'semantic' AND subtype = 'task' AND status = 'done'
       AND datetime(updated_at) >= datetime(?) AND datetime(updated_at) < datetime(?)
     ORDER BY datetime(updated_at) DESC
     LIMIT ?`,
    [startIso, endIso, limit]
  ).catch(() => []);
  return (rows || [])
    .map((row) => String(row.title || row.summary || '').trim())
    .filter(Boolean);
}

async function loadMemoryActivityForBrief(startMs, endMs, limit = 60) {
  const startIso = new Date(startMs).toISOString();
  const endIso = new Date(endMs).toISOString();
  const rows = await db.allQuery(
    `SELECT timestamp, app, window_title, text, metadata
     FROM events
     WHERE datetime(timestamp) >= datetime(?) AND datetime(timestamp) < datetime(?)
       AND (LOWER(type) LIKE '%screen%' OR LOWER(source) = 'sensors' OR LOWER(source_type) LIKE '%screen%')
     ORDER BY datetime(timestamp) DESC
     LIMIT ?`,
    [startIso, endIso, limit]
  ).catch(() => []);

  const items = [];
  const seen = new Set();
  for (const row of rows || []) {
    const activity = deriveConcreteActivityFromEvent(row);
    if (!activity) continue;
    const key = activity.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const ts = parseTs(row.timestamp || 0);
    const time = ts ? new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '';
    const app = String(row.app || '').trim();
    items.push(`${time ? `${time} — ` : ''}${app ? `${app}: ` : ''}${activity}`.trim());
    if (items.length >= 8) break;
  }
  return items;
}

function formatMorningBriefMarkdown(brief) {
  const lines = [];
  lines.push(`# Morning Briefing Title: ${brief.dateLabel}`);
  lines.push('');
  lines.push('## Daily Motivation');
  lines.push(`"${brief.quote}"`);
  lines.push('');
  lines.push('## Background Productivity');
  brief.videos.forEach((video, i) => lines.push(`${i + 1}. ${video.title} — ${video.url}`));
  lines.push('');
  lines.push('## Top 3 Priorities');
  brief.priorities.forEach((p, i) => {
    lines.push(`${i + 1}. ${p.title}`);
    lines.push(`Why this helps the week: ${p.why}`);
    lines.push(`Suggested task: ${p.task}`);
    lines.push('');
  });
  lines.push('## Rollovers');
  if (brief.rollovers.length) brief.rollovers.forEach((r) => lines.push(`- ${r}`));
  else lines.push('- No major rollover tasks from yesterday.');
  lines.push('');
  lines.push('Wins from yesterday:');
  if (brief.wins.length) brief.wins.forEach((w) => lines.push(`- ${w}`));
  else lines.push('- You maintained continuity and kept progress moving.');
  lines.push('');
  lines.push('## What Actually Happened Yesterday');
  if (Array.isArray(brief.activityLog) && brief.activityLog.length) brief.activityLog.forEach((a) => lines.push(`- ${a}`));
  else lines.push('- No detailed activity trace available.');
  lines.push('');
  lines.push('## Calendar Snapshot');
  if (brief.calendar.length) brief.calendar.forEach((c) => lines.push(`- ${c}`));
  else lines.push('- No meetings scheduled today.');
  lines.push('');
  lines.push('Preparation:');
  lines.push(`- ${brief.calendarPrep}`);
  lines.push('');
  lines.push('## Leadership Mindset');
  lines.push(`- What would make today feel successful? ${brief.leadership.success}`);
  lines.push(`- Where can I show up as a leader? ${brief.leadership.leader}`);
  lines.push(`- What could be quickly addressed that I'm avoiding or procrastinating? ${brief.leadership.avoidance}`);
  lines.push('');
  lines.push('## Quick Info');
  lines.push(`- Key headline: ${brief.newsHeadline.title} — ${brief.newsHeadline.url}`);
  lines.push('Articles you may like (recent):');
  brief.articles.forEach((a) => lines.push(`- ${a.title} — ${a.url}`));
  return lines.join('\n').trim();
}

async function generateMorningBrief({ force = false, scheduled = false } = {}) {
  const now = Date.now();
  const dateKey = getDateKey(now);
  const existing = getMorningBriefsStore();
  const todayBrief = existing.find((b) => b.date === dateKey);
  if (todayBrief && !force) return todayBrief;

  const { start: todayStart } = getDayWindow(now);
  const yesterdayStart = new Date(todayStart);
  yesterdayStart.setDate(yesterdayStart.getDate() - 1);

  const persistentTodos = store.get('persistentTodos') || [];
  const openSuggestions = store.get('suggestions') || [];
  const googleData = store.get('googleData') || {};
  const completedLog = store.get('completedTasksLog') || [];
  const [memoryTaskCandidates, memoryCompletedTasks, activityLog] = await Promise.all([
    loadMemoryTaskCandidatesForBrief(todayStart.getTime(), 28),
    loadMemoryCompletedTasksForBrief(yesterdayStart.getTime(), todayStart.getTime(), 20),
    loadMemoryActivityForBrief(yesterdayStart.getTime(), todayStart.getTime(), 80)
  ]);
  const quote = selectQuoteForDate(dateKey);
  const videos = getFocusVideoPicks(dateKey);

  const prioritiesPool = [
    ...memoryTaskCandidates.map((t) => toPriorityCandidate({
      title: t.title,
      description: t.description,
      priority: t.priority,
      due_date: t.due
    }, 'memory_task')),
    ...persistentTodos.filter((t) => !t.completed).map((t) => toPriorityCandidate(t, 'todo')),
    ...openSuggestions.map((s) => toPriorityCandidate(s, 'suggestion'))
  ].filter(Boolean);

  const priorities = prioritiesPool
    .sort((a, b) => {
      const weight = { high: 3, medium: 2, low: 1 };
      const p = (weight[b.priority] || 0) - (weight[a.priority] || 0);
      if (p !== 0) return p;
      return parseTs(a.due) - parseTs(b.due);
    })
    .slice(0, 3)
    .map((p, i) => ({
      title: p.title,
      why: i === 0
        ? 'Finishing this early sets the tone and reduces downstream pressure.'
        : i === 1
          ? 'This keeps your key projects moving without context-switch drag.'
          : 'Closing this now protects focus later in the week.',
      task: toTaskForPriority(p, i)
    }));

  while (priorities.length < 3) {
    const i = priorities.length;
    const fallbacks = [
      'Deliver one high-impact block before noon',
      'Unblock one dependency with a concrete follow-up',
      'Prepare one upcoming commitment in advance'
    ];
    priorities.push({
      title: fallbacks[i],
      why: 'This keeps your week structured and proactive.',
      task: toTaskForPriority(null, i)
    });
  }

  const rollovers = persistentTodos
    .filter((t) => !t.completed && parseTs(t.createdAt) < todayStart.getTime())
    .slice(0, 6)
    .map((t) => `${t.title}${t.reason ? ` — ${t.reason}` : ''}`);
  memoryTaskCandidates
    .filter((t) => t.updatedAt && t.updatedAt < todayStart.getTime())
    .slice(0, 4)
    .forEach((task) => {
      const label = `${task.title}${task.description ? ` — ${task.description}` : ''}`;
      if (!rollovers.some((r) => r.toLowerCase().includes(task.title.toLowerCase()))) {
        rollovers.push(label.slice(0, 200));
      }
    });

  const wins = completedLog
    .filter((w) => {
      const ts = parseTs(w.completedAt);
      return ts >= yesterdayStart.getTime() && ts < todayStart.getTime();
    })
    .slice(0, 3)
    .map((w) => w.title || 'Completed a planned task');
  memoryCompletedTasks.slice(0, 4).forEach((title) => {
    if (!wins.some((w) => String(w || '').toLowerCase() === String(title || '').toLowerCase())) {
      wins.push(title);
    }
  });

  const calendar = (googleData.calendar || [])
    .map((ev) => ({
      title: ev.summary || ev.title || 'Meeting',
      start: parseTs(ev.start_time || ev.start),
      attendees: Array.isArray(ev.attendees) ? ev.attendees.length : 0
    }))
    .filter((ev) => ev.start >= todayStart.getTime() && ev.start < (todayStart.getTime() + 24 * 60 * 60 * 1000))
    .sort((a, b) => a.start - b.start)
    .slice(0, 6)
    .map((ev) => `${new Date(ev.start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} — ${ev.title}${ev.attendees ? ` (${ev.attendees} attendees)` : ''}`);

  const calendarPrep = calendar.length
    ? 'Review your first meeting agenda and prep one decision or update before it starts.'
    : 'Use the first 90 minutes as a protected deep-work block.';

  const leadership = {
    success: 'Finish your first priority and leave clear momentum for the second one.',
    leader: 'Send one proactive update before someone has to ask for status.',
    avoidance: rollovers[0] ? `Start the first 10-minute step of: ${rollovers[0].slice(0, 90)}.` : 'Clear one delayed follow-up in the next 30 minutes.'
  };

  let mappedNews = [];
  const liveFeedRows = await fetchQuickLinksFromFeeds();
  if (liveFeedRows.length) {
    mappedNews = liveFeedRows.map((row) => ({
      title: `${row.title} (${row.source})`.slice(0, 160),
      url: row.url || 'https://duckduckgo.com',
      snippet: row.published_at || row.source || ''
    }));
  } else {
    const newsResults = await searchFreeWeb(`positive business technology headline ${dateKey}`, 6);
    mappedNews = (newsResults || []).map((row) => ({
      title: (row.name || row.snippet || 'Article').slice(0, 140),
      url: row.url || 'https://duckduckgo.com',
      snippet: row.snippet || ''
    }));
  }
  const newsHeadline = mappedNews[0] || { title: 'No headline available yet', url: 'https://duckduckgo.com' };
  const articles = mappedNews.slice(1, 4).length
    ? mappedNews.slice(1, 4)
    : [
        { title: 'Curated article slot 1', url: 'https://duckduckgo.com/?q=productivity+leadership+this+week' },
        { title: 'Curated article slot 2', url: 'https://duckduckgo.com/?q=positive+business+news+this+week' },
        { title: 'Curated article slot 3', url: 'https://duckduckgo.com/?q=technology+breakthrough+this+week' }
      ];

  const dateLabel = new Date(now).toLocaleDateString(undefined, {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });

  const briefData = {
    id: `brief_${dateKey}`,
    date: dateKey,
    dateLabel,
    createdAt: now,
    quote,
    videos,
    priorities,
    rollovers,
    wins,
    activityLog,
    calendar,
    calendarPrep,
    leadership,
    newsHeadline,
    articles,
    scheduled: Boolean(scheduled)
  };

  const content = formatMorningBriefMarkdown(briefData);
  const next = [{ ...briefData, content }, ...existing.filter((b) => b.date !== dateKey)].slice(0, 60);
  store.set('morningBriefs', next);

  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('morning-brief-updated', next[0]);
  }
  return next[0];
}

async function callDeepSeek(prompt, apiKey, temperature = 0.1) {
  const parsed = await callLLM(prompt, apiKey, temperature);
  return Array.isArray(parsed) ? parsed : [];
}

function normalizeProfileField(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value && typeof value === 'object') {
    return Object.entries(value)
      .flatMap(([key, inner]) => {
        if (Array.isArray(inner)) return inner.map(item => `${key}: ${item}`);
        if (inner && typeof inner === 'object') return Object.keys(inner).map(subKey => `${key}.${subKey}`);
        if (typeof inner === 'string' || typeof inner === 'number' || typeof inner === 'boolean') return [`${key}: ${inner}`];
        return [key];
      })
      .filter(Boolean);
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return [String(value)];
  return [];
}

function buildDetailedGlobalCoreMemory({ userProfile = {}, dailySummary = {}, historicalSummaries = {}, suggestions = [], todos = [] }) {
  const patterns = normalizeProfileField(userProfile.patterns).slice(0, 20);
  const preferences = normalizeProfileField(userProfile.preferences || userProfile.deep_preferences).slice(0, 20);
  const clusters = normalizeProfileField(userProfile.top_intent_clusters).slice(0, 12);
  const recentDays = Object.values(historicalSummaries || {})
    .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')))
    .slice(0, 10);
  const activeTodos = (todos || []).filter(t => !t.completed).slice(0, 12);
  const topSuggestions = (suggestions || []).slice(0, 10);

  const summariesText = recentDays.map(s => `${s.date}: ${s.narrative || ''}`).join(' | ');
  const todoText = activeTodos.map(t => `${t.title} (${t.priority || 'medium'})`).join('; ');
  const suggestionText = topSuggestions.map(s => `${s.title || s.text}`).join('; ');

  return [
    `Identity Context: ${userProfile.name || 'User'} with evolving work and personal priorities.`,
    `Current Focus: ${dailySummary.narrative || 'No daily narrative yet.'}`,
    `Behavioral Patterns: ${patterns.join(', ') || 'Patterns still building from event history.'}`,
    `Preferences: ${preferences.join(', ') || 'No explicit preferences extracted yet.'}`,
    `Intent Clusters: ${clusters.join(', ') || 'Cluster model still warming up.'}`,
    `Active Commitments: ${todoText || 'No open commitments in persistent tasks.'}`,
    `Recommended Actions: ${suggestionText || 'No ranked suggestions currently available.'}`,
    `Recent Timeline (last 10 days): ${summariesText || 'No historical summaries available yet.'}`,
    `Task Guidance: Prioritize commitments that are calendar-bound within 48 hours, then follow-up obligations, then deep work blocks.`,
    `Execution Policy: Prefer concrete next actions that can be automated safely through browser-agent flows when a deeplink/action plan exists.`
  ].join('\n');
}

function persistGlobalCoreMemory(coreNarrative) {
  const cleaned = (coreNarrative || '').toString().trim();
  const core = cleaned || 'Core memory is initializing.';

  // Keep one canonical core node in sqlite graph.
  db.runQuery(`DELETE FROM nodes WHERE id = 'core_global'`).catch(() => {});
  db.runQuery(
    `INSERT OR REPLACE INTO nodes (id, type, data, embedding) VALUES ('global_core', 'core', ?, '[]')`,
    [JSON.stringify({ title: 'Global Core Memory', narrative: core, updated_at: new Date().toISOString() })]
  ).catch(() => {});

  // Keep one canonical core node in legacy store graph.
  const { nodes, edges } = getGraph();
  const filteredNodes = (nodes || []).filter(n => n.id !== 'core_global' && n.id !== 'global_core');
  filteredNodes.push({ id: 'global_core', type: 'global_core', data: { title: 'Global Core Memory', narrative: core, updated_at: new Date().toISOString() } });
  const filteredEdges = (edges || []).map(edge => ({
    ...edge,
    from: edge.from === 'core_global' ? 'global_core' : edge.from,
    to: edge.to === 'core_global' ? 'global_core' : edge.to
  }));
  saveGraph(filteredNodes, filteredEdges);

  store.set('proactiveMemory', { ...(store.get('proactiveMemory') || {}), core });
  return core;
}

function buildAssistantContext(query, graphNodes) {
  const dailySummary = store.get('dailySummary') || {};
  const userProfile = store.get('userProfile') || {};
  const proactiveMemory = store.get('proactiveMemory') || {};
  const suggestions = (store.get('suggestions') || []).slice(0, 8);
  const todos = (store.get('persistentTodos') || []).filter(t => !t.completed).slice(0, 8);
  const sensorEvents = getSensorEvents().slice(0, 5);
  const historicalSummaries = store.get('historicalSummaries') || {};
  const recentHistorical = Object.keys(historicalSummaries)
    .sort((a, b) => b.localeCompare(a))
    .slice(0, 5)
    .map(date => historicalSummaries[date]);

  const matchedNodes = (graphNodes || []).slice(0, 10).map(n =>
    `[${n.type}] ${n.id}: ${JSON.stringify(n.data).slice(0, 300)}`
  ).join('\n');

  const summaryText = dailySummary?.narrative || 'No summary available.';
  const summaryTasks = (dailySummary?.tasks || []).slice(0, 5).map(t => `- ${t.title} (${t.priority || 'medium'})`).join('\n');
  const suggestionText = suggestions.map(s => `- ${s.title}: ${s.reason || s.description || ''}`).join('\n');
  const todoText = todos.map(t => `- ${t.title}: ${t.description || t.reason || ''}`).join('\n');
  const sensorText = sensorEvents.map(event =>
    `- ${event.activeApp || event.sourceName || 'Screen'}${event.activeWindowTitle ? ` / ${event.activeWindowTitle}` : ''}: ${(event.text || '').slice(0, 180)}`
  ).join('\n');
  const historicalText = recentHistorical.map(s =>
    `- ${s.date}: ${s.narrative || ''}`
  ).join('\n');
  const profilePatterns = normalizeProfileField(userProfile.patterns);
  const profilePreferences = normalizeProfileField(userProfile.preferences || userProfile.deep_preferences);
  const profileIntentClusters = normalizeProfileField(userProfile.top_intent_clusters);

  return `
USER QUERY:
${query}

TODAY SUMMARY:
${summaryText}

CURRENT TASKS:
${summaryTasks || 'None'}

OPEN SUGGESTIONS:
${suggestionText || 'None'}

PERSISTENT TODOS:
${todoText || 'None'}

RECENT SENSOR CAPTURES:
${sensorText || 'None'}

RECENT HISTORICAL SUMMARIES:
${historicalText || 'None'}

USER PROFILE:
Patterns: ${profilePatterns.slice(0, 8).join(', ') || 'None'}
Preferences: ${profilePreferences.slice(0, 8).join(', ') || 'None'}
Top intent clusters: ${profileIntentClusters.slice(0, 8).join(', ') || 'None'}

GLOBAL CORE:
${(proactiveMemory.core || '').slice(0, 2000)}

MATCHED GRAPH NODES:
${matchedNodes || 'None'}
`.trim();
}

function normalizeQueryTerms(query) {
  return (query || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .map(term => term.trim())
    .filter(term => term.length >= 3);
}

function parseTemporalWindowFromQuery(query) {
  const lower = (query || '').toLowerCase();
  const now = new Date();
  const startOfDay = (date) => new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
  const endOfDay = (date) => new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);

  if (/\btoday\b/.test(lower)) {
    const start = startOfDay(now);
    return { label: 'today', start, end: endOfDay(now) };
  }

  if (/\byesterday\b/.test(lower)) {
    const y = new Date(now);
    y.setDate(y.getDate() - 1);
    return { label: 'yesterday', start: startOfDay(y), end: endOfDay(y) };
  }

  const lastDays = lower.match(/\blast\s+(\d+)\s+days?\b/);
  if (lastDays) {
    const days = Math.max(1, Math.min(30, parseInt(lastDays[1], 10) || 1));
    const start = startOfDay(new Date(now.getFullYear(), now.getMonth(), now.getDate() - (days - 1)));
    return { label: `last_${days}_days`, start, end: endOfDay(now) };
  }

  const isoDate = lower.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (isoDate) {
    const day = new Date(`${isoDate[1]}T00:00:00`);
    if (!Number.isNaN(day.getTime())) {
      return { label: isoDate[1], start: startOfDay(day), end: endOfDay(day) };
    }
  }

  return null;
}

function parseTs(value) {
  if (value === null || value === undefined) return 0;
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return n;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}

function inWindow(ts, window) {
  if (!window) return true;
  const ms = parseTs(ts);
  if (!ms) return false;
  return ms >= window.start.getTime() && ms <= window.end.getTime();
}

function queryRequestsWebSearch(query) {
  const lower = (query || '').toLowerCase();
  return /\b(web|internet|online|search|look up|latest|news|current)\b/.test(lower);
}

function decodeHtmlEntities(text) {
  return String(text || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#x2F;/g, '/');
}

function normalizeDDGResultUrl(url) {
  const raw = String(url || '');
  if (!raw) return '';
  try {
    const parsed = new URL(raw, 'https://duckduckgo.com');
    const uddg = parsed.searchParams.get('uddg');
    if (uddg) return decodeURIComponent(uddg);
    return parsed.href;
  } catch (_) {
    return raw;
  }
}

async function decideAssistantRetrieval(query, apiKey, temporalWindow) {
  const fallback = {
    use_web: queryRequestsWebSearch(query),
    use_memory: true,
    reason: 'heuristic_default'
  };

  const lower = (query || '').toLowerCase();
  if (/^\s*(web|internet|search)\b/.test(lower)) {
    fallback.use_web = true;
  }
  if (/^\s*(web|internet)\s+only\b/.test(lower) || /\bjust search\b/.test(lower)) {
    fallback.use_web = true;
    fallback.use_memory = false;
    fallback.reason = 'heuristic_web_only';
  }
  if (temporalWindow) {
    fallback.use_memory = true;
  }

  try {
    const parsed = await callLLM(`Decide retrieval strategy for this query.\nQuery: ${query}\nReturn ONLY JSON: {"use_web":boolean,"use_memory":boolean,"reason":"short"}.`, apiKey, 0);
    const strategy = {
      use_web: Boolean(parsed?.use_web),
      use_memory: Boolean(parsed?.use_memory),
      reason: String(parsed?.reason || 'llm_decision')
    };
    if (temporalWindow) strategy.use_memory = true;
    if (!strategy.use_web && !strategy.use_memory) strategy.use_memory = true;
    return strategy;
  } catch (_) {
    return fallback;
  }
}

async function searchFreeWeb(query, count = 5) {
  if (!query) return [];
  try {
    const size = Math.max(1, Math.min(10, count));
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await fetch(ddgUrl);
    if (!response.ok) return [];
    const data = await response.json();

    const rows = [];
    if (data?.AbstractURL) {
      rows.push({
        name: data?.Heading || 'DuckDuckGo',
        url: data.AbstractURL,
        snippet: data.AbstractText || ''
      });
    }

    const related = Array.isArray(data?.RelatedTopics) ? data.RelatedTopics : [];
    related.forEach((topic) => {
      if (rows.length >= size) return;
      if (topic?.FirstURL && topic?.Text) {
        rows.push({ name: topic.Text.slice(0, 90), url: topic.FirstURL, snippet: topic.Text });
      } else if (Array.isArray(topic?.Topics)) {
        topic.Topics.forEach((inner) => {
          if (rows.length >= size) return;
          if (inner?.FirstURL && inner?.Text) {
            rows.push({ name: inner.Text.slice(0, 90), url: inner.FirstURL, snippet: inner.Text });
          }
        });
      }
    });

    if (rows.length >= Math.min(2, size)) return rows.slice(0, size);

    // Fallback: parse HTML results page when instant-answer is sparse.
    const htmlResp = await fetch(`https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`);
    if (!htmlResp.ok) return rows.slice(0, size);
    const html = await htmlResp.text();
    const matches = [...html.matchAll(/<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)];
    for (const match of matches) {
      if (rows.length >= size) break;
      const href = normalizeDDGResultUrl(decodeHtmlEntities(match[1] || ''));
      const title = decodeHtmlEntities((match[2] || '').replace(/<[^>]+>/g, ' ').trim());
      if (!href) continue;
      rows.push({
        name: title || 'DuckDuckGo Result',
        url: href,
        snippet: title
      });
    }
    return rows.slice(0, size);
  } catch (e) {
    console.warn('[FreeWebSearch] failed:', e?.message || e);
    return [];
  }
}

function decodeXmlEntities(text) {
  return String(text || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function stripHtmlTags(text) {
  return decodeXmlEntities(String(text || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim());
}

function extractXmlTag(xml, tag) {
  const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i');
  const match = String(xml || '').match(regex);
  if (!match) return '';
  return stripHtmlTags(match[1]);
}

function parseRssItems(xml, sourceName, maxItems = 4) {
  const rows = [];
  const blocks = [...String(xml || '').matchAll(/<item\b[\s\S]*?<\/item>/gi)].map((m) => m[0]);
  for (const block of blocks) {
    const title = extractXmlTag(block, 'title');
    const link = extractXmlTag(block, 'link');
    const pubDate = extractXmlTag(block, 'pubDate');
    if (!title || !link) continue;
    rows.push({
      title: title.slice(0, 160),
      url: link,
      source: sourceName,
      published_at: pubDate
    });
    if (rows.length >= maxItems) break;
  }
  return rows;
}

async function fetchQuickLinksFromFeeds() {
  const feeds = [
    { name: 'Reuters World', url: 'https://feeds.reuters.com/Reuters/worldNews' },
    { name: 'Reuters Business', url: 'https://feeds.reuters.com/reuters/businessNews' },
    { name: 'BBC World', url: 'https://feeds.bbci.co.uk/news/world/rss.xml' },
    { name: 'NYT Home', url: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml' },
    { name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml' }
  ];

  const combined = [];
  for (const feed of feeds) {
    try {
      const res = await fetch(feed.url);
      if (!res.ok) continue;
      const xml = await res.text();
      combined.push(...parseRssItems(xml, feed.name, 3));
    } catch (_) {}
  }

  const seen = new Set();
  const deduped = [];
  for (const row of combined) {
    const key = `${row.url}|${row.title}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(row);
    if (deduped.length >= 12) break;
  }
  return deduped;
}

async function ensureSummaryChunksIndexed() {
  try {
    const existing = await db.getQuery(`SELECT COUNT(*) AS count FROM text_chunks WHERE data_source = 'summaries'`);
    const existingCount = Number(existing?.count || 0);
    if (existingCount >= 40) return;

    const historicalSummaries = store.get('historicalSummaries') || {};
    const rows = Object.values(historicalSummaries)
      .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')))
      .slice(0, 180);
    if (!rows.length) return;

    await db.runQuery(`DELETE FROM text_chunks WHERE data_source = 'summaries'`).catch(() => {});

    for (const summary of rows) {
      const date = summary?.date || new Date().toISOString().slice(0, 10);
      const text = [
        `Date: ${date}`,
        `Narrative: ${summary?.narrative || ''}`,
        `Top people: ${(summary?.top_people || []).join(', ')}`,
        `Topics: ${(summary?.topics || []).join(', ')}`,
        `Intent clusters: ${(summary?.intent_clusters || []).join(', ')}`
      ].join('\n').trim();
      if (!text || text.length < 20) continue;

      const emb = await generateEmbedding(text, process.env.OPENAI_API_KEY);
      const chunkId = `sum_${String(date).replace(/[^a-zA-Z0-9]/g, '')}_0`;
      await db.runQuery(
        `INSERT OR REPLACE INTO text_chunks
         (id, event_id, node_id, chunk_index, text, embedding, timestamp, date, app, data_source, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          chunkId,
          null,
          'global_core',
          0,
          text.slice(0, 5000),
          JSON.stringify(emb || []),
          `${date}T12:00:00.000Z`,
          date,
          'Summary',
          'summaries',
          JSON.stringify({ summary_date: date })
        ]
      );
    }
  } catch (e) {
    console.warn('[SummaryChunkIndex] Failed to ensure summary chunks:', e?.message || e);
  }
}

function scoreTextAgainstTerms(text, terms) {
  if (!text || !terms.length) return 0;
  const lower = text.toLowerCase();
  return terms.reduce((score, term) => score + (lower.includes(term) ? 1 : 0), 0);
}

function buildRetrievalPlan(query) {
  const lower = (query || '').toLowerCase();
  const vague = lower.length < 20 || /\b(help|what should i do|what now|anything important)\b/.test(lower);
  const asksWhy = /\bwhy|because|reason\b/.test(lower);
  const asksToday = /\btoday|now|current|recent|lately\b/.test(lower);
  const asksMemory = /\bremember|history|pattern|usually|semantics|episode|core\b/.test(lower) || vague;
  const asksPeople = /\bwho|person|people|relationship|contact|john|maria|name\b/.test(lower);
  const asksWork = /\bproject|work|doc|draft|deadline|task|todo|meeting|email\b/.test(lower);

  const plan = [
    { source: 'daily_summary', budget: asksToday ? 1 : 0, reason: 'Current day state and active tasks.' },
    { source: 'open_suggestions', budget: asksWhy || asksToday || vague ? 6 : 3, reason: 'Current suggested actions and rationale.' },
    { source: 'persistent_todos', budget: asksToday || asksWork ? 6 : 3, reason: 'Outstanding manually tracked commitments.' },
    { source: 'sensor_captures', budget: asksToday || asksWork ? 5 : 2, reason: 'Recent on-screen work context.' },
    { source: 'historical_summaries', budget: asksMemory || asksPeople ? 5 : 2, reason: 'Recent history and narrative continuity.' },
    { source: 'graph_core', budget: asksMemory || vague ? 3 : 1, reason: 'Core memory and app cores.' },
    { source: 'graph_semantics', budget: asksPeople || asksWork || asksMemory ? 8 : 3, reason: 'Facts and durable associations.' },
    { source: 'graph_episodes', budget: asksPeople || asksWork || asksToday ? 8 : 3, reason: 'Concrete evidence and past events.' },
    { source: 'graph_tasks', budget: asksWhy || asksWork ? 8 : 3, reason: 'Task graph and linked obligations.' }
  ].filter(item => item.budget > 0);

  return {
    terms: normalizeQueryTerms(query),
    sources: plan,
    mode: vague ? 'exploratory' : 'targeted'
  };
}

function retrieveGraphNodesByType(nodes, type, terms, limit) {
  return (nodes || [])
    .filter(node => node.type === type)
    .map(node => {
      const serialized = JSON.stringify(node.data || {});
      return { node, score: scoreTextAgainstTerms(`${node.id} ${serialized}`, terms) };
    })
    .filter(entry => entry.score > 0 || !terms.length)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map(entry => entry.node);
}

function traverseGraphFromCore(query, limit = 14, maxDepth = 4) {
  const { nodes, edges } = getGraph();
  const terms = normalizeQueryTerms(query);
  const nodeById = new Map((nodes || []).map(node => [node.id, node]));
  const startNodes = (nodes || []).filter(node => node.type === 'global_core' || node.type === 'app_core');
  const queue = startNodes.map(node => ({ id: node.id, depth: 0, path: [node.id] }));
  const visited = new Set(queue.map(item => item.id));
  const matches = [];
  const trace = [`Start from core nodes: ${startNodes.map(node => node.id).join(', ') || 'none'}`];

  while (queue.length && matches.length < limit) {
    const current = queue.shift();
    const node = nodeById.get(current.id);
    if (!node) continue;

    const serialized = JSON.stringify(node.data || {});
    const score = scoreTextAgainstTerms(`${node.id} ${serialized}`, terms);
    if (score > 0 || (terms.length === 0 && current.depth <= 1)) {
      matches.push({
        node,
        depth: current.depth,
        path: [...current.path],
        score
      });
      let printableType = node.type;
      if (node.type === 'global_core' || node.type === 'app_core') printableType = 'Core';
      if (node.type === 'insight') printableType = 'Insight';
      if (node.type === 'semantic') printableType = 'Semantic';
      if (node.type === 'episode') printableType = 'Episode';
      if (node.type === 'raw') printableType = 'Raw Data';

      trace.push(`Traversed to ${printableType} (depth ${current.depth}): Found contextual node ${node.id}`);
    }

    if (current.depth >= maxDepth) continue;

    edges
      .filter(edge => edge.from === current.id || edge.to === current.id)
      .forEach(edge => {
        const nextId = edge.from === current.id ? edge.to : edge.from;
        if (visited.has(nextId)) return;
        visited.add(nextId);
        queue.push({ id: nextId, depth: current.depth + 1, path: [...current.path, nextId] });
      });
  }

  return {
    matches,
    trace
  };
}

function buildChatRetrievalContext(query) {
  const { nodes } = getGraph();
  const plan = buildRetrievalPlan(query);
  const historicalSummaries = store.get('historicalSummaries') || {};
  const dailySummary = store.get('dailySummary') || {};
  const suggestions = (store.get('suggestions') || []).slice(0, 20);
  const todos = (store.get('persistentTodos') || []).filter(t => !t.completed).slice(0, 20);
  const sensorEvents = getSensorEvents().slice(0, 20);
  const proactiveMemory = store.get('proactiveMemory') || {};
  const userProfile = store.get('userProfile') || {};
  const profilePatterns = normalizeProfileField(userProfile.patterns);
  const profilePreferences = normalizeProfileField(userProfile.preferences || userProfile.deep_preferences);
  const graphTraversal = traverseGraphFromCore(query);

  const recentHistorical = Object.keys(historicalSummaries)
    .sort((a, b) => b.localeCompare(a))
    .map(date => historicalSummaries[date]);

  const sections = [];
  const usedSources = [];

  for (const item of plan.sources) {
    if (item.source === 'daily_summary' && dailySummary?.narrative) {
      usedSources.push(item.source);
      sections.push(`DAILY SUMMARY:\n${dailySummary.narrative}\nTasks:\n${(dailySummary.tasks || []).slice(0, item.budget).map(t => `- ${t.title}: ${t.reason || t.description || ''}`).join('\n') || 'None'}`);
    }

    if (item.source === 'open_suggestions') {
      const rows = suggestions
        .map(s => ({ value: s, score: scoreTextAgainstTerms(`${s.title} ${s.reason || ''} ${s.description || ''}`, plan.terms) }))
        .sort((a, b) => b.score - a.score)
        .slice(0, item.budget)
        .map(entry => entry.value);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`OPEN SUGGESTIONS:\n${rows.map(s => `- ${s.title}: ${s.reason || s.description || ''}`).join('\n')}`);
      }
    }

    if (item.source === 'persistent_todos') {
      const rows = todos
        .map(t => ({ value: t, score: scoreTextAgainstTerms(`${t.title} ${t.description || ''} ${t.reason || ''}`, plan.terms) }))
        .sort((a, b) => b.score - a.score)
        .slice(0, item.budget)
        .map(entry => entry.value);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`PERSISTENT TODOS:\n${rows.map(t => `- ${t.title}: ${t.description || t.reason || ''}`).join('\n')}`);
      }
    }

    if (item.source === 'sensor_captures') {
      const rows = sensorEvents
        .map(event => ({
          value: event,
          score: scoreTextAgainstTerms(`${event.activeApp || ''} ${event.activeWindowTitle || ''} ${event.text || ''}`, plan.terms)
        }))
        .sort((a, b) => b.score - a.score)
        .slice(0, item.budget)
        .map(entry => entry.value)
        .filter(event => (event.text || '').trim());
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`RECENT SENSOR CAPTURES:\n${rows.map(event => `- ${event.activeApp || event.sourceName || 'Screen'}${event.activeWindowTitle ? ` / ${event.activeWindowTitle}` : ''}: ${(event.text || '').slice(0, 220)}`).join('\n')}`);
      }
    }

    if (item.source === 'historical_summaries') {
      const rows = recentHistorical
        .map(summary => ({ value: summary, score: scoreTextAgainstTerms(`${summary.date} ${summary.narrative || ''} ${(summary.top_people || []).join(' ')}`, plan.terms) }))
        .sort((a, b) => b.score - a.score)
        .slice(0, item.budget)
        .map(entry => entry.value);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`HISTORICAL SUMMARIES:\n${rows.map(summary => `- ${summary.date}: ${summary.narrative || ''}`).join('\n')}`);
      }
    }

    if (item.source === 'graph_core') {
      const coreNodes = [
        ...retrieveGraphNodesByType(nodes, 'global_core', plan.terms, 1),
        ...retrieveGraphNodesByType(nodes, 'app_core', plan.terms, Math.max(1, item.budget - 1))
      ];
      if (coreNodes.length || proactiveMemory.core) {
        usedSources.push(item.source);
        const coreLines = coreNodes.map(node => `- [${node.type}] ${node.data?.title || node.id}: ${(node.data?.narrative || '').slice(0, 400)}`);
        if (proactiveMemory.core) coreLines.unshift(`- [global_core] Core Memory: ${(proactiveMemory.core || '').slice(0, 500)}`);
        sections.push(`CORE MEMORY:\n${coreLines.slice(0, item.budget).join('\n')}`);
      }
    }

    if (item.source === 'graph_semantics') {
      const rows = graphTraversal.matches
        .map(match => match.node)
        .filter(node => node.type === 'semantic')
        .slice(0, item.budget);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`SEMANTIC FACTS:\n${rows.map(node => `- ${node.data?.fact || JSON.stringify(node.data || {})}`).join('\n')}`);
      }
    }

    if (item.source === 'graph_episodes') {
      const rows = graphTraversal.matches
        .map(match => match.node)
        .filter(node => node.type === 'episode')
        .slice(0, item.budget);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`EPISODES:\n${rows.map(node => `- ${node.data?.title || node.id}: ${(node.data?.narrative || '').slice(0, 260)}`).join('\n')}`);
      }
    }

    if (item.source === 'graph_tasks') {
      const rows = graphTraversal.matches
        .map(match => match.node)
        .filter(node => node.type === 'task')
        .slice(0, item.budget);
      if (rows.length) {
        usedSources.push(item.source);
        sections.push(`GRAPH TASKS:\n${rows.map(node => `- ${node.data?.description || node.data?.title || node.id}`).join('\n')}`);
      }
    }
  }

  sections.push(`USER PROFILE:\n- Patterns: ${profilePatterns.slice(0, 8).join(', ') || 'None'}\n- Preferences: ${profilePreferences.slice(0, 8).join(', ') || 'None'}`);

  return {
    plan,
    usedSources,
    contextText: sections.join('\n\n'),
    trace: [
      `Mode: ${plan.mode}`,
      ...plan.sources.map(source => `Check ${source.source} with budget ${source.budget}: ${source.reason}`),
      ...graphTraversal.trace.slice(0, 10)
    ],
    graphPaths: graphTraversal.matches.slice(0, 8).map(match => ({
      nodeId: match.node.id,
      nodeType: match.node.type,
      depth: match.depth,
      path: match.path
    }))
  };
}

// ── Graph-Based Search ───────────────────────────────────────────────────
ipcMain.handle('search-graph', async (event, query, filters = {}) => {
  try {
    const q = `%${query || ''}%`;
    const targetLayer = filters.layer || null;

    console.log('[search-graph] Query:', query, 'Filters:', filters);

    // Check database state
    const nodeCount = await db.getQuery(`SELECT COUNT(*) as count FROM nodes`);
    const eventCount = await db.getQuery(`SELECT COUNT(*) as count FROM events`).catch(() => ({ count: 0 }));
    console.log('[search-graph] Database state:', { nodes: nodeCount.count, events: eventCount.count });

    let results = [];

    // 1. Search semantic nodes (Facts, Episodes, Insights)
    if (!targetLayer || targetLayer === 'Graph') {
      const nodeRows = await db.allQuery(
        `SELECT * FROM nodes WHERE (data LIKE ? OR id LIKE ?) LIMIT 100`,
        [q, q]
      );
      console.log('[search-graph] Found nodes:', nodeRows.length);
      results.push(...nodeRows.map(r => {
        const data = JSON.parse(r.data);
        return {
          ...data,
          id: r.id,
          node_type: r.type,
          layer: 'Graph',
          _raw: r.data,
          // Extract normalized fields for the UI
          timestamp: data.timestamp || data.start || data.date || null,
          title: data.title || data.fact || data.insight || data.name || `Memory ${r.type}`,
          narrative: data.narrative || data.summary || data.context || data.fact || ''
        };
      }));
    }

    // 2. Search raw events (L1 - "blog data")
    if (!targetLayer || targetLayer === 'Raw Data') {
      const eventRows = await db.allQuery(
        `SELECT * FROM events WHERE (text LIKE ? OR type LIKE ? OR metadata LIKE ?) LIMIT 100`,
        [q, q, q]
      );
      console.log('[search-graph] Found events:', eventRows.length);
      results.push(...eventRows.map(r => ({
        title: `Raw ${r.type} event`,
        narrative: r.text || 'No description',
        id: r.id,
        node_type: 'raw_event',
        layer: 'Raw Data',
        timestamp: r.timestamp,
        _raw: JSON.stringify({ metadata: JSON.parse(r.metadata), text: r.text })
      })));
    }

    // Sort/Normalize results
    results = results.sort((a,b) => (b.timestamp || 0) - (a.timestamp || 0));

    // Filter by node_type if specified in sub-filters
    if (filters.nodeType) {
      const beforeFilter = results.length;
      results = results.filter(r => r.node_type === filters.nodeType);
      console.log('[search-graph] Filtered by nodeType', filters.nodeType, 'from', beforeFilter, 'to', results.length);
    }

    console.log('[search-graph] Final results:', results.length);
    return results;
  } catch (e) {
    console.error('Failed to search graph:', e);
    return [];
  }
});

// Update profile handler to include rich memory
ipcMain.handle('get-user-profile', () => {
  const profile = store.get('userProfile') || {};
  const mem = store.get('proactiveMemory') || { core: '' };
  return { ...profile, proactiveMemory: mem };
});

async function persistChatTurnAsRawEvent({ chatSessionId, role, content, chatHistory = [], retrieval = null }) {
  const text = String(content || '').trim();
  if (!text) return;
  try {
    
    const now = new Date().toISOString();
    const historyWindow = Array.isArray(chatHistory) ? chatHistory.slice(-12) : [];
    const retrievalSnapshot = retrieval ? {
      used_sources: Array.isArray(retrieval.usedSources) ? retrieval.usedSources.slice(0, 8) : [],
      seed_count: Number(retrieval?.seed_nodes?.length || 0),
      evidence_count: Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0),
      strategy_mode: retrieval?.retrieval_plan?.strategy_mode || retrieval?.strategy?.strategy_mode || null
    } : null;
    await ingestRawEvent({
      type: 'chat_message',
      timestamp: now,
      source: 'chat',
      text,
      metadata: {
        id: `chat_${chatSessionId || 'session'}_${role}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        source_type: 'chat_message',
        app: 'Weave Chat',
        title: role === 'assistant' ? 'Assistant reply' : 'User message',
        role,
        chat_session_id: chatSessionId || null,
        chat_history_window: historyWindow,
        retrieval_snapshot: retrievalSnapshot,
        memory_layer: 'raw_data',
        event_kind: 'chat_turn'
      }
    });
  } catch (error) {
    console.warn('[chat-memory] Failed to persist chat turn:', error?.message || error);
  }
}

function safeParseJson(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function chatMessageId(sessionId, msg = {}, index = 0) {
  const basis = [
    sessionId || 'session',
    msg.role || 'user',
    Number(msg.ts || 0),
    String(msg.content || '').slice(0, 400),
    index
  ].join('|');
  return `chatmsg_${crypto.createHash('sha1').update(basis).digest('hex').slice(0, 18)}`;
}

function normalizeChatSessionSnapshot(sessions = []) {
  if (!Array.isArray(sessions)) return [];
  return sessions
    .filter((session) => session && session.id)
    .map((session) => ({
      id: String(session.id),
      title: String(session.title || 'New chat').slice(0, 180),
      createdAt: Number(session.createdAt || Date.now()),
      updatedAt: Number(session.updatedAt || session.createdAt || Date.now()),
      messages: Array.isArray(session.messages)
        ? session.messages
            .filter((msg) => msg && typeof msg.content === 'string')
            .map((msg) => ({
              role: msg.role === 'assistant' ? 'assistant' : 'user',
              content: String(msg.content || '').slice(0, 32000),
              retrieval: msg.retrieval || null,
              thinking_trace: msg.thinking_trace || null,
              ts: Number(msg.ts || Date.now())
            }))
        : []
    }));
}

async function saveChatSessionsToDb(sessions = []) {
  const normalized = normalizeChatSessionSnapshot(sessions);
  const keepIds = normalized.map((session) => session.id).filter(Boolean);
  const placeholders = keepIds.map(() => '?').join(',');


  for (const session of normalized) {
    const createdIso = new Date(session.createdAt || Date.now()).toISOString();
    const updatedIso = new Date(session.updatedAt || session.createdAt || Date.now()).toISOString();
    await db.runQuery(
      `INSERT OR REPLACE INTO chat_sessions (id, title, created_at, updated_at, metadata)
       VALUES (?, ?, ?, ?, ?)`,
      [session.id, session.title, createdIso, updatedIso, JSON.stringify({ message_count: session.messages.length })]
    );

    await db.runQuery(`DELETE FROM chat_messages WHERE session_id = ?`, [session.id]).catch(() => {});
    for (let idx = 0; idx < session.messages.length; idx += 1) {
      const msg = session.messages[idx];
      const messageId = chatMessageId(session.id, msg, idx);
      await db.runQuery(
        `INSERT OR REPLACE INTO chat_messages
           (id, session_id, role, content, retrieval, thinking_trace, ts, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          messageId,
          session.id,
          msg.role,
          msg.content,
          msg.retrieval ? JSON.stringify(msg.retrieval) : null,
          msg.thinking_trace ? JSON.stringify(msg.thinking_trace) : null,
          Number(msg.ts || Date.now()),
          new Date(Number(msg.ts || Date.now())).toISOString()
        ]
      );
    }
  }
  return { saved: normalized.length };
}

async function appendChatMessageToDb({ sessionId, role, content, ts = Date.now(), retrieval = null, thinkingTrace = null }) {
  const sid = String(sessionId || '').trim();
  const text = String(content || '').trim();
  if (!sid || !text) return;
  const when = Number(ts || Date.now());
  const nowIso = new Date(when).toISOString();
  const messageId = chatMessageId(sid, { role, content: text, ts: when }, when);
  const title = role === 'user' ? text.slice(0, 30) : 'New chat';

  await db.runQuery(
    `INSERT OR IGNORE INTO chat_sessions (id, title, created_at, updated_at, metadata)
     VALUES (?, ?, ?, ?, ?)`,
    [sid, title || 'New chat', nowIso, nowIso, JSON.stringify({ message_count: 0 })]
  ).catch(() => {});

  await db.runQuery(
    `INSERT OR REPLACE INTO chat_messages
     (id, session_id, role, content, retrieval, thinking_trace, ts, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      messageId,
      sid,
      role === 'assistant' ? 'assistant' : 'user',
      text.slice(0, 32000),
      retrieval ? JSON.stringify(retrieval) : null,
      thinkingTrace ? JSON.stringify(thinkingTrace) : null,
      when,
      nowIso
    ]
  );

  const countRow = await db.getQuery(`SELECT COUNT(1) AS count FROM chat_messages WHERE session_id = ?`, [sid]).catch(() => ({ count: 0 }));
  await db.runQuery(
    `UPDATE chat_sessions
     SET title = CASE WHEN (title IS NULL OR title = '' OR title = 'New chat') AND ? = 'user' THEN ? ELSE title END,
         updated_at = ?,
         metadata = ?
     WHERE id = ?`,
    [
      role === 'assistant' ? 'assistant' : 'user',
      title || 'New chat',
      nowIso,
      JSON.stringify({ message_count: Number(countRow?.count || 0) }),
      sid
    ]
  ).catch(() => {});
}

async function loadChatSessionsFromDb(limit = 25) {
  const sessions = await db.allQuery(
    `SELECT id, title, created_at, updated_at
     FROM chat_sessions
     ORDER BY updated_at DESC
     LIMIT ?`,
    [Math.max(1, Math.min(60, Number(limit || 25)))]
  ).catch(() => []);

  const out = [];
  for (const session of sessions) {
    const messages = await db.allQuery(
      `SELECT role, content, retrieval, thinking_trace, ts
       FROM chat_messages
       WHERE session_id = ?
       ORDER BY ts ASC
       LIMIT 220`,
      [session.id]
    ).catch(() => []);
    out.push({
      id: session.id,
      title: session.title || 'New chat',
      createdAt: Date.parse(session.created_at || '') || Date.now(),
      updatedAt: Date.parse(session.updated_at || '') || Date.now(),
      messages: (messages || []).map((msg) => ({
        role: msg.role === 'assistant' ? 'assistant' : 'user',
        content: String(msg.content || ''),
        retrieval: safeParseJson(msg.retrieval, null),
        thinking_trace: safeParseJson(msg.thinking_trace, null),
        ts: Number(msg.ts || Date.now())
      }))
    });
  }
  return out;
}

// Accept full chat sessions from renderer and persist as raw events (daily snapshot)
ipcMain.handle('save-chat-sessions-to-memory', async (event, sessions) => {
  try {
    if (!Array.isArray(sessions)) return { success: false, message: 'invalid_sessions' };
    
    await saveChatSessionsToDb(sessions).catch((e) => {
      console.warn('[save-chat-sessions-to-memory] durable save failed', e?.message || e);
    });
    for (const session of sessions) {
      try {
        const meta = {
          session_id: session.id,
          title: session.title || null,
          message_count: Array.isArray(session.messages) ? session.messages.length : 0,
          saved_from_ui: true
        };
        // Persist each message as a raw event so it enters the L1 event stream
        for (const msg of (session.messages || [])) {
          await ingestRawEvent({
            type: 'chat_message',
            timestamp: msg.ts ? new Date(msg.ts).toISOString() : new Date().toISOString(),
            source: 'chat_ui',
            text: String(msg.content || '').slice(0, 32000),
            metadata: {
              ...meta,
              role: msg.role,
              thinking_trace: msg.thinking_trace || null
            }
          }).catch((e) => console.warn('[save-chat-sessions-to-memory] msg ingest failed', e?.message || e));
        }
      } catch (e) {
        console.warn('[save-chat-sessions-to-memory] session failed', e?.message || e);
      }
    }
    return { success: true };
  } catch (e) {
    console.error('[save-chat-sessions-to-memory] failed', e?.message || e);
    return { success: false, message: String(e?.message || e) };
  }
});

ipcMain.handle('save-chat-sessions', async (_event, sessions) => {
  try {
    const result = await saveChatSessionsToDb(sessions || []);
    return { success: true, ...result };
  } catch (e) {
    console.error('[save-chat-sessions] failed', e?.message || e);
    return { success: false, message: String(e?.message || e) };
  }
});

ipcMain.handle('get-chat-sessions', async (_event, options = {}) => {
  try {
    const sessions = await loadChatSessionsFromDb(options?.limit || 25);
    return sessions;
  } catch (e) {
    console.error('[get-chat-sessions] failed', e?.message || e);
    return [];
  }
});

// Schedule daily chat session flush request to renderer via UI event once per day
let lastDailyChatSaveAt = 0;
function scheduleDailyChatSave() {
  try {
    const now = Date.now();
    const oneDayMs = 24 * 60 * 60 * 1000;
    if (now - lastDailyChatSaveAt < oneDayMs) return;
    lastDailyChatSaveAt = now;
    if (mainWindow && mainWindow.webContents) {
      // renderer should respond by calling saveChatSessionsToMemory with its local sessions snapshot
      mainWindow.webContents.send('request-chat-save');
    }
  } catch (e) {
    console.warn('[scheduleDailyChatSave] failed to request renderer save', e?.message || e);
  }
}

// Kick off a daily timer (runs once an hour check) to request renderer to push chat snapshots
setInterval(() => {
  try { scheduleDailyChatSave(); } catch (e) { /* ignore */ }
}, 60 * 60 * 1000);

// Full Memory Graph for Settings Explorer
ipcMain.handle("get-full-memory-graph", async () => {
  try {
    const nodes = await db.allQuery(`SELECT id, layer, subtype, title, summary, metadata, anchor_date, anchor_at, created_at, updated_at FROM memory_nodes LIMIT 2000`).catch(() => []);
    const edges = await db.allQuery(`SELECT from_node_id AS source, to_node_id AS target, edge_type, weight, trace_label FROM memory_edges LIMIT 5000`).catch(() => []);
    return { nodes, edges };
  } catch (err) {
    console.error("Failed to fetch full memory graph:", err);
    return { nodes: [], edges: [] };
  }
});

// AI Assistant Chat Logic
ipcMain.handle('ask-ai-assistant', async (event, query, options = {}) => {
  const senderId = event?.sender?.id || 0;
  const requestId = String(options?.requestId || `chatreq_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
  const timeoutMs = Math.max(4000, Number(options?.timeoutMs || DEFAULT_CHAT_TIMEOUT_MS));
  const requestState = startActiveChatRequest(senderId, requestId);
  try {
    console.log('[ChatMemory] Using SQLite memory DB:', typeof db.getDbPath === 'function' ? db.getDbPath() : 'unknown');
    const proactiveMemory = store.get('proactiveMemory') || { core: '' };
    const historicalSummaries = store.get('historicalSummaries') || {};
    const searchIndex = store.get('searchIndex') || { people: {}, topics: {} };
    const normalizedChatHistory = Array.isArray(options?.chat_history)
      ? options.chat_history
          .filter((item) => item && typeof item.content === 'string')
          .slice(-12)
          .map((item) => ({
            role: item.role === 'assistant' ? 'assistant' : 'user',
            content: String(item.content || '').slice(0, 2000),
            ts: item.ts || null
          }))
      : [];
    const resolvedChatSessionId = String(options?.chat_session_id || `chat_${Date.now()}`);
    const localToolResponse = maybeHandleLocalChatToolQuery(query);
    if (localToolResponse) {
      const toolResponse = {
        ...localToolResponse,
        requestId,
        status: 'complete',
        degraded: false
      };
      enqueueChatPersistence(`${resolvedChatSessionId}:${requestId}:tool`, async () => {
        await persistChatTurnAsRawEvent({
          chatSessionId: resolvedChatSessionId,
          role: 'user',
          content: query,
          chatHistory: normalizedChatHistory
        });
        await appendChatMessageToDb({
          sessionId: resolvedChatSessionId,
          role: 'user',
          content: query,
          ts: Date.now()
        }).catch((err) => console.warn('[chat-memory] Failed to append user tool turn:', err?.message || err));
        await persistChatTurnAsRawEvent({
          chatSessionId: resolvedChatSessionId,
          role: 'assistant',
          content: toolResponse.content || '',
          chatHistory: normalizedChatHistory.concat([{ role: 'user', content: String(query || ''), ts: Date.now() }]),
          retrieval: toolResponse.retrieval || null
        });
        await appendChatMessageToDb({
          sessionId: resolvedChatSessionId,
          role: 'assistant',
          content: toolResponse.content || '',
          ts: Date.now(),
          retrieval: toolResponse.retrieval || null,
          thinkingTrace: toolResponse.thinking_trace || null
        }).catch((err) => console.warn('[chat-memory] Failed to append assistant tool turn:', err?.message || err));
      });
      try { event.sender.send('chat-step', { requestId, status: 'completed', step: 'terminal', label: 'Complete' }); } catch (_) {}
      return toolResponse;
    }

    const response = await withTimeout(answerChatQuery({
      apiKey: process.env.DEEPSEEK_API_KEY,
      query,
      options: {
        ...options,
        requestId,
        timeoutMs,
        chat_history: normalizedChatHistory,
        standing_notes: proactiveMemory.core || '',
        historical_summaries: historicalSummaries,
        search_index: searchIndex,
        cancellation: {
          isCancelled: () => Boolean(requestState.cancelled)
        }
      },
      onStep: (data) => {
        if (requestState.cancelled) return;
        const now = Date.now();
        const normalizedStatus = String(data?.status || '').toLowerCase();
        const terminal = normalizedStatus === 'completed' || normalizedStatus === 'failed' || normalizedStatus === 'cancelled' || normalizedStatus === 'timed_out';
        if (!terminal && (now - requestState.lastStepEmitAt) < CHAT_STEP_EMIT_INTERVAL_MS) return;
        requestState.lastStepEmitAt = now;
        try { event.sender.send('chat-step', compactChatStepPayload(data, requestId)); } catch (_) {}
      }
    }), timeoutMs + 1000, 'ask-ai-assistant');
    const finalResponse = {
      ...response,
      requestId,
      status: response?.status || 'complete',
      degraded: Boolean(response?.degraded)
    };
    enqueueChatPersistence(`${resolvedChatSessionId}:${requestId}:answer`, async () => {
      await persistChatTurnAsRawEvent({
        chatSessionId: resolvedChatSessionId,
        role: 'user',
        content: query,
        chatHistory: normalizedChatHistory
      });
      await appendChatMessageToDb({
        sessionId: resolvedChatSessionId,
        role: 'user',
        content: query,
        ts: Date.now()
      }).catch((err) => console.warn('[chat-memory] Failed to append user turn:', err?.message || err));
      await persistChatTurnAsRawEvent({
        chatSessionId: resolvedChatSessionId,
        role: 'assistant',
        content: finalResponse?.content || '',
        chatHistory: normalizedChatHistory.concat([{ role: 'user', content: String(query || ''), ts: Date.now() }]),
        retrieval: finalResponse?.retrieval || null
      });
      await appendChatMessageToDb({
        sessionId: resolvedChatSessionId,
        role: 'assistant',
        content: finalResponse?.content || '',
        ts: Date.now(),
        retrieval: finalResponse?.retrieval || null,
        thinkingTrace: finalResponse?.thinking_trace || null
      }).catch((err) => console.warn('[chat-memory] Failed to append assistant turn:', err?.message || err));
    });
    try { event.sender.send('chat-step', { requestId, status: finalResponse.status === 'timed_out' ? 'timed_out' : 'completed', step: 'terminal', label: finalResponse.status }); } catch (_) {}
    return finalResponse;
  } catch (e) {
    const errorMessage = String(e?.message || e || 'Unknown chat error');
    const timedOut = /timed out/i.test(errorMessage);
    if (timedOut) {
      console.warn('[Chat] Request timed out; returning bounded fallback.');
    } else {
      console.error('Chat error:', e);
    }
    try { event.sender.send('chat-step', { requestId, status: timedOut ? 'timed_out' : 'failed', step: 'terminal', label: timedOut ? 'timed_out' : 'failed', detail: errorMessage.slice(0, 200) }); } catch (_) {}
    return {
      requestId,
      status: timedOut ? 'timed_out' : 'failed',
      degraded: true,
      content: timedOut
        ? "I returned early to keep chat responsive. Try narrowing the timeframe or asking me to go deeper on one part."
        : "I encountered an error while thinking. Please try again.",
      thinking_trace: {
        thinking_summary: `The assistant recovered from an internal error: ${errorMessage}`,
        filters: [],
        search_queries: { context: [], messages: [], lexical: [], web: [] },
        results_summary: {
          headline: 'The response pipeline failed before completion.',
          details: ['A fallback response was returned so the chat can continue.']
        },
        data_sources: ['System error fallback'],
        stage_trace: [
          {
            step: 'error_recovery',
            label: 'Error recovery',
            status: 'failed',
            detail: errorMessage
          }
        ],
        reasoning_chain: [
          {
            stage: 'error_recovery',
            summary: 'Encountered an internal exception and returned a safe fallback.',
            detail: errorMessage
          }
        ],
        answer_basis: 'error_fallback'
      },
      retrieval: {
        usedSources: ['System error fallback'],
        memory_sources: ['System error fallback'],
        mode: 'error',
        stage_trace: [
          {
            step: 'error_recovery',
            label: 'Error recovery',
            status: 'failed',
            detail: errorMessage
          }
        ],
        thinking_trace: {
          thinking_summary: `The assistant recovered from an internal error: ${errorMessage}`,
          data_sources: ['System error fallback']
        }
      }
    };
  } finally {
    finishActiveChatRequest(senderId, requestId);
  }
});

ipcMain.handle('cancel-ai-assistant-request', async (event, requestId) => {
  return { cancelled: cancelActiveChatRequest(event?.sender?.id || 0, String(requestId || '')) };
});

// Generate proactive todos — strictly AI generated from memory retrieval.
ipcMain.handle('generate-proactive-todos', async (event, payload = {}) => {
  console.log('[Main] IPC generate-proactive-todos invoked from', event?.sender?.getURL ? event.sender.getURL() : 'main');
  try {
    const llmConfig = getSuggestionLLMConfig();
    if (!llmConfig) {
      return getStoredRadarState().allSignals || [];
    }
    const radarState = await withTimeout(
      buildRadarState({
        llmConfig,
        manualTodos: (store.get("persistentTodos") || []).filter((todo) => !todo?.completed),
        maxCentralSignals: 5,
        maxRelationshipSignals: 5,
        maxTodoSignals: 5,
        existingState: getStoredRadarState()
      }),
      45000,
      'buildRadarState'
    );
    persistRadarState(radarState);
    return radarState.allSignals || [];
  } catch (error) {
    console.error('Error generating proactive todos:', error);
    return getStoredRadarState().allSignals || [];
  }
});

// Execute AI task through the macOS Accessibility desktop agent
ipcMain.handle('execute-ai-task', async (event, task) => {
  if (task && !task.id) task.id = generateTaskId();
  console.log(`Executing AI Task autonomously: ${task.title} (${task.id})`);
  return await executeAITaskInternal(task);
});

ipcMain.handle('get-voice-control-status', async () => {
  const settings = getVoiceControlSettings();
  return {
    ...settings,
    registered: globalShortcut.isRegistered(settings.shortcut),
    session: activeVoiceSession
  };
});

ipcMain.handle('set-voice-control-enabled', async (event, enabled) => {
  const current = getVoiceControlSettings();
  store.set('voiceControlSettings', {
    ...current,
    enabled: Boolean(enabled)
  });
  registerVoiceShortcut();
  return {
    ...getVoiceControlSettings(),
    registered: globalShortcut.isRegistered(getVoiceControlSettings().shortcut),
    session: activeVoiceSession
  };
});

ipcMain.handle('voice-capture-failed', async (event, payload = {}) => {
  if (!activeVoiceSession || (payload.sessionId && payload.sessionId !== activeVoiceSession.id)) {
    return { status: 'ignored' };
  }
  setVoiceSession({
    status: 'failed',
    error: payload.error || 'Voice capture failed'
  });
  clearVoiceSessionLater();
  return { status: 'ok' };
});

ipcMain.handle('update-voice-session-transcript', async (_event, payload = {}) => {
  if (!activeVoiceSession || (payload.sessionId && payload.sessionId !== activeVoiceSession.id)) {
    return { status: 'ignored' };
  }
  const partialTranscript = String(payload.partial_transcript || payload.transcript || '').trim();
  setVoiceSession({
    partial_transcript: partialTranscript,
    transcript: partialTranscript || activeVoiceSession.transcript || '',
    engine: payload.engine || activeVoiceSession.engine || 'native_local',
    latency_ms: Number.isFinite(payload.latency_ms) ? payload.latency_ms : (activeVoiceSession.latency_ms || null)
  });
  return { status: 'ok' };
});

ipcMain.handle('submit-voice-transcript', async (_event, payload = {}) => {
  if (!activeVoiceSession || (payload.sessionId && payload.sessionId !== activeVoiceSession.id)) {
    throw new Error('No active voice session');
  }

  const transcript = String(payload.transcript || '').trim();
  setVoiceSession({
    status: 'transcribing',
    transcript,
    partial_transcript: transcript || activeVoiceSession.partial_transcript || '',
    final_transcript: transcript,
    engine: payload.engine || 'native_local',
    error: null
  });

  if (!transcript) {
    setVoiceSession({
      status: 'failed',
      error: 'Could not hear anything'
    });
    clearVoiceSessionLater();
    return { status: 'failed', error: 'empty-transcript' };
  }

  try {
    return await runVoiceGoal(transcript, {
      confidence: payload.confidence ?? null,
      provider: payload.engine || 'native_local',
      mode: 'local_first'
    });
  } catch (error) {
    setVoiceSession({
      status: 'failed',
      error: error?.message || 'Voice transcript execution failed'
    });
    clearVoiceSessionLater();
    throw error;
  }
});

ipcMain.handle('submit-voice-audio', async (event, payload = {}) => {
  if (!activeVoiceSession || (payload.sessionId && payload.sessionId !== activeVoiceSession.id)) {
    throw new Error('No active voice session');
  }

  try {
    const settings = getVoiceControlSettings();
    const expectedEngine = settings.speech_engine === 'cloud_only' ? 'openai_cloud' : 'local_stt';
    setVoiceSession({
      status: 'transcribing',
      engine: expectedEngine,
      error: null
    });

    const transcription = await transcribeVoiceAudio({
      audioBase64: payload.audioBase64,
      mimeType: payload.mimeType,
      preferredEngine: settings.speech_engine || 'native_local_first'
    });
    const provider = transcription.provider || 'openai_cloud';
    setVoiceSession({
      engine: provider
    });

    return await runVoiceGoal(transcription.text, {
      confidence: transcription.confidence,
      provider,
      mode: provider === 'local_stt' ? 'local_first' : 'cloud_fallback'
    });
  } catch (error) {
    setVoiceSession({
      status: 'failed',
      error: error?.message || 'Transcription failed'
    });
    clearVoiceSessionLater();
    throw error;
  }
});

// Update a running automation plan mid-flight (best-effort).
ipcMain.handle('update-ai-task-plan', async (event, payload) => {
  try {
    return { status: 'offline', error: 'Browser extension support has been removed' };
  } catch (e) {
    return { status: 'error', error: e && e.message ? e.message : String(e) };
  }
});

// Update an existing task's status
ipcMain.handle('complete-task', async (event, taskId) => {
  try {
    const todos = store.get('persistentTodos') || [];
    const task = todos.find(t => t.id === taskId);
    if (task) {
      task.completed = true;
      store.set('persistentTodos', todos);
      const log = store.get('completedTasksLog') || [];
      log.unshift({
        id: `done_${taskId}_${Date.now()}`,
        taskId,
        title: task.title || 'Completed task',
        completedAt: new Date().toISOString()
      });
      store.set('completedTasksLog', log.slice(0, 500));
      return true;
    }
  } catch (e) {
    console.error(e);
  }
  return false;
});

// Check extension connection status
ipcMain.handle('get-extension-status', async () => {
  return { connected: false, lastSeen: null, recentlySeen: false, transport: 'disabled' };
});

ipcMain.handle('get-accessibility-status', async () => {
  const permission = await checkAccessibilityPermission();
  const observation = permission.trusted ? await observeDesktopState({ includeScreenshot: false }).catch(() => null) : null;
  const managedBrowser = await getManagedBrowserStatus().catch(() => null);
  return {
    trusted: Boolean(permission.trusted),
    status: permission.status || (permission.trusted ? 'trusted' : 'not_trusted'),
    error: permission.error || null,
    frontmostApp: observation?.frontmost_app || '',
    windowTitle: observation?.window_title || '',
    adaptiveVision: true,
    perceptionMode: observation?.perception_mode || observation?.vision_mode || 'ax_only',
    managedBrowser
  };
});

ipcMain.handle('get-managed-browser-status', async () => {
  return await getManagedBrowserStatus();
});

ipcMain.handle('open-accessibility-settings', async () => {
  try {
    return await openAccessibilitySettings();
  } catch (error) {
    return {
      status: 'error',
      error: error?.message || 'Unable to open Accessibility settings'
    };
  }
});

ipcMain.handle('open-screen-recording-settings', async () => {
  try {
    return await openScreenRecordingSettings();
  } catch (error) {
    return {
      status: 'error',
      error: error?.message || 'Unable to open Screen Recording settings'
    };
  }
});

ipcMain.handle('accessibility-run-diagnostic', async () => {
  const permission = await checkAccessibilityPermission();
  if (!permission.trusted) {
    return { status: 'error', error: permission.error || 'Accessibility permission not granted' };
  }
  const observation = await observeDesktopState();
  return {
    status: 'ok',
    observation
  };
});

// Helper function to generate task ID
function generateTaskId() {
  return 'task_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
}

function sendTaskToExtension(task, script) {
  if (!isExtensionConnected()) throw new Error('Extension not connected');
  return new Promise((resolve, reject) => {
    const taskId = task.id || generateTaskId();
    const timeoutHandle = setTimeout(() => {
      extensionSocket.off('message', handleResult);
      reject(new Error('Extension automation timeout (120s)'));
    }, 120000);
    let safeScript = script;
    if (safeScript && typeof safeScript === 'string' && safeScript.includes(':contains(')) {
      console.warn('Dropping unsupported :contains() selectors in extension script; using operator loop instead.');
      safeScript = null;
    }
    extensionSocket.send(JSON.stringify({
      type: 'execute-task',
      task: {
        id: taskId,
        title: task.title,
        url: task.url || (task.title.toLowerCase().includes('perplexity') ? 'https://www.perplexity.ai' : (task.title.toLowerCase().includes('gmail') ? 'https://mail.google.com' : null)),
        script: safeScript,
        draft: task.ai_draft || '',
        plan: task.plan || null
      }
    }));

    const handleResult = (data) => {
      let res;
      try {
        res = JSON.parse(data);
      } catch(e) { return; }

      if (res.type === 'task-result') {
        if (res.taskId && res.taskId !== taskId) return;
        extensionSocket.off('message', handleResult);
        clearTimeout(timeoutHandle);
        if (res.status === 'success') {
          console.log('Extension task completed successfully.');
          resolve({
            status: 'success',
            result: res.result || null,
            taskId
          });
        } else {
          console.error('Extension task failed:', res.error);
          reject(new Error(res.error));
        }
      }
    };
    extensionSocket.on('message', handleResult);
  });
}

function sendExtensionRequest(message, expectType, matcher, timeoutMs = 8000) {
  if (!isExtensionConnected()) throw new Error('Extension not connected');
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      extensionSocket.off('message', onMsg);
      reject(new Error('Extension request timeout'));
    }, timeoutMs);
    const onMsg = (data) => {
      try {
        const msg = JSON.parse(data.toString());
  // Accept either the exact expected wrapper type (e.g., 'extension-event') or ACTION_RESULT-style messages
  const normalizedType = (msg.type || '').toString().toLowerCase();
  const expectLower = (expectType || '').toString().toLowerCase();
  const hasTaskRef = (typeof msg.task_id !== 'undefined' || typeof msg.taskId !== 'undefined');
  const looksLikeActionResult = (hasTaskRef && typeof msg.status !== 'undefined');
  if (!(normalizedType === expectLower || looksLikeActionResult || (matcher && matcher(msg)))) return;
  if (matcher && !matcher(msg)) return;
        clearTimeout(timer);
        extensionSocket.off('message', onMsg);
        resolve(msg);
      } catch (e) {}
    };
    extensionSocket.on('message', onMsg);
    extensionSocket.send(JSON.stringify(message));
  });
}

async function sendExtensionRequestWithRetry(message, expectType, matcher, timeoutMs = 8000, attempts = 2) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      return await sendExtensionRequest(message, expectType, matcher, timeoutMs);
    } catch (e) {
      lastErr = e;
      if (!isExtensionConnected()) break;
      await new Promise((r) => setTimeout(r, 500));
    }
  }
  throw lastErr || new Error('Extension request failed');
}

function buildDesktopGoal(task = {}) {
  return [
    task.raw_goal ? `Raw goal: ${task.raw_goal}` : '',
    `Task: ${task.title || 'Untitled task'}`,
    task.description ? `Context: ${task.description}` : '',
    task.ai_draft ? `Draft content: ${task.ai_draft}` : '',
    Array.isArray(task.step_plan) && task.step_plan.length ? `Step plan:\n${task.step_plan.map((item, index) => `${index + 1}. ${item}`).join('\n')}` : '',
    Array.isArray(task.action_plan) && task.action_plan.length ? `Action plan:\n${task.action_plan.map((item, index) => `${index + 1}. ${item.step || item.url || ''}`).join('\n')}` : '',
    task.target_surface ? `Target surface: ${task.target_surface}` : '',
    task.execution_mode ? `Execution mode: ${task.execution_mode}` : ''
  ].filter(Boolean).join('\n\n');
}

function getVoiceControlSettings() {
  const stored = store.get('voiceControlSettings') || {};
  const storedShortcut = String(stored.shortcut || '').trim();
  const normalizedShortcut = (!storedShortcut || storedShortcut === LEGACY_VOICE_SHORTCUT)
    ? DEFAULT_VOICE_SHORTCUT
    : storedShortcut;
  return {
    enabled: stored.enabled !== false,
    shortcut: normalizedShortcut,
    speech_engine: stored.speech_engine || 'native_local_first',
    hud_mode: 'floating'
  };
}

function emitVoiceSessionUpdate(sessionState = null) {
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('voice-session-update', sessionState);
  }
  if (voiceHudWindow && !voiceHudWindow.isDestroyed()) {
    voiceHudWindow.webContents.send('voice-session-update', sessionState);
  }
}

function setVoiceSession(sessionPatch = {}) {
  activeVoiceSession = {
    ...(activeVoiceSession || {}),
    ...sessionPatch
  };
  emitVoiceSessionUpdate(activeVoiceSession);
  return activeVoiceSession;
}

function clearVoiceSessionLater(delayMs = 3000) {
  setTimeout(() => {
    if (activeVoiceSession && ['completed', 'failed'].includes(activeVoiceSession.status)) {
      activeVoiceSession = null;
      emitVoiceSessionUpdate(null);
    }
  }, delayMs);
  hideVoiceHudLater(delayMs);
}

function parseVoiceArgsEnv(raw = '') {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return [];
  try {
    const parsed = JSON.parse(trimmed);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch (_) {
    return [];
  }
}

async function runLocalVoiceTranscription({ audioBuffer, mimeType = 'audio/webm' } = {}) {
  const command = String(process.env.VOICE_LOCAL_STT_COMMAND || '').trim();
  if (!command) return null;
  const extension = /wav/i.test(mimeType) ? 'wav' : /mp4|m4a/i.test(mimeType) ? 'm4a' : 'webm';
  const tempFile = path.join(app.getPath('temp'), `weave-voice-${Date.now()}.${extension}`);
  await fs.promises.writeFile(tempFile, audioBuffer);

  const timeout = Number(process.env.VOICE_LOCAL_STT_TIMEOUT_MS || 20000);
  const argsTemplate = parseVoiceArgsEnv(process.env.VOICE_LOCAL_STT_ARGS_JSON);
  const args = (argsTemplate.length ? argsTemplate : ['{{audio_path}}'])
    .map((arg) => arg.replace(/\{\{audio_path\}\}/g, tempFile));

  try {
    const output = await new Promise((resolve, reject) => {
      execFile(command, args, { timeout }, (error, stdout, stderr) => {
        if (error) {
          reject(new Error((stderr || error.message || '').toString().trim() || 'Local transcription failed'));
          return;
        }
        resolve(String(stdout || '').trim());
      });
    });

    if (!output) throw new Error('Local transcription returned empty output');
    let parsed = null;
    try { parsed = JSON.parse(output); } catch (_) {}
    const text = parsed
      ? String(parsed.text || parsed.transcript || parsed.output_text || '').trim()
      : output.split('\n').map((line) => line.trim()).filter(Boolean).pop() || '';
    if (!text) throw new Error('Local transcription did not produce text');
    return {
      text,
      confidence: parsed?.confidence ?? null,
      raw: parsed || output,
      provider: 'local_stt'
    };
  } finally {
    fs.promises.unlink(tempFile).catch(() => {});
  }
}

async function transcribeVoiceAudio({ audioBase64 = '', mimeType = 'audio/webm', preferredEngine = 'native_local_first' } = {}) {
  const audioBuffer = Buffer.from(String(audioBase64 || ''), 'base64');
  if (!audioBuffer.length) {
    throw new Error('No audio captured');
  }

  const cloudOnly = preferredEngine === 'cloud_only';
  const localOnly = preferredEngine === 'local_only';
  let localError = null;

  if (!cloudOnly) {
    try {
      const localResult = await runLocalVoiceTranscription({ audioBuffer, mimeType });
      if (localResult?.text) return localResult;
    } catch (error) {
      localError = error;
      if (localOnly) throw error;
    }
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    if (localError) throw localError;
    throw new Error('OPENAI_API_KEY is not configured and no local transcription engine is available');
  }

  const extension = /wav/i.test(mimeType) ? 'wav' : /mp4|m4a/i.test(mimeType) ? 'm4a' : 'webm';
  const form = new FormData();
  form.append('file', audioBuffer, {
    filename: `voice-command.${extension}`,
    contentType: mimeType
  });
  form.append('model', 'gpt-4o-mini-transcribe');

  const response = await axios.post('https://api.openai.com/v1/audio/transcriptions', form, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...form.getHeaders()
    },
    maxBodyLength: Infinity,
    maxContentLength: Infinity
  });

  return {
    text: String(response.data?.text || '').trim(),
    confidence: response.data?.confidence ?? null,
    raw: response.data || {},
    provider: 'openai_cloud'
  };
}

function buildVoiceTask(goalText, source = 'voice', executionMode = 'voice_autonomous') {
  const rawGoal = String(goalText || '').trim();
  return {
    id: `${source === 'voice' ? 'voice_task' : 'settings_prompt'}_${Date.now()}`,
    title: rawGoal,
    description: source === 'voice' ? `Voice command: ${rawGoal}` : `Settings desktop prompt: ${rawGoal}`,
    transcript: rawGoal,
    raw_goal: rawGoal,
    source,
    execution_mode: executionMode,
    ai_draft: rawGoal,
    agentMode: true
  };
}

async function runVoiceGoal(rawGoal, transcriptMeta = {}) {
  const trimmed = String(rawGoal || '').trim();
  if (!trimmed) {
    setVoiceSession({
      status: 'failed',
      error: 'Could not hear anything',
      transcription_meta: transcriptMeta || null
    });
    clearVoiceSessionLater();
    return { status: 'failed', error: 'empty-transcript' };
  }

  const startedAt = activeVoiceSession?.started_at ? new Date(activeVoiceSession.started_at).getTime() : Date.now();
  setVoiceSession({
    status: 'acting',
    transcript: trimmed,
    partial_transcript: trimmed,
    final_transcript: trimmed,
    transcription_meta: transcriptMeta,
    latency_ms: Math.max(0, Date.now() - startedAt),
    error: null,
    agent_stage: 'starting',
    effect_summary: '',
    remaining_gap: 'begin the requested action'
  });

  const result = await executeAITaskInternal(buildVoiceTask(trimmed, 'voice', 'voice_autonomous'));
  setVoiceSession({
    status: result?.status === 'success' ? 'completed' : 'failed',
    result: result?.result || '',
    error: result?.status === 'success' ? null : (result?.failure_reason || result?.error || 'Agent action failed')
  });
  clearVoiceSessionLater();
  return result;
}

async function executeAITaskInternal(task) {
  if (task && !task.id) task.id = generateTaskId();
  if (task && !task.raw_goal) task.raw_goal = String(task.ai_draft || task.transcript || task.title || '').trim();
  const inferredUrl = inferDirectUrlFromGoal(String(task?.raw_goal || task?.title || ''));
  const shouldRouteToExtension = Boolean(
    task?.forceExtension
    || task?.source === 'voice'
    || task?.execution_mode === 'voice_autonomous'
    || task?.execution_mode === 'browser_extension'
    || String(task?.id || '').startsWith('settings_prompt_')
  );

  if (shouldRouteToExtension) {
    if (!isExtensionConnected()) {
      throw new Error('Browser extension is not connected. Open Chrome and make sure the extension is loaded and connected.');
    }
    const extensionTask = {
      ...task,
      title: String(task.title || task.raw_goal || '').trim(),
      url: task.url || inferredUrl || null,
      draft: task.ai_draft || ''
    };
    const extResult = await sendTaskToExtension(extensionTask, null);
    return {
      status: 'success',
      result: extResult?.result?.title
        ? `Extension handled voice prompt in tab: ${extResult.result.title}`
        : 'Extension handled voice prompt',
      execution_target: 'browser_extension',
      extension_result: extResult?.result || null
    };
  }

  const permission = await checkAccessibilityPermission();
  if (!permission.trusted) {
    throw new Error(permission.error || 'Accessibility permission is required. Enable this app in System Settings > Privacy & Security > Accessibility.');
  }
  return await runAgentLoop({ ...task, agentMode: true, forceExtension: false });
}

function registerVoiceShortcut() {
  const settings = getVoiceControlSettings();
  globalShortcut.unregister(settings.shortcut || DEFAULT_VOICE_SHORTCUT);
  globalShortcut.unregister(DEFAULT_VOICE_SHORTCUT);
  globalShortcut.unregister(LEGACY_VOICE_SHORTCUT);
  if (!settings.enabled) return false;
  try {
    return globalShortcut.register(settings.shortcut, () => {
      if (!mainWindow) return;
      ensureVoiceHudVisible();
      if (activeVoiceSession?.status === 'listening') {
        setVoiceSession({ status: 'transcribing' });
        mainWindow.webContents.send('voice-command-toggle', { action: 'stop', session: activeVoiceSession });
        return;
      }
      if (activeVoiceSession && ['transcribing', 'acting'].includes(activeVoiceSession.status)) {
        setVoiceSession({
          status: activeVoiceSession.status,
          error: 'Voice agent is busy',
          last_checked_at: new Date().toISOString()
        });
        return;
      }

      const sessionId = `voice_${Date.now()}`;
      const sessionState = setVoiceSession({
        id: sessionId,
        status: 'listening',
        started_at: new Date().toISOString(),
        transcript: '',
        partial_transcript: '',
        final_transcript: '',
        transcription_meta: null,
        engine: 'native_local',
        latency_ms: null,
        error: null,
        shortcut: settings.shortcut
      });
      mainWindow.webContents.send('voice-command-toggle', { action: 'start', session: sessionState });
    });
  } catch (error) {
    console.error('[Voice] Failed to register shortcut:', error);
    return false;
  }
}

function summarizeObservationForPlanner(observation = {}) {
  return {
    surface_driver: observation?.surface_driver || 'ax',
    perception_mode: observation?.perception_mode || 'ax_tree',
    window_id: observation?.window_id || null,
    bounds: observation?.bounds || null,
    focused_element_id: observation?.focused_element_id || null,
    frontmost_app: observation?.frontmost_app || '',
    window_title: observation?.window_title || '',
    url: observation?.url || '',
    tab_title: observation?.tab_title || '',
    surface_type: observation?.surface_type || '',
    browser_tree: observation?.browser_tree ? {
      url: observation.browser_tree.url,
      title: observation.browser_tree.title,
      ax_nodes_count: Array.isArray(observation.browser_tree.ax_nodes) ? observation.browser_tree.ax_nodes.length : 0
    } : null,
    ax_tree: observation?.ax_tree ? {
      id: observation.ax_tree.id,
      role: observation.ax_tree.role,
      title: observation.ax_tree.title,
      identifier: observation.ax_tree.identifier,
      child_count: Array.isArray(observation.ax_tree.children) ? observation.ax_tree.children.length : 0
    } : null,
    vision_mode: observation?.vision_mode || 'ax_only',
    visual_change_summary: observation?.visual_change_summary || '',
    screenshot_summary: observation?.screenshot_summary || null,
    target_confidence: observation?.target_confidence ?? null,
    text_sample: String(observation?.text_sample || '').slice(0, 500),
    interactive_candidates: Array.isArray(observation?.interactive_candidates)
      ? observation.interactive_candidates.slice(0, 6).map((item) => ({
          id: item.id || null,
          index: item.index,
          role: item.role,
          name: item.name,
          description: item.description,
          group: item.group,
          identifier: item.identifier || null
        }))
      : []
  };
}

function verifyGoalProgress(observation = {}, goalBundle = {}, history = []) {
  const surfaceDriver = String(observation?.surface_driver || 'ax');
  const surfaceType = String(observation?.surface_type || '');
  const title = String(observation?.window_title || '').toLowerCase();
  const url = String(observation?.url || '').toLowerCase();
  const textSample = String(observation?.text_sample || '').toLowerCase();
  const query = String(goalBundle?.query_text || '').toLowerCase();
  const entityTerms = Array.isArray(goalBundle?.entity_terms) ? goalBundle.entity_terms.map((item) => String(item || '').toLowerCase()) : [];
  const ordinal = Number(goalBundle?.ordinal_target || 0) || null;
  const resultCandidates = Array.isArray(observation?.interactive_candidates)
    ? observation.interactive_candidates.filter((item) => item.group === 'result_link')
    : [];
  const lastSuccessfulClick = [...history].reverse().find((entry) => ['CLICK_AX', 'PRESS_AX', 'CDP_CLICK'].includes(entry?.action?.kind) && entry?.result_status === 'success');
  const lastSuccessfulType = [...history].reverse().find((entry) => ['TYPE_TEXT', 'SET_VALUE', 'SET_AX_VALUE', 'CDP_TYPE'].includes(entry?.action?.kind) && entry?.result_status === 'success');
  const lastSuccessfulOpen = [...history].reverse().find((entry) => ['OPEN_URL', 'CDP_NAVIGATE', 'ACTIVATE_APP', 'FOCUS_WINDOW'].includes(entry?.action?.kind) && entry?.result_status === 'success');
  const queryEntered = !query || textSample.includes(query) || resultCandidates.some((item) => `${item.name || ''} ${item.description || ''}`.toLowerCase().includes(query));
  const resultsVisible = surfaceType === 'search_results' || resultCandidates.length > 0 || (surfaceDriver === 'cdp' && /[\?&]q=/.test(url));
  const enoughResults = !ordinal || resultCandidates.length >= ordinal;
  const openedNonSearchDestination = Boolean(lastSuccessfulClick) && surfaceType !== 'search_results' && !/google search|google/.test(title) && (!surfaceDriver || !/google\./.test(url) || !/[\?&]q=/.test(url));
  const mentionsEntity = !entityTerms.length || entityTerms.some((term) => term && (`${title} ${textSample}`).includes(term));

  if (goalBundle?.surface_goal === 'web_search') {
    if (!queryEntered) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'enter the search query', confidence: 0.35 };
    }
    if (!resultsVisible) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'load visible search results', confidence: 0.45 };
    }
    if (ordinal && !enoughResults) {
      return { is_complete: false, completion_reason: '', remaining_gap: `find at least ${ordinal} visible results`, confidence: 0.55 };
    }
    if (ordinal && !openedNonSearchDestination) {
      return { is_complete: false, completion_reason: '', remaining_gap: `open the ${ordinal} result and confirm the destination page changed`, confidence: 0.7 };
    }
    return {
      is_complete: true,
      completion_reason: ordinal
        ? `The ${ordinal} search result appears to have opened a destination page.`
        : 'Search results are visibly loaded.',
      remaining_gap: '',
      confidence: 0.88
    };
  }

  if (goalBundle?.surface_goal === 'email_flow') {
    const composerVisible = /email_composer/.test(surfaceType) || /\bcompose|reply|draft\b/.test(`${title} ${textSample}`);
    const draftPrepared = composerVisible && (Boolean(lastSuccessfulType) || /\bto:|subject:|draft\b/.test(textSample));
    if (!composerVisible) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'open the relevant email thread or compose surface', confidence: 0.4 };
    }
    if (!draftPrepared) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'prepare the draft in the visible email context', confidence: 0.62 };
    }
    return {
      is_complete: true,
      completion_reason: 'The email draft appears to be prepared in the current compose context.',
      remaining_gap: '',
      confidence: 0.82
    };
  }

  if (goalBundle?.surface_goal === 'calendar_flow') {
    const calendarContextVisible = /calendar_view|calendar_editor/.test(surfaceType) || /\bcalendar|meeting|agenda|event\b/.test(`${title} ${textSample}`);
    const prepVisible = /agenda|attendees|invite|notes|event/.test(textSample) || /calendar_editor/.test(surfaceType);
    if (!calendarContextVisible) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'open the relevant calendar or meeting context', confidence: 0.38 };
    }
    if (!prepVisible) {
      return { is_complete: false, completion_reason: '', remaining_gap: 'reach the event details or prep-ready meeting surface', confidence: 0.58 };
    }
    return {
      is_complete: true,
      completion_reason: 'The calendar or meeting context appears ready for preparation.',
      remaining_gap: '',
      confidence: 0.8
    };
  }

  if (goalBundle?.surface_goal === 'generic_navigation') {
    const visibleReadyState = mentionsEntity && (Boolean(lastSuccessfulOpen || lastSuccessfulClick) || surfaceType !== 'generic_app');
    if (visibleReadyState) {
      return {
        is_complete: true,
        completion_reason: 'The requested destination appears to be visibly open.',
        remaining_gap: '',
        confidence: 0.68
      };
    }
  }

  return {
    is_complete: false,
    completion_reason: '',
    remaining_gap: goalBundle?.completion_check || 'reach the requested destination',
    confidence: 0.2
  };
}

function buildEffectSummary(before = {}, after = {}, action = {}, verificationBefore = {}, verificationAfter = {}) {
  const beforeTitle = String(before?.window_title || '');
  const afterTitle = String(after?.window_title || '');
  const beforeUrl = String(before?.url || '');
  const afterUrl = String(after?.url || '');
  const beforeSurface = String(before?.surface_type || '');
  const afterSurface = String(after?.surface_type || '');
  const beforeText = String(before?.text_sample || '').toLowerCase();
  const afterText = String(after?.text_sample || '').toLowerCase();

  if (beforeUrl && afterUrl && beforeUrl !== afterUrl) {
    return `url changed from "${beforeUrl}" to "${afterUrl}"`;
  }
  if (beforeTitle && afterTitle && beforeTitle !== afterTitle) {
    return `window title changed from "${beforeTitle}" to "${afterTitle}"`;
  }
  if (beforeSurface !== afterSurface && afterSurface) {
    return `${afterSurface.replace(/_/g, ' ')} appeared`;
  }
  if (['SET_VALUE', 'SET_AX_VALUE', 'CDP_TYPE'].includes(action?.kind) && action?.text && afterText.includes(String(action.text).toLowerCase())) {
    return `field now contains "${action.text}"`;
  }
  if (['KEY_PRESS', 'CDP_KEY_PRESS'].includes(action?.kind) && String(action.key || '').toLowerCase() === 'enter' && verificationAfter?.remaining_gap !== verificationBefore?.remaining_gap) {
    return 'submission changed the visible state';
  }
  if (['CLICK_AX', 'PRESS_AX', 'CDP_CLICK'].includes(action?.kind) && beforeTitle === afterTitle && beforeSurface === afterSurface && beforeUrl === afterUrl) {
    return 'no visible change after click';
  }
  if (Array.isArray(after?.interactive_candidates) && Array.isArray(before?.interactive_candidates) && after.interactive_candidates.length > before.interactive_candidates.length) {
    return 'new interactive candidates appeared';
  }
  if (verificationBefore?.remaining_gap !== verificationAfter?.remaining_gap) {
    return `remaining gap changed to: ${verificationAfter?.remaining_gap || 'complete'}`;
  }
  if (after?.visual_change_summary) {
    return after.visual_change_summary;
  }
  return 'no visible change after action';
}

function getRecentFailures(history = []) {
  return history
    .filter((entry) => entry && entry.error)
    .slice(-4)
    .map((entry) => ({
      action: entry.action?.kind || '',
      stage: entry.stage || '',
      error: entry.error,
      effect_summary: entry.effect_summary || '',
      remaining_gap: entry.remaining_gap || ''
    }));
}

function nextFailureReason({ action = {}, currentStage = '', resultMsg = {}, goalBundle = {}, observationChanged = true, staleObservationCount = 0 } = {}) {
  const message = String(resultMsg.message || resultMsg.error || '').toLowerCase();
  if (resultMsg.status === 'error') {
    if (/browser_target_not_found/i.test(message)) return 'browser_target_not_found';
    if (/cdp_navigation_failed/i.test(message)) return 'cdp_navigation_failed';
    if (/browser_ax_blind_spot/i.test(message)) return 'browser_ax_blind_spot';
    if (/cross_app_handoff_failed/i.test(message)) return 'cross_app_handoff_failed';
    if (/timed out waiting/i.test(message)) {
      return currentStage === 'loading' ? 'stuck_waiting_for_load' : 'stale_observation';
    }
    if (/ax element not found/i.test(message)) {
      if (goalBundle.surface_goal === 'web_search' && goalBundle.ordinal_target && currentStage === 'clicking') return 'ordinal_result_not_found';
      return currentStage === 'clicking' ? 'click_target_ambiguous' : 'no_click_target_found';
    }
    return 'action_error';
  }
  if (!observationChanged && ['CLICK_AX', 'PRESS_AX', 'CDP_CLICK'].includes(action.kind)) return 'click_had_no_effect';
  if (!observationChanged && staleObservationCount >= 2) return 'stale_observation';
  return null;
}

function buildObservationOptions({
  goalBundle = {},
  history = [],
  staleObservationCount = 0,
  latestObservation = null,
  pendingVisionRequest = null
} = {}) {
  const screenshotEveryStep = String(process.env.AGENT_SCREENSHOT_EVERY_STEP || 'true').toLowerCase() !== 'false';
  const lastHistory = history[history.length - 1] || null;
  const browserSurface = /search|browser|auth/.test(String(latestObservation?.surface_type || ''));
  const forceScreenshot = staleObservationCount > 0
    || Boolean(lastHistory && lastHistory.effect_summary === 'no visible change after action')
    || Boolean(lastHistory && ['ordinal_result_not_found', 'click_target_ambiguous'].includes(lastHistory.error));
  const includeScreenshot = Boolean(forceScreenshot && staleObservationCount >= 1)
    || Boolean(pendingVisionRequest === 'burst')
    || Boolean(goalBundle.surface_goal === 'web_search' && browserSurface && staleObservationCount >= 2 && (!latestObservation?.interactive_candidates || latestObservation.interactive_candidates.length < 2));

  if (screenshotEveryStep) {
    return {
      includeScreenshot: true,
      forceScreenshot: true,
      forceWindowClip: Boolean(pendingVisionRequest === 'window_clip' && staleObservationCount >= 2),
      visionRequest: pendingVisionRequest || null,
      staleObservation: staleObservationCount >= 1,
      lastActionNoEffect: Boolean(lastHistory && lastHistory.effect_summary === 'no visible change after action')
    };
  }

  return {
    includeScreenshot,
    forceScreenshot: Boolean(forceScreenshot && (pendingVisionRequest === 'burst' || staleObservationCount >= 2)),
    forceWindowClip: Boolean(pendingVisionRequest === 'window_clip' && staleObservationCount >= 2),
    visionRequest: pendingVisionRequest || null,
    staleObservation: staleObservationCount >= 2,
    lastActionNoEffect: Boolean(lastHistory && lastHistory.effect_summary === 'no visible change after action')
  };
}

function inferDirectUrlFromGoal(rawGoal = '') {
  const text = String(rawGoal || '').trim();
  const lower = text.toLowerCase();

  const explicit = text.match(/\bhttps?:\/\/[^\s]+/i);
  if (explicit) return explicit[0];

  const bareDomain = text.match(/\b([a-z0-9-]+\.)+(com|org|net|io|co|ai|dev|app|news|me|fr|uk)\b/i);
  if (bareDomain) return `https://${bareDomain[0]}`;

  const aliasMap = [
    { re: /\bnytimes|new york times|nyt\b/i, url: 'https://www.nytimes.com' },
    { re: /\byoutube\b/i, url: 'https://www.youtube.com' },
    { re: /\bnotion\b/i, url: 'https://www.notion.so' },
    { re: /\bslack\b/i, url: 'https://app.slack.com' },
    { re: /\bgmail\b/i, url: 'https://mail.google.com' },
    { re: /\bcalendar\b/i, url: 'https://calendar.google.com' },
    { re: /\bgithub\b/i, url: 'https://github.com' },
    { re: /\bx|twitter\b/i, url: 'https://x.com' }
  ];
  const hit = aliasMap.find((item) => item.re.test(lower));
  return hit ? hit.url : '';
}

function buildExecutionPlan(task = {}, goalBundle = {}) {
  const rawGoal = String(task.raw_goal || task.transcript || task.ai_draft || task.title || '').trim();
  const lower = rawGoal.toLowerCase();
  const appSet = new Set();
  const browserBootstrap = [];
  const directUrl = inferDirectUrlFromGoal(rawGoal);

  if (/\bchrome|google|gmail|calendar|notion|slack|browser|web\b/.test(lower)) appSet.add('Google Chrome');
  if (/\bslack\b/.test(lower)) appSet.add('Slack');
  if (/\bnotion\b/.test(lower)) appSet.add('Notion');
  if (/\bcalendar|meeting|agenda|event\b/.test(lower)) appSet.add('Calendar');
  if (/\bmail|email|gmail|inbox|draft|reply\b/.test(lower)) appSet.add('Mail');
  if (/\bfinder|file|folder|desktop\b/.test(lower)) appSet.add('Finder');
  if (directUrl) appSet.add('Google Chrome');

  const explicitUrls = Array.isArray(task.action_plan)
    ? task.action_plan
      .map((step) => String(step?.url || '').trim())
      .filter((url) => /^https?:\/\//i.test(url))
    : [];
  explicitUrls.forEach((url) => {
    browserBootstrap.push({
      kind: 'OPEN_URL',
      app: 'Google Chrome',
      url,
      background: true,
      reason: 'Bootstrap browser surface in background before the adaptive loop starts.'
    });
  });

  if (directUrl && !browserBootstrap.some((step) => step.url === directUrl)) {
    browserBootstrap.push({
      kind: 'OPEN_URL',
      app: 'Google Chrome',
      url: directUrl,
      background: true,
      reason: 'Bootstrap a direct target URL inferred from the user goal.'
    });
  }

  if (!browserBootstrap.length && goalBundle.surface_goal === 'web_search') {
    browserBootstrap.push({
      kind: 'OPEN_URL',
      app: 'Google Chrome',
      url: 'https://www.google.com',
      background: true,
      reason: 'Bootstrap search surface in background before the adaptive loop starts.'
    });
  }

  return {
    version: 'v1',
    task_id: task.id || null,
    created_at: new Date().toISOString(),
    goal: rawGoal,
    planning_mode: 'reactive_desktop_agent',
    primary_url: directUrl || explicitUrls[0] || '',
    app_bootstrap: Array.from(appSet).map((appName) => ({
      kind: 'LAUNCH_APP_BACKGROUND',
      app: appName,
      reason: 'Pre-launch app in background to reduce handoff latency.'
    })),
    browser_bootstrap: browserBootstrap,
    loop: {
      observe_every_action: true,
      planner_mode: 'react',
      use_dom_for_browser: true,
      use_ax_for_native_apps: true,
      stop_only_on_visible_completion: true,
      max_steps: Number(process.env.AGENT_MAX_STEPS || 18)
    }
  };
}

async function runExecutionPlanBootstrap(executionPlan = {}, { useManagedBrowser = false } = {}) {
  const bootstrapLog = [];

  for (const step of Array.isArray(executionPlan.app_bootstrap) ? executionPlan.app_bootstrap : []) {
    try {
      const result = await executeHybridAction(step, { useManagedBrowser: false, activeDriver: 'ax' });
      bootstrapLog.push({ ...step, status: result?.status || 'success' });
    } catch (error) {
      bootstrapLog.push({ ...step, status: 'error', error: error?.message || String(error) });
    }
  }

  for (const step of Array.isArray(executionPlan.browser_bootstrap) ? executionPlan.browser_bootstrap : []) {
    try {
      const result = await executeHybridAction(step, {
        defaultBrowser: 'Google Chrome',
        useManagedBrowser,
        activeDriver: useManagedBrowser ? 'cdp' : 'ax',
        background: Boolean(step.background)
      });
      bootstrapLog.push({ ...step, status: result?.status || 'success' });
    } catch (error) {
      bootstrapLog.push({ ...step, status: 'error', error: error?.message || String(error) });
    }
  }

  return bootstrapLog;
}

function isManagedBrowserTask(task = {}, goalBundle = {}, latestObservation = null) {
  if (latestObservation?.surface_driver === 'cdp') return true;
  if (task?.browser_mode === 'managed' || task?.use_managed_browser === true) return true;
  const targetSurface = String(task.target_surface || '').toLowerCase();
  const title = String(task.title || '').toLowerCase();
  const aiDraft = String(task.ai_draft || '').toLowerCase();
  const hasBrowserPlan = Array.isArray(task.action_plan) && task.action_plan.some((step) => /https?:\/\//i.test(String(step?.url || '')));
  const explicitManagedTarget = /managed_browser|managed_chrome|dedicated_debug_chrome/.test(targetSurface)
    || /managed chrome|debug chrome/.test(`${title} ${aiDraft}`);
  return explicitManagedTarget || Boolean(task?.managed_browser) || hasBrowserPlan && explicitManagedTarget;
}

function chooseSurfaceDriver({ preferredDriver = 'ax', fallbackManagedBrowser = false } = {}) {
  if (preferredDriver === 'cdp' && fallbackManagedBrowser) return 'cdp';
  if (preferredDriver === 'vision') return 'vision';
  return 'ax';
}

function determineActionDriver(action = {}, currentDriver = 'ax', fallbackManagedBrowser = false) {
  const kind = String(action.kind || '').toUpperCase();
  if (kind.startsWith('CDP_')) return 'cdp';
  if (kind === 'OPEN_URL' && fallbackManagedBrowser) return 'cdp';
  if (['ACTIVATE_APP', 'FOCUS_WINDOW', 'CLICK_AX', 'PRESS_AX', 'FOCUS_AX', 'SET_AX_VALUE', 'SET_VALUE', 'TYPE_TEXT', 'WAIT_FOR_AX', 'SCROLL_AX', 'KEY_PRESS', 'CLICK_POINT'].includes(kind)) {
    return 'ax';
  }
  if (kind === 'READ_UI_STATE') return currentDriver;
  return currentDriver;
}

async function observeHybridState({ activeDriver = 'ax', observeOptions = {}, fallbackManagedBrowser = false } = {}) {
  if (chooseSurfaceDriver({ preferredDriver: activeDriver, fallbackManagedBrowser }) === 'cdp') {
    return await observeManagedBrowserState(observeOptions);
  }
  return await observeDesktopState(observeOptions);
}

async function executeHybridAction(action = {}, context = {}) {
  const kind = String(action.kind || '').toUpperCase();
  const actionDriver = determineActionDriver(action, context.activeDriver || 'ax', context.useManagedBrowser);
  if (actionDriver === 'cdp') {
    return await executeManagedBrowserAction(action, context);
  }
  return await executeDesktopAction(action, context);
}

async function runAgentLoop(task) {
  const taskId = task.id || generateTaskId();
  const goal = buildDesktopGoal(task);
  const goalBundle = normalizeDesktopGoal(goal);
  const useManagedBrowser = isManagedBrowserTask(task, goalBundle, null);
  const executionPlan = buildExecutionPlan(task, goalBundle);
  let activeDriver = useManagedBrowser ? 'cdp' : 'ax';
  const targetUrl = String(task.url || executionPlan.primary_url || '').trim();
  const history = [];
  let latestObservation = null;
  let lastObservationSignature = '';
  let staleObservationCount = 0;
  let navigationProgress = 0;
  let failureReason = null;
  let currentStage = 'loading';
  let remainingGap = goalBundle.completion_check || 'reach the requested destination';
  let completionVerified = false;
  let pendingVisionRequest = null;
  let repeatedLoopRecoveries = 0;
  const maxSteps = Number(executionPlan?.loop?.max_steps || 18);

  console.log(`Starting macOS Accessibility Agent Loop for goal: ${task.title}`);
  if (useManagedBrowser) {
    await ensureManagedBrowser().catch((error) => {
      throw new Error(error.message || 'Failed to start managed Chrome');
    });
  }

  emitPlannerStep({
    taskId,
    step: -1,
    phase: 'execution-plan',
    execution_plan: executionPlan,
    stage: 'planning'
  });

  const bootstrapLog = await runExecutionPlanBootstrap(executionPlan, { useManagedBrowser });
  history.push({
    action: { kind: 'EXECUTION_PLAN_BOOTSTRAP' },
    stage: 'planning',
    reason: 'Pre-launched target apps and browser surfaces in background before autonomous loop.',
    result_status: bootstrapLog.some((item) => item.status === 'error') ? 'partial' : 'success',
    error: null,
    observation_before: null,
    observation_after: null,
    observation_changed: false,
    effect_summary: 'execution plan bootstrap finished',
    remaining_gap: remainingGap,
    completion_verified: false,
    bootstrap_log: bootstrapLog
  });

  if (Array.isArray(task.action_plan) && task.action_plan.length) {
    for (const step of task.action_plan.slice(0, 4)) {
      if (step?.url) {
        const openResult = await executeHybridAction({
          kind: 'OPEN_URL',
          url: step.url,
          app: /safari/i.test(step.target || '') ? 'Safari' : 'Google Chrome'
        }, { defaultBrowser: 'Google Chrome', useManagedBrowser, activeDriver: 'cdp' });
        history.push({ action: { kind: 'OPEN_URL', url: step.url }, result: openResult.status, error: openResult.error || null });
        activeDriver = 'cdp';
        await new Promise((resolve) => setTimeout(resolve, 800));
      }
    }
  } else if (targetUrl) {
    const openResult = await executeHybridAction({
      kind: 'OPEN_URL',
      url: targetUrl,
      app: 'Google Chrome'
    }, { defaultBrowser: 'Google Chrome', useManagedBrowser, activeDriver: useManagedBrowser ? 'cdp' : 'ax' });
    history.push({ action: { kind: 'OPEN_URL', url: targetUrl }, result: openResult.status, error: openResult.error || null });
    if (useManagedBrowser) activeDriver = 'cdp';
    await new Promise((resolve) => setTimeout(resolve, 800));
  }

  let repeatedActionCount = 0;
  let lastActionSig = '';
  for (let step = 0; step < maxSteps; step++) {
    if (!latestObservation) {
      latestObservation = await observeHybridState({
        activeDriver,
        fallbackManagedBrowser: useManagedBrowser,
        observeOptions: buildObservationOptions({
          goalBundle,
          history,
          staleObservationCount,
          latestObservation,
          pendingVisionRequest
        })
      });
      pendingVisionRequest = null;
    }
    const verificationBefore = verifyGoalProgress(latestObservation, goalBundle, history);
    remainingGap = verificationBefore.remaining_gap || remainingGap;
    if (verificationBefore.is_complete) {
      return {
        status: 'success',
        result: verificationBefore.completion_reason,
        execution_plan: executionPlan,
        navigation_progress: navigationProgress,
        failure_reason: null,
        surface_type: latestObservation?.surface_type || '',
        observation_changed: true,
        stage: 'confirming',
        remaining_gap: '',
        completion_verified: true
      };
    }

    const plannerResult = await (async () => {
      const plannerContext = {
        goal_bundle: goalBundle,
        remaining_gap: remainingGap,
        last_effect_summary: history[history.length - 1]?.effect_summary || '',
        recent_failures: getRecentFailures(history),
        current_stage: currentStage,
        observation_before: history[history.length - 1]?.observation_before || null,
        observation_after: history[history.length - 1]?.observation_after || null,
        observation_changed: Boolean(history[history.length - 1]?.observation_changed)
      };
      const r = planNextAction(goal, history, latestObservation, plannerContext);
      if (r && typeof r.on === 'function') {
        const emitter = r;
        emitter.on('chunk', (chunk) => emitPlannerStep({ taskId, step, chunk, phase: 'thinking' }));
        const finalText = await new Promise((resolve, reject) => {
          emitter.on('done', (txt) => resolve(txt));
          emitter.on('error', (err) => reject(err));
        });
        try {
          const clean = finalText.replace(/^```json/i, '').replace(/```$/g, '').trim();
          return JSON.parse(clean);
        } catch (e) {
          throw new Error('Failed to parse planner JSON: ' + e.message + '\n' + finalText.slice(0, 200));
        }
      }
      return r;
    })();
    const action = plannerResult;
    if (!action || !action.kind) {
      latestObservation = null;
      continue;
    }
    pendingVisionRequest = action.vision_request || null;
    console.log(`Step ${step} - Planned action:`, action && action.kind ? action.kind : typeof action);
    currentStage = ['READ_UI_STATE', 'CDP_GET_TREE'].includes(action.kind) ? 'reading'
      : ['TYPE_TEXT', 'SET_VALUE', 'SET_AX_VALUE', 'CDP_TYPE'].includes(action.kind) ? 'typing'
      : ['KEY_PRESS', 'CDP_KEY_PRESS'].includes(action.kind) ? 'submitting'
      : ['CLICK_AX', 'PRESS_AX', 'CDP_CLICK'].includes(action.kind) ? 'clicking'
      : ['SCROLL_AX', 'CDP_SCROLL'].includes(action.kind) ? 'scrolling'
      : ['WAIT_FOR_AX', 'CDP_WAIT_FOR', 'CDP_NAVIGATE', 'OPEN_URL'].includes(action.kind) ? 'loading'
      : action.kind === 'DONE' ? 'confirming'
      : 'loading';
    emitPlannerStep({
      taskId,
      step,
      phase: 'planned',
      action: action.kind,
      driver: latestObservation?.surface_driver || activeDriver,
      reason: action.reason || null,
      app: latestObservation?.frontmost_app || '',
      window: latestObservation?.window_title || '',
      surface_type: latestObservation?.surface_type || '',
      stage: currentStage,
      goal_bundle: goalBundle,
      remaining_gap: remainingGap,
      completion_verified: false,
      perception_mode: latestObservation?.perception_mode || 'ax_tree',
      vision_used: Boolean(latestObservation?.vision_used),
      vision_mode: latestObservation?.vision_mode || 'ax_only'
    });

    const actionSig = JSON.stringify(action);
    if (actionSig === lastActionSig) {
      repeatedActionCount += 1;
    } else {
      repeatedActionCount = 0;
      lastActionSig = actionSig;
    }
    if (repeatedActionCount >= 3) {
      if (repeatedLoopRecoveries < 1) {
        repeatedLoopRecoveries += 1;
        history.push({
          action,
          stage: currentStage,
          reason: action.reason || '',
          result_status: 'recovered',
          error: 'repeated_action_loop',
          observation_before: summarizeObservationForPlanner(latestObservation),
          observation_after: summarizeObservationForPlanner(latestObservation),
          observation_changed: false,
          effect_summary: 'repeated action loop detected; forcing richer observation before retrying',
          remaining_gap: remainingGap,
          completion_verified: false
        });
        pendingVisionRequest = 'window_clip';
        latestObservation = null;
        lastActionSig = '';
        repeatedActionCount = 0;
        continue;
      }
      console.warn(`Step ${step} - repeated action detected, aborting after recovery attempt`);
      failureReason = 'repeated_action_loop';
      break;
    }

    if (action.kind === 'DONE') {
      const doneVerification = verifyGoalProgress(latestObservation, goalBundle, history);
      if (doneVerification.is_complete) {
        console.log(`Agent finished: ${doneVerification.completion_reason || action.message}`);
        return {
          status: 'success',
          result: doneVerification.completion_reason || action.message,
          execution_plan: executionPlan,
          navigation_progress: navigationProgress,
          failure_reason: null,
          surface_type: latestObservation?.surface_type || '',
          observation_changed: staleObservationCount === 0,
          stage: 'confirming',
          remaining_gap: '',
          completion_verified: true
        };
      }
      remainingGap = doneVerification.remaining_gap || remainingGap;
      history.push({
        action,
        stage: 'confirming',
        reason: action.reason || action.message || '',
        result_status: 'rejected',
        error: 'completion_not_verified',
        observation_before: summarizeObservationForPlanner(latestObservation),
        observation_after: summarizeObservationForPlanner(latestObservation),
        observation_changed: false,
        effect_summary: 'completion check failed; task is not done yet',
        remaining_gap: remainingGap,
        completion_verified: false
      });
      emitPlannerStep({
        taskId,
        step,
        phase: 'executed',
        action: action.kind,
        result: 'rejected',
        reason: action.reason || action.message || null,
        app: latestObservation?.frontmost_app || '',
        window: latestObservation?.window_title || '',
        surface_type: latestObservation?.surface_type || '',
        observation_changed: false,
        navigation_progress: navigationProgress,
        failure_reason: null,
        stage: 'confirming',
        effect_summary: 'completion check failed; continuing',
        remaining_gap: remainingGap,
        completion_verified: false,
        perception_mode: latestObservation?.perception_mode || 'ax_tree',
        vision_used: Boolean(latestObservation?.vision_used),
        vision_mode: latestObservation?.vision_mode || 'ax_only'
      });
      latestObservation = null;
      continue;
    }

    const observationBefore = latestObservation;
    const actionObserveOptions = buildObservationOptions({
      goalBundle,
      history,
      staleObservationCount,
      latestObservation: observationBefore,
      pendingVisionRequest
    });
    const actionDriver = determineActionDriver(action, activeDriver, useManagedBrowser);
    const resultMsg = await executeHybridAction(action, {
      defaultBrowser: 'Google Chrome',
      defaultApp: task.target_surface || null,
      observeOptions: actionObserveOptions,
      useManagedBrowser,
      activeDriver
    }).catch((error) => ({
      status: 'error',
      message: error.message,
      error: error.message
    }));

    if (resultMsg.status === 'error') {
      console.error(`Step ${step} failed:`, resultMsg.message || resultMsg.error);
    }

    const followupObservationOptions = buildObservationOptions({
      goalBundle,
      history,
      staleObservationCount,
      latestObservation: observationBefore,
      pendingVisionRequest
    });
    activeDriver = actionDriver;
    latestObservation = ((action.kind === 'READ_UI_STATE' || action.kind === 'CDP_GET_TREE') && pendingVisionRequest)
      ? await observeHybridState({ activeDriver, fallbackManagedBrowser: useManagedBrowser, observeOptions: followupObservationOptions })
      : (resultMsg.observation || await observeHybridState({ activeDriver, fallbackManagedBrowser: useManagedBrowser, observeOptions: followupObservationOptions }));
    pendingVisionRequest = null;
    const observationSignature = JSON.stringify({
      app: latestObservation?.frontmost_app || '',
      window: latestObservation?.window_title || '',
      surface: latestObservation?.surface_type || '',
      text: String(latestObservation?.text_sample || '').slice(0, 400),
      elements: Array.isArray(latestObservation?.visible_elements)
        ? latestObservation.visible_elements.slice(0, 8).map((item) => `${item.index}:${item.role}:${item.name || item.description || item.value || ''}`)
        : []
    });
    const observationChanged = observationSignature !== lastObservationSignature;
    if (observationChanged) {
      navigationProgress += 1;
      staleObservationCount = 0;
      lastObservationSignature = observationSignature;
    } else {
      staleObservationCount += 1;
    }
    const stepFailureReason = nextFailureReason({
      action,
      currentStage,
      resultMsg,
      goalBundle,
      observationChanged,
      staleObservationCount
    });
    const provisionalEntry = {
      action,
      stage: currentStage,
      reason: action.reason || '',
      result_status: resultMsg.status,
      error: stepFailureReason || resultMsg.message || resultMsg.error || null,
      observation_before: summarizeObservationForPlanner(observationBefore),
      observation_after: summarizeObservationForPlanner(latestObservation),
      observation_changed: observationChanged,
      effect_summary: '',
      remaining_gap: '',
      completion_verified: false
    };
    const verificationAfter = verifyGoalProgress(latestObservation, goalBundle, [...history, provisionalEntry]);
    completionVerified = verificationAfter.is_complete;
    remainingGap = verificationAfter.remaining_gap || '';
    const effectSummary = buildEffectSummary(observationBefore, latestObservation, action, verificationBefore, verificationAfter);
    provisionalEntry.effect_summary = effectSummary;
    provisionalEntry.remaining_gap = remainingGap;
    provisionalEntry.completion_verified = completionVerified;
    history.push(provisionalEntry);
    failureReason = stepFailureReason;
    emitPlannerStep({
      taskId,
      step,
      phase: 'executed',
      action: action.kind,
      driver: latestObservation?.surface_driver || activeDriver,
      result: resultMsg.status,
      reason: action.reason || null,
      app: latestObservation?.frontmost_app || '',
      window: latestObservation?.window_title || '',
      surface_type: latestObservation?.surface_type || '',
      observation_changed: observationChanged,
      navigation_progress: navigationProgress,
      failure_reason: stepFailureReason,
      stage: currentStage,
      effect_summary: effectSummary,
      remaining_gap: remainingGap,
      completion_verified: completionVerified,
      perception_mode: latestObservation?.perception_mode || 'ax_tree',
      vision_used: Boolean(latestObservation?.vision_used),
      vision_mode: latestObservation?.vision_mode || 'ax_only',
      visual_change_summary: latestObservation?.visual_change_summary || '',
      target_confidence: latestObservation?.target_confidence ?? null,
      timeline_thumbnail: latestObservation?.screenshot?.present ? {
        data_url: latestObservation.screenshot.data_url,
        captured_at: latestObservation.screenshot.captured_at
      } : null
    });
    if (completionVerified) {
      return {
        status: 'success',
        result: verificationAfter.completion_reason || 'Task completed',
        execution_plan: executionPlan,
        navigation_progress: navigationProgress,
        failure_reason: null,
        surface_type: latestObservation?.surface_type || '',
        observation_changed: observationChanged,
        stage: 'confirming',
        remaining_gap: '',
        completion_verified: true
      };
    }
    if (stepFailureReason === 'click_had_no_effect') {
      pendingVisionRequest = pendingVisionRequest || 'burst';
      latestObservation = null;
      await new Promise(r => setTimeout(r, 250));
      continue;
    }
    if (staleObservationCount >= 3) {
      if (['SCROLL_AX', 'CDP_SCROLL'].includes(action.kind)) {
        failureReason = goalBundle.surface_goal === 'web_search' ? 'results_not_found' : 'scroll_exhausted';
      } else if (currentStage === 'typing') {
        failureReason = 'search_field_not_found';
      } else {
        failureReason = 'stale_observation';
      }
      break;
    }

    await new Promise(r => setTimeout(r, 450));
  }

  return {
    status: 'error',
    error: failureReason || 'agent-loop-max-steps-reached',
    failure_reason: failureReason || 'agent-loop-max-steps-reached',
    execution_plan: executionPlan,
    navigation_progress: navigationProgress,
    surface_type: latestObservation?.surface_type || '',
    observation_changed: staleObservationCount === 0,
    stage: currentStage,
    remaining_gap: remainingGap,
    completion_verified: false
  };
}

function flushPendingAITasks() {
  pendingAITasks = [];
}

// Helper function to parse text response (fallback)
function parseTextResponse(text) {
  // Simple fallback parsing if JSON fails
  const tasks = [];
  const lines = text.split('\n');

  lines.forEach((line, index) => {
    if (line.includes('-') || line.includes('•')) {
      const taskText = line.replace(/^[-•]\s*/, '').trim();
      if (taskText.length > 0) {
        tasks.push({
          id: generateTaskId(),
          title: taskText,
          description: 'Generated from activity analysis',
          category: 'general',
          priority: 'medium',
          action: 'Complete',
          source: 'ai',
          estimatedTime: 15,
          created: Date.now(),
          completed: false
        });
      }
    }
  });

  return tasks.slice(0, 7); // Limit to 7 tasks
}

// Health check endpoint for extension
oauthApp.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: Date.now(),
    version: '1.0.0'
  });
});

// Puppeteer runner used when extension injection cannot perform an action
async function runPuppeteerTask(task) {
  const start = Date.now();
  const url = task.url || 'about:blank';
  const script = task.script || null;

  // Try to find Chrome executable path: allow override via env
  const chromePath = process.env.CHROME_PATH || null;
  const launchOpts = {
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  };
  if (chromePath) {
    launchOpts.executablePath = chromePath;
  } else {
    launchOpts.channel = 'chrome';
  }

  let browser;
  let page;
  try {
    browser = await puppeteer.launch(launchOpts);
    page = await browser.newPage();
    page.setDefaultNavigationTimeout(30000);
    await page.goto(url, { waitUntil: 'networkidle2' });

    // Detect roadblocks (cookie banners, captcha, login) and pause for user
    try {
      const roadblock = await page.evaluate(() => {
        const hasCaptcha = !!document.querySelector('iframe[src*="recaptcha"], div#g-recaptcha, form[action*="sorry"], input[name="captcha"]');
        const hasLogin = !!document.querySelector('input[type="password"], form[action*="login"], form[action*="signin"]');
        const hasCookies = !!document.querySelector('button#L2AGLb, button[aria-label*="Accept"], button[aria-label*="Agree"], form[action*="consent"] button');
        return { hasCaptcha, hasLogin, hasCookies };
      });
      if (roadblock?.hasCaptcha || roadblock?.hasLogin || roadblock?.hasCookies) {
        await requestUserTakeover('Please complete cookies, captcha, or login in the browser, then click “I’m done, continue”.');
      }
    } catch (_) {}

    if (script) {
      // Evaluate provided script in page context; script should be an async function body or return a value
      const wrapped = `(async function(){ try { ${script}\n } catch(e) { return { status: 'error', error: e.message }; } })()`;
      const res = await page.evaluate(wrapped);
      // Leave the tab open after execution
      return { status: 'success', result: res, durationMs: Date.now() - start };
    }

    // Leave the tab open after execution
    return { status: 'success', result: 'n/a', durationMs: Date.now() - start };
  } catch (err) {
    return { status: 'error', error: err.message, durationMs: Date.now() - start };
  }
}

let sharedPuppeteer = null;
let puppeteerWaiter = null;

function requestUserTakeover(reason) {
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('puppeteer-waiting', { reason });
  }
  if (puppeteerWaiter) return puppeteerWaiter;
  puppeteerWaiter = new Promise((resolve) => {
    puppeteerWaiter._resolve = resolve;
  });
  return puppeteerWaiter;
}

function resolveUserTakeover() {
  if (puppeteerWaiter && puppeteerWaiter._resolve) {
    puppeteerWaiter._resolve(true);
  }
  puppeteerWaiter = null;
}

async function getSharedBrowser() {
  if (sharedPuppeteer) return sharedPuppeteer;
  const chromePath = process.env.CHROME_PATH || null;
  const launchOpts = {
    headless: false,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  };
  if (chromePath) {
    launchOpts.executablePath = chromePath;
  } else {
    launchOpts.channel = 'chrome';
  }
  sharedPuppeteer = await puppeteer.launch(launchOpts);
  return sharedPuppeteer;
}

async function runPuppeteerGoogleTest(queryOverride) {
  const start = Date.now();
  try {
    const browser = await getSharedBrowser();
    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(30000);

    await page.goto('https://www.google.com/ncr', { waitUntil: 'domcontentloaded' });

    // Handle consent overlays if present
    try {
      const consentButtons = [
        'button#L2AGLb',
        'button[aria-label="Accept all"]',
        'button[aria-label*="Accept"]',
        'button[aria-label*="Agree"]',
        'form[action*="consent"] button'
      ];
      for (const sel of consentButtons) {
        const btn = await page.$(sel);
        if (btn) { await btn.click(); break; }
      }
    } catch (_) {}

    await page.waitForSelector('input[name="q"], textarea[name="q"]', { timeout: 15000 });
    const input = await page.$('input[name="q"]') || await page.$('textarea[name="q"]');
    if (!input) throw new Error('Search input not found');
    const searchQuery = (queryOverride && String(queryOverride).trim()) ? String(queryOverride).trim() : 'hello';
    await input.click();
    await input.type(searchQuery, { delay: 30 });
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded' }).catch(() => {}),
      page.keyboard.press('Enter')
    ]);

    const waitForResults = async () => {
      try {
        await page.waitForSelector('h3', { timeout: 15000 });
        return true;
      } catch (_) {
        return false;
      }
    };

    let hasResults = await waitForResults();
    if (!hasResults) {
      // If captcha/robot check is present, pause until user completes it
      const captchaSelector = 'iframe[src*="recaptcha"], div#g-recaptcha, form[action*="sorry"], input[name="captcha"]';
      const captcha = await page.$(captchaSelector);
      if (captcha) {
        console.log('Captcha detected. Waiting for user to complete...');
        await requestUserTakeover('Captcha detected on Google. Please complete it, then click “I’m done, continue”.');
        await page.waitForFunction(() => !!document.querySelector('h3'), { timeout: 0 });
        hasResults = true;
      }
    }

    if (hasResults) {
      const results = await page.$$('h3');
      if (results.length >= 2) {
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded' }).catch(() => {}),
          results[1].click()
        ]);
      } else if (results.length === 1) {
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded' }).catch(() => {}),
          results[0].click()
        ]);
      } else {
        // Fallback to generic organic links
        const links = await page.$$('a[href]');
        if (links.length >= 2) {
          await Promise.all([
            page.waitForNavigation({ waitUntil: 'domcontentloaded' }).catch(() => {}),
            links[1].click()
          ]);
        } else if (links.length === 1) {
          await Promise.all([
            page.waitForNavigation({ waitUntil: 'domcontentloaded' }).catch(() => {}),
            links[0].click()
          ]);
        }
      }
    }

    if (page.waitForTimeout) {
      await page.waitForTimeout(2000);
    } else {
      await new Promise(r => setTimeout(r, 2000));
    }
    await page.close();
    // Do not close the Chrome app after test
    return { status: 'success', durationMs: Date.now() - start };
  } catch (err) {
    // Do not close the Chrome app on error either
    return { status: 'error', error: err.message, durationMs: Date.now() - start };
  }
}

// API endpoint for Chrome extension data
oauthApp.post('/api/sync-extension-data', async (req, res) => {
  try {
    const { urls, stats, timestamp, source } = req.body;

    console.log(`Received ${urls?.length || 0} URLs from ${source}`);

    // Store extension data
    if (urls && Array.isArray(urls)) {
      // Store in memory or database for AI processing
      global.extensionData = {
        urls: urls,
        stats: stats,
        lastReceived: timestamp,
        source: source
      };

      // Update last sync time
      store.set('lastExtensionSync', timestamp);

      // Trigger proactive task generation if AI is enabled
      if (store.get('aiEnabled') !== false) {
        // Use the extension data for AI task generation
        console.log('Using extension data for proactive task generation');
      }
    }

    res.json({
      success: true,
      message: `Processed ${urls?.length || 0} URLs`,
      timestamp: Date.now()
    });

  } catch (error) {
    console.error('Error processing extension data:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Health check endpoint for extension
oauthApp.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: Date.now(),
    version: '1.0.0'
  });
});

// IPC handlers
ipcMain.handle('start-google-auth', async () => {
  const googleScopes = [
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/calendar.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
  ];
  if (RELATIONSHIP_FEATURE_ENABLED) googleScopes.push('https://www.googleapis.com/auth/contacts.readonly');
  const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?` +
    `client_id=${GOOGLE_CLIENT_ID}&` +
    `redirect_uri=${encodeURIComponent(getRedirectUri())}&` +
    `response_type=code&` +
    `scope=${encodeURIComponent(googleScopes.join(' '))}&` +
    `access_type=offline`;

  const { shell } = require('electron');

  // Clear any existing tokens before starting a fresh flow
  store.delete('googleTokens');

  // Open the Google Auth page in the user's default external browser
  await shell.openExternal(authUrl);

  return new Promise((resolve, reject) => {
    let elapsed = 0;
    const interval = setInterval(() => {
      elapsed += 1000;
      const tokens = store.get('googleTokens');
      if (tokens) {
        clearInterval(interval);
        resolve(tokens);
      } else if (elapsed > 5 * 60 * 1000) {
        // Timeout after 5 minutes
        clearInterval(interval);
        reject(new Error('Authentication timed out after 5 minutes'));
      }
    }, 1000);
  });
});

// Suggestions persistence (simple electron-store backed)
ipcMain.handle('get-suggestions', async () => {
  try {
    const state = getStoredRadarState();
    return Array.isArray(state?.allSignals) ? state.allSignals.slice(0, MAX_PRACTICAL_SUGGESTIONS * 2) : [];
  } catch (e) {
    console.error('Failed to get suggestions:', e);
    return [];
  }
});

ipcMain.handle('save-suggestions', async (event, suggestions) => {
  try {
    const current = getStoredRadarState();
    const centralSignals = (Array.isArray(suggestions) ? suggestions : []).filter((item) => String(item.signal_type || '').toLowerCase() === 'central');
    const relationshipSignals = (Array.isArray(suggestions) ? suggestions : []).filter((item) => String(item.signal_type || '').toLowerCase() === 'relationship');
    const todoSignals = (Array.isArray(suggestions) ? suggestions : []).filter((item) => String(item.signal_type || "").toLowerCase() === "todo");
    persistRadarState({
      ...current,
      centralSignals,
      relationshipSignals,
      todoSignals,
      allSignals: [...centralSignals, ...relationshipSignals, ...todoSignals],
      generated_at: new Date().toISOString()
    });
    return true;
  } catch (e) {
    console.error('Failed to save suggestions:', e);
    return false;
  }
});

ipcMain.handle('get-radar-state', async () => {
  return getStoredRadarState();
});

// Debug function to manually trigger suggestions
ipcMain.handle('debug-trigger-suggestions', async () => {
  console.log('[Debug] Manually triggering suggestion engine...');
  await runSuggestionEngineJob({ force: true });
  return { success: true, message: 'Suggestions triggered manually' };
});

ipcMain.handle('trigger-suggestion-refresh', async (_event, payload = {}) => {
  try {
    console.log('[Radar] User-triggered refresh requested');
    const refreshResult = await runSuggestionEngineJob({ force: true }).catch((error) => ({
      success: false,
      error: error?.message || String(error)
    }));
    const radarState = getStoredRadarState();
    const persistentTodos = store.get('persistentTodos') || [];
    return {
      success: !refreshResult?.error,
      radarState,
      suggestions: Array.isArray(radarState?.allSignals) ? radarState.allSignals : [],
      persistentTodos: Array.isArray(persistentTodos) ? persistentTodos.slice(0, 25) : [],
      source: 'chat-backed-radar',
      payload,
      refresh: refreshResult
    };
  } catch (error) {
    console.error('[Radar] User-triggered refresh failed:', error);
    return {
      success: false,
      radarState: getStoredRadarState(),
      suggestions: Array.isArray(getStoredRadarState()?.allSignals) ? getStoredRadarState().allSignals : [],
      error: error?.message || String(error)
    };
  }
});

// Run suggestion engine: accepts optional events payload or uses global.extensionData
ipcMain.handle('run-suggestion-engine', async (event, payload) => {
  try {
    // Lazy-load services to avoid startup cost

    const events = payload && payload.events ? payload.events : (global.extensionData && global.extensionData.urls ? global.extensionData.urls : []);

    const openLoops = [];
    const contactProfiles = {};

    for (const ev of events) {
      const res = extractor.processEvent(ev, contactProfiles);
      if (res.contactUpdate) {
        contactProfiles[res.contactUpdate.id] = res.contactUpdate;
      }
      if (res.openLoops && res.openLoops.length) {
        // Attach contact_id from inferred contact only (avoid mixing unrelated summary text).
        res.openLoops.forEach(l => {
          l.contact_id = res.contactUpdate?.id || null;
          openLoops.push(l);
        });
      }
    }

    // For demo, also include any stored message events if present
    const storedMessages = store.get('messageEvents') || [];
    for (const m of storedMessages) {
      const res = extractor.processEvent(m, contactProfiles);
      if (res.contactUpdate) contactProfiles[res.contactUpdate.id] = res.contactUpdate;
      if (res.openLoops) res.openLoops.forEach(l => { l.contact_id = m.from || null; openLoops.push(l); });
    }

    // Generate suggestions
    const userProfile = store.get('userProfile') || { id: 'local_user', learned: {} };
    const suggestions = scoring.generateSuggestionsFromOpenLoops(openLoops, userProfile, contactProfiles, { topK: 12 });

    const final = rankAndLimitSuggestions(suggestions, {
      maxTotal: MAX_PRACTICAL_SUGGESTIONS,
      maxPerCategory: 2,
      maxFollowups: 1,
      now: Date.now()
    });
    store.set('suggestions', final);
    return final;
  } catch (e) {
    console.error('run-suggestion-engine failed:', e);
    return [];
  }
});

// Log automation runs for audit trail
ipcMain.handle('log-automation', async (event, record) => {
  try {
    const logs = store.get('automationLogs') || [];
    const entry = Object.assign({ id: `log_${Date.now()}_${Math.random().toString(36).slice(2,6)}`, timestamp: new Date().toISOString() }, record || {});
    logs.unshift(entry);
    // Keep last 500 entries
    store.set('automationLogs', logs.slice(0,500));
    return entry;
  } catch (e) {
    console.error('Failed to log automation:', e);
    return null;
  }
});

// ── User-defined Scheduled Automations ────────────────────────────────────────

ipcMain.handle('list-automations', async () => {
  try {
    const rows = await db.allQuery(
      `SELECT id, name, description, prompt, interval_minutes, enabled, last_run_at, next_run_at, created_at FROM scheduled_automations ORDER BY created_at DESC`,
      []
    );
    return { success: true, automations: rows };
  } catch (e) {
    console.error('Failed to list automations:', e);
    return { success: false, automations: [] };
  }
});

ipcMain.handle('delete-automation', async (_event, automationId) => {
  try {
    await db.runQuery(`DELETE FROM scheduled_automations WHERE id = ?`, [automationId]);
    return { success: true };
  } catch (e) {
    console.error('Failed to delete automation:', e);
    return { success: false };
  }
});

ipcMain.handle('toggle-automation', async (_event, automationId, enabled) => {
  try {
    await db.runQuery(`UPDATE scheduled_automations SET enabled = ? WHERE id = ?`, [enabled ? 1 : 0, automationId]);
    return { success: true };
  } catch (e) {
    return { success: false };
  }
});

ipcMain.handle('open-url', async (_event, url) => {
  const safe = String(url || '').trim();
  if (!safe || !/^https?:\/\//.test(safe)) return { success: false, error: 'invalid_url' };
  await shell.openExternal(safe).catch(() => {});
  return { success: true };
});

// Automation scheduler: polls every minute for due automations
let automationSchedulerTimer = null;
let recursiveImprovementTimer = null;

function recursiveImprovementEnabled() {
  const env = String(process.env.RECURSIVE_AGENT_ENABLED || 'false').toLowerCase();
  return env !== 'false' && env !== '0' && env !== 'no';
}

function recursiveImprovementIntervalMs() {
  const mins = Math.max(10, Number(process.env.RECURSIVE_AGENT_INTERVAL_MINUTES || 45));
  return mins * 60 * 1000;
}

async function runRecursiveImprovementOnce(trigger = 'scheduled') {
  if (!recursiveImprovementEnabled()) return { skipped: true, reason: 'disabled' };
  if (shouldDeferBackgroundWork('RecursiveImprovement')) return { skipped: true, reason: 'active_use' };
  try {
    const apiKey = process.env.DEEPSEEK_API_KEY || null;
    const cycle = await runRecursiveImprovementCycle({ apiKey });

    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('recursive-improvement-cycle', {
        trigger,
        cycle
      });
    }

    return { skipped: false, cycle };
  } catch (error) {
    console.warn('[RecursiveImprovement] cycle failed:', error?.message || error);
    return { skipped: false, error: error?.message || String(error) };
  }
}

function startRecursiveImprovementLoop() {
  if (!recursiveImprovementEnabled()) {
    return;
  }
  if (recursiveImprovementTimer) return;

  const intervalMs = recursiveImprovementIntervalMs();
  recursiveImprovementTimer = setInterval(() => {
    runRecursiveImprovementOnce('scheduled').catch(() => null);
  }, intervalMs);

  setTimeout(() => {
    runRecursiveImprovementOnce('bootstrap').catch(() => null);
  }, STARTUP_HEAVY_JOB_DELAY_MS);

  console.log(`[RecursiveImprovement] Started (interval: ${Math.round(intervalMs / 60000)}m)`);
}

function startAutomationScheduler() {
  if (automationSchedulerTimer) return;
  automationSchedulerTimer = setInterval(async () => {
    try {
      const now = new Date().toISOString();
      const due = await db.allQuery(
        `SELECT * FROM scheduled_automations WHERE enabled = 1 AND (next_run_at IS NULL OR next_run_at <= ?)`,
        [now]
      ).catch(() => []);

      for (const automation of due) {
        try {
          const apiKey = process.env.DEEPSEEK_API_KEY;
          const result = await answerChatQuery({
            apiKey,
            query: String(automation.prompt || '').trim(),
            options: {
              standing_notes: '',
              mode: 'chat',
              recursion_enabled: true,
              from_automation: true,
              automation_id: automation.id,
              automation_name: automation.name
            }
          });

          // Persist result to memory as a raw event
          
          await ingestRawEvent({
            type: 'automation_result',
            source: 'scheduled_automation',
            text: `[Automation: ${automation.name}] ${result?.content || ''}`,
            metadata: { automation_id: automation.id, automation_name: automation.name }
          }).catch(() => null);

          // Push to renderer if window is open
          if (mainWindow && mainWindow.webContents) {
            mainWindow.webContents.send('automation-result', {
              automation_id: automation.id,
              name: automation.name,
              content: result?.content || '',
              ui_blocks: result?.ui_blocks || [],
              ran_at: now
            });
          }
        } catch (runErr) {
          console.error(`[Automation] Failed to run "${automation.name}":`, runErr.message);
        }

        // Schedule next run regardless of success/failure
        const nextRun = new Date(Date.now() + automation.interval_minutes * 60 * 1000).toISOString();
        await db.runQuery(
          `UPDATE scheduled_automations SET last_run_at = ?, next_run_at = ? WHERE id = ?`,
          [now, nextRun, automation.id]
        ).catch(() => null);
      }
    } catch (e) {
      console.error('[AutomationScheduler] Tick error:', e.message);
    }
  }, 60 * 1000); // check every minute
  console.log('[AutomationScheduler] Started');
}

ipcMain.handle('run-recursive-improvement', async () => {
  return await runRecursiveImprovementOnce('manual');
});

ipcMain.handle('get-recursive-improvement-status', async () => {
  try {
    const latest = await getLatestRecursiveImprovementLog();
    return {
      enabled: recursiveImprovementEnabled(),
      interval_minutes: Math.round(recursiveImprovementIntervalMs() / 60000),
      latest
    };
  } catch (error) {
    return {
      enabled: recursiveImprovementEnabled(),
      interval_minutes: Math.round(recursiveImprovementIntervalMs() / 60000),
      latest: null,
      error: error?.message || String(error)
    };
  }
});

ipcMain.handle('sync-google-data', async (_event, payload = {}) => {
  return await fullGoogleSync(payload || {});
});

ipcMain.handle('get-google-sync-status', async () => {
  return getGoogleSyncStatusSnapshot();
});

// Full GSuite sync flow extracted so it can be invoked after OAuth and by IPC
async function fullGoogleSync({ since, forceHistoricalBackfill = false, mode = 'incremental_sync', includeContacts = true } = {}) {
  includeContacts = Boolean(includeContacts && RELATIONSHIP_FEATURE_ENABLED);
  
  const apiKey = process.env.DEEPSEEK_API_KEY;

  const sendProgress = (phase, done, total) => {
    store.set('googleSyncHealth', {
      ...(store.get('googleSyncHealth') || {}),
      phase,
      done,
      total,
      mode,
      lastProgressAt: new Date().toISOString()
    });
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('initial-sync-progress', { phase, done, total });
    }
  };

  try {
    sendProgress('Fetching GSuite Data...', 0, 100);
    const existingGoogleData = store.get('googleData') || {};
    const sinceFloor = forceHistoricalBackfill
      ? GOOGLE_SYNC_BASELINE_ISO
      : (since || existingGoogleData.lastSync || GOOGLE_SYNC_BASELINE_ISO);
    const googleDelta = await getGoogleData({ since: sinceFloor, includeContacts });
    const syncMeta = googleDelta._meta || {};
    const shouldAdvanceLastSync = !syncMeta.hardFailure;

    const googleData = mergeGoogleData(existingGoogleData, {
      ...googleDelta,
      lastSync: shouldAdvanceLastSync ? new Date().toISOString() : (existingGoogleData.lastSync || null)
    });
    store.set('googleData', googleData);
    store.set('googleSyncHealth', {
      ...(store.get('googleSyncHealth') || {}),
      lastCheckAt: new Date().toISOString(),
      cursorAdvanced: shouldAdvanceLastSync,
      syncMeta,
      mode,
      lastBackfillAt: forceHistoricalBackfill ? new Date().toISOString() : (store.get('googleSyncHealth') || {}).lastBackfillAt || null
    });

    // --- L1: Ingestion to SQLite ---
    const allUnified = [
      ...(googleDelta.gmail || []).map(m => ({
        type: 'email',
        timestamp: m.timestamp,
        source: 'Gmail',
        text: [
          m.from ? `From: ${m.from}` : '',
          m.to ? `To: ${Array.isArray(m.to) ? m.to.join(', ') : m.to}` : '',
          m.subject ? `Subject: ${m.subject}` : '',
          m.snippet || ''
        ].filter(Boolean).join('\n'),
        metadata: { id: m.id, threadId: m.threadId, from: m.from, to: m.to, labels: m.labelIds, app: 'Gmail', subject: m.subject }
      })),
      ...(googleDelta.calendar || []).map(c => ({
        type: 'calendar_event',
        timestamp: c.start_time,
        source: 'Calendar',
        text: [
          c.summary ? `Event: ${c.summary}` : '',
          c.start_time ? `Start: ${c.start_time}` : '',
          c.end_time ? `End: ${c.end_time}` : '',
          Array.isArray(c.attendees) && c.attendees.length
            ? `Attendees: ${(c.attendees || []).map(a => a.email || a).join(', ')}`
            : '',
          c.description || ''
        ].filter(Boolean).join('\n'),
        metadata: {
          id: `cal_${c.id || 'evt'}_${(c.updated ? new Date(c.updated).getTime() : 0) || (c.start_time ? new Date(c.start_time).getTime() : Date.now())}`,
          original_event_id: c.id,
          attendees: c.attendees,
          updated: c.updated,
          app: 'Calendar',
          summary: c.summary
        }
      })),
      ...((includeContacts ? (googleDelta.contacts || []) : [])).map(c => ({
        type: 'contact_profile',
        timestamp: new Date().toISOString(),
        source: 'Google Contacts',
        text: [
          c.name ? `Name: ${c.name}` : '',
          c.first_name ? `First name: ${c.first_name}` : '',
          c.last_name ? `Last name: ${c.last_name}` : '',
          c.company ? `Company: ${c.company}` : '',
          c.role ? `Role: ${c.role}` : '',
          c.emails?.length ? `Emails: ${c.emails.join(', ')}` : '',
          c.phones?.length ? `Phones: ${c.phones.join(', ')}` : '',
          c.notes || ''
        ].filter(Boolean).join('\n'),
        metadata: {
          ...c,
          app: 'Google Contacts'
        }
      })),
      ...(googleDelta.drive || []).map(d => ({
        type: d.mimeType?.includes('spreadsheet') ? 'spreadsheet' : 'doc',
        timestamp: d.last_modified,
        source: 'Drive',
        text: `Document: ${d.name}\nMimeType: ${d.mimeType}\nShared: ${(d.shared_with || []).join(', ')}`,
        metadata: { id: d.id, webViewLink: d.webViewLink, mimeType: d.mimeType }
      }))
    ];

    console.log(`[fullSync] Ingesting ${allUnified.length} new raw events...`);
    for (let i = 0; i < allUnified.length; i++) {
      await ingestRawEvent(allUnified[i]);
      if (i % 5 === 0) sendProgress('Ingesting raw data...', i, allUnified.length);
    }

    let contactsMerged = 0;
    if (RELATIONSHIP_FEATURE_ENABLED && includeContacts && Array.isArray(googleDelta.contacts) && googleDelta.contacts.length) {
      sendProgress('Merging Google Contacts...', 0, googleDelta.contacts.length);
      const contactMerge = await syncGoogleContactsIntoRelationshipGraph({ contacts: googleDelta.contacts, force: true });
      contactsMerged = Number(contactMerge?.imported || contactMerge?.merged || 0);
      store.set('googleSyncHealth', {
        ...(store.get('googleSyncHealth') || {}),
        contactsMerged
      });
    }

    // --- L2 - L5: Memory Graph Build ---
    sendProgress('Building Episode Memory (L2)...', 0, 1);
    await engine.runEpisodeJob(apiKey || null);
    await runRelationshipGraphUpdate({ backfill: true }).catch((e) => console.warn('[fullGoogleSync] Relationship graph update failed:', e?.message || e));
    await runSemanticWindowGeneration().catch((e) => console.warn('[fullGoogleSync] Semantic window generation failed:', e?.message || e));

    if (apiKey) {
      sendProgress('Synthesizing Core Insights (L4/L5)...', 0, 1);
      await engine.runWeeklyInsightJob(apiKey);
    }

    await persistGoogleSyncProjectNote();
    sendProgress('Sync Complete', 100, 100);
    store.set('initialSyncDone', true);
    store.set('googleSyncHealth', {
      ...(store.get('googleSyncHealth') || {}),
      mode,
      phase: 'Sync Complete',
      done: 100,
      total: 100,
      rawIngested: allUnified.length,
      contactsMerged,
      episodesBuilt: 1,
      semanticNodesUpdated: 1
    });
    return googleData;
  } catch (error) {
    console.error('Error syncing Google Data:', error);
    sendProgress('Error', 0, 0);
    throw error;
  }
}

ipcMain.handle('get-google-data', () => {
  return store.get('googleData') || { gmail: [], calendar: [], contacts: [], drive: [] };
});

// ── Memory Graph Status & Chat Integration ───────────────────────────────

ipcMain.handle('get-memory-graph-status', async () => {
  try {

    const eventCount = await db.getQuery(`SELECT COUNT(*) as count FROM events`).catch(() => ({ count: 0 }));
    const nodeCounts = await db.allQuery(`SELECT layer, COUNT(*) as count FROM memory_nodes GROUP BY layer`).catch(() => []);
    const edgeCount = await db.getQuery(`SELECT COUNT(*) as count FROM memory_edges`).catch(() => ({ count: 0 }));
    const sourceCounts = await db.allQuery(`SELECT source as source_type, COUNT(*) as count FROM events GROUP BY source`).catch(() => []);
    const typedRawCounts = await db.allQuery(
      `SELECT source as source_type, COUNT(*) as count, MAX(timestamp) as latest
       FROM events
       GROUP BY source`
    ).catch(() => []);
    const retrievalDocCount = await db.getQuery(`SELECT COUNT(*) as count FROM retrieval_docs`).catch(() => ({ count: 0 }));
    const suggestionCount = await db.getQuery(`SELECT COUNT(*) as count FROM suggestion_artifacts`).catch(() => ({ count: 0 }));
    const graphVersion = await db.getQuery(
      `SELECT version, status, completed_at FROM graph_versions ORDER BY started_at DESC LIMIT 1`
    ).catch(() => null);
    const health = store.get('memoryGraphHealth') || {};

    const counts = {
      events: eventCount.count || 0,
      nodes: nodeCounts.reduce((acc, row) => {
        acc[row.layer] = row.count;
        return acc;
      }, {}),
      edges: edgeCount.count || 0,
      retrievalDocs: retrievalDocCount?.count || 0,
      suggestions: suggestionCount?.count || 0,
      rawSources: sourceCounts.reduce((acc, row) => {
        acc[row.source_type || 'unknown'] = row.count || 0;
        return acc;
      }, {}),
      typedRaw: typedRawCounts.reduce((acc, row) => {
        acc[row.source_type] = { count: row.count || 0, latest: row.latest || null };
        return acc;
      }, {}),
      gmailRawEvents: (sourceCounts.find((row) => String(row.source_type || '').toLowerCase().includes('email')) || {}).count || 0
    };

    // Get processing status
    const status = {
      episodeJobLocked: episodeJobLock,
      suggestionJobLocked: suggestionJobLock,
      heavyJobActive: heavyJobState.activeJob,
      processingActive: Boolean(episodeGenerationTimer || suggestionEngineTimer || minutelySyncTimer || minutelySyncInterval || sensorCaptureTimer),
      processorTimersActive: Boolean(episodeGenerationTimer || suggestionEngineTimer),
      lastEpisodeRun: store.get('lastEpisodeRun') || null,
      lastSuggestionRun: store.get('lastSuggestionRun') || null,
      latestGraphVersion: graphVersion || null,
      health
    };

    return { counts, status };
  } catch (error) {
    console.error('[MemoryGraph] Status query failed:', error);
    return { counts: { events: 0, nodes: {}, edges: 0 }, status: { error: error.message } };
  }
});

ipcMain.handle('search-memory-graph', async (event, query, options = {}) => {
  try {
    const { limit = 20, nodeTypes = [], app, date_range: dateRange, data_source: dataSource } = options;
    const appFilters = Array.isArray(app) ? app.map((item) => String(item || '').toLowerCase()).filter(Boolean) : (app ? [String(app).toLowerCase()] : []);
    const startMs = dateRange?.start ? Date.parse(String(dateRange.start)) : null;
    const endMs = dateRange?.end ? Date.parse(String(dateRange.end)) : null;
    const normalizedDataSource = dataSource && dataSource !== 'auto' ? String(dataSource).toLowerCase() : null;
    const parseNodeMetadata = (row) => {
      try { return JSON.parse(row?.metadata || '{}'); } catch (_) { return {}; }
    };
    const getCanonicalNodeTimestamp = (row) => {
      const metadata = parseNodeMetadata(row);
      return (
        metadata.event_time ||
        metadata.occurred_at ||
        metadata.anchor_at ||
        metadata.latest_activity_at ||
        metadata.cluster_anchor_at ||
        metadata.cluster_latest_at ||
        metadata.timestamp ||
        row?.anchor_at ||
        row?.created_at ||
        row?.updated_at ||
        null
      );
    };
    const getCanonicalNodeTsMs = (row) => {
      const ts = Date.parse(String(getCanonicalNodeTimestamp(row) || ''));
      return Number.isFinite(ts) ? ts : 0;
    };
    const rowMatchesNodeFilters = (row) => {
      const metadata = parseNodeMetadata(row);
      if (appFilters.length) {
        const appHay = `${metadata.app || ''} ${(metadata.apps || []).join(' ')} ${metadata.window_title || ''}`.toLowerCase();
        if (!appFilters.some((needle) => appHay.includes(needle))) return false;
      }
      if (Number.isFinite(startMs) || Number.isFinite(endMs)) {
        const ts = getCanonicalNodeTsMs(row);
        if (!Number.isFinite(ts)) return false;
        if (Number.isFinite(startMs) && ts < startMs) return false;
        if (Number.isFinite(endMs) && ts > endMs) return false;
      }
      if (normalizedDataSource) {
        const dsHay = String(metadata.data_source || metadata.source_type_group || '').toLowerCase();
        if (!dsHay.includes(normalizedDataSource)) return false;
      }
      return true;
    };
    const buildFallbackMemoryQuery = () => {
      const appHint = appFilters[0] || '';
      if (appHint) return `${appHint} recent activity open loop`;
      if (dateRange?.start || dateRange?.end) return 'recent activity in selected date range';
      return 'recent activity open loop next step';
    };
    const effectiveQuery = String(query || '').trim() || buildFallbackMemoryQuery();
    const tokens = effectiveQuery
      .toLowerCase()
      .replace(/[^a-z0-9._/-]+/g, ' ')
      .split(/\s+/)
      .map((item) => item.trim())
      .filter((item) => item.length >= 2)
      .slice(0, 8);
    const ftsQuery = tokens.length ? tokens.map((term) => `"${term.replace(/"/g, '""')}"`).join(' OR ') : null;
    let results = [];

    // Router-based retrieval first: query-first, core-first, or hybrid based on intent.
    if (effectiveQuery && effectiveQuery.length >= 2) {
      try {
        const thought = await buildRetrievalThought({
          query: effectiveQuery,
          mode: 'chat',
          dateRange: dateRange || null,
          app: appFilters.length ? appFilters : null
        });
        const routed = await buildHybridGraphRetrieval({
          query: effectiveQuery,
          options: {
            mode: 'chat',
            app: appFilters.length ? appFilters : null,
            date_range: thought?.applied_date_range || dateRange || null,
            source_types: normalizedDataSource ? [normalizedDataSource] : null,
            retrieval_thought: thought
          },
          seedLimit: 12,
          hopLimit: 4
        }).catch(() => null);
        const routedEvidence = Array.isArray(routed?.evidence) ? routed.evidence : [];
        if (routedEvidence.length) {
          results = routedEvidence.slice(0, Math.max(limit * 2, 30)).map((ev, index) => {
            const text = String(ev.text || '').trim();
            const fallbackTitle = text.split('\n')[0].slice(0, 140) || `Memory ${ev.layer || ev.type || index + 1}`;
            return {
              id: ev.node_id || ev.id || `ev_${index}`,
              layer: ev.layer || ev.type || 'memory',
              subtype: ev.subtype || null,
              title: ev.title || fallbackTitle,
              summary: text.slice(0, 220),
              anchor_at: ev.anchor_at || ev.timestamp || null,
              created_at: ev.anchor_at || ev.timestamp || null,
              metadata: JSON.stringify({
                timestamp: ev.anchor_at || ev.timestamp || null,
                occurred_at: ev.anchor_at || ev.timestamp || null,
                anchor_at: ev.anchor_at || null,
                latest_activity_at: ev.latest_activity_at || ev.timestamp || null,
                app: ev.app || null,
                source_type_group: ev.source_type_group || null,
                score: ev.score || null,
                match_reason: ev.reason || null,
                strategy_mode: routed?.strategy?.strategy_mode || null,
                entry_mode: routed?.strategy?.entry_mode || null
              }),
              source_refs: ev.source_refs || [],
              updated_at: ev.latest_activity_at || ev.timestamp || new Date().toISOString()
            };
          });
        }
      } catch (_) {}
    }

    if (!results.length && ftsQuery) {
      const docs = await db.allQuery(
        `SELECT d.node_id
         FROM retrieval_docs_fts
         JOIN retrieval_docs d ON d.doc_id = retrieval_docs_fts.doc_id
         WHERE retrieval_docs_fts MATCH ?
         LIMIT ?`,
        [ftsQuery, Math.max(limit * 3, 40)]
      ).catch(() => []);
      const nodeIds = Array.from(new Set(docs.map((row) => row.node_id).filter(Boolean)));
      if (nodeIds.length) {
        const placeholders = nodeIds.map(() => '?').join(',');
        results = await db.allQuery(
          `SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
           FROM memory_nodes n
           WHERE n.id IN (${placeholders}) AND n.layer IN ('episode', 'semantic')`,
          nodeIds
        );
      }
    }

    if (!results.length) {
      let sql = `
        SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.canonical_text, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
        FROM memory_nodes n
        WHERE n.layer IN ('episode', 'semantic') AND (n.title LIKE ? OR n.summary LIKE ? OR n.canonical_text LIKE ?)
      `;
      const params = [`%${effectiveQuery}%`, `%${effectiveQuery}%`, `%${effectiveQuery}%`];
      if (nodeTypes.length > 0) {
        sql += ` AND n.layer IN (${nodeTypes.map(() => '?').join(',')})`;
        params.push(...nodeTypes);
      }
      sql += ` LIMIT ?`;
      params.push(limit);
      results = await db.allQuery(sql, params);
    }

    if (nodeTypes.length > 0) {
      results = results.filter((row) => nodeTypes.includes(row.layer));
    }

    const hasExtendedFilters = appFilters.length || dateRange || dataSource;
    if (hasExtendedFilters) {
      const chunkWhere = [];
      const chunkParams = [];
      if (effectiveQuery) {
        chunkWhere.push(`text LIKE ?`);
        chunkParams.push(`%${effectiveQuery}%`);
      } else {
        chunkWhere.push(`1 = 1`);
      }
      if (appFilters.length) {
        chunkWhere.push(`(${appFilters.map(() => `LOWER(app) LIKE LOWER(?)`).join(' OR ')})`);
        appFilters.forEach((a) => chunkParams.push(`%${String(a)}%`));
      }
      if (dateRange?.start) {
        chunkWhere.push(`timestamp >= ?`);
        chunkParams.push(dateRange.start);
      }
      if (dateRange?.end) {
        chunkWhere.push(`timestamp <= ?`);
        chunkParams.push(dateRange.end);
      }
      if (dataSource && dataSource !== 'auto') {
        chunkWhere.push(`data_source = ?`);
        chunkParams.push(dataSource);
      }
      const chunkRows = await db.allQuery(
        `SELECT DISTINCT event_id, node_id
         FROM text_chunks
         WHERE ${chunkWhere.join(' AND ')}
         LIMIT ?`,
        [...chunkParams, Math.max(50, limit * 8)]
      );
      const allowed = new Set();
      chunkRows.forEach((r) => {
        if (r.node_id) allowed.add(r.node_id);
        if (r.event_id) allowed.add(r.event_id);
      });
      if (!allowed.size) {
        results = [];
      } else {
        results = results.filter((row) => allowed.has(row.id));
      }
    }

    if (hasExtendedFilters && results.length) {
      results = results.filter((row) => rowMatchesNodeFilters(row));
    }

    results = results.sort((a, b) => getCanonicalNodeTsMs(b) - getCanonicalNodeTsMs(a));

    return results.slice(0, limit).map(row => ({
      id: row.id,
      type: row.layer,
      data: {
        title: row.title,
        summary: row.summary,
        timestamp: getCanonicalNodeTimestamp(row),
        occurred_at: getCanonicalNodeTimestamp(row),
        anchor_at: row.anchor_at || parseNodeMetadata(row).anchor_at || null,
        created_at: row.created_at || null,
        updated_at: row.updated_at || null,
        source_refs: (() => { try { return JSON.parse(row.source_refs || '[]'); } catch (_) { return []; } })(),
        ...(parseNodeMetadata(row))
      }
    }));
  } catch (error) {
    console.error('[MemoryGraph] Search failed:', error);
    return [];
  }
});

ipcMain.handle('get-related-nodes', async (event, nodeId, relationType = null) => {
  try {

    let sql = `
      SELECT n.id, n.layer, n.title, n.summary, n.metadata, n.anchor_at, n.created_at, n.updated_at, e.edge_type
      FROM memory_nodes n
      JOIN memory_edges e ON (n.id = e.from_node_id OR n.id = e.to_node_id)
      WHERE (e.from_node_id = ? OR e.to_node_id = ?) AND n.id != ?
    `;
    const params = [nodeId, nodeId, nodeId];

    if (relationType) {
      sql += ` AND e.edge_type = ?`;
      params.push(relationType);
    }

    const results = await db.allQuery(sql, params);
    return results.map(row => ({
      id: row.id,
      type: row.layer,
      data: {
        title: row.title,
        summary: row.summary,
        timestamp: (() => {
          try {
            const metadata = JSON.parse(row.metadata || '{}');
            return metadata.event_time || metadata.occurred_at || metadata.anchor_at || metadata.latest_activity_at || metadata.timestamp || row.anchor_at || row.created_at || row.updated_at || null;
          } catch (_) {
            return row.anchor_at || row.created_at || row.updated_at || null;
          }
        })(),
        ...(JSON.parse(row.metadata || '{}'))
      },
      relation: row.edge_type
    }));
  } catch (error) {
    console.error('[MemoryGraph] Related nodes query failed:', error);
    return [];
  }
});

ipcMain.handle('get-core-memory', async () => {
  try {
    const coreNode = await db.get(`SELECT * FROM memory_nodes WHERE id = 'core_living_doc'`).catch(() => null);
    const rows = await db.allQuery(
      `SELECT title, summary, confidence
       FROM memory_nodes
       WHERE layer = 'insight'
       ORDER BY confidence DESC, updated_at DESC
       LIMIT 6`
    ).catch(() => []);
    return {
      core: coreNode ? { title: coreNode.title, summary: coreNode.summary, canonical_text: coreNode.canonical_text, updated_at: coreNode.updated_at } : null,
      top_insights: rows.map((row) => ({
        title: row.title,
        summary: row.summary,
        confidence: row.confidence
      }))
    };
  } catch (error) {
    console.error('[MemoryGraph] Core memory query failed:', error);
    return { error: error.message };
  }
});

ipcMain.handle('trigger-memory-graph-job', async (event, jobType) => {
  try {
    switch (jobType) {
      case 'episodes':
        await runEpisodeGeneration();
        store.set('lastEpisodeRun', new Date().toISOString());
        return { success: true, message: 'Episode generation triggered' };
      case 'suggestions':
        await runSuggestionEngineJob({ force: true });
        store.set('lastSuggestionRun', new Date().toISOString());
        return { success: true, message: 'Suggestion engine triggered' };
      case 'weekly_insights':
        await runWeeklyInsightJobScheduled();
        return { success: true, message: 'Weekly insights triggered' };
      default:
        return { success: false, error: 'Unknown job type' };
    }
  } catch (error) {
    console.error(`[MemoryGraph] Job ${jobType} failed:`, error);
    return { success: false, error: error.message };
  }
});

ipcMain.handle('search-raw-events', async (event, query) => {
  try {
    const q = `%${query || ''}%`;

    const results = await db.allQuery(`
      SELECT id, type, timestamp, occurred_at, date, source, text, metadata
      FROM events
      WHERE text LIKE ? OR type LIKE ? OR source LIKE ?
      ORDER BY COALESCE(occurred_at, timestamp) DESC
      LIMIT 50
    `, [q, q, q]);

    return results.map(row => ({
      ...row,
      metadata: JSON.parse(row.metadata || '{}')
    }));
  } catch (error) {
    console.error('[RawEvents] Search failed:', error);
    return [];
  }
});

ipcMain.handle('get-memory-drilldown', async (event, refs = []) => {
  try {
    const ids = Array.from(new Set((Array.isArray(refs) ? refs : [refs]).filter(Boolean).map((item) => {
      if (typeof item === 'string') return item;
      return item.ref || item.event_id || item.id || null;
    }).filter(Boolean))).slice(0, 24);
    if (!ids.length) return { items: [] };
    const placeholders = ids.map(() => '?').join(',');
    const rows = await db.allQuery(
      `SELECT id, source_type, source_account, occurred_at, app, window_title, url, domain, participants, title, raw_text, redacted_text, metadata
       FROM events
       WHERE id IN (${placeholders})
       ORDER BY COALESCE(occurred_at, timestamp) DESC`,
      ids
    ).catch(() => []);
    return {
      items: rows.map((row) => ({
        id: row.id,
        source_type: row.source_type,
        source_account: row.source_account,
        occurred_at: row.occurred_at,
        app: row.app,
        window_title: row.window_title,
        url: row.url,
        domain: row.domain,
        participants: (() => {
          try {
            return JSON.parse(row.participants || '[]');
          } catch (_) {
            return [];
          }
        })(),
        title: row.title,
        raw_text: row.raw_text,
        redacted_text: row.redacted_text,
        metadata: JSON.parse(row.metadata || '{}')
      }))
    };
  } catch (error) {
    console.error('[MemoryGraph] Drilldown failed:', error);
    return { items: [], error: error.message };
  }
});

ipcMain.handle('reset-memory-system', async (event, options = {}) => {
  try {
    const result = await resetZeroBaseMemory({
      includeEvents: options?.includeEvents !== false,
      rederive: Boolean(options?.rederive)
    });
    if (options?.reimport !== false) {
      await fullGoogleSync({ since: null, forceHistoricalBackfill: true }).catch((e) => {
        console.warn('[reset-memory-system] Google reimport skipped:', e?.message || e);
      });
      await runMinutelySync().catch((e) => console.warn('[reset-memory-system] Minutely sync skipped:', e?.message || e));
      await captureDesktopSensorSnapshot('reset').catch((e) => console.warn('[reset-memory-system] Desktop capture skipped:', e?.message || e));
      await runEpisodeGeneration().catch((e) => console.warn('[reset-memory-system] Episode generation failed:', e?.message || e));
      await runSuggestionEngineJob().catch((e) => console.warn('[reset-memory-system] Suggestion generation failed:', e?.message || e));
    }
    return { success: true, ...result };
  } catch (error) {
    console.error('[MemoryGraph] Reset failed:', error);
    return { success: false, error: error.message };
  }
});

ipcMain.handle('run-puppeteer-test', async () => {
  const res = await runPuppeteerGoogleTest();
  if (res.status !== 'success') throw new Error(res.error || 'Puppeteer test failed');
  return res;
});

ipcMain.handle('run-puppeteer-prompt', async (event, promptText) => {
  const text = (promptText || '').toString().trim();
  if (!text) return { status: 'error', error: 'empty-prompt' };

  const lower = text.toLowerCase();
  const urlMatch = text.match(/https?:\/\/[^\s]+/i);
  const domainMatch = text.match(/(?:^|\s)([a-z0-9-]+\.[a-z]{2,})(?:\s|$)/i);
  let query = '';
  const m = text.match(/search\s+(.+)/i);
  if (m && m[1]) query = m[1].trim();
  if (!query && /google/.test(lower)) query = 'hello';

  // If a URL is provided, go directly there
  if (urlMatch) {
    return await runPuppeteerTask({ title: 'Puppeteer Prompt', url: urlMatch[0], script: null });
  }

  // If a domain is provided and not just "google", go directly there
  if (domainMatch && !lower.includes('google')) {
    const domain = domainMatch[1];
    return await runPuppeteerTask({ title: 'Puppeteer Prompt', url: `https://${domain}`, script: null });
  }

  if (lower.includes('google') && (lower.includes('search') || query)) {
    const res = await runPuppeteerGoogleTest(query);
    if (res.status !== 'success') throw new Error(res.error || 'Puppeteer prompt failed');
    return res;
  }

  return await runPuppeteerTask({ title: 'Puppeteer Prompt', url: 'https://www.google.com', script: null });
});

ipcMain.handle('puppeteer-continue', async () => {
  resolveUserTakeover();
  return { status: 'ok' };
});

ipcMain.handle('send-email', async (event, payload) => {
  const { to, subject, body, threadId, messageId } = payload || {};
  if (!to || !body) throw new Error('Missing email recipient or body');

  // Use first connected Google account
  const accounts = store.get('googleAccounts') || [];
  const account = accounts[0];
  if (!account?.tokens) throw new Error('No connected Google account');

  const auth = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, getRedirectUri());
  auth.setCredentials(account.tokens);
  const gmail = google.gmail({ version: 'v1', auth });

  const safeSubject = subject || 'Re:';
  const headers = [
    `To: ${to}`,
    `Subject: ${safeSubject}`,
    'Content-Type: text/plain; charset="UTF-8"',
    'MIME-Version: 1.0'
  ];
  if (messageId) headers.push(`In-Reply-To: ${messageId}`, `References: ${messageId}`);

  const raw = Buffer.from(`${headers.join('\r\n')}\r\n\r\n${body}`, 'utf-8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');

  const sendRequest = { userId: 'me', requestBody: { raw } };
  if (threadId) sendRequest.requestBody.threadId = threadId;

  const res = await gmail.users.messages.send(sendRequest);
  return { status: 'sent', id: res.data.id || null, threadId: res.data.threadId || null };
});

ipcMain.handle('get-daily-summaries', (event, query) => {
  const summaries = store.get('dailySummaries') || [];
  const historicalSummaries = store.get('historicalSummaries') || {};
  const normalizedHistorical = Object.keys(historicalSummaries)
    .sort((a, b) => b.localeCompare(a))
    .map(date => historicalSummaries[date]);
  const source = summaries.length ? summaries : normalizedHistorical;
  if (!query) return source;
  // If query is an object with date, return summaries for that date
  try {
    if (typeof query === 'object' && query.date) {
      return source.filter(s => s.date === query.date);
    }
  } catch (e) {}
  const q = query.toString().toLowerCase();
  return source.filter(s => (s.searchIndex && s.searchIndex.includes(q)) || (s.date && s.date.includes(q)));
});

// Run diagnostic via extension bridge
ipcMain.handle('extension-run-diagnostic', async (event, opts) => {
  return { status: 'error', error: 'Browser extension support has been removed' };
});

function extractPriorityFromText(text) {
  const lowerText = text.toLowerCase();
  if (lowerText.includes('urgent') || lowerText.includes('asap') ||
      lowerText.includes('deadline') || lowerText.includes('critical')) {
    return 'high';
  } else if (lowerText.includes('important') || lowerText.includes('priority')) {
    return 'medium';
  }
  return 'low';
}

function extractCategoryFromText(text) {
  const lowerText = text.toLowerCase();
  if (lowerText.includes('email') || lowerText.includes('reply') || lowerText.includes('contact')) {
    return 'communication';
  } else if (lowerText.includes('learn') || lowerText.includes('study') || lowerText.includes('course')) {
    return 'learning';
  } else if (lowerText.includes('meet') || lowerText.includes('network') || lowerText.includes('connect')) {
    return 'networking';
  } else if (lowerText.includes('project') || lowerText.includes('task') || lowerText.includes('work')) {
    return 'projects';
  }
  return 'general';
}

function dateKeyFromTimestamp(ts) {
  if (!ts) return null;
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function buildDailySummariesLocal(googleData, browserHistory) {
  const summaries = new Map();

  const ensure = (key) => {
    if (!summaries.has(key)) {
      summaries.set(key, {
        date: key,
        emails: [],
        events: [],
        docs: [],
        history: [],
        evidence: { emails: [], events: [], docs: [], history: [] },
        stats: { emails: 0, events: 0, docs: 0, history: 0 },
        highlights: { people: [], domains: [], docs: [], events: [], emails: [] },
        keywords: [],
        searchIndex: '',
        narrative: ''
      });
    }
    return summaries.get(key);
  };

  (googleData?.gmail || []).forEach(email => {
    const key = dateKeyFromTimestamp(email.timestamp || email.internalDate);
    if (!key) return;
    const from = email.from || '';
    const subject = email.subject || '';
    const entry = {
      id: email.id || null,
      subject,
      from,
      timestamp: email.timestamp || null
    };
    const s = ensure(key);
    s.emails.push(entry);
    s.evidence.emails.push({ source: 'gmail', id: entry.id, timestamp: entry.timestamp, from: entry.from, subject: entry.subject });
    s.stats.emails += 1;
  });

  (googleData?.calendar || []).forEach(ev => {
    const start = ev.start_time || ev.start;
    const key = dateKeyFromTimestamp(start);
    if (!key) return;
    const title = ev.event_title || ev.summary || '';
    const entry = {
      id: ev.id || null,
      title,
      start_time: start || null
    };
    const s = ensure(key);
    s.events.push(entry);
    s.evidence.events.push({ source: 'calendar', id: entry.id, start_time: entry.start_time, title: entry.title, attendees: ev.attendees || [] });
    s.stats.events += 1;
  });

  (googleData?.drive || []).forEach(doc => {
    const ts = doc.last_modified || doc.modified || doc.modifiedTime;
    const key = dateKeyFromTimestamp(ts);
    if (!key) return;
    const name = doc.doc_name || doc.name || '';
    const entry = {
      id: doc.id || null,
      name,
      last_modified: ts || null
    };
    const s = ensure(key);
    s.docs.push(entry);
    s.evidence.docs.push({ source: 'drive', id: entry.id, last_modified: entry.last_modified, name: entry.name, webViewLink: doc.webViewLink || null, shared_with: doc.shared_with || [] });
    s.stats.docs += 1;
  });

  (browserHistory || []).forEach(h => {
    const key = dateKeyFromTimestamp(h.timestamp);
    if (!key) return;
    const entry = {
      url: h.url,
      title: h.title || '',
      domain: h.domain || '',
      timestamp: h.timestamp
    };
    const s = ensure(key);
    s.history.push(entry);
    s.evidence.history.push({ source: h.browser || 'browser', url: entry.url, title: entry.title, domain: entry.domain, timestamp: entry.timestamp });
    s.stats.history += 1;
  });

  for (const s of summaries.values()) {
    const people = new Map();
    s.emails.forEach(e => {
      const name = (e.from || '').split('<')[0].trim();
      if (!name) return;
      people.set(name, (people.get(name) || 0) + 1);
    });
    s.highlights.people = Array.from(people.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([name]) => name);

    const domains = new Map();
    s.history.forEach(h => {
      if (!h.domain) return;
      domains.set(h.domain, (domains.get(h.domain) || 0) + 1);
    });
    s.highlights.domains = Array.from(domains.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([d]) => d);

    s.highlights.docs = s.docs.slice(0, 5).map(d => d.name);
    s.highlights.events = s.events.slice(0, 5).map(e => e.title);
    s.highlights.emails = s.emails.slice(0, 5).map(e => e.subject);

    const keywordSet = new Set();
    [...s.highlights.people, ...s.highlights.domains, ...s.highlights.docs, ...s.highlights.events, ...s.highlights.emails]
      .forEach(k => {
        const val = (k || '').toString().toLowerCase();
        if (val) keywordSet.add(val);
      });
    s.keywords = Array.from(keywordSet).slice(0, 20);
    s.searchIndex = s.keywords.join(' ');

    // Credibly-efficient narrative: counts + 2-5 highlights, no over-summarizing.
    const parts = [];
    if (s.stats.emails) parts.push(`${s.stats.emails} email${s.stats.emails === 1 ? '' : 's'}`);
    if (s.stats.events) parts.push(`${s.stats.events} event${s.stats.events === 1 ? '' : 's'}`);
    if (s.stats.docs) parts.push(`${s.stats.docs} doc${s.stats.docs === 1 ? '' : 's'}`);
    if (s.stats.history) parts.push(`${s.stats.history} page visit${s.stats.history === 1 ? '' : 's'}`);
    const base = parts.length ? `You had ${parts.join(', ')}.` : `No notable activity captured.`;

    const highlightBits = [];
    if (s.highlights.events?.length) highlightBits.push(`Events: ${s.highlights.events.slice(0, 2).join('; ')}`);
    if (s.highlights.docs?.length) highlightBits.push(`Docs: ${s.highlights.docs.slice(0, 2).join('; ')}`);
    if (s.highlights.people?.length) highlightBits.push(`People: ${s.highlights.people.slice(0, 2).join('; ')}`);
    if (s.highlights.domains?.length) highlightBits.push(`Top sites: ${s.highlights.domains.slice(0, 2).join('; ')}`);
    const highlights = highlightBits.length ? `Highlights — ${highlightBits.slice(0, 3).join('. ')}.` : '';

    s.narrative = `${base}${highlights ? ' ' + highlights : ''}`.trim();

    // Make searchIndex consistent: include narrative + key titles (lowercased) for recall.
    const extra = [
      s.narrative,
      ...(s.highlights.people || []),
      ...(s.highlights.domains || []),
      ...(s.highlights.docs || []),
      ...(s.highlights.events || []),
      ...(s.highlights.emails || [])
    ]
      .map(x => (x || '').toString().toLowerCase())
      .filter(Boolean)
      .join(' ');
    s.searchIndex = `${s.searchIndex} ${extra}`.trim();
  }

  return Array.from(summaries.values()).sort((a, b) => b.date.localeCompare(a.date));
}

// Helper function to categorize URL
function categorizeURL(url) {
  const domain = new URL(url).hostname.toLowerCase();

  if (domain.includes('github') || domain.includes('stackoverflow') || domain.includes('linkedin')) {
    return 'work';
  }
  if (domain.includes('twitter') || domain.includes('facebook') || domain.includes('instagram')) {
    return 'social';
  }
  if (domain.includes('youtube') || domain.includes('netflix')) {
    return 'entertainment';
  }
  if (domain.includes('amazon') || domain.includes('ebay')) {
    return 'shopping';
  }
  if (domain.includes('coursera') || domain.includes('udemy')) {
    return 'learning';
  }

  return 'general';
}

ipcMain.handle('get-extension-data', async () => {
  return {};
});

ipcMain.handle('clear-extension-data', async () => {
  global.extensionData = null;
  store.delete('extensionData');
  return { success: true };
});

ipcMain.handle('get-relationship-contacts', async (_event, payload = {}) => {
  if (!RELATIONSHIP_FEATURE_ENABLED) return [];
  await syncAppleContactsIntoRelationshipGraph({
    force: Boolean(payload?.forceAppleContactsSync),
    limit: payload?.appleContactsLimit || 500
  }).catch((error) => {
    console.warn('[get-relationship-contacts] Apple Contacts sync skipped:', error?.message || error);
  });
  const googleData = store.get('googleData') || {};
  if (Array.isArray(googleData.contacts) && googleData.contacts.length && payload?.forceGoogleContactsSync) {
    await syncGoogleContactsIntoRelationshipGraph({
      contacts: googleData.contacts,
      force: true
    }).catch((error) => {
      console.warn('[get-relationship-contacts] Google Contacts sync skipped:', error?.message || error);
    });
  }
  return getRelationshipContacts({
    limit: payload?.limit || 50,
    status: payload?.status || null
  });
});

ipcMain.handle('sync-apple-contacts', async (_event, payload = {}) => {
  if (!RELATIONSHIP_FEATURE_ENABLED) return { imported: 0, skipped: true, disabled: true };
  return syncAppleContactsIntoRelationshipGraph({
    force: payload?.force !== false,
    limit: payload?.limit || 500
  });
});

ipcMain.handle('get-relationship-contact-detail', async (_event, contactId) => {
  if (!RELATIONSHIP_FEATURE_ENABLED) return null;
  return getRelationshipContactDetail(contactId);
});

ipcMain.handle('update-person-profile', async (_event, payload = {}) => {
  if (!RELATIONSHIP_FEATURE_ENABLED) return { success: false, disabled: true };
  const contactId = payload?.contactId || payload?.contact_id;
  return updateRelationshipContactProfile(contactId, payload || {});
});

ipcMain.handle('generate-relationship-draft', async (_event, payload = {}) => {
  if (!RELATIONSHIP_FEATURE_ENABLED) return { draft: '', context: null, disabled: true };
  const { generateRelationshipDraft } = require('./services/agent/relationship-suggestions-engine');
  const contactId = payload?.contactId || payload?.contact_id;
  if (!contactId) return { draft: '', context: null, error: 'missing_contact_id' };
  const llmConfig = getSuggestionLLMConfig();
  const result = await generateRelationshipDraft(
    contactId,
    payload?.triggerType || payload?.trigger_type || 'dormancy',
    payload?.triggerContext || payload?.trigger_context || {},
    llmConfig
  ).catch((err) => ({ draft: '', ai_generated: false, context: null, error: err?.message }));
  return result || { draft: '', context: null, error: 'generation_failed' };
});

// Persistent todos management
ipcMain.handle('get-persistent-todos', () => {
  const todos = (store.get('persistentTodos') || []).filter((todo) => !todo?.completed && todo?.source !== 'auto_suggestion');
  store.set('persistentTodos', todos);
  return todos;
});

ipcMain.handle('save-persistent-todos', (event, todos) => {
  const next = (Array.isArray(todos) ? todos : []).filter((todo) => !todo?.completed);
  store.set('persistentTodos', next);
});

ipcMain.handle('get-morning-briefs', () => {
  return getMorningBriefsStore();
});

ipcMain.handle('generate-morning-brief', async (event, opts = {}) => {
  return await generateMorningBrief({
    force: Boolean(opts?.force),
    scheduled: Boolean(opts?.scheduled)
  });
});

ipcMain.handle('store-user-data', (event, data) => {
  store.set('userData', data);
});

ipcMain.handle('get-user-data', () => {
  return store.get('userData') || {};
});

ipcMain.handle('get-google-tokens', () => {
  return store.get('googleTokens') || null;
});

ipcMain.handle('get-suggestion-llm-settings', async () => {
  const settings = getSuggestionLLMSettings();
  return {
    provider: settings.provider,
    model: settings.model,
    baseUrl: settings.baseUrl,
    hasApiKey: Boolean(settings.apiKey)
  };
});

ipcMain.handle('save-suggestion-llm-settings', async (_event, payload = {}) => {
  const previous = store.get('suggestionLLMSettings') || {};
  const provider = sanitizeSuggestionProvider(payload.provider || 'deepseek');
  const model = String(payload.model || 'deepseek-chat').trim();
  const baseUrl = String(payload.baseUrl || payload.base_url || 'http://127.0.0.1:11434').trim();
  const apiKeyInput = String(payload.apiKey || '').trim();
  const apiKey = apiKeyInput || String(previous.apiKey || '').trim();
  const next = {
    provider,
    model,
    baseUrl,
    apiKey
  };
  store.set('suggestionLLMSettings', next);
  return {
    provider: next.provider,
    model: next.model,
    baseUrl: next.baseUrl,
    hasApiKey: Boolean(next.apiKey)
  };
});

ipcMain.handle('get-sensor-status', async () => {
  return getSensorStatus();
});

ipcMain.handle('get-sensor-events', async () => {
  return getSensorEvents();
});

ipcMain.handle('save-sensor-settings', async (_event, settings) => {
  const next = {
    ...getSensorSettings(),
    ...(settings || {})
  };
  next.intervalMinutes = 15;
  store.set('sensorSettings', next);
  return getSensorStatus();
});

ipcMain.handle('capture-sensor-snapshot', async () => {
  captureDesktopSensorSnapshot('manual').catch((err) => console.warn('Manual sensor capture failed:', err));
  return {
    event: null,
    status: getSensorStatus()
  };
});

ipcMain.handle('delete-all-settings', async () => {
  // Factory reset: wipe in-memory state, SQLite data, browser/session storage, and userData files.
  try { if (sensorCaptureTimer) clearInterval(sensorCaptureTimer); } catch (_) {}
  try { if (episodeGenerationTimer) clearInterval(episodeGenerationTimer); } catch (_) {}
  try { if (suggestionEngineTimer) clearInterval(suggestionEngineTimer); } catch (_) {}
  try { if (relationshipGraphTimer) clearInterval(relationshipGraphTimer); } catch (_) {}
  try { if (minutelySyncInterval) clearInterval(minutelySyncInterval); } catch (_) {}
  try { if (weeklyInsightTimer) clearTimeout(weeklyInsightTimer); } catch (_) {}
  try { if (dailySummaryTimer) clearTimeout(dailySummaryTimer); } catch (_) {}
  try { if (patternUpdateTimer) clearTimeout(patternUpdateTimer); } catch (_) {}
  try { if (morningBriefTimer) clearTimeout(morningBriefTimer); } catch (_) {}

  try {
    await session.defaultSession.clearStorageData({
      storages: ['appcache', 'cookies', 'filesystem', 'indexdb', 'localstorage', 'shadercache', 'serviceworkers', 'websql']
    });
    await session.defaultSession.clearCache();
  } catch (error) {
    console.warn('[delete-all-settings] Session storage cleanup failed:', error?.message || error);
  }

  try {
    await db.closeDB?.();
  } catch (error) {
    console.warn('[delete-all-settings] DB close failed:', error?.message || error);
  }

  try {
    const userDataPath = app.getPath('userData');
    if (await existsAsync(userDataPath)) {
      const entries = await fs.promises.readdir(userDataPath);
      for (const name of entries) {
        const abs = path.join(userDataPath, name);
        try {
          await fs.promises.rm(abs, { recursive: true, force: true });
        } catch (innerErr) {
          console.warn('[delete-all-settings] Failed removing userData entry:', abs, innerErr?.message || innerErr);
        }
      }
    }
  } catch (error) {
    console.warn('[delete-all-settings] userData cleanup failed:', error?.message || error);
  }

  try { store.clear(); } catch (_) {}
  app.relaunch();
  app.exit(0);
});

ipcMain.handle('execute-todo', async (event, todo) => {
  // Implement todo execution logic
  console.log('Executing todo:', todo);
  return { success: true, message: 'Todo executed successfully' };
});


async function initializeBackgroundServices() {
  try {
    // Call initDB early so the db object is available for IPC handlers

    await db.ensureDB();
    await ingestion.initIngestion();
    console.log('SQLite Graph DB Initialized');
    await repairEmailEventTimestamps();
  } catch (err) {
    console.error('Failed to initialize database or ingestion:', err);
  }

  try {
    scheduleDailyTasks();
    startSensorCaptureLoop();
  } catch (err) {
    console.error('Failed to start background services:', err);
  }
}

app.whenReady().then(async () => {
  createWindow();
  createVoiceHudWindow();
  db.initDB().catch(err => console.log('[DB] Early init catch:', err.message));

  // Emergency CPU throttling - set process priority to low
  if (EMERGENCY_THROTTLE_ENABLED) {
    try {
      // Set process priority to low on macOS to prevent system unresponsiveness
      const { exec } = require('child_process');
      exec(`renice +10 -p ${process.pid}`, (error, stdout, stderr) => {
        if (error) {
          console.log('[Emergency] Could not set process priority:', error.message);
        } else {
          console.log('[Emergency] Process priority set to low for CPU throttling');
        }
      });
    } catch (e) {
      console.log('[Emergency] Failed to set process priority:', e.message);
    }
  }

  initializeBackgroundServices();
  registerVoiceShortcut();
  hydrateStudySessionFromStore();
  startScreenshotCleanupLoop();
  emitStudySessionUpdate();
  try {
    updatePerformanceState({
      onBattery: Boolean(powerMonitor?.isOnBatteryPower?.()),
      thermalState: 'unknown'
    });
    powerMonitor.on('on-battery', () => updatePerformanceState({ onBattery: true }));
    powerMonitor.on('on-ac', () => updatePerformanceState({ onBattery: false }));
    powerMonitor.on('thermal-state-change', (_event, details = {}) => {
      updatePerformanceState({ thermalState: String(details.state || 'unknown') });
    });
    powerMonitor.on('idle', () => updatePerformanceState());
    powerMonitor.on('active', () => {
      updatePerformanceState();
      if (screenshotsPausedForDisplayOff && !/lock/i.test(periodicScreenshotPauseReason)) {
        resumePeriodicScreenshotCapture('system active');
      }
    });
    powerMonitor.on('suspend', () => {
      pausePeriodicScreenshotCapture('system suspended');
    });
    powerMonitor.on('resume', () => {
      resumePeriodicScreenshotCapture('system resumed');
    });
    powerMonitor.on('display-sleep', () => {
      pausePeriodicScreenshotCapture('display asleep');
    });
    powerMonitor.on('display-wake', () => {
      resumePeriodicScreenshotCapture('display awake');
    });
    powerMonitor.on('lock-screen', () => {
      pausePeriodicScreenshotCapture('screen locked');
    });
    powerMonitor.on('unlock-screen', () => {
      resumePeriodicScreenshotCapture('screen unlocked');
    });
  } catch (error) {
    console.warn('[Performance] powerMonitor hooks unavailable:', error?.message || error);
  }

  // Start OAuth server
  const startOAuthServer = (port) => {
    const server = oauthApp.listen(port, () => {
      oauthPort = port;
      console.log(`OAuth server running on port ${oauthPort}`);
    });
    server.on('error', (err) => {
      if (err && err.code === 'EADDRINUSE') {
        const nextPort = port === 3003 ? 3004 : port + 1;
        console.warn(`OAuth port ${port} in use. Retrying on ${nextPort}...`);
        setTimeout(() => startOAuthServer(nextPort), 300);
      } else {
        console.error('OAuth server failed to start:', err);
      }
    });
  };

  startOAuthServer(Number(oauthPort) || 3002);

  setTimeout(() => startSourceWarmup(), STARTUP_SOURCE_WARMUP_DELAY_MS);
  setTimeout(() => startPeriodicScreenshotCapture(), Math.max(15000, getPeriodicScreenshotWakeDelayMs()));

  // Initialize memory graph processing
  setTimeout(() => startMemoryGraphProcessing(), STARTUP_MEMORY_GRAPH_DELAY_MS);

  // Start user-defined automation scheduler
  setTimeout(() => startAutomationScheduler(), STARTUP_AUTOMATION_DELAY_MS);
  if (recursiveImprovementEnabled()) {
    setTimeout(() => startRecursiveImprovementLoop(), STARTUP_RECURSIVE_DELAY_MS);
  }

  // Auto-trigger initial sync on first launch
  const syncDone = store.get('initialSyncDone') || false;
  if (!syncDone) {
    console.log(`[initialSync] First launch detected — scheduling initial historical sync in ${Math.round(STARTUP_INITIAL_SYNC_DELAY_MS / 60000)}m...`);
    setTimeout(() => {
      fullGoogleSync({
        pageVisits: browserHistory,
        apiKey: process.env.DEEPSEEK_API_KEY,
        onProgress: (progress) => {
          if (mainWindow && mainWindow.webContents) {
            mainWindow.webContents.send('initial-sync-progress', progress);
          }
        }
      }, store).then(async (result) => {
        console.log('[initialSync] Initial sync completed');
        const daysCount = await processSyncResult(result);
        console.log(`[initialSync] Scheduled sync complete: ${daysCount} days summarised.`);

        store.set('historicalSummaries', result.summaries);
        store.set('initialSyncDone', true);

        const existingProfile = store.get('userProfile') || {};
        store.set('userProfile', {
          ...existingProfile,
          patterns: [...new Set([...(existingProfile.patterns    || []), ...result.userPatterns])].slice(0, 40),
          preferences: [...new Set([...(existingProfile.preferences || []), ...result.userPreferences])].slice(0, 40),
          top_intent_clusters: result.topIntentClusters || []
        });

        console.log(`[initialSync] Complete — ${Object.keys(result.summaries).length} days summarised.`);
        if (mainWindow && mainWindow.webContents) {
          mainWindow.webContents.send('initial-sync-complete', {
            daysProcessed: Object.keys(result.summaries).length
          });
        }
      }).catch((err) => {
        console.error('[initialSync] Initial sync failed:', err?.message || err);
      });
    }, STARTUP_INITIAL_SYNC_DELAY_MS);
  }

  // Periodic sync every 5 minutes
  setInterval(() => {
    fullGoogleSync().catch((err) => {
      console.error('[PeriodicSync] Sync failed:', err?.message || err);
    });
  }, GSUITE_SYNC_INTERVAL_MS);

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
    if (!voiceHudWindow || voiceHudWindow.isDestroyed()) {
      createVoiceHudWindow();
    }
  });
});

app.on('window-all-closed', () => {
  // Clear scheduled timers
  if (dailySummaryTimer) clearTimeout(dailySummaryTimer);
  if (patternUpdateTimer) clearTimeout(patternUpdateTimer);
  if (screenshotCleanupTimer) clearInterval(screenshotCleanupTimer);
  if (relationshipGraphTimer) clearInterval(relationshipGraphTimer);
  stopPeriodicScreenshotCapture();

  if (process.platform !== 'darwin') {
    app.quit();
  }
});

// Also cleanup on app quit
app.on('before-quit', () => {
  // Clear scheduled timers
  if (dailySummaryTimer) clearTimeout(dailySummaryTimer);
  if (patternUpdateTimer) clearTimeout(patternUpdateTimer);
  if (screenshotCleanupTimer) clearInterval(screenshotCleanupTimer);
  if (relationshipGraphTimer) clearInterval(relationshipGraphTimer);
  stopPeriodicScreenshotCapture();
  globalShortcut.unregisterAll();
});
}
