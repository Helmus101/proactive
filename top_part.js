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

const STARTUP_TRACE_PATH = '/tmp/proactive-startup.log';
function startupTrace(message, extra = '') {
  try {
    fs.appendFileSync(STARTUP_TRACE_PATH, `${new Date().toISOString()} ${message}${extra ? ` ${extra}` : ''}\n`);
  } catch (_) {}
}
startupTrace('[Startup] main module loaded');

/**
 * Defer module loading until a property is actually accessed.
 */
function lazyRequire(modulePath) {
  let cachedModule;
  return new Proxy(() => {}, {
    get(target, prop) {
      if (prop === '_isLazyProxy') return true;
      if (!cachedModule) {
        startupTrace(`[Lazy] Loading module: ${modulePath}`);
        cachedModule = require(modulePath);
      }
      return cachedModule[prop];
    },
    apply(target, thisArg, argumentsList) {
      if (!cachedModule) {
        startupTrace(`[Lazy] Loading module (apply): ${modulePath}`);
        cachedModule = require(modulePath);
      }
      return typeof cachedModule === 'function' ? cachedModule.apply(thisArg, argumentsList) : cachedModule;
    }
  });
}

async function existsAsync(path) {
  try {
    await fsPromises.access(path);
    return true;
  } catch {
    return false;
  }
}

// Heavy modules lazy-loaded
const execFile = (...args) => lazyRequire('child_process').execFile(...args);
const axios = lazyRequire('axios');
const Store = lazyRequire('electron-store');
const sqlite3 = (process.env.NODE_ENV === 'production') ? lazyRequire('sqlite3') : lazyRequire('sqlite3').verbose();
const ingestion = lazyRequire('./services/ingestion');
const ingestRawEvent = (...args) => ingestion.ingestRawEvent(...args);
const express = lazyRequire('express');
const FormData = lazyRequire('form-data');
require('dotenv').config(); // Load environment variables

const db = lazyRequire('./services/db');
const engine = lazyRequire('./services/agent/intelligence-engine');
const extractor = lazyRequire('./services/extractors/openLoopExtractor');
const scoring = lazyRequire('./services/scoring');

const answerChatQuery = (...args) => lazyRequire('./services/agent/chat-engine').answerChatQuery(...args);
const buildRadarState = (...args) => lazyRequire('./services/agent/radar-engine').buildRadarState(...args);
const buildHybridGraphRetrieval = (...args) => lazyRequire('./services/agent/hybrid-graph-retrieval').buildHybridGraphRetrieval(...args);
const buildRetrievalThought = (...args) => lazyRequire('./services/agent/retrieval-thought-system').buildRetrievalThought(...args);
const upsertMemoryNode = (...args) => lazyRequire('./services/agent/graph-store').upsertMemoryNode(...args);
const generateEmbedding = (...args) => lazyRequire('./services/embedding-engine').generateEmbedding(...args);
const generateTopTodosFromMemoryQuery = (...args) => lazyRequire('./services/agent/suggestion-engine').generateTopTodosFromMemoryQuery(...args);
const generateAndPersistTasksFromLLM = (...args) => lazyRequire('./services/agent/suggestion-engine').generateAndPersistTasksFromLLM(...args);
const getLatestRecursiveImprovementLog = (...args) => lazyRequire('./services/agent/recursive-improvement-engine').getLatestRecursiveImprovementLog(...args);

const getRelationshipContactDetail = (...args) => lazyRequire('./services/relationship-graph').getRelationshipContactDetail(...args);
const getRelationshipContacts = (...args) => lazyRequire('./services/relationship-graph').getRelationshipContacts(...args);
const syncAppleContactsIntoRelationshipGraph = (...args) => lazyRequire('./services/relationship-graph').syncAppleContactsIntoRelationshipGraph(...args);
const syncGoogleContactsIntoRelationshipGraph = (...args) => lazyRequire('./services/relationship-graph').syncGoogleContactsIntoRelationshipGraph(...args);
const updateRelationshipContactProfile = (...args) => lazyRequire('./services/relationship-graph').updateRelationshipContactProfile(...args);
const resetZeroBaseMemory = (...args) => lazyRequire('./services/agent/zero-base-memory').resetZeroBaseMemory(...args);
const runDailyInsights = (...args) => lazyRequire('./services/agent/intelligence-engine').runDailyInsights(...args);
const runEpisodeJob = (...args) => lazyRequire('./services/agent/intelligence-engine').runEpisodeJob(...args);
const runHourlySemanticPulse = (...args) => lazyRequire('./services/agent/intelligence-engine').runHourlySemanticPulse(...args);
const runLivingCoreJob = (...args) => lazyRequire("./services/agent/intelligence-engine").runLivingCoreJob(...args);
const runRecursiveImprovementCycle = (...args) => lazyRequire('./services/agent/recursive-improvement-engine').runRecursiveImprovementCycle(...args);
const runRelationshipGraphJob = (...args) => lazyRequire('./services/relationship-graph').runRelationshipGraphJob(...args);
const runSemanticSummaryWindow = (...args) => lazyRequire('./services/agent/intelligence-engine').runSemanticSummaryWindow(...args);
const runWeeklyInsightJob = (...args) => lazyRequire('./services/agent/intelligence-engine').runWeeklyInsightJob(...args);

const runInitialSync = (...args) => lazyRequire('./services/summarizer/initialSync').runInitialSync(...args);
const searchSummaries = (...args) => lazyRequire('./services/summarizer/initialSync').searchSummaries(...args);
const generateTodaySummaryWithContext = (...args) => lazyRequire('./services/summarizer/aiDailySummary').generateTodaySummaryWithContext(...args);
const buildDailySummaries = (...args) => lazyRequire('./services/summarizer/dailySummary').buildDailySummaries(...args);

const planNextAction = (...args) => lazyRequire('./services/agent/agentPlanner').planNextAction(...args);
const normalizeDesktopGoal = (...args) => lazyRequire('./services/agent/agentPlanner').normalizeDesktopGoal(...args);
const checkAccessibilityPermission = (...args) => lazyRequire('./services/desktop-control').checkAccessibilityPermission(...args);
const observeDesktopState = (...args) => lazyRequire('./services/desktop-control').observeDesktopState(...args);
const executeDesktopAction = (...args) => lazyRequire('./services/desktop-control').executeDesktopAction(...args);
const openAccessibilitySettings = (...args) => lazyRequire('./services/desktop-control').openAccessibilitySettings(...args);
const openScreenRecordingSettings = (...args) => lazyRequire('./services/desktop-control').openScreenRecordingSettings(...args);
const ensureManagedBrowser = (...args) => lazyRequire('./services/browser-driver').ensureManagedBrowser(...args);
const observeManagedBrowserState = (...args) => lazyRequire('./services/browser-driver').observeManagedBrowserState(...args);
const executeManagedBrowserAction = (...args) => lazyRequire('./services/browser-driver').executeManagedBrowserAction(...args);
const getManagedBrowserStatus = (...args) => lazyRequire('./services/browser-driver').getManagedBrowserStatus(...args);

const buildGlobalGraph = (...args) => lazyRequire('./services/agent/intelligence-engine').buildGlobalGraph(...args);
const detectTasks = (...args) => lazyRequire('./services/agent/intelligence-engine').detectTasks(...args);
const generateSuggestionFromGraph = (...args) => lazyRequire('./services/agent/intelligence-engine').generateSuggestionFromGraph(...args);
const generateCoreGlobal = (...args) => lazyRequire('./services/agent/intelligence-engine').generateCoreGlobal(...args);
const callLLM = (...args) => lazyRequire('./services/agent/intelligence-engine').callLLM(...args);

const normalizeSuggestion = (...args) => lazyRequire('./services/agent/intent-first-suggestions').normalizeSuggestion(...args);
const rankAndLimitSuggestions = (...args) => lazyRequire('./services/agent/intent-first-suggestions').rankAndLimitSuggestions(...args);
const rebuildInvertedIndex = (...args) => lazyRequire('./services/summarizer/indexing').rebuildInvertedIndex(...args);

// Global state
let mainWindow;
let voiceHudWindow = null;
const appInteractionState = {
  focused: false,
  minimized: false,
  chatActive: false,
  lastInteractionAt: 0
};

// Initialize store early
const store = new Store();

function markAppInteraction(reason = 'interaction') {
  appInteractionState.lastInteractionAt = Date.now();
  if (reason) {
    if (typeof debouncedStoreSet === 'function') {
      debouncedStoreSet('lastAppInteraction', {
        reason,
        at: new Date(appInteractionState.lastInteractionAt).toISOString()
      });
    }
  }
}

function createWindow() {
  startupTrace('[Startup] createWindow begin');
  console.log('[Startup] createWindow begin');
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.js')
    }
  });

  startupTrace('[Startup] BrowserWindow created');
  console.log('[Startup] BrowserWindow created');
  mainWindow.webContents.on('did-fail-load', (_event, errorCode, errorDescription, validatedURL) => {
    console.error('[Startup] renderer did-fail-load:', errorCode, errorDescription, validatedURL);
  });
  mainWindow.webContents.on('render-process-gone', (_event, details) => {
    console.error('[Startup] renderer process gone:', details);
  });
  mainWindow.webContents.on('console-message', (_event, level, message, line, sourceId) => {
    console.log(`[Renderer:${level}] ${message} (${sourceId}:${line})`);
  });
  mainWindow.webContents.on('did-finish-load', () => {
    startupTrace('[Startup] renderer did-finish-load');
    console.log('[Startup] renderer did-finish-load');
  });
  mainWindow.loadFile('renderer/index.html')
    .then(() => {
      startupTrace('[Startup] loadFile resolved');
      console.log('[Startup] loadFile resolved');
    })
    .catch((error) => {
      startupTrace('[Startup] loadFile failed', error?.message || String(error));
      console.error('[Startup] loadFile failed:', error?.message || error);
    });

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

  if (typeof activeVoiceSession !== 'undefined' && activeVoiceSession && activeVoiceSession.status === 'acting' && payload && payload.taskId && String(payload.taskId).startsWith('voice_task_')) {
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
