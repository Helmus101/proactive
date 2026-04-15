process.stdout.on('error', (err) => {
  if (err.code === 'EIO') return;
  throw err;
});

const { app, BrowserWindow, ipcMain, session, desktopCapturer, systemPreferences, screen, globalShortcut, powerMonitor } = require('electron');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { execFile } = require('child_process');
const axios = require('axios');
const Store = require('electron-store');
const express = require('express');
const FormData = require('form-data');
require('dotenv').config(); // Load environment variables

// Database import
const db = require('./services/db');

// ── Summarizer services ──────────────────────────────────────────────────────
const { runInitialSync, searchSummaries } = require('./services/summarizer/initialSync');
const { generateTodaySummaryWithContext } = require('./services/summarizer/aiDailySummary');
const { buildDailySummaries } = require('./services/summarizer/dailySummary');
const { planNextAction, normalizeDesktopGoal } = require('./services/agent/agentPlanner');
const { checkAccessibilityPermission, observeDesktopState, executeDesktopAction, openAccessibilitySettings } = require('./services/desktop-control');
const { ensureManagedBrowser, observeManagedBrowserState, executeManagedBrowserAction, getManagedBrowserStatus } = require('./services/browser-driver');
const { 
  buildGlobalGraph,
  detectTasks, 
  generateSuggestionFromGraph, 
  generateCoreGlobal 
} = require('./services/agent/intelligence-engine');
const {
  normalizeSuggestion,
  rankAndLimitSuggestions
} = require('./services/agent/intent-first-suggestions');
const { rebuildInvertedIndex } = require('./services/summarizer/indexing');

// Initialize electron-store for data persistence
const store = new Store();

// Express server for OAuth callback
const oauthApp = express();
let oauthPort = process.env.OAUTH_PORT || 3002; // Moved from 3001 to avoid overlap with WebSocket server

let mainWindow;
let authWindow;
let voiceHudWindow = null;
let pendingAITasks = [];
let sensorCaptureTimer = null;
let sensorCaptureInProgress = false;
let activeVoiceSession = null;
let activeStudySession = null;
const DEFAULT_VOICE_SHORTCUT = 'CommandOrControl+Shift+Space';
const LEGACY_VOICE_SHORTCUT = 'CommandOrControl+Space';
const PLANNER_STEP_THROTTLE_MS = 700;
const SCREENSHOT_RETENTION_DAYS = 7;

// Memory Graph Processing Timers
let episodeGenerationTimer = null;
let suggestionEngineTimer = null;
let semanticsTimer = null;
let dailyInsightTimer = null;
let weeklyInsightTimer = null;
  }
  if (livingCoreTimer) {
    clearTimeout(livingCoreTimer);
    livingCoreTimer = null;
let livingCoreTimer = null;
let episodeJobLock = false;
let suggestionJobLock = false;
let lastSuggestionLockSkipLogAt = 0;
let suggestionRunQueued = false;
let lastCaptureSuggestionTriggerAt = 0;
const MAX_PRACTICAL_SUGGESTIONS = 10;
const CAPTURE_TRIGGER_MIN_INTERVAL_MS = 15 * 60 * 1000;
const SUGGESTION_REFRESH_INTERVAL_MINUTES = 30;
const LOW_POWER_OCR_MIN_INTERVAL_MS = 5 * 60 * 1000;
const LOW_POWER_HEAVY_JOB_MIN_GAP_MS = 90 * 60 * 1000;
let lastLowPowerOCRAt = 0;
let lastEpisodeHeavyRunAt = 0;
let lastSuggestionHeavyRunAt = 0;
const performanceState = {
  onBattery: false,
  thermalState: 'unknown'
};

function isReducedLoadMode() {
  return Boolean(performanceState.onBattery) || ['serious', 'critical'].includes(String(performanceState.thermalState || '').toLowerCase());
}

function canRunHeavyJob(lastRunAt = 0) {
  if (!isReducedLoadMode()) return true;
  return (Date.now() - Number(lastRunAt || 0)) >= LOW_POWER_HEAVY_JOB_MIN_GAP_MS;
}

function updatePerformanceState(next = {}) {
  Object.assign(performanceState, next || {});
  const mode = isReducedLoadMode() ? 'reduced' : 'normal';
  store.set('performanceState', {
    ...performanceState,
    mode,
    updated_at: new Date().toISOString()
  });
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
    mainWindow.webContents.send('study-session-update', payload);
  }
}

function setStudySessionState(nextState = {}) {
  activeStudySession = {
    ...(getStudySessionState() || {}),
    ...(nextState || {})
  };
  store.set('studySessionState', activeStudySession);
  emitStudySessionUpdate();
  return activeStudySession;
}

function inferStudySignal(text = '', metadata = {}) {
  const rawText = String(text || '').trim();
  const windowTitle = String(metadata?.activeWindowTitle || '').trim();
  const appName = String(metadata?.activeApp || '').trim();
  const hay = `${rawText} ${windowTitle} ${appName}`.toLowerCase().trim();
  if (!hay) return '';
  if (/\bquiz|exam|grade|result|score|feedback\b/.test(hay)) return 'revision';
  if (/\bproblem|exercise|worksheet|question|leetcode|solve|proof\b/.test(hay)) return 'solving';
  if (/\bessay|draft|write|thesis|paragraph|summary|proposal|report\b/.test(hay)) return 'drafting';
  if (/\bread|chapter|lecture|slides|notes|article|paper|documentation\b/.test(hay)) return 'reading';
  if (/\bcalendar|meeting|agenda|switch|tab|window\b/.test(hay)) return 'context-switch';
  if (/\b(youtube|netflix|spotify|tiktok|instagram|x\\.com|twitter|reddit)\b/.test(hay) && !/\b(lecture|course|tutorial|documentation|lesson|study)\b/.test(hay)) return 'distraction';
  // Do not force "idle" when there is readable context; let downstream extractor infer concrete activity.
  if (rawText.length < 12 && !windowTitle && !appName) return 'idle';
  return '';
}

function pruneOldSensorCaptures(events = []) {
  const retentionMs = SCREENSHOT_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const cutoff = Date.now() - retentionMs;
  const kept = [];
  for (const event of Array.isArray(events) ? events : []) {
    const ts = Number(event?.timestamp || 0);
    if (!ts || ts >= cutoff) {
      kept.push(event);
      continue;
    }
    if (event?.imagePath && fs.existsSync(event.imagePath)) {
      try { fs.unlinkSync(event.imagePath); } catch (_) {}
    }
  }
  return kept;
}

function startScreenshotCleanupLoop() {
  const runCleanup = () => {
    const before = getSensorEvents();
    const after = pruneOldSensorCaptures(before);
    if (after.length !== before.length) {
      store.set('sensorEvents', after);
      console.log(`[SensorCapture] Pruned ${before.length - after.length} expired screenshots (>${SCREENSHOT_RETENTION_DAYS} days)`);
    }
  };

  runCleanup();
  if (screenshotCleanupTimer) {
    clearInterval(screenshotCleanupTimer);
    screenshotCleanupTimer = null;
  }
  screenshotCleanupTimer = setInterval(runCleanup, 6 * 60 * 60 * 1000);
}

// ── Graph Helpers ──────────────────────────────────────────────────────────
function getGraph() {
  return {
    nodes: store.get('graphNodes') || [],
    edges: store.get('graphEdges') || []
  };
}

function saveGraph(nodes, edges) {
  store.set('graphNodes', nodes);
  store.set('graphEdges', edges);
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
    const engine = require('./services/agent/intelligence-engine');
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
const { WebSocketServer } = require('ws');
const NATIVE_HOST_NAME = 'com.proactive.browser_agent';
const EXTENSION_BRIDGE_PORT = 3003;

// Local relay used by the native host to bridge desktop <-> extension messages.
let extensionSocket = null;
let extensionLastSeen = null;
let extensionTransport = 'native-messaging';
let wss = null;
let relayHeartbeatInterval = null;

function emitExtensionConnectionEvent(type, extra = {}) {
  if (mainWindow && mainWindow.webContents) {
    mainWindow.webContents.send('extension-event', { type, timestamp: Date.now(), ...extra });
  }
}

function ensureNativeHostAllowsExtensionId(extensionId) {
  const id = String(extensionId || '').trim();
  if (!/^[a-p]{32}$/.test(id)) return false;

  const origin = `chrome-extension://${id}/`;
  const sourcePath = path.join(__dirname, `${NATIVE_HOST_NAME}.json`);
  const targetPath = path.join(
    app.getPath('home'),
    'Library',
    'Application Support',
    'Google',
    'Chrome',
    'NativeMessagingHosts',
    `${NATIVE_HOST_NAME}.json`
  );

  let changed = false;
  const patchManifest = (manifestPath) => {
    try {
      if (!fs.existsSync(manifestPath)) return;
      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
      const origins = Array.isArray(manifest.allowed_origins) ? manifest.allowed_origins : [];
      if (!origins.includes(origin)) {
        manifest.allowed_origins = [...origins, origin];
        fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
        changed = true;
      }
    } catch (e) {
      console.warn('[NativeHost] Failed to patch manifest origin:', manifestPath, e?.message || e);
    }
  };

  patchManifest(sourcePath);
  patchManifest(targetPath);
  if (changed) {
    console.log(`[NativeHost] Added allowed origin for extension ${id}`);
  }
  return changed;
}

function isExtensionConnected() {
  if (!extensionSocket) return false;
  if (!extensionLastSeen) return true;
  return (Date.now() - extensionLastSeen) < 45000;
}

async function waitForExtensionConnection(timeoutMs = 90000, pollMs = 500) {
  const start = Date.now();
  while ((Date.now() - start) < timeoutMs) {
    if (isExtensionConnected()) return true;
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
  return false;
}

function bindExtensionRelayServer(server) {
  if (!server) return;
  server.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    console.log('Native host bridge connected to Proactive relay');
    extensionSocket = ws;
    extensionLastSeen = Date.now();
    extensionTransport = 'native-messaging';
    flushPendingAITasks();
    emitExtensionConnectionEvent('connected', { transport: 'native-messaging' });

    ws.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        const bridgeMessage = (
          message.type === 'HOST_STATUS' ||
          message.type === 'EXTENSION_BRIDGE_STATUS' ||
          message.type === 'APP_BRIDGE_PONG' ||
          message.type === 'run-diagnostic-result' ||
          message.type === 'task-progress' ||
          message.type === 'task-result' ||
          message.type === 'ACTION_RESULT' ||
          message.type === 'ACTION_PROGRESS' ||
          message.type === 'action_result' ||
          message.type === 'action-result' ||
          (typeof message.task_id !== 'undefined' && typeof message.status !== 'undefined')
        );

        if (bridgeMessage) {
          extensionLastSeen = Date.now();
        }
        if (message.extensionId) {
          ensureNativeHostAllowsExtensionId(message.extensionId);
        }
        if (message.transport) {
          extensionTransport = String(message.transport);
        }
        if (message.type === 'HOST_STATUS' || message.type === 'EXTENSION_BRIDGE_STATUS') {
          emitExtensionConnectionEvent('connected', {
            transport: 'native-messaging',
            status: message.status || 'connected'
          });
        }
        if (message.type === 'puppeteer-execute') {
          (async () => {
            try {
              const task = message.task || {};
              console.log('Received puppeteer-execute for', task.title, task.url);
              const result = await runPuppeteerTask(task);
              ws.send(JSON.stringify({ type: 'task-result', status: result.status === 'success' ? 'success' : 'error', result: result }));
            } catch (err) {
              ws.send(JSON.stringify({ type: 'task-result', status: 'error', error: err.message }));
            }
          })();
        }
        if (bridgeMessage) {
          console.log('Extension event (raw):', message.type || '<action-result>', message.progress || message.result || '');
          if (mainWindow && mainWindow.webContents) {
            try { mainWindow.webContents.send('extension-event', message); } catch (e) { console.warn('Failed to forward extension-event to renderer', e); }
          }
        }
      } catch (_) {}
    });

    ws.on('close', () => {
      if (extensionSocket === ws) {
        console.log('Native host bridge disconnected');
        extensionSocket = null;
        extensionLastSeen = null;
        extensionTransport = 'native-messaging';
        emitExtensionConnectionEvent('disconnected', { transport: 'native-messaging' });
      }
    });
  });

  if (relayHeartbeatInterval) clearInterval(relayHeartbeatInterval);
  relayHeartbeatInterval = setInterval(() => {
    if (!wss) return;
    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) return ws.terminate();
      ws.isAlive = false;
      ws.ping();
      try {
        ws.send(JSON.stringify({ type: 'APP_BRIDGE_PING', timestamp: Date.now() }));
      } catch (_) {}
    });

    if (extensionSocket && extensionLastSeen && (Date.now() - extensionLastSeen) > 60000) {
      try { extensionSocket.terminate(); } catch (_) {}
      extensionSocket = null;
      extensionLastSeen = null;
      extensionTransport = 'native-messaging';
      emitExtensionConnectionEvent('disconnected', { transport: 'native-messaging', reason: 'heartbeat-timeout' });
    }
  }, 30000);

  server.on('close', () => {
    if (relayHeartbeatInterval) {
      clearInterval(relayHeartbeatInterval);
      relayHeartbeatInterval = null;
    }
  });
}

function startExtensionRelayServer() {
  try {
    const server = new WebSocketServer({ port: EXTENSION_BRIDGE_PORT });
    server.on('listening', () => {
      wss = server;
      console.log(`Native host relay listening on port ${EXTENSION_BRIDGE_PORT}`);
    });
    server.on('error', (err) => {
      if (err?.code === 'EADDRINUSE') {
        console.warn(`[Native host relay] Port ${EXTENSION_BRIDGE_PORT} already in use. Desktop-extension bridge disabled for this run.`);
        emitExtensionConnectionEvent('disconnected', { transport: 'native-messaging', reason: 'relay-port-in-use' });
      } else {
        console.warn('[Native host relay] Failed to start:', err?.message || err);
      }
      try { server.close(); } catch (_) {}
    });
    bindExtensionRelayServer(server);
  } catch (err) {
    if (err?.code === 'EADDRINUSE') {
      console.warn(`[Native host relay] Port ${EXTENSION_BRIDGE_PORT} already in use. Desktop-extension bridge disabled for this run.`);
    } else {
      console.warn('[Native host relay] Startup error:', err?.message || err);
    }
  }
}

startExtensionRelayServer();

// Scheduled tasks
let dailySummaryTimer = null;
let patternUpdateTimer = null;
let minutelySyncTimer = null;
let minutelySyncInterval = null;
let morningBriefTimer = null;
let screenshotCleanupTimer = null;

const MINUTELY_MS = 60 * 1000;
const GOOGLE_SYNC_BASELINE_ISO = '2020-01-01T00:00:00.000Z';
const GOOGLE_SYNC_OVERLAP_MS = 5 * 60 * 1000;
const GOOGLE_SYNC_FUTURE_DRIFT_MS = 5 * 60 * 1000;
const DEFAULT_SENSOR_SETTINGS = {
  enabled: true, // Auto-enable for continuous capture
  intervalMinutes: 0.5, // 30 seconds
  maxEvents: 200 // Increase for more frequent captures
};

function getSensorSettings() {
  const stored = store.get('sensorSettings') || {};
  // Force fixed 30-second capture interval
  const intervalMinutes = 0.5;
  const maxEvents = Math.max(50, parseInt(stored.maxEvents, 10) || DEFAULT_SENSOR_SETTINGS.maxEvents);
  return {
    enabled: stored.enabled !== undefined ? Boolean(stored.enabled) : DEFAULT_SENSOR_SETTINGS.enabled,
    intervalMinutes,
    maxEvents
  };
}

function getSensorEvents() {
  return store.get('sensorEvents') || [];
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
    lastCaptureAt: events[0]?.timestamp || null,
    totalCaptures: events.length,
    screenPermission,
    transport: 'desktop-capturer',
    study_session: getStudySessionState(),
    performance_mode: isReducedLoadMode() ? 'reduced' : 'normal'
  };
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

function mergeSuggestionQueues(existing = [], incoming = [], limit = MAX_PRACTICAL_SUGGESTIONS) {
  const out = [];
  const seen = new Set();
  const push = (item) => {
    if (!item || item.completed) return;
    const key = suggestionQueueKey(item);
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(item);
  };

  // Keep existing items first, then append new ones until cap.
  (Array.isArray(existing) ? existing : []).forEach(push);
  (Array.isArray(incoming) ? incoming : []).forEach(push);
  return out.slice(0, Math.max(1, Number(limit || MAX_PRACTICAL_SUGGESTIONS)));
}

function ensureSensorStorageDir() {
  const capturesDir = path.join(app.getPath('userData'), 'sensor-captures');
  if (!fs.existsSync(capturesDir)) {
    fs.mkdirSync(capturesDir, { recursive: true });
  }
  return capturesDir;
}

// Content filtering functions
function isSensitiveContent(text, windowTitle, appName) {
  const content = `${text} ${windowTitle} ${appName}`.toLowerCase();
  
  // Banking and financial sites
  const bankingKeywords = [
    'bank', 'chase', 'wells fargo', 'bank of america', 'citibank', 'capital one',
    'paypal', 'venmo', 'cash app', 'zelle', 'transfer', 'routing number',
    'account number', 'credit card', 'debit card', 'balance', 'transaction',
    'login', 'signin', 'password', 'security code', 'ssn', 'social security'
  ];
  
  // Adult content keywords
  const adultKeywords = [
    'porn', 'xxx', 'sex', 'adult', 'nsfw', 'erotic', 'nude', 'naked',
    'hookup', 'dating', 'escort', 'cam', 'strip', 'adultfriendfinder'
  ];
  
  // Password and security sensitive sites
  const passwordKeywords = [
    'password', 'passphrase', 'secret key', 'private key', 'api key',
    'two factor', '2fa', 'authentication', 'security question',
    'reset password', 'change password', 'forgot password'
  ];
  
  // Check each category
  const isBanking = bankingKeywords.some(keyword => content.includes(keyword));
  const isAdult = adultKeywords.some(keyword => content.includes(keyword));
  const isPasswordRelated = passwordKeywords.some(keyword => content.includes(keyword));
  
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
      return { shouldFilter: true, reason: 'Sensitive domain detected' };
    }
  }
  
  // Check content-based filtering
  const contentCheck = isSensitiveContent(text, windowTitle, appName);
  if (contentCheck.isSensitive) {
    return { shouldFilter: true, reason: contentCheck.reason };
  }
  
  return { shouldFilter: false };
}

async function deleteSensitiveCapture(imagePath, eventId, reason) {
  try {
    // Delete the image file
    if (fs.existsSync(imagePath)) {
      fs.unlinkSync(imagePath);
    }
    
    // Remove from sensor events
    const events = getSensorEvents();
    const filteredEvents = events.filter(event => event.id !== eventId);
    store.set('sensorEvents', filteredEvents);
    
    console.log(`[Content Filter] Deleted sensitive capture: ${reason}`);
  } catch (error) {
    console.error('[Content Filter] Failed to delete sensitive capture:', error);
  }
}

function runVisionOCR(imagePath) {
  const scriptPath = path.join(__dirname, 'ocr_vision.swift');
  return new Promise((resolve) => {
    execFile('/usr/bin/xcrun', ['swift', scriptPath, imagePath], { timeout: 60000 }, (error, stdout, stderr) => {
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
    execFile('/usr/bin/xcrun', ['swift', scriptPath], { timeout: 15000 }, (error, stdout, stderr) => {
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
  if (sensorCaptureInProgress) {
    // Avoid overlapping captures
    return null;
  }
  sensorCaptureInProgress = true;
  const settings = getSensorSettings();
  const capturesDir = ensureSensorStorageDir();
  const timestamp = Date.now();
  const filename = `capture_${timestamp}.png`;
  const imagePath = path.join(capturesDir, filename);
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
      if (fs.existsSync(imagePath)) {
        captureMode = 'frontmost-window';
        sourceName = windowContext.windowTitle || windowContext.appName || 'Window';
      }
    } catch (_) {}
  }

  if (captureMode !== 'frontmost-window') {
    const primary = screen.getPrimaryDisplay();
    const fullSize = primary?.size || { width: 1920, height: 1080 };
    const reducedLoad = isReducedLoadMode();
    const maxWidth = reducedLoad ? 960 : 1440;
    const scale = Math.min(1, maxWidth / Math.max(1, Number(fullSize.width || 1920)));
    const thumbWidth = Math.max(640, Math.floor((fullSize.width || 1920) * scale));
    const thumbHeight = Math.max(360, Math.floor((fullSize.height || 1080) * scale));
    const sources = await desktopCapturer.getSources({
      types: ['screen'],
      thumbnailSize: { width: thumbWidth, height: thumbHeight }
    });
    const source = sources[0];
    if (!source) {
      throw new Error('No screen source available for capture');
    }

    const pngBuffer = source.thumbnail.toPNG();
    if (!pngBuffer || !pngBuffer.length) {
      sensorCaptureInProgress = false;
      throw new Error('Screen capture returned an empty image');
    }
    try {
      await fs.promises.writeFile(imagePath, pngBuffer);
    } catch (writeErr) {
      sensorCaptureInProgress = false;
      throw writeErr;
    }
    sourceName = source.name || 'Screen';
  }

  const contextSuffix = [windowContext.appName, windowContext.windowTitle].filter(Boolean).join(' - ');
  const studySession = getStudySessionState();
  const inStudySession = studySession?.status === 'active' && Boolean(studySession?.session_id);

  const event = {
    id: `sensor_${timestamp}`,
    type: 'screen_capture',
    title: `Desktop capture: ${contextSuffix || sourceName || 'Screen'}`,
    timestamp,
    captured_at: new Date(timestamp).toISOString(),
    captured_at_local: new Date(timestamp).toLocaleString(),
    sourceName,
    captureMode,
    activeApp: windowContext.appName || '',
    activeWindowTitle: windowContext.windowTitle || '',
    windowId: windowContext.windowId || null,
    windowBounds: windowContext.bounds || null,
    windowContextStatus: windowContext.status || 'unavailable',
    imagePath,
    text: '',
    textCaptureSource: 'none',
    ocrLines: [],
    ocrConfidence: 0,
    ocrStatus: 'processing',
    reason,
    study_session_id: inStudySession ? studySession.session_id : null,
    study_goal: inStudySession ? (studySession.goal || '') : '',
    study_subject: inStudySession ? (studySession.subject || '') : ''
  };
  if (windowContext.error) event.windowContextError = windowContext.error;
  // Always run OCR so screenshot text capture is not reliant on AX-only extraction.
  const axText = String(windowContext.extractedText || '').trim();
  const ocr = await runVisionOCR(imagePath);
  const ocrText = String(ocr.text || '').trim();
  const mergedText = [axText, ocrText].filter(Boolean).join('\n').trim();
  event.text = mergedText;
  event.textCaptureSource = axText && ocrText ? 'ax+ocr' : (ocrText ? 'ocr' : (axText ? 'ax' : 'none'));
  event.ocrLines = ocr.lines || [];
  event.ocrConfidence = ocr.confidence || 0;
  event.ocrStatus = ocr.status || (ocrText ? 'complete' : 'no_text');
  if (ocr.error) event.ocrError = ocr.error;
  if (isReducedLoadMode()) lastLowPowerOCRAt = Date.now();

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
    null // URL not available for screen captures
  );
  
  if (filterCheck.shouldFilter) {
    // Clean up and delete the sensitive capture
    sensorCaptureInProgress = false;
    await deleteSensitiveCapture(imagePath, event.id, filterCheck.reason);
    return null; // Return null to indicate capture was filtered
  }

  const existing = pruneOldSensorCaptures(getSensorEvents());
  const nextEvents = [event, ...existing].slice(0, settings.maxEvents);
  const retainedPaths = new Set(nextEvents.map(item => item.imagePath));
  existing.forEach(item => {
    if (item.imagePath && !retainedPaths.has(item.imagePath) && fs.existsSync(item.imagePath)) {
      try { fs.unlinkSync(item.imagePath); } catch (_) {}
    }
  });

  store.set('sensorEvents', nextEvents);
  
  // L1 Ingestion
  try {
    const { ingestRawEvent } = require('./services/ingestion');
    await ingestRawEvent({ 
      type: 'ScreenCapture', 
      timestamp: event.timestamp, 
      source: 'Sensors', 
      text: [
        event.activeApp ? `App: ${event.activeApp}` : '',
        event.activeWindowTitle ? `Window: ${event.activeWindowTitle}` : '',
        event.text ? `Captured text:\n${event.text}` : ''
      ].filter(Boolean).join('\n'),
      metadata: {
        ...event,
        app: event.activeApp || 'Desktop',
        window_title: event.activeWindowTitle || '',
        text_capture_source: event.textCaptureSource || 'none',
        study_context: event.study_context,
        study_signal: event.study_signal,
        study_session_id: event.study_session_id,
        study_goal: event.study_goal,
        study_subject: event.study_subject
      }
    });
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastDesktopCaptureAt: new Date().toISOString(),
      desktopCaptureStatus: 'idle',
      lastDesktopCaptureSource: event.textCaptureSource || 'none',
      lastDesktopCaptureApp: event.activeApp || ''
    });
    
    // Throttle expensive background suggestion generation to avoid UI slowdown.
    if ((Date.now() - lastCaptureSuggestionTriggerAt) >= CAPTURE_TRIGGER_MIN_INTERVAL_MS) {
      lastCaptureSuggestionTriggerAt = Date.now();
      setTimeout(() => {
        runSuggestionEngineJob().catch(err => 
          console.log('Background suggestion engine trigger failed:', err?.message || err)
        );
      }, 10000); // 10 second delay to allow ingestion to complete
    }
  } catch (e) {
    console.error('[captureDesktopSensorSnapshot] L1 ingestion failed:', e);
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      desktopCaptureStatus: 'error',
      lastDesktopCaptureError: e?.message || String(e),
      lastDesktopCaptureErrorAt: new Date().toISOString()
    });
  }

  sensorCaptureInProgress = false;
  return event;
}

function startSensorCaptureLoop() {
  const settings = getSensorSettings();
  if (sensorCaptureTimer) {
    clearInterval(sensorCaptureTimer);
    sensorCaptureTimer = null;
  }
  if (!settings.enabled) return;

  const intervalMs = settings.intervalMinutes * 60 * 1000;
  sensorCaptureTimer = setInterval(() => {
    // Fire-and-forget scheduled capture to keep it in the background
    captureDesktopSensorSnapshot('scheduled').catch((error) => {
      console.warn('Scheduled sensor capture failed:', error && error.message ? error.message : error);
    });
  }, intervalMs);
}

// Schedule daily tasks
function scheduleDailyTasks() {
  const now = new Date();

  if (dailySummaryTimer) clearTimeout(dailySummaryTimer);
  if (patternUpdateTimer) clearTimeout(patternUpdateTimer);
  if (morningBriefTimer) clearTimeout(morningBriefTimer);
  
  // Schedule morning brief at 7:00 AM
  const morningBriefTime = new Date();
  morningBriefTime.setHours(7, 0, 0, 0);
  if (morningBriefTime <= now) {
    morningBriefTime.setDate(morningBriefTime.getDate() + 1);
  }
  const morningBriefDelay = morningBriefTime.getTime() - now.getTime();

  morningBriefTimer = setTimeout(async () => {
    console.log('Running scheduled morning brief generation...');
    try {
      const brief = await generateMorningBrief({ force: true, scheduled: true });
      console.log(`Scheduled morning brief completed (${brief?.date || 'unknown date'})`);
    } catch (error) {
      console.error('Error in scheduled morning brief:', error);
    }

    // Schedule next day
    scheduleDailyTasks();
  }, morningBriefDelay);

  console.log(`Morning brief scheduled for: ${morningBriefTime.toLocaleString()}`);

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
    scheduleDailyTasks();
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
      const browsingHistory = await getBrowserHistory();
      
      // Analyze patterns and update profile
      await updateUserPatterns(userProfile, browsingHistory);
      store.set('userProfile', userProfile);
      
      console.log('Scheduled pattern update completed');
    } catch (error) {
      console.error('Error in scheduled pattern update:', error);
    }
    
    // Schedule next day's pattern update
    scheduleDailyTasks();
  }, patternDelay);
  
  console.log(`Pattern update scheduled for: ${patternTime.toLocaleString()}`);

  // Schedule minutely GSuite sync
  const lastSync = store.get('googleData')?.lastSync;
  const lastSyncMs = parseSyncCursorMs(lastSync);
  const timeSinceSync = lastSyncMs ? (now.getTime() - lastSyncMs) : MINUTELY_MS;
  const syncDelay = Math.max(0, MINUTELY_MS - timeSinceSync);

  if (minutelySyncTimer) clearTimeout(minutelySyncTimer);
  minutelySyncTimer = setTimeout(async () => {
    await runMinutelySync();
    startMinutelySyncLoop();
  }, syncDelay);

  console.log(`Minutely sync scheduled for: ${new Date(Date.now() + syncDelay).toLocaleString()}`);
}

async function runMinutelySync() {
  console.log('Running automated minutely GSuite sync...');
  if (global.__gsuite_sync_lock) {
    console.log('GSuite sync already running; skipping this cycle');
    return;
  }
  global.__gsuite_sync_lock = true;
  try {
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastSyncAttemptAt: new Date().toISOString(),
      syncStatus: 'running'
    });
    const existingGoogleData = store.get('googleData') || {};
    const googleDelta = await getGoogleData({ since: existingGoogleData.lastSync });
    const syncMeta = googleDelta._meta || {};
    const existingEmailIds = new Set((existingGoogleData.gmail || []).map((m) => m.id));
    const existingEventsById = new Map((existingGoogleData.calendar || []).map((e) => [e.id, e]));
    let newEmailCount = 0;
    let newEventCount = 0;
    let editedEventCount = 0;

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
    store.set('memoryGraphHealth', {
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
      const { ingestRawEvent } = require('./services/ingestion');
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

      const browserHistory = await getBrowserHistory();
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
      
      console.log(`[runMinutelySync] Ingested ${ingestedCount} new raw events into SQLite L1. Checks: emails=${newEmailCount}, new_events=${newEventCount}, edited_events=${editedEventCount}, cursor_advanced=${shouldAdvanceLastSync}`);
    } catch (gErr) {
      console.warn('[runMinutelySync] L1 ingestion failed:', gErr.message || gErr);
      store.set('memoryGraphHealth', {
        ...(store.get('memoryGraphHealth') || {}),
        syncStatus: 'error',
        lastSyncError: gErr?.message || String(gErr),
        lastSyncErrorAt: new Date().toISOString()
      });
    }
  } finally {
    global.__gsuite_sync_lock = false;
  }
}

function startMinutelySyncLoop() {
  if (minutelySyncInterval) clearInterval(minutelySyncInterval);
  store.set('memoryGraphHealth', {
    ...(store.get('memoryGraphHealth') || {}),
    minutelySyncLoopActive: true
  });
  minutelySyncInterval = setInterval(() => {
    runMinutelySync().catch((e) => console.warn('Minutely sync interval failed:', e?.message || e));
  }, MINUTELY_MS);
}

function startSourceWarmup() {
  setTimeout(() => {
    runMinutelySync().catch((e) => console.warn('[Warmup] Minutely sync failed:', e?.message || e));
  }, 8000);

  setTimeout(() => {
    if (!getSensorSettings().enabled) return;
    captureDesktopSensorSnapshot('startup').catch((e) => console.warn('[Warmup] Startup capture failed:', e?.message || e));
  }, 12000);
}

// ── Memory Graph Processing Functions ───────────────────────────────

async function runEpisodeGeneration() {
  if (episodeJobLock) {
    console.log('[EpisodeJob] Already running, skipping this cycle');
    return;
  }
  if (!canRunHeavyJob(lastEpisodeHeavyRunAt)) {
    console.log('[EpisodeJob] Skipping to reduce system load (battery/thermal mode)');
    return;
  }
  lastEpisodeHeavyRunAt = Date.now();
  episodeJobLock = true;
  
  try {
    console.log('[EpisodeJob] Running 30-minute episode generation...');
    const { runEpisodeJob } = require('./services/agent/intelligence-engine');
    
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastEpisodeAttemptAt: new Date().toISOString(),
      episodeStatus: 'running'
    });
    const newEpisodeIds = await runEpisodeJob(process.env.DEEPSEEK_API_KEY || null);
    console.log(`[EpisodeJob] Generated ${newEpisodeIds.length} new episodes`);
    store.set('lastEpisodeRun', new Date().toISOString());
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastEpisodeRunAt: new Date().toISOString(),
      lastEpisodeCount: newEpisodeIds.length,
      episodeStatus: 'idle'
    });
    
    // Send status to UI
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', {
        type: 'episodes_generated',
        count: newEpisodeIds.length,
        timestamp: Date.now()
      });
    }
  } catch (error) {
    console.error('[EpisodeJob] Error:', error.message || error);
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      episodeStatus: 'error',
      lastEpisodeError: error?.message || String(error),
      lastEpisodeErrorAt: new Date().toISOString()
    });
  } finally {
    episodeJobLock = false;
  }
}

async function runSemanticWindowGeneration() {
  try {
    if (!canRunHeavyJob(lastEpisodeHeavyRunAt)) {
      console.log('[SemanticWindow] Skipping to reduce system load');
      return;
    }
    console.log('[SemanticWindow] Running half-hour semantic summary...');
    const { runSemanticSummaryWindow } = require('./services/agent/intelligence-engine');
    const result = await runSemanticSummaryWindow(30 * 60 * 1000, process.env.DEEPSEEK_API_KEY || null);
    const semIds = Array.isArray(result) ? result.filter(Boolean) : (result ? [result] : []);
    if (semIds.length) {
      console.log('[SemanticWindow] Created semantic nodes:', semIds.join(', '));
      if (mainWindow && mainWindow.webContents) {
        for (const id of semIds) {
          mainWindow.webContents.send('memory-graph-update', {
            type: 'semantic_window_generated',
            id,
            count: semIds.length,
            timestamp: Date.now()
          });
        }
      }
    } else {
      console.log('[SemanticWindow] No events to summarize in this window');
    }
  } catch (e) {
    console.error('[SemanticWindow] Error:', e?.message || e);
  }
}

async function runSuggestionEngineJob() {
  if (suggestionJobLock) {
    suggestionRunQueued = true;
    const now = Date.now();
    if ((now - lastSuggestionLockSkipLogAt) > (5 * 60 * 1000)) {
      console.log('[SuggestionEngine] Already running; queued one follow-up run');
      lastSuggestionLockSkipLogAt = now;
    }
    return;
  }
  if (!canRunHeavyJob(lastSuggestionHeavyRunAt)) {
    console.log('[SuggestionEngine] Skipping to reduce system load (battery/thermal mode)');
    return;
  }
  lastSuggestionHeavyRunAt = Date.now();
  suggestionJobLock = true;
  
  try {
    const llmConfig = getSuggestionLLMConfig();
    const envToggle = String(process.env.PROACTIVE_AUTO_CREATE_TODOS || '').toLowerCase();
    const persistedToggle = store.get('autoCreateTodos');
    const autoCreateEnabled = (envToggle === '' || envToggle === 'true') && (persistedToggle !== false);
    if (!llmConfig) {
      console.warn('[SuggestionEngine] No active LLM configuration; skipping suggestion generation');
      if (mainWindow && mainWindow.webContents) {
        mainWindow.webContents.send('proactive-suggestions', store.get('suggestions') || []);
      }
      return;
    }

    const episodeCountRow = await db.getQuery(`SELECT COUNT(1) AS count FROM memory_nodes WHERE layer = 'episode'`).catch(() => ({ count: 0 }));
    const episodeCount = Number(episodeCountRow?.count || 0);
    const lastEpisodeRunMs = Date.parse(String(store.get('lastEpisodeRun') || '')) || 0;
    const episodeGraphStale = !lastEpisodeRunMs || (Date.now() - lastEpisodeRunMs) > (45 * 60 * 1000);
    if (!episodeJobLock && (episodeCount === 0 || episodeGraphStale)) {
      console.log('[SuggestionEngine] Refreshing episode graph before suggestion generation...');
      await runEpisodeGeneration().catch((err) => {
        console.warn('[SuggestionEngine] Episode refresh failed before suggestion run:', err?.message || err);
      });
    }

    console.log(`[SuggestionEngine] Running 30-minute suggestion engine (${llmConfig.provider})...`);
    const { generateTopTodosFromMemoryQuery, generateAndPersistTasksFromLLM } = require('./services/agent/suggestion-engine');
    const proactiveMemory = store.get('proactiveMemory') || { core: '' };
    
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastSuggestionAttemptAt: new Date().toISOString(),
      suggestionStatus: 'running'
    });
    const newSuggestions = await generateTopTodosFromMemoryQuery(llmConfig, {
      query: 'Look through my memory and generate top 5 todos or actions I need to do right now.',
      standing_notes: proactiveMemory.core || '',
      study_context: getStudySessionState()
    });
    console.log(`[SuggestionEngine] Generated ${newSuggestions.length} actionable suggestions`);
    store.set('lastSuggestionRun', new Date().toISOString());
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      lastSuggestionRunAt: new Date().toISOString(),
      lastSuggestionCount: newSuggestions.length,
      suggestionStatus: 'idle'
    });
    
    // Keep only AI-generated suggestions; no non-AI fallbacks allowed.
    const latestSuggestions = (Array.isArray(newSuggestions) ? newSuggestions : [])
      .filter((item) => item && item.ai_generated !== false)
      .sort((a, b) => Number(b.score || b.confidence || 0) - Number(a.score || a.confidence || 0))
      .slice(0, MAX_PRACTICAL_SUGGESTIONS);
    const existingSuggestions = store.get('suggestions') || [];
    const outgoingSuggestions = mergeSuggestionQueues(existingSuggestions, latestSuggestions, MAX_PRACTICAL_SUGGESTIONS);
    store.set('suggestions', outgoingSuggestions);
    if (latestSuggestions.length) {
      // Auto-convert top suggestions into persistent todos if they are not duplicates.
      // Gate this behavior behind an environment variable and a persisted setting so it can be disabled.
      if (autoCreateEnabled) {
        try {
          const persistentTodos = store.get('persistentTodos') || [];
          // Use the project's normalizeSuggestion helper to create a compact todo representation
          const candidates = latestSuggestions.slice(0, 3).map((sugg) => {
            const ns = normalizeSuggestion(sugg, { now: Date.now() });
            return {
              id: ns.id || `todo_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
              title: ns.title || ns.description || 'Task',
              description: ns.description || ns.reason || '',
              reason: ns.reason || '',
              priority: ns.priority || 'medium',
              category: ns.category || 'work',
              createdAt: Date.now(),
              completed: false,
              source: 'auto_suggestion'
            };
          });

          // Merge and dedupe using existing deduplicateTasks helper
          const merged = deduplicateTasks([...(persistentTodos || []), ...candidates]);
          // Keep a reasonable cap on persistent todos
          const capped = merged.slice(0, 100);
          store.set('persistentTodos', capped);
          try {
            console.log('[SuggestionEngine] Auto-created persistent todos from suggestions:', candidates.map(c => c.title));
          } catch (_) {}
          if (mainWindow && mainWindow.webContents) {
            try { mainWindow.webContents.send('persistent-todos-updated', capped); } catch (_) {}
          }
        } catch (e) {
          console.warn('[SuggestionEngine] Auto-create todos failed:', e?.message || e);
        }
      } else {
        try { console.log('[SuggestionEngine] Auto-create persistent todos disabled by config'); } catch (_) {}
      }
    }

    // Optionally call LLM to produce 3-5 structured tasks and persist them as well
    try {
      const generateTasksEnabled = autoCreateEnabled && llmConfig;
      if (generateTasksEnabled) {
        generateAndPersistTasksFromLLM(llmConfig, {
          standing_notes: proactiveMemory.core || '',
          study_context: getStudySessionState()
        }).catch((e) => console.warn('[TaskLLM] generation failed:', e?.message || e));
      }
    } catch (e) {
      console.warn('[SuggestionEngine] LLM task generation failed to start:', e?.message || e);
    }

    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('proactive-suggestions', outgoingSuggestions);
    }
    console.log(`[SuggestionEngine] Published ${outgoingSuggestions.length} AI suggestions`);
  } catch (error) {
    console.error('[SuggestionEngine] Error:', error.message || error);
    store.set('memoryGraphHealth', {
      ...(store.get('memoryGraphHealth') || {}),
      suggestionStatus: 'error',
      lastSuggestionError: error?.message || String(error),
      lastSuggestionErrorAt: new Date().toISOString()
    });
  } finally {
    suggestionJobLock = false;
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
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[WeeklyInsight] No DeepSeek API key, skipping weekly insights');
      return;
    }

    console.log('[WeeklyInsight] Running weekly insight generation...');
    const { runWeeklyInsightJob } = require('./services/agent/intelligence-engine');
    
    await runWeeklyInsightJob(apiKey);
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
  }
}

async function runDailyInsightsScheduled() {
  try {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[DailyInsight] No DeepSeek API key, skipping daily insights');
      return;
    }
    console.log('[DailyInsight] Running daily insight generation...');
    const { runDailyInsights } = require('./services/agent/intelligence-engine');
    const created = await runDailyInsights(apiKey);
    console.log('[DailyInsight] Promoted insights:', created.length);
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', { type: 'daily_insights_completed', count: created.length, timestamp: Date.now() });
    }
  } catch (e) {
    console.error('[DailyInsight] Error:', e?.message || e);
  }
}

function startMemoryGraphProcessing() {
  console.log('[MemoryGraph] Starting automated processing...');
  store.set('memoryGraphHealth', {
    ...(store.get('memoryGraphHealth') || {}),
    processorStartedAt: new Date().toISOString(),
    processorTimersActive: true
  });
  
  // Episode generation every 30 minutes
  // Episode generation & semantic window summaries aligned to wall-clock half-hours (:00 and :30)
  try {
    const halfHourMs = 30 * 60 * 1000;
    // clear any previous timers
    if (episodeGenerationTimer) try { clearInterval(episodeGenerationTimer); } catch (_) {}
    if (semanticsTimer) try { clearInterval(semanticsTimer); } catch (_) {}

    const now = Date.now();
    // compute next half-hour boundary
    const nextBoundary = Math.ceil(now / halfHourMs) * halfHourMs;
    const delay = Math.max(1000, nextBoundary - now);

    // schedule first aligned run, then set repeating intervals
    setTimeout(() => {
      runEpisodeGeneration().catch((e) => console.warn('[MemoryGraph] Aligned episode generation failed:', e?.message || e));
      try { episodeGenerationTimer = setInterval(runEpisodeGeneration, halfHourMs); } catch (_) {}
      // also kick off semantic window at same boundary
      runSemanticWindowGeneration().catch((e) => console.warn('[MemoryGraph] Aligned semantic window failed:', e?.message || e));
      try { semanticsTimer = setInterval(runSemanticWindowGeneration, halfHourMs); } catch (_) {}
    }, delay);
    console.log('[MemoryGraph] Aligned episode & semantic scheduling to half-hour boundaries, first run in', Math.round(delay / 1000), 's');
  } catch (e) {
    console.warn('[MemoryGraph] Failed to schedule aligned half-hour jobs, falling back to interval timers:', e?.message || e);
    if (episodeGenerationTimer) clearInterval(episodeGenerationTimer);
    episodeGenerationTimer = setInterval(runEpisodeGeneration, 30 * 60 * 1000);
    if (semanticsTimer) clearInterval(semanticsTimer);
    semanticsTimer = setInterval(runSemanticWindowGeneration, 30 * 60 * 1000);
  }
  
  // Suggestion engine every 30 minutes
  if (suggestionEngineTimer) clearInterval(suggestionEngineTimer);
  suggestionEngineTimer = setInterval(runSuggestionEngineJob, SUGGESTION_REFRESH_INTERVAL_MINUTES * 60 * 1000);
  
  // Schedule weekly insights for Sunday 11:59 PM
  scheduleWeeklyInsights();

  // Schedule daily insights (every day at 23:00 local time by default)
  scheduleDailyInsights();
  scheduleLivingCore();
  
  // Warm the graph immediately on startup, then follow with the scheduled loop.
  setTimeout(() => {
    runEpisodeGeneration().catch((e) => console.warn('[MemoryGraph] Initial episode generation failed:', e?.message || e));
  }, 5000);
  
  // Suggestions can lag slightly behind the graph warmup.
  setTimeout(() => {
    runSuggestionEngineJob().catch((e) => console.warn('[MemoryGraph] Initial suggestion generation failed:', e?.message || e));
  }, 30000);
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
  console.log('[MemoryGraph] Automated processing stopped');
}

// Update user patterns based on browsing behavior
async function updateUserPatterns(userProfile, browsingHistory) {
  const today = new Date().toDateString();
  const todayHistory = browsingHistory.filter(item => 
    new Date(item.last_visit_time).toDateString() === today
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
    const browserHistory = await getBrowserHistory();
    const recentBrowserHistory = browserHistory.filter(visit => 
      visit.last_visit_time > sevenDaysAgo
    ).map(visit => ({
      domain: this.extractDomain(visit.url),
      path: this.extractPath(visit.url),
      page_title: visit.title,
      timestamp: visit.last_visit_time,
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
        await fullGoogleSync({ since: null, forceHistoricalBackfill: true });
        if (mainWindow && mainWindow.webContents) mainWindow.webContents.send('gsuite-sync-complete', store.get('googleData'));
        
        // Auto-trigger memory graph processing after Google sync
        console.log('[oauth2callback] Triggering memory graph processing after Google sync');
        setTimeout(async () => {
          try {
            await runEpisodeGeneration();
            await runSuggestionEngineJob();
            console.log('[oauth2callback] Memory graph processing completed');
            if (mainWindow && mainWindow.webContents) {
              mainWindow.webContents.send('memory-graph-update', {
                type: 'google_sync_completed',
                timestamp: Date.now()
              });
            }
          } catch (e) {
            console.error('[oauth2callback] Memory graph processing failed:', e);
          }
        }, 3000); // Wait 3 seconds for data to settle
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
      const { ingestRawEvent } = require('./services/ingestion');
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
  const fs = require('fs');
  const path = require('path');
  const os = require('os');

  if (!fs.existsSync(dbPath)) return [];

  // Copy first to avoid the browser's exclusive lock
  const tmpPath = path.join(os.tmpdir(), `chromium_hist_${Date.now()}_${Math.random().toString(36).slice(2)}.db`);
  try {
    fs.copyFileSync(dbPath, tmpPath);
  } catch (e) {
    console.warn(`Could not copy ${browserName} DB at ${dbPath}:`, e.message);
    return [];
  }

  try {
    const sqlite3 = require('sqlite3');

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
          db.close(() => { try { fs.unlinkSync(tmpPath); } catch (_) {} });
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
          try {
            console.log(`${browserName} (${dbPath}): ${entries.length} URLs`);
          } catch (logErr) {
            if (String(logErr?.code || '') !== 'EIO') throw logErr;
          }
          resolve(entries);
        }
      );
    });
  } catch (sqlErr) {
    try { fs.unlinkSync(tmpPath); } catch (_) {}
    console.warn(`sqlite3 unavailable for ${browserName}:`, sqlErr.message);
    return [];
  }
}

// Collect Chromium-family history: Chrome, Brave, Arc — all profiles
async function getChromiumHistory() {
  const fs   = require('fs');
  const path = require('path');
  const os   = require('os');
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
    if (!fs.existsSync(base)) continue;

    // Try Default profile + numbered profiles (Profile 1, Profile 2 ...)
    let profileDirs;
    try {
      profileDirs = ['Default', ...fs.readdirSync(base).filter(d => /^Profile \d+$/.test(d))];
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
  const fs = require('fs');
  const path = require('path');
  const os = require('os');

  try {
    if (process.platform !== 'darwin') return [];

    const safariHistoryPath = path.join(os.homedir(), 'Library/Safari/History.db');

    if (!fs.existsSync(safariHistoryPath)) {
      console.log('Safari history file not found');
      return [];
    }

    // Safari also locks its DB while open — copy it first
    const tmpPath = path.join(os.tmpdir(), `safari_history_${Date.now()}.db`);
    try {
      fs.copyFileSync(safariHistoryPath, tmpPath);
    } catch (e) {
      if (e.code === 'EPERM') {
        console.log('Skipping Safari history — Full Disk Access is required for this terminal/app.');
      } else {
        console.warn('Could not copy Safari history DB:', e.message);
      }
      return [];
    }

    try {
      const sqlite3 = require('sqlite3');

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
          db.close(() => {
            try { fs.unlinkSync(tmpPath); } catch (_) {}
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
      try { fs.unlinkSync(tmpPath); } catch (_) {}
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
    const browserHistory = await getBrowserHistory();
    
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
    drive: mergeUniqueById(existing.drive, incoming.drive, 'id'),
    lastSync: incoming.lastSync || existing.lastSync || null
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
    let updated = 0;
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
      ).catch(() => {});
      updated += 1;
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
async function getGoogleData({ since } = {}) {
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
            drive: { successAccounts: 0, failedAccounts: 0, lastError: null }
          }
        }
      };
    }

    let allGmailInbox = [];
    let allGmailSent = [];
    let allCalItems = [];
    let allDriveFiles = [];
    const sinceCursorMs = normalizeSyncCursorMs(since);
    const useIncrementalCursor = Boolean(sinceCursorMs);
    const effectiveSinceMs = useIncrementalCursor
      ? Math.max(Date.parse(GOOGLE_SYNC_BASELINE_ISO), sinceCursorMs - GOOGLE_SYNC_OVERLAP_MS)
      : Date.parse(GOOGLE_SYNC_BASELINE_ISO);
    const gmailAfter = useIncrementalCursor
      ? `after:${Math.floor(effectiveSinceMs / 1000)}`
      : 'after:2020/01/01';
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
      const drive    = google.drive({ version: 'v3', auth });

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

      // ---- Drive: New, modified, or full historical fetch ----
      try {
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
      drive: allDriveFiles,
      _meta: syncMeta
    };
  } catch (error) {
    console.error('Error getting Google data:', error);
    return {
      gmail: [],
      gmailSent: [],
      calendar: [],
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
    const browserHistory = await getBrowserHistory();
    console.log(`Retrieved ${browserHistory.length} URLs from browser history`);
    
    // Get Google data
    const googleData = await getGoogleData();
    const sensorEvents = getSensorEvents();
    
    // Prepare data for AI analysis
    const urlLines = browserHistory.slice(0, 100).map(item => 
      `${item.url} | ${item.title} | ${new Date(item.last_visit_time).toLocaleString()}`
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
      const engine = require('./services/agent/intelligence-engine');
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
    const browserHistory = await getBrowserHistory();

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
ipcMain.handle('get-event-details', (event, eventId) => {
  const historicalSummaries = store.get('historicalSummaries') || {};
  
  // Search through all summaries for this event ID
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
  if (!apiKey) throw new Error('DEEPSEEK_API_KEY is not set');

  const response = await fetch('https://api.deepseek.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: 'deepseek-chat',
      messages: [{ role: 'user', content: prompt }],
      temperature
    })
  });

  if (!response.ok) {
    throw new Error(`DeepSeek API error: ${response.status}`);
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content?.trim() || '';
  if (!content) return [];

  const fencedMatch = content.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  const candidate = fencedMatch ? fencedMatch[1].trim() : content;

  try {
    const parsed = JSON.parse(candidate);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
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
    const response = await fetch('https://api.deepseek.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: 'deepseek-chat',
        temperature: 0,
        max_tokens: 120,
        messages: [{
          role: 'user',
          content: `Decide retrieval strategy for this query.\nQuery: ${query}\nReturn ONLY JSON: {"use_web":boolean,"use_memory":boolean,"reason":"short"}.`
        }]
      })
    });
    const data = await response.json();
    const raw = (data?.choices?.[0]?.message?.content || '').trim();
    const parsed = JSON.parse(raw.replace(/^```json/i, '').replace(/^```/, '').replace(/```$/, '').trim());
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

    const { generateEmbedding } = require('./services/embedding-engine');
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
    const db = require('./services/db');
    const q = `%${query || ''}%`;
    const targetLayer = filters.layer || null; 

    console.log('[search-graph] Query:', query, 'Filters:', filters);

    // Check database state
    const nodeCount = await db.getQuery(`SELECT COUNT(*) as count FROM nodes`);
    const eventCount = await db.getQuery(`SELECT COUNT(*) as count FROM events`);
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
    const { ingestRawEvent } = require('./services/ingestion');
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
    .slice(0, 40)
    .map((session) => ({
      id: String(session.id),
      title: String(session.title || 'New chat').slice(0, 180),
      createdAt: Number(session.createdAt || Date.now()),
      updatedAt: Number(session.updatedAt || session.createdAt || Date.now()),
      messages: Array.isArray(session.messages)
        ? session.messages
            .filter((msg) => msg && typeof msg.content === 'string')
            .slice(-180)
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

  if (keepIds.length) {
    await db.runQuery(`DELETE FROM chat_messages WHERE session_id NOT IN (${placeholders})`, keepIds).catch(() => {});
    await db.runQuery(`DELETE FROM chat_sessions WHERE id NOT IN (${placeholders})`, keepIds).catch(() => {});
  } else {
    await db.runQuery(`DELETE FROM chat_messages`).catch(() => {});
    await db.runQuery(`DELETE FROM chat_sessions`).catch(() => {});
    return { saved: 0 };
  }

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
    const { ingestRawEvent } = require('./services/ingestion');
    await saveChatSessionsToDb(sessions).catch((e) => {
      console.warn('[save-chat-sessions-to-memory] durable save failed', e?.message || e);
    });
    for (const session of sessions.slice(0, 50)) {
      try {
        const meta = {
          session_id: session.id,
          title: session.title || null,
          message_count: Array.isArray(session.messages) ? session.messages.length : 0,
          saved_from_ui: true
        };
        // Persist each message as a raw event so it enters the L1 event stream
        for (const msg of (session.messages || []).slice(-200)) {
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

// AI Assistant Chat Logic
ipcMain.handle('ask-ai-assistant', async (event, query, options = {}) => {
  try {
    const { answerChatQuery } = require('./services/agent/chat-engine');
    const proactiveMemory = store.get('proactiveMemory') || { core: '' };
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
    const response = await answerChatQuery({
      apiKey: process.env.DEEPSEEK_API_KEY,
      query,
      options: {
        ...options,
        chat_history: normalizedChatHistory,
        standing_notes: proactiveMemory.core || ''
      },
      onStep: (data) => {
        try { event.sender.send('chat-step', data); } catch (_) {}
      }
    });
    const resolvedChatSessionId = String(options?.chat_session_id || `chat_${Date.now()}`);
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
      content: response?.content || '',
      chatHistory: normalizedChatHistory.concat([{ role: 'user', content: String(query || ''), ts: Date.now() }]),
      retrieval: response?.retrieval || null
    });
    await appendChatMessageToDb({
      sessionId: resolvedChatSessionId,
      role: 'assistant',
      content: response?.content || '',
      ts: Date.now(),
      retrieval: response?.retrieval || null,
      thinkingTrace: response?.thinking_trace || null
    }).catch((err) => console.warn('[chat-memory] Failed to append assistant turn:', err?.message || err));
    return response;
  } catch (e) {
    console.error('Chat error:', e);
    return {
      content: "I encountered an error while thinking. Please try again.",
      retrieval: { usedSources: [], mode: 'error' }
    };
  }
});

// Generate proactive todos — strictly AI generated from memory retrieval.
ipcMain.handle('generate-proactive-todos', async (event) => {
  console.log('[Main] IPC generate-proactive-todos invoked from', event?.sender?.getURL ? event.sender.getURL() : 'main');
  try {
    const { generateTopTodosFromMemoryQuery } = require('./services/agent/suggestion-engine');
    const hasConcreteAction = (label = '') => /\b(open|draft|reply|send|prepare|review|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share|wish|check\s*in|reconnect)\b/i.test(String(label || ''));
    const looksValidAISuggestion = (item) => Boolean(
      item &&
      ['study', 'relationship', 'work', 'personal', 'creative', 'followup'].includes(String(item.type || '').toLowerCase()) &&
      item.title &&
      item.reason &&
      Array.isArray(item.evidence) &&
      item.evidence.length > 0 &&
      item.time_anchor &&
      item.display &&
      item.display.headline &&
      item.display.summary &&
      item.display.insight &&
      Array.isArray(item.epistemic_trace) &&
      item.epistemic_trace.length >= 2 &&
      Array.isArray(item.suggested_actions) &&
      item.suggested_actions.length >= 1 &&
      item.primary_action &&
      item.ai_generated !== false &&
      hasConcreteAction(item.primary_action?.label || '') &&
      !/\b(take the next step|keep momentum|be proactive|work on this|handle this)\b/i.test(String(item.title || ''))
    );
    const proactiveMemory = store.get('proactiveMemory') || { core: '' };
    const llmConfig = getSuggestionLLMConfig();
    if (!llmConfig) {
      return store.get('suggestions') || [];
    }
    const suggestions = await generateTopTodosFromMemoryQuery(llmConfig, {
      query: 'Look through my memory and generate top 5 todos or actions I need to do right now.',
      standing_notes: proactiveMemory.core || '',
      study_context: getStudySessionState()
    });
    const validatedSuggestions = (Array.isArray(suggestions) ? suggestions : []).filter(looksValidAISuggestion);
    const existingSuggestions = store.get('suggestions') || [];
    const mergedSuggestions = mergeSuggestionQueues(existingSuggestions, validatedSuggestions, MAX_PRACTICAL_SUGGESTIONS);
    store.set('suggestions', mergedSuggestions);
    return mergedSuggestions.map((item) => ({
      ...item,
      ai_doable: Boolean(item.ai_doable),
      assignee: item.assignee || (item.ai_doable ? 'ai' : 'human'),
      action_type: item.action_type || null,
      execution_mode: item.execution_mode || (item.ai_doable ? 'draft_or_execute' : 'manual'),
      target_surface: item.target_surface || null,
      expected_benefit: item.expected_benefit || '',
      display: item.display || null,
      epistemic_trace: Array.isArray(item.epistemic_trace) ? item.epistemic_trace : [],
      suggested_actions: Array.isArray(item.suggested_actions) ? item.suggested_actions : [],
      primary_action: item.primary_action || null,
      opportunity_type: item.opportunity_type || null,
      reason_codes: Array.isArray(item.reason_codes) ? item.reason_codes : [],
      time_anchor: item.time_anchor || null,
      candidate_score: Number(item.candidate_score || 0),
      prerequisites: Array.isArray(item.prerequisites) ? item.prerequisites : [],
      step_plan: Array.isArray(item.step_plan) ? item.step_plan : [],
      action_plan: Array.isArray(item.action_plan) ? item.action_plan : [],
      ai_draft: item.ai_draft || '',
      source: 'zero-base-suggestion-engine',
      completed: false,
      createdAt: item.created_at ? new Date(item.created_at).getTime() : Date.now()
    }));
  } catch (error) {
    console.error('Error generating proactive todos:', error);
    return store.get('suggestions') || [];
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
    if (!isExtensionConnected()) return { status: 'offline' };
    const taskId = payload && payload.taskId;
    if (!taskId) return { status: 'error', error: 'Missing taskId' };
    const plan = payload && payload.plan ? payload.plan : {};
    extensionSocket.send(JSON.stringify({ type: 'update-task-plan', taskId, plan }));
    return { status: 'sent' };
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
  const lastSeen = extensionLastSeen;
  const recentlySeen = lastSeen ? (Date.now() - lastSeen) < 15000 : false;
  return { connected: isExtensionConnected(), lastSeen, recentlySeen, transport: extensionTransport || 'native-messaging' };
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

/**
 * Utility to register the Native Messaging Host on macOS
 */
function registerNativeHost() {
  const fs = require('fs');
  const os = require('os');
  const hostName = NATIVE_HOST_NAME;
  const sourcePath = path.join(__dirname, `${hostName}.json`);
  const targetDir = path.join(os.homedir(), 'Library', 'Application Support', 'Google', 'Chrome', 'NativeMessagingHosts');
  const targetPath = path.join(targetDir, `${hostName}.json`);

  try {
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }
    
    // Update the path in the JSON to the absolute path of the wrapper script
    const manifest = JSON.parse(fs.readFileSync(sourcePath, 'utf8'));
    manifest.path = path.join(__dirname, 'native-host.sh');
    const configuredIds = String(process.env.PROACTIVE_EXTENSION_IDS || process.env.PROACTIVE_EXTENSION_ID || '')
      .split(',')
      .map(id => id.trim())
      .filter(id => /^[a-p]{32}$/.test(id));
    const existingOrigins = Array.isArray(manifest.allowed_origins) ? manifest.allowed_origins : [];
    const configuredOrigins = configuredIds.map(id => `chrome-extension://${id}/`);
    manifest.allowed_origins = Array.from(new Set([...existingOrigins, ...configuredOrigins]));
    
    fs.writeFileSync(targetPath, JSON.stringify(manifest, null, 2));
    console.log(`Native Messaging Host registered at: ${targetPath}`);
    
    // Ensure the shell script is executable
    const shellScriptPath = path.join(__dirname, 'native-host.sh');
    fs.chmodSync(shellScriptPath, '755');
  } catch (e) {
    console.error("Failed to register native host:", e.message);
  }
}

// Call registration on startup
registerNativeHost();


function flushPendingAITasks() {
  if (!extensionSocket || pendingAITasks.length === 0) return;
  const queue = [...pendingAITasks];
  pendingAITasks = [];
  queue.forEach(async (item) => {
    try {
      await sendTaskToExtension(item.task, item.script);
    } catch (e) {
      console.error('Queued AI task failed:', e);
    }
  });
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
  const authUrl = `https://accounts.google.com/o/oauth2/v2/auth?` +
    `client_id=${GOOGLE_CLIENT_ID}&` +
    `redirect_uri=${encodeURIComponent(getRedirectUri())}&` +
    `response_type=code&` +
    `scope=${encodeURIComponent([
      'https://www.googleapis.com/auth/userinfo.email',
      'https://www.googleapis.com/auth/userinfo.profile',
      'https://www.googleapis.com/auth/gmail.send',
      'https://www.googleapis.com/auth/gmail.readonly',
      'https://www.googleapis.com/auth/calendar.readonly',
      'https://www.googleapis.com/auth/drive.readonly'
    ].join(' '))}&` +
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
    const s = store.get('suggestions') || [];
    return s;
  } catch (e) {
    console.error('Failed to get suggestions:', e);
    return [];
  }
});

ipcMain.handle('save-suggestions', async (event, suggestions) => {
  try {
    const final = rankAndLimitSuggestions(Array.isArray(suggestions) ? suggestions : [], {
      maxTotal: MAX_PRACTICAL_SUGGESTIONS,
      maxPerCategory: 2,
      maxFollowups: 1,
      now: Date.now()
    });
    store.set('suggestions', final);
    return true;
  } catch (e) {
    console.error('Failed to save suggestions:', e);
    return false;
  }
});

ipcMain.handle('clear-suggestions', async () => {
  try {
    store.delete('suggestions');
    return true;
  } catch (e) {
    console.error('Failed to clear suggestions:', e);
    return false;
  }
});

// Debug function to manually trigger suggestions
ipcMain.handle('debug-trigger-suggestions', async () => {
  console.log('[Debug] Manually triggering suggestion engine...');
  await runSuggestionEngineJob();
  return { success: true, message: 'Suggestions triggered manually' };
});

// Run suggestion engine: accepts optional events payload or uses global.extensionData
ipcMain.handle('run-suggestion-engine', async (event, payload) => {
  try {
    // Lazy-load services to avoid startup cost
    const extractor = require('./services/extractors/openLoopExtractor');
    const scoring = require('./services/scoring');

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

ipcMain.handle('sync-google-data', async () => {
  return await fullGoogleSync();
});

// Full GSuite sync flow extracted so it can be invoked after OAuth and by IPC
async function fullGoogleSync({ since, forceHistoricalBackfill = false } = {}) {
  const { ingestRawEvent } = require('./services/ingestion');
  const engine = require('./services/agent/intelligence-engine');
  const apiKey = process.env.DEEPSEEK_API_KEY;

  const sendProgress = (phase, done, total) => {
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
    const googleDelta = await getGoogleData({ since: sinceFloor });
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
      syncMeta
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

    // --- L2 - L5: Memory Graph Build ---
    sendProgress('Building Episode Memory (L2)...', 0, 1);
    await engine.runEpisodeJob(apiKey || null);

    if (apiKey) {
      sendProgress('Synthesizing Core Insights (L4/L5)...', 0, 1);
      await engine.runWeeklyInsightJob(apiKey);
    }

    sendProgress('Sync Complete', 100, 100);
    store.set('initialSyncDone', true);
    return googleData;
  } catch (error) {
    console.error('Error syncing Google Data:', error);
    sendProgress('Error', 0, 0);
    throw error;
  }
}

ipcMain.handle('get-google-data', () => {
  return store.get('googleData') || { gmail: [], calendar: [], drive: [] };
});

// ── Memory Graph Status & Chat Integration ───────────────────────────────

ipcMain.handle('get-memory-graph-status', async () => {
  try {
    const db = require('./services/db');
    
    const eventCount = await db.getQuery(`SELECT COUNT(*) as count FROM events`);
    const nodeCounts = await db.allQuery(`SELECT layer, COUNT(*) as count FROM memory_nodes GROUP BY layer`);
    const edgeCount = await db.getQuery(`SELECT COUNT(*) as count FROM memory_edges`);
    const sourceCounts = await db.allQuery(`SELECT COALESCE(source_type, type) as source_type, COUNT(*) as count FROM events GROUP BY COALESCE(source_type, type)`);
    const typedRawCounts = await db.allQuery(
      `SELECT COALESCE(source_type, type) as source_type, COUNT(*) as count, MAX(COALESCE(occurred_at, timestamp)) as latest
       FROM events
       GROUP BY COALESCE(source_type, type)`
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
    const db = require('./services/db');
    const { limit = 20, nodeTypes = [], app, date_range: dateRange, data_source: dataSource } = options;
    const appFilters = Array.isArray(app) ? app.map((item) => String(item || '').toLowerCase()).filter(Boolean) : (app ? [String(app).toLowerCase()] : []);
    const startMs = dateRange?.start ? Date.parse(String(dateRange.start)) : null;
    const endMs = dateRange?.end ? Date.parse(String(dateRange.end)) : null;
    const normalizedDataSource = dataSource && dataSource !== 'auto' ? String(dataSource).toLowerCase() : null;
    const rowMatchesNodeFilters = (row) => {
      const metadata = (() => {
        try { return JSON.parse(row?.metadata || '{}'); } catch (_) { return {}; }
      })();
      if (appFilters.length) {
        const appHay = `${metadata.app || ''} ${(metadata.apps || []).join(' ')} ${metadata.window_title || ''}`.toLowerCase();
        if (!appFilters.some((needle) => appHay.includes(needle))) return false;
      }
      if (Number.isFinite(startMs) || Number.isFinite(endMs)) {
        const ts = Date.parse(String(metadata.latest_activity_at || metadata.anchor_at || row?.updated_at || ''));
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
        const { buildRetrievalThought } = require('./services/agent/retrieval-thought-system');
        const { buildHybridGraphRetrieval } = require('./services/agent/hybrid-graph-retrieval');
        const thought = buildRetrievalThought({
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
          seedLimit: 6,
          hopLimit: 2
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
              metadata: JSON.stringify({
                anchor_at: ev.anchor_at || null,
                latest_activity_at: ev.latest_activity_at || ev.timestamp || null,
                app: ev.app || null,
                source_type_group: ev.source_type_group || null,
                score: ev.score || null,
                match_reason: ev.reason || null,
                strategy_mode: routed?.strategy?.strategy_mode || null,
                entry_mode: routed?.strategy?.entry_mode || null
              }),
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
          `SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.metadata, n.updated_at
           FROM memory_nodes n
           WHERE n.id IN (${placeholders})`,
          nodeIds
        );
      }
    }

    if (!results.length) {
      let sql = `
        SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.metadata, n.updated_at
        FROM memory_nodes n
        WHERE n.title LIKE ? OR n.summary LIKE ? OR n.canonical_text LIKE ?
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

    return results.slice(0, limit).map(row => ({
      id: row.id,
      type: row.layer,
      data: {
        title: row.title,
        summary: row.summary,
        timestamp: row.updated_at || null,
        ...(JSON.parse(row.metadata || '{}'))
      }
    }));
  } catch (error) {
    console.error('[MemoryGraph] Search failed:', error);
    return [];
  }
});

ipcMain.handle('get-related-nodes', async (event, nodeId, relationType = null) => {
  try {
    const db = require('./services/db');
    
    let sql = `
      SELECT n.id, n.layer, n.title, n.summary, n.metadata, e.edge_type
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
    const db = require('./services/db');
    const rows = await db.allQuery(
      `SELECT title, summary, confidence
       FROM memory_nodes
       WHERE layer = 'insight'
       ORDER BY confidence DESC, updated_at DESC
       LIMIT 6`
    ).catch(() => []);
    return {
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
        await runSuggestionEngineJob();
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
    const db = require('./services/db');
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
    const db = require('./services/db');
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
    const { resetZeroBaseMemory } = require('./services/agent/zero-base-memory');
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
  if (!isExtensionConnected()) {
    const connected = await waitForExtensionConnection(15000, 300);
    if (!connected) return { status: 'error', error: 'extension-not-connected' };
  }
  const requestId = 'diag_' + Math.random().toString(36).slice(2,9);
  const payload = { type: 'run-diagnostic', requestId, opts: opts || {} };
  return new Promise((resolve) => {
    let listener = null;
    const timeout = setTimeout(() => {
      if (listener) extensionSocket?.off('message', listener);
      resolve({ status: 'error', error: 'timeout' });
    }, 7000);
    listener = (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg && msg.type === 'run-diagnostic-result' && msg.requestId === requestId) {
          clearTimeout(timeout);
          extensionSocket?.off('message', listener);
          resolve(msg.result || { status: 'ok' });
        }
      } catch (e) {}
    };
    // attach temporary listener on the socket
    try {
      extensionSocket.send(JSON.stringify(payload));
      extensionSocket.on('message', listener);
    } catch (e) {
      clearTimeout(timeout);
      resolve({ status: 'error', error: String(e) });
    }
  });
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
  return await getExtensionData();
});

ipcMain.handle('clear-extension-data', async () => {
  try {
    // Clear extension data from memory and storage
    global.extensionData = null;
    store.delete('extensionData');
    store.delete('lastExtensionSync');
    return { success: true };
  } catch (error) {
    console.error('Error clearing extension data:', error);
    return { success: false, error: error.message };
  }
});

ipcMain.handle('get-sensor-status', async () => {
  return getSensorStatus();
});

ipcMain.handle('start-study-session', async (_event, payload = {}) => {
  const goal = String(payload?.goal || '').trim();
  const subject = String(payload?.subject || '').trim();
  const next = setStudySessionState({
    status: 'active',
    session_id: `study_${Date.now()}`,
    goal,
    subject,
    started_at: new Date().toISOString(),
    ended_at: null
  });
  return next;
});

ipcMain.handle('stop-study-session', async () => {
  const current = getStudySessionState();
  const next = setStudySessionState({
    status: 'idle',
    session_id: null,
    goal: '',
    subject: '',
    started_at: current?.started_at || null,
    ended_at: new Date().toISOString()
  });
  return next;
});

ipcMain.handle('get-study-session-status', async () => {
  return getStudySessionState();
});

ipcMain.handle('get-sensor-events', async () => {
  return getSensorEvents();
});

ipcMain.handle('save-sensor-settings', async (event, settings) => {
  const next = {
    ...getSensorSettings(),
    ...(settings || {})
  };
  // Keep interval fixed to every 30 seconds regardless of incoming payload.
  next.intervalMinutes = 0.5;
  store.set('sensorSettings', next);
  startSensorCaptureLoop();
  return getSensorStatus();
});

ipcMain.handle('capture-sensor-snapshot', async () => {
  // Trigger a manual capture in background and return current status immediately
  captureDesktopSensorSnapshot('manual').catch(err => console.warn('Manual sensor capture failed:', err));
  return {
    event: null,
    status: getSensorStatus()
  };
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
  const model = String(payload.model || (provider === 'ollama' ? 'llama3.1:8b' : 'deepseek-chat')).trim();
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

// Persistent todos management
ipcMain.handle('get-persistent-todos', () => {
  const todos = (store.get('persistentTodos') || []).filter((todo) => !todo?.completed);
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

ipcMain.handle('delete-all-settings', async () => {
  // Factory reset: wipe in-memory state, SQLite data, browser/session storage, and userData files.
  try { if (sensorCaptureTimer) clearInterval(sensorCaptureTimer); } catch (_) {}
  try { if (episodeGenerationTimer) clearInterval(episodeGenerationTimer); } catch (_) {}
  try { if (suggestionEngineTimer) clearInterval(suggestionEngineTimer); } catch (_) {}
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
    if (fs.existsSync(userDataPath)) {
      const entries = fs.readdirSync(userDataPath);
      for (const name of entries) {
        const abs = path.join(userDataPath, name);
        try {
          fs.rmSync(abs, { recursive: true, force: true });
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

app.whenReady().then(async () => {
  try {
    await db.initDB();
    console.log('SQLite Graph DB Initialized');
    await repairEmailEventTimestamps();
  } catch (e) {
    console.error('Failed to init SQLite Graph DB:', e);
  }
  createWindow();
  createVoiceHudWindow();
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

  // Initialize daily task scheduling
  scheduleDailyTasks();
  generateMorningBrief({ force: false, scheduled: false }).catch((e) => {
    console.warn('Initial morning brief generation failed:', e?.message || e);
  });
  startSensorCaptureLoop();
  startSourceWarmup();
  
  // Initialize memory graph processing
  startMemoryGraphProcessing();

  // ── Auto-trigger initial sync on first launch ──────────────────────────
  const syncDone = store.get('initialSyncDone') || false;
  if (!syncDone) {
    console.log('[initialSync] First launch detected — scheduling initial historical sync in 5s...');
    setTimeout(async () => {
      try {
        if (mainWindow && mainWindow.webContents) {
          mainWindow.webContents.send('initial-sync-started');
        }
        const googleData     = store.get('googleData') || await getGoogleData();
        const browserHistory = await getBrowserHistory();

        const result = await runInitialSync({
          userId:         'local',
          messages:       googleData.gmail    || [],
          docs:           googleData.drive    || [],
          calendarEvents: googleData.calendar || [],
          pageVisits:     browserHistory,
          apiKey:         process.env.DEEPSEEK_API_KEY,
          onProgress: (progress) => {
            if (mainWindow && mainWindow.webContents) {
              mainWindow.webContents.send('initial-sync-progress', progress);
            }
          }
  , store });

        const daysCount = await processSyncResult(result);
        console.log(`[initialSync] Scheduled sync complete: ${daysCount} days summarised.`);

        store.set('historicalSummaries', result.summaries);
        store.set('initialSyncDone', true);

        const existingProfile = store.get('userProfile') || {};
        store.set('userProfile', {
          ...existingProfile,
          patterns:            [...new Set([...(existingProfile.patterns    || []), ...result.userPatterns])].slice(0, 40),
          preferences:         [...new Set([...(existingProfile.preferences || []), ...result.userPreferences])].slice(0, 40),
          top_intent_clusters: result.topIntentClusters || []
        });

        console.log(`[initialSync] Complete — ${Object.keys(result.summaries).length} days summarised.`);
        if (mainWindow && mainWindow.webContents) {
          mainWindow.webContents.send('initial-sync-complete', {
            daysProcessed: Object.keys(result.summaries).length
          });
        }
      } catch (err) {
        console.error('[initialSync] Auto-sync failed:', err.message);
        if (mainWindow && mainWindow.webContents) {
          mainWindow.webContents.send('initial-sync-error', { error: err.message });
        }
      }
    }, 5000);
  }

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
  globalShortcut.unregisterAll();
});

// Handle Chrome extension messages
ipcMain.handle('extension-message', (event, message) => {
  // Process messages from Chrome extension
  console.log('Extension message:', message);
  
  // Store data from extension
  const currentData = store.get('extensionData') || {};
  currentData[Date.now()] = message;
  store.set('extensionData', currentData);
  
  return { received: true };
});

async function runLivingCoreJobScheduled() {
  try {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
      console.warn('[LivingCore] No DeepSeek API key, skipping Living Core generation');
      return;
    }
    console.log('[LivingCore] Running Living Core generation...');
    const { runLivingCoreJob } = require('./services/agent/intelligence-engine');
    const created = await runLivingCoreJob(apiKey);
    console.log('[LivingCore] Created core nodes:', created.length);
    if (mainWindow && mainWindow.webContents) {
      mainWindow.webContents.send('memory-graph-update', { type: 'living_core_completed', count: created.length, timestamp: Date.now() });
    }
  } catch (e) {
    console.error('[LivingCore] Error:', e?.message || e);
  }
}

function scheduleLivingCore() {
  if (livingCoreTimer) clearTimeout(livingCoreTimer);
  const now = new Date();
  const next = new Date(now);
  // schedule for 01:00 local time tomorrow
  next.setDate(next.getDate() + 1);
  next.setHours(1, 0, 0, 0);
  const delay = next.getTime() - now.getTime();
  livingCoreTimer = setTimeout(async function tick() {
    await runLivingCoreJobScheduled();
    // schedule next day
    livingCoreTimer = setTimeout(tick, 24 * 60 * 60 * 1000);
  }, delay);
  console.log('[LivingCore] Scheduled for:', next.toLocaleString());
}
