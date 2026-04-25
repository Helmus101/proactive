const { powerMonitor } = require('electron');

const APP_ACTIVE_CAPTURE_COOLDOWN_MS = 3 * 60 * 1000;

const performanceState = {
  onBattery: false,
  thermalState: 'unknown'
};

const appInteractionState = {
  focused: false,
  minimized: false,
  lastInteractionAt: 0,
  chatActive: false // This will be synced from ChatManagement
};

let deps = {
  mainWindow: null,
  debouncedStoreSet: null,
  startSensorCaptureLoop: null,
  startPeriodicScreenshotCapture: null,
  getScreenshotsPausedForDisplayOff: () => false,
  getPeriodicScreenshotRunning: () => false,
  isChatActive: () => false
};

function init(options) {
  Object.assign(deps, options);
}

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
  const PERIODIC_SCREENSHOT_INTERVAL_MS = (process.env.EMERGENCY_THROTTLE_ENABLED === 'true') ? 5 * 60 * 1000 : 45 * 1000;
  if (mode === 'deep-idle') return 45 * 60 * 1000;
  if (mode === 'reduced')   return 20 * 60 * 1000;
  return PERIODIC_SCREENSHOT_INTERVAL_MS;
}

function getPeriodicScreenshotWakeDelayMs(mode = getPerformanceMode()) {
  if (mode === 'deep-idle') return 60 * 1000;
  if (mode === 'reduced')   return 30 * 1000;
  return 15 * 1000; // Normal
}

function updatePerformanceState(next = {}) {
  const oldMode = getPerformanceMode();
  Object.assign(performanceState, next || {});
  const newMode = getPerformanceMode();

  if (deps.debouncedStoreSet) {
    deps.debouncedStoreSet("performanceState", {
      ...performanceState,
      mode: newMode,
      updated_at: new Date().toISOString()
    });
  }

  if (oldMode !== newMode) {
    console.log(`[Performance] Mode changed from ${oldMode} to ${newMode}. Restarting capture timers with updated cadence.`);
    if (deps.startSensorCaptureLoop) deps.startSensorCaptureLoop(newMode);
    if (deps.getPeriodicScreenshotRunning() && !deps.getScreenshotsPausedForDisplayOff()) {
      if (deps.startPeriodicScreenshotCapture) deps.startPeriodicScreenshotCapture(newMode);
    }
    
    // Notify renderer so it can disable expensive CSS effects like blurs
    if (deps.mainWindow && !deps.mainWindow.isDestroyed()) {
      deps.mainWindow.webContents.send('performance-mode-changed', newMode);
    }
  }
}

function markAppInteraction(reason = 'interaction') {
  appInteractionState.lastInteractionAt = Date.now();
  if (reason && deps.debouncedStoreSet) {
    deps.debouncedStoreSet('lastAppInteraction', {
      reason,
      at: new Date(appInteractionState.lastInteractionAt).toISOString()
    });
  }
}

function isAppInteractionHot() {
  if (!deps.mainWindow || deps.mainWindow.isDestroyed()) return false;
  if (appInteractionState.minimized) return false;
  if (!appInteractionState.focused) return false;
  if (deps.isChatActive()) return true;
  return (Date.now() - Number(appInteractionState.lastInteractionAt || 0)) < APP_ACTIVE_CAPTURE_COOLDOWN_MS;
}

function setFocused(focused) {
  appInteractionState.focused = !!focused;
}

function setMinimized(minimized) {
  appInteractionState.minimized = !!minimized;
}

module.exports = {
  init,
  performanceState,
  appInteractionState,
  getPerformanceMode,
  isReducedLoadMode,
  updatePerformanceState,
  getPeriodicScreenshotIntervalMs,
  getPeriodicScreenshotWakeDelayMs,
  markAppInteraction,
  isAppInteractionHot,
  setFocused,
  setMinimized,
  APP_ACTIVE_CAPTURE_COOLDOWN_MS
};
