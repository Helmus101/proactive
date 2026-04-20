import sys

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

old_code = """function isReducedLoadMode() {
  return Boolean(performanceState.onBattery) || ['serious', 'critical'].includes(String(performanceState.thermalState || '').toLowerCase());
}

function canRunHeavyJob(lastRunAt = 0) {
  if (!isReducedLoadMode()) return true;
  return (Date.now() - Number(lastRunAt || 0)) >= LOW_POWER_HEAVY_JOB_MIN_GAP_MS;
}

function updatePerformanceState(next = {}) {
  const oldMode = isReducedLoadMode() ? "reduced" : "normal";
  Object.assign(performanceState, next || {});
  const newMode = isReducedLoadMode() ? "reduced" : "normal";

  store.set("performanceState", {
    ...performanceState,
    mode: newMode,
    updated_at: new Date().toISOString()
  });

  if (oldMode !== newMode) {
    console.log(`[Performance] Mode changed from ${oldMode} to ${newMode}. Restarting capture timers.`);
    startSensorCaptureLoop(newMode);
    startPeriodicScreenshotCapture(newMode);
  }
}"""

new_code = """function getPerformanceMode() {
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

function canRunHeavyJob(lastRunAt = 0) {
  if (!isReducedLoadMode()) return true;
  return (Date.now() - Number(lastRunAt || 0)) >= LOW_POWER_HEAVY_JOB_MIN_GAP_MS;
}

function updatePerformanceState(next = {}) {
  const oldMode = getPerformanceMode();
  Object.assign(performanceState, next || {});
  const newMode = getPerformanceMode();

  store.set("performanceState", {
    ...performanceState,
    mode: newMode,
    updated_at: new Date().toISOString()
  });

  if (oldMode !== newMode) {
    console.log(`[Performance] Mode changed from ${oldMode} to ${newMode}. Restarting capture timers.`);
    startSensorCaptureLoop(newMode);
    startPeriodicScreenshotCapture(newMode);
  }
}"""

if old_code in content:
    new_content = content.replace(old_code, new_code)
    with open(file_path, 'w') as f:
        f.write(new_content)
    print("Success")
else:
    print("Old code not found")
