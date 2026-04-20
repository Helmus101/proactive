import sys

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

old_code_1 = """function startSensorCaptureLoop(mode = null) {
  const settings = getSensorSettings();
  if (sensorCaptureTimer) {
    clearInterval(sensorCaptureTimer);
    sensorCaptureTimer = null;
  }
  if (!settings.enabled) return;

  captureDesktopSensorSnapshot("loop-start").catch((error) => {
    console.warn("Initial sensor capture failed:", error && error.message ? error.message : error);
  });

  // Dynamic interval: 15m normal, 30m reduced
  const isReduced = (typeof mode !== "undefined" && mode === "reduced") || isReducedLoadMode();
  const intervalMs = isReduced ? 30 * 60 * 1000 : intervalMinutesToMs(settings.intervalMinutes);"""

new_code_1 = """function startSensorCaptureLoop(mode = null) {
  const settings = getSensorSettings();
  if (sensorCaptureTimer) {
    clearInterval(sensorCaptureTimer);
    sensorCaptureTimer = null;
  }
  if (!settings.enabled) return;

  captureDesktopSensorSnapshot("loop-start").catch((error) => {
    console.warn("Initial sensor capture failed:", error && error.message ? error.message : error);
  });

  const currentMode = mode || getPerformanceMode();
  let intervalMs;
  if (currentMode === 'deep-idle') intervalMs = 60 * 60 * 1000;
  else if (currentMode === 'reduced') intervalMs = 30 * 60 * 1000;
  else intervalMs = intervalMinutesToMs(settings.intervalMinutes);"""

old_code_2 = """// Periodic screenshot capture every 30 seconds
function startPeriodicScreenshotCapture(mode = null) {
  if (periodicScreenshotTimer) {
    clearInterval(periodicScreenshotTimer);
    periodicScreenshotTimer = null;
  }

  const isReduced = (typeof mode !== "undefined" && mode === "reduced") || isReducedLoadMode();
  const intervalMs = isReduced ? 120000 : 30000; // 2m vs 30s"""

new_code_2 = """// Periodic screenshot capture every 30 seconds
function startPeriodicScreenshotCapture(mode = null) {
  if (periodicScreenshotTimer) {
    clearInterval(periodicScreenshotTimer);
    periodicScreenshotTimer = null;
  }

  const currentMode = mode || getPerformanceMode();
  let intervalMs;
  if (currentMode === 'deep-idle') intervalMs = 300000; // 5m
  else if (currentMode === 'reduced') intervalMs = 120000; // 2m
  else intervalMs = 30000; // 30s"""

if old_code_1 in content and old_code_2 in content:
    new_content = content.replace(old_code_1, new_code_1)
    new_content = new_content.replace(old_code_2, new_code_2)
    with open(file_path, 'w') as f:
        f.write(new_content)
    print("Success")
else:
    if old_code_1 not in content:
        print("Old code 1 not found")
    if old_code_2 not in content:
        print("Old code 2 not found")
