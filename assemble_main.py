
import os

top_globals = """
let mainWindow;
let voiceHudWindow = null;
const appInteractionState = {
  focused: false,
  minimized: false,
  chatActive: false,
  lastInteractionAt: 0
};
"""

mark_app_interaction = """
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
"""

# Functions extracted from a clean read
create_window = """
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
"""

create_voice_hud_window = """
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
"""

with open('appReady.js', 'r') as f:
    app_when_ready = f.read()

with open('main.js', 'r') as f:
    lines = f.readlines()

# Final assembly
with open('main.js.new', 'w') as f:
    f.writelines(lines[:27]) # up to lazyRequire end
    f.write("\n")
    f.write(top_globals)
    f.write(mark_app_interaction)
    f.write(create_window)
    f.write(create_voice_hud_window)
    f.write("\n")
    f.write(app_when_ready)
    f.write("\n")
    f.writelines(lines[401:]) # rest of the file

print("Assembly complete.")
