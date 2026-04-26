const { BrowserWindow, path, AppState, ChatManagement, BrowserHistory, HeavyJobQueue, RadarState } = require('./index');

let mainWindow;
let voiceHudWindow = null;

function getMainWindow() {
  return mainWindow;
}

function getVoiceHudWindow() {
  return voiceHudWindow;
}

function createWindow(preloadPath, indexPath) {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: preloadPath
    }
  });

  mainWindow.loadFile(indexPath)
    .catch((error) => {
      console.error('[Startup] loadFile failed:', error?.message || error);
    });

  mainWindow.on('focus', () => {
    AppState.markAppInteraction('focus');
  });
  
  // ... other event listeners from main.js ...
  
  return mainWindow;
}

function createVoiceHudWindow(hudPath) {
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
  voiceHudWindow.loadFile(hudPath);
  voiceHudWindow.on('closed', () => {
    voiceHudWindow = null;
  });
  return voiceHudWindow;
}

module.exports = {
  createWindow,
  createVoiceHudWindow,
  getMainWindow,
  getVoiceHudWindow
};
