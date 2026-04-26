const { app, BrowserWindow, ipcMain, powerMonitor } = require('electron');
const path = require('path');
const { createWindow, createVoiceHudWindow, getMainWindow } = require('./window');
const { setupLifecycle } = require('./lifecycle');

// Global state
let activeStudySession = null; // Wait, I should remove this!

// ... imports and initializations ...

setupLifecycle(initializeBackgroundServices, registerVoiceShortcut, startScreenshotCleanupLoop);

// Import handlers
require('./handlers/chat');
require('./handlers/suggestions');
require('./handlers/memory');
require('./handlers/sync');
require('./handlers/system');
