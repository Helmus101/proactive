const { app, powerMonitor } = require('electron');
const { createWindow, createVoiceHudWindow } = require('./window');

function setupLifecycle(initializeBackgroundServices, registerVoiceShortcut, startScreenshotCleanupLoop) {
  app.whenReady().then(async () => {
    createWindow();
    createVoiceHudWindow();
    
    initializeBackgroundServices();
    registerVoiceShortcut();
    startScreenshotCleanupLoop();
  });

  app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
      app.quit();
    }
  });

  app.on('activate', () => {
    // Re-create window if necessary
  });
}

module.exports = { setupLifecycle };
