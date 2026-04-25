app.whenReady().then(async () => {
  startupTrace('[Startup] app.whenReady callback');
  createWindow();
  createVoiceHudWindow();

  // Defer non-critical initialization
  setTimeout(() => {
    if (typeof db !== 'undefined' && db.initDB) {
        db.initDB().catch(err => console.log('[DB] Early init catch:', err.message));
    }

    // Emergency CPU throttling - set process priority to low
    if (typeof EMERGENCY_THROTTLE_ENABLED !== 'undefined' && EMERGENCY_THROTTLE_ENABLED) {
      try {
        const exec = (...args) => lazyRequire('child_process').exec(...args);
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

    if (typeof initializeBackgroundServices === 'function') initializeBackgroundServices();
    if (typeof registerVoiceShortcut === 'function') registerVoiceShortcut();
    if (typeof hydrateStudySessionFromStore === 'function') hydrateStudySessionFromStore();
    if (typeof startScreenshotCleanupLoop === 'function') startScreenshotCleanupLoop();
    if (typeof emitStudySessionUpdate === 'function') emitStudySessionUpdate();

    try {
      if (typeof powerMonitor !== 'undefined') {
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
            if (typeof screenshotsPausedForDisplayOff !== 'undefined' && screenshotsPausedForDisplayOff && !/lock/i.test(periodicScreenshotPauseReason)) {
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
      }
    } catch (error) {
      console.warn('[Performance] powerMonitor hooks unavailable:', error?.message || error);
    }

    // Start OAuth server
    const startOAuthServer = (port) => {
      if (typeof oauthApp === 'undefined') return;
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

    if (typeof oauthApp !== 'undefined') {
        startOAuthServer(Number(oauthPort) || 3002);
    }

    if (typeof startSourceWarmup === 'function') setTimeout(() => startSourceWarmup(), typeof STARTUP_SOURCE_WARMUP_DELAY_MS !== 'undefined' ? STARTUP_SOURCE_WARMUP_DELAY_MS : 5000);
    if (typeof startPeriodicScreenshotCapture === 'function') setTimeout(() => startPeriodicScreenshotCapture(), Math.max(15000, typeof getPeriodicScreenshotWakeDelayMs === 'function' ? getPeriodicScreenshotWakeDelayMs() : 15000));

    // Initialize memory graph processing
    if (typeof startMemoryGraphProcessing === 'function') setTimeout(() => startMemoryGraphProcessing(), typeof STARTUP_MEMORY_GRAPH_DELAY_MS !== 'undefined' ? STARTUP_MEMORY_GRAPH_DELAY_MS : 10000);

    // Start user-defined automation scheduler
    if (typeof startAutomationScheduler === 'function') setTimeout(() => startAutomationScheduler(), typeof STARTUP_AUTOMATION_DELAY_MS !== 'undefined' ? STARTUP_AUTOMATION_DELAY_MS : 15000);
    if (typeof recursiveImprovementEnabled === 'function' && recursiveImprovementEnabled()) {
      if (typeof startRecursiveImprovementLoop === 'function') setTimeout(() => startRecursiveImprovementLoop(), typeof STARTUP_RECURSIVE_DELAY_MS !== 'undefined' ? STARTUP_RECURSIVE_DELAY_MS : 20000);
    }

    // Auto-trigger initial sync on first launch
    const syncDone = store.get('initialSyncDone') || false;
    if (!syncDone) {
      console.log(`[initialSync] First launch detected — scheduling initial historical sync...`);
      setTimeout(() => {
        if (typeof fullGoogleSync === 'function') {
            fullGoogleSync({
              pageVisits: typeof browserHistory !== 'undefined' ? browserHistory : [],
              apiKey: process.env.DEEPSEEK_API_KEY,
              onProgress: (progress) => {
                if (mainWindow && mainWindow.webContents) {
                  mainWindow.webContents.send('initial-sync-progress', progress);
                }
              }
            }, store).then(async (result) => {
              console.log('[initialSync] Initial sync completed');
              if (typeof processSyncResult === 'function') {
                  const daysCount = await processSyncResult(result);
                  console.log(`[initialSync] Scheduled sync complete: ${daysCount} days summarised.`);
              }

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
        }
      }, typeof STARTUP_INITIAL_SYNC_DELAY_MS !== 'undefined' ? STARTUP_INITIAL_SYNC_DELAY_MS : 30000);
    }

    // Periodic sync every 5 minutes
    setInterval(() => {
      if (typeof fullGoogleSync === 'function') {
          fullGoogleSync().catch((err) => {
            console.error('[PeriodicSync] Sync failed:', err?.message || err);
          });
      }
    }, typeof GSUITE_SYNC_INTERVAL_MS !== 'undefined' ? GSUITE_SYNC_INTERVAL_MS : 300000);

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
      }
      if (!voiceHudWindow || voiceHudWindow.isDestroyed()) {
        createVoiceHudWindow();
      }
    });
  }, 1);
});
