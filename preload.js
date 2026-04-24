const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
  // Google OAuth
  startGoogleAuth: () => ipcRenderer.invoke('start-google-auth'),
  getGoogleTokens: () => ipcRenderer.invoke('get-google-tokens'),
  syncGoogleData: () => ipcRenderer.invoke('sync-google-data'),

  // Data storage
  storeUserData: (data) => ipcRenderer.invoke('store-user-data', data),
  getUserData: () => ipcRenderer.invoke('get-user-data'),
  getUserProfile: () => ipcRenderer.invoke('get-user-profile'),

  // AI functionality
  generateProactiveTodos: (userData) => ipcRenderer.invoke('generate-proactive-todos', userData),
  executeTodo: (todo) => ipcRenderer.invoke('execute-todo', todo),
  executeAITask: (task) => ipcRenderer.invoke('execute-ai-task', task),
  updateAITaskPlan: (payload) => ipcRenderer.invoke('update-ai-task-plan', payload),
  sendEmail: (payload) => ipcRenderer.invoke('send-email', payload),
  getDailySummary: () => ipcRenderer.invoke('get-daily-summary'),
  getDailySummaries: (query) => ipcRenderer.invoke('get-daily-summaries', query),
  askAIAssistant: (query, options = {}) => ipcRenderer.invoke('ask-ai-assistant', query, options),
  onChatStep: (cb) => ipcRenderer.on('chat-step', (_, data) => cb(data)),
  offChatStep: () => ipcRenderer.removeAllListeners('chat-step'),
  runPuppeteerTest: () => ipcRenderer.invoke('run-puppeteer-test'),
  runPuppeteerPrompt: (prompt) => ipcRenderer.invoke('run-puppeteer-prompt', prompt),
  puppeteerContinue: () => ipcRenderer.invoke('puppeteer-continue'),
  onPuppeteerWaiting: (callback) => ipcRenderer.on('puppeteer-waiting', (_e, payload) => callback(payload)),

  // Suggestions store
  getRadarState: () => ipcRenderer.invoke('get-radar-state'),
  getSuggestions: () => ipcRenderer.invoke('get-suggestions'),
  saveSuggestions: (suggestions) => ipcRenderer.invoke('save-suggestions', suggestions),
  triggerSuggestionRefresh: (payload = {}) => ipcRenderer.invoke('trigger-suggestion-refresh', payload),
  runSuggestionEngine: (payload) => ipcRenderer.invoke('run-suggestion-engine', payload),
  logAutomation: (record) => ipcRenderer.invoke('log-automation', record),

  // Todos
  getPersistentTodos: () => ipcRenderer.invoke('get-persistent-todos'),
  savePersistentTodos: (todos) => ipcRenderer.invoke('save-persistent-todos', todos),
  completeTask: (taskId) => ipcRenderer.invoke('complete-task', taskId),

  // Extension & Browser Data
  getExtensionStatus: () => ipcRenderer.invoke('get-extension-status'),
  getExtensionData: () => ipcRenderer.invoke('get-extension-data'),
  clearExtensionData: () => ipcRenderer.invoke('clear-extension-data'),
  deleteAllSettings: () => ipcRenderer.invoke('delete-all-settings'),
  extensionRunDiagnostic: (opts) => ipcRenderer.invoke('extension-run-diagnostic', opts || {}),
  getEventDetails: (eventId) => ipcRenderer.invoke('get-event-details', eventId),
  getSensorStatus: () => ipcRenderer.invoke('get-sensor-status'),
  getSensorEvents: () => ipcRenderer.invoke('get-sensor-events'),
  saveSensorSettings: (settings) => ipcRenderer.invoke('save-sensor-settings', settings),
  captureSensorSnapshot: () => ipcRenderer.invoke('capture-sensor-snapshot'),
  getAccessibilityStatus: () => ipcRenderer.invoke('get-accessibility-status'),
  openAccessibilitySettings: () => ipcRenderer.invoke('open-accessibility-settings'),
  openScreenRecordingSettings: () => ipcRenderer.invoke('open-screen-recording-settings'),
  getVoiceControlStatus: () => ipcRenderer.invoke('get-voice-control-status'),
  setVoiceControlEnabled: (enabled) => ipcRenderer.invoke('set-voice-control-enabled', enabled),
  voiceCaptureFailed: (payload = {}) => ipcRenderer.invoke('voice-capture-failed', payload),
  updateVoiceSessionTranscript: (payload = {}) => ipcRenderer.invoke('update-voice-session-transcript', payload),
  submitVoiceTranscript: (payload = {}) => ipcRenderer.invoke('submit-voice-transcript', payload),
  submitVoiceAudio: (payload = {}) => ipcRenderer.invoke('submit-voice-audio', payload),

  // ── Daily Summary & Historical Sync ──────────────────────────────────────

  /**
   * Trigger the initial historical backfill.
   * Can also be called manually to re-run (e.g., from a Settings page).
   * Returns { success, daysProcessed, userPatterns, userPreferences }
   */
  runInitialSync: () => ipcRenderer.invoke('run-initial-sync'),

  /**
   * Check whether the initial sync is done and how many days are stored.
   * Returns { done: boolean, daysCount: number }
   */
  getInitialSyncStatus: () => ipcRenderer.invoke('get-initial-sync-status'),

  /**
   * Fetch historical daily summaries, sorted newest-first.
   * @param {{ startDate?: string, endDate?: string, limit?: number }} opts
   * @returns {Promise<DailySummary[]>}
   */
  getHistoricalSummaries: (opts) => ipcRenderer.invoke('get-historical-summaries', opts || {}),

  /**
   * Full-text search across all historical summaries.
   * Great for looking up a person by name, a topic, or a domain.
   * @param {string} query  e.g. "John", "birthday", "linkedin"
   * @returns {Promise<Array<{ date, score, snippet, narrative, top_people, intent_clusters }>>}
   */
  searchDailySummaries: (query) => ipcRenderer.invoke('search-daily-summaries', query),
  searchGraph: (query) => ipcRenderer.invoke('search-graph', query),

  // ── Memory Graph Integration ────────────────────────────────────────────
  getMemoryGraphStatus: () => ipcRenderer.invoke('get-memory-graph-status'),
  getFullMemoryGraph: () => ipcRenderer.invoke('get-full-memory-graph'),
  searchMemoryGraph: (query, options) => ipcRenderer.invoke('search-memory-graph', query, options),
  getRelatedNodes: (nodeId, relationType) => ipcRenderer.invoke('get-related-nodes', nodeId, relationType),
  getCoreMemory: () => ipcRenderer.invoke('get-core-memory'),
  triggerMemoryGraphJob: (jobType) => ipcRenderer.invoke('trigger-memory-graph-job', jobType),
  searchRawEvents: (query) => ipcRenderer.invoke('search-raw-events', query),
  getSuggestionLLMSettings: () => ipcRenderer.invoke('get-suggestion-llm-settings'),
  saveSuggestionLLMSettings: (payload) => ipcRenderer.invoke('save-suggestion-llm-settings', payload || {}),
  getRelationshipContacts: (payload = {}) => ipcRenderer.invoke('get-relationship-contacts', payload),
  getRelationshipContactDetail: (contactId) => ipcRenderer.invoke('get-relationship-contact-detail', contactId),
  updatePersonProfile: (payload = {}) => ipcRenderer.invoke('update-person-profile', payload),
  generateRelationshipDraft: (payload = {}) => ipcRenderer.invoke('generate-relationship-draft', payload),
  syncAppleContacts: (payload = {}) => ipcRenderer.invoke('sync-apple-contacts', payload),

  // Chat sessions: allow renderer to push sessions to main for long-term memory ingestion
  saveChatSessionsToMemory: (sessions) => ipcRenderer.invoke('save-chat-sessions-to-memory', sessions),
  saveChatSessions: (sessions) => ipcRenderer.invoke('save-chat-sessions', sessions),
  getChatSessions: (options = {}) => ipcRenderer.invoke('get-chat-sessions', options),

  // ── Initial Sync Event Listeners ─────────────────────────────────────────

  /** Fired when the auto-sync kicks off on first launch. */
  onInitialSyncStarted:  (cb) => ipcRenderer.on('initial-sync-started',  (_e)        => cb()),

  /**
   * Periodic progress updates during the sync.
   * @param {(progress: { phase: string, done: number, total: number, currentDate?: string }) => void} cb
   */
  onInitialSyncProgress: (cb) => ipcRenderer.on('initial-sync-progress', (_e, data)  => cb(data)),

  /**
   * Fired when the sync completes successfully.
   * @param {(result: { daysProcessed: number }) => void} cb
   */
  onInitialSyncComplete: (cb) => ipcRenderer.on('initial-sync-complete', (_e, data)  => cb(data)),

  /** Fired if the auto-sync encounters a fatal error. */
  onInitialSyncError:    (cb) => ipcRenderer.on('initial-sync-error',    (_e, data)  => cb(data)),

  // ── Misc Event Listeners ─────────────────────────────────────────────────
  onAuthSuccess:        (cb) => ipcRenderer.on('auth-success',        (event, ...args) => cb(...args)),
  onGSuiteSyncComplete: (cb) => ipcRenderer.on('gsuite-sync-complete', (event, ...args) => cb(...args)),
  onExtensionEvent:     (cb) => ipcRenderer.on('extension-event',      (event, ...args) => cb(...args)),
  onPlannerStep:        (cb) => ipcRenderer.on('planner-step',         (_e, data) => cb(data)),
  onMemoryGraphUpdate:  (cb) => ipcRenderer.on('memory-graph-update',  (_e, data) => cb(data)),
  onProactiveSuggestions: (cb) => ipcRenderer.on('proactive-suggestions', (_e, suggestions) => cb(suggestions)),
  onVoiceCommandToggle: (cb) => ipcRenderer.on('voice-command-toggle', (_e, payload) => cb(payload)),
  onVoiceSessionUpdate: (cb) => ipcRenderer.on('voice-session-update', (_e, payload) => cb(payload)),
  removeAllListeners:   (channel) => ipcRenderer.removeAllListeners(channel),

  // ── Scheduled Automations ─────────────────────────────────────────────────
  listAutomations:    ()                         => ipcRenderer.invoke('list-automations'),
  deleteAutomation:   (id)                       => ipcRenderer.invoke('delete-automation', id),
  toggleAutomation:   (id, enabled)              => ipcRenderer.invoke('toggle-automation', id, enabled),
  onAutomationResult: (cb)                       => ipcRenderer.on('automation-result', (_e, data) => cb(data))
});
