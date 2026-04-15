// DOM Elements
const goalInput = document.getElementById('goalInput');
const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const settingsBtn = document.getElementById('settingsBtn');
const settingsPanel = document.getElementById('settingsPanel');
const closeSettings = document.getElementById('closeSettings');
const saveSettings = document.getElementById('saveSettings');
const apiKeyInput = document.getElementById('apiKey');
const modelSelect = document.getElementById('model');
const maxStepsInput = document.getElementById('maxSteps');
const statusIndicator = document.getElementById('statusIndicator');
const progressSection = document.getElementById('progressSection');
const stepCounter = document.getElementById('stepCounter');
const progressGoal = document.getElementById('progressGoal');
const progressFill = document.getElementById('progressFill');
const logContainer = document.getElementById('logContainer');
const clearLog = document.getElementById('clearLog');
const inputSection = document.getElementById('inputSection');

let isRunning = false;

// Load saved settings
async function loadSettings() {
  const result = await chrome.storage.local.get(['apiKey', 'model', 'maxSteps']);
  if (result.apiKey) apiKeyInput.value = result.apiKey;
  if (result.model) modelSelect.value = result.model;
  if (result.maxSteps) maxStepsInput.value = result.maxSteps;
}

// Save settings
saveSettings.addEventListener('click', async () => {
  await chrome.storage.local.set({
    apiKey: apiKeyInput.value,
    model: modelSelect.value,
    maxSteps: parseInt(maxStepsInput.value) || 30
  });
  settingsPanel.classList.remove('visible');
  addLogEntry('Settings saved successfully', 'complete');
});

// Toggle settings panel
settingsBtn.addEventListener('click', () => {
  settingsPanel.classList.add('visible');
});

closeSettings.addEventListener('click', () => {
  settingsPanel.classList.remove('visible');
});

// Clear log
clearLog.addEventListener('click', () => {
  logContainer.innerHTML = '<div class="log-empty">Actions will appear here...</div>';
});

// Add log entry
function addLogEntry(message, type = 'think') {
  const empty = logContainer.querySelector('.log-empty');
  if (empty) empty.remove();

  const icons = {
    click: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5"/></svg>',
    type: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="4" width="20" height="16" rx="2"/><path d="M6 8h.01M10 8h.01M14 8h.01M18 8h.01M8 12h.01M12 12h.01M16 12h.01M7 16h10"/></svg>',
    scroll: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14M5 12l7 7 7-7"/></svg>',
    navigate: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6M15 3h6v6M10 14L21 3"/></svg>',
    think: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4M12 8h.01"/></svg>',
    complete: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>',
    error: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6M9 9l6 6"/></svg>'
  };

  const now = new Date();
  const time = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });

  const entry = document.createElement('div');
  entry.className = 'log-entry';
  entry.innerHTML = `
    <div class="log-icon ${type}">${icons[type] || icons.think}</div>
    <div class="log-content">${message}</div>
    <div class="log-time">${time}</div>
  `;

  logContainer.appendChild(entry);
  logContainer.scrollTop = logContainer.scrollHeight;
}

// Update status
function updateStatus(status, text) {
  statusIndicator.className = `status-indicator ${status}`;
  statusIndicator.querySelector('.status-text').textContent = text;
}

// Update progress
function updateProgress(step, maxSteps) {
  stepCounter.textContent = `Step ${step} / ${maxSteps}`;
  progressFill.style.width = `${(step / maxSteps) * 100}%`;
}

// Start the agent
startBtn.addEventListener('click', async () => {
  const goal = goalInput.value.trim();
  if (!goal) {
    addLogEntry('Please enter a goal', 'error');
    return;
  }

  const settings = await chrome.storage.local.get(['apiKey', 'model', 'maxSteps']);
  if (!settings.apiKey) {
    addLogEntry('Please set your DeepSeek API key in settings', 'error');
    settingsPanel.classList.add('visible');
    return;
  }

  isRunning = true;
  updateStatus('running', 'Running');
  progressSection.classList.add('visible');
  progressGoal.textContent = goal;
  startBtn.disabled = true;
  goalInput.disabled = true;

  addLogEntry(`<strong>Goal:</strong> ${goal}`, 'think');

  // Send message to background script to start the agent
  chrome.runtime.sendMessage({
    type: 'START_AGENT',
    goal: goal,
    apiKey: settings.apiKey,
    model: settings.model || 'deepseek-chat',
    maxSteps: settings.maxSteps || 30
  });
});

// Stop the agent
stopBtn.addEventListener('click', () => {
  chrome.runtime.sendMessage({ type: 'STOP_AGENT' });
  stopAgent();
});

function stopAgent() {
  isRunning = false;
  updateStatus('', 'Stopped');
  progressSection.classList.remove('visible');
  startBtn.disabled = false;
  goalInput.disabled = false;
  addLogEntry('Agent stopped by user', 'error');
}

// Listen for messages from background script
chrome.runtime.onMessage.addListener((message) => {
  switch (message.type) {
    case 'LOG':
      addLogEntry(message.content, message.logType || 'think');
      break;
    case 'PROGRESS':
      updateProgress(message.step, message.maxSteps);
      break;
    case 'COMPLETE':
      isRunning = false;
      updateStatus('', 'Complete');
      progressSection.classList.remove('visible');
      startBtn.disabled = false;
      goalInput.disabled = false;
      addLogEntry(`<strong>Task completed!</strong> ${message.summary || ''}`, 'complete');
      break;
    case 'ERROR':
      isRunning = false;
      updateStatus('error', 'Error');
      progressSection.classList.remove('visible');
      startBtn.disabled = false;
      goalInput.disabled = false;
      addLogEntry(`<strong>Error:</strong> ${message.error}`, 'error');
      break;
    case 'STOPPED':
      stopAgent();
      break;
  }
});

// Check if agent is already running
chrome.runtime.sendMessage({ type: 'GET_STATUS' }, (response) => {
  if (response && response.isRunning) {
    isRunning = true;
    updateStatus('running', 'Running');
    progressSection.classList.add('visible');
    progressGoal.textContent = response.goal || '';
    updateProgress(response.step || 0, response.maxSteps || 30);
    startBtn.disabled = true;
    goalInput.disabled = true;
  }
});

// Initialize
loadSettings();
