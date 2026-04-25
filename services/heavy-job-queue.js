const { powerMonitor } = require('electron');

const LOW_POWER_HEAVY_JOB_MIN_GAP_MS = 60 * 60 * 1000;
const HEAVY_JOB_RETRY_COOLDOWN_MS = 60 * 1000;

const heavyJobState = {
  activeJob: null,
  startedAt: 0,
  perJobLastSkipAt: {}
};

const pendingHeavyJobs = new Map();

const HEAVY_JOB_QUEUE_ORDER = [
  'gsuite_sync',
  'radar_generation',
  'episode_generation',
  'relationship_graph',
  'relationship_graph_backfill',
  'semantic_window',
  'semantic_pulse',
  'daily_insight',
  'weekly_insight',
  'living_core'
];

let deps = {
  appState: null,
  chatManagement: null,
  updateMemoryGraphHealth: () => {},
  emitMemoryGraphUpdate: () => {}
};

function init(options) {
  Object.assign(deps, options);
}

function canRunHeavyJob(lastRunAt = 0) {
  const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
  const mode = deps.appState ? deps.appState.getPerformanceMode() : 'normal';
  
  if (mode === 'deep-idle') {
    return idleTime > 600 && (Date.now() - lastRunAt) > (4 * 60 * 60 * 1000);
  }
  if (mode === 'reduced') {
    return idleTime > 180 && (Date.now() - lastRunAt) > (2 * 60 * 60 * 1000);
  }
  
  return idleTime > 60 && (Date.now() - lastRunAt) > LOW_POWER_HEAVY_JOB_MIN_GAP_MS;
}

function shouldDeferBackgroundWork(label = 'background') {
  const idleTime = (powerMonitor && typeof powerMonitor.getSystemIdleTime === 'function') ? powerMonitor.getSystemIdleTime() : 0;
  const appBusy = (deps.appState && deps.appState.isAppInteractionHot()) || (deps.chatManagement && deps.chatManagement.isChatActive());
  const mode = deps.appState ? deps.appState.getPerformanceMode() : 'normal';
  
  let deferThreshold = 30;
  if (mode === 'reduced') deferThreshold = 120;
  if (mode === 'deep-idle') deferThreshold = 300;
  
  const defer = idleTime < deferThreshold || appBusy || mode === 'reduced';
  
  if (defer && Math.random() < 0.01) console.log(`[${label}] Deferring heavy work; idle=${idleTime}s appBusy=${appBusy} mode=${mode}`);
  return defer;
}

function beginHeavyJob(jobName, options = {}) {
  const name = String(jobName || 'heavy_job');
  const now = Date.now();
  if (heavyJobState.activeJob && heavyJobState.activeJob !== name) {
    const lastSkipAt = Number(heavyJobState.perJobLastSkipAt[name] || 0);
    if ((now - lastSkipAt) > HEAVY_JOB_RETRY_COOLDOWN_MS) {
      console.log(`[${name}] Skipping because ${heavyJobState.activeJob} is already running`);
      heavyJobState.perJobLastSkipAt[name] = now;
    }
    deps.updateMemoryGraphHealth({
      heavyJobActive: heavyJobState.activeJob,
      lastHeavyJobSkipped: name,
      lastHeavyJobSkippedAt: new Date(now).toISOString()
    });
    deps.emitMemoryGraphUpdate({
      type: 'job_status',
      job: name,
      status: 'skipped',
      reason: 'busy',
      blocked_by: heavyJobState.activeJob
    });
    return false;
  }
  heavyJobState.activeJob = name;
  heavyJobState.startedAt = now;
  deps.updateMemoryGraphHealth({
    heavyJobActive: name,
    heavyJobStartedAt: new Date(now).toISOString()
  });
  deps.emitMemoryGraphUpdate({
    type: 'job_status',
    job: name,
    status: 'running',
    source: options?.source || 'background'
  });
  return true;
}

function endHeavyJob(jobName, meta = {}) {
  const name = String(jobName || '');
  const now = Date.now();
  if (heavyJobState.activeJob === name) {
    const durationMs = Math.max(0, now - Number(heavyJobState.startedAt || now));
    heavyJobState.activeJob = null;
    heavyJobState.startedAt = 0;
    deps.updateMemoryGraphHealth({
      heavyJobActive: null,
      lastHeavyJobCompleted: name,
      lastHeavyJobCompletedAt: new Date(now).toISOString(),
      lastHeavyJobDurationMs: durationMs
    });
    deps.emitMemoryGraphUpdate({
      type: 'job_status',
      job: name,
      status: meta?.status || 'completed',
      duration_ms: durationMs,
      error: meta?.error || null
    });
    setTimeout(() => {
      drainPendingHeavyJobs();
    }, 150);
  }
}

function enqueueHeavyJob(jobName, runner, options = {}) {
  const name = String(jobName || 'heavy_job');
  if (pendingHeavyJobs.has(name)) return false;
  pendingHeavyJobs.set(name, {
    runner,
    source: options?.source || 'background',
    queuedAt: Date.now()
  });
  deps.updateMemoryGraphHealth({
    pendingHeavyJobs: Array.from(pendingHeavyJobs.keys()),
    lastQueuedHeavyJob: name,
    lastQueuedHeavyJobAt: new Date().toISOString()
  });
  deps.emitMemoryGraphUpdate({
    type: 'job_status',
    job: name,
    status: 'queued',
    source: options?.source || 'background'
  });
  return true;
}

function drainPendingHeavyJobs() {
  if (deps.chatManagement && deps.chatManagement.isChatActive()) return false;
  if (heavyJobState.activeJob || !pendingHeavyJobs.size) return false;
  const queue = Array.from(pendingHeavyJobs.entries());
  const ordered = HEAVY_JOB_QUEUE_ORDER.map((name) => queue.find(([key]) => key === name)).filter(Boolean);
  const fallback = queue.filter(([key]) => !HEAVY_JOB_QUEUE_ORDER.includes(key));
  const next = [...ordered, ...fallback][0];
  if (!next) return false;
  const [jobName, job] = next;
  pendingHeavyJobs.delete(jobName);
  deps.updateMemoryGraphHealth({
    pendingHeavyJobs: Array.from(pendingHeavyJobs.keys())
  });
  Promise.resolve()
    .then(() => job.runner())
    .catch((error) => console.warn(`[${jobName}] Queued run failed:`, error?.message || error));
  return true;
}

module.exports = {
  init,
  heavyJobState,
  pendingHeavyJobs,
  HEAVY_JOB_QUEUE_ORDER,
  canRunHeavyJob,
  shouldDeferBackgroundWork,
  beginHeavyJob,
  endHeavyJob,
  enqueueHeavyJob,
  drainPendingHeavyJobs
};
