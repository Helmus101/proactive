const activeChatRequestsBySender = new Map();
const activeChatRequestRegistry = new Map();
const queuedChatPersistenceKeys = new Set();
let chatPersistenceQueue = Promise.resolve();

let deps = {
  appState: null
};

function init(options) {
  deps.appState = options.appState;
}

function makeChatRequestKey(senderId, requestId) {
  return `${senderId}:${requestId}`;
}

function isChatActive() {
  return activeChatRequestRegistry.size > 0;
}

function startActiveChatRequest(senderId, requestId) {
  const key = makeChatRequestKey(senderId, requestId);
  if (deps.appState) {
    deps.appState.appInteractionState.chatActive = true;
    deps.appState.markAppInteraction("chat-start");
    // Trigger immediate update to defer background work and lower UI effects
    deps.appState.updatePerformanceState(); 
  }
  const previousKey = activeChatRequestsBySender.get(senderId);
  if (previousKey && previousKey !== key) {
    const previous = activeChatRequestRegistry.get(previousKey);
    if (previous) previous.cancelled = true;
  }
  const record = {
    senderId,
    requestId,
    key,
    cancelled: false,
    startedAt: Date.now(),
    lastStepEmitAt: 0
  };
  activeChatRequestsBySender.set(senderId, key);
  activeChatRequestRegistry.set(key, record);
  return record;
}

function getActiveChatRequest(senderId, requestId) {
  return activeChatRequestRegistry.get(makeChatRequestKey(senderId, requestId)) || null;
}

function cancelActiveChatRequest(senderId, requestId) {
  const record = getActiveChatRequest(senderId, requestId);
  if (!record) return false;
  record.cancelled = true;
  return true;
}

function finishActiveChatRequest(senderId, requestId) {
  const key = makeChatRequestKey(senderId, requestId);
  
  const activeKey = activeChatRequestsBySender.get(senderId);
  if (activeKey === key) activeChatRequestsBySender.delete(senderId);
  activeChatRequestRegistry.delete(key);
  
  if (activeChatRequestRegistry.size === 0 && deps.appState) {
    deps.appState.appInteractionState.chatActive = false;
  }
}

function slimNode(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) return obj.map(slimNode);
  const slim = {};
  const keep = ['id', 'node_id', 'event_id', 'title', 'layer', 'score', 'useful_score', 'status', 'requestId', 'step', 'label', 'detail', 'stages', 'trace', 'thinking_trace', 'stage_trace', 'timestamp', 'app'];
  for (const key of keep) {
    if (key in obj) {
      if (['trace', 'thinking_trace', 'stage_trace', 'stages'].includes(key)) {
        slim[key] = slimNode(obj[key]);
      } else {
        slim[key] = obj[key];
      }
    }
  }
  return slim;
}

function compactChatStepPayload(data = {}, requestId) {
  // Use slimNode to recursively remove heavy fields (embeddings, raw text, canonical_text)
  const payload = slimNode({ ...data, requestId });
  if (Array.isArray(payload.preview_items)) {
    payload.preview_items = payload.preview_items.slice(0, 5);
  }
  return payload;
}

function enqueueChatPersistence(key, task) {
  if (!key || typeof task !== 'function' || queuedChatPersistenceKeys.has(key)) return;
  queuedChatPersistenceKeys.add(key);
  chatPersistenceQueue = chatPersistenceQueue
    .then(async () => {
      try {
        await task();
      } catch (error) {
        console.warn('[chat-memory] queued persistence failed:', error?.message || error);
      } finally {
        queuedChatPersistenceKeys.delete(key);
      }
    })
    .catch((error) => {
      queuedChatPersistenceKeys.delete(key);
      console.warn('[chat-memory] queue failure:', error?.message || error);
    });
}

module.exports = {
  init,
  isChatActive,
  startActiveChatRequest,
  getActiveChatRequest,
  cancelActiveChatRequest,
  finishActiveChatRequest,
  compactChatStepPayload,
  enqueueChatPersistence,
  activeChatRequestRegistry
};
