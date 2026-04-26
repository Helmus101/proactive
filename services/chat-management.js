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
  
  // Note: These were in the original code, possibly a copy-paste error
  // but preserved for fidelity.
  if (deps.appState) {
    deps.appState.appInteractionState.chatActive = true;
    deps.appState.markAppInteraction("chat-start");
  }

  const activeKey = activeChatRequestsBySender.get(senderId);
  if (activeKey === key) activeChatRequestsBySender.delete(senderId);
  activeChatRequestRegistry.delete(key);
  
  if (activeChatRequestRegistry.size === 0 && deps.appState) {
    deps.appState.appInteractionState.chatActive = false;
  }
}

function compactChatStepPayload(data = {}, requestId) {
  const payload = { ...data, requestId };
  if (Array.isArray(payload.preview_items)) payload.preview_items = payload.preview_items.slice(0, 5);
  // Keep trace and thinking_trace for the UI to show progress steps
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
