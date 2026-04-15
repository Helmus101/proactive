// Minimal script to keep the Offscreen document active
console.log('AI Keep Alive Active');
setInterval(() => {
  // Echo ping to background script
  try {
    chrome.runtime.sendMessage({ type: 'keep-alive-ping' }, (resp) => {
      if (chrome.runtime && chrome.runtime.lastError) {
        // background may be unloaded; nothing to do
        return;
      }
    });
  } catch (e) {}
}, 30000);
