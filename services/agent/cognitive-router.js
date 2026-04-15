const EventEmitter = require('events');
function graphDerivation() {
  // lazy require to avoid circular dependency at load time
  // eslint-disable-next-line global-require
  return require('./graph-derivation');
}

class CognitiveRouter extends EventEmitter {
  constructor() {
    super();
    this.on('eventIngested', this.handleEvent.bind(this));
  }

  isHighPriority(event) {
    if (!event) return false;
    
    // High priority if it's an email being SENT, or a task explicitly created/checked off
    const textLower = String(event.text || event.title || '').toLowerCase();
    const isEmailAction = event.type === 'email' || event.source_type === 'gmail';
    const isTaskAction = event.type === 'task' || event.source_type === 'calendar';
    const isCommunication = event.domain === 'mail.google.com' || event.domain === 'messages.google.com' || event.domain === 'slack.com';

    if (isEmailAction && (textLower.includes('sent') || textLower.includes('reply'))) return true;
    if (isTaskAction && (textLower.includes('done') || textLower.includes('completed'))) return true;
    if (isCommunication) return true;

    return false;
  }

  async handleEvent(event) {
    try {
      if (this.isHighPriority(event)) {
        console.log(`[CognitiveRouter] High priority event detected (${event.id}). Triggering immediate micro-update.`);
        // Run a lightweight graph derivation pass on the very recent context
  await graphDerivation().deriveGraphFromEvents({
          versionSeed: 'realtime_micro',
          limit: 20 // only grab the very recent events to build a fast edge
        }).catch((e) => {
          console.warn(`[CognitiveRouter] micro-update failed:`, e.message);
        });
      }
    } catch (e) {
      console.error(`[CognitiveRouter] error handling event:`, e.message);
    }
  }
  
  dispatch(event) {
    this.emit('eventIngested', event);
  }
}

const router = new CognitiveRouter();
module.exports = router;
