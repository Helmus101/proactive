const regexes = require('./regexes');
const { v4: uuidv4 } = require('uuid');

/**
 * Create an OpenLoop object
 */
function createOpenLoop({ type, description, evidence = [], created_at = new Date().toISOString(), due_by = null }) {
  const e0 = evidence && evidence[0] ? evidence[0] : {};
  const focusKey = String(e0.id || e0.summary || description || type)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .slice(0, 72);
  return {
    id: `openloop_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
    type,
    description,
    evidence,
    focus_key: `${type}:${focusKey}`,
    created_at,
    due_by,
    resolved: false
  };
}

/**
 * Extract open loops from a MessageEvent (email snippet or subject)
 * Returns array of OpenLoop objects (may be empty)
 */
function extractFromMessageEvent(msgEvent) {
  const text = `${msgEvent.subject || ''}\n${msgEvent.snippet || ''}`;
  const evidence = [{ type: 'message', id: msgEvent.id, summary: msgEvent.snippet || msgEvent.subject }];
  const loops = [];

  // Promise patterns
  if (regexes.matchesAny(text, regexes.promisePatterns)) {
    // If contains deadline
    let due = null;
    if (regexes.matchesAny(text, regexes.deadlinePatterns)) {
      due = null; // Could parse exact date; placeholder for later
    }
    loops.push(createOpenLoop({ type: 'promise_to_send', description: 'Sender promised to send or follow up', evidence, due_by: due }));
  }

  // Awaiting reply patterns
  if (regexes.matchesAny(text, regexes.awaitingReplyPatterns) || (msgEvent.opened_count && msgEvent.opened_count >= 3 && msgEvent.reply_status === 'no_reply')) {
    loops.push(createOpenLoop({ type: 'awaiting_reply', description: 'Message appears to be awaiting reply', evidence }));
  }

  // Scheduling requests
  if (regexes.matchesAny(text, regexes.schedulingPatterns)) {
    loops.push(createOpenLoop({ type: 'schedule_meeting', description: 'Message requests scheduling a meeting', evidence }));
  }

  // Recruiter signals
  if (regexes.matchesAny(text, regexes.recruiterPatterns)) {
    loops.push(createOpenLoop({ type: 'recruiter_followup', description: 'Recruiter or job opportunity detected', evidence }));
  }

  return loops;
}

/**
 * Extract open loops from a PageVisit (e.g., job application flow or research)
 */
function extractFromPageVisit(pageVisit) {
  const text = `${pageVisit.title || ''} ${pageVisit.url || ''}`;
  const evidence = [{ type: 'page', id: pageVisit.id || null, summary: pageVisit.title || pageVisit.url }];
  const loops = [];

  if (regexes.matchesAny(text, regexes.jobPagePatterns)) {
    loops.push(createOpenLoop({ type: 'job_page_visit', description: 'Visited job/careers page repeatedly', evidence }));
  }

  return loops;
}

/**
 * Process a generic event and return { openLoops: [], contactUpdate: null }
 * contactProfiles is an optional map of contactId -> ContactProfile objects; if available, the extractor will attach loops to contacts when it can infer identity.
 */
function processEvent(event, contactProfiles = {}) {
  let openLoops = [];
  let contactUpdate = null;

  if (!event || !event.id) return { openLoops, contactUpdate };

  if (event.subject || event.snippet) {
    // Assume MessageEvent
    openLoops = extractFromMessageEvent(event);
    // Try to infer contact
    const from = event.from || '';
    const contactId = from.toLowerCase();
    if (contactId) {
      const contact = contactProfiles[contactId] || { id: contactId, user_id: event.user_id, identity: { email: from, name: null }, interactions: [], open_loops: [] };
      // Add interaction summary
      contact.interactions = contact.interactions || [];
      contact.interactions.push({ date: event.timestamp || new Date().toISOString(), type: 'email_received', summary: event.subject || event.snippet });
      // Attach open loops to contact
      for (const loop of openLoops) {
        contact.open_loops = contact.open_loops || [];
        contact.open_loops.push(loop);
      }
      contactUpdate = contact;
    }
  } else if (event.url || event.domain) {
    // PageVisit
    openLoops = extractFromPageVisit(event);
    // No contact by default for page visits, but return loops
  }

  return { openLoops, contactUpdate };
}

module.exports = {
  extractFromMessageEvent,
  extractFromPageVisit,
  processEvent,
  createOpenLoop
};
