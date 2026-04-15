/**
 * Scoring utilities for suggestions
 * Exports: scoreCandidate(openLoop, userProfile, contactProfile)
 *          generateSuggestionsFromOpenLoops(openLoops, userProfile, contactProfiles, options)
 */
const {
  inferIntentLabel,
  normalizeSuggestion,
  rankAndLimitSuggestions
} = require('./agent/intent-first-suggestions');

function clamp(v, a=0, b=1) { return Math.max(a, Math.min(b, v)); }

function normalizeLinear(value, max) {
  if (!isFinite(value) || max <= 0) return 0;
  return clamp(value / max, 0, 1);
}

function safeStr(value) {
  return value == null ? '' : String(value);
}

function timeToDueScore(dueBy) {
  if (!dueBy) return 0;
  const now = Date.now();
  const due = new Date(dueBy).getTime();
  const diff = due - now; // ms
  if (isNaN(due)) return 0;
  // If overdue -> highest urgency
  if (diff <= 0) return 1;
  // Map within 7 days window
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  return clamp(1 - (diff / sevenDays), 0, 1);
}

function recencyBoost(createdAt) {
  if (!createdAt) return 0;
  const now = Date.now();
  const created = new Date(createdAt).getTime();
  const diff = now - created; // ms since creation
  if (diff < 0) return 0;
  const oneWeek = 7 * 24 * 60 * 60 * 1000;
  const score = clamp(1 - (diff / oneWeek), 0, 1);
  return score * 0.1; // small boost
}

function computeUrgency(loop) {
  // Use due date, reopened behavior (evidence length), and loop type
  let urgency = 0;
  urgency = Math.max(urgency, timeToDueScore(loop.due_by));
  // more evidence items -> likely higher urgency (user revisited, multiple signals)
  urgency = Math.max(urgency, normalizeLinear(loop.evidence ? loop.evidence.length : 0, 4));
  // certain types carry implicit urgency
  if (loop.type === 'recruiter_followup') urgency = Math.max(urgency, 0.7);
  if (loop.type === 'awaiting_reply') urgency = Math.max(urgency, 0.5);
  return clamp(urgency, 0, 1);
}

function computeImportance(loop, contactProfile, userProfile) {
  // Importance heuristics: contact category, alignment with user goals
  let importance = 0.2; // baseline
  const category = contactProfile && contactProfile.identity && contactProfile.identity.category;
  if (category) {
    if (/recruiter|investor|manager/i.test(category)) importance = Math.max(importance, 0.8);
    else if (/professor|alum/i.test(category)) importance = Math.max(importance, 0.6);
    else importance = Math.max(importance, 0.4);
  }
  // If user's explicit goals mention keywords that match loop description, boost
  try {
    if (userProfile && Array.isArray(userProfile.goals)) {
      const desc = (loop.description || '').toLowerCase();
      for (const g of userProfile.goals) {
        if (!g || !g.text) continue;
        const t = g.text.toLowerCase();
        if (t && desc.includes(t.split(' ')[0])) {
          importance = Math.max(importance, 0.9);
          break;
        }
      }
    }
  } catch(e){}

  return clamp(importance, 0, 1);
}

function computeAvoidancePenalty(loop, userProfile) {
  if (!userProfile || !userProfile.learned) return 0;
  const avoidList = userProfile.learned.avoidance_patterns || [];
  if (!Array.isArray(avoidList) || avoidList.length === 0) return 0;
  const desc = (loop.description || '').toLowerCase();
  for (const a of avoidList) {
    if (!a) continue;
    if (desc.includes(a.toLowerCase())) return 1; // full penalty
  }
  return 0;
}

function scoreCandidate(loop, userProfile = {}, contactProfile = {}) {
  const urgency = computeUrgency(loop);
  const importance = computeImportance(loop, contactProfile, userProfile);
  const clusterWeight = (userProfile && userProfile.learned && userProfile.learned.cluster_weights && userProfile.learned.cluster_weights[loop.type]) || 0.5;
  const avoidance = computeAvoidancePenalty(loop, userProfile);
  const recency = recencyBoost(loop.created_at);

  // weights (tunable)
  const wUrg = 0.4;
  const wImp = 0.35;
  const wCluster = 0.2;
  const wAvoid = -0.25;

  let raw = (wUrg * urgency) + (wImp * importance) + (wCluster * clusterWeight) + (wAvoid * avoidance) + recency;
  const score = clamp((raw + 1) / 2, 0, 1); // scale into [0,1]
  return { score, components: { urgency, importance, clusterWeight, avoidance, recency, raw } };
}

function generateSuggestionsFromOpenLoops(openLoops, userProfile = {}, contactProfiles = {}, options = {}) {
  const topK = options.topK || 7;
  const suggestions = [];

  function normTitle(t) { return (t || '').toLowerCase().replace(/[^a-z0-9]/g,'').trim(); }
  function extractEvidenceHint(loop) {
    const ev0 = loop && loop.evidence && loop.evidence[0] ? loop.evidence[0] : null;
    const summary = ev0 && ev0.summary ? safeStr(ev0.summary) : '';
    const id = ev0 && ev0.id ? safeStr(ev0.id) : '';
    const type = ev0 && ev0.type ? safeStr(ev0.type) : '';
    return { summary, id, type };
  }
  function buildSpecificTitle(loop) {
    const hint = extractEvidenceHint(loop);
    const contactId = safeStr(loop.contact_id || '');
    const person = contactId ? contactId.split('<')[0].trim() : '';
    const subj = hint.summary || loop.description || '';

    if (loop.type === 'awaiting_reply') {
      if (person) return `Reply to ${person}${subj ? ` about “${subj.slice(0, 60)}”` : ''}`;
      return `Reply to pending message${subj ? ` about “${subj.slice(0, 60)}”` : ''}`;
    }
    if (loop.type === 'schedule_meeting') {
      if (person) return `Schedule time with ${person}${subj ? ` (re: “${subj.slice(0, 50)}”)` : ''}`;
      return `Schedule the requested meeting${subj ? ` (re: “${subj.slice(0, 50)}”)` : ''}`;
    }
    if (loop.type === 'recruiter_followup') {
      if (person) return `Reply to ${person}${subj ? ` about “${subj.slice(0, 55)}”` : ''}`;
      return `Reply to the recruiter thread${subj ? ` about “${subj.slice(0, 55)}”` : ''}`;
    }
    if (loop.type === 'job_page_visit') {
      return `Complete the next job step${subj ? ` for “${subj.slice(0, 60)}”` : ''}`;
    }
    if (loop.type === 'promise_to_send') {
      if (person) return `Send the promised update to ${person}`;
      return `Send the promised update`;
    }
    return (loop.description || '').slice(0, 80) || 'Investigate next step';
  }
  function trimToSingleClause(text, maxLen = 140) {
    const raw = safeStr(text).replace(/\s+/g, ' ').trim();
    if (!raw) return '';
    const parts = raw.split(/\b(?:and|then|also|plus|while)\b|[;|]/i).map((p) => p.trim()).filter(Boolean);
    return (parts[0] || raw).slice(0, maxLen);
  }
  function inferIntentTags(text) {
    const t = safeStr(text).toLowerCase();
    const tags = [];
    if (/reply|email|message|thread|follow up/.test(t)) tags.push('communication');
    if (/meeting|calendar|agenda|event/.test(t)) tags.push('meeting');
    if (/doc|document|proposal|report|draft|slide/.test(t)) tags.push('document');
    if (/code|bug|error|deploy|manifest|extension|api/.test(t)) tags.push('engineering');
    if (/homework|assignment|study|class|exam/.test(t)) tags.push('study');
    if (!tags.length) tags.push('general');
    return tags;
  }
  function makeSingleFocusTitle(title, loop) {
    const base = trimToSingleClause(title, 120);
    const intents = inferIntentTags(`${base} ${safeStr(loop && loop.description)}`);
    if (intents.length <= 1 && !/\b(and|then|also|plus)\b/i.test(base)) return base;
    const hint = extractEvidenceHint(loop);
    const focus = trimToSingleClause(hint.summary || loop.description || '', 70);
    const intent = intents[0];
    if (intent === 'communication') return `Reply to one pending thread${focus ? `: ${focus}` : ''}`.slice(0, 120);
    if (intent === 'meeting') return `Schedule or prep one meeting${focus ? `: ${focus}` : ''}`.slice(0, 120);
    if (intent === 'document') return `Complete one document step${focus ? `: ${focus}` : ''}`.slice(0, 120);
    if (intent === 'engineering') return `Fix one specific issue${focus ? `: ${focus}` : ''}`.slice(0, 120);
    if (intent === 'study') return `Finish one study task${focus ? `: ${focus}` : ''}`.slice(0, 120);
    return base || 'Complete one concrete task';
  }
  function mapCategoryToFixedSet(category) {
    const raw = safeStr(category).toLowerCase();
    if (/work|job|career|project|business|recruit/i.test(raw)) return 'Work';
    if (/education|learn|study|course|class|thesis|research/i.test(raw)) return 'Education';
    if (/health|fitness|workout|gym|sleep|doctor/i.test(raw)) return 'Health';
    if (/social|relationship|friend|family|network/i.test(raw)) return 'Social';
    if (/finance|money|invoice|tax|bill|payment|bank/i.test(raw)) return 'Finance';
    return 'Personal';
  }
  function buildDedupeKey(loop, suggestion) {
    if (loop && loop.focus_key) return `focus:${loop.focus_key}`;
    const hint = extractEvidenceHint(loop);
    const msgKey = hint.type === 'message' && hint.id ? `msg:${hint.id}` : '';
    const urlKey = suggestion && suggestion.deeplink ? `url:${suggestion.deeplink}` : '';
    const person = safeStr(loop.contact_id || '').toLowerCase();
    const action = safeStr(suggestion && suggestion.action_type).toLowerCase();
    const t = normTitle(suggestion && suggestion.title);
    return msgKey || urlKey || `${action}|${person}|${t}`.trim();
  }
  function isRepetitiveExecutableAction(actionType, category) {
    const a = safeStr(actionType).toLowerCase();
    const c = safeStr(category).toLowerCase();
    if (/education/.test(c)) return false;
    return ['follow_up', 'send_message', 'send_email', 'schedule_meeting', 'apply_or_review'].includes(a);
  }

  for (const loop of openLoops) {
    const contactId = loop.contact_id || (loop.evidence && loop.evidence[0] && loop.evidence[0].from) || null;
    const contact = contactId ? (contactProfiles[contactId] || {}) : {};
    const scored = scoreCandidate(loop, userProfile, contact);
    const title = makeSingleFocusTitle(buildSpecificTitle(loop), loop);
    const action_type = mapLoopTypeToAction(loop.type);
    const category = mapCategoryToFixedSet(mapLoopToCategory(loop.type));

    // Best-effort deeplink for Gmail-like evidence
    const hint = extractEvidenceHint(loop);
    let deeplink = null;
    if (hint.type === 'message' && hint.id) {
      deeplink = `https://mail.google.com/mail/u/0/#inbox/${hint.id}`;
    }

    // Best-effort draft for common follow-up actions
    let ai_draft = null;
    if (action_type === 'follow_up' || action_type === 'send_message' || action_type === 'send_email') {
      const person = safeStr(loop.contact_id || '').split('<')[0].trim();
      ai_draft = person
        ? `Hi ${person},\n\nQuick follow-up on this — do you have an update or next steps?\n\nThanks,\n`
        : `Hi there,\n\nQuick follow-up on this — do you have an update or next steps?\n\nThanks,\n`;
    }
    const aiEligible = isRepetitiveExecutableAction(action_type, category);
    const intent = inferIntentLabel(`${title} ${loop.description || ''} ${hint.summary || ''}`);
    const suggestion = normalizeSuggestion({
      id: `suggestion_${Date.now()}_${Math.random().toString(36).slice(2,8)}`,
      user_id: userProfile.id || null,
      title,
      intent,
      action_type,
      evidence: loop.evidence || [],
      time_window: recommendTimeWindow(loop, userProfile),
      deeplink,
      draft_template_id: null,
      score: scored.score,
      score_components: scored.components,
      status: 'suggested',
      open_loop_id: loop.id,
      category,
      assignee: (scored.score > 0.6 && aiEligible) ? 'ai' : 'human',
      ai_draft,
      reason: buildReason(loop, hint),
      plan: buildPlan(loop, hint),
      confidence: clamp(0.48 + scored.score * 0.45 + (hint.summary ? 0.08 : 0), 0, 1),
      continuity: clamp(0.3 + ((loop.evidence || []).length * 0.12), 0, 1)
    });
    suggestions.push(suggestion);
  }

  // Deduplicate suggestions by stable evidence keys + action/person/title fingerprint.
  const seen = new Map(); // key -> suggestion
  for (let i = 0; i < suggestions.length; i++) {
    const s = suggestions[i];
    const loop = openLoops[i];
    const key = buildDedupeKey(loop, s);
    if (!key) continue;
    if (!seen.has(key)) seen.set(key, s);
    else {
      // prefer higher score
      const prev = seen.get(key);
      if ((s.score || 0) > (prev.score || 0)) seen.set(key, s);
    }
  }

  const deduped = Array.from(seen.values());
  return rankAndLimitSuggestions(deduped, { maxTotal: Math.min(topK, 5), maxPerCategory: 2, maxFollowups: 1 });
}

function buildReason(loop, hint) {
  const summary = safeStr(hint && hint.summary);
  if (loop.type === 'awaiting_reply') {
    return `This thread still looks unresolved because the message asks for a response${summary ? ` about ${summary.slice(0, 80)}` : ''}.`;
  }
  if (loop.type === 'schedule_meeting') {
    return `A scheduling request is still open${summary ? ` around ${summary.slice(0, 80)}` : ''}, so sending a concrete slot now should unblock it.`;
  }
  if (loop.type === 'recruiter_followup') {
    return `A recruiter-related thread remains open${summary ? ` about ${summary.slice(0, 80)}` : ''}, and replying now keeps the process moving.`;
  }
  if (loop.type === 'job_page_visit') {
    return `You revisited this opportunity multiple times without acting${summary ? ` (${summary.slice(0, 80)})` : ''}, so the next concrete step should happen now.`;
  }
  if (loop.type === 'promise_to_send') {
    return `You already committed to sending something${summary ? ` related to ${summary.slice(0, 80)}` : ''}, and closing that loop now preserves trust.`;
  }
  return 'Recent evidence suggests there is one unresolved next step worth completing now.';
}

function buildPlan(loop, hint) {
  const summary = safeStr(hint && hint.summary).slice(0, 80);
  if (loop.type === 'awaiting_reply' || loop.type === 'recruiter_followup' || loop.type === 'promise_to_send') {
    return [
      'Open the exact thread',
      summary ? `Write the concrete reply for ${summary}` : 'Write the concrete reply',
      'Send it and confirm the thread is cleared'
    ];
  }
  if (loop.type === 'schedule_meeting') {
    return [
      'Open the thread with the scheduling request',
      'Offer one or two specific time slots',
      'Confirm the calendar hold or next reply'
    ];
  }
  if (loop.type === 'job_page_visit') {
    return [
      'Open the job page you revisited',
      'Complete the next concrete step such as applying or drafting outreach',
      'Log what is still missing before you leave'
    ];
  }
  return [
    'Open the source context',
    'Complete the next unfinished step',
    'Verify the outcome before switching away'
  ];
}

function mapLoopTypeToAction(loopType) {
  const map = {
    'promise_to_send': 'send_message',
    'awaiting_reply': 'follow_up',
    'schedule_meeting': 'schedule_meeting',
    'recruiter_followup': 'follow_up',
    'job_page_visit': 'apply_or_review'
  };
  return map[loopType] || 'investigate';
}

function recommendTimeWindow(loop, userProfile) {
  // Simple recommendation: use user's productive hours if available, else next 24 hours
  const now = Date.now();
  if (userProfile && userProfile.learned && Array.isArray(userProfile.learned.productive_hours) && userProfile.learned.productive_hours.length) {
    const hour = userProfile.learned.productive_hours[0];
    const start = new Date(now);
    start.setHours(hour, 0, 0, 0);
    let end = new Date(start);
    end.setHours(start.getHours() + 1);
    if (start.getTime() < now) {
      start.setDate(start.getDate() + 1);
      end.setDate(end.getDate() + 1);
    }
    return { start: start.toISOString(), end: end.toISOString() };
  }
  const start = new Date(now + (60*60*1000));
  const end = new Date(now + (3*60*60*1000));
  return { start: start.toISOString(), end: end.toISOString() };
}

module.exports = { scoreCandidate, generateSuggestionsFromOpenLoops };

function mapLoopToCategory(loopType) {
  const map = {
    'promise_to_send': 'miscellaneous',
    'awaiting_reply': 'work',
    'schedule_meeting': 'work',
    'recruiter_followup': 'work',
    'job_page_visit': 'work'
  };
  return map[loopType] || 'miscellaneous';
}
