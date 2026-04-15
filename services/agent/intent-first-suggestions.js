function clamp(value, min = 0, max = 1) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, n));
}

function safeText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function trimToSingleClause(text, maxLen = 180) {
  const raw = safeText(text).replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  const parts = raw.split(/\b(?:and|then|also|plus|while)\b|[;|]/i).map((part) => part.trim()).filter(Boolean);
  return (parts[0] || raw).slice(0, maxLen);
}

function normalizeCategory(category) {
  const raw = safeText(category).toLowerCase().trim();
  if (raw.includes('follow')) return 'followup';
  if (raw.includes('creative')) return 'creative';
  if (raw.includes('personal')) return 'personal';
  if (raw.includes('study')) return 'study';
  if (raw.includes('relationship')) return 'relationship';
  if (raw.includes('plan') || raw.includes('work')) return 'work';
  return 'work';
}

function inferIntentLabel(text) {
  const t = safeText(text).toLowerCase();
  if (/\b(reply|email|message|thread|inbox|follow up)\b/.test(t)) return 'Close the communication loop';
  if (/\b(meeting|calendar|agenda|attendees|event|call)\b/.test(t)) return 'Prepare the next meeting outcome';
  if (/\b(doc|document|proposal|report|draft|slide|deck|sheet|notes)\b/.test(t)) return 'Move the draft toward a shareable state';
  if (/\b(code|bug|error|deploy|api|manifest|extension|fix|pull request|commit)\b/.test(t)) return 'Resolve the implementation blocker';
  if (/\b(homework|assignment|study|exam|class|lecture|submit|problem set)\b/.test(t)) return 'Finish the academic deliverable';
  if (/\b(product|checkout|purchase|buy|pricing)\b/.test(t)) return 'Make the pending product decision';
  if (/\b(profile|linkedin|reach out|connect|recruiter|candidate|job)\b/.test(t)) return 'Advance the outreach or application';
  return 'Advance the active piece of work';
}

function titleHasConcreteTarget(title) {
  const t = safeText(title).trim();
  if (t.length < 14) return false;
  if (/\b(this|that|it|something|anything|stuff|things)\b/i.test(t)) return false;
  if (/["']/i.test(t)) return true;
  if (/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b/.test(t)) return true;
  if (/\b(email|thread|meeting|agenda|proposal|report|deck|draft|bug|ticket|assignment|homework|profile|application|checkout|doc|file|calendar)\b/i.test(t)) return true;
  return t.split(/\s+/).length >= 4;
}

function hasBroadVerb(text) {
  return /\b(take action|follow up|return to|work on|keep working|continue|resume|investigate|look into|handle this|move forward)\b/i.test(safeText(text));
}

function buildPlan(input) {
  if (Array.isArray(input?.plan) && input.plan.length) {
    return input.plan
      .map((step) => trimToSingleClause(step, 110))
      .filter(Boolean)
      .slice(0, 3);
  }

  if (Array.isArray(input?.action_plan) && input.action_plan.length) {
    return input.action_plan
      .map((step) => trimToSingleClause(step?.intent || step?.action || '', 110))
      .filter(Boolean)
      .slice(0, 3);
  }

  const draft = safeText(input?.ai_draft || '');
  const match = draft.match(/Plan:\s*1\)\s*(.*?)\s*2\)\s*(.*?)\s*3\)\s*(.*)$/i);
  if (match) {
    return match.slice(1, 4).map((part) => trimToSingleClause(part, 110)).filter(Boolean);
  }

  const focus = trimToSingleClause(input?.title || input?.description || input?.reason || '', 80);
  return [
    input?.deeplink ? 'Open the exact source context' : 'Open the source context',
    focus ? `Complete the next unfinished step for ${focus}` : 'Complete the next unfinished step',
    'Verify the result before switching away'
  ].slice(0, 3);
}

function computeSpecificity(title, reason, plan) {
  let score = 0;
  if (titleHasConcreteTarget(title)) score += 0.45;
  if (!hasBroadVerb(title)) score += 0.2;
  if (/\b(because|after|before|from|since|you|detected|opened|edited|revisited|starts|due|thread|event|draft)\b/i.test(reason || '')) score += 0.15;
  if (Array.isArray(plan) && plan.length >= 2) score += 0.2;
  return clamp(score);
}

function priorityFromScore(score) {
  if (score >= 0.8) return 'high';
  if (score >= 0.58) return 'medium';
  return 'low';
}

function normalizeSuggestion(raw, options = {}) {
  const now = options.now || Date.now();
  const title = trimToSingleClause(raw?.title || raw?.nextAction || raw?.text || '', 120);
  const reason = trimToSingleClause(raw?.reason || raw?.whyNow || raw?.trigger || raw?.description || '', 180);
  const evidenceText = [reason, safeText(raw?.description), safeText(raw?.trigger)].filter(Boolean).join(' ');
  const intent = trimToSingleClause(
    raw?.intent ||
      raw?.goal ||
      raw?.objective ||
      raw?.inferred_intent ||
      inferIntentLabel(`${title} ${evidenceText}`),
    120
  );
  const plan = buildPlan(raw);
  const evidence = Array.isArray(raw?.evidence) ? raw.evidence.slice(0, 6) : [];
  const confidence = clamp(raw?.confidence ?? raw?.intent_confidence ?? raw?.score ?? options.defaultConfidence ?? 0.55);
  const urgency = clamp(raw?.urgency ?? raw?.score ?? 0.5);
  const continuity = clamp(
    raw?.continuity ??
      (evidence.length ? Math.min(1, 0.35 + evidence.length * 0.12) : 0.28)
  );
  const specificity = computeSpecificity(title, reason, plan);
  const evidenceDensity = clamp(evidence.length / 4);
  const baseScore = clamp(
    raw?.score ??
      (confidence * 0.32 + specificity * 0.28 + evidenceDensity * 0.18 + continuity * 0.12 + urgency * 0.1)
  );

  return {
    ...raw,
    id: raw?.id || `sug_${now}_${Math.random().toString(36).slice(2, 8)}`,
    title,
    description: trimToSingleClause(raw?.description || intent, 180),
    intent,
    reason,
    plan,
    confidence: Number(confidence.toFixed(4)),
    specificity: Number(specificity.toFixed(4)),
    evidence_density: Number(evidenceDensity.toFixed(4)),
    continuity: Number(continuity.toFixed(4)),
    score: Number(baseScore.toFixed(4)),
    priority: raw?.priority || priorityFromScore(baseScore),
    category: normalizeCategory(raw?.category),
    assignee: raw?.assignee || (raw?.action_plan?.length ? 'ai' : 'human'),
    evidence,
    action_plan: Array.isArray(raw?.action_plan) ? raw.action_plan.slice(0, 3) : [],
    ai_draft: safeText(raw?.ai_draft || '').trim(),
    createdAt: raw?.createdAt || raw?.created_at || now,
    created_at: raw?.created_at || new Date(now).toISOString()
  };
}

function passesIntentFirstQuality(raw) {
  const suggestion = normalizeSuggestion(raw);
  if (!suggestion.title || !suggestion.intent || !suggestion.reason) return false;
  if (hasBroadVerb(suggestion.title)) return false;
  if (!titleHasConcreteTarget(suggestion.title)) return false;
  if (suggestion.intent.length < 12) return false;
  if (/\b(stay on top|be proactive|keep momentum|make progress|general follow-up)\b/i.test(suggestion.intent)) return false;
  if (!/\b(because|after|before|from|since|you|thread|event|draft|opened|edited|revisited|starts|due|detected)\b/i.test(suggestion.reason)) return false;
  if (!Array.isArray(suggestion.plan) || suggestion.plan.length < 2) return false;
  if (suggestion.specificity < 0.55) return false;
  return true;
}

function dedupeKey(suggestion) {
  const topic = safeText(suggestion?.topic_key || suggestion?.topicKey || suggestion?.open_loop_id || suggestion?.source_target || '').toLowerCase();
  if (topic) return `topic:${topic}`;
  const deeplink = safeText(suggestion?.deeplink || suggestion?.action_plan?.[0]?.url || '').toLowerCase();
  if (deeplink) return `url:${deeplink}`;
  const title = safeText(suggestion?.title).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const intent = safeText(suggestion?.intent).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  return `${intent}|${title}`;
}

function rankAndLimitSuggestions(items, options = {}) {
  const maxTotal = Number(options.maxTotal || 5);
  const maxPerCategory = Number(options.maxPerCategory || 2);
  const maxFollowups = Number(options.maxFollowups || 1);
  const normalized = (Array.isArray(items) ? items : [])
    .map((item) => normalizeSuggestion(item, options))
    .filter((item) => passesIntentFirstQuality(item))
    .sort((a, b) => (b.score || 0) - (a.score || 0));

  const deduped = [];
  const seen = new Set();
  for (const item of normalized) {
    const key = dedupeKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }

  const byCategory = new Map();
  let followupCount = 0;
  const output = [];
  for (const item of deduped) {
    if (output.length >= maxTotal) break;
    const category = normalizeCategory(item.category);
    const used = byCategory.get(category) || 0;
    if (used >= maxPerCategory) continue;
    if (category === 'followup') {
      if (followupCount >= maxFollowups) continue;
      followupCount += 1;
    }
    byCategory.set(category, used + 1);
    output.push({ ...item, category });
  }
  return output;
}

module.exports = {
  clamp,
  trimToSingleClause,
  inferIntentLabel,
  normalizeSuggestion,
  passesIntentFirstQuality,
  rankAndLimitSuggestions,
  priorityFromScore
};
