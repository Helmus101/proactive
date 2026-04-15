function safeText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function uniquePush(arr, value, limit = 7) {
  const next = String(value || '').replace(/\s+/g, ' ').trim();
  if (!next || arr.includes(next)) return;
  arr.push(next);
  if (arr.length > limit) arr.splice(limit);
}

function safeUnique(items = [], limit = 12) {
  return Array.from(new Set((items || []).map((item) => String(item || '').trim()).filter(Boolean))).slice(0, limit);
}

const LEXICAL_STOPWORDS = new Set([
  'what', 'whats', "what's", 'how', 'should', 'could', 'would', 'can', 'please',
  'show', 'tell', 'give', 'find', 'based', 'latest', 'current', 'recent', 'context',
  'about', 'around', 'from', 'with', 'this', 'that', 'these', 'those', 'here', 'there',
  'focus', 'thing', 'things', 'stuff', 'the', 'and', 'for', 'into', 'onto', 'over',
  'under', 'after', 'before', 'today', 'yesterday', 'tomorrow', 'tonight'
]);

function isUsefulLexicalTerm(term) {
  const t = String(term || '').trim().toLowerCase();
  if (!t) return false;
  if (LEXICAL_STOPWORDS.has(t)) return false;
  if (t.length < 3) return false;
  // Keep short terms only when they carry strong structure.
  if (t.length < 4 && !/[.@:/_-]/.test(t) && !/\d/.test(t)) return false;
  return true;
}

function normalizeTerms(text) {
  return safeText(text)
    .toLowerCase()
    .replace(/[^a-z0-9\s._/-]/g, ' ')
    .split(/\s+/)
    .map((term) => term.trim())
    .filter((term) => isUsefulLexicalTerm(term))
    .slice(0, 18);
}

function extractQuotedPhrases(text) {
  return [...safeText(text).matchAll(/"([^"]{3,80})"/g)]
    .map((match) => String(match[1] || '').trim())
    .filter(Boolean)
    .slice(0, 4);
}

function extractExactTokens(text) {
  const source = safeText(text);
  const found = new Set();
  const patterns = [
    /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,
    /\bhttps?:\/\/[^\s)]+/gi,
    /\b[a-z0-9.-]+\.[a-z]{2,}\b/gi,
    /\b(?:[A-Za-z0-9_-]+\/)?[A-Za-z0-9_-]+\.(?:js|ts|tsx|jsx|json|md|py|java|rb|go)\b/g,
    /\b[A-Za-z0-9_-]+(?:Error|Exception)\b/g,
    /\b[a-z]+(?:[A-Z][a-z0-9]+){1,}\b/g
  ];
  patterns.forEach((regex) => {
    for (const match of source.matchAll(regex)) {
      const value = String(match[0] || '').trim();
      if (value.length >= 3) found.add(value.toLowerCase());
      if (found.size >= 12) break;
    }
  });
  return Array.from(found).slice(0, 12);
}

function extractNamedEntities(text) {
  return [...safeText(text).matchAll(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b/g)]
    .map((match) => String(match[1] || '').trim())
    .filter((value) => value.length >= 3 && !/^(Today|Yesterday|This Morning|This Afternoon|Tonight)$/i.test(value))
    .slice(0, 8);
}

function extractNoiseTerms(text) {
  const source = safeText(text).toLowerCase();
  const matches = new Set();
  const patterns = [
    /\b(today|tonight|tomorrow|yesterday|recently|lately|currently|right now|this morning|this afternoon|this evening|last week|last month)\b/g,
    /\b(this|that|these|those|here|there|thing|stuff)\b/g,
    /\b(what|whats|what's|how|show|tell|give|find|did|does|is|are|was|were|the|a|an)\b/g
  ];
  patterns.forEach((regex) => {
    for (const match of source.matchAll(regex)) {
      const value = String(match[1] || match[0] || '').trim();
      if (value) matches.add(value);
    }
  });
  return Array.from(matches).slice(0, 12);
}

function clamp(num, min, max) {
  return Math.max(min, Math.min(max, num));
}

function startOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function endOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function normalizeDateRange(dateRange) {
  if (!dateRange || typeof dateRange !== 'object') return null;
  const start = dateRange.start || dateRange.startTimestamp;
  const end = dateRange.end || dateRange.endTimestamp;
  const startIso = start ? new Date(start).toISOString() : null;
  const endIso = end ? new Date(end).toISOString() : null;
  if (!startIso || !endIso) return null;
  return {
    start: startIso,
    end: endIso,
    label: dateRange.label || null,
    source: dateRange.source || 'explicit',
    granularity: dateRange.granularity || null
  };
}

function buildTemporalRange({ label, start, end, granularity, source = 'inferred' }) {
  if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null;
  return {
    label,
    start: start.toISOString(),
    end: end.toISOString(),
    granularity,
    source
  };
}

function inferTemporalWindow(query, now = new Date()) {
  const lower = safeText(query).toLowerCase();
  if (!lower.trim()) return null;

  if (/\btoday\b/.test(lower)) {
    return buildTemporalRange({
      label: 'today',
      start: startOfDay(now),
      end: endOfDay(now),
      granularity: 'day'
    });
  }

  if (/\byesterday\b/.test(lower)) {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return buildTemporalRange({
      label: 'yesterday',
      start: startOfDay(d),
      end: endOfDay(d),
      granularity: 'day'
    });
  }

  if (/\bearlier today\b/.test(lower)) {
    return buildTemporalRange({
      label: 'earlier_today',
      start: startOfDay(now),
      end: now,
      granularity: 'day'
    });
  }

  if (/\bthis morning\b/.test(lower)) {
    const start = startOfDay(now);
    const end = new Date(start);
    end.setHours(11, 59, 59, 999);
    return buildTemporalRange({
      label: 'this_morning',
      start,
      end: now < end ? now : end,
      granularity: 'day_part'
    });
  }

  if (/\bthis afternoon\b/.test(lower)) {
    const start = startOfDay(now);
    start.setHours(12, 0, 0, 0);
    const end = new Date(start);
    end.setHours(17, 59, 59, 999);
    return buildTemporalRange({
      label: 'this_afternoon',
      start,
      end: now < end ? now : end,
      granularity: 'day_part'
    });
  }

  if (/\btonight\b|\bthis evening\b/.test(lower)) {
    const start = startOfDay(now);
    start.setHours(18, 0, 0, 0);
    return buildTemporalRange({
      label: 'tonight',
      start,
      end: now,
      granularity: 'day_part'
    });
  }

  if (/\blast week\b/.test(lower)) {
    const currentWeekStart = startOfDay(now);
    currentWeekStart.setDate(now.getDate() - ((now.getDay() + 6) % 7));
    const start = new Date(currentWeekStart);
    start.setDate(start.getDate() - 7);
    const end = new Date(currentWeekStart);
    end.setMilliseconds(-1);
    return buildTemporalRange({
      label: 'last_week',
      start,
      end,
      granularity: 'week'
    });
  }

  if (/\blast month\b/.test(lower)) {
    const start = new Date(now.getFullYear(), now.getMonth() - 1, 1, 0, 0, 0, 0);
    const end = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
    end.setMilliseconds(-1);
    return buildTemporalRange({
      label: 'last_month',
      start,
      end,
      granularity: 'month'
    });
  }

  const agoMatch = lower.match(/\b(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago\b/);
  if (agoMatch) {
    const value = clamp(parseInt(agoMatch[1], 10) || 1, 1, 90);
    const unit = agoMatch[2];
    const end = new Date(now);
    const start = new Date(now);
    if (unit.startsWith('minute')) start.setMinutes(start.getMinutes() - value);
    else if (unit.startsWith('hour')) start.setHours(start.getHours() - value);
    else start.setDate(start.getDate() - value);
    return buildTemporalRange({
      label: `${value}_${unit}_ago`,
      start,
      end,
      granularity: unit.startsWith('minute') ? 'minute' : (unit.startsWith('hour') ? 'hour' : 'day')
    });
  }

  const lastDays = lower.match(/\blast\s+(\d+)\s+days?\b/);
  if (lastDays) {
    const days = clamp(parseInt(lastDays[1], 10) || 1, 1, 30);
    const start = startOfDay(new Date(now.getFullYear(), now.getMonth(), now.getDate() - (days - 1)));
    return buildTemporalRange({
      label: `last_${days}_days`,
      start,
      end: endOfDay(now),
      granularity: days <= 3 ? 'day' : 'range'
    });
  }

  const isoDate = lower.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (isoDate) {
    const day = new Date(`${isoDate[1]}T00:00:00`);
    if (!Number.isNaN(day.getTime())) {
      return buildTemporalRange({
        label: isoDate[1],
        start: startOfDay(day),
        end: endOfDay(day),
        granularity: 'day'
      });
    }
  }

  return null;
}

function widenTemporalWindow(dateRange, now = new Date()) {
  const normalized = normalizeDateRange(dateRange);
  if (!normalized) return null;
  const start = new Date(normalized.start);
  const end = new Date(normalized.end);
  const spanMs = Math.max(1, end.getTime() - start.getTime());
  const dayMs = 24 * 60 * 60 * 1000;

  if (spanMs <= 6 * 60 * 60 * 1000) {
    return buildTemporalRange({
      label: 'widened_same_day',
      start: startOfDay(start),
      end: endOfDay(end),
      granularity: 'day',
      source: 'widened'
    });
  }
  if (spanMs <= dayMs * 1.1) {
    const widenedStart = startOfDay(new Date(end.getFullYear(), end.getMonth(), end.getDate() - 2));
    return buildTemporalRange({
      label: 'widened_to_3_days',
      start: widenedStart,
      end: endOfDay(end),
      granularity: 'range',
      source: 'widened'
    });
  }
  if (spanMs <= dayMs * 3.2) {
    const widenedStart = startOfDay(new Date(end.getFullYear(), end.getMonth(), end.getDate() - 6));
    return buildTemporalRange({
      label: 'widened_to_7_days',
      start: widenedStart,
      end: endOfDay(end),
      granularity: 'range',
      source: 'widened'
    });
  }

  const widenedStart = startOfDay(new Date(end.getFullYear(), end.getMonth(), end.getDate() - 13));
  return buildTemporalRange({
    label: 'widened_to_14_days',
    start: widenedStart,
    end: endOfDay(end > now ? now : end),
    granularity: 'range',
    source: 'widened'
  });
}

function stripEmbeddingWeakTerms(text) {
  return safeText(text)
    .replace(/\b(today|tonight|tomorrow|yesterday|recently|lately|currently|right now|this morning|this afternoon|this evening)\b/gi, ' ')
    .replace(/\b(this|that|these|those|here|there|thing|stuff)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeQueryForEmbedding(text) {
  return stripEmbeddingWeakTerms(stripQuestionFormatting(text))
    .replace(/\b(today|yesterday|tomorrow|last week|last month|this morning|this afternoon|this evening|tonight|recently|lately|currently|right now)\b/gi, ' ')
    .replace(/\b(this|that|these|those|here|there|thing|stuff)\b/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeQueryList(queries = [], max = 7) {
  const out = [];
  for (const q of Array.isArray(queries) ? queries : []) {
    const cleaned = sanitizeQueryForEmbedding(q);
    if (!cleaned || cleaned.split(/\s+/).length < 2) continue;
    uniquePush(out, cleaned, max);
  }
  return out.slice(0, max);
}

function stripQuestionFormatting(text) {
  return safeText(text)
    .replace(/^[\s]*(what(?:'s| is)?|how(?:'s| is)?|show me|tell me|give me|find)\b[:\s]*/i, '')
    .replace(/[?]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function inferApps(text) {
  const lower = safeText(text).toLowerCase();
  const apps = [];
  if (/\bgmail|email|inbox|thread\b/.test(lower)) apps.push('Gmail');
  if (/\bmessages|imessage|sms|text message|pap\b/.test(lower)) apps.push('Messages');
  if (/\bslack\b/.test(lower)) apps.push('Slack');
  if (/\bchrome|browser|extension\b/.test(lower)) apps.push('Chrome');
  if (/\bgithub|pull request|pr\b/.test(lower)) apps.push('GitHub');
  if (/\bdocs|document|doc|slides|deck\b/.test(lower)) apps.push('Google Docs');
  if (/\bcalendar|meeting|agenda\b/.test(lower)) apps.push('Calendar');
  if (/\bnotion\b/.test(lower)) apps.push('Notion');
  if (/\bcursor|vscode|code editor\b/.test(lower)) apps.push('Cursor');
  return apps.slice(0, 3);
}

function inferSurfaceFamilies(text, candidateType = '', appScope = [], sourceScope = []) {
  const lower = `${safeText(text)} ${safeText(candidateType)} ${(Array.isArray(appScope) ? appScope.join(' ') : safeText(appScope))} ${(Array.isArray(sourceScope) ? sourceScope.join(' ') : safeText(sourceScope))}`.toLowerCase();
  const families = [];
  if (/\b(email|gmail|thread|reply|message|inbox|subject|sender|from:|to:)\b/.test(lower) || (Array.isArray(sourceScope) && sourceScope.includes('communication'))) {
    families.push('communication');
  }
  if (/\b(calendar|meeting|agenda|attendees|invite|event)\b/.test(lower) || (Array.isArray(sourceScope) && sourceScope.includes('calendar'))) {
    families.push('calendar');
  }
  if (/\b(cursor|code|commit|pr|pull request|diff|tsx|ts|js|json|manifest|schema|function|handler|error|exception|stack trace|extension)\b/.test(lower)) {
    families.push('coding');
  }
  if (/\b(chrome|safari|browser|dashboard|search|landing page|metrics|headline|website|page|waitlist|signup|sign up|form|ui)\b/.test(lower) || (Array.isArray(sourceScope) && sourceScope.includes('desktop'))) {
    families.push('browser');
  }
  return safeUnique(families, 3);
}

function inferActionStateTerms(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  const stateTerms = [];
  if (/\bstatus|progress|update|stand\b/.test(lower)) stateTerms.push('status');
  if (/\bcount|entries|signups|total|number\b/.test(lower)) stateTerms.push('count');
  if (/\berror|bug|issue|failing|failure\b/.test(lower)) stateTerms.push('error');
  if (/\breply|follow up|follow-up|thread\b/.test(lower)) stateTerms.push('follow up');
  if (/\bmeeting|agenda|prep\b/.test(lower)) stateTerms.push('meeting');
  if (/\bdraft|document|proposal|brief\b/.test(lower)) stateTerms.push('draft');
  if (/\bfix|implementation|handler|database|schema\b/.test(lower)) stateTerms.push('implementation');
  if (!stateTerms.length) {
    stateTerms.push(...normalizeTerms(stripEmbeddingWeakTerms(stripQuestionFormatting(text))).slice(1, 3));
  }
  return safeUnique(stateTerms, 4);
}

function inferConceptualTerms(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  const hints = [];
  if (/\bwaitlist|signup|sign up|form\b/.test(lower)) hints.push('user signups', 'conversion funnel', 'landing page');
  if (/\brelationship|contact|follow up|follow-up|person\b/.test(lower)) hints.push('relationship health', 'next best action', 'recent interactions');
  if (/\bmeeting|calendar|event\b/.test(lower)) hints.push('meeting prep', 'attendee context', 'calendar plan');
  if (/\berror|bug|issue|extension\b/.test(lower)) hints.push('runtime failure', 'implementation context', 'debugging flow');
  if (/\bproposal|doc|deck|report\b/.test(lower)) hints.push('document review', 'comments', 'share decision');
  return safeUnique(hints, 4);
}

function inferOutcomeTerms(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  const hints = [];
  if (/\bwaitlist|signup|form\b/.test(lower)) hints.push('total entries count', 'submission status', 'database update');
  if (/\bemail|reply|thread\b/.test(lower)) hints.push('reply needed', 'latest thread summary', 'subject update');
  if (/\bmeeting|calendar|agenda\b/.test(lower)) hints.push('upcoming event details', 'prep status', 'attendee list');
  if (/\berror|bug|issue|extension\b/.test(lower)) hints.push('fix status', 'error message', 'failing flow');
  if (/\btask|decision|project\b/.test(lower)) hints.push('next concrete step', 'follow through status', 'open loop');
  return safeUnique(hints, 4);
}

function buildSurfaceSyntaxQueries({ entity, stateTerms = [], technicalHints = [], exactTokens = [], surfaceFamilies = [], candidateType = '', max = 2 }) {
  const queries = [];
  const state = stateTerms[0] || '';
  const exact = exactTokens[0] || '';
  const technical = technicalHints[0] || '';
  const family = surfaceFamilies[0] || '';

  if (family === 'communication') {
    uniquePush(queries, `Subject: ${entity || exact || state} ${state}`.trim(), max);
    uniquePush(queries, `From: ${entity || exact} ${state || 'follow up'}`.trim(), max);
    uniquePush(queries, `Thread: ${entity || state} ${candidateType || 'email'}`.trim(), max);
  } else if (family === 'coding') {
    uniquePush(queries, `Commit: ${entity || state} ${technical || exact}`.trim(), max);
    uniquePush(queries, `${exact || technical} ${entity || state} handler`.trim(), max);
    uniquePush(queries, `PR: ${entity || state} ${technical || 'fix'}`.trim(), max);
  } else if (family === 'calendar') {
    uniquePush(queries, `Attendee: ${entity || state} agenda`.trim(), max);
    uniquePush(queries, `Event: ${entity || state} meeting`.trim(), max);
  } else if (family === 'browser') {
    uniquePush(queries, `${entity || state} dashboard ${state || 'status'}`.trim(), max);
    uniquePush(queries, `${entity || state} landing page ${technical || 'metrics'}`.trim(), max);
  }

  return queries.filter(Boolean).slice(0, max);
}

function buildStructuralQueries({ entity, exactTokens = [], technicalHints = [], candidateType = '', max = 2 }) {
  const queries = [];
  const exact = exactTokens.find((item) => /\.[a-z0-9]{2,5}\b|\/|@|https?:/i.test(String(item || ''))) || exactTokens[0] || '';
  const technical = technicalHints[0] || '';
  const base = entity || candidateType || '';
  uniquePush(queries, [exact, technical, base].filter(Boolean).join(' ').trim(), max);
  uniquePush(queries, [candidateType, exact, 'implementation'].filter(Boolean).join(' ').trim(), max);
  return queries.filter(Boolean).slice(0, max);
}

function buildMessageQueriesFromBundle(bundle, { max = 5 } = {}) {
  const queries = [];
  const surface = bundle?.surface;
  const literal = bundle?.literal;
  const outcome = bundle?.outcome;
  if (surface && /^(Subject:|From:|Thread:)/i.test(surface)) uniquePush(queries, surface, max);
  if (literal) uniquePush(queries, `${literal} email thread`, max);
  if (outcome) uniquePush(queries, `${outcome} reply needed`, max);
  if (bundle?.entity) uniquePush(queries, `From: ${bundle.entity} recent thread`, max);
  if (bundle?.conceptual) uniquePush(queries, `${bundle.conceptual} inbox follow up`, max);
  return queries.filter(Boolean).slice(0, max);
}

function buildMultiAngleQueryBundle(baseText, {
  max = 7,
  candidateType = '',
  appScope = [],
  sourceScope = [],
  mode = 'chat',
  deepScan = false
} = {}) {
  const strippedNoiseTerms = extractNoiseTerms(baseText);
  const cleaned = stripEmbeddingWeakTerms(stripQuestionFormatting(baseText));
  const cleanedExact = safeText(cleaned).replace(/\s+/g, ' ').trim();
  const terms = normalizeTerms(cleaned);
  const namedEntities = extractNamedEntities(baseText);
  const exactTokens = extractExactTokens(baseText);
  const apps = inferApps(baseText);
  const technicalHints = safeUnique([
    ...inferTechnicalHints(baseText),
    ...inferCoOccurringTerms(baseText, candidateType).filter((item) => /\.[a-z]{2,4}\b|handler|schema|database|library|stack trace|error|exception|function|manifest/i.test(item))
  ], 5);
  const surfaceFamilies = inferSurfaceFamilies(baseText, candidateType, appScope, sourceScope);
  const actionStateTerms = inferActionStateTerms(baseText, candidateType);
  const conceptualTerms = inferConceptualTerms(baseText, candidateType);
  const outcomeTerms = inferOutcomeTerms(baseText, candidateType);
  const inverse = inferInverseFrame(baseText);
  const entity = namedEntities[0] || terms[0] || '';
  const coreState = actionStateTerms[0] || terms[1] || '';
  const contextTerm = exactTokens[0] || technicalHints[0] || apps[0] || candidateType || terms[2] || '';

  const bundle = {
    literal: '',
    conceptual: '',
    technical: '',
    surface: '',
    structural: '',
    outcome: '',
    deep: '',
    entity: entity || '',
    extra: []
  };

  bundle.literal = [entity, coreState, contextTerm].filter(Boolean).join(' ').trim() || cleanedExact;
  bundle.conceptual = safeUnique([entity, ...conceptualTerms, coreState], 4).join(' ').trim() || bundle.literal;
  bundle.technical = safeUnique([entity, coreState, ...technicalHints], 5).join(' ').trim() || '';
  const surfaceCandidates = buildSurfaceSyntaxQueries({
    entity,
    stateTerms: actionStateTerms,
    technicalHints,
    exactTokens,
    surfaceFamilies,
    candidateType
  });
  bundle.surface = surfaceCandidates[0] || [entity, contextTerm || 'dashboard', coreState || 'status'].filter(Boolean).join(' ').trim();
  const structuralCandidates = buildStructuralQueries({
    entity,
    exactTokens,
    technicalHints,
    candidateType
  });
  bundle.structural = structuralCandidates[0] || [exactTokens[0] || '', candidateType || '', 'structure'].filter(Boolean).join(' ').trim();
  bundle.outcome = safeUnique([entity, ...outcomeTerms], 4).join(' ').trim() || '';
  
  if (deepScan) {
    bundle.deep = safeUnique([entity, 'relationship', 'patterns', 'habits', coreState], 4).join(' ').trim();
  }

  const extras = [];
  if (entity && coreState) uniquePush(extras, `${entity} ${coreState}`, 3);
  if (exactTokens[0] && bundle.literal) uniquePush(extras, `${exactTokens[0]} ${bundle.literal}`, 3);
  if (mode === 'suggestion' && candidateType) uniquePush(extras, `${candidateType} ${bundle.outcome || bundle.literal}`.trim(), 3);
  bundle.extra = extras.slice(0, 2);

  const semanticQueries = [];
  const lensQueries = [bundle.literal, bundle.technical, bundle.conceptual, bundle.surface, bundle.structural];
  if (bundle.deep) lensQueries.push(bundle.deep);
  
  // 5-lens default, then fill with precise extras up to max.
  lensQueries.forEach((query) => uniquePush(semanticQueries, query, max));
  if (cleanedExact.length >= 4) uniquePush(semanticQueries, cleanedExact, max);
  for (const phrase of extractQuotedPhrases(baseText)) uniquePush(semanticQueries, phrase, max);
  [bundle.outcome, ...bundle.extra].forEach((query) => uniquePush(semanticQueries, query, max));
  if (inverse) uniquePush(semanticQueries, inverse, max);
  if (apps[0] && bundle.surface) uniquePush(semanticQueries, `${apps[0]} ${bundle.surface}`, max);
  if (entity && bundle.structural) uniquePush(semanticQueries, `${entity} ${bundle.structural}`, max);

  const lexicalTerms = safeUnique([
    ...normalizeTerms(cleanedExact).slice(0, 10),
    ...extractQuotedPhrases(baseText).map((item) => item.toLowerCase()),
    ...exactTokens,
    ...namedEntities.map((item) => item.toLowerCase()),
    ...surfaceFamilies.flatMap((family) => {
      if (family === 'communication') return ['subject:', 'from:', 'thread:'];
      if (family === 'coding') return ['commit:', 'pr:'];
      return [];
    })
  ], 18).filter((term) => isUsefulLexicalTerm(term)).slice(0, 18);

  return {
    query_bundle: bundle,
    semantic_queries: semanticQueries.slice(0, max),
    message_queries: buildMessageQueriesFromBundle(bundle, { max: 5 }),
    lexical_terms: lexicalTerms,
    debug: {
      inferred_entities: namedEntities,
      inferred_surfaces: surfaceFamilies,
      inferred_technical_hints: technicalHints,
      stripped_noise_terms: strippedNoiseTerms
    }
  };
}

function inferTechnicalHints(text) {
  const lower = safeText(text).toLowerCase();
  const hints = [];
  if (/\bextension|manifest|background\.js|service worker|native messaging\b/.test(lower)) {
    hints.push('manifest.json', 'background script', 'native messaging');
  }
  if (/\bbug|error|exception|fail|issue|crash\b/.test(lower)) {
    hints.push('stack trace', 'runtime error', 'debugging');
  }
  if (/\bemail|thread|reply\b/.test(lower)) {
    hints.push('subject line', 'reply needed', 'email thread');
  }
  if (/\bmeeting|calendar|event\b/.test(lower)) {
    hints.push('agenda', 'attendees', 'calendar event');
  }
  if (/\bproposal|doc|draft|slide|deck|report\b/.test(lower)) {
    hints.push('draft', 'comments', 'document');
  }
  return Array.from(new Set(hints)).slice(0, 4);
}

function inferCoOccurringTerms(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  if (/\bextension|chrome\b/.test(lower)) return ['manifest.json', 'background.js', 'content script', 'permissions'];
  if (/\bemail|thread|reply|inbox\b/.test(lower)) return ['follow up', 'subject', 'sender', 'unread'];
  if (/\bmeeting|calendar|event\b/.test(lower)) return ['agenda', 'notes', 'attendees', 'prep'];
  if (/\bbug|error|issue\b/.test(lower)) return ['error message', 'logs', 'stack trace', 'failing flow'];
  if (/\bproposal|draft|doc|deck\b/.test(lower)) return ['document', 'comments', 'review', 'share'];
  return [];
}

function inferInverseFrame(text) {
  const lower = safeText(text).toLowerCase();
  if (/\bbug\b/.test(lower)) return 'runtime error stack trace debugging';
  if (/\bsolution|fix\b/.test(lower)) return 'problem blocker failing flow';
  if (/\bwhat happened|what was i doing|what did i do\b/.test(lower)) return 'recent activity current context';
  if (/\bwaitlist\b/.test(lower)) return 'signup page landing page email capture';
  return '';
}

function inferIntent(text, mode, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  if (/\bwhat was i doing|what happened|20 minutes ago|30 minutes ago|an hour ago\b/.test(lower)) {
    return 'recall recent activity from time-anchored context';
  }
  if (/\bemail|reply|thread|message|inbox\b/.test(lower)) return 'recover communication context and next action';
  if (/\bmeeting|calendar|agenda\b/.test(lower)) return 'recover meeting context and preparation needs';
  if (/\bbug|error|extension|manifest|background\.js\b/.test(lower)) return 'recover implementation context and technical evidence';
  if (mode === 'suggestion') return 'infer the next concrete action from recent evidence';
  return 'recover the most relevant context across recent work';
}

function inferEntryMode(text, intent = '', mode = 'chat') {
  const lower = `${safeText(text)} ${safeText(intent)}`.toLowerCase();
  if (mode === 'suggestion') return 'core_first';
  if (/\b(find|search|lookup|show me|where is|locate|exact|verbatim|quote)\b/.test(lower)) return 'query_first';
  if (/\bwhat do you know|about me|my habits|pattern|preference|long-term|relationship with|study habits|global context|core\b/.test(lower)) return 'core_first';
  if (/\bwhy|how does this connect|across|over time|trend|theme|recurring|multi-hop\b/.test(lower)) return 'hybrid';
  return 'hybrid';
}

function inferPreferredSourceTypes(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  if (/\bemail|gmail|thread|reply|message|inbox\b/.test(lower)) return ['communication'];
  if (/\bmeeting|calendar|agenda|attendees|event\b/.test(lower)) return ['calendar'];
  if (/\bwhat was i doing|what happened|extension|chrome|cursor|screen|desktop\b/.test(lower)) return ['desktop', 'communication', 'calendar'];
  return [];
}

function inferHardSourceTypes(text, candidateType = '') {
  const lower = `${safeText(text)} ${safeText(candidateType)}`.toLowerCase();
  if (/\bemail|gmail|thread|reply|subject|sender|from:|to:\b/.test(lower)) return ['communication'];
  if (/\bmeeting|calendar|agenda|attendees|invite|event\b/.test(lower)) return ['calendar'];
  if (/\bbrowser history|visited|website|page|url|domain\b/.test(lower)) return ['desktop'];
  return null;
}

function inferSummaryVsRaw(text) {
  const lower = safeText(text).toLowerCase();
  if (/\b(exact|verbatim|quote|quoted|precise|wording|show me the email|exact email|exact message|what did .* say)\b/.test(lower)) {
    return 'raw';
  }
  if (/\b(status|progress|update|where do things stand|what's the status|how is .* going|what did i work on|summary)\b/.test(lower)) {
    return 'summary';
  }
  if (/\bemail|thread|meeting|calendar|agenda|waitlist|bug|error|extension\b/.test(lower)) {
    return 'summary';
  }
  return 'summary';
}

function shouldDefaultRecentWindow(text, mode, normalizedDateRange) {
  if (mode !== 'chat' || normalizedDateRange) return false;
  const lower = safeText(text).toLowerCase();
  if (/\b(exact|verbatim|quote|quoted|precise|wording)\b/.test(lower)) return false;
  return /\b(status|progress|update|where do things stand|how is .* going|what did i work on|what's the status|waitlist|project|task|work)\b/.test(lower);
}

function buildDefaultRecentWindow(now = new Date()) {
  const start = startOfDay(new Date(now.getFullYear(), now.getMonth(), now.getDate() - 6));
  return buildTemporalRange({
    label: 'default_last_7_days',
    start,
    end: endOfDay(now),
    granularity: 'range',
    source: 'default'
  });
}

function inferWebGate(query) {
  const lower = safeText(query).toLowerCase();
  if (!lower.trim()) return { strategyMode: 'memory_only', webGateReason: 'No external or current signal detected.' };
  if (/\b(web|internet|online|search the web|look up|google|website|site|homepage|news|latest|current|today|public)\b/.test(lower)) {
    return { strategyMode: 'memory_then_web', webGateReason: 'The question explicitly asks for public or current web information if memory is insufficient.' };
  }
  if (/\b(reuters|bbc|nytimes|techcrunch|the verge|wikipedia|linkedin|github)\b/.test(lower)) {
    return { strategyMode: 'memory_then_web', webGateReason: 'The question references an external public source, so web corroboration may be needed after memory retrieval.' };
  }
  if (/\bprice|stock|funding|launch|release|announcement|article\b/.test(lower)) {
    return { strategyMode: 'memory_then_web', webGateReason: 'The question appears current or public-facing, so web corroboration may be needed after memory retrieval.' };
  }
  return { strategyMode: 'memory_only', webGateReason: 'Memory retrieval should be sufficient unless the internal evidence is weak.' };
}

function shouldUseQuerylessMode(text, dateRange) {
  // Enforce query-driven retrieval for all memory access paths.
  // We keep temporal filters, but we always generate explicit queries.
  return false;
}

function fallbackSemanticQueries(text, candidateType = '', max = 7) {
  const cleaned = stripEmbeddingWeakTerms(stripQuestionFormatting(text));
  const terms = normalizeTerms(cleaned);
  const entity = extractNamedEntities(text)[0] || '';
  const technical = inferTechnicalHints(text)[0] || '';
  const base = entity || terms[0] || candidateType || 'recent activity';
  const action = terms[1] || 'status';
  const queries = [];
  uniquePush(queries, `${base} ${action}`.trim(), max);
  uniquePush(queries, `${base} next concrete step`, max);
  uniquePush(queries, `${base} recent context`, max);
  if (technical) uniquePush(queries, `${base} ${technical}`, max);
  uniquePush(queries, `${base} open loop`, max);
  return queries.slice(0, max);
}

function buildSearchQueries(baseText, { max = 7, candidateType = '' } = {}) {
  return buildMultiAngleQueryBundle(baseText, { max, candidateType }).semantic_queries;
}

function buildMessageQueries(baseText, { max = 5, candidateType = '' } = {}) {
  const lower = safeText(baseText).toLowerCase();
  const terms = normalizeTerms(stripEmbeddingWeakTerms(baseText));
  const queries = [];
  const looksMessageLike = /\b(email|gmail|message|thread|reply|follow up|inbox|slack|chat)\b/.test(lower) || /email/i.test(candidateType);

  if (!looksMessageLike) return [];

  uniquePush(queries, `${terms.slice(0, 5).join(' ')} email thread`, max);
  uniquePush(queries, `${terms.slice(0, 4).join(' ')} reply needed`, max);
  uniquePush(queries, `${terms.slice(0, 4).join(' ')} subject sender`, max);
  uniquePush(queries, `${terms.slice(0, 4).join(' ')} inbox follow up`, max);
  uniquePush(queries, `${candidateType} ${terms.slice(0, 4).join(' ')}`, max);

  return queries.filter(Boolean).slice(0, max);
}

function buildRetrievalThought({
  query = '',
  mode = 'chat',
  candidate = null,
  dateRange = null,
  app = null
} = {}) {
  const candidateType = safeText(candidate?.type || '');
  const candidateText = [
    safeText(candidate?.data || ''),
    safeText(candidate?.retrieval_intent || ''),
    safeText(candidate?.trigger_summary || ''),
    safeText(candidate?.title || candidate?.id || '')
  ].filter(Boolean).join(' ');
  const mergedText = [safeText(query), candidateText].filter(Boolean).join(' ');
  const inferredTemporalWindow = inferTemporalWindow(query || mergedText);
  const summaryVsRaw = inferSummaryVsRaw(query || mergedText);
  const defaultRecentWindow = shouldDefaultRecentWindow(query || mergedText, mode, normalizeDateRange(dateRange) || inferredTemporalWindow)
    ? buildDefaultRecentWindow()
    : null;
  const normalizedDateRange = normalizeDateRange(dateRange) || inferredTemporalWindow || defaultRecentWindow;
  const apps = []
    .concat(Array.isArray(app) ? app : (app ? [app] : []))
    .concat(inferApps(mergedText))
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .slice(0, 3);

  const queryless = shouldUseQuerylessMode(query || mergedText, normalizedDateRange);
  const webGate = inferWebGate(query || mergedText);
  const preferredSourceTypes = inferPreferredSourceTypes(mergedText || query, candidateType);
  const hardSourceTypes = inferHardSourceTypes(mergedText || query, candidateType);
  
  const requiresDeepContext = /\b(relationship|pattern|habit|preference|long-term|study habits|multi-hop|recurring|theme|trend|habitual|regularly|typical)\b/i.test(mergedText || query);

  const structuredQueries = buildMultiAngleQueryBundle(mergedText || query, {
    max: 7,
    candidateType,
    appScope: apps,
    sourceScope: hardSourceTypes || preferredSourceTypes || [],
    mode,
    deepScan: requiresDeepContext
  });
  let semanticQueries = Array.isArray(structuredQueries?.semantic_queries) && structuredQueries.semantic_queries.length
    ? structuredQueries.semantic_queries
    : fallbackSemanticQueries(mergedText || query, candidateType, 7);
  semanticQueries = sanitizeQueryList(semanticQueries, 7);
  if (semanticQueries.length < 5) {
    const filler = sanitizeQueryList(fallbackSemanticQueries(mergedText || query, candidateType, 7), 7);
    for (const q of filler) uniquePush(semanticQueries, q, 7);
  }
  const messageQueries = sanitizeQueryList(Array.isArray(structuredQueries?.message_queries) ? structuredQueries.message_queries : [], 5);
  const intent = inferIntent(query || mergedText, mode, candidateType);
  const entryMode = inferEntryMode(query || mergedText, intent, mode);
  const alpha = (summaryVsRaw === 'raw' || entryMode === 'query_first') ? 0.45 : 0.7;
  const reasoning = [];
  const lexicalTerms = ((structuredQueries && structuredQueries.lexical_terms) ? structuredQueries.lexical_terms : []).filter((term) => isUsefulLexicalTerm(term));
  if (!lexicalTerms.length) {
    lexicalTerms.push(
      ...Array.from(new Set([
        ...normalizeTerms(stripEmbeddingWeakTerms(mergedText || query)).slice(0, 10),
        ...extractExactTokens(mergedText || query),
        ...extractQuotedPhrases(mergedText || query).map((item) => item.toLowerCase()),
        ...extractNamedEntities(mergedText || query).map((item) => item.toLowerCase())
      ])).filter((term) => isUsefulLexicalTerm(term)).slice(0, 16)
    );
  }
  const temporalReasoning = [];
  const strategyMode = webGate.strategyMode;
  const filters = {
    app: apps.length ? apps : null,
    date_range: normalizedDateRange,
    source_types: hardSourceTypes
  };

  reasoning.push(`Intent: ${intent}.`);
  reasoning.push(`Entry mode: ${entryMode}.`);
  reasoning.push('Mode: semantic multi-query retrieval with literal, conceptual, technical, surface, and outcome angles.');
  reasoning.push(`Summary mode: ${summaryVsRaw === 'raw' ? 'raw evidence retrieval' : 'bounded summary retrieval'}.`);
  if (apps.length) reasoning.push(`App hints: ${apps.join(', ')}.`);
  if (hardSourceTypes?.length) reasoning.push(`Hard source scope: ${hardSourceTypes.join(', ')}.`);
  if (normalizedDateRange) reasoning.push(`Date filter: ${normalizedDateRange.start} -> ${normalizedDateRange.end}.`);
  if (inferredTemporalWindow && !dateRange) {
    temporalReasoning.push(`Inferred temporal window from user phrasing: ${inferredTemporalWindow.label}.`);
  } else if (defaultRecentWindow) {
    temporalReasoning.push('Applied the default 7-day window for a status/progress style question.');
  } else if (normalizedDateRange) {
    temporalReasoning.push('Using explicit temporal filter supplied by caller.');
  } else {
    temporalReasoning.push('No temporal window inferred from the user phrasing.');
  }
  reasoning.push('Embedding rule: relative time and vague deixis are handled by filters, not semantic queries.');
  reasoning.push(`Web gate: ${webGate.webGateReason}`);

  return {
    mode: 'semantic',
    strategy_mode: strategyMode,
    entry_mode: entryMode,
    alpha,
    summary_vs_raw: summaryVsRaw,
    time_scope: {
      label: normalizedDateRange?.label || inferredTemporalWindow?.label || defaultRecentWindow?.label || 'all_time',
      source: normalizedDateRange?.source || (inferredTemporalWindow ? 'inferred' : (defaultRecentWindow ? 'default' : 'none')),
      range: normalizedDateRange
    },
    app_scope: apps,
    source_scope: hardSourceTypes || preferredSourceTypes || [],
    web_gate_reason: webGate.webGateReason,
    intent,
    semantic_queries: semanticQueries,
    message_queries: messageQueries,
    lexical_terms: lexicalTerms,
    query_bundle: structuredQueries?.query_bundle || null,
    query_debug: structuredQueries?.debug || {
      inferred_entities: extractNamedEntities(mergedText || query),
      inferred_surfaces: inferSurfaceFamilies(mergedText || query, candidateType, apps, hardSourceTypes || preferredSourceTypes || []),
      inferred_technical_hints: inferTechnicalHints(mergedText || query),
      stripped_noise_terms: extractNoiseTerms(mergedText || query)
    },
    preferred_source_types: preferredSourceTypes,
    filters,
    temporal_reasoning: temporalReasoning,
    initial_date_range: normalizedDateRange,
    applied_date_range: normalizedDateRange,
    widened_date_range: null,
    date_filter_status: normalizedDateRange ? 'applied' : 'not_used',
    fallback_policy: {
      mode: 'widen_once',
      attempted: false,
      widened: false
    },
    seed_limit: mode === 'suggestion' ? 4 : 5,
    hop_limit: 2,
    context_budget_tokens: mode === 'suggestion' ? 550 : 800,
    search_queries: semanticQueries,
    search_queries_messages: messageQueries,
    app_hints: apps,
    date_range: normalizedDateRange,
    reasoning
  };
}

function summarizeRetrievalThought(thought) {
  const plan = thought || {};
  const lines = [];
  lines.push(`Retrieval thought mode=${plan.mode || 'semantic'} strategy=${plan.strategy_mode || 'memory_only'} entry=${plan.entry_mode || 'hybrid'} intent=${plan.intent || 'unknown'}.`);
  lines.push(`Summary mode=${plan.summary_vs_raw || 'summary'}.`);
  if (Array.isArray(plan.semantic_queries || plan.search_queries) && (plan.semantic_queries || plan.search_queries).length) {
    lines.push(`Screen queries: ${(plan.semantic_queries || plan.search_queries).join(' | ')}.`);
  }
  if (Array.isArray(plan.message_queries || plan.search_queries_messages) && (plan.message_queries || plan.search_queries_messages).length) {
    lines.push(`Message queries: ${(plan.message_queries || plan.search_queries_messages).join(' | ')}.`);
  }
  return lines.concat(Array.isArray(plan.reasoning) ? plan.reasoning : []);
}

function isMessageLikeRow(row) {
  const metadata = row && typeof row.metadata === 'string'
    ? (() => {
        try {
          return JSON.parse(row.metadata);
        } catch (_) {
          return {};
        }
      })()
    : (row?.metadata || {});
  const hay = `${safeText(row?.app)} ${safeText(row?.text)} ${safeText(metadata.event_type)} ${safeText(metadata.source)}`.toLowerCase();
  return /\bgmail|email|thread|reply|inbox|message|chat|slack\b/.test(hay);
}

function buildSpeculativePrefetchPlan() {
  const now = new Date();
  const start = new Date(now);
  start.setHours(start.getHours() - 12);
  const normalizedDateRange = {
    start: start.toISOString(),
    end: now.toISOString(),
    granularity: 'range',
    source: 'prefetch'
  };

  return {
    mode: 'prefetch',
    strategy_mode: 'memory_only',
    entry_mode: 'core_first',
    summary_vs_raw: 'summary',
    time_scope: { label: 'recent_12_hours', range: normalizedDateRange },
    app_scope: null,
    source_scope: null,
    web_gate_reason: 'prefetch block',
    intent: 'Speculative prefetch of recent context to prime cache',
    semantic_queries: ['recent open loops', 'ongoing task status', 'unanswered communications'],
    message_queries: [],
    lexical_terms: ['todo', 'draft', 'review'],
    query_bundle: null,
    query_debug: null,
    preferred_source_types: [],
    filters: { date_range: normalizedDateRange },
    temporal_reasoning: ['Prefetching recent context'],
    initial_date_range: normalizedDateRange,
    applied_date_range: normalizedDateRange,
    widened_date_range: null,
    date_filter_status: 'applied',
    fallback_policy: { mode: 'no_fallback', attempted: false, widened: false },
    seed_limit: 6,
    hop_limit: 2,
    context_budget_tokens: 600,
    search_queries: ['recent open loops', 'ongoing task status', 'unanswered communications'],
    search_queries_messages: [],
    app_hints: [],
    date_range: normalizedDateRange,
    reasoning: []
  };
}

module.exports = {
  buildRetrievalThought,
  summarizeRetrievalThought,
  inferTemporalWindow,
  widenTemporalWindow,
  buildSearchQueries,
  buildMessageQueries,
  buildMultiAngleQueryBundle,
  isMessageLikeRow,
  buildSpeculativePrefetchPlan
};
