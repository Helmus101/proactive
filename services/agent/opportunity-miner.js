const db = require('../db');

// Lazy require to avoid circular dependencies
function contactDetector() {
  // eslint-disable-next-line global-require
  return require('./contact-detector');
}

function asObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function parseList(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function parseTs(value) {
  if (!value && value !== 0) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : 0;
}

function trim(text, max = 220) {
  return String(text || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normalizeTarget(text = '') {
  return String(text || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function sourceRefsFromRow(row) {
  return parseList(row?.source_refs);
}

const DEFAULT_SOCIAL_STRATEGY = {
  tiers: {
    inner_circle: { half_life_days: 7, priority: 1.0 },
    network: { half_life_days: 14, priority: 0.78 },
    leads: { half_life_days: 3, priority: 0.92 }
  },
  default_tier: 'network',
  temperature_threshold: 0.42,
  core_goal: 'maintain strong trusted relationships while creating useful opportunities'
};

function safeLower(value) {
  return String(value || '').toLowerCase();
}

function isLikelyCommunicationEvent(row = {}) {
  const type = safeLower(row.type);
  const app = safeLower(row.app);
  const source = safeLower(row.source);
  return /message|email|mail|calendar|meeting|slack|teams/.test(`${type} ${app} ${source}`);
}

function lexicalSentimentScore(text = '') {
  const t = safeLower(text);
  if (!t) return 0;
  const positive = ['thanks', 'great', 'awesome', 'glad', 'appreciate', 'excited', 'love', 'nice', 'helpful', 'happy'];
  const negative = ['urgent', 'blocked', 'delay', 'late', 'frustrated', 'disappointed', 'issue', 'problem', 'concern', 'cannot'];
  let score = 0;
  for (const token of positive) if (t.includes(token)) score += 1;
  for (const token of negative) if (t.includes(token)) score -= 1;
  return score;
}

function resolveTierForPerson(row = {}, strategy = DEFAULT_SOCIAL_STRATEGY) {
  const metadata = asObj(row.metadata);
  const explicit = safeLower(metadata.social_tier || metadata.relationship_tier || metadata.segment || metadata.group);
  if (explicit.includes('inner')) return 'inner_circle';
  if (explicit.includes('lead')) return 'leads';
  if (explicit.includes('network')) return 'network';

  const confidence = Number(row.confidence || metadata.importance || 0.5);
  if (confidence >= 0.85) return 'inner_circle';
  if (confidence <= 0.45) return 'leads';
  return strategy.default_tier || 'network';
}

function computeSocialTemperature(daysSince, halfLifeDays = 7, priorityWeight = 1) {
  const d = Math.max(0, Number(daysSince || 0));
  const half = Math.max(1, Number(halfLifeDays || 7));
  const base = Math.exp((-Math.log(2) * d) / half);
  return Math.max(0, Math.min(1, base * Math.max(0.5, Number(priorityWeight || 1))));
}

async function loadSocialStrategy(options = {}) {
  if (options && typeof options.social_strategy === 'object' && options.social_strategy) {
    return {
      ...DEFAULT_SOCIAL_STRATEGY,
      ...options.social_strategy,
      tiers: {
        ...DEFAULT_SOCIAL_STRATEGY.tiers,
        ...(options.social_strategy.tiers || {})
      }
    };
  }

  const row = await db.getQuery(
    `SELECT value
     FROM kv_cache
     WHERE key IN ('core:social_strategy', 'social_strategy')
     ORDER BY CASE key WHEN 'core:social_strategy' THEN 0 ELSE 1 END
     LIMIT 1`
  ).catch(() => null);

  const parsed = asObj(row?.value);
  return {
    ...DEFAULT_SOCIAL_STRATEGY,
    ...(parsed || {}),
    tiers: {
      ...DEFAULT_SOCIAL_STRATEGY.tiers,
      ...((parsed && parsed.tiers) ? parsed.tiers : {})
    }
  };
}

function buildOutreachOptions({ name, hookText = '', strategy = DEFAULT_SOCIAL_STRATEGY, tier = 'network' } = {}) {
  const person = String(name || 'this person').trim();
  const safeHook = trim(hookText || 'a relevant update from your recent activity', 140);
  const coreGoal = trim(strategy?.core_goal || DEFAULT_SOCIAL_STRATEGY.core_goal, 120);
  const tierLabel = tier === 'inner_circle' ? 'inner circle' : (tier === 'leads' ? 'lead' : 'network');

  return [
    {
      type: 'low_friction',
      label: `Send quick check-in to ${person}`,
      draft: `Hey ${person}, thought of you today after seeing something that reminded me of our recent thread. Hope your week is going well.`
    },
    {
      type: 'value_add',
      label: `Share relevant resource with ${person}`,
      draft: `Saw this and thought of your interest in ${safeHook}. Want me to send the link over?`
    },
    {
      type: 'the_ask',
      label: `Send focused ask to ${person}`,
      draft: `${person}, quick ask aligned with my current goal (${coreGoal}): could we do a short check-in this week to align on next steps?`
    }
  ].map((item) => ({ ...item, social_tier: tierLabel }));
}

function sentimentForPerson(peopleName = '', recentEvents = []) {
  const first = safeLower(String(peopleName || '').split(' ')[0]);
  const events = (recentEvents || [])
    .filter((row) => isLikelyCommunicationEvent(row))
    .filter((row) => {
      if (!first || first.length < 3) return true;
      const hay = `${row.text || ''} ${row.window_title || ''} ${asObj(row.metadata).activity_summary || ''}`.toLowerCase();
      return hay.includes(first);
    })
    .slice(0, 10);

  if (!events.length) {
    return { trend: 'neutral', score: 0, samples: 0 };
  }

  const scores = events.map((row) => lexicalSentimentScore(`${row.text || ''} ${row.window_title || ''} ${asObj(row.metadata).activity_summary || ''}`));
  const avg = scores.reduce((acc, n) => acc + n, 0) / Math.max(1, scores.length);
  const firstHalf = scores.slice(0, Math.ceil(scores.length / 2));
  const secondHalf = scores.slice(Math.ceil(scores.length / 2));
  const firstAvg = firstHalf.reduce((acc, n) => acc + n, 0) / Math.max(1, firstHalf.length);
  const secondAvg = secondHalf.reduce((acc, n) => acc + n, 0) / Math.max(1, secondHalf.length);
  const gradient = secondAvg - firstAvg;

  if (avg <= -0.8 || gradient <= -0.7) return { trend: 'negative', score: Number(avg.toFixed(2)), samples: scores.length };
  if (avg >= 0.8 || gradient >= 0.7) return { trend: 'positive', score: Number(avg.toFixed(2)), samples: scores.length };
  return { trend: 'neutral', score: Number(avg.toFixed(2)), samples: scores.length };
}

async function detectSocialHeatmapNudges(semanticRows, recentEvents, now, strategy) {
  const out = [];
  const threshold = Number(strategy?.temperature_threshold || DEFAULT_SOCIAL_STRATEGY.temperature_threshold);
  for (const row of semanticRows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const lastTs = parseTs(metadata.latest_interaction_at || row.updated_at || row.created_at);
    if (!lastTs) continue;

    const days = Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000));
    const tier = resolveTierForPerson(row, strategy);
    const tierCfg = strategy?.tiers?.[tier] || DEFAULT_SOCIAL_STRATEGY.tiers[tier] || DEFAULT_SOCIAL_STRATEGY.tiers.network;
    const temp = computeSocialTemperature(days, tierCfg.half_life_days, tierCfg.priority);
    if (temp > threshold) continue;

    const contactName = trim(row.title || 'Contact', 80);
    const sentiment = sentimentForPerson(contactName, recentEvents);
    const riskBoost = sentiment.trend === 'negative' ? 0.08 : 0;
    const hookText = (parseList(metadata.topics || []).slice(0, 1)[0]) || (parseList(metadata.interests || []).slice(0, 1)[0]) || 'recent topic you both touched';
    const outreachOptions = buildOutreachOptions({ name: contactName, hookText, strategy, tier });

    out.push({
      ...candidateBase({
        opportunityType: 'social_decay_nudge',
        seedNodeId: row.id,
        title: `Relationship cooling: ${contactName}`,
        triggerSummary: `${contactName} is below social temperature threshold (${temp.toFixed(2)}). Last interaction ${Math.round(days)} days ago.`,
        confidence: Math.min(0.97, 0.68 + (1 - temp) * 0.24 + riskBoost),
        reasonCodes: ['social_heatmap_decay', `tier_${tier}`, sentiment.trend === 'negative' ? 'sentiment_risk' : 'sentiment_stable'],
        timeAnchor: days >= 1 ? `last touch ${Math.round(days)}d ago` : 'today',
        candidateActions: [
          `Send check-in to ${contactName}`,
          `Share relevant update with ${contactName}`
        ],
        sourceRefs: sourceRefsFromRow(row),
        canonicalTarget: normalizeTarget(contactName),
        score: Math.min(0.99, 0.7 + (1 - temp) * 0.22 + riskBoost)
      }),
      social_tier: tier,
      social_temperature: Number(temp.toFixed(4)),
      sentiment_gradient: sentiment,
      outreach_options: outreachOptions
    });
  }
  return out;
}

async function detectContextualValueHooks(semanticRows, recentEvents, now, strategy) {
  const out = [];
  const events = Array.isArray(recentEvents) ? recentEvents.slice(0, 40) : [];
  for (const row of semanticRows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const name = trim(row.title || 'Contact', 80);
    const tier = resolveTierForPerson(row, strategy);
    const interests = [
      ...parseList(metadata.topics || []),
      ...parseList(metadata.interests || []),
      ...parseList(metadata.intent_clusters || [])
    ].map((x) => String(x || '').toLowerCase()).filter(Boolean).slice(0, 8);
    if (!interests.length) continue;

    let matched = null;
    let eventRef = null;
    for (const event of events) {
      const meta = asObj(event.metadata);
      const hay = `${event.window_title || ''} ${event.text || ''} ${meta.activity_summary || ''}`.toLowerCase();
      const hit = interests.find((interest) => interest.length >= 3 && hay.includes(interest));
      if (hit) {
        matched = hit;
        eventRef = event;
        break;
      }
    }
    if (!matched || !eventRef) continue;

    const sentiment = sentimentForPerson(name, recentEvents);
    const hook = trim(eventRef.window_title || eventRef.text || asObj(eventRef.metadata).activity_summary || matched, 140);
    const outreachOptions = buildOutreachOptions({ name, hookText: matched, strategy, tier });

    out.push({
      ...candidateBase({
        opportunityType: 'contextual_value_hook',
        seedNodeId: row.id,
        title: `Value hook for ${name}: share ${matched}`,
        triggerSummary: `You saw "${hook}" recently, and ${name} is linked to "${matched}" in memory.`,
        confidence: Math.min(0.96, 0.7 + (tier === 'inner_circle' ? 0.1 : 0.04) + (sentiment.trend === 'negative' ? 0.05 : 0)),
        reasonCodes: ['relational_value_hook', `tier_${tier}`, 'interest_match'],
        timeAnchor: 'today',
        candidateActions: [
          `Share this resource with ${name}`,
          `Send short note connecting it to ${matched}`
        ],
        sourceRefs: [eventRef.id, ...sourceRefsFromRow(row)].filter(Boolean),
        canonicalTarget: normalizeTarget(name),
        score: Math.min(0.98, 0.72 + (tier === 'inner_circle' ? 0.1 : 0.03))
      }),
      social_tier: tier,
      value_hook: {
        matched_interest: matched,
        source_event_id: eventRef.id,
        source_event_text: hook
      },
      outreach_options: outreachOptions
    });
  }
  return out;
}

async function fetchMemoryRows(layer, extraWhere = '', params = [], limit = 300) {
  return db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, created_at, updated_at
     FROM memory_nodes
     WHERE layer = ? ${extraWhere}
     LIMIT ?`,
    [layer, ...params, Math.max(20, Number(limit || 300))]
  ).catch(() => []);
}

async function fetchRecentEvents(now = Date.now(), limit = 100, windowHours = 24) {
  const sinceIso = new Date(now - Math.max(1, Number(windowHours || 24)) * 60 * 60 * 1000).toISOString();
  return db.allQuery(
    `SELECT id, type, source, app, window_title, text, timestamp, metadata
     FROM events
     WHERE datetime(timestamp) >= datetime(?)
     ORDER BY datetime(timestamp) DESC
     LIMIT ?`,
    [sinceIso, limit]
  ).catch(() => []);
}

function hasHighValueRecentSignal(events = []) {
  const rows = Array.isArray(events) ? events : [];
  for (const row of rows) {
    const type = String(row.type || '').toLowerCase();
    const app = String(row.app || '').toLowerCase();
    const text = `${row.window_title || ''} ${row.text || ''}`.toLowerCase();
    if (type.includes('calendar') && /(deadline|due|meeting|birthday|interview)/i.test(text)) return true;
    if ((type.includes('message') || type.includes('email') || app.includes('slack') || app.includes('mail')) && /(follow up|reply|urgent|asap|pending)/i.test(text)) return true;
    if (type.includes('task') && /(due|overdue|todo|blocked)/i.test(text)) return true;
  }
  return false;
}

function sampleRows(rows = [], limit = 120) {
  return (rows || [])
    .slice()
    .sort((a, b) => {
      const ta = parseTs(a.updated_at || a.created_at);
      const tb = parseTs(b.updated_at || b.created_at);
      if (tb !== ta) return tb - ta;
      return Number(b.confidence || 0) - Number(a.confidence || 0);
    })
    .slice(0, Math.max(10, Number(limit || 120)));
}

async function fetchSupportingEdges(nodeId, limit = 6) {
  if (!nodeId) return [];
  const rows = await db.allQuery(
    `SELECT from_node_id, to_node_id, edge_type, created_at
     FROM memory_edges
     WHERE from_node_id = ? OR to_node_id = ?
     ORDER BY datetime(created_at) DESC
     LIMIT ?`,
    [nodeId, nodeId, limit]
  ).catch(() => []);
  return (rows || []).map((row) => ({
    from: row.from_node_id,
    to: row.to_node_id,
    relation: row.edge_type,
    timestamp: row.created_at
  }));
}

function formatAnchorTime(ts) {
  if (!ts) return null;
  const d = new Date(ts);
  if (isNaN(d.getTime())) return null;
  const now = Date.now();
  const diffMs = now - d.getTime();
  const diffH = diffMs / (60 * 60 * 1000);
  const diffDays = diffMs / (24 * 60 * 60 * 1000);
  if (diffDays < 0.5) {
    // same day — use HH:MM
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
  if (diffDays < 1.5) return 'yesterday';
  if (diffDays < 7) return `${Math.round(diffDays)} days ago`;
  return null;
}

function candidateBase({ opportunityType, seedNodeId, title, triggerSummary, confidence, reasonCodes, timeAnchor, candidateActions, sourceRefs, canonicalTarget, score }) {
  return {
    id: `${opportunityType}:${seedNodeId}`,
    opportunity_type: opportunityType,
    seed_node_id: seedNodeId,
    title: trim(title, 120),
    trigger_summary: trim(triggerSummary, 220),
    confidence: Math.max(0, Math.min(1, Number(confidence || 0.6))),
    reason_codes: Array.from(new Set((reasonCodes || []).filter(Boolean))).slice(0, 6),
    time_anchor: trim(timeAnchor || 'now', 80),
    candidate_actions: Array.from(new Set((candidateActions || []).map((x) => trim(x, 80)).filter(Boolean))).slice(0, 3),
    supporting_node_ids: [seedNodeId],
    supporting_edge_paths: [],
    source_refs: Array.isArray(sourceRefs) ? sourceRefs.slice(0, 8) : [],
    canonical_target: canonicalTarget || normalizeTarget(title),
    score: Number(score || 0),
    retrieval_intent: `${trim(title, 90)} ${trim(triggerSummary, 120)} unresolved next action`
  };
}

async function detectUnresolvedFollowups(rows, now) {
  const out = [];
  for (const row of rows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const sourceGroup = String(metadata.source_type_group || '').toLowerCase();
    if (!['communication', 'calendar'].includes(sourceGroup)) continue;
    const text = `${row.summary || ''} ${row.canonical_text || ''} ${metadata.intent || ''}`.toLowerCase();
    if (!/\b(reply|follow up|pending|unanswered|open|needs response|action)\b/.test(text)) continue;
    const lastTs = parseTs(metadata.latest_interaction_at || row.updated_at || row.created_at);
    if (!lastTs) continue;
    const days = Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000));
    if (days < 2) continue;
    const contactName = (row.title || 'contact').trim();
    const lastSeenLabel = days < 1 ? 'today' : days < 2 ? 'yesterday' : `${Math.round(days)} days ago`;
    const anchorTime = formatAnchorTime(lastTs);
    const timeAnchorLabel = anchorTime ? `last seen ${anchorTime}` : lastSeenLabel;
    out.push(candidateBase({
      opportunityType: 'unresolved_followup',
      seedNodeId: row.id,
      title: `Reply to ${contactName} — thread open ${Math.round(days)}d`,
      triggerSummary: `${contactName} has an unanswered thread, last active ${lastSeenLabel}. No closure signal found.`,
      confidence: Math.min(0.95, 0.6 + Math.min(0.3, days / 10)),
      reasonCodes: ['open_thread', 'followup_needed', 'inactivity_window'],
      timeAnchor: timeAnchorLabel,
      candidateActions: [`Draft reply to ${contactName}`, 'Send short status check-in'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(contactName),
      score: Math.min(0.98, 0.65 + Math.min(0.25, days / 12))
    }));
  }
  return out;
}

async function detectUnfinishedLoops(rows, now) {
  const out = [];
  for (const row of rows || []) {
    if (row.subtype !== 'task') continue;
    if (String(row.status || '').toLowerCase() === 'done') continue;
    const metadata = asObj(row.metadata);
    const dueTs = parseTs(metadata.due_date || metadata.deadline || 0);
    const ageH = Math.max(0, (now - parseTs(row.updated_at || row.created_at)) / (60 * 60 * 1000));
    const title = row.title || row.summary || 'Task';
    const startedTs = parseTs(row.updated_at || row.created_at);
    const anchorTime = formatAnchorTime(startedTs);
    const ageLabel = ageH < 1 ? 'just now' : ageH < 24 ? `${Math.round(ageH)}h ago` : `${Math.round(ageH / 24)}d ago`;
    const timeAnchorLabel = dueTs ? 'before deadline' : (anchorTime ? `started ${anchorTime}` : (ageH > 18 ? 'today' : 'next block'));
    out.push(candidateBase({
      opportunityType: 'unfinished_work_loop',
      seedNodeId: row.id,
      title: `Complete: ${title} (open ${ageLabel})`,
      triggerSummary: dueTs
        ? `${title} is unfinished with a deadline signal. Started ${ageLabel}, no completion recorded.`
        : `${title} opened ${ageLabel} with no completion signal.`,
      confidence: Math.min(0.93, 0.62 + Math.min(0.22, ageH / 24) + (dueTs ? 0.08 : 0)),
      reasonCodes: dueTs ? ['unfinished_loop', 'deadline_risk'] : ['unfinished_loop'],
      timeAnchor: timeAnchorLabel,
      candidateActions: dueTs
        ? [`Complete next step in: ${title}`, 'Submit/update status']
        : [`Pick up where you left off: ${title}`],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(title),
      score: Math.min(0.96, 0.6 + Math.min(0.25, ageH / 24) + (dueTs ? 0.08 : 0))
    }));
  }
  return out;
}

async function detectDeadlineRisk(rows, now) {
  const out = [];
  for (const row of rows || []) {
    const metadata = asObj(row.metadata);
    const dueTs = parseTs(metadata.due_date || metadata.deadline || metadata.start || metadata.latest_activity_at || 0);
    if (!dueTs) continue;
    const hours = (dueTs - now) / (60 * 60 * 1000);
    if (hours < -12 || hours > 72) continue;
    const title = row.title || row.summary || 'Upcoming item';
    const deadlineLabel = hours < 0 ? 'overdue' : hours < 1 ? 'due in <1h' : `due in ${Math.round(hours)}h`;
    out.push(candidateBase({
      opportunityType: 'deadline_risk',
      seedNodeId: row.id,
      title: `${title} — ${deadlineLabel}`,
      triggerSummary: `${title} has a near-term deadline (${deadlineLabel}). No completion signal found.`,
      confidence: Math.min(0.98, 0.72 + (hours <= 24 ? 0.15 : 0.08)),
      reasonCodes: ['deadline_window', 'time_sensitive'],
      timeAnchor: hours < 0 ? 'overdue now' : hours <= 2 ? `in ${Math.round(hours * 60)} min` : hours <= 24 ? `in ${Math.round(hours)}h` : 'tomorrow',
      candidateActions: ['Complete deadline-critical step', 'Send deadline status update'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(title),
      score: Math.min(0.99, 0.78 + (hours <= 24 ? 0.14 : 0.08))
    }));
  }
  return out;
}

async function detectDormantContacts(rows, now) {
  const out = [];
  for (const row of rows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const lastTs = parseTs(metadata.latest_interaction_at || row.updated_at || row.created_at);
    if (!lastTs) continue;
    const days = Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000));
    const importance = Number(row.confidence || metadata.importance || 0.5);
    if (days < 14 || importance < 0.7) continue;
    out.push(candidateBase({
      opportunityType: 'dormant_important_contact',
      seedNodeId: row.id,
      title: `Reconnect with ${row.title}`,
      triggerSummary: `${row.title} is high-importance but dormant for ${Math.round(days)} days.`,
      confidence: Math.min(0.9, 0.62 + Math.min(0.2, days / 30) + Math.min(0.08, importance / 10)),
      reasonCodes: ['relationship_decay', 'high_importance'],
      timeAnchor: 'this week',
      candidateActions: ['Send quick reconnect message'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(row.title),
      score: Math.min(0.95, 0.64 + Math.min(0.2, days / 28))
    }));
  }
  return out;
}

async function detectRelationshipIntelligence(semanticRows, episodeRows, recentEvents, now) {
  const out = [];

  // 1. Birthdays
  for (const row of semanticRows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const birthday = metadata.birthday; // Expected: "MM-DD" or "YYYY-MM-DD"
    if (birthday) {
      const today = new Date(now);
      const bDate = new Date(birthday);
      if (!isNaN(bDate.getTime())) {
        if (bDate.getMonth() === today.getMonth() && bDate.getDate() === today.getDate()) {
          out.push(candidateBase({
            opportunityType: 'birthday_reminder',
            seedNodeId: row.id,
            title: `It's ${row.title}'s birthday!`,
            triggerSummary: `Birthday signal detected for ${row.title} from contact metadata.`,
            confidence: 0.98,
            reasonCodes: ['birthday_metadata', 'relationship_intelligence'],
            timeAnchor: 'today',
            candidateActions: [`Send birthday message to ${row.title}`, `Plan a call with ${row.title}`],
            sourceRefs: sourceRefsFromRow(row),
            canonicalTarget: normalizeTarget(row.title),
            score: 0.98
          }));
        }
      }
    }
  }

  for (const event of recentEvents || []) {
    const title = (event.window_title || event.text || '').toLowerCase();
    if (event.type === 'calendar' && title.includes('birthday')) {
      const nameMatch = title.match(/(.+)['’]s birthday/) || title.match(/birthday of (.+)/) || title.match(/birthday:? (.+)/);
      const name = nameMatch ? nameMatch[1].trim() : 'Contact';
      out.push(candidateBase({
        opportunityType: 'birthday_reminder',
        seedNodeId: event.id,
        title: `Birthday: ${name}`,
        triggerSummary: `Detected birthday calendar event: ${event.window_title || event.text}`,
        confidence: 0.92,
        reasonCodes: ['birthday_calendar_event', 'relationship_intelligence'],
        timeAnchor: 'today',
        candidateActions: [`Reach out to ${name}`],
        sourceRefs: [event.id],
        canonicalTarget: normalizeTarget(name),
        score: 0.92
      }));
    }
  }

  // 2. News Mentions
  const newsDomains = ['techcrunch.com', 'nytimes.com', 'theverge.com', 'wired.com', 'bloomberg.com', 'forbes.com', 'wsj.com'];
  for (const event of recentEvents || []) {
    const meta = asObj(event.metadata);
    const domain = (meta.domain || '').toLowerCase();
    if (newsDomains.some(d => domain.includes(d))) {
      const eventTitle = event.window_title || event.text || '';
      if (!eventTitle) continue;
      for (const person of semanticRows || []) {
        if (person.subtype !== 'person') continue;
        const name = person.title;
        if (name && name.length > 3 && eventTitle.includes(name)) {
          out.push(candidateBase({
            opportunityType: 'person_news',
            seedNodeId: person.id,
            title: `News about ${name}`,
            triggerSummary: `Detected "${name}" in a news article: "${eventTitle}" on ${domain}.`,
            confidence: 0.85,
            reasonCodes: ['person_news_mention', 'relationship_intelligence'],
            timeAnchor: 'now',
            candidateActions: [`Share news with ${name}`, `Read about ${name}`],
            sourceRefs: [event.id, ...sourceRefsFromRow(person)],
            canonicalTarget: normalizeTarget(name),
            score: 0.84
          }));
        }
      }
    }
  }

  // 3. Article Sharing
  const recentLinks = recentEvents.filter(e => (e.type === 'link' || e.type === 'browser') && asObj(e.metadata).url);
  for (const linkEvent of recentLinks.slice(0, 10)) {
    const linkTitle = linkEvent.window_title || linkEvent.text || '';
    if (!linkTitle || linkTitle.length < 10) continue;
    const url = asObj(linkEvent.metadata).url;

    for (const person of semanticRows || []) {
      if (person.subtype !== 'person') continue;
      const meta = asObj(person.metadata);
      const topics = parseList(meta.topics || []);
      const matchedTopic = topics.find(t => linkTitle.toLowerCase().includes(String(t).toLowerCase()));
      if (matchedTopic) {
        out.push(candidateBase({
          opportunityType: 'article_share',
          seedNodeId: person.id,
          title: `Share with ${person.title}`,
          triggerSummary: `Found article matching ${person.title}'s interest in "${matchedTopic}": ${linkTitle}`,
          confidence: 0.8,
          reasonCodes: ['topic_interest_match', 'relationship_intelligence'],
          timeAnchor: 'today',
          candidateActions: [`Share article with ${person.title}`],
          sourceRefs: [linkEvent.id, ...sourceRefsFromRow(person)],
          canonicalTarget: normalizeTarget(person.title),
          score: 0.8
        }));
      }
    }
  }

  // 4. Follow-ups (unresolved threads)
  for (const row of semanticRows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const sourceGroup = String(metadata.source_type_group || '').toLowerCase();
    if (!['communication', 'calendar'].includes(sourceGroup)) continue;
    const text = `${row.summary || ''} ${row.canonical_text || ''} ${metadata.intent || ''}`.toLowerCase();
    if (!/\b(reply|follow up|pending|unanswered|open|needs response|action)\b/.test(text)) continue;
    const lastTs = parseTs(metadata.latest_interaction_at || row.updated_at || row.created_at);
    if (!lastTs) continue;
    const days = Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000));
    if (days < 2) continue;
    const contactName = (row.title || 'contact').trim();
    const lastSeenLabel = days < 1 ? 'today' : days < 2 ? 'yesterday' : `${Math.round(days)} days ago`;
    const anchorTime = formatAnchorTime(lastTs);
    const timeAnchorLabel = anchorTime ? `last seen ${anchorTime}` : lastSeenLabel;
    out.push(candidateBase({
      opportunityType: 'followup_reminder',
      seedNodeId: row.id,
      title: `Follow up with ${contactName}`,
      triggerSummary: `${contactName} has an unanswered thread, last active ${lastSeenLabel}.`,
      confidence: Math.min(0.95, 0.6 + Math.min(0.3, days / 10)),
      reasonCodes: ['open_thread', 'followup_needed', 'relationship_intelligence'],
      timeAnchor: timeAnchorLabel,
      candidateActions: [`Draft reply to ${contactName}`, 'Send short status check-in'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(contactName),
      score: Math.min(0.98, 0.65 + Math.min(0.25, days / 12))
    }));
  }

  // 5. Connecting (dormant or weak ties)
  for (const row of semanticRows || []) {
    if (row.subtype !== 'person') continue;
    const metadata = asObj(row.metadata);
    const lastTs = parseTs(metadata.latest_interaction_at || row.updated_at || row.created_at);
    if (!lastTs) continue;
    const days = Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000));
    const importance = Number(row.confidence || metadata.importance || 0.5);
    if (days < 7 || importance < 0.6) continue; // Lower threshold for connecting
    const contactName = (row.title || 'contact').trim();
    out.push(candidateBase({
      opportunityType: 'connection_opportunity',
      seedNodeId: row.id,
      title: `Reconnect with ${contactName}`,
      triggerSummary: `${contactName} hasn't been contacted in ${Math.round(days)} days. Time to reconnect.`,
      confidence: Math.min(0.9, 0.62 + Math.min(0.2, days / 30) + Math.min(0.08, importance / 10)),
      reasonCodes: ['relationship_decay', 'connection_opportunity', 'relationship_intelligence'],
      timeAnchor: 'this week',
      candidateActions: ['Send quick reconnect message', 'Schedule a catch-up call'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(contactName),
      score: Math.min(0.95, 0.64 + Math.min(0.2, days / 28))
    }));
  }

  return out;
}

function enrichWithRecentRecall(candidates, recentEvents) {
  const rows = Array.isArray(recentEvents) ? recentEvents : [];
  return (candidates || []).map((candidate) => {
    const target = candidate.canonical_target;
    const support = [];
    for (const row of rows) {
      const meta = asObj(row.metadata);
      const text = `${row.text || ''} ${row.window_title || ''} ${meta.activity_summary || ''}`.toLowerCase();
      const firstWord = target.split(' ')[0] || '';
      if (!firstWord || firstWord.length < 4) continue;
      if (!text.includes(firstWord)) continue;
      support.push({
        node_id: row.id,
        source: row.source || row.app || row.type || 'Event',
        text: trim(meta.activity_summary || row.text || row.window_title || '', 160),
        timestamp: row.timestamp
      });
      if (support.length >= 2) break;
    }
    const additionalScore = support.length ? Math.min(0.08, support.length * 0.04) : 0;
    return {
      ...candidate,
      score: Number((candidate.score + additionalScore).toFixed(4)),
      recent_recall: support
    };
  });
}

async function attachSupportingEdges(candidates, maxPerCandidate = 4) {
  const out = [];
  for (const c of candidates || []) {
    const edges = await fetchSupportingEdges(c.seed_node_id, maxPerCandidate);
    out.push({
      ...c,
      supporting_edge_paths: edges,
      supporting_node_ids: Array.from(new Set([c.seed_node_id, ...edges.flatMap((e) => [e.from, e.to]).filter(Boolean)])).slice(0, 10)
    });
  }
  return out;
}

function dedupeCandidates(candidates = []) {
  const byKey = new Map();
  for (const c of candidates) {
    const key = `${c.opportunity_type}:${c.canonical_target}`;
    const existing = byKey.get(key);
    if (!existing || Number(c.score || 0) > Number(existing.score || 0)) {
      byKey.set(key, c);
    }
  }
  return Array.from(byKey.values());
}

async function detectContactOpportunities(now = Date.now()) {
  try {
    const detector = contactDetector();
    const contacts = await detector.detectAndScoreContacts(now);
    
    const candidates = [];
    
    for (const contact of contacts) {
      // Birthday reminder
      if (contact.birthday) {
        const [month, day] = contact.birthday.slice(5, 10).split('-');
        const now_date = new Date(now);
        const this_year = new Date(now_date.getFullYear(), parseInt(month) - 1, parseInt(day));
        const days_until = Math.ceil((this_year - now) / (24 * 60 * 60 * 1000));
        
        if (days_until >= 0 && days_until <= 7) {
          candidates.push({
            opportunity_type: 'birthday_reminder',
            title: `${contact.name}'s birthday is ${days_until === 0 ? 'today' : `in ${days_until} days`}`,
            trigger_summary: `Send birthday message to ${contact.name}`,
            canonical_target: contact.name,
            confidence: 0.95,
            score: 0.90,
            seed_node_id: contact.id,
            supporting_node_ids: contact.source_node_ids,
            reason_codes: ['birthday']
          });
        }
      }
      
      // Weak tie reconnection
      if (contact.is_weak_tie) {
        candidates.push({
          opportunity_type: 'weak_tie_reconnect',
          title: `Reconnect with ${contact.name} (weak tie)`,
          trigger_summary: `Last contact: ${Math.floor(contact.days_since_contact)} days ago. Consider a casual check-in.`,
          canonical_target: contact.name,
          confidence: contact.strength,
          score: 0.65,
          seed_node_id: contact.id,
          supporting_node_ids: contact.source_node_ids,
          reason_codes: ['weak_tie']
        });
      }
      
      // Overdue follow-up
      if (contact.is_overdue_followup) {
        candidates.push({
          opportunity_type: 'followup_reminder',
          title: `Follow up with ${contact.name}`,
          trigger_summary: `No contact for ${Math.floor(contact.days_since_contact)} days, but strong relationship.`,
          canonical_target: contact.name,
          confidence: 0.85,
          score: 0.80,
          seed_node_id: contact.id,
          supporting_node_ids: contact.source_node_ids,
          reason_codes: ['overdue_followup']
        });
      }
    }
    
    return candidates;
  } catch (error) {
    console.warn('[OpportunityMiner] Contact detection failed:', error.message);
    return [];
  }
}

async function detectRelationshipGraphOpportunities(now = Date.now()) {
  try {
    const { getRelationshipContacts, buildRelationshipDraftContext } = require('../relationship-graph');
    const contacts = await getRelationshipContacts({ limit: 80 });
    const candidates = [];

    for (const contact of contacts || []) {
      const metadata = asObj(contact.metadata);
      const score = Number(contact.strength_score || 0);
      const status = String(contact.status || '').toLowerCase();
      const lastTs = parseTs(contact.last_interaction_at);
      const days = lastTs ? Math.max(0, (now - lastTs) / (24 * 60 * 60 * 1000)) : 365;
      const shouldNudge = ['decaying', 'cooling', 'needs_followup'].includes(status) || score < 0.55 || days > 14;
      if (!shouldNudge) continue;

      const draftContext = await buildRelationshipDraftContext(contact.id, { limit: 4 }).catch(() => null);
      const receipt = (draftContext?.receipts || []).find((item) => item.text) || null;
      const hook = receipt ? trim(receipt.text, 140) : trim(metadata?.score_inputs ? `relationship score ${score.toFixed(2)}` : 'recent relationship history', 120);
      const action = status === 'needs_followup' ? `Reply to ${contact.display_name}` : `Draft check-in to ${contact.display_name}`;

      candidates.push({
        ...candidateBase({
          opportunityType: status === 'needs_followup' ? 'relationship_followup' : 'relationship_decay_nudge',
          seedNodeId: contact.id,
          title: `${action}`,
          triggerSummary: `${contact.display_name} is ${status || 'cooling'}; last interaction ${Math.round(days)} days ago. Hook: ${hook}`,
          confidence: Math.max(0.62, Math.min(0.94, 0.86 - (score * 0.25) + Math.min(0.12, days / 120))),
          reasonCodes: ['relationship_graph', status || 'low_strength', receipt ? 'grounded_context_hook' : 'score_signal'],
          timeAnchor: days < 1 ? 'today' : `${Math.round(days)}d since last touch`,
          candidateActions: [action],
          sourceRefs: (draftContext?.draft_context_refs || []).filter(Boolean),
          canonicalTarget: normalizeTarget(contact.display_name),
          score: Math.max(0.64, Math.min(0.97, 0.9 - (score * 0.22) + Math.min(0.1, days / 140)))
        }),
        relationship_contact_id: contact.id,
        relationship_status: status,
        relationship_score_inputs: metadata.score_inputs || null,
        draft_context_refs: draftContext?.draft_context_refs || [],
        target_surface: receipt && /linkedin/i.test(String(receipt.source || '')) ? 'linkedin' : 'gmail',
        social_temperature: Number(score || 0),
        value_hook: receipt ? {
          source_event_id: receipt.event_id || null,
          source_event_text: hook
        } : null
      });
    }

    return candidates;
  } catch (error) {
    console.warn('[OpportunityMiner] Relationship graph opportunities failed:', error?.message || error);
    return [];
  }
}

async function mineProactiveOpportunities(now = Date.now(), options = {}) {
  const socialStrategy = await loadSocialStrategy(options);
  const forceDeepScan = Boolean(options.deep_scan || options.force_full_scan);
  const baseRecentEvents = await fetchRecentEvents(now, forceDeepScan ? 120 : 40, forceDeepScan ? 24 : 6);
  const eventTriggeredDeepScan = hasHighValueRecentSignal(baseRecentEvents);
  const deepScan = forceDeepScan || eventTriggeredDeepScan;

  const semanticRowsRaw = await fetchMemoryRows('semantic', `AND status != 'archived'`, [], deepScan ? 300 : 150);
  const cloudRowsRaw = await fetchMemoryRows('cloud', `AND status = 'open'`, [], deepScan ? 180 : 80);
  const episodeRowsRaw = await fetchMemoryRows('episode', `AND status != 'archived'`, [], deepScan ? 220 : 90);

  const semanticRows = sampleRows(semanticRowsRaw, deepScan ? 260 : 120);
  const cloudRows = sampleRows(cloudRowsRaw, deepScan ? 140 : 60);
  const episodeRows = sampleRows(episodeRowsRaw, deepScan ? 180 : 70);
  const recentEvents = baseRecentEvents;

  const candidates = [];
  candidates.push(...await detectSocialHeatmapNudges(semanticRows, recentEvents, now, socialStrategy));
  candidates.push(...await detectContextualValueHooks(semanticRows, recentEvents, now, socialStrategy));
  candidates.push(...await detectUnresolvedFollowups(semanticRows, now));
  candidates.push(...await detectUnfinishedLoops(semanticRows, now));
  candidates.push(...await detectDeadlineRisk([...semanticRows, ...episodeRows], now));
  candidates.push(...await detectRelationshipGraphOpportunities(now));
  candidates.push(...await detectContactOpportunities(now));

  if (deepScan) {
    candidates.push(...await detectDormantContacts(semanticRows, now));
    candidates.push(...await detectRelationshipIntelligence(semanticRows, episodeRows, recentEvents, now));
  }

  const recalled = enrichWithRecentRecall(candidates, recentEvents);
  const deduped = dedupeCandidates(recalled);
  const withEdges = await attachSupportingEdges(deduped, deepScan ? 4 : 2);

  const max = Math.max(3, Number(options.limit || (deepScan ? 24 : 12)));
  return withEdges
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, max)
    .map((c) => ({
      ...c,
      score: Number((c.score || 0).toFixed(4)),
      confidence: Number((c.confidence || 0).toFixed(4)),
      social_strategy: {
        default_tier: socialStrategy.default_tier,
        temperature_threshold: Number(socialStrategy.temperature_threshold || DEFAULT_SOCIAL_STRATEGY.temperature_threshold)
      }
    }));
}

module.exports = {
  mineProactiveOpportunities,
  detectContactOpportunities,
  detectRelationshipGraphOpportunities,
  __test__: {
    detectUnresolvedFollowups,
    detectUnfinishedLoops,
    detectDeadlineRisk,
    detectDormantContacts,
    detectRelationshipIntelligence,
    dedupeCandidates,
    enrichWithRecentRecall,
    normalizeTarget,
    detectContactOpportunities,
    detectRelationshipGraphOpportunities
  }
};
