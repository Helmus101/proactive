const db = require('../db');

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

function isStudyText(text = '') {
  return /\b(study|exam|quiz|assignment|revision|flashcard|vocab|idiom|chapter|problem set|homework)\b/i.test(String(text || ''));
}

function sourceRefsFromRow(row) {
  return parseList(row?.source_refs);
}

async function fetchMemoryRows(layer, extraWhere = '', params = []) {
  return db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, created_at, updated_at
     FROM memory_nodes
     WHERE layer = ? ${extraWhere}
     LIMIT 300`,
    [layer, ...params]
  ).catch(() => []);
}

async function fetchRecentEvents(now = Date.now(), limit = 100) {
  const sinceIso = new Date(now - 24 * 60 * 60 * 1000).toISOString();
  return db.allQuery(
    `SELECT id, type, source, app, window_title, text, timestamp, metadata
     FROM events
     WHERE datetime(timestamp) >= datetime(?)
     ORDER BY datetime(timestamp) DESC
     LIMIT ?`,
    [sinceIso, limit]
  ).catch(() => []);
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
    const isStudy = isStudyText(`${title} ${row.summary || ''} ${row.canonical_text || ''}`);
    const startedTs = parseTs(row.updated_at || row.created_at);
    const anchorTime = formatAnchorTime(startedTs);
    const ageLabel = ageH < 1 ? 'just now' : ageH < 24 ? `${Math.round(ageH)}h ago` : `${Math.round(ageH / 24)}d ago`;
    const timeAnchorLabel = dueTs ? 'before deadline' : (anchorTime ? `started ${anchorTime}` : (ageH > 18 ? 'today' : 'next block'));
    out.push(candidateBase({
      opportunityType: isStudy ? 'unfinished_study_loop' : 'unfinished_work_loop',
      seedNodeId: row.id,
      title: isStudy
        ? `Resume: ${title} (started ${ageLabel})`
        : `Complete: ${title} (open ${ageLabel})`,
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

async function detectWeakStudyConcept(cloudRows, now) {
  const out = [];
  for (const row of cloudRows || []) {
    const text = `${row.title || ''} ${row.summary || ''}`;
    if (!isStudyText(text)) continue;
    const metadata = asObj(row.metadata);
    const repeated = Number(metadata.repeat_count || metadata.evidence_count || 0);
    if (repeated < 2 && !/\bweak|struggle|mistake|incorrect|failed\b/i.test(text)) continue;
    const latest = parseTs(metadata.latest_activity_at || row.updated_at || row.created_at);
    const hours = latest ? Math.max(0, (now - latest) / (60 * 60 * 1000)) : 24;
    const conceptName = (row.title || 'concept').trim();
    const repeatLabel = repeated >= 2 ? `missed ${repeated}x` : 'flagged weak';
    const lastReviewLabel = hours < 2 ? 'recently' : hours < 24 ? `${Math.round(hours)}h ago` : `${Math.round(hours / 24)}d ago`;
    out.push(candidateBase({
      opportunityType: 'weak_repeated_study_concept',
      seedNodeId: row.id,
      title: `Drill: ${conceptName} (${repeatLabel})`,
      triggerSummary: `${conceptName} has been ${repeatLabel} across study sessions, last seen ${lastReviewLabel}. Needs a targeted review block.`,
      confidence: Math.min(0.94, 0.64 + Math.min(0.2, repeated / 6) + Math.max(0, (24 - Math.min(hours, 24)) / 120)),
      reasonCodes: ['repeated_weak_signal', 'study_gap'],
      timeAnchor: hours < 24 ? `last reviewed ${lastReviewLabel}` : 'today',
      candidateActions: [`Run focused drill on: ${conceptName}`, 'Redo missed questions for this concept'],
      sourceRefs: sourceRefsFromRow(row),
      canonicalTarget: normalizeTarget(conceptName),
      score: Math.min(0.97, 0.68 + Math.min(0.2, repeated / 6))
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

async function mineProactiveOpportunities(now = Date.now(), options = {}) {
  const semanticRows = await fetchMemoryRows('semantic', `AND status != 'archived'`);
  const cloudRows = await fetchMemoryRows('cloud', `AND status = 'open'`);
  const episodeRows = await fetchMemoryRows('episode', `AND status != 'archived'`);
  const recentEvents = await fetchRecentEvents(now, 120);

  const candidates = [];
  candidates.push(...await detectUnresolvedFollowups(semanticRows, now));
  candidates.push(...await detectUnfinishedLoops(semanticRows, now));
  candidates.push(...await detectDeadlineRisk([...semanticRows, ...episodeRows], now));
  candidates.push(...await detectDormantContacts(semanticRows, now));
  candidates.push(...await detectWeakStudyConcept(cloudRows, now));
  candidates.push(...await detectRelationshipIntelligence(semanticRows, episodeRows, recentEvents, now));

  const recalled = enrichWithRecentRecall(candidates, recentEvents);
  const deduped = dedupeCandidates(recalled);
  const withEdges = await attachSupportingEdges(deduped, 4);

  const max = Math.max(3, Number(options.limit || 24));
  return withEdges
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, max)
    .map((c) => ({
      ...c,
      score: Number((c.score || 0).toFixed(4)),
      confidence: Number((c.confidence || 0).toFixed(4))
    }));
}

module.exports = {
  mineProactiveOpportunities,
  __test__: {
    detectUnresolvedFollowups,
    detectUnfinishedLoops,
    detectDeadlineRisk,
    detectDormantContacts,
    detectWeakStudyConcept,
    detectRelationshipIntelligence,
    dedupeCandidates,
    enrichWithRecentRecall,
    normalizeTarget
  }
};
