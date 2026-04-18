const { generateEmbedding, cosineSimilarity } = require('../embedding-engine');
const db = require('../db');
const { normalizeEventEnvelope, extractEntities } = require('../ingestion');
const {
  asObj,
  stableHash,
  upsertMemoryNode,
  upsertMemoryEdge,
  buildRetrievalDocText,
  upsertRetrievalDoc,
  removeMemoryArtifactsByVersion,
  logGraphVersion
} = require('./graph-store');

const GRAPH_VERSION_PREFIX = 'zero_base_memory_v1';
const EPISODE_WINDOW_MS = 90 * 60 * 1000;

function parseTs(value) {
  if (!value && value !== 0) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : 0;
}

function isoFromTs(ts) {
  return ts ? new Date(ts).toISOString() : null;
}

function applyTimeDecay(confidence, latestActivityIso) {
  if (!latestActivityIso) return confidence;
  const daysInactive = (Date.now() - Date.parse(latestActivityIso)) / (1000 * 60 * 60 * 24);
  if (daysInactive < 7) return confidence;
  const decay = (daysInactive - 7) * 0.015; // ~0.1 decay per week of inactivity
  return Math.max(0.1, confidence - decay);
}

function uniq(items, limit = 24) {
  return Array.from(new Set((items || []).filter(Boolean))).slice(0, limit);
}

function uniqBy(items, keyFn, limit = 24) {
  const out = [];
  const seen = new Set();
  for (const item of items || []) {
    const key = keyFn(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
    if (out.length >= limit) break;
  }
  return out;
}

function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9._/-]+/g, ' ')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter((item) => item.length >= 3)
    .slice(0, 36);
}

function overlapScore(left, right) {
  const l = new Set((left || []).map((item) => String(item || '').toLowerCase()).filter(Boolean));
  const r = new Set((right || []).map((item) => String(item || '').toLowerCase()).filter(Boolean));
  let hits = 0;
  for (const item of l) {
    if (r.has(item)) hits += 1;
  }
  return hits;
}

function normalizeContextValue(value) {
  return String(value || '').trim().toLowerCase();
}

function semanticTokensForEnvelope(envelope) {
  const blocked = new Set([
    normalizeContextValue(envelope.domain),
    normalizeContextValue(envelope.app)
  ].filter(Boolean));

  const tokens = uniq([
    ...(envelope.participants || []),
    ...(envelope.topics || []),
    ...tokenize(envelope.title),
    ...tokenize(envelope.text)
  ], 56);

  return tokens
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .filter((item) => !blocked.has(normalizeContextValue(item)));
}

function groupForTypeGroup(typeGroup) {
  if (typeGroup === 'calendar') return 'calendar';
  if (typeGroup === 'communication') return 'communication';
  if (typeGroup === 'desktop') return 'desktop';
  return 'artifact';
}

function isLikelyPerson(text) {
  const value = String(text || '').trim();
  if (!value) return false;
  if (/^(app|topic|project)\s*:/i.test(value)) return false;
  if (/^(chrome|cursor|gmail|calendar|desktop|drive|google docs)$/i.test(value)) return false;
  return true;
}

function buildDerivedVersion(seed = 'current') {
  return `${GRAPH_VERSION_PREFIX}:${seed}`;
}

async function loadEventsForGraph({ eventIds = null, since = null, limit = 800 } = {}) {
  if (Array.isArray(eventIds) && eventIds.length) {
    const placeholders = eventIds.map(() => '?').join(',');
    return db.allQuery(
      `SELECT *
       FROM events
       WHERE id IN (${placeholders})
       ORDER BY occurred_at ASC, timestamp ASC`,
      eventIds
    );
  }

  if (since) {
    return db.allQuery(
      `SELECT *
       FROM events
       WHERE COALESCE(occurred_at, timestamp) >= ?
       ORDER BY COALESCE(occurred_at, timestamp) ASC
       LIMIT ?`,
      [since, limit]
    );
  }

  return db.allQuery(
    `SELECT *
     FROM events
     ORDER BY COALESCE(occurred_at, timestamp) ASC`
  );
}

function envelopeFromRow(row) {
  const metadata = asObj(row.metadata);
  const entities = extractEntities([
    row.title,
    row.redacted_text,
    row.text,
    JSON.stringify(metadata || {})
  ].filter(Boolean).join('\n'));
  return normalizeEventEnvelope({
    id: row.id,
    type: row.source_type || row.type,
    timestamp: row.occurred_at || row.timestamp,
    source: row.source || row.app,
    text: row.redacted_text || row.text || '',
    metadata: {
      ...metadata,
      source_account: row.source_account || metadata.source_account,
      app: row.app || metadata.app,
      window_title: row.window_title || metadata.window_title,
      url: row.url || metadata.url,
      domain: row.domain || metadata.domain,
      participants: (() => {
        try {
          return JSON.parse(row.participants || '[]');
        } catch (_) {
          return metadata.participants || [];
        }
      })(),
      title: row.title || metadata.title,
      source_ref: row.source_ref || metadata.source_ref,
      raw_text: row.raw_text || metadata.raw_text || row.text || '',
      redacted_text: row.redacted_text || metadata.redacted_text || row.text || '',
      id: row.id
    },
    entities
  });
}

function assignEpisode(groups, envelope) {
  const ts = parseTs(envelope.occurred_at || envelope.timestamp);
  const semanticTokens = semanticTokensForEnvelope(envelope);
  const envelopeApp = normalizeContextValue(envelope.app);
  const envelopeDomain = normalizeContextValue(envelope.domain);

  const candidates = [];
  for (const group of groups) {
    const gapMs = Math.abs(group.latestTs - ts);
    const participantHits = overlapScore(group.participants, envelope.participants);
    const topicHits = overlapScore(group.tokens, semanticTokens);
    const sameDomain = envelopeDomain && group.domain && envelopeDomain === group.domain;
    const sameApp = envelopeApp && group.apps.has(envelopeApp);
    const sameSourceRef = envelope.source_ref && group.sourceRefs.has(envelope.source_ref);
    const sameCommunicationFlow = group.typeGroup === 'communication' && envelope.type_group === 'communication';
    const MAX_EPISODE_SPAN = 4 * 60 * 60 * 1000; // soft ceiling of 4 hours

    if (Math.abs(ts - group.startTs) > MAX_EPISODE_SPAN || gapMs > 2 * 60 * 60 * 1000) continue;

    // Hard boundary for parallel but unrelated contexts happening in the same half-hour window.
    const contextDisjointShortGap = gapMs <= 45 * 60 * 1000
      && !sameSourceRef
      && !sameDomain
      && !sameApp
      && participantHits === 0
      && topicHits < 3;
    if (contextDisjointShortGap) continue;

    const hasStrongSemanticBridge = participantHits >= 1 || topicHits >= 4;
    if (gapMs > 15 * 60 * 1000 && !sameSourceRef && !sameDomain && !hasStrongSemanticBridge) continue;

    const eligible =
      sameSourceRef ||
      (sameDomain && gapMs <= 3 * 60 * 60 * 1000) ||
      (sameApp && topicHits >= 2 && gapMs <= 90 * 60 * 1000) ||
      (participantHits >= 1 && gapMs <= 2 * 60 * 60 * 1000) ||
      (topicHits >= 3 && gapMs <= 90 * 60 * 1000) ||
      (sameCommunicationFlow && participantHits >= 1 && gapMs <= 4 * 60 * 60 * 1000) ||
      (group.typeGroup === envelope.type_group && topicHits >= 2 && gapMs <= 75 * 60 * 1000);

    if (!eligible) continue;

    let score = 0;
    if (sameSourceRef) score += 8;
    if (sameDomain) score += 5;
    if (sameApp) score += 3;
    score += participantHits * 2.5;
    score += topicHits * 1.25;
    if (sameCommunicationFlow) score += 1.5;
    if (group.typeGroup === envelope.type_group) score += 0.75;
    if (gapMs > 45 * 60 * 1000) score -= 1.5;

    candidates.push({ group, score });
  }

  candidates.sort((a, b) => b.score - a.score);
  const bestCandidate = candidates[0];

  if (bestCandidate && bestCandidate.score >= 2.5) {
  const targets = [bestCandidate];
  // Also assign to other high-scoring groups to increase density/overlap
  for (let i = 1; i < candidates.length; i++) {
    const c = candidates[i];
    // Many-to-many mapping: link if score is above threshold
    if (c.score >= 4.0 || c.score >= bestCandidate.score * 0.75) {
      targets.push(c);
    }
  }

    for (const { group } of targets) {
      group.events.push(envelope);
      group.latestTs = Math.max(group.latestTs, ts);
      group.startTs = Math.min(group.startTs, ts);
      group.anchorTs = Math.min(group.anchorTs, ts);
      group.participants = uniq(group.participants.concat(envelope.participants || []), 24);
      group.tokens = uniq(group.tokens.concat(semanticTokens), 72);
      group.topics = uniq(group.topics.concat(envelope.topics || []), 24);
      if (envelopeApp) group.apps.add(envelopeApp);
      if (!group.domain && envelopeDomain) group.domain = envelopeDomain;
      if (envelope.source_ref) group.sourceRefs.add(envelope.source_ref);
    }
    return;
  }

  groups.push({
    id: `ep_${stableHash(`${groupForTypeGroup(envelope.type_group)}|${envelope.source_ref || envelope.id}|${envelope.timestamp}`)}`,
    typeGroup: envelope.type_group,
    domain: envelopeDomain || null,
    startTs: ts,
    anchorTs: ts,
    latestTs: ts,
    participants: uniq(envelope.participants || [], 24),
    tokens: semanticTokens,
    topics: uniq(envelope.topics || [], 24),
    apps: new Set(envelopeApp ? [envelopeApp] : []),
    sourceRefs: new Set(envelope.source_ref ? [envelope.source_ref] : []),
    events: [envelope]
  });
}

function clusterEnvelopes(envelopes) {
  const groups = [];
  const ordered = [...envelopes].sort((a, b) => parseTs(a.occurred_at || a.timestamp) - parseTs(b.occurred_at || b.timestamp));
  for (const envelope of ordered) assignEpisode(groups, envelope);
  return groups;
}

function summarizeEpisode(group) {
  const titles = uniq(group.events.map((event) => event.title).filter(Boolean), 6);
  const participants = uniq(group.events.flatMap((event) => event.participants || []), 8);
  const domains = uniq(group.events.map((event) => event.domain).filter(Boolean), 4);
  const topics = uniq(group.events.flatMap((event) => event.topics || []), 10);
  const apps = uniq(group.events.map((event) => event.app).filter(Boolean), 4);
  const summary = [
    titles.slice(0, 2).join(' | '),
    participants.length ? `People: ${participants.join(', ')}` : '',
    domains.length ? `Domains: ${domains.join(', ')}` : '',
    apps.length ? `Apps: ${apps.join(', ')}` : '',
    `${group.events.length} source events`
  ].filter(Boolean).join(' • ');

  return {
    title: titles[0] || `${groupForTypeGroup(group.typeGroup)} episode`,
    summary,
    participants,
    domains,
    topics,
    apps
  };
}

function inferStudyEpisodeSubtype(group, fallbackSubtype = 'desktop') {
  const events = Array.isArray(group?.events) ? group.events : [];
  if (!events.length) return fallbackSubtype;
  const hasStudySession = events.some((event) => Boolean(event?.metadata?.study_context?.in_session || event?.metadata?.study_session_id));
  if (!hasStudySession) return fallbackSubtype;
  const signals = events.map((event) => String(event?.metadata?.study_signal || '').toLowerCase()).filter(Boolean);
  const hay = `${signals.join(' ')} ${events.map((event) => String(event?.text || '')).join(' ')}`.toLowerCase();

  if (/\bdistraction\b|youtube|twitter|x\.com|instagram|reddit/.test(hay)) return 'distraction';
  if (/\bcontext-switch\b|switch/.test(hay)) return 'context_switch';
  if (/\bsolving\b|problem|exercise|quiz|question|leetcode/.test(hay)) return 'problem_solving';
  if (/\bdrafting\b|essay|thesis|write|paragraph|outline/.test(hay)) return 'writing';
  if (/\brevision\b|review|feedback|grade|result|score/.test(hay)) return 'revision';
  return 'study_reading';
}

function deriveSemanticNodes(group, episodeData) {
  const nodes = [];
  const episodeId = group.id;
  const latestIso = isoFromTs(group.latestTs);
  const anchorIso = episodeData.anchor_at;
  const anchorDate = episodeData.anchor_date;

  for (const person of uniq(group.events.flatMap((event) => event.participants || []).filter((item) => isLikelyPerson(item)), 12)) {
    nodes.push({
      id: `sem_${stableHash(`person|${person}|${episodeId}`)}`,
      layer: 'semantic',
      subtype: 'person',
      title: person,
      summary: `Person involved in ${episodeData.title}`,
      canonical_text: `${person}\n${episodeData.summary}`,
      confidence: 0.72,
      status: 'active',
      source_refs: episodeData.source_refs,
      metadata: {
        name: person,
        latest_interaction_at: latestIso,
        anchor_at: anchorIso,
        anchor_date: anchorDate,
        episode_id: episodeId,
        source_type_group: episodeData.source_type_group,
        participants: episodeData.participants
      },
      trace_label: `Person mentioned: ${person}`,
      edge_type: 'MENTIONS'
    });
  }

  const factTexts = uniq(group.events.flatMap((event) => [
    event.domain ? `Domain ${event.domain}` : null,
    event.app ? `App ${event.app}` : null,
    event.title ? `Topic ${event.title}` : null
  ]), 12);
  for (const fact of factTexts) {
    nodes.push({
      id: `sem_${stableHash(`fact|${fact}|${episodeId}`)}`,
      layer: 'semantic',
      subtype: 'fact',
      title: fact,
      summary: `Fact derived from ${episodeData.title}`,
      canonical_text: fact,
      confidence: 0.66,
      status: 'active',
      source_refs: episodeData.source_refs,
      metadata: {
        fact,
        anchor_at: anchorIso,
        anchor_date: anchorDate,
        episode_id: episodeId,
        source_type_group: episodeData.source_type_group,
        domains: episodeData.domains,
        topics: episodeData.topics
      },
      trace_label: fact,
      edge_type: 'MENTIONS'
    });
  }

  const taskLines = uniq(group.events.flatMap((event) => {
    return String(event.text || '')
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => /\b(todo|follow up|reply|send|prepare|review|fix|finish|draft|schedule|confirm)\b/i.test(line))
      .filter((line) => !/^(from|to|subject|date|cc)\s*:/i.test(line))
      .filter((line) => line.length <= 180);
  }), 8);
  for (const task of taskLines) {
    nodes.push({
      id: `sem_${stableHash(`task|${task}|${episodeId}`)}`,
      layer: 'semantic',
      subtype: 'task',
      title: task,
      summary: `Open task signal from ${episodeData.title}`,
      canonical_text: task,
      confidence: 0.63,
      status: 'open',
      source_refs: episodeData.source_refs,
      metadata: {
        title: task,
        completed: false,
        anchor_at: anchorIso,
        anchor_date: anchorDate,
        episode_id: episodeId,
        source_type_group: episodeData.source_type_group
      },
      trace_label: task,
      edge_type: 'GENERATED_FROM'
    });
  }

  const decisionSignals = uniq(group.events.flatMap((event) => {
    const lower = String(event.text || '').toLowerCase();
    const hits = [];
    if (/\bdecided\b/.test(lower)) hits.push(`Decision in ${event.app || event.source}`);
    if (/\bapproved\b/.test(lower)) hits.push(`Approval in ${event.app || event.source}`);
    if (/\bship(ped|ping)?\b/.test(lower)) hits.push(`Shipping mentioned in ${event.app || event.source}`);
    return hits;
  }), 6);
  for (const decision of decisionSignals) {
    nodes.push({
      id: `sem_${stableHash(`decision|${decision}|${episodeId}`)}`,
      layer: 'semantic',
      subtype: 'decision',
      title: decision,
      summary: `Decision signal from ${episodeData.title}`,
      canonical_text: decision,
      confidence: 0.61,
      status: 'active',
      source_refs: episodeData.source_refs,
      metadata: {
        decision,
        anchor_at: anchorIso,
        anchor_date: anchorDate,
        episode_id: episodeId,
        source_type_group: episodeData.source_type_group
      },
      trace_label: decision,
      edge_type: 'GENERATED_FROM'
    });
  }

  const links = uniqBy(group.events.flatMap((event) => {
    return [event.url, event.metadata?.webViewLink, event.metadata?.link]
      .filter(Boolean)
      .map((url) => ({
        id: `sem_${stableHash(`link|${url}|${episodeId}`)}`,
        layer: 'semantic',
        subtype: 'link',
        title: url,
        summary: `Linked artifact for ${episodeData.title}`,
        canonical_text: url,
        confidence: 0.7,
        status: 'active',
        source_refs: episodeData.source_refs,
        metadata: {
          url,
          domain: event.domain || null,
          anchor_at: anchorIso,
          anchor_date: anchorDate,
          episode_id: episodeId,
          source_type_group: episodeData.source_type_group
        },
        trace_label: url,
        edge_type: 'RELATED_TO'
      }));
  }), (item) => item?.metadata?.url, 6);
  nodes.push(...links);

  return nodes;
}

function buildCloudCandidates(episodes) {
  const personMentions = new Map();
  const taskMentions = new Map();
  const topicMentions = new Map();

  for (const episode of episodes) {
    const people = uniq(episode.participants || [], 10);
    const semanticNodes = episode.semanticNodes || [];
    const tasks = uniq(semanticNodes.filter((node) => node.subtype === 'task').map((node) => node.title), 10);
    const topics = uniq([...(episode.topics || []), ...(episode.domains || [])], 10);

    for (const person of people) {
      const key = `person:${person.toLowerCase()}`;
      const prev = personMentions.get(key) || { label: person, episodes: [], semanticNodeIds: [] };
      prev.episodes.push(episode);
      const semNode = semanticNodes.find(n => n.subtype === 'person' && n.title === person);
      if (semNode) prev.semanticNodeIds.push(semNode.id);
      personMentions.set(key, prev);
    }

    for (const task of tasks) {
      const key = `task:${task.toLowerCase()}`;
      const prev = taskMentions.get(key) || { label: task, episodes: [], semanticNodeIds: [] };
      prev.episodes.push(episode);
      const semNode = semanticNodes.find(n => n.subtype === 'task' && n.title === task);
      if (semNode) prev.semanticNodeIds.push(semNode.id);
      taskMentions.set(key, prev);
    }

    for (const topic of topics) {
      const key = `topic:${topic.toLowerCase()}`;
      const prev = topicMentions.get(key) || { label: topic, episodes: [], semanticNodeIds: [] };
      prev.episodes.push(episode);
      const semNode = semanticNodes.find(n => (n.subtype === 'fact' || n.subtype === 'link') && n.title === topic);
      if (semNode) prev.semanticNodeIds.push(semNode.id);
      topicMentions.set(key, prev);
    }
  }

  return { personMentions, taskMentions, topicMentions };
}

function deriveInsights(episodes) {
  const out = [];
  const { personMentions, taskMentions, topicMentions } = buildCloudCandidates(episodes);

  for (const item of personMentions.values()) {
    if (item.episodes.length < 2) continue;
    const latest = Math.max(...item.episodes.map((ep) => parseTs(ep.end)));
    const anchor = Math.min(...item.episodes.map((ep) => parseTs(ep.anchor_at || ep.start)));
    const days = Math.max(0, (Date.now() - latest) / (24 * 60 * 60 * 1000));
    let confidence = Math.min(0.88, 0.5 + Math.min(0.28, item.episodes.length * 0.08) + Math.min(0.1, days / 30));
    confidence = applyTimeDecay(confidence, isoFromTs(latest));
    if (confidence < 0.35) continue;
    out.push({
      id: `ins_${stableHash(`reengage|${item.label}`)}`,
      layer: 'insight',
      subtype: 'relationship_pattern',
      title: `Re-engage ${item.label}`,
      summary: `${item.label} appears across ${item.episodes.length} episodes and may need follow-up.`,
      canonical_text: `${item.label} repeated relationship context follow up`,
      confidence,
      status: 'active',
      source_refs: uniq(item.episodes.flatMap((ep) => ep.source_refs), 32),
      metadata: {
        pattern_type: 'relationship_reengage',
        label: item.label,
        anchor_at: isoFromTs(anchor),
        anchor_date: isoFromTs(anchor)?.slice(0, 10) || null,
        latest_activity_at: isoFromTs(latest),
        supporting_episode_ids: item.episodes.map((ep) => ep.id),
        supporting_semantic_ids: uniq(item.semanticNodeIds),
        repeated_count: item.episodes.length
      }
    });
  }

  for (const item of taskMentions.values()) {
    if (item.episodes.length < 2) continue;
    const latest = Math.max(...item.episodes.map((ep) => parseTs(ep.latest_activity_at || ep.end)));
    const anchor = Math.min(...item.episodes.map((ep) => parseTs(ep.anchor_at || ep.start)));
    
    let confidence = Math.min(0.86, 0.56 + Math.min(0.24, item.episodes.length * 0.08));
    confidence = applyTimeDecay(confidence, isoFromTs(latest));
    if (confidence < 0.35) continue;
    
    out.push({
      id: `ins_${stableHash(`open_loop|${item.label}`)}`,
      layer: 'insight',
      subtype: 'open_loop_pattern',
      title: `Open loop: ${item.label}`,
      summary: `This action signal repeats across ${item.episodes.length} episodes and still looks unresolved.`,
      canonical_text: `${item.label} repeated open loop task`,
      confidence,
      status: 'active',
      source_refs: uniq(item.episodes.flatMap((ep) => ep.source_refs), 32),
      metadata: {
        pattern_type: 'open_loop',
        label: item.label,
        anchor_at: isoFromTs(anchor),
        anchor_date: isoFromTs(anchor)?.slice(0, 10) || null,
        latest_activity_at: isoFromTs(latest),
        supporting_episode_ids: item.episodes.map((ep) => ep.id),
        supporting_semantic_ids: uniq(item.semanticNodeIds),
        repeated_count: item.episodes.length
      }
    });
  }

  for (const item of topicMentions.values()) {
    if (item.episodes.length < 3) continue;
    const latest = Math.max(...item.episodes.map((ep) => parseTs(ep.latest_activity_at || ep.end)));
    const anchor = Math.min(...item.episodes.map((ep) => parseTs(ep.anchor_at || ep.start)));
    out.push({
      id: `ins_${stableHash(`topic_pattern|${item.label}`)}`,
      layer: 'insight',
      subtype: 'topic_pattern',
      title: `Recurring topic: ${item.label}`,
      summary: `${item.label} recurs across ${item.episodes.length} episodes and may be becoming a durable pattern.`,
      canonical_text: `${item.label} recurring topic pattern`,
      confidence: applyTimeDecay(Math.min(0.84, 0.55 + Math.min(0.22, item.episodes.length * 0.07)), isoFromTs(latest)),
      status: 'active',
      source_refs: uniq(item.episodes.flatMap((ep) => ep.source_refs), 32),
      metadata: {
        pattern_type: 'topic_pattern',
        label: item.label,
        anchor_at: isoFromTs(anchor),
        anchor_date: isoFromTs(anchor)?.slice(0, 10) || null,
        latest_activity_at: isoFromTs(latest),
        supporting_episode_ids: item.episodes.map((ep) => ep.id),
        supporting_semantic_ids: uniq(item.semanticNodeIds),
        repeated_count: item.episodes.length
      }
    });
  }

  return out;
}

function deriveSemanticGroups(episodes) {
  const buckets = new Map();
  for (const episode of episodes || []) {
    const labels = uniq([
      ...(episode.study_subjects || []).map((item) => `subject:${String(item).toLowerCase()}`),
      ...(episode.topics || []).slice(0, 4).map((item) => `topic:${String(item).toLowerCase()}`),
      ...(episode.domains || []).slice(0, 3).map((item) => `domain:${String(item).toLowerCase()}`)
    ], 8);
    for (const label of labels) {
      const prev = buckets.get(label) || { label, episodes: [] };
      prev.episodes.push(episode);
      buckets.set(label, prev);
    }
  }

  const nodes = [];
  for (const item of buckets.values()) {
    if ((item.episodes || []).length < 2) continue;
    const supporting = uniq(item.episodes.map((ep) => ep.id), 24);
    const anchorTs = Math.min(...item.episodes.map((ep) => parseTs(ep.anchor_at || ep.start)));
    const latestTs = Math.max(...item.episodes.map((ep) => parseTs(ep.latest_activity_at || ep.end)));
    const titleLabel = String(item.label || '').replace(/^(subject|topic|domain):/, '');
    let confidence = Math.min(0.86, 0.58 + Math.min(0.24, supporting.length * 0.08));
    confidence = applyTimeDecay(confidence, isoFromTs(latestTs));
    if (confidence < 0.35) continue;
    
    nodes.push({
      id: `semgrp_${stableHash(`episode_group|${item.label}`)}`,
      layer: 'semantic',
      subtype: 'episode_group',
      title: `Pattern: ${titleLabel}`,
      summary: `${titleLabel} appears across ${supporting.length} episodes within chronological memory.`,
      canonical_text: `${titleLabel}\nRepeated across ${supporting.length} episodes`,
      confidence,
      status: 'active',
      source_refs: uniq(item.episodes.flatMap((ep) => ep.source_refs || []), 48),
      metadata: {
        label: item.label,
        supporting_episode_ids: supporting,
        repeated_count: supporting.length,
        anchor_at: isoFromTs(anchorTs),
        anchor_date: isoFromTs(anchorTs)?.slice(0, 10) || null,
        latest_activity_at: isoFromTs(latestTs)
      }
    });
  }

  return nodes.slice(0, 80);
}

async function embedText(text) {
  return generateEmbedding(text, process.env.OPENAI_API_KEY);
}

function inferCaptureEpisodeSubtype(group) {
  // For desktop episodes, use the most common capture_category across events
  // to distinguish e.g. 'communication' (email/chat) from generic 'desktop'.
  const events = Array.isArray(group?.events) ? group.events : [];
  const categories = events
    .map((e) => String(e?.metadata?.capture_category || '').trim())
    .filter(Boolean);
  if (!categories.length) return group.typeGroup || 'desktop';
  const freq = {};
  for (const c of categories) freq[c] = (freq[c] || 0) + 1;
  const dominant = Object.entries(freq).sort((a, b) => b[1] - a[1])[0]?.[0];
  // Map capture_category values to valid episode subtypes
  if (dominant === 'communication') return 'communication';
  if (dominant === 'calendar') return 'calendar';
  return group.typeGroup || 'desktop';
}

async function resolveCrossAppEntities(semanticNodes) {
  const resolvedNodes = [];
  const idMap = new Map(); // originalId -> resolvedNode

  for (const node of semanticNodes) {
    const meta = node.metadata || {};
    let resolved = null;

    // Multi-signal resolution:
    // 1. Identifiers (email, url)
    const identifiers = [];
    if (node.subtype === 'person' && meta.name && meta.name.includes('@')) identifiers.push(meta.name.toLowerCase());
    if (node.subtype === 'link' && meta.url) identifiers.push(meta.url.toLowerCase());
    
    // 2. Stable Hash on identifier
    const identifierHash = identifiers.length ? stableHash(identifiers[0]) : null;

    for (const existing of resolvedNodes) {
      if (node.subtype !== existing.subtype) continue;

      const existingMeta = existing.metadata || {};
      let match = false;

      // Check identifier match
      if (identifierHash) {
        const existingIdentifiers = [];
        if (existing.subtype === 'person' && existingMeta.name && existingMeta.name.includes('@')) existingIdentifiers.push(existingMeta.name.toLowerCase());
        if (existing.subtype === 'link' && existingMeta.url) existingIdentifiers.push(existingMeta.url.toLowerCase());
        
        if (existingIdentifiers.some(id => identifiers.includes(id))) {
          match = true;
        }
      }

      // Check vector similarity (already done in writeEpisodeGroup, but we can be more aggressive here)
      if (!match && node.embedding && existing.embedding) {
        const sim = cosineSimilarity(node.embedding, existing.embedding);
        if (sim > 0.92) match = true;
      }

      // Check participant overlap for facts/tasks
      if (!match && (node.subtype === 'fact' || node.subtype === 'task')) {
        const pOverlap = overlapScore(meta.participants || [], existingMeta.participants || []);
        if (pOverlap >= 2 && node.title.toLowerCase() === existing.title.toLowerCase()) match = true;
      }

      if (match) {
        resolved = existing;
        break;
      }
    }

    if (resolved) {
      // Merge
      resolved.source_refs = uniq([...(resolved.source_refs || []), ...(node.source_refs || [])], 64);
      resolved.confidence = Math.max(resolved.confidence, node.confidence);
      // Combine metadata if needed
      if (node.subtype === 'person' && !resolved.metadata.name && node.metadata.name) {
        resolved.metadata.name = node.metadata.name;
      }
      idMap.set(node.id, resolved);
    } else {
      resolvedNodes.push(node);
      idMap.set(node.id, node);
    }
  }

  return { resolvedNodes, idMap };
}

async function writeEpisodeGroup(group, version) {
  const baseSubtype = group.typeGroup === 'desktop'
    ? inferCaptureEpisodeSubtype(group)
    : (group.typeGroup || 'desktop');
  const episodeSubtype = inferStudyEpisodeSubtype(group, baseSubtype);
  const summary = summarizeEpisode(group);
  const anchorAt = isoFromTs(group.anchorTs || group.startTs);
  const startAt = isoFromTs(group.startTs);
  const latestActivityAt = isoFromTs(group.latestTs);
  const episodeData = {
    title: summary.title,
    summary: summary.summary,
    canonical_text: [
      summary.title,
      summary.summary,
      group.events.map((event) => event.text).join('\n')
    ].filter(Boolean).join('\n').slice(0, 4000),
    participants: summary.participants,
    domains: summary.domains,
    topics: summary.topics,
    apps: summary.apps,
    source_refs: uniq(group.events.map((event) => event.id), 48),
    source_type_group: group.typeGroup,
    study_subjects: uniq(group.events.map((event) => event?.metadata?.study_subject).filter(Boolean), 8),
    study_signals: uniq(group.events.map((event) => event?.metadata?.study_signal).filter(Boolean), 12),
    anchor_at: anchorAt,
    anchor_date: anchorAt ? anchorAt.slice(0, 10) : null,
    start: startAt,
    end: latestActivityAt,
    latest_activity_at: latestActivityAt,
    event_count: group.events.length
  };

  const embedding = await embedText(buildRetrievalDocText({
    title: episodeData.title,
    summary: episodeData.summary,
    text: episodeData.canonical_text,
    data: episodeData
  }));

  await upsertMemoryNode({
    id: group.id,
    layer: 'episode',
    subtype: episodeSubtype,
    title: episodeData.title,
    summary: episodeData.summary,
    canonicalText: episodeData.canonical_text,
    confidence: Math.min(0.96, 0.62 + Math.min(0.24, group.events.length * 0.06)),
    status: 'active',
    sourceRefs: episodeData.source_refs,
    metadata: episodeData,
    graphVersion: version,
    embedding,
    anchorDate: episodeData.anchor_date || null
  });

  await upsertRetrievalDoc({
    docId: `node:${group.id}`,
    sourceType: 'node',
    nodeId: group.id,
    app: summary.apps[0] || null,
    timestamp: episodeData.anchor_at || episodeData.start,
    text: buildRetrievalDocText({
      title: episodeData.title,
      summary: episodeData.summary,
      text: episodeData.canonical_text,
      data: { layer: 'episode', subtype: episodeSubtype, ...episodeData }
    }),
    metadata: {
      layer: 'episode',
      subtype: episodeSubtype,
      source_refs: episodeData.source_refs,
      graph_version: version,
      anchor_at: episodeData.anchor_at,
      anchor_date: episodeData.anchor_date,
      latest_activity_at: episodeData.latest_activity_at
    }
  });

  // Persist each source screenshot/raw capture as a raw_event memory node linked to this episode.
  for (const event of group.events) {
    const rawNodeId = `raw_${stableHash(String(event.id || `${event.timestamp}|${event.title || ''}`))}`;
    const rawTs = isoFromTs(parseTs(event.occurred_at || event.timestamp)) || episodeData.anchor_at || new Date().toISOString();
    const rawTitle = String(event.title || event.window_title || event.app || 'Raw event').slice(0, 200);
    const rawSummary = String(event.text || '').replace(/\s+/g, ' ').trim().slice(0, 240);
    const rawCanonical = [
      rawTitle,
      event.app ? `App: ${event.app}` : '',
      event.window_title ? `Window: ${event.window_title}` : '',
      rawSummary
    ].filter(Boolean).join('\n');
    const rawEmbedding = await embedText(rawCanonical);

    await upsertMemoryNode({
      id: rawNodeId,
      layer: 'raw',
      subtype: event.type_group || 'event',
      title: rawTitle,
      summary: rawSummary || 'Raw captured event',
      canonicalText: rawCanonical,
      confidence: 0.5,
      status: 'active',
      sourceRefs: uniq([event.id, event.source_ref].filter(Boolean), 8),
      metadata: {
        event_id: event.id,
        source_type: event.type,
        source: event.source,
        app: event.app || null,
        window_title: event.window_title || null,
        url: event.url || null,
        domain: event.domain || null,
        timestamp: rawTs,
        anchor_at: rawTs,
        anchor_date: String(rawTs || '').slice(0, 10) || null,
        source_type_group: event.type_group || null,
        study_subject: event?.metadata?.study_subject || null,
        study_signal: event?.metadata?.study_signal || null
      },
      graphVersion: version,
      embedding: rawEmbedding,
      anchorDate: String(rawTs || '').slice(0, 10) || null
    });

    await upsertMemoryEdge({
      fromNodeId: rawNodeId,
      toNodeId: group.id,
      edgeType: 'PART_OF_EPISODE',
      weight: 0.62,
      traceLabel: 'Raw event grouped into 90-minute episode window',
      evidenceCount: 1,
      metadata: {
        event_id: event.id
      }
    });

    await upsertRetrievalDoc({
      docId: `node:${rawNodeId}`,
      sourceType: 'node',
      nodeId: rawNodeId,
      app: event.app || null,
      timestamp: rawTs,
      text: rawCanonical,
      metadata: {
        layer: 'raw',
        subtype: event.type_group || 'event',
        source_refs: uniq([event.id, event.source_ref].filter(Boolean), 8),
        graph_version: version,
        anchor_at: rawTs,
        anchor_date: String(rawTs).slice(0, 10),
        latest_activity_at: rawTs,
        source_type_group: event.type_group || null
      }
    });
  }

  const _semanticNodes = deriveSemanticNodes(group, episodeData);
  for (const node of _semanticNodes) {
    node.embedding = await embedText(node.canonical_text || node.summary || node.title);
  }

  const { resolvedNodes: semanticNodes } = await resolveCrossAppEntities(_semanticNodes);

  for (const node of semanticNodes) {
    const nodeEmbedding = node.embedding;
    await upsertMemoryNode({
      id: node.id,
      layer: node.layer,
      subtype: node.subtype,
      title: node.title,
      summary: node.summary,
      canonicalText: node.canonical_text,
      confidence: node.confidence,
      status: node.status,
      sourceRefs: node.source_refs,
      metadata: node.metadata,
      graphVersion: version,
      embedding: nodeEmbedding,
      anchorDate: node.metadata?.anchor_date || null
    });
    await upsertMemoryEdge({
      fromNodeId: group.id,
      toNodeId: node.id,
      edgeType: 'ABSTRACTED_TO',
      weight: node.confidence,
      traceLabel: node.trace_label,
      evidenceCount: episodeData.event_count,
      metadata: {
        episode_id: group.id,
        subtype: node.subtype,
        original_relation: node.edge_type
      }
    });
    await upsertRetrievalDoc({
      docId: `node:${node.id}`,
      sourceType: 'node',
      nodeId: node.id,
      app: summary.apps[0] || null,
      timestamp: episodeData.anchor_at || episodeData.start,
      text: buildRetrievalDocText({
        title: node.title,
        summary: node.summary,
        text: node.canonical_text,
        data: { layer: node.layer, subtype: node.subtype, ...node.metadata }
      }),
      metadata: {
        layer: node.layer,
        subtype: node.subtype,
        source_refs: node.source_refs,
        graph_version: version,
        anchor_at: episodeData.anchor_at,
        anchor_date: episodeData.anchor_date,
        latest_activity_at: episodeData.latest_activity_at,
        source_type_group: episodeData.source_type_group
      }
    });
  }

  const orderedEvents = [...group.events].sort((a, b) => parseTs(a.occurred_at || a.timestamp) - parseTs(b.occurred_at || b.timestamp));
  for (let index = 1; index < orderedEvents.length; index += 1) {
    const prevId = `evtref_${stableHash(orderedEvents[index - 1].id)}`;
    const nextId = `evtref_${stableHash(orderedEvents[index].id)}`;
    await upsertMemoryEdge({
      fromNodeId: group.id,
      toNodeId: group.id,
      edgeType: 'FOLLOWS_UP',
      weight: 0.4,
      traceLabel: `${prevId} -> ${nextId}`,
      evidenceCount: 1,
      metadata: {
        previous_event_id: orderedEvents[index - 1].id,
        next_event_id: orderedEvents[index].id
      }
    });
  }

  return {
    id: group.id,
    layer: 'episode',
    embedding,
    ...episodeData,
    semanticNodes
  };
}

async function writeHigherLayerNode(node, version) {
  const embedding = await embedText(node.canonical_text || node.summary || node.title);
  const retrievalTimestamp = node.metadata?.anchor_at || node.metadata?.latest_activity_at || new Date().toISOString();
  await upsertMemoryNode({
    id: node.id,
    layer: node.layer,
    subtype: node.subtype,
    title: node.title,
    summary: node.summary,
    canonicalText: node.canonical_text,
    confidence: node.confidence,
    status: node.status,
    sourceRefs: node.source_refs,
    metadata: node.metadata,
    graphVersion: version,
    embedding,
    anchorDate: node.metadata?.anchor_date || null
  });
  await upsertRetrievalDoc({
    docId: `node:${node.id}`,
    sourceType: 'node',
    nodeId: node.id,
    app: null,
    timestamp: retrievalTimestamp,
    text: buildRetrievalDocText({
      title: node.title,
      summary: node.summary,
      text: node.canonical_text,
      data: { layer: node.layer, subtype: node.subtype, ...node.metadata }
    }),
    metadata: {
      layer: node.layer,
      subtype: node.subtype,
      source_refs: node.source_refs,
      graph_version: version,
      anchor_at: node.metadata?.anchor_at || null,
      anchor_date: node.metadata?.anchor_date || null,
      latest_activity_at: node.metadata?.latest_activity_at || null
    }
  });
  return { ...node, embedding };
}

async function addSimilarityEdges(nodes, threshold = 0.88) {
  if (!Array.isArray(nodes) || nodes.length < 2) return;
  
  // limit comparisons to prevent performance issues
  const pool = nodes.slice(0, 1000);
  for (let i = 0; i < pool.length; i++) {
    for (let j = i + 1; j < pool.length; j++) {
      const a = pool[i];
      const b = pool[j];
      if (!a.embedding || !b.embedding) continue;
      
      const sim = cosineSimilarity(a.embedding, b.embedding);
      if (sim > threshold) {
        await upsertMemoryEdge({
          fromNodeId: a.id,
          toNodeId: b.id,
          edgeType: 'RELATED_TO',
          weight: Number(sim.toFixed(4)),
          traceLabel: `High cosine similarity (${sim.toFixed(3)})`,
          evidenceCount: 1,
          metadata: { similarity: sim, auto_derived: true }
        });
      }
    }
  }
}

async function deriveGraphFromEvents({ eventIds = null, since = null, limit = 800, versionSeed = 'current' } = {}) {
  const version = buildDerivedVersion(versionSeed);
  const rows = await loadEventsForGraph({ eventIds, since, limit });
  const envelopes = rows.map(envelopeFromRow).filter((item) => item && item.id);

  await logGraphVersion(version, 'started', {
    event_ids: eventIds || null,
    since: since || null,
    limit,
    envelope_count: envelopes.length
  });

  await removeMemoryArtifactsByVersion(version);
  const groups = clusterEnvelopes(envelopes);
  const episodes = [];
  const allCreatedNodes = [];

  for (const group of groups) {
    const episode = await writeEpisodeGroup(group, version);
    episodes.push(episode);
    allCreatedNodes.push(episode);
    if (Array.isArray(episode.semanticNodes)) {
      allCreatedNodes.push(...episode.semanticNodes);
    }
  }

  const insights = deriveInsights(episodes);
  const semanticGroups = deriveSemanticGroups(episodes);

  for (const semanticGroup of semanticGroups) {
    const node = await writeHigherLayerNode(semanticGroup, version);
    allCreatedNodes.push(node);
    for (const episodeId of semanticGroup.metadata?.supporting_episode_ids || []) {
      await upsertMemoryEdge({
        fromNodeId: episodeId,
        toNodeId: semanticGroup.id,
        edgeType: 'ABSTRACTED_TO',
        weight: semanticGroup.confidence,
        traceLabel: semanticGroup.title,
        evidenceCount: Number(semanticGroup.metadata?.repeated_count || 1),
        metadata: {
          layer: 'semantic'
        }
      });
    }
  }

  for (const insight of insights) {
    const node = await writeHigherLayerNode(insight, version);
    allCreatedNodes.push(node);
    for (const episodeId of insight.metadata?.supporting_episode_ids || []) {
      await upsertMemoryEdge({
        fromNodeId: episodeId,
        toNodeId: insight.id,
        edgeType: 'ABSTRACTED_TO',
        weight: insight.confidence,
        traceLabel: insight.title,
        evidenceCount: Number(insight.metadata?.repeated_count || 1),
        metadata: {
          layer: 'insight'
        }
      });
    }
    for (const semId of insight.metadata?.supporting_semantic_ids || []) {
      await upsertMemoryEdge({
        fromNodeId: semId,
        toNodeId: insight.id,
        edgeType: 'ABSTRACTED_TO',
        weight: insight.confidence,
        traceLabel: 'Semantic node abstracted to recurring insight pattern',
        evidenceCount: 1,
        metadata: {
          layer: 'insight'
        }
      });
    }
  }

  // Dense connection pass
  await addSimilarityEdges(allCreatedNodes, 0.88);

  await logGraphVersion(version, 'completed', {
    envelope_count: envelopes.length,
    episode_count: episodes.length,
    insight_count: insights.length
  });

  // Ensure all nodes are connected to multiple edges (Minimum 2-3)
  try {
    const candidates = await db.allQuery(`
      SELECT id, layer, subtype, title, summary, canonical_text, metadata, embedding
      FROM memory_nodes m
      WHERE (SELECT COUNT(*) FROM memory_edges e WHERE e.from_node_id = m.id OR e.to_node_id = m.id) < 3
      AND m.graph_version = ?
      LIMIT 300
    `, [version]).catch(() => []);

    if (candidates && candidates.length) {
      const episodeRows = await db.allQuery(`SELECT id, anchor_date, created_at, updated_at, embedding FROM memory_nodes WHERE layer = 'episode'`).catch(() => []);
      for (const node of candidates) {
        try {
          const nodeEmbedding = (() => {
            try { return JSON.parse(node.embedding || '[]'); } catch (_) { return []; }
          })();
          if (!nodeEmbedding.length) continue;

          const episodeScores = episodeRows.map(ep => {
            const epEmbedding = (() => {
              try { return JSON.parse(ep.embedding || '[]'); } catch (_) { return []; }
            })();
            if (!epEmbedding.length) return { ep, sim: 0 };
            return { ep, sim: cosineSimilarity(nodeEmbedding, epEmbedding) };
          }).sort((a, b) => b.sim - a.sim);

          // Link to top 2-3 similar episodes
          const topEpisodes = episodeScores.slice(0, 3).filter(item => item.sim > 0.4);
          for (const item of topEpisodes) {
            await upsertMemoryEdge({
              fromNodeId: node.id,
              toNodeId: item.ep.id,
              edgeType: 'RELATED_TO',
              weight: Number((item.sim * 0.5).toFixed(4)),
              traceLabel: 'Density-pass: linked to similar episode',
              evidenceCount: 1,
              metadata: { density_link: true, similarity: item.sim }
            });
          }
        } catch (e) { /* ignore per-node errors */ }
      }
    }
  } catch (e) {
    console.warn('[graph-derivation] connection density pass failed:', e?.message || e);
  }

  return {
    version,
    envelopeCount: envelopes.length,
    episodeIds: episodes.map((episode) => episode.id),
    insightIds: insights.map((insight) => insight.id)
  };
}

module.exports = {
  GRAPH_VERSION_PREFIX,
  buildDerivedVersion,
  deriveGraphFromEvents,
  envelopeFromRow,
  clusterEnvelopes
};
