const db = require('../db');
const Store = require('electron-store');
const {
  generateFeedSuggestions,
  qualityGateSuggestion,
  isWeakTitle,
  isConcreteActionLabel,
  hasTemplateTone,
  startsWithImperativeVerb
} = require('./feed-generation');
const { upsertRetrievalDoc } = require('./graph-store');
const { callLLM } = require('./intelligence-engine');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');
const { normalizeSuggestion } = require('./intent-first-suggestions');

const store = new Store();
const ACTIVE_SUGGESTION_LIMIT = 7;

async function clearSuggestionArtifacts() {
  await db.runQuery(`DELETE FROM suggestion_artifacts`).catch(() => {});
  const docs = await db.allQuery(`SELECT doc_id FROM retrieval_docs WHERE source_type = 'suggestion'`).catch(() => []);
  for (const row of docs) {
    await db.runQuery(`DELETE FROM retrieval_docs_fts WHERE doc_id = ?`, [row.doc_id]).catch(() => {});
  }
  await db.runQuery(`DELETE FROM retrieval_docs WHERE source_type = 'suggestion'`).catch(() => {});
}

function parseMeta(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

async function fetchActiveSuggestionArtifacts(limit = ACTIVE_SUGGESTION_LIMIT) {
  const rows = await db.allQuery(
    `SELECT id, type, title, body, trigger_summary, source_node_ids, source_edge_paths, confidence, status, metadata, created_at
     FROM suggestion_artifacts
     WHERE status = 'active'
     ORDER BY datetime(created_at) DESC
     LIMIT ?`,
    [limit]
  ).catch(() => []);
  return (rows || []).map((row) => {
    const meta = parseMeta(row.metadata);
    const sourceNodeIds = (() => {
      try { return JSON.parse(row.source_node_ids || '[]'); } catch (_) { return []; }
    })();
    const sourceEdgePaths = (() => {
      try { return JSON.parse(row.source_edge_paths || '[]'); } catch (_) { return []; }
    })();
    return {
      id: row.id,
      type: row.type || 'next_action',
      title: row.title || '',
      body: row.body || '',
      trigger_summary: row.trigger_summary || '',
      source_node_ids: sourceNodeIds,
      source_edge_paths: sourceEdgePaths,
      confidence: Number(row.confidence || 0),
      status: row.status || 'active',
      created_at: row.created_at,
      ...meta
    };
  });
}

function hasConcreteSuggestionAction(text = '') {
  const value = String(text || '').trim();
  if (!value) return false;
  if (!/\b(open|draft|reply|send|prepare|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share|drill|resume|close|start|run|review)\b/i.test(value)) return false;
  return !/\b(review|open)\s+(this|it|item|context|memory|something|task)$/i.test(value);
}

function isActionableSuggestion(item = {}) {
  if (!item || item.completed) return false;
  const title = String(item.title || '').trim();
  const reason = String(item.reason || item.description || item.body || '').trim();
  const primaryLabel = String(item.primary_action?.label || item.recommended_action || '').trim();
  const actions = Array.isArray(item.suggested_actions) ? item.suggested_actions : [];
  const plan = Array.isArray(item.plan) ? item.plan : [];
  const stepPlan = Array.isArray(item.step_plan) ? item.step_plan : [];
  if (!title || !reason) return false;
  if (/\b(take the next step|keep momentum|be proactive|work on this|handle this|make progress|stay on top)\b/i.test(title)) return false;
  if (!hasConcreteSuggestionAction(title) && !hasConcreteSuggestionAction(primaryLabel) && !actions.some((action) => hasConcreteSuggestionAction(action?.label || action?.payload?.action || ''))) return false;
  if (!primaryLabel && !actions.length && !plan.length && !stepPlan.length) return false;
  return true;
}

function suggestionQueueKey(item = {}) {
  const compact = (value) => String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return [
    compact(item.title),
    compact(item.primary_action?.label || item.recommended_action),
    compact(item.type || item.category)
  ].join('|');
}

function rankActiveSuggestions(items = [], limit = ACTIVE_SUGGESTION_LIMIT) {
  const seen = new Set();
  const ranked = [];
  for (const item of Array.isArray(items) ? items : []) {
    if (!isActionableSuggestion(item)) continue;
    const key = suggestionQueueKey(item);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    ranked.push(item);
  }
  const priorityValue = (item) => ({ high: 3, medium: 2, low: 1 }[String(item.priority || 'medium').toLowerCase()] || 2);
  const createdMs = (item) => {
    const value = item?.created_at || item?.createdAt || 0;
    if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
    const parsed = Date.parse(String(value || ''));
    return Number.isFinite(parsed) ? parsed : 0;
  };
  ranked.sort((a, b) => {
    const priorityDelta = priorityValue(b) - priorityValue(a);
    if (priorityDelta) return priorityDelta;
    const scoreDelta = Number(b.score || b.confidence || 0) - Number(a.score || a.confidence || 0);
    if (scoreDelta) return scoreDelta;
    return createdMs(b) - createdMs(a);
  });
  return ranked.slice(0, Math.max(1, Math.min(ACTIVE_SUGGESTION_LIMIT, Number(limit || ACTIVE_SUGGESTION_LIMIT))));
}

async function pruneActiveSuggestionArtifacts(activeIds = []) {
  const keep = new Set((activeIds || []).filter(Boolean).map(String));
  const rows = await db.allQuery(
    `SELECT id FROM suggestion_artifacts WHERE status = 'active' ORDER BY datetime(created_at) DESC`
  ).catch(() => []);
  for (const row of rows || []) {
    if (!keep.has(String(row.id))) {
      await db.runQuery(`UPDATE suggestion_artifacts SET status = 'inactive' WHERE id = ?`, [row.id]).catch(() => {});
    }
  }
}

async function persistSuggestionArtifact(suggestion) {
  await db.runQuery(
    `INSERT OR REPLACE INTO suggestion_artifacts
     (id, type, title, body, trigger_summary, source_node_ids, source_edge_paths, confidence, status, metadata, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      suggestion.id,
      suggestion.type || 'next_action',
      suggestion.title || '',
      suggestion.body || suggestion.description || '',
      suggestion.trigger_summary || '',
      JSON.stringify(Array.isArray(suggestion.source_node_ids) ? suggestion.source_node_ids : []),
      JSON.stringify(Array.isArray(suggestion.source_edge_paths) ? suggestion.source_edge_paths : []),
      Number(suggestion.confidence || 0),
      suggestion.status || 'active',
      JSON.stringify({
        category: suggestion.category || 'work',
        priority: suggestion.priority || 'medium',
        intent: suggestion.intent || '',
        reason: suggestion.reason || '',
        display: suggestion.display || null,
        epistemic_trace: Array.isArray(suggestion.epistemic_trace) ? suggestion.epistemic_trace : [],
        suggested_actions: Array.isArray(suggestion.suggested_actions) ? suggestion.suggested_actions : [],
        suggestion_id: suggestion.suggestion_id || suggestion.id,
        opportunity_type: suggestion.opportunity_type || null,
        reason_codes: Array.isArray(suggestion.reason_codes) ? suggestion.reason_codes : [],
        time_anchor: suggestion.time_anchor || null,
        candidate_score: Number(suggestion.candidate_score || 0),
        provider: suggestion.provider || null,
        social_tier: suggestion.social_tier || null,
        social_temperature: Number(suggestion.social_temperature || 0),
        sentiment_gradient: suggestion.sentiment_gradient || null,
        value_hook: suggestion.value_hook || null,
        outreach_options: Array.isArray(suggestion.outreach_options) ? suggestion.outreach_options : [],
        social_strategy: suggestion.social_strategy || null,
        relationship_contact_id: suggestion.relationship_contact_id || null,
        relationship_status: suggestion.relationship_status || null,
        relationship_score_inputs: suggestion.relationship_score_inputs || null,
        draft_context_refs: Array.isArray(suggestion.draft_context_refs) ? suggestion.draft_context_refs : [],
        primary_action: suggestion.primary_action || null,
        ai_generated: Boolean(suggestion.ai_generated),
        ai_doable: Boolean(suggestion.ai_doable),
        action_type: suggestion.action_type || null,
        execution_mode: suggestion.execution_mode || (suggestion.ai_doable ? 'draft_or_execute' : 'manual'),
        target_surface: suggestion.target_surface || null,
        prerequisites: Array.isArray(suggestion.prerequisites) ? suggestion.prerequisites : [],
        assignee: suggestion.assignee || (suggestion.ai_doable ? 'ai' : 'human'),
        plan: Array.isArray(suggestion.plan) ? suggestion.plan : [],
        step_plan: Array.isArray(suggestion.step_plan) ? suggestion.step_plan : [],
        expected_benefit: suggestion.expected_benefit || '',
        ai_draft: suggestion.ai_draft || '',
        action_plan: Array.isArray(suggestion.action_plan) ? suggestion.action_plan : [],
        retrieval_trace: suggestion.retrieval_trace || null
      }),
      suggestion.created_at || new Date().toISOString()
    ]
  );

  await upsertRetrievalDoc({
    docId: `suggestion:${suggestion.id}`,
    sourceType: 'suggestion',
    nodeId: suggestion.id,
    timestamp: suggestion.created_at || new Date().toISOString(),
    text: [
      suggestion.title,
      suggestion.body,
      suggestion.trigger_summary,
      suggestion.reason,
      suggestion.expected_benefit,
      suggestion.provider ? `provider: ${suggestion.provider}` : '',
      suggestion.social_tier ? `social tier: ${suggestion.social_tier}` : '',
      suggestion.social_temperature ? `social temperature: ${Number(suggestion.social_temperature).toFixed(2)}` : '',
      suggestion.value_hook ? JSON.stringify(suggestion.value_hook) : '',
      Array.isArray(suggestion.outreach_options) ? suggestion.outreach_options.map((o) => o.label || o.type).join(' | ') : '',
      suggestion.action_type,
      suggestion.ai_doable ? 'AI can do' : 'Manual task',
      (suggestion.plan || []).join('\n'),
      (suggestion.step_plan || []).join('\n'),
      suggestion.ai_draft || ''
    ].filter(Boolean).join('\n'),
    metadata: {
      layer: 'suggestion',
      subtype: suggestion.type || 'next_action',
      source_refs: suggestion.source_node_ids || [],
      provider: suggestion.provider || null,
      social_tier: suggestion.social_tier || null,
      social_temperature: Number(suggestion.social_temperature || 0),
      value_hook: suggestion.value_hook || null,
      outreach_options: Array.isArray(suggestion.outreach_options) ? suggestion.outreach_options : [],
      relationship_contact_id: suggestion.relationship_contact_id || null,
      relationship_status: suggestion.relationship_status || null,
      relationship_score_inputs: suggestion.relationship_score_inputs || null,
      draft_context_refs: Array.isArray(suggestion.draft_context_refs) ? suggestion.draft_context_refs : [],
      ai_doable: Boolean(suggestion.ai_doable),
      action_type: suggestion.action_type || null,
      execution_mode: suggestion.execution_mode || (suggestion.ai_doable ? 'draft_or_execute' : 'manual'),
      target_surface: suggestion.target_surface || null,
      assignee: suggestion.assignee || (suggestion.ai_doable ? 'ai' : 'human')
    }
  });
}

async function runSuggestionEngine(apiKey, options = {}) {
  const now = Date.now();
  const existingActive = rankActiveSuggestions(await fetchActiveSuggestionArtifacts(ACTIVE_SUGGESTION_LIMIT * 2), ACTIVE_SUGGESTION_LIMIT);
  let suggestions = await generateFeedSuggestions(apiKey, now, {
    ...options,
    retry_round: 0
  });
  if (!Array.isArray(suggestions) || !suggestions.length) {
    suggestions = await generateFeedSuggestions(apiKey, Date.now(), {
      ...options,
      retry_round: 1
    });
  }
  if (!Array.isArray(suggestions) || !suggestions.length) {
    await pruneActiveSuggestionArtifacts(existingActive.map((item) => item.id));
    return existingActive;
  }
  const incoming = rankActiveSuggestions(suggestions, ACTIVE_SUGGESTION_LIMIT);
  for (const suggestion of incoming) {
    await persistSuggestionArtifact(suggestion);
  }
  const active = rankActiveSuggestions([...incoming, ...existingActive], ACTIVE_SUGGESTION_LIMIT);
  await pruneActiveSuggestionArtifacts(active.map((item) => item.id));
  return active;
}

module.exports = {
  runSuggestionEngine,
  isActionableSuggestion,
  rankActiveSuggestions
};

function firstEvidenceLine(evidence = []) {
  const item = (Array.isArray(evidence) ? evidence : []).find(Boolean);
  if (!item) return '';
  return String(item.text || item.title || item.id || '').trim().slice(0, 180);
}

function toObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function parseTs(value) {
  if (!value && value !== 0) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : 0;
}

function tokenizeTerms(text = '') {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9._/-]+/g, ' ')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter((item) => item.length >= 3)
    .slice(0, 18);
}

function termHits(text = '', terms = []) {
  if (!terms.length) return 0;
  const hay = String(text || '').toLowerCase();
  let hits = 0;
  for (const term of terms) {
    if (hay.includes(term)) hits += 1;
  }
  return hits;
}

function layerWeight(layer = '', subtype = '') {
  const l = String(layer || '').toLowerCase();
  const s = String(subtype || '').toLowerCase();
  if (l === 'core') return 1.35;
  if (l === 'semantic' && s === 'task') return 1.24;
  if (l === 'semantic') return 1.15;
  if (l === 'insight') return 1.1;
  if (l === 'cloud') return 1.0;
  if (l === 'episode') return 0.95;
  return 0.82;
}

function toEvidenceText(item = {}) {
  const title = String(item.title || '').trim();
  const summary = String(item.summary || '').trim();
  const canonical = String(item.canonical_text || '').trim();
  return [title, summary, canonical].filter(Boolean).join(' — ').slice(0, 240);
}

function actionLabelForTitle(title = '') {
  const t = String(title || '').trim();
  if (isConcreteActionLabel(t)) return t;
  const clipped = t.split(/\s+/).slice(0, 7).join(' ');
  return `Review ${clipped || 'memory context'}`;
}

function normalizeSuggestionType(value = '', fallbackText = '') {
  const raw = String(value || '').toLowerCase().trim();
  const valid = ['study', 'relationship', 'work', 'personal', 'creative', 'followup'];
  if (valid.includes(raw)) return raw;
  const hay = `${raw} ${String(fallbackText || '').toLowerCase()}`;
  if (/\bstudy|quiz|exam|class|assignment|homework|lecture|review|flashcard|vocab\b/.test(hay)) return 'study';
  if (/\brelationship|follow ?up|reply|check-?in|reconnect|birthday|anniversary|friend|mentor|alex|maya|sam|leo\b/.test(hay)) return 'followup';
  if (/\bwork|project|client|meeting|presentation|proposal|deadline|task|job\b/.test(hay)) return 'work';
  if (/\bpersonal|home|health|fitness|hobby|family|bill|shopping\b/.test(hay)) return 'personal';
  if (/\bcreative|design|writing|art|music|video|ideation|brainstorm\b/.test(hay)) return 'creative';
  return 'work';
}

function normalizeTimeAnchor(value = '') {
  const v = String(value || '').trim().toLowerCase();
  if (!v) return 'today';
  if (/before/.test(v)) return v;
  if (/today|tonight|now|this week|tomorrow/.test(v)) return v;
  return 'today';
}

function computeExpiresAt(timeAnchor = '') {
  const now = Date.now();
  const anchor = String(timeAnchor || '').toLowerCase();
  const oneHour = 60 * 60 * 1000;
  const oneDay = 24 * oneHour;
  if (/\bnow\b/.test(anchor)) return new Date(now + (8 * oneHour)).toISOString();
  if (/\bbefore\b.*\btomorrow\b/.test(anchor) || /\btomorrow\b/.test(anchor)) return new Date(now + oneDay).toISOString();
  if (/\bthis week\b/.test(anchor)) return new Date(now + (3 * oneDay)).toISOString();
  return new Date(now + oneDay).toISOString();
}

function cleanSingleActionTitle(title = '') {
  const raw = String(title || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  const first = raw.split(/\s*(?:\band then\b|\bthen\b|;|\|)\s*/i)[0].trim();
  return first.slice(0, 120);
}

async function buildSuggestionEvidenceBundle(anchorEvidence = null, query = '') {
  const baseIds = new Set();
  const lines = [];
  const addLine = (text) => {
    const line = String(text || '').replace(/\s+/g, ' ').trim();
    if (!line) return;
    if (!lines.includes(line)) lines.push(line);
  };

  if (anchorEvidence?.id) baseIds.add(String(anchorEvidence.id));

  let sourceRefs = [];
  if (anchorEvidence?.id && String(anchorEvidence.layer || '').toLowerCase() !== 'raw') {
    const nodeRow = await db.getQuery(
      `SELECT source_refs, metadata, title, summary, layer, subtype
       FROM memory_nodes
       WHERE id = ?
       LIMIT 1`,
      [String(anchorEvidence.id)]
    ).catch(() => null);
    if (nodeRow) {
      try { sourceRefs = JSON.parse(nodeRow.source_refs || '[]'); } catch (_) { sourceRefs = []; }
      sourceRefs.slice(0, 6).forEach((id) => baseIds.add(String(id)));
      addLine(`${nodeRow.layer}${nodeRow.subtype ? `/${nodeRow.subtype}` : ''}: ${String(nodeRow.title || nodeRow.summary || '').slice(0, 160)}`);

      const related = await db.allQuery(
        `SELECT n.id, n.layer, n.subtype, n.title, n.summary
         FROM memory_edges e
         JOIN memory_nodes n ON (n.id = e.from_node_id OR n.id = e.to_node_id)
         WHERE (e.from_node_id = ? OR e.to_node_id = ?)
           AND n.id != ?
         ORDER BY e.weight DESC, e.evidence_count DESC
         LIMIT 5`,
        [String(anchorEvidence.id), String(anchorEvidence.id), String(anchorEvidence.id)]
      ).catch(() => []);
      for (const row of related || []) {
        addLine(`Related ${row.layer}${row.subtype ? `/${row.subtype}` : ''}: ${String(row.title || row.summary || '').slice(0, 140)}`);
      }
    }
  }

  const idList = Array.from(baseIds).slice(0, 18);
  if (idList.length) {
    const placeholders = idList.map(() => '?').join(',');
    const rows = await db.allQuery(
      `SELECT id, type, source, title, text, timestamp
       FROM events
       WHERE id IN (${placeholders})
       ORDER BY COALESCE(timestamp, occurred_at) DESC
       LIMIT 8`,
      idList
    ).catch(() => []);
    for (const row of rows || []) {
      addLine(`Event ${row.type || 'activity'}: ${String(row.title || row.text || '').slice(0, 160)}`);
    }
  }

  return {
    evidence_ids: Array.from(baseIds).slice(0, 8),
    evidence_lines: lines.slice(0, 7)
  };
}

async function ensureMemoryLayersReady(apiKey) {
  const countsFromDb = async () => {
    const rows = await db.allQuery(
      `SELECT layer, COUNT(*) AS count
       FROM memory_nodes
       GROUP BY layer`
    ).catch(() => []);
    return (rows || []).reduce((acc, row) => {
      acc[String(row.layer || '').toLowerCase()] = Number(row.count || 0);
      return acc;
    }, {});
  };

  let counts = await countsFromDb();
  const episodeCount = Number(counts.episode || 0);
  const semanticCount = Number(counts.semantic || 0);
  const insightCount = Number(counts.insight || 0);
  const cloudCount = Number(counts.cloud || 0);

  const now = Date.now();
  const lastHydrationAt = Number(store.get('memoryLayerHydrationAt') || 0);
  const shouldHydrateEpisodes = (episodeCount < 3 || semanticCount < 5) && (now - lastHydrationAt > (10 * 60 * 1000));
  if (shouldHydrateEpisodes) {
    try {
      const { runEpisodeJob } = require('./intelligence-engine');
      await runEpisodeJob();
      store.set('memoryLayerHydrationAt', now);
      counts = await countsFromDb();
    } catch (_) {}
  }

  const lastInsightAt = Number(store.get('memoryInsightHydrationAt') || 0);
  const shouldHydrateInsights = apiKey && insightCount < 2 && (cloudCount > 0 || semanticCount > 10) && (now - lastInsightAt > (6 * 60 * 60 * 1000));
  if (shouldHydrateInsights) {
    try {
      const { runWeeklyInsightJob } = require('./intelligence-engine');
      await runWeeklyInsightJob(apiKey);
      store.set('memoryInsightHydrationAt', now);
      counts = await countsFromDb();
    } catch (_) {}
  }

  return counts;
}

async function retrieveCoreToFactsContext(query = '', maxNodes = 100) {
  const terms = tokenizeTerms(query);
  const coreNodes = await db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, confidence, metadata, updated_at
     FROM memory_nodes
     WHERE layer = 'core' OR id = 'global_core'
     ORDER BY confidence DESC, datetime(updated_at) DESC
     LIMIT 4`
  ).catch(() => []);

  const startRows = coreNodes.length
    ? coreNodes
    : await db.allQuery(
      `SELECT id, layer, subtype, title, summary, canonical_text, confidence, metadata, updated_at
       FROM memory_nodes
       WHERE layer IN ('semantic', 'insight')
       ORDER BY confidence DESC, datetime(updated_at) DESC
       LIMIT 4`
    ).catch(() => []);

  const seen = new Set(startRows.map((row) => row.id).filter(Boolean));
  const depthById = new Map(startRows.map((row) => [row.id, 0]));
  let frontier = startRows.map((row) => row.id).filter(Boolean);
  const edgeTrace = [];

  for (let depth = 1; depth <= 3 && frontier.length; depth++) {
    const placeholders = frontier.map(() => '?').join(',');
    const edges = await db.allQuery(
      `SELECT from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, created_at
       FROM memory_edges
       WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})
       ORDER BY weight DESC, evidence_count DESC
       LIMIT 320`,
      [...frontier, ...frontier]
    ).catch(() => []);
    const next = [];
    for (const edge of edges || []) {
      const from = edge.from_node_id;
      const to = edge.to_node_id;
      if (!from || !to) continue;
      const fromSeen = seen.has(from);
      const toSeen = seen.has(to);
      const neighbor = fromSeen && !toSeen ? to : (toSeen && !fromSeen ? from : null);
      if (neighbor && !seen.has(neighbor)) {
        seen.add(neighbor);
        depthById.set(neighbor, depth);
        next.push(neighbor);
      }
      const edgeText = `${edge.edge_type || ''} ${edge.trace_label || ''}`;
      const hitBoost = termHits(edgeText, terms);
      edgeTrace.push({
        from,
        to,
        relation: edge.edge_type || 'RELATES_TO',
        trace_label: edge.trace_label || '',
        depth,
        score: Number(edge.weight || 0) + Number(edge.evidence_count || 0) * 0.05 + hitBoost * 0.1
      });
    }
    frontier = Array.from(new Set(next)).slice(0, 120);
    if (seen.size >= maxNodes) break;
  }

  const nodeIds = Array.from(seen).slice(0, maxNodes);
  if (!nodeIds.length) {
    return {
      evidence: [],
      edge_paths: [],
      strategy: { strategy_mode: 'core_first_graph_walk' },
      retrieval_run_id: null
    };
  }

  const placeholders = nodeIds.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, confidence, metadata, updated_at
     FROM memory_nodes
     WHERE id IN (${placeholders})`,
    nodeIds
  ).catch(() => []);

  const scored = (rows || []).map((row) => {
    const depth = Number(depthById.get(row.id) || 3);
    const meta = toObj(row.metadata);
    const hay = `${row.title || ''}\n${row.summary || ''}\n${row.canonical_text || ''}\n${JSON.stringify(meta || {})}`;
    const hits = termHits(hay, terms);
    const freshnessRaw = parseTs(row.updated_at || meta.latest_activity_at || meta.anchor_at || 0);
    const freshnessDays = freshnessRaw ? (Date.now() - freshnessRaw) / (24 * 60 * 60 * 1000) : 99;
    const freshnessBoost = freshnessDays <= 2 ? 0.12 : (freshnessDays <= 7 ? 0.06 : 0);
    const score = layerWeight(row.layer, row.subtype) + hits * 0.18 + Number(row.confidence || 0) * 0.22 + freshnessBoost - depth * 0.08;
    return {
      ...row,
      depth,
      score,
      text: toEvidenceText(row),
      timestamp: row.updated_at || meta.latest_activity_at || meta.anchor_at || null
    };
  }).sort((a, b) => (b.score || 0) - (a.score || 0));

  const quotas = {
    core: 2,
    semantic: 12,
    insight: 8,
    cloud: 5,
    episode: 12
  };
  const used = { core: 0, semantic: 0, insight: 0, cloud: 0, episode: 0 };
  const chosen = [];
  for (const row of scored) {
    const layer = String(row.layer || '').toLowerCase();
    const quota = quotas[layer];
    if (Number.isFinite(quota)) {
      if (used[layer] >= quota) continue;
      used[layer] += 1;
    } else if (chosen.length >= 24) {
      continue;
    }
    chosen.push(row);
    if (chosen.length >= 30) break;
  }

  const chosenSet = new Set(chosen.map((r) => r.id));
  const chosenEdges = edgeTrace
    .filter((edge) => chosenSet.has(edge.from) && chosenSet.has(edge.to))
    .sort((a, b) => (b.score || 0) - (a.score || 0))
    .slice(0, 20);

  return {
    evidence: chosen.map((row) => ({
      id: row.id,
      node_id: row.id,
      layer: row.layer,
      type: row.layer,
      subtype: row.subtype || null,
      score: Number((row.score || 0).toFixed(6)),
      text: row.text || row.title || row.id,
      title: row.title || '',
      latest_activity_at: row.timestamp || null,
      timestamp: row.timestamp || null
    })),
    edge_paths: chosenEdges,
    strategy: {
      strategy_mode: 'core_first_graph_walk',
      summary_vs_raw: 'summary',
      time_scope: 'all_time'
    },
    retrieval_run_id: null
  };
}

async function generateTopTodosFromMemoryQuery(llmConfig, options = {}) {
  const now = Date.now();
  const query = String(
    options?.query ||
    'Look through my memory and generate the top 7 todos or actions I need to do right now.'
  ).trim();
  const layerCounts = await ensureMemoryLayersReady((llmConfig && llmConfig.provider === 'deepseek') ? llmConfig.apiKey : null).catch(() => ({}));
  
  const [coreRetrieval, branchRetrieval] = await Promise.all([
    retrieveCoreToFactsContext(query, 100).catch(() => null),
    buildHybridGraphRetrieval({
      query,
      options: {
        mode: 'suggestion',
        strategy: 'spiral'
      },
      seedLimit: 20,
      hopLimit: 8
    }).catch(() => null)
  ]);

  const mergedEvidence = [];
  const seenEvidence = new Set();
  const pushEvidence = (item) => {
    const key = `${item?.node_id || item?.id || ''}:${item?.layer || item?.type || ''}`;
    if (!key || seenEvidence.has(key)) return;
    seenEvidence.add(key);
    mergedEvidence.push(item);
  };
  (coreRetrieval?.evidence || []).forEach(pushEvidence);
  (branchRetrieval?.evidence || []).forEach(pushEvidence);
  if (!mergedEvidence.length) {
    const rawFallback = await db.allQuery(
      `SELECT id, type, title, text, timestamp
       FROM events
       ORDER BY datetime(timestamp) DESC
       LIMIT 16`
    ).catch(() => []);
    for (const row of rawFallback || []) {
      pushEvidence({
        id: row.id,
        node_id: null,
        layer: 'raw',
        type: 'raw',
        subtype: row.type || null,
        score: 0.45,
        text: String(row.title || row.text || '').slice(0, 220),
        title: String(row.title || '').slice(0, 140),
        latest_activity_at: row.timestamp || null,
        timestamp: row.timestamp || null
      });
    }
  }
  const evidence = mergedEvidence.slice(0, 24);
  const edgePaths = [
    ...(Array.isArray(coreRetrieval?.edge_paths) ? coreRetrieval.edge_paths : []),
    ...(Array.isArray(branchRetrieval?.edge_paths) ? branchRetrieval.edge_paths : [])
  ].slice(0, 24);

  const evidenceDigest = evidence.map((item, idx) => {
    const layer = item.layer || item.type || 'memory';
    const subtype = item.subtype ? `/${item.subtype}` : '';
    const when = item.latest_activity_at || item.anchor_at || item.timestamp || '';
    return `${idx + 1}. [${layer}${subtype}] ${String(item.text || item.title || '').slice(0, 220)}${when ? ` (${when})` : ''}`;
  }).join('\n');
  const edgeDigest = edgePaths.map((edge) => {
    return `- ${edge.from || edge.from_node_id} -> ${edge.to || edge.to_node_id} via ${edge.relation || edge.edge_type}${edge.trace_label ? ` (${edge.trace_label})` : ''}`;
  }).join('\n');

  const standingNotes = String(options?.standing_notes || '').trim();
  const phase1Prompt = `
  You are an Action-Oriented Planner.
  Your goal is to identify highly actionable, concrete to-dos from the user's memory.
  First ask memory this question: "What are the top seven specific things to do now?"
  Then return exactly 7 highly specific to-do items as a strict JSON array of plain strings.

  Return strict JSON array of strings only:
  ["Action 1", "Action 2", "Action 3", "Action 4", "Action 5", "Action 6", "Action 7"]

  RULES:
  - Use only STANDING NOTES + MEMORY EVIDENCE + GRAPH EDGES.
  - Every to-do MUST be concrete and imperative.
  - Every to-do MUST reference a specific person, file, event, or artifact from the evidence.
  - Avoid generic advice or "considerations."

  STANDING NOTES:
  ${standingNotes || 'None'}

  MEMORY LAYER COUNTS:
  ${JSON.stringify(layerCounts || {})}

  MEMORY EVIDENCE:
  ${evidenceDigest || 'No evidence available.'}

  GRAPH EDGES:
  ${edgeDigest || 'No edge traces available.'}
  `;

  const rawTopFive = await callLLM(phase1Prompt, llmConfig, 0.22, { maxTokens: 450, economy: true, task: "suggestion" }).catch(() => null);
  const topFiveItems = Array.isArray(rawTopFive) ? rawTopFive.filter(i => typeof i === 'string') : [];

  if (!topFiveItems.length) return [];

  const phase2Prompt = `
  I asked memory for top 7 concrete todos:
  ${JSON.stringify(topFiveItems)}

  Generate proactive suggestions. Up to 8 candidates.
  Format JSON array: [{"type": "work|followup|study|personal|creative|relationship", "title": "imperative", "reason": "why now", "description": "", "outcome": "", "evidence": ["id"], "time_anchor": "today|now", "priority": "low|medium|high", "confidence": 0.0, "primary_action": "label", "secondary_action": "", "source_index": 1, "expires_at": "ISO"}]

  Rules:
  - Title MUST start with verb + specific entity/time.
  - NEVER start with "Take the next step", "Review", "Update".
  - reason MUST reference evidence.
  `;

  const aiRows = await callLLM(phase2Prompt, llmConfig, 0.22, { maxTokens: 500, economy: true, task: "suggestion" }).catch(() => null);
  const rows = Array.isArray(aiRows) ? aiRows.slice(0, 10) : [];
  const selected = rows.filter((row) => !isWeakTitle(row?.title || ''));
  if (!selected.length) return [];

  const built = await Promise.all(selected.map(async (raw, index) => {
    const sourceIndex = Math.max(1, Math.min(evidence.length || 1, Number(raw?.source_index || (index + 1))));
    const anchorEvidence = evidence[sourceIndex - 1] || evidence[index] || null;
    const enrichedEvidence = await buildSuggestionEvidenceBundle(anchorEvidence, raw?.title || raw?.reason || query);
    const normalized = normalizeSuggestion({
      title: cleanSingleActionTitle(raw?.title || ''),
      description: raw?.description || '',
      reason: raw?.reason || '',
      category: normalizeSuggestionType(raw?.type || raw?.category || '', `${raw?.title || ''} ${raw?.reason || ''}`),
      priority: raw?.priority || 'medium',
      confidence: Number(raw?.confidence || 0.58),
      created_at: new Date(now).toISOString()
    }, { now });
    const actionLabelCandidate = String(raw?.primary_action || raw?.action_label || '').trim();
    const actionLabel = isConcreteActionLabel(actionLabelCandidate)
      ? actionLabelCandidate
      : actionLabelForTitle(normalized.title || 'memory context');
    const traceText = String(anchorEvidence?.text || '').trim().slice(0, 180) || firstEvidenceLine(evidence);
    const strategyMode = coreRetrieval?.strategy?.strategy_mode || branchRetrieval?.strategy?.strategy_mode || 'core_first_graph_walk';
    const suggestionType = normalizeSuggestionType(raw?.type || raw?.category || '', `${normalized.title} ${normalized.reason}`);
    const timeAnchor = normalizeTimeAnchor(raw?.time_anchor || 'today');
    const expiresAt = String(raw?.expires_at || '').trim() || computeExpiresAt(timeAnchor);
    const evidenceIds = Array.isArray(raw?.evidence) ? raw.evidence.map((id) => String(id || '').trim()).filter(Boolean) : [];
    const mergedEvidenceIds = Array.from(new Set([
      ...(anchorEvidence?.id ? [String(anchorEvidence.id)] : []),
      ...evidenceIds,
      ...(enrichedEvidence?.evidence_ids || [])
    ])).slice(0, 8);
    const evidenceLine = (enrichedEvidence?.evidence_lines || [traceText]).filter(Boolean).slice(0, 2).join(' • ');
    const outcome = String(raw?.outcome || '').trim().slice(0, 160);

    return {
      id: normalized.id || `sug_memory_${now}_${index}`,
      type: suggestionType,
      title: normalized.title,
      description: normalized.description || normalized.intent || '',
      reason: normalized.reason || 'Because current memory signals show this specific action is due now.',
      outcome,
      category: (suggestionType === 'relationship' || suggestionType === 'followup') ? 'followup' : suggestionType,
      priority: normalized.priority || 'medium',
      confidence: Number(normalized.confidence || 0.58),
      time_anchor: timeAnchor,
      expires_at: expiresAt,
      evidence: mergedEvidenceIds,
      evidence_line: evidenceLine,
      display: {
        headline: normalized.title || `Action ${index + 1}`,
        summary: (normalized.reason || normalized.description || '').slice(0, 180),
        insight: `Grounded in memory retrieval (${strategyMode}) with core-to-facts traversal.`
      },
      epistemic_trace: [
        {
          node_id: String(anchorEvidence?.id || `memory_${index + 1}`),
          source: String(anchorEvidence?.layer || anchorEvidence?.type || 'Memory'),
          text: traceText || 'Retrieved memory signal',
          timestamp: anchorEvidence?.latest_activity_at || anchorEvidence?.timestamp || new Date(now).toISOString()
        },
        {
          node_id: `retrieval_${branchRetrieval?.retrieval_run_id || now}`,
          source: 'Core-First + Hybrid Retrieval',
          text: `Started from core memory, traversed edges, then expanded by query: ${query}`,
          timestamp: new Date(now).toISOString()
        }
      ],
      suggested_actions: [
        {
          label: actionLabel,
          type: 'manual',
          payload: { action: 'open_memory_timeline' }
        }
      ],
      primary_action: {
        label: actionLabel,
        type: 'manual',
        payload: { action: 'open_memory_timeline' }
      },
      secondary_action: String(raw?.secondary_action || '').trim() || null,
      ai_generated: true,
      ai_doable: false,
      action_type: (suggestionType === 'relationship' || suggestionType === 'followup') ? 'followup_review' : `${suggestionType}_review`,
      execution_mode: 'manual',
      assignee: 'human',
      source: 'memory-query-top5',
      retrieval_trace: {
        retrieval_run_id: branchRetrieval?.retrieval_run_id || null,
        strategy_mode: strategyMode,
        evidence_count: Number(evidence.length || 0),
        layer_counts: layerCounts
      },
      completed: false,
      created_at: new Date(now).toISOString()
    };
  }));

  const filteredBuilt = built
    .filter((item) => item?.title && item?.reason)
    .filter((item) => !isWeakTitle(item.title) && startsWithImperativeVerb(item.title))
    .filter((item) => {
      // Check for specificity anchor in title
      return /[A-Z][a-z]{2,}/.test(item.title) || /[\d]{1,2}:\d{2}/.test(item.title) || /\.[a-z]{2,4}\b/i.test(item.title);
    })
    .filter((item) => Array.isArray(item.evidence) && item.evidence.length > 0)
    .slice(0, 7);

  return filteredBuilt;
}

module.exports.generateTopTodosFromMemoryQuery = generateTopTodosFromMemoryQuery;

async function generateAndPersistTasksFromLLM(llmConfig, options = {}) {
  if (!llmConfig) {
    console.warn('[TaskLLM] No LLM config provided; skipping LLM task generation');
    return [];
  }
  const recentMemoryRows = await db.allQuery(
    `SELECT layer, title, summary, updated_at
     FROM memory_nodes
     WHERE layer IN ('episode', 'task', 'insight')
     ORDER BY datetime(updated_at) DESC
     LIMIT 24`
  ).catch(() => []);
  const memoryContext = (Array.isArray(recentMemoryRows) ? recentMemoryRows : [])
    .map((row) => {
      const layer = String(row.layer || 'memory');
      const title = String(row.title || '').trim();
      const summary = String(row.summary || '').trim();
      const updatedAt = String(row.updated_at || '').trim();
      const details = [title, summary].filter(Boolean).join(' — ');
      return details ? `- [${layer}] ${details}${updatedAt ? ` (${updatedAt})` : ''}` : '';
    })
    .filter(Boolean)
    .slice(0, 18)
    .join('\n');
  const standingNotes = String(options?.standing_notes || '').trim();
  const prompt = `You are a proactive Action-Oriented Planner.
First ask memory: "what are concrete todos that are still open right now?"
Then return a JSON array of 3 to 5 highly actionable tasks that should be done next.

Rules:
- Use only evidence from MEMORY CONTEXT and STANDING NOTES.
- Prefer unresolved or repeated items with concrete next actions.
- Keep titles imperative and specific.
- Every title MUST name a concrete target (person, file, artifact, or topic).
- Every reason MUST reference specific evidence from the context.
- Avoid weak language (consider, might, maybe).
- Each item must be an object with keys:
  title (short imperative),
  description (optional),
  reason (one sentence grounded in memory),
  time_anchor (optional, e.g. "now" or "today 10:00"),
  category (optional: work|followup|study|personal|creative|relationship),
  priority (optional: low|medium|high).
- Return strict JSON only.

STANDING NOTES:
${standingNotes || 'None'}

MEMORY CONTEXT:
${memoryContext || '- No memory context available; return []'} `;
  const payload = await callLLM(prompt, llmConfig, 0.24, { maxTokens: 550, economy: true, task: "suggestion" }).catch(() => null);
  const rows = Array.isArray(payload) ? payload : [];
  if (!rows.length) return [];

  // Normalize into persistent todo shape
  const persistentTodos = store.get('persistentTodos') || [];
  const candidates = rows.slice(0, 7)
    .filter((r) => {
      const title = String(r.title || r.task || r.action || '').trim();
      return title && !isWeakTitle(title) && startsWithImperativeVerb(title);
    })
    .map((r) => {
      const title = String(r.title || r.task || r.action || '').trim();
      return {
      id: `todo_llm_${Math.random().toString(36).slice(2, 9)}`,
      title: title || 'Task',
      description: String(r.description || '').trim(),
      reason: String(r.reason || '').trim(),
      time_anchor: r.time_anchor || r.time || null,
      priority: r.priority || 'medium',
      category: r.category || 'work',
      createdAt: Date.now(),
      completed: false,
      source: 'llm_proactive'
    };
  });

  // Deduplicate by title (simple) using existing deduplicateTasks function in main; reimplement lightweight here
  function dedupeLocal(arr) {
    const seen = new Set();
    const out = [];
    for (const t of arr) {
      const key = String((t.title || '')).toLowerCase().replace(/[^a-z0-9]+/g, '').trim();
      if (!key) continue;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(t);
    }
    return out;
  }

  const merged = dedupeLocal([...(persistentTodos || []), ...candidates]);
  const capped = merged.slice(0, 100);
  store.set('persistentTodos', capped);
  console.log('[TaskLLM] Persisted', candidates.length, 'LLM-generated tasks');
  return candidates;
}

module.exports.generateAndPersistTasksFromLLM = generateAndPersistTasksFromLLM;
