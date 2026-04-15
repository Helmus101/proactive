const db = require('../db');
const { generateEmbedding, cosineSimilarity } = require('../embedding-engine');
const {
  buildRetrievalThought,
  summarizeRetrievalThought
} = require('./retrieval-thought-system');
const {
  asObj,
  logRetrievalRun
} = require('./graph-store');

const DEFAULT_SEED_LIMIT = 5;
const DEFAULT_HOP_LIMIT = 2;
const MAX_EXPANDED = 10;

const LAYER_RANKS = {
  'core': 5,
  'insight': 4,
  'cloud': 3,
  'semantic': 2,
  'episode': 1,
  'raw': 0,
  'event': 0
};

const EDGE_WEIGHTS = {
  'PROMOTED_FROM': 1.2,
  'ABSTRACTED_TO': 1.2,
  'PART_OF_EPISODE': 1.0,
  'MENTIONS': 0.8,
  'GENERATED_FROM': 0.8,
  'RELATED_TO': 0.5,
  'FOLLOWS_UP': 0.5
};

function parseTs(value) {
  if (!value && value !== 0) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : 0;
}

function sortKeyForRow(row) {
  return parseTs(row.anchor_at || row.timestamp || row.occurred_at);
}

function normalizeDateRange(dateRange) {
  if (!dateRange || typeof dateRange !== 'object') return null;
  const start = dateRange.start ? new Date(dateRange.start) : null;
  const end = dateRange.end ? new Date(dateRange.end) : null;
  if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null;
  return { start, end };
}

function rowMatchesFilters(row, filters = {}) {
  const appFilter = Array.isArray(filters.app) ? filters.app : (filters.app ? [filters.app] : []);
  if (appFilter.length) {
    const app = String(row.app || '').toLowerCase();
    if (!appFilter.some((item) => app.includes(String(item || '').toLowerCase()))) return false;
  }

  const sourceTypeFilter = Array.isArray(filters.source_types)
    ? filters.source_types
    : (filters.source_types ? [filters.source_types] : []);
  if (sourceTypeFilter.length) {
    const hay = `${row.source_type || ''} ${row.layer || ''} ${row.subtype || ''} ${row.source_type_group || ''}`.toLowerCase();
    if (!sourceTypeFilter.some((item) => hay.includes(String(item || '').toLowerCase()))) return false;
  }

  const dateRange = normalizeDateRange(filters.date_range);
  if (dateRange) {
    const ts = sortKeyForRow(row);
    if (!ts || ts < dateRange.start.getTime() || ts > dateRange.end.getTime()) return false;
  }
  return true;
}

function reciprocalRankFusion(rankings, k = 60) {
  const scores = new Map();
  for (const ranking of rankings) {
    ranking.forEach((row, index) => {
      const key = row.key;
      const prev = scores.get(key) || { score: 0, row };
      prev.score += 1 / (k + index + 1);
      if ((row.base_score || 0) > (prev.row.base_score || 0)) prev.row = row;
      scores.set(key, prev);
    });
  }
  return Array.from(scores.values())
    .map((item) => ({
      ...item.row,
      fused_score: Number(item.score.toFixed(6))
    }))
    .sort((a, b) => {
      if ((b.fused_score || 0) !== (a.fused_score || 0)) return (b.fused_score || 0) - (a.fused_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    });
}

function sourceAgreementBonus(row, preferred = []) {
  if (!preferred.length) return 0;
  const hay = `${row.source_type || ''} ${row.layer || ''} ${row.subtype || ''} ${row.source_type_group || ''}`.toLowerCase();
  return preferred.some((item) => hay.includes(String(item || '').toLowerCase())) ? 0.08 : 0;
}

function countExactTermHits(text, terms = []) {
  const hay = String(text || '').toLowerCase();
  if (!hay || !Array.isArray(terms) || !terms.length) return 0;
  let count = 0;
  for (const term of terms) {
    const needle = String(term || '').trim().toLowerCase();
    if (!needle || needle.length < 3) continue;
    if (hay.includes(needle)) count += 1;
  }
  return count;
}

function dateFreshnessBonus(row, appliedDateRange) {
  if (!appliedDateRange?.start || !appliedDateRange?.end) return 0;
  const ts = sortKeyForRow(row);
  if (!ts) return 0;
  const start = parseTs(appliedDateRange.start);
  const end = parseTs(appliedDateRange.end);
  if (!start || !end || ts < start || ts > end) return 0;
  return 0.05;
}

function rerankFusedResults(rows, retrievalPlan) {
  const preferred = Array.isArray(retrievalPlan?.preferred_source_types) ? retrievalPlan.preferred_source_types : [];
  const lexicalTerms = Array.isArray(retrievalPlan?.lexical_terms) ? retrievalPlan.lexical_terms : [];
  const summaryVsRaw = retrievalPlan?.summary_vs_raw || 'summary';
  const entryMode = String(retrievalPlan?.entry_mode || 'hybrid');
  return (rows || [])
    .map((row) => {
      const lexicalBonus = String(row.match_reason || '').startsWith('lexical:') ? 0.14 : 0;
      const semanticBonus = String(row.match_reason || '').startsWith('semantic:') ? 0.08 : 0;
      const coreWalkBonus = String(row.match_reason || '').startsWith('core_walk') ? 0.11 : 0;
      const episodeBonus = row.layer === 'episode' ? (summaryVsRaw === 'summary' ? 0.09 : 0.03) : 0;
      const rawEvidenceBonus = summaryVsRaw === 'raw' && (row.source_type === 'event' || row.layer === 'event') ? 0.08 : 0;
      
      // Passive-First Heuristic: Boost direct observations
      const isPassive = row.layer === 'raw' || row.source_type === 'screen' || row.source_type === 'capture' || row.source_type_group === 'desktop';
      const passiveBoost = isPassive ? 0.15 : 0;

      const sourceBonus = sourceAgreementBonus(row, preferred);
      const dateBonus = dateFreshnessBonus(row, retrievalPlan?.applied_date_range);
      const exactTermHits = countExactTermHits(row.text, lexicalTerms);
      const exactnessBonus = Math.min(0.16, exactTermHits * 0.04);
      const entryModeBonus = entryMode === 'core_first'
        ? (coreWalkBonus + (semanticBonus * 0.6) + (lexicalBonus * 0.25))
        : (entryMode === 'query_first'
          ? (lexicalBonus + (semanticBonus * 0.9) + (coreWalkBonus * 0.25))
          : ((coreWalkBonus * 0.7) + (lexicalBonus * 0.7) + (semanticBonus * 0.7)));
      const rerankScore = Number(((row.fused_score || row.base_score || 0) + lexicalBonus + semanticBonus + coreWalkBonus + entryModeBonus + episodeBonus + rawEvidenceBonus + sourceBonus + dateBonus + exactnessBonus + passiveBoost).toFixed(6));
      return { ...row, rerank_score: rerankScore, exact_term_hits: exactTermHits };
    })
    .sort((a, b) => {
      if ((b.rerank_score || 0) !== (a.rerank_score || 0)) return (b.rerank_score || 0) - (a.rerank_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    });
}

async function coreDownRanking(nodeRows = [], retrievalPlan = {}, limit = 80) {
  if (!Array.isArray(nodeRows) || !nodeRows.length) return [];
  const mapById = new Map(nodeRows.map((row) => [row.id, row]));
  let frontier = nodeRows
    .filter((row) => row.layer === 'core' || row.id === 'global_core')
    .map((row) => row.id)
    .slice(0, 8);
  if (!frontier.length) {
    frontier = nodeRows
      .filter((row) => row.layer === 'insight' || row.layer === 'semantic')
      .sort((a, b) => (Number(b.confidence || 0) - Number(a.confidence || 0)))
      .map((row) => row.id)
      .slice(0, 6);
  }
  if (!frontier.length) return [];

  const terms = Array.isArray(retrievalPlan?.lexical_terms) ? retrievalPlan.lexical_terms : [];
  const visited = new Set(frontier);
  const scoreById = new Map(frontier.map((id) => [id, 1.2]));

  for (let depth = 1; depth <= 2 && frontier.length; depth++) {
    const placeholders = frontier.map(() => '?').join(',');
    const edges = await db.allQuery(
      `SELECT from_node_id, to_node_id, weight, evidence_count, edge_type, trace_label
       FROM memory_edges
       WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})
       ORDER BY weight DESC, evidence_count DESC
       LIMIT 320`,
      [...frontier, ...frontier]
    ).catch(() => []);
    const next = [];
    for (const edge of edges || []) {
      const left = edge.from_node_id;
      const right = edge.to_node_id;
      if (!left || !right) continue;
      const fromKnown = visited.has(left);
      const toKnown = visited.has(right);
      const neighborId = fromKnown && !toKnown ? right : (toKnown && !fromKnown ? left : null);
      if (!neighborId || !mapById.has(neighborId)) continue;

      const currentId = fromKnown ? left : right;
      const current = mapById.get(currentId) || { layer: 'core' };
      const neighbor = mapById.get(neighborId);
      if ((LAYER_RANKS[neighbor.layer] || 0) > (LAYER_RANKS[current.layer] || 0)) continue;

      const row = mapById.get(neighborId);
      const text = `${row.title || ''} ${row.summary || ''} ${row.canonical_text || ''} ${edge.edge_type || ''} ${edge.trace_label || ''}`;
      const termBoost = Math.min(0.24, countExactTermHits(text, terms) * 0.04);
      const edgeWeightMultiplier = EDGE_WEIGHTS[edge.edge_type] || 1.0;
      const base = (Math.max(0.2, 0.95 - (depth * 0.18)) + (Number(edge.weight || 0) * 0.06) + (Number(edge.evidence_count || 0) * 0.02)) * edgeWeightMultiplier + termBoost;
      const prev = Number(scoreById.get(neighborId) || 0);
      scoreById.set(neighborId, Math.max(prev, Number(base.toFixed(6))));
      if (!visited.has(neighborId)) {
        visited.add(neighborId);
        next.push(neighborId);
      }
    }
    frontier = Array.from(new Set(next)).slice(0, 140);
  }

  return Array.from(scoreById.entries())
    .map(([id, score]) => {
      const row = mapById.get(id);
      if (!row) return null;
      return {
        key: `node:${row.id}`,
        source_type: 'node',
        node_id: row.id,
        event_id: null,
        layer: row.layer,
        subtype: row.subtype,
        anchor_at: row.anchor_at || row.timestamp,
        latest_activity_at: row.latest_activity_at || row.timestamp,
        timestamp: row.timestamp,
        app: row.app,
        source_type_group: row.source_type_group || row.metadata?.source_type_group || null,
        text: [row.title, row.summary, row.canonical_text].filter(Boolean).join('\n'),
        source_refs: row.source_refs || [],
        base_score: Number(score || 0),
        match_reason: 'core_walk'
      };
    })
    .filter(Boolean)
    .sort((a, b) => (b.base_score || 0) - (a.base_score || 0))
    .slice(0, limit);
}

async function recursiveDownTraversal(nodeRows = [], retrievalPlan = {}, limit = 60) {
  if (!Array.isArray(nodeRows) || !nodeRows.length) return [];
  const mapById = new Map(nodeRows.map((row) => [row.id, row]));
  
  let frontier = nodeRows
    .filter((row) => row.layer === 'core' || row.layer === 'insight')
    .sort((a, b) => (Number(b.confidence || 0) - Number(a.confidence || 0)))
    .map((row) => row.id)
    .slice(0, 12);

  if (!frontier.length) return [];

  const visited = new Set(frontier);
  const results = [];

  for (let depth = 1; depth <= 3 && frontier.length && results.length < limit; depth++) {
    const placeholders = frontier.map(() => '?').join(',');
    const edges = await db.allQuery(
      `SELECT from_node_id, to_node_id, edge_type, weight FROM memory_edges 
       WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})
       ORDER BY weight DESC LIMIT 200`,
      [...frontier, ...frontier]
    ).catch(() => []);

    const nextFrontier = [];
    for (const edge of edges) {
      const left = edge.from_node_id;
      const right = edge.to_node_id;
      const neighborId = visited.has(left) ? right : left;
      
      if (!neighborId || visited.has(neighborId)) continue;
      
      const neighbor = mapById.get(neighborId);
      if (!neighbor) continue; 
      
      visited.add(neighborId);
      
      const isTarget = neighbor.layer === 'episode' || neighbor.layer === 'raw' || neighbor.layer === 'event';
      if (isTarget) {
        results.push({
          key: `node:${neighbor.id}`,
          source_type: 'node',
          node_id: neighbor.id,
          layer: neighbor.layer,
          subtype: neighbor.subtype,
          text: [neighbor.title, neighbor.summary, neighbor.canonical_text].filter(Boolean).join('\n'),
          base_score: Number((0.9 - (depth * 0.12)).toFixed(6)),
          match_reason: 'core_to_raw',
          timestamp: neighbor.timestamp || neighbor.anchor_at,
          app: neighbor.app,
          source_refs: neighbor.source_refs || []
        });
      }
      
      if (neighbor.layer !== 'raw' && neighbor.layer !== 'event') {
        nextFrontier.push(neighborId);
      }
    }
    frontier = nextFrontier.slice(0, 60);
  }
  return results;
}

async function loadMemoryNodeCandidates(filters = {}) {
  const dateRange = normalizeDateRange(filters.date_range);
  let sql = `SELECT id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date
     FROM memory_nodes
     WHERE status != 'archived'`;
  const params = [];

  // Apply SQL-level date pre-filter using the indexed anchor_date column.
  // Nodes without anchor_date (older rows) pass through and are filtered in JS below.
  if (dateRange) {
    const startDate = dateRange.start.toISOString().slice(0, 10);
    const endDate = dateRange.end.toISOString().slice(0, 10);
    sql += ` AND (anchor_date IS NULL OR (anchor_date >= ? AND anchor_date <= ?))`;
    params.push(startDate, endDate);
  }

  sql += ` LIMIT 2400`;

  const rows = await db.allQuery(sql, params).catch(() => []);

  return rows
    .map((row) => {
      const metadata = asObj(row.metadata);
      const sourceRefs = (() => {
        try {
          return JSON.parse(row.source_refs || '[]');
        } catch (_) {
          return [];
        }
      })();
      return {
        ...row,
        metadata,
        source_refs: sourceRefs,
        app: metadata.apps?.[0] || metadata.app || null,
        source_type_group: metadata.source_type_group || null,
        anchor_at: metadata.anchor_at || metadata.start || null,
        latest_activity_at: metadata.latest_activity_at || metadata.end || metadata.latest_interaction_at || row.updated_at || row.created_at || null,
        timestamp: metadata.anchor_at || metadata.start || metadata.end || metadata.latest_interaction_at || row.updated_at || row.created_at || null
      };
    })
    .filter((row) => rowMatchesFilters(row, filters));
}

async function vectorSearchNodes(nodeRows, semanticQueries = []) {
  const rankings = [];
  for (const query of semanticQueries || []) {
    const queryEmbedding = await generateEmbedding(query, process.env.OPENAI_API_KEY);
    const ranked = nodeRows
      .map((row) => {
        let embedding = [];
        try {
          embedding = JSON.parse(row.embedding || '[]');
        } catch (_) {
          embedding = [];
        }
        return {
          key: `node:${row.id}`,
          source_type: 'node',
          node_id: row.id,
          event_id: null,
          layer: row.layer,
          subtype: row.subtype,
          anchor_at: row.anchor_at || row.timestamp,
          latest_activity_at: row.latest_activity_at || row.timestamp,
          timestamp: row.timestamp,
          app: row.app,
          source_type_group: row.source_type_group || row.metadata?.source_type_group || null,
          text: [
            row.title,
            row.summary,
            row.canonical_text
          ].filter(Boolean).join('\n'),
          source_refs: row.source_refs || [],
          base_score: cosineSimilarity(queryEmbedding, embedding),
          match_reason: `semantic:${query}`
        };
      })
      .filter((row) => row.base_score > 0)
      .sort((a, b) => (b.base_score || 0) - (a.base_score || 0))
      .slice(0, 30);
    rankings.push(ranked);
  }
  return rankings;
}

async function lexicalSearchDocs(terms = [], filters = {}) {
  if (!terms.length) return [];
  const query = terms.map((term) => `"${String(term).replace(/"/g, '""')}"`).join(' OR ');
  const rows = await db.allQuery(
    `SELECT d.doc_id, d.source_type, d.node_id, d.event_id, d.app, d.timestamp, d.text, d.metadata,
            bm25(retrieval_docs_fts) AS bm25_score
     FROM retrieval_docs_fts
     JOIN retrieval_docs d ON d.doc_id = retrieval_docs_fts.doc_id
     WHERE retrieval_docs_fts MATCH ?
     LIMIT 120`,
    [query]
  ).catch(() => []);

  return rows
    .map((row) => {
      const metadata = asObj(row.metadata);
      return {
        key: row.doc_id,
        source_type: row.source_type,
        node_id: row.node_id,
        event_id: row.event_id,
        layer: metadata.layer || metadata.type || row.source_type,
        subtype: metadata.subtype || null,
        source_type_group: metadata.source_type_group || metadata.envelope?.type_group || null,
        anchor_at: metadata.anchor_at || row.timestamp,
        latest_activity_at: metadata.latest_activity_at || row.timestamp,
        timestamp: row.timestamp,
        app: row.app,
        text: row.text,
        activity_summary: metadata.activity_summary || metadata.envelope?.metadata?.activity_summary || null,
        content_type: metadata.content_type || metadata.envelope?.metadata?.content_type || null,
        uncertainty: metadata.capture_uncertainty || metadata.envelope?.metadata?.capture_uncertainty || null,
        source_refs: metadata.source_refs || [],
        base_score: 1 / (1 + Math.exp(Number(row.bm25_score || 0) / 4.0)),
        match_reason: `lexical:${terms.join(',')}`
      };
    })
    .filter((row) => rowMatchesFilters(row, filters))
    .sort((a, b) => (b.base_score || 0) - (a.base_score || 0));
}

async function querylessRecentDocs(filters = {}, limit = 24) {
  const rows = await db.allQuery(
    `SELECT doc_id, source_type, node_id, event_id, app, timestamp, text, metadata
     FROM retrieval_docs
     ORDER BY timestamp DESC
     LIMIT ?`,
    [Math.max(60, limit * 4)]
  ).catch(() => []);

  return rows
    .map((row, index) => {
      const metadata = asObj(row.metadata);
      return {
        key: row.doc_id,
        source_type: row.source_type,
        node_id: row.node_id,
        event_id: row.event_id,
        layer: metadata.layer || metadata.type || row.source_type,
        subtype: metadata.subtype || null,
        source_type_group: metadata.source_type_group || metadata.envelope?.type_group || null,
        anchor_at: metadata.anchor_at || row.timestamp,
        latest_activity_at: metadata.latest_activity_at || row.timestamp,
        timestamp: row.timestamp,
        app: row.app,
        text: row.text,
        activity_summary: metadata.activity_summary || metadata.envelope?.metadata?.activity_summary || null,
        content_type: metadata.content_type || metadata.envelope?.metadata?.content_type || null,
        uncertainty: metadata.capture_uncertainty || metadata.envelope?.metadata?.capture_uncertainty || null,
        source_refs: metadata.source_refs || [],
        base_score: Number((1 - (index * 0.02)).toFixed(6)),
        match_reason: 'recency'
      };
    })
    .filter((row) => rowMatchesFilters(row, filters))
    .slice(0, limit);
}

function expansionScore(layer, subtype) {
  if (layer === 'insight') return 7;
  if (layer === 'semantic' && subtype === 'task') return 6;
  if (layer === 'semantic' && subtype === 'person') return 6;
  if (layer === 'semantic' && subtype === 'decision') return 6;
  if (layer === 'semantic' && subtype === 'fact') return 5;
  if (layer === 'semantic' && subtype === 'link') return 4;
  if (layer === 'episode') return 3;
  if (layer === 'cloud') return 3;
  return 1;
}

async function expandGraph(seedNodes = [], hopLimit = DEFAULT_HOP_LIMIT, maxExpanded = MAX_EXPANDED) {
  const seedIds = seedNodes.map(s => s.node_id || s.id).filter(Boolean);
  const seen = new Set(seedIds);
  const queue = seedNodes
    .filter(s => s.node_id || s.id)
    .map((s) => ({ id: s.node_id || s.id, layer: s.layer, depth: 0 }));
  const expanded = [];
  const edgePaths = [];

  while (queue.length && expanded.length < maxExpanded) {
    const current = queue.shift();
    if (current.depth >= hopLimit) continue;
    const edges = await db.allQuery(
      `SELECT id, from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata
       FROM memory_edges
       WHERE from_node_id = ? OR to_node_id = ?
       LIMIT 80`,
      [current.id, current.id]
    ).catch(() => []);

    const neighbors = [];
    for (const edge of edges) {
      const neighborId = edge.from_node_id === current.id ? edge.to_node_id : edge.from_node_id;
      if (!neighborId || seen.has(neighborId)) continue;
      const node = await db.getQuery(
        `SELECT id, layer, subtype, title, summary, metadata, source_refs, created_at, updated_at
         FROM memory_nodes
         WHERE id = ?`,
        [neighborId]
      ).catch(() => null);
      if (!node) continue;

      // Restrict traversal: only higher or equal rank to lower/equal rank
      if ((LAYER_RANKS[node.layer] || 0) > (LAYER_RANKS[current.layer] || 0)) continue;

      const metadata = asObj(node.metadata);
      neighbors.push({
        id: node.id,
        layer: node.layer,
        subtype: node.subtype || null,
        title: node.title || metadata.name || metadata.fact || node.id,
        summary: node.summary || metadata.summary || '',
        anchor_at: metadata.anchor_at || metadata.start || null,
        latest_activity_at: metadata.latest_activity_at || metadata.end || node.updated_at || node.created_at || null,
        timestamp: metadata.end || metadata.start || metadata.latest_interaction_at || node.updated_at || node.created_at || null,
        depth: current.depth + 1,
        sort_score: expansionScore(node.layer, node.subtype) * (EDGE_WEIGHTS[edge.edge_type] || 1.0),
        edge: {
          from: current.id,
          to: node.id,
          relation: edge.edge_type,
          trace_label: edge.trace_label || null,
          weight: Number(edge.weight || 1),
          evidence_count: Number(edge.evidence_count || 1),
          depth: current.depth + 1
        }
      });
    }

    neighbors
      .sort((a, b) => b.sort_score - a.sort_score)
      .slice(0, 8)
      .forEach((item) => {
        if (seen.has(item.id)) return;
        seen.add(item.id);
        expanded.push({
          id: item.id,
          layer: item.layer,
          type: item.layer,
          subtype: item.subtype,
          title: item.title,
          summary: item.summary,
          timestamp: item.timestamp,
          depth: item.depth
        });
        edgePaths.push(item.edge);
        queue.push({ id: item.id, layer: item.layer, depth: item.depth });
      });
  }

  return { expandedNodes: expanded, edgePaths };
}

function alphaBlendedSearch(lexicalRanking, semanticRankings, alpha = 0.45) {
  const scores = new Map();
  const allSemantic = [].concat(...semanticRankings);
  const keys = new Set([
    ...lexicalRanking.map((r) => r.key),
    ...allSemantic.map((r) => r.key)
  ]);

  for (const key of keys) {
    const lex = lexicalRanking.find((r) => r.key === key);
    const sem = allSemantic.filter((r) => r.key === key).sort((a, b) => b.base_score - a.base_score)[0];

    const sScore = sem ? sem.base_score : 0;
    const lScore = lex ? lex.base_score : 0;

    // Alpha-blending: sScore is cosine (0-1), lScore is normalized BM25 (0-1)
    const combined = (alpha * sScore) + ((1 - alpha) * lScore);

    if (combined > 0) {
      scores.set(key, {
        row: sem || lex,
        score: combined
      });
    }
  }

  return Array.from(scores.values())
    .map((item) => ({
      ...item.row,
      fused_score: Number(item.score.toFixed(6))
    }))
    .sort((a, b) => b.fused_score - a.fused_score);
}

async function buildHybridGraphRetrieval({
  query,
  options = {},
  seedLimit = DEFAULT_SEED_LIMIT,
  hopLimit = DEFAULT_HOP_LIMIT,
  recursionDepth = 0,
  passiveOnly = false
} = {}) {
  const basePlan = options.retrieval_thought || buildRetrievalThought({
    query,
    mode: options.mode || 'chat',
    candidate: options.candidate || null,
    dateRange: options.date_range || null,
    app: options.app || null
  });
  const retrievalPlan = {
    ...basePlan,
    filters: {
      ...(basePlan.filters || {}),
      app: options.app || basePlan.filters?.app || null,
      date_range: options.date_range || basePlan.filters?.date_range || null,
      source_types: options.source_types || basePlan.filters?.source_types || null
    },
    seed_limit: seedLimit || basePlan.seed_limit || DEFAULT_SEED_LIMIT,
    hop_limit: hopLimit || basePlan.hop_limit || DEFAULT_HOP_LIMIT,
    context_budget_tokens: basePlan.context_budget_tokens || (options.mode === 'suggestion' ? 550 : 800),
    search_queries: basePlan.search_queries || basePlan.semantic_queries || [],
    search_queries_messages: basePlan.search_queries_messages || basePlan.message_queries || [],
    temporal_reasoning: Array.isArray(basePlan.temporal_reasoning) ? basePlan.temporal_reasoning : [],
    initial_date_range: basePlan.initial_date_range || basePlan.filters?.date_range || null,
    applied_date_range: options.date_range || basePlan.applied_date_range || basePlan.filters?.date_range || null,
    widened_date_range: basePlan.widened_date_range || null,
    date_filter_status: basePlan.date_filter_status || ((options.date_range || basePlan.filters?.date_range) ? 'applied' : 'not_used'),
    fallback_policy: basePlan.fallback_policy || { mode: 'widen_once', attempted: false, widened: false }
  };

  const nodeRows = await loadMemoryNodeCandidates(retrievalPlan.filters);
  
  let finalNodeRows = nodeRows;
  let finalFilters = retrievalPlan.filters;

  if (passiveOnly) {
    const passiveLayers = ['core', 'insight', 'cloud'];
    finalNodeRows = nodeRows.filter(r => passiveLayers.includes(r.layer));
    finalFilters = { ...finalFilters, passive_only: true };
  }

  const lexicalRanking = await lexicalSearchDocs(retrievalPlan.lexical_terms || [], finalFilters);
  const semanticRankings = retrievalPlan.mode === 'queryless'
    ? []
    : await vectorSearchNodes(finalNodeRows, retrievalPlan.semantic_queries || retrievalPlan.search_queries || []);
  const coreRanking = await coreDownRanking(finalNodeRows, retrievalPlan, 80);
  const coreToRawRanking = (options.strategy === 'core_to_raw' || retrievalPlan.strategy === 'core_to_raw')
    ? await recursiveDownTraversal(finalNodeRows, retrievalPlan, 60)
    : [];
  const recencyRanking = (retrievalPlan.mode === 'queryless' && !passiveOnly)
    ? await querylessRecentDocs(retrievalPlan.filters, 24)
    : [];

  const alpha = options.alpha !== undefined ? options.alpha : (retrievalPlan.alpha !== undefined ? retrievalPlan.alpha : 0.7);
  let fused = alphaBlendedSearch(lexicalRanking, [...semanticRankings, coreRanking, coreToRawRanking, recencyRanking], alpha);

  // Recursive Retrieval Pass
  if (recursionDepth > 0 && !passiveOnly) {
    const anchorNodes = fused
      .filter(row => (row.layer === 'core' || row.layer === 'insight') && row.fused_score > 0.6)
      .slice(0, 3);
    
    if (anchorNodes.length > 0) {
      const recursionQueries = anchorNodes.map(node => node.text.slice(0, 300));
      const recursionRankings = await vectorSearchNodes(finalNodeRows, recursionQueries);
      if (recursionRankings.length > 0) {
        fused = alphaBlendedSearch(lexicalRanking, [...semanticRankings, ...recursionRankings, coreRanking, recencyRanking], alpha);
      }
    }
  }

  const reranked = rerankFusedResults(fused, retrievalPlan);
  const seeds = reranked.slice(0, retrievalPlan.seed_limit);
  // canonical list of seed node ids/keys used for tracing and logging
  const seedNodeIds = Array.from(new Set(seeds.map((s) => (s.node_id || s.event_id || s.key)).filter(Boolean))).slice(0, retrievalPlan.seed_limit);
  const graph = await expandGraph(seeds, retrievalPlan.hop_limit, MAX_EXPANDED);

  // Spiral retrieval ordering: Insights -> Semantics -> Episodes
  let evidenceRows = reranked.slice(0, 18);
  if ((retrievalPlan.strategy_mode || retrievalPlan.strategy || options.strategy) === 'spiral') {
    // insights from expanded graph
    const insightNodes = graph.expandedNodes.filter((n) => n.layer === 'insight');
    // semantics from reranked evidence
    const semanticRows = evidenceRows.filter((r) => r.layer === 'semantic' || (r.source_type === 'node' && r.layer === 'semantic'));
    // episodes from expanded graph
    const episodeNodes = graph.expandedNodes.filter((n) => n.layer === 'episode');

    const coreNodes = await loadMemoryNodeCandidates({ layer: 'core' }).catch(() => []);
    const rawDocs = lexicalRanking.slice(0, 18).map((r) => ({ ...r, layer: r.layer || 'event' }));

    // build evidence ordering with dedupe by key/node id
    const ordered = [];
    const seenKeys = new Set();
    function pushRow(r, key) {
      const k = key || (r.node_id || r.key || r.id);
      if (!k || seenKeys.has(k)) return;
      seenKeys.add(k);
      ordered.push(r);
    }

    insightNodes.forEach((n) => pushRow({ key: `node:${n.id}`, node_id: n.id, layer: 'insight', title: n.title, text: n.summary || n.title }, `node:${n.id}`));
    semanticRows.forEach((r) => pushRow(r, r.node_id || r.key));
    episodeNodes.forEach((n) => pushRow({ key: `node:${n.id}`, node_id: n.id, layer: 'episode', title: n.title, text: n.summary || n.title, base_score: n.sort_score || 0 }, `node:${n.id}`));

    coreNodes.forEach((n) => pushRow({ key: `node:${n.id}`, node_id: n.id, layer: 'core', title: n.title, text: n.summary || n.title }, `node:${n.id}`));
    rawDocs.forEach((r) => pushRow(r, r.key));

    // fallback: append remaining reranked rows preserving their order
    for (const r of evidenceRows) pushRow(r, r.key || r.node_id || r.id);

    evidenceRows = ordered.slice(0, 60);
  }

  const evidence = evidenceRows.slice(0, 18).map((row) => ({
    id: row.node_id || row.event_id || row.key,
    node_id: row.node_id || null,
    event_id: row.event_id || null,
    layer: row.layer || row.source_type,
    type: row.layer || row.source_type,
    subtype: row.subtype || null,
    anchor_at: row.anchor_at || null,
    latest_activity_at: row.latest_activity_at || row.timestamp || null,
    timestamp: row.timestamp || null,
    app: row.app || null,
    activity_summary: row.activity_summary || null,
    content_type: row.content_type || null,
    uncertainty: row.uncertainty || null,
    score: Number((row.rerank_score || row.fused_score || row.base_score || 0).toFixed(6)),
    reason: row.match_reason,
    source_refs: row.source_refs || [],
    text: String(row.text || '').slice(0, 240)
  }));

  const traceSummary = [
    `Mode: ${retrievalPlan.mode}`,
    `Seeds: ${seedNodeIds.join(', ') || 'none'}`,
    ...(retrievalPlan.applied_date_range ? [`Applied date window: ${retrievalPlan.applied_date_range.start} -> ${retrievalPlan.applied_date_range.end}`] : []),
    `Stage 1: hybrid seed search returned ${seeds.length} primary seeds.`,
    `Stage 2: graph expansion added ${graph.expandedNodes.length} connected nodes.`,
    ...summarizeRetrievalThought(retrievalPlan)
  ];

  const contextSections = [];
  if (seeds.length) {
    contextSections.push(`SEED NODES:\n${seeds.map((seed) => `- [${seed.layer || 'node'}] ${String(seed.text || '').slice(0, 180)}`).join('\n')}`);
  }
  if (graph.expandedNodes.length) {
    contextSections.push(`EXPANDED GRAPH:\n${graph.expandedNodes.slice(0, MAX_EXPANDED).map((node) => `- [${node.layer}${node.subtype ? `/${node.subtype}` : ''}] ${node.title}${node.summary ? ` — ${node.summary}` : ''}`).join('\n')}`);
  }
  if (graph.edgePaths.length) {
    contextSections.push(`TRACE:\n${graph.edgePaths.slice(0, 12).map((edge) => `- ${edge.from} -> ${edge.to} via ${edge.relation}${edge.trace_label ? ` (${edge.trace_label})` : ''}`).join('\n')}`);
  }

  const retrievalRunId = await logRetrievalRun({
    query,
    mode: retrievalPlan.mode,
    metadata: {
      plan: retrievalPlan,
      seeds: seedNodeIds,
      evidence_count: evidence.length
    }
  });

  const drilldownRefs = Array.from(new Set(
    evidence.flatMap((item) => [
      ...(item.source_refs || []),
      item.event_id || null
    ]).filter(Boolean)
  )).slice(0, 18);

  return {
    retrieval_run_id: retrievalRunId,
    retrieval_plan: retrievalPlan,
    generated_queries: {
      query_bundle: retrievalPlan.query_bundle || null,
      semantic: retrievalPlan.semantic_queries || [],
      messages: retrievalPlan.message_queries || [],
      lexical_terms: retrievalPlan.lexical_terms || [],
      debug: retrievalPlan.query_debug || null
    },
    thought_summary: summarizeRetrievalThought(retrievalPlan),
    trace_summary: traceSummary,
    temporal_reasoning: retrievalPlan.temporal_reasoning || [],
    initial_date_range: retrievalPlan.initial_date_range || null,
    applied_date_range: retrievalPlan.applied_date_range || null,
    widened_date_range: retrievalPlan.widened_date_range || null,
    date_filter_status: retrievalPlan.date_filter_status || 'not_used',
    strategy: {
      strategy_mode: retrievalPlan.strategy_mode || 'memory_only',
      entry_mode: retrievalPlan.entry_mode || 'hybrid',
      summary_vs_raw: retrievalPlan.summary_vs_raw || 'summary',
      time_scope: retrievalPlan.time_scope || null,
      app_scope: retrievalPlan.app_scope || [],
      source_scope: retrievalPlan.source_scope || [],
      web_gate_reason: retrievalPlan.web_gate_reason || ''
    },
    seed_results: seeds.map((seed) => ({
      id: seed.node_id || seed.event_id || seed.key,
      node_id: seed.node_id || null,
      event_id: seed.event_id || null,
      source_type: seed.source_type || null,
      layer: seed.layer || seed.source_type,
      subtype: seed.subtype || null,
      title: String(seed.text || '').split('\n')[0].slice(0, 140),
      score: Number((seed.rerank_score || seed.fused_score || seed.base_score || 0).toFixed(6)),
      reason: seed.match_reason,
      anchor_at: seed.anchor_at || null,
      latest_activity_at: seed.latest_activity_at || seed.timestamp || null
      ,
      activity_summary: seed.activity_summary || null,
      content_type: seed.content_type || null,
      uncertainty: seed.uncertainty || null
    })),
    seed_nodes: seeds.map((seed) => ({
      id: seed.node_id || seed.event_id || seed.key,
      node_id: seed.node_id || null,
      event_id: seed.event_id || null,
      source_type: seed.source_type || null,
      layer: seed.layer || seed.source_type,
      type: seed.layer || seed.source_type,
      subtype: seed.subtype || null,
      anchor_at: seed.anchor_at || null,
      latest_activity_at: seed.latest_activity_at || seed.timestamp || null,
      title: String(seed.text || '').split('\n')[0].slice(0, 140),
      text: String(seed.text || '').slice(0, 220),
      app: seed.app || null,
      activity_summary: seed.activity_summary || null,
      content_type: seed.content_type || null,
      uncertainty: seed.uncertainty || null,
      score: Number((seed.rerank_score || seed.fused_score || seed.base_score || 0).toFixed(6)),
      reason: seed.match_reason
    })),
    expanded_nodes: graph.expandedNodes,
    graph_expansion_results: graph.expandedNodes,
    edge_paths: graph.edgePaths,
    trace_labels: graph.edgePaths.map((edge) => edge.trace_label).filter(Boolean),
    lazy_source_refs: drilldownRefs.map((ref) => ({ ref })),
    drilldown_refs: drilldownRefs,
    ranking_policy: {
      date_field: 'anchor_at',
      freshness_field: 'latest_activity_at',
      seed_then_expand: true,
      summary_vs_raw: retrievalPlan.summary_vs_raw || 'summary'
    },
    evidence_count: evidence.length,
    evidence,
    contextText: contextSections.join('\n\n')
  };
}

module.exports = {
  buildHybridGraphRetrieval
};
