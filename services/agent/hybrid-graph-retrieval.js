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

const DEFAULT_SEED_LIMIT = 10;
const DEFAULT_HOP_LIMIT = 10;
const MAX_EXPANDED = 300;

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

function parseSourceRefs(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  try {
    const parsed = JSON.parse(value || '[]');
    return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
  } catch (_) {
    return [];
  }
}

function rowMatchesFilters(row, filters = {}) {
  if (row.id === 'global_core' || row.layer === 'core') return true;

  if (filters.prioritize_screen_capture) {
    const isBrowserHistory = String(row.app || '').toLowerCase().includes('browser') || 
                            String(row.app || '').toLowerCase().includes('chrome') || 
                            String(row.source_type || '').toLowerCase().includes('history') ||
                            row.source_type === 'visit';
    if (isBrowserHistory) return false;
  }

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
  const summaryVsRaw = retrievalPlan?.summary_vs_raw || 'summary';
  const entryMode = String(retrievalPlan?.entry_mode || 'hybrid');
  return (rows || [])
    .map((row) => {
      const semanticBonus = String(row.match_reason || '').startsWith('semantic:') ? 0.08 : 0;
      const coreWalkBonus = String(row.match_reason || '').startsWith('core_walk') ? 0.11 : 0;
      const episodeBonus = row.layer === 'episode' ? (summaryVsRaw === 'summary' ? 0.09 : 0.03) : 0;
      const rawEvidenceBonus = summaryVsRaw === 'raw' && (row.source_type === 'event' || row.layer === 'event') ? 0.08 : 0;
      
      // ScreenCapture Prioritization: 30-min baseline check
      const isScreenCapture = row.source_type === 'screen' || row.source_type === 'capture' || row.subtype === 'screencapture';
      const isBrowserHistory = String(row.app || '').toLowerCase().includes('browser') || String(row.app || '').toLowerCase().includes('chrome') || String(row.source_type || '').toLowerCase().includes('history');
      
      let passiveBoost = 0;
      if (retrievalPlan?.filters?.prioritize_screen_capture) {
        if (isScreenCapture) passiveBoost = 0.15;
        else if (isBrowserHistory) passiveBoost = 0.05;
      } else {
        if (isScreenCapture || isBrowserHistory || row.source_type_group === 'desktop') passiveBoost = 0.12;
      }

      const sourceBonus = sourceAgreementBonus(row, preferred);
      const dateBonus = dateFreshnessBonus(row, retrievalPlan?.applied_date_range);
    
    const entryModeBonus = entryMode === 'core_first'
      ? (coreWalkBonus + (semanticBonus * 0.6))
      : (entryMode === 'query_first'
        ? (semanticBonus * 0.9 + (coreWalkBonus * 0.25))
        : ((coreWalkBonus * 0.7) + (semanticBonus * 0.7)));
    const rerankScore = Number(((row.fused_score || row.base_score || 0) + semanticBonus + coreWalkBonus + entryModeBonus + episodeBonus + rawEvidenceBonus + sourceBonus + dateBonus + passiveBoost).toFixed(6));
    return { ...row, rerank_score: rerankScore };
  })
    .sort((a, b) => {
      if ((b.rerank_score || 0) !== (a.rerank_score || 0)) return (b.rerank_score || 0) - (a.rerank_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    });
}

async function coreDownRanking(nodeRows = [], retrievalPlan = {}, limit = 80) {
  if (!Array.isArray(nodeRows) || !nodeRows.length) return [];
  const mapById = new Map(nodeRows.map((row) => [row.id, row]));
  
  // Primary anchoring frontier: Global Core and Core nodes
  let coreFrontier = nodeRows
    .filter((row) => row.id === 'global_core' || row.layer === 'core')
    .map((row) => row.id)
    .slice(0, 12);

  // Secondary seed frontier: Insights and Semantics
  let seedFrontier = nodeRows
    .filter((row) => row.layer === 'insight' || row.layer === 'semantic')
    .sort((a, b) => (Number(b.confidence || 0) - Number(a.confidence || 0)))
    .map((row) => row.id)
    .slice(0, 10);

  let frontier = Array.from(new Set([...coreFrontier, ...seedFrontier]));
  if (!frontier.length) return [];

  const visited = new Set(frontier);
  const scoreById = new Map(frontier.map((id) => {
    const row = mapById.get(id);
    let base = 0.8;
    if (id === 'global_core') base = 1.6;
    else if (row?.layer === 'core') base = 1.3;
    else if (row?.layer === 'insight') base = 1.1;
    return [id, base];
  }));

  for (let depth = 1; depth <= 10 && frontier.length; depth++) {
    const placeholders = frontier.map(() => '?').join(',');
    const edges = await db.allQuery(
      `SELECT from_node_id, to_node_id, weight, evidence_count, edge_type, trace_label
       FROM memory_edges
       WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})
       ORDER BY weight DESC, evidence_count DESC
       LIMIT 400`,
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

      const edgeWeightMultiplier = EDGE_WEIGHTS[edge.edge_type] || 1.0;
      const base = (Math.max(0.2, 0.95 - (depth * 0.18)) + (Number(edge.weight || 0) * 0.06) + (Number(edge.evidence_count || 0) * 0.02)) * edgeWeightMultiplier;
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

// Seed discovery helper used before canonical bounded graph expansion.
// This can bias retrieval toward lower-ranked evidence, but it is not the main
// graph-expansion stage exposed to chat.
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

  for (let depth = 1; depth <= 10 && frontier.length && results.length < limit; depth++) {
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
  
  // Always try to fetch global_core regardless of date filters
  const globalCore = await db.getQuery(`SELECT * FROM memory_nodes WHERE id = 'global_core'`).catch(() => null);
  if (globalCore && !rows.find(r => r.id === 'global_core')) {
    rows.push(globalCore);
  }

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

async function loadEventEvidenceRows(refs = [], limit = 100) {
  const ids = Array.from(new Set((refs || []).filter(Boolean))).slice(0, Math.max(1, limit));
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT id, source_type, occurred_at, title, redacted_text, raw_text, app, source_account, metadata
     FROM events
     WHERE id IN (${placeholders})
     ORDER BY COALESCE(occurred_at, timestamp) DESC`,
    ids
  ).catch(() => []);

  return rows.map((row, index) => {
    const metadata = asObj(row.metadata);
    const text = metadata.cleaned_capture_text || row.redacted_text || row.raw_text || '';
    return {
      key: `event:${row.id}`,
      source_type: 'event',
      node_id: null,
      event_id: row.id,
      layer: 'event',
      subtype: row.source_type || null,
      anchor_at: row.occurred_at || null,
      latest_activity_at: row.occurred_at || null,
      timestamp: row.occurred_at || null,
      app: row.app || null,
      source_account: row.source_account || null,
      title: row.title || row.source_type || row.id,
      text: String(text).slice(0, 8000),
      source_refs: [row.id],
      base_score: Number((0.82 - (index * 0.015)).toFixed(6)),
      match_reason: 'episode_source_ref'
    };
  });
}

function expansionScore(layer, subtype) {
  if (layer === 'insight') return 10;
  if (layer === 'semantic' && subtype === 'task') return 9;
  if (layer === 'semantic' && subtype === 'person') return 9;
  if (layer === 'semantic' && subtype === 'decision') return 8;
  if (layer === 'semantic' && subtype === 'fact') return 7;
  if (layer === 'episode') return 6;
  if (layer === 'raw' || layer === 'event') return 5;
  if (layer === 'semantic' && subtype === 'link') return 4;
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
  const supportNodes = [];
  const evidenceNodes = [];
  const edgePaths = [];

  const effectiveHopLimit = Math.min(10, Math.max(1, hopLimit || DEFAULT_HOP_LIMIT));

  while (queue.length && expanded.length < maxExpanded) {
    const current = queue.shift();
    if (current.depth >= effectiveHopLimit) continue;
    const edges = await db.allQuery(
      `SELECT id, from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata
       FROM memory_edges
       WHERE from_node_id = ? OR to_node_id = ?
       LIMIT 200`,
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
        source_refs: parseSourceRefs(node.source_refs),
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
      .slice(0, 30)
      .forEach((item) => {
        if (seen.has(item.id)) return;
        seen.add(item.id);
        const expandedNode = {
          id: item.id,
          layer: item.layer,
          type: item.layer,
          subtype: item.subtype,
          title: item.title,
          summary: item.summary,
          timestamp: item.timestamp,
          depth: item.depth,
          source_refs: item.source_refs || []
        };
        expanded.push(expandedNode);
        if (item.layer === 'episode' || item.layer === 'raw' || item.layer === 'event') evidenceNodes.push(expandedNode);
        else supportNodes.push(expandedNode);
        edgePaths.push(item.edge);
        queue.push({ id: item.id, layer: item.layer, depth: item.depth });
      });
  }

  return {
    expandedNodes: expanded,
    supportNodes,
    evidenceNodes,
    edgePaths
  };
}

function alphaBlendedSearch(lexicalRanking, semanticRankings, alpha = 0.45) {
  const scores = new Map();
  const allSemantic = [].concat(...semanticRankings);
  
  // Entirely relying on vector search (semantic) logic. Lexical ranking is ignored.
  for (const sem of allSemantic) {
    const key = sem.key;
    const prev = scores.get(key);
    if (!prev || sem.base_score > prev.score) {
      scores.set(key, {
        row: sem,
        score: sem.base_score
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
  const oldestCapture = await db.getQuery(`SELECT occurred_at FROM events WHERE type = 'ScreenCapture' ORDER BY occurred_at ASC LIMIT 1`).catch(() => null);
  let prioritizeScreenCapture = false;
  if (oldestCapture && oldestCapture.occurred_at) {
    const oldestTs = new Date(oldestCapture.occurred_at).getTime();
    const thirtyMinsAgo = Date.now() - (30 * 60 * 1000);
    if (oldestTs < thirtyMinsAgo) {
      prioritizeScreenCapture = true;
    }
  }

  const basePlan = options.retrieval_thought || await buildRetrievalThought({
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
      source_types: options.source_types || basePlan.filters?.source_types || null,
      prioritize_screen_capture: prioritizeScreenCapture
    },
    seed_limit: seedLimit || basePlan.seed_limit || DEFAULT_SEED_LIMIT,
    hop_limit: hopLimit || basePlan.hop_limit || DEFAULT_HOP_LIMIT,
    context_budget_tokens: basePlan.context_budget_tokens || (options.mode === 'suggestion' ? 1200 : 2000),
    search_queries: basePlan.search_queries || basePlan.semantic_queries || [],
    search_queries_messages: basePlan.search_queries_messages || basePlan.message_queries || [],
    web_queries: basePlan.web_queries || basePlan.query_sets?.web_queries || [],
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

  const lexicalRanking = [];
  const rawEventLexicalRanking = [];
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
  let fused = alphaBlendedSearch(
    [...lexicalRanking, ...rawEventLexicalRanking],
    [...semanticRankings, coreRanking, coreToRawRanking, recencyRanking],
    alpha
  );

  // Recursive Retrieval Pass
  if (recursionDepth > 0 && !passiveOnly) {
    const anchorNodes = fused
      .filter(row => (row.layer === 'core' || row.layer === 'insight') && row.fused_score > 0.6)
      .slice(0, 3);
    
    if (anchorNodes.length > 0) {
      const recursionQueries = anchorNodes.map(node => node.text.slice(0, 300));
      const recursionRankings = await vectorSearchNodes(finalNodeRows, recursionQueries);
      if (recursionRankings.length > 0) {
        fused = alphaBlendedSearch(
          [...lexicalRanking, ...rawEventLexicalRanking],
          [...semanticRankings, ...recursionRankings, coreRanking, recencyRanking],
          alpha
        );
      }
    }
  }

  const reranked = rerankFusedResults(fused, retrievalPlan);
  const seeds = reranked.slice(0, retrievalPlan.seed_limit);
  // canonical list of seed node ids/keys used for tracing and logging
  const seedNodeIds = Array.from(new Set(seeds.map((s) => (s.node_id || s.event_id || s.key)).filter(Boolean))).slice(0, retrievalPlan.seed_limit);
  
  // Ensure recursive expansion from Core nodes even if not in primary seeds
  const coreNodesForExpansion = finalNodeRows
    .filter(r => r.layer === 'core')
    .slice(0, 6)
    .map(r => ({ ...r, node_id: r.id }));
    
  const graph = await expandGraph([...seeds, ...coreNodesForExpansion], retrievalPlan.hop_limit, MAX_EXPANDED);
  const primaryNodes = seeds.map((seed) => ({
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
    text: String(seed.text || '').slice(0, 8000),
    app: seed.app || null,
    activity_summary: seed.activity_summary || null,
    content_type: seed.content_type || null,
    uncertainty: seed.uncertainty || null,
    score: Number((seed.rerank_score || seed.fused_score || seed.base_score || 0).toFixed(6)),
    reason: seed.match_reason,
    source_refs: seed.source_refs || []
  }));
  const supportNodes = Array.from(new Map([
    ...((Array.isArray(graph.supportNodes) ? graph.supportNodes : []).map((item) => [item.id, item])),
    ...(primaryNodes
      .filter((item) => item.layer === 'semantic' && ['task', 'person', 'decision', 'fact'].includes(String(item.subtype || '')))
      .map((item) => [item.id, {
        id: item.id,
        layer: item.layer,
        type: item.type,
        subtype: item.subtype,
        title: item.title,
        summary: item.text,
        timestamp: item.latest_activity_at || item.anchor_at || null,
        depth: 0,
        source_refs: item.source_refs || []
      }]))
  ]).values());
  const evidenceNodes = Array.from(new Map([
    ...((Array.isArray(graph.evidenceNodes) ? graph.evidenceNodes : []).map((item) => [item.id, item])),
    ...(primaryNodes
      .filter((item) => item.layer === 'episode' || item.layer === 'raw' || item.layer === 'event')
      .map((item) => [item.id, {
        id: item.id,
        layer: item.layer,
        type: item.type,
        subtype: item.subtype,
        title: item.title,
        summary: item.text,
        timestamp: item.latest_activity_at || item.anchor_at || null,
        depth: 0,
        source_refs: item.source_refs || []
      }]))
  ]).values());

  const episodeSourceRefMap = new Map();
  [...primaryNodes, ...graph.expandedNodes]
    .filter((item) => item && item.layer === 'episode')
    .forEach((item) => {
      const refs = parseSourceRefs(item.source_refs);
      refs.forEach((ref) => {
        if (!episodeSourceRefMap.has(ref)) episodeSourceRefMap.set(ref, new Set());
        episodeSourceRefMap.get(ref).add(item.id);
      });
    });
  const sourceRefEvidenceRows = await loadEventEvidenceRows(Array.from(episodeSourceRefMap.keys()), 100);
  const sourceRefEdges = sourceRefEvidenceRows.flatMap((row) => {
    const parents = Array.from(episodeSourceRefMap.get(row.event_id) || []);
    return parents.map((episodeId) => ({
      from: episodeId,
      to: row.event_id,
      relation: 'SOURCE_REF',
      trace_label: 'episode->event',
      weight: 1,
      evidence_count: 1,
      depth: 1,
      synthetic: true
    }));
  });
  sourceRefEvidenceRows.forEach((row) => {
    const identity = row.event_id || row.key;
    if (!identity) return;
    if (!evidenceNodes.find((item) => item.id === identity)) {
      evidenceNodes.push({
        id: identity,
        layer: row.layer,
        type: row.layer,
        subtype: row.subtype,
        title: row.title,
        summary: row.text,
        timestamp: row.timestamp,
        depth: 1,
        source_refs: row.source_refs || []
      });
    }
  });

  // Spiral retrieval ordering: Insights -> Semantics -> Episodes
  let evidenceRows = reranked.slice(0, 100);
  if ((retrievalPlan.strategy_mode || retrievalPlan.strategy || options.strategy) === 'spiral') {
    // insights from expanded graph
    const insightNodes = graph.expandedNodes.filter((n) => n.layer === 'insight');
    // semantics from reranked evidence
    const semanticRows = evidenceRows.filter((r) => r.layer === 'semantic' || (r.source_type === 'node' && r.layer === 'semantic'));
    // episodes from expanded graph
    const episodeNodes = graph.expandedNodes.filter((n) => n.layer === 'episode');

    const coreNodes = await loadMemoryNodeCandidates({ layer: 'core' }).catch(() => []);
    const rawDocs = [...rawEventLexicalRanking, ...lexicalRanking].slice(0, 24).map((r) => ({ ...r, layer: r.layer || 'event' }));

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

    evidenceRows = ordered.slice(0, 100);
  }

  const prioritizedEvidenceRows = [];
  const prioritizedSeen = new Set();
  const pushEvidenceRow = (row, key) => {
    const identity = key || row.node_id || row.event_id || row.key || row.id;
    if (!identity || prioritizedSeen.has(identity)) return;
    prioritizedSeen.add(identity);
    prioritizedEvidenceRows.push(row);
  };
  seeds.forEach((row) => pushEvidenceRow(row));
  evidenceNodes.forEach((node) => pushEvidenceRow({
    key: node.layer === 'event' ? `event:${node.id}` : `node:${node.id}`,
    node_id: node.id,
    event_id: node.layer === 'event' ? node.id : null,
    layer: node.layer,
    subtype: node.subtype,
    text: [node.title, node.summary].filter(Boolean).join('\n'),
    timestamp: node.timestamp,
    base_score: 0.88 - ((node.depth || 1) * 0.08),
    match_reason: 'downward_evidence'
  }));
  sourceRefEvidenceRows.forEach((row) => pushEvidenceRow(row, row.event_id || row.key));
  supportNodes.forEach((node) => pushEvidenceRow({
    key: `node:${node.id}`,
    node_id: node.id,
    layer: node.layer,
    subtype: node.subtype,
    text: [node.title, node.summary].filter(Boolean).join('\n'),
    timestamp: node.timestamp,
    base_score: 0.62 - ((node.depth || 1) * 0.06),
    match_reason: 'downward_support'
  }));
  evidenceRows.forEach((row) => pushEvidenceRow(row));

  const evidence = prioritizedEvidenceRows.slice(0, 100).map((row) => ({
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
    text: String(row.text || '').slice(0, 8000)
  }));

  const traceSummary = [
    `Mode: ${retrievalPlan.mode}`,
    `Router: ${retrievalPlan.source_mode || retrievalPlan.strategy_mode || 'memory_only'}`,
    `Seeds: ${seedNodeIds.join(', ') || 'none'}`,
    ...(retrievalPlan.applied_date_range ? [`Applied date window: ${retrievalPlan.applied_date_range.start} -> ${retrievalPlan.applied_date_range.end}`] : []),
    `Stage 1: hybrid seed search returned ${seeds.length} primary seeds.`,
    `Stage 2: graph expansion added ${graph.expandedNodes.length} connected nodes (${supportNodes.length} support, ${evidenceNodes.length} evidence).`,
    ...(sourceRefEvidenceRows.length ? [`Stage 3: loaded ${sourceRefEvidenceRows.length} raw source events attached to matched episodes.`] : []),
    ...summarizeRetrievalThought(retrievalPlan)
  ];

  const contextSections = [];
  if (seeds.length) {
    contextSections.push(`SEED NODES:\n${seeds.map((seed) => `- [${seed.layer || 'node'}] ${String(seed.text || '').slice(0, 180)}`).join('\n')}`);
  }
  if (graph.expandedNodes.length) {
    contextSections.push(`EXPANDED GRAPH:\n${graph.expandedNodes.slice(0, MAX_EXPANDED).map((node) => `- [${node.layer}${node.subtype ? `/${node.subtype}` : ''}] ${node.title}${node.summary ? ` — ${node.summary}` : ''}`).join('\n')}`);
  }
  const allEdgePaths = [...graph.edgePaths, ...sourceRefEdges];
  if (allEdgePaths.length) {
    contextSections.push(`TRACE:\n${allEdgePaths.slice(0, 20).map((edge) => `- ${edge.from} -> ${edge.to} via ${edge.relation}${edge.trace_label ? ` (${edge.trace_label})` : ''}`).join('\n')}`);
  }

  const retrievalRunId = await logRetrievalRun({
    query,
    mode: retrievalPlan.mode,
    metadata: {
      plan: retrievalPlan,
      seeds: seedNodeIds,
      evidence_count: evidence.length,
      source_ref_events: sourceRefEvidenceRows.length
    }
  });

  const drilldownRefs = Array.from(new Set(
    evidence.flatMap((item) => [
      ...(item.source_refs || []),
      item.event_id || null
    ]).filter(Boolean)
  )).slice(0, 100);

  return {
    retrieval_run_id: retrievalRunId,
    retrieval_plan: retrievalPlan,
    generated_queries: {
      query_bundle: retrievalPlan.query_bundle || null,
      semantic: retrievalPlan.semantic_queries || [],
      messages: retrievalPlan.message_queries || [],
      web: retrievalPlan.web_queries || [],
      lexical_terms: retrievalPlan.lexical_terms || [],
      debug: retrievalPlan.query_debug || null
    },
    router: {
      source_mode: retrievalPlan.source_mode || retrievalPlan.strategy_mode || 'memory_only',
      router_reason: retrievalPlan.router_reason || retrievalPlan.web_gate_reason || '',
      time_scope: retrievalPlan.time_scope || null,
      summary_vs_raw: retrievalPlan.summary_vs_raw || 'summary'
    },
    query_sets: retrievalPlan.query_sets || {
      memory_queries: retrievalPlan.semantic_queries || [],
      message_queries: retrievalPlan.message_queries || [],
      web_queries: retrievalPlan.web_queries || []
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
    seed_nodes: primaryNodes,
    primary_nodes: primaryNodes,
    support_nodes: supportNodes,
    evidence_nodes: evidenceNodes,
    expanded_nodes: graph.expandedNodes,
    graph_expansion_results: graph.expandedNodes,
    edge_paths: allEdgePaths,
    trace_labels: allEdgePaths.map((edge) => edge.trace_label).filter(Boolean),
    lazy_source_refs: drilldownRefs.map((ref) => ({ ref })),
    drilldown_refs: drilldownRefs,
    ranking_policy: {
      date_field: 'anchor_at',
      freshness_field: 'latest_activity_at',
      seed_then_expand: true,
      summary_vs_raw: retrievalPlan.summary_vs_raw || 'summary'
    },
    packed_context_stats: {
      primary_nodes: primaryNodes.length,
      support_nodes: supportNodes.length,
      evidence_nodes: evidenceNodes.length,
      packed_evidence: evidence.length
    },
    evidence_count: evidence.length,
    evidence,
    contextText: contextSections.join('\n\n')
  };
}

module.exports = {
  buildHybridGraphRetrieval
};
