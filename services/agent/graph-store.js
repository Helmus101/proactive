const crypto = require('crypto');
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

function toText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function stableHash(input) {
  return crypto.createHash('sha1').update(String(input || '')).digest('hex').slice(0, 16);
}

const EDGE_INVERSIONS = {
  'MENTIONS': 'MENTIONED_BY',
  'FOLLOWS_UP': 'PRECEDED_BY',
  'PART_OF_EPISODE': 'CONTAINS_EVENT',
  'ABSTRACTED_TO': 'ABSTRACTED_FROM',
  'HYPOTHESIZES_FROM': 'SUPPORTS_HYPOTHESIS',
  'PROMOTED_FROM': 'PROMOTED_TO',
  'RELATED_TO': 'RELATED_TO',
  'GENERATED_FROM': 'GENERATED',
  'CONTRADICTS': 'CONTRADICTS',
  'SUPERSEDES': 'SUPERSEDED_BY',
  'DEPENDS_ON': 'PREREQUISITE_FOR'
};

const MEMORY_LAYERS = {
  RAW: 'raw',
  EPISODE: 'episode',
  SEMANTIC: 'semantic',
  CLOUD: 'cloud',
  INSIGHT: 'insight',
  CORE: 'core'
};

async function upsertGraphNode({
  id,
  type,
  subtype = null,
  sourceRef = null,
  version = null,
  data = {},
  embedding = []
}) {
  await db.runQuery(
    `INSERT OR REPLACE INTO nodes (id, type, subtype, source_ref, version, data, embedding)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      id,
      type,
      subtype,
      sourceRef,
      version,
      JSON.stringify(data || {}),
      JSON.stringify(Array.isArray(embedding) ? embedding : [])
    ]
  );
}

async function upsertMemoryNode({
  id,
  layer,
  subtype = null,
  title = '',
  summary = '',
  canonicalText = '',
  confidence = 0,
  status = 'active',
  sourceRefs = [],
  metadata = {},
  graphVersion = null,
  createdAt = null,
  updatedAt = null,
  embedding = [],
  anchorDate = null
}) {
  const now = new Date().toISOString();
  const titleText = String(title || '').trim();
  const summaryText = String(summary || '').trim();
  const canonical = String(canonicalText || titleText || summaryText || '').trim();
  const resolvedAnchorDate = anchorDate || asObj(metadata).anchor_date || null;
  await db.runQuery(
    `INSERT OR REPLACE INTO memory_nodes
     (id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      id,
      layer,
      subtype,
      titleText,
      summaryText,
      canonical,
      Number(confidence || 0),
      status || 'active',
      JSON.stringify(Array.isArray(sourceRefs) ? sourceRefs : []),
      JSON.stringify(metadata || {}),
      graphVersion,
      createdAt || now,
      updatedAt || now,
      JSON.stringify(Array.isArray(embedding) ? embedding : []),
      resolvedAnchorDate
    ]
  );

  // Mirror into the legacy table while other parts of the app still read it.
  await upsertGraphNode({
    id,
    type: layer,
    subtype,
    sourceRef: Array.isArray(sourceRefs) ? sourceRefs[0] || null : null,
    version: graphVersion,
    data: {
      title: titleText,
      summary: summaryText,
      canonical_text: canonical,
      confidence: Number(confidence || 0),
      status: status || 'active',
      source_refs: Array.isArray(sourceRefs) ? sourceRefs : [],
      ...asObj(metadata)
    },
    embedding
  }).catch(() => {});
}

async function updateMemoryNode(id, updates = {}) {
  const existing = await db.getQuery(`SELECT * FROM memory_nodes WHERE id = ?`, [id]);
  if (!existing) return null;

  const metadata = { ...asObj(existing.metadata), ...asObj(updates.metadata) };
  const node = {
    id: existing.id,
    layer: updates.layer || existing.layer,
    subtype: updates.subtype || existing.subtype,
    title: updates.title || existing.title,
    summary: updates.summary || existing.summary,
    canonicalText: updates.canonical_text || existing.canonical_text,
    confidence: updates.confidence !== undefined ? updates.confidence : existing.confidence,
    status: updates.status || existing.status,
    sourceRefs: updates.source_refs || asObj(existing.source_refs),
    metadata,
    graphVersion: existing.graph_version,
    createdAt: existing.created_at,
    updatedAt: new Date().toISOString(),
    embedding: updates.embedding || asObj(existing.embedding),
    anchorDate: updates.anchor_date || existing.anchor_date
  };

  await upsertMemoryNode(node);
  return node;
}

async function upsertGraphEdge({
  fromId,
  toId,
  relation,
  traceLabel = null,
  data = {}
}) {
  await db.runQuery(
    `INSERT OR IGNORE INTO edges (from_id, to_id, relation, trace_label, data)
     VALUES (?, ?, ?, ?, ?)`,
    [fromId, toId, relation, traceLabel, JSON.stringify(data || {})]
  );
}

async function upsertMemoryEdge({
  fromNodeId,
  toNodeId,
  edgeType,
  weight = 1,
  traceLabel = null,
  evidenceCount = 1,
  metadata = {},
  createdAt = null
}) {
  const now = createdAt || new Date().toISOString();
  function buildParams(from, to, type, label) {
    return [
      from, to, type,
      Number.isFinite(Number(weight)) ? Number(weight) : 1,
      label,
      Math.max(1, Number(evidenceCount || 1)),
      JSON.stringify(metadata || {}),
      now
    ];
  }

  await db.runQuery(
    `INSERT OR IGNORE INTO memory_edges
     (from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    buildParams(fromNodeId, toNodeId, edgeType, traceLabel)
  );

  const inverseType = EDGE_INVERSIONS[edgeType];
  if (inverseType) {
    await db.runQuery(
      `INSERT OR IGNORE INTO memory_edges
       (from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      buildParams(toNodeId, fromNodeId, inverseType, traceLabel ? `(Inverse) ${traceLabel}` : null)
    );
  }

  await upsertGraphEdge({
    fromId: fromNodeId,
    toId: toNodeId,
    relation: edgeType,
    traceLabel,
    data: metadata
  }).catch(() => {});
}

function buildRetrievalDocText({ title = '', summary = '', text = '', data = {} }) {
  const meta = asObj(data);
  return [
    title,
    summary,
    text,
    meta.fact,
    meta.name,
    meta.domain,
    meta.url,
    meta.subject,
    meta.description,
    meta.trigger_summary
  ].filter(Boolean).join('\n');
}

async function upsertRetrievalDoc({
  docId,
  sourceType,
  nodeId = null,
  eventId = null,
  app = null,
  timestamp = null,
  text = '',
  metadata = {}
}) {
  const content = String(text || '').trim();
  if (!content) return;

  await db.runQuery(
    `INSERT OR REPLACE INTO retrieval_docs
     (doc_id, source_type, node_id, event_id, app, timestamp, text, metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [docId, sourceType, nodeId, eventId, app, timestamp, content, JSON.stringify(metadata || {})]
  );
  await db.runQuery(`DELETE FROM retrieval_docs_fts WHERE doc_id = ?`, [docId]).catch(() => {});
  await db.runQuery(
    `INSERT INTO retrieval_docs_fts (doc_id, text) VALUES (?, ?)`,
    [docId, content]
  );
}

async function removeNodeArtifactsByVersion(version) {
  if (!version) return;
  const rows = await db.allQuery(
    `SELECT id FROM nodes WHERE version = ?`,
    [version]
  ).catch(() => []);
  const ids = rows.map((row) => row.id).filter(Boolean);
  if (!ids.length) return;

  const placeholders = ids.map(() => '?').join(',');
  await db.runQuery(`DELETE FROM retrieval_docs WHERE node_id IN (${placeholders})`, ids).catch(() => {});
  for (const id of ids) {
    await db.runQuery(`DELETE FROM retrieval_docs_fts WHERE doc_id = ?`, [`node:${id}`]).catch(() => {});
  }
  await db.runQuery(`DELETE FROM edges WHERE from_id IN (${placeholders}) OR to_id IN (${placeholders})`, [...ids, ...ids]).catch(() => {});
  await db.runQuery(`DELETE FROM nodes WHERE id IN (${placeholders})`, ids).catch(() => {});
}

async function removeMemoryArtifactsByVersion(version) {
  if (!version) return;
  const rows = await db.allQuery(`SELECT id FROM memory_nodes WHERE graph_version = ?`, [version]).catch(() => []);
  const ids = rows.map((row) => row.id).filter(Boolean);
  if (!ids.length) return;
  const placeholders = ids.map(() => '?').join(',');
  await db.runQuery(`DELETE FROM retrieval_docs WHERE node_id IN (${placeholders})`, ids).catch(() => {});
  for (const id of ids) {
    await db.runQuery(`DELETE FROM retrieval_docs_fts WHERE doc_id = ?`, [`node:${id}`]).catch(() => {});
  }
  await db.runQuery(`DELETE FROM memory_edges WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})`, [...ids, ...ids]).catch(() => {});
  await db.runQuery(`DELETE FROM memory_nodes WHERE id IN (${placeholders})`, ids).catch(() => {});
  await removeNodeArtifactsByVersion(version).catch(() => {});
}

async function clearZeroBaseMemory({ includeEvents = false } = {}) {
  const statements = [
    `DELETE FROM memory_edges`,
    `DELETE FROM memory_nodes`,
    `DELETE FROM suggestion_artifacts`,
    `DELETE FROM retrieval_docs_fts`,
    `DELETE FROM retrieval_docs`,
    `DELETE FROM text_chunks`,
    `DELETE FROM graph_versions`,
    `DELETE FROM retrieval_runs`,
    `DELETE FROM edges`,
    `DELETE FROM nodes`
  ];
  if (includeEvents) {
    statements.unshift(`DELETE FROM event_entities`);
    statements.unshift(`DELETE FROM events`);
  }
  for (const sql of statements) {
    await db.runQuery(sql).catch(() => {});
  }
}

async function logGraphVersion(version, status, metadata = {}) {
  const now = new Date().toISOString();
  if (status === 'started') {
    await db.runQuery(
      `INSERT OR REPLACE INTO graph_versions (version, status, started_at, completed_at, metadata)
       VALUES (?, ?, ?, NULL, ?)`,
      [version, status, now, JSON.stringify(metadata || {})]
    );
    return;
  }

  const existing = await db.getQuery(`SELECT started_at FROM graph_versions WHERE version = ?`, [version]).catch(() => null);
  await db.runQuery(
    `INSERT OR REPLACE INTO graph_versions (version, status, started_at, completed_at, metadata)
     VALUES (?, ?, ?, ?, ?)`,
    [version, status, existing?.started_at || now, now, JSON.stringify(metadata || {})]
  );
}

async function logRetrievalRun({
  query = '',
  mode = '',
  metadata = {}
}) {
  const id = `retr_${Date.now()}_${stableHash(`${query}|${mode}|${JSON.stringify(metadata || {})}`)}`;
  await db.runQuery(
    `INSERT OR REPLACE INTO retrieval_runs (id, query, mode, created_at, metadata)
     VALUES (?, ?, ?, ?, ?)`,
    [id, query, mode, new Date().toISOString(), JSON.stringify(metadata || {})]
  ).catch(() => {});
  return id;
}

module.exports = {
  asObj,
  toText,
  stableHash,
  MEMORY_LAYERS,
  upsertGraphNode,
  upsertMemoryNode,
  updateMemoryNode,
  upsertGraphEdge,
  upsertMemoryEdge,
  buildRetrievalDocText,
  upsertRetrievalDoc,
  removeNodeArtifactsByVersion,
  removeMemoryArtifactsByVersion,
  clearZeroBaseMemory,
  logGraphVersion,
  logRetrievalRun
};
