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
  anchorDate = null,
  anchorAt = null,
  importance = null,
  connectionCount = null,
  lastReheated = null
}) {
  const now = new Date().toISOString();
  const titleText = String(title || '').trim();
  const summaryText = String(summary || '').trim();
  const canonical = String(canonicalText || titleText || summaryText || '').trim();
  const metadataObj = asObj(metadata);
  const resolvedAnchorAt = anchorAt || metadataObj.anchor_at || metadataObj.latest_activity_at || metadataObj.occurred_at || metadataObj.timestamp || createdAt || now;
  const resolvedAnchorDate = anchorDate || asObj(metadata).anchor_date || (resolvedAnchorAt ? resolvedAnchorAt.slice(0, 10) : now.slice(0, 10));

  // Calculate importance: 1-10 scale based on connection density
  // importance = min(10, 1 + (connection_count / 10))
  const sourceRefsArray = Array.isArray(sourceRefs) ? sourceRefs : [];
  const connCount = typeof connectionCount === 'number' ? connectionCount : sourceRefsArray.length;
  let nodeImportance = importance;
  if (typeof nodeImportance !== 'number') {
    nodeImportance = Math.min(10, Math.max(1, 1 + Math.floor(connCount / 10)));
  }

  // Track when node was last reheated (last time evidence pointed to it)
  const nodeLastReheated = lastReheated || metadataObj.last_reheated || now;

  const resolvedMetadata = {
    ...metadataObj,
    node_type: metadataObj.node_type || subtype || layer,
    anchor_at: resolvedAnchorAt,
    anchor_date: resolvedAnchorDate,
    timestamp: metadataObj.timestamp || resolvedAnchorAt,
    last_seen: metadataObj.last_seen || metadataObj.latest_activity_at || resolvedAnchorAt,
    importance: nodeImportance,
    priority: metadataObj.priority ?? nodeImportance,
    connection_count: connCount,
    last_reheated: nodeLastReheated,
    sentiment: metadataObj.sentiment ?? metadataObj.sentiment_score ?? null
  };

  await db.runQuery(
    `INSERT OR REPLACE INTO memory_nodes
     (id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date, anchor_at, importance, connection_count, last_reheated)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      id,
      layer,
      subtype,
      titleText,
      summaryText,
      canonical,
      Number(confidence || 0),
      status || 'active',
      JSON.stringify(sourceRefsArray),
      JSON.stringify(resolvedMetadata),
      graphVersion,
      createdAt || now,
      updatedAt || now,
      JSON.stringify(Array.isArray(embedding) ? embedding : []),
      resolvedAnchorDate,
      resolvedAnchorAt,
      nodeImportance,
      connCount,
      nodeLastReheated
    ]
  );

  // Mirror into the legacy table while other parts of the app still read it.
  await upsertGraphNode({
    id,
    type: layer,
    subtype,
    sourceRef: sourceRefsArray[0] || null,
    version: graphVersion,
    data: {
      title: titleText,
      summary: summaryText,
      canonical_text: canonical,
      confidence: Number(confidence || 0),
      status: status || 'active',
      source_refs: sourceRefsArray,
      importance: nodeImportance,
      connection_count: connCount,
      last_reheated: nodeLastReheated,
      ...resolvedMetadata
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
    anchorDate: updates.anchor_date || existing.anchor_date,
    anchorAt: updates.anchor_at || existing.anchor_at
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

function buildRetrievalDocBreadcrumb(metadata = {}, fallback = {}) {
  const meta = asObj(metadata);
  const envelope = asObj(meta.envelope);
  const app = meta.source_app || meta.app || envelope.app || fallback.app || '';
  const appId = meta.app_id || envelope.app_id || envelope.metadata?.app_id || '';
  const dataSource = meta.data_source || envelope.metadata?.data_source || meta.storage_data_source || '';
  const context = meta.context_title || meta.window_title || envelope.window_title || meta.title || '';
  const timestamp = meta.occurred_at || meta.anchor_at || envelope.occurred_at || fallback.timestamp || '';
  const file = meta.file || meta.path || envelope.metadata?.file || envelope.metadata?.path || '';
  const surface = meta.content_type || envelope.metadata?.content_type || meta.layer || '';
  const activity = meta.activity_type || envelope.metadata?.activity_type || '';
  const people = Array.isArray(meta.person_labels) && meta.person_labels.length
    ? meta.person_labels
    : (Array.isArray(envelope.participants) ? envelope.participants : []);
  return [
    dataSource ? `[SOURCE: ${String(dataSource).slice(0, 40)}]` : '',
    app ? `[APP: ${String(app).slice(0, 40)}]` : '',
    appId ? `[APP_ID: ${String(appId).slice(0, 60)}]` : '',
    file ? `[FILE: ${String(file).slice(0, 80)}]` : '',
    context ? `[CONTEXT: ${String(context).slice(0, 80)}]` : '',
    timestamp ? `[TIME: ${String(timestamp).slice(0, 19)}]` : '',
    surface ? `[SURFACE: ${String(surface).slice(0, 30)}]` : '',
    activity ? `[ACTIVITY: ${String(activity).slice(0, 30)}]` : '',
    people.length ? `[PEOPLE: ${people.slice(0, 5).join(', ')}]` : ''
  ].filter(Boolean).join('');
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
  const metadataObj = asObj(metadata);
  const breadcrumb = metadataObj.retrieval_breadcrumb || buildRetrievalDocBreadcrumb(metadataObj, { app, timestamp });
  const searchableContent = breadcrumb && !content.startsWith(breadcrumb)
    ? `${breadcrumb}\n${content}`.trim()
    : content;
  const enrichedMetadata = {
    ...metadataObj,
    retrieval_breadcrumb: breadcrumb || metadataObj.retrieval_breadcrumb || null,
    source_app: metadataObj.source_app || metadataObj.app || app || null,
    app_id: metadataObj.app_id || null,
    entity_tags: Array.isArray(metadataObj.entity_tags) ? metadataObj.entity_tags : (metadataObj.entity_labels || []),
    occurred_at: metadataObj.occurred_at || metadataObj.anchor_at || timestamp || null
  };

  await db.runQuery(
    `INSERT OR REPLACE INTO retrieval_docs
     (doc_id, source_type, node_id, event_id, app, timestamp, text, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
     [docId, sourceType, nodeId, eventId, app, timestamp, searchableContent, JSON.stringify(enrichedMetadata)]
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
  await db.runQuery(`DELETE FROM memory_edges WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})`, [...ids, ...ids]).catch(() => {});
  await db.runQuery(`DELETE FROM memory_nodes WHERE id IN (${placeholders})`, ids).catch(() => {});
  await removeNodeArtifactsByVersion(version).catch(() => {});
}

async function clearZeroBaseMemory({ includeEvents = false } = {}) {
  const statements = [
    `DELETE FROM memory_edges`,
    `DELETE FROM memory_nodes`,
    `DELETE FROM suggestion_artifacts`,
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
