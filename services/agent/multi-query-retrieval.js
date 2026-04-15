const db = require('../db');
const { generateEmbedding, cosineSimilarity } = require('../embedding-engine');
const {
  buildRetrievalThought,
  buildSearchQueries,
  isMessageLikeRow
} = require('./retrieval-thought-system');

function asObj(value) {
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

function normalizeDateRange(dateRange) {
  if (!dateRange || typeof dateRange !== 'object') return null;
  const start = dateRange.start ? new Date(dateRange.start) : null;
  const end = dateRange.end ? new Date(dateRange.end) : null;
  if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null;
  return { start, end };
}

function normalizeAppFilter(app) {
  if (!app) return [];
  if (Array.isArray(app)) return app.map((a) => String(a || '').trim()).filter(Boolean);
  return [String(app).trim()].filter(Boolean);
}

function normalizeDataSource(dataSource) {
  const raw = String(dataSource || 'auto').toLowerCase();
  if (raw === 'raw' || raw === 'summaries' || raw === 'auto') return raw;
  return 'auto';
}

function normalizeTerms(text) {
  return String(text || '')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 3)
    .slice(0, 24);
}

function lexicalScore(text, terms) {
  if (!terms.length) return 0;
  const hay = String(text || '').toLowerCase();
  let score = 0;
  for (const term of terms) {
    if (hay.includes(term)) score += 1;
  }
  return score;
}

function uniquePush(arr, value, limit) {
  if (!value) return;
  if (arr.includes(value)) return;
  arr.push(value);
  if (arr.length > limit) arr.splice(limit);
}

function buildQueryVariants(query, max = 7) {
  const built = buildSearchQueries(query, { max });
  if (Array.isArray(built) && built.length) return built;
  const fallback = String(query || '').trim() || 'recent activity next concrete step';
  return [
    fallback,
    `${fallback} status`,
    `${fallback} open loop`
  ].slice(0, max);
}

function chooseAutoDataSource(query, dateRange) {
  const lower = String(query || '').toLowerCase();
  if (/week|month|summary|overview|pattern|usually|history/.test(lower)) return 'summaries';
  if (/exact|error|message|line|today|yesterday|now/.test(lower)) return 'raw';
  if (!dateRange) return 'raw';
  const spanMs = dateRange.end.getTime() - dateRange.start.getTime();
  const spanDays = spanMs / (24 * 60 * 60 * 1000);
  return spanDays > 3 ? 'summaries' : 'raw';
}

function rowInDateRange(row, dateRange) {
  if (!dateRange) return true;
  const ts = parseTs(row.timestamp) || parseTs(row.date);
  if (!ts) return false;
  return ts >= dateRange.start.getTime() && ts <= dateRange.end.getTime();
}

function rowMatchesApp(row, appFilter) {
  if (!appFilter.length) return true;
  const app = String(row.app || '').toLowerCase();
  if (!app) return false;
  return appFilter.some((a) => app.includes(String(a).toLowerCase()));
}

function dedupeByIdentity(results, limit) {
  const byId = new Map();
  for (const row of results) {
    const key = row.node_id || row.event_id || row.id;
    if (!key) continue;
    const prev = byId.get(key);
    if (!prev || (row.score || 0) > (prev.score || 0)) {
      byId.set(key, row);
    }
  }
  return Array.from(byId.values())
    .sort((a, b) => {
      if ((b.score || 0) !== (a.score || 0)) return (b.score || 0) - (a.score || 0);
      return parseTs(b.timestamp) - parseTs(a.timestamp);
    })
    .slice(0, limit);
}

function recencyScore(row, endTs = Date.now()) {
  const ts = parseTs(row.timestamp) || parseTs(row.date);
  if (!ts) return 0;
  const ageMs = Math.max(0, endTs - ts);
  const horizonMs = 7 * 24 * 60 * 60 * 1000;
  return Math.max(0, 1 - (ageMs / horizonMs));
}

async function loadAllChunks() {
  return db.allQuery(`
    SELECT id, event_id, node_id, chunk_index, text, embedding, timestamp, date, app, data_source, metadata
    FROM text_chunks
    ORDER BY timestamp DESC
    LIMIT 2500
  `);
}

async function expandNeighbors(seedIds, maxDepth = 2, maxNodes = 80) {
  const seeds = Array.from(new Set((seedIds || []).filter(Boolean)));
  if (!seeds.length) return [];

  const seen = new Set(seeds);
  const queue = seeds.map((id) => ({ id, depth: 0 }));
  const expanded = [];

  while (queue.length && expanded.length < maxNodes) {
    const current = queue.shift();
    if (current.depth >= maxDepth) continue;

    const edges = await db.allQuery(
      `SELECT from_id, to_id, relation, data FROM edges WHERE from_id = ? OR to_id = ? LIMIT 250`,
      [current.id, current.id]
    );

    for (const edge of edges) {
      const neighbor = edge.from_id === current.id ? edge.to_id : edge.from_id;
      if (!neighbor || seen.has(neighbor)) continue;
      seen.add(neighbor);
      expanded.push({
        id: neighbor,
        relation: edge.relation,
        via: current.id,
        depth: current.depth + 1,
        edge_data: asObj(edge.data)
      });
      queue.push({ id: neighbor, depth: current.depth + 1 });
      if (expanded.length >= maxNodes) break;
    }
  }

  if (!expanded.length) return [];
  const ids = expanded.map((e) => e.id);
  const placeholders = ids.map(() => '?').join(',');
  const rows = await db.allQuery(`SELECT id, type, data FROM nodes WHERE id IN (${placeholders})`, ids);
  const byId = new Map(rows.map((r) => [r.id, r]));

  return expanded.map((entry) => ({
    ...entry,
    node_type: byId.get(entry.id)?.type || 'event_or_chunk',
    data: asObj(byId.get(entry.id)?.data)
  }));
}

async function retrieveMultiQueryContext({ query, options = {}, limit = 24 } = {}) {
  const normalizedQuery = String(query || '').trim();
  const dateRange = normalizeDateRange(options.date_range || null);
  const appFilter = normalizeAppFilter(options.app);
  const requestedDataSource = normalizeDataSource(options.data_source);
  const resolvedDataSource = requestedDataSource === 'auto'
    ? chooseAutoDataSource(normalizedQuery, dateRange)
    : requestedDataSource;
  const retrievalThought = options.retrieval_thought || buildRetrievalThought({
    query: normalizedQuery,
    mode: options.mode || 'chat',
    candidate: options.candidate || null,
    dateRange: dateRange
      ? { start: dateRange.start.toISOString(), end: dateRange.end.toISOString() }
      : null,
    app: appFilter
  });
  const queryVariants = Array.isArray(retrievalThought.search_queries)
    ? retrievalThought.search_queries.slice(0, 7)
    : buildQueryVariants(normalizedQuery, 7);
  const ensuredQueryVariants = queryVariants.length
    ? queryVariants
    : buildQueryVariants(normalizedQuery || 'recent activity', 7);
  const messageQueryVariants = Array.isArray(retrievalThought.search_queries_messages)
    ? retrievalThought.search_queries_messages.slice(0, 5)
    : [];

  const chunkRows = await loadAllChunks();
  const filteredChunks = chunkRows.filter((row) => {
    if (!rowMatchesApp(row, appFilter)) return false;
    if (!rowInDateRange(row, dateRange)) return false;
    if (resolvedDataSource === 'raw' && row.data_source !== 'raw') return false;
    if (resolvedDataSource === 'summaries' && row.data_source !== 'summaries') return false;
    return true;
  });
  const messageLikeRows = filteredChunks.filter((row) => isMessageLikeRow({
    ...row,
    metadata: asObj(row.metadata)
  }));

  const scored = [];
  const scoreRowsForVariants = async (variants, rows, channel) => {
    for (const variant of variants) {
      const variantEmbedding = await generateEmbedding(variant, process.env.OPENAI_API_KEY);
      const terms = normalizeTerms(variant);

      for (const row of rows) {
        let chunkEmbedding = [];
        try {
          chunkEmbedding = JSON.parse(row.embedding || '[]');
        } catch (_) {
          chunkEmbedding = [];
        }
        const vector = cosineSimilarity(variantEmbedding, chunkEmbedding);
        const lexical = lexicalScore(`${row.text} ${JSON.stringify(asObj(row.metadata))}`, terms) * 0.05;
        const channelBoost = channel === 'messages' ? 0.06 : 0;
        const score = Number((vector + lexical + channelBoost).toFixed(6));
        if (score <= 0) continue;
        scored.push({
          ...row,
          score,
          query_variant: variant,
          search_channel: channel
        });
      }
    }
  };

  await scoreRowsForVariants(ensuredQueryVariants, filteredChunks, 'screen');
  if (messageQueryVariants.length) {
    await scoreRowsForVariants(messageQueryVariants, messageLikeRows.length ? messageLikeRows : filteredChunks, 'messages');
  }
  // Safety fallback when embeddings are sparse: keep query-driven lexical+recency scoring.
  if (!scored.length) {
    const refTs = dateRange ? dateRange.end.getTime() : Date.now();
    const lexicalTerms = normalizeTerms(ensuredQueryVariants.join(' '));
    for (const row of filteredChunks) {
      const lexical = lexicalScore(`${row.text} ${JSON.stringify(asObj(row.metadata))}`, lexicalTerms) * 0.05;
      const score = Number((0.2 + recencyScore(row, refTs) + lexical).toFixed(6));
      if (score <= 0) continue;
      scored.push({
        ...row,
        score,
        query_variant: ensuredQueryVariants[0] || normalizedQuery || 'recent activity',
        search_channel: 'screen'
      });
    }
  }

  const merged = dedupeByIdentity(scored, limit);
  const seedNodeIds = Array.from(new Set(merged.map((r) => r.node_id || r.event_id).filter(Boolean))).slice(0, 20);
  const expandedNeighbors = await expandNeighbors(seedNodeIds, 2, 80);

  const evidence = merged.map((row) => ({
    id: row.id,
    event_id: row.event_id,
    node_id: row.node_id,
    score: row.score,
    timestamp: row.timestamp || null,
    app: row.app || null,
    data_source: row.data_source || 'raw',
    search_channel: row.search_channel || 'screen',
    query_variant: row.query_variant || null,
    text: String(row.text || '').slice(0, 280)
  }));

  const contextText = merged
    .slice(0, 14)
    .map((row) => `- [${row.app || 'Unknown'} | ${row.data_source}] ${String(row.text || '').slice(0, 180)}`)
    .join('\n');

  return {
    query_variants: ensuredQueryVariants,
    message_query_variants: messageQueryVariants,
    applied_filters: {
      app: appFilter.length ? appFilter : undefined,
      date_range: dateRange
        ? { start: dateRange.start.toISOString(), end: dateRange.end.toISOString() }
        : undefined,
      data_source: options.data_source ? resolvedDataSource : undefined,
      retrieval_mode: retrievalThought.mode
    },
    data_source_resolved: resolvedDataSource,
    seed_nodes: seedNodeIds,
    expanded_neighbors: expandedNeighbors,
    evidence,
    contextText,
    retrieval_thought: retrievalThought,
    source_mix: merged.reduce((acc, row) => {
      const key = row.data_source || 'raw';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {})
  };
}

module.exports = {
  retrieveMultiQueryContext,
  buildQueryVariants
};
