const db = require('../db');
const { expandAppScopeValues, inferExplicitAppsFromText } = require('../app-scope-catalog');
const { buildRawEvidenceText } = require('../raw-evidence-text');
const { generateEmbedding, cosineSimilarity } = require('../embedding-engine');
const {
  buildRetrievalThought,
  summarizeRetrievalThought
} = require('./retrieval-thought-system');
const {
  asObj,
  logRetrievalRun,
  updateMemoryNode,
  upsertRetrievalDoc
} = require('./graph-store');
const { externalRerank } = require('./reranker');

function intelligenceEngine() {
  try {
    return require('./intelligence-engine');
  } catch (_) {
    return {};
  }
}

const DEFAULT_SEED_LIMIT = 25;
const DEFAULT_HOP_LIMIT = 3;
const MAX_EXPANDED = 90;

function estimateTokensHeuristic(text) {
  return Math.ceil((text || '').length / 4);
}

function formatContext({
  budget = 2000,
  primarySeeds = [],
  hierarchicalExpandedNodes = [],
  seeds = [],
  expandedNodes = [],
  edgePaths = []
} = {}) {
  let usedTokens = 0;
  const contextSections = [];

  const addSection = (header, items, formatter) => {
    if (!items || !items.length) return;
    const formattedItems = [];
    const seenText = new Set();

    for (const item of items) {
      const line = formatter(item);
      const textKey = line.trim();
      if (seenText.has(textKey)) continue;
      seenText.add(textKey);

      const tokens = estimateTokensHeuristic(line);
      if (usedTokens + tokens > budget) {
        if (usedTokens < budget) {
          const remaining = budget - usedTokens;
          if (remaining > 20) {
            formattedItems.push(line.slice(0, remaining * 4) + '... [TRUNCATED]');
            usedTokens = budget;
          }
        }
        break;
      }
      formattedItems.push(line);
      usedTokens += tokens;
    }
    if (formattedItems.length) {
      contextSections.push(`${header}:\n${formattedItems.join('\n')}`);
    }
  };

  addSection('PRIMARY SEARCH SEEDS', primarySeeds, (seed) => `- [${seed.layer || 'node'}] ${seed.title || seed.node_id}: ${String(seed.text || '').slice(0, 4000)}`);
  addSection('HIERARCHICAL EXPANSION', hierarchicalExpandedNodes, (node) => `- [${node.layer}] ${node.title}: ${String(node.text || node.summary || '').slice(0, 4000)}`);
  addSection('SEED NODES', seeds, (seed) => `- [${seed.layer || 'node'}] ${String(seed.text || '').slice(0, 180)}`);
  addSection('EXPANDED GRAPH', expandedNodes.slice(0, MAX_EXPANDED), (node) => `- [${node.layer}${node.subtype ? `/${node.subtype}` : ''}] ${node.title}${node.summary ? ` — ${node.summary}` : ''}`);

  if (edgePaths && edgePaths.length) {
    addSection('TRACE', edgePaths.slice(0, 20), (edge) => `- ${edge.from} -> ${edge.to} via ${edge.relation}${edge.trace_label ? ` (${edge.trace_label})` : ''}`);
  }

  return contextSections.join('\n\n');
}

const embeddingCache = new Map();
const EMBEDDING_CACHE_LIMIT = 2000;

function getEmbedding(row) {
  if (!row) return [];
  if (Array.isArray(row.embedding)) return row.embedding;
  if (!row.embedding) return [];

  const cacheKey = row.id || row.doc_id || row.key || row.embedding;
  if (embeddingCache.has(cacheKey)) {
    return embeddingCache.get(cacheKey);
  }

  try {
    const parsed = JSON.parse(row.embedding);
    if (embeddingCache.size >= EMBEDDING_CACHE_LIMIT) {
      const firstKey = embeddingCache.keys().next().value;
      embeddingCache.delete(firstKey);
    }
    embeddingCache.set(cacheKey, parsed);
    return parsed;
  } catch (_) {
    return [];
  }
}

const LAYER_RANKS = {
  'core': 5,
  'insight': 4,
  'cloud': 3,
  'semantic': 2,
  'episode': 1,
  'raw': 0,
  'event': 0
};

const HIERARCHY_SEQUENCE = ['core', 'insight', 'semantic', 'episode', 'raw'];

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

function buildDateRangeFromRecentDays(days = 1) {
  const bounded = Math.max(1, Math.min(30, Number(days || 1)));
  const end = new Date();
  const start = new Date(end.getTime() - (bounded * 24 * 60 * 60 * 1000));
  return { start: start.toISOString(), end: end.toISOString() };
}

function buildCalendarDayRange(dayOffset = 0) {
  const target = new Date();
  target.setDate(target.getDate() - Number(dayOffset || 0));
  const start = new Date(target);
  start.setHours(0, 0, 0, 0);
  const end = new Date(target);
  end.setHours(23, 59, 59, 999);
  return { start: start.toISOString(), end: end.toISOString() };
}

function inferHardFiltersFromQuery(query = '', existingFilters = {}) {
  const q = String(query || '').toLowerCase();
  const current = existingFilters && typeof existingFilters === 'object' ? existingFilters : {};
  const sourceAliases = [
    ['email', 'message'],
    ['message', 'message'],
    ['chat', 'message'],
    ['calendar', 'calendar'],
    ['meeting', 'calendar'],
    ['browser', 'visit'],
    ['history', 'visit'],
    ['youtube', 'visit'],
    ['trailer', 'visit'],
    ['video', 'visit'],
    ['watching', 'visit'],
    ['watched', 'visit'],
    ['screen', 'screen'],
    ['desktop', 'screen'],
    ['capture', 'screen']
  ];

  const inferredApps = inferExplicitAppsFromText(q, 8);
  const inferredSources = sourceAliases.filter(([alias]) => q.includes(alias)).map(([, source]) => source);

  let inferredDateRange = null;
  if (!current.date_range) {
    if (/\btoday\b/.test(q)) inferredDateRange = buildCalendarDayRange(0);
    else if (/\byesterday\b/.test(q)) inferredDateRange = buildCalendarDayRange(1);
    else if (/\b(last|past)\s+7\s*days\b|\bthis week\b/.test(q)) inferredDateRange = buildDateRangeFromRecentDays(7);
    else if (/\b(last|past)\s+30\s*days\b|\bthis month\b/.test(q)) inferredDateRange = buildDateRangeFromRecentDays(30);
    else if (/\brecent\b|\blatest\b|\bjust now\b/.test(q)) inferredDateRange = buildDateRangeFromRecentDays(3);
  }

  const mergedApp = current.app || (inferredApps.length ? Array.from(new Set(inferredApps)) : null);
  const mergedSourceTypes = current.source_types || (inferredSources.length ? Array.from(new Set(inferredSources)) : null);
  const mergedDateRange = current.date_range || inferredDateRange;

  return {
    app: mergedApp,
    source_types: mergedSourceTypes,
    date_range: mergedDateRange,
    hard_predicates: {
      app_inferred: !current.app && inferredApps.length > 0,
      source_inferred: !current.source_types && inferredSources.length > 0,
      date_inferred: !current.date_range && Boolean(inferredDateRange)
    }
  };
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

function tokenizeLexicalQuery(text = '') {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9@._\-/\s]/g, ' ')
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token.length >= 2)
    .slice(0, 16);
}

function buildFtsMatchQuery(tokens = []) {
  const safe = (tokens || [])
    .map((token) => String(token || '').replace(/"/g, '').trim())
    .filter(Boolean)
    .slice(0, 10);
  if (!safe.length) return '';
  return safe.map((token) => `"${token}"`).join(' OR ');
}

function normalizeFilterList(value) {
  return (Array.isArray(value) ? value : (value ? [value] : []))
    .flatMap((item) => Array.isArray(item) ? item : [item])
    .filter(Boolean)
    .map((item) => String(item).trim().toLowerCase())
    .filter(Boolean);
}

function expandAppIdFilterValues(values = []) {
  return expandAppScopeValues(values);
}

function expandAppNameFilterValues(values = []) {
  return expandAppScopeValues(values);
}

function expandSourceTypeFilters(values = []) {
  const expanded = new Set();
  for (const value of normalizeFilterList(values)) {
    expanded.add(value);
    if (value === 'communication') {
      ['email', 'gmail', 'mail', 'message', 'thread', 'slack', 'chat'].forEach((item) => expanded.add(item));
    } else if (value === 'desktop') {
      ['screen', 'capture', 'screenshot', 'screenshot_ocr', 'desktop', 'browser', 'history', 'visit'].forEach((item) => expanded.add(item));
    } else if (value === 'calendar') {
      ['calendar', 'meeting', 'event', 'agenda', 'invite'].forEach((item) => expanded.add(item));
    } else if (value === 'task') {
      ['task', 'todo', 'action', 'deadline', 'follow_up'].forEach((item) => expanded.add(item));
    }
  }
  return Array.from(expanded);
}

function appendMetadataSqlFilters({
  where,
  params,
  metadataFilters = {},
  metadataExpr = 'metadata',
  extraTextExprs = [],
  hardContentTypeFilter = true
} = {}) {
  const filters = metadataFilters && typeof metadataFilters === 'object' ? metadataFilters : {};
  const appendLikeAny = (values, expressions = [metadataExpr]) => {
    const normalized = normalizeFilterList(values);
    if (!normalized.length) return;
    where.push(`(${normalized.map(() => expressions.map((expr) => `LOWER(COALESCE(${expr}, '')) LIKE ?`).join(' OR ')).join(' OR ')})`);
    for (const value of normalized) {
      for (let i = 0; i < expressions.length; i++) {
        params.push(`%${value}%`);
      }
    }
  };

  appendLikeAny(expandAppIdFilterValues(filters.app_id || filters.app_ids), [metadataExpr, ...extraTextExprs]);
  appendLikeAny(filters.entity_tags || filters.entity_ids || filters.person_ids || filters.people, [metadataExpr, ...extraTextExprs]);
  if (hardContentTypeFilter) appendLikeAny(filters.content_type || filters.content_types, [metadataExpr]);
  appendLikeAny(filters.data_source || filters.data_sources, [metadataExpr, ...extraTextExprs]);
  appendLikeAny(filters.activity_type, [metadataExpr]);
  appendLikeAny(filters.session_id, [metadataExpr]);
  appendLikeAny(filters.status, [metadataExpr]);
  appendLikeAny(filters.relationship_tier, [metadataExpr]);
}

function filterMatchesHaystack(filters = [], haystack = '', { passIfEmptyHaystack = true } = {}) {
  const values = normalizeFilterList(filters);
  if (!values.length) return true;
  const hay = String(haystack || '').toLowerCase();
  if (!hay.trim()) return passIfEmptyHaystack;
  return values.some((needle) => hay.includes(needle));
}

function appIdentityMatches(filters = [], row = {}, metadata = {}) {
  const values = expandAppIdFilterValues(filters);
  if (!values.length) return true;
  const hay = [
    metadata.app_id,
    metadata.bundleId,
    metadata.bundle_id,
    metadata.application_id,
    metadata.source_app,
    metadata.app,
    metadata.activeApp,
    row.app,
    row.source_app
  ].filter(Boolean).join(' ').toLowerCase();
  if (!hay.trim()) return true;
  return values.some((needle) => hay.includes(needle));
}

function isRawScreenshotEvidence(row = {}, metadataInput = null) {
  const metadata = metadataInput || asObj(row.metadata);
  const hay = [
    row.source_type,
    row.layer,
    row.subtype,
    row.source_type_group,
    row.data_source,
    metadata.event_type,
    metadata.source_type,
    metadata.data_source,
    metadata.storage_data_source,
    metadata.source_integrity
  ].filter(Boolean).join(' ').toLowerCase();
  return /\b(screen\s*capture|screencapture|screen|capture|screenshot|screenshot_ocr|ocr|raw_event)\b/.test(hay);
}

function isTrueBrowserHistoryEvidence(row = {}, metadataInput = null) {
  const metadata = metadataInput || asObj(row.metadata);
  if (isRawScreenshotEvidence(row, metadata)) return false;
  const hay = [
    row.source_type,
    row.subtype,
    row.source_type_group,
    row.data_source,
    metadata.event_type,
    metadata.source_type,
    metadata.data_source,
    metadata.storage_data_source,
    metadata.source_integrity
  ].filter(Boolean).join(' ').toLowerCase();
  return /\b(browser_history|history|visit)\b/.test(hay);
}

function contentTypeFilterPasses(row = {}, filters = {}, metadataInput = null) {
  const contentTypes = normalizeFilterList(filters.content_type || filters.content_types);
  if (!contentTypes.length) return true;
  const metadata = metadataInput || asObj(row.metadata);
  const actual = `${metadata.content_type || ''} ${metadata.source_integrity || ''} ${row.subtype || ''}`.toLowerCase();
  if (!actual.trim()) return true;
  if (contentTypes.some((needle) => actual.includes(needle))) return true;
  // OCR captures often classify YouTube/browser pages as "general"; keep recall
  // high and let usefulness scoring/reranking decide final evidence quality.
  return isRawScreenshotEvidence(row, metadata);
}

function classifyFilterDropReason(row = {}, filters = {}) {
  const metadata = asObj(row.metadata);
  if (filters.prioritize_screen_capture && isTrueBrowserHistoryEvidence(row, metadata)) return 'prioritize_screen_capture';

  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    const app = `${row.app || ''} ${metadata.source_app || ''} ${metadata.app || ''} ${metadata.app_id || ''}`.toLowerCase();
    if (!appFilter.some((item) => app.includes(item))) return 'app';
  }

  const appIdFilter = expandAppIdFilterValues(filters.app_id || filters.app_ids);
  if (appIdFilter.length && !appIdentityMatches(appIdFilter, row, metadata)) return 'app_id';

  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    const hay = `${row.source_type || ''} ${row.layer || ''} ${row.subtype || ''} ${row.source_type_group || ''} ${metadata.source_type || ''} ${metadata.content_type || ''} ${metadata.source_integrity || ''}`.toLowerCase();
    if (!filterMatchesHaystack(sourceTypeFilter, hay)) return 'source_types';
  }

  const dataSources = normalizeFilterList(filters.data_source || filters.data_sources);
  if (dataSources.length) {
    const actual = `${metadata.data_source || ''} ${metadata.storage_data_source || ''} ${row.source_type || ''} ${row.data_source || ''}`.toLowerCase();
    if (actual.trim() && !dataSources.some((needle) => actual.includes(needle))) return 'data_source';
  }

  if (!contentTypeFilterPasses(row, filters, metadata)) return 'content_type';
  return null;
}

function rowMatchesFilters(row, filters = {}) {
  if (row.id === 'global_core' || row.layer === 'core' || row.layer === 'insight') return true;
  const metadata = asObj(row.metadata);
  const chatHay = [
    row.source_type,
    row.subtype,
    row.source_type_group,
    row.app,
    row.data_source,
    metadata.event_type,
    metadata.source,
    metadata.source_app,
    metadata.app,
    metadata.app_id,
    metadata.data_source
  ].filter(Boolean).join(' ').toLowerCase();
  if (!filters.include_chat_messages && /\b(chatmessage|chat_message|app\.chat|app\.chat\.ui|source:\s*chat)\b/.test(chatHay)) {
    return false;
  }

  if (filters.prioritize_screen_capture) {
    if (isTrueBrowserHistoryEvidence(row, metadata)) return false;
  }

  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    const app = `${row.app || ''} ${metadata.source_app || ''} ${metadata.app || ''} ${metadata.app_id || ''}`.toLowerCase();
    if (!appFilter.some((item) => app.includes(item))) return false;
  }

  const appIdFilter = expandAppIdFilterValues(filters.app_id || filters.app_ids);
  if (appIdFilter.length) {
    if (!appIdentityMatches(appIdFilter, row, metadata)) return false;
  }

  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    const hay = `${row.source_type || ''} ${row.layer || ''} ${row.subtype || ''} ${row.source_type_group || ''} ${metadata.source_type || ''} ${metadata.content_type || ''} ${metadata.source_integrity || ''}`.toLowerCase();
    if (!filterMatchesHaystack(sourceTypeFilter, hay)) return false;
  }

  const entityFilter = filters.entity_ids || filters.entities || filters.person_ids || filters.people;
  const entityList = Array.isArray(entityFilter) ? entityFilter : (entityFilter ? [entityFilter] : []);
  if (entityList.length) {
    const haystack = [
      ...(metadata.entity_ids || []),
      ...(metadata.entity_labels || []),
      ...(metadata.person_ids || []),
      ...(metadata.person_labels || []),
      row.title,
      row.summary
    ].filter(Boolean).map((item) => String(item).toLowerCase());
    if (!entityList.some((needle) => haystack.some((value) => value.includes(String(needle || '').toLowerCase())))) return false;
  }

  const entityTags = normalizeFilterList(filters.entity_tags);
  if (entityTags.length) {
    const actual = normalizeFilterList([
      ...(metadata.entity_tags || []),
      ...(metadata.entity_labels || []),
      ...(metadata.topic_labels || []),
      ...(metadata.person_labels || [])
    ]);
    if (actual.length && !entityTags.some((needle) => actual.some((value) => value.includes(needle)))) return false;
  }

  const dataSources = normalizeFilterList(filters.data_source || filters.data_sources);
  if (dataSources.length) {
    const actual = `${metadata.data_source || ''} ${metadata.storage_data_source || ''} ${row.source_type || ''} ${row.data_source || ''}`.toLowerCase();
    if (actual.trim() && !dataSources.some((needle) => actual.includes(needle))) return false;
  }

  const contentTypes = normalizeFilterList(filters.content_type || filters.content_types);
  if (contentTypes.length) {
    if (!contentTypeFilterPasses(row, filters, metadata)) return false;
  }

  if (filters.status) {
    const statuses = Array.isArray(filters.status) ? filters.status : [filters.status];
    const status = String(row.status || metadata.status || 'active').toLowerCase();
    if (!statuses.some((item) => status === String(item || '').toLowerCase())) return false;
  }

  if (filters.sentiment_score && typeof filters.sentiment_score === 'object') {
    const score = Number(row.sentiment_score ?? metadata.sentiment_score ?? 0);
    const min = filters.sentiment_score.min ?? -1;
    const max = filters.sentiment_score.max ?? 1;
    if (score < min || score > max) return false;
  }

  const dateRange = normalizeDateRange(filters.date_range);
  if (dateRange) {
    const ts = sortKeyForRow(row);
    if (!ts || ts < dateRange.start.getTime() || ts > dateRange.end.getTime()) return false;
  }
  return true;
}

function applyMetadataPreFilter(rows, metadataFilters = {}) {
  /**
   * Hard metadata filtering - reduces candidate set from millions to hundreds
   * before expensive vector  operations
   * Returns filtered rows with metadata predicates applied
   */
  if (!rows || !rows.length) return rows;

  return rows.filter(row => {
    // Core/Insight nodes always pass through
    if (row.id === 'global_core' || row.layer === 'core' || row.layer === 'insight') {
      return true;
    }
    const metadata = typeof row.metadata === 'string' ? (() => {
      try { return JSON.parse(row.metadata); } catch (_) { return {}; }
    })() : (row.metadata || {});

    // Filter by sentiment score if specified
    if (metadataFilters.sentiment_score !== undefined) {
      const requiredSentiment = metadataFilters.sentiment_score;
      const rowSentiment = Number(row.sentiment_score ?? metadata.sentiment_score ?? 0);

      if (typeof requiredSentiment === 'object' && requiredSentiment !== null) {
        // Range filter: { min: -1, max: 0 }
        const min = requiredSentiment.min !== undefined ? requiredSentiment.min : -1.0;
        const max = requiredSentiment.max !== undefined ? requiredSentiment.max : 1.0;
        if (rowSentiment < min || rowSentiment > max) {
          return false;
        }
      } else if (typeof requiredSentiment === 'number') {
        // Exact match or threshold
        if (Math.abs(rowSentiment - requiredSentiment) > 0.1) return false;
      }
    }

    // Filter by status if specified
    if (metadataFilters.status !== undefined) {
      const requiredStatuses = Array.isArray(metadataFilters.status)
        ? metadataFilters.status
        : [metadataFilters.status];
      const rowStatus = row.status || metadata.status || 'active';
      if (!requiredStatuses.includes(rowStatus)) {
        return false;
      }
    }

    // Filter by importance (node metadata field)
    if (metadataFilters.importance !== undefined) {
      const reqImportance = metadataFilters.importance;
      const rowImportance = Number(row.importance ?? metadata.importance ?? 5);

      if (typeof reqImportance === 'object' && reqImportance !== null) {
        const min = reqImportance.min !== undefined ? reqImportance.min : 1;
        const max = reqImportance.max !== undefined ? reqImportance.max : 10;
        if (rowImportance < min || rowImportance > max) return false;
      } else if (typeof reqImportance === 'number') {
        if (rowImportance < reqImportance) return false;
      }
    }

    // Filter by activity type if specified
    if (metadataFilters.activity_type !== undefined) {
      const reqActivityTypes = Array.isArray(metadataFilters.activity_type)
        ? metadataFilters.activity_type
        : [metadataFilters.activity_type];
      const rowActivityType = metadata.activity_type;
      if (rowActivityType && !reqActivityTypes.includes(rowActivityType)) {
        return false;
      }
    }

    // Filter by content type if specified
    if (metadataFilters.content_type !== undefined) {
      const reqContentTypes = normalizeFilterList(metadataFilters.content_type);
      if (reqContentTypes.length && !contentTypeFilterPasses(row, metadataFilters, metadata)) return false;
    }

    // Filter by session_id if specified
    if (metadataFilters.session_id !== undefined) {
      const reqSessionId = metadataFilters.session_id;
      const rowSessionId = row.session_id || metadata.session_id;
      if (reqSessionId && rowSessionId !== reqSessionId) {
        return false;
      }
    }

    // Filter by connection  count (node density)
    if (metadataFilters.connection_count !== undefined) {
      const reqConnCount = metadataFilters.connection_count;
      const rowConnCount = Number(row.connection_count ?? metadata.connection_count ?? 0);

      if (typeof reqConnCount === 'object' && reqConnCount !== null) {
        const min = reqConnCount.min !== undefined ? reqConnCount.min : 0;
        const max = reqConnCount.max !== undefined ? reqConnCount.max : 1000;
        if (rowConnCount < min || rowConnCount > max) return false;
      } else if (typeof reqConnCount === 'number') {
        if (rowConnCount < reqConnCount) return false;
      }
    }

    if (metadataFilters.person_ids !== undefined || metadataFilters.entity_ids !== undefined || metadataFilters.people !== undefined || metadataFilters.entity_tags !== undefined) {
      const required = normalizeFilterList(metadataFilters.person_ids || metadataFilters.entity_ids || metadataFilters.people || metadataFilters.entity_tags);
      const actual = normalizeFilterList([
        ...(metadata.person_ids || []),
        ...(metadata.entity_ids || []),
        ...(metadata.person_labels || []),
        ...(metadata.entity_labels || []),
        ...(metadata.entity_tags || []),
        ...(metadata.topic_labels || [])
      ]);
      if (required.length && actual.length && !required.some((needle) => actual.some((value) => value.includes(needle)))) return false;
    }

    if (metadataFilters.app_id !== undefined || metadataFilters.app_ids !== undefined) {
      const required = expandAppIdFilterValues(metadataFilters.app_id || metadataFilters.app_ids);
      if (required.length && !appIdentityMatches(required, row, metadata)) return false;
    }

    if (metadataFilters.data_source !== undefined || metadataFilters.data_sources !== undefined) {
      const required = normalizeFilterList(metadataFilters.data_source || metadataFilters.data_sources);
      const actual = `${metadata.data_source || ''} ${metadata.storage_data_source || ''} ${row.source_type || ''} ${row.data_source || ''}`.toLowerCase();
      if (required.length && actual.trim() && !required.some((needle) => actual.includes(needle))) return false;
    }

    if (metadataFilters.relationship_tier !== undefined) {
      const tiers = normalizeFilterList(metadataFilters.relationship_tier);
      const tier = String(metadata.relationship_tier || '').toLowerCase();
      if (tiers.length && !tiers.includes(tier)) return false;
    }

    return true;
  });
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

function queryOverlapScore(row, query = '') {
  const tokens = tokenizeLexicalQuery(query)
    .filter((token) => !['what', 'when', 'where', 'which', 'show', 'find', 'recent', 'latest', 'status'].includes(token))
    .slice(0, 12);
  if (!tokens.length) return 0;
  const hay = `${row.title || ''} ${row.text || ''} ${row.summary || ''}`.toLowerCase();
  let hits = 0;
  for (const token of tokens) {
    if (hay.includes(token)) hits += 1;
  }
  return Math.min(0.22, (hits / tokens.length) * 0.22);
}

function metadataUsefulnessScore(row, filters = {}) {
  const metadata = asObj(row.metadata);
  let score = 0;
  const appValues = expandAppNameFilterValues(filters.app);
  if (appValues.length) {
    const appHay = `${row.app || ''} ${metadata.source_app || ''} ${metadata.app || ''} ${metadata.app_id || ''}`.toLowerCase();
    if (appValues.some((item) => appHay.includes(item))) score += 0.12;
  }
  const sourceTypes = expandSourceTypeFilters(filters.source_types);
  if (sourceTypes.length) {
    const sourceHay = `${row.source_type || ''} ${row.layer || ''} ${row.subtype || ''} ${row.source_type_group || ''} ${metadata.source_type || ''} ${metadata.content_type || ''} ${metadata.data_source || ''}`.toLowerCase();
    if (sourceTypes.some((item) => sourceHay.includes(item))) score += 0.12;
  }
  const appIds = expandAppIdFilterValues(filters.app_id || filters.app_ids);
  if (appIds.length && appIdentityMatches(appIds, row, metadata)) score += 0.14;
  const entities = normalizeFilterList(filters.entity_tags || filters.entity_ids || filters.person_ids || filters.people);
  if (entities.length) {
    const entityHay = normalizeFilterList([
      ...(metadata.entity_tags || []),
      ...(metadata.entity_labels || []),
      ...(metadata.person_labels || []),
      ...(metadata.person_ids || []),
      row.title || ''
    ]).join(' ');
    if (entities.some((item) => entityHay.includes(item))) score += 0.14;
  }
  const contentTypes = normalizeFilterList(filters.content_type || filters.content_types);
  if (contentTypes.length && contentTypes.some((item) => `${metadata.content_type || ''} ${row.subtype || ''}`.toLowerCase().includes(item))) score += 0.08;
  const dataSources = normalizeFilterList(filters.data_source || filters.data_sources);
  if (dataSources.length && dataSources.some((item) => `${metadata.data_source || ''} ${metadata.storage_data_source || ''} ${row.data_source || ''}`.toLowerCase().includes(item))) score += 0.08;
  return Math.min(0.5, score);
}

function sourceEvidenceUsefulness(row, retrievalPlan = {}) {
  const layer = String(row.layer || row.source_type || '').toLowerCase();
  const reason = String(row.match_reason || '').toLowerCase();
  let score = 0;
  if (reason.startsWith('semantic:')) score += 0.12;
  if (reason.startsWith('lexical:')) score += 0.08;
  if (reason.includes('chunk')) score += 0.1;
  if (reason.includes('recency')) score += 0.08;
  if (layer === 'raw' || layer === 'event') score += retrievalPlan.summary_vs_raw === 'raw' ? 0.16 : 0.08;
  if (layer === 'episode') score += 0.08;
  if (layer === 'semantic') score += 0.06;
  return score;
}

function rankUsefulNodes(rows = [], query = '', retrievalPlan = {}) {
  const filters = {
    ...(retrievalPlan.filters || {}),
    ...(retrievalPlan.metadata_filters || {})
  };
  return (rows || [])
    .map((row) => {
      const base = Number(row.rerank_score || row.fused_score || row.base_score || 0);
      const usefulScore = Number((
        base
        + queryOverlapScore(row, query)
        + metadataUsefulnessScore(row, filters)
        + sourceEvidenceUsefulness(row, retrievalPlan)
        + dateFreshnessBonus(row, retrievalPlan.applied_date_range)
      ).toFixed(6));
      return {
        ...row,
        useful_score: usefulScore,
        usefulness_reasons: [
          row.match_reason || null,
          metadataUsefulnessScore(row, filters) > 0 ? 'metadata_match' : null,
          queryOverlapScore(row, query) > 0 ? 'query_overlap' : null,
          dateFreshnessBonus(row, retrievalPlan.applied_date_range) > 0 ? 'date_match' : null
        ].filter(Boolean)
      };
    })
    .sort((a, b) => {
      if ((b.useful_score || 0) !== (a.useful_score || 0)) return (b.useful_score || 0) - (a.useful_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    });
}

function rerankFusedResults(rows, retrievalPlan) {
  const preferred = Array.isArray(retrievalPlan?.preferred_source_types) ? retrievalPlan.preferred_source_types : [];
  const summaryVsRaw = retrievalPlan?.summary_vs_raw || 'summary';
  const entryMode = String(retrievalPlan?.entry_mode || 'hybrid');
  const userSentiment = retrievalPlan?.user_sentiment_context; // Optional: user mood/context

  return (rows || [])
    .map((row) => {
      // Increased weight/bonus for nodes that were directly matched via semantic search queries.
      const semanticBonus = String(row.match_reason || '').startsWith('semantic:') ? 0.22 : 0;
      const coreWalkBonus = String(row.match_reason || '').startsWith('core_walk') ? 0.16 : 0;
      const episodeBonus = row.layer === 'episode' ? (summaryVsRaw === 'summary' ? 0.09 : 0.03) : 0;
      const rawEvidenceBonus = summaryVsRaw === 'raw' && (row.source_type === 'event' || row.layer === 'event') ? 0.08 : 0;

      // Metadata-aware bonuses
      // Sentiment alignment: if user context is positive, prefer positive sentiment rows
      let sentimentBonus = 0;
      if (typeof row.sentiment_score === 'number' && userSentiment) {
        const userSentimentValue = typeof userSentiment === 'number' ? userSentiment : 0;
        if (userSentimentValue > 0 && row.sentiment_score > 0.3) sentimentBonus = 0.06;
        if (userSentimentValue < 0 && row.sentiment_score < -0.3) sentimentBonus = 0.06;
        if (Math.abs(userSentimentValue) < 0.3 && Math.abs(row.sentiment_score) < 0.5) sentimentBonus = 0.04;
      }

      // Status-aware bonus: active/recent status > archived
      let statusBonus = 0;
      const rowStatus = row.status || 'active';
      if (rowStatus === 'active') statusBonus += 0.04;
      else if (rowStatus === 'completed') statusBonus += 0.02;

      // Importance-aware bonus: high-importance nodes get a lift
      let importanceBonus = 0;
      if (typeof row.importance === 'number' && row.importance >= 8) {
        importanceBonus = 0.05;
      } else if (typeof row.importance === 'number' && row.importance >= 6) {
        importanceBonus = 0.02;
      }

      // ScreenCapture prioritization should prefer OCR evidence without
      // rejecting Chrome screenshot captures as browser history.
      const metadata = asObj(row.metadata);
      const isScreenCapture = isRawScreenshotEvidence(row, metadata);
      const isBrowserHistory = isTrueBrowserHistoryEvidence(row, metadata);

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
        ? (semanticBonus * 1.1 + (coreWalkBonus * 0.25))
        : ((coreWalkBonus * 0.7) + (semanticBonus * 0.9)));

    const rerankScore = Number(((row.fused_score || row.base_score || 0) + semanticBonus + coreWalkBonus + entryModeBonus + episodeBonus + rawEvidenceBonus + sourceBonus + dateBonus + passiveBoost + sentimentBonus + statusBonus + importanceBonus).toFixed(6));
    return { ...row, rerank_score: rerankScore };
  })
    .sort((a, b) => {
      if ((b.rerank_score || 0) !== (a.rerank_score || 0)) return (b.rerank_score || 0) - (a.rerank_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    });
}

async function coreDownRanking(nodeRows = [], retrievalPlan = {}, limit = 80, semanticSeeds = []) {
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

  // Add semantic search hits to the frontier for targeted traversal
  let semanticHitIds = (semanticSeeds || [])
    .map(s => s.node_id)
    .filter(id => id && mapById.has(id));

  let frontier = Array.from(new Set([...coreFrontier, ...seedFrontier, ...semanticHitIds]));
  if (!frontier.length) return [];

  const visited = new Set(frontier);
  const scoreById = new Map();

  // Initialize scores
  for (const id of frontier) {
    const row = mapById.get(id);
    let base = 0.8;
    if (id === 'global_core') base = 1.6;
    else if (row?.layer === 'core') base = 1.3;
    else if (row?.layer === 'insight') base = 1.1;

    // Give extra weight to semantic hit seeds
    if (semanticHitIds.includes(id)) base += 0.4;

    scoreById.set(id, base);
  }

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
      const similarity = cosineSimilarity(getEmbedding(current), getEmbedding(neighbor));
      const simMultiplier = similarity > 0 ? (1.0 + (similarity * 0.4)) : 1.0;

      const base = (Math.max(0.2, 0.95 - (depth * 0.18)) + (Number(edge.weight || 0) * 0.06) + (Number(edge.evidence_count || 0) * 0.02)) * edgeWeightMultiplier * simMultiplier;
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
        metadata: row.metadata || {},
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
    .slice(0, 6);

  if (!frontier.length) return [];

  const visited = new Set(frontier);
  const results = [];

  for (let depth = 1; depth <= 5 && frontier.length && results.length < limit; depth++) {
    const placeholders = frontier.map(() => '?').join(',');
    const edges = await db.allQuery(
      `SELECT from_node_id, to_node_id, edge_type, weight FROM memory_edges
       WHERE from_node_id IN (${placeholders}) OR to_node_id IN (${placeholders})
       ORDER BY weight DESC LIMIT 80`,
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
  let sql = `SELECT id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date, importance, connection_count, last_reheated
     FROM memory_nodes
     WHERE status != 'archived'`;
  const params = [];

  // Apply SQL-level date pre-filter using the indexed anchor_date column.
  // Nodes without anchor_date (older rows) pass through and are filtered in JS below.
  if (dateRange) {
    const startDate = dateRange.start.toISOString().slice(0, 10);
    const endDate = dateRange.end.toISOString().slice(0, 10);
    sql += ` AND (layer IN ('core', 'insight') OR anchor_date IS NULL OR (anchor_date >= ? AND anchor_date <= ?))`;
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
        app: metadata.apps?.[0] || metadata.source_app || metadata.app || null,
        source_type_group: metadata.source_type_group || null,
        anchor_at: metadata.anchor_at || metadata.start || null,
        latest_activity_at: metadata.latest_activity_at || metadata.end || metadata.latest_interaction_at || row.updated_at || row.created_at || null,
        timestamp: metadata.anchor_at || metadata.start || metadata.end || metadata.latest_interaction_at || row.updated_at || row.created_at || null,
        sentiment_score: metadata.sentiment_score,
        session_id: metadata.session_id,
        importance: row.importance ?? metadata.importance,
        connection_count: row.connection_count ?? metadata.connection_count,
        last_reheated: row.last_reheated || metadata.last_reheated
      };
    })
    .filter((row) => rowMatchesFilters(row, filters));
}

async function vectorSearchNodes(nodeRows, semanticQueries = [], perQueryLimit = 30) {
  const rankings = [];
  for (const query of semanticQueries || []) {
    const queryEmbedding = await generateEmbedding(query, process.env.OPENAI_API_KEY);
    const ranked = nodeRows
      .map((row) => {
const embedding = getEmbedding(row);
        return {
          key: `node:${row.id}`,
          source_type: 'node',
          node_id: row.id,
          event_id: null,
          layer: row.layer,
          subtype: row.subtype,
          title: row.title,
          anchor_at: row.anchor_at || row.timestamp,
          latest_activity_at: row.latest_activity_at || row.timestamp,
          timestamp: row.timestamp,
          app: row.app,
          source_type_group: row.source_type_group || row.metadata?.source_type_group || null,
          metadata: row.metadata || {},
          confidence: row.confidence,
          status: row.status,
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
      .slice(0, Math.max(6, perQueryLimit || 30));
    rankings.push(ranked);
  }
  return rankings;
}

async function loadTextChunkCandidates(filters = {}, limit = 1200, diagnostics = null) {
  const dateRange = normalizeDateRange(filters.date_range);
  const where = [];
  const params = [];
  if (dateRange) {
    where.push('timestamp >= ? AND timestamp <= ?');
    params.push(dateRange.start.toISOString(), dateRange.end.toISOString());
  }
  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    where.push(`(${appFilter.map(() => 'LOWER(COALESCE(app, \'\')) LIKE ?').join(' OR ')})`);
    params.push(...appFilter.map((item) => `%${item}%`));
  }
  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    where.push(`(${sourceTypeFilter.map(() => "(LOWER(COALESCE(data_source, '')) LIKE ? OR LOWER(COALESCE(metadata, '')) LIKE ?)").join(' OR ')})`);
    params.push(...sourceTypeFilter.flatMap((item) => [`%${item}%`, `%${item}%`]));
  }
  appendMetadataSqlFilters({
    where,
    params,
    metadataFilters: filters,
    metadataExpr: 'metadata',
    extraTextExprs: ['data_source', 'app', 'text'],
    hardContentTypeFilter: false
  });
  params.push(Math.max(100, Number(limit || 1200)));

  const rows = await db.allQuery(
    `SELECT id, event_id, node_id, chunk_index, text, embedding, timestamp, date, app, data_source, metadata
     FROM text_chunks
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY COALESCE(timestamp, date) DESC
     LIMIT ?`,
    params
  ).catch(() => []);

  if (diagnostics) {
    diagnostics.text_chunk_candidates_before_filter = rows.length;
    diagnostics.text_chunk_candidates_after_sql_filter = rows.length;
  }

  const mapped = rows
    .map((row) => {
      const metadata = asObj(row.metadata);
      return {
        key: `chunk:${row.id}`,
        source_type: metadata.event_type || metadata.source_type || row.data_source || 'chunk',
        node_id: row.node_id || null,
        event_id: row.event_id || null,
        layer: 'raw',
        subtype: metadata.content_type || metadata.event_type || row.data_source || 'chunk',
        title: metadata.context_title || metadata.window_title || metadata.title || `Memory chunk ${row.chunk_index ?? ''}`.trim(),
        anchor_at: metadata.occurred_at || row.timestamp,
        latest_activity_at: metadata.occurred_at || row.timestamp,
        timestamp: metadata.occurred_at || row.timestamp,
        app: row.app || metadata.source_app || metadata.app || null,
        source_type_group: metadata.source_type_group || metadata.data_source || row.data_source || null,
        metadata: {
          ...metadata,
          data_source: metadata.data_source || row.data_source,
          storage_data_source: metadata.storage_data_source || row.data_source
        },
        text: String(row.text || ''),
        embedding: row.embedding,
        source_refs: [row.event_id].filter(Boolean),
        match_reason: 'semantic:chunk'
      };
    });

  const filtered = [];
  for (const row of mapped) {
    const dropReason = classifyFilterDropReason(row, filters);
    if (dropReason) {
      if (diagnostics && dropReason === 'prioritize_screen_capture') {
        diagnostics.dropped_by_prioritize_screen_capture = (diagnostics.dropped_by_prioritize_screen_capture || 0) + 1;
      }
      if (diagnostics && dropReason === 'content_type') {
        diagnostics.dropped_by_content_type = (diagnostics.dropped_by_content_type || 0) + 1;
      }
      if (!rowMatchesFilters(row, filters)) continue;
    }
    if (rowMatchesFilters(row, filters)) filtered.push(row);
  }

  if (diagnostics) diagnostics.text_chunk_candidates_after_row_filter = filtered.length;
  return filtered;
}

async function vectorSearchTextChunks(filters = {}, semanticQueries = [], perQueryLimit = 12, diagnostics = null) {
  const chunkRows = await loadTextChunkCandidates(filters, 1200, diagnostics);
  if (!chunkRows.length) return [];
  const rankings = [];
  for (const query of semanticQueries || []) {
    const queryEmbedding = await generateEmbedding(query, process.env.OPENAI_API_KEY);
    const ranked = chunkRows
      .map((row) => {
        const embedding = getEmbedding(row);
        return {
          ...row,
          base_score: cosineSimilarity(queryEmbedding, embedding),
          match_reason: `semantic:chunk:${query}`
        };
      })
      .filter((row) => row.base_score > 0)
      .sort((a, b) => (b.base_score || 0) - (a.base_score || 0))
      .slice(0, Math.max(4, perQueryLimit || 12));
    rankings.push(ranked);
  }
  return rankings;
}

async function lexicalSearchRetrievalDocs(filters = {}, lexicalTerms = [], limit = 40) {
  const terms = Array.isArray(lexicalTerms) ? lexicalTerms : [];
  const tokens = terms.length ? terms : tokenizeLexicalQuery(terms.join(' '));
  if (!tokens.length) return [];
  const matchQuery = buildFtsMatchQuery(tokens);
  if (!matchQuery) return [];

  const dateRange = normalizeDateRange(filters.date_range);
  const sqlWhere = ['retrieval_docs_fts MATCH ?'];
  const params = [matchQuery];
  if (dateRange) {
    sqlWhere.push('d.timestamp >= ? AND d.timestamp <= ?');
    params.push(dateRange.start.toISOString(), dateRange.end.toISOString());
  }
  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    sqlWhere.push(`(${appFilter.map(() => 'LOWER(COALESCE(d.app, \'\')) LIKE ?').join(' OR ')})`);
    params.push(...appFilter.map((item) => `%${item}%`));
  }
  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    sqlWhere.push(`(${sourceTypeFilter.map(() => "(LOWER(COALESCE(d.source_type, '')) LIKE ? OR LOWER(COALESCE(d.metadata, '')) LIKE ?)").join(' OR ')})`);
    params.push(...sourceTypeFilter.flatMap((item) => [`%${item}%`, `%${item}%`]));
  }
  appendMetadataSqlFilters({
    where: sqlWhere,
    params,
    metadataFilters: filters,
    metadataExpr: 'd.metadata',
    extraTextExprs: ['d.source_type', 'd.app', 'd.text'],
    hardContentTypeFilter: false
  });
  params.push(Math.max(20, limit));

  const rows = await db.allQuery(
    `SELECT d.doc_id, d.source_type, d.node_id, d.event_id, d.app, d.timestamp, d.text, d.metadata,
            bm25(retrieval_docs_fts) AS fts_rank
     FROM retrieval_docs_fts
     JOIN retrieval_docs d ON d.doc_id = retrieval_docs_fts.doc_id
     WHERE ${sqlWhere.join(' AND ')}
     ORDER BY fts_rank ASC
     LIMIT ?`,
    params
  ).catch(() => []);

  return rows
    .map((row, index) => {
      const metadata = asObj(row.metadata);
      const rank = Number(row.fts_rank || 0);
      return {
        key: row.doc_id,
        source_type: row.source_type,
        node_id: row.node_id || null,
        event_id: row.event_id || null,
        layer: metadata.layer || metadata.type || row.source_type,
        subtype: metadata.subtype || null,
        source_type_group: metadata.source_type_group || metadata.envelope?.type_group || null,
        anchor_at: metadata.anchor_at || row.timestamp,
        latest_activity_at: metadata.latest_activity_at || row.timestamp,
        timestamp: row.timestamp,
        app: row.app,
        metadata,
        sentiment_score: metadata.sentiment_score,
        session_id: metadata.session_id,
        status: metadata.status,
        text: String(row.text || ''),
        source_refs: metadata.source_refs || [],
        base_score: Number((1 / (1 + Math.max(0, rank) + (index * 0.02))).toFixed(6)),
        match_reason: 'lexical:fts'
      };
    })
    .filter((row) => rowMatchesFilters(row, filters))
    .slice(0, limit);
}

async function lexicalSearchRawEvents(filters = {}, lexicalTerms = [], limit = 24) {
  const terms = Array.isArray(lexicalTerms) ? lexicalTerms : [];
  const tokens = terms.length ? terms : tokenizeLexicalQuery(terms.join(' '));
  if (!tokens.length) return [];

  const where = [];
  const params = [];
  const dateRange = normalizeDateRange(filters.date_range);
  if (dateRange) {
    where.push('(e.occurred_at >= ? AND e.occurred_at <= ?)');
    params.push(dateRange.start.toISOString(), dateRange.end.toISOString());
  }
  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    where.push(`(${appFilter.map(() => 'LOWER(COALESCE(e.app, \'\')) LIKE ?').join(' OR ')})`);
    params.push(...appFilter.map((item) => `%${item}%`));
  }
  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    where.push(`(${sourceTypeFilter.map(() => "(LOWER(COALESCE(e.source_type, '')) LIKE ? OR LOWER(COALESCE(e.metadata, '')) LIKE ?)").join(' OR ')})`);
    params.push(...sourceTypeFilter.flatMap((item) => [`%${item}%`, `%${item}%`]));
  }
  appendMetadataSqlFilters({
    where,
    params,
    metadataFilters: filters,
    metadataExpr: 'e.metadata',
    extraTextExprs: ['e.source_type', 'e.app', 'e.title', 'e.redacted_text', 'e.raw_text'],
    hardContentTypeFilter: false
  });
  const lexicalWhere = [];
  for (const token of tokens.slice(0, 8)) {
    lexicalWhere.push(`(e.redacted_text LIKE ? OR e.raw_text LIKE ? OR e.title LIKE ?)`);
    params.push(`%${token}%`, `%${token}%`, `%${token}%`);
  }
  if (!lexicalWhere.length) return [];
  where.push(`(${lexicalWhere.join(' OR ')})`);

  const rows = await db.allQuery(
    `SELECT e.id, e.source_type, e.occurred_at, e.title, e.redacted_text, e.raw_text, e.app, e.metadata
     FROM events e
     WHERE ${where.join(' AND ')}
     ORDER BY COALESCE(e.occurred_at, e.timestamp) DESC
     LIMIT ?`,
    [...params, Math.max(20, limit * 3)]
  ).catch(() => []);

  return rows
    .map((row, index) => {
      const metadata = asObj(row.metadata);
      return {
        key: `event:${row.id}`,
        source_type: 'event',
        node_id: null,
        event_id: row.id,
        layer: 'event',
        subtype: row.source_type || null,
        source_type_group: metadata.source_type_group || metadata.envelope?.type_group || null,
        anchor_at: row.occurred_at || null,
        latest_activity_at: row.occurred_at || null,
        timestamp: row.occurred_at || null,
        app: row.app || null,
        metadata,
        sentiment_score: metadata.sentiment_score,
        session_id: metadata.session_id,
        status: metadata.status,
        text: buildRawEvidenceText(row, metadata, { maxChars: 12000 }),
        source_refs: [row.id],
        base_score: Number((0.86 - (index * 0.015)).toFixed(6)),
        match_reason: 'lexical:event'
      };
    })
    .filter((row) => rowMatchesFilters(row, filters))
    .slice(0, limit);
}

async function querylessRecentDocs(filters = {}, limit = 24) {
  const dateRange = normalizeDateRange(filters.date_range);
  let sql = `SELECT doc_id, source_type, node_id, event_id, app, timestamp, text, metadata
     FROM retrieval_docs`;
  const where = [];
  const params = [];

  if (dateRange) {
    where.push('timestamp >= ? AND timestamp <= ?');
    params.push(dateRange.start.toISOString(), dateRange.end.toISOString());
  }
  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    where.push(`(${appFilter.map(() => 'LOWER(COALESCE(app, \'\')) LIKE ?').join(' OR ')})`);
    params.push(...appFilter.map((item) => `%${item}%`));
  }
  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    where.push(`(${sourceTypeFilter.map(() => "(LOWER(COALESCE(source_type, '')) LIKE ? OR LOWER(COALESCE(metadata, '')) LIKE ?)").join(' OR ')})`);
    params.push(...sourceTypeFilter.flatMap((item) => [`%${item}%`, `%${item}%`]));
  }
  appendMetadataSqlFilters({
    where,
    params,
    metadataFilters: filters,
    metadataExpr: 'metadata',
    extraTextExprs: ['source_type', 'app', 'text'],
    hardContentTypeFilter: false
  });

  if (where.length) {
    sql += ` WHERE ${where.join(' AND ')}`;
  }

  sql += ` ORDER BY timestamp DESC LIMIT ?`;
  params.push(Math.max(60, limit * 4));

  const rows = await db.allQuery(sql, params).catch(() => []);

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
        metadata,
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

async function querylessRecentEvents(filters = {}, limit = 24) {
  const dateRange = normalizeDateRange(filters.date_range);
  const where = [];
  const params = [];

  if (dateRange) {
    where.push('COALESCE(e.occurred_at, e.timestamp) >= ? AND COALESCE(e.occurred_at, e.timestamp) <= ?');
    params.push(dateRange.start.toISOString(), dateRange.end.toISOString());
  }
  const appFilter = expandAppNameFilterValues(filters.app);
  if (appFilter.length) {
    where.push(`(${appFilter.map(() => 'LOWER(COALESCE(e.app, \'\')) LIKE ?').join(' OR ')})`);
    params.push(...appFilter.map((item) => `%${item}%`));
  }
  const sourceTypeFilter = expandSourceTypeFilters(filters.source_types);
  if (sourceTypeFilter.length) {
    where.push(`(${sourceTypeFilter.map(() => "(LOWER(COALESCE(e.source_type, '')) LIKE ? OR LOWER(COALESCE(e.metadata, '')) LIKE ?)").join(' OR ')})`);
    params.push(...sourceTypeFilter.flatMap((item) => [`%${item}%`, `%${item}%`]));
  }
  appendMetadataSqlFilters({
    where,
    params,
    metadataFilters: filters,
    metadataExpr: 'e.metadata',
    extraTextExprs: ['e.source_type', 'e.app', 'e.title', 'e.redacted_text', 'e.raw_text'],
    hardContentTypeFilter: false
  });

  const rows = await db.allQuery(
    `SELECT e.id, e.source_type, e.occurred_at, e.timestamp, e.title, e.redacted_text, e.raw_text, e.text, e.app, e.source_account, e.metadata
     FROM events e
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY COALESCE(e.occurred_at, e.timestamp) DESC
     LIMIT ?`,
    [...params, Math.max(40, limit * 3)]
  ).catch(() => []);

  return rows
    .map((row, index) => {
      const metadata = asObj(row.metadata);
      return {
        key: `event:${row.id}`,
        source_type: 'event',
        node_id: null,
        event_id: row.id,
        layer: 'event',
        subtype: row.source_type || null,
        source_type_group: metadata.source_type_group || metadata.envelope?.type_group || null,
        anchor_at: row.occurred_at || row.timestamp || metadata.occurred_at || null,
        latest_activity_at: row.occurred_at || row.timestamp || metadata.occurred_at || null,
        timestamp: row.occurred_at || row.timestamp || metadata.occurred_at || null,
        app: row.app || metadata.source_app || metadata.app || null,
        source_account: row.source_account || null,
        metadata,
        sentiment_score: metadata.sentiment_score,
        session_id: metadata.session_id,
        status: metadata.status,
        title: row.title || metadata.context_title || row.source_type || row.id,
        text: buildRawEvidenceText(row, metadata, { maxChars: 12000 }),
        source_refs: [row.id],
        base_score: Number((0.9 - (index * 0.012)).toFixed(6)),
        match_reason: 'recency:event'
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
    const text = buildRawEvidenceText(row, metadata, { maxChars: 12000 });
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
      text: String(text).slice(0, 12000),
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

function findSemanticNeighbors(targetNode, pool, threshold = 0.85, limit = 5) {
  const targetEmbedding = getEmbedding(targetNode);
  if (!targetEmbedding || !targetEmbedding.length) return [];

  return pool
    .filter(node => node.id !== targetNode.id)
    .map(node => ({
      node,
      similarity: cosineSimilarity(targetEmbedding, getEmbedding(node))
    }))
    .filter(item => item.similarity > threshold)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, limit)
    .map(item => ({
      ...item.node,
      similarity: item.similarity,
      isSemanticJump: true
    }));
}

async function expandGraphHierarchical(seedNodes = [], pool = []) {
  const seedIds = seedNodes.map(s => s.node_id || s.id).filter(Boolean);
  const seen = new Set(seedIds);
  const expanded = [];
  const edgePaths = [];
  const poolMap = new Map(pool.map(n => [n.id, n]));

  for (const seed of seedNodes) {
    const seedId = seed.node_id || seed.id;
    if (!seedId) continue;

    let currentLayer = String(seed.layer || 'raw').toLowerCase();
    if (currentLayer === 'event') currentLayer = 'raw';
    const currentIdx = HIERARCHY_SEQUENCE.indexOf(currentLayer);

    // Valid target layers are same layer or exactly one layer below
    const validLayers = [currentLayer];
    if (currentIdx !== -1 && currentIdx < HIERARCHY_SEQUENCE.length - 1) {
      validLayers.push(HIERARCHY_SEQUENCE[currentIdx + 1]);
    }

    const edges = await db.allQuery(
      `SELECT id, from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count
       FROM memory_edges
       WHERE from_node_id = ? OR to_node_id = ?
       LIMIT 100`,
      [seedId, seedId]
    ).catch(() => []);

    for (const edge of edges) {
      const neighborId = edge.from_node_id === seedId ? edge.to_node_id : edge.from_node_id;
      if (!neighborId || seen.has(neighborId)) continue;

      let node = poolMap.get(neighborId);
      if (!node) {
        node = await db.getQuery(
          `SELECT id, layer, subtype, title, summary, canonical_text, metadata, source_refs, embedding, updated_at, created_at
           FROM memory_nodes
           WHERE id = ?`,
          [neighborId]
        ).catch(() => null);
        if (node) {
          node.metadata = asObj(node.metadata);
          node.source_refs = parseSourceRefs(node.source_refs);
        }
      }

      if (!node) continue;

      let neighborLayer = String(node.layer || 'raw').toLowerCase();
      if (neighborLayer === 'event') neighborLayer = 'raw';

      // Strict rule: same layer or exactly one layer below
      if (!validLayers.includes(neighborLayer)) continue;

      seen.add(neighborId);
      const metadata = node.metadata || {};
      const expandedNode = {
        id: node.id,
        layer: node.layer,
        type: node.layer,
        subtype: node.subtype || null,
        title: node.title || metadata.name || metadata.fact || node.id,
        summary: node.summary || metadata.summary || '',
        text: [node.title, node.summary, node.canonical_text].filter(Boolean).join('\n'),
        source_refs: parseSourceRefs(node.source_refs),
        timestamp: metadata.end || metadata.start || node.updated_at || node.created_at || null,
        depth: 1,
        match_reason: `hierarchical_expansion:${seedId}`
      };

      expanded.push(expandedNode);
      edgePaths.push({
        from: seedId,
        to: node.id,
        relation: edge.edge_type,
        trace_label: edge.trace_label || 'hierarchical_expansion',
        weight: Number(edge.weight || 1),
        depth: 1
      });
    }
  }

  return {
    expandedNodes: expanded,
    edgePaths
  };
}

async function expandGraph(seedNodes = [], hopLimit = DEFAULT_HOP_LIMIT, maxExpanded = MAX_EXPANDED, pool = []) {
  const seedIds = seedNodes.map(s => s.node_id || s.id).filter(Boolean);
  const seen = new Set(seedIds);
  const queue = seedNodes
    .filter(s => s.node_id || s.id)
    .map((s) => ({ id: s.node_id || s.id, layer: s.layer, depth: 0 }));
  const expanded = [];
  const supportNodes = [];
  const evidenceNodes = [];
  const edgePaths = [];

  const poolMap = new Map(pool.map(n => [n.id, n]));

  const effectiveHopLimit = Math.min(4, Math.max(1, hopLimit || DEFAULT_HOP_LIMIT));

  while (queue.length && expanded.length < maxExpanded) {
    const current = queue.shift();
    if (current.depth >= effectiveHopLimit) continue;

    // 1. Explicit Edge Expansion
    const edges = await db.allQuery(
      `SELECT id, from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata
       FROM memory_edges
       WHERE from_node_id = ? OR to_node_id = ?
       LIMIT 30`,
      [current.id, current.id]
    ).catch(() => []);

    const neighbors = [];
    for (const edge of edges) {
      const neighborId = edge.from_node_id === current.id ? edge.to_node_id : edge.from_node_id;
      if (!neighborId || seen.has(neighborId)) continue;

      let node = poolMap.get(neighborId);
      if (!node) {
        // Fallback for nodes not in candidate pool (e.g. if filters excluded them)
        // but we still allow core/insight expansion if they exist
        node = await db.getQuery(
          `SELECT id, layer, subtype, title, summary, metadata, source_refs, embedding, created_at, updated_at
           FROM memory_nodes
           WHERE id = ?`,
          [neighborId]
        ).catch(() => null);

        if (node) {
          node.metadata = asObj(node.metadata);
          node.source_refs = parseSourceRefs(node.source_refs);
        }
      }

      if (!node) continue;

      // Restrict traversal: only higher or equal rank to lower/equal rank
      if ((LAYER_RANKS[node.layer] || 0) > (LAYER_RANKS[current.layer] || 0)) continue;

      const metadata = node.metadata || {};
      neighbors.push({
        id: node.id,
        layer: node.layer,
        subtype: node.subtype || null,
        title: node.title || metadata.name || metadata.fact || node.id,
        summary: node.summary || metadata.summary || '',
        source_refs: parseSourceRefs(node.source_refs),
        embedding: node.embedding,
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

    // 2. Semantic Jump Expansion (Similarity-Based Traversal)
    const currentNodeData = poolMap.get(current.id);
    // Add jump if node is isolated or is a primary search hit (depth 0)
    const shouldJump = (neighbors.length < 2) || (current.depth === 0);
    if (currentNodeData && pool.length > 0 && shouldJump) {
      const jumps = findSemanticNeighbors(currentNodeData, pool, 0.88, 3);
      for (const jump of jumps) {
        if (seen.has(jump.id)) continue;

        // Restrict traversal: only higher or equal rank to lower/equal rank
        if ((LAYER_RANKS[jump.layer] || 0) > (LAYER_RANKS[current.layer] || 0)) continue;

        const metadata = asObj(jump.metadata);
        neighbors.push({
          id: jump.id,
          layer: jump.layer,
          subtype: jump.subtype || null,
          title: jump.title || metadata.name || metadata.fact || jump.id,
          summary: jump.summary || metadata.summary || '',
          source_refs: parseSourceRefs(jump.source_refs),
          embedding: jump.embedding,
          anchor_at: jump.anchor_at || jump.metadata?.anchor_at || null,
          latest_activity_at: jump.latest_activity_at || jump.metadata?.latest_activity_at || jump.updated_at || jump.created_at || null,
          timestamp: jump.timestamp || jump.metadata?.end || jump.updated_at || null,
          depth: current.depth + 1,
          sort_score: expansionScore(jump.layer, jump.subtype) * jump.similarity,
          edge: {
            from: current.id,
            to: jump.id,
            relation: 'SIMILAR_TO',
            trace_label: 'semantic_jump',
            weight: jump.similarity,
            evidence_count: 1,
            depth: current.depth + 1
          }
        });
      }
    }

    neighbors
      .sort((a, b) => b.sort_score - a.sort_score)
      .slice(0, 16)
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
  const semanticLists = Array.isArray(semanticRankings) ? semanticRankings : [];
  const lexicalLists = Array.isArray(lexicalRanking?.[0])
    ? lexicalRanking
    : (Array.isArray(lexicalRanking) ? [lexicalRanking] : []);
  const rankConstant = 60;

  const addRankedList = (ranking = [], channel = 'semantic') => {
    ranking.forEach((row, index) => {
      const key = row.key;
      if (!key) return;
      const metadata = asObj(row.metadata);
      const sourceBonus = metadata.source_app || metadata.app_id || row.app ? 0.006 : 0;
      const entityBonus = (metadata.entity_tags?.length || metadata.entity_labels?.length || metadata.person_labels?.length) ? 0.008 : 0;
      const priorityBonus = Math.min(0.01, Math.max(0, Number(row.importance ?? metadata.priority ?? metadata.importance ?? 0)) / 1000);
      const base = 1 / (rankConstant + index + 1);
      const weighted = base * (channel === 'lexical' ? alpha : (1 - alpha));
      const prev = scores.get(key) || { row, lexical: 0, semantic: 0, score: 0 };
      prev[channel] += weighted;
      prev.score += weighted + sourceBonus + entityBonus + priorityBonus;
      if ((row.base_score || 0) > (prev.row?.base_score || 0)) prev.row = row;
      scores.set(key, prev);
    });
  };

  for (const ranking of lexicalLists) {
    addRankedList(ranking, 'lexical');
  }

  for (const ranking of semanticLists) {
    addRankedList(ranking, 'semantic');
  }

  return Array.from(scores.values())
    .map((item) => ({
      ...item.row,
      lexical_rrf_score: Number((item.lexical || 0).toFixed(6)),
      semantic_rrf_score: Number((item.semantic || 0).toFixed(6)),
      fused_score: Number((item.score || 0).toFixed(6)),
      fusion_method: 'metadata_weighted_rrf'
    }))
    .sort((a, b) => b.fused_score - a.fused_score);
}

function heuristicJudgeGoldSet(rows = [], query = '', limit = 5) {
  const qTokens = new Set(tokenizeLexicalQuery(query));
  return (rows || [])
    .map((row) => {
      const text = `${row.title || ''} ${row.text || ''}`.toLowerCase();
      let overlap = 0;
      for (const token of qTokens) {
        if (text.includes(token)) overlap += 1;
      }
      const overlapScore = qTokens.size ? (overlap / qTokens.size) : 0;
      const freshness = (() => {
        const ts = parseTs(row.latest_activity_at || row.anchor_at || row.timestamp);
        if (!ts) return 0;
        const days = (Date.now() - ts) / (24 * 60 * 60 * 1000);
        if (days <= 3) return 0.08;
        if (days <= 14) return 0.04;
        return 0;
      })();
      const lexicalBonus = String(row.match_reason || '').startsWith('lexical:') ? 0.06 : 0;
      const judgeScore = Number(((row.rerank_score || row.fused_score || row.base_score || 0) + (overlapScore * 0.22) + freshness + lexicalBonus).toFixed(6));
      return { ...row, judge_score: judgeScore };
    })
    .sort((a, b) => {
      if ((b.judge_score || 0) !== (a.judge_score || 0)) return (b.judge_score || 0) - (a.judge_score || 0);
      return sortKeyForRow(b) - sortKeyForRow(a);
    })
    .slice(0, Math.max(1, limit));
}

async function judgeGoldSet(rows = [], query = '', limit = 5) {
  const boundedRows = (rows || []).slice(0, Math.max(1, rows.length));
  if (!boundedRows.length) return [];

  const docs = boundedRows.map((row) => {
    const timeHint = row.latest_activity_at || row.anchor_at || row.timestamp || '';
    const appHint = row.app || '';
    const layerHint = row.layer || row.source_type || 'memory';
    return `[layer:${layerHint}] [app:${appHint}] [time:${timeHint}] ${String(row.title || '')}\n${String(row.text || '').slice(0, 1200)}`.trim();
  });

  const providerRanks = await externalRerank({
    query,
    documents: docs,
    topN: Math.max(1, Math.min(limit, boundedRows.length))
  }).catch(() => null);

  if (Array.isArray(providerRanks) && providerRanks.length) {
    const selected = [];
    const seen = new Set();
    for (const item of providerRanks) {
      const idx = Number(item.index);
      if (!Number.isFinite(idx) || idx < 0 || idx >= boundedRows.length || seen.has(idx)) continue;
      seen.add(idx);
      selected.push({
        ...boundedRows[idx],
        judge_score: Number((item.score || 0).toFixed(6)),
        judge_provider: item.provider || 'external'
      });
      if (selected.length >= limit) break;
    }
    if (selected.length) return selected;
  }

  return heuristicJudgeGoldSet(boundedRows, query, limit).map((row) => ({ ...row, judge_provider: 'heuristic' }));
}

async function agenticReviewContext(query, nodes, apiKey) {
  if (!apiKey || !nodes.length) return { sufficient: true };

  const nodeContext = nodes.map((n, i) => `[${i}] ${n.title}: ${String(n.text || n.summary || '').slice(0, 300)}`).join('\n');
  const prompt = `
You are evaluating if the retrieved memory context is sufficient to answer the user's query.
User Query: "${query}"

Retrieved Context:
${nodeContext}

Is this context sufficient to provide a detailed and accurate answer?
Respond with strict JSON: {"sufficient": true/false, "missing_info": "description of what is missing", "suggested_node_indices": [index of nodes that might have relevant edges]}
`;

  const llm = intelligenceEngine().callLLM;
  if (typeof llm !== 'function') return { sufficient: true };
  const result = await llm(prompt, apiKey, 0.1, { task: 'review' });
  return result || { sufficient: true };
}

async function buildHybridGraphRetrieval({
  query,
  options = {},
  seedLimit = DEFAULT_SEED_LIMIT,
  hopLimit = DEFAULT_HOP_LIMIT,
  recursionDepth = 0,
  passiveOnly = false,
  onProgress = null
} = {}) {
  const emit = (step, status, overrides = {}) => {
    if (onProgress) {
      onProgress({ step, status, label: overrides.label || step.replace(/_/g, ' '), ...overrides });
    }
  };

  emit('query_generated', 'completed', { query });

  const oldestCapture = await db.getQuery(`SELECT occurred_at FROM events WHERE type = 'ScreenCapture' ORDER BY occurred_at ASC LIMIT 1`).catch(() => null);
  let prioritizeScreenCapture = false;
  if (oldestCapture && oldestCapture.occurred_at) {
    const oldestTs = new Date(oldestCapture.occurred_at).getTime();
    const thirtyMinsAgo = Date.now() - (30 * 60 * 1000);
    if (oldestTs < thirtyMinsAgo) {
      prioritizeScreenCapture = true;
    }
  }

  const economyMode = Boolean(options?.economy) || String(process.env.CREDIT_SAVER_MODE || '').toLowerCase() === 'true';
  const basePlan = options.retrieval_thought || await buildRetrievalThought({
    query,
    mode: options.mode || 'chat',
    candidate: options.candidate || null,
    dateRange: options.date_range || null,
    app: options.app || null,
    economy: economyMode
  });
  const retrievalPlan = {
    ...basePlan,
    filters: {
      ...(basePlan.filters || {}),
      app: options.app || basePlan.filters?.app || null,
      date_range: options.date_range || basePlan.filters?.date_range || null,
      source_types: options.source_types || basePlan.filters?.source_types || null,
      data_source: options.data_source || basePlan.filters?.data_source || basePlan.metadata_filters?.data_source || null,
      prioritize_screen_capture: prioritizeScreenCapture
    },
    seed_limit: economyMode ? Math.min(seedLimit || basePlan.seed_limit || DEFAULT_SEED_LIMIT, 15) : (seedLimit || basePlan.seed_limit || DEFAULT_SEED_LIMIT),
    hop_limit: economyMode ? Math.min(hopLimit || basePlan.hop_limit || DEFAULT_HOP_LIMIT, 5) : (hopLimit || basePlan.hop_limit || DEFAULT_HOP_LIMIT),
    context_budget_tokens: basePlan.context_budget_tokens || (options.mode === 'suggestion' ? (economyMode ? 900 : 1200) : (economyMode ? 1200 : 2000)),
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

  const inferredHardFilters = inferHardFiltersFromQuery(query, retrievalPlan.filters || {});
  retrievalPlan.filters = {
    ...(retrievalPlan.filters || {}),
    app: inferredHardFilters.app,
    source_types: inferredHardFilters.source_types,
    date_range: inferredHardFilters.date_range,
    hard_predicates: inferredHardFilters.hard_predicates
  };
  retrievalPlan.metadata_filters = {
    ...(retrievalPlan.metadata_filters || {}),
    ...(options.metadata_filters || {})
  };

  const lowerQuery = String(query || '').toLowerCase();
  const requestedSourceTypes = normalizeFilterList(retrievalPlan.filters?.source_types || retrievalPlan.source_scope || []);
  const wantsScreenEvidence = /\b(ocr|screenshot|screen|capture|visible text|window text|desktop)\b/.test(lowerQuery)
    || requestedSourceTypes.some((item) => /screen|capture|desktop|screenshot/.test(item));

  if (wantsScreenEvidence && retrievalPlan.summary_vs_raw === 'raw' && !retrievalPlan.metadata_filters?.data_source && !retrievalPlan.filters?.data_source) {
    retrievalPlan.metadata_filters = {
      ...(retrievalPlan.metadata_filters || {}),
      data_source: ['screenshot_ocr', 'raw_event']
    };
  }

  const isChatMode = String(options.mode || retrievalPlan.mode || 'chat') === 'chat';
  const strictFunnel = economyMode || isChatMode;
  const perQueryVectorLimit = strictFunnel ? 8 : 16;
  const primarySeedLimit = strictFunnel ? 6 : (economyMode ? 5 : 8);
  const coreDownLimit = strictFunnel ? 18 : (economyMode ? 24 : 36);
  const coreToRawLimit = strictFunnel ? 12 : (economyMode ? 20 : 32);
  const recencyLimit = strictFunnel ? 8 : 14;
  const recursiveAnchorLimit = strictFunnel ? 2 : 3;
  const retrievalDiagnostics = {
    text_chunk_candidates_before_filter: 0,
    text_chunk_candidates_after_sql_filter: 0,
    text_chunk_candidates_after_row_filter: 0,
    dropped_by_prioritize_screen_capture: 0,
    dropped_by_content_type: 0
  };
  const wantsRawEvidence = retrievalPlan.summary_vs_raw === 'raw'
    || normalizeFilterList(retrievalPlan.filters?.data_source || retrievalPlan.metadata_filters?.data_source).some((item) => /raw|ocr|event|email_api|calendar_api|browser_history/.test(item))
    || normalizeFilterList(retrievalPlan.filters?.source_types || retrievalPlan.source_scope).some((item) => /communication|screen|capture|desktop|calendar|event|message|email/.test(item));
  const rerankEvidenceCap = isChatMode ? (wantsRawEvidence ? 40 : 24) : (strictFunnel ? 30 : 50);
  const packedEvidenceCap = isChatMode ? (wantsRawEvidence ? 14 : 8) : (strictFunnel ? 90 : 150);
  const drilldownCap = isChatMode ? 48 : (strictFunnel ? 80 : 140);
  const graphExpansionCap = isChatMode ? 36 : (strictFunnel ? 50 : (economyMode ? 70 : MAX_EXPANDED));

  const nodeRows = await loadMemoryNodeCandidates(retrievalPlan.filters);
  emit('candidates_loaded', 'completed', { count: nodeRows.length });

  // Apply hard metadata filtering to reduce search space before expensive vector operations
  // This is the "Hard Gate" that filters from millions to hundreds of candidates
  const metadataFilters = {
    // Add metadata filters from filters and options
    ...(retrievalPlan.metadata_filters || {}),
    ...(options.metadata_filters || {})
  };
  const preFilteredNodeRows = applyMetadataPreFilter(nodeRows, metadataFilters);
  const preFilterReduction = nodeRows.length > 0 ? Number(((1 - (preFilteredNodeRows.length / nodeRows.length)) * 100).toFixed(1)) : 0;
  if (preFilterReduction > 0) {
    emit('metadata_prefilter', 'completed', {
      before: nodeRows.length,
      after: preFilteredNodeRows.length,
      reduction_pct: preFilterReduction,
      detail: `Hard metadata filtering reduced candidates by ${preFilterReduction}%`
    });
  }
  const hasOperationalMetadataFilters = Object.keys(metadataFilters).some((key) => metadataFilters[key] !== null && metadataFilters[key] !== undefined);
  const metadataPrefilterFellBack = nodeRows.length && !preFilteredNodeRows.length && hasOperationalMetadataFilters;
  if (metadataPrefilterFellBack) {
    emit('metadata_prefilter_fallback', 'completed', {
      before: nodeRows.length,
      after: nodeRows.length,
      detail: 'Hard metadata filters produced zero candidates; falling back to date/app/source filters to preserve recall.'
    });
  }

  let finalNodeRows = preFilteredNodeRows.length || !nodeRows.length ? preFilteredNodeRows : nodeRows;
  let finalFilters = metadataPrefilterFellBack
    ? retrievalPlan.filters
    : { ...retrievalPlan.filters, ...metadataFilters };

  if (passiveOnly) {
    const passiveLayers = ['core', 'insight', 'cloud'];
    finalNodeRows = nodeRows.filter(r => passiveLayers.includes(r.layer));
    finalFilters = { ...finalFilters, passive_only: true };
  }

  const lexicalRanking = [];
  const rawEventLexicalRanking = [];
  const lexicalTerms = retrievalPlan.lexical_terms || tokenizeLexicalQuery(query);
  if (lexicalTerms.length) {
    const lexDocs = await lexicalSearchRetrievalDocs(finalFilters, lexicalTerms, strictFunnel ? 40 : 60);
    lexicalRanking.push(...lexDocs);
    const lexEvents = await lexicalSearchRawEvents(finalFilters, lexicalTerms, strictFunnel ? 24 : 36);
    rawEventLexicalRanking.push(...lexEvents);
  }

  emit('search_stage', 'started', { label: 'Search', detail: 'Finding primary memory seeds...' });
  emit('vector_search_started', 'started', { query_count: (retrievalPlan.search_queries || []).length });
  const semanticRankings = retrievalPlan.mode === 'queryless'
    ? []
    : await vectorSearchNodes(finalNodeRows, retrievalPlan.semantic_queries || retrievalPlan.search_queries || [], perQueryVectorLimit);
  const chunkSemanticRankings = retrievalPlan.mode === 'queryless'
    ? []
    : await vectorSearchTextChunks(finalFilters, retrievalPlan.semantic_queries || retrievalPlan.search_queries || [], Math.max(4, Math.floor(perQueryVectorLimit / 2)), retrievalDiagnostics);

  // Consolidate and rank all search results picking the top 10
  const consolidatedSeeds = reciprocalRankFusion([...semanticRankings, ...chunkSemanticRankings]);
  const primarySeeds = rankUsefulNodes(consolidatedSeeds, query, retrievalPlan).slice(0, primarySeedLimit);

  // Lazy enrichment for top seeds that lack a bulleted summary
  const apiKey = process.env.DEEPSEEK_API_KEY || options.apiKey;
  if (apiKey) {
    for (const seed of primarySeeds) {
      if (seed.source_type === 'node' && seed.node_id) {
        const node = finalNodeRows.find(n => n.id === seed.node_id);
        if (node && (!node.summary || !node.summary.startsWith('• '))) {
          const generateNodeTLDR = intelligenceEngine().generateNodeTLDR;
          if (typeof generateNodeTLDR !== 'function') continue;
          const tldr = await generateNodeTLDR(node, apiKey);
          if (tldr) {
            node.summary = tldr;
            // Update in-memory seed text/summary so LLM benefits
            seed.summary = tldr;
            seed.text = `${node.title}\n${tldr}`;

            // Persist to DB and Retrieval Index
            await updateMemoryNode(node.id, { summary: tldr }).catch(() => null);
            await upsertRetrievalDoc({
              docId: `node:${node.id}`,
              sourceType: 'node',
              nodeId: node.id,
              timestamp: node.timestamp || new Date().toISOString(),
              text: `${node.title}\n${tldr}`,
              metadata: { layer: node.layer, title: node.title }
            }).catch(() => null);
          }
        }
      }
    }
  }

  emit('primary_search_results', 'completed', {
    count: primarySeeds.length,
    preview_items: primarySeeds.map(s => `${s.text?.split('\n')[0] || s.node_id} (Score: ${s.fused_score})`)
  });

  const hierarchicalExpansion = await expandGraphHierarchical(primarySeeds, finalNodeRows);
  emit('iterative_expansion', 'completed', {
    count: hierarchicalExpansion.expandedNodes.length,
    detail: `Expanded graph from top 10 seeds following hierarchical sequence.`
  });

  const semanticSeeds = [].concat(...semanticRankings, ...chunkSemanticRankings).filter(r => r.base_score > 0.7);
  emit('seeds_identified', 'completed', { count: semanticSeeds.length });

  emit('search_stage', 'completed', { label: 'Search', detail: `Found ${semanticSeeds.length} primary memory seeds.` });

  emit('expansion_stage', 'started', { label: 'Expand', detail: 'Traversing graph for supporting evidence...' });
  emit('traversal_started', 'started', { mode: retrievalPlan.entry_mode || 'hybrid' });
  const coreRanking = await coreDownRanking(finalNodeRows, retrievalPlan, coreDownLimit, semanticSeeds);
  const coreToRawRanking = (options.strategy === 'core_to_raw' || retrievalPlan.strategy === 'core_to_raw')
    ? await recursiveDownTraversal(finalNodeRows, retrievalPlan, coreToRawLimit)
    : [];
  const wantsRecentRanking = !passiveOnly && (
    retrievalPlan.mode === 'queryless'
    || Boolean(retrievalPlan.filters?.date_range)
    || /\b(recent|latest|today|yesterday|this week|last week|right now|currently)\b/i.test(query || '')
    || wantsRawEvidence
  );
  const recencyRanking = wantsRecentRanking
    ? [
        ...(await querylessRecentDocs(finalFilters, recencyLimit)),
        ...(await querylessRecentEvents(finalFilters, recencyLimit))
      ]
    : [];

  const alpha = options.alpha !== undefined ? options.alpha : (retrievalPlan.alpha !== undefined ? retrievalPlan.alpha : 0.7);
  let fused = alphaBlendedSearch(
    [...lexicalRanking, ...rawEventLexicalRanking],
    [...semanticRankings, ...chunkSemanticRankings, coreRanking, coreToRawRanking, recencyRanking],
    alpha
  );

  // Recursive Retrieval Pass
  if (recursionDepth > 0 && !passiveOnly) {
    emit('deep_search', 'started', { label: 'Deep search', detail: 'Expanding from anchor nodes...' });
    const anchorNodes = fused
      .filter(row => (row.layer === 'core' || row.layer === 'insight') && row.fused_score > 0.6)
      .slice(0, recursiveAnchorLimit);

    if (anchorNodes.length > 0) {
      const recursionQueries = anchorNodes.map(node => node.text.slice(0, 300));
      const recursionRankings = await vectorSearchNodes(finalNodeRows, recursionQueries, perQueryVectorLimit);
      if (recursionRankings.length > 0) {
        fused = alphaBlendedSearch(
          [...lexicalRanking, ...rawEventLexicalRanking],
          [...semanticRankings, ...chunkSemanticRankings, ...recursionRankings, coreRanking, recencyRanking],
          alpha
        );
      }
    }
    emit('deep_search', 'completed');
  }

  const reranked = rerankFusedResults(fused, retrievalPlan);
  const usefulRanked = rankUsefulNodes(reranked, query, retrievalPlan);
  emit('node_usefulness', 'completed', {
    count: usefulRanked.length,
    detail: 'Ranked vector and lexical candidates by metadata match, query overlap, recency, and source usefulness.',
    preview_items: usefulRanked.slice(0, 5).map((row) => `${row.layer || row.source_type}:${row.node_id || row.event_id || row.key} (${row.useful_score})`)
  });
  let seeds = usefulRanked.slice(0, retrievalPlan.seed_limit);

  emit('node_found', 'completed', { count: seeds.length, preview_items: seeds.slice(0, 3).map(s => s.text?.split('\n')[0] || s.node_id) });

  // Agentic Review Loop
  if (apiKey && seeds.length > 0 && !economyMode) {
    emit('agentic_review', 'started', { detail: 'Reviewing retrieved context for sufficiency...' });
    const review = await agenticReviewContext(query, seeds.slice(0, 5), apiKey);

    if (!review.sufficient) {
      emit('context_insufficient', 'completed', { detail: review.missing_info });

      const suggestedIndices = Array.isArray(review.suggested_node_indices) ? review.suggested_node_indices : [0];
      const targetNodes = suggestedIndices.map(idx => seeds[idx]).filter(Boolean);

      if (targetNodes.length > 0) {
        emit('agentic_traversal', 'started', { label: 'Guided expansion', detail: `Traversing edges based on LLM suggestion...` });
        const expandedResults = await expandGraph(targetNodes, 2, 50, finalNodeRows);

        // Add expanded nodes to seeds
        const newSeeds = expandedResults.expandedNodes.map(n => ({
          key: `node:${n.id}`,
          node_id: n.id,
          layer: n.layer,
          text: n.summary || n.title,
          base_score: 0.8,
          match_reason: 'agentic_traversal'
        }));

        seeds = [...seeds, ...newSeeds];
        emit('agentic_traversal', 'completed', { count: newSeeds.length });
      }
    } else {
      emit('agentic_review', 'completed', { detail: 'Context is sufficient.' });
    }
  }

  // canonical list of seed node ids/keys used for tracing and logging
  const seedNodeIds = Array.from(new Set(seeds.map((s) => (s.node_id || s.event_id || s.key)).filter(Boolean))).slice(0, retrievalPlan.seed_limit);

  // Ensure recursive expansion from Core nodes even if not in primary seeds
  const coreNodesForExpansion = finalNodeRows
    .filter(r => r.layer === 'core')
    .slice(0, 6)
    .map(r => ({ ...r, node_id: r.id }));

  const graph = await expandGraph([...seeds, ...coreNodesForExpansion], retrievalPlan.hop_limit, graphExpansionCap, finalNodeRows);
  emit('expansion_stage', 'completed', { label: 'Expand', detail: `Expanded to ${graph.expandedNodes.length} connected nodes.` });
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
  })).filter(n => n.layer === 'episode' || n.layer === 'semantic' || n.layer === 'raw' || n.layer === 'event');
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
  const sourceRefEvidenceRows = await loadEventEvidenceRows(Array.from(episodeSourceRefMap.keys()), isChatMode ? 8 : 32);
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
  const judgeCandidateRows = usefulRanked.slice(0, Math.max(50, rerankEvidenceCap));
  const judgedGoldSet = await judgeGoldSet(judgeCandidateRows, query, isChatMode ? 5 : Math.min(12, packedEvidenceCap));
  let evidenceRows = isChatMode ? judgedGoldSet : usefulRanked.slice(0, rerankEvidenceCap);
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

    evidenceRows = ordered.slice(0, rerankEvidenceCap);
  }

  const prioritizedEvidenceRows = [];
  const prioritizedSeen = new Set();
  const pushEvidenceRow = (row, key) => {
    const identity = key || row.node_id || row.event_id || row.key || row.id;
    if (!identity || prioritizedSeen.has(identity)) return;
    prioritizedSeen.add(identity);
    prioritizedEvidenceRows.push(row);
  };

  // Prioritize primary seeds and their hierarchical expansion
  primarySeeds.forEach((row) => pushEvidenceRow(row));
  hierarchicalExpansion.expandedNodes.forEach((node) => pushEvidenceRow({
    key: `node:${node.id}`,
    node_id: node.id,
    layer: node.layer,
    subtype: node.subtype,
    text: node.text || [node.title, node.summary].filter(Boolean).join('\n'),
    timestamp: node.timestamp,
    base_score: 0.95,
    match_reason: node.match_reason || 'hierarchical_expansion'
  }));

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

  const evidence = prioritizedEvidenceRows.slice(0, packedEvidenceCap).map((row) => ({
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
    useful_score: Number((row.useful_score || row.rerank_score || row.fused_score || row.base_score || 0).toFixed(6)),
    usefulness_reasons: row.usefulness_reasons || [],
    reason: row.match_reason,
    source_refs: row.source_refs || [],
    text: String(row.text || '').slice(0, 8000)
    })).filter(n => n.layer === 'episode' || n.layer === 'semantic' || n.layer === 'raw' || n.layer === 'event');

  const traceSummary = [
    `Mode: ${retrievalPlan.mode}`,
    `Router: ${retrievalPlan.source_mode || retrievalPlan.strategy_mode || 'memory_only'}`,
    `Hard predicates: app=${JSON.stringify(retrievalPlan.filters?.app || null)}, source_types=${JSON.stringify(retrievalPlan.filters?.source_types || null)}, date_range=${JSON.stringify(retrievalPlan.filters?.date_range || null)}`,
    `Seeds: ${seedNodeIds.join(', ') || 'none'}`,
    ...(retrievalPlan.applied_date_range ? [`Applied date window: ${retrievalPlan.applied_date_range.start} -> ${retrievalPlan.applied_date_range.end}`] : []),
    `Stage 1: hybrid seed search returned ${seeds.length} primary seeds.`,
    `Stage 2: graph expansion added ${graph.expandedNodes.length} connected nodes (${supportNodes.length} support, ${evidenceNodes.length} evidence).`,
    `Stage 3: judge reranker selected ${judgedGoldSet.length} gold evidence rows from ${judgeCandidateRows.length} candidates (${judgedGoldSet[0]?.judge_provider || 'heuristic'}).`,
    `Diagnostics: text chunks sql=${retrievalDiagnostics.text_chunk_candidates_after_sql_filter}, row_filter=${retrievalDiagnostics.text_chunk_candidates_after_row_filter}, dropped_screen_priority=${retrievalDiagnostics.dropped_by_prioritize_screen_capture}, dropped_content_type=${retrievalDiagnostics.dropped_by_content_type}.`,
    ...(sourceRefEvidenceRows.length ? [`Stage 3: loaded ${sourceRefEvidenceRows.length} raw source events attached to matched episodes.`] : []),
    ...summarizeRetrievalThought(retrievalPlan)
  ];

  const budget = retrievalPlan.context_budget_tokens || 2000;
  const contextText = formatContext({
    budget,
    primarySeeds,
    hierarchicalExpandedNodes: hierarchicalExpansion.expandedNodes,
    seeds,
    expandedNodes: graph.expandedNodes,
    edgePaths: [...graph.edgePaths, ...sourceRefEdges]
  });

  const retrievalRunId = await logRetrievalRun({
    query,
    mode: retrievalPlan.mode,
    metadata: {
      plan: retrievalPlan,
      seeds: seedNodeIds,
      evidence_count: evidence.length,
      source_ref_events: sourceRefEvidenceRows.length,
      diagnostics: retrievalDiagnostics
    }
  });

  const allSourceRefIds = [
    ...evidence.flatMap((item) => [
      ...(item.source_refs || []),
      item.event_id || null
    ]),
    ...primaryNodes.flatMap((item) => parseSourceRefs(item.source_refs)),
    ...supportNodes.flatMap((item) => parseSourceRefs(item.source_refs)),
    ...evidenceNodes.flatMap((item) => parseSourceRefs(item.source_refs)),
    ...graph.expandedNodes.flatMap((item) => parseSourceRefs(item.source_refs)),
    ...hierarchicalExpansion.expandedNodes.flatMap((item) => parseSourceRefs(item.source_refs)),
    ...sourceRefEvidenceRows.map((row) => row.event_id || row.key)
  ].filter(Boolean);

  const drilldownRefs = Array.from(new Set(allSourceRefIds)).slice(0, drilldownCap);

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
    edge_paths: [...graph.edgePaths, ...sourceRefEdges],
    trace_labels: [...graph.edgePaths, ...sourceRefEdges].map((edge) => edge.trace_label).filter(Boolean),
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
      packed_evidence: evidence.length,
      ...retrievalDiagnostics
    },
    diagnostics: retrievalDiagnostics,
    evidence_count: evidence.length,
    evidence,
    contextText
  };
}

module.exports = {
  buildHybridGraphRetrieval,
  estimateTokensHeuristic,
  formatContext
};
