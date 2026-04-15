const db = require('../db');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');
const { buildRetrievalThought, widenTemporalWindow } = require('./retrieval-thought-system');

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function uniq(items = [], limit = 12) {
  return Array.from(new Set((items || []).filter(Boolean))).slice(0, limit);
}

function normalizeChatHistoryWindow(history = [], limit = 12) {
  if (!Array.isArray(history)) return [];
  return history
    .filter((item) => item && typeof item.content === 'string')
    .slice(-Math.max(1, limit))
    .map((item) => ({
      role: item.role === 'assistant' ? 'assistant' : 'user',
      content: String(item.content || '').trim().slice(0, 1200),
      ts: item.ts || null
    }))
    .filter((item) => item.content);
}

function buildQueryWithChatContext(query, chatHistory = []) {
  const userTurns = chatHistory.filter((item) => item.role === 'user').map((item) => item.content).slice(-4);
  if (!userTurns.length) return String(query || '').trim();
  return `${String(query || '').trim()}\n\nConversation context:\n${userTurns.map((item) => `- ${item}`).join('\n')}`;
}

function formatChatHistoryForPrompt(chatHistory = []) {
  if (!chatHistory.length) return 'None';
  return chatHistory
    .map((item) => `${item.role === 'assistant' ? 'Assistant' : 'User'}: ${item.content}`)
    .join('\n');
}

function decodeHtmlEntities(text) {
  return String(text || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function normalizeDDGResultUrl(url) {
  const raw = String(url || '');
  if (!raw) return '';
  try {
    const parsed = new URL(raw, 'https://duckduckgo.com');
    const uddg = parsed.searchParams.get('uddg');
    if (uddg) return decodeURIComponent(uddg);
    return parsed.href;
  } catch (_) {
    return raw;
  }
}

function shouldUseExternalWebSearch(query, retrievalThought, retrieval) {
  if (retrievalThought?.strategy_mode !== 'memory_then_web') return false;
  if (retrievalThought?.mode === 'queryless') return false;
  const evidenceCount = Number(retrieval?.evidence_count || 0);
  const seedCount = Number(retrieval?.seed_nodes?.length || 0);
  return seedCount < 2 || evidenceCount < 3;
}

async function searchFreeWeb(query, count = 5) {
  if (!query) return [];
  try {
    const size = Math.max(1, Math.min(8, count));
    const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
    const response = await fetch(ddgUrl);
    if (!response.ok) return [];
    const data = await response.json().catch(() => ({}));
    const rows = [];

    if (data?.AbstractURL) {
      rows.push({
        title: data?.Heading || 'DuckDuckGo',
        url: data.AbstractURL,
        snippet: data.AbstractText || ''
      });
    }

    const related = Array.isArray(data?.RelatedTopics) ? data.RelatedTopics : [];
    for (const topic of related) {
      if (rows.length >= size) break;
      if (topic?.FirstURL && topic?.Text) {
        rows.push({ title: topic.Text.slice(0, 100), url: topic.FirstURL, snippet: topic.Text });
      } else if (Array.isArray(topic?.Topics)) {
        for (const inner of topic.Topics) {
          if (rows.length >= size) break;
          if (inner?.FirstURL && inner?.Text) {
            rows.push({ title: inner.Text.slice(0, 100), url: inner.FirstURL, snippet: inner.Text });
          }
        }
      }
    }

    if (rows.length >= Math.min(2, size)) {
      return rows.slice(0, size).map((item) => ({ ...item, url: normalizeDDGResultUrl(item.url) }));
    }

    const htmlResp = await fetch(`https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`);
    if (!htmlResp.ok) return rows.slice(0, size);
    const html = await htmlResp.text();
    const matches = [...html.matchAll(/<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)];
    for (const match of matches) {
      if (rows.length >= size) break;
      const href = normalizeDDGResultUrl(decodeHtmlEntities(match[1] || ''));
      const title = decodeHtmlEntities((match[2] || '').replace(/<[^>]+>/g, ' ').trim());
      if (!href) continue;
      rows.push({
        title: title || 'DuckDuckGo result',
        url: href,
        snippet: title
      });
    }
    return rows.slice(0, size);
  } catch (_) {
    return [];
  }
}

function labelSourceType(sourceType) {
  const lower = String(sourceType || '').toLowerCase();
  if (lower.includes('email') || lower.includes('gmail') || lower.includes('communication')) return 'Email';
  if (lower.includes('calendar')) return 'Calendar';
  if (lower.includes('browser') || lower.includes('history') || lower.includes('visit')) return 'Browser History';
  if (lower.includes('message') || lower.includes('chat')) return 'Messages';
  if (lower.includes('desktop') || lower.includes('screen') || lower.includes('capture')) return 'Recent activity';
  if (lower.includes('event')) return 'Events';
  return null;
}

function summarizeRange(range) {
  if (!range?.start || !range?.end) return null;
  return `${range.start} -> ${range.end}`;
}

function strongestClusterPhrase(retrieval) {
  const bestSeed = Array.isArray(retrieval?.seed_nodes) ? retrieval.seed_nodes[0] : null;
  if (bestSeed?.title) return `Strongest cluster: ${bestSeed.title}`;
  const bestExpanded = Array.isArray(retrieval?.expanded_nodes) ? retrieval.expanded_nodes[0] : null;
  if (bestExpanded?.title) return `Strongest cluster: ${bestExpanded.title}`;
  const topEvidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence[0] : null;
  if (topEvidence?.text) return `Strongest cluster: ${String(topEvidence.text).slice(0, 100)}`;
  return 'No strong memory cluster was found.';
}

function buildConnectionCandidates(retrieval) {
  const nodes = [
    ...(retrieval?.expanded_nodes || []),
    ...(retrieval?.seed_nodes || [])
  ];
  const edgeLabels = new Set((retrieval?.edge_paths || []).map((edge) => String(edge?.trace_label || '').toLowerCase()).filter(Boolean));
  const candidates = [];
  const seen = new Set();

  for (const node of nodes) {
    if (!node) continue;
    const key = `${node.layer}:${node.subtype}:${node.id || node.title}`;
    if (seen.has(key)) continue;
    if (node.layer === 'semantic' && ['person', 'task', 'decision', 'fact'].includes(node.subtype)) {
      seen.add(key);
      candidates.push({
        label: node.title || node.id,
        reason: edgeLabels.size
          ? `${node.subtype} connected through an explicit path in the same storyline`
          : `${node.subtype} connected through the current episode graph`
      });
    } else if (node.layer === 'cloud' || node.layer === 'insight') {
      seen.add(key);
      candidates.push({
        label: node.title || node.id,
        reason: `${node.layer} pattern linked to the same supporting episodes`
      });
    }
    if (candidates.length >= 5) break;
  }

  return candidates;
}

function buildInterpretedMemorySummary(retrieval, drilldownEvidence = []) {
  const lines = [];
  const seeds = Array.isArray(retrieval?.seed_nodes) ? retrieval.seed_nodes.slice(0, 4) : [];
  const expanded = Array.isArray(retrieval?.expanded_nodes) ? retrieval.expanded_nodes.slice(0, 6) : [];

  for (const seed of seeds) {
    const reason = String(seed.reason || '').replace(/^lexical:/, 'keyword match: ').replace(/^semantic:/, 'semantic match: ');
    const snippet = String(seed.text || '').replace(/\s+/g, ' ').trim();
    const activity = seed.activity_summary ? ` — ${seed.activity_summary}` : (snippet ? ` — ${snippet.slice(0, 180)}` : '');
    lines.push(`- Seed: ${seed.title || seed.id}${reason ? ` (${reason})` : ''}${activity}`);
  }

  for (const node of expanded) {
    const summary = String(node.summary || '').replace(/\s+/g, ' ').trim();
    lines.push(`- Connected ${node.layer}${node.subtype ? `/${node.subtype}` : ''}: ${node.title || node.id}${summary ? ` — ${summary.slice(0, 160)}` : ''}`);
  }

  for (const item of drilldownEvidence.slice(0, 3)) {
    const text = String(item.text || '').replace(/\s+/g, ' ').trim();
    lines.push(`- Supporting detail: ${item.title || item.id}${text ? ` — ${text.slice(0, 220)}` : ''}`);
  }

  return lines.length ? lines.join('\n') : 'None';
}

function buildPriorityEvidenceLines(retrieval, drilldownEvidence = [], limit = 6) {
  const lines = [];
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  const ranked = [...evidence]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, Math.max(1, limit - 2));
  for (const item of ranked) {
    const layer = item.layer || item.type || 'memory';
    const text = String(item.text || item.title || '').replace(/\s+/g, ' ').trim().slice(0, 170);
    if (!text) continue;
    lines.push(`- [${layer}] ${text}`);
  }
  for (const row of (drilldownEvidence || []).slice(0, 2)) {
    const text = String(row.text || row.title || '').replace(/\s+/g, ' ').trim().slice(0, 170);
    if (!text) continue;
    lines.push(`- [raw:${row.source_type || 'event'}] ${text}`);
  }
  return lines.slice(0, limit);
}

async function fetchSourceLabels(refs = [], evidence = []) {
  const ids = uniq(refs, 16);
  const labels = [];

  if (ids.length) {
    const placeholders = ids.map(() => '?').join(',');
    const rows = await db.allQuery(
      `SELECT id, source_type
       FROM events
       WHERE id IN (${placeholders})`,
      ids
    ).catch(() => []);
    rows.forEach((row) => {
      const label = labelSourceType(row.source_type);
      if (label) labels.push(label);
    });
  }

  for (const item of evidence || []) {
    const label = labelSourceType(item.source_type || item.layer || item.type);
    if (label) labels.push(label);
  }

  return uniq(labels, 6);
}

function buildThinkingSummary(query, retrieval, drilldownEvidence = []) {
  if (!retrieval) {
    return 'Answered from the current conversation without memory retrieval.';
  }
  const intent = retrieval?.retrieval_plan?.intent || 'memory lookup';
  const strategyMode = retrieval?.retrieval_plan?.strategy_mode || 'memory_only';
  const summaryVsRaw = retrieval?.retrieval_plan?.summary_vs_raw || 'summary';
  const parts = [
    `Planned for ${intent} using ${retrieval?.retrieval_plan?.mode || 'semantic'} retrieval in ${summaryVsRaw} mode.`
  ];
  parts.push(`Execution strategy: ${strategyMode}.`);
  if (retrieval?.applied_date_range) {
    parts.push('Applied a date-scoped memory filter before searching.');
  }
  if (retrieval?.date_filter_status === 'widened' && retrieval?.widened_date_range) {
    parts.push('Widened the original time window once because the first pass was sparse.');
  }
  if (drilldownEvidence.length) {
    parts.push('Pulled raw evidence because the question asked for precise wording.');
  }
  if (!retrieval?.seed_nodes?.length) {
    parts.push('Memory support was weak, so the answer may rely on limited evidence.');
  }
  if (retrieval?.web_search_used) {
    parts.push('Added external web search because the question appeared to need outside or current information.');
  }
  return parts.join(' ');
}

function buildThinkingStrategy(retrieval, drilldownEvidence = []) {
  const plan = retrieval?.retrieval_plan || {};
  const strategyMode = plan.strategy_mode || 'memory_only';
  const entryMode = plan.entry_mode || 'hybrid';
  const summaryVsRaw = plan.summary_vs_raw || 'summary';
  const timeScope = plan.time_scope?.label || (retrieval?.applied_date_range ? 'filtered_range' : 'all_time');
  const appScope = Array.isArray(plan.app_scope) ? plan.app_scope : (Array.isArray(plan.filters?.app) ? plan.filters.app : []);
  const sourceScope = Array.isArray(plan.source_scope) ? plan.source_scope : (Array.isArray(plan.filters?.source_types) ? plan.filters.source_types : []);
  const answerBasis = retrieval?.web_search_used
    ? (drilldownEvidence.length ? 'memory_plus_raw_plus_web' : 'memory_plus_web')
    : (drilldownEvidence.length ? 'memory_plus_raw' : 'memory_only');
  return {
    strategy_mode: strategyMode,
    entry_mode: entryMode,
    summary_vs_raw: summaryVsRaw,
    time_scope: timeScope,
    app_scope: appScope,
    source_scope: sourceScope,
    web_gate_reason: plan.web_gate_reason || '',
    answer_basis: answerBasis,
    text: [
      `Strategy mode: ${strategyMode}`,
      `Entry mode: ${entryMode}`,
      `Summary mode: ${summaryVsRaw}`,
      `Time scope: ${timeScope}`,
      appScope.length ? `App scope: ${appScope.join(', ')}` : '',
      sourceScope.length ? `Source scope: ${sourceScope.join(', ')}` : '',
      plan.web_gate_reason ? `Web gate: ${plan.web_gate_reason}` : '',
      `Answer basis: ${answerBasis}`
    ].filter(Boolean)
  };
}

function buildThinkingFilters(retrieval) {
  if (!retrieval) return [];
  const filters = [];
  if (retrieval?.applied_date_range) {
    filters.push({
      label: 'Date window',
      value: summarizeRange(retrieval.applied_date_range)
    });
  }
  if (retrieval?.initial_date_range && retrieval?.date_filter_status === 'widened') {
    filters.push({
      label: 'Original request',
      value: summarizeRange(retrieval.initial_date_range)
    });
  }
  if (retrieval?.widened_date_range && retrieval?.date_filter_status === 'widened') {
    filters.push({
      label: 'Widened window',
      value: summarizeRange(retrieval.widened_date_range)
    });
  }
  const appFilter = retrieval?.retrieval_plan?.filters?.app;
  const appValues = Array.isArray(appFilter) ? appFilter : (appFilter ? [appFilter] : []);
  if (appValues.length) {
    filters.push({
      label: 'App filter',
      value: appValues.join(', ')
    });
  }
  const sourceTypeFilter = retrieval?.retrieval_plan?.filters?.source_types;
  const sourceValues = Array.isArray(sourceTypeFilter) ? sourceTypeFilter : (sourceTypeFilter ? [sourceTypeFilter] : []);
  if (sourceValues.length) {
    filters.push({
      label: 'Source scope',
      value: sourceValues.join(', ')
    });
  }
  if (retrieval?.date_filter_status && retrieval.date_filter_status !== 'not_used') {
    filters.push({
      label: 'Filter status',
      value: retrieval.date_filter_status
    });
  }
  return filters;
}

function buildThinkingSearchQueries(retrieval) {
  const lexicalStop = new Set([
    'what', 'whats', "what's", 'how', 'should', 'based', 'latest', 'current', 'recent', 'context',
    'show', 'tell', 'give', 'find', 'this', 'that', 'these', 'those', 'the', 'and'
  ]);
  const isUsefulLexical = (term) => {
    const t = String(term || '').trim().toLowerCase();
    if (!t || lexicalStop.has(t)) return false;
    if (t.length < 3) return false;
    if (t.length < 4 && !/[.@:/_-]/.test(t) && !/\d/.test(t)) return false;
    return true;
  };
  const contextQueries = Array.isArray(retrieval?.generated_queries?.semantic)
    ? retrieval.generated_queries.semantic
    : (Array.isArray(retrieval?.query_variants)
    ? retrieval.query_variants
    : (Array.isArray(retrieval?.retrieval_plan?.semantic_queries) ? retrieval.retrieval_plan.semantic_queries : []));
  const messageQueries = Array.isArray(retrieval?.generated_queries?.messages)
    ? retrieval.generated_queries.messages
    : (Array.isArray(retrieval?.message_query_variants)
    ? retrieval.message_query_variants
    : (Array.isArray(retrieval?.retrieval_plan?.message_queries) ? retrieval.retrieval_plan.message_queries : []));
  const lexicalTerms = Array.isArray(retrieval?.generated_queries?.lexical_terms)
    ? retrieval.generated_queries.lexical_terms
    : [];
  return {
    context: contextQueries.slice(0, 7),
    messages: messageQueries.slice(0, 7),
    lexical: lexicalTerms.filter((term) => isUsefulLexical(term)).slice(0, 10)
  };
}

function buildThinkingResultsSummary(retrieval, drilldownEvidence = []) {
  if (!retrieval) {
    return {
      headline: 'Used the current conversation only.',
      details: []
    };
  }
  const seedCount = Number(retrieval?.seed_nodes?.length || 0);
  const expandedCount = Number(retrieval?.expanded_nodes?.length || 0);
  const evidenceCount = Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0);
  const headline = seedCount || expandedCount || evidenceCount
    ? `Found ${seedCount} seed memories, expanded to ${expandedCount} connected nodes, and kept ${evidenceCount} evidence items.`
    : 'Found little or no matching memory in the requested scope.';
  const details = [
    strongestClusterPhrase(retrieval)
  ];
  if (Array.isArray(retrieval?.seed_results) && retrieval.seed_results.length) {
    details.push(`Initial seed search found ${retrieval.seed_results.length} ranked seed results before graph expansion.`);
  }
  if (Array.isArray(retrieval?.graph_expansion_results) && retrieval.graph_expansion_results.length) {
    details.push(`Graph expansion added ${retrieval.graph_expansion_results.length} connected nodes, capped at 10.`);
  }
  if (drilldownEvidence.length) {
    details.push(`Loaded ${drilldownEvidence.length} raw evidence item${drilldownEvidence.length === 1 ? '' : 's'} for exact wording.`);
  }
  if (retrieval?.web_search_used && Array.isArray(retrieval?.web_results) && retrieval.web_results.length) {
    details.push(`External web lookup added ${retrieval.web_results.length} public sources.`);
  }
  if (Array.isArray(retrieval?.trace_summary) && retrieval.trace_summary.length) {
    details.push(String(retrieval.trace_summary[retrieval.trace_summary.length - 1] || '').slice(0, 180));
  }
  return {
    headline,
    details: details.filter(Boolean).slice(0, 3)
  };
}

async function buildThinkingTrace({ query, retrieval, drilldownEvidence = [] }) {
  const dataSources = await fetchSourceLabels(
    [
      ...(retrieval?.drilldown_refs || []),
      ...(retrieval?.lazy_source_refs || []).map((item) => item?.ref)
    ],
    retrieval?.evidence || []
  );

  const strategy = buildThinkingStrategy(retrieval, drilldownEvidence);
  return {
    thinking_summary: buildThinkingSummary(query, retrieval, drilldownEvidence),
    strategy,
    filters: buildThinkingFilters(retrieval),
    search_queries: buildThinkingSearchQueries(retrieval),
    results_summary: buildThinkingResultsSummary(retrieval, drilldownEvidence),
    data_sources: dataSources,
    connection_candidates: buildConnectionCandidates(retrieval),
    seed_results: Array.isArray(retrieval?.seed_results) ? retrieval.seed_results : [],
    graph_expansion_results: Array.isArray(retrieval?.graph_expansion_results) ? retrieval.graph_expansion_results : [],
    web_search_used: Boolean(retrieval?.web_search_used),
    web_results_summary: Array.isArray(retrieval?.web_results) ? retrieval.web_results.map((item) => ({
      title: item.title,
      url: item.url
    })) : [],
    memory_sources: dataSources,
    web_sources: Array.isArray(retrieval?.web_sources) ? retrieval.web_sources : [],
    generated_queries: retrieval?.generated_queries || null,
    lexical_terms: Array.isArray(retrieval?.generated_queries?.lexical_terms) ? retrieval.generated_queries.lexical_terms : [],
    web_results: Array.isArray(retrieval?.web_results) ? retrieval.web_results : [],
    answer_basis: strategy.answer_basis,
    temporal_reasoning: Array.isArray(retrieval?.temporal_reasoning) ? retrieval.temporal_reasoning : [],
    initial_date_range: retrieval?.initial_date_range || null,
    applied_date_range: retrieval?.applied_date_range || null,
    widened_date_range: retrieval?.widened_date_range || null,
    date_filter_status: retrieval?.date_filter_status || 'not_used'
  };
}

async function fetchActiveSuggestions() {
  const rows = await db.allQuery(
    `SELECT id, title, body, trigger_summary, metadata, created_at
     FROM suggestion_artifacts
     WHERE status = 'active'
     ORDER BY created_at DESC
     LIMIT 5`
  ).catch(() => []);

  return rows.map((row) => ({
    id: row.id,
    title: row.title,
    body: row.body,
    trigger_summary: row.trigger_summary,
    metadata: safeJsonParse(row.metadata, {}),
    created_at: row.created_at
  }));
}

async function fetchRecentEpisodes() {
  const rows = await db.allQuery(
    `SELECT id, title, summary, metadata, updated_at
     FROM memory_nodes
     WHERE layer = 'episode'
     LIMIT 40`
  ).catch(() => []);
  return rows
    .map((row) => ({
    id: row.id,
    title: row.title,
    summary: row.summary,
    metadata: safeJsonParse(row.metadata, {}),
    updated_at: row.updated_at
  }))
    .sort((a, b) => {
      const aTs = Date.parse(a.metadata?.latest_activity_at || a.updated_at || 0) || 0;
      const bTs = Date.parse(b.metadata?.latest_activity_at || b.updated_at || 0) || 0;
      return bTs - aTs;
    })
    .slice(0, 6);
}

function needsRawDrilldown(query) {
  const lower = String(query || '').toLowerCase();
  return /\b(exact|verbatim|quote|quoted|precise|wording|what did .* say|show me the email|exact email|exact message)\b/.test(lower);
}

async function fetchDrilldownEvidence(refs = []) {
  const ids = Array.from(new Set((refs || []).filter(Boolean))).slice(0, 6);
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT id, source_type, occurred_at, title, redacted_text, raw_text, app, source_account
     FROM events
     WHERE id IN (${placeholders})
     ORDER BY COALESCE(occurred_at, timestamp) DESC`,
    ids
  ).catch(() => []);
  return rows.map((row) => ({
    id: row.id,
    source_type: row.source_type,
    occurred_at: row.occurred_at,
    title: row.title,
    app: row.app,
    source_account: row.source_account,
    text: String(row.redacted_text || row.raw_text || '').slice(0, 700)
  }));
}

function retrievalLooksSparse(retrieval) {
  const evidenceCount = Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0);
  const seedCount = Number(retrieval?.seed_nodes?.length || 0);
  return seedCount < 2 || evidenceCount < 3;
}

function withTemporalFallback(thought, widenedRange) {
  return {
    ...thought,
    filters: {
      ...(thought?.filters || {}),
      date_range: widenedRange
    },
    applied_date_range: widenedRange,
    widened_date_range: widenedRange,
    date_filter_status: 'widened',
    temporal_reasoning: [
      ...(thought?.temporal_reasoning || []),
      `Initial time window looked sparse, so retrieval widened once to ${widenedRange.start} -> ${widenedRange.end}.`
    ],
    fallback_policy: {
      ...(thought?.fallback_policy || { mode: 'widen_once' }),
      attempted: true,
      widened: true
    }
  };
}

async function executeParallelRetrieval(baseQuery, baseThought, options) {
  const bundle = [
    String(baseQuery || '').trim(),
    ...((baseThought.semantic_queries || []).map((item) => String(item || '').trim())),
    ...((baseThought.message_queries || []).map((item) => String(item || '').trim()))
  ].filter(Boolean);
  const queries = Array.from(new Set(bundle)).slice(0, 4);
  
  // Detect when a query requires deep context (e.g., long-term relationship, patterns)
  const requiresDeepContext = /\b(relationship|pattern|over the last|long-term|habit|habitual|recurring|years?|months?)\b/i.test(baseQuery);
  const recursionDepth = (requiresDeepContext && !options.passiveOnly) ? 1 : 0;

  // Parallel multi-agent dispatch
  const results = await Promise.all(queries.map((q) => buildHybridGraphRetrieval({
    query: q,
    options: {
      ...options,
      retrieval_thought: { ...baseThought, semantic_queries: [q] }
    },
    seedLimit: Math.max(3, Math.floor(10 / queries.length)),
    hopLimit: 2,
    recursionDepth,
    passiveOnly: options.passiveOnly || false
  })));

  if (results.length === 1) return results[0];

  const mergeById = (lists) => {
    const map = new Map();
    for (const list of lists) {
      for (const item of list || []) {
        const key = item.id || item.node_id || item.key;
        if (!key) continue;
        if (!map.has(key) || (item.score || 0) > (map.get(key).score || 0)) {
          map.set(key, item);
        }
      }
    }
    return Array.from(map.values()).sort((a, b) => (b.score || 0) - (a.score || 0));
  };
  
  const evidence = mergeById(results.map((r) => r.evidence)).slice(0, Math.max(...results.map((r) => r.evidence_count)));
  const expanded_nodes = mergeById(results.map((r) => r.expanded_nodes));
  const seed_nodes = mergeById(results.map((r) => r.seed_nodes));

  return {
    ...results[0],
    retrieval_run_id: results.map((r) => r.retrieval_run_id).join(','),
    thought_summary: Array.from(new Set(results.flatMap((r) => r.thought_summary))),
    trace_summary: Array.from(new Set(results.flatMap((r) => r.trace_summary))),
    evidence,
    evidence_count: evidence.length,
    expanded_nodes,
    seed_nodes,
    edge_paths: results.flatMap((r) => r.edge_paths),
    drilldown_refs: Array.from(new Set(results.flatMap((r) => r.drilldown_refs))),
    lazy_source_refs: Array.from(new Set(results.flatMap((r) => r.lazy_source_refs.map((x) => x.ref)))).map((ref) => ({ ref })),
    contextText: results.map((r) => r.contextText).join('\n\n---\n\n')
  };
}

async function answerChatQuery({ apiKey, query, options = {}, onStep }) {
  const emit = (data) => { try { onStep?.(data); } catch (_) {} };
  const chatHistory = normalizeChatHistoryWindow(options?.chat_history, 12);
  const retrievalQuery = buildQueryWithChatContext(query, chatHistory);

  const baseThought = buildRetrievalThought({
    query: retrievalQuery,
    mode: 'chat',
    dateRange: options?.date_range,
    app: options?.app
  });

  emit({
    step: 'query_analysis',
    intent: baseThought.mode || 'semantic',
    strategy_mode: baseThought.strategy_mode || 'memory_only',
    time_scope: baseThought.time_scope?.label || 'all_time',
    query_count: (baseThought.semantic_queries || []).length
  });

  // Passive-First Heuristic: attempt retrieval from Core, Insight, Cloud layers first
  let retrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
    mode: 'chat',
    app: options?.app,
    date_range: baseThought.applied_date_range || options?.date_range,
    source_types: options?.source_types,
    retrieval_thought: baseThought,
    passiveOnly: true
  });

  const passiveMaxScore = retrieval.evidence?.length ? Math.max(...retrieval.evidence.map((e) => e.score || 0)) : 0;
  const isPassiveSufficient = (retrieval.evidence_count >= 3 && passiveMaxScore > 0.75) || (passiveMaxScore > 0.9);

  if (!isPassiveSufficient) {
    emit({ step: 'passive_insufficient', passive_score: passiveMaxScore });
    retrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
      mode: 'chat',
      app: options?.app,
      date_range: baseThought.applied_date_range || options?.date_range,
      source_types: options?.source_types,
      retrieval_thought: baseThought,
      passiveOnly: false
    });
  } else {
    emit({ step: 'passive_sufficient', passive_score: passiveMaxScore });
  }

  emit({
    step: 'memory_retrieval',
    seed_count: retrieval?.seed_nodes?.length || 0,
    query_count: (baseThought.semantic_queries || baseThought.retrieval_plan?.semantic_queries || []).length
  });

  const canWiden = Boolean(baseThought?.initial_date_range) && !baseThought?.fallback_policy?.attempted;
  if (canWiden && retrievalLooksSparse(retrieval)) {
    const widenedRange = widenTemporalWindow(baseThought.initial_date_range);
    if (widenedRange) {
      emit({
        step: 'temporal_widen',
        from_range: baseThought.initial_date_range,
        to_range: widenedRange
      });
      const widenedThought = withTemporalFallback(baseThought, widenedRange);
      const widenedRetrieval = await executeParallelRetrieval(retrievalQuery, widenedThought, {
        mode: 'chat',
        app: options?.app,
        date_range: widenedRange,
        source_types: options?.source_types,
        retrieval_thought: widenedThought
      });
      retrieval = {
        ...widenedRetrieval,
        initial_date_range: baseThought.initial_date_range,
        widened_date_range: widenedRange,
        date_filter_status: 'widened',
        temporal_reasoning: widenedThought.temporal_reasoning
      };
    }
  }

  const maxScore = retrieval.evidence?.length ? Math.max(...retrieval.evidence.map((e) => e.score || 0)) : 0;
  if (retrieval.evidence_count === 0 || (retrieval.evidence_count < 3 && maxScore < 0.45)) {
    emit({
      step: 'confidence_gating',
      status: 'needs_clarification',
      reason: 'Memory retrieval returned insufficient high-confidence context.'
    });
    
    return {
       content: "I couldn't find enough specific details in your memory graph to answer that accurately. Could you provide a bit more context, like a specific timeframe or associated project?",
       needs_clarification: true,
       retrieval,
       thinking_trace: ["Confidence Gating: Memory retrieval returned insufficient high-confidence context. Prompting user for clarification."]
    };
  }

  emit({
    step: 'graph_expansion',
    expanded_count: retrieval?.expanded_nodes?.length || 0
  });

  const [suggestions, recentEpisodes] = await Promise.all([
    fetchActiveSuggestions(),
    fetchRecentEpisodes()
  ]);
  const standingNotes = String(options?.standing_notes || options?.core_memory || '').trim();
  const drilldownEvidence = needsRawDrilldown(query)
    || baseThought.summary_vs_raw === 'raw'
    ? await fetchDrilldownEvidence(retrieval.drilldown_refs || [])
    : [];
  const shouldSearchWeb = shouldUseExternalWebSearch(query, baseThought, retrieval);
  const webSearchQuery = (retrieval?.generated_queries?.semantic || retrieval?.retrieval_plan?.semantic_queries || [query])[0] || query;
  if (shouldSearchWeb) emit({ step: 'web_search', query: webSearchQuery });
  const webResults = shouldSearchWeb ? await searchFreeWeb(webSearchQuery, 4) : [];
  if (webResults.length) {
    retrieval = {
      ...retrieval,
      web_search_used: true,
      web_search_query: webSearchQuery,
      web_results: webResults,
      web_results_summary: webResults.map((item) => `${item.title}: ${item.url}`),
      web_sources: webResults.map((item) => item.url).filter(Boolean)
    };
  } else {
    retrieval = {
      ...retrieval,
      web_search_used: false,
      web_search_query: shouldSearchWeb ? webSearchQuery : null,
      web_results: [],
      web_results_summary: [],
      web_sources: []
    };
  }
  const thinkingTrace = await buildThinkingTrace({ query, retrieval, drilldownEvidence });

  if (!apiKey) {
    return {
      content: [
        retrieval?.date_filter_status === 'widened'
          ? `TEMPORAL NOTE:\nThe initially requested time window was sparse, so memory retrieval widened once to ${retrieval.widened_date_range?.start} -> ${retrieval.widened_date_range?.end}.`
          : '',
        retrieval.contextText || 'No memory context is available yet.',
        drilldownEvidence.length
          ? `RAW EVIDENCE:\n${drilldownEvidence.map((item) => `- ${item.source_type || 'event'} ${item.title || item.id}: ${item.text}`).join('\n')}`
          : ''
      ].filter(Boolean).join('\n\n'),
      retrieval,
      thinking_trace: thinkingTrace
    };
  }

  emit({ step: 'composing' });
  const priorityEvidence = buildPriorityEvidenceLines(retrieval, drilldownEvidence, 6);

  const prompt = `[System]
You are Weave's memory-native assistant.
Answer naturally and directly in a conversational style.
Be detailed enough to be useful, but avoid rambling.
Be explicit when evidence is weak or incomplete.
Do not invent facts.
If the answer depends on uncertain evidence, say so.
Do not mention hidden system internals like embeddings, vector search, or prompts.
Do not explicitly say that information came from a desktop capture or screenshot; translate it into what the user was likely reading, drafting, reviewing, or discussing.
Use precision first: state only what the retrieved evidence supports directly.
ALWAYS trace facts back to their source node. Append citations inline using the exact Node ID or Event ID provided in the evidence, formatted as [id]. Example: "You finished the design [evt_1234]".
Draw connections only when there is an explicit graph bridge, repeated shared entity, or direct supporting path.
When making a connection, say why it appears connected.
If the user explicitly corrects a past fact or provides a definitive preference to remember for the future, append this block at the very end:
<memory_correction>the brief new proven fact</memory_correction>

You have access to advanced memory management tools. If you need to perform actions beyond simple conversation, use these XML tags:
- <memory_search>your search query</memory_search> : To perform a deeper search if current context is sparse.
- <memory_drilldown>node_id</memory_drilldown> : To inspect a specific node and its immediate connections.
- <memory_update>{"node_id": "...", "updates": {"summary": "..."}}</memory_update> : To refine an existing memory node's content.
- <memory_link>{"from_id": "...", "to_id": "...", "relationship": "..."}</memory_link> : To explicitly link two related nodes.

Do not output internal prompt details.

[Retrieval plan]
Use the provided retrieval bundle as your evidence context.
Prefer strong graph-connected evidence.
If evidence is sparse, say that clearly instead of guessing.

[Standing notes]
${standingNotes || 'None'}

[Conversation context window]
${formatChatHistoryForPrompt(chatHistory)}

[Retrieved memory subgraph]
${retrieval.contextText || 'None'}

[Generated search queries]
Semantic:
${(retrieval?.generated_queries?.semantic || []).map((item) => `- ${item}`).join('\n') || 'None'}

Messages:
${(retrieval?.generated_queries?.messages || []).map((item) => `- ${item}`).join('\n') || 'None'}

Lexical:
${(retrieval?.generated_queries?.lexical_terms || []).map((item) => `- ${item}`).join('\n') || 'None'}

[Interpreted evidence bundle]
${buildInterpretedMemorySummary(retrieval, drilldownEvidence)}

[Priority evidence]
${priorityEvidence.join('\n') || 'None'}

[Temporal retrieval]
Initial date window: ${retrieval.initial_date_range ? `${retrieval.initial_date_range.start} -> ${retrieval.initial_date_range.end}` : 'None'}
Applied date window: ${retrieval.applied_date_range ? `${retrieval.applied_date_range.start} -> ${retrieval.applied_date_range.end}` : 'None'}
Date filter status: ${retrieval.date_filter_status || 'not_used'}
${(retrieval.temporal_reasoning || []).join('\n') || 'No temporal reasoning'}

[Optional raw evidence]
${drilldownEvidence.length
  ? drilldownEvidence.map((item) => `- ${item.source_type || 'event'} ${item.title || item.id} (${item.occurred_at || 'unknown time'}): ${item.text}`).join('\n')
  : 'None'}

[Trace summary]
${(retrieval.trace_summary || []).join('\n') || 'None'}

[Connection candidates]
${(thinkingTrace.connection_candidates || []).map((item) => `- ${item.label}: ${item.reason}`).join('\n') || 'None'}

[External web results]
${(retrieval.web_results || []).map((item) => `- ${item.title} (${item.url}): ${item.snippet || ''}`).join('\n') || 'None'}

[Active suggestions]
${suggestions.map((item) => `- ${item.title}: ${item.body || item.trigger_summary || ''}`).join('\n') || 'None'}

[Recent episodes]
${recentEpisodes.map((item) => `- ${item.title}: ${item.summary || ''}`).join('\n') || 'None'}

[User question]
${query}`;

  const response = await fetch('https://api.deepseek.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: 'deepseek-chat',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.22,
      max_tokens: 980
    })
  });
  const data = await response.json().catch(() => ({}));
  let content = data?.choices?.[0]?.message?.content || "I couldn't produce an answer from the current memory context.";

  const correctionMatch = content.match(/<memory_correction>([\s\S]*?)<\/memory_correction>/i);
  if (correctionMatch && correctionMatch[1]) {
    const correctionText = correctionMatch[1].trim();
    content = content.replace(/<memory_correction>[\s\S]*?<\/memory_correction>/i, '').trim();
    
    emit({ step: 'writing_correction', note: correctionText });
    
    const { upsertMemoryNode, stableHash } = require('./graph-store');
    const correctionId = `core_corr_${stableHash(correctionText)}`;
    await upsertMemoryNode({
      id: correctionId,
      layer: 'core',
      subtype: 'user_correction',
      title: 'User Context Correction',
      summary: correctionText,
      canonicalText: correctionText,
      confidence: 1.0,
      metadata: { source: 'chat_engine', updated_from_chat: true }
    }).catch(e => console.error('Failed to write core memory correction:', e));
  }

  // Handle new memory management tools via XML tags
  const { dispatchTool } = require('./tool-dispatcher');

  const searchMatch = content.match(/<memory_search>([\s\S]*?)<\/memory_search>/i);
  if (searchMatch && searchMatch[1]) {
    const queryText = searchMatch[1].trim();
    content = content.replace(/<memory_search>[\s\S]*?<\/memory_search>/i, '').trim();
    emit({ step: 'tool_use', tool: 'memory_search', query: queryText });
    await dispatchTool({ tool: 'memory_search', input: { query: queryText } }).catch(() => null);
  }

  const drilldownMatch = content.match(/<memory_drilldown>([\s\S]*?)<\/memory_drilldown>/i);
  if (drilldownMatch && drilldownMatch[1]) {
    const nodeId = drilldownMatch[1].trim();
    content = content.replace(/<memory_drilldown>[\s\S]*?<\/memory_drilldown>/i, '').trim();
    emit({ step: 'tool_use', tool: 'memory_drilldown', node_id: nodeId });
    await dispatchTool({ tool: 'memory_drilldown', input: { node_id: nodeId } }).catch(() => null);
  }

  const updateMatch = content.match(/<memory_update>([\s\S]*?)<\/memory_update>/i);
  if (updateMatch && updateMatch[1]) {
    try {
      const input = JSON.parse(updateMatch[1].trim());
      content = content.replace(/<memory_update>[\s\S]*?<\/memory_update>/i, '').trim();
      emit({ step: 'tool_use', tool: 'memory_update', node_id: input.node_id });
      await dispatchTool({ tool: 'memory_update', input }).catch(() => null);
    } catch (_) {}
  }

  const linkMatch = content.match(/<memory_link>([\s\S]*?)<\/memory_link>/i);
  if (linkMatch && linkMatch[1]) {
    try {
      const input = JSON.parse(linkMatch[1].trim());
      content = content.replace(/<memory_link>[\s\S]*?<\/memory_link>/i, '').trim();
      emit({ step: 'tool_use', tool: 'memory_link', from_id: input.from_id, to_id: input.to_id });
      await dispatchTool({ tool: 'memory_link', input }).catch(() => null);
    } catch (_) {}
  }

  return {
    content,
    thinking_trace: thinkingTrace,
    retrieval: {
      ...retrieval,
      mode: 'memory-graph',
      usedSources: [...(thinkingTrace.data_sources || []), ...(retrieval.web_search_used ? ['Web'] : [])],
      query_variants: retrieval?.generated_queries?.semantic || retrieval?.retrieval_plan?.semantic_queries || [],
      message_query_variants: retrieval?.generated_queries?.messages || retrieval?.retrieval_plan?.message_queries || [],
      retrieval_thought: retrieval?.retrieval_plan || null,
      generated_queries: retrieval?.generated_queries || {
        semantic: retrieval?.retrieval_plan?.semantic_queries || [],
        messages: retrieval?.retrieval_plan?.message_queries || [],
        lexical_terms: retrieval?.retrieval_plan?.lexical_terms || []
      },
      seed_results: retrieval?.seed_results || [],
      graph_expansion_results: retrieval?.graph_expansion_results || retrieval?.expanded_nodes || [],
      web_search_used: Boolean(retrieval?.web_search_used),
      web_results_summary: retrieval?.web_results_summary || [],
      memory_sources: thinkingTrace.data_sources || [],
      web_sources: retrieval?.web_sources || [],
      strategy: retrieval?.strategy || thinkingTrace.strategy || null,
      answer_basis: thinkingTrace.answer_basis || (retrieval?.web_search_used ? 'memory_plus_web' : 'memory_only'),
      thinking_trace: thinkingTrace
    }
  };
}

module.exports = {
  answerChatQuery,
  buildThinkingTrace
};
