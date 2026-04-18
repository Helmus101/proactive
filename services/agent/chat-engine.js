const db = require('../db');
const { ingestRawEvent } = require('../ingestion');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');
const { buildRetrievalThought, widenTemporalWindow, inferSurfaceFamilies } = require('./retrieval-thought-system');

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

function assessWebSearchNecessity(query, retrievalThought, retrieval) {
  const mode = retrievalThought?.source_mode || retrievalThought?.strategy_mode || 'memory_only';
  const normalizedQuery = String(query || '');
  const lowerQuery = normalizedQuery.toLowerCase();
  if (retrievalThought?.mode === 'queryless') {
    return { shouldSearchWeb: false, reason: 'Queryless retrieval does not require external search.' };
  }
  if (mode === 'web_only') {
    return { shouldSearchWeb: true, reason: 'The router classified this as a web-first question.' };
  }

  const evidenceCount = Number(retrieval?.evidence_count || 0);
  const seedCount = Number(retrieval?.seed_nodes?.length || retrieval?.primary_nodes?.length || 0);
  const maxScore = Array.isArray(retrieval?.evidence) && retrieval.evidence.length
    ? Math.max(...retrieval.evidence.map((e) => Number(e?.score || 0)))
    : 0;
  const asksCurrent = /\b(latest|current|today|news|public|internet|online|look up|search the web|google|website|site)\b/i.test(normalizedQuery);
  const looksPersonal = /\b(i|my|me|mine|we|our|us)\b/i.test(normalizedQuery);
  const asksPersonalContext = /\b(what did i|did i|my notes|my history|my project|my work|my context|follow up|unfinished tasks|status of)\b/i.test(normalizedQuery);
  const asksWorldKnowledge = /\b(what is|who is|when is|where is|tell me about|explain)\b/i.test(lowerQuery);
  const sparseMemory = seedCount < 2 || evidenceCount < 3 || maxScore < 0.52;

  if (mode === 'memory_then_web') {
    if (asksCurrent && sparseMemory) {
      return { shouldSearchWeb: true, reason: 'The request needs fresh or public context and the memory pass was not strong enough.' };
    }
    if (sparseMemory && !looksPersonal && !asksPersonalContext && asksWorldKnowledge) {
      return { shouldSearchWeb: true, reason: 'The request reads like general world knowledge, and the memory pass was not strong enough.' };
    }
    if (!asksCurrent && sparseMemory) {
      return { shouldSearchWeb: false, reason: 'The router allowed web fallback, but the request still looks memory-native, so it will ask for clarification before searching the web.' };
    }
    return { shouldSearchWeb: false, reason: 'Memory retrieval looked sufficient, so web search was not necessary.' };
  }

  if (asksCurrent && sparseMemory) {
    return { shouldSearchWeb: true, reason: 'The request appears current and the memory pass was weak.' };
  }

  return { shouldSearchWeb: false, reason: 'Memory retrieval looked sufficient, so web search was not necessary.' };
}

function formatStageDetail(items = [], fallback = '') {
  const cleaned = (items || []).map((item) => String(item || '').trim()).filter(Boolean);
  return cleaned.length ? cleaned.join(' ') : fallback;
}

function buildStageEvent(step, status, overrides = {}) {
  return {
    step,
    status,
    label: overrides.label || step.replace(/_/g, ' '),
    detail: overrides.detail || '',
    counts: overrides.counts || {},
    preview_items: Array.isArray(overrides.preview_items) ? overrides.preview_items : [],
    ...overrides
  };
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

function buildPriorityEvidenceLines(retrieval, drilldownEvidence = [], limit = 12) {
  const lines = [];
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  const ranked = [...evidence]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, Math.max(1, limit - 2));
  for (const item of ranked) {
    const layer = item.layer || item.type || 'memory';
    const text = String(item.text || item.title || '').replace(/\s+/g, ' ').trim().slice(0, 8000);
    if (!text) continue;
    lines.push(`- [${layer}] ${text}`);
  }
  for (const row of (drilldownEvidence || []).slice(0, 2)) {
    const text = String(row.text || row.title || '').replace(/\s+/g, ' ').trim().slice(0, 8000);
    if (!text) continue;
    lines.push(`- [raw:${row.source_type || 'event'}] ${text}`);
  }
  return lines.slice(0, limit);
}

function buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence = []) {
  const lines = [];
  const topEvidence = (retrieval?.evidence || []).slice(0, 5);
  if (topEvidence.length) {
    lines.push('Here is what I found in your memory:');
    for (const ev of topEvidence) {
      const snippet = String(ev.text || ev.title || '').replace(/\s+/g, ' ').trim().slice(0, 180);
      if (!snippet) continue;
      const id = ev.id || ev.node_id || ev.event_id || 'memory';
      lines.push(`- ${snippet} [${id}]`);
    }
  }

  const raw = (drilldownEvidence || []).slice(0, 2);
  if (raw.length) {
    lines.push('');
    lines.push('Raw supporting details:');
    for (const item of raw) {
      const id = item.id || 'event';
      const snippet = String(item.text || '').replace(/\s+/g, ' ').trim().slice(0, 220);
      lines.push(`- ${snippet} [${id}]`);
    }
  }

  if (!lines.length) {
    return "I couldn't find enough concrete memory evidence to answer that accurately. If you give me a timeframe or keyword, I'll run a deeper scan.";
  }
  return lines.join('\n');
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
  const strategyMode = plan.source_mode || plan.strategy_mode || 'memory_only';
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
    source_mode: strategyMode,
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
  const webQueries = Array.isArray(retrieval?.query_sets?.web_queries)
    ? retrieval.query_sets.web_queries
    : (Array.isArray(retrieval?.generated_queries?.web) ? retrieval.generated_queries.web : []);
  return {
    context: contextQueries.slice(0, 7),
    messages: messageQueries.slice(0, 7),
    lexical: lexicalTerms.filter((term) => isUsefulLexical(term)).slice(0, 10),
    web: webQueries.slice(0, 7)
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
  const primaryCount = Number(retrieval?.primary_nodes?.length || seedCount);
  const supportCount = Number(retrieval?.support_nodes?.length || 0);
  const evidenceNodeCount = Number(retrieval?.evidence_nodes?.length || 0);
  const headline = seedCount || expandedCount || evidenceCount
    ? `Found ${primaryCount} primary nodes, expanded to ${expandedCount} connected nodes, and packed ${evidenceCount} evidence items.`
    : 'Found little or no matching memory in the requested scope.';
  const details = [
    strongestClusterPhrase(retrieval)
  ];
  if (Array.isArray(retrieval?.seed_results) && retrieval.seed_results.length) {
    details.push(`Initial seed search found ${retrieval.seed_results.length} ranked seed results before graph expansion.`);
  }
  
  // Add iterative flow details if available in stageTrace
  const stageTrace = retrieval?.stage_trace || [];
  const primarySearchEvents = stageTrace.filter(s => s.step === 'primary_search_results');
  if (primarySearchEvents.length) {
    const totalSeeds = primarySearchEvents.reduce((sum, s) => sum + (s.count || 0), 0);
    const allSeeds = primarySearchEvents.flatMap(e => e.preview_items || []);
    const uniqueSeeds = Array.from(new Set(allSeeds.map(i => String(i).split(' (Score:')[0]))).slice(0, 3);
    const topSeeds = uniqueSeeds.join(', ');
    details.push(`Iterative flow: identified ${totalSeeds} top-tier seeds via RRF consolidation${topSeeds ? ` (${topSeeds})` : ''}.`);
  }
  const iterativeExpansionEvents = stageTrace.filter(s => s.step === 'iterative_expansion');
  if (iterativeExpansionEvents.length) {
    const totalExpanded = iterativeExpansionEvents.reduce((sum, s) => sum + (s.count || 0), 0);
    details.push(`Hierarchical expansion: added ${totalExpanded} neighbors from top seeds.`);
  }

  if (Array.isArray(retrieval?.graph_expansion_results) && retrieval.graph_expansion_results.length) {
    details.push(`Graph expansion added ${retrieval.graph_expansion_results.length} connected nodes (${supportCount} support, ${evidenceNodeCount} evidence).`);
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
    details: details.filter(Boolean).slice(0, 4),
    seed_count: seedCount,
    primary_count: primaryCount,
    support_count: supportCount,
    evidence_node_count: evidenceNodeCount,
    evidence_count: evidenceCount
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
  const resultsSummary = buildThinkingResultsSummary(retrieval, drilldownEvidence);
  
  // Add Deep Search indicator if recursion was used
  if (retrieval?.retrieval_plan?.recursion_depth > 0) {
    resultsSummary.details.push("Performed recursive 'Deep Search' expansion from high-confidence anchor nodes.");
  }

  return {
    thinking_summary: buildThinkingSummary(query, retrieval, drilldownEvidence),
    strategy,
    router: retrieval?.router || {
      source_mode: retrieval?.retrieval_plan?.source_mode || retrieval?.retrieval_plan?.strategy_mode || 'memory_only',
      router_reason: retrieval?.retrieval_plan?.router_reason || retrieval?.retrieval_plan?.web_gate_reason || '',
      time_scope: retrieval?.retrieval_plan?.time_scope || null,
      summary_vs_raw: retrieval?.retrieval_plan?.summary_vs_raw || 'summary'
    },
    filters: buildThinkingFilters(retrieval),
    search_queries: buildThinkingSearchQueries(retrieval),
    results_summary: resultsSummary,
    data_sources: dataSources,
    connection_candidates: buildConnectionCandidates(retrieval),
    seed_results: Array.isArray(retrieval?.seed_results) ? retrieval.seed_results : [],
    primary_nodes: Array.isArray(retrieval?.primary_nodes) ? retrieval.primary_nodes : (Array.isArray(retrieval?.seed_nodes) ? retrieval.seed_nodes : []),
    support_nodes: Array.isArray(retrieval?.support_nodes) ? retrieval.support_nodes : [],
    evidence_nodes: Array.isArray(retrieval?.evidence_nodes) ? retrieval.evidence_nodes : [],
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
    query_sets: retrieval?.query_sets || null,
    stage_trace: Array.isArray(retrieval?.stage_trace) ? retrieval.stage_trace : [],
    initial_date_range: retrieval?.initial_date_range || null,
    applied_date_range: retrieval?.applied_date_range || null,
    widened_date_range: retrieval?.widened_date_range || null,
    date_filter_status: retrieval?.date_filter_status || 'not_used',
    layers: Array.from(new Set((retrieval?.evidence || []).map(e => e.layer))).filter(Boolean)
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
  const ids = Array.from(new Set((refs || []).filter(Boolean))).slice(0, 20);
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT id, source_type, occurred_at, title, redacted_text, raw_text, app, source_account, metadata
     FROM events
     WHERE id IN (${placeholders})
     ORDER BY COALESCE(occurred_at, timestamp) DESC`,
    ids
  ).catch(() => []);
  return rows.map((row) => {
    const metadata = safeJsonParse(row.metadata, {});
    const text = metadata.cleaned_capture_text || row.redacted_text || row.raw_text || '';
    return {
      id: row.id,
      source_type: row.source_type,
      occurred_at: row.occurred_at,
      title: row.title,
      app: row.app,
      source_account: row.source_account,
      text: String(text).slice(0, 8000)
    };
  });
}

function retrievalLooksSparse(retrieval) {
  const evidenceCount = Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0);
  const seedCount = Number(retrieval?.seed_nodes?.length || 0);
  return seedCount < 2 || evidenceCount < 3;
}

async function executeParallelRetrieval(baseQuery, baseThought, options, onProgress = null) {
  const bundle = [
    String(baseQuery || '').trim(),
    ...((baseThought.semantic_queries || []).map((item) => String(item || '').trim())),
    ...((baseThought.message_queries || []).map((item) => String(item || '').trim()))
  ].filter(Boolean);
  const queries = Array.from(new Set(bundle)).slice(0, 7);
  
  // Detect when a query requires deep context (e.g., long-term relationship, patterns)
  const requiresDeepContext = /\b(relationship|pattern|over the last|long-term|habit|habitual|recurring|years?|months?)\b/i.test(baseQuery);
  const recursionDepth = (requiresDeepContext && !options.passiveOnly) ? 1 : 0;

  // Parallel multi-agent dispatch
  const results = await Promise.allSettled(queries.map((q) => buildHybridGraphRetrieval({
    query: q,
    options: {
      ...options,
      retrieval_thought: { ...baseThought, semantic_queries: [q] }
    },
    seedLimit: Math.max(5, Math.floor(20 / queries.length)),
    hopLimit: 8,
    recursionDepth,
    passiveOnly: options.passiveOnly || false,
    onProgress
  })));

  const successfulResults = results
    .filter((r) => r.status === 'fulfilled')
    .map((r) => r.value);

  if (successfulResults.length === 0) {
    throw new Error('All parallel retrieval attempts failed.');
  }

  if (successfulResults.length === 1) return successfulResults[0];

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
  
  const evidence = mergeById(successfulResults.map((r) => r.evidence)).slice(0, Math.max(...successfulResults.map((r) => r.evidence_count)));
  const expanded_nodes = mergeById(successfulResults.map((r) => r.expanded_nodes));
  const seed_nodes = mergeById(successfulResults.map((r) => r.seed_nodes));

  return {
    ...successfulResults[0],
    retrieval_run_id: successfulResults.map((r) => r.retrieval_run_id).join(','),
    retrieval_plan: {
      ...successfulResults[0].retrieval_plan,
      recursion_depth: recursionDepth
    },
    thought_summary: Array.from(new Set(successfulResults.flatMap((r) => r.thought_summary))),
    trace_summary: Array.from(new Set(successfulResults.flatMap((r) => r.trace_summary))),
    evidence,
    evidence_count: evidence.length,
    expanded_nodes,
    seed_nodes,
    edge_paths: successfulResults.flatMap((r) => r.edge_paths),
    drilldown_refs: Array.from(new Set(successfulResults.flatMap((r) => r.drilldown_refs))),
    lazy_source_refs: Array.from(new Set(successfulResults.flatMap((r) => r.lazy_source_refs.map((x) => x.ref)))).map((ref) => ({ ref })),
    contextText: successfulResults.map((r) => r.contextText).join('\n\n---\n\n')
  };
}

async function answerChatQuery({ apiKey, query, options = {}, onStep }) {
  const stageTrace = [];
  const emit = (data) => {
    try {
      if (data && (data.step === 'primary_search_results' || data.step === 'iterative_expansion')) {
        stageTrace.push(buildStageEvent(data.step, data.status || 'completed', data));
      }
      onStep?.(data);
    } catch (_) {}
  };
  
  // Normalize API key: prefer explicit param, then environment, then options; treat empty string as absent
  if (!apiKey) {
    apiKey = process.env.DEEPSEEK_API_KEY || options?.apiKey || null;
  }
  const emitStage = (step, status, overrides = {}) => {
    const event = buildStageEvent(step, status, overrides);
    stageTrace.push(event);
    emit(event);
    return event;
  };
  const chatHistory = normalizeChatHistoryWindow(options?.chat_history, 12);
  const retrievalQuery = buildQueryWithChatContext(query, chatHistory);

  // Persist user chat turn as a raw event
  await ingestRawEvent({
    type: 'ChatMessage',
    source: 'Chat',
    text: query,
    metadata: {
      role: 'user',
      chat_session_id: options?.chat_session_id,
      timestamp: new Date().toISOString()
    }
  }).catch(e => console.warn('Failed to persist user chat turn:', e));

  const baseThought = await buildRetrievalThought({
    query: retrievalQuery,
    mode: 'chat',
    dateRange: options?.date_range,
    app: options?.app
  });
  emitStage('routing', 'completed', {
    label: 'Routing',
    detail: formatStageDetail([
      `Source mode: ${baseThought.source_mode || baseThought.strategy_mode || 'memory_only'}.`,
      baseThought.router_reason || baseThought.web_gate_reason || ''
    ], 'Planned source routing.'),
    counts: {
      memory_queries: Number(baseThought.query_sets?.memory_queries?.length || baseThought.semantic_queries?.length || 0),
      message_queries: Number(baseThought.query_sets?.message_queries?.length || baseThought.message_queries?.length || 0),
      web_queries: Number(baseThought.query_sets?.web_queries?.length || baseThought.web_queries?.length || 0)
    },
    preview_items: [
      baseThought.source_mode || baseThought.strategy_mode || 'memory_only',
      baseThought.time_scope?.label || null,
      baseThought.summary_vs_raw || null
    ].filter(Boolean)
  });
  emitStage('query_generation', 'completed', {
    label: 'Query generation',
    detail: formatStageDetail([
      `Prepared ${baseThought.query_sets?.memory_queries?.length || baseThought.semantic_queries?.length || 0} memory queries,`,
      `${baseThought.query_sets?.message_queries?.length || baseThought.message_queries?.length || 0} message queries,`,
      `and ${baseThought.query_sets?.web_queries?.length || baseThought.web_queries?.length || 0} web fallback queries.`,
      'Initial reasoning will decide later whether web search is actually necessary.'
    ], 'Prepared retrieval queries.'),
    counts: {
      memory_queries: Number(baseThought.query_sets?.memory_queries?.length || baseThought.semantic_queries?.length || 0),
      message_queries: Number(baseThought.query_sets?.message_queries?.length || baseThought.message_queries?.length || 0),
      web_queries: Number(baseThought.query_sets?.web_queries?.length || baseThought.web_queries?.length || 0)
    },
    preview_items: [
      ...((baseThought.query_sets?.memory_queries || baseThought.semantic_queries || []).slice(0, 2)),
      ...((baseThought.query_sets?.web_queries || baseThought.web_queries || []).slice(0, 1))
    ]
  });

  let retrieval = {
    retrieval_plan: baseThought,
    router: {
      source_mode: baseThought.source_mode || baseThought.strategy_mode || 'memory_only',
      router_reason: baseThought.router_reason || baseThought.web_gate_reason || '',
      time_scope: baseThought.time_scope || null,
      summary_vs_raw: baseThought.summary_vs_raw || 'summary'
    },
    query_sets: baseThought.query_sets || {
      memory_queries: baseThought.semantic_queries || [],
      message_queries: baseThought.message_queries || [],
      web_queries: baseThought.web_queries || []
    },
    generated_queries: {
      semantic: baseThought.semantic_queries || [],
      messages: baseThought.message_queries || [],
      web: baseThought.web_queries || [],
      lexical_terms: baseThought.lexical_terms || []
    },
    seed_nodes: [],
    seed_results: [],
    primary_nodes: [],
    support_nodes: [],
    evidence_nodes: [],
    expanded_nodes: [],
    graph_expansion_results: [],
    edge_paths: [],
    evidence: [],
    evidence_count: 0,
    contextText: ''
  };

  if ((baseThought.source_mode || baseThought.strategy_mode) !== 'web_only') {
    // Passive-First Heuristic: attempt retrieval from Core, Insight, Cloud layers first
    retrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
      mode: 'chat',
      app: options?.app,
      date_range: baseThought.applied_date_range || options?.date_range,
      source_types: options?.source_types,
      retrieval_thought: baseThought,
      passiveOnly: true
    }, emit);

    const passiveMaxScore = retrieval.evidence?.length ? Math.max(...retrieval.evidence.map((e) => e.score || 0)) : 0;
    const isPassiveSufficient = (retrieval.evidence_count >= 3 && passiveMaxScore > 0.75) || (passiveMaxScore > 0.9);

    if (!isPassiveSufficient) {
      retrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
        mode: 'chat',
        app: options?.app,
        date_range: baseThought.applied_date_range || options?.date_range,
        source_types: options?.source_types,
        retrieval_thought: baseThought,
        passiveOnly: false
      }, emit);
    }

    const canWiden = Boolean(baseThought?.initial_date_range || baseThought?.filters?.app) && !baseThought?.fallback_policy?.attempted;
    if (canWiden && retrievalLooksSparse(retrieval)) {
      const widenedRange = baseThought.initial_date_range ? widenTemporalWindow(baseThought.initial_date_range) : null;
      let widenedApps = null;
      if (baseThought.filters?.app) {
        const families = inferSurfaceFamilies(retrievalQuery, '', baseThought.filters.app);
        if (families.includes('communication')) widenedApps = ['Gmail', 'Slack', 'Messages', 'WhatsApp', 'Signal'];
        else if (families.includes('coding')) widenedApps = ['GitHub', 'Cursor', 'Xcode', 'VSCode'];
        else if (families.includes('browser')) widenedApps = ['Chrome', 'Safari', 'Arc'];
      }

      if (widenedRange || widenedApps) {
        const widenedFilters = {
          ...(baseThought.filters || {}),
          date_range: widenedRange || baseThought.filters?.date_range,
          app: widenedApps || baseThought.filters?.app
        };
        const widenedThought = {
          ...baseThought,
          filters: widenedFilters,
          applied_date_range: widenedRange || baseThought.applied_date_range,
          widened_date_range: widenedRange,
          date_filter_status: 'widened',
          temporal_reasoning: [
            ...(baseThought.temporal_reasoning || []),
            widenedRange ? `Initial time window looked sparse, so retrieval widened to ${widenedRange.start} -> ${widenedRange.end}.` : '',
            widenedApps ? `Initial app filter was too restrictive, so broadened to related apps: ${widenedApps.join(', ')}.` : ''
          ].filter(Boolean),
          fallback_policy: {
            ...(baseThought.fallback_policy || { mode: 'widen_once' }),
            attempted: true,
            widened: true
          }
        };

        const widenedRetrieval = await executeParallelRetrieval(retrievalQuery, widenedThought, {
          mode: 'chat',
          app: widenedApps || options?.app,
          date_range: widenedRange || baseThought.applied_date_range,
          source_types: options?.source_types,
          retrieval_thought: widenedThought
        }, onStep);
        retrieval = {
          ...widenedRetrieval,
          initial_date_range: baseThought.initial_date_range,
          widened_date_range: widenedRange,
          date_filter_status: 'widened',
          temporal_reasoning: widenedThought.temporal_reasoning
        };
      }
    }

    if (retrievalLooksSparse(retrieval) && !options.passiveOnly) {
      const deepRetrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
        ...options,
        mode: 'chat',
        recursionDepth: 1,
        passiveOnly: false
      }, emit);
      if ((deepRetrieval.evidence_count || 0) > (retrieval.evidence_count || 0)) {
         retrieval = {
           ...deepRetrieval,
           initial_date_range: retrieval.initial_date_range,
           widened_date_range: retrieval.widened_date_range,
           date_filter_status: retrieval.date_filter_status
         };
      }
    }

    retrieval = {
      ...retrieval,
      stage_trace: stageTrace,
      router: retrieval.router || {
        source_mode: baseThought.source_mode || baseThought.strategy_mode || 'memory_only',
        router_reason: baseThought.router_reason || baseThought.web_gate_reason || '',
        time_scope: baseThought.time_scope || null,
        summary_vs_raw: baseThought.summary_vs_raw || 'summary'
      },
      query_sets: retrieval.query_sets || retrieval.generated_queries || baseThought.query_sets || null
    };

    emitStage('ranking', 'completed', {
      label: 'Ranking and packing',
      detail: `Packed ${retrieval.evidence_count || 0} evidence items from primary nodes, support nodes, and downward evidence.`,
      counts: retrieval.packed_context_stats || {
        evidence: Number(retrieval.evidence_count || 0)
      },
      preview_items: (retrieval.evidence || []).slice(0, 3).map((item) => item.text || item.id)
    });
  } else {
    emitStage('memory_search', 'skipped', {
      label: 'Memory search',
      detail: 'Skipped memory retrieval because the router selected web-only mode.'
    });
    emitStage('seed_selection', 'skipped', {
      label: 'Node retrieval',
      detail: 'No memory seed selection was needed for this request.'
    });
    emitStage('edge_expansion', 'skipped', {
      label: 'Edge expansion',
      detail: 'Skipped graph expansion because no memory seeds were selected.'
    });
    emitStage('ranking', 'skipped', {
      label: 'Ranking and packing',
      detail: 'Skipped memory context packing because the request is web-only.'
    });
  }

  const maxScore = retrieval.evidence?.length ? Math.max(...retrieval.evidence.map((e) => e.score || 0)) : 0;
  const sparseMemory = retrieval.evidence_count === 0 || (retrieval.evidence_count < 3 && maxScore < 0.45);

  const [suggestions, recentEpisodes] = await Promise.all([
    fetchActiveSuggestions(),
    fetchRecentEpisodes()
  ]);
  const standingNotes = String(options?.standing_notes || options?.core_memory || '').trim();
  const drilldownEvidence = (retrieval.drilldown_refs || []).length
    ? await fetchDrilldownEvidence(retrieval.drilldown_refs || [])
    : [];
  const webAssessment = assessWebSearchNecessity(query, baseThought, retrieval);
  const shouldSearchWeb = webAssessment.shouldSearchWeb;
  const webSearchQuery = (retrieval?.query_sets?.web_queries || retrieval?.generated_queries?.web || retrieval?.generated_queries?.semantic || retrieval?.retrieval_plan?.web_queries || retrieval?.retrieval_plan?.semantic_queries || [query])[0] || query;
  let webResults = [];
  if (shouldSearchWeb || (baseThought.source_mode || baseThought.strategy_mode) === 'web_only') {
    emitStage('web_search', 'started', {
      label: 'Web search',
      detail: `${webAssessment.reason} Searching the web using: ${webSearchQuery}`
    });
    webResults = await searchFreeWeb(webSearchQuery, 4);
    emitStage('web_search', 'completed', {
      label: 'Web search',
      detail: webResults.length
        ? `Retrieved ${webResults.length} public web results.`
        : 'No web results were returned for this query.',
      counts: { web_results: webResults.length },
      preview_items: webResults.slice(0, 3).map((item) => item.title || item.url)
    });
  } else {
    emitStage('web_search', 'skipped', {
      label: 'Web search',
      detail: webAssessment.reason
    });
  }
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
  if (sparseMemory && !webResults.length && !drilldownEvidence.length) {
    // If no API key is available, ask the user for clarification as before.
    if (!apiKey) {
      emitStage('synthesis', 'completed', {
        label: 'Synthesis',
        detail: 'Available evidence was too sparse across memory and web, so the assistant is asking for clarification.'
      });
      return {
         content: "I couldn't find enough specific details in your memory graph to answer that accurately. Could you provide a bit more context, like a specific timeframe or associated project?",
         needs_clarification: true,
         retrieval: {
           ...retrieval,
           stage_trace: stageTrace
         },
         thinking_trace: ["Confidence Gating: Retrieval returned insufficient high-confidence context across memory and web. Prompting user for clarification."]
      };
    }

    // If an API key is present, proceed to synthesize an answer using the LLM
    // even when memory evidence is sparse. Emit a low-confidence note in the trace.
    emitStage('synthesis', 'started', {
      label: 'Synthesis',
      detail: 'Evidence was sparse across memory and web, but an LLM key is available — proceeding to generate an answer with low confidence.'
    });
  }
  const thinkingTrace = await buildThinkingTrace({ query, retrieval, drilldownEvidence });
  emitStage('synthesis', 'started', {
    label: 'Synthesis',
    detail: 'Reasoning over the packed context bundle to draft the answer.'
  });

  let content = "I couldn't produce an answer from the current memory context.";
  if (!apiKey) {
      content = [
        retrieval?.date_filter_status === 'widened'
          ? `TEMPORAL NOTE:\nThe initially requested time window was sparse, so memory retrieval widened once to ${retrieval.widened_date_range?.start} -> ${retrieval.widened_date_range?.end}.`
          : '',
        retrieval.contextText || 'No memory context is available yet.',
        webResults.length
          ? `WEB RESULTS:\n${webResults.map((item) => `- ${item.title || item.url}: ${item.url}`).join('\n')}`
          : '',
        drilldownEvidence.length
          ? `RAW EVIDENCE:\n${drilldownEvidence.map((item) => `- ${item.source_type || 'event'} ${item.title || item.id}: ${item.text}`).join('\n')}`
          : ''
      ].filter(Boolean).join('\n\n');
  } else {
    const priorityEvidence = buildPriorityEvidenceLines(retrieval, drilldownEvidence, 6);

    const prompt = `[System]
    You are Weave's memory-native assistant.
    Answer naturally and directly in a conversational style.
    Be grounded, direct, and concise.
    Do not invent facts.
    Do not mention hidden system internals like embeddings, vector search, or prompts.
    Do not explicitly say that information came from a desktop capture or screenshot; translate it into what the user was likely reading, drafting, reviewing, or discussing.

    [Retrieved memory context]
    ${retrieval.contextText || 'None'}

    [Standing notes]
    ${standingNotes || 'None'}

    [Conversation history]
    ${formatChatHistoryForPrompt(chatHistory)}

    [Priority evidence]
    ${priorityEvidence.join('\n') || 'None'}

    [Web results]
    ${webResults.length ? webResults.map((item) => `- ${item.title || item.url}\n  ${item.url}\n  ${item.snippet || ''}`).join('\n') : 'None'}

    [User question]
    ${query}`;

    try {
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

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`LLM request failed (${response.status})${body ? `: ${body.slice(0, 180)}` : ''}`);
      }
      const data = await response.json().catch(() => ({}));
      content = data?.choices?.[0]?.message?.content || content;
    } catch (llmError) {
      content = buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
    }
  }
  emitStage('synthesis', 'completed', {
    label: 'Synthesis',
    detail: webResults.length
      ? 'Generated the answer from memory plus web support.'
      : 'Generated the answer from memory context.',
    counts: {
      evidence: Number(retrieval.evidence_count || 0),
      web_results: Number(webResults.length || 0)
    }
  });

  // Persist assistant chat turn as a raw event
  await ingestRawEvent({
    type: 'ChatMessage',
    source: 'Chat',
    text: content,
    metadata: {
      role: 'assistant',
      chat_session_id: options?.chat_session_id,
      timestamp: new Date().toISOString()
    }
  }).catch(e => console.warn('Failed to persist assistant chat turn:', e));
  emitStage('memory_writeback', 'completed', {
    label: 'Write-back',
    detail: 'Stored this chat turn back into memory as raw chat events.'
  });

  const correctionMatch = content.match(/<memory_correction>([\s\S]*?)<\/memory_correction>/i);
  if (correctionMatch && correctionMatch[1]) {
    const correctionText = correctionMatch[1].trim();
    content = content.replace(/<memory_correction>[\s\S]*?<\/memory_correction>/i, '').trim();
    
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
    await dispatchTool({ tool: 'memory_search', input: { query: queryText } }).catch(() => null);
  }

  const drilldownMatch = content.match(/<memory_drilldown>([\s\S]*?)<\/memory_drilldown>/i);
  if (drilldownMatch && drilldownMatch[1]) {
    const nodeId = drilldownMatch[1].trim();
    content = content.replace(/<memory_drilldown>[\s\S]*?<\/memory_drilldown>/i, '').trim();
    await dispatchTool({ tool: 'memory_drilldown', input: { node_id: nodeId } }).catch(() => null);
  }

  const updateMatch = content.match(/<memory_update>([\s\S]*?)<\/memory_update>/i);
  if (updateMatch && updateMatch[1]) {
    try {
      const input = JSON.parse(updateMatch[1].trim());
      content = content.replace(/<memory_update>[\s\S]*?<\/memory_update>/i, '').trim();
      await dispatchTool({ tool: 'memory_update', input }).catch(() => null);
    } catch (_) {}
  }

  const linkMatch = content.match(/<memory_link>([\s\S]*?)<\/memory_link>/i);
  if (linkMatch && linkMatch[1]) {
    try {
      const input = JSON.parse(linkMatch[1].trim());
      content = content.replace(/<memory_link>[\s\S]*?<\/memory_link>/i, '').trim();
      await dispatchTool({ tool: 'memory_link', input }).catch(() => null);
    } catch (_) {}
  }

  return {
    content,
    thinking_trace: thinkingTrace,
    retrieval: {
      ...retrieval,
      stage_trace: stageTrace,
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
      router: retrieval?.router || thinkingTrace.router || null,
      query_sets: retrieval?.query_sets || null,
      primary_nodes: retrieval?.primary_nodes || retrieval?.seed_nodes || [],
      support_nodes: retrieval?.support_nodes || [],
      evidence_nodes: retrieval?.evidence_nodes || [],
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
