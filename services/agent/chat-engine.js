const db = require('../db');
const { ingestRawEvent } = require('../ingestion');
const { callLLM } = require('./intelligence-engine');
const { buildHybridGraphRetrieval, formatContext, estimateTokensHeuristic } = require('./hybrid-graph-retrieval');
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

function normalizeChatHistoryWindow(history = [], limit = 10) {
  if (!Array.isArray(history)) return [];
  return history
    .filter((item) => item && typeof item.content === 'string')
    .slice(-Math.max(1, limit))
    .map((item) => ({
      role: item.role === 'assistant' ? 'assistant' : 'user',
      content: String(item.content || '').trim().slice(0, 600),
      ts: item.ts || null
    }))
    .filter((item) => item.content);
}

function buildQueryWithChatContext(query, chatHistory = []) {
  const userTurns = chatHistory.filter((item) => item.role === 'user').map((item) => item.content).slice(-10);
  if (!userTurns.length) return String(query || '').trim();
  return `${String(query || '').trim()}\n\nConversation context:\n${userTurns.map((item) => `- ${item}`).join('\n')}`;
}

function tokenizeQueryTerms(query = '') {
  return String(query || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .map((term) => term.trim())
    .filter((term) => term.length >= 3)
    .slice(0, 24);
}

function searchSummaryRollups(query = '', summaries = {}, searchIndex = {}, limit = 6) {
  const terms = tokenizeQueryTerms(query);
  if (!terms.length || !summaries || typeof summaries !== 'object') return [];

  const dateBoost = new Set();
  const peopleIndex = searchIndex?.people || {};
  const topicIndex = searchIndex?.topics || {};
  for (const term of terms) {
    const peopleDates = Array.isArray(peopleIndex[term]) ? peopleIndex[term] : [];
    const topicDates = Array.isArray(topicIndex[term]) ? topicIndex[term] : [];
    [...peopleDates, ...topicDates].forEach((d) => dateBoost.add(d));
  }

  const rows = Object.entries(summaries || {}).map(([date, summary]) => {
    const narrative = String(summary?.narrative || '').toLowerCase();
    const people = [
      ...(Array.isArray(summary?.top_people) ? summary.top_people : []),
      ...(Array.isArray(summary?.top_contacts) ? summary.top_contacts : [])
    ].map((x) => String(x || '').toLowerCase());
    const topics = [
      ...(Array.isArray(summary?.topics) ? summary.topics : []),
      ...(Array.isArray(summary?.tags) ? summary.tags : []),
      ...(Array.isArray(summary?.intent_clusters) ? summary.intent_clusters : [])
    ].map((x) => String(x || '').toLowerCase());

    let score = dateBoost.has(date) ? 0.35 : 0;
    for (const term of terms) {
      if (narrative.includes(term)) score += 0.2;
      if (people.some((p) => p.includes(term))) score += 0.25;
      if (topics.some((t) => t.includes(term))) score += 0.2;
    }

    const recencyBonus = (() => {
      const ts = Date.parse(`${date}T00:00:00Z`);
      if (!Number.isFinite(ts)) return 0;
      const days = (Date.now() - ts) / (24 * 60 * 60 * 1000);
      if (days <= 3) return 0.3;
      if (days <= 14) return 0.18;
      if (days <= 30) return 0.1;
      return 0;
    })();

    score += recencyBonus;

    return {
      date,
      score,
      narrative: String(summary?.narrative || '').trim(),
      top_people: Array.isArray(summary?.top_people) ? summary.top_people : [],
      topics: Array.isArray(summary?.topics) ? summary.topics : [],
      intent_clusters: Array.isArray(summary?.intent_clusters) ? summary.intent_clusters : []
    };
  });

  return rows
    .filter((r) => r.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, Math.max(1, limit));
}

function formatSummaryContext(summaryHits = []) {
  const hits = Array.isArray(summaryHits) ? summaryHits : [];
  if (!hits.length) return '';
  return hits
    .slice(0, 6)
    .map((item) => {
      const people = (item.top_people || []).slice(0, 3).join(', ');
      const topics = (item.topics || []).slice(0, 3).join(', ');
      const narrative = String(item.narrative || '').replace(/\s+/g, ' ').trim().slice(0, 220);
      return `- [${item.date}] ${narrative}${people ? ` | people: ${people}` : ''}${topics ? ` | topics: ${topics}` : ''}`;
    })
    .join('\n');
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

  for (const item of drilldownEvidence.slice(0, 6)) {
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
  for (const row of (drilldownEvidence || []).slice(0, 20)) {
    const text = String(row.text || row.title || '').replace(/\s+/g, ' ').trim().slice(0, 8000);
    if (!text) continue;
    lines.push(`- [raw:${row.source_type || 'event'}] ${text}`);
  }
  return lines.slice(0, limit);
}

function buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence = []) {
  const lines = [];
  const topEvidence = (retrieval?.evidence || []).slice(0, 15);
  if (topEvidence.length) {
    lines.push('Here is what I found in your memory:');
    for (const ev of topEvidence) {
      const snippet = String(ev.text || ev.title || '').replace(/\s+/g, ' ').trim().slice(0, 180);
      if (!snippet) continue;
      const id = ev.id || ev.node_id || ev.event_id || 'memory';
      lines.push(`- ${snippet} [${id}]`);
    }
  }

  const raw = (drilldownEvidence || []).slice(0, 15);
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
    context: contextQueries.slice(0, 15),
    messages: messageQueries.slice(0, 15),
    lexical: lexicalTerms.filter((term) => isUsefulLexical(term)).slice(0, 20),
    web: webQueries.slice(0, 15)
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
  const expandedCount = Number(retrieval?.expanded_nodes?.length || retrieval?.graph_expansion_results?.length || 0);
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
  if (Array.isArray(retrieval?.summary_hits) && retrieval.summary_hits.length) {
    details.push(`Compressive memory: reused ${retrieval.summary_hits.length} daily summary snapshots before raw evidence expansion.`);
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

function buildReasoningChain(retrieval = {}) {
  const plan = retrieval.retrieval_plan || {};
  const judgment = retrieval.judgment || {};
  const reflection = retrieval.reflection || {};
  const sourceMode = String(plan.source_mode || plan.strategy_mode || 'memory_only').replace(/_/g, ' ');
  const queryCount = {
    memory: Number((retrieval?.query_sets?.memory_queries || plan?.query_sets?.memory_queries || retrieval?.generated_queries?.semantic || plan?.semantic_queries || []).length || 0),
    messages: Number((retrieval?.query_sets?.message_queries || plan?.query_sets?.message_queries || retrieval?.generated_queries?.messages || plan?.message_queries || []).length || 0),
    web: Number((retrieval?.query_sets?.web_queries || plan?.query_sets?.web_queries || retrieval?.generated_queries?.web || plan?.web_queries || []).length || 0)
  };
  const chain = [];
  chain.push({
    stage: 'hypothesis',
    summary: `Classified the query as ${sourceMode} retrieval.`,
    detail: plan.router_reason || plan.web_gate_reason || ''
  });
  chain.push({
    stage: 'search_plan',
    summary: `Prepared ${queryCount.memory} memory, ${queryCount.messages} message, and ${queryCount.web} web query variants.`,
    detail: ''
  });
  const seedCount = Number(retrieval.seed_nodes?.length || 0);
  const expandedCount = Number(retrieval.expanded_nodes?.length || retrieval.graph_expansion_results?.length || 0);
  const evidenceCount = Number(retrieval.evidence_count || retrieval.evidence?.length || 0);
  chain.push({
    stage: 'expansion',
    summary: `Retrieved ${seedCount} seed nodes and expanded to ${expandedCount} connected nodes, packing ${evidenceCount} evidence items.`,
    detail: ''
  });
  if (judgment && Object.keys(judgment).length) {
    chain.push({
      stage: 'judge',
      summary: `Judge confidence ${Number(judgment.confidence_score || 0).toFixed(2)} and sufficiency ${judgment.sufficient ? 'approved' : 'needs more evidence'}.`,
      detail: judgment.reason || ''
    });
  }
  chain.push({
    stage: 'synthesis',
    summary: `Drafted the response using ${retrieval.web_search_used ? 'memory plus web' : 'memory-only'} context.`,
    detail: retrieval.web_search_used ? 'Web results were included in the answer.' : 'No external web results were needed.'
  });
  if (reflection && Object.keys(reflection).length) {
    chain.push({
      stage: 'reflection',
      summary: reflection.approved ? 'Reflection approved the draft.' : 'Reflection requested revisions.',
      detail: reflection.reason || ''
    });
  }
  return chain;
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
    planner: retrieval?.plan || null,
    judge: retrieval?.judgment || null,
    reflector: retrieval?.reflection || null,
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
    summary_hits: Array.isArray(retrieval?.summary_hits) ? retrieval.summary_hits : [],
    summary_context: retrieval?.summary_context || '',
    web_sources: Array.isArray(retrieval?.web_sources) ? retrieval.web_sources : [],
    generated_queries: retrieval?.generated_queries || null,
    lexical_terms: Array.isArray(retrieval?.generated_queries?.lexical_terms) ? retrieval.generated_queries.lexical_terms : [],
    web_results: Array.isArray(retrieval?.web_results) ? retrieval.web_results : [],
    answer_basis: strategy.answer_basis,
    temporal_reasoning: Array.isArray(retrieval?.temporal_reasoning) ? retrieval.temporal_reasoning : [],
    query_sets: retrieval?.query_sets || null,
    search_phases: Array.isArray(retrieval?.retrieval_plan?.search_phases) ? retrieval.retrieval_plan.search_phases : [],
    stage_trace: Array.isArray(retrieval?.stage_trace) ? retrieval.stage_trace : [],
    reasoning_chain: buildReasoningChain(retrieval),
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
  const ids = Array.from(new Set((refs || []).filter(Boolean))).slice(0, 60);
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT id, source_type, occurred_at, title, redacted_text, raw_text, text, app, source_account, metadata
     FROM events
     WHERE id IN (${placeholders})
     ORDER BY COALESCE(occurred_at, timestamp) DESC`,
    ids
  ).catch(() => []);
  return rows.map((row) => {
    const metadata = safeJsonParse(row.metadata, {});
    const text = metadata.cleaned_capture_text || row.redacted_text || row.raw_text || row.text || '';
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
  const queries = Array.from(new Set(bundle)).slice(0, 15);

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

  const budget = baseThought.context_budget_tokens || 2000;
  const contextText = formatContext({
    budget,
    primarySeeds: seed_nodes.slice(0, 10),
    hierarchicalExpandedNodes: [], // We don't easily have these separated after merge
    seeds: seed_nodes,
    expandedNodes: expanded_nodes,
    edgePaths: successfulResults.flatMap((r) => r.edge_paths)
  });

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
    contextText
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

  // 1. Router Stage
  emitStage('routing', 'started', { label: 'Hypothesis', detail: 'Deciding whether this query should use memory, web, or hybrid retrieval.' });
  const { baseThought, retrievalQuery, chatHistory } = await runRouterStage({ query, options });
  const summaryHits = searchSummaryRollups(query, options?.historical_summaries || {}, options?.search_index || {}, 6);
  const summaryContext = formatSummaryContext(summaryHits);
  emitStage('routing', 'completed', {
    label: 'Hypothesis',
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
      baseThought.summary_vs_raw || null,
      summaryHits.length ? `summary hits: ${summaryHits.length}` : null
    ].filter(Boolean)
  });

  emitStage('query_generation', 'completed', {
    label: 'Search planning',
    detail: formatStageDetail([
      `Prepared ${baseThought.query_sets?.memory_queries?.length || baseThought.semantic_queries?.length || 0} memory queries,`,
      `${baseThought.query_sets?.message_queries?.length || baseThought.message_queries?.length || 0} message queries,`,
      `and ${baseThought.query_sets?.web_queries?.length || baseThought.web_queries?.length || 0} web fallback queries.`
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

  // 2. Planner Stage
  emitStage('planning', 'started', { label: 'Planner', detail: 'Generating a concise execution plan and refining key query terms.' });
  let plan = await runPlannerStage({ query: retrievalQuery, routerOutput: baseThought, apiKey });
  emitStage('planning', 'completed', {
    label: 'Planner',
    detail: plan ? 'Created a formal execution plan with refined queries.' : 'Using default routing plan.',
    preview_items: plan?.reasoning_plan || []
  });

  // 3. Retriever Stage (always memory first, then web if needed)
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
    contextText: '',
    summary_hits: summaryHits,
    summary_context: summaryContext
  };
  let webResults = [];
  let drilldownEvidence = [];
  let judgment = { sufficient: false };
  let iteration = 0;
  const maxIterations = 2;

  while (!judgment.sufficient && iteration < maxIterations) {
    iteration++;
    emitStage('retrieving', 'started', { label: iteration > 1 ? `Phase 1: Initial memory search (Attempt ${iteration})` : 'Phase 1: Initial memory search', detail: 'Applying time-first filters, searching all memory layers, and collecting top candidates.' });

    // Use refined queries if available and it's not the first pass or if they exist
    const currentThought = (iteration > 1 && judgment.suggested_queries)
      ? { ...baseThought, semantic_queries: judgment.suggested_queries }
      : (plan?.refined_queries ? { ...baseThought, semantic_queries: plan.refined_queries } : baseThought);

    retrieval = await executeParallelRetrieval(retrievalQuery, currentThought, {
      mode: 'chat',
      app: options?.app,
      date_range: currentThought.applied_date_range || options?.date_range,
      source_types: options?.source_types,
      retrieval_thought: currentThought,
      passiveOnly: iteration === 1
    }, emit);

    if (iteration === 1 && retrievalLooksSparse(retrieval)) {
      retrieval = await executeParallelRetrieval(retrievalQuery, currentThought, {
        mode: 'chat',
        app: options?.app,
        date_range: currentThought.applied_date_range || options?.date_range,
        source_types: options?.source_types,
        retrieval_thought: currentThought,
        passiveOnly: false
      }, emit);
    }

    // Widen if still sparse
    const canWiden = Boolean(currentThought?.initial_date_range || currentThought?.filters?.app) && !currentThought?.fallback_policy?.attempted;
    if (canWiden && retrievalLooksSparse(retrieval)) {
      const widenedRange = currentThought.initial_date_range ? widenTemporalWindow(currentThought.initial_date_range) : null;
      let widenedApps = null;
      if (currentThought.filters?.app) {
        const families = inferSurfaceFamilies(retrievalQuery, '', currentThought.filters.app);
        if (families.includes('communication')) widenedApps = ['Gmail', 'Slack', 'Messages', 'WhatsApp', 'Signal'];
        else if (families.includes('coding')) widenedApps = ['GitHub', 'Cursor', 'Xcode', 'VSCode'];
        else if (families.includes('browser')) widenedApps = ['Chrome', 'Safari', 'Arc'];
      }

      if (widenedRange || widenedApps) {
        const widenedThought = {
          ...currentThought,
          filters: { ...(currentThought.filters || {}), date_range: widenedRange || currentThought.filters?.date_range, app: widenedApps || currentThought.filters?.app },
          applied_date_range: widenedRange || currentThought.applied_date_range,
          widened_date_range: widenedRange,
          date_filter_status: 'widened',
          fallback_policy: { ...(currentThought.fallback_policy || { mode: 'widen_once' }), attempted: true, widened: true }
        };
        retrieval = await executeParallelRetrieval(retrievalQuery, widenedThought, {
          mode: 'chat',
          app: widenedApps || options?.app,
          date_range: widenedRange || currentThought.applied_date_range,
          source_types: options?.source_types,
          retrieval_thought: widenedThought
        }, onStep);
      }
    }

    emitStage('ranking', 'completed', {
      label: 'Phase 2: Node expansion + rerank',
      detail: `Packed ${retrieval.evidence_count || 0} evidence items from primary nodes, support nodes, and downward evidence.`,
      counts: retrieval.packed_context_stats || {
        evidence: Number(retrieval.evidence_count || 0)
      },
      preview_items: (retrieval.evidence || []).slice(0, 3).map((item) => item.text || item.id)
    });

    if (summaryContext) {
      retrieval.contextText = `[Daily Summary Snapshots]\n${summaryContext}\n\n${retrieval.contextText || ''}`.trim();
    }

    const webAssessment = assessWebSearchNecessity(query, currentThought, retrieval);
    const shouldSearchWeb = webAssessment.shouldSearchWeb;
    
    if (shouldSearchWeb) {
      const webSearchQuery = (judgment.suggested_queries?.[0]) || (currentThought?.semantic_queries?.[0]) || query;
      emitStage('web_search', 'started', {
        label: 'Web search',
        detail: `${webAssessment.reason} Searching the web using: ${webSearchQuery}`
      });
      webResults = await searchFreeWeb(webSearchQuery, 4);
      emitStage('web_search', 'completed', {
        label: 'Web search',
        detail: webResults.length ? `Retrieved ${webResults.length} public web results.` : 'No web results were returned.',
        counts: { web_results: webResults.length },
        preview_items: webResults.slice(0, 3).map((item) => item.title || item.url)
      });
    } else {
      emitStage('web_search', 'skipped', {
        label: 'Web search',
        detail: webAssessment.reason
      });
    }

    drilldownEvidence = (retrieval.drilldown_refs || []).length
      ? await fetchDrilldownEvidence(retrieval.drilldown_refs || [])
      : [];

    // 4. Judge Stage
    emitStage('judging', 'started', { label: 'Evidence test', detail: 'Checking whether the current memory and web evidence supports the answer.' });
    judgment = await runJudgeStage({ query: retrievalQuery, plan, evidence: [...(retrieval.evidence || []), ...webResults], apiKey });
    emitStage('judging', 'completed', {
      label: 'Evidence test',
      detail: judgment.reason,
      status: judgment.sufficient ? 'completed' : 'retry'
    });

    if (judgment.sufficient || !apiKey) break;
  }

  // Final metadata updates for retrieval object
  retrieval.stage_trace = stageTrace;
  retrieval.web_search_used = webResults.length > 0;
  retrieval.web_results = webResults;
  retrieval.web_sources = webResults.map(r => r.url).filter(Boolean);
  retrieval.plan = plan;
  retrieval.judgment = judgment;
  retrieval.summary_hits = summaryHits;
  retrieval.summary_context = summaryContext;

  const thinkingTrace = await buildThinkingTrace({ query, retrieval, drilldownEvidence });
  const standingNotes = String(options?.standing_notes || options?.core_memory || '').trim();

  // 5. Synthesizer Stage (with Reflector loop)
  let content = '';
  let reflection = { approved: false };
  let synthIteration = 0;
  const maxSynthIterations = 2;

  if (!apiKey) {
    emitStage('synthesis', 'started', { label: 'Synthesis' });
    content = buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
    reflection.approved = true;
  } else {
    while (!reflection.approved && synthIteration < maxSynthIterations) {
      synthIteration++;
      emitStage('synthesis', 'started', { label: synthIteration > 1 ? `Answer drafting (Attempt ${synthIteration})` : 'Answer drafting', detail: 'Reasoning over the packed context bundle to draft the answer.' });
      content = await runSynthesizerStage({
        query,
        retrieval,
        chatHistory,
        standingNotes,
        drilldownEvidence,
        webResults,
        apiKey,
        reflectorFeedback: synthIteration > 1 ? reflection : null
      });

      // 6. Reflector Stage (with confidence gating)
      emitStage('reflecting', 'started', { label: 'Critique', detail: 'Reviewing the draft for completeness, accuracy, and hallucination risk.' });
      reflection = await runReflectorStage({ query, evidence: [...(retrieval.evidence || []), ...webResults], answer: content, apiKey, confidenceScore: judgment?.confidence_score });
      retrieval.reflection = reflection;
      emitStage('reflecting', 'completed', {
        label: 'Reflecting',
        detail: reflection.reason,
        status: reflection.approved ? 'completed' : 'retry'
      });

      if (reflection.approved) break;
    }
  }

  emitStage('synthesis', 'completed', {
    label: 'Synthesis',
    detail: webResults.length ? 'Generated the answer from memory plus web support.' : 'Generated the answer from memory context.',
    counts: {
      evidence: Number(retrieval.evidence_count || 0),
      web_results: Number(webResults.length || 0)
    }
  });

  // Update thinkingTrace again to include reflection if it happened
  thinkingTrace.reflector = reflection;

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
      metadata: { source: 'chat_engine', updated_from_chat: true },
      anchorAt: new Date().toISOString(),
      anchorDate: new Date().toISOString().slice(0, 10)
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

  // New action tags
  const uiBlocks = [];

  const contactCreateMatch = content.match(/<contact_create>([\s\S]*?)<\/contact_create>/i);
  if (contactCreateMatch && contactCreateMatch[1]) {
    try {
      const input = JSON.parse(contactCreateMatch[1].trim());
      content = content.replace(/<contact_create>[\s\S]*?<\/contact_create>/i, '').trim();
      const toolResult = await dispatchTool({ tool: 'contact_create', input }).catch(() => null);
      if (toolResult?.status === 'success') {
        uiBlocks.push({
          type: 'info',
          title: `Contact created: ${input.name}`,
          body: [input.email, input.phone, input.notes].filter(Boolean).join(' · ') || 'No additional details.',
          actions: [{ label: 'View contact', action: 'view_contact', data: { node_id: toolResult.output?.node_id, name: input.name } }]
        });
      }
    } catch (_) {}
  }

  const contactUpdateMatch = content.match(/<contact_update>([\s\S]*?)<\/contact_update>/i);
  if (contactUpdateMatch && contactUpdateMatch[1]) {
    try {
      const input = JSON.parse(contactUpdateMatch[1].trim());
      content = content.replace(/<contact_update>[\s\S]*?<\/contact_update>/i, '').trim();
      const toolResult = await dispatchTool({ tool: 'contact_update', input }).catch(() => null);
      if (toolResult?.status === 'success') {
        uiBlocks.push({
          type: 'info',
          title: `Contact updated: ${input.name}`,
          body: `Updated: ${(toolResult.output?.updated_fields || []).join(', ')}`,
          actions: [{ label: 'View contact', action: 'view_contact', data: { node_id: toolResult.output?.node_id, name: input.name } }]
        });
      }
    } catch (_) {}
  }

  const memCreateMatch = content.match(/<memory_create>([\s\S]*?)<\/memory_create>/i);
  if (memCreateMatch && memCreateMatch[1]) {
    try {
      const input = JSON.parse(memCreateMatch[1].trim());
      content = content.replace(/<memory_create>[\s\S]*?<\/memory_create>/i, '').trim();
      const toolResult = await dispatchTool({ tool: 'memory_create', input }).catch(() => null);
      if (toolResult?.status === 'success') {
        uiBlocks.push({
          type: 'info',
          title: `Saved to memory: ${input.title}`,
          body: input.summary,
          actions: []
        });
      }
    } catch (_) {}
  }

  const autoCreateMatch = content.match(/<automation_create>([\s\S]*?)<\/automation_create>/i);
  if (autoCreateMatch && autoCreateMatch[1]) {
    try {
      const input = JSON.parse(autoCreateMatch[1].trim());
      content = content.replace(/<automation_create>[\s\S]*?<\/automation_create>/i, '').trim();
      const toolResult = await dispatchTool({ tool: 'automation_create', input }).catch(() => null);
      if (toolResult?.status === 'success') {
        const intervalLabel = input.interval_minutes >= 60
          ? `every ${Math.round(input.interval_minutes / 60)}h`
          : `every ${input.interval_minutes}m`;
        uiBlocks.push({
          type: 'info',
          title: `Automation scheduled: ${input.name}`,
          body: `Runs ${intervalLabel} — "${input.prompt}"`,
          actions: [{ label: 'View automations', action: 'view_automations', data: {} }]
        });
      }
    } catch (_) {}
  }

  // Explicit ui_card tags (LLM-authored interactive cards)
  const uiCardRegex = /<ui_card>([\s\S]*?)<\/ui_card>/gi;
  let uiCardMatch;
  while ((uiCardMatch = uiCardRegex.exec(content)) !== null) {
    try {
      const card = JSON.parse(uiCardMatch[1].trim());
      uiBlocks.push(card);
    } catch (_) {}
  }
  content = content.replace(/<ui_card>[\s\S]*?<\/ui_card>/gi, '').trim();

  return {
    content,
    ui_blocks: uiBlocks,
    thinking_trace: thinkingTrace,
    retrieval: {
      ...retrieval,
      stage_trace: stageTrace,
      mode: 'memory-graph',
      usedSources: [...(thinkingTrace.data_sources || []), ...(retrieval.web_search_used ? ['Web'] : [])],
      query_variants: retrieval?.generated_queries?.semantic || retrieval?.retrieval_plan?.semantic_queries || [],
      message_query_variants: retrieval?.generated_queries?.messages || retrieval?.retrieval_plan?.message_queries || [],
      retrieval_thought: retrieval?.retrieval_plan || null,
      provider: apiKey ? 'deepseek' : 'local',
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

function isCreditSaverMode() {
  const envValue = process.env.CREDIT_SAVER_MODE || process.env.DEEPSEEK_CREDIT_SAVER || 'true';
  return String(envValue).toLowerCase() !== 'false';
}

function isSimpleQuery(query) {
  const simple = /^\s*(recent|deadline|when|who|what is|quick|show|list|any messages?|unread|my tasks?|what's next)\b/i;
  const complex = /\b(relationship|pattern|explain|why|how|analyze|over the last|long-term|habit|recurring)\b/i;
  return simple.test(query) && !complex.test(query);
}

function heuristicJudge(evidence) {
  const evidenceCount = (evidence || []).length;
  const topScores = evidence.slice(0, 5).map(e => Number(e.score || 0.5));
  const avgTopScore = topScores.length ? topScores.reduce((a, b) => a + b) / topScores.length : 0;
  const evidenceQuality = (evidenceCount >= 5 && avgTopScore >= 0.65) 
    ? 'strong'
    : (evidenceCount >= 3 && avgTopScore >= 0.55)
    ? 'moderate'
    : 'weak';
  const sufficient = evidenceQuality !== 'weak' || evidenceCount >= 8;
  const confidenceScore = Math.min(0.99, Math.max(0.3, 
    (evidenceCount / 10) * 0.5 + (avgTopScore * 0.5)
  ));
  return {
    sufficient,
    confidence_score: confidenceScore,
    reason: `[Heuristic] ${evidenceQuality} evidence: ${evidenceCount} items, avg score ${avgTopScore.toFixed(2)}.`,
    suggested_queries: []
  };
}

async function runRouterStage({ query, options }) {
  const chatHistory = normalizeChatHistoryWindow(options?.chat_history, 10);
  const retrievalQuery = buildQueryWithChatContext(query, chatHistory);

  const baseThought = await buildRetrievalThought({
    query: retrievalQuery,
    mode: 'chat',
    dateRange: options?.date_range,
    app: options?.app,
    economy: options?.economy || isCreditSaverMode()
  });

  return {
    baseThought,
    retrievalQuery,
    chatHistory
  };
}

async function runPlannerStage({ query, routerOutput, apiKey }) {
  if (!apiKey) return null;

  if (isSimpleQuery(query)) {
    return {
      reasoning_plan: ['Using heuristic expansion for simple query.'],
      refined_queries: [],
      evidence_criteria: 'Heuristic-based (simple query optimization)'
    };
  }

  const prompt = `[System]
Retrieval planner. Goal: Take query + router strategy, produce execution plan.
[Query]
${query}
[Router]
${JSON.stringify(routerOutput)}
[Instruction]
Return JSON: {"reasoning_plan": ["step1",...], "refined_queries": ["q1",...], "evidence_criteria": "string"}`;

  try {
    const plan = await callLLM(prompt, apiKey, 0.22, { maxTokens: 600, task: 'routing' });
    return plan;
  } catch (e) {
    console.error('[Planner] Stage failed:', e.message);
    return null;
  }
}

async function runJudgeStage({ query, plan, evidence, apiKey }) {
  if (!apiKey || !evidence?.length) return { sufficient: true, reason: 'No API key or no evidence for judging.' };

  const heuristicResult = heuristicJudge(evidence);
  if (isCreditSaverMode() || heuristicResult.confidence_score > 0.72) {
    return heuristicResult;
  }

  const evidenceSnippet = (evidence || []).slice(0, 15).map(e => `[${e.layer || e.type}] ${String(e.text || e.title || '').slice(0, 250)}`).join('\n');
  const prompt = `[System]
Quick sufficiency check. Given evidence, is answer supported?
[Query]
${query}
[Evidence (${evidence.length} items)]
${evidenceSnippet}
[Instruction]
Return JSON: {"sufficient": bool, "confidence_score": 0.5-1.0, "reason": "string"}`;

  try {
    const judgment = await callLLM(prompt, apiKey, 0.1, { maxTokens: 400, task: 'routing' });
    return judgment || heuristicResult;
  } catch (e) {
    return heuristicResult;
  }
}

async function runReflectorStage({ query, evidence, answer, apiKey, confidenceScore = 0.5 }) {
  if (!apiKey) return { approved: true, reason: 'No API key for reflection.' };

  if (Number(confidenceScore || 0.5) > 0.85) {
    return { 
      approved: true, 
      reason: 'Confidence gate passed (>0.85). Skipping reflector.' 
    };
  }

  const answerLength = String(answer || '').length;
  if (answerLength < 50) {
    return { 
      approved: false, 
      reason: 'Answer too short.',
      critique: 'Response incomplete.',
      suggestions: 'Expand with more detail.'
    };
  }

  if (isCreditSaverMode() || answerLength < 500) {
    return {
      approved: true,
      reason: 'Heuristic checks passed.'
    };
  }

  const evidenceSnippet = (evidence || []).slice(0, 12).map(e => `[${e.layer || e.type}] ${String(e.text || e.title || '').slice(0, 200)}`).join('\n');
  const prompt = `[System]
Check draft for hallucinations and tone.
[Query]
${query}
[Evidence]
${evidenceSnippet}
[Draft (${answerLength} chars)]
${answer.slice(0, 1000)}
[Instruction]
Return JSON: {"approved": bool, "critique": "string", "reason": "string"}`;

  try {
    const reflection = await callLLM(prompt, apiKey, 0.1, { maxTokens: 500, task: 'routing' });
    return reflection || { approved: true, reason: 'LLM reflection parse failed, assuming approved.' };
  } catch (e) {
    return { approved: true, reason: 'Reflector fallback.' };
  }
}

async function runSynthesizerStage({ query, retrieval, chatHistory, standingNotes, drilldownEvidence, webResults, apiKey, reflectorFeedback = null }) {
  const budget = retrieval?.retrieval_plan?.context_budget_tokens || 2000;
  let usedTokens = 0;
  const contextLines = [];
  const seenText = new Set();

  const addLine = (line) => {
    const textKey = line.trim();
    if (!textKey || seenText.has(textKey)) return;
    const tokens = estimateTokensHeuristic(line);
    if (usedTokens + tokens > budget) return;
    contextLines.push(line);
    usedTokens += tokens;
    seenText.add(textKey);
  };

  // 1. Core context from retrieval (already somewhat deduplicated)
  if (retrieval.contextText) {
    retrieval.contextText.split('\n').forEach(addLine);
  }

  // 2. Priority evidence
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  [...evidence]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, 15)
    .forEach(item => {
      addLine(`- [${item.layer || 'memory'}] ${String(item.text || item.title || '').replace(/\s+/g, ' ').trim().slice(0, 500)}`);
    });

  // 3. Drilldown / Raw evidence
  (drilldownEvidence || []).slice(0, 10).forEach(row => {
    addLine(`- [raw:${row.source_type || 'event'}] ${String(row.text || row.title || '').replace(/\s+/g, ' ').trim().slice(0, 1000)}`);
  });

  // 4. Web Results
  (webResults || []).slice(0, 5).forEach(item => {
    addLine(`- [web] ${item.title}: ${item.snippet} (${item.url})`);
  });

  const prompt = `[System]
Weave assistant. Conversational, grounded, direct, concise. No invented facts.

[Available Actions]
You can take actions by embedding XML tags anywhere in your response. They are processed silently and removed before display.

Create a contact (use when user asks to add/create a person):
<contact_create>{"name":"Full Name","email":"email@example.com","phone":"+1234567890","notes":"optional notes"}</contact_create>

Update a contact's profile (use when user asks to add info to an existing contact):
<contact_update>{"name":"Person Name","updates":{"email":"new@email.com","phone":"...","notes":"..."}}</contact_update>

Save something to memory (use when user explicitly asks to remember something):
<memory_create>{"layer":"semantic","subtype":"fact","title":"Short title","summary":"What to remember"}</memory_create>

Create a scheduled automation (use when user asks to run something every X minutes/hours):
<automation_create>{"name":"Automation Name","description":"What it does","prompt":"The prompt to run on schedule","interval_minutes":30}</automation_create>

Show an interactive card to the user (use to confirm actions or offer follow-up options):
<ui_card>{"type":"info","title":"Card Title","body":"Card description","actions":[{"label":"Button Label","action":"action_id","data":{}}]}</ui_card>

[Grounded Context]
${contextLines.join('\n') || 'None'}

[Conversation History]
${formatChatHistoryForPrompt(chatHistory)}

[Standing Notes]
${standingNotes || 'None'}

${reflectorFeedback ? `[Reflector Feedback]\nRejected for: ${reflectorFeedback.critique}\nSuggestion: ${reflectorFeedback.suggestions}\nFix in the final response.` : ''}

[User question]
${query}`;

  try {
    const content = await callLLM(prompt, apiKey, 0.22, { task: 'synthesis', maxTokens: 1200 });
    return content || "I couldn't produce an answer from the current memory context.";
  } catch (llmError) {
    return buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
  }
}

module.exports = {
  answerChatQuery,
  buildThinkingTrace
};
