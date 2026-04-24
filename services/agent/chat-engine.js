const db = require('../db');
const { ingestRawEvent } = require('../ingestion');
const { expandAppScopeValues } = require('../app-scope-catalog');
const { buildRawEvidenceText } = require('../raw-evidence-text');
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

function normalizeAssistantContent(raw, fallback = "I couldn't produce an answer from the current memory context.") {
  if (typeof raw === 'string') {
    const text = raw.trim();
    return text || fallback;
  }
  if (Array.isArray(raw)) {
    const joined = raw
      .map((item) => (typeof item === 'string' ? item : (item?.text || item?.content || item?.answer || '')))
      .filter(Boolean)
      .join('\n')
      .trim();
    return joined || fallback;
  }
  if (raw && typeof raw === 'object') {
    const candidate = raw.content || raw.answer || raw.response || raw.text || raw.message || raw.output || null;
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
    if (Array.isArray(candidate)) {
      const joined = candidate.map((item) => String(item || '')).join('\n').trim();
      if (joined) return joined;
    }
    try {
      const serialized = JSON.stringify(raw, null, 2).trim();
      return serialized || fallback;
    } catch (_) {
      return fallback;
    }
  }
  return fallback;
}

function sanitizeAssistantOutput(raw = '') {
  const text = String(raw || '');
  return text
    .replace(/<memory_correction>[\s\S]*?<\/memory_correction>/gi, '')
    .replace(/<memory_search>[\s\S]*?<\/memory_search>/gi, '')
    .replace(/<memory_drilldown>[\s\S]*?<\/memory_drilldown>/gi, '')
    .replace(/<memory_update>[\s\S]*?<\/memory_update>/gi, '')
    .replace(/<memory_link>[\s\S]*?<\/memory_link>/gi, '')
    .replace(/<contact_create>[\s\S]*?<\/contact_create>/gi, '')
    .replace(/<contact_update>[\s\S]*?<\/contact_update>/gi, '')
    .replace(/<memory_create>[\s\S]*?<\/memory_create>/gi, '')
    .replace(/<automation_create>[\s\S]*?<\/automation_create>/gi, '')
    .replace(/<action_create>[\s\S]*?<\/action_create>/gi, '')
    .replace(/<ui_card>[\s\S]*?<\/ui_card>/gi, '')
    .trim();
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
     LIMIT 7`
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

async function fetchRecentChatHistory(sessionId = null, limit = 8) {
  const params = [];
  const where = [];
  if (sessionId) {
    where.push('session_id = ?');
    params.push(sessionId);
  }
  params.push(Math.max(2, Math.min(12, Number(limit || 8))));
  const rows = await db.allQuery(
    `SELECT role, content, ts, created_at
     FROM chat_messages
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY COALESCE(ts, strftime('%s', created_at) * 1000) DESC
     LIMIT ?`,
    params
  ).catch(() => []);
  return (rows || [])
    .reverse()
    .map((row) => ({
      role: row.role === 'assistant' ? 'assistant' : 'user',
      content: String(row.content || '').trim().slice(0, 900),
      ts: row.ts || row.created_at || null
    }))
    .filter((item) => item.content);
}

function isTaskCreationQuery(query = '') {
  const lower = String(query || '').toLowerCase();
  return /\b(my|me|today|now|current|top|priority|priorities)\b/.test(lower)
    && /\b(to-?dos?|todos?|tasks?|next actions?|what'?s next|priorities)\b/.test(lower);
}

function suggestionToEvidence(item = {}, index = 0) {
  const meta = item.metadata && typeof item.metadata === 'object' ? item.metadata : {};
  const reason = item.reason || meta.reason || item.body || item.description || '';
  const action = item.primary_action?.label || meta.primary_action?.label || item.recommended_action || '';
  const plan = Array.isArray(item.plan) ? item.plan : (Array.isArray(meta.plan) ? meta.plan : []);
  const trace = Array.isArray(item.epistemic_trace) ? item.epistemic_trace : (Array.isArray(meta.epistemic_trace) ? meta.epistemic_trace : []);
  const text = [
    `${index + 1}. ${item.title || meta.title || 'Untitled action'}`,
    reason ? `Why: ${reason}` : '',
    action ? `Action: ${action}` : '',
    plan.length ? `Plan: ${plan.slice(0, 3).join(' -> ')}` : '',
    trace.length ? `Evidence: ${trace.slice(0, 2).map((t) => t.text || t.source || t.node_id).filter(Boolean).join(' | ')}` : ''
  ].filter(Boolean).join('\n');
  return {
    id: item.id || `suggestion_${index}`,
    layer: 'suggestion',
    type: item.type || meta.type || 'next_action',
    title: item.title || meta.title || '',
    text,
    score: Number(item.score || item.confidence || 0.7),
    useful_score: Number(item.score || item.confidence || 0.7) + 0.25,
    reason: 'actionable_top_todo',
    timestamp: item.created_at || item.createdAt || null
  };
}

async function buildActionableTodoEvidence({ query = '', apiKey = null, options = {} } = {}) {
  if (!isTaskCreationQuery(query)) return [];
  let suggestions = [];
  if (apiKey) {
    try {
      const { buildRadarState } = require('./radar-engine');
      const radar = await buildRadarState({
        llmConfig: {
          provider: 'deepseek',
          apiKey,
          model: 'deepseek-chat'
        },
        manualTodos: []
      });
      suggestions = Array.isArray(radar?.allSignals) ? radar.allSignals : [];
    } catch (error) {
      console.warn('[ChatTasks] Failed to generate radar-backed task evidence:', error?.message || error);
    }
  }
  if (!Array.isArray(suggestions) || !suggestions.length) {
    suggestions = await fetchActiveSuggestions();
  }
  return (Array.isArray(suggestions) ? suggestions : [])
    .slice(0, 7)
    .map(suggestionToEvidence)
    .filter((item) => item.text);
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

function wantsActiveRawRetrieval(query = '', thought = {}) {
  const lower = String(query || '').toLowerCase();
  const summaryVsRaw = thought?.summary_vs_raw || '';
  const metadataFilters = thought?.metadata_filters || {};
  const filters = thought?.filters || {};
  const sourceScope = [
    ...(Array.isArray(filters.source_types) ? filters.source_types : (filters.source_types ? [filters.source_types] : [])),
    ...(Array.isArray(thought.source_scope) ? thought.source_scope : [])
  ].map((item) => String(item || '').toLowerCase());
  const dataSourceScope = [
    ...(Array.isArray(filters.data_source) ? filters.data_source : (filters.data_source ? [filters.data_source] : [])),
    ...(Array.isArray(metadataFilters.data_source) ? metadataFilters.data_source : (metadataFilters.data_source ? [metadataFilters.data_source] : []))
  ].map((item) => String(item || '').toLowerCase());
  const hasOperationalMetadata = Object.keys(metadataFilters).some((key) => metadataFilters[key] !== null && metadataFilters[key] !== undefined);

  return summaryVsRaw === 'raw'
    || needsRawDrilldown(query)
    || /\b(raw|recent|latest|today|yesterday|screenshot|ocr|screen|email|message|calendar|browser history|exact|verbatim)\b/.test(lower)
    || sourceScope.some((item) => /communication|screen|capture|desktop|calendar|event|email|message|browser|visit/.test(item))
    || dataSourceScope.some((item) => /raw|ocr|event|email_api|calendar_api|browser_history|screenshot/.test(item))
    || hasOperationalMetadata;
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
    const text = buildRawEvidenceText(row, metadata, { maxChars: 16000 });
    return {
      id: row.id,
      source_type: row.source_type,
      occurred_at: row.occurred_at,
      title: row.title,
      app: row.app,
      source_account: row.source_account,
      text: String(text).slice(0, 16000)
    };
  });
}

function retrievalLooksSparse(retrieval) {
  const evidenceCount = Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0);
  const seedCount = Number(retrieval?.seed_nodes?.length || 0);
  return seedCount < 2 || evidenceCount < 3;
}

function retrievalHasVectorOrRawEvidence(retrieval) {
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  return evidence.some((item) => {
    const reason = String(item?.reason || item?.match_reason || '').toLowerCase();
    const layer = String(item?.layer || item?.type || '').toLowerCase();
    return /semantic|chunk|lexical|recency|downward|episode_source_ref/.test(reason)
      || layer === 'raw'
      || layer === 'event'
      || Boolean(item?.event_id);
  });
}

function shouldUseDailySummarySupplement(retrieval, summaryContext = '', options = {}) {
  if (!summaryContext) return false;
  if (options?.force_daily_summary_context) return true;
  const evidenceCount = Number(retrieval?.evidence_count || retrieval?.evidence?.length || 0);
  const vectorSeedCount = (retrieval?.seed_results || []).filter((item) => /semantic|chunk|lexical|recency/i.test(String(item?.reason || ''))).length;
  return evidenceCount === 0 || vectorSeedCount === 0;
}

function mergePlannerQueries(baseThought = {}, plan = null) {
  const refined = Array.isArray(plan?.refined_queries) ? plan.refined_queries : [];
  if (!refined.length) return baseThought;
  const merged = Array.from(new Set([
    ...refined.map((item) => String(item || '').trim()).filter(Boolean),
    ...((baseThought.semantic_queries || []).map((item) => String(item || '').trim()).filter(Boolean))
  ])).slice(0, 15);
  return {
    ...baseThought,
    semantic_queries: merged,
    search_queries: merged,
    query_sets: {
      ...(baseThought.query_sets || {}),
      memory_queries: merged
    }
  };
}

function extractMemoryKeywords(query = '') {
  const stop = new Set([
    'the', 'and', 'for', 'that', 'with', 'from', 'this', 'have', 'what', 'when', 'where',
    'who', 'how', 'your', 'about', 'into', 'over', 'under', 'been', 'were', 'they', 'them',
    'okay', 'please', 'just', 'need', 'want', 'did', 'are', 'was', 'has', 'had', 'can', 'you'
  ]);
  return String(query || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 3 && !stop.has(t))
    .slice(0, 6);
}

async function lookupDirectMemoryFacts(query = '', limit = 8) {
  const terms = extractMemoryKeywords(query);
  if (!terms.length) return [];
  const clauses = [];
  const params = [];
  clauses.push(`COALESCE(source, '') != 'Chat'`);
  clauses.push(`COALESCE(type, source_type, '') != 'ChatMessage'`);
  for (const term of terms) {
    clauses.push(`(
      LOWER(COALESCE(title, '')) LIKE ? OR
      LOWER(COALESCE(summary, '')) LIKE ? OR
      LOWER(COALESCE(canonical_text, '')) LIKE ?
    )`);
    const like = `%${term}%`;
    params.push(like, like, like);
  }
  params.push(Math.max(1, Number(limit || 8)));

  const rows = await db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, updated_at
     FROM memory_nodes
     WHERE (${clauses.join(' OR ')})
       AND layer IN ('semantic', 'insight', 'core', 'episode')
     ORDER BY
       CASE layer
         WHEN 'semantic' THEN 0
         WHEN 'insight' THEN 1
         WHEN 'core' THEN 2
         ELSE 3
       END,
       datetime(updated_at) DESC
     LIMIT ?`,
    params
  ).catch(() => []);

  return (rows || []).map((row) => ({
    id: row.id,
    layer: row.layer || 'semantic',
    title: row.title || '',
    text: String(row.summary || row.canonical_text || row.title || '').slice(0, 1200),
    score: 0.56,
    reason: `Direct fact lookup matched ${(row.subtype || row.layer || 'memory')}`
  })).filter((item) => item.text);
}

async function lookupRelationshipEvidence(query = '', limit = 8) {
  try {
    const { searchRelationshipContext } = require('../relationship-graph');
    return await searchRelationshipContext(query, limit);
  } catch (_) {
    return [];
  }
}

function normalizeList(value) {
  return (Array.isArray(value) ? value : (value ? [value] : []))
    .map((item) => String(item || '').trim())
    .filter(Boolean);
}

async function lookupDirectRawEvents(query = '', thought = {}, limit = 8) {
  const terms = extractMemoryKeywords(query)
    .filter((term) => !['conversation', 'context', 'today', 'watching'].includes(term))
    .slice(0, 6);
  if (!terms.length) return [];

  const filters = thought?.filters || {};
  const metadataFilters = thought?.metadata_filters || {};
  const clauses = [];
  const params = [];
  clauses.push(`COALESCE(source, '') != 'Chat'`);
  clauses.push(`COALESCE(type, source_type, '') != 'ChatMessage'`);
  clauses.push(`LOWER(COALESCE(app, '')) NOT IN ('chat', 'chat_ui')`);
  clauses.push(`LOWER(COALESCE(metadata, '')) NOT LIKE '%app.chat%'`);

  const dateRange = thought?.applied_date_range || filters.date_range || thought?.date_range || null;
  if (dateRange?.start && dateRange?.end) {
    clauses.push(`COALESCE(occurred_at, timestamp) >= ? AND COALESCE(occurred_at, timestamp) <= ?`);
    params.push(dateRange.start, dateRange.end);
  }

  const apps = expandAppScopeValues(normalizeList(filters.app || thought?.app_scope || metadataFilters.app));
  if (apps.length) {
    clauses.push(`(${apps.map(() => `(LOWER(COALESCE(app, '')) LIKE ? OR (COALESCE(app, '') = '' AND LOWER(COALESCE(metadata, '')) LIKE ?))`).join(' OR ')})`);
    for (const app of apps) {
      const like = `%${app.toLowerCase()}%`;
      params.push(like, like);
    }
  }

  const sourceTypes = normalizeList(filters.source_types || thought?.source_scope || metadataFilters.source_types);
  if (sourceTypes.length) {
    const expanded = new Set(sourceTypes.map((item) => item.toLowerCase()));
    if (expanded.has('desktop')) ['screen', 'capture', 'screenshot', 'screenshot_ocr', 'raw_event'].forEach((item) => expanded.add(item));
    clauses.push(`(${Array.from(expanded).map(() => `(LOWER(COALESCE(source_type, '')) LIKE ? OR LOWER(COALESCE(type, '')) LIKE ? OR LOWER(COALESCE(metadata, '')) LIKE ?)`).join(' OR ')})`);
    params.push(...Array.from(expanded).flatMap((item) => [`%${item}%`, `%${item}%`, `%${item}%`]));
  }

  const lexicalClauses = [];
  for (const term of terms) {
    lexicalClauses.push(`(
      LOWER(COALESCE(title, '')) LIKE ? OR
      LOWER(COALESCE(text, '')) LIKE ? OR
      LOWER(COALESCE(raw_text, '')) LIKE ? OR
      LOWER(COALESCE(redacted_text, '')) LIKE ? OR
      LOWER(COALESCE(metadata, '')) LIKE ?
    )`);
    const like = `%${term.toLowerCase()}%`;
    params.push(like, like, like, like, like);
  }
  clauses.push(`(${lexicalClauses.join(' OR ')})`);

  params.push(Math.max(8, Number(limit || 8)));
  const rows = await db.allQuery(
    `SELECT id, source_type, occurred_at, timestamp, title, text, raw_text, redacted_text, app, metadata
     FROM events
     WHERE ${clauses.join(' AND ')}
     ORDER BY
       CASE
         WHEN LOWER(COALESCE(text, '') || ' ' || COALESCE(raw_text, '') || ' ' || COALESCE(metadata, '')) LIKE '%official trailer%' THEN 0
         WHEN LOWER(COALESCE(text, '') || ' ' || COALESCE(raw_text, '') || ' ' || COALESCE(metadata, '')) LIKE '%youtube%' THEN 1
         ELSE 2
       END,
       COALESCE(occurred_at, timestamp) DESC
     LIMIT ?`,
    params
  ).catch(() => []);

  return (rows || []).map((row, index) => {
    const metadata = safeJsonParse(row.metadata, {});
    const text = buildRawEvidenceText(row, metadata, { maxChars: 16000 }).trim();
    const rankHay = `${row.title || ''} ${text}`.toLowerCase();
    const exactVideoBonus = (rankHay.includes('official trailer') ? 0.18 : 0)
      + (rankHay.includes('youtube') ? 0.08 : 0)
      + (rankHay.includes('audio playing') ? 0.08 : 0)
      + (rankHay.includes('captured text') ? 0.05 : 0);
    return {
      id: row.id,
      event_id: row.id,
      layer: 'event',
      type: 'event',
      subtype: row.source_type || metadata.event_type || null,
      title: row.title || metadata.context_title || row.source_type || row.id,
      text: text.slice(0, 16000),
      app: row.app || metadata.source_app || metadata.app || null,
      timestamp: row.occurred_at || row.timestamp || metadata.occurred_at || null,
      score: Number((0.98 + exactVideoBonus - (index * 0.02)).toFixed(6)),
      useful_score: Number((1.1 + exactVideoBonus - (index * 0.02)).toFixed(6)),
      reason: 'direct_raw_event_lexical',
      source_refs: [row.id]
    };
  })
    .filter((item) => item.text)
    .sort((a, b) => (b.useful_score || 0) - (a.useful_score || 0));
}

async function executeParallelRetrieval(baseQuery, baseThought, options, onProgress = null) {
  const bundle = [
    String(baseQuery || '').trim(),
    ...((baseThought.semantic_queries || []).map((item) => String(item || '').trim())),
    ...((baseThought.message_queries || []).map((item) => String(item || '').trim()))
  ].filter(Boolean);
  const maxParallelQueries = options?.economy ? 2 : 2;
  const queries = Array.from(new Set(bundle)).slice(0, maxParallelQueries);

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
    seedLimit: Math.max(3, Math.floor(8 / Math.max(1, queries.length))),
    hopLimit: requiresDeepContext ? 3 : 2,
    recursionDepth,
    passiveOnly: options.passiveOnly || false,
    onProgress
  })));

  const successfulResults = results
    .filter((r) => r.status === 'fulfilled')
    .map((r) => r.value);

  if (successfulResults.length === 0) {
    const errors = results
      .filter((r) => r.status === 'rejected')
      .map((r) => String(r.reason?.message || r.reason || 'unknown retrieval error'))
      .slice(0, 3);

    const fallback = {
      retrieval_run_id: `fallback_${Date.now()}`,
      retrieval_plan: {
        ...(baseThought || {}),
        recursion_depth: recursionDepth,
        retrieval_error: 'all_parallel_attempts_failed'
      },
      thought_summary: ['All parallel retrieval attempts failed; using safe empty retrieval fallback.'],
      trace_summary: errors.length ? errors : ['Parallel retrieval failure with no detailed error message.'],
      evidence: [],
      evidence_count: 0,
      expanded_nodes: [],
      seed_nodes: [],
      edge_paths: [],
      drilldown_refs: [],
      lazy_source_refs: [],
      contextText: '',
      generated_queries: {
        semantic: baseThought?.semantic_queries || [],
        messages: baseThought?.message_queries || [],
        web: baseThought?.web_queries || [],
        lexical_terms: baseThought?.lexical_terms || []
      },
      query_sets: baseThought?.query_sets || {
        memory_queries: baseThought?.semantic_queries || [String(baseQuery || '').trim()].filter(Boolean),
        message_queries: baseThought?.message_queries || [],
        web_queries: baseThought?.web_queries || []
      },
      router: {
        source_mode: baseThought?.source_mode || baseThought?.strategy_mode || 'memory_only',
        router_reason: baseThought?.router_reason || 'Parallel retrieval failed; returned empty fallback retrieval.',
        time_scope: baseThought?.time_scope || null,
        summary_vs_raw: baseThought?.summary_vs_raw || 'summary'
      },
      fallback_reason: 'all_parallel_attempts_failed',
      retrieval_errors: errors
    };

    try {
      onProgress?.({
        step: 'parallel_retrieval_fallback',
        status: 'completed',
        label: 'Parallel retrieval fallback',
        detail: `All retrieval branches failed; continuing with empty fallback. ${errors[0] || ''}`.trim(),
        counts: { failed_queries: Number(queries.length || 0) },
        preview_items: errors
      });
    } catch (_) {}

    return fallback;
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
  const compatStageBuffer = [];
  const emit = (data) => {
    try {
      if (data && (data.step === 'primary_search_results' || data.step === 'iterative_expansion')) {
        const event = buildStageEvent(data.step, data.status || 'completed', data);
        stageTrace.push(event);
        compatStageBuffer.push(event);
        return;
      }
      onStep?.(data);
    } catch (_) {}
  };

  // Normalize API key: prefer explicit param, then environment, then options; treat empty string as absent
  if (!apiKey) {
    apiKey = process.env.DEEPSEEK_API_KEY || options?.apiKey || null;
  }
  const flushCompatStages = () => {
    if (!compatStageBuffer.length) return;
    while (compatStageBuffer.length) {
      onStep?.(compatStageBuffer.shift());
    }
  };

  let thinkingState = {
    stage: 'query_analysis',
    progress: 15,
    label: 'Analyzing your question'
  };

  const emitStage = (step, status, overrides = {}, surface = true) => {
    const event = {
      ...buildStageEvent(step, status, overrides),
      type: 'thinking_stage',
      stage: thinkingState.stage,
      progress: thinkingState.progress,
      label: overrides.label || thinkingState.label || undefined
    };
    stageTrace.push(event);
    if (surface) emit(event);
    return event;
  };

  const emitThinkingStage = (stage, progress, label, detail = '') => {
    thinkingState = {
      stage,
      progress,
      label,
      detail
    };
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
  emitStage('routing', 'started', { label: 'Hypothesis', detail: 'Deciding whether this query should use memory, web, or hybrid retrieval.' }, false);
  const { baseThought, retrievalQuery, activeQuery, chatHistory } = await runRouterStage({ query, options });
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
  emitThinkingStage('query_analysis', 15, 'Analyzing your question');

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
  emitStage('planning', 'started', { label: 'Planner', detail: 'Generating a concise execution plan and refining key query terms.' }, false);
  let plan = await runPlannerStage({ query: activeQuery, routerOutput: baseThought, apiKey });
  emitStage('planning', 'completed', {
    label: 'Planner',
    detail: plan ? 'Created a formal execution plan with refined queries.' : 'Using default routing plan.',
    preview_items: plan?.reasoning_plan || []
  }, false);

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
    emitThinkingStage('hybrid_retrieval', 60, 'Searching your memory');
    emitStage('memory_search', 'started', { label: iteration > 1 ? `Phase 1: Initial memory search (Attempt ${iteration})` : 'Phase 1: Initial memory search', detail: 'Applying time-first filters, searching all memory layers, and collecting top candidates.' });

    // Use refined queries if available and it's not the first pass or if they exist
    const currentThought = (iteration > 1 && judgment.suggested_queries)
      ? { ...baseThought, semantic_queries: judgment.suggested_queries }
      : mergePlannerQueries(baseThought, plan);

    retrieval = await executeParallelRetrieval(activeQuery, currentThought, {
      mode: 'chat',
      app: options?.app,
      date_range: currentThought.applied_date_range || options?.date_range,
      source_types: options?.source_types,
      metadata_filters: currentThought.metadata_filters || options?.metadata_filters || {},
      retrieval_thought: currentThought,
      passiveOnly: false
    }, emit);

    if (iteration === 1 && retrievalLooksSparse(retrieval)) {
      retrieval = await executeParallelRetrieval(activeQuery, currentThought, {
        mode: 'chat',
        app: options?.app,
        date_range: currentThought.applied_date_range || options?.date_range,
        source_types: options?.source_types,
        metadata_filters: currentThought.metadata_filters || options?.metadata_filters || {},
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
        const families = inferSurfaceFamilies(activeQuery, '', currentThought.filters.app);
        const widenedSet = new Set();
        if (families.includes('communication')) ['Gmail', 'Slack', 'Messages', 'WhatsApp', 'Signal'].forEach((item) => widenedSet.add(item));
        if (families.includes('coding')) ['GitHub', 'Cursor', 'Xcode', 'VSCode'].forEach((item) => widenedSet.add(item));
        if (families.includes('browser')) ['Chrome', 'Safari', 'Arc'].forEach((item) => widenedSet.add(item));
        widenedApps = widenedSet.size ? Array.from(widenedSet) : null;
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
        retrieval = await executeParallelRetrieval(activeQuery, widenedThought, {
          mode: 'chat',
          app: widenedApps || options?.app,
          date_range: widenedRange || currentThought.applied_date_range,
          source_types: options?.source_types,
          metadata_filters: widenedThought.metadata_filters || options?.metadata_filters || {},
          retrieval_thought: widenedThought
        }, onStep);
      }
    }

    if (wantsActiveRawRetrieval(activeQuery, currentThought)) {
      const directRawEvents = await lookupDirectRawEvents(activeQuery, currentThought, 8);
      if (directRawEvents.length) {
        const existingIds = new Set((retrieval.evidence || []).map((item) => item?.event_id || item?.id).filter(Boolean));
        const freshRawEvents = directRawEvents.filter((item) => !existingIds.has(item.event_id || item.id));
        if (freshRawEvents.length) {
          retrieval.evidence = [...freshRawEvents, ...(retrieval.evidence || [])];
          retrieval.evidence_count = Number(retrieval.evidence.length || 0);
          retrieval.drilldown_refs = Array.from(new Set([
            ...freshRawEvents.map((item) => item.event_id || item.id),
            ...(retrieval.drilldown_refs || [])
          ].filter(Boolean))).slice(0, 32);
          retrieval.packed_context_stats = {
            ...(retrieval.packed_context_stats || {}),
            packed_evidence: retrieval.evidence_count,
            direct_raw_event_hits: freshRawEvents.length
          };
          emitStage('direct_raw_event_lookup', 'completed', {
            label: 'Direct raw event lookup',
            detail: `Recovered ${freshRawEvents.length} raw events with lexical/date/app filters.`,
            counts: { direct_raw_event_hits: freshRawEvents.length },
            preview_items: freshRawEvents.slice(0, 3).map((item) => item.text || item.title || item.id)
          });
        }
      }
    }

    if (isTaskCreationQuery(activeQuery)) {
      const todoEvidence = await buildActionableTodoEvidence({ query: activeQuery, apiKey, options });
      if (todoEvidence.length) {
        const existingIds = new Set((retrieval.evidence || []).map((item) => item?.id || item?.event_id).filter(Boolean));
        const freshTodoEvidence = todoEvidence.filter((item) => !existingIds.has(item.id));
        if (freshTodoEvidence.length) {
          retrieval.evidence = [...freshTodoEvidence, ...(retrieval.evidence || [])].slice(0, 18);
          retrieval.evidence_count = retrieval.evidence.length;
          retrieval.contextText = `${retrieval.contextText || ''}\n\n[Actionable Top Todos]\n${freshTodoEvidence.map((item) => `- ${item.text.replace(/\n/g, ' | ')}`).join('\n')}`.trim();
          retrieval.packed_context_stats = {
            ...(retrieval.packed_context_stats || {}),
            packed_evidence: retrieval.evidence_count,
            actionable_todo_hits: freshTodoEvidence.length
          };
          retrieval.generated_actionable_todos = freshTodoEvidence;
          emitStage('actionable_todo_generation', 'completed', {
            label: 'Actionable todo generation',
            detail: `Generated ${freshTodoEvidence.length} actionable todo candidates from memory/suggestions.`,
            counts: { actionable_todos: freshTodoEvidence.length },
            preview_items: freshTodoEvidence.slice(0, 3).map((item) => item.title || item.text)
          });
        }
      }
    }

    const relationshipEvidence = await lookupRelationshipEvidence(activeQuery, 8);
    if (relationshipEvidence.length) {
      const existingIds = new Set((retrieval.evidence || []).map((item) => item?.id || item?.relationship_contact_id).filter(Boolean));
      const freshRelationshipEvidence = relationshipEvidence.filter((item) => !existingIds.has(item.id));
      if (freshRelationshipEvidence.length) {
        retrieval.evidence = [...freshRelationshipEvidence, ...(retrieval.evidence || [])].slice(0, 18);
        retrieval.evidence_count = retrieval.evidence.length;
        retrieval.contextText = `${retrieval.contextText || ''}\n\n[Relationship Graph]\n${freshRelationshipEvidence.map((item) => `- ${item.text}`).join('\n')}`.trim();
        retrieval.packed_context_stats = {
          ...(retrieval.packed_context_stats || {}),
          packed_evidence: retrieval.evidence_count,
          relationship_contact_hits: freshRelationshipEvidence.length
        };
        emitStage('relationship_graph_lookup', 'completed', {
          label: 'Relationship graph lookup',
          detail: `Recovered ${freshRelationshipEvidence.length} contact records from relationship memory.`,
          counts: { relationship_contacts: freshRelationshipEvidence.length },
          preview_items: freshRelationshipEvidence.slice(0, 3).map((item) => item.title || item.id)
        });
      }
    }

    emitStage('memory_search', 'completed', { label: 'Phase 1: Initial memory search', detail: 'Completed memory retrieval pass.' });
    flushCompatStages();

    emitStage('seed_selection', 'completed', {
      label: 'Seed selection',
      detail: `Selected ${retrieval.seed_results?.length || retrieval.seed_nodes?.length || 0} candidate seeds from memory retrieval.`,
      counts: {
        seed_results: Number(retrieval.seed_results?.length || retrieval.seed_nodes?.length || 0)
      }
    });

    emitStage('edge_expansion', 'completed', {
      label: 'Edge expansion',
      detail: `Expanded ${retrieval.graph_expansion_results?.length || retrieval.expanded_nodes?.length || 0} graph nodes from the selected seeds.`,
      counts: {
        graph_nodes: Number(retrieval.graph_expansion_results?.length || retrieval.expanded_nodes?.length || 0)
      }
    });

    emitStage('ranking', 'completed', {
      label: 'Phase 2: Node expansion + rerank',
      detail: `Packed ${retrieval.evidence_count || 0} evidence items from primary nodes, support nodes, and downward evidence.`,
      counts: retrieval.packed_context_stats || {
        evidence: Number(retrieval.evidence_count || 0)
      },
      preview_items: (retrieval.evidence || []).slice(0, 3).map((item) => item.text || item.id)
    });
    emitThinkingStage('reranking', 80, 'Organizing results');

    if (shouldUseDailySummarySupplement(retrieval, summaryContext, options)) {
      retrieval.contextText = `${retrieval.contextText || ''}\n\n[Supplemental Daily Summary Snapshots]\n${summaryContext}`.trim();
      retrieval.summary_context_used = true;
    } else {
      retrieval.summary_context_used = false;
    }

    if (retrievalLooksSparse(retrieval) && !wantsActiveRawRetrieval(activeQuery, currentThought)) {
      const directFacts = await lookupDirectMemoryFacts(activeQuery, 8);
      if (directFacts.length) {
        const existingIds = new Set((retrieval.evidence || []).map((item) => item?.id).filter(Boolean));
        const mergedFacts = directFacts.filter((item) => !existingIds.has(item.id));
        if (mergedFacts.length) {
          retrieval.evidence = [...(retrieval.evidence || []), ...mergedFacts];
          retrieval.evidence_count = Number(retrieval.evidence.length || 0);
          const lines = mergedFacts.slice(0, 8).map((item) => `- [${item.layer}] ${item.text}`);
          retrieval.contextText = `${retrieval.contextText || ''}\n\n[Direct Memory Facts]\n${lines.join('\n')}`.trim();
          emitStage('direct_memory_fallback', 'completed', {
            label: 'Direct memory fact lookup',
            detail: `Recovered ${mergedFacts.length} semantic/core facts from memory_nodes lexical matching.`,
            counts: { direct_fact_hits: mergedFacts.length },
            preview_items: mergedFacts.slice(0, 3).map((item) => item.title || item.id)
          });
        }
      }
    }

    const webAssessment = assessWebSearchNecessity(query, currentThought, retrieval);
    const shouldSearchWeb = webAssessment.shouldSearchWeb;
    const webSearchQuery = (judgment.suggested_queries?.[0]) || (currentThought?.semantic_queries?.[0]) || query;
    emitStage('web_search', 'started', {
      label: 'Web search',
      detail: `${webAssessment.reason} Searching the web using: ${webSearchQuery}`
    });
    if (shouldSearchWeb) {
      webResults = await searchFreeWeb(webSearchQuery, 4);
      emitStage('web_search', 'completed', {
        label: 'Web search',
        detail: webResults.length ? `Retrieved ${webResults.length} public web results.` : 'No web results were returned.',
        counts: { web_results: webResults.length },
        preview_items: webResults.slice(0, 3).map((item) => item.title || item.url)
      });
    } else {
      emitStage('web_search', 'completed', {
        label: 'Web search',
        detail: webAssessment.reason,
        status: 'completed'
      });
    }

    drilldownEvidence = (retrieval.drilldown_refs || []).length
      ? await fetchDrilldownEvidence(retrieval.drilldown_refs || [])
      : [];

    // 4. Judge Stage
    emitStage('judging', 'started', { label: 'Evidence test', detail: 'Checking whether the current memory and web evidence supports the answer.' }, false);
    judgment = await runJudgeStage({
      query: activeQuery,
      plan,
      retrieval,
      evidence: [...(retrieval.evidence || []), ...webResults],
      apiKey
    });
    emitStage('judging', 'completed', {
      label: 'Evidence test',
      detail: judgment.reason,
      status: judgment.sufficient ? 'completed' : 'retry'
    }, false);

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
    emitThinkingStage('synthesis', 95, 'Generating response');
    emitStage('synthesis', 'started', { label: 'Synthesis' });
    content = buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
    reflection.approved = true;
  } else {
    while (!reflection.approved && synthIteration < maxSynthIterations) {
      synthIteration++;
      emitThinkingStage('synthesis', 95, 'Generating response');
      emitStage('synthesis', 'started', { label: synthIteration > 1 ? `Answer drafting (Attempt ${synthIteration})` : 'Answer drafting', detail: 'Reasoning over the packed context bundle to draft the answer.' });
      content = normalizeAssistantContent(await runSynthesizerStage({
        query,
        retrieval,
        chatHistory,
        standingNotes,
        drilldownEvidence,
        webResults,
        apiKey,
        reflectorFeedback: synthIteration > 1 ? reflection : null
      }));

      // 6. Reflector Stage (with confidence gating)
      emitStage('reflecting', 'started', { label: 'Critique', detail: 'Reviewing the draft for completeness, accuracy, and hallucination risk.' }, false);
      reflection = await runReflectorStage({ query, evidence: [...(retrieval.evidence || []), ...webResults], answer: content, apiKey, confidenceScore: judgment?.confidence_score });
      retrieval.reflection = reflection;
      emitStage('reflecting', 'completed', {
        label: 'Reflecting',
        detail: reflection.reason,
        status: reflection.approved ? 'completed' : 'retry'
      }, false);

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
  content = sanitizeAssistantOutput(normalizeAssistantContent(content));

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
  emitThinkingStage('complete', 100, 'Done');

  const uiBlocks = [];
  content = sanitizeAssistantOutput(content);

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

function lexicalOverlapRatio(query = '', evidence = []) {
  const stop = new Set(['what', 'when', 'where', 'which', 'with', 'from', 'this', 'that', 'have', 'doing', 'memory', 'context', 'according', 'today']);
  const tokens = String(query || '')
    .toLowerCase()
    .replace(/[^a-z0-9._/-\s]/g, ' ')
    .split(/\s+/)
    .map((item) => item.trim())
    .filter((item) => item.length >= 4 && !stop.has(item))
    .slice(0, 12);
  if (!tokens.length) return 1;
  const hay = (evidence || []).map((item) => `${item?.text || ''} ${item?.title || ''} ${item?.reason || ''}`).join(' ').toLowerCase();
  const hits = tokens.filter((token) => hay.includes(token)).length;
  return hits / tokens.length;
}

function heuristicJudge(evidence, query = '', retrieval = null) {
  const evidenceCount = (evidence || []).length;
  const topScores = evidence.slice(0, 5).map(e => Number(e.score || 0.5));
  const avgTopScore = topScores.length ? topScores.reduce((a, b) => a + b) / topScores.length : 0;
  const overlap = lexicalOverlapRatio(query, evidence);
  const hasVectorOrRaw = retrieval ? retrievalHasVectorOrRawEvidence(retrieval) : (evidence || []).some((item) => {
    const reason = String(item?.reason || '').toLowerCase();
    const layer = String(item?.layer || item?.type || '').toLowerCase();
    return /semantic|chunk|lexical|recency|downward/.test(reason) || layer === 'raw' || layer === 'event' || Boolean(item?.event_id);
  });
  const evidenceQuality = (evidenceCount >= 5 && avgTopScore >= 0.65)
    ? 'strong'
    : (evidenceCount >= 3 && avgTopScore >= 0.55)
    ? 'moderate'
    : 'weak';
  const sufficient = hasVectorOrRaw && overlap >= 0.18 && (evidenceQuality !== 'weak' || evidenceCount >= 8);
  const confidenceScore = Math.min(0.99, Math.max(0.3,
    (evidenceCount / 10) * 0.38 + (avgTopScore * 0.42) + (overlap * 0.2)
  ));
  return {
    sufficient,
    confidence_score: confidenceScore,
    reason: `[Heuristic] ${evidenceQuality} evidence: ${evidenceCount} items, avg score ${avgTopScore.toFixed(2)}, query overlap ${overlap.toFixed(2)}, vector/raw support ${hasVectorOrRaw ? 'yes' : 'no'}.`,
    suggested_queries: []
  };
}

async function runRouterStage({ query, options }) {
  let chatHistory = normalizeChatHistoryWindow(options?.chat_history, 10);
  if (!chatHistory.length) {
    chatHistory = await fetchRecentChatHistory(options?.chat_session_id, 8);
  }
  const retrievalQuery = buildQueryWithChatContext(query, chatHistory);

  const baseThought = await buildRetrievalThought({
    query,
    mode: 'chat',
    dateRange: options?.date_range,
    app: options?.app,
    economy: options?.economy || isCreditSaverMode()
  });

  return {
    baseThought,
    retrievalQuery,
    activeQuery: String(query || '').trim(),
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

async function runJudgeStage({ query, plan, retrieval = null, evidence, apiKey }) {
  if (!evidence?.length) return { sufficient: false, reason: 'No evidence for judging.' };

  const heuristicResult = heuristicJudge(evidence, query, retrieval);
  if (!apiKey) return heuristicResult;
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

function compactEvidenceLine(text = '', maxChars = 420) {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return '';
  const lower = cleaned.toLowerCase();
  if (lower.startsWith('- [raw:') || lower.includes('full ocr / raw capture:')) {
    return cleaned.slice(0, Math.max(1200, maxChars));
  }
  if (/^(full ocr:|content:|captured text:|window:|app:)/.test(lower) && cleaned.length > 260) {
    return cleaned.slice(0, Math.min(220, maxChars));
  }
  return cleaned.slice(0, Math.max(80, maxChars));
}

function classifyAnswerMode(query = '') {
  const lower = String(query || '').toLowerCase();
  const asksExternalWrite = /\b(save|add|create|make|schedule|send|email|message|text|post|publish|delete|update|move|rename)\b/.test(lower)
    && /\b(task|todo|reminder|calendar|event|contact|email|message|file|note|database|db|automation|card)\b/.test(lower);
  const asksGroundedGeneration = /\b(create|make|generate|draft|write|compose|plan|prioriti[sz]e|suggest|recommend|summari[sz]e|turn .* into|build|outline|organize)\b/.test(lower)
    || /\b(what should i do|what'?s next|next actions?|top to-?dos?|top todos?|tasks? are my top|my priorities|action plan|game plan)\b/.test(lower);
  const asksExactLookup = /\b(exact|verbatim|quote|which|who|when|where|what did i|what was i|did i|have i|according to memory|recall|remember|watched|talked to)\b/.test(lower);

  if (asksExternalWrite) return 'external_action_request';
  if (asksGroundedGeneration) return 'grounded_generation';
  if (asksExactLookup) return 'memory_lookup';
  return 'balanced_answer';
}

function buildSynthesisPolicy({ query = '', evidence = [], retrieval = null }) {
  const q = String(query || '').toLowerCase();
  const budget = Number(retrieval?.retrieval_plan?.context_budget_tokens || 2000);
  const highConfidenceCount = (evidence || []).filter((item) => Number(item?.score || 0) >= 0.7).length;
  const answerMode = classifyAnswerMode(query);
  const likelyFactual = /\b(when|what|who|where|which|did|does|is|are|was|were|list|show|find|recall|remember)\b/.test(q);
  const likelyReasoning = /\b(why|how|compare|difference|pattern|insight|explain|summarize)\b/.test(q);
  const likelyGenerative = answerMode === 'grounded_generation' || answerMode === 'external_action_request';

  const maxTokens = likelyGenerative ? 1150 : (likelyFactual ? 800 : (likelyReasoning ? 1000 : 900));
  const temperature = likelyGenerative ? 0.22 : (likelyFactual ? 0.12 : (likelyReasoning ? 0.2 : 0.16));
  const contextBudget = Math.max(700, Math.min(budget, highConfidenceCount >= 5 ? budget : Math.floor(budget * 0.8)));
  const evidenceLimit = likelyGenerative ? 10 : (highConfidenceCount >= 5 ? 8 : 5);
  const rawLimit = likelyGenerative ? 7 : (likelyFactual ? 4 : 6);

  return {
    maxTokens,
    temperature,
    contextBudget,
    evidenceLimit,
    rawLimit,
    lineMaxChars: likelyGenerative ? 560 : (likelyFactual ? 360 : 460),
    answerMode,
    minWords: likelyGenerative || likelyReasoning || !likelyFactual ? 150 : 90
  };
}

async function runSynthesizerStage({ query, retrieval, chatHistory, standingNotes, drilldownEvidence, webResults, apiKey, reflectorFeedback = null }) {
  const policy = buildSynthesisPolicy({ query, evidence: retrieval?.evidence || [], retrieval });
  const budget = policy.contextBudget;
  let usedTokens = 0;
  const contextLines = [];
  const seenText = new Set();

  const addLine = (line) => {
    const compact = compactEvidenceLine(line, policy.lineMaxChars);
    const textKey = compact.trim();
    if (!textKey || seenText.has(textKey)) return;
    const tokens = estimateTokensHeuristic(compact);
    if (usedTokens + tokens > budget) return;
    contextLines.push(compact);
    usedTokens += tokens;
    seenText.add(textKey);
  };

  // 1. Priority evidence from vector/metadata retrieval. This must outrank
  // compressive summaries so chat answers are grounded in the selected nodes.
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  [...evidence]
    .sort((a, b) => Number(b.useful_score || b.score || 0) - Number(a.useful_score || a.score || 0))
    .slice(0, Math.max(policy.evidenceLimit, 8))
    .forEach(item => {
      const metadataHint = [
        item.app ? `app:${item.app}` : '',
        item.timestamp ? `time:${item.timestamp}` : '',
        item.reason ? `reason:${item.reason}` : '',
        Array.isArray(item.usefulness_reasons) && item.usefulness_reasons.length ? `useful:${item.usefulness_reasons.join(',')}` : ''
      ].filter(Boolean).join(' ');
      addLine(`- [${item.layer || 'memory'}${metadataHint ? ` ${metadataHint}` : ''}] ${String(item.text || item.title || '').replace(/\s+/g, ' ').trim().slice(0, 700)}`);
    });

  // 2. Drilldown / raw evidence for exact wording or source-backed answers.
  (drilldownEvidence || []).slice(0, policy.rawLimit).forEach(row => {
    addLine(`- [raw:${row.source_type || 'event'}] ${String(row.text || row.title || '').replace(/\s+/g, ' ').trim().slice(0, 1000)}`);
  });

  // 3. Graph context after selected evidence. This may include supplemental
  // daily summaries only when retrieval was sparse.
  if (retrieval.contextText) {
    retrieval.contextText.split('\n').forEach(addLine);
  }

  // 4. Web results.
  (webResults || []).slice(0, 5).forEach(item => {
    addLine(`- [web] ${item.title}: ${item.snippet} (${item.url})`);
  });

  const contextMemoryXml = (() => {
    const lines = contextLines.slice(0, 48);
    if (!lines.length) return '<context_memory></context_memory>';
    const xmlRows = lines.map((line, index) => {
      const escaped = String(line || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
      return `  <source id="${index + 1}">${escaped}</source>`;
    }).join('\n');
    return `<context_memory>\n${xmlRows}\n</context_memory>`;
  })();

  const prompt = `[System]
You are Weave, a relationship-intelligence copilot. Your tone is warm, capable, calm, and direct.
Behave like a thoughtful operator with strong judgment, not a generic chatbot and not a yes-man.
Tell the user when their framing is weak, when evidence is thin, or when a recommendation has tradeoffs.
Use plain ASCII punctuation only. No invented facts. No hype. No therapy language. No urgency theater.
If evidence is incomplete, say so clearly and separate what is known from what is inferred.
Prefer specific names, dates, and supporting details from grounded context over generic advice.
When useful, give a short answer first, then the reasoning, then the best next move.
Answer mode: ${policy.answerMode}.

Mode rules:
- memory_lookup: answer only from grounded memory/web context. Give the direct answer first. You may add related grounded memories or adjacent context after the direct answer, but never use related context as proof for the exact claim.
- grounded_generation: create the requested output in the chat response (todo list, plan, draft, outline, synthesis, recommendation) using grounded memory/web context as inputs. Memory remains the source of truth; web context may add public/current background only when present in <context_memory>.
- external_action_request: if the user asks you to create/save/send/schedule/update something outside the chat, draft the artifact or give exact steps unless the runtime has actually performed the external write. Do not imply it was saved or sent.

Do not claim that a missing dedicated task-manager record means no todos exist if <context_memory> contains actionable suggestions, active suggestions, recent work, emails, calendar items, or open-loop evidence.
When generating things, always use the retrieved memory/web context as constraints and cite uncertainty plainly. Do not freewheel generic advice when memory is sparse.
When the answer can be developed, write at least ${policy.minWords} words. Start with the answer, then add related grounded context, adjacent memories, useful implications, or next steps. If there is truly no supporting context, stay shorter and ask one concise follow-up question.
Do not create contacts, automations, external actions, or UI cards unless the user explicitly asks you to create/save them and the runtime actually supports that action.
Do not output XML tags or tool instructions.
Answer strictly from the grounded memory/web context in <context_memory>. If evidence is missing, clearly say what is missing and ask one concise follow-up question.
If the user asks for strategy, recommendations, prioritization, or interpretation, make a real judgment instead of hedging across every option.
If the user asks for a draft, make it sound natural and specific to the relationship context rather than polished marketing copy.
If the request is complex, you may open with one short orienting sentence before the main answer, but do not add filler acknowledgments.

[Grounded Context]
${contextMemoryXml}

[Conversation History]
${formatChatHistoryForPrompt(chatHistory)}

Use Conversation History only to resolve follow-ups like "that", "do it", "what about the second one", or corrections to your previous answer. The grounded context remains authoritative for factual claims.

[Standing Notes]
${standingNotes || 'None'}

${reflectorFeedback ? `[Reflector Feedback]\nRejected for: ${reflectorFeedback.critique}\nSuggestion: ${reflectorFeedback.suggestions}\nFix in the final response.` : ''}

[User question]
${query}`;

  try {
    const content = await callLLM(prompt, apiKey, policy.temperature, { task: 'synthesis', maxTokens: policy.maxTokens });
    return sanitizeAssistantOutput(normalizeAssistantContent(content));
  } catch (llmError) {
    return buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
  }
}

module.exports = {
  answerChatQuery,
  buildThinkingTrace
};
