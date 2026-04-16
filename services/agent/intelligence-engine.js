const crypto = require('crypto');
const db = require('../db');
// Lazy require graph-derivation to avoid circular require/initialization order issues
function graphDerivation() {
  // require on demand to break cyclic module dependencies
  // eslint-disable-next-line global-require
  return require('./graph-derivation');
}
const { upsertMemoryNode, updateMemoryNode, upsertMemoryEdge, upsertRetrievalDoc } = require('./graph-store');
const { generateEmbedding } = require('../embedding-engine');

const DEEPSEEK_ENDPOINT = 'https://api.deepseek.com/v1/chat/completions';
const llmCache = new Map();
let lastAiParseLogAt = 0;

function cleanModelJsonText(raw = '') {
  return String(raw || '')
    .trim()
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/^```[a-z]*\n?/i, '')
    .replace(/```$/g, '')
    .trim();
}

function extractFirstJsonBlock(text = '') {
  const src = String(text || '');
  const startObj = src.indexOf('{');
  const startArr = src.indexOf('[');
  let start = -1;
  let openChar = '';
  let closeChar = '';
  if (startObj === -1 && startArr === -1) return '';
  if (startObj !== -1 && (startArr === -1 || startObj < startArr)) {
    start = startObj;
    openChar = '{';
    closeChar = '}';
  } else {
    start = startArr;
    openChar = '[';
    closeChar = ']';
  }

  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < src.length; i++) {
    const ch = src[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === openChar) depth += 1;
    if (ch === closeChar) {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  return src.slice(start).trim();
}

function tryParseJsonLoose(raw = '') {
  const cleaned = cleanModelJsonText(raw);
  if (!cleaned) return null;
  try {
    return JSON.parse(cleaned);
  } catch (_) {}

  const extracted = extractFirstJsonBlock(cleaned);
  if (!extracted) return null;
  try {
    return JSON.parse(extracted);
  } catch (_) {}

  const withoutTrailingCommas = extracted.replace(/,\s*([}\]])/g, '$1');
  try {
    return JSON.parse(withoutTrailingCommas);
  } catch (_) {}

  // Best-effort: close missing trailing braces/brackets on truncated outputs.
  let repaired = withoutTrailingCommas;
  const openBraces = (repaired.match(/\{/g) || []).length;
  const closeBraces = (repaired.match(/\}/g) || []).length;
  if (openBraces > closeBraces) repaired += '}'.repeat(openBraces - closeBraces);
  const openBrackets = (repaired.match(/\[/g) || []).length;
  const closeBrackets = (repaired.match(/\]/g) || []).length;
  if (openBrackets > closeBrackets) repaired += ']'.repeat(openBrackets - closeBrackets);
  try {
    return JSON.parse(repaired);
  } catch (_) {}

  return null;
}

async function callDeepSeek(prompt, apiKey, temperature = 0.3) {
  try {
    if (!apiKey) return null;
    const cacheKey = crypto.createHash('sha1').update(`${temperature}|${String(prompt || '').slice(0, 7000)}`).digest('hex');
    const cached = llmCache.get(cacheKey);
    if (cached && (Date.now() - cached.at) < 5 * 60 * 1000) return cached.value;

    const response = await fetch(DEEPSEEK_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({
        model: 'deepseek-chat',
        messages: [{ role: 'user', content: prompt }],
        temperature,
        max_tokens: 1024
      })
    });
    const responseText = await response.text();
    let data = null;
    try {
      data = JSON.parse(responseText);
    } catch (_) {
      data = null;
    }
    if (!response.ok) {
      const errMsg = data?.error?.message || response.statusText || 'DeepSeek request failed';
      throw new Error(`${response.status}: ${errMsg}`);
    }
    const raw = data?.choices?.[0]?.message?.content || 'null';
    const value = tryParseJsonLoose(raw);
    if (value == null) {
      const now = Date.now();
      if ((now - lastAiParseLogAt) > (2 * 60 * 1000)) {
        const preview = String(raw || '').replace(/\s+/g, ' ').slice(0, 220);
        console.warn('[AI] Parse fallback: model returned non-JSON content; using null result. Preview:', preview);
        lastAiParseLogAt = now;
      }
      return null;
    }
    llmCache.set(cacheKey, { at: Date.now(), value });
    if (llmCache.size > 120) {
      const oldest = [...llmCache.entries()].sort((a, b) => a[1].at - b[1].at).slice(0, llmCache.size - 120);
      oldest.forEach(([key]) => llmCache.delete(key));
    }
    return value;
  } catch (e) {
    const msg = String(e?.message || e);
    const now = Date.now();
    if ((now - lastAiParseLogAt) > (2 * 60 * 1000)) {
      console.error('[AI] Call failed:', msg);
      lastAiParseLogAt = now;
    }
    return null;
  }
}

function normalizeLLMConfig(configOrApiKey = null) {
  if (!configOrApiKey) return null;
  if (typeof configOrApiKey === 'string') {
    return {
      provider: 'deepseek',
      apiKey: configOrApiKey,
      model: 'deepseek-chat'
    };
  }
  const cfg = { ...(configOrApiKey || {}) };
  cfg.provider = String(cfg.provider || 'deepseek').toLowerCase();
  if (!cfg.model) {
    cfg.model = cfg.provider === 'ollama' ? 'llama3.1:8b' : 'deepseek-chat';
  }
  return cfg;
}

async function callOllama(prompt, config = {}, temperature = 0.3) {
  try {
    const baseUrl = String(config.baseUrl || config.base_url || process.env.OLLAMA_BASE_URL || 'http://127.0.0.1:11434').replace(/\/+$/, '');
    const endpoint = `${baseUrl}/api/chat`;
    const model = String(config.model || process.env.OLLAMA_MODEL || 'llama3.1:8b');
    const headers = { 'Content-Type': 'application/json' };
    if (config.apiKey) headers.Authorization = `Bearer ${config.apiKey}`;

    const response = await fetch(endpoint, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model,
        stream: false,
        format: 'json',
        options: {
          temperature
        },
        messages: [
          { role: 'user', content: prompt }
        ]
      })
    });
    const responseText = await response.text();
    let data = null;
    try {
      data = JSON.parse(responseText);
    } catch (_) {
      data = null;
    }
    if (!response.ok) {
      const errMsg = data?.error || data?.message || response.statusText || 'Ollama request failed';
      throw new Error(`${response.status}: ${errMsg}`);
    }

    const raw = data?.message?.content || data?.response || 'null';
    return tryParseJsonLoose(raw);
  } catch (e) {
    const msg = String(e?.message || e);
    const now = Date.now();
    if ((now - lastAiParseLogAt) > (2 * 60 * 1000)) {
      console.error('[AI] Ollama call failed:', msg);
      lastAiParseLogAt = now;
    }
    return null;
  }
}

async function callLLM(prompt, configOrApiKey = null, temperature = 0.3) {
  const config = normalizeLLMConfig(configOrApiKey);
  if (!config) return null;
  if (config.provider === 'ollama') {
    return callOllama(prompt, config, temperature);
  }
  return callDeepSeek(prompt, config.apiKey, temperature);
}

async function generateNodeTLDR(node, apiKey) {
  if (!node || !apiKey) return null;
  const isEpisode = node.layer === 'episode';
  const bulletCount = isEpisode ? '10-20' : '3';
  const prompt = `
You are a detailed memory assistant. Given a memory node (layer: ${node.layer}), produce exactly ${bulletCount} key bullet points that provide a highly detailed reconstruction of the activity.
Return strict JSON: {"tldr": ["bullet 1", "bullet 2", ...]}

Title: ${String(node.title || '').slice(0, 240)}
Summary: ${String(node.summary || '').slice(0, 500)}
Content: ${String(node.canonical_text || '').slice(0, 5000)}
`;
  const payload = await callLLM(prompt, normalizeLLMConfig(apiKey), 0.2);
  if (payload && Array.isArray(payload.tldr)) {
    const limit = isEpisode ? 20 : 3;
    return payload.tldr.slice(0, limit).map(b => `• ${b.replace(/^•\s*/, '')}`).join('\n');
  }
  return null;
}

async function runEnrichmentJob(apiKey) {
  const result = await graphDerivation().deriveGraphFromEvents({
    versionSeed: 'current'
  });
  
  const layersToEnrich = ['raw', 'episode', 'semantic', 'cloud', 'insight', 'core'];
  const placeholders = layersToEnrich.map(() => '?').join(',');
  
  const nodesToEnrich = await db.allQuery(
    `SELECT id, layer, title, summary, canonical_text FROM memory_nodes 
     WHERE layer IN (${placeholders}) AND (summary NOT LIKE '• %' OR summary IS NULL)
     ORDER BY confidence DESC LIMIT 50`,
    layersToEnrich
  ).catch(() => []);

  for (const node of nodesToEnrich) {
    try {
      const tldr = await generateNodeTLDR(node, apiKey);
      if (tldr) {
        await updateMemoryNode(node.id, { summary: tldr });
        
        await upsertRetrievalDoc({
          docId: `node:${node.id}`,
          sourceType: 'node',
          nodeId: node.id,
          timestamp: new Date().toISOString(),
          text: `${node.title}\n${tldr}`,
          metadata: {
            layer: node.layer,
            title: node.title
          }
        });
      }
    } catch (e) {
      console.warn(`[runEnrichmentJob] Failed to enrich node ${node.id}:`, e.message);
    }
  }
  
  return result;
}

async function runEpisodeJob() {
  const result = await runEnrichmentJob(process.env.DEEPSEEK_API_KEY);
  return result.episodeIds || [];
}

async function runWeeklyInsightJob(apiKey) {
  const cloudRows = await db.allQuery(
    `SELECT id, subtype, title, summary, canonical_text, confidence, source_refs, metadata
     FROM memory_nodes
     WHERE layer = 'cloud' AND status = 'open'
     ORDER BY confidence DESC
     LIMIT 32`
  ).catch(() => []);
  if (!cloudRows.length) return [];

  const strongClouds = cloudRows
    .map((row) => ({
      ...row,
      metadata: (() => {
        try {
          return JSON.parse(row.metadata || '{}');
        } catch (_) {
          return {};
        }
      })(),
      source_refs: (() => {
        try {
          return JSON.parse(row.source_refs || '[]');
        } catch (_) {
          return [];
        }
      })()
    }))
    .filter((row) => Number(row.confidence || 0) >= 0.78);

  if (!strongClouds.length) return [];

  const prompt = `
You are promoting durable insights from repeated memory clouds.
Return strict JSON:
[
  {
    "cloud_id": "...",
    "title": "...",
    "summary": "...",
    "confidence": 0.0
  }
]

Clouds:
${JSON.stringify(strongClouds.map((row) => ({
    id: row.id,
    subtype: row.subtype,
    title: row.title,
    summary: row.summary,
    confidence: row.confidence,
    repeated_count: row.metadata?.repeated_count || 0
  })))}
`;

  const payload = await callDeepSeek(prompt, apiKey, 0.2);
  const rows = Array.isArray(payload) ? payload : [];
  const created = [];

  for (const item of rows) {
    const cloud = strongClouds.find((row) => row.id === item?.cloud_id);
    if (!cloud) continue;
    const insightId = `ins_${crypto.createHash('sha1').update(`${cloud.id}|${item.title || cloud.title}`).digest('hex').slice(0, 16)}`;
    const title = String(item.title || cloud.title).trim().slice(0, 180);
    const summary = String(item.summary || cloud.summary).trim().slice(0, 320);
    const confidence = Math.max(Number(cloud.confidence || 0), Math.min(0.96, Number(item.confidence || 0.82)));
    await upsertMemoryNode({
      id: insightId,
      layer: 'insight',
      subtype: cloud.subtype,
      title,
      summary,
      canonicalText: `${title}\n${summary}`,
      confidence,
      status: 'promoted',
      sourceRefs: cloud.source_refs,
      metadata: {
        promoted_from_cloud_id: cloud.id,
        anchor_at: cloud.metadata?.anchor_at || null,
        anchor_date: cloud.metadata?.anchor_date || null,
        latest_activity_at: cloud.metadata?.latest_activity_at || null,
        supporting_episode_ids: cloud.metadata?.supporting_episode_ids || []
      },
      graphVersion: 'zero_base_memory_v1:current'
    });
    await upsertMemoryEdge({
      fromNodeId: cloud.id,
      toNodeId: insightId,
      edgeType: 'PROMOTED_FROM',
      weight: confidence,
      traceLabel: 'LLM-promoted durable insight',
      evidenceCount: Number(cloud.metadata?.repeated_count || 1),
      metadata: {}
    });
    await upsertRetrievalDoc({
      docId: `node:${insightId}`,
      sourceType: 'node',
      nodeId: insightId,
      timestamp: cloud.metadata?.anchor_at || cloud.metadata?.latest_activity_at || new Date().toISOString(),
      text: `${title}\n${summary}`,
      metadata: {
        layer: 'insight',
        subtype: cloud.subtype,
        source_refs: cloud.source_refs,
        anchor_at: cloud.metadata?.anchor_at || null,
        anchor_date: cloud.metadata?.anchor_date || null,
        latest_activity_at: cloud.metadata?.latest_activity_at || null
      }
    });
    created.push(insightId);
  }

  return created;
}

function summarizeClusterFallback(group = {}) {
  const events = Array.isArray(group.events) ? group.events : [];
  if (!events.length) return null;
  const titles = events.map((event) => String(event.title || '').trim()).filter(Boolean).slice(0, 2);
  const apps = Array.from(new Set(events.map((event) => String(event.app || '').trim()).filter(Boolean))).slice(0, 3);
  const domains = Array.from(new Set(events.map((event) => String(event.domain || '').trim()).filter(Boolean))).slice(0, 3);
  const title = titles[0] || `Activity cluster (${events.length} events)`;
  const summary = [
    titles.length > 1 ? `Included: ${titles.join(' | ')}` : '',
    apps.length ? `Apps: ${apps.join(', ')}` : '',
    domains.length ? `Domains: ${domains.join(', ')}` : '',
    `${events.length} events in the same context window`
  ].filter(Boolean).join(' • ');
  return { title: String(title).slice(0, 220), summary: String(summary).slice(0, 1024), topics: domains, confidence: 0.68 };
}

// Summarize raw events for a short time window (e.g., last 30 minutes).
// Returns an array of created semantic node ids.
async function runSemanticSummaryWindow(windowMs = 30 * 60 * 1000, llmConfigOrKey = null) {
  try {
    const now = Date.now();
    const start = new Date(now - windowMs).toISOString();
    const end = new Date(now).toISOString();
    const rows = await db.allQuery(
      `SELECT id, type, source_type, source, app, occurred_at, timestamp, title, text, redacted_text, raw_text,
              metadata, source_ref, domain, participants, window_title, url
       FROM events
       WHERE timestamp >= ? AND timestamp <= ?
       ORDER BY timestamp ASC
       LIMIT 500`,
      [start, end]
    ).catch(() => []);
    if (!rows || !rows.length) return [];

  const envelopes = rows.map((row) => graphDerivation().envelopeFromRow(row)).filter((item) => item && item.id);
  const groups = graphDerivation().clusterEnvelopes(envelopes)
      .sort((a, b) => {
        if (b.events.length !== a.events.length) return b.events.length - a.events.length;
        return b.latestTs - a.latestTs;
      })
      .slice(0, 5);
    if (!groups.length) return [];

    const createdIds = [];
    for (let index = 0; index < groups.length; index += 1) {
      const group = groups[index];
      const blob = group.events
        .sort((a, b) => Date.parse(a.timestamp || a.occurred_at || 0) - Date.parse(b.timestamp || b.occurred_at || 0))
        .slice(0, 160)
        .map((event) => ({
          id: event.id,
          type: event.type,
          title: event.title,
          app: event.app,
          domain: event.domain,
          participants: event.participants || [],
          text: String(event.text || '').slice(0, 900),
          timestamp: event.timestamp || event.occurred_at
        }));

      const prompt = `You are summarizing one distinct activity cluster from the last ${Math.round(windowMs / 60000)} minutes.
Do not mix this with any other parallel activity.
Return strict JSON only:
{"title":"...","summary":"...","topics":["..."],"confidence":0.0}
Keep title under 12 words. Keep summary to 1-3 sentences.
Cluster events:
${JSON.stringify(blob)}`;

      const payload = await callLLM(prompt, normalizeLLMConfig(llmConfigOrKey || process.env.DEEPSEEK_API_KEY || null), 0.2);
      const parsed = Array.isArray(payload) ? payload[0] : payload;
      const fallback = summarizeClusterFallback(group);
      const title = String(parsed?.title || fallback?.title || '').trim().slice(0, 220);
      const summary = String(parsed?.summary || parsed?.description || fallback?.summary || '').trim().slice(0, 1024);
      if (!title || !summary) continue;
      const topics = Array.isArray(parsed?.topics) && parsed.topics.length
        ? parsed.topics.slice(0, 12).map((item) => String(item || '').trim()).filter(Boolean)
        : (fallback?.topics || []);
      const confidence = Math.max(0.2, Math.min(0.99, Number(parsed?.confidence || fallback?.confidence || 0.76)));
      const semId = `sem_${crypto.createHash('sha1').update(`${title}|${String(Date.now())}|${index}`).digest('hex').slice(0, 16)}`;

      await upsertMemoryNode({
        id: semId,
        layer: 'semantic',
        subtype: 'summary_window',
        title,
        summary,
        canonicalText: `${title}\n${summary}`,
        confidence,
        status: 'open',
        sourceRefs: blob.map((event) => event.id).filter(Boolean).slice(0, 160),
        metadata: {
          window_ms: windowMs,
          event_count: blob.length,
          topics,
          cluster_index: index,
          cluster_count: groups.length,
          cluster_anchor_at: new Date(group.startTs).toISOString(),
          cluster_latest_at: new Date(group.latestTs).toISOString(),
          source_type_group: group.typeGroup
        },
        graphVersion: 'semantic_window_v2'
      });

      for (const ev of blob.slice(0, 120)) {
        await upsertMemoryEdge({
          fromNodeId: `event:${ev.id}`,
          toNodeId: semId,
          edgeType: 'PART_OF',
          weight: 0.6,
          traceLabel: 'supports_semantic_window_cluster',
          evidenceCount: 1,
          metadata: {
            window_ms: windowMs,
            cluster_index: index
          }
        }).catch(() => null);
      }

      await upsertRetrievalDoc({
        docId: `node:${semId}`,
        sourceType: 'node',
        nodeId: semId,
        timestamp: new Date(group.latestTs || Date.now()).toISOString(),
        text: `${title}\n${summary}`,
        metadata: {
          layer: 'semantic',
          subtype: 'summary_window',
          source_type_group: group.typeGroup,
          cluster_index: index,
          cluster_count: groups.length
        }
      });

      createdIds.push(semId);
    }

    return createdIds;
  } catch (e) {
    console.warn('[runSemanticSummaryWindow] failed:', e?.message || e);
    return [];
  }
}

// Daily insights runner that can promote multiple insights based on recent clouds/semantics.
async function runDailyInsights(apiKey) {
  try {
    // Reuse weekly logic but operate on recent day and allow multiple promotions.
    // Select candidate clouds from last 48 hours
    const since = new Date(Date.now() - (48 * 60 * 60 * 1000)).toISOString();
    const cloudRows = await db.allQuery(
      `SELECT id, subtype, title, summary, canonical_text, confidence, source_refs, metadata
       FROM memory_nodes
       WHERE layer = 'cloud' AND status = 'open' AND COALESCE(metadata, '') != '' AND json_extract(metadata, '$.latest_activity_at') >= ?
       ORDER BY confidence DESC
       LIMIT 48`,
      [since]
    ).catch(() => []);
    if (!cloudRows.length) return [];

    // Keep strong clouds
    const strongClouds = cloudRows.map((row) => ({
      ...row,
      metadata: (() => { try { return JSON.parse(row.metadata || '{}'); } catch (_) { return {}; } })(),
      source_refs: (() => { try { return JSON.parse(row.source_refs || '[]'); } catch (_) { return []; } })()
    })).filter(r => Number(r.confidence || 0) >= 0.6);
    if (!strongClouds.length) return [];

    const prompt = `Promote 1-5 high-quality insights from these memory clouds. Return strict JSON array of {cloud_id, title, summary, confidence}.\nClouds:\n${JSON.stringify(strongClouds.map(c=>({id:c.id, title:c.title, summary:c.summary, confidence:c.confidence, repeated_count:c.metadata?.repeated_count||0})))} `;
    const payload = await callLLM(prompt, normalizeLLMConfig(apiKey || process.env.DEEPSEEK_API_KEY || null), 0.2);
    const rows = Array.isArray(payload) ? payload : [];
    const created = [];
    for (const item of rows) {
      const cloud = strongClouds.find((c) => c.id === item?.cloud_id);
      if (!cloud) continue;
      const insightId = `ins_${crypto.createHash('sha1').update(`${cloud.id}|${item.title || cloud.title}`).digest('hex').slice(0, 16)}`;
      const title = String(item.title || cloud.title).trim().slice(0, 180);
      const summary = String(item.summary || cloud.summary).trim().slice(0, 320);
      const confidence = Math.max(Number(cloud.confidence || 0), Math.min(0.96, Number(item.confidence || 0.82)));
      await upsertMemoryNode({
        id: insightId,
        layer: 'insight',
        subtype: cloud.subtype,
        title,
        summary,
        canonicalText: `${title}\n${summary}`,
        confidence,
        status: 'promoted',
        sourceRefs: cloud.source_refs,
        metadata: { promoted_from_cloud_id: cloud.id, anchor_at: cloud.metadata?.anchor_at || null },
        graphVersion: 'daily_insights_v1'
      });
      await upsertMemoryEdge({
        fromNodeId: cloud.id,
        toNodeId: insightId,
        edgeType: 'PROMOTED_FROM',
        weight: confidence,
        traceLabel: 'Daily LLM-promoted insight',
        evidenceCount: Number(cloud.metadata?.repeated_count || 1),
        metadata: {}
      });
      await upsertRetrievalDoc({
        docId: `node:${insightId}`,
        sourceType: 'node',
        nodeId: insightId,
        timestamp: cloud.metadata?.anchor_at || cloud.metadata?.latest_activity_at || new Date().toISOString(),
        text: `${title}\n${summary}`,
        metadata: { layer: 'insight', subtype: cloud.subtype }
      });
      created.push(insightId);
    }
    return created;
  } catch (e) {
    console.warn('[runDailyInsights] failed:', e?.message || e);
    return [];
  }
}

async function runLivingCoreJob(apiKey) {
  try {
    const insightRows = await db.allQuery(
      `SELECT id, subtype, title, summary, canonical_text, confidence, source_refs, metadata
       FROM memory_nodes
       WHERE layer = 'insight' AND confidence > 0.9
       ORDER BY confidence DESC
       LIMIT 50`
    ).catch(() => []);

    if (!insightRows.length) return [];

    const highConfidenceInsights = insightRows.map(row => ({
      ...row,
      metadata: (() => { try { return JSON.parse(row.metadata || '{}'); } catch (_) { return {}; } })(),
      source_refs: (() => { try { return JSON.parse(row.source_refs || '[]'); } catch (_) { return []; } })()
    }));

    const prompt = `
You are synthesizing the "Living Core" of a user's memory. These represent durable, long-term knowledge, core beliefs, and fundamental context.
Given a list of high-confidence insights, group related ones and synthesize them into 1-3 core nodes.
Return strict JSON array:
[
  {
    "title": "...",
    "summary": "...",
    "supporting_insight_ids": ["id1", "id2"]
  }
]

Insights:
${JSON.stringify(highConfidenceInsights.map(i => ({ id: i.id, title: i.title, summary: i.summary })))}
`;

    const payload = await callLLM(prompt, normalizeLLMConfig(apiKey || process.env.DEEPSEEK_API_KEY || null), 0.2);
    const coreSyntheses = Array.isArray(payload) ? payload : [];
    const created = [];

    for (const synthesis of coreSyntheses) {
      const coreId = `core_${crypto.createHash('sha1').update(synthesis.title).digest('hex').slice(0, 16)}`;
      const title = String(synthesis.title).trim().slice(0, 180);
      const summary = String(synthesis.summary).trim().slice(0, 500);
      
      await upsertMemoryNode({
        id: coreId,
        layer: 'core',
        title,
        summary,
        canonicalText: `${title}\n${summary}`,
        confidence: 0.98,
        status: 'active',
        sourceRefs: synthesis.supporting_insight_ids || [],
        metadata: {
          supporting_insight_ids: synthesis.supporting_insight_ids || []
        },
        graphVersion: 'living_core_v1'
      });

      if (Array.isArray(synthesis.supporting_insight_ids)) {
        for (const insId of synthesis.supporting_insight_ids) {
          await upsertMemoryEdge({
            fromNodeId: insId,
            toNodeId: coreId,
            edgeType: 'ABSTRACTED_TO',
            weight: 0.95,
            traceLabel: 'Insight abstracted to Living Core',
            evidenceCount: 1,
            metadata: {}
          });
        }
      }

      await upsertRetrievalDoc({
        docId: `node:${coreId}`,
        sourceType: 'node',
        nodeId: coreId,
        timestamp: new Date().toISOString(),
        text: `${title}\n${summary}`,
        metadata: { layer: 'core' }
      });
      
      created.push(coreId);
    }
    return created;
  } catch (e) {
    console.warn('[runLivingCoreJob] failed:', e?.message || e);
    return [];
  }
}

async function buildGlobalGraph() {
  const result = await graphDerivation().deriveGraphFromEvents({
    versionSeed: 'current'
  });
  const rows = await db.allQuery(
    `SELECT layer, COUNT(*) as count
     FROM memory_nodes
     GROUP BY layer`
  ).catch(() => []);
  return {
    version: result.version,
    counts: rows.reduce((acc, row) => {
      acc[row.layer] = row.count;
      return acc;
    }, {}),
    episodeIds: result.episodeIds || []
  };
}

module.exports = {
  callDeepSeek,
  callLLM,
  runEpisodeJob,
  runEnrichmentJob,
  runSemanticSummaryWindow,
  runWeeklyInsightJob,
  runDailyInsights,
  runLivingCoreJob,
  buildGlobalGraph
};
