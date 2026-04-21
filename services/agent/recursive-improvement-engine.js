const db = require('../db');
const { callLLM } = require('./intelligence-engine');
const { dispatchTool, evaluateToolPolicy } = require('./tool-dispatcher');

const MAX_ACTIONS_PER_CYCLE = 3;

function asObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

async function getCount(sql, params = []) {
  const row = await db.getQuery(sql, params).catch(() => null);
  return Number(row?.count || 0);
}

function normalizeProposal(raw = {}) {
  const action = String(raw.action || '').trim();
  const reason = String(raw.reason || '').trim();
  const confidence = Math.max(0, Math.min(1, Number(raw.confidence || 0.6)));
  const priority = Math.max(0, Math.min(1, Number(raw.priority || 0.6)));
  const payload = raw.payload && typeof raw.payload === 'object' ? raw.payload : {};
  return { action, reason, confidence, priority, payload };
}

function actionToToolRequest(proposal = {}) {
  const action = String(proposal.action || '').toLowerCase();
  const payload = proposal.payload || {};

  if (action === 'contact_create') {
    return {
      tool: 'contact_create',
      input: {
        name: String(payload.name || '').trim(),
        email: payload.email || null,
        phone: payload.phone || null,
        notes: payload.notes || null
      }
    };
  }

  if (action === 'memory_create') {
    return {
      tool: 'memory_create',
      input: {
        layer: String(payload.layer || 'insight').trim(),
        subtype: String(payload.subtype || 'improvement').trim(),
        title: String(payload.title || '').trim(),
        summary: String(payload.summary || '').trim(),
        confidence: Number(payload.confidence || 0.82),
        metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : { created_via: 'recursive_improvement_cycle' }
      }
    };
  }

  if (action === 'automation_create') {
    return {
      tool: 'automation_create',
      input: {
        name: String(payload.name || '').trim(),
        description: String(payload.description || '').trim(),
        prompt: String(payload.prompt || '').trim(),
        interval_minutes: Math.max(5, Number(payload.interval_minutes || 60)),
        metadata: payload.metadata && typeof payload.metadata === 'object'
          ? payload.metadata
          : { created_via: 'recursive_improvement_cycle', recursive: true }
      }
    };
  }

  if (action === 'action_create') {
    return {
      tool: 'action_create',
      input: {
        title: String(payload.title || '').trim(),
        body: String(payload.body || payload.description || '').trim(),
        trigger_summary: String(payload.trigger_summary || payload.body || payload.title || '').trim(),
        category: String(payload.category || 'work').trim(),
        priority: String(payload.priority || 'medium').trim(),
        confidence: Math.max(0.45, Math.min(0.99, Number(payload.confidence || 0.78))),
        ai_doable: Boolean(payload.ai_doable),
        action_type: payload.action_type || (payload.ai_doable ? 'draft_message' : 'manual_next_step'),
        execution_mode: payload.execution_mode || (payload.ai_doable ? 'draft_or_execute' : 'manual'),
        assignee: payload.assignee || (payload.ai_doable ? 'ai' : 'human'),
        suggested_actions: Array.isArray(payload.suggested_actions) ? payload.suggested_actions : [],
        reason_codes: Array.isArray(payload.reason_codes) ? payload.reason_codes : ['recursive_action_create'],
        provider: payload.provider || 'deepseek',
        created_via: 'recursive_improvement_cycle'
      }
    };
  }

  return null;
}

async function gatherRecursiveContext() {
  const [
    contactCount,
    openAutomationCount,
    insightCount,
    weeklySuggestions,
    recursiveAutos
  ] = await Promise.all([
    getCount(`SELECT COUNT(*) AS count FROM memory_nodes WHERE layer='semantic' AND subtype='person'`),
    getCount(`SELECT COUNT(*) AS count FROM scheduled_automations WHERE enabled = 1`),
    getCount(`SELECT COUNT(*) AS count FROM memory_nodes WHERE layer='insight' AND status != 'archived'`),
    getCount(`SELECT COUNT(*) AS count FROM suggestion_artifacts WHERE status='active' AND datetime(created_at) >= datetime('now', '-7 days')`),
    getCount(`SELECT COUNT(*) AS count FROM scheduled_automations WHERE enabled = 1 AND LOWER(COALESCE(metadata, '')) LIKE '%recursive%'`)
  ]);

  const latestSuggestions = await db.allQuery(
    `SELECT id, title, trigger_summary, metadata, created_at
     FROM suggestion_artifacts
     WHERE status='active'
     ORDER BY datetime(created_at) DESC
     LIMIT 8`
  ).catch(() => []);

  return {
    contact_count: contactCount,
    active_automation_count: openAutomationCount,
    insight_count: insightCount,
    active_suggestion_count_7d: weeklySuggestions,
    recursive_automation_count: recursiveAutos,
    latest_suggestions: (latestSuggestions || []).map((row) => ({
      id: row.id,
      title: row.title,
      trigger_summary: row.trigger_summary,
      created_at: row.created_at,
      metadata: asObj(row.metadata)
    }))
  };
}

function buildImprovementPrompt(context = {}) {
  return `[System]\nYou are the recursive improvement architect for this personal agent.\nYour job is to propose small self-improvements the system can implement immediately using ONLY safe built-in actions.\n\nAllowed actions (strict):\n- contact_create\n- memory_create\n- automation_create\n- action_create\n\nRules:\n1) Return ONLY valid JSON: {"proposals":[...]}\n2) Proposals length: 1 to ${MAX_ACTIONS_PER_CYCLE}\n3) Each proposal object shape:\n   {"action":"contact_create|memory_create|automation_create|action_create","reason":"short","confidence":0-1,"priority":0-1,"payload":{...}}\n4) Prefer improvements that increase recursion (automation that generates future improvements, memory notes that improve policy, contact scaffolding if none exists).\n5) Keep actions safe and local. No file writes, no shell commands, no destructive operations.\n6) For automation_create, interval_minutes must be >= 15 and <= 360.\n\n[Current context]\n${JSON.stringify(context, null, 2)}`;
}

function fallbackProposals(context = {}) {
  const proposals = [];

  if (Number(context.recursive_automation_count || 0) < 1) {
    proposals.push({
      action: 'automation_create',
      reason: 'Bootstrap recursive self-improvement loop with scheduled reflection.',
      confidence: 0.84,
      priority: 0.88,
      payload: {
        name: 'Recursive Improvement Loop',
        description: 'Reviews memory, suggestions, and contacts; then proposes one concrete improvement action.',
        prompt: 'Review active suggestions, relationship heatmap, and memory insights. If you can improve the system, create exactly one action using supported tags such as <automation_create>, <memory_create>, or <contact_create>. Keep it safe and concrete.',
        interval_minutes: 60,
        metadata: {
          recursive: true,
          created_via: 'fallback_recursive_bootstrap'
        }
      }
    });
  }

  if (Number(context.contact_count || 0) < 3) {
    proposals.push({
      action: 'contact_create',
      reason: 'Seed contact graph for relational proactive behaviors.',
      confidence: 0.68,
      priority: 0.62,
      payload: {
        name: 'Relationship Anchor Contact',
        notes: 'System-generated placeholder to bootstrap relational proactive logic.'
      }
    });
  }

  proposals.push({
    action: 'action_create',
    reason: 'Create one immediate executable action so each recursive cycle yields visible output.',
    confidence: 0.79,
    priority: 0.81,
    payload: {
      title: 'Review latest recursive cycle outcomes',
      body: 'Check new automations, contacts, and policy notes; then confirm one concrete improvement to keep the loop productive.',
      trigger_summary: 'Recursive cycle produced new artifacts that should be validated and iterated.',
      category: 'work',
      priority: 'high',
      ai_doable: false,
      action_type: 'manual_next_step',
      reason_codes: ['recursive_visibility', 'feedback_loop']
    }
  });

  proposals.push({
    action: 'memory_create',
    reason: 'Persist recursive safety policy to guide future improvement cycles.',
    confidence: 0.72,
    priority: 0.74,
    payload: {
      layer: 'insight',
      subtype: 'policy',
      title: 'Recursive improvement policy',
      summary: 'Prioritize safe, reversible improvements. Favor contact quality, proactive memory hygiene, and automation loops that reduce manual work.',
      metadata: {
        created_via: 'fallback_recursive_policy'
      }
    }
  });

  return proposals.slice(0, MAX_ACTIONS_PER_CYCLE);
}

function toCycleLog(result = {}) {
  return {
    cycle_id: `recursive_cycle_${Date.now()}`,
    created_at: new Date().toISOString(),
    ...result
  };
}

async function persistCycleLog(cycleLog = {}) {
  const now = new Date().toISOString();
  await db.runQuery(
    `INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)`,
    ['recursive_improvement:last', JSON.stringify(cycleLog), 'recursive_improvement', now]
  ).catch(() => null);

  await db.runQuery(
    `INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)`,
    [`recursive_improvement:history:${Date.now()}`, JSON.stringify(cycleLog), 'recursive_improvement_history', now]
  ).catch(() => null);
}

async function runRecursiveImprovementCycle({ apiKey = null, maxActions = MAX_ACTIONS_PER_CYCLE } = {}) {
  const context = await gatherRecursiveContext();
  const prompt = buildImprovementPrompt(context);

  let proposals = [];
  let source = 'fallback';

  if (apiKey) {
    try {
      const llm = await callLLM(prompt, apiKey, 0.18, { maxTokens: 700, economy: true, task: 'routing' });
      const parsed = llm && typeof llm === 'object' ? llm : {};
      proposals = Array.isArray(parsed.proposals) ? parsed.proposals.map(normalizeProposal) : [];
      source = proposals.length ? 'llm' : 'fallback';
    } catch (_) {
      proposals = [];
      source = 'fallback';
    }
  }

  if (!proposals.length) {
    proposals = fallbackProposals(context).map(normalizeProposal);
  }

  proposals = proposals
    .filter((p) => ['contact_create', 'memory_create', 'automation_create', 'action_create'].includes(String(p.action || '').toLowerCase()))
    .sort((a, b) => (Number(b.priority || 0) - Number(a.priority || 0)))
    .slice(0, Math.max(1, Math.min(MAX_ACTIONS_PER_CYCLE, Number(maxActions || MAX_ACTIONS_PER_CYCLE))));

  const executed = [];
  for (const proposal of proposals) {
    const toolRequest = actionToToolRequest(proposal);
    if (!toolRequest) {
      executed.push({ proposal, status: 'skipped', reason: 'unsupported_action' });
      continue;
    }

    const policy = evaluateToolPolicy(toolRequest, {});
    if (policy.decision !== 'auto_allow') {
      executed.push({ proposal, status: 'blocked', reason: policy.reason, policy });
      continue;
    }

    try {
      const result = await dispatchTool(toolRequest, {});
      executed.push({
        proposal,
        status: result?.status || 'error',
        tool: toolRequest.tool,
        output: result?.output || null,
        error: result?.error || null
      });
    } catch (error) {
      executed.push({
        proposal,
        status: 'error',
        tool: toolRequest.tool,
        output: null,
        error: error?.message || String(error)
      });
    }
  }

  const cycleLog = toCycleLog({
    source,
    context,
    proposal_count: proposals.length,
    executed_count: executed.filter((x) => x.status === 'success').length,
    executed
  });

  await persistCycleLog(cycleLog);
  return cycleLog;
}

async function getLatestRecursiveImprovementLog() {
  const row = await db.getQuery(`SELECT value, created_at FROM kv_cache WHERE key = ?`, ['recursive_improvement:last']).catch(() => null);
  return {
    created_at: row?.created_at || null,
    data: asObj(row?.value)
  };
}

module.exports = {
  runRecursiveImprovementCycle,
  getLatestRecursiveImprovementLog
};
