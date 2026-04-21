const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const db = require('../db');
const { 
  upsertMemoryNode, 
  updateMemoryNode, 
  upsertMemoryEdge, 
  stableHash,
  asObj 
} = require('./graph-store');

// Lazy require to avoid circular dependency if any
function hybridRetrieval() {
  return require('./hybrid-graph-retrieval');
}

const DANGEROUS_SHELL_PATTERNS = [
  /\brm\s+-rf\s+\/\b/i,
  /\bsudo\s+rm\b/i,
  /\bmkfs\./i,
  /\bdd\s+if=/i,
  /\bshutdown\b/i,
  /\breboot\b/i
];

const TOOL_SCHEMAS = {
  shell_exec: ['command'],
  browser_navigate: ['url'],
  browser_click: ['target_id'],
  browser_type: ['target_id', 'text'],
  browser_scroll: [],
  browser_wait: [],
  read_file: ['path'],
  write_file: ['path', 'content'],
  ax_snapshot: [],
  ax_press: ['target_id'],
  ax_set_value: ['target_id', 'text'],
  key_press: ['key'],
  scroll: [],
  screenshot: [],
  applescript: ['script'],
  memory_search: ['query'],
  memory_drilldown: ['node_id'],
  memory_update: ['node_id', 'updates'],
  memory_link: ['from_id', 'to_id', 'relationship'],
  memory_create: ['layer', 'subtype', 'title', 'summary'],
  action_create: ['title'],
  contact_create: ['name'],
  contact_update: ['name', 'updates'],
  automation_create: ['name', 'prompt', 'interval_minutes'],
  automation_list: []
};

function validateToolRequest(request = {}) {
  const tool = String(request.tool || '').trim();
  if (!tool) throw new Error('Missing tool');
  if (!TOOL_SCHEMAS[tool]) throw new Error(`Unknown tool: ${tool}`);
  const input = request.input || {};
  for (const field of TOOL_SCHEMAS[tool]) {
    if (input[field] === undefined || input[field] === null || input[field] === '') {
      throw new Error(`Missing required field for ${tool}: ${field}`);
    }
  }
  return { tool, input };
}

function normalizePath(value) {
  return path.resolve(String(value || '').trim());
}

function evaluateToolPolicy(request = {}, policyContext = {}) {
  const tool = String(request.tool || '');
  const input = request.input || {};
  const command = String(input.command || '').trim();
  const sessionAllows = new Set(Array.isArray(policyContext.session_allows) ? policyContext.session_allows : []);
  const approvalKey = `${tool}:${tool === 'shell_exec' ? command : ''}`.trim();
  if (sessionAllows.has(approvalKey)) {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'session_allowlist' };
  }

  if (tool === 'shell_exec') {
    if (DANGEROUS_SHELL_PATTERNS.some((rx) => rx.test(command))) {
      return { decision: 'deny', risk_level: 'critical', reason: 'dangerous_shell_pattern' };
    }
    if (/[>|<]|&&|\|\|/.test(command) || /\b(rm|mv|chmod|chown|cp)\b/i.test(command)) {
      return { decision: 'require_approval', risk_level: 'high', reason: 'side_effect_shell_command' };
    }
    return { decision: 'auto_allow', risk_level: 'low', reason: 'read_only_shell_command' };
  }

  if (tool === 'write_file' || tool === 'applescript') {
    return { decision: 'require_approval', risk_level: 'high', reason: 'sensitive_write_action' };
  }

  if (tool.startsWith('browser_') && ['browser_click', 'browser_type'].includes(tool)) {
    return { decision: 'require_approval', risk_level: 'medium', reason: 'state_changing_browser_action' };
  }

  if (tool === 'memory_update' || tool === 'memory_link') {
    return { decision: 'require_approval', risk_level: 'medium', reason: 'memory_modification' };
  }

  if (tool === 'memory_search' || tool === 'memory_drilldown') {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'read_only_memory_action' };
  }

  if (tool === 'memory_create' || tool === 'contact_create' || tool === 'contact_update') {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'user_initiated_write' };
  }

  if (tool === 'action_create') {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'user_initiated_action_creation' };
  }

  if (tool === 'automation_create') {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'user_initiated_automation' };
  }

  if (tool === 'automation_list') {
    return { decision: 'auto_allow', risk_level: 'low', reason: 'read_only_automation_query' };
  }

  return { decision: 'auto_allow', risk_level: 'low', reason: 'safe_by_default' };
}

async function runShellCommand(command, { timeoutMs = 30000, cancellation_token = null } = {}) {
  return await new Promise((resolve) => {
    const child = execFile('/bin/zsh', ['-lc', command], {
      timeout: timeoutMs,
      maxBuffer: 2 * 1024 * 1024
    }, (error, stdout, stderr) => {
      if (error) {
        resolve({
          status: 'error',
          output: { stdout: String(stdout || ''), stderr: String(stderr || ''), code: error.code || null },
          error: error.message
        });
        return;
      }
      resolve({
        status: 'success',
        output: { stdout: String(stdout || ''), stderr: String(stderr || ''), code: 0 },
        error: null
      });
    });
    if (cancellation_token) {
      cancellation_token.registerCleanup(() => {
        try { child.kill('SIGTERM'); } catch (_) {}
      });
    }
  });
}

async function dispatchTool(request = {}, runtime = {}) {
  const startedAt = Date.now();
  const { tool, input } = validateToolRequest(request);
  const token = request.cancellation_token || null;
  if (token?.isCancelled?.()) {
    return {
      status: 'cancelled',
      tool,
      output: null,
      error: 'task_cancelled',
      duration_ms: 0,
      execution_class: 'hard_stop'
    };
  }

  const executionClass = request.execution_class || (
    tool === 'shell_exec' || tool.startsWith('browser_') ? 'hard_stop' : 'ui_atomic'
  );
  token?.setInFlight?.(executionClass);

  let result;
  if (tool === 'shell_exec') {
    result = await runShellCommand(String(input.command || ''), {
      timeoutMs: Number(input.timeout_ms || 30000),
      cancellation_token: token
    });
  } else if (tool === 'browser_navigate') {
    result = await runtime.executeManagedBrowserAction({ kind: 'CDP_NAVIGATE', url: input.url }, runtime.context || {});
  } else if (tool === 'browser_click') {
    result = await runtime.executeManagedBrowserAction({ kind: 'CDP_CLICK', target_id: input.target_id }, runtime.context || {});
  } else if (tool === 'browser_type') {
    result = await runtime.executeManagedBrowserAction({ kind: 'CDP_TYPE', target_id: input.target_id, text: input.text, submit: Boolean(input.submit) }, runtime.context || {});
  } else if (tool === 'browser_scroll') {
    result = await runtime.executeManagedBrowserAction({ kind: 'CDP_SCROLL', direction: input.direction || 'down', amount: Number(input.amount || 1) }, runtime.context || {});
  } else if (tool === 'browser_wait') {
    result = await runtime.executeManagedBrowserAction({ kind: 'CDP_WAIT_FOR', text: input.text || '', timeout_ms: Number(input.timeout_ms || 2500) }, runtime.context || {});
  } else if (tool === 'ax_snapshot') {
    result = { status: 'success', observation: await runtime.observeDesktopState(input.observeOptions || {}) };
  } else if (tool === 'ax_press') {
    result = await runtime.executeDesktopAction({ kind: 'PRESS_AX', target_id: input.target_id, label: input.label || '', role: input.role || '' }, runtime.context || {});
  } else if (tool === 'ax_set_value') {
    result = await runtime.executeDesktopAction({ kind: 'SET_AX_VALUE', target_id: input.target_id, text: input.text || '' }, runtime.context || {});
  } else if (tool === 'key_press') {
    result = await runtime.executeDesktopAction({ kind: 'KEY_PRESS', key: input.key, modifiers: input.modifiers || [] }, runtime.context || {});
  } else if (tool === 'scroll') {
    result = await runtime.executeDesktopAction({ kind: 'SCROLL_AX', direction: input.direction || 'down', amount: Number(input.amount || 1) }, runtime.context || {});
  } else if (tool === 'screenshot') {
    result = { status: 'success', observation: await runtime.observeDesktopState({ includeScreenshot: true }) };
  } else if (tool === 'read_file') {
    const filePath = normalizePath(input.path);
    const content = await fs.promises.readFile(filePath, 'utf8');
    result = { status: 'success', output: { path: filePath, content } };
  } else if (tool === 'write_file') {
    const filePath = normalizePath(input.path);
    await fs.promises.writeFile(filePath, String(input.content || ''), 'utf8');
    result = { status: 'success', output: { path: filePath, bytes: Buffer.byteLength(String(input.content || '')) } };
  } else if (tool === 'applescript') {
    result = await runtime.runOsascript(input.script);
    result = { status: 'success', output: result };
  } else if (tool === 'memory_search') {
    const retrieval = await hybridRetrieval().buildHybridGraphRetrieval({
      query: input.query,
      options: { mode: 'chat' }
    });
    result = { status: 'success', output: retrieval };
  } else if (tool === 'memory_drilldown') {
    const node = await db.getQuery(
      `SELECT * FROM memory_nodes WHERE id = ?`,
      [input.node_id]
    ).catch(() => null);
    if (node) {
      const edges = await db.allQuery(
        `SELECT * FROM memory_edges WHERE from_node_id = ? OR to_node_id = ? LIMIT 50`,
        [input.node_id, input.node_id]
      ).catch(() => []);
      
      // Reconstruction logic:
      // 1) direct source_refs on target node
      // 2) source_refs from directly linked nodes (episodes/raw_event/semantic)
      const directSourceRefs = (() => {
        try { return JSON.parse(node.source_refs || '[]'); } catch (_) { return []; }
      })();

      const neighborIds = Array.from(new Set(
        edges.flatMap((edge) => [edge.from_node_id, edge.to_node_id]).filter((id) => id && id !== input.node_id)
      )).slice(0, 60);

      let neighborRows = [];
      if (neighborIds.length) {
        const placeholders = neighborIds.map(() => '?').join(',');
        neighborRows = await db.allQuery(
          `SELECT id, layer, subtype, source_refs, metadata
           FROM memory_nodes
           WHERE id IN (${placeholders})`,
          neighborIds
        ).catch(() => []);
      }

      const neighborSourceRefs = neighborRows.flatMap((row) => {
        try { return JSON.parse(row.source_refs || '[]'); } catch (_) { return []; }
      });

      const allSourceRefs = Array.from(new Set([...directSourceRefs, ...neighborSourceRefs])).filter(Boolean).slice(0, 260);

      let rawEvents = [];
      if (allSourceRefs.length) {
        const placeholders = allSourceRefs.map(() => '?').join(',');
        rawEvents = await db.allQuery(
          `SELECT id, type, source_type, timestamp, occurred_at, source, app, title, redacted_text, raw_text, metadata,
                  (CASE
                    WHEN LOWER(COALESCE(source_type, type, '')) LIKE '%screen%' THEN 1
                    WHEN LOWER(COALESCE(source_type, type, '')) LIKE '%capture%' THEN 1
                    ELSE 0
                  END) as is_capture
           FROM events
           WHERE id IN (${placeholders})
           ORDER BY is_capture DESC, COALESCE(occurred_at, timestamp) DESC
           LIMIT 140`,
          allSourceRefs
        ).catch(() => []);
      }

      const captures = rawEvents.filter((e) => Number(e.is_capture || 0) === 1);
      const bestEvents = (captures.length ? captures : rawEvents).slice(0, 28);
      const reconstruction = bestEvents.length > 0
        ? bestEvents.map((e) => {
            const when = e.occurred_at || e.timestamp || 'unknown time';
            const header = [e.title, e.app || e.source].filter(Boolean).join(' • ');
            const body = String(e.redacted_text || e.raw_text || '').replace(/\s+/g, ' ').trim();
            return `[${when}] ${header || e.id}: ${body}`.slice(0, 900);
          }).join('\n---\n')
        : null;

      result = {
        status: 'success',
        output: {
          node,
          edges,
          rawEvents,
          reconstruction,
          source_ref_count: allSourceRefs.length,
          capture_count: captures.length
        }
      };
    } else {
      result = { status: 'error', error: `Node not found: ${input.node_id}` };
    }
  } else if (tool === 'memory_update') {
    const updated = await updateMemoryNode(input.node_id, input.updates);
    if (updated) {
      result = { status: 'success', output: updated };
    } else {
      result = { status: 'error', error: `Failed to update node: ${input.node_id}` };
    }
  } else if (tool === 'memory_link') {
    await upsertMemoryEdge({
      fromNodeId: input.from_id,
      toNodeId: input.to_id,
      edgeType: input.relationship,
      weight: input.weight || 1.0,
      traceLabel: input.trace_label || 'Manual LLM link'
    });
    result = { status: 'success', output: { linked: true } };
  } else if (tool === 'memory_create') {
    const { stableHash: sh } = require('./graph-store');
    const nodeId = `manual_${input.layer}_${sh(`${input.subtype}:${input.title}`)}`;
    const created = await upsertMemoryNode({
      id: nodeId,
      layer: input.layer,
      subtype: input.subtype,
      title: input.title,
      summary: input.summary,
      confidence: input.confidence || 0.9,
      status: 'active',
      metadata: input.metadata || {},
      source_refs: []
    });
    result = { status: 'success', output: { node_id: nodeId, created } };
  } else if (tool === 'action_create') {
    const crypto = require('crypto');
    const actionId = `action_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const nowIso = new Date().toISOString();
    const metadata = {
      category: input.category || 'work',
      priority: input.priority || 'medium',
      ai_generated: input.ai_generated !== false,
      ai_doable: Boolean(input.ai_doable),
      action_type: input.action_type || (input.ai_doable ? 'draft_message' : 'manual_next_step'),
      execution_mode: input.execution_mode || (input.ai_doable ? 'draft_or_execute' : 'manual'),
      assignee: input.assignee || (input.ai_doable ? 'ai' : 'human'),
      suggested_actions: Array.isArray(input.suggested_actions) ? input.suggested_actions : [],
      reason_codes: Array.isArray(input.reason_codes) ? input.reason_codes : ['recursive_action_create'],
      provider: input.provider || 'deepseek',
      created_via: input.created_via || 'recursive_improvement_cycle'
    };

    const cols = await db.allQuery(`PRAGMA table_info(suggestion_artifacts)`).catch(() => []);
    const colSet = new Set((cols || []).map((row) => row?.name).filter(Boolean));
    const hasExpandedSchema = colSet.has('type') && colSet.has('title') && colSet.has('metadata');

    if (hasExpandedSchema) {
      await db.runQuery(
        `INSERT OR REPLACE INTO suggestion_artifacts
         (id, type, title, body, trigger_summary, source_node_ids, source_edge_paths, confidence, status, metadata, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          actionId,
          input.type || 'next_action',
          String(input.title || '').trim(),
          String(input.body || input.description || '').trim(),
          String(input.trigger_summary || input.body || input.title || '').trim(),
          JSON.stringify(Array.isArray(input.source_node_ids) ? input.source_node_ids : []),
          JSON.stringify(Array.isArray(input.source_edge_paths) ? input.source_edge_paths : []),
          Number(input.confidence || 0.75),
          'active',
          JSON.stringify(metadata),
          nowIso
        ]
      );
    } else {
      const legacyData = {
        id: actionId,
        type: input.type || 'next_action',
        title: String(input.title || '').trim(),
        body: String(input.body || input.description || '').trim(),
        trigger_summary: String(input.trigger_summary || input.body || input.title || '').trim(),
        source_node_ids: Array.isArray(input.source_node_ids) ? input.source_node_ids : [],
        source_edge_paths: Array.isArray(input.source_edge_paths) ? input.source_edge_paths : [],
        confidence: Number(input.confidence || 0.75),
        status: 'active',
        created_at: nowIso,
        ...metadata
      };
      await db.runQuery(
        `INSERT OR REPLACE INTO suggestion_artifacts (id, suggestion_id, data) VALUES (?, ?, ?)`,
        [actionId, actionId, JSON.stringify(legacyData)]
      );
    }

    result = {
      status: 'success',
      output: {
        action_id: actionId,
        title: String(input.title || '').trim(),
        category: metadata.category,
        ai_doable: metadata.ai_doable
      }
    };
  } else if (tool === 'contact_create') {
    const { stableHash: sh } = require('./graph-store');
    const name = String(input.name || '').trim();
    const nodeId = `person_${sh(name.toLowerCase())}`;
    const metadata = {
      name,
      email: input.email || null,
      phone: input.phone || null,
      notes: input.notes || null,
      created_via: 'chat'
    };
    await upsertMemoryNode({
      id: nodeId,
      layer: 'semantic',
      subtype: 'person',
      title: name,
      summary: input.notes || `Contact: ${name}`,
      confidence: 0.95,
      status: 'active',
      metadata,
      source_refs: []
    });
    result = { status: 'success', output: { node_id: nodeId, name, metadata } };
  } else if (tool === 'contact_update') {
    const name = String(input.name || '').trim();
    const rows = await db.allQuery(
      `SELECT id, metadata FROM memory_nodes WHERE layer='semantic' AND subtype='person' AND LOWER(title) LIKE LOWER(?) LIMIT 3`,
      [`%${name}%`]
    ).catch(() => []);
    if (!rows.length) {
      result = { status: 'error', error: `Contact not found: ${name}` };
    } else {
      const row = rows[0];
      let existing = {};
      try { existing = JSON.parse(row.metadata || '{}'); } catch (_) {}
      const merged = Object.assign({}, existing, input.updates || {});
      await updateMemoryNode(row.id, { metadata: merged });
      result = { status: 'success', output: { node_id: row.id, name, updated_fields: Object.keys(input.updates || {}) } };
    }
  } else if (tool === 'automation_create') {
    const crypto = require('crypto');
    const autoId = `auto_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const now = new Date().toISOString();
    const intervalMinutes = Math.max(1, parseInt(input.interval_minutes, 10) || 60);
    const nextRunAt = new Date(Date.now() + intervalMinutes * 60 * 1000).toISOString();
    await db.runQuery(
      `INSERT INTO scheduled_automations (id, name, description, prompt, interval_minutes, enabled, next_run_at, created_at, metadata)
       VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)`,
      [autoId, input.name, input.description || '', input.prompt, intervalMinutes, nextRunAt, now, JSON.stringify(input.metadata || {})]
    );
    result = { status: 'success', output: { automation_id: autoId, name: input.name, interval_minutes: intervalMinutes, next_run_at: nextRunAt } };
  } else if (tool === 'automation_list') {
    const rows = await db.allQuery(
      `SELECT id, name, description, interval_minutes, enabled, last_run_at, next_run_at, created_at FROM scheduled_automations ORDER BY created_at DESC LIMIT 50`,
      []
    ).catch(() => []);
    result = { status: 'success', output: { automations: rows } };
  } else {
    result = { status: 'error', error: `Tool not implemented: ${tool}` };
  }

  token?.clearInFlight?.();
  return {
    status: result?.status || 'error',
    tool,
    output: result?.output || result?.observation || result?.result || null,
    error: result?.error || null,
    raw_result: result,
    duration_ms: Date.now() - startedAt,
    execution_class: executionClass
  };
}

function adaptActionToToolRequest(action = {}, context = {}) {
  const kind = String(action.kind || '').toUpperCase();
  if (kind === 'CDP_NAVIGATE' || kind === 'OPEN_URL') {
    return { tool: 'browser_navigate', input: { url: action.url }, execution_class: 'hard_stop' };
  }
  if (kind === 'CDP_CLICK') {
    return { tool: 'browser_click', input: { target_id: action.target_id }, execution_class: 'hard_stop' };
  }
  if (kind === 'CDP_TYPE') {
    return { tool: 'browser_type', input: { target_id: action.target_id, text: action.text || '', submit: Boolean(action.submit) }, execution_class: 'hard_stop' };
  }
  if (kind === 'CDP_SCROLL') return { tool: 'browser_scroll', input: { direction: action.direction || 'down', amount: Number(action.amount || 1) }, execution_class: 'hard_stop' };
  if (kind === 'CDP_WAIT_FOR') return { tool: 'browser_wait', input: { text: action.text || '', timeout_ms: Number(action.timeout_ms || 2500) }, execution_class: 'hard_stop' };
  if (kind === 'READ_UI_STATE' || kind === 'CDP_GET_TREE') return { tool: 'ax_snapshot', input: { observeOptions: context.observeOptions || {} }, execution_class: 'ui_atomic' };
  if (kind === 'CLICK_AX' || kind === 'PRESS_AX') return { tool: 'ax_press', input: { target_id: action.target_id || null, label: action.label || '', role: action.role || '' }, execution_class: 'ui_atomic' };
  if (kind === 'SET_AX_VALUE') return { tool: 'ax_set_value', input: { target_id: action.target_id || null, text: action.text || '' }, execution_class: 'ui_atomic' };
  if (kind === 'SET_VALUE') return { tool: 'ax_set_value', input: { target_id: action.target_id || null, text: action.text || '' }, execution_class: 'ui_atomic' };
  if (kind === 'TYPE_TEXT') return { tool: 'ax_set_value', input: { target_id: action.target_id || null, text: action.text || '' }, execution_class: 'ui_atomic' };
  if (kind === 'KEY_PRESS' || kind === 'CDP_KEY_PRESS') return { tool: 'key_press', input: { key: action.key || 'enter', modifiers: action.modifiers || [] }, execution_class: 'ui_atomic' };
  if (kind === 'SCROLL_AX') return { tool: 'scroll', input: { direction: action.direction || 'down', amount: Number(action.amount || 1) }, execution_class: 'ui_atomic' };
  return null;
}

module.exports = {
  TOOL_SCHEMAS,
  evaluateToolPolicy,
  dispatchTool,
  adaptActionToToolRequest
};
