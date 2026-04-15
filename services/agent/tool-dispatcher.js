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
  memory_link: ['from_id', 'to_id', 'relationship']
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
      
      // Reconstruction logic: fetch raw events if it's an episode or linked to one
      let rawEvents = [];
      const sourceRefs = (() => {
        try { return JSON.parse(node.source_refs || '[]'); } catch (_) { return []; }
      })();
      
      if (sourceRefs.length) {
        const placeholders = sourceRefs.map(() => '?').join(',');
        rawEvents = await db.allQuery(
          `SELECT id, type, timestamp, source, title, redacted_text, raw_text, metadata, 
                  (CASE WHEN type = 'ScreenCapture' THEN 1 ELSE 0 END) as is_capture
           FROM events 
           WHERE id IN (${placeholders}) 
           ORDER BY is_capture DESC, timestamp DESC 
           LIMIT 40`,
          sourceRefs
        ).catch(() => []);
      }

      // Enhanced reconstruction: if multiple captures exist, try to build a timeline snippet
      const captures = rawEvents.filter(e => e.type === 'ScreenCapture' || e.is_capture);
      const reconstruction = captures.length > 0 
        ? captures.map(c => `[${c.timestamp}] ${c.title}: ${c.redacted_text || c.raw_text || ''}`).join('\n---\n')
        : null;

      result = { status: 'success', output: { node, edges, rawEvents, reconstruction } };
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

