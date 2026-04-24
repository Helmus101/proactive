const crypto = require('crypto');
const db = require('../db');
const { callLLM } = require('./intelligence-engine');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');

function nowIso() {
  return new Date().toISOString();
}

function stableId(prefix, value) {
  return `${prefix}_${crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 18)}`;
}

function safeJsonParse(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch (_) {
    return fallback;
  }
}

function asList(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return [value];
}

function uniq(values = [], limit = 12) {
  return Array.from(new Set((values || []).filter(Boolean))).slice(0, limit);
}

function trim(value = '', max = 220) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text.length > max ? `${text.slice(0, Math.max(0, max - 1)).trim()}...` : text;
}

function makeTraceFromEvidence(evidence = [], limit = 3) {
  return (Array.isArray(evidence) ? evidence : []).slice(0, limit).map((item) => ({
    source: item.source || item.layer || item.type || 'memory',
    text: trim(item.text || item.title || '', 180),
    node_id: item.id || item.node_id || item.event_id || null
  })).filter((item) => item.text);
}

function makeRelationshipFallback(contactRows = [], limit = 5) {
  return (contactRows || []).slice(0, limit).map((contact, index) => {
    const person = String(contact.display_name || '').trim();
    const whyNow = [
      contact.status ? `${contact.status.replace(/_/g, ' ')} relationship` : '',
      contact.last_interaction_at ? `last interaction ${contact.last_interaction_at.slice(0, 10)}` : '',
      contact.relationship_summary || ''
    ].filter(Boolean).join('. ');
    const move = contact.status === 'needs_followup'
      ? `Send ${person} a short follow-up tied to your last conversation`
      : `Reconnect with ${person} using a specific reference from your shared context`;
    return {
      id: stableId('radar_rel', `${person}|${contact.last_interaction_at || index}`),
      title: person ? `${person} needs a relationship move` : `Relationship move ${index + 1}`,
      category: 'relationship_intelligence',
      signal_type: 'relationship',
      priority: index < 2 ? 'high' : 'medium',
      why_now: trim(whyNow || 'The relationship graph suggests this tie needs attention.', 220),
      evidence: trim(contact.relationship_summary || `${person} appears in the relationship graph as a current follow-up candidate.`, 220),
      move,
      person,
      recommended_action: move,
      primary_action: { label: 'Draft opener' },
      suggested_actions: [{ label: 'Draft opener' }, { label: 'View relationship' }, { label: 'Snooze' }],
      display: {
        person,
        headline: person ? `${person} needs attention` : 'Relationship move',
        summary: trim(whyNow || move, 180)
      },
      epistemic_trace: [
        {
          source: 'relationship_graph',
          text: trim(contact.relationship_summary || `${person} is currently marked ${contact.status || 'active'}.`, 180),
          node_id: contact.id || null
        }
      ],
      createdAt: Date.now(),
      ai_generated: true
    };
  });
}

function makeTodoFallback(manualTodos = [], retrieval = null, limit = 5) {
  const existing = (manualTodos || []).filter((todo) => todo && !todo.completed).slice(0, limit);
  if (existing.length) {
    return existing.map((todo, index) => ({
      id: todo.id || stableId('radar_todo', `${todo.title}|${index}`),
      title: todo.title || `Todo ${index + 1}`,
      category: 'work',
      signal_type: 'todo',
      priority: todo.priority || 'medium',
      why_now: trim(todo.description || todo.reason || 'This task is already in your manual to-do list.', 220),
      evidence: trim(todo.description || todo.reason || todo.title || '', 220),
      move: todo.title || 'Handle this task',
      person: '',
      recommended_action: todo.title || 'Handle task',
      primary_action: { label: 'Handle task' },
      suggested_actions: [{ label: 'Handle task' }, { label: 'Mark done' }, { label: 'Snooze' }],
      display: {
        person: '',
        headline: todo.title || `Todo ${index + 1}`,
        summary: trim(todo.description || todo.reason || '', 180)
      },
      epistemic_trace: [],
      createdAt: Date.now(),
      ai_generated: true
    }));
  }

  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  return evidence.slice(0, limit).map((item, index) => ({
    id: stableId('radar_todo', `${item.id || item.title}|${index}`),
    title: trim(item.title || item.text || `Todo ${index + 1}`, 72),
    category: 'work',
    signal_type: 'todo',
    priority: index < 2 ? 'high' : 'medium',
    why_now: 'This is one of the strongest recent work signals in memory.',
    evidence: trim(item.text || item.title || '', 220),
    move: trim(item.title || item.text || 'Handle the next step here', 120),
    person: '',
    recommended_action: trim(item.title || 'Handle task', 100),
    primary_action: { label: 'Handle task' },
    suggested_actions: [{ label: 'Handle task' }, { label: 'Mark done' }, { label: 'Snooze' }],
    display: {
      person: '',
      headline: trim(item.title || `Todo ${index + 1}`, 80),
      summary: trim(item.text || '', 180)
    },
    epistemic_trace: makeTraceFromEvidence([item], 1),
    createdAt: Date.now(),
    ai_generated: true
  }));
}

function normalizeSignal(section, row = {}, index = 0) {
  const person = trim(row.person || row.contact || row.target || '', 80);
  const whyNow = trim(row.why_now || row.whyNow || row.reason || row.summary || '', 240);
  const evidence = trim(row.evidence || row.supporting_evidence || row.support || '', 240);
  const move = trim(row.move || row.suggested_move || row.action || row.next_step || '', 180);
  const title = trim(row.title || (person ? `${person} needs a move` : `${section} signal ${index + 1}`), 90);
  const category = section === 'relationship' ? 'relationship_intelligence' : 'work';
  const primaryLabel = section === 'relationship' ? 'Draft opener' : 'Handle task';
  return {
    id: row.id || stableId(`radar_${section}`, `${title}|${person}|${index}`),
    title,
    category,
    signal_type: section,
    priority: String(row.priority || (index < 2 ? 'high' : 'medium')).toLowerCase(),
    description: whyNow,
    reason: whyNow,
    why_now: whyNow,
    evidence,
    move,
    recommended_action: move || primaryLabel,
    display: {
      person,
      headline: title,
      summary: trim(whyNow || move || evidence, 180)
    },
    primary_action: { label: primaryLabel },
    suggested_actions: section === 'relationship'
      ? [{ label: 'Draft opener' }, { label: 'View relationship' }, { label: 'Snooze' }]
      : [{ label: 'Handle task' }, { label: 'Mark done' }, { label: 'Snooze' }],
    epistemic_trace: Array.isArray(row.epistemic_trace) && row.epistemic_trace.length ? row.epistemic_trace : [],
    createdAt: Date.now(),
    ai_generated: true
  };
}

async function retrieveBoundedContext(query, options = {}) {
  const startedAt = Date.now();
  const retrieval = await buildHybridGraphRetrieval({
    query,
    options: { mode: 'suggestion', strategy: 'spiral' },
    seedLimit: Number(options.seedLimit || 10),
    hopLimit: Number(options.hopLimit || 4)
  }).catch(() => null);

  return {
    retrieval,
    took_ms: Date.now() - startedAt,
    evidence: Array.isArray(retrieval?.evidence) ? retrieval.evidence.slice(0, Number(options.evidenceLimit || 12)) : []
  };
}

async function fetchRelationshipCandidates(limit = 8) {
  const rows = await db.allQuery(
    `SELECT id, display_name, status, relationship_summary, last_interaction_at, strength_score, warmth_score, depth_score
     FROM relationship_contacts
     WHERE json_extract(COALESCE(metadata, '{}'), '$.apple_contacts') = 1
     ORDER BY CASE status WHEN 'decaying' THEN 0 WHEN 'cooling' THEN 1 WHEN 'needs_followup' THEN 2 ELSE 3 END,
              depth_score DESC,
              strength_score DESC
     LIMIT ?`,
    [Math.max(1, Math.min(12, Number(limit || 8)))]
  ).catch(() => []);
  return rows || [];
}

async function runSectionLLM({ section, llmConfig, context, relationshipCandidates = [], manualTodos = [], limit = 5 }) {
  const evidenceDigest = (context?.evidence || []).slice(0, 10).map((item, index) => (
    `${index + 1}. [${item.layer || item.type || 'memory'}] ${trim(item.text || item.title || '', 220)}`
  )).join('\n');
  const relationshipDigest = relationshipCandidates.slice(0, 8).map((item) => (
    `- ${item.display_name}: ${item.status}; ${trim(item.relationship_summary || '', 140)}`
  )).join('\n');
  const manualDigest = (manualTodos || []).filter((todo) => todo && !todo.completed).slice(0, 8).map((todo) => (
    `- ${trim(todo.title || '', 90)}${todo.description ? `: ${trim(todo.description, 140)}` : ''}`
  )).join('\n');

  const prompt = `[System]
You are Weave's radar planner. Return a strict JSON array with up to ${limit} ${section} signals.

For relationship signals, every item must include:
- title
- person
- why_now
- evidence
- move
- priority

For todo signals, every item must include:
- title
- why_now
- evidence
- move
- priority

Rules:
- Be concrete and grounded.
- Do not return generic motivation advice.
- Relationship items must feel like: person/context, why now, supporting evidence, concrete next move.
- Todo items must be the strongest productive next steps from current memory.
- JSON only.

[Query]
${section === 'relationship' ? 'What are the top relationship moves right now?' : 'What are the top productive to-dos right now?'}

[Relationship candidates]
${relationshipDigest || 'None'}

[Manual todos]
${manualDigest || 'None'}

[Memory evidence]
${evidenceDigest || 'None'}
`;

  const raw = await callLLM(prompt, llmConfig, 0.18, { maxTokens: 700, economy: true, task: 'suggestion' }).catch(() => null);
  const parsed = safeJsonParse(raw, []);
  return Array.isArray(parsed) ? parsed.map((row, index) => normalizeSignal(section, row, index)).slice(0, limit) : [];
}

async function deepenSignals({ section, llmConfig, signals = [], context }) {
  if (!signals.length || !llmConfig) return signals;
  const prompt = `[System]
You are Weave's radar expander. Rewrite each signal into stronger grounded form and return strict JSON array.
Keep the same number of items.
For each item return:
- title
- person
- why_now
- evidence
- move
- priority

[Signals]
${JSON.stringify(signals.map((item) => ({
    title: item.title,
    person: item.display?.person || '',
    why_now: item.why_now || item.reason || '',
    evidence: item.evidence || '',
    move: item.recommended_action || '',
    priority: item.priority
  })))}

[Memory evidence]
${(context?.evidence || []).slice(0, 10).map((item, index) => `${index + 1}. ${trim(item.text || item.title || '', 220)}`).join('\n') || 'None'}
`;
  const raw = await callLLM(prompt, llmConfig, 0.12, { maxTokens: 700, economy: true, task: 'suggestion' }).catch(() => null);
  const parsed = safeJsonParse(raw, []);
  if (!Array.isArray(parsed) || !parsed.length) return signals;
  return parsed.map((row, index) => {
    const normalized = normalizeSignal(section, row, index);
    const trace = signals[index]?.epistemic_trace?.length ? signals[index].epistemic_trace : makeTraceFromEvidence(context?.evidence || [], 2);
    return { ...normalized, epistemic_trace: trace };
  }).slice(0, signals.length);
}

async function buildRadarState({ llmConfig = null, manualTodos = [], maxRelationshipSignals = 5, maxTodoSignals = 5 } = {}) {
  const timings = {};
  const relationshipCandidates = await fetchRelationshipCandidates(8);

  const relationshipContext = await retrieveBoundedContext('What are the top relationship moves right now?', {
    seedLimit: 8,
    hopLimit: 4,
    evidenceLimit: 10
  });
  timings.relationship_retrieval_ms = relationshipContext.took_ms;

  const todoContext = await retrieveBoundedContext('What are the top productive to-dos right now?', {
    seedLimit: 8,
    hopLimit: 4,
    evidenceLimit: 10
  });
  timings.todo_retrieval_ms = todoContext.took_ms;

  let relationshipSignals = [];
  let todoSignals = [];
  let relationshipError = null;
  let todoError = null;

  try {
    relationshipSignals = await runSectionLLM({
      section: 'relationship',
      llmConfig,
      context: relationshipContext,
      relationshipCandidates,
      manualTodos,
      limit: maxRelationshipSignals
    });
    if (!relationshipSignals.length) {
      relationshipSignals = makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals);
    }
    relationshipSignals = await deepenSignals({
      section: 'relationship',
      llmConfig,
      signals: relationshipSignals,
      context: relationshipContext
    });
  } catch (error) {
    relationshipError = String(error?.message || error);
    relationshipSignals = makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals);
  }

  try {
    todoSignals = await runSectionLLM({
      section: 'todo',
      llmConfig,
      context: todoContext,
      relationshipCandidates,
      manualTodos,
      limit: maxTodoSignals
    });
    if (!todoSignals.length) {
      todoSignals = makeTodoFallback(manualTodos, todoContext.retrieval, maxTodoSignals);
    }
    todoSignals = await deepenSignals({
      section: 'todo',
      llmConfig,
      signals: todoSignals,
      context: todoContext
    });
  } catch (error) {
    todoError = String(error?.message || error);
    todoSignals = makeTodoFallback(manualTodos, todoContext.retrieval, maxTodoSignals);
  }

  relationshipSignals = relationshipSignals.map((item) => ({
    ...item,
    signal_type: 'relationship',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(relationshipContext.evidence, 2)
  }));
  todoSignals = todoSignals.map((item) => ({
    ...item,
    signal_type: 'todo',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(todoContext.evidence, 2)
  }));

  const allSignals = [...relationshipSignals, ...todoSignals]
    .sort((a, b) => {
      const weight = { high: 3, medium: 2, low: 1 };
      const delta = (weight[b.priority] || 0) - (weight[a.priority] || 0);
      if (delta !== 0) return delta;
      return Number(b.createdAt || 0) - Number(a.createdAt || 0);
    });

  return {
    generated_at: nowIso(),
    allSignals,
    relationshipSignals,
    todoSignals,
    sections: {
      relationship: { status: relationshipError ? 'partial' : 'ready', error: relationshipError, count: relationshipSignals.length },
      todo: { status: todoError ? 'partial' : 'ready', error: todoError, count: todoSignals.length }
    },
    timings
  };
}

module.exports = {
  buildRadarState
};
