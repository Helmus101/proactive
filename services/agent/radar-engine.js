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
  let category = 'work';
  let primaryLabel = 'Handle task';
  if (section === 'relationship') {
    category = 'relationship_intelligence';
    primaryLabel = 'Draft opener';
  } else if (section === 'central') {
    category = 'insight';
    primaryLabel = 'View insight';
  }

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
      : section === 'central'
        ? [{ label: 'View insight' }, { label: 'Mark read' }, { label: 'Snooze' }]
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

  const sectionQuery = {
    central: 'What are the top high-level insights or patterns from recent activity?',
    relationship: 'What are the top relationship moves right now?',
    todo: 'What are the top productive to-dos right now?'
  }[section] || 'What are the top signals right now?';

  const prompt = `[System]
You are Weave's radar planner. Return a strict JSON array with up to ${limit} ${section} signals.

Every item must include:
- title: concise headline
- person: (optional) person involved
- why_now: why this is relevant right now
- evidence: specific grounding from memory
- move: concrete next action
- priority: high, medium, or low
- suggestion_type: one of [Nurture, Life Event, Check-in, Task, Opportunity, Insight]

Rules:
- Be concrete and grounded.
- Do not return generic motivation advice.
- Relationship items (Nurture, Life Event, Check-in) must feel like: person/context, why now, supporting evidence, concrete next move.
- Todo items (Task, Opportunity) must be the strongest productive next steps from current memory.
- Central items (Insight, Opportunity) should capture high-level patterns or strategic shifts.
- JSON only.

[Query]
${sectionQuery}

[Relationship candidates]
${relationshipDigest || 'None'}

[Manual todos]
${manualDigest || 'None'}

[Memory evidence]
${evidenceDigest || 'None'}
`;

  const raw = await callLLM(prompt, llmConfig, 0.18, { maxTokens: 700, economy: true, task: 'suggestion' }).catch(() => null);
  const parsed = safeJsonParse(raw, []);
  return Array.isArray(parsed) ? parsed.map((row, index) => {
    const normalized = normalizeSignal(section, row, index);
    if (row.suggestion_type) normalized.suggestion_type = row.suggestion_type;
    return normalized;
  }).slice(0, limit) : [];
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
  - suggestion_type

  [Signals]
  ${JSON.stringify(signals.map((item) => ({
    title: item.title,
    person: item.display?.person || '',
    why_now: item.why_now || item.reason || '',
    evidence: item.evidence || '',
    move: item.recommended_action || '',
    priority: item.priority,
    suggestion_type: item.suggestion_type
  })))}

[Memory evidence]
${(context?.evidence || []).slice(0, 10).map((item, index) => `${index + 1}. ${trim(item.text || item.title || '', 220)}`).join('\n') || 'None'}
`;
  const raw = await callLLM(prompt, llmConfig, 0.12, { maxTokens: 700, economy: true, task: 'suggestion' }).catch(() => null);
  const parsed = safeJsonParse(raw, []);
  if (!Array.isArray(parsed) || !parsed.length) return signals;
  return parsed.map((row, index) => {
    const normalized = normalizeSignal(section, row, index);
    if (row.suggestion_type) normalized.suggestion_type = row.suggestion_type;
    const trace = signals[index]?.epistemic_trace?.length ? signals[index].epistemic_trace : makeTraceFromEvidence(context?.evidence || [], 2);
    return { ...normalized, epistemic_trace: trace };
  }).slice(0, signals.length);
}

async function buildRadarState({ llmConfig = null, manualTodos = [], maxCentralSignals = 3, maxRelationshipSignals = 5, maxTodoSignals = 5 } = {}) {
  const timings = {};
  const relationshipCandidates = await fetchRelationshipCandidates(8);

  const [centralContext, relationshipContext, todoContext] = await Promise.all([
    retrieveBoundedContext('What are the top high-level insights or patterns from recent activity?', {
      seedLimit: 8,
      hopLimit: 4,
      evidenceLimit: 10
    }),
    retrieveBoundedContext('What are the top relationship moves right now?', {
      seedLimit: 8,
      hopLimit: 4,
      evidenceLimit: 10
    }),
    retrieveBoundedContext('What are the top productive to-dos right now?', {
      seedLimit: 8,
      hopLimit: 4,
      evidenceLimit: 10
    })
  ]);

  timings.central_retrieval_ms = centralContext.took_ms;
  timings.relationship_retrieval_ms = relationshipContext.took_ms;
  timings.todo_retrieval_ms = todoContext.took_ms;

  const results = await Promise.all([
    (async () => {
      try {
        let signals = await runSectionLLM({
          section: 'central',
          llmConfig,
          context: centralContext,
          relationshipCandidates,
          manualTodos,
          limit: maxCentralSignals
        });
        if (signals.length) {
          signals = await deepenSignals({
            section: 'central',
            llmConfig,
            signals,
            context: centralContext
          });
        }
        return { section: 'central', signals, error: null };
      } catch (error) {
        return { section: 'central', signals: [], error: String(error?.message || error) };
      }
    })(),
    (async () => {
      try {
        let signals = await runSectionLLM({
          section: 'relationship',
          llmConfig,
          context: relationshipContext,
          relationshipCandidates,
          manualTodos,
          limit: maxRelationshipSignals
        });
        if (!signals.length) {
          signals = makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals);
        }
        signals = await deepenSignals({
          section: 'relationship',
          llmConfig,
          signals,
          context: relationshipContext
        });
        return { section: 'relationship', signals, error: null };
      } catch (error) {
        return { section: 'relationship', signals: makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals), error: String(error?.message || error) };
      }
    })(),
    (async () => {
      try {
        let signals = await runSectionLLM({
          section: 'todo',
          llmConfig,
          context: todoContext,
          relationshipCandidates,
          manualTodos,
          limit: maxTodoSignals
        });
        if (!signals.length) {
          signals = makeTodoFallback(manualTodos, todoContext.retrieval, maxTodoSignals);
        }
        signals = await deepenSignals({
          section: 'todo',
          llmConfig,
          signals,
          context: todoContext
        });
        return { section: 'todo', signals, error: null };
      } catch (error) {
        return { section: 'todo', signals: makeTodoFallback(manualTodos, todoContext.retrieval, maxTodoSignals), error: String(error?.message || error) };
      }
    })()
  ]);

  const centralResult = results.find(r => r.section === 'central');
  const relationshipResult = results.find(r => r.section === 'relationship');
  const todoResult = results.find(r => r.section === 'todo');

  const centralSignals = centralResult.signals.map((item) => ({
    ...item,
    signal_type: 'central',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(centralContext.evidence, 2)
  }));
  const relationshipSignals = relationshipResult.signals.map((item) => ({
    ...item,
    signal_type: 'relationship',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(relationshipContext.evidence, 2)
  }));
  const todoSignals = todoResult.signals.map((item) => ({
    ...item,
    signal_type: 'todo',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(todoContext.evidence, 2)
  }));

  const allSignals = [...centralSignals, ...relationshipSignals, ...todoSignals]
    .sort((a, b) => {
      const weight = { high: 3, medium: 2, low: 1 };
      const delta = (weight[b.priority] || 0) - (weight[a.priority] || 0);
      if (delta !== 0) return delta;
      return Number(b.createdAt || 0) - Number(a.createdAt || 0);
    });

  return {
    generated_at: nowIso(),
    allSignals,
    centralSignals,
    relationshipSignals,
    todoSignals,
    sections: {
      central: { status: centralResult.error ? 'partial' : 'ready', error: centralResult.error, count: centralSignals.length },
      relationship: { status: relationshipResult.error ? 'partial' : 'ready', error: relationshipResult.error, count: relationshipSignals.length },
      todo: { status: todoResult.error ? 'partial' : 'ready', error: todoResult.error, count: todoSignals.length }
    },
    timings
  };
}

module.exports = {
  buildRadarState
};
