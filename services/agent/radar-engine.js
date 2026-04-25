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

function safeLower(value = '') {
  return String(value || '').trim().toLowerCase();
}

function formatEvidenceSnippet(item = {}, max = 220) {
  const app = trim(item.app || item.source_app || item.source || item.layer || '', 40);
  const timestamp = item.timestamp ? new Date(item.timestamp).toLocaleString() : '';
  const title = trim(item.title || '', 80);
  const text = trim(item.text || item.summary || item.description || item.title || '', max);
  const prefix = [app, timestamp].filter(Boolean).join(' @ ');
  return `${prefix ? `[${prefix}] ` : ''}${title && title !== text ? `${title} - ` : ''}${text}`.trim();
}

function makeTraceFromEvidence(evidence = [], limit = 3) {
  return (Array.isArray(evidence) ? evidence : []).slice(0, limit).map((item) => ({
    source: item.source || item.layer || item.type || 'memory',
    text: trim(item.text || item.title || '', 180),
    node_id: item.id || item.node_id || item.event_id || null
  })).filter((item) => item.text);
}

function makeRelationshipFallback(contactRows = [], limit = 5, existingSignals = []) {
  const existingPersons = new Set(
    existingSignals
      .map(s => String(s.person || s.display?.person || '').trim().toLowerCase())
      .filter(Boolean)
  );

  return (contactRows || [])
    .filter(contact => {
      const person = String(contact.display_name || '').trim();
      return !existingPersons.has(person.toLowerCase());
    })
    .slice(0, limit)
    .map((contact, index) => {
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

function makeRelationshipEvidenceFallback(retrieval = null, limit = 5, existingSignals = []) {
  const existingTitles = new Set(
    existingSignals
      .map(s => String(s.title || '').trim().toLowerCase())
      .filter(Boolean)
  );
  const evidence = Array.isArray(retrieval?.evidence) ? retrieval.evidence : [];
  return evidence
    .filter(item => {
      const title = String(item.title || item.text || '').trim().toLowerCase();
      return title && !existingTitles.has(title);
    })
    .slice(0, limit)
    .map((item, index) => {
      const baseTitle = trim(item.title || item.text || `Relationship move ${index + 1}`, 72);
    const move = `Follow up on ${baseTitle.toLowerCase()} with a specific personal reference from recent context`;
    return {
      id: stableId('radar_rel_evidence', `${item.id || item.title || index}`),
      title: baseTitle,
      category: 'relationship_intelligence',
      signal_type: 'relationship',
      priority: index < 2 ? 'high' : 'medium',
      why_now: 'This surfaced from recent memory as a relationship-relevant thread worth acting on now.',
      evidence: trim(item.text || item.title || '', 220),
      move,
      person: '',
      recommended_action: move,
      primary_action: { label: 'Draft opener' },
      suggested_actions: [{ label: 'Draft opener' }, { label: 'View relationship' }, { label: 'Snooze' }],
      display: {
        person: '',
        headline: baseTitle,
        summary: trim(item.text || item.title || '', 180)
      },
      epistemic_trace: makeTraceFromEvidence([item], 1),
      createdAt: Date.now(),
      ai_generated: true
    };
  });
}

function makeTodoFallback(manualTodos = [], retrieval = null, limit = 5, existingSignals = []) {
  const existingTitles = new Set(
    existingSignals
      .map(s => String(s.title || '').trim().toLowerCase())
      .filter(Boolean)
  );

  const existing = (manualTodos || [])
    .filter((todo) => {
      if (!todo || todo.completed) return false;
      const title = String(todo.title || '').trim().toLowerCase();
      return !existingTitles.has(title);
    })
    .slice(0, limit);

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
  return evidence
    .filter(item => {
      const title = String(item.title || item.text || '').trim().toLowerCase();
      return title && !existingTitles.has(title);
    })
    .slice(0, limit)
    .map((item, index) => ({
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
    person,
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
     ORDER BY CASE status WHEN 'decaying' THEN 0 WHEN 'cooling' THEN 1 WHEN 'needs_followup' THEN 2 ELSE 3 END,
              depth_score DESC,
              strength_score DESC
     LIMIT ?`,
    [Math.max(1, Math.min(12, Number(limit || 8)))]
  ).catch(() => []);
  return rows || [];
}

async function runSectionLLM({ section, llmConfig, context, relationshipCandidates = [], manualTodos = [], limit = 5, existingSignals = [] }) {
  const evidenceDigest = (context?.evidence || []).slice(0, 10).map((item, index) => (
    `${index + 1}. [${item.layer || item.type || 'memory'}] ${trim(item.text || item.title || '', 220)}`
  )).join('\n');
  const relationshipDigest = relationshipCandidates.slice(0, 8).map((item) => (
    `- ${item.display_name}: ${item.status}; ${trim(item.relationship_summary || '', 140)}`
  )).join('\n');
  const manualDigest = (manualTodos || []).filter((todo) => todo && !todo.completed).slice(0, 8).map((todo) => (
    `- ${trim(todo.title || '', 90)}${todo.description ? `: ${trim(todo.description, 140)}` : ''}`
  )).join('\n');
  const existingDigest = (existingSignals || []).map((s) => (
    `- ${s.title}${s.person ? ` (with ${s.person})` : ''}`
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
- DO NOT repeat or duplicate any of these existing signals:
${existingDigest || 'None'}
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

function buildRadarChatQuery(section, limit = 5) {
  if (section === 'relationship') {
    return `What are my top ${limit} relationship moves right now? Use my captured context and relationship memory. For each move, reason in terms of: the person or context, why now, the evidence that makes this timely, and the concrete next move I should make. Avoid generic reminders.`;
  }
  if (section === 'todo') {
    return `What are the top ${limit} to-dos I should do next? Use my captured context and current memory. Focus on the strongest concrete productive moves, with why now and the next step. Avoid generic productivity advice.`;
  }
  return `What are the top ${limit} signals right now from my current memory and captured context?`;
}

async function runChatBackedSection({ section, llmConfig, relationshipCandidates = [], manualTodos = [], limit = 5, existingSignals = [] }) {
  if (!llmConfig?.apiKey) return { answer: null, retrieval: null, thinking_trace: null, took_ms: 0, signals: [] };
  const startedAt = Date.now();
  const { answerChatQuery } = require('./chat-engine');
  const query = buildRadarChatQuery(section, limit);
  const chatResult = await answerChatQuery({
    apiKey: llmConfig.apiKey,
    query,
    options: {
      mode: 'chat',
      economy: true,
      internal_radar: true,
      skip_radar_backfill: true
    }
  }).catch((error) => ({
    content: '',
    retrieval: null,
    thinking_trace: null,
    error: error?.message || String(error)
  }));

  const evidenceDigest = (chatResult?.retrieval?.evidence || []).slice(0, 10).map((item, index) => (
    `${index + 1}. [${item.layer || item.type || 'memory'}] ${trim(item.text || item.title || '', 220)}`
  )).join('\n');
  const relationshipDigest = relationshipCandidates.slice(0, 8).map((item) => (
    `- ${item.display_name}: ${item.status}; ${trim(item.relationship_summary || '', 140)}`
  )).join('\n');
  const manualDigest = (manualTodos || []).filter((todo) => todo && !todo.completed).slice(0, 8).map((todo) => (
    `- ${trim(todo.title || '', 90)}${todo.description ? `: ${trim(todo.description, 140)}` : ''}`
  )).join('\n');
  const existingDigest = (existingSignals || []).map((s) => (
    `- ${s.title}${s.person ? ` (with ${s.person})` : ''}`
  )).join('\n');

  const prompt = `[System]
Convert this grounded Weave answer into a strict JSON array with up to ${limit} ${section} signal cards.

Every item must include:
- title
- person
- why_now
- evidence
- move
- priority
- suggestion_type

Rules:
- JSON only.
- Preserve the assistant's actual judgment. Do not invent extra moves.
- Relationship items must match: person/context, why now, supporting evidence, concrete next move.
- Todo items must match: strongest concrete productive move, why now, supporting evidence, concrete next step.
- Keep wording specific and grounded in the provided answer/evidence.
- Avoid duplicates of these existing signals:
${existingDigest || 'None'}

[Assistant answer]
${String(chatResult?.content || '').trim() || 'None'}

[Relationship candidates]
${relationshipDigest || 'None'}

[Manual todos]
${manualDigest || 'None'}

[Grounding evidence]
${evidenceDigest || 'None'}
`;

  const raw = await callLLM(prompt, llmConfig, 0.12, { maxTokens: 700, economy: true, task: 'suggestion' }).catch(() => null);
  const parsed = safeJsonParse(raw, []);
  const signals = Array.isArray(parsed)
    ? parsed.map((row, index) => {
        const normalized = normalizeSignal(section, row, index);
        if (row.suggestion_type) normalized.suggestion_type = row.suggestion_type;
        return normalized;
      }).slice(0, limit)
    : [];

  return {
    answer: chatResult?.content || '',
    retrieval: chatResult?.retrieval || null,
    thinking_trace: chatResult?.thinking_trace || chatResult?.retrieval?.thinking_trace || null,
    took_ms: Date.now() - startedAt,
    signals,
    error: chatResult?.error || null
  };
}

function buildGoldSetContext(evidence = [], limit = 5) {
  return (Array.isArray(evidence) ? evidence : [])
    .slice(0, Math.max(1, limit))
    .map((item, index) => `${index + 1}. ${formatEvidenceSnippet(item, 260)}`)
    .join('\n');
}

function buildRecentInterestsDigest(primaryEvidence = [], secondaryEvidence = [], limit = 6) {
  const seen = new Set();
  const values = [];
  [...(primaryEvidence || []), ...(secondaryEvidence || [])].forEach((item) => {
    const candidate = trim(item.title || item.text || '', 120);
    const key = safeLower(candidate);
    if (!candidate || seen.has(key)) return;
    seen.add(key);
    values.push(candidate);
  });
  return values.slice(0, limit).map((item) => `- ${item}`).join('\n');
}

async function fetchProjectNodes(limit = 8) {
  const rows = await db.allQuery(
    `SELECT title, summary, created_at
     FROM memory_nodes
     WHERE layer = 'semantic'
       AND subtype IN ('task', 'fact', 'decision')
       AND status NOT IN ('done', 'archived')
     ORDER BY datetime(COALESCE(updated_at, created_at)) DESC
     LIMIT ?`,
    [Math.max(1, Math.min(12, Number(limit || 8)))]
  ).catch(() => []);
  return (rows || []).map((row) => trim(row.title || row.summary || '', 120)).filter(Boolean);
}

function buildRelationshipScore(candidate = {}) {
  const parts = [];
  if (candidate.status) parts.push(`status=${candidate.status}`);
  if (candidate.warmth_score !== undefined && candidate.warmth_score !== null) parts.push(`warmth=${Number(candidate.warmth_score).toFixed(2)}`);
  if (candidate.strength_score !== undefined && candidate.strength_score !== null) parts.push(`strength=${Number(candidate.strength_score).toFixed(2)}`);
  if (candidate.depth_score !== undefined && candidate.depth_score !== null) parts.push(`depth=${Number(candidate.depth_score).toFixed(2)}`);
  return parts.join(', ') || 'unknown';
}

function filterEvidenceForContact(evidence = [], contactName = '', limit = 5) {
  const lowerName = safeLower(contactName);
  const byName = (Array.isArray(evidence) ? evidence : []).filter((item) => {
    const haystack = safeLower(`${item.title || ''} ${item.text || ''}`);
    return lowerName && haystack.includes(lowerName);
  });
  const selected = byName.length ? byName : (Array.isArray(evidence) ? evidence : []);
  return selected.slice(0, Math.max(1, limit));
}

function parseRelationalNudge(raw = '') {
  const text = String(raw || '').trim();
  const nudgeMatch = text.match(/\*\*The Nudge:\*\*\s*([\s\S]*?)(?:\n\*\*Draft:\*\*|$)/i);
  const draftMatch = text.match(/\*\*Draft:\*\*\s*"?([\s\S]*?)"?\s*$/i);
  return {
    nudge: trim((nudgeMatch?.[1] || '').replace(/\s+/g, ' ').trim(), 220),
    draft: trim((draftMatch?.[1] || '').replace(/\s+/g, ' ').trim(), 240)
  };
}

function parseRelationalNudgeArray(raw = '') {
  const parsed = safeJsonParse(raw, []);
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.items)) return parsed.items;
  return [];
}

function parseActionableContextTable(raw = '') {
  const lines = String(raw || '').split('\n').map((line) => line.trim()).filter(Boolean);
  return lines
    .filter((line) => line.startsWith('|'))
    .filter((line) => !/^\|\s*:?-+:?\s*\|/i.test(line))
    .filter((line) => !/^\|\s*Priority\s*\|/i.test(line))
    .map((line) => line.split('|').slice(1, -1).map((cell) => cell.trim()))
    .filter((cells) => cells.length >= 4)
    .map(([priority, task, context, source]) => ({
      priority: safeLower(priority) || 'medium',
      task: trim(task, 120),
      context: trim(context, 220),
      source: trim(source, 120)
    }))
    .filter((row) => row.task);
}

async function generateRelationshipSignalsFromPrompt({
  llmConfig,
  relationshipCandidates = [],
  relationshipRetrieval = null,
  recentInterestEvidence = [],
  limit = 5,
  existingSignals = []
}) {
  if (!llmConfig) return [];
  const existingPeople = new Set(existingSignals.map((item) => safeLower(item.person || item.display?.person || item.title)).filter(Boolean));
  const candidates = relationshipCandidates
    .filter((item) => !existingPeople.has(safeLower(item.display_name)))
    .slice(0, Math.max(limit, 5));
  const recentInterests = buildRecentInterestsDigest(recentInterestEvidence, relationshipRetrieval?.evidence || [], 6) || 'None';
  const candidateBundles = candidates.map((candidate, index) => {
    const contactName = trim(candidate.display_name || '', 80);
    const goldSet = filterEvidenceForContact(relationshipRetrieval?.evidence || [], contactName, 5);
    return {
      rank: index + 1,
      person: contactName,
      relationship_score: buildRelationshipScore(candidate),
      status: candidate.status || '',
      gold_set_context: buildGoldSetContext(goldSet.length ? goldSet : [{
        title: contactName,
        text: candidate.relationship_summary || `${contactName} is currently a relationship candidate in Weave.`,
        source: 'relationship_graph',
        timestamp: candidate.last_interaction_at || null
      }], 5),
      evidence_line: formatEvidenceSnippet(goldSet[0] || {
        text: candidate.relationship_summary || '',
        source: 'relationship_graph',
        timestamp: candidate.last_interaction_at || null
      }, 220),
      epistemic_trace: makeTraceFromEvidence(goldSet, 2)
    };
  }).filter((item) => item.person);

  if (!candidateBundles.length) return [];

  const prompt = `### TASK
You are generating batched Proactive Nudges for multiple relationship candidates.

### USER'S CURRENT FOCUS
${recentInterests}

### CANDIDATES
${candidateBundles.map((bundle) => `## ${bundle.rank}. ${bundle.person}
Relationship Score: ${bundle.relationship_score}
Context Evidence:
${bundle.gold_set_context}`).join('\n\n')}

### INSTRUCTIONS
1. For each candidate, identify the best "Shared Context" between the user's recent focus and that person's known interests or past conversations.
2. Determine urgency from the provided Relationship Score.
3. Draft the outreach:
   - Must be < 3 sentences.
   - Must mention a SPECIFIC detail from the context.
   - Must provide a "Value Add" tied to the context when possible.
4. Avoid "Just checking in," "How are things?", and "It's been a while."
5. Return only the strongest ${limit} candidates.

### OUTPUT FORMAT
Return strict JSON array. Each item must include:
- person
- nudge
- draft
- priority
- suggestion_type`;

  const raw = await callLLM(prompt, llmConfig, 0.18, { maxTokens: 900, economy: true, task: 'suggestion' }).catch(() => null);
  const parsedItems = parseRelationalNudgeArray(raw).slice(0, limit);
  return parsedItems.map((item, index) => {
    const person = trim(item.person || '', 80);
    const bundle = candidateBundles.find((candidate) => safeLower(candidate.person) === safeLower(person)) || candidateBundles[index];
    const nudge = trim(item.nudge || item.why_now || '', 220);
    const draft = trim(item.draft || item.move || '', 240);
    if (!person || !nudge || !draft) return null;
    return {
      id: stableId('radar_relationship_prompt', `${person}|${nudge}`),
      title: `${person} relationship move`,
      person,
      category: 'relationship_intelligence',
      signal_type: 'relationship',
      priority: ['high', 'medium', 'low'].includes(safeLower(item.priority)) ? safeLower(item.priority) : (bundle?.status === 'needs_followup' ? 'high' : 'medium'),
      why_now: nudge,
      evidence: bundle?.evidence_line || '',
      move: draft,
      recommended_action: draft,
      suggestion_type: trim(item.suggestion_type || (bundle?.status === 'needs_followup' ? 'Critical Follow-up' : 'Casual Re-engagement'), 60),
      primary_action: { label: 'Draft opener' },
      suggested_actions: [{ label: 'Draft opener' }, { label: 'View relationship' }, { label: 'Snooze' }],
      display: {
        person,
        headline: `${person} relationship move`,
        summary: trim(nudge, 180)
      },
      epistemic_trace: bundle?.epistemic_trace || [],
      createdAt: Date.now(),
      ai_generated: true,
      prompt_native: true
    };
  }).filter(Boolean);
}

async function generateTodoSignalsFromPrompt({
  llmConfig,
  todoRetrieval = null,
  projectNodes = [],
  limit = 5,
  timeWindow = '6 hours',
  existingSignals = []
}) {
  if (!llmConfig) return [];
  const goldSet = Array.isArray(todoRetrieval?.evidence) ? todoRetrieval.evidence.slice(0, 7) : [];
  if (!goldSet.length) return [];
  const existingTitles = new Set(existingSignals.map((item) => safeLower(item.title)).filter(Boolean));
  const prompt = `### TASK
Identify unresolved tasks and blockers from the last ${timeWindow} of activity.

### PROJECT HIERARCHY
${(projectNodes || []).map((item) => `- ${item}`).join('\n') || 'None'}

### EVIDENCE STREAM
${buildGoldSetContext(goldSet, 7)}

### INSTRUCTIONS
1. FILTER THE NOISE: Ignore passive consumption. Focus on intent-heavy apps like Slack, Terminal, VS Code, and Gmail.
2. DETECT "GHOST TASKS": Look for:
   - Unanswered questions in Slack/Email.
   - Errors or "TODO" comments in code files viewed.
   - Mentioned deadlines in project docs.
3. PRIORITIZE: Rank tasks based on their alignment with the Project Hierarchy.
4. PROVIDE RECEIPTS: Every suggested task must be accompanied by a "Source Link" (the App and Timestamp from the metadata).

### OUTPUT FORMAT
| Priority | Task | Context / "Why" | Source (App + Time) |
| :--- | :--- | :--- | :--- |`;

  const raw = await callLLM(prompt, llmConfig, 0.14, { maxTokens: 520, economy: true, task: 'suggestion' }).catch(() => null);
  const rows = parseActionableContextTable(raw).filter((row) => !existingTitles.has(safeLower(row.task))).slice(0, limit);
  return rows.map((row, index) => ({
    id: stableId('radar_todo_prompt', `${row.task}|${row.source}|${index}`),
    title: row.task,
    person: '',
    category: 'work',
    signal_type: 'todo',
    priority: ['high', 'medium', 'low'].includes(row.priority) ? row.priority : 'medium',
    why_now: row.context,
    evidence: row.source,
    move: row.task,
    recommended_action: row.task,
    suggestion_type: 'Task',
    primary_action: { label: 'Handle task' },
    suggested_actions: [{ label: 'Handle task' }, { label: 'Mark done' }, { label: 'Snooze' }],
    display: {
      person: '',
      headline: row.task,
      summary: trim(row.context, 180)
    },
    epistemic_trace: makeTraceFromEvidence(goldSet.filter((item) => formatEvidenceSnippet(item, 220).includes(row.source)).slice(0, 2), 2),
    createdAt: Date.now(),
    ai_generated: true,
    prompt_native: true
  }));
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

async function buildRadarState({ llmConfig = null, manualTodos = [], maxCentralSignals = 5, maxRelationshipSignals = 5, maxTodoSignals = 5, existingState = null } = {}) {
  const timings = {};
  const relationshipsEnabled = Number(maxRelationshipSignals || 0) > 0;
  const relationshipCandidates = relationshipsEnabled ? await fetchRelationshipCandidates(8) : [];
  const projectNodes = await fetchProjectNodes(8);

  const existingCentral = (existingState?.centralSignals || []).slice(0, 10);
  const existingRelationship = (existingState?.relationshipSignals || []).slice(0, 10);
  const existingTodo = (existingState?.todoSignals || []).slice(0, 10);

  const runRelationshipSuggestionsEngine = relationshipsEnabled
    ? require('./relationship-suggestions-engine').runRelationshipSuggestionsEngine
    : null;
  const centralContext = await retrieveBoundedContext('What are the top high-level insights or patterns from recent activity?', {
    seedLimit: 8,
    hopLimit: 4,
    evidenceLimit: 10
  });

  const relationshipChat = relationshipsEnabled ? await runChatBackedSection({
    section: 'relationship',
    llmConfig,
    relationshipCandidates,
    manualTodos,
    limit: maxRelationshipSignals,
    existingSignals: existingRelationship
  }) : { took_ms: 0, retrieval: null, signals: [], error: null };

  const todoChat = await runChatBackedSection({
    section: 'todo',
    llmConfig,
    relationshipCandidates,
    manualTodos,
    limit: maxTodoSignals,
    existingSignals: existingTodo
  });

  const deterministicRelSignals = relationshipsEnabled ? await runRelationshipSuggestionsEngine({
    llmConfig,
    limit: maxRelationshipSignals,
    deepScan: false
  }).catch(() => []) : [];

  timings.central_retrieval_ms = centralContext.took_ms;
  timings.relationship_chat_ms = relationshipChat.took_ms;
  timings.todo_chat_ms = todoChat.took_ms;

  const centralSignalsResult = await (async () => {
    try {
      let signals = await runSectionLLM({
        section: 'central',
        llmConfig,
        context: centralContext,
        relationshipCandidates,
        manualTodos,
        limit: maxCentralSignals,
        existingSignals: existingCentral
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
  })();

  const relationshipSignalsResult = await (async () => {
    try {
      if (!relationshipsEnabled) {
        return { section: 'relationship', signals: [], error: null };
      }
      timings.relationship_prompt_ms = relationshipChat?.took_ms || 0;

      let signals = Array.isArray(deterministicRelSignals) && deterministicRelSignals.length
        ? deterministicRelSignals.slice(0, maxRelationshipSignals)
        : [];

      if (signals.length < maxRelationshipSignals) {
        const existing = new Set(signals.map(s => safeLower(s.person || s.title)));
        const llmSignals = await generateRelationshipSignalsFromPrompt({
          llmConfig,
          relationshipCandidates,
          relationshipRetrieval: relationshipChat?.retrieval || null,
          recentInterestEvidence: [
            ...(centralContext?.evidence || []),
            ...((todoChat?.retrieval?.evidence) || [])
          ],
          limit: maxRelationshipSignals - signals.length,
          existingSignals: existingRelationship
        }).catch(() => []);
        const deduped = (llmSignals || []).filter(s => !existing.has(safeLower(s.person || s.title)));
        signals = [...signals, ...deduped].slice(0, maxRelationshipSignals);
      }

      if (!signals.length) {
        signals = Array.isArray(relationshipChat?.signals) ? relationshipChat.signals.slice(0, maxRelationshipSignals) : [];
      }
      if (!signals.length) {
        signals = makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals, existingRelationship);
      }
      if (!signals.length) {
        signals = makeRelationshipEvidenceFallback(relationshipChat?.retrieval, maxRelationshipSignals, existingRelationship);
      }

      return { section: 'relationship', signals, error: relationshipChat?.error || null };
    } catch (error) {
      return {
        section: 'relationship',
        signals: deterministicRelSignals?.length
          ? deterministicRelSignals.slice(0, maxRelationshipSignals)
          : makeRelationshipFallback(relationshipCandidates, maxRelationshipSignals, existingRelationship),
        error: String(error?.message || error)
      };
    }
  })();

  const todoSignalsResult = await (async () => {
    try {
      let signals = await generateTodoSignalsFromPrompt({
        llmConfig,
        todoRetrieval: todoChat?.retrieval || null,
        projectNodes,
        limit: maxTodoSignals,
        existingSignals: existingTodo
      });
      timings.todo_prompt_ms = todoChat?.took_ms || 0;
      if (!signals.length) {
        signals = Array.isArray(todoChat?.signals) ? todoChat.signals.slice(0, maxTodoSignals) : [];
      }
      if (!signals.length) {
        signals = makeTodoFallback(manualTodos, todoChat?.retrieval, maxTodoSignals, existingTodo);
      }
      if (signals.length && !signals.every((item) => item.prompt_native)) {
        signals = await deepenSignals({
          section: 'todo',
          llmConfig,
          signals,
          context: {
            evidence: todoChat?.retrieval?.evidence || [],
            retrieval: todoChat?.retrieval || null
          }
        });
      }
      if (!signals.length) {
        signals = makeTodoFallback(manualTodos, todoChat?.retrieval, maxTodoSignals, existingTodo);
      }
      return { section: 'todo', signals, error: todoChat?.error || null };
    } catch (error) {
      return { section: 'todo', signals: makeTodoFallback(manualTodos, todoChat?.retrieval, maxTodoSignals, existingTodo), error: String(error?.message || error) };
    }
  })();

  const results = [centralSignalsResult, relationshipSignalsResult, todoSignalsResult];

  const centralResult = results.find(r => r.section === 'central');
  const relationshipResult = results.find(r => r.section === 'relationship');
  const todoResult = results.find(r => r.section === 'todo');

  const centralSignals = centralResult.signals.map((item) => ({
    ...item,
    signal_type: 'central',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(centralContext.evidence, 2)
  }));
  const relationshipSignals = relationshipsEnabled ? relationshipResult.signals.map((item) => ({
    ...item,
    signal_type: 'relationship',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(relationshipChat?.retrieval?.evidence || [], 2)
  })) : [];
  const todoSignals = todoResult.signals.map((item) => ({
    ...item,
    signal_type: 'todo',
    epistemic_trace: item.epistemic_trace?.length ? item.epistemic_trace : makeTraceFromEvidence(todoChat?.retrieval?.evidence || [], 2)
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
      relationship: { status: relationshipsEnabled ? (relationshipResult.error ? 'partial' : 'ready') : 'disabled', error: relationshipResult.error, count: relationshipSignals.length },
      todo: { status: todoResult.error ? 'partial' : 'ready', error: todoResult.error, count: todoSignals.length }
    },
    timings
  };
}

module.exports = {
  buildRadarState
};
