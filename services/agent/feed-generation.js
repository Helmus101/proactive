const crypto = require('crypto');
const db = require('../db');
const { callLLM } = require('./intelligence-engine');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');
const { mineProactiveOpportunities } = require('./opportunity-miner');

function asObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function parseTs(value) {
  if (!value && value !== 0) return 0;
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : 0;
}

function trim(text, max = 220) {
  return String(text || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function cleanSeedPhrase(text = '', max = 90) {
  const raw = String(text || '').replace(/\s+/g, ' ').trim();
  if (!raw) return '';
  return raw
    .replace(/[`"'()[\]{}<>]/g, '')
    .replace(/\b(?:function|const|let|var|class|import|export|return|undefined|null|true|false)\b/gi, '')
    .replace(/\b[a-z0-9_.-]+\.(?:js|ts|tsx|jsx|json|md|css|html)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, max);
}

function isNoisySuggestionFocus(text = '') {
  const value = String(text || '').trim();
  if (!value) return true;
  const words = value.split(/\s+/).filter(Boolean);
  if (words.length < 3) return true;
  if (/\b(def|tmp|todo|fixme|wip|misc|test)\b/i.test(value) && words.length <= 4) return true;
  if (/[{}()[\];<>]/.test(value)) return true;
  if (/\b[a-z_]+\([^\)]*\)/i.test(value)) return true;
  if (/[A-Za-z0-9]+\.[A-Za-z0-9]+/.test(value) && /\b(js|ts|tsx|jsx|json|md|css|html)\b/i.test(value)) return true;
  const codeLikeTokens = words.filter((w) => /[_/\\]|[a-z][A-Z]|^\w+\.\w+$/.test(w)).length;
  if (codeLikeTokens >= Math.max(2, Math.floor(words.length * 0.5))) return true;
  return false;
}

function deriveMeaningfulFocus(seed = {}, graphContext = {}) {
  const candidates = [
    seed.title,
    seed.trigger_summary,
    graphContext?.trace_summary,
    ...(Array.isArray(graphContext?.evidence) ? graphContext.evidence.map((item) => item?.text || item?.summary || item?.title || '') : [])
  ];
  for (const candidate of candidates) {
    const cleaned = cleanSeedPhrase(candidate, 90);
    if (!cleaned || isNoisySuggestionFocus(cleaned)) continue;
    return cleaned;
  }
  return seed.category === 'followup' ? 'pending follow-up thread' : 'open task from recent activity';
}

function makeFallbackTitle(seed = {}, graphContext = {}) {
  const focus = deriveMeaningfulFocus(seed, graphContext);
  const preferred = Array.isArray(seed?.candidate_actions) && seed.candidate_actions.length
    ? seed.candidate_actions[0]
    : (seed.category === 'followup' ? `Draft follow-up for ${focus}` : `Finish next step for ${focus}`);
  return cleanSeedPhrase(preferred, 62);
}

function titleLooksBad(title = '') {
  const t = cleanSeedPhrase(title, 120);
  if (!t || isNoisySuggestionFocus(t)) return true;
  if (!/\b(open|draft|reply|send|prepare|review|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix)\b/i.test(t)) {
    if (t.split(/\s+/).length < 4) return true;
  }
  return false;
}

function aiDerivedTitle(payload = {}, seed = {}) {
  const fromTitle = cleanSeedPhrase(payload?.title || '', 120);
  if (fromTitle && !titleLooksBad(fromTitle)) return fromTitle;

  const fromIntent = cleanSeedPhrase(payload?.intent || '', 120);
  if (fromIntent && !isNoisySuggestionFocus(fromIntent)) {
    const verb = /\b(open|draft|reply|send|prepare|review|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix)\b/i.test(fromIntent);
    return verb ? fromIntent : `Complete ${fromIntent}`.slice(0, 120);
  }

  const fromReason = cleanSeedPhrase(payload?.reason || seed?.trigger_summary || '', 120);
  if (fromReason && !isNoisySuggestionFocus(fromReason)) return fromReason.slice(0, 120);

  const focus = deriveMeaningfulFocus(seed, {});
  return seed.category === 'followup'
    ? `Reply about ${focus}`.slice(0, 120)
    : `Finish ${focus}`.slice(0, 120);
}

function hasTemplateTone(text = '') {
  const t = String(text || '').toLowerCase();
  if (!t) return true;
  if (/\b(complete next step now|send follow-?up now|open source context|take the next step|move the thread forward|keep momentum)\b/.test(t)) return true;
  if (/^(complete|finish|do|handle)\s+(this|it|task|next step)\b/.test(t)) return true;
  return false;
}

function hasReceiptAttribution(text = '') {
  const t = String(text || '').toLowerCase();
  if (!t) return false;
  if (/\b(found in|saw on|from your|from the|in your|at \d{1,2}:\d{2}|\d+[hd] ago|hours ago|days ago|yesterday|today|calendar|email|screen|capture|last seen|last active|open since|since \d)\b/.test(t)) return true;
  return false;
}

function startsWithImperativeVerb(text = '') {
  const t = String(text || '').trim();
  if (/^review\b/i.test(t)) {
    return hasConcreteAction(t);
  }
  return /^(?:open|draft|reply|send|prepare|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share|drill|resume|tag|rename|close|start|run)\b/i.test(t);
}

function hasConcreteAction(text = '') {
  const value = String(text || '');
  const hasVerb = /\b(open|draft|reply|send|prepare|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share|drill|resume|tag|rename|close|start|run)\b/i.test(value);
  if (hasVerb) return true;
  const reviewMatch = value.match(/\breview\s+(\w+)/i);
  if (reviewMatch) {
    const target = reviewMatch[1].toLowerCase();
    if (!['this', 'it', 'item', 'context', 'memory', 'episode', 'artifact'].includes(target)) {
      return true;
    }
  }
  return false;
}

function isConcreteActionLabel(label = '') {
  const value = String(label || '').trim();
  if (!value) return false;
  if (/\b(open source context|open context|action \d+|execute next action|do it)\b/i.test(value)) return false;
  if (/^review$/i.test(value)) return false; // Review alone is not concrete
  return hasConcreteAction(value);
}

function isWeakTitle(title = '') {
  const t = String(title || '').trim().toLowerCase();
  if (!t) return true;
  if (/^review (episode|memory|item|context) \d+/.test(t)) return true;
  if (/^(do|handle|work on|be proactive|next step|review this|make progress|stay on top|organize|check in on|handle this)\b/.test(t)) return true;
  if (t.split(/\s+/).length < 3) return true;
  return false;
}

function hasContextAnchor(text = '') {
  const t = String(text || '');
  return /\b(today|tomorrow|now|in \d+ (?:minutes|hours?)|at \d{1,2}:\d{2}|before|after|this (?:morning|afternoon|evening)|next|\d+[hd] ago|hours ago|days ago|last seen|last active|open since|overdue|started \d|due in)\b/i.test(t);
}

/**
 * Extract concrete specifics from graph context to inject into the AI prompt.
 * Returns a structured text block the model can quote directly.
 */
function extractGraphSpecifics(seed, graphContext) {
  const lines = [];
  const seedNodes = Array.isArray(graphContext?.seed_nodes) ? graphContext.seed_nodes : [];
  const evidence = Array.isArray(graphContext?.evidence) ? graphContext.evidence : [];
  const expanded = Array.isArray(graphContext?.expanded_nodes) ? graphContext.expanded_nodes : [];

  // Person names from semantic person nodes
  const persons = [];
  for (const n of [...seedNodes, ...expanded]) {
    if ((n.subtype === 'person' || n.layer === 'semantic') && n.title && isLikelyHumanContact(n.title)) {
      persons.push(n.title);
    }
  }
  if (persons.length) lines.push(`People involved: ${[...new Set(persons)].slice(0, 3).join(', ')}`);

  // Apps and domains
  const apps = [];
  const domains = [];
  for (const n of [...seedNodes, ...evidence]) {
    if (n.app && !/^(unknown|null|undefined)$/i.test(n.app)) apps.push(n.app);
    if (n.domain) domains.push(n.domain);
  }
  if (apps.length) lines.push(`Apps: ${[...new Set(apps)].slice(0, 3).join(', ')}`);
  if (domains.length) lines.push(`Domains: ${[...new Set(domains)].slice(0, 3).join(', ')}`);

  // URLs from evidence or seed
  const urls = [];
  for (const n of [...evidence, ...seedNodes]) {
    const url = n.url || (n.text && /^https?:\/\//.test(n.text) ? n.text.split(/\s/)[0] : null);
    if (url) urls.push(url.slice(0, 100));
  }
  if (urls.length) lines.push(`URLs: ${[...new Set(urls)].slice(0, 2).join(', ')}`);

  // Most recent event text (the most specific piece of evidence)
  const topEvidence = evidence[0];
  if (topEvidence?.text && topEvidence.text.length > 20) {
    lines.push(`Latest evidence: "${trim(topEvidence.text, 200)}" [${normalizeSourceLabel(topEvidence.app || topEvidence.source || 'source')}]`);
  } else if (seedNodes[0]?.text && seedNodes[0].text.length > 20) {
    lines.push(`Top match: "${trim(seedNodes[0].text, 200)}" [${normalizeSourceLabel(seedNodes[0].app || seedNodes[0].layer || 'graph')}]`);
  }

  // Timestamps
  const ts = seed.time_anchor || (topEvidence?.anchor_at ? new Date(topEvidence.anchor_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : null);
  if (ts) lines.push(`Time anchor: ${ts}`);

  return lines.length ? lines.join('\n') : 'No specific entities extracted.';
}

function tokenizeForMatch(text = '') {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter((w) => w.length >= 4 && !['this', 'that', 'with', 'from', 'your', 'have', 'will', 'then', 'next', 'step'].includes(w));
}

function titleSummaryConsistent(title = '', summary = '') {
  const a = new Set(tokenizeForMatch(title));
  const b = new Set(tokenizeForMatch(summary));
  if (!a.size || !b.size) return false;
  let overlap = 0;
  for (const token of a) {
    if (b.has(token)) overlap += 1;
  }
  return overlap >= 1;
}

function normalizeSourceLabel(source = '') {
  const s = String(source || '').toLowerCase();
  if (!s) return 'Memory Graph';
  if (s.includes('calendar')) return 'Google Calendar';
  if (s.includes('gmail') || s.includes('email')) return 'Gmail';
  if (s.includes('screen') || s.includes('sensor') || s.includes('desktop')) return 'Screen Capture';
  if (s.includes('browser') || s.includes('history') || s.includes('chrome') || s.includes('arc') || s.includes('safari')) return 'Browser';
  if (s.includes('graph')) return 'Memory Graph';
  return source;
}

const GENERIC_TITLE_PHRASES = [
  'take the next', 'next concrete step', 'review and organize',
  'send a quick update', 'keep momentum', 'recent activity',
  'open your', 'quickly tag or rename', 'complete next step now',
  'continue working on', 'take action on', 'address this item',
  'stay on top of', 'be proactive', 'make progress', 'handle this', 'work on', 'check in on'
];

function qualityGateSuggestion({ title = '', body = '', reason = '', display = null, epistemicTrace = [], suggestedActions = [] } = {}) {
  const hasTemplate = hasTemplateTone(title) || hasTemplateTone(body) || hasTemplateTone(reason);
  const receiptEnough = Array.isArray(epistemicTrace) && epistemicTrace.length >= 2;
  const concreteActions = Array.isArray(suggestedActions) ? suggestedActions.filter((a) => isConcreteActionLabel(a?.label || '')) : [];
  const actionEnough = concreteActions.length >= 1;
  const concrete = hasConcreteAction(title) || hasConcreteAction(body) || suggestedActions.some((a) => hasConcreteAction(a?.label || a?.payload?.action || ''));
  const anchor = hasContextAnchor(body) || hasContextAnchor(reason) || hasContextAnchor(display?.summary || '');
  const receiptLanguage = hasReceiptAttribution(reason) || hasReceiptAttribution(display?.summary || '') || hasReceiptAttribution(display?.insight || '');
  const titleSummaryMatch = titleSummaryConsistent(title, display?.summary || body || '');
  // Reject generic verb-phrase titles that could apply to any suggestion
  const titleLower = (title || '').toLowerCase();
  const hasGenericTitle = GENERIC_TITLE_PHRASES.some((phrase) => titleLower.includes(phrase)) || isWeakTitle(title);
  const startsWithVerb = startsWithImperativeVerb(title);
  // Require a specific anchor in the title: proper noun (CamelCase word), HH:MM time, or file extension
  const hasTitleAnchor = /[A-Z][a-z]{2,}/.test(title) || /[\d]{1,2}:\d{2}/.test(title) || /\.[a-z]{2,4}\b/i.test(title);
  // Also check the broader context for grounding
  const traceText = Array.isArray(epistemicTrace)
    ? epistemicTrace.map((item) => `${item.source || ''} ${item.text || ''}`).join(' ')
    : '';
  const specificAnchorText = title + ' ' + (reason || '') + ' ' + (body || '') + ' ' + traceText;
  const hasSpecificAnchor = /[A-Z][a-z]{2,}/.test(specificAnchorText) || /[\d]{1,2}:\d{2}/.test(specificAnchorText) || /\.[a-z]{2,4}\b/i.test(specificAnchorText) || /\d+[hd] ago|\d+x\b/i.test(specificAnchorText);
  const pass = !hasTemplate && !hasGenericTitle && startsWithVerb && hasTitleAnchor && hasSpecificAnchor && receiptEnough && actionEnough && concrete && anchor && receiptLanguage && titleSummaryMatch;
  return {
    pass,
    reasons: {
      hasTemplate,
      hasGenericTitle,
      startsWithVerb,
      hasTitleAnchor,
      hasSpecificAnchor,
      receiptEnough,
      actionEnough,
      concrete,
      anchor,
      receiptLanguage,
      titleSummaryMatch
    }
  };
}

async function rewriteSuggestionWithAI(payload, seed, graphContext, now, apiKey) {
  const rewritePrompt = `
Rewrite this suggestion so it sounds specific, direct, and grounded in the receipts.
Return strict JSON only:
{
  "title":"...",
  "body":"...",
  "intent":"...",
  "reason":"...",
  "plan":["...","...","..."],
  "display":{"headline":"...","summary":"...","insight":"..."},
  "suggested_actions":[{"label":"...","type":"browser_operator","payload":{"action":"...","url":"...","template":"..."}}]
}

Rules:
- No template phrases (like "take the next step", "be proactive").
- Use concrete nouns from evidence.
- Title must start with an imperative verb and name a specific target.
- Body must be exactly two sentences: 1) Risk/Context, 2) Direct Command.
- Must reference what the user was actually doing recently.
- Action must be immediately executable.
- PROHIBITED: "review and organize", "make progress", "stay on top of".

Now: ${new Date(now).toISOString()}
Trigger: ${seed.trigger_summary}
Context:
${graphContext.contextText || 'None'}
Current JSON:
${JSON.stringify(payload || {}, null, 2)}
`;
  const rewritten = await callLLM(rewritePrompt, apiKey, 0.15, { maxTokens: 450, economy: true, task: 'suggestion' }).catch(() => null);
  return (rewritten && !Array.isArray(rewritten) && typeof rewritten === 'object') ? rewritten : payload;
}

function priorityFromScore(score) {
  if (score >= 0.84) return 'high';
  if (score >= 0.62) return 'medium';
  return 'low';
}

function inferPatternType(seed = {}) {
  const text = `${seed.type || ''} ${seed.category || ''} ${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  if (/\bdeadline|due|calendar|tomorrow|friday|monday|week\b/.test(text)) return 'temporal';
  if (/\bafter|when|trigger|opened|episode\b/.test(text)) return 'trigger';
  if (/\brepeated|again|stalled|followup|follow up|habit\b/.test(text)) return 'frequency';
  return 'contextual';
}

function inferSuggestionType(seed = {}) {
  const text = `${seed.type || ''} ${seed.category || ''} ${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  if (/\bdue|deadline|overdue|tomorrow|today|calendar\b/.test(text)) return 'predictive_reminder';
  if (/\bstudy|exam|quiz|assignment|flashcard|revision|idiom|vocab|chapter\b/.test(text)) return 'optimization';
  if (/\bresearch|review|compare|look up|source\b/.test(text)) return 'optimization';
  if (/\bcollab|group|peer|classmate|teammate\b/.test(text)) return 'opportunity';
  if (/\bunusual|stalled|drop|missed|late|risk\b/.test(text)) return 'anomaly';
  if (/\bcontext|episode|thread\b/.test(text)) return 'context_awareness';
  return 'optimization';
}

function looksStudyOpportunity(seed = {}) {
  const text = `${seed.type || ''} ${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  return /\b(study|exam|quiz|assignment|revision|flashcard|vocab|idiom|chapter|problem set|homework)\b/.test(text);
}

function inferValueScore(seed = {}) {
  const score = Number(seed.score || 0.6);
  let points = 8;
  if (isImportantActionSeed(seed)) points += 8;
  if (seed.category === 'followup') points += 3;
  if (score >= 0.8) points += 4;
  if (/\bdue|deadline|overdue|urgent\b/i.test(`${seed.title || ''} ${seed.trigger_summary || ''}`)) points += 2;
  return Math.max(0, Math.min(25, points));
}

function inferRiskScore(seed = {}) {
  const text = `${seed.type || ''} ${seed.category || ''} ${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  if (/\bdeadline|due today|overdue|missed\b/.test(text)) return 6;
  if (/\bmeeting|calendar|followup|follow up\b/.test(text)) return 4;
  if (/\bresearch|review|summarize|optimi/.test(text)) return 3;
  return 2;
}

function passesSuggestionGate({ confidence = 0, valueScore = 0, riskScore = 10 } = {}) {
  const conf = Math.max(0, Math.min(1, Number(confidence || 0)));
  const value = Math.max(0, Math.min(25, Number(valueScore || 0)));
  const risk = Math.max(0, Math.min(10, Number(riskScore || 10)));
  const finalScore = (value * conf) - risk;
  return {
    pass: conf >= 0.7 && risk <= 5 && finalScore > 8,
    finalScore
  };
}

function classifySuggestionExecution(seed, title = '', body = '') {
  const hay = `${seed?.type || ''} ${seed?.category || ''} ${title} ${body}`.toLowerCase();
  if (/followup|reply|send|reach back|email|message/.test(hay)) {
    return { ai_doable: true, action_type: 'draft_message', execution_mode: 'draft_or_execute', target_surface: 'gmail', assignee: 'ai' };
  }
  if (/calendar|meeting|prepare|agenda|brief|prep|schedule|confirm/.test(hay)) {
    return { ai_doable: true, action_type: 'prepare_brief', execution_mode: 'draft_or_execute', target_surface: 'calendar', assignee: 'ai' };
  }
  if (/review|research|compare|look up|check/.test(hay)) {
    return { ai_doable: true, action_type: 'research', execution_mode: 'draft_or_execute', target_surface: 'browser', assignee: 'ai' };
  }
  return { ai_doable: false, action_type: 'manual_next_step', execution_mode: 'manual', target_surface: null, assignee: 'human' };
}

function makeExpectedBenefit(seed) {
  if (seed.category === 'followup') return 'keep the relationship or thread moving before it stalls';
  if (seed.type === 'decision_followthrough') return 'turn a prior decision into visible progress';
  if (seed.type === 'task_execution') return 'close an open loop that is still unresolved';
  return 'move the active work thread forward with one concrete step';
}

function inferStudySubject(seed = {}, graphContext = {}) {
  const blobs = [
    seed.title,
    seed.trigger_summary,
    graphContext?.trace_summary,
    ...(Array.isArray(graphContext?.evidence) ? graphContext.evidence.map((item) => item?.text || item?.summary || '') : [])
  ].join(' ').toLowerCase();
  if (/\benglish|essay|literature|writing|grammar\b/.test(blobs)) return 'english';
  if (/\bmath|algebra|geometry|calculus|equation|problem\b/.test(blobs)) return 'math';
  if (/\bphysics|chemistry|biology|science\b/.test(blobs)) return 'science';
  if (/\bhistory|geography|philosophy|economics\b/.test(blobs)) return 'humanities';
  return '';
}

function inferRiskLevel(seed = {}, confidence = 0.6) {
  const score = Number(seed.score || confidence || 0);
  const text = `${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  if (/distraction|stalled|overdue|missed|low/.test(text) || score >= 0.86) return 'high';
  if (score >= 0.68) return 'medium';
  return 'low';
}

function inferSuggestionGroup(seed = {}, studyContext = null) {
  const risk = inferRiskLevel(seed, seed.score || 0.6);
  if (risk === 'high') return 'risk';
  if (studyContext?.status === 'active') return 'now';
  return 'next';
}

function isLikelyHumanContact(value) {
  const text = String(value || '').trim();
  if (!text) return false;
  if (/^(app|topic|project)\s*:/i.test(text)) return false;
  if (/^(chrome|cursor|gmail|calendar|desktop|drive|google docs)$/i.test(text)) return false;
  if (/^(noreply|no-reply|support|team|hello|info)@/i.test(text)) return false;
  return true;
}


function normalizeMiniPlan(items = [], fallback = []) {
  const merged = [...(Array.isArray(items) ? items : []), ...(Array.isArray(fallback) ? fallback : [])]
    .map((item) => trim(item, 140))
    .filter(Boolean);
  return Array.from(new Set(merged)).slice(0, 4);
}

function ensureBecauseReason(text = '', fallback = '') {
  const reason = trim(text || fallback || '', 220);
  if (!reason) return '';
  if (/\bbecause\b/i.test(reason)) return reason;
  return `Because ${reason.charAt(0).toLowerCase()}${reason.slice(1)}`;
}

function buildEpistemicTrace(seed, graphContext, now) {
  const seedNodes = Array.isArray(graphContext?.seed_nodes) ? graphContext.seed_nodes : [];
  const evidences = Array.isArray(graphContext?.evidence) ? graphContext.evidence : [];
  const edges = Array.isArray(graphContext?.edge_paths) ? graphContext.edge_paths : [];
  const items = [];

  for (const node of seedNodes.slice(0, 3)) {
    items.push({
      node_id: node?.id || '',
      source: normalizeSourceLabel(node?.app || node?.source || node?.layer || 'Memory Graph'),
      text: trim(node?.title || node?.summary || node?.canonical_text || seed?.trigger_summary || '', 160),
      timestamp: node?.latest_activity_at || node?.timestamp || new Date(now).toISOString(),
      anchor_at: node?.anchor_at || null
    });
  }
  for (const ev of evidences.slice(0, 3)) {
    items.push({
      node_id: ev?.id || '',
      source: normalizeSourceLabel(ev?.app || ev?.source || ev?.layer || 'Evidence'),
      text: trim(ev?.text || ev?.summary || ev?.title || '', 160),
      timestamp: ev?.timestamp || ev?.latest_activity_at || new Date(now).toISOString(),
      anchor_at: ev?.anchor_at || ev?.timestamp || null
    });
  }
  for (const edge of edges.slice(0, 2)) {
    items.push({
      node_id: `${edge?.from || ''}->${edge?.to || ''}`,
      source: 'Graph Edge',
      text: trim(`${edge?.from || 'node'} -> ${edge?.to || 'node'} (${edge?.relation || 'linked'})`, 160),
      timestamp: new Date(now).toISOString(),
      edge_meta: { weight: edge?.weight || 1, evidence_count: edge?.evidence_count || 0 }
    });
  }
  // Normalize anchor times into short HH:MM labels where available
  const normalizeAnchor = (iso) => {
    if (!iso) return null;
    try {
      const d = new Date(iso);
      if (isNaN(d.getTime())) return null;
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    } catch (_) { return null; }
  };

  const out = items
    .filter((item) => item.node_id && item.text)
    .map((item) => ({
      ...item,
      anchor_time: normalizeAnchor(item.anchor_at) || null
    }))
    .slice(0, 6);
  return out;
}

function buildSuggestedActions(seed, execution, actionPlan = [], aiDraft = '') {
  const concrete = Array.isArray(actionPlan) ? actionPlan : [];
  const baseActions = concrete.map((step) => ({
    label: trim(step?.step || `${execution?.action_type === 'draft_message' ? `Draft and send ${seed?.title || 'follow-up'}` : `Complete ${seed?.title || 'the task'} step`}`, 60),
    type: 'browser_operator',
    payload: {
      action: step?.target ? `open_${step.target}` : 'open_context',
      url: step?.url || null,
      template: aiDraft ? trim(aiDraft, 260) : null,
      source_node_id: step?.source_node_id || null
    }
  }));
  if (!baseActions.length && execution?.action_type === 'draft_message') {
    baseActions.push({
      label: 'Draft message',
      type: 'browser_operator',
      payload: {
        action: 'navigate_and_type',
        url: 'https://mail.google.com',
        template: trim(aiDraft || `Draft a concise follow-up for ${seed?.title || 'this thread'}.`, 260)
      }
    });
  }
  if (!baseActions.length) {
    baseActions.push({
      label: 'Open evidence and complete next step',
      type: 'browser_operator',
      payload: {
        action: 'open_context',
        url: null,
        template: null
      }
    });
  }
  return baseActions.slice(0, 3);
}

function buildSeedRetrievalIntent({ title = '', type = '', category = '', triggerSummary = '' } = {}) {
  const base = String(title || '').trim();
  const trigger = String(triggerSummary || '').trim();
  if (type === 'person_followup') {
    return `${base} last interaction recent thread follow up mentioned interests goals challenges upcoming events unresolved follow up`;
  }
  if (type === 'task_execution') {
    return `${base} next concrete step unresolved task implementation status blocker outcome`;
  }
  if (type === 'decision_followthrough') {
    return `${base} follow through next action decision status owner open loop`;
  }
  if (type === 'cloud_hypothesis') {
    return `${base} repeated pattern supporting evidence next best action shared topic context`;
  }
  if (type === 'recent_episode') {
    return `${base} recent episode next step unresolved context ${category === 'followup' ? 'thread follow up' : 'work status'}`.trim();
  }
  return `${base} ${trigger}`.trim();
}

function mapOpportunityTypeToSeedType(opportunityType = '') {
  const value = String(opportunityType || '').toLowerCase();
  if (value.includes('followup') || value.includes('contact')) return 'person_followup';
  if (value.includes('social_decay') || value.includes('value_hook')) return 'person_followup';
  if (value.includes('deadline')) return 'deadline_risk';
  if (value.includes('study')) return 'study_followthrough';
  if (value === 'person_news') return 'person_news';
  if (value === 'birthday_reminder') return 'birthday_reminder';
  if (value === 'article_share') return 'article_share';
  return 'task_execution';
}

function mapOpportunityToSeed(candidate = {}) {
  const title = trim(candidate.title || candidate.trigger_summary || 'Open opportunity', 100);
  const oppType = String(candidate.opportunity_type || '').toLowerCase();
  let category = 'work';
  if (/followup|contact/.test(oppType)) category = 'followup';
  if (/relationship_intelligence|person_news|birthday_reminder|article_share|followup_reminder|connection_opportunity|social_decay_nudge|contextual_value_hook/.test(oppType)) category = 'relationship_intelligence';
  
  const type = mapOpportunityTypeToSeedType(candidate.opportunity_type);
  const triggerSummary = trim(candidate.trigger_summary || candidate.time_anchor || title, 180);
  return {
    id: candidate.seed_node_id || candidate.id || `opp_${crypto.randomBytes(4).toString('hex')}`,
    type,
    category,
    title,
    query: `${title} next concrete step`,
    retrieval_intent: candidate.retrieval_intent || buildSeedRetrievalIntent({
      title,
      type,
      category,
      triggerSummary
    }),
    score: Number(candidate.score || candidate.confidence || 0.6),
    trigger_summary: triggerSummary,
    source_node_ids: Array.isArray(candidate.supporting_node_ids) ? candidate.supporting_node_ids.slice(0, 8) : [candidate.seed_node_id].filter(Boolean),
    source_edge_paths: Array.isArray(candidate.supporting_edge_paths) ? candidate.supporting_edge_paths : [],
    source_refs: Array.isArray(candidate.source_refs) ? candidate.source_refs : [],
    opportunity_type: candidate.opportunity_type || null,
    reason_codes: Array.isArray(candidate.reason_codes) ? candidate.reason_codes : [],
    time_anchor: candidate.time_anchor || '',
    candidate_actions: Array.isArray(candidate.candidate_actions) ? candidate.candidate_actions : [],
    candidate_score: Number(candidate.score || candidate.confidence || 0),
    social_tier: candidate.social_tier || null,
    social_temperature: Number(candidate.social_temperature || 0),
    sentiment_gradient: candidate.sentiment_gradient || null,
    value_hook: candidate.value_hook || null,
    outreach_options: Array.isArray(candidate.outreach_options) ? candidate.outreach_options : [],
    social_strategy: candidate.social_strategy || null,
    relationship_contact_id: candidate.relationship_contact_id || null,
    relationship_status: candidate.relationship_status || null,
    relationship_score_inputs: candidate.relationship_score_inputs || null,
    draft_context_refs: Array.isArray(candidate.draft_context_refs) ? candidate.draft_context_refs : [],
    target_surface: candidate.target_surface || null
  };
}

function buildActionPlan(seed, execution, graphContext) {
  const seedTitle = deriveMeaningfulFocus(seed, graphContext) || seed.title || 'this item';
  const firstSourceId = (seed.source_node_ids || [])[0] || (graphContext.seed_nodes || [])[0]?.id || null;
  const plan = [];

  if (execution.action_type === 'draft_message') {
    plan.push({ step: 'Open the relevant thread in Gmail', target: 'gmail', url: 'https://mail.google.com', source_node_id: firstSourceId });
    plan.push({ step: `Draft a specific follow-up for ${seedTitle}`, target: 'gmail', source_node_id: firstSourceId });
  } else if (execution.action_type === 'prepare_brief') {
    plan.push({ step: 'Open the relevant calendar or meeting context', target: 'calendar', url: 'https://calendar.google.com', source_node_id: firstSourceId });
    plan.push({ step: `Assemble a short brief or confirmation for ${seedTitle}`, target: 'calendar', source_node_id: firstSourceId });
  } else if (execution.action_type === 'research') {
    plan.push({ step: 'Open the relevant page or search surface', target: 'browser', url: 'https://duckduckgo.com', source_node_id: firstSourceId });
    plan.push({ step: `Look up the exact missing detail connected to ${seedTitle}`, target: 'browser', source_node_id: firstSourceId });
  }

  return plan.slice(0, 4);
}

function isImportantActionSeed(seed = {}) {
  const type = String(seed.type || '').toLowerCase();
  const title = String(seed.title || '').toLowerCase();
  const trigger = String(seed.trigger_summary || '').toLowerCase();
  if (type === 'task_execution' || type === 'decision_followthrough') return true;
  if (looksStudyOpportunity(seed)) return true;
  if (seed.category === 'relationship') return true;
  if (type === 'cloud_hypothesis' && /open loop|unresolved|repeated/.test(trigger)) return true;
  if (/\bdeadline|due|urgent|priority|follow through|unresolved|action\b/.test(`${title} ${trigger}`)) return true;
  return false;
}

function hasOpenWorkSignal(seed = {}) {
  const text = `${seed.title || ''} ${seed.trigger_summary || ''}`.toLowerCase();
  return /\b(open|unresolved|pending|due|deadline|follow up|reply|stalled|not been resolved|needs preparation|unfinished|action|review|confirm|meeting|agenda|draft|send|prepare|fix|update|submit)\b/.test(text);
}

function isVagueSuggestionText(title = '', body = '') {
  const text = `${title} ${body}`.toLowerCase();
  if (!title || title.trim().length < 16 || isWeakTitle(title)) return true;
  if (/\b(do this|work on this|keep going|make progress|be proactive|follow up|handle this|stay on top|take action)\b/.test(text)) return true;
  if (!startsWithImperativeVerb(title)) return true;
  return false;
}

function buildSuggestionDraft(seed, execution, reason, expectedBenefit) {
  if (execution.action_type === 'draft_message') {
    return `Draft a concise follow-up for ${seed.title}. Mention the unresolved thread, propose the exact next step, and keep the tone natural. Benefit: ${expectedBenefit}. Context: ${reason}`;
  }
  if (execution.action_type === 'prepare_brief') {
    return `Prepare a short brief for ${seed.title}. Include what matters, the open decision, and the exact next move. Benefit: ${expectedBenefit}. Context: ${reason}`;
  }
  if (execution.action_type === 'research') {
    return `Research the missing detail for ${seed.title} and summarize the answer with the best next step. Benefit: ${expectedBenefit}. Context: ${reason}`;
  }
  return '';
}

async function fetchRecentRawRecall(now = Date.now(), limit = 6) {
  const sinceIso = new Date(now - (24 * 60 * 60 * 1000)).toISOString();
  const rows = await db.allQuery(
    `SELECT id, type, source, app, window_title, text, timestamp, metadata
     FROM events
     WHERE datetime(timestamp) >= datetime(?)
     ORDER BY datetime(timestamp) DESC
     LIMIT ?`,
    [sinceIso, limit * 3]
  ).catch(() => []);
  const recall = [];
  for (const row of rows || []) {
    const metadata = asObj(row.metadata);
    const summary = trim(
      metadata.activity_summary ||
      metadata.cleaned_capture_text ||
      row.text ||
      row.window_title ||
      '',
      180
    );
    if (!summary) continue;
    recall.push({
      id: row.id,
      source: normalizeSourceLabel(row.source || row.app || row.type || 'Event'),
      text: summary,
      timestamp: row.timestamp || new Date(now).toISOString()
    });
    if (recall.length >= limit) break;
  }
  return recall;
}

async function fetchMemoryNodes(layer, extraWhere = '', params = []) {
  return db.allQuery(
    `SELECT id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, created_at, updated_at
     FROM memory_nodes
     WHERE layer = ? ${extraWhere}
     LIMIT 240`,
    [layer, ...params]
  ).catch(() => []);
}

async function fallbackSeedsFromRecentEvents(now = Date.now()) {
  const rows = await db.allQuery(
    `SELECT id, type, source, title, text, metadata, timestamp
     FROM events
     ORDER BY datetime(timestamp) DESC
     LIMIT 80`
  ).catch(() => []);
  const seen = new Set();
  const seeds = [];
  for (const row of rows || []) {
    const metadata = asObj(row.metadata);
    const title = trim(
      row.title ||
      metadata.title ||
      metadata.window_title ||
      metadata.activity_summary ||
      row.text ||
      '',
      100
    );
    if (!title || title.length < 10) continue;
    const key = title.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const ts = parseTs(row.timestamp || 0) || now;
    const ageHours = Math.max(0, (now - ts) / (60 * 60 * 1000));
    const triggerSummary = trim(
      metadata.activity_summary ||
      metadata.cleaned_capture_text ||
      row.text ||
      `Recent ${row.type || 'activity'} signal appears unresolved.`,
      180
    );
    seeds.push({
      id: row.id || `evt_seed_${seeds.length}`,
      type: /email|message|calendar/i.test(`${row.type || ''} ${row.source || ''}`) ? 'person_followup' : 'task_execution',
      category: /email|message|calendar/i.test(`${row.type || ''} ${row.source || ''}`) ? 'followup' : 'work',
      title,
      query: `${title} next concrete step`,
      retrieval_intent: buildSeedRetrievalIntent({
        title,
        type: 'task_execution',
        category: 'work',
        triggerSummary
      }),
      score: Math.max(0.58, Math.min(0.86, 0.78 - Math.min(0.25, ageHours / 96))),
      trigger_summary: triggerSummary,
      source_node_ids: [row.id].filter(Boolean),
      source_refs: []
    });
    if (seeds.length >= 8) break;
  }
  return seeds;
}

async function scoreGraphFeedSeeds(now = Date.now()) {
  const candidates = await mineProactiveOpportunities(now, { limit: 24 }).catch(() => []);
  const seeds = (Array.isArray(candidates) ? candidates : []).map(mapOpportunityToSeed);
  if (!seeds.length) {
    const episodeRows = await fetchMemoryNodes('episode', `AND status != 'archived'`);
    for (const row of episodeRows || []) {
      const metadata = asObj(row.metadata);
      const sourceRefs = (() => {
        try { return JSON.parse(row.source_refs || '[]'); } catch (_) { return []; }
      })();
      const sourceGroup = String(metadata.source_type_group || '').toLowerCase();
      const eventCount = Number(metadata.event_count || 0);
      const endTs = parseTs(metadata.latest_activity_at || metadata.end || row.updated_at || row.created_at);
      if (!endTs) continue;
      const ageHours = Math.max(0, (now - endTs) / (60 * 60 * 1000));
      if (ageHours > 48 || eventCount < 2) continue;
      seeds.push({
        id: row.id,
        type: 'recent_episode',
        category: sourceGroup === 'communication' || sourceGroup === 'calendar' ? 'followup' : 'work',
        title: trim(row.title || row.summary || 'Recent episode', 100),
        query: `${row.title || row.summary || 'Recent episode'} unresolved next step`,
        retrieval_intent: buildSeedRetrievalIntent({
          title: row.title || row.summary || 'Recent episode',
          type: 'recent_episode',
          category: sourceGroup === 'communication' || sourceGroup === 'calendar' ? 'followup' : 'work',
          triggerSummary: 'Recent episode evidence indicates an unfinished loop.'
        }),
        score: Math.min(0.84, 0.52 + Math.min(0.22, eventCount * 0.06)),
        trigger_summary: 'Recent episode evidence indicates an unfinished loop.',
        source_node_ids: [row.id],
        source_refs: sourceRefs,
        opportunity_type: 'recent_episode_fallback',
        reason_codes: ['recent_episode', 'unfinished_loop'],
        time_anchor: 'today',
        candidate_actions: ['Open the episode context and close one open item'],
        candidate_score: Math.min(0.84, 0.52 + Math.min(0.22, eventCount * 0.06))
      });
    }
  }
  if (!seeds.length) {
    const fallbackSeeds = await fallbackSeedsFromRecentEvents(now);
    seeds.push(...fallbackSeeds.map((seed) => ({
      ...seed,
      opportunity_type: 'recent_event_fallback',
      reason_codes: ['fallback_recent_event'],
      time_anchor: 'today',
      candidate_actions: ['Open the source and close one open loop'],
      candidate_score: Number(seed.score || 0)
    })));
  }
  const ranked = (Array.isArray(seeds) ? seeds : [])
    .sort((a, b) => {
      const aPriorityBoost = isImportantActionSeed(a) ? 0.22 : 0;
      const bPriorityBoost = isImportantActionSeed(b) ? 0.22 : 0;
      return ((b.score || 0) + bPriorityBoost) - ((a.score || 0) + aPriorityBoost);
    });
  const filtered = ranked.filter((seed) => hasOpenWorkSignal(seed));
  return (filtered.length ? filtered : ranked).slice(0, 18);
}

function fallbackSuggestion(seed, graphContext, now, options = {}) {
  const studySubject = inferStudySubject(seed, graphContext);
  const riskLevel = inferRiskLevel(seed, seed.score || 0.6);
  const suggestionGroup = inferSuggestionGroup(seed, options?.study_context || null);
  const bestSeed = (graphContext.seed_nodes || []).find((item) => item.layer === 'episode' || item.layer === 'semantic') || graphContext.seed_nodes?.[0];
  const bestSourceIds = Array.from(new Set([...(seed.source_node_ids || []), ...(bestSeed ? [bestSeed.id] : [])])).slice(0, 6);
  const focus = deriveMeaningfulFocus(seed, graphContext);
  const title = makeFallbackTitle(seed, graphContext);
  const expectedBenefit = makeExpectedBenefit(seed);
  const body = seed.category === 'followup'
    ? trim(`Draft the ${focus} follow-up in 15 minutes.`, 90)
    : trim(`Finish one concrete step on ${focus} now.`, 90);
  const execution = classifySuggestionExecution(seed, title, body);
  const stepPlan = normalizeMiniPlan(seed.step_plan, [
    seed.category === 'followup'
      ? `Open ${trim(seed.title, 32)}`
      : `Open ${trim(seed.title, 32)}`,
    execution.ai_doable
      ? 'Let AI draft it'
      : 'Do the next step now'
  ]).slice(0, 2).map((step) => trim(step, 42));
  const actionPlan = buildActionPlan(seed, execution, graphContext);
  const aiDraft = buildSuggestionDraft(seed, execution, seed.trigger_summary, expectedBenefit);
  const epistemicTrace = buildEpistemicTrace(seed, graphContext, now);
  const suggestedActions = buildSuggestedActions(seed, execution, actionPlan, aiDraft)
    .slice(0, 1)
    .map((action) => ({ ...action, label: trim(action.label, 46) }));
  const primaryAction = suggestedActions.find((item) => isConcreteActionLabel(item?.label || '')) || null;
  const patternType = inferPatternType(seed);
  const suggestionType = inferSuggestionType(seed);
  const valueScore = inferValueScore(seed);
  const riskScore = inferRiskScore(seed);
  const gate = passesSuggestionGate({
    confidence: seed.score || 0.6,
    valueScore,
    riskScore
  });
  return {
    id: `sug_${crypto.randomBytes(5).toString('hex')}`,
    suggestion_id: `prop_${crypto.randomBytes(4).toString('hex')}`,
    type: seed.category === 'followup' ? 'followup' : 'next_action',
    title,
    body,
    description: trim(seed.trigger_summary || body, 90),
    intent: seed.category === 'followup' ? 'Close follow-up' : 'Make progress',
    reason: trim(ensureBecauseReason(`found in recent context: ${seed.trigger_summary}`), 90),
    trigger_summary: trim(seed.trigger_summary, 90),
    expected_benefit: trim(expectedBenefit, 90),
    expected_impact: trim(expectedBenefit, 90),
    plan: stepPlan,
    step_plan: stepPlan,
    category: seed.category,
    priority: priorityFromScore(seed.score || 0.6),
    confidence: Math.max(0.55, Number(seed.score || 0.6)),
    value_score: valueScore,
    risk_score: riskScore,
    final_score: gate.finalScore,
    opportunity_type: seed.opportunity_type || null,
    reason_codes: Array.isArray(seed.reason_codes) ? seed.reason_codes : [],
    time_anchor: seed.time_anchor || null,
    candidate_score: Number(seed.candidate_score || seed.score || 0),
    pattern_type: patternType,
    suggestion_type: suggestionType,
    explanation: trim(`Suggested from ${patternType} memory evidence.`, 90),
    display: {
      headline: `Action: ${trim(title, 62)}`,
      summary: trim(seed.trigger_summary || body, 90),
      insight: trim(`Pattern: ${patternType}.`, 80)
    },
    epistemic_trace: epistemicTrace,
    suggested_actions: suggestedActions,
    primary_action: primaryAction,
    ai_generated: false,
    ai_doable: execution.ai_doable,
    action_type: execution.action_type,
    execution_mode: execution.execution_mode,
    target_surface: execution.target_surface,
    prerequisites: [],
    assignee: execution.assignee,
    ai_draft: aiDraft,
    action_plan: actionPlan,
    source_node_ids: bestSourceIds,
    source_edge_paths: graphContext.edge_paths,
    evidence_path: graphContext.edge_paths,
    evidence: graphContext.evidence || [],
    relationship_contact_id: seed.relationship_contact_id || null,
    relationship_status: seed.relationship_status || null,
    relationship_score_inputs: seed.relationship_score_inputs || null,
    draft_context_refs: seed.draft_context_refs || [],
    study_subject: studySubject || null,
    risk_level: riskLevel,
    recommended_action: stepPlan[0] || title,
    suggestion_group: suggestionGroup,
    retrieval_trace: {
      retrieval_plan: graphContext.retrieval_plan,
      seed_nodes: graphContext.seed_nodes,
      expanded_nodes: graphContext.expanded_nodes,
      edge_paths: graphContext.edge_paths,
      trace_summary: graphContext.trace_summary
    },
    created_at: new Date(now).toISOString()
  };
}

async function buildSuggestionFromSeed(seed, apiKey, now, options = {}) {
  const standingNotes = String(options?.standing_notes || '').trim();
  const graphContext = await buildHybridGraphRetrieval({
    query: seed.retrieval_intent || seed.query,
    options: {
  mode: 'suggestion',
  candidate: seed,
  strategy: 'spiral'
    },
    seedLimit: 4,
    hopLimit: 2
  });
  const recentRecall = await fetchRecentRawRecall(now, 6);

  if (!apiKey) {
    const fb = fallbackSuggestion(seed, graphContext, now, options);
    const fbGate = qualityGateSuggestion({
      title: fb.title,
      body: fb.body,
      reason: fb.reason,
      display: fb.display,
      epistemicTrace: fb.epistemic_trace,
      suggestedActions: fb.suggested_actions
    });
    return fbGate.pass ? fb : null;
  }

  const graphSpecifics = extractGraphSpecifics(seed, graphContext);
  const isStudySeed = looksStudyOpportunity(seed);
  let suggestionCategory = isStudySeed ? 'study' : (seed.category === 'followup' ? 'followup' : 'work');
  if (seed.category === 'relationship_intelligence') suggestionCategory = 'relationship';

  const prompt = `
  You are generating one proactive suggestion from a user's memory graph.
  The UI is a quick-action feed. Suggestions must be short, fast, and directly actionable.
  There are three kinds of suggestions:
  - STUDY: drill/review/resume a specific concept, session, or assignment. Category = "study".
  - RELATIONSHIP: follow-up, connecting, sharing articles, birthdays, and noticing news about people. Category = "relationship".
  - WORK/FOLLOWUP: close an open loop in work, communication, or planning. Category = "work" or "followup".
  This seed is type: ${suggestionCategory.toUpperCase()}.

  Return strict JSON:
  {
    "title": "...",
    "body": "...",
    "intent": "...",
    "reason": "...",
    "category": "${suggestionCategory}",
    "suggestion_category": "${suggestionCategory}",
    "pattern_type": "temporal|contextual|frequency|trigger",
    "suggestion_type": "predictive_reminder|optimization|context_awareness|comparative|anomaly|opportunity",
    "value_score": 0,
    "risk_score": 0,
    "confidence": 0.0,
    "plan": ["...", "...", "..."],
    "expected_benefit": "...",
    "prerequisites": ["..."],
    "explanation": "one sentence describing why this suggestion was generated",
    "display": { "headline": "...", "summary": "...", "insight": "..." },
    "epistemic_trace": [{ "node_id": "...", "source": "...", "text": "...", "timestamp": "ISO-8601" }],
    "suggested_actions": [{ "label": "...", "type": "browser_operator|open_source|manual", "payload": { "action": "...", "url": "...", "template": "..." } }]
  }

  Rules:
  - One immediate action only.
  - Keep the whole suggestion scannable in under 5 seconds.
  - Use a direct coach voice: blunt, clear, no fluff, no cheerleading. For RELATIONSHIP category, use a "Relationship Coach" voice: warm but professional, suggesting ways to maintain and grow connections.
  - The action must be specific enough that the user could do it next.
  - Make it concrete, not generic. For relationship suggestions, mention the specific person's name and the specific article/event found.
  - Title: 4-9 words, imperative verb first, specific target included.
  - Body: exactly 1 short sentence, max 90 characters.
  - Reason: exactly 1 short sentence, max 90 characters, grounded in a named receipt.
  - Plan: max 2 steps, each max 42 characters.
  - suggested_actions: exactly 1 best action unless a second action is truly necessary.
  - Assume any AI-executable action should remove one extra step for the user.
  - Prefer stakeholder updates, meeting prep, open-loop closure, and exact follow-ups over broad productivity advice.
  - Prioritize suggestions that match these types when evidence supports them:
    deadline intelligence, research optimization, collaboration intelligence, grade/performance risk, time optimization.
  - Do not mention hidden system internals or node IDs.
  - Avoid weak language: no "maybe", "could", "consider", or "might".
  - Confidence must be realistic and evidence-based. If below 0.70, return confidence below 0.70.
  - Keep risk_score between 0 and 10 (higher means higher downside if wrong).
  - Keep value_score between 0 and 25 (time/effort/stress saved).
  - explanation must explicitly reference the observed pattern.
  - Always output a non-empty epistemic_trace with 2-4 receipts.
  - Always output 1 concrete suggested_action with a short label.
  - Treat this as a context bundle: Trigger -> Evidence -> Insight -> Action.
  - Reason from three-node convergence when possible:
    episodic (time/context), semantic (facts/entities), insight (inferred pattern).
  - Do not output generic productivity phrasing.
  - Output must read like a to-do item from unfinished or pending work, not a motivational reminder.
  - Include only "what" and "why now"; skip analysis.
  - If evidence does not support a concrete suggestion, return no suggestion (empty object).

  GOOD VS BAD EXAMPLES:
  - GOOD Title: "Reply to Alex about the Q3 Budget"
  - BAD Title: "Follow up on your recent emails"
  - GOOD Body: "This budget thread has been idle for 2 days since Alex asked for your approval. Draft a short confirmation now to unblock the team."
  - BAD Body: "You have some unread emails from Alex. You should consider replying to them when you have time."

  SPECIFICITY RULES (mandatory — violating any of these will cause this suggestion to be rejected):
  - Title MUST start with a concrete verb + a specific named entity, filename, person name, or exact time. NEVER start with "Take the next step", "Review and organize", "Send a quick update", "Continue working on", or any phrase that could apply to ANY suggestion.
  - reason MUST reference a specific artifact, timestamp, or person NAME drawn from the epistemic_trace. If the trace contains "09:23 screen capture" or "Alex" or "clawdbot.js", the reason must use that exact reference — not "recent activity" or "recent context".
  - body/reason combined length: ≤160 chars. Every word must add specific information. Cut filler and padding.
  - time_anchor must be a specific clock time (e.g. "09:23 this morning") or a concrete relative reference ("2 hours ago", "in 47 minutes") — NOT just "today" or "this morning" alone.
  - Do NOT generate a suggestion about organizing, tagging, or renaming files unless the epistemic_trace explicitly names specific files.

Now: ${new Date(now).toISOString()}
Suggestion type: ${suggestionCategory.toUpperCase()}
Trigger: ${seed.trigger_summary}
Opportunity:
- type: ${seed.opportunity_type || 'unknown'}
- time_anchor: ${seed.time_anchor || 'now'}
- reason_codes: ${(seed.reason_codes || []).join(', ') || 'none'}
- social_tier: ${seed.social_tier || 'unknown'}
- social_temperature: ${Number(seed.social_temperature || 0).toFixed(2)}
- value_hook: ${seed.value_hook ? JSON.stringify(seed.value_hook) : 'none'}

KNOWN SPECIFICS (use these exact names/times — do not paraphrase):
${graphSpecifics}

Candidate actions:
${(seed.candidate_actions || []).map((item) => `- ${item}`).join('\n') || '- None'}
Outreach options:
${(seed.outreach_options || []).map((item) => `- [${item.type || 'option'}] ${item.label || ''}: ${item.draft || ''}`).join('\n') || '- None'}
Standing notes: ${standingNotes || 'None'}

Graph context:
${graphContext.contextText || 'None'}
Graph trace:
${trim(graphContext.trace_summary || 'None', 600)}
Recent 24h recall:
${recentRecall.map((r) => `- [${r.source}] ${r.text} (${r.timestamp})`).join('\n') || 'None'}
`;

  let payload = await callLLM(prompt, apiKey, 0.2, { maxTokens: 500, economy: true, task: 'suggestion' });
  if (!payload || Array.isArray(payload) || (typeof payload === 'object' && !Object.keys(payload).length)) {
    return fallbackSuggestion(seed, graphContext, now, options);
  }
  const needsRewrite = hasTemplateTone(payload?.title || '') || hasTemplateTone(payload?.body || '');
  if (needsRewrite) payload = await rewriteSuggestionWithAI(payload, seed, graphContext, now, apiKey);

  const title = aiDerivedTitle(payload, seed);
  const body = trim(payload.body || payload.intent || payload.reason || '');
  if (!title || !body) return fallbackSuggestion(seed, graphContext, now, options);
  const execution = classifySuggestionExecution(seed, title, body);
  const expectedBenefit = trim(payload.expected_benefit || makeExpectedBenefit(seed), 160);
  const studySubject = inferStudySubject(seed, graphContext);
  const confidence = Math.max(0, Math.min(1, Number(payload.confidence || seed.score || 0.6)));
  const valueScore = Math.max(0, Math.min(25, Number(payload.value_score || inferValueScore(seed))));
  const riskScore = Math.max(0, Math.min(10, Number(payload.risk_score || inferRiskScore(seed))));
  const gate = passesSuggestionGate({ confidence, valueScore, riskScore });
  const finalScore = gate.finalScore;
  const riskLevel = inferRiskLevel(seed, confidence);
  const suggestionGroup = inferSuggestionGroup(seed, options?.study_context || null);
  const patternType = ['temporal', 'contextual', 'frequency', 'trigger'].includes(String(payload.pattern_type || '').toLowerCase())
    ? String(payload.pattern_type || '').toLowerCase()
    : inferPatternType(seed);
  const suggestionType = [
    'predictive_reminder',
    'optimization',
    'context_awareness',
    'comparative',
    'anomaly',
    'opportunity'
  ].includes(String(payload.suggestion_type || '').toLowerCase())
    ? String(payload.suggestion_type || '').toLowerCase()
    : inferSuggestionType(seed);
  const receiptHint = (Array.isArray(graphContext?.evidence) && graphContext.evidence[0]?.text)
    ? trim(graphContext.evidence[0].text, 90)
    : trim(seed.trigger_summary || seed.title || 'current context', 90);
  const stepPlan = normalizeMiniPlan(payload.plan, [
    execution.ai_doable
      ? `Use AI draft for: ${receiptHint}`
      : `Open the exact context: ${receiptHint}`,
    execution.action_type === 'draft_message'
      ? 'Edit one concrete line, then send'
      : execution.action_type === 'prepare_brief'
        ? 'Finalize the brief and use it immediately'
        : 'Execute the next concrete action now'
  ]).slice(0, 2).map((step) => trim(step, 42));
  const actionPlan = buildActionPlan(seed, execution, graphContext);
  const relationshipDraft = seed.relationship_contact_id
    ? await (async () => {
        try {
          const { buildRelationshipDraftContext, buildDeterministicDraft } = require('../relationship-graph');
          const context = await buildRelationshipDraftContext(seed.relationship_contact_id, { limit: 6 });
          return context ? { context, draft: buildDeterministicDraft(context) } : null;
        } catch (_) {
          return null;
        }
      })()
    : null;
  const aiDraft = relationshipDraft?.draft || buildSuggestionDraft(seed, execution, payload.reason || seed.trigger_summary, expectedBenefit);
  const epistemicTrace = Array.isArray(payload.epistemic_trace) && payload.epistemic_trace.length
    ? payload.epistemic_trace.slice(0, 6).map((item) => ({
      node_id: trim(item?.node_id || '', 120),
      source: normalizeSourceLabel(trim(item?.source || 'Memory Graph', 80)),
      text: trim(item?.text || '', 180),
      timestamp: trim(item?.timestamp || new Date(now).toISOString(), 40)
    })).filter((item) => item.node_id && item.text)
    : buildEpistemicTrace(seed, graphContext, now);
  const suggestedActions = Array.isArray(payload.suggested_actions) && payload.suggested_actions.length
    ? payload.suggested_actions.slice(0, 1).map((action) => ({
      label: trim(action?.label || 'Action', 46),
      type: trim(action?.type || 'browser_operator', 40),
      payload: {
        action: trim(action?.payload?.action || 'open_context', 60),
        url: trim(action?.payload?.url || '', 220) || null,
        template: trim(action?.payload?.template || '', 320) || null
      }
    }))
    : buildSuggestedActions(seed, execution, actionPlan, aiDraft)
      .slice(0, 1)
      .map((action) => ({ ...action, label: trim(action.label, 46) }));
  const quickBody = trim(body, 90);
  const canonicalSummary = trim(seed.trigger_summary || quickBody, 90);
  const display = {
    headline: trim(payload?.display?.headline || `Action: ${title}`, 120),
    summary: trim(
      hasTemplateTone(payload?.display?.summary || '') || !titleSummaryConsistent(title, payload?.display?.summary || '')
        ? canonicalSummary
        : payload.display.summary,
      90
    ),
    insight: trim(payload?.display?.insight || payload.explanation || graphContext.trace_summary || '', 120)
  };
  const normalizedReason = trim(ensureBecauseReason(payload.reason || seed.trigger_summary, `found in recent context: ${seed.trigger_summary}`), 90);
  const quality = qualityGateSuggestion({
    title,
    body,
    reason: normalizedReason,
    display,
    epistemicTrace,
    suggestedActions
  });
  if (!quality.pass) {
    // Return a flagged suggestion so UI can surface it for review instead of silently dropping it.
    return {
      id: `sug_${crypto.randomBytes(5).toString('hex')}`,
      suggestion_id: `prop_${crypto.randomBytes(4).toString('hex')}`,
      type: seed.category === 'followup' ? 'followup' : 'next_action',
      title,
      body: quickBody,
      description: trim(isVagueSuggestionText(title, quickBody) ? (payload.reason || seed.trigger_summary || quickBody) : quickBody, 90),
      intent: trim(payload.intent || quickBody, 90),
      reason: normalizedReason,
      trigger_summary: seed.trigger_summary,
      expected_benefit: expectedBenefit,
      plan: stepPlan,
      step_plan: stepPlan,
      category: payload.category || seed.category,
      priority: priorityFromScore(seed.score || 0.6),
      confidence,
      value_score: valueScore,
      risk_score: riskScore,
      final_score: finalScore,
      explanation: trim(payload.explanation || `Low-quality suggestion: ${trim(seed.trigger_summary || graphContext.trace_summary || '', 90)}`, 120),
      display,
      epistemic_trace: epistemicTrace,
      suggested_actions: suggestedActions,
      primary_action: suggestedActions.find((item) => isConcreteActionLabel(item?.label || '')) || null,
      ai_generated: true,
      ai_doable: execution.ai_doable,
      action_type: execution.action_type,
      execution_mode: execution.execution_mode,
      target_surface: execution.target_surface,
      assignee: execution.assignee,
      ai_draft: aiDraft,
      action_plan: actionPlan,
      source_node_ids: Array.from(new Set([...(seed.source_node_ids || []), ...graphContext.seed_nodes.map((item) => item.id)])).slice(0, 6),
      source_edge_paths: graphContext.edge_paths,
      evidence: graphContext.evidence || [],
      provider: apiKey ? 'deepseek' : 'local',
      relationship_contact_id: seed.relationship_contact_id || null,
      relationship_status: seed.relationship_status || null,
      relationship_score_inputs: seed.relationship_score_inputs || null,
      draft_context_refs: seed.draft_context_refs?.length ? seed.draft_context_refs : (relationshipDraft?.context?.draft_context_refs || []),
      retrieval_trace: {
        retrieval_plan: graphContext.retrieval_plan,
        seed_nodes: graphContext.seed_nodes,
        expanded_nodes: graphContext.expanded_nodes,
        edge_paths: graphContext.edge_paths,
        trace_summary: graphContext.trace_summary
      },
      review_required: true,
      created_at: new Date(now).toISOString()
    };
  }

  return {
    id: `sug_${crypto.randomBytes(5).toString('hex')}`,
    suggestion_id: `prop_${crypto.randomBytes(4).toString('hex')}`,
    type: seed.category === 'followup' ? 'followup' : 'next_action',
    title,
    body: quickBody,
    description: trim(isVagueSuggestionText(title, quickBody) ? (payload.reason || seed.trigger_summary || quickBody) : quickBody, 90),
    intent: trim(payload.intent || quickBody, 90),
    reason: normalizedReason,
    trigger_summary: seed.trigger_summary,
    expected_benefit: expectedBenefit,
    expected_impact: expectedBenefit,
    plan: stepPlan,
    step_plan: stepPlan,
    category: payload.category || seed.category,
    suggestion_category: payload.suggestion_category || suggestionCategory,
    priority: priorityFromScore(seed.score || 0.6),
    confidence,
    value_score: valueScore,
    risk_score: riskScore,
    final_score: finalScore,
    opportunity_type: seed.opportunity_type || null,
    reason_codes: Array.isArray(seed.reason_codes) ? seed.reason_codes : [],
    time_anchor: seed.time_anchor || null,
    candidate_score: Number(seed.candidate_score || seed.score || confidence || 0),
    pattern_type: patternType,
    suggestion_type: suggestionType,
    explanation: trim(
      payload.explanation ||
      `Observed ${patternType}: ${trim(seed.trigger_summary || graphContext.trace_summary || '', 90)}.`,
      120
    ),
    display,
    epistemic_trace: epistemicTrace,
    suggested_actions: suggestedActions,
    primary_action: suggestedActions.find((item) => isConcreteActionLabel(item?.label || '')) || null,
    ai_generated: true,
    ai_doable: execution.ai_doable,
    action_type: execution.action_type,
    execution_mode: execution.execution_mode,
    target_surface: execution.target_surface,
    prerequisites: Array.isArray(payload.prerequisites) ? payload.prerequisites.map((item) => trim(item, 120)).filter(Boolean).slice(0, 4) : [],
    assignee: execution.assignee,
    ai_draft: aiDraft,
    action_plan: actionPlan,
    source_node_ids: Array.from(new Set([...(seed.source_node_ids || []), ...graphContext.seed_nodes.map((item) => item.id)])).slice(0, 6),
    source_edge_paths: graphContext.edge_paths,
    evidence_path: graphContext.edge_paths,
    evidence: graphContext.evidence || [],
    provider: apiKey ? 'deepseek' : 'local',
    relationship_contact_id: seed.relationship_contact_id || null,
    relationship_status: seed.relationship_status || null,
    relationship_score_inputs: seed.relationship_score_inputs || null,
    draft_context_refs: seed.draft_context_refs?.length ? seed.draft_context_refs : (relationshipDraft?.context?.draft_context_refs || []),
    study_subject: studySubject || null,
    risk_level: riskLevel,
    recommended_action: stepPlan[0] || title,
    suggestion_group: suggestionGroup,
    retrieval_trace: {
      retrieval_plan: graphContext.retrieval_plan,
      seed_nodes: graphContext.seed_nodes,
      expanded_nodes: graphContext.expanded_nodes,
      edge_paths: graphContext.edge_paths,
      trace_summary: graphContext.trace_summary
    },
    created_at: new Date(now).toISOString()
  };
}

async function generateFeedSuggestions(apiKey, now = Date.now(), options = {}) {
  const seeds = await scoreGraphFeedSeeds(now);
  // If no opportunistic seeds found, try a queryless spiral retrieval to surface recent evidence as seeds
  let effectiveSeeds = Array.isArray(seeds) ? seeds.slice() : [];
  if (!effectiveSeeds.length) {
    try {
      const graphContext = await buildHybridGraphRetrieval({ query: '', options: { mode: 'queryless', strategy: 'spiral' }, seedLimit: 6, hopLimit: 1 });
      const ev = Array.isArray(graphContext.evidence) ? graphContext.evidence.slice(0, 6) : [];
      for (const item of ev) {
        const title = trim(item.activity_summary || item.text || item.title || 'Recent activity', 100);
        if (!title) continue;
        effectiveSeeds.push({
          id: item.id || `ev_${Math.random().toString(36).slice(2,8)}`,
          type: 'recent_episode',
          category: 'work',
          title,
          query: `${title} next concrete step`,
          retrieval_intent: `${title} unresolved next step`,
          score: Number(item.score || 0.6),
          trigger_summary: title,
          source_node_ids: item.source_refs || [],
          source_refs: item.source_refs || []
        });
      }
    } catch (_) { /* ignore retrieval errors */ }
  }
  const prioritizedSeeds = [
    ...effectiveSeeds.filter((seed) => isImportantActionSeed(seed)),
    ...effectiveSeeds.filter((seed) => !isImportantActionSeed(seed))
  ];
  const suggestions = [];
  const seen = new Set();

  for (const seed of prioritizedSeeds) {
    const dedupeKey = `${seed.type}:${String(seed.title || '').toLowerCase()}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    // Credit efficiency: once we have 2 high-quality candidates, skip seeds with score < 0.65
    // to avoid burning API calls on weak signals.
    if (suggestions.length >= 2 && Number(seed.score || 0) < 0.65) continue;

    const suggestion = await buildSuggestionFromSeed(seed, apiKey, now, options);
    if (suggestion) suggestions.push(suggestion);
    if (suggestions.length >= 7) break;
  }

  if (!suggestions.length && prioritizedSeeds.length) {
    for (const seed of prioritizedSeeds.slice(0, 3)) {
      const graphContext = await buildHybridGraphRetrieval({
        query: seed.retrieval_intent || seed.query || `${seed.title || 'recent activity'} next concrete step`,
        options: { mode: 'suggestion', candidate: seed },
        seedLimit: 4,
        hopLimit: 2
      });
      const fb = fallbackSuggestion(seed, graphContext, now, options);
      if (fb?.primary_action && isConcreteActionLabel(fb.primary_action.label || '')) suggestions.push(fb);
    }
  }

  // Cross-suggestion dedup: if two suggestions share the same canonical entity, keep the higher-confidence one
  const entitySeen = new Map();
  for (const s of suggestions) {
    const entityKey = (s.opportunity_type || 'unknown') + ':' + (
      s.canonical_target ||
      String(s.title || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 40)
    );
    const existing = entitySeen.get(entityKey);
    if (!existing || Number(s.confidence || 0) > Number(existing.confidence || 0)) {
      entitySeen.set(entityKey, s);
    }
  }
  const deduped = Array.from(entitySeen.values());

  // If nothing survived, include a demo fallback suggestion so the UI/chat can show a proactive example.
  // This helps surface the pipeline during development or empty DB states.
  const final = deduped
    .sort((a, b) => (Number(b.confidence || 0) + (b.priority === 'high' ? 0.15 : 0)) - (Number(a.confidence || 0) + (a.priority === 'high' ? 0.15 : 0)))
    .slice(0, 7)
    .filter((item) => item.primary_action && isConcreteActionLabel(item.primary_action.label));

  if (!final.length) {
    const demoId = `sug_demo_${Date.now()}`;
    const demo = {
      id: demoId,
      suggestion_id: `prop_demo_${Date.now()}`,
      type: 'next_action',
      title: 'Search Google for "hello"',
      body: 'Open Google and search for the word "hello" to demo the extension-driven automation.',
      description: 'Demo proactive suggestion: open Google and search.',
      intent: 'demo_search',
      reason: 'Demo suggestion to validate operator-first automation flow',
      trigger_summary: 'demo',
      expected_benefit: 'Showcase automated typing and search via extension',
      plan: ['Open Google', 'Type "hello"', 'Press Enter'],
      step_plan: ['Open Google', 'Type "hello"', 'Press Enter'],
      category: 'work',
      priority: 'low',
      confidence: 0.6,
      value_score: 4,
      risk_score: 1,
      final_score: 2,
      display: { headline: 'Demo: Google search', summary: 'Type and search "hello"', insight: 'Developer demo suggestion' },
      epistemic_trace: [{ node_id: 'demo', source: 'system', text: 'Demo suggestion generated', timestamp: new Date().toISOString() }],
      suggested_actions: [{ label: 'Search Google: hello', type: 'browser_operator', payload: { action: 'navigate_and_type', url: 'https://www.google.com', template: 'hello' } }],
      primary_action: { label: 'Search Google: hello', type: 'browser_operator', payload: { action: 'navigate_and_type', url: 'https://www.google.com', template: 'hello' } },
      ai_generated: false,
      ai_doable: false,
      action_type: 'manual_next_step',
      execution_mode: 'manual',
      target_surface: 'browser',
      assignee: 'human',
      ai_draft: null,
      action_plan: ['Open Google', 'Type "hello"', 'Press Enter'],
      source_node_ids: [],
      source_edge_paths: [],
      evidence: [],
      retrieval_trace: {},
      created_at: new Date().toISOString()
    };
    return [demo];
  }

  return final;
}

module.exports = {
  scoreGraphFeedSeeds,
  generateFeedSuggestions,
  qualityGateSuggestion,
  isWeakTitle,
  isConcreteActionLabel,
  hasTemplateTone,
  startsWithImperativeVerb,
  __test__: {
    hasTemplateTone,
    hasReceiptAttribution,
    qualityGateSuggestion,
    normalizeSourceLabel
  }
};
