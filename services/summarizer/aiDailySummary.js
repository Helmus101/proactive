/**
 * aiDailySummary.js
 *
 * Generates rich, AI-powered paragraph summaries for individual days.
 * Each summary captures: notable people, activities, emotional tone,
 * intent clusters, inferred preferences, and behavioral patterns.
 *
 * Used both during initial sync (batch mode) and for ongoing daily generation.
 */

const { callLLM } = require('../agent/intelligence-engine');

function buildDayContext(date, events) {
  const emails   = events.filter(e => e.type === 'email');
  const docs     = events.filter(e => ['doc', 'spreadsheet', 'slide'].includes(e.type));
  const cal      = events.filter(e => e.type === 'calendar_event');
  const pages    = events.filter(e => e.type === 'browser_history');
  const captures = events.filter(e => e.type === 'screen_capture');

  const parts = [];

  // ── Emails ──────────────────────────────────────────────────────────────
  if (emails.length) {
    // Separate promotional / no-reply from real emails
    const real = emails.filter(e => {
      const from = (e.from || '').toLowerCase();
      const body  = ((e.subject || '') + ' ' + (e.body || '') + ' ' + (e.snippet || '')).toLowerCase();
      const promoWords = ['unsubscribe', 'newsletter', 'promo', 'sale', 'deal', 'no-reply', 'noreply'];
      return !promoWords.some(w => from.includes(w) || body.includes(w));
    });

    const emailLines = real.slice(0, 10).map(e => {
      const from   = (e.from_name || e.from || 'Unknown').split('<')[0].trim();
      const subj   = (e.subject || e.snippet || '').trim().slice(0, 90);
      const replied = e.is_replied ? ' [replied]' : (e.reply_status === 'replied' ? ' [replied]' : '');
      return `  - From "${from}"${subj ? `: "${subj}"` : ''}${replied}`;
    });

    parts.push(`Emails (${emails.length} total, ${real.length} non-promo):\n${emailLines.join('\n')}`);
  }

  // ── Calendar ─────────────────────────────────────────────────────────────
  if (cal.length) {
    const calLines = cal.slice(0, 8).map(c => {
      const title     = c.title || c.event_title || c.summary || '';
      const attendees = (c.attendees || []).slice(0, 4).join(', ');
      return `  - "${title}"${attendees ? ` (with: ${attendees})` : ''}`;
    });
    parts.push(`Calendar Events (${cal.length}):\n${calLines.join('\n')}`);
  }

  // ── Documents ────────────────────────────────────────────────────────────
  if (docs.length) {
    const docLines = docs.slice(0, 6).map(d => {
      const name  = d.doc_name || d.name || '';
      const state = d.state || '';
      return `  - "${name}"${state ? ` [${state}]` : ''}`;
    });
    parts.push(`Documents edited/opened (${docs.length}):\n${docLines.join('\n')}`);
  }

  // ── Browser history ──────────────────────────────────────────────────────
  if (pages.length) {
    const domainCounts = {};
    pages.forEach(p => {
      try {
        const host = new URL(p.url || '').hostname.replace(/^www\./, '');
        domainCounts[host] = (domainCounts[host] || 0) + 1;
      } catch (_) {}
    });
    const topDomains = Object.entries(domainCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([d, c]) => `${d}(${c}x)`)
      .join(', ');

    const topPageTitles = pages
      .slice(0, 6)
      .map(p => p.page_title || '')
      .filter(Boolean)
      .slice(0, 4)
      .join('; ');

    parts.push(
      `Browser history (${pages.length} pages): ${topDomains}` +
      (topPageTitles ? `\n  Notable pages: ${topPageTitles}` : '')
    );
  }

  if (captures.length) {
    const captureLines = captures.slice(0, 5).map(c => {
      const app = c.metadata?.sensor?.activeApp || c.title || 'Desktop';
      const win = c.metadata?.sensor?.activeWindowTitle || '';
      const excerpt = (c.text || '').replace(/\s+/g, ' ').trim().slice(0, 140);
      return `  - ${app}${win ? ` / ${win}` : ''}${excerpt ? `: ${excerpt}` : ''}`;
    });
    parts.push(`Screen captures (${captures.length}):\n${captureLines.join('\n')}`);
  }

  return `=== ${date} ===\n${parts.join('\n\n') || 'No notable digital activity'}`;
}

/**
 * Generate AI-powered summaries for a batch of days (up to 5-7 per call).
 *
 * @param {Array<{ date: string, events: Array }>} dayBuckets
 * @param {string} apiKey  - DeepSeek API key
 * @returns {Promise<Array<AIDaySummary>>}
 */
async function generateAISummariesForDays(dayBuckets, apiKey) {
  if (!dayBuckets.length) return [];

  const dayContexts = dayBuckets.map(({ date, events }) =>
    buildDayContext(date, events)
  );

  const prompt = `You are a personal life AI assistant. Analyze each day of digital activity below and write a dense, paragraph-length summary (3–5 sentences) in second person ("On this day, you...").

Each summary should capture:
- What the person was *primarily* focused on or working toward
- Key people interacted with (use first names or real names, NOT email addresses)
- Topics, projects, or themes that stand out
- Any inferred preferences (food orders, entertainment, hobbies, communication style)
- Behavioral patterns (e.g., "stayed focused in the morning", "browsed many job listings", "caught up on social connections")
- The general mood or energy level if inferable

Rules:
- Be specific and information-dense; avoid generic platitudes
- Include real names, real document titles, real calendar event names
- If a day has little activity, say so briefly (1-2 sentences)
- Summaries must read like a personal diary / life log

Also extract per day:
- top_people: real names of people mentioned (not email addresses)
- tags: 1-5 tags from [work, leisure, social, travel, health, finance, education, admin]
- topics: 1-3 specific topics or projects mentioned
- intent_clusters: list the most relevant from [job_search, study, relationship_networking, side_project, personal_care, leisure, work, health]

${dayContexts.join('\n\n')}

Respond with ONLY a valid JSON array. No markdown. No explanation:
[
  {
    "date": "YYYY-MM-DD",
    "narrative": "On this day, you...",
    "top_people": ["Name1", "Name2"],
    "tags": ["work", "finance"],
    "topics": ["Project X"],
    "intent_clusters": ["work"]
  }
]`;

  const parsed = await callLLM(prompt, apiKey, 0.4, { maxTokens: 500, economy: true });
  return parsed || [];
}

/**
 * Generate a single AI summary for today, given the last N days of summaries as context.
 * Used for the ongoing daily suggestion generation cycle.
 *
 * @param {Object} params
 * @param {Object} params.todayEvents  - { messages, docs, calendarEvents, pageVisits }
 * @param {Array}  params.historicalSummaries - last N days of DailySummary objects (sorted desc)
 * @param {Object} params.userProfile  - current user profile from store
 * @param {string} params.apiKey
 * @returns {Promise<{ narrative, top_people, patterns, preferences, intent_clusters, suggestions }>}
 */
/**
 * Generate a single AI summary for today, given the last N days of summaries as context.
 * Used for the ongoing daily suggestion generation cycle.
 */
async function generateTodaySummaryWithContext({
  todayEvents = {},
  historicalSummaries = [],
  userProfile = {},
  futureCal = [],
  apiKey
}) {
  if (!apiKey) throw new Error('DeepSeek API Key is required');

  const today = new Date().toISOString().slice(0, 10);

  // 1. Generate Narrative Summary (paragraph-length thematic focus)
  const narrativeData = await generateNarrativeSubAgent(today, todayEvents, historicalSummaries, apiKey);

  // 2. Specialized Proactive Jobs
  const suggestions = [];

  // Job A: Birthday / Relationship Prep (Looking forward 7 days)
  const relationshipSuggestions = await generateRelationshipSubAgent(today, futureCal, historicalSummaries, apiKey);
  if (relationshipSuggestions) suggestions.push(...relationshipSuggestions);

  // Job B: Work / Deadline Prep
  const workSuggestions = await generateWorkSubAgent(today, todayEvents, futureCal, historicalSummaries, apiKey);
  if (workSuggestions) suggestions.push(...workSuggestions);

  // Job C: Leisure / Habit Recommendations
  const leisureSuggestions = await generateLeisureSubAgent(today, userProfile, historicalSummaries, apiKey);
  if (leisureSuggestions) suggestions.push(...leisureSuggestions);

  // 3. Extract Patterns & Preferences (for profile sync)
  const intelligence = await extractIntelligenceSubAgent(today, todayEvents, historicalSummaries, apiKey);

  return {
    date: today,
    narrative: narrativeData.narrative,
    top_people: narrativeData.top_people,
    tags: narrativeData.tags,
    topics: narrativeData.topics,
    intent_clusters: narrativeData.intent_clusters,
    suggestions: suggestions.slice(0, 5), // Cap at 5 total actionable items
    patterns: intelligence.patterns || [],
    preferences: intelligence.preferences || []
  };
}

/**
 * Sub-Agent: Narrative Summary
 */
async function generateNarrativeSubAgent(today, todayEvents, historicalSummaries, apiKey) {
  const eventsContext = buildDayContext(today, todayEvents.all || []);
  const pastContext = historicalSummaries.slice(0, 7).map(s => `[${s.date}] ${s.narrative}`).join('\n');

  const prompt = `Today is ${today}. Write a rich paragraph (3-5 sentences) summarizing today in second person. Focus on main themes and specific people.
TODAY'S EVENTS:
${eventsContext}

PAST 7 DAYS CONTEXT:
${pastContext}

RESPOND WITH ONLY JSON:
{
  "narrative": "...",
  "top_people": ["Name1"],
  "tags": ["work", "leisure"],
  "topics": ["Project X"],
  "intent_clusters": ["job_search"]
}`;
  return callLLM(prompt, apiKey, 0.3, { maxTokens: 450, economy: true });
}

/**
 * Sub-Agent: Relationship Prep
 */
async function generateRelationshipSubAgent(today, futureCal, historicalSummaries, apiKey) {
  const upcoming = (futureCal || []).filter(c => {
    const title = (c.summary || c.title || '').toLowerCase();
    return title.includes('birthday') || title.includes('anniversary');
  }).slice(0, 3);

  if (!upcoming.length) return null;

  const prompt = `Analyze these upcoming relationship events:
${upcoming.map(c => `- ${c.summary} on ${c.start_time}`).join('\n')}

PAST CONTEXT (for relationship depth):
${historicalSummaries.map(s => `[${s.date}] People: ${s.top_people?.join(', ')} | narrative: ${s.narrative}`).join('\n')}

GENERATE 1-2 rich proactive suggestions (Birthday/Anniversary prep).
Include: tailored gift idea, message draft, outing idea BASED ON PAST CONTEXT.
Include evidence_id (the event id).

RESPOND WITH ONLY JSON ARRAY:
[{ "title": "...", "priority": "high", "category": "Relationship", "reason": "...", "context_path": "...", "evidence_id": "...", "action_type": "prepare" }]`;
  return callLLM(prompt, apiKey, 0.4, { maxTokens: 450, economy: true });
}

/**
 * Sub-Agent: Work / Deadline Prep
 */
async function generateWorkSubAgent(today, todayEvents, futureCal, historicalSummaries, apiKey) {
  const docs = (todayEvents.all || []).filter(e => e.type === 'doc' || e.type === 'spreadsheet');
  const captures = (todayEvents.all || []).filter(e => e.type === 'screen_capture').slice(0, 5);
  const upcomingWork = (futureCal || []).slice(0, 10).map(c => c.summary).join(', ');
  const captureHints = captures.map(c =>
    `${c.metadata?.sensor?.activeApp || 'Desktop'}${c.metadata?.sensor?.activeWindowTitle ? ` / ${c.metadata.sensor.activeWindowTitle}` : ''}: ${(c.text || '').slice(0, 120)}`
  ).join('\n');

  const prompt = `Analyze upcoming calendar and today's doc edits to suggest work prep.
[CAL]: ${upcomingWork}
[TODAY DOCS]: ${docs.map(e => e.title).join(', ')}
[RECENT SCREEN OCR]:
${captureHints || 'None'}

GENERATE 1-2 rich work suggestions (prep steps, doc reviews).
Include evidence_id.

RESPOND WITH ONLY JSON ARRAY:
[{ "title": "...", "priority": "medium", "category": "Work", "reason": "...", "evidence_id": "...", "action_type": "review" }]`;
  return callLLM(prompt, apiKey, 0.3, { maxTokens: 450, economy: true });
}

/**
 * Sub-Agent: Leisure
 */
async function generateLeisureSubAgent(today, userProfile, historicalSummaries, apiKey) {
  const prompt = `Based on user style (${userProfile.leisure_style || 'unknown'}) and history, suggest 1 leisure action for tonight.
RESPOND WITH ONLY JSON ARRAY:
[{ "title": "...", "priority": "low", "category": "Leisure", "reason": "...", "action_type": "schedule" }]`;
  return callLLM(prompt, apiKey, 0.5, { maxTokens: 450, economy: true });
}

/**
 * Sub-Agent: Intelligence
 */
async function extractIntelligenceSubAgent(today, todayEvents, historicalSummaries, apiKey) {
  const prompt = `Extract behavioral patterns and preferences from today's events (${today}).
RESPOND WITH ONLY JSON:
{ "patterns": ["..."], "preferences": ["..."] }`;
  return callLLM(prompt, apiKey, 0.2, { maxTokens: 450, economy: true });
}

module.exports = { buildDayContext, generateAISummariesForDays, generateTodaySummaryWithContext };
