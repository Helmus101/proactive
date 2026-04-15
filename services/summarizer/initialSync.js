/**
 * initialSync.js
 *
 * Performs the first-time historical backfill:
 * 1. Groups ALL raw events (emails, docs, calendar, browser history) by calendar day
 * 2. For recent days (last 90): generates AI paragraph summaries in batches
 * 3. For older days: falls back to heuristic summaries
 * 4. Each summary records evidence IDs linking back to raw source events
 * 5. Extracts cross-day user patterns and preferences
 *
 * Stores results at:
 *   store.set('historicalSummaries', { [YYYY-MM-DD]: DailySummary })
 *   store.set('userProfile', { patterns, preferences, ... })
 *   store.set('initialSyncDone', true)
 */

const { buildDailySummaries } = require('./dailySummary');
const { buildGlobalGraph } = require('../agent/intelligence-engine');
const { generateAISummariesForDays } = require('./aiDailySummary');

const AI_DAYS_LIMIT = 90;   // Use AI for the most recent N days
const BATCH_SIZE    = 5;    // Days per DeepSeek API call
const BATCH_DELAY_MS = 600; // Delay between batches to avoid rate limits

/**
 * Bucket raw events by calendar date (YYYY-MM-DD), normalising
 * the timestamp field across all event types.
 *
 * @param {Array} events  - flat array with ._type already set
 * @returns {Map<string, Array>}
 */
function bucketByDate(events) {
  const buckets = new Map();
  events.forEach(ev => {
    // Different event types use different timestamp field names
    const ts =
      ev.timestamp     ||
      ev.start_time    ||
      ev.last_modified ||
      ev.modifiedTime  ||
      ev.time          ||
      null;

    if (!ts) return;

    let date;
    try {
      date = new Date(ts).toISOString().slice(0, 10);
    } catch (_) {
      return;
    }

    if (!buckets.has(date)) buckets.set(date, []);
    buckets.get(date).push(ev);
  });
  return buckets;
}

/**
 * Merge an AI result with the existing heuristic summary for the same date.
 *
 * @param {string} date
 * @param {Object|null} aiResult      - result from generateAISummariesForDays
 * @param {Object}      heuristic     - result from buildDailySummaries
 * @param {string[]}    evidenceIds   - IDs of all raw events for this day
 * @param {string}      userId
 * @returns {DailySummary}
 */
function mergeSummary(date, aiResult, heuristic, events, evidenceIds, userId) {
  return {
    id:         `daily_${date}`,
    user_id:    userId,
    date,
    start_ts:   heuristic.start_ts || null,
    end_ts:     heuristic.end_ts   || null,

    // Narrative: prefer rich AI paragraph, fall back to heuristic
    narrative:  (aiResult?.narrative || heuristic.narrative || 'No notable activity.').trim(),

    // People / contacts / tags / topics
    top_people:   aiResult?.top_people || [],
    tags:         aiResult?.tags       || [],
    topics:       aiResult?.topics     || [],
    top_contacts: heuristic.top_contacts || [],

    // Counts
    counts: heuristic.counts || {
      emails: 0, calendar_events: 0, docs: 0, page_visits: 0
    },

    // Notable flags from heuristic rules
    notable_events: heuristic.notable_events || [],

    // Intent clusters (Section 130)
    intent_clusters: aiResult?.intent_clusters || [],

    // Attach all unified events for this day (Section 1.1)
    events: events || [],

    // Evidence links
    evidence_ids: evidenceIds,

    // Metadata
    ai_generated: !!aiResult,
    generated_at: new Date().toISOString()
  };
}

/**
 * Run the full initial historical sync.
 *
 * @param {Object} params
 * @param {string}   params.userId
 * @param {Array}    params.messages       - Gmail messages (each with .timestamp or .internalDate)
 * @param {Array}    params.docs           - Drive docs    (with .last_modified / .modifiedTime)
 * @param {Array}    params.calendarEvents - Calendar events (with .start_time / .start)
 * @param {Array}    params.pageVisits     - Browser history (with .timestamp / .last_visit_time)
 * @param {string}   params.apiKey         - DeepSeek API key (optional; falls back to heuristic only)
 * @param {Function} params.onProgress     - callback({ done, total, currentDate, phase })
 *
 * @returns {Promise<{
 *   summaries: Object,   // { [YYYY-MM-DD]: DailySummary }
 *   sortedDates: string[],
 *   userPatterns: string[],
 *   userPreferences: string[]
 * }>}
 */
async function runInitialSync({
  userId,
  messages       = [],
  docs           = [],
  calendarEvents = [],
  pageVisits     = [],
  apiKey,
  onProgress,
  store
} = {}) {

  // ── Step 1: Unify every event into a single schema (1.1) ─────────────────
  const allEvents = [
    ...messages.map(m => ({
      id:        `email_${m.id || Math.random().toString(36).slice(2, 11)}`,
      date:      new Date(m.timestamp || parseInt(m.internalDate, 10)).toISOString().slice(0, 10),
      time:      new Date(m.timestamp || parseInt(m.internalDate, 10)).toISOString().slice(11, 19),
      type:      'email',
      source_id: m.id,
      title:     m.subject || '(no subject)',
      text:      m.snippet || m.body || '',
      people:    [m.from, ...(m.to || [])].filter(Boolean),
      tags:      [], // Will be populated by metadata pass or heuristics
      metadata:  { email: { from: m.from, to: m.to || [] } },
      timestamp: m.timestamp || parseInt(m.internalDate, 10)
    })),
    ...docs.map(d => ({
      id:        `doc_${d.id || Math.random().toString(36).slice(2, 11)}`,
      date:      new Date(d.last_modified || d.modifiedTime).toISOString().slice(0, 10),
      time:      new Date(d.last_modified || d.modifiedTime).toISOString().slice(11, 19),
      type:      d.mimeType?.includes('spreadsheet') ? 'spreadsheet' : d.mimeType?.includes('presentation') ? 'slide' : 'doc',
      source_id: d.id,
      title:     d.name || '(untitled)',
      text:      '', // Snippet extraction usually requires another API call, for now leave empty
      people:    [], 
      tags:      [],
      metadata:  { doc: { last_modified: d.last_modified || d.modifiedTime, mimeType: d.mimeType } },
      timestamp: d.last_modified || d.modifiedTime
    })),
    ...calendarEvents.map(c => ({
      id:        `cal_${c.id || Math.random().toString(36).slice(2, 11)}`,
      date:      new Date(c.start_time || c.start).toISOString().slice(0, 10),
      time:      new Date(c.start_time || c.start).toISOString().slice(11, 19),
      type:      'calendar_event',
      source_id: c.id,
      title:     c.summary || c.title || '(no title)',
      text:      c.description || '',
      people:    (c.attendees || []).map(a => a.email || a.displayName || a).filter(Boolean),
      tags:      [],
      metadata:  { calendar: { start: c.start_time || c.start, end: c.end_time || c.end, location: c.location } },
      timestamp: c.start_time || c.start
    })),
    ...pageVisits.map(p => {
      const ts = p.timestamp || (p.last_visit_time ? (p.last_visit_time > 1e12 ? p.last_visit_time / 1000 : p.last_visit_time) : Date.now());
      const urlObj = new URL(p.url || 'http://unknown');
      return {
        id:        `hist_${Math.random().toString(36).slice(2, 11)}`,
        date:      new Date(ts).toISOString().slice(0, 10),
        time:      new Date(ts).toISOString().slice(11, 19),
        type:      'browser_history',
        source_id: p.id || '',
        title:     p.title || p.url || '',
        text:      '',
        people:    [],
        tags:      [],
        metadata:  { history: { url: p.url, domain: urlObj.hostname } },
        timestamp: ts
      };
    })
  ];

  // ── Step 2: Group by date ────────────────────────────────────────────────
  const buckets   = bucketByDate(allEvents);
  const sortedDates = Array.from(buckets.keys()).sort((a, b) => b.localeCompare(a));

  if (onProgress) onProgress({ phase: 'bucketing', done: 0, total: sortedDates.length });

  // ── Step 3: Heuristic summaries for ALL dates (quick, no API) ───────────
  const heuristicResults = buildDailySummaries({
    userId,
    messages,
    docs,
    calendarEvents,
    pageVisits
  });

  // Index heuristic results by date for easy lookup
  const heuristicByDate = {};
  heuristicResults.forEach(s => { heuristicByDate[s.date] = s; });

  // ── Step 4: Prepare evidence IDs per date ───────────────────────────────
  const evidenceByDate = {};
  buckets.forEach((events, date) => {
    evidenceByDate[date] = events.map(e => e.id).filter(Boolean);
  });

  const summariesByDate = {};

  // ── Step 5: Older dates → use heuristic only ────────────────────────────
  const olderDates = sortedDates.slice(AI_DAYS_LIMIT);
  olderDates.forEach(date => {
    summariesByDate[date] = mergeSummary(
      date,
      null,
      heuristicByDate[date] || {},
      buckets.get(date)     || [],
      evidenceByDate[date]  || [],
      userId
    );
  });

  // ── Step 6: Recent dates → AI summaries in batches ──────────────────────
  const recentDates = sortedDates.slice(0, AI_DAYS_LIMIT);
  let done = 0;

  if (onProgress) onProgress({ phase: 'ai_summaries', done: 0, total: recentDates.length });

  for (let i = 0; i < recentDates.length; i += BATCH_SIZE) {
    const batchDates  = recentDates.slice(i, i + BATCH_SIZE);
    const dayBuckets  = batchDates.map(date => ({
      date,
      events: buckets.get(date) || []
    }));

    let aiResults = [];

    if (apiKey) {
      try {
        aiResults = await generateAISummariesForDays(dayBuckets, apiKey);
      } catch (err) {
        console.warn(
          `[initialSync] AI batch failed for ${batchDates[0]}–${batchDates[batchDates.length - 1]}:`,
          err.message
        );
        // Continue with heuristic fallback for this batch
      }
    }

    batchDates.forEach(date => {
      const aiResult  = aiResults.find(r => r.date === date) || null;
      summariesByDate[date] = mergeSummary(
        date,
        aiResult,
        heuristicByDate[date] || {},
        buckets.get(date)     || [],
        evidenceByDate[date]  || [],
        userId
      );
    });

    done += batchDates.length;
    if (onProgress) {
      onProgress({
        phase: 'ai_summaries',
        done,
        total: recentDates.length,
        currentDate: batchDates[batchDates.length - 1]
      });
    }

    // Throttle between batches
    if (i + BATCH_SIZE < recentDates.length) {
      await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
    }
  }

  // ── Step 7: Extract cross-day patterns and preferences ───────────────────
  const allPatterns    = [];
  const allPreferences = [];
  const intentCounts   = {};

  Object.values(summariesByDate).forEach(s => {
    (s.patterns       || []).forEach(p => allPatterns.push(p));
    (s.preferences    || []).forEach(p => allPreferences.push(p));
    (s.intent_clusters || []).forEach(c => {
      intentCounts[c] = (intentCounts[c] || 0) + 1;
    });
  });

  // Deduplicate (simple text dedup)
  const dedupe = arr => [...new Set(arr.map(s => s.trim()).filter(Boolean))];

  const userPatterns    = dedupe(allPatterns);
  const userPreferences = dedupe(allPreferences);

  // Sort intent clusters by frequency
  const topIntentClusters = Object.entries(intentCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([cluster, count]) => ({ cluster, count }));

  if (onProgress) {
    onProgress({
      phase:   'complete',
      done:    sortedDates.length,
      total:   sortedDates.length,
      currentDate: null
    });
  }

  return {
    summaries:        summariesByDate,
    sortedDates,
    userPatterns,
    userPreferences,
    topIntentClusters
  };
}

/**
 * Search historical summaries for a keyword (e.g., a person's name, a topic).
 * Searches: narrative, top_people, top_contacts, top_domains, notable_events.
 *
 * @param {Object} summariesByDate  - { [YYYY-MM-DD]: DailySummary }
 * @param {string} query
 * @param {number} [limit=20]
 * @param {string[]} [indexedDates=[]] 
 * @returns {Array<{ date, score, snippet }>}
 */
function searchSummaries(summariesByDate, query, limit = 20, indexedDates = []) {
  if (!query || !summariesByDate) return [];

  const q = query.toLowerCase().trim();
  const results = [];

  // Determine the set of dates to search. 
  // If we have indexed dates, we search those with higher priority.
  // We still scan all dates for fuzzy matches if results are sparse.
  const datesToSearch = indexedDates.length > 0 ? [...new Set([...indexedDates, ...Object.keys(summariesByDate)])] : Object.keys(summariesByDate);

  datesToSearch.forEach(date => {
    const summary = summariesByDate[date];
    if (!summary) return;
    let score = indexedDates.includes(date) ? 5 : 0; // Bonus for indexed match
    const matches = [];

    // Check narrative
    const narrative = (summary.narrative || '').toLowerCase();
    if (narrative.includes(q)) {
      score += 3;
      // Extract snippet around match
      const idx = narrative.indexOf(q);
      const start = Math.max(0, idx - 60);
      const end   = Math.min(narrative.length, idx + q.length + 60);
      matches.push(`...${summary.narrative.slice(start, end)}...`);
    }

    // Check top_people
    const people = [
      ...(summary.top_people   || []),
      ...(summary.top_contacts || [])
    ];
    people.forEach(name => {
      if ((name || '').toLowerCase().includes(q)) {
        score += 2;
        matches.push(`Person: ${name}`);
      }
    });

    // Check top_domains
    (summary.top_domains || []).forEach(domain => {
      if ((domain || '').toLowerCase().includes(q)) {
        score += 1;
        matches.push(`Domain: ${domain}`);
      }
    });

    // Check notable_events
    (summary.notable_events || []).forEach(ev => {
      if ((ev.summary || '').toLowerCase().includes(q)) {
        score += 1;
        matches.push(`Event: ${ev.summary}`);
      }
    });

    // Check patterns / preferences
    [...(summary.patterns || []), ...(summary.preferences || [])].forEach(p => {
      if ((p || '').toLowerCase().includes(q)) {
        score += 1;
      }
    });

    if (score > 0) {
      results.push({
        date,
        score,
        snippet:   matches[0] || summary.narrative?.slice(0, 120) || '',
        narrative: summary.narrative || '',
        top_people: summary.top_people || [],
        intent_clusters: summary.intent_clusters || []
      });
    }
  });

  return results.sort((a, b) => b.score - a.score).slice(0, limit);
}

module.exports = { runInitialSync, searchSummaries };
