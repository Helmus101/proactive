'use strict';

const crypto = require('crypto');
const db = require('../db');

// ─── Utilities ────────────────────────────────────────────────────────────────

function asObj(v) {
  if (!v) return {};
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch (_) { return {}; }
}

function parseTs(v) {
  if (!v && v !== 0) return 0;
  if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
  const n = Date.parse(v);
  return Number.isFinite(n) ? n : 0;
}

function trim(text, max = 220) {
  return String(text || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function parseList(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v;
  try { const p = JSON.parse(v); return Array.isArray(p) ? p : []; } catch (_) { return []; }
}

function stableId(prefix, value) {
  return `${prefix}_${crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 16)}`;
}

function daysSince(timestamp, now) {
  const ts = parseTs(timestamp);
  if (!ts) return 365;
  return Math.max(0, (now - ts) / (24 * 60 * 60 * 1000));
}

const NEWS_DOMAINS = [
  'techcrunch.com', 'nytimes.com', 'theverge.com', 'wired.com', 'bloomberg.com',
  'forbes.com', 'wsj.com', 'reuters.com', 'ft.com', 'businessinsider.com', 'venturebeat.com'
];

// ─── Data Fetching ────────────────────────────────────────────────────────────

async function fetchContacts(limit = 120) {
  const rows = await db.allQuery(
    `SELECT id, display_name, company, role, status, strength_score, warmth_score, depth_score,
            last_interaction_at, interaction_count_30d, relationship_summary, metadata
     FROM relationship_contacts
     WHERE display_name IS NOT NULL AND length(display_name) > 1
     ORDER BY depth_score DESC, strength_score DESC
     LIMIT ?`,
    [Math.max(10, Number(limit || 120))]
  ).catch(() => []);
  return (rows || []).map(row => ({ ...row, metadata: asObj(row.metadata) }));
}

async function fetchInteractionHistory(contactId, limit = 6) {
  const rows = await db.allQuery(
    `SELECT event_id, source_app, context_snippet, timestamp, mention_type
     FROM relationship_mentions
     WHERE contact_id = ?
     ORDER BY datetime(timestamp) DESC
     LIMIT ?`,
    [contactId, limit]
  ).catch(() => []);
  return rows || [];
}

async function fetchInteractionCountsByContact(contactIds = []) {
  if (!contactIds.length) return {};
  const since30 = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const since60 = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString();
  const placeholders = contactIds.map(() => '?').join(',');
  const rows = await db.allQuery(
    `SELECT contact_id,
            COUNT(*) as total,
            SUM(CASE WHEN datetime(timestamp) >= datetime(?) THEN 1 ELSE 0 END) as count_30d,
            SUM(CASE WHEN datetime(timestamp) >= datetime(?) THEN 1 ELSE 0 END) as count_60d
     FROM relationship_mentions
     WHERE contact_id IN (${placeholders})
     GROUP BY contact_id`,
    [since30, since60, ...contactIds]
  ).catch(() => []);
  const map = {};
  for (const row of rows || []) {
    map[row.contact_id] = {
      total: Number(row.total || 0),
      count_30d: Number(row.count_30d || 0),
      count_60d: Number(row.count_60d || 0)
    };
  }
  return map;
}

async function fetchUpcomingCalendarEvents(now, windowHours = 4) {
  const from = new Date(now).toISOString();
  const to = new Date(now + windowHours * 60 * 60 * 1000).toISOString();
  const rows = await db.allQuery(
    `SELECT id, title, text, timestamp, occurred_at, metadata
     FROM events
     WHERE (type = 'calendar' OR source_type = 'google_calendar')
       AND datetime(COALESCE(occurred_at, timestamp)) BETWEEN datetime(?) AND datetime(?)
     ORDER BY datetime(COALESCE(occurred_at, timestamp)) ASC
     LIMIT 10`,
    [from, to]
  ).catch(() => []);
  return rows || [];
}

// ─── Fallback Draft Builders ──────────────────────────────────────────────────

function buildFallbackDraft(contact, triggerType, context = {}) {
  const firstName = trim(contact.display_name || 'there', 40).split(' ')[0];
  const receipts = Array.isArray(context.receipts) ? context.receipts : [];
  const latestSnippet = receipts.find(r => r.context_snippet)?.context_snippet || '';
  const topics = parseList(contact.metadata?.topics || []).slice(0, 2).join(' and ');

  switch (triggerType) {
    case 'birthday':
      return `Happy birthday ${firstName}! Hope you have a great one.`;

    case 'job_change': {
      const role = context.new_role || 'the new role';
      const co = context.new_company ? ` at ${context.new_company}` : '';
      return `Hey ${firstName}, congrats on ${role}${co}! How's the transition going so far?`;
    }

    case 'news_mention': {
      const headline = trim(context.news_headline || '', 80);
      return headline
        ? `Hey ${firstName}, saw you in the news — "${headline}". Exciting stuff. How's everything going?`
        : `Hey ${firstName}, saw you in the news today. Exciting! How are things going?`;
    }

    case 'meeting_prep': {
      const bullets = receipts.slice(0, 2)
        .map(r => r.context_snippet)
        .filter(Boolean)
        .map(s => `• ${trim(s, 100)}`);
      if (bullets.length) return `Context from last interaction:\n${bullets.join('\n')}`;
      return topics
        ? `• Shared topics from past conversations: ${topics}`
        : `No recent conversation context captured for ${firstName}.`;
    }

    default:
      return latestSnippet
        ? `Hey ${firstName}, following up on when we spoke about ${trim(latestSnippet, 90)}. How's that going?`
        : `Hey ${firstName}${topics ? `, came across something related to ${topics}` : ', wanted to check in'}. How are things?`;
  }
}

// ─── Nurture Signal Detection ─────────────────────────────────────────────────

async function detectNurtureSignals(contacts, interactionCounts, now) {
  const signals = [];

  for (const contact of contacts) {
    const strength = Number(contact.strength_score || 0);
    const depth = Number(contact.depth_score || 0);

    // Only nurture contacts with actual relationship history
    if (depth < 0.15 && strength < 0.25) continue;

    const days = daysSince(contact.last_interaction_at, now);

    // Threshold scales with relationship depth: stronger ties get nudged sooner
    const threshold = strength > 0.65 ? 40 : strength > 0.45 ? 55 : 80;
    if (days < threshold) continue;

    const counts = interactionCounts[contact.id] || {};
    const current30d = Number(contact.interaction_count_30d || counts.count_30d || 0);
    const prior30d = Math.max(0, Number(counts.count_60d || 0) - current30d);

    // Frequency drop: were they active before but silent now?
    const frequencyDrop = prior30d > 1 ? 1 - (current30d / prior30d) : 0;
    const hasDropped = frequencyDrop > 0.55 && prior30d >= 2;

    const receipts = await fetchInteractionHistory(contact.id, 3);
    const latestSnippet = receipts.find(r => r.context_snippet)?.context_snippet || '';

    const importance = Math.max(strength, depth);
    const urgency = Math.min(1, days / 110);
    const score = (importance * 0.5) + (urgency * 0.38) + (hasDropped ? 0.12 : 0);

    const triggerType = hasDropped ? 'frequency_drop' : 'dormancy';

    let whyNow;
    if (hasDropped && prior30d > 0) {
      whyNow = `You two had ${prior30d} interactions the month before but have been quiet for ${Math.round(days)} days.`;
    } else if (days > 90) {
      whyNow = `${Math.round(days)} days since your last contact — longer than usual for this relationship.`;
    } else {
      whyNow = `${Math.round(days)} days since last contact${strength > 0.5 ? ' — strong relationship worth maintaining' : ''}.`;
    }

    const timeLabel = days < 50 ? `${Math.round(days)}d` : `${Math.round(days / 7)}w`;

    signals.push({
      id: stableId('nurture', `${contact.id}_${Math.floor(days / 5)}`),
      suggestion_type: 'Nurture',
      signal_type: 'relationship',
      category: 'relationship_intelligence',
      person: contact.display_name,
      contact_id: contact.id,
      days_since_contact: Math.round(days),
      title: `${contact.display_name} — ${Math.round(days)}d since last contact`,
      why_now: trim(whyNow, 200),
      evidence: trim(contact.relationship_summary || latestSnippet || '', 180),
      trigger_event: {
        type: triggerType,
        description: hasDropped ? `${Math.round(days)}d dormant · was ${prior30d}/mo` : `${Math.round(days)}d dormant`,
        days: Math.round(days),
        prior30d,
        current30d
      },
      priority: score > 0.72 ? 'high' : score > 0.48 ? 'medium' : 'low',
      score: Math.min(0.97, score),
      relationship_score: strength,
      draft_opener: buildFallbackDraft(contact, triggerType, { receipts }),
      primary_action: { label: 'Draft reply' },
      suggested_actions: [
        { label: 'Draft reply' },
        { label: 'Schedule call' },
        { label: 'Snooze' }
      ],
      time_anchor: `${timeLabel} ago`,
      display: { person: contact.display_name, headline: contact.display_name, summary: whyNow }
    });
  }

  return signals.sort((a, b) => b.score - a.score).slice(0, 8);
}

// ─── Life Event Detection ─────────────────────────────────────────────────────

async function detectLifeEventSignals(contacts, recentEvents, now) {
  const signals = [];
  const contactsByLower = new Map(contacts.map(c => [c.display_name.toLowerCase(), c]));

  // 1. Birthday signals (from contact metadata)
  for (const contact of contacts) {
    const birthday = contact.metadata?.birthday;
    if (!birthday) continue;

    const parts = String(birthday).match(/(?:^|\D)(\d{1,2})-(\d{1,2})(?:$|\D)/);
    if (!parts) continue;

    // Handles "YYYY-MM-DD", "MM-DD"
    const raw = String(birthday);
    const dateParts = raw.match(/(\d{4})-(\d{2})-(\d{2})/) || raw.match(/(\d{2})-(\d{2})/);
    if (!dateParts) continue;

    let bMonth, bDay;
    if (dateParts.length === 4) {
      bMonth = parseInt(dateParts[2], 10) - 1;
      bDay = parseInt(dateParts[3], 10);
    } else {
      bMonth = parseInt(dateParts[1], 10) - 1;
      bDay = parseInt(dateParts[2], 10);
    }

    const today = new Date(now);
    const thisYear = new Date(today.getFullYear(), bMonth, bDay);
    const nextYear = new Date(today.getFullYear() + 1, bMonth, bDay);
    const daysUntil = (thisYear - now) / (24 * 60 * 60 * 1000) >= 0
      ? Math.round((thisYear - now) / (24 * 60 * 60 * 1000))
      : Math.round((nextYear - now) / (24 * 60 * 60 * 1000));

    if (daysUntil > 7) continue;

    const label = daysUntil === 0 ? 'today' : `in ${daysUntil} day${daysUntil === 1 ? '' : 's'}`;
    signals.push({
      id: stableId('birthday', `${contact.id}_${today.getFullYear()}`),
      suggestion_type: 'Life Event',
      signal_type: 'relationship',
      category: 'relationship_intelligence',
      person: contact.display_name,
      contact_id: contact.id,
      days_since_contact: Math.round(daysSince(contact.last_interaction_at, now)),
      title: `${contact.display_name}'s birthday is ${label}`,
      why_now: `Birthday ${label}. A short personal message now is worth more than a belated one later.`,
      evidence: contact.relationship_summary || '',
      trigger_event: { type: 'birthday', description: `Birthday ${label}`, days_until: daysUntil },
      priority: daysUntil === 0 ? 'high' : 'medium',
      score: daysUntil === 0 ? 0.97 : 0.85,
      relationship_score: Number(contact.strength_score || 0),
      draft_opener: buildFallbackDraft(contact, 'birthday', {}),
      primary_action: { label: 'Send birthday message' },
      suggested_actions: [{ label: 'Send birthday message' }, { label: 'Schedule call' }, { label: 'Snooze' }],
      time_anchor: `birthday ${label}`,
      display: { person: contact.display_name, headline: contact.display_name, summary: `Birthday ${label}` }
    });
  }

  // 2. Job change signals from semantic nodes
  const jobChangeRows = await db.allQuery(
    `SELECT id, title, summary, canonical_text, metadata, updated_at, created_at
     FROM memory_nodes
     WHERE layer = 'semantic'
       AND (
         subtype = 'job_change'
         OR (subtype = 'person' AND (
           lower(canonical_text) LIKE '%new role%' OR lower(canonical_text) LIKE '%joined%'
           OR lower(canonical_text) LIKE '%started at%' OR lower(canonical_text) LIKE '%promoted to%'
           OR lower(canonical_text) LIKE '%new position%' OR lower(canonical_text) LIKE '%now at%'
         ))
       )
       AND datetime(COALESCE(updated_at, created_at)) >= datetime(?)
     LIMIT 40`,
    [new Date(now - 45 * 24 * 60 * 60 * 1000).toISOString()]
  ).catch(() => []);

  for (const node of jobChangeRows || []) {
    const meta = asObj(node.metadata);
    const personName = trim(node.title || meta.person_name || '', 80);
    if (!personName || personName.length < 3) continue;

    const contact = contactsByLower.get(personName.toLowerCase())
      || contacts.find(c => {
        const parts = personName.toLowerCase().split(' ');
        return parts.length > 1 && c.display_name.toLowerCase().includes(parts[0]) && c.display_name.toLowerCase().includes(parts[parts.length - 1]);
      });
    if (!contact) continue;

    const newRole = trim(meta.new_role || meta.role || '', 60);
    const newCompany = trim(meta.new_company || meta.company || '', 60);
    const daysAgo = Math.round(daysSince(node.updated_at || node.created_at, now));

    signals.push({
      id: stableId('job_change', `${contact.id}_${node.id}`),
      suggestion_type: 'Life Event',
      signal_type: 'relationship',
      category: 'relationship_intelligence',
      person: contact.display_name,
      contact_id: contact.id,
      days_since_contact: Math.round(daysSince(contact.last_interaction_at, now)),
      title: `${contact.display_name} started a new role${newCompany ? ` at ${newCompany}` : ''}`,
      why_now: `Job changes are the highest-signal moment to reconnect — they're starting fresh and thinking about their network.${daysAgo < 14 ? ` This happened ${daysAgo} days ago — still a great window.` : ''}`,
      evidence: trim(node.summary || node.canonical_text || '', 180),
      trigger_event: {
        type: 'job_change',
        description: `Job change · ${daysAgo}d ago`,
        new_role: newRole,
        new_company: newCompany,
        days_ago: daysAgo
      },
      priority: daysAgo < 14 ? 'high' : 'medium',
      score: Math.min(0.95, 0.9 - (daysAgo / 80)),
      relationship_score: Number(contact.strength_score || 0),
      draft_opener: buildFallbackDraft(contact, 'job_change', { new_role: newRole, new_company: newCompany }),
      primary_action: { label: 'Send congratulations' },
      suggested_actions: [{ label: 'Send congratulations' }, { label: 'Schedule call' }, { label: 'Snooze' }],
      time_anchor: `job change ${daysAgo}d ago`,
      display: { person: contact.display_name, headline: contact.display_name, summary: `New role${newCompany ? ` at ${newCompany}` : ''} · ${daysAgo}d ago` }
    });
  }

  // 3. News mention signals from browsing history
  const newsEvents = (recentEvents || []).filter(e => {
    const domain = String(asObj(e.metadata).domain || '').toLowerCase();
    return NEWS_DOMAINS.some(d => domain.includes(d));
  });

  for (const event of newsEvents.slice(0, 30)) {
    const title = String(event.window_title || event.text || '');
    if (title.length < 10) continue;

    const titleLower = title.toLowerCase();
    for (const contact of contacts) {
      if (Number(contact.strength_score || 0) < 0.15) continue;
      const nameParts = contact.display_name.split(' ').filter(p => p.length >= 3);
      if (nameParts.length < 2) continue;
      const fullMatch = nameParts.every(part => titleLower.includes(part.toLowerCase()));
      if (!fullMatch) continue;

      const daysAgo = Math.round(daysSince(event.timestamp, now));
      const domain = asObj(event.metadata).domain || '';

      signals.push({
        id: stableId('news', `${contact.id}_${event.id}`),
        suggestion_type: 'Life Event',
        signal_type: 'relationship',
        category: 'relationship_intelligence',
        person: contact.display_name,
        contact_id: contact.id,
        days_since_contact: Math.round(daysSince(contact.last_interaction_at, now)),
        title: `${contact.display_name} is in the news`,
        why_now: `You saw news about ${contact.display_name} on ${domain || 'a news site'}${daysAgo === 0 ? ' today' : ` ${daysAgo}d ago`} — a natural opening to reach out.`,
        evidence: trim(title, 180),
        trigger_event: {
          type: 'news_mention',
          description: `In the news${daysAgo === 0 ? ' · today' : ` · ${daysAgo}d ago`}`,
          news_headline: trim(title, 120),
          domain
        },
        priority: 'medium',
        score: 0.78,
        relationship_score: Number(contact.strength_score || 0),
        draft_opener: buildFallbackDraft(contact, 'news_mention', { news_headline: title }),
        primary_action: { label: 'Send congratulations' },
        suggested_actions: [{ label: 'Send congratulations' }, { label: 'Share article' }, { label: 'Snooze' }],
        time_anchor: daysAgo === 0 ? 'today' : `${daysAgo}d ago`,
        display: { person: contact.display_name, headline: contact.display_name, summary: `In the news: ${trim(title, 80)}` }
      });
      break;
    }
  }

  return signals.sort((a, b) => b.score - a.score).slice(0, 6);
}

// ─── Meeting Prep Detection ───────────────────────────────────────────────────

async function detectMeetingPrepSignals(contacts, now) {
  const signals = [];
  const upcomingEvents = await fetchUpcomingCalendarEvents(now, 4);
  if (!upcomingEvents.length) return signals;

  // Build email → contact and name → contact lookup indices
  const byEmail = new Map();
  const byName = new Map();
  for (const contact of contacts) {
    const emails = parseList(contact.metadata?.emails || contact.metadata?.email ? [contact.metadata.email] : []);
    for (const email of emails) {
      if (email) byEmail.set(String(email).toLowerCase(), contact);
    }
    byName.set(contact.display_name.toLowerCase(), contact);
  }

  for (const event of upcomingEvents) {
    const meta = asObj(event.metadata);
    const attendeeList = parseList(meta.attendees || []);
    const organizer = meta.organizer;
    const allParticipants = [...attendeeList, organizer].filter(Boolean);

    const eventStartMs = parseTs(meta.start_time || meta.start || event.occurred_at || event.timestamp);
    const minutesUntil = eventStartMs ? Math.round((eventStartMs - now) / (60 * 1000)) : null;
    if (minutesUntil !== null && minutesUntil < 0) continue;

    const matched = [];
    for (const participant of allParticipants) {
      const email = typeof participant === 'string' ? participant : (participant?.email || '');
      const name = typeof participant === 'string' ? '' : (participant?.name || '');
      const contact = (email ? byEmail.get(email.toLowerCase()) : null)
        || (name ? byName.get(name.toLowerCase()) : null);
      if (contact && !matched.find(c => c.id === contact.id)) matched.push(contact);
    }

    for (const contact of matched.slice(0, 2)) {
      const receipts = await fetchInteractionHistory(contact.id, 4);
      const snippets = receipts.slice(0, 3).map(r => r.context_snippet).filter(Boolean);
      const topics = parseList(contact.metadata?.topics || []).slice(0, 4);

      const minuteLabel = minutesUntil !== null ? `${minutesUntil}m` : 'soon';
      const prepLines = [];
      if (snippets.length) {
        prepLines.push(...snippets.map(s => `• ${trim(s, 100)}`));
      } else if (topics.length) {
        prepLines.push(`• Shared topics: ${topics.join(', ')}`);
      }
      if (contact.relationship_summary) {
        prepLines.push(`• ${trim(contact.relationship_summary, 120)}`);
      }

      const prepContent = prepLines.length
        ? prepLines.join('\n')
        : `No recent conversation context captured for ${contact.display_name}.`;

      const daysSinceContact = Math.round(daysSince(contact.last_interaction_at, now));
      const eventTitle = trim(event.title || meta.summary || 'Upcoming meeting', 60);

      signals.push({
        id: stableId('meeting_prep', `${event.id}_${contact.id}`),
        suggestion_type: 'Meeting Prep',
        signal_type: 'relationship',
        category: 'relationship_intelligence',
        person: contact.display_name,
        contact_id: contact.id,
        days_since_contact: daysSinceContact,
        title: `Meeting with ${contact.display_name} in ${minuteLabel}`,
        why_now: eventTitle,
        evidence: trim(snippets[0] || contact.relationship_summary || '', 180),
        trigger_event: {
          type: 'meeting_prep',
          description: `Meeting in ${minuteLabel}`,
          minutes_until: minutesUntil,
          event_title: eventTitle
        },
        priority: minutesUntil !== null && minutesUntil < 60 ? 'high' : 'medium',
        score: 0.96,
        relationship_score: Number(contact.strength_score || 0),
        draft_opener: prepContent,
        draft_is_context: true, // This is context, not an outreach message
        primary_action: { label: 'Review context' },
        suggested_actions: [{ label: 'Review context' }, { label: 'Open in Gmail' }, { label: 'Snooze' }],
        time_anchor: `in ${minuteLabel}`,
        display: { person: contact.display_name, headline: contact.display_name, summary: `Meeting in ${minuteLabel}` }
      });
    }
  }

  return signals.sort((a, b) => {
    const aMin = asObj(a.trigger_event).minutes_until || 999;
    const bMin = asObj(b.trigger_event).minutes_until || 999;
    return aMin - bMin;
  }).slice(0, 3);
}

// ─── Warm Introduction Detection ─────────────────────────────────────────────

async function detectWarmIntroSignals(contacts) {
  const signals = [];

  // Only contacts with some relationship depth
  const qualified = contacts.filter(c =>
    Number(c.depth_score || 0) > 0.18 || Number(c.strength_score || 0) > 0.28
  );
  if (qualified.length < 4) return signals;

  // Map each topic to the contacts who share it
  const topicMap = new Map();
  for (const contact of qualified) {
    const topics = parseList(contact.metadata?.topics || []);
    for (const topic of topics) {
      const normalized = String(topic || '').toLowerCase().trim();
      if (normalized.length < 4) continue;
      if (!topicMap.has(normalized)) topicMap.set(normalized, []);
      topicMap.get(normalized).push(contact);
    }
  }

  const checked = new Set();
  for (const [topic, topicContacts] of topicMap) {
    if (topicContacts.length < 2 || topicContacts.length > 10) continue;

    for (let i = 0; i < topicContacts.length; i++) {
      for (let j = i + 1; j < topicContacts.length; j++) {
        const a = topicContacts[i];
        const b = topicContacts[j];
        const pairKey = [a.id, b.id].sort().join('_');
        if (checked.has(pairKey)) continue;
        checked.add(pairKey);

        const minStrength = Math.min(Number(a.strength_score || 0), Number(b.strength_score || 0));
        if (minStrength < 0.2) continue;

        const score = 0.65 + (minStrength * 0.2);
        signals.push({
          id: stableId('warm_intro', pairKey),
          suggestion_type: 'Warm Intro',
          signal_type: 'relationship',
          category: 'relationship_intelligence',
          person: `${a.display_name} × ${b.display_name}`,
          contact_id: a.id,
          contact_id_b: b.id,
          days_since_contact: 0,
          title: `Introduce ${a.display_name} and ${b.display_name}`,
          why_now: `Both share an interest in ${topic} but haven't been connected. Could be mutually valuable.`,
          evidence: `Shared topic: ${topic}`,
          trigger_event: {
            type: 'warm_intro',
            description: `Shared: ${topic}`,
            topic,
            contact_a: a.display_name,
            contact_b: b.display_name
          },
          priority: 'medium',
          score,
          relationship_score: minStrength,
          draft_opener: `${a.display_name} — you should meet ${b.display_name}. You're both interested in ${topic} and I think you'd find each other valuable. Want me to make the intro?`,
          primary_action: { label: 'Draft intro email' },
          suggested_actions: [{ label: 'Draft intro email' }, { label: 'Snooze' }],
          time_anchor: 'now',
          display: {
            person: `${a.display_name} × ${b.display_name}`,
            headline: `Introduce ${a.display_name} and ${b.display_name}`,
            summary: `Shared interest: ${topic}`
          }
        });
      }
    }
  }

  return signals.sort((a, b) => b.score - a.score).slice(0, 3);
}

// ─── AI Draft Enhancement ─────────────────────────────────────────────────────

async function enhanceDraftsWithLLM(signals, llmConfig) {
  if (!llmConfig?.apiKey || !signals.length) return signals;

  let callLLM, buildRelationshipDraftContext;
  try {
    callLLM = require('./intelligence-engine').callLLM;
    buildRelationshipDraftContext = require('../relationship-graph').buildRelationshipDraftContext;
  } catch (_) {
    return signals;
  }

  // Only enhance outreach signals (not meeting prep context cards)
  const toEnhance = signals
    .filter(s => s.contact_id && !s.draft_is_context)
    .slice(0, 5);

  const enhancedById = new Map();

  await Promise.all(toEnhance.map(async (signal) => {
    try {
      const context = await buildRelationshipDraftContext(signal.contact_id, { limit: 4 });
      const receipts = Array.isArray(context?.receipts) ? context.receipts : [];
      if (!receipts.length) return;

      const snippets = receipts
        .slice(0, 3)
        .filter(r => r.text || r.context_snippet)
        .map(r => `• [${r.source || 'memory'}] "${trim(r.context_snippet || r.text || '', 120)}"`)
        .join('\n');

      const firstName = (signal.person || '').split(' ')[0] || signal.person || 'them';
      const te = asObj(signal.trigger_event);
      const topics = parseList(context.contact?.metadata?.topics || []).slice(0, 3);

      const triggerInstruction = (() => {
        switch (te.type) {
          case 'birthday':
            return 'They have a birthday today or soon. Be warm and brief — one specific personal touch.';
          case 'job_change':
            return `They recently changed jobs (${te.new_role || 'new role'}${te.new_company ? ` at ${te.new_company}` : ''}). Congratulate them and ask one specific question about the transition.`;
          case 'news_mention':
            return `You saw them in the news: "${te.news_headline || 'recent news'}". Reference it naturally, not as a "saw you in the news!" opener.`;
          case 'warm_intro':
            return `You want to introduce ${firstName} to ${te.contact_b || 'someone in your network'} because they share an interest in ${te.topic || 'a common area'}.`;
          default:
            return `You haven't been in touch for ${signal.days_since_contact || '?'} days. Reference something specific from past conversations — pick up mid-thread, not start over.`;
        }
      })();

      const prompt = `Write a 2-3 sentence outreach opener for ${firstName}.

Recent shared context:
${snippets || 'No recent context captured.'}
${topics.length ? `\nShared topics: ${topics.join(', ')}` : ''}

Task: ${triggerInstruction}

Rules:
- Reference a SPECIFIC detail from the context above
- Never use: "just checking in", "hope you're doing well", "it's been a while", "touching base"
- Sound like a warm colleague, not a CRM automation
- Under 80 words
- Return ONLY the message text, no explanation or quotes`;

      const raw = await callLLM(prompt, llmConfig, 0.38, {
        maxTokens: 150,
        task: 'relationship_draft',
        economy: false
      }).catch(() => null);

      if (!raw?.trim()) return;

      const cleaned = raw.trim()
        .replace(/^["'"'""]/g, '').replace(/["'"'""]$/g, '')
        .replace(/^(Here['']s|Here is|Draft:|Message:|Opener:)\s*/i, '');

      if (cleaned.length > 20) {
        enhancedById.set(signal.id, { ...signal, draft_opener: cleaned, draft_ai_generated: true });
      }
    } catch (_) {
      // Keep original fallback draft
    }
  }));

  return signals.map(s => enhancedById.get(s.id) || s);
}

// ─── Main Orchestrator ────────────────────────────────────────────────────────

async function runRelationshipSuggestionsEngine(options = {}) {
  const now = Number(options.now) || Date.now();
  const deepScan = Boolean(options.deepScan || options.deep_scan);
  const limit = Number(options.limit || 12);
  const llmConfig = options.llmConfig || null;
  const recentEvents = Array.isArray(options.recentEvents) ? options.recentEvents : [];

  try {
    const contacts = await fetchContacts(deepScan ? 150 : 80);
    if (!contacts.length) return [];

    const contactIds = contacts.map(c => c.id);
    const interactionCounts = await fetchInteractionCountsByContact(contactIds);

    const [nurture, lifeEvents, meetingPrep] = await Promise.all([
      detectNurtureSignals(contacts, interactionCounts, now),
      detectLifeEventSignals(contacts, recentEvents, now),
      detectMeetingPrepSignals(contacts, now)
    ]);

    const warmIntros = deepScan ? await detectWarmIntroSignals(contacts) : [];

    // Priority order: meeting prep (time-sensitive) → life events → nurture → warm intros
    const all = [...meetingPrep, ...lifeEvents, ...nurture, ...warmIntros];

    // Deduplicate: keep highest-scoring signal per contact
    const byContact = new Map();
    for (const signal of all) {
      const key = signal.contact_id || signal.id;
      const existing = byContact.get(key);
      // Meeting prep always wins over other types for the same contact
      if (!existing
        || (signal.suggestion_type === 'Meeting Prep' && existing.suggestion_type !== 'Meeting Prep')
        || (signal.suggestion_type === existing.suggestion_type && signal.score > existing.score)) {
        byContact.set(key, signal);
      }
    }

    const deduped = Array.from(byContact.values())
      .sort((a, b) => {
        const typeOrder = { 'Meeting Prep': 0, 'Life Event': 1, 'Nurture': 2, 'Warm Intro': 3 };
        const tDelta = (typeOrder[a.suggestion_type] ?? 9) - (typeOrder[b.suggestion_type] ?? 9);
        if (tDelta !== 0) return tDelta;
        return b.score - a.score;
      })
      .slice(0, limit);

    const final = llmConfig ? await enhanceDraftsWithLLM(deduped, llmConfig) : deduped;
    return final;
  } catch (error) {
    console.warn('[RelationshipSuggestionsEngine] Error:', error?.message || error);
    return [];
  }
}

// ─── On-demand Draft Generation ───────────────────────────────────────────────

async function generateRelationshipDraft(contactId, triggerType, triggerContext, llmConfig) {
  let buildRelationshipDraftContext;
  try {
    buildRelationshipDraftContext = require('../relationship-graph').buildRelationshipDraftContext;
  } catch (_) {
    return null;
  }

  const context = await buildRelationshipDraftContext(contactId, { limit: 5 }).catch(() => null);
  if (!context) return null;

  const contact = context.contact || {};
  const receipts = Array.isArray(context.receipts) ? context.receipts : [];

  // Fallback draft (no LLM required)
  const fallback = buildFallbackDraft(contact, triggerType, { receipts, ...triggerContext });

  if (!llmConfig?.apiKey) return { draft: fallback, ai_generated: false, context };

  try {
    const { callLLM } = require('./intelligence-engine');
    const firstName = (contact.display_name || '').split(' ')[0] || 'them';
    const snippets = receipts
      .slice(0, 4)
      .filter(r => r.text || r.context_snippet)
      .map(r => `• [${r.source || 'memory'}] "${trim(r.context_snippet || r.text || '', 120)}"`)
      .join('\n');

    const topics = parseList(contact.metadata?.topics || []).slice(0, 3);
    const te = triggerContext || {};

    const triggerInstruction = (() => {
      switch (triggerType) {
        case 'birthday':
          return 'Their birthday is today or very soon. Be warm and brief — include one specific personal touch.';
        case 'job_change':
          return `They recently changed jobs${te.new_role ? ` (${te.new_role})` : ''}${te.new_company ? ` at ${te.new_company}` : ''}. Congratulate them and ask one specific question.`;
        case 'news_mention':
          return `You saw them in the news: "${te.news_headline || 'recent news'}". Reference it naturally.`;
        case 'meeting_prep':
          return `You have a meeting with them in ${te.minutes_until || '?'} minutes. Write a brief context summary of what you know from past conversations — not an outreach message.`;
        default:
          return `You haven't spoken in ${te.days || '?'} days. Reference something specific from your past conversations — feel like picking up mid-thread.`;
      }
    })();

    const prompt = `Write a 2-3 sentence outreach message for ${firstName}.

Recent conversation context:
${snippets || 'No context captured.'}
${topics.length ? `\nShared topics: ${topics.join(', ')}` : ''}

Task: ${triggerInstruction}

Rules:
- Must reference a SPECIFIC detail from above
- Avoid: "just checking in", "it's been a while", "hope you're well", "touching base"
- Sound like a warm colleague, not a sales email
- Under 80 words
- Return ONLY the message text`;

    const raw = await callLLM(prompt, llmConfig, 0.38, { maxTokens: 160, task: 'relationship_draft', economy: false });
    if (!raw?.trim()) return { draft: fallback, ai_generated: false, context };

    const cleaned = raw.trim()
      .replace(/^["'"'""]/g, '').replace(/["'"'""]$/g, '')
      .replace(/^(Here['']s|Here is|Draft:|Message:|Opener:)\s*/i, '');

    return { draft: cleaned.length > 15 ? cleaned : fallback, ai_generated: cleaned.length > 15, context };
  } catch (_) {
    return { draft: fallback, ai_generated: false, context };
  }
}

module.exports = {
  runRelationshipSuggestionsEngine,
  generateRelationshipDraft,
  detectNurtureSignals,
  detectLifeEventSignals,
  detectMeetingPrepSignals,
  detectWarmIntroSignals,
  enhanceDraftsWithLLM,
  buildFallbackDraft
};
