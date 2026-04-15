let parseISO, startOfDay, endOfDay, formatISO;
try {
  ({ parseISO, startOfDay, endOfDay, formatISO } = require('date-fns'));
} catch (e) {
  // Fallback minimal implementations to avoid crashing when date-fns isn't installed.
  parseISO = (s) => new Date(s);
  startOfDay = (d) => { const dt = new Date(d); dt.setHours(0,0,0,0); return dt; };
  endOfDay = (d) => { const dt = new Date(d); dt.setHours(23,59,59,999); return dt; };
  formatISO = (d) => (d instanceof Date ? d.toISOString() : new Date(d).toISOString());
}

/**
 * Build a daily summary from arrays of events: messages, docs, calendarEvents, pageVisits
 * Each event should have a timestamp property (ISO string) and an id.
 */
function buildDailySummaries({ userId, messages = [], docs = [], calendarEvents = [], pageVisits = [] }) {
  // Combine events and bucket by date (YYYY-MM-DD)
  const all = [];
  messages.forEach(m => all.push(Object.assign({ _type: 'message' }, m)));
  docs.forEach(d => all.push(Object.assign({ _type: 'doc' }, d)));
  calendarEvents.forEach(c => all.push(Object.assign({ _type: 'calendar' }, c)));
  pageVisits.forEach(p => all.push(Object.assign({ _type: 'page' }, p)));

  const buckets = new Map();

  all.forEach(ev => {
    const ts = ev.timestamp || ev.start_time || ev.last_modified || ev.time || null;
    if (!ts) return;
    const date = (new Date(ts)).toISOString().slice(0,10); // YYYY-MM-DD
    if (!buckets.has(date)) buckets.set(date, []);
    buckets.get(date).push(ev);
  });

  const summaries = [];

  for (const [date, events] of buckets.entries()) {
    const startTs = startOfDay(new Date(date));
    const endTs = endOfDay(new Date(date));
    const counts = { emails: 0, calendar_events: 0, docs: 0, page_visits: 0 };
    const domainCounts = {};
    const contactCounts = {};
    const evidenceIds = [];
    const notable = [];

    for (const ev of events) {
      evidenceIds.push(ev.id || null);
      if (ev._type === 'message') {
        counts.emails += 1;
        const from = ev.from || ev.sender || null;
        if (from) contactCounts[from] = (contactCounts[from] || 0) + 1;
        // detect opened/replied patterns
        if (ev.opened_count && ev.opened_count > 2 && ev.reply_status === 'no_reply') {
          notable.push({ type: 'awaiting_reply', id: ev.id, summary: ev.snippet || ev.subject });
        }
      } else if (ev._type === 'doc') {
        counts.docs += 1;
        const name = ev.doc_name || ev.name || '';
        if (name && /draft|in review|review|final|submitted/i.test(name)) {
          notable.push({ type: 'doc_state', id: ev.doc_id || ev.id, summary: name });
        }
      } else if (ev._type === 'calendar') {
        counts.calendar_events += 1;
        const title = ev.title || '';
        if (title && /interview|presentation|deadline|exam/i.test(title)) {
          notable.push({ type: 'calendar_flag', id: ev.id, summary: title });
        }
        const attendees = ev.attendees || [];
        attendees.forEach(a => { contactCounts[a] = (contactCounts[a] || 0) + 1; });
      } else if (ev._type === 'page') {
        counts.page_visits += 1;
        const url = ev.url || '';
        try {
          const u = new URL(url);
          domainCounts[u.hostname] = (domainCounts[u.hostname] || 0) + 1;
        } catch(e) {}
      }
    }

    // Build top lists
    const top_domains = Object.entries(domainCounts).sort((a,b)=>b[1]-a[1]).slice(0,6).map(x=>x[0]);
    const top_contacts = Object.entries(contactCounts).sort((a,b)=>b[1]-a[1]).slice(0,6).map(x=>x[0]);

    // Narrative heuristics — produce a richer paragraph summarizing emails, sites, docs, and calendar events.
    const isPromotional = (ev) => {
      try {
        const s = ((ev.subject || '') + ' ' + (ev.body || '') + ' ' + (ev.from || '')).toLowerCase();
        const promoWords = ['unsubscribe', 'newsletter', 'promo', 'sale', 'deal', 'offer', 'ads', 'advert', 'sponsored', 'buy now', 'limited time'];
        if ((ev.from || '').toLowerCase().includes('no-reply') || (ev.from || '').toLowerCase().includes('noreply')) return true;
        for (const w of promoWords) if (s.includes(w)) return true;
        return false;
      } catch (e) { return false; }
    };

    // Emails: list up to 5 important emails (non-promotional and/or opened/replied)
    const emailEvents = events.filter(e => e._type === 'message');
    const importantEmails = emailEvents.filter(e => !isPromotional(e)).sort((a,b) => (b.opened_count||0) - (a.opened_count||0));
    const topEmails = importantEmails.slice(0,5).map(e => {
      const who = e.from || e.sender || (e.from_name || '') || 'Someone';
      const subj = (e.subject || e.snippet || '').trim();
      return subj ? `${who}: “${subj.length>120?subj.slice(0,117)+'...':subj}”` : `${who}`;
    });

    // Sites: describe top domains and top page titles (up to 4)
    const topPages = events.filter(e => e._type === 'page').slice(0,6).map(p => ({ title: p.page_title || p.url || '', url: p.url || '' }));
    const topPageTitles = topPages.filter(p=>p.title).slice(0,4).map(p=>p.title.replace(/\s+/g,' ').trim());

    // Docs: list top doc names edited
    const docEvents = events.filter(e => e._type === 'doc');
    const topDocs = docEvents.slice(0,5).map(d => d.doc_name || d.name || '').filter(Boolean);

    // Calendar: summarize events
    const calEvents = events.filter(e => e._type === 'calendar');
    const topCal = calEvents.slice(0,6).map(c => c.title || c.event_title || '').filter(Boolean);

    // Build paragraph pieces
    const pieces = [];
    // Opening overview
    const overviewParts = [];
    if (counts.emails) overviewParts.push(`${counts.emails} email${counts.emails>1?'s':''}`);
    if (counts.calendar_events) overviewParts.push(`${counts.calendar_events} calendar event${counts.calendar_events>1?'s':''}`);
    if (counts.docs) overviewParts.push(`${counts.docs} document${counts.docs>1?'s':''} edited`);
    if (counts.page_visits) overviewParts.push(`${counts.page_visits} page visit${counts.page_visits>1?'s':''}`);
    if (overviewParts.length) pieces.push(`Today you had ${overviewParts.join(', ')}.`);

    // Emails detail
    if (topEmails.length) {
      pieces.push(`Important emails included ${topEmails.join('; ')}.`);
    } else if (emailEvents.length) {
      pieces.push(`You received ${emailEvents.length} email${emailEvents.length>1?'s':''}, mostly filters/notifications.`);
    }

    // Docs detail
    if (topDocs.length) {
      pieces.push(`Worked on documents like ${topDocs.slice(0,4).join(', ')}.`);
    }

    // Sites detail
    if (topPageTitles.length) {
      pieces.push(`Visited sites including ${top_domains.slice(0,4).join(', ')}; notable pages: ${topPageTitles.join('; ')}.`);
    }

    // Calendar detail
    if (topCal.length) {
      pieces.push(`Calendar events: ${topCal.slice(0,4).join('; ')}.`);
    }

    // Notable events compact
    if (notable.length) {
      pieces.push(`Notable items: ${notable.map(n=>n.summary).slice(0,4).join('; ')}.`);
    }

    // Fallback short summary
    let narrative = pieces.length ? pieces.join(' ') : 'No notable activity.';
    // Ensure narrative is at least a paragraph (single-line long enough)
    if (narrative.split('.').length < 3) {
      // Append detail from top domains or contacts if available
      const extras = [];
      if (top_domains && top_domains.length) extras.push(`Top domains: ${top_domains.slice(0,4).join(', ')}.`);
      if (top_contacts && top_contacts.length) extras.push(`Top contacts: ${top_contacts.slice(0,4).join(', ')}.`);
      if (extras.length) narrative += ' ' + extras.join(' ');
    }

    summaries.push({
      id: `daily_${date}_${Math.random().toString(36).slice(2,6)}`,
      user_id: userId,
      date: date,
      start_ts: formatISO(startTs),
      end_ts: formatISO(endTs),
      counts,
      top_domains,
      top_contacts,
      notable_events: notable,
      narrative,
      evidence_ids: evidenceIds
    });
  }

  // Return summaries sorted by date desc
  summaries.sort((a,b) => b.date.localeCompare(a.date));
  return summaries;
}

module.exports = { buildDailySummaries };
