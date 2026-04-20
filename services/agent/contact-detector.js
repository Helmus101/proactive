/**
 * Contact Detection Pipeline
 * 
 * Detects and builds contact profiles from:
 * - Semantic nodes (person entities)
 * - Emails (senders, recipients)
 * - Calendar (attendees, organizers)
 * - Episodes (person mentions, interaction context)
 */

const db = require('../db');
const { calculateRelationshipStrength } = require('./relationship-scorer');

function parseMetadata(metadata) {
  if (!metadata) return {};
  if (typeof metadata === 'string') {
    try {
      return JSON.parse(metadata);
    } catch (_) {
      return {};
    }
  }
  return metadata;
}

function parseSourceRefs(sourceRefs) {
  if (!sourceRefs) return [];
  if (typeof sourceRefs === 'string') {
    try {
      return JSON.parse(sourceRefs);
    } catch (_) {
      return [];
    }
  }
  if (Array.isArray(sourceRefs)) return sourceRefs;
  return [];
}

function normalizeEmail(email) {
  return String(email || '').toLowerCase().trim();
}

function normalizePhone(phone) {
  return String(phone || '').replace(/\D/g, '');
}

function extractNameFromEmail(email) {
  const match = String(email || '').match(/^([^.+@]+)(?:[.+]([^@]*))?@/);
  if (match) {
    const base = match[1].replace(/[-_]/g, ' ').split(/\s+/)
      .map(w => w.charAt(0).toUpperCase() + w.slice(1))
      .join(' ');
    return base || null;
  }
  return null;
}

/**
 * Extract contacts from semantic nodes (person entities)
 */
async function extractFromSemanticNodes(nowMs = Date.now()) {
  const contacts = {};

  const rows = await db.allQuery(
    `SELECT id, title, summary, metadata, created_at, updated_at 
     FROM memory_nodes 
     WHERE layer = 'semantic' AND subtype = 'person' 
     LIMIT 500`,
    []
  ).catch(() => []);

  for (const row of rows || []) {
    const meta = parseMetadata(row.metadata);
    const name = String(row.title || '').trim();
    if (!name) continue;

    const key = name.toLowerCase();
    contacts[key] = contacts[key] || {
      name,
      source_node_ids: [],
      emails: [],
      phones: [],
      interests: [],
      birthday: null,
      social_handles: {},
      notes: row.summary || '',
      interaction_history: [],
      sources: {}
    };

    contacts[key].source_node_ids.push(row.id);
    
    // Extract metadata fields
    if (meta.email) {
      const email = normalizeEmail(meta.email);
      if (email && !contacts[key].emails.includes(email)) {
        contacts[key].emails.push(email);
      }
    }
    if (meta.phone) {
      const phone = normalizePhone(meta.phone);
      if (phone && !contacts[key].phones.includes(phone)) {
        contacts[key].phones.push(phone);
      }
    }
    if (meta.topics && Array.isArray(meta.topics)) {
      for (const topic of meta.topics) {
        if (!contacts[key].interests.includes(topic)) {
          contacts[key].interests.push(topic);
        }
      }
    }
    if (meta.birthday) {
      contacts[key].birthday = contacts[key].birthday || meta.birthday;
    }
    if (meta.social_handles && typeof meta.social_handles === 'object') {
      contacts[key].social_handles = { ...contacts[key].social_handles, ...meta.social_handles };
    }

    // Track interaction dates
    const lastActivity = parseMetadata(meta.latest_interaction_at || row.updated_at);
    if (lastActivity) {
      contacts[key].last_contact_at = contacts[key].last_contact_at || lastActivity;
    }
    if (row.created_at) {
      contacts[key].first_contact_at = contacts[key].first_contact_at || row.created_at;
    }

    if (!contacts[key].sources.semantic) contacts[key].sources.semantic = [];
    contacts[key].sources.semantic.push(row.id);
  }

  return contacts;
}

/**
 * Extract contacts from emails
 */
async function extractFromEmails(nowMs = Date.now()) {
  const contacts = {};

  const rows = await db.allQuery(
    `SELECT id, metadata, timestamp, text 
     FROM events 
     WHERE type = 'email' OR source_type = 'gmail' 
     ORDER BY datetime(timestamp) DESC 
     LIMIT 1000`,
    []
  ).catch(() => []);

  for (const row of rows || []) {
    const meta = parseMetadata(row.metadata);
    const senders = [meta.from, meta.sender].filter(Boolean);
    const recipients = [meta.to, meta.cc, meta.bcc]
      .filter(Boolean)
      .flatMap(r => String(r || '').split(/[,;]/))
      .map(e => e.trim())
      .filter(Boolean);

    // Process all email addresses
    for (const emailStr of [...senders, ...recipients]) {
      const email = normalizeEmail(emailStr);
      if (!email || email.includes('noreply') || email.includes('no-reply')) continue;

      const inferred_name = extractNameFromEmail(email) || email;
      const key = inferred_name.toLowerCase();

      contacts[key] = contacts[key] || {
        name: inferred_name,
        source_node_ids: [],
        emails: [],
        phones: [],
        interests: [],
        birthday: null,
        social_handles: {},
        notes: '',
        interaction_history: [],
        sources: {}
      };

      if (!contacts[key].emails.includes(email)) {
        contacts[key].emails.push(email);
      }

      // Track interaction
      contacts[key].last_contact_at = contacts[key].last_contact_at || row.timestamp;
      contacts[key].first_contact_at = contacts[key].first_contact_at || row.timestamp;

      if (!contacts[key].sources.email) contacts[key].sources.email = [];
      contacts[key].sources.email.push(row.id);

      // Extract topics from email content
      const emailText = String(meta.subject || '') + ' ' + String(row.text || '');
      const topics = extractTopicsFromText(emailText);
      for (const topic of topics) {
        if (!contacts[key].interests.includes(topic)) {
          contacts[key].interests.push(topic);
        }
      }
    }
  }

  return contacts;
}

/**
 * Extract contacts from calendar
 */
async function extractFromCalendar(nowMs = Date.now()) {
  const contacts = {};

  const rows = await db.allQuery(
    `SELECT id, metadata, timestamp, title, text
     FROM events 
     WHERE type = 'calendar' OR source_type = 'google_calendar' 
     ORDER BY datetime(timestamp) DESC 
     LIMIT 500`,
    []
  ).catch(() => []);

  for (const row of rows || []) {
    const meta = parseMetadata(row.metadata);
    const attendees = meta.attendees || [];
    const organizer = meta.organizer;

    // Process attendees and organizer
    for (const attendeeStr of [...attendees, organizer].filter(Boolean)) {
      let email, name;
      
      if (typeof attendeeStr === 'object') {
        email = normalizeEmail(attendeeStr.email);
        name = attendeeStr.name || extractNameFromEmail(email);
      } else {
        email = normalizeEmail(attendeeStr);
        name = extractNameFromEmail(email);
      }

      if (!email || !name) continue;

      const key = name.toLowerCase();
      contacts[key] = contacts[key] || {
        name,
        source_node_ids: [],
        emails: [],
        phones: [],
        interests: [],
        birthday: null,
        social_handles: {},
        notes: '',
        interaction_history: [],
        sources: {}
      };

      if (!contacts[key].emails.includes(email)) {
        contacts[key].emails.push(email);
      }

      contacts[key].last_contact_at = contacts[key].last_contact_at || row.timestamp;
      contacts[key].first_contact_at = contacts[key].first_contact_at || row.timestamp;

      if (!contacts[key].sources.calendar) contacts[key].sources.calendar = [];
      contacts[key].sources.calendar.push(row.id);

      // Extract shared interests from meeting title/description
      const meetingText = String(row.title || '') + ' ' + String(meta.description || '');
      const topics = extractTopicsFromText(meetingText);
      for (const topic of topics) {
        if (!contacts[key].interests.includes(topic)) {
          contacts[key].interests.push(topic);
        }
      }
    }
  }

  return contacts;
}

/**
 * Extract contacts from episodes (communication interactions)
 */
async function extractFromEpisodes(nowMs = Date.now()) {
  const contacts = {};

  const rows = await db.allQuery(
    `SELECT id, title, summary, metadata, created_at, updated_at 
     FROM memory_nodes 
     WHERE layer = 'episode' AND status != 'archived' 
     ORDER BY datetime(updated_at) DESC 
     LIMIT 200`,
    []
  ).catch(() => []);

  for (const row of rows || []) {
    const meta = parseMetadata(row.metadata);
    const sourceType = String(meta.source_type_group || '').toLowerCase();

    // Only process communication episodes
    if (!['communication', 'collaboration'].includes(sourceType)) continue;

    // Extract person name from episode title if available
    const match = row.title.match(/^(?:to|from|with|about)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)/i);
    if (!match) continue;

    const name = match[1].trim();
    const key = name.toLowerCase();

    contacts[key] = contacts[key] || {
      name,
      source_node_ids: [],
      emails: [],
      phones: [],
      interests: [],
      birthday: null,
      social_handles: {},
      notes: row.summary || '',
      interaction_history: [],
      sources: {}
    };

    contacts[key].source_node_ids.push(row.id);
    contacts[key].last_contact_at = contacts[key].last_contact_at || row.updated_at;
    contacts[key].first_contact_at = contacts[key].first_contact_at || row.created_at;

    if (!contacts[key].sources.episode) contacts[key].sources.episode = [];
    contacts[key].sources.episode.push(row.id);

    // Extract topics from episode content
    const episodeText = String(row.title || '') + ' ' + String(row.summary || '');
    const topics = extractTopicsFromText(episodeText);
    for (const topic of topics) {
      if (!contacts[key].interests.includes(topic)) {
        contacts[key].interests.push(topic);
      }
    }
  }

  return contacts;
}

function extractTopicsFromText(text = '') {
  // Simple keyword extraction: look for capitalized words, common topics
  const words = String(text || '')
    .split(/\s+/)
    .filter(w => w.length > 3)
    .filter(w => /^[A-Z]/.test(w)); // Capitalized words (entities)

  return Array.from(new Set(words)).slice(0, 10);
}

/**
 * Dedup and merge contacts by name + (email or phone)
 */
function deduplicateContacts(contactsMap = {}) {
  const merged = {};

  for (const [key, contact] of Object.entries(contactsMap)) {
    // Try to find existing match by email or phone
    let foundKey = null;

    for (const [existingKey, existing] of Object.entries(merged)) {
      // Match by name (exact)
      if (existing.name.toLowerCase() === contact.name.toLowerCase()) {
        foundKey = existingKey;
        break;
      }

      // Match by email
      for (const email of contact.emails) {
        if (existing.emails.includes(email)) {
          foundKey = existingKey;
          break;
        }
      }
      if (foundKey) break;

      // Match by phone
      for (const phone of contact.phones) {
        if (existing.phones.includes(phone)) {
          foundKey = existingKey;
          break;
        }
      }
      if (foundKey) break;
    }

    if (foundKey) {
      // Merge into existing
      const existing = merged[foundKey];
      existing.emails = Array.from(new Set([...(existing.emails || []), ...(contact.emails || [])])).slice(0, 5);
      existing.phones = Array.from(new Set([...(existing.phones || []), ...(contact.phones || [])])).slice(0, 5);
      existing.interests = Array.from(new Set([...(existing.interests || []), ...(contact.interests || [])])).slice(0, 15);
      existing.source_node_ids = Array.from(new Set([...(existing.source_node_ids || []), ...(contact.source_node_ids || [])]));
      existing.notes = contact.notes || existing.notes;
      existing.birthday = contact.birthday || existing.birthday;
      existing.social_handles = { ...(existing.social_handles || {}), ...(contact.social_handles || {}) };
      
      // Update sources
      for (const [source, ids] of Object.entries(contact.sources || {})) {
        if (!existing.sources) existing.sources = {};
        if (!existing.sources[source]) existing.sources[source] = [];
        existing.sources[source] = Array.from(new Set([...existing.sources[source], ...(ids || [])]));
      }

      // Update interaction dates to be most recent/oldest
      if (contact.last_contact_at) {
        const lastTime = new Date(contact.last_contact_at).getTime();
        const existingTime = existing.last_contact_at ? new Date(existing.last_contact_at).getTime() : 0;
        if (lastTime > existingTime) {
          existing.last_contact_at = contact.last_contact_at;
        }
      }
      if (contact.first_contact_at) {
        const firstTime = new Date(contact.first_contact_at).getTime();
        const existingTime = existing.first_contact_at ? new Date(existing.first_contact_at).getTime() : Date.now();
        if (firstTime < existingTime) {
          existing.first_contact_at = contact.first_contact_at;
        }
      }
    } else {
      // Add as new contact
      merged[key] = contact;
    }
  }

  return merged;
}

/**
 * Main: detect all contacts and calculate relationship scores
 */
async function detectAndScoreContacts(nowMs = Date.now()) {
  console.log('[ContactDetector] Starting contact detection...');

  // Extract from all sources
  const [semantic, emails, calendar, episodes] = await Promise.all([
    extractFromSemanticNodes(nowMs),
    extractFromEmails(nowMs),
    extractFromCalendar(nowMs),
    extractFromEpisodes(nowMs)
  ]);

  console.log(`[ContactDetector] Found ${Object.keys(semantic).length} from semantic, ${Object.keys(emails).length} from emails, ${Object.keys(calendar).length} from calendar, ${Object.keys(episodes).length} from episodes`);

  // Merge all sources
  const allContacts = { ...semantic, ...emails, ...calendar, ...episodes };

  // Dedup
  const dedupedContacts = deduplicateContacts(allContacts);
  console.log(`[ContactDetector] After dedup: ${Object.keys(dedupedContacts).length} contacts`);

  // Calculate interaction counts and score relationships
  const scoredContacts = [];
  for (const [key, contact] of Object.entries(dedupedContacts)) {
    // Count interactions: sum from all sources
    const interactionCount = [
      ...(contact.sources.semantic || []),
      ...(contact.sources.email || []),
      ...(contact.sources.calendar || []),
      ...(contact.sources.episode || [])
    ].length;

    const relationshipScores = calculateRelationshipStrength({
      interaction_count: interactionCount,
      last_contact_at: contact.last_contact_at,
      first_contact_at: contact.first_contact_at,
      conversation_topics: contact.interests,
      average_message_length: 120, // Default; could refine with actual data
      is_deep_conversation: contact.sources.episode && contact.sources.episode.length > 2
    }, nowMs);

    scoredContacts.push({
      id: `contact_${key.replace(/\s+/g, '_')}`,
      name: contact.name,
      emails: contact.emails,
      phones: contact.phones,
      interests: contact.interests,
      birthday: contact.birthday,
      social_handles: contact.social_handles,
      notes: contact.notes,
      source_node_ids: contact.source_node_ids,
      sources: contact.sources,
      interaction_count: interactionCount,
      last_contact_at: contact.last_contact_at,
      first_contact_at: contact.first_contact_at,
      ...relationshipScores // includes strength, is_weak_tie, is_overdue_followup, recommendation
    });
  }

  // Sort by relationship strength descending
  const ranked = scoredContacts.sort((a, b) => (b.strength || 0) - (a.strength || 0));

  console.log(`[ContactDetector] Scored ${ranked.length} contacts. Top weak ties or overdue: ${ranked.filter(c => c.is_weak_tie || c.is_overdue_followup).length}`);

  return ranked;
}

module.exports = {
  detectAndScoreContacts,
  extractFromSemanticNodes,
  extractFromEmails,
  extractFromCalendar,
  extractFromEpisodes,
  deduplicateContacts,
  extractTopicsFromText
};