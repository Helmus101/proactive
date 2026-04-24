const crypto = require('crypto');
const db = require('./db');
const { fetchAppleContacts } = require('./apple-contacts');
const { upsertMemoryNode, upsertMemoryEdge } = require('./agent/graph-store');

const APPLE_CONTACTS_SYNC_TTL_MS = 12 * 60 * 60 * 1000;
let appleContactsSyncAt = 0;
let appleContactsSyncInFlight = null;
let appleContactsLastResult = { imported: 0, skipped: true, source: 'apple_contacts' };

function nowIso() {
  return new Date().toISOString();
}

function asObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function asList(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed;
    } catch (_) {}
    return value.split(/[,;]/).map((item) => item.trim()).filter(Boolean);
  }
  return [value].filter(Boolean);
}

function uniqList(values = []) {
  return Array.from(new Set(asList(values).map((item) => normalizeText(item)).filter(Boolean)));
}

function normalizeText(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeName(value = '') {
  return normalizeText(value)
    .replace(/[^\p{L}\p{N}@._+\-\s]/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizedIdentifier(value = '') {
  return normalizeName(value).toLowerCase();
}

function stableId(prefix, value) {
  return `${prefix}_${crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 18)}`;
}

function escapeRegExp(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function trim(value = '', max = 220) {
  const text = normalizeText(value);
  return text.length > max ? `${text.slice(0, Math.max(0, max - 1)).trim()}…` : text;
}

function uniqNormalized(values = [], limit = 24) {
  return Array.from(new Set(asList(values).map((item) => normalizeText(item)).filter(Boolean))).slice(0, limit);
}

function isEmail(value = '') {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
}

function nameFromEmail(email = '') {
  const local = String(email || '').split('@')[0] || '';
  const clean = local.replace(/[._+-]+/g, ' ').replace(/\d+/g, '').trim();
  if (!clean) return email;
  return clean.split(/\s+/).map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function splitNameParts(value = '') {
  const normalized = normalizeName(value || '');
  if (!normalized || isEmail(normalized)) return { first_name: '', last_name: '' };
  const parts = normalized.split(/\s+/).filter(Boolean);
  if (!parts.length) return { first_name: '', last_name: '' };
  if (parts.length === 1) return { first_name: parts[0], last_name: '' };
  return {
    first_name: parts[0],
    last_name: parts.slice(1).join(' ')
  };
}

function isLikelyPersonName(value = '') {
  const text = normalizeName(value);
  if (!text || text.length < 3 || text.length > 80) return false;
  if (/^(You|Me|I|Myself)$/i.test(text)) return false;
  if (isEmail(text)) return true;
  if (!/^[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}$/u.test(text)) return false;
  if (/\b(App|Chrome|Safari|Calendar|Gmail|Google|LinkedIn|Messages|Slack|Team|School|University|College|Inc|LLC|Ltd|Labs|Studio|Support|Settings|Terminal|Code)\b/.test(text)) return false;
  return true;
}
function chooseBetterDisplayName(existingName = '', incomingName = '') {
  const existing = normalizeName(existingName || '');
  const incoming = normalizeName(incomingName || '');
  if (!existing) return incoming;
  if (!incoming) return existing;
  if (isEmail(incoming) && !isEmail(existing)) return existing;
  if (isEmail(existing) && !isEmail(incoming)) return incoming;
  const existingParts = existing.split(/\s+/);
  const incomingParts = incoming.split(/\s+/);
  if (existingParts.length > incomingParts.length && existing.toLowerCase().startsWith(incoming.toLowerCase())) return existing;
  if (incomingParts.length > existingParts.length && incoming.toLowerCase().startsWith(existing.toLowerCase())) return incoming;
  return incoming.length >= existing.length ? incoming : existing;
}

function parseContactString(value = '') {
  if (!value || typeof value === 'object') return value;
  const text = String(value || '').trim();
  const emailMatch = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (!emailMatch) return { name: text, email: isEmail(text) ? text : '' };
  const email = emailMatch[0].toLowerCase();
  const name = text
    .replace(emailMatch[0], '')
    .replace(/[<>()"']/g, '')
    .trim();
  return { name: name || nameFromEmail(email), email };
}

async function ensureRelationshipTables() {
  await db.runQuery(`CREATE TABLE IF NOT EXISTS relationship_contacts (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    company TEXT,
    role TEXT,
    strength_score REAL DEFAULT 0,
    warmth_score REAL DEFAULT 0,
    depth_score REAL DEFAULT 0,
    network_centrality REAL DEFAULT 0,
    last_interaction_at TEXT,
    interaction_count_30d INTEGER DEFAULT 0,
    relationship_tier TEXT,
    status TEXT DEFAULT 'warm',
    relationship_summary TEXT,
    metadata TEXT,
    created_at TEXT,
    updated_at TEXT
  )`).catch(() => {});

  // Migration for relationship_contacts
  const relContactCols = await db.allQuery(`PRAGMA table_info(relationship_contacts)`).catch(() => []);
  const relContactExisting = new Set((relContactCols || []).map((c) => c?.name).filter(Boolean));
  const relContactRequired = [
    ['warmth_score', 'REAL DEFAULT 0'],
    ['depth_score', 'REAL DEFAULT 0'],
    ['network_centrality', 'REAL DEFAULT 0'],
    ['relationship_summary', 'TEXT']
  ];
  for (const [name, sqlType] of relContactRequired) {
    if (!relContactExisting.has(name)) {
      await db.runQuery(`ALTER TABLE relationship_contacts ADD COLUMN ${name} ${sqlType}`).catch(() => {});
    }
  }
  await db.runQuery(`CREATE TABLE IF NOT EXISTS relationship_contact_identifiers (
    id TEXT PRIMARY KEY,
    contact_id TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier_value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    source_label TEXT,
    confidence REAL DEFAULT 1,
    created_at TEXT,
    updated_at TEXT,
    FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
  )`).catch(() => {});
  await db.runQuery(`CREATE TABLE IF NOT EXISTS relationship_mentions (
    id TEXT PRIMARY KEY,
    contact_id TEXT NOT NULL,
    event_id TEXT,
    memory_node_id TEXT,
    retrieval_doc_id TEXT,
    timestamp TEXT,
    source_app TEXT,
    context_snippet TEXT,
    confidence REAL DEFAULT 0.7,
    mention_type TEXT,
    metadata TEXT,
    created_at TEXT,
    FOREIGN KEY(contact_id) REFERENCES relationship_contacts(id) ON DELETE CASCADE
  )`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_status ON relationship_contacts(status)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_last_interaction ON relationship_contacts(last_interaction_at)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_contacts_strength ON relationship_contacts(strength_score)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_identifiers_contact ON relationship_contact_identifiers(contact_id)`).catch(() => {});
  await db.runQuery(`CREATE UNIQUE INDEX IF NOT EXISTS idx_relationship_identifiers_unique ON relationship_contact_identifiers(identifier_type, normalized_value)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_contact ON relationship_mentions(contact_id)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_event ON relationship_mentions(event_id)`).catch(() => {});
  await db.runQuery(`CREATE INDEX IF NOT EXISTS idx_relationship_mentions_timestamp ON relationship_mentions(timestamp)`).catch(() => {});
}

function normalizeIdentifierInput(identifier, fallbackSource = 'memory') {
  const raw = typeof identifier === 'object' ? identifier : parseContactString(identifier);
  const value = normalizeText(raw?.value || raw?.email || raw?.handle || raw?.name || identifier);
  if (!value) return null;
  const type = raw?.type || (isEmail(value) ? 'email' : (String(value).startsWith('@') ? 'handle' : 'alias'));
  const normalized = normalizedIdentifier(value);
  if (!normalized) return null;
  return {
    identifier_type: type,
    identifier_value: value,
    normalized_value: normalized,
    source_label: raw?.source || raw?.source_label || fallbackSource,
    confidence: Number(raw?.confidence || 1)
  };
}

async function findContactByIdentifiers(identifiers = [], displayName = '') {
  await ensureRelationshipTables();
  const normalized = identifiers.map((item) => item.normalized_value).filter(Boolean);
  if (normalized.length) {
    const placeholders = normalized.map(() => '?').join(',');
    const row = await db.getQuery(
      `SELECT rc.*
       FROM relationship_contact_identifiers rci
       JOIN relationship_contacts rc ON rc.id = rci.contact_id
       WHERE rci.normalized_value IN (${placeholders})
       ORDER BY CASE rci.identifier_type WHEN 'email' THEN 0 ELSE 1 END
       LIMIT 1`,
      normalized
    ).catch(() => null);
    if (row) return row;
  }

  const normName = normalizedIdentifier(displayName);
  if (!normName) return null;
  return db.getQuery(
    `SELECT rc.*
     FROM relationship_contact_identifiers rci
     JOIN relationship_contacts rc ON rc.id = rci.contact_id
     WHERE rci.identifier_type IN ('name', 'alias') AND rci.normalized_value = ?
     LIMIT 1`,
    [normName]
  ).catch(() => null);
}

async function syncSemanticPersonNode(contact = {}, identifiers = [], evidence = {}) {
  const now = nowIso();
  const contactId = contact.id;
  const contactMeta = asObj(contact.metadata);
  const overrides = asObj(contactMeta.editable_overrides);
  const title = overrides.display_name || contact.display_name || contact.name || 'Contact';
  const nodeId = `person_${contactId.replace(/^rel_/, '')}`;
  const splitName = splitNameParts(title);
  const sourceRefs = uniqNormalized([
    ...asList(contactMeta.source_refs),
    ...asList(contactMeta.relationship_contact_ids),
    ...asList(contactMeta.interaction_refs),
    evidence.event_id,
    evidence.node_id
  ], 32);
  const topics = uniqNormalized([
    ...asList(contact.topics),
    ...asList(contactMeta.topics),
    ...asList(contactMeta.interests)
  ], 12);
  const metadata = {
    relationship_contact_id: contactId,
    apple_contact_id: contactId,
    name: title,
    display_name: title,
    first_name: overrides.first_name || contactMeta.first_name || splitName.first_name || '',
    last_name: overrides.last_name || contactMeta.last_name || splitName.last_name || '',
    email: identifiers.find((item) => item.identifier_type === 'email')?.identifier_value || null,
    company: overrides.company || contact.company || contactMeta.company || null,
    role: overrides.role || contact.role || contactMeta.role || null,
    location: overrides.location || contactMeta.location || contact.location || null,
    linkedin_url: overrides.linkedin_url || contactMeta.linkedin_url || contactMeta.urls?.find(u => u.includes('linkedin.com')) || null,
    latest_interaction_at: contact.last_interaction_at || evidence.timestamp || null,
    strength_score: Number(contact.strength_score || 0),
    relationship_tier: contact.relationship_tier || null,
    source_refs: sourceRefs,
    interaction_refs: uniqNormalized([...(contactMeta.interaction_refs || []), evidence.event_id].filter(Boolean), 32),
    topics,
    interests: uniqNormalized([...(contactMeta.interests || []), ...(overrides.interests || []), ...topics], 12),
    identifiers: identifiers.map((item) => ({ type: item.identifier_type, value: item.identifier_value })),
    notes: trim(overrides.notes || contactMeta.notes || '', 1200),
    editable_overrides: overrides
  };

  await upsertMemoryNode({
    id: nodeId,
    layer: 'semantic',
    subtype: 'person',
    title,
    summary: `Relationship contact: ${title}`,
    canonicalText: `${title}\n${contact.company || ''}\n${contact.role || ''}\n${topics.join(', ')}`.trim(),
    confidence: Math.max(0.65, Number(contact.strength_score || 0.65)),
    status: 'active',
    sourceRefs: metadata.source_refs,
    metadata,
    graphVersion: 'relationship_graph_v1',
    createdAt: now,
    updatedAt: now,
    anchorDate: String(contact.last_interaction_at || evidence.timestamp || now).slice(0, 10),
    anchorAt: contact.last_interaction_at || evidence.timestamp || now,
    connectionCount: metadata.source_refs.length
  }).catch(() => {});

  for (const topic of topics) {
    const topicId = `topic_${stableId('rel_topic', topic).replace(/^rel_topic_/, '')}`;
    await upsertMemoryNode({
      id: topicId,
      layer: 'semantic',
      subtype: 'topic',
      title: topic,
      summary: `Topic connected to ${title}`,
      canonicalText: `${topic}\nRelated contact: ${title}`,
      confidence: 0.58,
      status: 'active',
      sourceRefs: metadata.source_refs,
    metadata: {
      related_people: [title],
      display_name: title,
      relationship_contact_id: contactId,
      source_refs: metadata.source_refs
    },
      graphVersion: 'relationship_graph_v1',
      createdAt: now,
      updatedAt: now,
      anchorDate: String(contact.last_interaction_at || evidence.timestamp || now).slice(0, 10),
      anchorAt: contact.last_interaction_at || evidence.timestamp || now
    }).catch(() => {});

    await upsertMemoryEdge({
      fromNodeId: nodeId,
      toNodeId: topicId,
      edgeType: 'RELATED_TO',
      weight: 0.72,
      traceLabel: 'contact_interest',
      evidenceCount: Math.max(1, metadata.source_refs.length),
      metadata: { relationship_contact_id: contactId, topic }
    }).catch(() => {});
  }
  return nodeId;
}

async function getPersonNodeByContactId(contactId) {
  if (!contactId) return null;
  return db.getQuery(
    `SELECT *
     FROM memory_nodes
     WHERE subtype = 'person'
       AND json_extract(COALESCE(metadata, '{}'), '$.relationship_contact_id') = ?
     LIMIT 1`,
    [contactId]
  ).catch(() => null);
}

async function upsertRelationshipContact(contact = {}, identifiers = [], evidence = {}) {
  await ensureRelationshipTables();
  const parsedContact = typeof contact === 'string' ? parseContactString(contact) : (contact || {});
  let displayName = normalizeName(
    parsedContact.display_name ||
    parsedContact.name ||
    parsedContact.email ||
    identifiers.find((item) => isEmail(item?.value || item?.email || item))?.email ||
    ''
  );
  const primaryEmail = normalizeText(parsedContact.email || asList(parsedContact.emails)[0] || identifiers.find((item) => isEmail(item?.value || item?.email || item))?.value || '');
  if (displayName && !isLikelyPersonName(displayName) && primaryEmail && isEmail(primaryEmail)) {
    displayName = primaryEmail;
  }
  if (!displayName || (!isLikelyPersonName(displayName) && !isEmail(displayName))) return null;

  const identifierInputs = [
    { type: isEmail(displayName) ? 'email' : 'name', value: displayName, source_label: evidence.source || parsedContact.source || 'memory' },
    ...asList(parsedContact.email || parsedContact.emails).map((email) => ({ type: 'email', value: email, source_label: evidence.source || 'memory' })),
    ...asList(parsedContact.aliases || parsedContact.identifiers).map((value) => ({ type: 'alias', value, source_label: evidence.source || 'memory' })),
    ...asList(identifiers)
  ];
  const normalizedIdentifiers = Array.from(new Map(identifierInputs
    .map((item) => normalizeIdentifierInput(item, evidence.source || 'memory'))
    .filter(Boolean)
    .map((item) => [`${item.identifier_type}:${item.normalized_value}`, item])).values());

  const existing = await findContactByIdentifiers(normalizedIdentifiers, displayName);
  const id = existing?.id || stableId('rel', normalizedIdentifiers.find((item) => item.identifier_type === 'email')?.normalized_value || normalizedIdentifier(displayName));
  const now = nowIso();
  const chosenDisplayName = chooseBetterDisplayName(existing?.display_name, displayName);
  const existingMeta = asObj(existing?.metadata);
  const incomingMeta = asObj(parsedContact.metadata);
  const mergedTopics = uniqNormalized([
    ...(existingMeta.topics || []),
    ...(incomingMeta.topics || []),
    ...(existingMeta.interests || []),
    ...(incomingMeta.interests || [])
  ], 12);
  const metadata = {
    ...existingMeta,
    ...incomingMeta,
    emails: uniqList([...(existingMeta.emails || []), ...(incomingMeta.emails || []), ...asList(parsedContact.email || parsedContact.emails)]),
    phones: uniqList([...(existingMeta.phones || []), ...(incomingMeta.phones || []), ...asList(parsedContact.phone || parsedContact.phones)]),
    addresses: uniqList([...(existingMeta.addresses || []), ...(incomingMeta.addresses || [])]),
    urls: uniqList([...(existingMeta.urls || []), ...(incomingMeta.urls || [])]),
    notes: trim([existingMeta.notes, incomingMeta.notes].filter(Boolean).join('\n\n'), 1500),
    interaction_refs: uniqNormalized([...(existingMeta.interaction_refs || []), ...(incomingMeta.interaction_refs || []), evidence.event_id], 32),
    source_refs: uniqNormalized([...(existingMeta.source_refs || []), ...(incomingMeta.source_refs || []), evidence.event_id], 32),
    topics: mergedTopics,
    interests: uniqNormalized([...(existingMeta.interests || []), ...(incomingMeta.interests || []), ...mergedTopics], 12),
    sources: Array.from(new Set([
      ...asList(existingMeta.sources),
      ...asList(incomingMeta.sources),
      evidence.source || parsedContact.source || 'memory'
    ].filter(Boolean))),
    birthday: incomingMeta.birthday || existingMeta.birthday || null,
    apple_contacts: Boolean(incomingMeta.apple_contacts || existingMeta.apple_contacts),
    linkedin_observed: Boolean(parsedContact.linkedin_observed || existingMeta.linkedin_observed),
    score_inputs: existingMeta.score_inputs || null,
    first_name: incomingMeta.first_name || existingMeta.first_name || splitNameParts(chosenDisplayName).first_name || '',
    last_name: incomingMeta.last_name || existingMeta.last_name || splitNameParts(chosenDisplayName).last_name || '',
    display_name: chosenDisplayName,
    editable_overrides: asObj(existingMeta.editable_overrides)
  };
  const lastInteraction = parsedContact.last_interaction_at || evidence.timestamp || existing?.last_interaction_at || null;
  const company = parsedContact.company || existing?.company || null;
  const role = parsedContact.role || existing?.role || null;
  const tier = parsedContact.relationship_tier || existing?.relationship_tier || (isEmail(displayName) ? 'network' : 'observed_contact');

  await db.runQuery(
    `INSERT INTO relationship_contacts
     (id, display_name, company, role, strength_score, last_interaction_at, interaction_count_30d, relationship_tier, status, metadata, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, COALESCE((SELECT interaction_count_30d FROM relationship_contacts WHERE id = ?), 0), ?, COALESCE((SELECT status FROM relationship_contacts WHERE id = ?), 'warm'), ?,
             COALESCE((SELECT created_at FROM relationship_contacts WHERE id = ?), ?), ?)
     ON CONFLICT(id) DO UPDATE SET
       display_name = excluded.display_name,
       company = COALESCE(excluded.company, relationship_contacts.company),
       role = COALESCE(excluded.role, relationship_contacts.role),
       last_interaction_at = CASE
         WHEN relationship_contacts.last_interaction_at IS NULL OR datetime(excluded.last_interaction_at) > datetime(relationship_contacts.last_interaction_at)
         THEN excluded.last_interaction_at
         ELSE relationship_contacts.last_interaction_at
       END,
       relationship_tier = COALESCE(excluded.relationship_tier, relationship_contacts.relationship_tier),
       metadata = excluded.metadata,
       updated_at = excluded.updated_at`,
      [
      id,
      isEmail(chosenDisplayName) ? nameFromEmail(chosenDisplayName) : chosenDisplayName,
      company,
      role,
      Number(existing?.strength_score || parsedContact.strength_score || 0),
      lastInteraction,
      id,
      tier,
      id,
      JSON.stringify(metadata),
      id,
      now,
      now
    ]
  );

  for (const item of normalizedIdentifiers) {
    const identifierId = stableId('rci', `${item.identifier_type}|${item.normalized_value}`);
    await db.runQuery(
      `INSERT OR REPLACE INTO relationship_contact_identifiers
       (id, contact_id, identifier_type, identifier_value, normalized_value, source_label, confidence, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM relationship_contact_identifiers WHERE id = ?), ?), ?)`,
      [
        identifierId,
        id,
        item.identifier_type,
        item.identifier_value,
        item.normalized_value,
        item.source_label || evidence.source || 'memory',
        Number(item.confidence || 1),
        identifierId,
        now,
        now
      ]
    ).catch(() => {});
  }

  const row = await db.getQuery(`SELECT * FROM relationship_contacts WHERE id = ?`, [id]).catch(() => null);
  if (row) {
    await syncSemanticPersonNode(row, normalizedIdentifiers, evidence).catch(() => {});
  }
  return row;
}

async function linkRelationshipMention({ contactId, eventId = null, nodeId = null, retrievalDocId = null, timestamp = null, sourceApp = '', snippet = '', confidence = 0.7, mentionType = 'mention', metadata = {} } = {}) {
  if (!contactId) return null;
  await ensureRelationshipTables();
  const ts = timestamp || nowIso();
  const id = stableId('relm', `${contactId}|${eventId || ''}|${nodeId || ''}|${retrievalDocId || ''}|${mentionType}|${trim(snippet, 120)}|${ts.slice(0, 16)}`);
  await db.runQuery(
    `INSERT OR REPLACE INTO relationship_mentions
     (id, contact_id, event_id, memory_node_id, retrieval_doc_id, timestamp, source_app, context_snippet, confidence, mention_type, metadata, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM relationship_mentions WHERE id = ?), ?))`,
    [
      id,
      contactId,
      eventId,
      nodeId,
      retrievalDocId,
      ts,
      sourceApp || '',
      trim(snippet, 500),
      Number(confidence || 0.7),
      mentionType,
      JSON.stringify(metadata || {}),
      id,
      nowIso()
    ]
  ).catch(() => {});

  if (eventId) {
    const nodeIdForContact = `person_${String(contactId).replace(/^rel_/, '')}`;
    await db.runQuery(
      `INSERT OR IGNORE INTO memory_edges (from_node_id, to_node_id, edge_type, weight, trace_label, evidence_count, metadata, created_at)
       VALUES (?, ?, 'mentions_person', ?, ?, 1, ?, ?)`,
      [
        eventId,
        nodeIdForContact,
        Math.max(0.1, Math.min(1, Number(confidence || 0.7))),
        mentionType,
        JSON.stringify({ relationship_contact_id: contactId, source_app: sourceApp, snippet: trim(snippet, 200) }),
        nowIso()
      ]
    ).catch(() => {});
  }
  return { id, contact_id: contactId };
}

function participantsFromMetadata(metadata = {}, type = '') {
  const meta = metadata || {};
  const out = [];
  const add = (value, source) => {
    for (const item of asList(value)) {
      const parsed = parseContactString(item);
      const name = normalizeName(parsed?.name || parsed?.displayName || parsed?.email || item);
      const email = normalizeText(parsed?.email || '');
      if (name || email) out.push({ name: name || nameFromEmail(email), email, source });
    }
  };
  add(meta.from || meta.sender, 'gmail');
  add(meta.to, 'gmail');
  add(meta.cc, 'gmail');
  add(meta.bcc, 'gmail');
  add(meta.organizer, 'calendar');
  add(meta.attendees, 'calendar');
  add(meta.participants || meta.person_labels, String(type || '').toLowerCase().includes('calendar') ? 'calendar' : 'memory');
  add((meta.entity_labels || []).map((item) => String(item || '').replace(/^person:/i, '')).filter(Boolean), 'memory');
  return out;
}

function isCommunicationSurface(type = '', metadata = {}, sourceApp = '') {
  const hay = `${type || ''} ${sourceApp || ''} ${metadata.app || ''} ${metadata.activeApp || ''} ${metadata.content_type || ''} ${metadata.source_type || ''}`.toLowerCase();
  return /\b(message|chat|thread|gmail|mail|slack|teams|discord|whatsapp|telegram|signal|messages|imessage|sms)\b/.test(hay);
}

function extractObservedParticipants(text = '', metadata = {}, type = '', sourceApp = '') {
  const safeText = String(text || '');
  const lines = safeText.split(/\n+/).map((line) => normalizeText(line)).filter(Boolean);
  const results = [];
  const seen = new Set();

  const push = ({ name = '', email = '', company = '', role = '', source = 'observed_surface' } = {}) => {
    const normalizedName = normalizeName(name || '');
    const normalizedEmail = normalizeText(email || '').toLowerCase();
    const key = `${normalizedName.toLowerCase()}|${normalizedEmail}|${source}`;
    if ((!normalizedName || !isLikelyPersonName(normalizedName)) && !isEmail(normalizedEmail)) return;
    if (seen.has(key)) return;
    seen.add(key);
    results.push({
      name: normalizedName || nameFromEmail(normalizedEmail),
      email: normalizedEmail,
      company: trim(company || '', 90),
      role: trim(role || '', 90),
      source
    });
  };

  const windowTitle = normalizeText(metadata.window_title || metadata.activeWindowTitle || metadata.title || '');
  const url = normalizeText(metadata.url || '');
  const domain = normalizeText(metadata.domain || (() => {
    try {
      return url ? new URL(url).hostname.replace(/^www\./, '') : '';
    } catch (_) {
      return '';
    }
  })());

  if (domain.includes('linkedin.com') || /\blinkedin\b/i.test(windowTitle) || /\blinkedin\b/i.test(safeText)) {
    for (const candidate of extractLinkedInProfile(safeText, metadata)) {
      push({ ...candidate, source: 'linkedin_profile' });
    }
    const titleCandidate = windowTitle.match(/^([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s*(?:\||[-–])\s*LinkedIn/iu);
    if (titleCandidate) push({ name: titleCandidate[1], source: 'linkedin_profile' });
  }

  if (isCommunicationSurface(type, metadata, sourceApp)) {
    const titlePatterns = [
      /(?:messages?\s+with|chat with|conversation with)\s+([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/iu,
      /^([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s*(?:[-–|:])\s*(?:whatsapp|messages|imessage|signal|telegram|slack|discord|teams)\b/iu,
      /(?:whatsapp|messages|imessage|signal|telegram|slack|discord|teams)\s*(?:[-–|:])\s*([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})/iu
    ];
    for (const pattern of titlePatterns) {
      const match = windowTitle.match(pattern);
      if (match) push({ name: match[1], source: 'chat_header' });
    }

    for (const line of lines.slice(0, 80)) {
      const speakerMatch = line.match(/^([A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3})\s*:\s+/u);
      if (speakerMatch) push({ name: speakerMatch[1], source: 'chat_speaker' });

      const headerMatch = line.match(/^(?:from|to|cc|bcc)\s*:\s*(.+)$/i);
      if (headerMatch) {
        const parsed = parseContactString(headerMatch[1]);
        push({ name: parsed?.name || '', email: parsed?.email || '', source: 'message_header' });
      }

      const emailMatch = line.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
      if (emailMatch) {
        const parsed = parseContactString(line);
        push({ name: parsed?.name || '', email: parsed?.email || emailMatch[0], source: 'message_email' });
      }
    }
  }

  return results;
}

async function loadKnownIdentifiers() {
  await ensureRelationshipTables();
  const rows = await db.allQuery(
    `SELECT rci.contact_id, rci.identifier_type, rci.identifier_value, rci.normalized_value, rc.display_name
     FROM relationship_contact_identifiers rci
     JOIN relationship_contacts rc ON rc.id = rci.contact_id
     ORDER BY CASE rci.identifier_type WHEN 'email' THEN 0 WHEN 'name' THEN 1 ELSE 2 END
     LIMIT 3000`
  ).catch(() => []);
  return rows || [];
}

function extractLinkedInProfile(text = '', metadata = {}) {
  const hay = `${metadata.app || ''} ${metadata.window_title || metadata.activeWindowTitle || ''} ${metadata.url || ''} ${text || ''}`;
  if (!/\blinkedin\b/i.test(hay)) return [];
  const lines = String(text || '').split(/\n+/).map((line) => normalizeText(line)).filter(Boolean);
  const candidates = [];
  for (let i = 0; i < Math.min(lines.length, 30); i++) {
    const line = lines[i];
    if (!isLikelyPersonName(line)) continue;
    const next = lines.slice(i + 1, i + 4).join(' ');
    const companyMatch = next.match(/\b(?:at|@)\s+([A-Z][A-Za-z0-9&.\- ]{2,60})/) || next.match(/\b([A-Z][A-Za-z0-9&.\- ]{2,60})\s+·/);
    const role = next.split(/[|·]/)[0];
    candidates.push({
      name: line,
      role: trim(role, 90),
      company: companyMatch ? trim(companyMatch[1], 90) : '',
      linkedin_observed: true,
      source: 'linkedin_screenshot'
    });
  }
  return candidates.slice(0, 3);
}

function extractRelationshipTopics(text = '', metadata = {}) {
  const seeded = uniqNormalized([
    ...asList(metadata.topics),
    ...asList(metadata.topic_labels),
    ...asList(metadata.interests),
    ...asList(metadata.keywords)
  ], 12);
  if (seeded.length) return seeded;

  const source = normalizeText(text);
  const discovered = [];
  const patterns = [
    /\b(?:about|around|on)\s+([A-Za-z][A-Za-z0-9&/\- ]{3,40})/gi,
    /\binterest in\s+([A-Za-z][A-Za-z0-9&/\- ]{3,40})/gi,
    /\bthe\s+([a-z][a-z0-9&/\- ]{3,30})\s+is\b/gi,
    /\b([a-z][a-z0-9&/\- ]{3,30})\s+is next\b/gi
  ];
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(source)) !== null) {
      const candidate = trim(match[1], 48).replace(/[.,;:]$/, '');
      if (candidate && candidate.split(/\s+/).length <= 6) discovered.push(candidate);
    }
  }
  return uniqNormalized(discovered, 12);
}

async function linkMentionsForEvent({ eventId, type = '', source = '', text = '', metadata = {}, envelope = {}, cachedKnown = null } = {}) {
  await ensureRelationshipTables();
  const ts = envelope.occurred_at || metadata.occurred_at || metadata.timestamp || nowIso();
  const sourceApp = envelope.app || metadata.app || metadata.activeApp || source || '';
  const safeText = String(text || metadata.raw_text || metadata.redacted_text || '').slice(0, 12000);
  const relationshipTopics = extractRelationshipTopics(safeText, metadata);
  const linked = [];

  // 1. Process observed participants and ensure they exist as contacts
  const observed = extractObservedParticipants(safeText, metadata, type, sourceApp);
  for (const person of observed) {
    const contact = await upsertRelationshipContact(
      {
        display_name: person.name,
        email: person.email,
        company: person.company,
        role: person.role,
        metadata: {
          topics: relationshipTopics,
          source: person.source || 'observed'
        }
      },
      [
        person.email ? { type: 'email', value: person.email, source_label: person.source } : null,
        person.name ? { type: 'name', value: person.name, source_label: person.source } : null
      ].filter(Boolean),
      { event_id: eventId, source: person.source, timestamp: ts }
    );
    if (contact) {
      await linkRelationshipMention({
        contactId: contact.id,
        eventId,
        retrievalDocId: `event:${eventId}`,
        timestamp: ts,
        sourceApp,
        snippet: bestSnippetAround(safeText, person.name || person.email),
        confidence: 0.85,
        mentionType: 'observed_participant_mention',
        metadata: { source: person.source }
      });
      linked.push(contact);
    }
  }

  // 2. Scan for other known contacts mentioned in text
  const known = cachedKnown || await loadKnownIdentifiers();
  const seen = new Set(linked.map((item) => item.id));
  const identifierMap = new Map();
  const regexParts = [];

  for (const item of known) {
    if (seen.has(item.contact_id)) continue;
    const needle = String(item.normalized_value || '').toLowerCase();
    if (needle.length < 4) continue;

    if (!identifierMap.has(needle)) {
      identifierMap.set(needle, item);
    }
  }

  const sortedNeedles = Array.from(identifierMap.keys()).sort((a, b) => b.length - a.length);
  for (const needle of sortedNeedles) {
    const item = identifierMap.get(needle);
    const escaped = escapeRegExp(needle);
    if (item.identifier_type === 'email' || needle.includes('@')) {
      regexParts.push(escaped);
    } else {
      regexParts.push(`\\b${escaped}\\b`);
    }
  }

  if (regexParts.length > 0) {
    // Process in chunks to avoid regex engine limits
    const CHUNK_SIZE = 400;
    for (let i = 0; i < regexParts.length; i += CHUNK_SIZE) {
      const chunk = regexParts.slice(i, i + CHUNK_SIZE);
      const combinedRegex = new RegExp(chunk.join('|'), 'gi');
      let match;
      while ((match = combinedRegex.exec(safeText)) !== null) {
        const matchedValue = match[0].toLowerCase();
        const item = identifierMap.get(matchedValue);
        if (item && !seen.has(item.contact_id)) {
          const contactMention = await linkRelationshipMention({
            contactId: item.contact_id,
            eventId,
            retrievalDocId: `event:${eventId}`,
            timestamp: ts,
            sourceApp,
            snippet: bestSnippetAround(safeText, item.identifier_value),
            confidence: item.identifier_type === 'email' ? 0.95 : 0.82,
            mentionType: 'known_contact_mention',
            metadata: { matched_identifier: item.identifier_value, identifier_type: item.identifier_type }
          });
          if (contactMention) {
            const contactRow = await db.getQuery(`SELECT * FROM relationship_contacts WHERE id = ?`, [item.contact_id]);
            if (contactRow) linked.push(contactRow);
          }
          seen.add(item.contact_id);
        }
      }
    }
  }

  return linked;
}

function bestSnippetAround(text = '', needle = '', max = 360) {
  const source = normalizeText(text);
  const n = normalizeText(needle);
  if (!source) return '';
  if (!n) return trim(source, max);
  const idx = source.toLowerCase().indexOf(n.toLowerCase());
  if (idx < 0) return trim(source, max);
  const start = Math.max(0, idx - 130);
  return trim(source.slice(start, idx + n.length + 180), max);
}

function scoreStatus(score = 0, days = 0) {
  if (days > 21 || score < 0.35) return 'decaying';
  if (days > 10 || score < 0.55) return 'cooling';
  if (days > 3 && score >= 0.55) return 'needs_followup';
  return 'warm';
}

async function rescoreRelationshipContacts() {
  await ensureRelationshipTables();
  const contacts = await db.allQuery(`SELECT * FROM relationship_contacts LIMIT 5000`).catch(() => []);
  const now = Date.now();

  // Pre-calculate network centrality (degree)
  const centralityRows = await db.allQuery(`
    SELECT rm1.contact_id, COUNT(DISTINCT rm2.contact_id) as degree
    FROM relationship_mentions rm1
    JOIN relationship_mentions rm2 ON rm1.event_id = rm2.event_id
    WHERE rm1.contact_id != rm2.contact_id AND rm1.event_id IS NOT NULL
    GROUP BY rm1.contact_id
  `).catch(() => []);
  const centralityMap = new Map(centralityRows.map(r => [r.contact_id, r.degree]));
  const maxDegree = Math.max(1, ...centralityRows.map(r => r.degree), 1);

  // Pre-calculate stats for all contacts to avoid N+1 queries
  const since30 = new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();
  const statsRows = await db.allQuery(`
    SELECT 
      contact_id,
      COUNT(*) AS total_count,
      AVG(COALESCE(CAST(json_extract(metadata, '$.sentiment_score') AS REAL), 0)) AS sentiment_avg,
      COUNT(DISTINCT source_app) as app_diversity,
      MIN(timestamp) as first_interaction,
      SUM(CASE WHEN datetime(timestamp) >= datetime(?) THEN 1 ELSE 0 END) as count_30d
    FROM relationship_mentions
    GROUP BY contact_id
  `, [since30]).catch(() => []);
  
  const statsMap = new Map(statsRows.map(r => [r.contact_id, r]));

  for (const contact of contacts || []) {
    const lastMs = Date.parse(contact.last_interaction_at || '') || 0;
    const days = lastMs ? Math.max(0, (now - lastMs) / (24 * 60 * 60 * 1000)) : 365;
    
    const stats = statsMap.get(contact.id) || { total_count: 0, sentiment_avg: 0, app_diversity: 0, first_interaction: null, count_30d: 0 };

    const frequency = Math.min(1, Number(stats.count_30d || 0) / 8);
    const recency = Math.exp((-Math.log(2) * days) / 14);
    const tier = String(contact.relationship_tier || 'network').toLowerCase();
    const tierPriority = tier.includes('inner') ? 1 : (tier.includes('network') ? 0.78 : 0.62);
    const sentiment = Math.max(-1, Math.min(1, Number(stats.sentiment_avg || 0)));
    const sentimentScore = (sentiment + 1) / 2;

    // Warmth: Current quality/investment
    const warmth = Math.max(0, Math.min(1, (recency * 0.45) + (frequency * 0.35) + (sentimentScore * 0.2)));

    // Depth: Long-term significance
    const firstMs = Date.parse(stats.first_interaction || contact.created_at || '') || now;
    const ageDays = Math.max(1, (now - firstMs) / (24 * 60 * 60 * 1000));
    const volumeScore = Math.min(1, (stats.total_count || 0) / 50);
    const diversityScore = Math.min(1, (stats.app_diversity || 0) / 4);
    const ageScore = Math.min(1, ageDays / 365);
    const depth = Math.max(0, Math.min(1, (volumeScore * 0.4) + (diversityScore * 0.3) + (ageScore * 0.3)));

    // Network Centrality
    const degree = centralityMap.get(contact.id) || 0;
    const centrality = degree / maxDegree;

    // Overall Strength
    const score = Math.max(0, Math.min(1, (warmth * 0.5) + (depth * 0.3) + (centrality * 0.2)));
    
    const status = scoreStatus(score, days);
    
    // Generate simple summary if missing or if it looks auto-generated
    let summary = contact.relationship_summary;
    const isAutoSummary = !summary || summary.endsWith(".") && (summary.includes("relationship") || summary.includes("active") || summary.includes("cooled") || summary.includes("hub"));
    
    if (isAutoSummary) {
      const parts = [];
      if (depth > 0.7) parts.push("Long-standing relationship");
      else if (depth > 0.4) parts.push("Developing relationship");
      
      if (warmth > 0.7) parts.push("highly active recently");
      else if (warmth < 0.3) parts.push("has cooled off");
      
      if (centrality > 0.6) parts.push("acts as a key network hub");
      
      if (parts.length > 0) {
        summary = parts.join(", ") + ".";
      }
    }

    const metadata = {
      ...asObj(contact.metadata),
      score_inputs: {
        recency: Number(recency.toFixed(4)),
        frequency: Number(frequency.toFixed(4)),
        sentiment: Number(sentiment.toFixed(4)),
        tier_priority: Number(tierPriority.toFixed(4)),
        days_since_interaction: Number(days.toFixed(2)),
        warmth: Number(warmth.toFixed(4)),
        depth: Number(depth.toFixed(4)),
        centrality: Number(centrality.toFixed(4))
      }
    };

    await db.runQuery(
      `UPDATE relationship_contacts
       SET strength_score = ?, warmth_score = ?, depth_score = ?, network_centrality = ?, 
           interaction_count_30d = ?, status = ?, relationship_summary = ?, metadata = ?, updated_at = ?
       WHERE id = ?`,
      [
        Number(score.toFixed(4)), 
        Number(warmth.toFixed(4)), 
        Number(depth.toFixed(4)), 
        Number(centrality.toFixed(4)),
        Number(stats.count_30d || 0), 
        status, 
        summary,
        JSON.stringify(metadata), 
        nowIso(), 
        contact.id
      ]
    ).catch(() => {});
  }
}

async function backfillRelationshipContacts() {
  await ensureRelationshipTables();
  const rows = await db.allQuery(
    `SELECT id, type, source, app, title, text, timestamp, occurred_at, participants, metadata
     FROM events
     ORDER BY datetime(COALESCE(occurred_at, timestamp)) DESC
     LIMIT 2500`
  ).catch(() => []);

  const known = await loadKnownIdentifiers();
  const batchSize = 50;

  for (let i = 0; i < (rows || []).length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    for (const row of batch) {
      const metadata = { ...asObj(row.metadata), participants: asList(row.participants) };
      await linkMentionsForEvent({
        eventId: row.id,
        type: row.type,
        source: row.source,
        text: row.text,
        metadata,
        envelope: { app: row.app, title: row.title, occurred_at: row.occurred_at || row.timestamp },
        cachedKnown: known
      }).catch(() => {});
    }
    // Yield to main thread between batches
    await new Promise(resolve => setImmediate(resolve));
  }
}

async function runRelationshipGraphJob(options = {}) {
  await ensureRelationshipTables();
  if (options.backfill) await backfillRelationshipContacts();
  await rescoreRelationshipContacts();
  const count = await db.getQuery(`SELECT COUNT(*) AS count FROM relationship_contacts`).catch(() => ({ count: 0 }));
  return { contacts: Number(count?.count || 0), backfill: Boolean(options.backfill), updated_at: nowIso() };
}

async function syncAppleContactsIntoRelationshipGraph({ force = false, limit = 500 } = {}) {
  await ensureRelationshipTables();
  const now = Date.now();
  if (!force && appleContactsLastResult && (now - appleContactsSyncAt) < APPLE_CONTACTS_SYNC_TTL_MS) {
    return appleContactsLastResult;
  }
  if (appleContactsSyncInFlight) return appleContactsSyncInFlight;

  appleContactsSyncInFlight = (async () => {
    try {
      const rows = await fetchAppleContacts({ limit });
      let imported = 0;
      for (const row of rows || []) {
        const emails = asList(row.emails);
        const phones = asList(row.phones);
        const primaryEmail = emails[0] || '';
        const contact = await upsertRelationshipContact(
          {
            name: row.name || primaryEmail,
            email: primaryEmail,
            company: row.company || null,
            role: row.role || null,
            metadata: {
              emails,
              phones,
              addresses: asList(row.addresses),
              urls: asList(row.urls),
              birthday: row.birthday || null,
              notes: trim(row.notes || '', 500),
              apple_contacts: true
            },
            relationship_tier: 'apple_contact'
          },
          [
            ...emails.map((email) => ({ type: 'email', value: email, source_label: 'apple_contacts', confidence: 1 })),
            ...phones.map((phone) => ({ type: 'alias', value: phone, source_label: 'apple_contacts', confidence: 0.9 })),
            row.name ? { type: 'name', value: row.name, source_label: 'apple_contacts', confidence: 0.95 } : null
          ].filter(Boolean),
          { source: 'apple_contacts', timestamp: nowIso() }
        );
        if (contact?.id) imported += 1;
      }
      appleContactsSyncAt = Date.now();
      appleContactsLastResult = { imported, source: 'apple_contacts', skipped: false };
      return appleContactsLastResult;
    } catch (error) {
      appleContactsSyncAt = Date.now();
      appleContactsLastResult = { imported: 0, source: 'apple_contacts', skipped: false, error: String(error?.message || error) };
      return appleContactsLastResult;
    } finally {
      appleContactsSyncInFlight = null;
    }
  })();

  return appleContactsSyncInFlight;
}

async function getRelationshipContacts({ limit = 50, status = null } = {}) {
  await ensureRelationshipTables();
  const params = [];
  const where = [`json_extract(COALESCE(metadata, '{}'), '$.apple_contacts') = 1`];
  if (status) {
    where.push(`status = ?`);
    params.push(status);
  }
  params.push(Math.max(1, Math.min(200, Number(limit || 50))));
  const rows = await db.allQuery(
    `SELECT *
     FROM relationship_contacts
     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
     ORDER BY CASE status WHEN 'decaying' THEN 0 WHEN 'cooling' THEN 1 WHEN 'needs_followup' THEN 2 ELSE 3 END,
              strength_score ASC,
              datetime(last_interaction_at) DESC
     LIMIT ?`,
    params
  ).catch(() => []);
  return (rows || []).map((row) => {
    const metadata = asObj(row.metadata);
    const overrides = asObj(metadata.editable_overrides);
    return {
      ...row,
      display_name: overrides.display_name || metadata.display_name || row.display_name,
      company: overrides.company || row.company,
      role: overrides.role || row.role,
      metadata
    };
  });
}

async function getRelationshipContactDetail(contactId) {
  await ensureRelationshipTables();
  const contact = await db.getQuery(`SELECT * FROM relationship_contacts WHERE id = ?`, [contactId]).catch(() => null);
  if (!contact) return null;
  const identifiers = await db.allQuery(
    `SELECT identifier_type, identifier_value, source_label, confidence
     FROM relationship_contact_identifiers
     WHERE contact_id = ?
     ORDER BY CASE identifier_type WHEN 'email' THEN 0 WHEN 'name' THEN 1 ELSE 2 END`,
    [contactId]
  ).catch(() => []);
  const mentions = await db.allQuery(
    `SELECT *
     FROM relationship_mentions
     WHERE contact_id = ?
     ORDER BY datetime(timestamp) DESC
     LIMIT 20`,
    [contactId]
  ).catch(() => []);
  
  // Find related contacts (Network Intelligence)
  const relatedContacts = await db.allQuery(`
    SELECT DISTINCT rc.id, rc.display_name, COUNT(*) as co_occurrence_count
    FROM relationship_mentions rm1
    JOIN relationship_mentions rm2 ON rm1.event_id = rm2.event_id
    JOIN relationship_contacts rc ON rm2.contact_id = rc.id
    WHERE rm1.contact_id = ? AND rm2.contact_id != ? AND rm1.event_id IS NOT NULL
    GROUP BY rc.id
    ORDER BY co_occurrence_count DESC
    LIMIT 10
  `, [contactId, contactId]).catch(() => []);

  const metadata = asObj(contact.metadata);
  const overrides = asObj(metadata.editable_overrides);
  const personNode = await getPersonNodeByContactId(contactId);
  const personNodeMeta = asObj(personNode?.metadata);
  const splitName = splitNameParts(overrides.display_name || metadata.display_name || contact.display_name || '');

  return {
    ...contact,
    display_name: overrides.display_name || metadata.display_name || contact.display_name,
    company: overrides.company || contact.company,
    role: overrides.role || contact.role,
    metadata,
    identifiers, 
    mentions,
    related_contacts: relatedContacts,
    person_node: personNode ? { ...personNode, metadata: personNodeMeta } : null,
    imported_fields: {
      emails: asList(metadata.emails),
      phones: asList(metadata.phones),
      addresses: asList(metadata.addresses),
      urls: asList(metadata.urls),
      birthday: metadata.birthday || null
    },
    editable_fields: {
      display_name: overrides.display_name || metadata.display_name || contact.display_name || '',
      first_name: overrides.first_name || personNodeMeta.first_name || metadata.first_name || splitName.first_name || '',
      last_name: overrides.last_name || personNodeMeta.last_name || metadata.last_name || splitName.last_name || '',
      company: overrides.company || contact.company || '',
      role: overrides.role || contact.role || '',
      notes: overrides.notes || metadata.notes || '',
      interests: uniqNormalized([...(overrides.interests || []), ...(metadata.interests || [])], 24),
      topics: uniqNormalized([...(overrides.topics || []), ...(metadata.topics || [])], 24),
      identifiers: uniqNormalized(asList(overrides.identifiers), 24)
    },
    recent_interaction_refs: uniqNormalized(mentions.map((row) => row.event_id).filter(Boolean), 20),
    related_node_summaries: uniqNormalized([...(personNodeMeta.topics || []), ...(personNodeMeta.interests || [])], 20)
  };
}

async function updateRelationshipContactProfile(contactId, updates = {}) {
  await ensureRelationshipTables();
  const existing = await db.getQuery(`SELECT * FROM relationship_contacts WHERE id = ?`, [contactId]).catch(() => null);
  if (!existing) return null;
  const metadata = asObj(existing.metadata);
  const currentOverrides = asObj(metadata.editable_overrides);

  const editable = {
    display_name: normalizeName(updates.display_name || currentOverrides.display_name || metadata.display_name || existing.display_name || ''),
    first_name: normalizeName(updates.first_name || currentOverrides.first_name || metadata.first_name || ''),
    last_name: normalizeName(updates.last_name || currentOverrides.last_name || metadata.last_name || ''),
    company: normalizeText(updates.company || currentOverrides.company || existing.company || ''),
    role: normalizeText(updates.role || currentOverrides.role || existing.role || ''),
    notes: trim(updates.notes || currentOverrides.notes || metadata.notes || '', 1500),
    interests: uniqNormalized(updates.interests || currentOverrides.interests || metadata.interests || [], 24),
    topics: uniqNormalized(updates.topics || currentOverrides.topics || metadata.topics || [], 24),
    identifiers: uniqNormalized(updates.identifiers || currentOverrides.identifiers || [], 24)
  };
  if (!editable.display_name && (editable.first_name || editable.last_name)) {
    editable.display_name = normalizeName([editable.first_name, editable.last_name].filter(Boolean).join(' '));
  }

  const nextMetadata = {
    ...metadata,
    display_name: editable.display_name || metadata.display_name || existing.display_name,
    first_name: editable.first_name || metadata.first_name || '',
    last_name: editable.last_name || metadata.last_name || '',
    notes: editable.notes || metadata.notes || '',
    interests: uniqNormalized([...(metadata.interests || []), ...editable.interests], 24),
    topics: uniqNormalized([...(metadata.topics || []), ...editable.topics], 24),
    editable_overrides: editable
  };

  await db.runQuery(
    `UPDATE relationship_contacts
     SET display_name = ?, company = ?, role = ?, metadata = ?, updated_at = ?
     WHERE id = ?`,
    [
      editable.display_name || existing.display_name,
      editable.company || existing.company || null,
      editable.role || existing.role || null,
      JSON.stringify(nextMetadata),
      nowIso(),
      contactId
    ]
  ).catch(() => {});

  const refreshed = await db.getQuery(`SELECT * FROM relationship_contacts WHERE id = ?`, [contactId]).catch(() => null);
  if (refreshed) {
    const identifiers = await db.allQuery(
      `SELECT identifier_type, identifier_value, source_label, confidence
       FROM relationship_contact_identifiers
       WHERE contact_id = ?`,
      [contactId]
    ).catch(() => []);
    const normalizedIdentifiers = [
      ...identifiers.map((item) => normalizeIdentifierInput({
        type: item.identifier_type,
        value: item.identifier_value,
        source_label: item.source_label,
        confidence: item.confidence
      }, 'memory')).filter(Boolean),
      ...editable.identifiers.map((value) => normalizeIdentifierInput({ type: 'alias', value, source_label: 'weave_edit', confidence: 0.95 }, 'weave_edit')).filter(Boolean),
      ...(editable.display_name ? [normalizeIdentifierInput({ type: 'name', value: editable.display_name, source_label: 'weave_edit', confidence: 0.98 }, 'weave_edit')] : []).filter(Boolean)
    ];
    await syncSemanticPersonNode({ ...refreshed, metadata: nextMetadata }, normalizedIdentifiers, {
      source: 'weave_edit',
      timestamp: nowIso()
    }).catch(() => {});
  }

  return getRelationshipContactDetail(contactId);
}

async function buildRelationshipDraftContext(contactId, options = {}) {
  const detail = await getRelationshipContactDetail(contactId);
  if (!detail) return null;
  const mentionRows = detail.mentions || [];
  const eventIds = mentionRows.map((row) => row.event_id).filter(Boolean).slice(0, 12);
  let events = [];
  if (eventIds.length) {
    const placeholders = eventIds.map(() => '?').join(',');
    events = await db.allQuery(
      `SELECT id, type, app, title, text, timestamp, occurred_at, metadata
       FROM events
       WHERE id IN (${placeholders})
       ORDER BY datetime(COALESCE(occurred_at, timestamp)) DESC
       LIMIT 8`,
      eventIds
    ).catch(() => []);
  }
  const receipts = [
    ...mentionRows.slice(0, 6).map((row) => ({
      source: row.source_app || row.mention_type || 'memory',
      text: row.context_snippet || '',
      timestamp: row.timestamp,
      event_id: row.event_id
    })),
    ...events.slice(0, 4).map((row) => ({
      source: row.app || row.type || 'event',
      text: row.title || row.text || '',
      timestamp: row.occurred_at || row.timestamp,
      event_id: row.id
    }))
  ].filter((item) => item.text);
  return {
    contact: detail,
    receipts: receipts.slice(0, Number(options.limit || 8)),
    draft_context_refs: Array.from(new Set(receipts.map((item) => item.event_id).filter(Boolean))).slice(0, 12)
  };
}

function buildDeterministicDraft(context) {
  const name = context?.contact?.display_name || 'there';
  const receipt = (context?.receipts || []).find((item) => item.text) || {};
  const hook = trim(receipt.text || 'our recent conversation', 120);
  return `Hey ${name}, I saw ${hook} and thought it was worth following up. How are things moving on your side?`;
}

async function searchRelationshipContext(query = '', limit = 8) {
  await ensureRelationshipTables();
  const lower = String(query || '').toLowerCase();
  const relationshipQuery = /\b(contact|contacts|person|people|relationship|talked|messaged|emailed|met|follow up|follow-up|whatsapp|gmail|calendar|linkedin)\b/.test(lower)
    || /\bwho\b.*\b(talked|messaged|emailed|met|follow|contact|contacts|whatsapp|gmail|calendar|linkedin)\b/.test(lower);
  if (!relationshipQuery) return [];
  const terms = lower.replace(/[^a-z0-9@\s.-]/g, ' ').split(/\s+/).filter((t) => t.length >= 3 && !['who', 'what', 'when', 'have', 'with', 'today', 'recently', 'talked'].includes(t)).slice(0, 8);
  const params = [];
  let where = '';
  if (terms.length && !/\b(who|recent|talked|met|contacts|follow)\b/.test(lower)) {
    where = `WHERE ${terms.map(() => `(LOWER(rc.display_name) LIKE ? OR LOWER(COALESCE(rci.normalized_value, '')) LIKE ?)`).join(' OR ')}`;
    for (const term of terms) {
      params.push(`%${term}%`, `%${term}%`);
    }
  }
  params.push(Math.max(1, Math.min(20, Number(limit || 8))));
  const rows = await db.allQuery(
    `SELECT rc.*, GROUP_CONCAT(DISTINCT rci.identifier_value) AS identifiers
     FROM relationship_contacts rc
     LEFT JOIN relationship_contact_identifiers rci ON rci.contact_id = rc.id
     ${where}
     GROUP BY rc.id
     ORDER BY datetime(rc.last_interaction_at) DESC, rc.strength_score ASC
     LIMIT ?`,
    params
  ).catch(() => []);
  return (rows || []).map((row) => ({
    id: row.id,
    layer: 'relationship',
    type: 'relationship_contact',
    title: row.display_name,
    text: `${row.display_name}: ${row.status || 'warm'} relationship, last interaction ${row.last_interaction_at || 'unknown'}, strength ${Number(row.strength_score || 0).toFixed(2)}. ${row.company ? `Company: ${row.company}.` : ''} ${row.role ? `Role: ${row.role}.` : ''}`,
    score: 0.92,
    reason: 'relationship_graph_lookup',
    metadata: asObj(row.metadata),
    relationship_contact_id: row.id,
    identifiers: row.identifiers ? row.identifiers.split(',').filter(Boolean) : []
  }));
}

module.exports = {
  ensureRelationshipTables,
  upsertRelationshipContact,
  linkRelationshipMention,
  linkMentionsForEvent,
  runRelationshipGraphJob,
  getRelationshipContacts,
  getRelationshipContactDetail,
  updateRelationshipContactProfile,
  buildRelationshipDraftContext,
  buildDeterministicDraft,
  searchRelationshipContext,
  syncAppleContactsIntoRelationshipGraph,
  __test__: {
    normalizeName,
    isLikelyPersonName,
    extractLinkedInProfile,
    extractObservedParticipants,
    scoreStatus
  }
};
