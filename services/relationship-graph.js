const crypto = require('crypto');
const db = require('./db');

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

function trim(value = '', max = 220) {
  const text = normalizeText(value);
  return text.length > max ? `${text.slice(0, Math.max(0, max - 1)).trim()}…` : text;
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

function isLikelyPersonName(value = '') {
  const text = normalizeName(value);
  if (!text || text.length < 3 || text.length > 80) return false;
  if (isEmail(text)) return true;
  if (!/^[A-Z][\p{L}'-]+(?:\s+[A-Z][\p{L}'-]+){0,3}$/u.test(text)) return false;
  if (/\b(App|Chrome|Safari|Calendar|Gmail|Google|LinkedIn|Messages|Slack|Team|School|University|College|Inc|LLC|Ltd|Labs|Studio|Support|Settings|Terminal|Code)\b/.test(text)) return false;
  return true;
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
    last_interaction_at TEXT,
    interaction_count_30d INTEGER DEFAULT 0,
    relationship_tier TEXT,
    status TEXT DEFAULT 'warm',
    metadata TEXT,
    created_at TEXT,
    updated_at TEXT
  )`).catch(() => {});
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
  const title = contact.display_name || contact.name || 'Contact';
  const nodeId = `person_${contactId.replace(/^rel_/, '')}`;
  const metadata = {
    relationship_contact_id: contactId,
    name: title,
    email: identifiers.find((item) => item.identifier_type === 'email')?.identifier_value || null,
    company: contact.company || null,
    role: contact.role || null,
    latest_interaction_at: contact.last_interaction_at || evidence.timestamp || null,
    strength_score: Number(contact.strength_score || 0),
    relationship_tier: contact.relationship_tier || null,
    source_refs: [evidence.event_id].filter(Boolean),
    topics: asList(contact.topics || asObj(contact.metadata).topics).slice(0, 12)
  };

  await db.runQuery(
    `INSERT OR REPLACE INTO memory_nodes
     (id, layer, subtype, title, summary, canonical_text, confidence, status, source_refs, metadata, graph_version, created_at, updated_at, embedding, anchor_date, anchor_at)
     VALUES (?, 'semantic', 'person', ?, ?, ?, ?, 'active', ?,
             ?, 'relationship_graph_v1',
             COALESCE((SELECT created_at FROM memory_nodes WHERE id = ?), ?), ?,
             COALESCE((SELECT embedding FROM memory_nodes WHERE id = ?), '[]'), ?, ?)`,
    [
      nodeId,
      title,
      `Relationship contact: ${title}`,
      `${title}\n${contact.company || ''}\n${contact.role || ''}`.trim(),
      Math.max(0.65, Number(contact.strength_score || 0.65)),
      JSON.stringify(metadata.source_refs),
      JSON.stringify(metadata),
      nodeId,
      now,
      now,
      nodeId,
      String(contact.last_interaction_at || evidence.timestamp || now).slice(0, 10),
      contact.last_interaction_at || evidence.timestamp || now
    ]
  ).catch(() => {});
  return nodeId;
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
  const metadata = {
    ...asObj(existing?.metadata),
    ...asObj(parsedContact.metadata),
    sources: Array.from(new Set([
      ...asList(asObj(existing?.metadata).sources),
      evidence.source || parsedContact.source || 'memory'
    ].filter(Boolean))),
    linkedin_observed: Boolean(parsedContact.linkedin_observed || asObj(existing?.metadata).linkedin_observed),
    score_inputs: asObj(existing?.metadata).score_inputs || null
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
       display_name = CASE
         WHEN excluded.display_name LIKE '%@%' THEN relationship_contacts.display_name
         ELSE excluded.display_name
       END,
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
      isEmail(displayName) ? nameFromEmail(displayName) : displayName,
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
  return out;
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

async function linkMentionsForEvent({ eventId, type = '', source = '', text = '', metadata = {}, envelope = {} } = {}) {
  await ensureRelationshipTables();
  const ts = envelope.occurred_at || metadata.occurred_at || metadata.timestamp || nowIso();
  const sourceApp = envelope.app || metadata.app || metadata.activeApp || source || '';
  const safeText = String(text || metadata.raw_text || metadata.redacted_text || '').slice(0, 12000);
  const participants = participantsFromMetadata(metadata, type);
  const linked = [];

  for (const participant of participants) {
    const contact = await upsertRelationshipContact(
      {
        name: participant.name,
        email: participant.email,
        last_interaction_at: ts,
        relationship_tier: participant.source === 'calendar' || participant.source === 'gmail' ? 'network' : 'observed_contact'
      },
      [
        participant.email ? { type: 'email', value: participant.email, source_label: participant.source, confidence: 1 } : null,
        participant.name ? { type: 'name', value: participant.name, source_label: participant.source, confidence: 0.95 } : null
      ].filter(Boolean),
      { source: participant.source, event_id: eventId, timestamp: ts }
    );
    if (!contact) continue;
    await linkRelationshipMention({
      contactId: contact.id,
      eventId,
      retrievalDocId: `event:${eventId}`,
      timestamp: ts,
      sourceApp,
      snippet: safeText || envelope.title || contact.display_name,
      confidence: 0.98,
      mentionType: participant.source === 'calendar' ? 'calendar_attendee' : 'direct_participant',
      metadata: { source: participant.source }
    });
    linked.push(contact);
  }

  const known = await loadKnownIdentifiers();
  const textLower = safeText.toLowerCase();
  const seen = new Set(linked.map((item) => item.id));
  for (const item of known) {
    if (seen.has(item.contact_id)) continue;
    const needle = String(item.normalized_value || '').toLowerCase();
    if (needle.length < 4) continue;
    if (!textLower.includes(needle)) continue;
    await linkRelationshipMention({
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
    seen.add(item.contact_id);
  }

  for (const candidate of extractLinkedInProfile(safeText, metadata)) {
    const contact = await upsertRelationshipContact(candidate, [
      { type: 'name', value: candidate.name, source_label: 'linkedin_screenshot', confidence: 0.82 }
    ], { source: 'linkedin_screenshot', event_id: eventId, timestamp: ts });
    if (!contact) continue;
    await linkRelationshipMention({
      contactId: contact.id,
      eventId,
      retrievalDocId: `event:${eventId}`,
      timestamp: ts,
      sourceApp: sourceApp || 'LinkedIn',
      snippet: bestSnippetAround(safeText, candidate.name),
      confidence: 0.78,
      mentionType: 'linkedin_profile_observed',
      metadata: { company: candidate.company || '', role: candidate.role || '' }
    });
    linked.push(contact);
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
  for (const contact of contacts || []) {
    const lastMs = Date.parse(contact.last_interaction_at || '') || 0;
    const days = lastMs ? Math.max(0, (now - lastMs) / (24 * 60 * 60 * 1000)) : 365;
    const since30 = new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();
    const countRow = await db.getQuery(
      `SELECT COUNT(*) AS count, AVG(COALESCE(CAST(json_extract(metadata, '$.sentiment_score') AS REAL), 0)) AS sentiment_avg
       FROM relationship_mentions
       WHERE contact_id = ? AND datetime(timestamp) >= datetime(?)`,
      [contact.id, since30]
    ).catch(() => ({ count: 0, sentiment_avg: 0 }));
    const frequency = Math.min(1, Number(countRow?.count || 0) / 8);
    const recency = Math.exp((-Math.log(2) * days) / 14);
    const tier = String(contact.relationship_tier || 'network').toLowerCase();
    const tierPriority = tier.includes('inner') ? 1 : (tier.includes('network') ? 0.78 : 0.62);
    const sentiment = Math.max(-1, Math.min(1, Number(countRow?.sentiment_avg || 0)));
    const sentimentScore = (sentiment + 1) / 2;
    const score = Math.max(0, Math.min(1, (recency * 0.42) + (frequency * 0.28) + (sentimentScore * 0.15) + (tierPriority * 0.15)));
    const status = scoreStatus(score, days);
    const metadata = {
      ...asObj(contact.metadata),
      score_inputs: {
        recency: Number(recency.toFixed(4)),
        frequency: Number(frequency.toFixed(4)),
        sentiment: Number(sentiment.toFixed(4)),
        tier_priority: Number(tierPriority.toFixed(4)),
        days_since_interaction: Number(days.toFixed(2))
      }
    };
    await db.runQuery(
      `UPDATE relationship_contacts
       SET strength_score = ?, interaction_count_30d = ?, status = ?, metadata = ?, updated_at = ?
       WHERE id = ?`,
      [Number(score.toFixed(4)), Number(countRow?.count || 0), status, JSON.stringify(metadata), nowIso(), contact.id]
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
  for (const row of rows || []) {
    const metadata = { ...asObj(row.metadata), participants: asList(row.participants) };
    await linkMentionsForEvent({
      eventId: row.id,
      type: row.type,
      source: row.source,
      text: row.text,
      metadata,
      envelope: { app: row.app, title: row.title, occurred_at: row.occurred_at || row.timestamp }
    }).catch(() => {});
  }

  const personRows = await db.allQuery(
    `SELECT id, title, summary, metadata, updated_at, created_at
     FROM memory_nodes
     WHERE layer = 'semantic' AND subtype = 'person'
     LIMIT 1000`
  ).catch(() => []);
  for (const row of personRows || []) {
    const metadata = asObj(row.metadata);
    await upsertRelationshipContact({
      name: row.title,
      email: metadata.email || null,
      company: metadata.company || null,
      role: metadata.role || null,
      last_interaction_at: metadata.latest_interaction_at || row.updated_at || row.created_at,
      relationship_tier: metadata.relationship_tier || 'observed_contact',
      metadata: { semantic_node_id: row.id, notes: row.summary || '' }
    }, [
      metadata.email ? { type: 'email', value: metadata.email, source_label: 'semantic_node' } : null,
      row.title ? { type: 'name', value: row.title, source_label: 'semantic_node', confidence: 0.8 } : null
    ].filter(Boolean), { source: 'semantic_node', node_id: row.id, timestamp: row.updated_at || row.created_at }).catch(() => {});
  }
}

async function runRelationshipGraphJob(options = {}) {
  await ensureRelationshipTables();
  if (options.backfill) await backfillRelationshipContacts();
  await rescoreRelationshipContacts();
  const count = await db.getQuery(`SELECT COUNT(*) AS count FROM relationship_contacts`).catch(() => ({ count: 0 }));
  return { contacts: Number(count?.count || 0), backfill: Boolean(options.backfill), updated_at: nowIso() };
}

async function getRelationshipContacts({ limit = 50, status = null } = {}) {
  await ensureRelationshipTables();
  const params = [];
  const where = [];
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
  return (rows || []).map((row) => ({ ...row, metadata: asObj(row.metadata) }));
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
  return { ...contact, metadata: asObj(contact.metadata), identifiers, mentions };
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
  buildRelationshipDraftContext,
  buildDeterministicDraft,
  searchRelationshipContext,
  __test__: {
    normalizeName,
    isLikelyPersonName,
    extractLinkedInProfile,
    scoreStatus
  }
};
