const assert = require('assert');

process.env.PROACTIVE_USER_DATA_DIR = process.env.PROACTIVE_USER_DATA_DIR || '/tmp/proactive-relationship-test-db';

const db = require('../services/db');
const relationshipGraph = require('../services/relationship-graph');

async function tableExists(name) {
  const row = await db.getQuery(
    `SELECT name FROM sqlite_master WHERE type='table' AND name = ?`,
    [name]
  );
  return Boolean(row);
}

async function main() {
  await db.initDB();
  await relationshipGraph.ensureRelationshipTables();

  assert.ok(await tableExists('relationship_contacts'), 'relationship_contacts table should exist');
  assert.ok(await tableExists('relationship_contact_identifiers'), 'relationship_contact_identifiers table should exist');
  assert.ok(await tableExists('relationship_mentions'), 'relationship_mentions table should exist');

  const sarah = await relationshipGraph.upsertRelationshipContact(
    {
      name: 'Sarah Chen',
      email: 'sarah@example.com',
      last_interaction_at: new Date().toISOString(),
      metadata: { apple_contacts: true, emails: ['sarah@example.com'] },
      relationship_tier: 'apple_contact'
    },
    [{ type: 'email', value: 'sarah@example.com', source_label: 'apple_contacts' }],
    { source: 'apple_contacts', event_id: 'evt_gmail_sarah', timestamp: new Date().toISOString() }
  );
  assert.ok(sarah?.id, 'Apple contact should be created');

  const sarahFromCalendar = await relationshipGraph.upsertRelationshipContact(
    { name: 'Sarah C', email: 'sarah@example.com', last_interaction_at: new Date().toISOString() },
    [{ type: 'email', value: 'sarah@example.com', source_label: 'calendar' }],
    { source: 'calendar', event_id: 'evt_cal_sarah', timestamp: new Date().toISOString() }
  );
  assert.strictEqual(sarahFromCalendar.id, sarah.id, 'calendar attendee should dedupe by email');

  await relationshipGraph.linkMentionsForEvent({
    eventId: 'evt_screen_sarah',
    type: 'ScreenCapture',
    source: 'Sensors',
    text: 'Sarah Chen said the seed round is moving and team building is next.',
    metadata: {
      app: 'Google Chrome',
      window_title: 'Notes',
      data_source: 'screenshot_ocr',
      timestamp: new Date().toISOString()
    },
    envelope: {
      app: 'Google Chrome',
      occurred_at: new Date().toISOString(),
      title: 'Notes'
    }
  });

  const sarahMentions = await db.allQuery(
    `SELECT * FROM relationship_mentions WHERE contact_id = ?`,
    [sarah.id]
  );
  assert.ok(sarahMentions.length >= 1, 'screenshot mention should link to known contact');

  await relationshipGraph.runRelationshipGraphJob({ backfill: false });
  const contacts = await relationshipGraph.getRelationshipContacts({ limit: 10 });
  assert.ok(contacts.length >= 1, 'apple contacts should be queryable');
  assert.ok(contacts.every((contact) => typeof contact.strength_score === 'number'), 'contacts should have numeric strength scores');

  const relationshipEvidence = await relationshipGraph.searchRelationshipContext('who have i talked to recently', 5);
  assert.ok(relationshipEvidence.length >= 1, 'relationship chat lookup should return contacts');
  assert.ok(relationshipEvidence.some((item) => /Sarah Chen/.test(item.title)), 'relationship lookup should include known contacts');

  const draftContext = await relationshipGraph.buildRelationshipDraftContext(sarah.id);
  const draft = relationshipGraph.buildDeterministicDraft(draftContext);
  assert.ok(/Sarah Chen/.test(draft), 'draft should address the contact by name');
  assert.ok(/seed round|team building|follow/i.test(draft), 'draft should use grounded relationship context');

  const sarahPersonNode = await db.getQuery(
    `SELECT * FROM memory_nodes WHERE subtype = 'person' AND json_extract(metadata, '$.relationship_contact_id') = ? LIMIT 1`,
    [sarah.id]
  );
  assert.ok(sarahPersonNode?.id, 'contact should be persisted as a semantic person node');

  const sarahNodeMeta = JSON.parse(sarahPersonNode.metadata || '{}');
  assert.strictEqual(sarahNodeMeta.first_name, 'Sarah', 'person node should store first name');
  assert.strictEqual(sarahNodeMeta.last_name, 'Chen', 'person node should store last name');
  assert.ok(Array.isArray(sarahNodeMeta.interaction_refs) && sarahNodeMeta.interaction_refs.length >= 1, 'person node should retain interaction refs over time');

  const updated = await relationshipGraph.updateRelationshipContactProfile(sarah.id, {
    display_name: 'Sarah Chen',
    first_name: 'Sarah',
    last_name: 'Chen',
    company: 'Canva',
    role: 'Head of Product',
    interests: ['Hiring', 'Seed rounds'],
    topics: ['Partnership'],
    notes: 'User-edited relationship profile'
  });
  assert.strictEqual(updated.display_name, 'Sarah Chen', 'updated contact detail should use Weave display override');
  assert.strictEqual(updated.editable_fields.company, 'Canva', 'editable company should be stored');
  assert.ok(updated.editable_fields.interests.includes('Hiring'), 'editable interests should be returned');

  const topicNode = await db.getQuery(
    `SELECT * FROM memory_nodes WHERE subtype = 'topic' AND LOWER(title) LIKE LOWER(?) LIMIT 1`,
    ['Partnership']
  );
  assert.ok(topicNode?.id, 'topic nodes related to the contact should be persisted into memory');

  const refreshedPersonNode = await db.getQuery(
    `SELECT * FROM memory_nodes WHERE subtype = 'person' AND json_extract(metadata, '$.relationship_contact_id') = ? LIMIT 1`,
    [sarah.id]
  );
  const refreshedMeta = JSON.parse(refreshedPersonNode.metadata || '{}');
  assert.strictEqual(refreshedMeta.company, 'Canva', 'person node metadata should reflect Weave edits');
  assert.ok(Array.isArray(refreshedMeta.interests) && refreshedMeta.interests.includes('Hiring'), 'person node should keep edited interests');

  console.log('relationship-graph.test.js passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
