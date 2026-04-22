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
    { name: 'Sarah Chen', email: 'sarah@example.com', last_interaction_at: new Date().toISOString() },
    [{ type: 'email', value: 'sarah@example.com', source_label: 'gmail' }],
    { source: 'gmail', event_id: 'evt_gmail_sarah', timestamp: new Date().toISOString() }
  );
  assert.ok(sarah?.id, 'Gmail contact should be created');

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

  await relationshipGraph.linkMentionsForEvent({
    eventId: 'evt_linkedin_marcus',
    type: 'ScreenCapture',
    source: 'Sensors',
    text: [
      'Marcus Brown',
      'Founder at Anqer',
      'LinkedIn profile',
      'Building AI workflow tools'
    ].join('\n'),
    metadata: {
      app: 'Google Chrome',
      window_title: 'Marcus Brown | LinkedIn',
      url: 'https://www.linkedin.com/in/marcus-brown',
      data_source: 'screenshot_ocr',
      timestamp: new Date().toISOString()
    },
    envelope: {
      app: 'Google Chrome',
      occurred_at: new Date().toISOString(),
      title: 'Marcus Brown | LinkedIn'
    }
  });

  const marcus = await db.getQuery(
    `SELECT * FROM relationship_contacts WHERE LOWER(display_name) = LOWER(?) LIMIT 1`,
    ['Marcus Brown']
  );
  assert.ok(marcus?.id, 'LinkedIn screenshot should create a contact');
  assert.ok(/Anqer/i.test(marcus.company || marcus.role || marcus.metadata || ''), 'LinkedIn profile context should be retained');

  await relationshipGraph.runRelationshipGraphJob({ backfill: false });
  const contacts = await relationshipGraph.getRelationshipContacts({ limit: 10 });
  assert.ok(contacts.length >= 2, 'relationship contacts should be queryable');
  assert.ok(contacts.every((contact) => typeof contact.strength_score === 'number'), 'contacts should have numeric strength scores');

  const relationshipEvidence = await relationshipGraph.searchRelationshipContext('who have i talked to recently', 5);
  assert.ok(relationshipEvidence.length >= 2, 'relationship chat lookup should return contacts');
  assert.ok(relationshipEvidence.some((item) => /Sarah Chen|Marcus Brown/.test(item.title)), 'relationship lookup should include known contacts');

  const draftContext = await relationshipGraph.buildRelationshipDraftContext(sarah.id);
  const draft = relationshipGraph.buildDeterministicDraft(draftContext);
  assert.ok(/Sarah Chen/.test(draft), 'draft should address the contact by name');
  assert.ok(/seed round|team building|follow/i.test(draft), 'draft should use grounded relationship context');

  console.log('relationship-graph.test.js passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
