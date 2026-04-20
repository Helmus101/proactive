const assert = require('assert');

const { __test__ } = require('../services/agent/opportunity-miner');

function nowIso(offsetMs = 0) {
  return new Date(Date.now() + offsetMs).toISOString();
}

async function testUnresolvedFollowupDetector() {
  const rows = [{
    id: 'sem_person_1',
    subtype: 'person',
    title: 'Sarah',
    summary: 'Open follow up needed',
    canonical_text: 'pending reply',
    confidence: 0.8,
    source_refs: JSON.stringify(['evt_1']),
    metadata: JSON.stringify({
      source_type_group: 'communication',
      latest_interaction_at: nowIso(-(4 * 24 * 60 * 60 * 1000))
    }),
    updated_at: nowIso(-(4 * 24 * 60 * 60 * 1000)),
    created_at: nowIso(-(6 * 24 * 60 * 60 * 1000))
  }];

  const out = await __test__.detectUnresolvedFollowups(rows, Date.now());
  assert.ok(Array.isArray(out) && out.length === 1);
  assert.strictEqual(out[0].opportunity_type, 'unresolved_followup');
}

async function testUnfinishedLoopDetector() {
  const rows = [{
    id: 'task_1',
    subtype: 'task',
    status: 'open',
    title: 'Finish Chinese study Paper 2 idiom review',
    summary: 'unfinished',
    canonical_text: '',
    source_refs: JSON.stringify(['evt_2']),
    metadata: JSON.stringify({})
  }];
  const out = await __test__.detectUnfinishedLoops(rows, Date.now());
  assert.ok(out.length >= 1);
  assert.strictEqual(out[0].opportunity_type, 'unfinished_study_loop');
}

async function testDeadlineRiskDetector() {
  const rows = [{
    id: 'task_deadline_1',
    subtype: 'task',
    title: 'Submit economics outline',
    summary: '',
    source_refs: JSON.stringify([]),
    metadata: JSON.stringify({
      due_date: nowIso(8 * 60 * 60 * 1000)
    })
  }];
  const out = await __test__.detectDeadlineRisk(rows, Date.now());
  assert.ok(out.length >= 1);
  assert.strictEqual(out[0].opportunity_type, 'deadline_risk');
}

async function testDormantContactDetector() {
  const rows = [{
    id: 'person_2',
    subtype: 'person',
    title: 'Alex',
    confidence: 0.9,
    source_refs: JSON.stringify([]),
    metadata: JSON.stringify({
      latest_interaction_at: nowIso(-(20 * 24 * 60 * 60 * 1000)),
      importance: 0.9
    })
  }];
  const out = await __test__.detectDormantContacts(rows, Date.now());
  assert.ok(out.length >= 1);
  assert.strictEqual(out[0].opportunity_type, 'dormant_important_contact');
}

async function testWeakConceptDetector() {
  const rows = [{
    id: 'cloud_weak_1',
    title: 'Chinese study vocab',
    summary: 'repeated weak signal',
    source_refs: JSON.stringify([]),
    metadata: JSON.stringify({
      repeat_count: 3,
      latest_activity_at: nowIso(-2 * 60 * 60 * 1000)
    })
  }];
  const out = await __test__.detectWeakStudyConcept(rows, Date.now());
  assert.ok(out.length >= 1);
  assert.strictEqual(out[0].opportunity_type, 'weak_repeated_study_concept');
}

async function testRelationshipIntelligenceDetector() {
  const semanticRows = [{
    id: 'sem_person_birthday',
    subtype: 'person',
    title: 'Bob',
    metadata: JSON.stringify({
      birthday: new Date().toISOString().slice(0, 10), // today
      topics: ['AI', 'Robots']
    })
  }];
  const recentEvents = [{
    id: 'evt_link_1',
    type: 'link',
    window_title: 'AI is taking over',
    metadata: JSON.stringify({ url: 'https://techcrunch.com/ai' })
  }];
  const out = await __test__.detectRelationshipIntelligence(semanticRows, [], recentEvents, Date.now());
  assert.ok(out.length >= 2); // 1 birthday + 1 article share
  assert.ok(out.some(o => o.opportunity_type === 'birthday_reminder'));
  assert.ok(out.some(o => o.opportunity_type === 'article_share'));
}

function testDedupeCollapse() {
  const deduped = __test__.dedupeCandidates([
    { opportunity_type: 'unfinished_work_loop', canonical_target: 'project alpha', score: 0.61 },
    { opportunity_type: 'unfinished_work_loop', canonical_target: 'project alpha', score: 0.77 },
    { opportunity_type: 'unfinished_work_loop', canonical_target: 'project beta', score: 0.65 }
  ]);
  assert.strictEqual(deduped.length, 2);
  const alpha = deduped.find((item) => item.canonical_target === 'project alpha');
  assert.ok(alpha);
  assert.strictEqual(alpha.score, 0.77);
}

async function main() {
  await testUnresolvedFollowupDetector();
  await testUnfinishedLoopDetector();
  await testDeadlineRiskDetector();
  await testDormantContactDetector();
  await testWeakConceptDetector();
  await testRelationshipIntelligenceDetector();
  testDedupeCollapse();
  console.log('opportunity-miner.test.js passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
