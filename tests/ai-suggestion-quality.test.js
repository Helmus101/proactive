const assert = require('assert');

const db = require('../services/db');
const feedGeneration = require('../services/agent/feed-generation');

function testRejectsTemplateTone() {
  const gate = feedGeneration.__test__.qualityGateSuggestion({
    title: 'Complete next step now',
    body: 'Take the next step now.',
    reason: 'Because this should keep momentum.',
    display: { headline: 'Action', summary: 'Do it now', insight: 'Pattern' },
    epistemicTrace: [
      { node_id: 'n1', source: 'Gmail', text: 'Thread', timestamp: new Date().toISOString() },
      { node_id: 'n2', source: 'Calendar', text: 'Meeting', timestamp: new Date().toISOString() }
    ],
    suggestedActions: [{ label: 'Open context', type: 'browser_operator', payload: { action: 'open_context' } }]
  });
  assert.strictEqual(gate.pass, false);
  assert.strictEqual(gate.reasons.hasTemplate, true);
}

function testAcceptsReceiptGroundedActionableText() {
  const gate = feedGeneration.__test__.qualityGateSuggestion({
    title: 'Draft reply for Sarah birthday plan',
    body: 'You saw this thread in Gmail 2 hours ago. Draft and send the bakery confirmation before 14:00.',
    reason: 'Because I found this in your Gmail thread from March 12 and in your calendar event at 14:00 today.',
    display: {
      headline: 'Action: Draft Sarah follow-up',
      summary: 'Sarah birthday thread from Gmail needs a reply before 14:00 today.',
      insight: 'Same bakery topic appears in both receipts.'
    },
    epistemicTrace: [
      { node_id: 'n1', source: 'Gmail', text: 'Mochi House mention', timestamp: new Date().toISOString() },
      { node_id: 'n2', source: 'Google Calendar', text: 'Meeting in 6e at 14:00', timestamp: new Date().toISOString() }
    ],
    suggestedActions: [{ label: 'Draft birthday message', type: 'browser_operator', payload: { action: 'navigate_and_type' } }]
  });
  assert.strictEqual(gate.pass, true);
}

function testRejectsMismatchedTitleSummary() {
  const gate = feedGeneration.__test__.qualityGateSuggestion({
    title: 'Draft reply for Sarah birthday plan',
    body: 'You saw this thread in Gmail 2 hours ago. Draft and send the bakery confirmation before 14:00.',
    reason: 'Because I found this in your Gmail thread from March 12 and in your calendar event at 14:00 today.',
    display: {
      headline: 'Action: Draft Sarah follow-up',
      summary: 'Review apartment listings in Paris 7e now.',
      insight: 'Same bakery topic appears in both receipts.'
    },
    epistemicTrace: [
      { node_id: 'n1', source: 'Gmail', text: 'Mochi House mention', timestamp: new Date().toISOString() },
      { node_id: 'n2', source: 'Google Calendar', text: 'Meeting in 6e at 14:00', timestamp: new Date().toISOString() }
    ],
    suggestedActions: [{ label: 'Draft birthday message', type: 'browser_operator', payload: { action: 'navigate_and_type' } }]
  });
  assert.strictEqual(gate.pass, false);
  assert.strictEqual(gate.reasons.titleSummaryMatch, false);
}

function testRejectsGenericCtaLabel() {
  const gate = feedGeneration.__test__.qualityGateSuggestion({
    title: 'Draft reply for Sarah birthday plan',
    body: 'You saw this thread in Gmail 2 hours ago. Draft and send the bakery confirmation before 14:00.',
    reason: 'Because I found this in your Gmail thread from March 12 and in your calendar event at 14:00 today.',
    display: {
      headline: 'Action: Draft Sarah follow-up',
      summary: 'Sarah birthday thread from Gmail needs a reply before 14:00 today.',
      insight: 'Same bakery topic appears in both receipts.'
    },
    epistemicTrace: [
      { node_id: 'n1', source: 'Gmail', text: 'Mochi House mention', timestamp: new Date().toISOString() },
      { node_id: 'n2', source: 'Google Calendar', text: 'Meeting in 6e at 14:00', timestamp: new Date().toISOString() }
    ],
    suggestedActions: [{ label: 'Open context', type: 'browser_operator', payload: { action: 'open_context' } }]
  });
  assert.strictEqual(gate.pass, false);
  assert.strictEqual(gate.reasons.actionEnough, false);
}

async function testSuggestionEngineRetryKeepsOld() {
  const originalGenerate = feedGeneration.generateFeedSuggestions;
  const originalAllQuery = db.allQuery;
  const originalRunQuery = db.runQuery;

  let calls = 0;
  feedGeneration.generateFeedSuggestions = async () => {
    calls += 1;
    return [];
  };
  db.runQuery = async () => true;
  db.allQuery = async (sql) => {
    if (/FROM suggestion_artifacts/i.test(sql)) {
      return [{
        id: 'sug_old_1',
        type: 'next_action',
        title: 'Draft follow-up from yesterday thread',
        body: 'Context bundle body',
        trigger_summary: 'Thread is waiting',
        source_node_ids: JSON.stringify(['node_1']),
        source_edge_paths: JSON.stringify([]),
        confidence: 0.82,
        status: 'active',
        metadata: JSON.stringify({
          display: { headline: 'Action: Draft follow-up', summary: 'Found in Gmail', insight: 'Pending response' },
          epistemic_trace: [
            { node_id: 'node_1', source: 'Gmail', text: 'Pending reply', timestamp: new Date().toISOString() },
            { node_id: 'node_2', source: 'Calendar', text: 'Meeting today', timestamp: new Date().toISOString() }
          ],
          suggested_actions: [{ label: 'Draft reply', type: 'browser_operator', payload: { action: 'navigate_and_type' } }],
          reason: 'Because found in your Gmail thread from yesterday.',
          ai_generated: true
        }),
        created_at: new Date().toISOString()
      }];
    }
    return [];
  };

  try {
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
    const { runSuggestionEngine } = require('../services/agent/suggestion-engine');
    const result = await runSuggestionEngine('fake-key', {});
    assert.ok(Array.isArray(result) && result.length === 1, 'expected previous artifact to be returned');
    assert.strictEqual(calls, 2, 'expected one retry before keep-old');
    assert.strictEqual(result[0].id, 'sug_old_1');
  } finally {
    feedGeneration.generateFeedSuggestions = originalGenerate;
    db.allQuery = originalAllQuery;
    db.runQuery = originalRunQuery;
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
  }
}

async function testSuggestionEngineCapsActionablePoolAtSeven() {
  const originalGenerate = feedGeneration.generateFeedSuggestions;
  const originalAllQuery = db.allQuery;
  const originalRunQuery = db.runQuery;
  const writes = [];
  const updates = [];

  const makeSuggestion = (index) => ({
    id: `sug_new_${index}`,
    type: 'work',
    title: `Draft Project ${index} update`,
    body: `Prepare the concrete Project ${index} update from memory evidence.`,
    description: `Prepare the concrete Project ${index} update from memory evidence.`,
    reason: `Because Project ${index} appeared in recent memory evidence today.`,
    confidence: 0.95 - index * 0.01,
    status: 'active',
    category: 'work',
    priority: index < 3 ? 'high' : 'medium',
    ai_generated: true,
    suggested_actions: [{ label: `Draft Project ${index} update`, type: 'manual', payload: { action: 'open_memory_timeline' } }],
    primary_action: { label: `Draft Project ${index} update`, type: 'manual', payload: { action: 'open_memory_timeline' } },
    plan: [`Open Project ${index} evidence`, 'Draft the update'],
    epistemic_trace: [
      { node_id: `node_${index}`, source: 'Memory', text: `Project ${index} signal`, timestamp: new Date().toISOString() },
      { node_id: `node_${index}_2`, source: 'Memory', text: `Project ${index} follow-up`, timestamp: new Date().toISOString() }
    ],
    created_at: new Date(Date.now() - index * 1000).toISOString()
  });

  feedGeneration.generateFeedSuggestions = async () => Array.from({ length: 10 }, (_, i) => makeSuggestion(i + 1));
  db.allQuery = async (sql) => {
    if (/SELECT id FROM suggestion_artifacts/i.test(sql)) {
      return Array.from({ length: 10 }, (_, i) => ({ id: `sug_new_${i + 1}` }));
    }
    if (/FROM suggestion_artifacts/i.test(sql)) return [];
    if (/SELECT doc_id FROM retrieval_docs/i.test(sql)) return [];
    return [];
  };
  db.runQuery = async (sql, params = []) => {
    if (/INSERT OR REPLACE INTO suggestion_artifacts/i.test(sql)) writes.push({ sql, params });
    if (/UPDATE suggestion_artifacts SET status = 'inactive'/i.test(sql)) updates.push(params[0]);
    return true;
  };

  try {
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
    const { runSuggestionEngine } = require('../services/agent/suggestion-engine');
    const result = await runSuggestionEngine('fake-key', {});
    assert.strictEqual(result.length, 7, 'expected exactly seven active suggestions returned');
    assert.strictEqual(writes.length, 7, 'expected only seven suggestions persisted');
    assert.ok(updates.length >= 3, 'expected extra active artifacts to be pruned inactive');
    assert.ok(result.every((item) => item.primary_action || (item.suggested_actions || []).length), 'all returned suggestions must be actionable');
  } finally {
    feedGeneration.generateFeedSuggestions = originalGenerate;
    db.allQuery = originalAllQuery;
    db.runQuery = originalRunQuery;
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
  }
}

async function main() {
  testRejectsTemplateTone();
  testAcceptsReceiptGroundedActionableText();
  testRejectsMismatchedTitleSummary();
  testRejectsGenericCtaLabel();
  await testSuggestionEngineRetryKeepsOld();
  await testSuggestionEngineCapsActionablePoolAtSeven();
  console.log('ai-suggestion-quality.test.js passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
