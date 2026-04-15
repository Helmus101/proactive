const assert = require('assert');
const {
  normalizeSuggestion,
  passesIntentFirstQuality,
  rankAndLimitSuggestions
} = require('../services/agent/intent-first-suggestions');

function testRejectsGenericSuggestions() {
  assert.strictEqual(passesIntentFirstQuality({
    title: 'Take action on LinkedIn',
    intent: 'Advance the outreach or application',
    reason: 'You revisited LinkedIn several times today.',
    plan: ['Open LinkedIn', 'Do something']
  }), false);

  assert.strictEqual(passesIntentFirstQuality({
    title: 'Follow up on captured work',
    intent: 'Advance the active piece of work',
    reason: 'You opened this recently.',
    plan: ['Open the work', 'Continue it']
  }), false);
}

function testNormalizesRichSuggestions() {
  const suggestion = normalizeSuggestion({
    title: 'Reply to Alexandra about the onboarding draft',
    intent: 'Close the communication loop on the onboarding draft',
    reason: 'Alexandra asked for a response in the thread and the draft was reopened today before tomorrow morning.',
    plan: ['Open the exact thread', 'Write the concrete reply', 'Send it before switching away'],
    confidence: 0.81,
    evidence: [{ id: 'evt_1', type: 'message' }]
  });

  assert.strictEqual(suggestion.intent.includes('onboarding draft'), true);
  assert.strictEqual(Array.isArray(suggestion.plan), true);
  assert.strictEqual(suggestion.plan.length, 3);
  assert.strictEqual(passesIntentFirstQuality(suggestion), true);
}

function testRankingPrefersSpecificity() {
  const ranked = rankAndLimitSuggestions([
    {
      title: 'Work on the draft',
      intent: 'Move the draft toward a shareable state',
      reason: 'You edited it today before tomorrow.',
      plan: ['Open it', 'Continue']
    },
    {
      title: 'Finalize the pricing slide for the investor deck',
      intent: 'Move the investor deck toward a shareable state',
      reason: 'You edited the deck twice today and the investor meeting starts tomorrow morning.',
      plan: ['Open the investor deck', 'Finish the pricing slide', 'Export the updated deck'],
      evidence: [{ id: 'evt_2', type: 'doc' }]
    }
  ], { maxTotal: 5 });

  assert.strictEqual(ranked.length, 1);
  assert.strictEqual(ranked[0].title, 'Finalize the pricing slide for the investor deck');
}

function main() {
  testRejectsGenericSuggestions();
  testNormalizesRichSuggestions();
  testRankingPrefersSpecificity();
  console.log('intent-first suggestion tests passed');
}

main();
