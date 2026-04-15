const extractor = require('../services/extractors/openLoopExtractor');
const scoring = require('../services/scoring');

const sampleMessage = {
  id: 'msg_1',
  user_id: 'user_abc',
  from: 'recruiter@example.com',
  to: ['you@example.com'],
  subject: 'Opportunity: Product Manager role',
  snippet: "Hi — I'd love to chat. I'll send details tomorrow. Are you available next week?",
  timestamp: new Date().toISOString(),
  opened_count: 4,
  reply_status: 'no_reply'
};

const samplePage = {
  id: 'page_1',
  user_id: 'user_abc',
  url: 'https://company.com/careers/12345/apply',
  domain: 'company.com',
  title: 'Product Manager - Apply Now',
  timestamp: new Date().toISOString(),
  duration_ms: 120000
};

const userProfile = {
  id: 'user_abc',
  learned: { cluster_weights: { 'recruiter_followup': 0.8, 'awaiting_reply': 0.6 }, productive_hours: [9,10,14] },
  goals: [{ text: 'Land a role at McKinsey', cluster: 'job_search' }]
};

// Run extractor on both
const r1 = extractor.processEvent(sampleMessage);
const r2 = extractor.processEvent(samplePage);

const openLoops = [...(r1.openLoops || []), ...(r2.openLoops || [])];

console.log('Open Loops:', JSON.stringify(openLoops, null, 2));

const suggestions = scoring.generateSuggestionsFromOpenLoops(openLoops, userProfile, {});
console.log('Suggestions:', JSON.stringify(suggestions, null, 2));
