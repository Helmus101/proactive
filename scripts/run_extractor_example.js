const extractor = require('../services/extractors/openLoopExtractor');

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

console.log('--- Running extractor on sample message ---');
const res1 = extractor.processEvent(sampleMessage);
console.log(JSON.stringify(res1, null, 2));

console.log('\n--- Running extractor on sample page visit ---');
const res2 = extractor.processEvent(samplePage);
console.log(JSON.stringify(res2, null, 2));
