const { generateFeedSuggestions } = require('../services/agent/feed-generation');

(async () => {
  try {
    const suggestions = await generateFeedSuggestions(null, Date.now(), {});
    console.log('Suggestions:', suggestions.map(s => ({ id: s.id, title: s.title, review_required: s.review_required || false }))); 
  } catch (e) {
    console.error('Feed generation failed:', e.message);
    process.exitCode = 2;
  }
})();
