/**
 * indexing.js
 *
 * Implements Section 3.1: Inverted Index for fast retrieval of context.
 * Maps: 
 *   people -> [dates]
 *   topics -> [dates]
 */

function rebuildInvertedIndex(summaries) {
  const peopleIndex = {};
  const topicIndex = {};

  Object.entries(summaries).forEach(([date, summary]) => {
    // Index people
    const people = [
      ...(summary.top_people || []),
      ...(summary.top_contacts || [])
    ];
    
    people.forEach(person => {
      const name = person.toLowerCase().trim();
      if (!name) return;
      if (!peopleIndex[name]) peopleIndex[name] = [];
      if (!peopleIndex[name].includes(date)) {
        peopleIndex[name].push(date);
      }
    });

    // Index topics / tags
    const topics = [
      ...(summary.topics || []),
      ...(summary.tags   || []),
      ...(summary.intent_clusters || [])
    ];

    topics.forEach(topic => {
      const t = topic.toLowerCase().trim();
      if (!t) return;
      if (!topicIndex[t]) topicIndex[t] = [];
      if (!topicIndex[t].includes(date)) {
        topicIndex[t].push(date);
      }
    });
  });

  return {
    people: peopleIndex,
    topics: topicIndex,
    lastUpdated: new Date().toISOString()
  };
}

module.exports = { rebuildInvertedIndex };
