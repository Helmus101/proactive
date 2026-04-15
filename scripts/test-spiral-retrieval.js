const { buildHybridGraphRetrieval } = require('../services/agent/hybrid-graph-retrieval');

(async () => {
  try {
    const res = await buildHybridGraphRetrieval({ query: 'Project X next steps', options: { strategy: 'spiral', mode: 'suggestion' }, seedLimit: 3, hopLimit: 1 });
    console.log('Got retrieval:', { seed_count: res.seed_results.length, evidence_count: res.evidence_count, strategy: res.strategy });
  } catch (e) {
    console.error('Retrieval failed:', e.message);
    process.exitCode = 2;
  }
})();
