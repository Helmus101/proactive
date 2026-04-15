const Store = require('electron-store');
const store = new Store({ name: 'proactive-test' });
const { buildGlobalGraph } = require('../services/agent/intelligence-engine');

async function run() {
  const mock = {
    Gmail: { rawItems: [ { id: 'email_1', title: 'Meeting with Bob', type: 'email', text: 'We should sync next week about Q2.', timestamp: Date.now() } ], appName: 'Gmail' },
    Drive: { rawItems: [ { id: 'doc_1', name: 'Project Plan', last_modified: Date.now(), type: 'doc', text: 'Project plan v1' } ], appName: 'Drive' }
  };

  const res = await buildGlobalGraph({ appDataMap: mock, apiKey: process.env.DEEPSEEK_API_KEY || '', store });
  console.log('Graph result', { nodes: res.nodes.length, edges: res.edges.length });
  console.log('Sample nodes', res.nodes.slice(0,5));
}

run().catch(e => { console.error(e); process.exit(1); });
