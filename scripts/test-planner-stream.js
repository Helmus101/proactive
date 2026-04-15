const { planNextAction } = require('../services/agent/agentPlanner');

async function run() {
  const goal = 'search for "latest electron release notes"';
  // Observation chosen to avoid heuristic shortcuts
  const observation = {
    url: 'https://example.com/somepage',
    interactive_elements: [
      { type: 'div', text: 'Welcome' }
    ],
    inner_text_sample: 'This page has no obvious search box'
  };

  const r = planNextAction(goal, [], observation);
  if (r && typeof r.on === 'function') {
    console.log('Planner returned emitter (streaming). Waiting for chunks...');
    r.on('chunk', (c) => process.stdout.write(c));
    r.on('done', (final) => { console.log('\n--- DONE ---\n', final); process.exit(0); });
    r.on('error', (e) => { console.error('Planner stream error:', e); process.exit(1); });
  } else {
    console.log('Planner returned synchronous action:', r);
    process.exit(0);
  }
}

run();
