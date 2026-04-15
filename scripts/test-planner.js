const { planNextAction } = require('../services/agent/agentPlanner');

async function run() {
  const goal = 'hello';
  const observation = {
    url: 'https://www.google.com/search?q=existing',
    interactive_elements: [{ name: 'q', selector: 'input[name="q"]', type: 'input' }],
    inner_text_sample: 'some text here'
  };

  const res = planNextAction(goal, [], observation);
  if (res && typeof res.on === 'function') {
    console.log('Planner returned emitter; waiting for done...');
    res.on('chunk', c => process.stdout.write(c));
    res.on('done', (txt) => { console.log('\nDONE:', txt); process.exit(0); });
    res.on('error', (e) => { console.error('Planner error', e); process.exit(1); });
  } else {
    console.log('Planner returned action object:', res);
    process.exit(0);
  }
}

run();
