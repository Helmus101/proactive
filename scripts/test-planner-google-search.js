const { planNextAction } = require('../services/agent/agentPlanner');

const goal = 'Search for hello on Google';
const history = [];
const observation = {
  surface_driver: 'cdp',
  frontmost_app: 'Google Chrome',
  surface_type: 'search_home',
  interactive_candidates: [
    { id: 'cdp-search-1', group: 'search_field', role: 'textbox', name: 'Search', description: 'Google search field' },
    { id: 'cdp-search-btn-1', group: 'primary_button', role: 'button', name: 'Google Search', description: 'Search' }
  ],
  visible_elements: [],
  text_sample: '',
  browser_tree: { url: 'https://www.google.com', title: 'Google' }
};

const result = planNextAction(goal, history, observation, {});
console.log('Planner output:', JSON.stringify(result, null, 2));
