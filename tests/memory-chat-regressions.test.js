const assert = require('assert');

const db = require('../services/db');
const { scoreGraphFeedSeeds } = require('../services/agent/feed-generation');
const { buildThinkingTrace } = require('../services/agent/chat-engine');
const { normalizeEventEnvelope } = require('../services/ingestion');
const { clusterEnvelopes } = require('../services/agent/graph-derivation');
const { buildRetrievalThought } = require('../services/agent/retrieval-thought-system');
const { planNextAction, normalizeDesktopGoal } = require('../services/agent/agentPlanner');

async function testEpisodeSeedSourceRefs() {
  const originalAllQuery = db.allQuery;
  db.allQuery = async (sql, params = []) => {
    const layer = params[0];
    if (layer === 'semantic') return [];
    if (layer === 'cloud') return [];
    if (layer === 'episode') {
      return [{
        id: 'ep_1',
        layer: 'episode',
        subtype: 'communication',
        title: 'Alexandra follow-up',
        summary: 'Recent email thread',
        canonical_text: '',
        confidence: 0.8,
        status: 'active',
        source_refs: JSON.stringify(['evt_1', 'evt_2']),
        metadata: JSON.stringify({
          source_type_group: 'communication',
          event_count: 3,
          end: new Date().toISOString()
        }),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }];
    }
    return [];
  };

  try {
    const seeds = await scoreGraphFeedSeeds(Date.now());
    assert.ok(Array.isArray(seeds));
    assert.ok(seeds.length >= 1);
    const episodeSeed = seeds.find((item) => item.id === 'ep_1');
    assert.ok(episodeSeed, 'expected episode seed to be created');
    assert.deepStrictEqual(episodeSeed.source_refs, ['evt_1', 'evt_2']);
  } finally {
    db.allQuery = originalAllQuery;
  }
}

async function testThinkingTraceShape() {
  const originalAllQuery = db.allQuery;
  db.allQuery = async (sql, params = []) => {
    if (/FROM events/i.test(sql)) {
      return [
        { id: 'evt_email_1', source_type: 'EmailThread' },
        { id: 'evt_cal_1', source_type: 'CalendarEvent' }
      ];
    }
    return [];
  };

  try {
    const trace = await buildThinkingTrace({
      query: 'What was the exact email wording from Alexandra yesterday?',
      retrieval: {
        retrieval_plan: {
          intent: 'exact memory lookup',
          mode: 'semantic',
          strategy_mode: 'memory_then_web',
          summary_vs_raw: 'raw',
          time_scope: { label: 'yesterday' },
          app_scope: ['Gmail'],
          source_scope: ['communication'],
          web_gate_reason: 'The question may need outside corroboration if memory is sparse.',
          semantic_queries: ['Alexandra email wording', 'recent Alexandra follow-up'],
          message_queries: ['Alexandra email yesterday'],
          filters: { app: 'Gmail' }
        },
        seed_nodes: [{ id: 'node_1', title: 'Alexandra email thread' }],
        expanded_nodes: [{ id: 'node_2', title: 'Calendar follow-up' }],
        evidence_count: 4,
        evidence: [
          { source_type: 'EmailThread', text: 'Alexandra sent a follow-up email' },
          { source_type: 'CalendarEvent', text: 'Meeting with Alexandra' }
        ],
        trace_summary: ['Mode: semantic', 'Applied date window: 2026-04-04T00:00:00.000Z -> 2026-04-04T23:59:59.999Z'],
        temporal_reasoning: ['Parsed "yesterday" into an absolute date filter.'],
        initial_date_range: { start: '2026-04-04T00:00:00.000Z', end: '2026-04-04T23:59:59.999Z' },
        applied_date_range: { start: '2026-04-04T00:00:00.000Z', end: '2026-04-04T23:59:59.999Z' },
        widened_date_range: null,
        date_filter_status: 'applied',
        drilldown_refs: ['evt_email_1', 'evt_cal_1'],
        lazy_source_refs: [{ ref: 'evt_email_1' }, { ref: 'evt_cal_1' }],
        generated_queries: {
          semantic: ['Alexandra email wording', 'recent Alexandra follow-up'],
          messages: ['Alexandra email yesterday'],
          lexical_terms: ['alexandra', 'wording']
        },
        seed_results: [{ id: 'node_1', title: 'Alexandra email thread', reason: 'lexical:alexandra' }],
        graph_expansion_results: [{ id: 'node_2', title: 'Calendar follow-up' }],
        web_search_used: true,
        web_sources: ['https://example.com/alexandra'],
        web_results: [{ title: 'Alexandra profile', url: 'https://example.com/alexandra', snippet: 'Example' }],
        query_variants: ['Alexandra email wording', 'recent Alexandra follow-up'],
        message_query_variants: ['Alexandra email yesterday']
      },
      drilldownEvidence: [{ id: 'evt_email_1', text: 'Exact quoted email body' }]
    });

    assert.ok(trace);
    assert.ok(typeof trace.thinking_summary === 'string' && trace.thinking_summary.length > 0);
    assert.ok(Array.isArray(trace.filters));
    assert.strictEqual(trace.strategy.strategy_mode, 'memory_then_web');
    assert.strictEqual(trace.strategy.summary_vs_raw, 'raw');
    assert.strictEqual(trace.answer_basis, 'memory_plus_raw_plus_web');
    assert.deepStrictEqual(trace.search_queries.context, ['Alexandra email wording', 'recent Alexandra follow-up']);
    assert.deepStrictEqual(trace.search_queries.messages, ['Alexandra email yesterday']);
    assert.deepStrictEqual(trace.search_queries.lexical, ['alexandra', 'wording']);
    assert.ok(typeof trace.results_summary?.headline === 'string' && trace.results_summary.headline.includes('Found'));
    assert.ok(trace.data_sources.includes('Email'));
    assert.ok(trace.data_sources.includes('Calendar'));
    assert.ok(Array.isArray(trace.connection_candidates));
    assert.ok(Array.isArray(trace.seed_results) && trace.seed_results.length === 1);
    assert.ok(Array.isArray(trace.graph_expansion_results) && trace.graph_expansion_results.length === 1);
    assert.strictEqual(trace.web_search_used, true);
    assert.deepStrictEqual(trace.web_sources, ['https://example.com/alexandra']);
  } finally {
    db.allQuery = originalAllQuery;
  }
}

function testSourceAwareTimestampNormalization() {
  const envelope = normalizeEventEnvelope({
    id: 'evt_calendar_1',
    type: 'CalendarEvent',
    timestamp: '2026-04-05T18:30:00.000Z',
    source: 'Calendar',
    text: 'Meeting with Alexandra',
    metadata: {
      start_time: '2026-04-03T09:00:00.000Z',
      updated: '2026-04-05T18:30:00.000Z',
      title: 'Alexandra sync'
    }
  });

  assert.strictEqual(envelope.occurred_at, '2026-04-03T09:00:00.000Z');
  assert.strictEqual(envelope.occurred_date, '2026-04-03');
}

function testEpisodeAnchorStaysOnFirstDay() {
  const first = normalizeEventEnvelope({
    id: 'evt_email_1',
    type: 'EmailThread',
    timestamp: '2026-04-03T10:00:00.000Z',
    source: 'Gmail',
    text: 'Subject: Albert School\nFollow up with Alexandra',
    metadata: {
      from: 'alexandra@albertschool.com',
      subject: 'Albert School follow-up',
      timestamp: '2026-04-03T10:00:00.000Z',
      threadId: 'thread_1'
    }
  });
  const second = normalizeEventEnvelope({
    id: 'evt_email_2',
    type: 'EmailThread',
    timestamp: '2026-04-04T08:00:00.000Z',
    source: 'Gmail',
    text: 'Reply from Alexandra',
    metadata: {
      from: 'alexandra@albertschool.com',
      subject: 'Albert School follow-up',
      timestamp: '2026-04-04T08:00:00.000Z',
      threadId: 'thread_1'
    }
  });

  const groups = clusterEnvelopes([second, first]);
  assert.strictEqual(groups.length, 1);
  assert.strictEqual(new Date(groups[0].anchorTs).toISOString(), '2026-04-03T10:00:00.000Z');
  assert.strictEqual(new Date(groups[0].latestTs).toISOString(), '2026-04-04T08:00:00.000Z');
}

async function testRetrievalThoughtExtractsExactTerms() {
  const thought = await buildRetrievalThought({
    query: 'What was the exact email from alexandra@albertschool.com about manifest.json yesterday?'
  });

  assert.ok(thought.lexical_terms.includes('alexandra@albertschool.com'));
  assert.ok(thought.lexical_terms.includes('manifest.json'));
  assert.ok(thought.semantic_queries.some((item) => /manifest\.json/i.test(item)));
  assert.deepStrictEqual(thought.filters.source_types, ['communication']);
  assert.ok(thought.query_bundle);
  assert.ok(Array.isArray(thought.query_debug?.stripped_noise_terms));
  assert.ok(thought.query_debug.stripped_noise_terms.includes('yesterday'));
}

async function testRetrievalThoughtDefaultsToSevenDaySummaryWindow() {
  const thought = await buildRetrievalThought({
    query: "What's the status of the Weave waitlist form?"
  });

  assert.strictEqual(thought.summary_vs_raw, 'summary');
  assert.strictEqual(thought.strategy_mode, 'memory_only');
  assert.strictEqual(thought.time_scope.label, 'default_last_7_days');
  assert.strictEqual(thought.time_scope.source, 'default');
  assert.ok(thought.applied_date_range?.start);
  assert.ok(thought.applied_date_range?.end);
  assert.ok(thought.semantic_queries.length >= 5 && thought.semantic_queries.length <= 7);
  assert.ok(thought.semantic_queries.every((item) => !/\byesterday\b|\blast week\b|\bwhat(?:'s| is)\b/i.test(item)));
}

async function testRetrievalThoughtBuildsCodingAndCommunicationAngles() {
  const codingThought = await buildRetrievalThought({
    query: 'How is the browser extension bug in manifest.json going?'
  });
  assert.ok(Array.isArray(codingThought.query_debug?.inferred_technical_hints));
  assert.ok(codingThought.query_debug.inferred_technical_hints.length >= 1);

  const messageThought = await buildRetrievalThought({
    query: 'Can you find the follow-up email from Sarah about the browser extension?'
  });
  assert.ok(messageThought.semantic_queries.length >= 1);
}

function testDesktopPlannerReadsUiAfterOpen() {
  const action = planNextAction(
    'Find the second New York Times article on Bank of America',
    [{ action: { kind: 'OPEN_URL', url: 'https://www.nytimes.com' }, result: 'success', error: null }],
    {
      frontmost_app: 'Google Chrome',
      window_title: 'New York Times',
      surface_type: 'browser_page',
      permission_state: { trusted: true },
      visible_elements: [],
      text_sample: ''
    }
  );

  assert.ok(action);
  assert.strictEqual(action.kind, 'READ_UI_STATE');
}

function testNormalizeDesktopGoalForGoogleSearch() {
  const goal = normalizeDesktopGoal('Search Google for hello and open the second result');
  assert.strictEqual(goal.surface_goal, 'web_search');
  assert.strictEqual(goal.query_text, 'hello');
  assert.strictEqual(goal.ordinal_target, 2);
  assert.strictEqual(goal.target_kind, 'search_result');
  assert.ok(/2 result/i.test(goal.success_predicate));
  assert.ok(Array.isArray(goal.step_hints));
  assert.ok(goal.step_hints.length >= 4);
}

function testDesktopPlannerTypesAndSubmitsSearch() {
  const typeAction = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'OPEN_URL', url: 'https://www.google.com' }, result: 'success', error: null },
      { action: { kind: 'READ_UI_STATE' }, result: 'success', error: null }
    ],
    {
      frontmost_app: 'Google Chrome',
      window_title: 'Google',
      surface_type: 'search_home',
      permission_state: { trusted: true },
      visible_elements: [
        { index: 0, role: 'text field', name: 'Search', description: 'Search', value: '', enabled: true }
      ],
      interactive_candidates: [
        { index: 0, role: 'text field', name: 'Search', description: 'Search', value: '', enabled: true, group: 'search_field', hint: '1. text field Search' }
      ],
      text_sample: 'Google Search'
    }
  );

  assert.strictEqual(typeAction.kind, 'SET_VALUE');
  assert.strictEqual(typeAction.text, 'hello');

  const submitAction = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'OPEN_URL', url: 'https://www.google.com' }, result: 'success', error: null },
      { action: { kind: 'READ_UI_STATE' }, result: 'success', error: null },
      { action: { kind: 'SET_VALUE', text: 'hello' }, result: 'success', error: null }
    ],
    {
      frontmost_app: 'Google Chrome',
      window_title: 'Google',
      surface_type: 'search_home',
      permission_state: { trusted: true },
      visible_elements: [
        { index: 0, role: 'text field', name: 'Search', description: 'Search', value: 'hello', enabled: true }
      ],
      interactive_candidates: [
        { index: 0, role: 'text field', name: 'Search', description: 'Search', value: 'hello', enabled: true, group: 'search_field', hint: '1. text field Search' }
      ],
      text_sample: 'Google Search hello'
    }
  );

  assert.strictEqual(submitAction.kind, 'KEY_PRESS');
  assert.strictEqual(submitAction.key, 'enter');
}

function testDesktopPlannerCanChooseVisiblePressTarget() {
  const action = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'OPEN_URL', url: 'https://www.google.com' }, result: 'success', error: null },
      { action: { kind: 'READ_UI_STATE' }, result: 'success', error: null },
      { action: { kind: 'SET_VALUE', text: 'hello' }, result: 'success', error: null },
      { action: { kind: 'KEY_PRESS', key: 'enter' }, result: 'success', error: null }
    ],
    {
      frontmost_app: 'Google Chrome',
      window_title: 'hello - Google Search',
      surface_type: 'search_results',
      permission_state: { trusted: true },
      visible_elements: [
        { index: 0, role: 'link', name: 'Hello World Program', description: 'search result', value: '', enabled: true },
        { index: 1, role: 'link', name: 'Hello Definition & Meaning', description: 'search result', value: '', enabled: true }
      ],
      interactive_candidates: [
        { index: 0, role: 'link', name: 'Hello World Program', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '1. link Hello World Program' },
        { index: 1, role: 'link', name: 'Hello Definition & Meaning', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '2. link Hello Definition & Meaning' }
      ],
      text_sample: 'Search results for hello'
    }
  );

  assert.ok(action);
  assert.ok(['PRESS_AX', 'CLICK_AX'].includes(action.kind));
  assert.strictEqual(action.target_index, 1);
}

function testDesktopPlannerChangesTacticAfterNoEffectClick() {
  const action = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'OPEN_URL', url: 'https://www.google.com' }, result_status: 'success', error: null },
      { action: { kind: 'READ_UI_STATE' }, result_status: 'success', error: null },
      { action: { kind: 'SET_VALUE', text: 'hello' }, result_status: 'success', error: null },
      { action: { kind: 'KEY_PRESS', key: 'enter' }, result_status: 'success', error: null },
      { action: { kind: 'PRESS_AX', target_index: 1 }, result_status: 'success', error: 'click_had_no_effect', effect_summary: 'no visible change after click' }
    ],
    {
      frontmost_app: 'Google Chrome',
      window_title: 'hello - Google Search',
      surface_type: 'search_results',
      permission_state: { trusted: true },
      visible_elements: [
        { index: 0, role: 'link', name: 'Hello World Program', description: 'search result', value: '', enabled: true },
        { index: 1, role: 'link', name: 'Hello Definition & Meaning', description: 'search result', value: '', enabled: true },
        { index: 2, role: 'link', name: 'Hello Kitty Official', description: 'search result', value: '', enabled: true }
      ],
      interactive_candidates: [
        { index: 0, role: 'link', name: 'Hello World Program', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '1. link Hello World Program' },
        { index: 1, role: 'link', name: 'Hello Definition & Meaning', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '2. link Hello Definition & Meaning' },
        { index: 2, role: 'link', name: 'Hello Kitty Official', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '3. link Hello Kitty Official' }
      ],
      text_sample: 'Search results for hello'
    },
    {
      remaining_gap: 'open the 2 result and confirm the destination page changed',
      last_effect_summary: 'no visible change after click',
      recent_failures: [{ action: 'PRESS_AX', stage: 'clicking', error: 'click_had_no_effect', effect_summary: 'no visible change after click' }]
    }
  );

  assert.ok(action);
  assert.ok(['PRESS_AX', 'SCROLL_AX', 'READ_UI_STATE'].includes(action.kind));
  assert.notStrictEqual(action.target_index, 1);
}

function testDesktopPlannerUsesCdpSearchActions() {
  const readAction = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'CDP_NAVIGATE', url: 'https://www.google.com' }, result: 'success', error: null }
    ],
    {
      surface_driver: 'cdp',
      frontmost_app: 'Managed Chrome',
      window_title: 'Google',
      tab_title: 'Google',
      url: 'https://www.google.com',
      surface_type: 'search_home',
      permission_state: { trusted: true },
      visible_elements: [],
      interactive_candidates: [],
      text_sample: 'Google Search'
    }
  );

  assert.strictEqual(readAction.kind, 'CDP_GET_TREE');

  const typeAction = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'CDP_NAVIGATE', url: 'https://www.google.com' }, result: 'success', error: null },
      { action: { kind: 'CDP_GET_TREE' }, result: 'success', error: null }
    ],
    {
      surface_driver: 'cdp',
      frontmost_app: 'Managed Chrome',
      window_title: 'Google',
      tab_title: 'Google',
      url: 'https://www.google.com',
      surface_type: 'search_home',
      permission_state: { trusted: true },
      visible_elements: [],
      interactive_candidates: [
        { index: 0, id: 'cdp-1', role: 'input', name: 'Search', description: 'Search', value: '', enabled: true, group: 'search_field', hint: '1. input Search' }
      ],
      text_sample: 'Google Search'
    }
  );

  assert.strictEqual(typeAction.kind, 'CDP_TYPE');
  assert.strictEqual(typeAction.text, 'hello');

  const clickAction = planNextAction(
    'Search Google for hello and open the second result',
    [
      { action: { kind: 'CDP_NAVIGATE', url: 'https://www.google.com' }, result: 'success', error: null },
      { action: { kind: 'CDP_GET_TREE' }, result: 'success', error: null },
      { action: { kind: 'CDP_TYPE', text: 'hello' }, result: 'success', error: null },
      { action: { kind: 'CDP_KEY_PRESS', key: 'Enter' }, result: 'success', error: null }
    ],
    {
      surface_driver: 'cdp',
      frontmost_app: 'Managed Chrome',
      window_title: 'hello - Google Search',
      tab_title: 'hello - Google Search',
      url: 'https://www.google.com/search?q=hello',
      surface_type: 'search_results',
      permission_state: { trusted: true },
      visible_elements: [],
      interactive_candidates: [
        { index: 0, id: 'cdp-r1', role: 'a', name: 'Hello World Program', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '1. link Hello World Program' },
        { index: 1, id: 'cdp-r2', role: 'a', name: 'Hello Definition & Meaning', description: 'search result', value: '', enabled: true, group: 'result_link', hint: '2. link Hello Definition & Meaning' }
      ],
      text_sample: 'Search results for hello'
    }
  );

  assert.strictEqual(clickAction.kind, 'CDP_CLICK');
  assert.strictEqual(clickAction.target_id, 'cdp-r2');
}

async function testSuggestionEnginePersistsExecutionMetadata() {
  const feedGeneration = require('../services/agent/feed-generation');
  const originalGenerate = feedGeneration.generateFeedSuggestions;
  const originalRunQuery = db.runQuery;
  const writes = [];

  feedGeneration.generateFeedSuggestions = async () => ([
    {
      id: 'sug_1',
      type: 'followup',
      title: 'Draft a follow-up to Alexandra',
      body: 'Send the next step on the Albert School thread.',
      trigger_summary: 'Alexandra asked for the next step.',
      source_node_ids: ['node_1'],
      source_edge_paths: [],
      confidence: 0.88,
      status: 'active',
      category: 'followup',
      priority: 'high',
      intent: 'Close the loop',
      reason: 'The thread is waiting on you.',
      ai_doable: true,
      action_type: 'draft_message',
      execution_mode: 'draft_or_execute',
      target_surface: 'gmail',
      prerequisites: ['Need the current thread open'],
      assignee: 'ai',
      plan: ['Open the thread', 'Draft the follow-up', 'Review and send'],
      step_plan: ['Open the thread', 'Draft the follow-up', 'Review and send'],
      expected_benefit: 'remove the reply-writing step',
      ai_draft: 'Draft the reply',
      action_plan: [{ step: 'Open Gmail', url: 'https://mail.google.com' }],
      created_at: new Date().toISOString()
    }
  ]);

  db.runQuery = async (sql, params = []) => {
    writes.push({ sql, params });
    return true;
  };

  try {
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
    const { runSuggestionEngine } = require('../services/agent/suggestion-engine');
    await runSuggestionEngine(null, { standing_notes: 'Willem prefers fast follow-ups.' });
    const artifactInsert = writes.find((entry) => /INSERT OR REPLACE INTO suggestion_artifacts/i.test(entry.sql));
    assert.ok(artifactInsert, 'expected suggestion_artifacts insert');
    const metadata = JSON.parse(artifactInsert.params[9]);
    assert.strictEqual(metadata.execution_mode, 'draft_or_execute');
    assert.strictEqual(metadata.target_surface, 'gmail');
    assert.deepStrictEqual(metadata.prerequisites, ['Need the current thread open']);
    assert.deepStrictEqual(metadata.step_plan, ['Open the thread', 'Draft the follow-up', 'Review and send']);
    assert.strictEqual(metadata.expected_benefit, 'remove the reply-writing step');
  } finally {
    feedGeneration.generateFeedSuggestions = originalGenerate;
    delete require.cache[require.resolve('../services/agent/suggestion-engine')];
    db.runQuery = originalRunQuery;
  }
}

async function main() {
  await testEpisodeSeedSourceRefs();
  await testThinkingTraceShape();
  testSourceAwareTimestampNormalization();
  testEpisodeAnchorStaysOnFirstDay();
  await testRetrievalThoughtExtractsExactTerms();
  await testRetrievalThoughtDefaultsToSevenDaySummaryWindow();
  await testRetrievalThoughtBuildsCodingAndCommunicationAngles();
  await testSuggestionEnginePersistsExecutionMetadata();
  testNormalizeDesktopGoalForGoogleSearch();
  testDesktopPlannerReadsUiAfterOpen();
  testDesktopPlannerTypesAndSubmitsSearch();
  testDesktopPlannerCanChooseVisiblePressTarget();
  testDesktopPlannerChangesTacticAfterNoEffectClick();
  testDesktopPlannerUsesCdpSearchActions();
  console.log('memory-chat-regressions.test.js passed');
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
