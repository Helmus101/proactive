const assert = require('assert');

const db = require('../services/db');
const { scoreGraphFeedSeeds } = require('../services/agent/feed-generation');
const { buildThinkingTrace } = require('../services/agent/chat-engine');
const { normalizeEventEnvelope, buildCanonicalEventMetadata, inferAppId } = require('../services/ingestion');
const { clusterEnvelopes } = require('../services/agent/graph-derivation');
const { buildRetrievalThought } = require('../services/agent/retrieval-thought-system');
const { buildHybridGraphRetrieval } = require('../services/agent/hybrid-graph-retrieval');
const { upsertRetrievalDoc } = require('../services/agent/graph-store');
const { planNextAction, normalizeDesktopGoal } = require('../services/agent/agentPlanner');
const { generateEmbedding } = require('../services/embedding-engine');

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

function testCanonicalRawMetadataContract() {
  const metadata = buildCanonicalEventMetadata({
    id: 'evt_1',
    type: 'EmailThread',
    source: 'Gmail',
    timestampISO: '2026-04-20T10:00:00.000Z',
    date: '2026-04-20',
    metadata: {
      from: 'alex@example.com',
      subject: 'Anqer launch',
      app: 'Gmail'
    },
    entities: ['project:Anqer'],
    participants: ['alex@example.com'],
    topics: ['Anqer launch'],
    sentimentScore: -0.25,
    sessionId: 'sess_1',
    status: 'pending'
  });

  assert.strictEqual(metadata.memory_schema_version, 2);
  assert.strictEqual(metadata.source_app, 'Gmail');
  assert.strictEqual(metadata.app_id, 'com.google.gmail');
  assert.strictEqual(metadata.data_source, 'email_api');
  assert.strictEqual(metadata.context_title, 'Anqer launch');
  assert.ok(metadata.entity_tags.includes('project:Anqer'));
  assert.ok(Array.isArray(metadata.entity_ids) && metadata.entity_ids.length >= 1);
  assert.ok(Array.isArray(metadata.person_ids) && metadata.person_ids.length === 1);
  assert.strictEqual(metadata.session_id, 'sess_1');
  assert.strictEqual(metadata.sentiment_score, -0.25);
  assert.strictEqual(metadata.status, 'pending');
  assert.strictEqual(metadata.relationship_tier, 'network');
  assert.strictEqual(metadata.social_half_life_days, 21);
  assert.ok(metadata.retrieval_breadcrumb.includes('[SOURCE: email_api]'));
  assert.ok(metadata.retrieval_breadcrumb.includes('[APP: Gmail]'));
  assert.ok(metadata.retrieval_breadcrumb.includes('[APP_ID: com.google.gmail]'));
  assert.ok(metadata.retrieval_breadcrumb.includes('[PEOPLE: alex@example.com]'));

  assert.strictEqual(inferAppId('', { app_id: 'Google Chrome' }), 'com.google.chrome');
  assert.strictEqual(inferAppId('', { app_id: 'VS Code' }), 'com.microsoft.vscode');
}

async function testRetrievalDocsStoreSearchableBreadcrumbs() {
  const originalRunQuery = db.runQuery;
  const writes = [];
  db.runQuery = async (sql, params = []) => {
    writes.push({ sql, params });
    return true;
  };

  try {
    await upsertRetrievalDoc({
      docId: 'event:evt_1',
      sourceType: 'event',
      eventId: 'evt_1',
      app: 'Gmail',
      timestamp: '2026-04-20T10:00:00.000Z',
      text: 'Fixed launch follow-up with Alex.',
      metadata: {
        source_app: 'Gmail',
        context_title: 'Anqer launch',
        person_labels: ['alex@example.com'],
        content_type: 'email',
        activity_type: 'viewing'
      }
    });

    const insert = writes.find((entry) => /INSERT OR REPLACE INTO retrieval_docs/i.test(entry.sql));
    assert.ok(insert, 'expected retrieval_docs insert');
    assert.ok(String(insert.params[6]).includes('[APP: Gmail]'));
    assert.ok(String(insert.params[6]).includes('[CONTEXT: Anqer launch]'));
    const metadata = JSON.parse(insert.params[7]);
    assert.strictEqual(metadata.retrieval_breadcrumb.includes('[PEOPLE: alex@example.com]'), true);
    assert.strictEqual(metadata.source_app, 'Gmail');
  } finally {
    db.runQuery = originalRunQuery;
  }
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
  assert.ok(thought.metadata_filters?.app_id?.includes('com.google.gmail'));
  assert.ok(thought.metadata_filters?.entity_tags?.some((item) => /alexandra@albertschool\.com/i.test(item)));
  assert.ok(Array.isArray(thought.hypothetical_documents) && thought.hypothetical_documents.length === 1);
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
  assert.ok(thought.reasoning.some((line) => /metadata prefilter/i.test(line)));
}

async function testRetrievalThoughtRoutesMemoryWebAndHybrid() {
  const memory = await buildRetrievalThought({
    query: 'What did I work on with Alex last week?'
  });
  const web = await buildRetrievalThought({
    query: 'What is the latest news on OpenAI today?'
  });
  const hybrid = await buildRetrievalThought({
    query: 'Use my notes and the latest study techniques to help me study algebra.'
  });
  const bio = await buildRetrievalThought({
    query: 'write a bio about me'
  });

  assert.strictEqual(memory.source_mode, 'memory_only');
  assert.strictEqual(web.source_mode, 'web_only');
  assert.strictEqual(hybrid.source_mode, 'memory_then_web');
  assert.strictEqual(bio.source_mode, 'memory_only');
  assert.ok(typeof hybrid.router_reason === 'string' && hybrid.router_reason.length > 0);
  assert.ok(Array.isArray(hybrid.query_sets?.memory_queries));
  assert.ok(Array.isArray(hybrid.query_sets?.web_queries));
}

async function testRetrievalThoughtBuildsCodingAndCommunicationAngles() {
  const codingThought = await buildRetrievalThought({
    query: 'How is the browser extension bug in manifest.json going?'
  });
  assert.ok(Array.isArray(codingThought.query_debug?.inferred_technical_hints));
  assert.ok(codingThought.query_debug.inferred_technical_hints.length >= 1);
  assert.ok(codingThought.metadata_filters?.content_type?.includes('code'));

  const messageThought = await buildRetrievalThought({
    query: 'Can you find the follow-up email from Sarah about the browser extension?'
  });
  assert.ok(messageThought.semantic_queries.length >= 1);
  assert.ok(messageThought.metadata_filters?.content_type?.includes('email'));

  const activityThought = await buildRetrievalThought({
    query: 'according to memory what trailer was I just watching and what was I working on in VSCode today?'
  });
  assert.strictEqual(activityThought.summary_vs_raw, 'raw');
  assert.ok(activityThought.filters?.source_types?.includes('desktop'));
  assert.ok(activityThought.filters?.app?.some((item) => /chrome/i.test(item)));
  assert.ok(activityThought.filters?.app?.some((item) => /vscode/i.test(item)));
  assert.ok(activityThought.metadata_filters?.data_source?.includes('screenshot_ocr'));
  assert.ok(activityThought.metadata_filters?.data_source?.includes('browser_history'));
  assert.ok(activityThought.metadata_filters?.app_id?.includes('com.google.chrome'));
  assert.ok(activityThought.metadata_filters?.app_id?.includes('com.microsoft.vscode'));

  const contextualTrailerThought = await buildRetrievalThought({
    query: 'what trailer did I watch today\n\nConversation context:\n- what trailer was i watching\n- what is the trailer i was watching'
  });
  const entityTags = contextualTrailerThought.metadata_filters?.entity_tags || [];
  assert.ok(!entityTags.some((item) => /conversation|context/i.test(item)), 'chat scaffolding must not become a hard entity metadata filter');
}

async function testHybridRetrievalPrefersDownwardExpansion() {
  const originalAllQuery = db.allQuery;
  const originalGetQuery = db.getQuery;
  const originalRunQuery = db.runQuery;

  const nodes = {
    core_1: {
      id: 'core_1',
      layer: 'core',
      subtype: 'goal',
      title: 'Study strategy',
      summary: 'Long-term study goal',
      canonical_text: 'Study strategy long-term study goal',
      confidence: 0.95,
      status: 'active',
      source_refs: '[]',
      metadata: JSON.stringify({ anchor_at: '2026-04-10T09:00:00.000Z' }),
      created_at: '2026-04-10T09:00:00.000Z',
      updated_at: '2026-04-10T09:00:00.000Z',
      embedding: '[]',
      anchor_date: '2026-04-10'
    },
    semantic_1: {
      id: 'semantic_1',
      layer: 'semantic',
      subtype: 'task',
      title: 'Review algebra',
      summary: 'Weak algebra concepts',
      canonical_text: 'Review algebra weak concepts',
      confidence: 0.9,
      status: 'active',
      source_refs: '[]',
      metadata: JSON.stringify({ anchor_at: '2026-04-11T09:00:00.000Z' }),
      created_at: '2026-04-11T09:00:00.000Z',
      updated_at: '2026-04-11T09:00:00.000Z',
      embedding: '[]',
      anchor_date: '2026-04-11'
    },
    episode_1: {
      id: 'episode_1',
      layer: 'episode',
      subtype: 'study',
      title: 'Algebra practice session',
      summary: 'Recent quiz misses',
      canonical_text: 'Algebra practice session recent quiz misses',
      confidence: 0.86,
      status: 'active',
      source_refs: JSON.stringify(['evt_1']),
      metadata: JSON.stringify({ anchor_at: '2026-04-12T09:00:00.000Z' }),
      created_at: '2026-04-12T09:00:00.000Z',
      updated_at: '2026-04-12T09:00:00.000Z',
      embedding: '[]',
      anchor_date: '2026-04-12'
    },
    insight_1: {
      id: 'insight_1',
      layer: 'insight',
      subtype: 'pattern',
      title: 'Higher-layer pattern',
      summary: 'Should not be traversed upward from lower nodes',
      canonical_text: 'pattern',
      confidence: 0.8,
      status: 'active',
      source_refs: '[]',
      metadata: JSON.stringify({ anchor_at: '2026-04-13T09:00:00.000Z' }),
      created_at: '2026-04-13T09:00:00.000Z',
      updated_at: '2026-04-13T09:00:00.000Z',
      embedding: '[]',
      anchor_date: '2026-04-13'
    }
  };

  db.allQuery = async (sql) => {
    if (/FROM memory_nodes/i.test(sql)) return Object.values(nodes);
    if (/FROM memory_edges/i.test(sql)) {
      return [
        { from_node_id: 'core_1', to_node_id: 'semantic_1', edge_type: 'ABSTRACTED_TO', weight: 1, evidence_count: 2, trace_label: 'core->semantic' },
        { from_node_id: 'semantic_1', to_node_id: 'episode_1', edge_type: 'PART_OF_EPISODE', weight: 1, evidence_count: 2, trace_label: 'semantic->episode' },
        { from_node_id: 'episode_1', to_node_id: 'insight_1', edge_type: 'RELATED_TO', weight: 1, evidence_count: 1, trace_label: 'episode->insight' }
      ];
    }
    if (/FROM events/i.test(sql)) {
      return [{
        id: 'evt_1',
        source_type: 'EmailThread',
        occurred_at: '2026-04-12T10:00:00.000Z',
        title: 'Algebra practice follow-up',
        redacted_text: 'You missed quadratic factoring on questions 3 and 4.',
        raw_text: 'You missed quadratic factoring on questions 3 and 4.',
        app: 'Gmail',
        source_account: 'study@example.com'
      }];
    }
    return [];
  };
  db.getQuery = async (sql, params = []) => {
    if (/SELECT occurred_at FROM events/i.test(sql)) return null;
    if (/WHERE id = 'global_core'/i.test(sql)) return null;
    if (/FROM memory_nodes WHERE id = \?/i.test(sql)) return nodes[params[0]] || null;
    return null;
  };
  db.runQuery = async () => true;

  try {
    const retrieval = await buildHybridGraphRetrieval({
      query: 'study algebra',
      options: {
        retrieval_thought: {
          mode: 'semantic',
          strategy_mode: 'memory_only',
          source_mode: 'memory_only',
          router_reason: 'Personal study history.',
          summary_vs_raw: 'summary',
          semantic_queries: [],
          message_queries: [],
          web_queries: [],
          query_sets: { memory_queries: [], message_queries: [], web_queries: [] },
          filters: {},
          hop_limit: 2,
          seed_limit: 3
        }
      },
      seedLimit: 3,
      hopLimit: 2
    });

    assert.ok(Array.isArray(retrieval.primary_nodes) && retrieval.primary_nodes.length >= 1);
    assert.ok(retrieval.evidence_nodes.some((item) => item.id === 'episode_1'));
    assert.ok(retrieval.evidence_nodes.some((item) => item.id === 'evt_1'));
    assert.ok(retrieval.drilldown_refs.includes('evt_1'));
    assert.ok(retrieval.edge_paths.some((edge) => edge.relation === 'SOURCE_REF' && edge.to === 'evt_1'));
    assert.ok(!retrieval.evidence_nodes.some((item) => item.id === 'insight_1'));
    assert.ok(retrieval.edge_paths.every((edge) => Number(edge.depth || 0) <= 4));
  } finally {
    db.allQuery = originalAllQuery;
    db.getQuery = originalGetQuery;
    db.runQuery = originalRunQuery;
  }
}

async function testHybridRetrievalUsesTextChunkVectorsWithMetadata() {
  const originalAllQuery = db.allQuery;
  const originalGetQuery = db.getQuery;
  const originalRunQuery = db.runQuery;
  const chunkText = '[SOURCE: email_api][APP: Gmail][APP_ID: com.google.gmail][TIME: 2026-04-20T10:00:00][ENTITIES: person:willem, topic:websocket] Willem mentioned websocket manifest permissions in the Anqer extension.';
  const embedding = await generateEmbedding(chunkText, null);

  db.allQuery = async (sql) => {
    if (/FROM memory_nodes/i.test(sql)) return [];
    if (/FROM retrieval_docs_fts/i.test(sql)) return [];
    if (/FROM text_chunks/i.test(sql)) {
      return [{
        id: 'chk_evt_1_0',
        event_id: 'evt_1',
        node_id: null,
        chunk_index: 0,
        text: chunkText,
        embedding: JSON.stringify(embedding),
        timestamp: '2026-04-20T10:00:00.000Z',
        date: '2026-04-20',
        app: 'Gmail',
        data_source: 'raw',
        metadata: JSON.stringify({
          event_type: 'EmailThread',
          source_app: 'Gmail',
          app_id: 'com.google.gmail',
          data_source: 'email_api',
          content_type: 'email',
          entity_tags: ['person:willem', 'topic:websocket'],
          person_labels: ['willem'],
          occurred_at: '2026-04-20T10:00:00.000Z'
        })
      }];
    }
    if (/FROM events/i.test(sql)) return [];
    if (/FROM memory_edges/i.test(sql)) return [];
    return [];
  };
  db.getQuery = async () => null;
  db.runQuery = async () => true;

  try {
    const retrieval = await buildHybridGraphRetrieval({
      query: 'What websocket bug did Willem mention?',
      options: {
        retrieval_thought: {
          mode: 'semantic',
          strategy_mode: 'memory_only',
          source_mode: 'memory_only',
          router_reason: 'Personal memory request.',
          summary_vs_raw: 'raw',
          semantic_queries: [chunkText],
          message_queries: [],
          lexical_terms: ['websocket', 'willem'],
          metadata_filters: {
            app_id: ['com.google.gmail'],
            entity_tags: ['willem']
          },
          filters: {
            source_types: ['communication']
          },
          query_sets: { memory_queries: [chunkText], message_queries: [], web_queries: [] },
          hop_limit: 1,
          seed_limit: 3
        }
      },
      seedLimit: 3,
      hopLimit: 1
    });

    const evidenceText = JSON.stringify(retrieval.evidence || []);
    assert.ok(/websocket manifest permissions/i.test(evidenceText), 'expected text chunk vector evidence to be retrieved');
  } finally {
    db.allQuery = originalAllQuery;
    db.getQuery = originalGetQuery;
    db.runQuery = originalRunQuery;
  }
}

async function testHybridRetrievalMatchesLegacyDisplayNameAppIds() {
  const originalAllQuery = db.allQuery;
  const originalGetQuery = db.getQuery;
  const originalRunQuery = db.runQuery;
  const chunkText = '[SOURCE: screenshot_ocr][APP: Google Chrome][APP_ID: Google Chrome][TIME: 2026-04-21T13:38:28] Project Hail Mary - Official Trailer - YouTube - Audio playing.';
  const embedding = await generateEmbedding(chunkText, null);

  db.allQuery = async (sql) => {
    if (/FROM memory_nodes/i.test(sql)) return [];
    if (/FROM retrieval_docs_fts/i.test(sql)) return [];
    if (/FROM text_chunks/i.test(sql)) {
      return [{
        id: 'chk_evt_trailer_0',
        event_id: 'evt_trailer',
        node_id: null,
        chunk_index: 0,
        text: chunkText,
        embedding: JSON.stringify(embedding),
        timestamp: '2026-04-21T13:38:28.000Z',
        date: '2026-04-21',
        app: 'Google Chrome',
        data_source: 'raw_event',
        metadata: JSON.stringify({
          event_type: 'ScreenCapture',
          source_app: 'Google Chrome',
          app: 'Google Chrome',
          app_id: 'Google Chrome',
          data_source: 'raw_event',
          storage_data_source: 'raw',
          content_type: 'general',
          occurred_at: '2026-04-21T13:38:28.000Z'
        })
      }];
    }
    if (/FROM events/i.test(sql)) return [];
    if (/FROM memory_edges/i.test(sql)) return [];
    return [];
  };
  db.getQuery = async () => null;
  db.runQuery = async () => true;

  try {
    const retrieval = await buildHybridGraphRetrieval({
      query: 'what trailer was I watching?',
      options: {
        retrieval_thought: {
          mode: 'semantic',
          strategy_mode: 'memory_only',
          source_mode: 'memory_only',
          router_reason: 'Personal raw memory request.',
          summary_vs_raw: 'raw',
          semantic_queries: [chunkText],
          message_queries: [],
          lexical_terms: ['trailer', 'youtube'],
          metadata_filters: {
            app_id: ['com.google.chrome'],
            data_source: ['screenshot_ocr', 'raw_event', 'browser_history'],
            content_type: ['browser_page', 'video']
          },
          filters: {
            source_types: ['desktop'],
            app: ['Chrome'],
            prioritize_screen_capture: true
          },
          query_sets: { memory_queries: [chunkText], message_queries: [], web_queries: [] },
          hop_limit: 1,
          seed_limit: 3
        }
      },
      seedLimit: 3,
      hopLimit: 1
    });

    const evidenceText = JSON.stringify(retrieval.evidence || []);
    assert.ok(/Project Hail Mary - Official Trailer/i.test(evidenceText), 'expected legacy Chrome app_id row to survive metadata filters');
    assert.ok((retrieval.evidence || []).length >= 1, 'expected at least one raw evidence item');
    assert.strictEqual(retrieval.diagnostics?.dropped_by_prioritize_screen_capture, 0, 'Chrome OCR rows must not be dropped as browser history');
    assert.strictEqual(retrieval.diagnostics?.dropped_by_content_type, 0, 'general OCR rows must not be dropped by browser/video content filter');
  } finally {
    db.allQuery = originalAllQuery;
    db.getQuery = originalGetQuery;
    db.runQuery = originalRunQuery;
  }
}

async function testAnswerChatQueryEmitsStructuredPipelineStages() {
  const originalAllQuery = db.allQuery;
  const ingestion = require('../services/ingestion');
  const retrievalThoughtSystem = require('../services/agent/retrieval-thought-system');
  const hybrid = require('../services/agent/hybrid-graph-retrieval');
  const originalIngest = ingestion.ingestRawEvent;
  const originalThought = retrievalThoughtSystem.buildRetrievalThought;
  const originalHybrid = hybrid.buildHybridGraphRetrieval;

  ingestion.ingestRawEvent = async () => true;
  retrievalThoughtSystem.buildRetrievalThought = async () => ({
    mode: 'semantic',
    source_mode: 'memory_only',
    strategy_mode: 'memory_only',
    router_reason: 'Personal memory request.',
    summary_vs_raw: 'summary',
    time_scope: { label: 'all_time' },
    query_sets: {
      memory_queries: ['alex project'],
      message_queries: [],
      web_queries: ['alex project']
    },
    semantic_queries: ['alex project'],
    message_queries: [],
    web_queries: ['alex project'],
    lexical_terms: ['alex']
  });
  const hybridCalls = [];
  hybrid.buildHybridGraphRetrieval = async (args = {}) => {
    hybridCalls.push(args);
    return ({
    retrieval_plan: {
      mode: 'semantic',
      source_mode: 'memory_only',
      strategy_mode: 'memory_only',
      summary_vs_raw: 'summary',
      time_scope: { label: 'all_time' },
      query_sets: {
        memory_queries: ['alex project'],
        message_queries: [],
        web_queries: ['alex project']
      },
      semantic_queries: ['alex project'],
      message_queries: [],
      web_queries: ['alex project']
    },
    router: {
      source_mode: 'memory_only',
      router_reason: 'Personal memory request.',
      time_scope: { label: 'all_time' },
      summary_vs_raw: 'summary'
    },
    query_sets: {
      memory_queries: ['alex project'],
      message_queries: [],
      web_queries: ['alex project']
    },
    generated_queries: {
      semantic: ['alex project'],
      messages: [],
      web: ['alex project'],
      lexical_terms: ['alex']
    },
    seed_results: [{ id: 'node_1', title: 'Alex project', reason: 'semantic:alex project' }],
    seed_nodes: [{ id: 'node_1', title: 'Alex project', reason: 'semantic:alex project' }],
    primary_nodes: [{ id: 'node_1', title: 'Alex project', reason: 'semantic:alex project' }],
    support_nodes: [{ id: 'node_2', title: 'Project notes' }],
    evidence_nodes: [{ id: 'node_3', title: 'Recent work log' }],
    expanded_nodes: [{ id: 'node_2', title: 'Project notes' }, { id: 'node_3', title: 'Recent work log' }],
    graph_expansion_results: [{ id: 'node_2', title: 'Project notes' }, { id: 'node_3', title: 'Recent work log' }],
    edge_paths: [{ from: 'node_1', to: 'node_2', relation: 'RELATED_TO', depth: 1 }],
    packed_context_stats: {
      primary_nodes: 1,
      support_nodes: 1,
      evidence_nodes: 1,
      packed_evidence: 3
    },
    evidence_count: 3,
    evidence: [
      { id: 'node_1', layer: 'semantic', text: 'Alex project status', score: 0.9, source_type: 'EmailThread' },
      { id: 'node_2', layer: 'episode', text: 'Project notes', score: 0.8, source_type: 'EmailThread' },
      { id: 'node_3', layer: 'raw', text: 'Recent work log', score: 0.7, source_type: 'EmailThread' }
    ],
    contextText: 'Alex project status\nProject notes\nRecent work log',
    drilldown_refs: [],
    lazy_source_refs: [],
    temporal_reasoning: []
    });
  };
  db.allQuery = async () => [];

  try {
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    const { answerChatQuery } = require('../services/agent/chat-engine');
    const steps = [];
    const result = await answerChatQuery({
      apiKey: null,
      query: 'What did I do with Alex?',
      options: {
        historical_summaries: {
          '2026-04-20': {
            narrative: 'Daily summary says Alex project happened.',
            top_people: ['Alex'],
            topics: ['project']
          }
        }
      },
      onStep: (event) => steps.push(event)
    });

    const orderedSteps = steps.map((item) => item.step);
    assert.deepStrictEqual(orderedSteps, [
      'routing',
      'query_generation',
      'memory_search',
      'memory_search',
      'seed_selection',
      'edge_expansion',
      'ranking',
      'web_search',
      'web_search',
      'synthesis',
      'synthesis',
      'memory_writeback'
    ]);
    assert.ok(steps.every((item) => typeof item.status === 'string'));
    assert.ok(steps.find((item) => item.step === 'web_search' && item.status === 'completed'));
    assert.ok(Array.isArray(result.retrieval.stage_trace) && result.retrieval.stage_trace.length >= steps.length);
    assert.ok(hybridCalls.length >= 1);
    assert.ok(hybridCalls.every((call) => call.passiveOnly === false), 'chat retrieval should actively search vectors/nodes, not passive summaries first');
    assert.strictEqual(result.retrieval.summary_context_used, false);
    assert.ok(!/Daily Summary Snapshots/i.test(result.retrieval.contextText || ''), 'daily summaries should not be prepended when vector evidence is present');
  } finally {
    ingestion.ingestRawEvent = originalIngest;
    retrievalThoughtSystem.buildRetrievalThought = originalThought;
    hybrid.buildHybridGraphRetrieval = originalHybrid;
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    db.allQuery = originalAllQuery;
  }
}

async function testAnswerChatQueryUsesWebFallbackForSparseWorldKnowledge() {
  const originalAllQuery = db.allQuery;
  const ingestion = require('../services/ingestion');
  const retrievalThoughtSystem = require('../services/agent/retrieval-thought-system');
  const hybrid = require('../services/agent/hybrid-graph-retrieval');
  const originalIngest = ingestion.ingestRawEvent;
  const originalThought = retrievalThoughtSystem.buildRetrievalThought;
  const originalHybrid = hybrid.buildHybridGraphRetrieval;
  const originalFetch = global.fetch;

  ingestion.ingestRawEvent = async () => true;
  retrievalThoughtSystem.buildRetrievalThought = async () => ({
    mode: 'semantic',
    source_mode: 'memory_then_web',
    strategy_mode: 'memory_then_web',
    router_reason: 'Needs memory context plus external corroboration.',
    summary_vs_raw: 'summary',
    time_scope: { label: 'all_time' },
    query_sets: {
      memory_queries: ['AI Day event details'],
      message_queries: [],
      web_queries: ['AI Day event details']
    },
    semantic_queries: ['AI Day event details'],
    message_queries: [],
    web_queries: ['AI Day event details'],
    lexical_terms: ['ai day']
  });
  hybrid.buildHybridGraphRetrieval = async () => ({
    retrieval_plan: {
      mode: 'semantic',
      source_mode: 'memory_then_web',
      strategy_mode: 'memory_then_web',
      summary_vs_raw: 'summary',
      time_scope: { label: 'all_time' },
      query_sets: {
        memory_queries: ['AI Day event details'],
        message_queries: [],
        web_queries: ['AI Day event details']
      },
      semantic_queries: ['AI Day event details'],
      message_queries: [],
      web_queries: ['AI Day event details']
    },
    router: {
      source_mode: 'memory_then_web',
      router_reason: 'Needs memory context plus external corroboration.',
      time_scope: { label: 'all_time' },
      summary_vs_raw: 'summary'
    },
    query_sets: {
      memory_queries: ['AI Day event details'],
      message_queries: [],
      web_queries: ['AI Day event details']
    },
    generated_queries: {
      semantic: ['AI Day event details'],
      messages: [],
      web: ['AI Day event details'],
      lexical_terms: ['ai day']
    },
    seed_results: [],
    seed_nodes: [],
    primary_nodes: [],
    support_nodes: [],
    evidence_nodes: [],
    expanded_nodes: [],
    graph_expansion_results: [],
    edge_paths: [],
    packed_context_stats: {
      primary_nodes: 0,
      support_nodes: 0,
      evidence_nodes: 0,
      packed_evidence: 0
    },
    evidence_count: 0,
    evidence: [],
    contextText: '',
    drilldown_refs: [],
    lazy_source_refs: [],
    temporal_reasoning: []
  });
  global.fetch = async (url) => {
    if (String(url).includes('api.duckduckgo.com')) {
      return {
        ok: true,
        json: async () => ({
          Heading: 'AI Day',
          AbstractURL: 'https://example.com/ai-day',
          AbstractText: 'AI Day is an event about artificial intelligence.',
          RelatedTopics: []
        })
      };
    }
    return {
      ok: true,
      text: async () => ''
    };
  };
  db.allQuery = async () => [];

  try {
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    const { answerChatQuery } = require('../services/agent/chat-engine');
    const steps = [];
    const result = await answerChatQuery({
      apiKey: null,
      query: 'what is ai day',
      onStep: (event) => steps.push(event)
    });

    assert.strictEqual(result.needs_clarification, undefined);
    assert.strictEqual(result.retrieval.web_search_used, true);
    assert.ok(Array.isArray(result.retrieval.web_results) && result.retrieval.web_results.length >= 1);
    assert.ok(steps.some((item) => item.step === 'web_search' && item.status === 'started'));
    assert.ok(steps.some((item) => item.step === 'web_search' && item.status === 'completed'));
  } finally {
    ingestion.ingestRawEvent = originalIngest;
    retrievalThoughtSystem.buildRetrievalThought = originalThought;
    hybrid.buildHybridGraphRetrieval = originalHybrid;
    global.fetch = originalFetch;
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    db.allQuery = originalAllQuery;
  }
}

async function testAnswerChatQueryUsesDrilldownEvidenceBeforeClarifying() {
  const originalAllQuery = db.allQuery;
  const ingestion = require('../services/ingestion');
  const retrievalThoughtSystem = require('../services/agent/retrieval-thought-system');
  const hybrid = require('../services/agent/hybrid-graph-retrieval');
  const originalIngest = ingestion.ingestRawEvent;
  const originalThought = retrievalThoughtSystem.buildRetrievalThought;
  const originalHybrid = hybrid.buildHybridGraphRetrieval;

  ingestion.ingestRawEvent = async () => true;
  retrievalThoughtSystem.buildRetrievalThought = async () => ({
    mode: 'semantic',
    source_mode: 'memory_only',
    strategy_mode: 'memory_only',
    router_reason: 'Personal memory request.',
    summary_vs_raw: 'summary',
    time_scope: { label: 'all_time' },
    query_sets: {
      memory_queries: ['algebra practice'],
      message_queries: [],
      web_queries: []
    },
    semantic_queries: ['algebra practice'],
    message_queries: [],
    web_queries: [],
    lexical_terms: ['algebra']
  });
  hybrid.buildHybridGraphRetrieval = async () => ({
    retrieval_plan: {
      mode: 'semantic',
      source_mode: 'memory_only',
      strategy_mode: 'memory_only',
      summary_vs_raw: 'summary',
      time_scope: { label: 'all_time' }
    },
    router: {
      source_mode: 'memory_only',
      router_reason: 'Personal memory request.',
      time_scope: { label: 'all_time' },
      summary_vs_raw: 'summary'
    },
    query_sets: {
      memory_queries: ['algebra practice'],
      message_queries: [],
      web_queries: []
    },
    generated_queries: {
      semantic: ['algebra practice'],
      messages: [],
      web: [],
      lexical_terms: ['algebra']
    },
    seed_results: [{ id: 'episode_1', title: 'Algebra practice session', reason: 'semantic:algebra practice' }],
    seed_nodes: [{ id: 'episode_1', title: 'Algebra practice session', layer: 'episode', source_refs: ['evt_1'] }],
    primary_nodes: [{ id: 'episode_1', title: 'Algebra practice session', layer: 'episode', source_refs: ['evt_1'] }],
    support_nodes: [],
    evidence_nodes: [{ id: 'episode_1', title: 'Algebra practice session', layer: 'episode', source_refs: ['evt_1'] }],
    expanded_nodes: [],
    graph_expansion_results: [],
    edge_paths: [],
    packed_context_stats: {
      primary_nodes: 1,
      support_nodes: 0,
      evidence_nodes: 1,
      packed_evidence: 1
    },
    evidence_count: 1,
    evidence: [
      { id: 'episode_1', layer: 'episode', text: 'Algebra practice session', score: 0.7, source_refs: ['evt_1'], source_type: 'EmailThread' }
    ],
    contextText: 'Algebra practice session',
    drilldown_refs: ['evt_1'],
    lazy_source_refs: [{ ref: 'evt_1' }],
    temporal_reasoning: []
  });
  db.allQuery = async (sql) => {
    if (/FROM events/i.test(sql)) {
      return [{
        id: 'evt_1',
        source_type: 'EmailThread',
        occurred_at: '2026-04-12T10:00:00.000Z',
        title: 'Algebra practice follow-up',
        redacted_text: 'You missed quadratic factoring on questions 3 and 4.',
        raw_text: 'You missed quadratic factoring on questions 3 and 4.',
        app: 'Gmail',
        source_account: 'study@example.com'
      }];
    }
    return [];
  };

  try {
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    const { answerChatQuery } = require('../services/agent/chat-engine');
    const result = await answerChatQuery({
      apiKey: null,
      query: 'what happened in my algebra practice session?'
    });

    assert.strictEqual(result.needs_clarification, undefined);
    assert.ok(/quadratic factoring/i.test(result.content));
  } finally {
    ingestion.ingestRawEvent = originalIngest;
    retrievalThoughtSystem.buildRetrievalThought = originalThought;
    hybrid.buildHybridGraphRetrieval = originalHybrid;
    delete require.cache[require.resolve('../services/agent/chat-engine')];
    db.allQuery = originalAllQuery;
  }
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
  testCanonicalRawMetadataContract();
  await testRetrievalDocsStoreSearchableBreadcrumbs();
  testEpisodeAnchorStaysOnFirstDay();
  await testRetrievalThoughtExtractsExactTerms();
  await testRetrievalThoughtDefaultsToSevenDaySummaryWindow();
  await testRetrievalThoughtRoutesMemoryWebAndHybrid();
  await testRetrievalThoughtBuildsCodingAndCommunicationAngles();
  await testHybridRetrievalPrefersDownwardExpansion();
  await testHybridRetrievalUsesTextChunkVectorsWithMetadata();
  await testHybridRetrievalMatchesLegacyDisplayNameAppIds();
  await testAnswerChatQueryEmitsStructuredPipelineStages();
  await testAnswerChatQueryUsesWebFallbackForSparseWorldKnowledge();
  await testAnswerChatQueryUsesDrilldownEvidenceBeforeClarifying();
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
