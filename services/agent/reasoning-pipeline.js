/**
 * Deliberate 6-Stage Reasoning Pipeline
 *
 * Implements a fast, credit-efficient reasoning stack for chat:
 * 1. Thinking: Strategic plan, routing, and query decomposition.
 * 2. Initial Search: Parallel vector search for memory/web seeds.
 * 3. Node Expansion: Graph traversal and context enrichment.
 * 4. Judge: Evaluate context sufficiency.
 * 5. Reflect: Self-correction, gap detection, and contradiction check.
 * 6. Synthesis: Final conversational answer generation.
 */

const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');
const { buildRetrievalThought, widenTemporalWindow } = require('./retrieval-thought-system');
const db = require('../db');

// Lazy require to avoid circular dependency
function getIntelligence() {
  return require('./intelligence-engine');
}

class ReasoningPipeline {
  constructor() {
    this.stages = [
      'thinking',
      'initialSearch',
      'nodeExpansion',
      'judge',
      'reflect',
      'synthesis'
    ];
  }

  async run(context, onStep) {
    const emit = (data) => {
      if (onStep) {
        onStep(data);
      }
    };

    console.log('[ReasoningPipeline] Starting pipeline for query:', context.query);
    context.stageTrace = [];
    context.allEvidence = [];
    
    const startTime = Date.now();

    for (const stage of this.stages) {
      try {
        const stageStartTime = Date.now();
        console.log(`[ReasoningPipeline] Executing stage: ${stage}`);
        
        const result = await this[stage](context, emit);
        context = { ...context, ...result };
        
        const duration = Date.now() - stageStartTime;
      } catch (error) {
        console.error(`[ReasoningPipeline] Error in stage ${stage}:`, error);
        context.error = error.message;
        if (stage === 'synthesis') throw error; 
      }
    }

    context.totalDuration = Date.now() - startTime;
    return context;
  }

  /**
   * Stage 1: Thinking (Strategic Plan)
   */
  async thinking(context, emit) {
    const { query, options } = context;
    const { callLLM } = getIntelligence();

    const emitStage = (step, status, overrides = {}) => {
      emit({
        step,
        status,
        label: overrides.label || step.replace(/_/g, ' '),
        ...overrides
      });
    };

    emitStage('routing', 'started');
    const { runRouterStage } = require('./chat-engine');
    const { baseThought, retrievalQuery, chatHistory } = await runRouterStage({ query, options });
    
    emitStage('routing', 'completed', {
      detail: `Source mode: ${baseThought.source_mode || baseThought.strategy_mode || 'memory_only'}.`,
      preview_items: [baseThought.source_mode, baseThought.time_scope?.label].filter(Boolean)
    });

    emitStage('planning', 'started');
    let plan = {
      reasoning_plan: ['Analyze query', 'Retrieve context', 'Synthesize answer'],
      refined_queries: baseThought.semantic_queries || [retrievalQuery],
      web_queries: baseThought.web_queries || [],
      evidence_criteria: 'Relevant memory nodes'
    };

    if (options?.apiKey) {
      const prompt = `[System]
You are an expert retrieval planner for a memory-native AI assistant. 
Decompose the user query into a formal execution plan.

[User Query]
${retrievalQuery}

[Router Output]
${JSON.stringify(baseThought, null, 2)}

[Instruction]
Return a strict JSON object:
{
  "reasoning_plan": ["step 1", "step 2"],
  "refined_queries": ["query 1", "query 2"],
  "web_queries": ["query 1"],
  "evidence_criteria": "description"
}
Return only valid JSON.`;

      const llmPlan = await callLLM(prompt, options.apiKey, 0.2, { 
        economy: true, 
        task: 'routing',
        maxTokens: 600
      });
      if (llmPlan) plan = llmPlan;
    }

    emitStage('planning', 'completed', {
      detail: plan ? 'Created a formal execution plan.' : 'Using default plan.',
      preview_items: plan?.reasoning_plan || []
    });

    return {
      baseThought,
      retrievalQuery,
      chatHistory,
      plan,
      routingMode: baseThought.source_mode || 'hybrid',
      memoryQueries: plan?.refined_queries || baseThought.semantic_queries || [retrievalQuery],
      webQueries: plan?.web_queries || baseThought.web_queries || []
    };
  }

  /**
   * Stage 2: Initial Search (Vector Search)
   */
  async initialSearch(context, emit) {
    const { memoryQueries, routingMode, baseThought, options, retrievalQuery } = context;
    if (routingMode === 'web_only') return { seeds: [], retrieved: { nodes: [] } };

    emit({ step: 'retrieving', status: 'started', label: 'Retrieving' });
    
    const { executeParallelRetrieval } = require('./chat-engine');
    const retrieval = await executeParallelRetrieval(retrievalQuery, {
      ...baseThought,
      semantic_queries: memoryQueries
    }, {
      mode: 'chat',
      app: options?.app,
      date_range: baseThought.applied_date_range || options?.date_range,
      retrieval_thought: baseThought,
      passiveOnly: true 
    }, emit);

    return { seeds: retrieval.seed_nodes || [], retrieval, retrieved: { nodes: retrieval.evidence || [] } };
  }

  /**
   * Stage 3: Node Expansion (Graph Traversal)
   */
  async nodeExpansion(context, emit) {
    let { retrieval, retrievalQuery, baseThought, options, routingMode, webQueries } = context;
    
    const { retrievalLooksSparse, executeParallelRetrieval, searchFreeWeb, fetchDrilldownEvidence } = require('./chat-engine');

    if (routingMode !== 'web_only' && retrievalLooksSparse(retrieval)) {
      retrieval = await executeParallelRetrieval(retrievalQuery, baseThought, {
        mode: 'chat',
        app: options?.app,
        date_range: baseThought.applied_date_range || options?.date_range,
        retrieval_thought: baseThought,
        passiveOnly: false
      }, emit);
    }

    let webResults = [];
    const { assessWebSearchNecessity } = require('./chat-engine');
    const webAssessment = assessWebSearchNecessity(context.query, baseThought, retrieval);
    if (webAssessment.shouldSearchWeb || routingMode === 'web_only') {
      const wq = webQueries?.[0] || retrievalQuery;
      emit({ step: 'web_search', status: 'started', label: 'Web search', detail: `Searching the web for: ${wq}` });
      webResults = await searchFreeWeb(wq, 4);
      emit({ step: 'web_search', status: 'completed', counts: { web_results: webResults.length } });
    }

    const drilldownEvidence = (retrieval.drilldown_refs || []).length
      ? await fetchDrilldownEvidence(retrieval.drilldown_refs || [])
      : [];

    return { 
      retrieval, 
      webResults, 
      drilldownEvidence,
      expandedContext: [...(retrieval.evidence || []), ...webResults],
      retrieved: { nodes: [...(retrieval.evidence || []), ...webResults] }
    };
  }

  /**
   * Stage 4: Judge (Evidence Evaluation)
   */
  async judge(context, emit) {
    const { retrievalQuery, expandedContext, options, plan } = context;
    const { runJudgeStage } = require('./chat-engine');

    emit({ step: 'judging', status: 'started', label: 'Judging' });
    const judgment = await runJudgeStage({ 
      query: retrievalQuery, 
      plan, 
      evidence: expandedContext, 
      apiKey: options?.apiKey 
    });
    
    emit({ 
      step: 'judging', 
      status: 'completed', 
      detail: judgment.reason,
      status: judgment.sufficient ? 'completed' : 'retry'
    });

    return { judgment };
  }

  /**
   * Stage 5: Reflect (Self-Correction)
   */
  async reflect(context, emit) {
    const { retrievalQuery, expandedContext, options, judgment } = context;
    const { callLLM } = getIntelligence();

    if (!options?.apiKey) return { reflection: { approved: true, ok: true } };

    emit({ step: 'reflecting', status: 'started', label: 'Reflecting' });
    
    const contextSnippet = expandedContext.slice(0, 15).map(e => String(e.text || e.title || '').slice(0, 200)).join('\n');

    const prompt = `[System]
You are a critical reflector. Analyze the retrieved evidence and query.
Identify any contradictions or missing links in the context that might lead to a wrong answer.

[User Query]
${retrievalQuery}

[Evidence]
${contextSnippet}

[Instruction]
Return strict JSON:
{
  "approved": boolean,
  "critique": "detailed feedback",
  "reason": "summary"
}
Return only valid JSON.`;

    const reflection = await callLLM(prompt, options.apiKey, 0.1, {
      economy: true,
      task: 'routing',
      maxTokens: 500
    });

    emit({ step: 'reflecting', status: 'completed', detail: reflection?.reason || 'Self-reflection complete.' });
    
    const res = reflection || { approved: true };
    return { reflection: { ...res, ok: res.approved } };
  }

  /**
   * Stage 6: Synthesis (Final Answer)
   */
  async synthesis(context, emit) {
    const { query, retrieval, chatHistory, options, drilldownEvidence, webResults, reflection } = context;
    const { runSynthesizerStage, runReflectorStage, buildGroundedFallbackAnswer } = require('./chat-engine');

    emit({ step: 'synthesis', status: 'started', label: 'Synthesis' });

    const standingNotes = options?.standing_notes || options?.core_memory || '';
    
    let content = null;
    if (options?.apiKey) {
      content = await runSynthesizerStage({
        query,
        retrieval,
        chatHistory,
        standingNotes,
        drilldownEvidence,
        webResults,
        apiKey: options?.apiKey,
        reflectorFeedback: reflection?.approved ? null : reflection
      });
    } else {
      content = buildGroundedFallbackAnswer(query, retrieval, drilldownEvidence);
    }

    const finalReflection = options?.apiKey ? await runReflectorStage({
      query,
      evidence: [...(retrieval.evidence || []), ...webResults],
      answer: content,
      apiKey: options?.apiKey
    }) : { approved: true, ok: true };

    emit({ step: 'synthesis', status: 'completed' });

    const synthesized = {
      answer: content,
      evidence: (retrieval.evidence || []).slice(0, 3),
      confidence: 0.8
    };

    return { content, reflection: finalReflection, synthesized };
  }
}

module.exports = { ReasoningPipeline };
