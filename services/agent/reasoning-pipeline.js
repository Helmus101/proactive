/**
 * Deliberate Reasoning Pipeline
 *
 * Implements a staged reasoning stack for proactive suggestions:
 * 1. Router: Decide query type (memory-first, web-first, hybrid)
 * 2. Planner: Decompose query into subproblems and retrieval plans
 * 3. Retriever: Search memory/web and expand via edges
 * 4. Judge: Score relevance, confidence, completeness
 * 5. Synthesizer: Produce final answer/suggestion with evidence
 * 6. Reflector: Check for gaps, contradictions, revise if needed
 */

const { buildRetrievalThought } = require('./retrieval-thought-system');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');

// Lazy require to avoid circular dependency
function callLLM() {
  return require('./intelligence-engine').callLLM;
}

class ReasoningPipeline {
  constructor() {
    this.stages = ['router', 'planner', 'retriever', 'judge', 'synthesizer', 'reflector'];
  }

  async run(context) {
    console.log('[ReasoningPipeline] Starting pipeline for context:', context.query || context.mode);

    for (const stage of this.stages) {
      try {
        console.log(`[ReasoningPipeline] Executing stage: ${stage}`);
        context = await this[stage](context);
        if (context.error) {
          console.warn(`[ReasoningPipeline] Stage ${stage} failed:`, context.error);
          break; // Skip to reflector or end
        }
      } catch (error) {
        console.error(`[ReasoningPipeline] Error in stage ${stage}:`, error);
        context.error = error.message;
        break;
      }
    }

    console.log('[ReasoningPipeline] Pipeline complete');
    return context;
  }

  /**
   * Router: Decide whether query is memory-first, web-first, or hybrid
   */
  async router(context) {
    const { query, mode, candidate } = context;

    // Simple heuristic: if query mentions "recent" or personal terms, memory-first
    // If "search" or external info, web-first
    // Else hybrid
    const queryLower = (query || '').toLowerCase();
    let routingMode = 'hybrid';

    if (queryLower.includes('my') || queryLower.includes('i ') || queryLower.includes('recent') || mode === 'proactive') {
      routingMode = 'memory-first';
    } else if (queryLower.includes('search') || queryLower.includes('find online') || queryLower.includes('web')) {
      routingMode = 'web-first';
    }

    context.routingMode = routingMode;
    console.log(`[Router] Decided mode: ${routingMode}`);
    return context;
  }

  /**
   * Planner: Generate subquestions and decide search queries
   */
  async planner(context) {
    const { query, routingMode } = context;

    // Use retrieval-thought-system to build plan
    const retrievalThought = await buildRetrievalThought({
      query,
      mode: routingMode,
      candidate: context.candidate || {}
    });

    context.plan = {
      subquestions: retrievalThought.queries || [],
      temporalWindows: retrievalThought.temporalWindows || [],
      apps: retrievalThought.apps || [],
      sourceTypes: retrievalThought.sourceTypes || []
    };

    // Fallback: if no subquestions, generate basic ones from query
    if (context.plan.subquestions.length === 0) {
      const words = query.toLowerCase().split(/\s+/).filter(w => w.length > 3);
      context.plan.subquestions = words.slice(0, 3).map(w => `${query} related to ${w}`);
    }

    console.log(`[Planner] Generated plan with ${context.plan.subquestions.length} subquestions`);
    return context;
  }

  /**
   * Retriever: Search memory/web, expand via edges
   */
  async retriever(context) {
    const { plan, routingMode } = context;

    // For now, focus on memory retrieval
    const retrievalResults = [];

    for (const subquery of plan.subquestions) {
      try {
        const result = await buildHybridGraphRetrieval({
          query: subquery,
          mode: routingMode,
          limit: 10
        });
        retrievalResults.push(...(result.nodes || []));
      } catch (error) {
        console.warn(`[Retriever] Error retrieving for ${subquery}:`, error.message);
        // Continue with empty results
      }
    }

    // Expand edges: get related nodes
    const expanded = [];
    for (const node of retrievalResults) {
      // Simple expansion: get neighbors
      expanded.push(node);
      // TODO: Implement edge expansion
    }

    context.retrieved = {
      nodes: expanded,
      edges: [], // TODO
      webResults: routingMode === 'web-first' ? [] : null // TODO: Integrate web search
    };

    console.log(`[Retriever] Retrieved ${context.retrieved.nodes.length} nodes`);
    return context;
  }

  /**
   * Judge: Score relevance, confidence, completeness
   */
  async judge(context) {
    const { retrieved, query } = context;

    // Simple scoring: relevance based on similarity, confidence on node count
    let totalRelevance = 0;
    let totalConfidence = 0;

    for (const node of retrieved.nodes) {
      // TODO: Implement proper scoring
      totalRelevance += 0.8; // Placeholder
      totalConfidence += node.confidence || 0.5;
    }

    const avgRelevance = retrieved.nodes.length > 0 ? totalRelevance / retrieved.nodes.length : 0;
    const avgConfidence = retrieved.nodes.length > 0 ? totalConfidence / retrieved.nodes.length : 0;
    const completeness = Math.min(retrieved.nodes.length / 5, 1); // Scale with expected results

    context.judgment = {
      relevance: avgRelevance,
      confidence: avgConfidence,
      completeness: completeness,
      sufficient: avgRelevance > 0.7 && completeness > 0.5
    };

    console.log(`[Judge] Scored: relevance=${avgRelevance.toFixed(2)}, confidence=${avgConfidence.toFixed(2)}, sufficient=${context.judgment.sufficient}`);
    return context;
  }

  /**
   * Synthesizer: Produce final answer with evidence and uncertainty
   */
  async synthesizer(context) {
    const { query, retrieved, judgment } = context;

    if (!judgment.sufficient) {
      context.synthesized = {
        answer: 'Insufficient information to provide a confident response.',
        evidence: [],
        confidence: 0.3
      };
      return context;
    }

    // Use LLM to synthesize
    const prompt = `Based on the following retrieved information, answer the query: "${query}"

Retrieved nodes:
${retrieved.nodes.map(n => `- ${n.title || n.summary}`).join('\n')}

Provide a concise answer with evidence and confidence level.`;

    const llmResponse = await callLLM(prompt, { maxTokens: 500 });

    context.synthesized = {
      answer: llmResponse.text || 'Unable to synthesize answer.',
      evidence: retrieved.nodes.slice(0, 3), // Top evidence
      confidence: judgment.confidence,
      trace: {
        goal: query,
        assumptions: ['Retrieved info is relevant'],
        evidence: retrieved.nodes,
        conclusion: llmResponse.text,
        confidence: judgment.confidence
      }
    };

    console.log(`[Synthesizer] Generated answer with confidence ${judgment.confidence.toFixed(2)}`);
    return context;
  }

  /**
   * Reflector: Check for missing links, contradictions, revise
   */
  async reflector(context) {
    const { synthesized, judgment } = context;

    // Check for contradictions or gaps
    const contradictions = []; // TODO: Detect contradictions in evidence
    const missingLinks = judgment.completeness < 0.8 ? ['More context needed'] : [];

    if (contradictions.length > 0 || missingLinks.length > 0) {
      console.log(`[Reflector] Found issues: contradictions=${contradictions.length}, missing=${missingLinks.length}`);
      // Revise: Perhaps re-run retriever with broader query
      context.reflection = {
        contradictions,
        missingLinks,
        revised: true // Flag for future revision
      };
      // For now, just log
    } else {
      context.reflection = { ok: true };
    }

    console.log('[Reflector] Reflection complete');
    return context;
  }
}

module.exports = { ReasoningPipeline };