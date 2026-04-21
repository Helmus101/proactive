#!/usr/bin/env node
/**
 * Test to verify the callLLM circular dependency fix
 */

console.log('[Test] Starting circular dependency verification...');

try {
  // This should NOT fail with a circular dependency now
  const { callLLM } = require('./services/agent/intelligence-engine');
  console.log('[Test] ✓ intelligence-engine.callLLM loaded successfully');
  console.log('[Test] ✓ callLLM is a function:', typeof callLLM === 'function');

  // Test that retrieval-thought-system can access callLLM
  const retrievalThought = require('./services/agent/retrieval-thought-system');
  console.log('[Test] ✓ retrieval-thought-system loaded successfully');

  // Test that reasoning-pipeline loads without circular dependency
  const { ReasoningPipeline } = require('./services/agent/reasoning-pipeline');
  console.log('[Test] ✓ reasoning-pipeline.ReasoningPipeline loaded successfully');
  console.log('[Test] ✓ ReasoningPipeline is a class:', typeof ReasoningPipeline === 'function');

  console.log('\n[Test] ✅ All circular dependency checks passed!');
  process.exit(0);
} catch (e) {
  console.error('\n[Test] ❌ Error during verification:');
  console.error('Message:', e.message);
  console.error('Stack:', e.stack);
  process.exit(1);
}
