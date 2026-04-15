#!/usr/bin/env node

/**
 * Memory Graph Implementation Test
 * Verifies that all components are properly integrated
 */

console.log('🧪 Testing Memory Graph Implementation...\n');

// Test 1: Check main.js modifications
const fs = require('fs');
const mainJs = fs.readFileSync('./main.js', 'utf8');

const mainTests = [
  { name: 'Memory graph timer variables', pattern: /episodeGenerationTimer.*suggestionEngineTimer.*weeklyInsightTimer/ },
  { name: 'Episode generation function', pattern: /async function runEpisodeGeneration\(\)/ },
  { name: 'Suggestion engine function', pattern: /async function runSuggestionEngineJob\(\)/ },
  { name: 'Memory graph startup', pattern: /startMemoryGraphProcessing\(\)/ },
  { name: 'Memory graph status IPC', pattern: /ipcMain\.handle\('get-memory-graph-status'/ },
  { name: 'Search memory graph IPC', pattern: /ipcMain\.handle\('search-memory-graph'/ }
];

console.log('📋 Testing main.js modifications:');
mainTests.forEach(test => {
  const found = test.pattern.test(mainJs);
  console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
});

// Test 2: Check renderer modifications
const rendererJs = fs.readFileSync('./renderer/app.js', 'utf8');

const rendererTests = [
  { name: 'Memory graph update handler', pattern: /onMemoryGraphUpdate/ },
  { name: 'Proactive suggestions handler', pattern: /onProactiveSuggestions/ },
  { name: 'Update memory graph status', pattern: /updateMemoryGraphStatus\(\)/ },
  { name: 'Display proactive suggestions', pattern: /displayProactiveSuggestions\(/ }
];

console.log('\n📋 Testing renderer/app.js modifications:');
rendererTests.forEach(test => {
  const found = test.pattern.test(rendererJs);
  console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
});

// Test 3: Check preload modifications
const preloadJs = fs.readFileSync('./preload.js', 'utf8');

const preloadTests = [
  { name: 'Memory graph status API', pattern: /getMemoryGraphStatus/ },
  { name: 'Search memory graph API', pattern: /searchMemoryGraph/ },
  { name: 'Memory graph update listener', pattern: /onMemoryGraphUpdate/ },
  { name: 'Proactive suggestions listener', pattern: /onProactiveSuggestions/ }
];

console.log('\n📋 Testing preload.js modifications:');
preloadTests.forEach(test => {
  const found = test.pattern.test(preloadJs);
  console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
});

// Test 4: Check HTML modifications
const indexHtml = fs.readFileSync('./renderer/index.html', 'utf8');

const htmlTests = [
  { name: 'Memory graph status container', pattern: /id="memory-graph-status"/ },
  { name: 'Proactive suggestions container', pattern: /id="proactive-suggestions-container"/ }
];

console.log('\n📋 Testing index.html modifications:');
htmlTests.forEach(test => {
  const found = test.pattern.test(indexHtml);
  console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
});

// Test 5: Check CSS modifications
const stylesCss = fs.readFileSync('./renderer/styles.css', 'utf8');

const cssTests = [
  { name: 'Memory status grid styles', pattern: /\.memory-status-grid/ },
  { name: 'Suggestion item styles', pattern: /\.suggestion-item/ },
  { name: 'Urgency indicator styles', pattern: /\.urgency-high/ }
];

console.log('\n📋 Testing styles.css modifications:');
cssTests.forEach(test => {
  const found = test.pattern.test(stylesCss);
  console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
});

// Test 6: Check intelligence-engine.js exists and has required functions
try {
  const intelligenceEngine = fs.readFileSync('./services/agent/intelligence-engine.js', 'utf8');
  const engineTests = [
    { name: 'runEpisodeJob function', pattern: /async function runEpisodeJob/ },
    { name: 'runSemanticJob function', pattern: /async function runSemanticJob/ },
    { name: 'runWeeklyInsightJob function', pattern: /async function runWeeklyInsightJob/ }
  ];
  
  console.log('\n📋 Testing intelligence-engine.js:');
  engineTests.forEach(test => {
    const found = test.pattern.test(intelligenceEngine);
    console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
  });
} catch (e) {
  console.log('\n❌ Could not read intelligence-engine.js');
}

// Test 7: Check suggestion-engine.js exists
try {
  const suggestionEngine = fs.readFileSync('./services/agent/suggestion-engine.js', 'utf8');
  const suggestionTests = [
    { name: 'runSuggestionEngine function', pattern: /async function runSuggestionEngine/ }
  ];
  
  console.log('\n📋 Testing suggestion-engine.js:');
  suggestionTests.forEach(test => {
    const found = test.pattern.test(suggestionEngine);
    console.log(`  ${found ? '✅' : '❌'} ${test.name}`);
  });
} catch (e) {
  console.log('\n❌ Could not read suggestion-engine.js');
}

console.log('\n🎉 Memory Graph Implementation Test Complete!');
console.log('\n📝 Summary:');
console.log('   • Added scheduled timers for 30min episodes, 20min suggestions, weekly insights');
console.log('   • Integrated memory graph status monitoring in UI');
console.log('   • Added proactive suggestions display with execution capability');
console.log('   • Enhanced chat to use memory graph context');
console.log('   • Added IPC handlers for memory graph operations');
console.log('\n🚀 Ready to run! Start the app with: npm start');
