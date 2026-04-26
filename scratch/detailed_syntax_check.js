const fs = require('fs');
const vm = require('vm');

const content = fs.readFileSync('main.js', 'utf8');
try {
  new vm.Script(content);
  console.log('Script is valid');
} catch (e) {
  console.log('Error at:', e.stack);
  const lineMatch = e.stack.match(/main.js:(\d+)/);
  if (lineMatch) {
    const lineNum = parseInt(lineMatch[1]);
    const lines = content.split('\n');
    console.log('Context:');
    for (let i = Math.max(0, lineNum - 5); i < Math.min(lines.length, lineNum + 5); i++) {
      console.log(`${i + 1}: ${lines[i]}`);
    }
  }
}
