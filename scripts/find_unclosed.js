const fs = require('fs');
const s = fs.readFileSync('main.js', 'utf8');
const stack = [];
const pairs = { '{': '}', '(': ')', '[': ']' };
for (let i = 0; i < s.length; i++) {
  const c = s[i];
  if (c === '\'' || c === '"' || c === '`') {
    const q = c; i++; while (i < s.length && s[i] !== q) { if (s[i] === '\\') i+=2; else i++; } continue;
  }
  if (s.substr(i,2) === '//') { i = s.indexOf('\n', i); if (i === -1) break; continue; }
  if (s.substr(i,2) === '/*') { const j = s.indexOf('*/', i+2); if (j === -1) break; i = j+1; continue; }
  if ('{(['.includes(c)) stack.push({c, i});
  else if ('})]'.includes(c)) { const top = stack.pop(); if (!top) { console.log('Unmatched closing', c, 'at', i); process.exit(0);} const expected = pairs[top.c]; if (expected !== c) { console.log('Mismatched', top.c, 'at', top.i, 'expected', expected, 'but got', c, 'at', i); process.exit(0); } }
}
if (stack.length) {
  console.log('Unclosed tokens count', stack.length);
  for (let k = Math.max(0, stack.length-10); k < stack.length; k++) {
    const it = stack[k];
    // compute line/col
    const lines = s.slice(0, it.i).split('\n');
    const lineNo = lines.length;
    const col = lines[lines.length-1].length+1;
    const ctx = s.split('\n')[lineNo-1] || '';
    console.log(`#${k}: token='${it.c}' at index=${it.i} line=${lineNo} col=${col}`);
    console.log('   context:', ctx.trim().slice(0,200));
  }
} else console.log('All balanced');
