const fs = require('fs');
const s = fs.readFileSync('main.js', 'utf8');
const stack = [];
const pairs = { '{': '}', '(': ')', '[': ']' };
const mismatches = [];
for (let i = 0; i < s.length; i++) {
  const c = s[i];
  if (c === '\'' || c === '"' || c === '`') {
    const q = c; i++; while (i < s.length && s[i] !== q) { if (s[i] === '\\') i+=2; else i++; } continue;
  }
  if (s.substr(i,2) === '//') { i = s.indexOf('\n', i); if (i === -1) break; continue; }
  if (s.substr(i,2) === '/*') { const j = s.indexOf('*/', i+2); if (j === -1) break; i = j+1; continue; }
  if ('{(['.includes(c)) stack.push({c, i});
  else if ('})]'.includes(c)) { const top = stack.pop(); if (!top) { mismatches.push({type:'unmatched_closing', c, i}); continue; } const expected = pairs[top.c]; if (expected !== c) { mismatches.push({type:'mismatch', top, found:{c,i}}); /* continue to try to proceed */ } }
}
console.log('Mismatches:', mismatches.length);
if (mismatches.length) console.log(JSON.stringify(mismatches.slice(0,10), null, 2));
console.log('Unclosed tokens count', stack.length);
for (let k = Math.max(0, stack.length-20); k < stack.length; k++) {
  const it = stack[k];
  const lines = s.slice(0, it.i).split('\n');
  const lineNo = lines.length;
  const col = lines[lines.length-1].length+1;
  const ctx = s.split('\n')[lineNo-1] || '';
  console.log(`#${k}: token='${it.c}' at index=${it.i} line=${lineNo} col=${col}`);
  console.log('   context:', ctx.trim().slice(0,200));
}
