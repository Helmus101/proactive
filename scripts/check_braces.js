const fs = require('fs');
const s = fs.readFileSync('main.js', 'utf8');
const stack = [];
const pairs = { '{': '}', '(': ')', '[': ']' };
for (let i = 0; i < s.length; i++) {
  const c = s[i];
  if (c === '\'' || c === '"' || c === '`') {
    // skip strings
    const q = c;
    i++;
    while (i < s.length && s[i] !== q) {
      if (s[i] === '\\') i += 2; else i++;
    }
    continue;
  }
  if (s.substr(i, 2) === '//') {
    // skip line comment
    i = s.indexOf('\n', i);
    if (i === -1) break;
    continue;
  }
  if (s.substr(i, 2) === '/*') {
    const j = s.indexOf('*/', i+2);
    if (j === -1) break; i = j+1; continue;
  }
  if ('{(['.includes(c)) stack.push({c, i});
  else if ('})]'.includes(c)) {
    const top = stack.pop();
    if (!top) { console.log('Unmatched closing', c, 'at', i); process.exit(0); }
    const expected = pairs[top.c];
    if (expected !== c) { console.log('Mismatched', top.c, 'at', top.i, 'expected', expected, 'but got', c, 'at', i); process.exit(0); }
  }
}
if (stack.length) {
  console.log('Unclosed tokens count', stack.length);
  console.log('Last 10 unclosed:', stack.slice(-10));
} else console.log('All balanced');
