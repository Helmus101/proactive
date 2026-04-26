const fs = require('fs');
const content = fs.readFileSync('main.js', 'utf8');
const lines = content.split('\n');
const start = 1407; // 1-indexed line 1408
const end = 1831;   // 1-indexed line 1831

let balance = 0;
for (let i = start; i < end; i++) {
  const line = lines[i];
  const opens = (line.match(/{/g) || []).length;
  const closes = (line.match(/}/g) || []).length;
  balance += opens - closes;
  console.log(`${i + 1}: ${balance} | ${line}`);
}
