const fs = require('fs');
const content = fs.readFileSync('main.js', 'utf8');
const lines = content.split('\n');

let balance = 0;
let lastBalance = 0;
for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  // Ignore braces in strings and comments (simple regex)
  const cleanLine = line.replace(/\/\/.*/, '').replace(/\/\*.*?\*\//g, '').replace(/`.*?`/g, '""').replace(/".*?"/g, '""').replace(/'.*?'/g, "''");
  const opens = (cleanLine.match(/{/g) || []).length;
  const closes = (cleanLine.match(/}/g) || []).length;
  balance += opens - closes;
  if (balance < 0) {
    console.log(`Unbalanced at line ${i + 1}: ${balance} | ${line}`);
    balance = 0; // reset to keep searching
  }
}
if (balance !== 0) {
    console.log(`Final balance: ${balance}`);
}
