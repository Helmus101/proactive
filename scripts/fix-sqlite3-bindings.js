const fs = require('fs');
const path = require('path');

const pkgRoot = path.resolve(__dirname, '..');
const sqliteDir = path.join(pkgRoot, 'node_modules', 'sqlite3');
const built = path.join(sqliteDir, 'build', 'Release', 'node_sqlite3.node');

function copyIfExists(src, dest) {
  try {
    if (fs.existsSync(src)) {
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(src, dest);
      console.log(`Copied ${src} -> ${dest}`);
      return true;
    }
  } catch (e) {
    console.warn('copyIfExists failed', e.message);
  }
  return false;
}

if (!fs.existsSync(sqliteDir)) {
  console.log('sqlite3 package not found; skipping fix-sqlite3-bindings');
  process.exit(0);
}

const targets = [
  'lib/binding/node-v119-darwin-arm64/node_sqlite3.node',
  'addon-build/default/install-root/node_sqlite3.node'
];

let any = false;
for (const t of targets) {
  const dest = path.join(sqliteDir, t);
  if (copyIfExists(built, dest)) any = true;
}

if (!any) {
  console.log('No build/Release/node_sqlite3.node found to copy.');
}
