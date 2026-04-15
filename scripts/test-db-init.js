const db = require('../services/db');
(async () => {
  try {
    await db.initDB();
    const row = await db.getQuery('SELECT COUNT(*) as count FROM retrieval_docs');
    console.log('retrieval_docs count:', row && row.count);
    const fts = await db.allQuery("SELECT name, type FROM sqlite_master WHERE name LIKE 'retrieval_docs%'");
    console.log('FTS tables:', fts);
  } catch (e) {
    console.error('DB init test failed:', e && e.stack ? e.stack : e);
    process.exit(1);
  }
})();
