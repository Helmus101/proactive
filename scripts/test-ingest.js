const ingest = require('../services/ingestion');

(async () => {
  try {
    const res = await ingest.ingestRawEvent({ type: 'test_event', timestamp: Date.now(), source: 'unit-test', text: 'This is a test', metadata: { id: 'unit_test_123' } });
    console.log('Ingest OK:', res);
  } catch (e) {
    console.error('Ingest FAIL:', e && e.stack ? e.stack : e);
    process.exit(1);
  }
})();
