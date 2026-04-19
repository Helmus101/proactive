import re

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Update get-event-details to fetch from DB
old_handler = r"""ipcMain\.handle\('get-event-details', \(event, eventId\) => \{
  const historicalSummaries = store\.get\('historicalSummaries'\) \|\| \{\};
  
  // Search through all summaries for this event ID
  for \(const date in historicalSummaries\) \{
    const summary = historicalSummaries\[date\];
    if \(summary\.events\) \{
      const found = summary\.events\.find\(e => e\.id === eventId \|\| e\.source_id === eventId\);
      if \(found\) return found;
    \}
  \}
  return null;
\}\);"""

new_handler = """ipcMain.handle('get-event-details', async (event, eventId) => {
  const db = require('./services/db');
  try {
    const row = await db.getQuery(`SELECT * FROM events WHERE id = ? OR source_ref = ?`, [eventId, eventId]);
    if (row) {
      try {
        row.metadata = JSON.parse(row.metadata || '{}');
      } catch (_) {}
      return row;
    }
  } catch (err) {
    console.error('[get-event-details] Error:', err);
  }

  const historicalSummaries = store.get('historicalSummaries') || {};
  for (const date in historicalSummaries) {
    const summary = historicalSummaries[date];
    if (summary.events) {
      const found = summary.events.find(e => e.id === eventId || e.source_id === eventId);
      if (found) return found;
    }
  }
  return null;
});"""

content = re.sub(old_handler, new_handler, content)

with open(file_path, 'w') as f:
    f.write(content)
