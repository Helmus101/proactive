import sys

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Change 1: FTS query
old_str1 = """	        results = await db.allQuery(
	          `SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
	           FROM memory_nodes n
	           WHERE n.id IN (${placeholders}) AND n.layer IN ('episode', 'semantic', 'insight')`,"""
new_str1 = """	        results = await db.allQuery(
	          `SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
	           FROM memory_nodes n
	           WHERE n.id IN (${placeholders}) AND n.layer IN ('episode', 'semantic')`,"""

content = content.replace(old_str1, new_str1)

# Change 2: LIKE query
old_str2 = """	    if (!results.length) {
	      let sql = `
	        SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.canonical_text, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
	        FROM memory_nodes n
	        WHERE n.layer IN ('episode', 'semantic', 'insight') AND (n.title LIKE ? OR n.summary LIKE ? OR n.canonical_text LIKE ?)
	      `;"""
new_str2 = """	    if (!results.length) {
	      let sql = `
	        SELECT n.id, n.layer, n.subtype, n.title, n.summary, n.canonical_text, n.metadata, n.anchor_at, n.created_at, n.updated_at, n.source_refs
	        FROM memory_nodes n
	        WHERE n.layer IN ('episode', 'semantic') AND (n.title LIKE ? OR n.summary LIKE ? OR n.canonical_text LIKE ?)
	      `;"""

content = content.replace(old_str2, new_str2)

with open(file_path, 'w') as f:
    f.write(content)
