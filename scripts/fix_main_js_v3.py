import re

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Add source_refs to the mapped result from routedEvidence
old_block = """	                entry_mode: routed?.strategy?.entry_mode || null
	              }),
	              updated_at: ev.latest_activity_at || ev.timestamp || new Date().toISOString()
	            };"""

new_block = """	                entry_mode: routed?.strategy?.entry_mode || null
	              }),
	              source_refs: ev.source_refs || [],
	              updated_at: ev.latest_activity_at || ev.timestamp || new Date().toISOString()
	            };"""

content = content.replace(old_block, new_block)

with open(file_path, 'w') as f:
    f.write(content)
