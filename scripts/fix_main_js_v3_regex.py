import re

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Add source_refs to the mapped result from routedEvidence
pattern = r"(entry_mode: routed\?\.strategy\?\.entry_mode \|\| null\s+)\}\),"
replacement = r"\1}),\n              source_refs: ev.source_refs || [],"

content = re.sub(pattern, replacement, content)

with open(file_path, 'w') as f:
    f.write(content)
