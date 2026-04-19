import re

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Replace ('episode', 'semantic', 'insight') with ('episode', 'semantic')
# specifically in the context of the search handler.
content = re.sub(
    r"n\.layer\s+IN\s+\('episode',\s+'semantic',\s+'insight'\)",
    "n.layer IN ('episode', 'semantic')",
    content
)

with open(file_path, 'w') as f:
    f.write(content)
