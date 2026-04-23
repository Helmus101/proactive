import sys

app_js_path = '/home/engine/project/renderer/app.js'
fragment_path = '/home/engine/project/renderer/contact-detail-fragment.js'

with open(app_js_path, 'r') as f:
    content = f.read()

with open(fragment_path, 'r') as f:
    fragment = f.read()

# Find the start and end of the showContactDetail function
start_marker = 'showContactDetail(contact) {'
# We need to find the matching closing brace. Since the function is well-formatted, 
# we can look for the next function or the end of the block.
# Actually, the original function ends around line 1052.

start_idx = content.find(start_marker)
if start_idx == -1:
    print("Could not find start marker")
    sys.exit(1)

# Find the end of the function. It ends before 'async setupContactsView() {'
end_marker = 'async setupContactsView() {'
end_idx = content.find(end_marker)

if end_idx == -1:
    print("Could not find end marker")
    sys.exit(1)

new_content = content[:start_idx] + fragment + '\n\n    ' + content[end_idx:]

with open(app_js_path, 'w') as f:
    f.write(new_content)
