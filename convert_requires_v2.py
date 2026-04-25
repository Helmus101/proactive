
import re

file_path = 'main.js'
with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
# Variables we already moved to the top
moved_vars = ['let mainWindow;', 'let voiceHudWindow = null;', 'const appInteractionState =']

for line in lines:
    stripped = line.strip()
    
    # Skip original declarations of variables we moved to the top
    # But only if it's NOT the ones we just added (lines 50-58)
    # Actually, it's safer to just let them be shadowed or find their exact line and remove them later.
    
    # Handle destructured requires
    match = re.match(r'^const\s+\{(.*?)\}\s+=\s+require\((.*?)\);', stripped)
    if match:
        funcs = [f.strip() for f in match.group(1).split(',')]
        module = match.group(2)
        # Convert if it's a local service or heavy module
        if './' in module or 'child_process' in module or 'googleapis' in module or 'puppeteer-core' in module:
            new_line = ""
            for f in funcs:
                new_line += f"const {f} = (...args) => lazyRequire({module}).{f}(...args);\n"
            new_lines.append(new_line)
            continue
    
    # Handle single requires
    match = re.match(r'^const\s+(.*?)\s+=\s+require\((.*?)\);', stripped)
    if match:
        var_name = match.group(1)
        module = match.group(2)
        if './' in module or any(m in module for m in ['axios', 'electron-store', 'express', 'form-data', 'sqlite3']):
            new_lines.append(f"const {var_name} = lazyRequire({module});\n")
            continue
            
    # Handle the sqlite3 verbose case
    if "require('sqlite3').verbose()" in line:
        line = line.replace("require('sqlite3')", "lazyRequire('sqlite3')").replace("require('sqlite3').verbose()", "lazyRequire('sqlite3').verbose()")
        new_lines.append(line)
        continue

    new_lines.append(line)

with open(file_path + '.new', 'w') as f:
    f.writelines(new_lines)
