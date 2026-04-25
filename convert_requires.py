
import re

file_path = 'main.js'
with open(file_path, 'r') as f:
    lines = f.readlines()

new_lines = []
skip_original_declarations = False

for line in lines:
    # Skip original declarations of variables we moved to the top
    if 'let mainWindow;' in line and 'let mainWindow;' != line.strip(): # keep the one we just added at the top
         pass # Actually I added it at line 50. The original was at 716+8=724.
    
    # Identify requires to convert
    match = re.match(r'^const\s+\{(.*?)\}\s+=\s+require\((.*?)\);', line.strip())
    if match:
        funcs = [f.strip() for f in match.group(1).split(',')]
        module = match.group(2)
        if './' in module or module in ["'child_process'"]:
            new_line = ""
            for f in funcs:
                new_line += f"const {f} = (...args) => lazyRequire({module}).{f}(...args);\n"
            new_lines.append(new_line)
            continue
    
    match = re.match(r'^const\s+(.*?)\s+=\s+require\((.*?)\);', line.strip())
    if match:
        var_name = match.group(1)
        module = match.group(2)
        if './' in module or module in ["'axios'", "'electron-store'", "'express'", "'form-data'", "'googleapis'", "'puppeteer-core'"]:
            new_lines.append(f"const {var_name} = lazyRequire({module});\n")
            continue
            
    new_lines.append(line)

with open(file_path + '.new', 'w') as f:
    f.writelines(new_lines)
