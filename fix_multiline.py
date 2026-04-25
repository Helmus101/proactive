
import re

with open('main.js', 'r') as f:
    content = f.read()

def replace_multiline(match):
    funcs_str = match.group(1)
    module = match.group(2)
    funcs = [f.strip() for f in re.split(r'[,\s]+', funcs_str) if f.strip()]
    new_lines = []
    for f in funcs:
        new_lines.append(f"const {f} = (...args) => lazyRequire({module}).{f}(...args);")
    return "\n".join(new_lines)

# Regex for multiline const { ... } = require(...)
pattern = r'const\s+\{([\s\S]*?)\}\s+=\s+require\((.*?)\);'
new_content = re.sub(pattern, replace_multiline, content)

with open('main.js', 'w') as f:
    f.write(new_content)
