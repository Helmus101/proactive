
import re

def find_duplicated_declarations(filename):
    with open(filename, 'r') as f:
        declarations = {}
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            # Look for let/const/var at the beginning of the line
            m = re.match(r'^(?:let|const|var)\s+([a-zA-Z0-9_]+)', line)
            if m:
                name = m.group(1)
                if name in declarations:
                    print(f"Duplicated declaration: {name} at lines {declarations[name]} and {line_num}")
                else:
                    declarations[name] = line_num

find_duplicated_declarations('main.js')
