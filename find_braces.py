
import re

def find_unbalanced_braces(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    # Remove strings
    content = re.sub(r"'[^']*'", "''", content)
    content = re.sub(r'"[^"]*"', '""', content)
    content = re.sub(r'`[^`]*`', '""', content)
    # Remove comments
    content = re.sub(r'//.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    stack = []
    for pos, char in enumerate(content):
        if char == '{':
            stack.append(pos)
        elif char == '}':
            if stack:
                stack.pop()
            else:
                print(f"Unmatched closing brace at position {pos}")
    
    for pos in stack:
        print(f"Unclosed opening brace at position {pos}")

find_unbalanced_braces('main.js')
