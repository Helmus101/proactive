
import re

def track_balance(filename):
    with open(filename, 'r') as f:
        stack = []
        for line_num, line in enumerate(f, 1):
            # Remove comments and strings
            line = re.sub(r'//.*', '', line)
            line = re.sub(r'/\*.*?\*/', '', line, flags=re.DOTALL)
            # This is still not perfect but better
            line = re.sub(r"'[^']*'", "''", line)
            line = re.sub(r'"[^"]*"', '""', line)
            line = re.sub(r'`[^`]*`', '""', line)
            
            for char in line:
                if char == '{':
                    stack.append(line_num)
                elif char == '}':
                    if stack:
                        stack.pop()
                    else:
                        print(f"Extra closing brace at line {line_num}")
        
        for line_num in stack:
            print(f"Unclosed opening brace from line {line_num}")

track_balance('main.js')
