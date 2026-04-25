
import re

def check_balance(content):
    # Remove comments and strings
    content = re.sub(r'//.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    content = re.sub(r"'[^']*'", "''", content)
    content = re.sub(r'"[^"]*"', '""', content)
    content = re.sub(r'`[^`]*`', '""', content)
    
    balance = 0
    for char in content:
        if char == '{':
            balance += 1
        elif char == '}':
            balance -= 1
    return balance

with open('main.js', 'r') as f:
    lines = f.readlines()

for i in range(0, 1000, 10):
    content = "".join(lines[:i])
    print(f"Lines 0-{i}: balance {check_balance(content)}")
