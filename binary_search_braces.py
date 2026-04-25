
import re

def check_balance(content):
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

low = 0
high = len(lines)
while high - low > 1:
    mid = (low + high) // 2
    content = "".join(lines[:mid])
    if check_balance(content) >= 1:
        high = mid
    else:
        low = mid
    print(f"Low: {low}, High: {high}, Balance at mid ({mid}): {check_balance(content)}")

print(f"Potential unclosed brace at line {high}")
