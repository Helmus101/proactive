
def find_balance_point(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    # Very crude removal of strings and comments
    content = re_sub_all(content)
    
    balance = 0
    for i, char in enumerate(content):
        if char == '{':
            balance += 1
        elif char == '}':
            balance -= 1
        
    print(f"Final balance: {balance}")

def re_sub_all(content):
    import re
    content = re.sub(r"'[^']*'", "''", content)
    content = re.sub(r'"[^"]*"', '""', content)
    content = re.sub(r'//.*', '', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    return content

import re
find_balance_point('main.js')
