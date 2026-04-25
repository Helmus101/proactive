
import sys

def move_code():
    with open('main.js', 'r') as f:
        lines = f.readlines()

    def find_block(start_line_pattern, end_line_pattern, start_offset=0):
        start_idx = -1
        for i in range(start_offset, len(lines)):
            if lines[i] is not None and start_line_pattern in lines[i]:
                start_idx = i
                break
        if start_idx == -1: return None, None
        
        # Find end by matching braces or a specific pattern
        if end_line_pattern == '}':
            brace_count = 0
            for i in range(start_idx, len(lines)):
                if lines[i] is None: continue
                brace_count += lines[i].count('{')
                brace_count -= lines[i].count('}')
                if brace_count == 0:
                    return start_idx, i
        elif end_line_pattern == '});':
            brace_count = 0
            for i in range(start_idx, len(lines)):
                if lines[i] is None: continue
                brace_count += lines[i].count('{')
                brace_count -= lines[i].count('}')
                if brace_count == 0 and '});' in lines[i]:
                    return start_idx, i
        else:
            for i in range(start_idx + 1, len(lines)):
                if lines[i] is not None and end_line_pattern in lines[i]:
                    return start_idx, i
        return None, None

    # Identify blocks to extract
    blocks = []
    
    # markAppInteraction
    s, e = find_block('function markAppInteraction', '}')
    if s is not None:
        blocks.append(('markAppInteraction', lines[s:e+1]))
        for i in range(s, e+1): lines[i] = None

    # createWindow
    s, e = find_block('function createWindow()', '}')
    if s is not None:
        blocks.append(('createWindow', lines[s:e+1]))
        for i in range(s, e+1): lines[i] = None

    # createVoiceHudWindow
    s, e = find_block('function createVoiceHudWindow()', '}')
    if s is not None:
        blocks.append(('createVoiceHudWindow', lines[s:e+1]))
        for i in range(s, e+1): lines[i] = None

    # app.whenReady
    s, e = find_block('app.whenReady().then', '});')
    if s is not None:
        ready_block = lines[s:e+1]
        
        # Optimize app.whenReady block
        new_ready_block = []
        new_ready_block.append("app.whenReady().then(async () => {\n")
        new_ready_block.append("  startupTrace('[Startup] app.whenReady callback');\n")
        new_ready_block.append("  createWindow();\n")
        new_ready_block.append("  createVoiceHudWindow();\n")
        new_ready_block.append("  \n")
        new_ready_block.append("  // Defer non-critical initialization\n")
        new_ready_block.append("  setTimeout(() => {\n")
        
        # Add the rest of the lines, skipping what we already added
        for line in ready_block:
            if any(x in line for x in ['app.whenReady', 'startupTrace', 'createWindow()', 'createVoiceHudWindow()']):
                continue
            if line.strip() == '});': continue
            new_ready_block.append("  " + line)
            
        new_ready_block.append("  }, 1);\n")
        new_ready_block.append("});\n")
        
        blocks.append(('appWhenReady', new_ready_block))
        for i in range(s, e+1): lines[i] = None

    # Remove duplicates of globals
    found_top_globals = False
    for i in range(len(lines)):
        if lines[i] is None: continue
        if 'let mainWindow;' in lines[i] or 'let voiceHudWindow = null;' in lines[i] or 'const appInteractionState =' in lines[i]:
            if not found_top_globals:
                # Assuming the first one we find is the one we added at the top
                # Actually, I added them at line 50.
                if i < 100:
                    # Keep this one
                    continue
            
            if 'const appInteractionState =' in lines[i]:
                # find end of object
                curr = i
                brace_count = 0
                while curr < len(lines):
                    if lines[curr] is not None:
                        brace_count += lines[curr].count('{')
                        brace_count -= lines[curr].count('}')
                        lines[curr] = None
                        if brace_count == 0: break
                    curr += 1
            else:
                lines[i] = None

    # Reconstruct lines
    clean_lines = [l for l in lines if l is not None]
    
    # Find insertion point (after top globals)
    insert_idx = 0
    for i in range(len(clean_lines)):
        if 'const appInteractionState =' in clean_lines[i]:
            # find end
            curr = i
            brace_count = 0
            while curr < len(clean_lines):
                brace_count += clean_lines[curr].count('{')
                brace_count -= clean_lines[curr].count('}')
                if brace_count == 0:
                    insert_idx = curr + 1
                    break
                curr += 1
            break
            
    # Insert extracted blocks
    to_insert = []
    for name, content in blocks:
        to_insert.extend(content)
        to_insert.append('\n')
        
    final_lines = clean_lines[:insert_idx] + to_insert + clean_lines[insert_idx:]
    
    with open('main.js.new', 'w') as f:
        f.writelines(final_lines)

move_code()
