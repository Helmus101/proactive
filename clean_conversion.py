
import re
import os

def clean_conversion():
    if not os.path.exists('appReady.js'):
        print("Missing appReady.js")
        return

    with open('main.js', 'r') as f:
        content = f.read()

    lazy_require_def = """
/**
 * Defer module loading until a property is actually accessed.
 */
function lazyRequire(modulePath) {
  let cachedModule;
  return new Proxy(() => {}, {
    get(target, prop) {
      if (prop === '_isLazyProxy') return true;
      if (!cachedModule) {
        if (typeof startupTrace === 'function') startupTrace(`[Lazy] Loading module: ${modulePath}`);
        cachedModule = require(modulePath);
      }
      return cachedModule[prop];
    },
    apply(target, thisArg, argumentsList) {
      if (!cachedModule) {
        if (typeof startupTrace === 'function') startupTrace(`[Lazy] Loading module (apply): ${modulePath}`);
        cachedModule = require(modulePath);
      }
      return typeof cachedModule === 'function' ? cachedModule.apply(thisArg, argumentsList) : cachedModule;
    }
  });
}
"""

    def replace_require(match):
        funcs_str = match.group(1)
        module = match.group(2)
        m_name = module.strip("'\"")
        
        if m_name == 'electron' or m_name in ['crypto', 'path', 'os', 'fs']:
            return match.group(0) # Keep as is
            
        funcs = [f.strip() for f in re.split(r'[,\s]+', funcs_str) if f.strip()]
        new_lines = []
        for f in funcs:
            new_lines.append(f"const {f} = (...args) => lazyRequire({module}).{f}(...args);")
        return "\n".join(new_lines)

    def replace_single_require(match):
        var_name = match.group(1)
        module = match.group(2)
        m_name = module.strip("'\"")
        if m_name in ['electron', 'crypto', 'path', 'os', 'fs', 'child_process'] or var_name in ['app', 'BrowserWindow']:
            return match.group(0)
        return f"const {var_name} = lazyRequire({module});"

    # 1. Convert requires
    # Multiline destructuring
    content = re.sub(r'const\s+\{([\s\S]*?)\}\s+=\s+require\((.*?)\);', replace_require, content)
    # Single
    content = re.sub(r'const\s+([a-zA-Z0-9_$]+)\s+=\s+require\((.*?)\);', replace_single_require, content)
    # sqlite3 verbose
    content = content.replace("require('sqlite3').verbose()", "lazyRequire('sqlite3').verbose()")
    
    # 2. Extract blocks
    def extract_named_block(content, name):
        match = re.search(r'function ' + name + r'\s*\([\s\S]*?\{', content)
        if not match: return content, None
        start = match.start()
        brace_count = 1
        curr = match.end()
        while brace_count > 0 and curr < len(content):
            if content[curr] == '{': brace_count += 1
            elif content[curr] == '}': brace_count -= 1
            curr += 1
        block = content[start:curr]
        # Remove from content
        new_content = content[:start] + content[curr:]
        return new_content, block

    content, createWindow = extract_named_block(content, 'createWindow')
    content, createVoiceHudWindow = extract_named_block(content, 'createVoiceHudWindow')
    content, markAppInteraction = extract_named_block(content, 'markAppInteraction')
    
    # Extract app.whenReady
    match = re.search(r'app\.whenReady\(\)\.then\([\s\S]*?async\s*\(\)\s*=>\s*\{', content)
    if match:
        start = match.start()
        brace_count = 1
        curr = match.end()
        while brace_count > 0 and curr < len(content):
            if content[curr] == '{': brace_count += 1
            elif content[curr] == '}': brace_count -= 1
            curr += 1
        # appWhenReady = content[start:curr] # we don't use the extracted one, we use robust one
        content = content[:start] + content[curr:]

    # Remove original global declarations if they still exist
    content = content.replace("let mainWindow;", "")
    content = content.replace("let voiceHudWindow = null;", "")
    # Remove appInteractionState declaration
    content = re.sub(r'const appInteractionState = \{[\s\S]*?\};', '', content)
    # Remove store declaration
    content = re.sub(r'const store = new Store\(\);', '', content)

    # 3. Assembly
    lines = content.splitlines()
    top_idx = 0
    for i, line in enumerate(lines):
        if 'const fsPromises =' in line:
            top_idx = i + 1
            break
            
    final_content = "\n".join(lines[:top_idx]) + "\n" + lazy_require_def + "\n"
    final_content += "let mainWindow;\nlet voiceHudWindow = null;\nconst appInteractionState = { focused: false, minimized: false, chatActive: false, lastInteractionAt: 0 };\n"
    final_content += "const store = new Store();\n"
    
    if markAppInteraction: final_content += markAppInteraction + "\n"
    if createWindow: final_content += createWindow + "\n"
    if createVoiceHudWindow: final_content += createVoiceHudWindow + "\n"
    
    with open('appReady.js', 'r') as f:
        final_content += f.read() + "\n"
        
    final_content += "\n".join(lines[top_idx:])
    
    with open('main.js.new', 'w') as f:
        f.write(final_content)

clean_conversion()
print("Clean conversion complete.")
