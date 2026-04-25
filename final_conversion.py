
import re

def final_conversion():
    with open('main.js', 'r') as f:
        lines = f.readlines()

    # Define lazyRequire and some helpers
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

    def extract_block(lines, start_line_idx, end_pattern):
        brace_count = 0
        for i in range(start_line_idx, len(lines)):
            if lines[i] is None: continue
            brace_count += lines[i].count('{')
            brace_count -= lines[i].count('}')
            if brace_count == 0 and end_pattern in lines[i]:
                block = lines[start_line_idx:i+1]
                for j in range(start_line_idx, i+1): lines[j] = None
                return block
        return None

    # Extract functions and blocks
    # Line numbers are 1-based, so subtract 1
    createWindow = extract_block(lines, 1322-1, '}')
    createVoiceHudWindow = extract_block(lines, 1386-1, '}')
    markAppInteraction = extract_block(lines, 982-1, '}')
    appWhenReady = extract_block(lines, 12090-1, '});')

    # Remove original globals
    lines[691-1] = None # let mainWindow;
    lines[693-1] = None # let voiceHudWindow = null;
    extract_block(lines, 771-1, '};') # const appInteractionState
    lines[108-1] = None # const store = new Store();

    # Convert requires to lazy
    for i in range(len(lines)):
        line = lines[i]
        if line is None: continue
        stripped = line.strip()
        
        # Handle destructured requires
        match = re.match(r'^const\s+\{(.*?)\}\s+=\s+require\((.*?)\);', stripped)
        if match:
            funcs = [f.strip() for f in match.group(1).split(',')]
            module = match.group(2)
            if './' in module or 'child_process' in module or 'googleapis' in module or 'puppeteer-core' in module:
                new_line = ""
                for f in funcs:
                    new_line += f"const {f} = (...args) => lazyRequire({module}).{f}(...args);\n"
                lines[i] = new_line
                continue
        
        # Handle single requires
        match = re.match(r'^const\s+(.*?)\s+=\s+require\((.*?)\);', stripped)
        if match:
            var_name = match.group(1)
            module = match.group(2)
            if './' in module or any(m in module for m in ['axios', 'electron-store', 'express', 'form-data', 'sqlite3']):
                lines[i] = f"const {var_name} = lazyRequire({module});\n"
                continue
                
        if "require('sqlite3').verbose()" in line:
            lines[i] = line.replace("require('sqlite3')", "lazyRequire('sqlite3')").replace("require('sqlite3').verbose()", "lazyRequire('sqlite3').verbose()")

    # Optimize appWhenReady
    optimized_appWhenReady = []
    optimized_appWhenReady.append("app.whenReady().then(async () => {\n")
    optimized_appWhenReady.append("  startupTrace('[Startup] app.whenReady callback');\n")
    optimized_appWhenReady.append("  createWindow();\n")
    optimized_appWhenReady.append("  createVoiceHudWindow();\n")
    optimized_appWhenReady.append("  \n")
    optimized_appWhenReady.append("  // Defer non-critical initialization\n")
    optimized_appWhenReady.append("  setTimeout(() => {\n")
    for line in appWhenReady:
        if any(x in line for x in ['app.whenReady', 'startupTrace', 'createWindow()', 'createVoiceHudWindow()']):
            continue
        if line.strip() == '});': continue
        optimized_appWhenReady.append("    " + line)
    optimized_appWhenReady.append("  }, 1);\n")
    optimized_appWhenReady.append("});\n")

    # Assemble new file
    with open('main.js.new', 'w') as f:
        # Top Electron/Node setup
        f.writelines([l for l in lines[:26] if l is not None])
        f.write(lazy_require_def)
        f.write("\nlet mainWindow;\nlet voiceHudWindow = null;\nconst appInteractionState = {\n  focused: false,\n  minimized: false,\n  chatActive: false,\n  lastInteractionAt: 0\n};\n")
        
        # Insert converted requires before Store initialization
        # Many requires are before 108
        f.writelines([l for l in lines[26:108] if l is not None])
        
        f.write("\nconst store = new Store();\n")
        f.writelines([l for l in markAppInteraction if l is not None])
        f.writelines([l for l in createWindow if l is not None])
        f.writelines([l for l in createVoiceHudWindow if l is not None])
        f.writelines(optimized_appWhenReady)
        f.write("\n")
        
        # The rest
        f.writelines([l for l in lines[108:] if l is not None])

final_conversion()
print("Final conversion complete.")
