import sys

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

old_code = """    powerMonitor.on('on-ac', () => updatePerformanceState({ onBattery: false }));
    powerMonitor.on('thermal-state-change', (_event, details = {}) => {
      updatePerformanceState({ thermalState: String(details.state || 'unknown') });
    });"""

new_code = """    powerMonitor.on('on-ac', () => updatePerformanceState({ onBattery: false }));
    powerMonitor.on('thermal-state-change', (_event, details = {}) => {
      updatePerformanceState({ thermalState: String(details.state || 'unknown') });
    });
    powerMonitor.on('idle', () => updatePerformanceState());
    powerMonitor.on('active', () => updatePerformanceState());"""

if old_code in content:
    new_content = content.replace(old_code, new_code)
    with open(file_path, 'w') as f:
        f.write(new_content)
    print("Success")
else:
    print("Old code not found")
