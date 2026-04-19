import re

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# 1. Update imagePath to use permanent directory
pattern1 = r'const imagePath = path\.join\(os\.tmpdir\(\), filename\);'
replacement1 = 'const sensorStorageDir = ensureSensorStorageDir();\n  const imagePath = path.join(sensorStorageDir, filename);'
content = re.sub(pattern1, replacement1, content)

# 2. Update event object to include the imagePath
pattern2 = r'imagePath: null,'
replacement2 = 'imagePath: imagePath,'
content = re.sub(pattern2, replacement2, content)

# 3. REMOVE the unlink call
pattern3 = r'fs\.promises\.unlink\(imagePath\)\.catch\(\(\) => \{\}\);'
replacement3 = '// Keep screenshot for better identification'
content = re.sub(pattern3, replacement3, content)

with open(file_path, 'w') as f:
    f.write(content)
