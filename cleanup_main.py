
import sys

def remove_ranges(content, ranges):
    lines = content.splitlines()
    to_remove = set()
    for start, end in ranges:
        for i in range(start - 1, end):
            to_remove.add(i)
    
    new_lines = [line for i, line in enumerate(lines) if i not in to_remove]
    return "\n".join(new_lines)

file_path = '/home/engine/project/main.js'
with open(file_path, 'r') as f:
    content = f.read()

# Ranges to remove (1-based, inclusive)
ranges = [
    (383, 433),   # ChatManagement logic
    (435, 461),   # more ChatManagement logic
    (463, 712),   # BrowserHistory logic
    (784, 851),   # RadarState logic
    (992, 1009),  # HeavyJobQueue state/constants
    (1011, 1068), # AppState performance logic
    (1070, 1173), # HeavyJobQueue logic
    (1197, 1205), # markAppInteraction duplicate
    (1207, 1238), # more AppState logic (isAppInteractionHot, updatePerformanceState)
    (1537, 1599), # createWindow duplicate
    (1601, 1632), # createVoiceHudWindow duplicate
    (1634, 1653), # ensureVoiceHudVisible duplicate
    (1663, 1689), # emitPlannerStep duplicate
]

new_content = remove_ranges(content, ranges)

with open(file_path, 'w') as f:
    f.write(new_content)

print("Cleanup completed.")
