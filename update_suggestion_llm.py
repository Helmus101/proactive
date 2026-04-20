import sys

file_path = '/home/engine/project/services/agent/suggestion-engine.js'
with open(file_path, 'r') as f:
    content = f.read()

# Replace 1
old_1 = 'callLLM(prompt, normalizeLLMConfig(configOrApiKey), 0.2, { maxTokens: 500, economy: true })'
new_1 = 'callLLM(prompt, normalizeLLMConfig(configOrApiKey), 0.2, { maxTokens: 500, economy: true, task: "suggestion" })'
content = content.replace(old_1, new_1)

# Replace 2
old_2 = 'callLLM(prompt, normalizeLLMConfig(llmConfigOrKey || process.env.DEEPSEEK_API_KEY || null), 0.2)'
new_2 = 'callLLM(prompt, normalizeLLMConfig(llmConfigOrKey || process.env.DEEPSEEK_API_KEY || null), 0.2, { task: "suggestion" })'
content = content.replace(old_2, new_2)

# Replace 3
old_3 = 'callLLM(phase1Prompt, llmConfig, 0.22, { maxTokens: 450, economy: true })'
new_3 = 'callLLM(phase1Prompt, llmConfig, 0.22, { maxTokens: 450, economy: true, task: "suggestion" })'
content = content.replace(old_3, new_3)

# Replace 4
old_4 = 'callLLM(phase2Prompt, llmConfig, 0.22, { maxTokens: 500, economy: true })'
new_4 = 'callLLM(phase2Prompt, llmConfig, 0.22, { maxTokens: 500, economy: true, task: "suggestion" })'
content = content.replace(old_4, new_4)

# Replace 5
old_5 = 'callLLM(prompt, llmConfig, 0.24, { maxTokens: 550, economy: true })'
new_5 = 'callLLM(prompt, llmConfig, 0.24, { maxTokens: 550, economy: true, task: "suggestion" })'
content = content.replace(old_5, new_5)

with open(file_path, 'w') as f:
    f.write(content)
print("Success")
