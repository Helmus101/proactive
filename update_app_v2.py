import sys

file_path = 'renderer/app.js'
with open(file_path, 'r') as f:
    content = f.read()

# 1. Update cacheDom
if "this.contactsList = document.getElementById('contacts-list');" not in content:
    content = content.replace('this.settingsGraphContainer = document.getElementById("settings-graph-container");', 
                              'this.settingsGraphContainer = document.getElementById("settings-graph-container");\n        this.contactsList = document.getElementById(\'contacts-list\');')

if "this.relationshipIntelContainer" not in content:
    content = content.replace("this.contactsList = document.getElementById('contacts-list');",
                              "this.contactsList = document.getElementById('contacts-list');\n        this.relationshipIntelContainer = document.getElementById('relationship-intel-container');\n        this.relationshipIntelContacts = document.getElementById('relationship-intel-contacts');")

# 2. Update renderSuggestions to be async and handle Relationship Intelligence
content = content.replace("renderSuggestions() {", "async renderSuggestions() {")

rel_intel_logic = """
        const showRelIntel = this.currentFilter === 'relationship_intelligence' || this.currentFilter === 'all';
        if (showRelIntel && this.relationshipIntelContainer) {
            try {
                const relIntel = await window.electronAPI.getRelationshipIntelligence();
                if (relIntel && relIntel.contacts && relIntel.contacts.length > 0) {
                    this.relationshipIntelContainer.classList.remove('hidden');
                    this.relationshipIntelContacts.innerHTML = relIntel.contacts.map(contact => `
                        <div class="glass-card contact-mini-card" style="min-width: 200px; padding: 12px; cursor: pointer;">
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <div class="suggestion-icon followup" style="width: 24px; height: 24px; font-size: 14px;">
                                    <span class="material-symbols-outlined">person</span>
                                </div>
                                <div style="font-weight: 600; font-size: 13px;">${this.escapeHtml(contact.name)}</div>
                            </div>
                            <div style="font-size: 11px; color: var(--text-secondary); margin-top: 4px;">
                                ${contact.metadata.interaction_count || 0} interactions
                            </div>
                        </div>
                    `).join('');
                } else {
                    this.relationshipIntelContainer.classList.add('hidden');
                }
            } catch (error) {
                console.error('Failed to load relationship intelligence:', error);
                this.relationshipIntelContainer.classList.add('hidden');
            }
        } else if (this.relationshipIntelContainer) {
            this.relationshipIntelContainer.classList.add('hidden');
        }
"""

content = content.replace("const visibleTodos = this.getVisibleTodos();", "const visibleTodos = this.getVisibleTodos();" + rel_intel_logic)

# Update call sites of renderSuggestions to await it
content = content.replace("this.renderSuggestions();", "await this.renderSuggestions();")

with open(file_path, 'w') as f:
    f.write(content)
