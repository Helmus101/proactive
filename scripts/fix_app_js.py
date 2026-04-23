import re

app_js_path = '/home/engine/project/renderer/app.js'

with open(app_js_path, 'r') as f:
    content = f.read()

new_methods = """
    async loadContactDetailById(id) {
        try {
            const detail = await window.electronAPI.getRelationshipContactDetail(id);
            if (detail) {
                const mapped = {
                    id: detail.id,
                    name: detail.display_name,
                    company: detail.company,
                    role: detail.role,
                    last_contact_at: detail.last_interaction_at,
                    is_overdue_followup: detail.status === 'needs_followup',
                    is_weak_tie: detail.status === 'cooling' || detail.status === 'decaying',
                    strength: detail.strength_score,
                    warmth: detail.warmth_score,
                    depth: detail.depth_score,
                    centrality: detail.network_centrality,
                    summary: detail.relationship_summary,
                    interaction_count: detail.interaction_count_30d,
                    recommendation: detail.metadata?.recommendation || '',
                    emails: detail.metadata?.emails || [],
                    phones: detail.metadata?.phones || [],
                    interests: detail.metadata?.interests || [],
                    related_contacts: detail.related_contacts || []
                };
                this.showContactDetail(mapped);
                
                // Also update active state in list
                document.querySelectorAll('.contact-item').forEach(i => {
                    i.classList.toggle('active', i.dataset.contactId === id);
                });
            }
        } catch (err) {
            console.error('Failed to load contact detail:', err);
        }
    }

    async startRelationshipDraft(contactId, type = 'followup') {
        try {
            this.setPresenceMode('thinking');
            const result = await window.electronAPI.generateRelationshipDraft({ contactId });
            if (result && result.draft) {
                this.switchView('action-view');
                if (this.chatInput) {
                    this.chatInput.value = result.draft;
                    this.chatInput.style.height = 'auto';
                    this.chatInput.style.height = f"{Math.min(this.chatInput.scrollHeight, 120)}px";
                    this.chatInput.focus();
                }
            }
            this.setPresenceMode('waiting');
        } catch (err) {
            console.error('Failed to start relationship draft:', err);
            this.setPresenceMode('waiting');
        }
    }
"""

if 'async setupContactsView() {' in content:
    print("Found setupContactsView")
    content = content.replace('async setupContactsView() {', new_methods + '\n    async setupContactsView() {')
else:
    print("Could not find setupContactsView")

with open(app_js_path, 'w') as f:
    f.write(content)
print("Finished")
