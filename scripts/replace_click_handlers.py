import sys

app_js_path = '/home/engine/project/renderer/app.js'

with open(app_js_path, 'r') as f:
    content = f.read()

old_click_handlers = """        // Add click handlers
        document.querySelectorAll('.contact-item').forEach((item) => {
            item.addEventListener('click', () => {
                document.querySelectorAll('.contact-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                const id = item.dataset.contactId;
                const contact = this.contacts.find(c => c.id === id);
                if (contact) this.showContactDetail(contact);
            });
        });

        // Show first contact by default
        if (sorted.length > 0) {
            this.showContactDetail(sorted[0]);
        }"""

new_click_handlers = """        // Add click handlers
        document.querySelectorAll('.contact-item').forEach((item) => {
            item.addEventListener('click', async () => {
                document.querySelectorAll('.contact-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                const id = item.dataset.contactId;
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
                    }
                } catch (err) {
                    console.error('Failed to load contact detail:', err);
                }
            });
        });

        // Show first contact by default
        if (sorted.length > 0) {
            const firstId = sorted[0].id;
            window.electronAPI.getRelationshipContactDetail(firstId).then(detail => {
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
                }
            });
        }"""

if old_click_handlers in content:
    new_content = content.replace(old_click_handlers, new_click_handlers)
    with open(app_js_path, 'w') as f:
        f.write(new_content)
    print("Successfully replaced click handlers")
else:
    # Try a more flexible search
    print("Could not find exact click handlers, trying flexible search")
    import re
    # Just a placeholder, actually I should probably just use what I have.
    # Let me check the content of app.js around line 962 again.
    sys.exit(1)
