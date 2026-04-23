import sys
import re

app_js_path = '/home/engine/project/renderer/app.js'

with open(app_js_path, 'r') as f:
    content = f.read()

# Methods to add
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
                    this.chatInput.style.height = `${Math.min(this.chatInput.scrollHeight, 120)}px`;
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

new_show_contact_detail = """    showContactDetail(contact) {
        const detailPanel = document.getElementById('contact-detail');
        if (!detailPanel) return;

        const strengthPercent = Math.round((contact.strength || 0) * 100);
        const warmthPercent = Math.round((contact.warmth || 0) * 100);
        const depthPercent = Math.round((contact.depth || 0) * 100);
        const centralityPercent = Math.round((contact.centrality || 0) * 100);

        let suggestedActionsHtml = '<h4>Suggested Actions</h4>';
        
        if (contact.is_overdue_followup) {
            suggestedActionsHtml += `<button class="action-btn" data-action="followup">📧 Send Follow-up</button>`;
        }
        if (contact.is_weak_tie) {
            suggestedActionsHtml += `<button class="action-btn" data-action="reconnect">👋 Reconnect (weak tie)</button>`;
        }
        suggestedActionsHtml += `<button class="action-btn" data-action="share">📎 Share Article</button>`;

        detailPanel.innerHTML = `
            <div class="contact-detail">
                <h3>${this.escapeHtml(contact.name)}</h3>
                ${contact.role || contact.company ? `<div style="color: var(--text-secondary); margin-top: -8px; margin-bottom: 16px;">${this.escapeHtml(contact.role || '')} ${contact.role && contact.company ? 'at' : ''} ${this.escapeHtml(contact.company || '')}</div>` : ''}
                
                ${contact.summary ? `<div class="relationship-summary" style="margin-bottom: 16px; font-style: italic; color: var(--text-primary); border-left: 2px solid var(--accent-blue); padding-left: 8px;">${this.escapeHtml(contact.summary)}</div>` : ''}

                <div style="margin: 16px 0;">
                    <div style="font-size: 12px; color: var(--text-tertiary); margin-bottom: 4px;">Contact Information</div>
                    <div class="contact-info">
                        ${contact.emails && contact.emails.length > 0 ? contact.emails.map(e => `<div>📧 ${this.escapeHtml(e)}</div>`).join('') : '<div style="opacity: 0.5;">No email</div>'}
                        ${contact.phones && contact.phones.length > 0 ? contact.phones.map(p => `<div>📞 ${this.escapeHtml(p)}</div>`).join('') : ''}
                    </div>
                </div>

                <div style="margin: 16px 0;">
                    <div style="font-size: 12px; color: var(--text-tertiary); margin-bottom: 4px;">Relationship Intelligence</div>
                    <div class="relationship-metrics" style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                        <div class="metric-item">
                            <div style="font-size: 11px; color: var(--text-tertiary);">Warmth (Investment)</div>
                            <div style="font-weight: 600;">${warmthPercent}%</div>
                            <div class="strength-bar"><div class="strength-bar-fill" style="width: ${warmthPercent}%"></div></div>
                        </div>
                        <div class="metric-item">
                            <div style="font-size: 11px; color: var(--text-tertiary);">Depth (Significance)</div>
                            <div style="font-weight: 600;">${depthPercent}%</div>
                            <div class="strength-bar"><div class="strength-bar-fill" style="width: ${depthPercent}%"></div></div>
                        </div>
                        <div class="metric-item">
                            <div style="font-size: 11px; color: var(--text-tertiary);">Network Intelligence</div>
                            <div style="font-weight: 600;">${centralityPercent}%</div>
                            <div class="strength-bar"><div class="strength-bar-fill" style="width: ${centralityPercent}%"></div></div>
                        </div>
                        <div class="metric-item">
                            <div style="font-size: 11px; color: var(--text-tertiary);">Overall Strength</div>
                            <div style="font-weight: 600;">${strengthPercent}%</div>
                            <div class="strength-bar"><div class="strength-bar-fill" style="width: ${strengthPercent}%"></div></div>
                        </div>
                    </div>
                </div>

                ${contact.related_contacts && contact.related_contacts.length > 0 ? `
                <div style="margin: 16px 0;">
                    <div style="font-size: 12px; color: var(--text-tertiary); margin-bottom: 4px;">Common Connections (Bridges)</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 6px;">
                        ${contact.related_contacts.map(rc => `
                            <span class="pill-btn bridge-contact" style="font-size: 11px; padding: 2px 8px; cursor: pointer;" data-contact-id="${rc.id}">${this.escapeHtml(rc.display_name)}</span>
                        `).join('')}
                    </div>
                </div>
                ` : ''}

                ${contact.interests && contact.interests.length > 0 ? `
                <div style="margin: 16px 0;">
                    <div style="font-size: 12px; color: var(--text-tertiary); margin-bottom: 4px;">Shared Interests</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 6px;">
                        ${contact.interests.slice(0, 6).map(interest => `
                            <span style="background: var(--accent-blue-subtle); color: var(--accent-blue-strong); padding: 4px 8px; border-radius: 4px; font-size: 12px;">
                                ${this.escapeHtml(interest)}
                            </span>
                        `).join('')}
                    </div>
                </div>
                ` : ''}

                <div class="suggested-actions">
                    ${suggestedActionsHtml}
                </div>
            </div>
        `;

        // Add event listeners to bridge contacts
        detailPanel.querySelectorAll('.bridge-contact').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = btn.dataset.contactId;
                this.loadContactDetailById(id);
            });
        });

        // Add event listeners to suggested actions
        detailPanel.querySelectorAll('.action-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const action = btn.dataset.action;
                if (action === 'followup' || action === 'reconnect') {
                    this.startRelationshipDraft(contact.id);
                } else if (action === 'share') {
                    this.showToast('Article sharing coming soon');
                }
            });
        });
    }
"""

# Replace showContactDetail
content = re.sub(r'^\s*showContactDetail\(contact\) \{.*?^\s*\}' , new_show_contact_detail, content, flags=re.DOTALL | re.MULTILINE)

# Replace renderContactsList handlers
old_handlers = re.escape("""        // Add click handlers
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
        }""")

new_handlers = """        // Add click handlers
        document.querySelectorAll('.contact-item').forEach((item) => {
            item.addEventListener('click', () => {
                const id = item.dataset.contactId;
                this.loadContactDetailById(id);
            });
        });

        // Show first contact by default
        if (sorted.length > 0) {
            this.loadContactDetailById(sorted[0].id);
        }"""

content = re.sub(old_handlers, new_handlers, content)

# Add new methods before setupContactsView
if 'async setupContactsView() {' in content:
    content = content.replace('async setupContactsView() {', new_methods + '\n    async setupContactsView() {')

with open(app_js_path, 'w') as f:
    f.write(content)
print("Updated app.js successfully")
