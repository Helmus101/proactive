    showContactDetail(contact) {
        const detailPanel = document.getElementById('contact-detail');
        if (!detailPanel) return;

        const strengthPercent = Math.round((contact.strength || 0) * 100);
        const warmthPercent = Math.round((contact.warmth || 0) * 100);
        const depthPercent = Math.round((contact.depth || 0) * 100);
        const centralityPercent = Math.round((contact.centrality || 0) * 100);

        let suggestedActionsHtml = '<h4>Suggested Actions</h4>';
        
        if (contact.is_overdue_followup) {
            suggestedActionsHtml += `<button class="action-btn">📧 Send Follow-up</button>`;
        }
        if (contact.is_weak_tie) {
            suggestedActionsHtml += `<button class="action-btn">👋 Reconnect (weak tie)</button>`;
        }
        suggestedActionsHtml += `<button class="action-btn">📎 Share Article</button>`;

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
                            <span class="pill-btn" style="font-size: 11px; padding: 2px 8px;">${this.escapeHtml(rc.display_name)}</span>
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
    }
