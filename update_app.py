import sys

file_path = 'renderer/app.js'
with open(file_path, 'r') as f:
    content = f.read()

# 1. Update cacheDom
old_cache_dom = "this.settingsGraphContainer = document.getElementById(\"settings-graph-container\");"
new_cache_dom = """this.settingsGraphContainer = document.getElementById("settings-graph-container");
        this.contactsList = document.getElementById('contacts-list');"""
content = content.replace(old_cache_dom, new_cache_dom)

# 2. Update switchView
old_switch_view = """if (viewId === "library-view") {
            this.renderLibrary("all");
        }"""
new_switch_view = """if (viewId === "library-view") {
            this.renderLibrary("all");
        }
        if (viewId === "contacts-view") {
            this.renderContacts();
        }"""
content = content.replace(old_switch_view, new_switch_view)

# 3. Update getVisibleTodos to handle relationship_intelligence filter
old_get_visible = """if (this.currentFilter === 'followups') return todo.category === 'followup';"""
new_get_visible = """if (this.currentFilter === 'followups') return todo.category === 'followup' || todo.category === 'relationship';
        if (this.currentFilter === 'relationship_intelligence') return todo.category === 'relationship';"""
content = content.replace(old_get_visible, new_get_visible)

# 4. Update renderSuggestions to add Relationship Intelligence group
old_grouped = """next: visibleTodos.filter((todo) => !todo.suggestion_group || todo.suggestion_group === 'next'),
            risk: visibleTodos.filter((todo) => todo.suggestion_group === 'risk')"""
new_grouped = """next: visibleTodos.filter((todo) => !todo.suggestion_group || todo.suggestion_group === 'next'),
            relationship: visibleTodos.filter((todo) => todo.category === 'relationship'),
            risk: visibleTodos.filter((todo) => todo.suggestion_group === 'risk')"""
content = content.replace(old_grouped, new_grouped)

old_render_call = """renderGroup('Next', 'next'),
            renderGroup('Risk Alerts', 'risk')"""
new_render_call = """renderGroup('Next', 'next'),
            renderGroup('Relationship Intelligence', 'relationship'),
            renderGroup('Risk Alerts', 'risk')"""
content = content.replace(old_render_call, new_render_call)

# 5. Add new methods before the end of WeaveApp class
new_methods = """
    async renderContacts() {
        if (!this.contactsList) return;
        this.contactsList.innerHTML = '<div class="loading-placeholder">Loading contacts...</div>';
        try {
            const contacts = await window.electronAPI.getContacts();
            if (!contacts || !contacts.length) {
                this.contactsList.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-icon"><span class="material-symbols-outlined">contacts</span></div>
                        <div class="empty-text">No contacts discovered yet</div>
                        <div class="empty-sub">We'll automatically build relationship profiles as you interact with people.</div>
                    </div>
                `;
                return;
            }

            this.contactsList.innerHTML = contacts.map(contact => {
                const meta = contact.metadata || {};
                const topics = meta.shared_topics || [];
                const topicsHtml = topics.slice(0, 3).map(t => `<span class="thinking-trace-chip">${this.escapeHtml(t)}</span>`).join('');
                return `
                    <div class="suggestion-card liquid-card contact-card" data-id="${contact.id}">
                        <div class="suggestion-header">
                            <div class="suggestion-icon followup">
                                <span class="material-symbols-outlined">person</span>
                            </div>
                            <div class="suggestion-content">
                                <div class="suggestion-title">${this.escapeHtml(contact.name)}</div>
                                <div class="suggestion-description">${this.escapeHtml(contact.summary || '')}</div>
                                <div class="suggestion-meta" style="margin-top:8px;">
                                    <span>${meta.interaction_count || 0} interactions</span>
                                    ${meta.latest_interaction_at ? `<span>Last seen ${new Date(meta.latest_interaction_at).toLocaleDateString()}</span>` : ''}
                                </div>
                                <div class="thinking-timeline-preview" style="margin-top:8px;">
                                    ${topicsHtml}
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        } catch (error) {
            console.error('Failed to load contacts:', error);
            this.contactsList.innerHTML = '<div class="error-state">Failed to load contacts</div>';
        }
    }
"""

content = content.replace("    triggerSearchAnimation(active) {", new_methods + "\n    triggerSearchAnimation(active) {")

with open(file_path, 'w') as f:
    f.write(content)
