const fs = require('fs');
const path = '/home/engine/project/renderer/app.js';
let content = fs.readFileSync(path, 'utf8');

const newCardBody = `        const expanded = this.expandedCards.has(todo.id);
        const suggestionCategory = todo.suggestion_category || todo.category || 'work';
        const isStudy = suggestionCategory === 'study';
        const aiCanAutomate = Boolean(todo.ai_doable && todo.action_type && todo.action_type !== 'manual_next_step');
        const compact = (value, limit = 90) => {
            const text = String(value || '').replace(/\\s+/g, ' ').trim();
            return text.length > limit ? \`\${text.slice(0, Math.max(0, limit - 1)).trim()}…\` : text;
        };
        const riskLabel = todo.risk_level ? \`Risk: \${todo.risk_level}\` : '';
        const bundleSummary = todo.display?.summary || '';
        const primaryAction = todo.primary_action || (Array.isArray(todo.suggested_actions) ? todo.suggested_actions.find((a) => this.isConcreteActionLabel(a?.label)) : null);
        const rawSubtitle = todo.reason || todo.trigger_summary || bundleSummary || todo.why_now || todo.description || '';
        const whyNowCompact = compact(rawSubtitle, 88);
        const evidenceCompact = todo.evidence_line || (Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? compact(\`\${todo.epistemic_trace[0].source || 'Source'}: \${todo.epistemic_trace[0].text || ''}\`, 86)
            : '');
        const receiptSummary = Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? todo.epistemic_trace.slice(0, 1).map((r) => compact(\`\${r.source || 'Source'}: \${r.text || ''}\`, 86)).join(' • ')
            : '';
        const categoryLabel = this.prettyCategory(todo.category);
        const actionLabel = primaryAction?.label || 'Review details';
        const cardTone = this.cardToneClass(todo.category);
        
        return \`
                <article class="suggestion-card conversation-entry \${this.escapeHtml(cardTone)}\${isStudy ? ' suggestion-study' : ''}" data-id="\${this.escapeHtml(todo.id)}" tabindex="0" style="animation-delay:\${index * 40}ms;">
                    <div class="suggestion-header">
                        <div class="suggestion-content">
                            <div class="suggestion-kicker">
                                <span>\${this.escapeHtml(categoryLabel)}</span>
                                <span>\${this.escapeHtml(this.prettyPriority(todo.priority))}</span>
                                \${todo.time_anchor ? \`<span>\${this.escapeHtml(todo.time_anchor)}</span>\` : ''}
                            </div>
                            <div class="suggestion-title">\${this.escapeHtml(todo.title)}</div>
                            \${whyNowCompact ? \`<div class="suggestion-description">\${this.escapeHtml(whyNowCompact)}</div>\` : ''}
                            \${evidenceCompact ? \`<div class="suggestion-context" style="font-style: italic; opacity: 0.8;">\${this.escapeHtml(evidenceCompact)}</div>\` : ''}
                            <div class="suggestion-primary-line" style="font-weight: 500; color: var(--accent-blue-strong);">\${this.escapeHtml(actionLabel)}</div>
                            \${aiCanAutomate ? '<div class="suggestion-context suggestion-subtle-note" style="font-size: 11px; margin-top: 4px;">AI-assisted automation available</div>' : ''}
                            \${!expanded ? '<div class="suggestion-primary-wrap"><button class="presence-primary-action" type="button" data-action="info" style="padding: 6px 12px; font-size: 11px;">View details</button></div>' : ''}
                        </div>
                    </div>
                    \${expanded ? \`
                        <div class="suggestion-details suggestion-details-quick" style="display:block; margin-top: 16px; border-top: 1px solid var(--glass-border); padding-top: 16px;">
                            \${todo.reason ? \`<div class="suggestion-why" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Context</span>\${this.escapeHtml(todo.reason)}</div>\` : ''}
                            \${todo.step_plan.length ? \`<div class="suggestion-steps" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Plan</span>\${todo.step_plan.map((step, stepIndex) => \`<div class="suggestion-step">\${stepIndex + 1}. \${this.escapeHtml(step)}</div>\`).join('')}</div>\` : ''}
                            \${receiptSummary ? \`<div class="suggestion-why" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Source</span>\${this.escapeHtml(receiptSummary)}</div>\` : ''}
                            <div class="reminder-actions" style="opacity:1; margin-top:20px; display: flex; gap: 8px;">
                                \${todo.source === 'morning-brief'
                                    ? '<button class="pill-btn" type="button" data-action="brief-open">Open brief</button><button class="pill-btn" type="button" data-action="brief-archive">Archive</button>'
                                    : \`\${todo.ai_doable ? '<button class="pill-btn pill-btn-primary" type="button" data-action="execute">Execute</button>' : ''}<button class="pill-btn" type="button" data-action="done">Mark done</button><button class="pill-btn" type="button" data-action="snooze">Snooze</button><button class="pill-btn destructive" type="button" data-action="remove">Dismiss</button>\`
                                }
                            </div>
                        </div>
                    \` : ''}
                </article>
            \`;`;

const startPattern = '    renderSuggestionCard(todo, index = 0) {';
const nextMethod = '    renderMorningBriefBanner() {';

const startIdx = content.indexOf(startPattern);
const endIdx = content.indexOf(nextMethod);

if (startIdx !== -1 && endIdx !== -1) {
    const header = content.substring(startIdx, content.indexOf('{', startIdx) + 1);
    content = content.substring(0, startIdx) + header + "\\n" + newCardBody + "\\n    }" + content.substring(endIdx);
    fs.writeFileSync(path, content);
    console.log('Successfully updated renderSuggestionCard');
} else {
    console.log('Could not find renderSuggestionCard block');
}
