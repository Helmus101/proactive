'use strict';

class WeaveApp {
    constructor() {
        this.todos = [];
        this.currentFilter = 'all';
        this.activeView = 'today-view';
        this.expandedCards = new Set();
        this.compactMode = localStorage.getItem('compactMode') === 'true';
        this.notificationsEnabled = localStorage.getItem('notificationsEnabled') !== 'false';
        this.soundEnabled = localStorage.getItem('soundEnabled') === 'true';
        this.desktopCaptureEnabled = localStorage.getItem('desktopCaptureEnabled') !== 'false';
        this.lastReasoning = null;
        this.memorySearchResults = [];
        this.morningBriefs = [];
        this.activeBriefId = null;
        this.voiceControlEnabled = true;
        this.voiceSession = null;
        this.voiceRecorder = null;
        this.voiceStream = null;
        this.voiceChunks = [];
        this.voiceRecognition = null;
        this.voiceRecognitionMode = null;
        this.voiceRecognitionStartedAt = 0;
        this.voicePartialTranscript = '';
        this.voiceFinalTranscript = '';
        this.lastPlannerStatusAt = 0;
        this.desktopTimelineEntries = [];
        this.init();
    }

    async init() {
        this.cacheDom();
        this.applyCompactMode();
        this.applyTheme(localStorage.getItem('theme') || 'light');
        this.updateGreeting();
        this.setupNavigation();
        this.setupSuggestions();
        await this.setupChat();
        this.setupSettings();
        this.setupLibrary();
        window.electronAPI.onProactiveSuggestions?.((suggestions) => {
            const normalized = this.normalizeTodos(suggestions || []);
            this.todos = normalized.filter((todo) => !todo.completed);
            window.electronAPI.savePersistentTodos(this.todos).catch(() => {});
            this.renderSuggestions();
        });
        window.electronAPI.onMorningBriefUpdated?.((brief) => {
            if (!brief) return;
            this.morningBriefs = [brief, ...(this.morningBriefs || []).filter((b) => b.id !== brief.id)].slice(0, 60);
            if (!this.activeBriefId) this.activeBriefId = brief.id;
            this.renderSuggestions();
            this.renderMorningBriefList();
            this.showMorningBrief(this.activeBriefId);
        });
        window.electronAPI.onPlannerStep?.((payload) => this.handlePlannerStep(payload));
        window.electronAPI.onMemoryGraphUpdate?.((payload) => this.handleMemoryGraphUpdate(payload));
        window.electronAPI.onVoiceCommandToggle?.((payload) => this.handleVoiceCommandToggle(payload));
        window.electronAPI.onVoiceSessionUpdate?.((payload) => this.handleVoiceSessionUpdate(payload));
        await this.loadInitialData();
        setInterval(() => this.updateChatSyncStatus(), 10000);
    }

    cacheDom() {
        this.views = Array.from(document.querySelectorAll('.view'));
        this.navButtons = Array.from(document.querySelectorAll('.nav-pill'));
        this.filterChips = Array.from(document.querySelectorAll('.context-chip'));
        this.remindersList = document.getElementById('reminders-list');
        this.refreshButton = document.getElementById('refresh-tasks-btn');
        this.chatMessages = document.getElementById('chat-messages');
        this.chatInput = document.getElementById('chat-input');
        this.sendChatButton = document.getElementById('send-chat-btn');
        this.chatHistoryList = document.getElementById('chat-history-list');
        this.chatThreadTitle = document.getElementById('chat-thread-title');
        this.newChatButton = document.getElementById('new-chat-btn');
        this.previewMessage = document.getElementById('preview-message');
        this.googleStatus = document.getElementById('google-status');
        this.historyList = document.getElementById('settings-history-list');
        this.historyMeta = document.getElementById('settings-history-meta');
        this.screenCapturesList = document.getElementById('screen-captures-list');
        this.captureStatusText = document.getElementById('capture-status-text');
        this.desktopTestStatus = document.getElementById('desktop-test-status');
        this.desktopPromptStatus = document.getElementById('desktop-prompt-status');
        this.desktopPromptInput = document.getElementById('desktop-prompt-input');
        this.desktopPromptRunButton = document.getElementById('run-desktop-prompt-btn');
        this.desktopPerceptionMode = document.getElementById('desktop-perception-mode');
        this.desktopTimeline = document.getElementById('desktop-control-timeline');
        this.voiceControlStatus = document.getElementById('voice-control-status');
        this.voiceShortcutValue = document.getElementById('voice-shortcut-value');
        this.voiceControlToggle = document.getElementById('voice-control-toggle');
        this.voiceOverlay = document.getElementById('voice-command-overlay');
        this.voiceOverlayTitle = document.getElementById('voice-overlay-title');
        this.voiceOverlayStatus = document.getElementById('voice-overlay-status');
        this.voiceOverlayTranscript = document.getElementById('voice-overlay-transcript');
        this.voiceOverlayMeta = document.getElementById('voice-overlay-meta');
        this.memoryResults = document.getElementById('memory-explorer-results');
        this.memorySearchInput = document.getElementById('memory-search-input');
        this.memoryFilterType = document.getElementById('memory-filter-type');
        this.morningBriefModal = document.getElementById('morning-brief-modal');
        this.morningBriefList = document.getElementById('morning-brief-list');
        this.morningBriefContent = document.getElementById('morning-brief-content');
        this.manualGenerateSuggestionsButton = document.getElementById('manual-generate-suggestions-btn');
        this.suggestionProviderSelect = document.getElementById('suggestion-llm-provider');
        this.suggestionModelInput = document.getElementById('suggestion-llm-model');
        this.suggestionBaseUrlInput = document.getElementById('suggestion-llm-base-url');
        this.suggestionApiKeyInput = document.getElementById('suggestion-llm-api-key');
        this.suggestionProviderStatus = document.getElementById('suggestion-llm-status');
        this.discoveryOrbitContainer = document.getElementById('discovery-orbit-container');
        this.discoveryOrbitLabel = document.getElementById('discovery-orbit-label');
        this.libraryList = document.getElementById("library-list");
        this.libraryFilters = Array.from(document.querySelectorAll("[data-lib-filter]"));
        this.librarySearchInput = document.getElementById("library-search-input");
        this.librarySearchButton = document.getElementById("library-search-btn");
        this.librarySearchQueries = document.getElementById("library-search-queries");
        this.settingsGraphContainer = document.getElementById("settings-graph-container");
    this.deleteAllDataButton = document.getElementById('delete-all-data-btn');
    }

    async loadInitialData() {
        try {
            this.todos = (await window.electronAPI.getPersistentTodos()) || [];
        } catch (error) {
            console.error('Failed to load suggestions:', error);
            this.todos = [];
        }

        if (!this.getVisibleTodos().length) {
            await this.generateSuggestions({ replace: true, silent: true });
        } else {
            this.renderSuggestions();
        }

        await this.loadGreetingContext();
        await this.loadGoogleStatus();
        await this.loadMorningBriefs();
    }

    async loadGreetingContext() {
        this.userName = localStorage.getItem('displayName') || 'Willem';
        this.roomName = localStorage.getItem('roomName') || 'Focus Room';
        try {
            const userData = await window.electronAPI.getUserData();
            if (userData?.name) this.userName = String(userData.name);
            if (userData?.roomName) this.roomName = String(userData.roomName);
        } catch (_) {}
        this.updateGreeting();
    }

    setupNavigation() {
        this.navButtons.forEach((button) => {
            button.addEventListener('click', () => this.switchView(button.dataset.view));
        });
    }

    switchView(viewId) {
        this.activeView = viewId;
        this.views.forEach((view) => {
            const isActive = view.id === viewId;
            view.classList.toggle('hidden', !isActive);
            view.classList.toggle('active', isActive);
        });

        this.navButtons.forEach((button) => {
            button.classList.toggle('active', button.dataset.view === viewId);
        });

        if (viewId === "library-view") {
            this.renderLibrary("all");
        }
        if (viewId === 'chat-view') {
            this.chatInput?.focus();
            this.renderFullMemoryGraph();
            this.scrollChatToBottom();
        }

        if (viewId === 'settings-view') {
            this.loadSettingsData();
        }
    }

    setupSuggestions() {
        this.filterChips.forEach((chip) => {
            chip.addEventListener('click', () => {
                this.filterChips.forEach((item) => item.classList.remove('active'));
                chip.classList.add('active');
                this.currentFilter = chip.dataset.filter || 'all';
                this.renderSuggestions();
            });
        });

        this.refreshButton?.addEventListener('click', async () => {
            await this.generateSuggestions({ replace: true, silent: false });
        });
        this.manualGenerateSuggestionsButton?.addEventListener('click', async () => {
            await this.generateSuggestions({ replace: true, silent: false });
        });

        // Defensive delegation fallback: if cached references are missing or listeners fail,
        // handle clicks at document level for the common control IDs.
        document.addEventListener('click', async (ev) => {
            try {
                const target = ev.target.closest && ev.target.closest('#manual-generate-suggestions-btn, #refresh-tasks-btn');
                if (!target) return;
                ev.preventDefault();
                // Use the app-scoped method (arrow captures `this`)
                await this.generateSuggestions({ replace: true, silent: false });
            } catch (err) {
                console.error('[Renderer] fallback generateSuggestions handler failed:', err);
                try { this.showToast('Suggestion generation failed'); } catch (_) {}
            }
        });

        this.remindersList?.addEventListener('click', async (event) => {
            const card = event.target.closest('.suggestion-card');
            if (!card) return;

            const taskId = card.dataset.id;
            const action = event.target.closest('[data-action]')?.dataset.action;
            if (!taskId && !action) return;

            if (action === 'done') {
                event.stopPropagation();
                await this.completeSuggestion(taskId);
                return;
            }

            if (action === 'execute') {
                event.stopPropagation();
                await this.executeSuggestionAutomation(taskId, 'execute');
                return;
            }

            if (action === 'draft') {
                event.stopPropagation();
                await this.executeSuggestionAutomation(taskId, 'draft');
                return;
            }

            if (action === 'snooze') {
                event.stopPropagation();
                await this.snoozeSuggestion(taskId);
                return;
            }

            if (action === 'context') {
                event.stopPropagation();
                await this.openSuggestionInfo(taskId);
                return;
            }

            if (action === 'remove') {
                event.stopPropagation();
                await this.removeSuggestion(taskId);
                return;
            }

            if (action === 'info') {
                event.stopPropagation();
                await this.openSuggestionInfo(taskId);
                return;
            }

            if (action === 'brief-open') {
                event.stopPropagation();
                this.openMorningBriefModal();
                return;
            }

            if (action === 'brief-archive') {
                event.stopPropagation();
                this.openMorningBriefModal();
                return;
            }

            if (this.expandedCards.has(taskId)) {
                await this.openSuggestionInfo(taskId);
            } else {
                this.toggleCard(taskId);
            }
        });

        this.remindersList?.addEventListener('keydown', async (event) => {
            const card = event.target.closest('.suggestion-card');
            if (!card || (event.key !== 'Enter' && event.key !== ' ')) return;

            event.preventDefault();
            const taskId = card.dataset.id;
            if (!taskId) return;
            this.toggleCard(taskId);
        });

        if (!this.suggestionAutoTimer) {
            this.suggestionAutoTimer = window.setInterval(() => {
                if (document.hidden) return;
                this.generateSuggestions({ replace: true, silent: true }).catch(() => {});
            }, 30 * 60 * 1000);
        }

        document.getElementById('morning-brief-close-btn')?.addEventListener('click', () => this.closeMorningBriefModal());
        this.morningBriefModal?.addEventListener('click', (event) => {
            if (event.target?.id === 'morning-brief-modal') this.closeMorningBriefModal();
        });
    }

    async generateSuggestions({ replace = true, silent = false } = {}) {
        if (!this.remindersList) return;

        if (!silent) {
            this.remindersList.innerHTML = this.emptyStateHTML(
                'refresh',
                'Generating suggestions...',
                'Reviewing your recent context and building a fresh queue.'
            );
        }

        try {
            console.debug('[Renderer] generateSuggestions invoked; electronAPI present=', !!window.electronAPI, 'generateProactiveTodos=', typeof window.electronAPI?.generateProactiveTodos);
            if (!window.electronAPI || typeof window.electronAPI.generateProactiveTodos !== 'function') {
                throw new Error('electronAPI.generateProactiveTodos is not available');
            }
            const generated = await window.electronAPI.generateProactiveTodos();
            console.debug('[Renderer] generateSuggestions received', Array.isArray(generated) ? `${generated.length} items` : typeof generated);
            const normalized = this.normalizeTodos(generated);

            if (replace) {
                this.todos = normalized;
            } else {
                this.todos = this.mergeTodos(this.todos, normalized);
            }
            this.todos = this.todos.filter((todo) => !todo.completed);

            await window.electronAPI.savePersistentTodos(this.todos);
            this.renderSuggestions();
        } catch (error) {
            console.error('Failed to generate suggestions:', error);
            this.showToast('Suggestion generation failed');
            this.renderSuggestions();
        }
    }

    normalizeTodos(items) {
        const normalized = (Array.isArray(items) ? items : [])
            .map((item, index) => {
                const category = this.normalizeCategory(item.category);
                const plan = Array.isArray(item.plan)
                    ? item.plan.filter(Boolean).slice(0, 3)
                    : [];
                const suggestedActions = Array.isArray(item.suggested_actions) ? item.suggested_actions.slice(0, 3) : [];
                const mappedActionPlan = suggestedActions.map((action, i) => ({
                    step: action?.label || `Action ${i + 1}`,
                    target: action?.type || 'browser_operator',
                    url: action?.payload?.url || null
                }));
                const primaryAction = item.primary_action || suggestedActions.find((action) => this.isConcreteActionLabel(action?.label));
                const whyNow = (item.display?.summary || item.trigger_summary || item.reason || item.description || item.body || '').trim();
                const evidenceLine = Array.isArray(item.epistemic_trace) && item.epistemic_trace.length
                    ? `${item.epistemic_trace[0].source || 'Source'}: ${item.epistemic_trace[0].text || ''}`.trim()
                    : '';
                return {
                    id: item.id || `suggestion_${Date.now()}_${index}`,
                    title: (item.title || 'Untitled suggestion').trim(),
                    description: (item.description || item.body || whyNow || 'No extra context yet.').trim(),
                    intent: (item.intent || item.description || '').trim(),
                    reason: (item.reason || item.description || '').trim(),
                    why_now: whyNow,
                    evidence_line: evidenceLine,
                    trigger_summary: (item.trigger_summary || item.triggerSummary || '').trim(),
                    plan,
                    confidence: Number(item.confidence || 0),
                    evidence: Array.isArray(item.evidence) ? item.evidence : [],
                    source_node_ids: Array.isArray(item.source_node_ids) ? item.source_node_ids : [],
                    source_edge_paths: Array.isArray(item.source_edge_paths) ? item.source_edge_paths : [],
                    retrieval_trace: item.retrieval_trace || item.retrievalTrace || null,
                    priority: item.priority || 'medium',
                    category,
                    ai_draft: item.ai_draft || '',
                    ai_doable: Boolean(item.ai_doable || item.assignee === 'ai'),
                    assignee: item.assignee || ((item.ai_doable || item.assignee === 'ai') ? 'ai' : 'human'),
                    action_type: item.action_type || null,
                    completed: Boolean(item.completed),
                    createdAt: item.createdAt || Date.now(),
                    source: item.source || 'generated',
                    deeplink: item.deeplink || null,
                    suggested_actions: suggestedActions,
                    primary_action: primaryAction || null,
                    display: item.display || null,
                    epistemic_trace: Array.isArray(item.epistemic_trace) ? item.epistemic_trace : [],
                    execution_mode: item.execution_mode || ((item.ai_doable || item.assignee === 'ai') ? 'draft_or_execute' : 'manual'),
                    target_surface: item.target_surface || null,
                    expected_benefit: (item.expected_benefit || '').trim(),
                    expected_impact: (item.expected_impact || item.expected_benefit || '').trim(),
                    prerequisites: Array.isArray(item.prerequisites) ? item.prerequisites : [],
                    step_plan: Array.isArray(item.step_plan) ? item.step_plan.filter(Boolean).slice(0, 4) : plan,
                    snoozedUntil: item.snoozedUntil || 0,
                    study_subject: (item.study_subject || '').trim(),
                    risk_level: (item.risk_level || '').trim().toLowerCase(),
                    evidence_path: Array.isArray(item.evidence_path) ? item.evidence_path : [],
                    recommended_action: (item.recommended_action || '').trim(),
                    suggestion_group: (item.suggestion_group || '').trim().toLowerCase(),
                    opportunity_type: (item.opportunity_type || '').trim().toLowerCase(),
                    time_anchor: (item.time_anchor || '').trim(),
                    reason_codes: Array.isArray(item.reason_codes) ? item.reason_codes : [],
                    candidate_score: Number(item.candidate_score || 0),
                    action_plan: Array.isArray(item.action_plan) && item.action_plan.length
                        ? item.action_plan
                        : mappedActionPlan
                };
            })
            .filter((item) => item.title && item.primary_action && this.isConcreteActionLabel(item.primary_action?.label));
        const deduped = [];
        const seen = new Set();
        for (const item of normalized) {
            const key = this.todoKey(item);
            if (!key || seen.has(key)) continue;
            seen.add(key);
            deduped.push(item);
        }
        return deduped;
    }

    mergeTodos(existing, incoming) {
        const merged = new Map();
        [...existing, ...incoming].forEach((item) => {
            const key = this.todoKey(item);
            if (!merged.has(key) || (!merged.get(key).reason && item.reason)) {
                merged.set(key, item);
            }
        });

        return Array.from(merged.values())
            .sort((a, b) => {
                const priorityDelta = this.priorityWeight(b.priority) - this.priorityWeight(a.priority);
                if (priorityDelta !== 0) return priorityDelta;
                return (b.createdAt || 0) - (a.createdAt || 0);
            })
            .slice(0, 10);
    }

    renderSuggestions() {
        if (!this.remindersList) return;

        const visibleTodos = this.getVisibleTodos();
        const briefBanner = this.renderMorningBriefBanner();

        if (!visibleTodos.length) {
            this.remindersList.innerHTML = briefBanner + this.emptyStateHTML(
                'lightbulb',
                'No suggestions yet',
                'Tap refresh to generate AI suggestions based on your current context.'
            );
            return;
        }

        const grouped = {
            now: visibleTodos.filter((todo) => (todo.suggestion_group || 'next') === 'now'),
            next: visibleTodos.filter((todo) => !todo.suggestion_group || todo.suggestion_group === 'next'),
            risk: visibleTodos.filter((todo) => todo.suggestion_group === 'risk')
        };

        const renderGroup = (title, key) => {
            const items = grouped[key] || [];
            if (!items.length) return '';
            return `
                <div class="study-suggestion-group">
                    <div class="study-suggestion-group-title">${this.escapeHtml(title)}</div>
                    ${items.map((todo, index) => this.renderSuggestionCard(todo, index)).join('')}
                </div>
            `;
        };

        this.remindersList.innerHTML = briefBanner + [
            renderGroup('Now', 'now'),
            renderGroup('Next', 'next'),
            renderGroup('Risk Alerts', 'risk')
        ].join('');
    }

    renderSuggestionCard(todo, index = 0) {
        const expanded = this.expandedCards.has(todo.id);
        const suggestionCategory = todo.suggestion_category || todo.category || 'work';
        const isStudy = suggestionCategory === 'study';
        const icon = isStudy ? 'school' : this.categoryIcon(todo.category);
        // ai_doable + non-manual = something the automation layer can actually execute
        const aiCanAutomate = Boolean(todo.ai_doable && todo.action_type && todo.action_type !== 'manual_next_step');
        const evidencePath = Array.isArray(todo.evidence_path) && todo.evidence_path.length
            ? todo.evidence_path
            : (Array.isArray(todo.source_edge_paths) ? todo.source_edge_paths : []);
        const evidenceSummary = evidencePath.slice(0, 3).map((edge) => {
            const from = edge?.from || '?';
            const to = edge?.to || '?';
            const relation = edge?.relation || 'links';
            return `${from} -> ${to} (${relation})`;
        }).join(' • ');
        const riskLabel = todo.risk_level ? `Risk: ${todo.risk_level}` : '';
        const bundleHeadline = todo.display?.headline || '';
        const bundleSummary = todo.display?.summary || '';
        const bundleInsight = todo.display?.insight || '';
        const primaryAction = todo.primary_action || (Array.isArray(todo.suggested_actions) ? todo.suggested_actions.find((a) => this.isConcreteActionLabel(a?.label)) : null);
        // Prefer the most specific one-liner: reason > trigger_summary > display.summary
        const rawSubtitle = todo.reason || todo.trigger_summary || bundleSummary || todo.why_now || todo.description || '';
        const whyNowCompact = rawSubtitle.length > 100 ? rawSubtitle.slice(0, 97) + '…' : rawSubtitle;
        const evidenceCompact = todo.evidence_line || (Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? `${todo.epistemic_trace[0].source || 'Source'}: ${todo.epistemic_trace[0].text || ''}`
            : '');
        const receiptSummary = Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? todo.epistemic_trace.slice(0, 3).map((r) => `${r.source || 'Source'}: ${r.text || ''}`).join(' • ')
            : '';
        const suggestedActionLabels = Array.isArray(todo.suggested_actions) && todo.suggested_actions.length
            ? todo.suggested_actions
                .slice(0, 3)
                .map((a) => a.label)
                .filter((label) => this.isConcreteActionLabel(label))
            : [];

        return `
                <article class="suggestion-card liquid-card${isStudy ? ' suggestion-study' : ''}" data-id="${this.escapeHtml(todo.id)}" tabindex="0" style="animation-delay:${index * 40}ms;">
                    <div class="suggestion-header">
                        <div class="suggestion-icon ${isStudy ? 'study' : this.escapeHtml(todo.category)}">
                            <span class="material-symbols-outlined">${icon}</span>
                        </div>
                        <div class="suggestion-content">
                            <div class="suggestion-title-row">
                                <span class="suggestion-type-label">${isStudy ? 'study' : this.escapeHtml(todo.category || 'work')}</span>
                                ${aiCanAutomate ? '<span class="suggestion-auto-badge">AI can automate</span>' : ''}
                            </div>
                            <div class="suggestion-title">${this.escapeHtml(todo.title)}</div>
                            <div class="suggestion-description">${this.escapeHtml(whyNowCompact)}</div>
                            ${evidenceCompact ? `<div class="suggestion-context">${this.escapeHtml(evidenceCompact)}</div>` : ''}
                            ${primaryAction ? `<div class="suggestion-context"><strong>→</strong> ${this.escapeHtml(primaryAction.label)}</div>` : ''}
                        </div>
                    </div>
                    ${expanded ? `
                        <div class="suggestion-details" style="display:block;">
                            ${todo.trigger_summary ? `<div class="suggestion-why"><span>Trigger</span>${this.escapeHtml(todo.trigger_summary)}</div>` : ''}
                            ${todo.intent ? `<div class="suggestion-goal"><span>Intent</span>${this.escapeHtml(todo.intent)}</div>` : ''}
                            ${bundleHeadline ? `<div class="suggestion-goal"><span>Bundle</span>${this.escapeHtml(bundleHeadline)}</div>` : ''}
                            ${bundleSummary ? `<div class="suggestion-why"><span>Context</span>${this.escapeHtml(bundleSummary)}</div>` : ''}
                            ${bundleInsight ? `<div class="suggestion-goal"><span>Insight</span>${this.escapeHtml(bundleInsight)}</div>` : ''}
                            ${todo.reason ? `<div class="suggestion-why"><span>Reason</span>${this.escapeHtml(todo.reason)}</div>` : ''}
                            ${receiptSummary ? `<div class="suggestion-why"><span>Receipts</span>${this.escapeHtml(receiptSummary)}</div>` : ''}
                            ${primaryAction ? `<div class="suggestion-goal"><span>Primary CTA</span>${this.escapeHtml(primaryAction.label)}</div>` : ''}
                            ${suggestedActionLabels.length > 1 ? `<div class="suggestion-goal"><span>Other actions</span>${this.escapeHtml(suggestedActionLabels.slice(1).join(' • '))}</div>` : ''}
                            ${todo.recommended_action ? `<div class="suggestion-goal"><span>Recommended action</span>${this.escapeHtml(todo.recommended_action)}</div>` : ''}
                            ${todo.opportunity_type ? `<div class="suggestion-goal"><span>Opportunity</span>${this.escapeHtml(todo.opportunity_type.replace(/_/g, ' '))}</div>` : ''}
                            ${todo.time_anchor ? `<div class="suggestion-why"><span>Time anchor</span>${this.escapeHtml(todo.time_anchor)}</div>` : ''}
                            ${todo.step_plan.length ? `<div class="suggestion-steps">${todo.step_plan.map((step, stepIndex) => `<div class="suggestion-step">${stepIndex + 1}. ${this.escapeHtml(step)}</div>`).join('')}</div>` : ''}
                            ${todo.expected_benefit ? `<div class="suggestion-goal"><span>Expected benefit</span>${this.escapeHtml(todo.expected_benefit)}</div>` : ''}
                            ${todo.expected_impact ? `<div class="suggestion-goal"><span>Expected impact</span>${this.escapeHtml(todo.expected_impact)}</div>` : ''}
                            ${todo.prerequisites?.length ? `<div class="suggestion-why"><span>Prerequisites</span>${this.escapeHtml(todo.prerequisites.join(' • '))}</div>` : ''}
                            ${evidenceSummary ? `<div class="suggestion-why"><span>Why this suggestion?</span>${this.escapeHtml(evidenceSummary)}</div>` : ''}
                            ${todo.ai_draft ? `<div class="reminder-description">${this.escapeHtml(todo.ai_draft)}</div>` : ''}
                            ${todo.source_node_ids?.length ? `<div class="suggestion-goal"><span>Sources</span>${this.escapeHtml(todo.source_node_ids.slice(0, 4).join(', '))}</div>` : ''}
                            ${todo.source_edge_paths?.length ? `<div class="suggestion-why"><span>Trace</span>${this.escapeHtml(todo.source_edge_paths.slice(0, 3).map((edge) => `${edge.from} -> ${edge.to} (${edge.relation})`).join(' • '))}</div>` : ''}
                            <div class="suggestion-meta" style="margin-top:12px;">
                                <span>${this.escapeHtml(this.prettyCategory(todo.category))}</span>
                                <span>${this.escapeHtml(this.prettyPriority(todo.priority))} priority</span>
                                ${todo.confidence ? `<span>${this.escapeHtml(this.formatConfidence(todo.confidence))}</span>` : ''}
                                ${todo.study_subject ? `<span>${this.escapeHtml(todo.study_subject)}</span>` : ''}
                                ${riskLabel ? `<span>${this.escapeHtml(riskLabel)}</span>` : ''}
                                <span>${this.escapeHtml(todo.ai_doable ? 'AI can do' : 'Manual')}</span>
                                ${todo.action_type ? `<span>${this.escapeHtml(String(todo.action_type).replace(/_/g, ' '))}</span>` : ''}
                            </div>
                            <div class="reminder-actions" style="opacity:1; margin-top:16px;">
                                ${todo.source === 'morning-brief'
                                    ? '<button class="pill-btn" type="button" data-action="brief-open">Open brief</button><button class="pill-btn" type="button" data-action="brief-archive">Archive</button>'
                                    : `${todo.ai_doable ? '<button class="pill-btn" type="button" data-action="draft">Draft</button><button class="pill-btn" type="button" data-action="execute">Do it</button>' : ''}<button class="pill-btn" type="button" data-action="info">More info</button><button class="pill-btn" type="button" data-action="done">Complete</button><button class="pill-btn" type="button" data-action="snooze">Snooze</button><button class="pill-btn destructive" type="button" data-action="remove">Remove</button>`
                                }
                            </div>
                        </div>
                    ` : ''}
                </article>
            `;
    }

    renderMorningBriefBanner() {
        const latest = (this.morningBriefs || [])[0];
        if (!latest) return '';
        const subtitle = (latest.priorities || []).slice(0, 3).map((p) => p.title).filter(Boolean).join(' • ');
        return `
            <article class="suggestion-card liquid-card" data-id="brief_banner" tabindex="0" style="margin-bottom:12px;">
                <div class="suggestion-header">
                    <div class="suggestion-icon work">
                        <span class="material-symbols-outlined">wb_sunny</span>
                    </div>
                    <div class="suggestion-content">
                        <div class="suggestion-title">Morning Brief • ${this.escapeHtml(latest.dateLabel || '')}</div>
                        <div class="suggestion-description">${this.escapeHtml(subtitle || 'Your daily 5-minute kickoff is ready.')}</div>
                    </div>
                    <button class="ghost-button" type="button" data-action="brief-open">Open</button>
                </div>
            </article>
        `;
    }

    toggleCard(taskId) {
        if (this.expandedCards.has(taskId)) {
            this.expandedCards.delete(taskId);
        } else {
            this.expandedCards.add(taskId);
        }
        this.renderSuggestions();
    }

    async completeSuggestion(taskId) {
        try {
            await window.electronAPI.completeTask(taskId);
        } catch (_) {}
        this.todos = this.todos.filter((todo) => todo.id !== taskId);
        this.expandedCards.delete(taskId);

        try {
            await window.electronAPI.savePersistentTodos(this.todos);
        } catch (error) {
            console.error('Failed to save completed suggestion:', error);
        }

        this.renderSuggestions();

        if (!this.getVisibleTodos().length) {
            await this.generateSuggestions({ replace: true, silent: true });
        }

        this.showToast('Task completed');
    }

    async removeSuggestion(taskId) {
        this.todos = this.todos.filter((todo) => todo.id !== taskId);
        this.expandedCards.delete(taskId);
        try {
            await window.electronAPI.savePersistentTodos(this.todos);
            this.showToast('Task removed');
        } catch (error) {
            console.error('Failed to remove suggestion:', error);
            this.showToast('Remove failed');
        }
        this.renderSuggestions();
    }

    async snoozeSuggestion(taskId, minutes = 120) {
        const todo = this.todos.find((item) => item.id === taskId);
        if (!todo) return;
        todo.snoozedUntil = Date.now() + (minutes * 60 * 1000);
        try {
            await window.electronAPI.savePersistentTodos(this.todos);
            this.showToast(`Snoozed for ${Math.round(minutes / 60)}h`);
        } catch (error) {
            console.error('Failed to snooze suggestion:', error);
            this.showToast('Snooze failed');
        }
        this.renderSuggestions();
    }

    async findContextForSuggestion(taskId) {
        const todo = this.todos.find((item) => item.id === taskId);
        if (!todo) return;
        const query = `${todo.title} ${todo.reason || ''}`.trim();
        try {
            const [nodes, rawEvents] = await Promise.all([
                window.electronAPI.searchMemoryGraph(query, { limit: 20 }),
                window.electronAPI.searchRawEvents(query)
            ]);
            const normalizedNodes = (nodes || []).map((item) => ({
                id: item.id,
                type: item.type,
                data: item.data || {},
                timestamp: item.data?.timestamp || item.data?.date || 0
            }));
            const normalizedEvents = (rawEvents || []).map((event) => ({
                id: event.id,
                type: 'raw_event',
                data: {
                    title: event.text || event.type || 'Raw Event',
                    source: event.source,
                    metadata: event.metadata || {},
                    text: event.text || ''
                },
                timestamp: event.timestamp || 0
            }));
            this.memorySearchResults = [...normalizedNodes, ...normalizedEvents]
                .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime());
            if (!this.memorySearchResults.length) {
                this.showToast('No related context found');
                return;
            }
            this.openMemoryModal(0);
        } catch (error) {
            console.error('Failed to find suggestion context:', error);
            this.showToast('Context lookup failed');
        }
    }

    async openSuggestionInfo(taskId) {
        const todo = this.todos.find((item) => item.id === taskId);
        if (!todo) return;

        let relatedSummary = '';
        try {
            const query = `${todo.title} ${todo.reason || ''}`.trim();
            const [nodes, rawEvents] = await Promise.all([
                window.electronAPI.searchMemoryGraph(query, { limit: 6 }),
                window.electronAPI.searchRawEvents(query)
            ]);
            const relatedNodes = (nodes || []).slice(0, 4).map((item) => item.data?.title || item.id).filter(Boolean);
            const relatedEvents = (rawEvents || []).slice(0, 2).map((item) => item.text || item.type).filter(Boolean);
            const lines = [];
            if (relatedNodes.length) lines.push(`Related memory: ${relatedNodes.join(' • ')}`);
            if (relatedEvents.length) lines.push(`Raw evidence: ${relatedEvents.join(' • ')}`);
            relatedSummary = lines.join('\n');
        } catch (error) {
            console.error('Failed to load suggestion info context:', error);
        }

        const modal = document.getElementById('memory-detail-modal');
        const layer = document.getElementById('modal-memory-layer');
        const title = document.getElementById('modal-memory-title');
        const time = document.getElementById('modal-memory-time');
        const narrative = document.getElementById('modal-memory-narrative');
        const raw = document.getElementById('modal-memory-raw');
        if (!modal || !layer || !title || !time || !narrative || !raw) return;

        const narrativeParts = [
            todo.description ? `Summary\n${todo.description}` : '',
            todo.trigger_summary ? `\nTrigger\n${todo.trigger_summary}` : '',
            todo.intent ? `\nIntent\n${todo.intent}` : '',
            todo.reason ? `\nReason\n${todo.reason}` : '',
            todo.plan?.length ? `\nPlan\n${todo.plan.map((step, index) => `${index + 1}. ${step}`).join('\n')}` : '',
            relatedSummary ? `\nContext\n${relatedSummary}` : ''
        ].filter(Boolean);

        layer.textContent = `${String(todo.category || 'task').toUpperCase()} • TASK`;
        title.textContent = todo.title || 'Task';
        time.textContent = todo.createdAt ? new Date(todo.createdAt).toLocaleString() : 'No timestamp';
        narrative.textContent = narrativeParts.join("\n") || "No additional information available.";
        raw.textContent = JSON.stringify(todo, null, 2);

        modal.classList.remove('hidden');
        modal.style.display = 'flex';
    }

    async executeSuggestionAutomation(taskId, mode = 'execute') {
        const todo = this.todos.find((item) => item.id === taskId);
        if (!todo || !todo.ai_doable) return;
        try {
            let url = todo.deeplink || (todo.action_plan?.[0]?.url) || null;
            if (!url) {
                if (todo.action_type === 'draft_message') url = 'https://mail.google.com';
                else if (todo.action_type === 'prepare_brief') url = 'https://calendar.google.com';
                else if (todo.action_type === 'research') url = 'https://duckduckgo.com';
                else url = 'https://www.google.com';
            }
            await window.electronAPI.executeAITask({
                id: todo.id,
                title: mode === 'draft'
                    ? `Draft only: ${todo.title}. Prepare the next step for review and do not submit or confirm anything externally.`
                    : todo.title,
                url,
                ai_draft: todo.ai_draft || todo.description || todo.reason || '',
                action_type: todo.action_type || null,
                action_plan: todo.action_plan || [],
                execution_mode: mode,
                forceExtension: true,
                agentMode: true
            });
            this.showToast(mode === 'draft' ? 'Draft prepared' : 'Automation executed');
        } catch (error) {
            console.error('Automation failed:', error);
            this.showToast('Automation failed');
        }
    }

    getVisibleTodos() {
        const now = Date.now();
        return this.todos.filter((todo) => {
            if (todo.completed) return false;
            if (todo.snoozedUntil && Number(todo.snoozedUntil) > now) return false;
            if (this.currentFilter === 'all') return true;
            if (this.currentFilter === 'followups') return todo.category === 'followup';
            return todo.category === this.currentFilter;
        });
    }

    formatConfidence(value) {
        const pct = Math.max(0, Math.min(100, Math.round(Number(value || 0) * 100)));
        return `${pct}% intent confidence`;
    }


    async updateChatSyncStatus() {
        const statusEl = document.getElementById('chat-sync-status');
        if (!statusEl) return;

        try {
            const status = await window.electronAPI.getMemoryGraphStatus();
            const isSyncing = status?.episodeStatus === 'running' || status?.syncStatus === 'running';
            const textEl = statusEl.querySelector('.sync-text');
            
            if (isSyncing) {
                statusEl.classList.add('syncing');
                if (textEl) textEl.textContent = 'Syncing memory...';
            } else {
                statusEl.classList.remove('syncing');
                if (textEl) textEl.textContent = 'Memory synced';
            }
        } catch (e) {
            console.warn('Failed to update sync status:', e);
        }
    }

    async setupChat() {
        this.chatSessions = await this.loadChatSessions();
        const sorted = [...this.chatSessions].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
        this.activeChatId = sorted[0]?.id || null;
        this.newChatButton?.addEventListener('click', () => this.startNewChatDraft());

        this.sendChatButton?.addEventListener('click', () => this.sendChatMessage());

        this.chatInput?.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                this.sendChatMessage();
            }
        });

        this.chatInput?.addEventListener('input', () => {
            this.chatInput.style.height = 'auto';
            this.chatInput.style.height = `${Math.min(this.chatInput.scrollHeight, 120)}px`;
        });

        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();
    }

    async sendChatMessage() {
        const message = this.chatInput?.value.trim();
        if (!message) return;

        let chat = this.getActiveChat();
        if (!chat) {
            chat = this.createNewChatSession(message);
        }
        const contextWindow = (chat.messages || [])
            .slice(-12)
            .map((item) => ({
                role: item.role,
                content: item.content,
                ts: item.ts || null
            }));

        this.pushMessageToActiveChat('user', message);
        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();
        this.chatInput.value = '';
        this.chatInput.style.height = '44px';

        const thinkingPanel = this.appendThinkingPanel();
        this.triggerSearchAnimation(true);

        const handleChatStep = (data) => {
            this.updateThinkingPanel(thinkingPanel, data);
            this.scrollChatToBottom();
        };

        if (window.electronAPI?.onChatStep) window.electronAPI.onChatStep(handleChatStep);

        try {
            const response = await window.electronAPI.askAIAssistant(message, {
                chat_session_id: this.activeChatId,
                chat_history: contextWindow
            });
            if (window.electronAPI?.offChatStep) window.electronAPI.offChatStep();
            this.triggerSearchAnimation(false);
            this.finalizeThinkingPanel(thinkingPanel, response);
            await this.typeAssistantResponse(response, { includeThinkingTrace: false });
        } catch (error) {
            console.error('Chat failed:', error);
            this.triggerSearchAnimation(false);
            if (window.electronAPI?.offChatStep) window.electronAPI.offChatStep();
            this.finalizeThinkingPanel(thinkingPanel, {
                thinking_trace: {
                    thinking_summary: 'The retrieval step failed before a full answer was ready.',
                    filters: [],
                    search_queries: { context: [], messages: [] },
                    results_summary: { headline: 'No retrieval results were available.', details: [] },
                    data_sources: []
                }
            });
            await this.typeAssistantResponse('I ran into a problem while preparing that answer.', { includeThinkingTrace: false });
        }
    }

    appendChatMessage(role, payload) {
        if (!this.chatMessages) return;

        const emptyState = this.chatMessages.querySelector('.empty-state');
        if (emptyState) emptyState.remove();

        const message = document.createElement('div');
        message.className = `message ${role}`;
        const content = typeof payload === 'object' && payload !== null ? (payload.content || '') : payload;
        const retrieval = typeof payload === 'object' && payload !== null ? (payload.retrieval || null) : null;
        const thinkingTrace = typeof payload === 'object' && payload !== null ? (payload.thinking_trace || retrieval?.thinking_trace || null) : null;
        message.innerHTML = role === 'assistant'
            ? this.renderAssistantHTML(content, retrieval, thinkingTrace)
            : this.escapeHtml(content).replace(/\n/g, '<br>');
        this.chatMessages.appendChild(message);
        this.scrollChatToBottom();
    }

    async typeAssistantResponse(rawPayload, options = {}) {
        const content = this.formatAssistantOutput(typeof rawPayload === 'object' && rawPayload !== null ? rawPayload.content : rawPayload);
        const retrieval = typeof rawPayload === 'object' && rawPayload !== null ? (rawPayload.retrieval || null) : null;
        const thinkingTrace = typeof rawPayload === 'object' && rawPayload !== null ? (rawPayload.thinking_trace || retrieval?.thinking_trace || null) : null;
        const includeThinkingTrace = options.includeThinkingTrace !== false;
        const message = document.createElement('div');
        message.className = 'message assistant';
        if (!this.chatMessages) return;
        this.chatMessages.appendChild(message);

        let i = 0;
        const maxChars = 2600;
        const bounded = content.length > maxChars ? `${content.slice(0, maxChars)}

Would you like me to continue with more detail?` : content;
        const chunkSize = bounded.length > 900 ? 10 : 4;
        const frameDelay = bounded.length > 900 ? 6 : 12;
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

        while (i < bounded.length) {
            const slice = bounded.slice(0, i + chunkSize);
            message.innerHTML = this.escapeHtml(slice).replace(/\n/g, '<br>');
            i += chunkSize;
            this.scrollChatToBottom();
            await sleep(frameDelay);
        }

        // Final pass: render concise rich text (no markdown headings).
        message.innerHTML = this.renderAssistantHTML(bounded, retrieval, includeThinkingTrace ? thinkingTrace : null);
        this.scrollChatToBottom();

        this.pushMessageToActiveChat('assistant', bounded, retrieval, thinkingTrace);
        this.renderChatHistory();
    }
    appendThinkingPanel() {
        const wrapper = document.createElement("div");
        const bootstrapStages = [
            {
                step: 'routing',
                status: 'started',
                label: 'Routing',
                detail: 'Preparing the retrieval pipeline.'
            }
        ];
        wrapper.innerHTML = `
            <div class="thinking-panel expanded">
                <div class="thinking-live-label">Live retrieval pipeline</div>
                <div class="thinking-step-label">Preparing retrieval...</div>
                <div class="thinking-timeline">${this.renderThinkingTimeline(bootstrapStages)}</div>
                <div class="thinking-final-card"></div>
            </div>
        `;
        const panel = wrapper.firstElementChild;
        panel.__thinkingStages = bootstrapStages;
        this.chatMessages.appendChild(panel);
        this.scrollChatToBottom();
        return panel;
    }

    normalizeThinkingStage(raw) {
        if (typeof raw === 'string') {
            return {
                step: raw,
                status: 'completed',
                label: raw.replace(/_/g, ' '),
                detail: ''
            };
        }
        if (!raw || typeof raw !== 'object') return null;
        return {
            step: raw.step || 'thinking',
            status: raw.status || 'completed',
            label: raw.label || String(raw.step || 'thinking').replace(/_/g, ' '),
            detail: raw.detail || '',
            counts: raw.counts || {},
            preview_items: Array.isArray(raw.preview_items) ? raw.preview_items : []
        };
    }

    renderThinkingTimeline(stages = []) {
        if (!Array.isArray(stages) || !stages.length) return '';
        return stages.map((stage) => {
            const counts = stage?.counts && typeof stage.counts === 'object'
                ? Object.entries(stage.counts)
                    .filter(([, value]) => value !== null && value !== undefined && value !== 0)
                    .slice(0, 3)
                    .map(([key, value]) => `${this.escapeHtml(String(key).replace(/_/g, ' '))}: ${this.escapeHtml(String(value))}`)
                    .join(' • ')
                : '';
            const previews = Array.isArray(stage?.preview_items)
                ? stage.preview_items.filter(Boolean).slice(0, 3)
                : [];
            return `
                <div class="thinking-timeline-row status-${this.escapeHtml(stage.status || 'completed')}">
                    <div class="thinking-timeline-marker"></div>
                    <div class="thinking-timeline-body">
                        <div class="thinking-timeline-head">
                            <span class="thinking-timeline-title">${this.escapeHtml(stage.label || stage.step || 'stage')}</span>
                            <span class="thinking-timeline-status">${this.escapeHtml(stage.status || 'completed')}</span>
                        </div>
                        ${stage.detail ? `<div class="thinking-timeline-detail">${this.escapeHtml(stage.detail)}</div>` : ''}
                        ${counts ? `<div class="thinking-timeline-meta">${counts}</div>` : ''}
                        ${previews.length ? `<div class="thinking-timeline-preview">${previews.map((item) => `<span class="thinking-trace-chip">${this.escapeHtml(String(item))}</span>`).join('')}</div>` : ''}
                    </div>
                </div>
            `;
        }).join('');
    }

    updateThinkingPanel(panel, payload) {
        if (!panel) return;
        const label = panel.querySelector('.thinking-step-label');
        const timeline = panel.querySelector('.thinking-timeline');
        const nextStage = this.normalizeThinkingStage(payload);
        if (!nextStage || !timeline) return;

        const stages = Array.isArray(panel.__thinkingStages) ? panel.__thinkingStages.slice() : [];
        const existingIndex = stages.findIndex((item) => item.step === nextStage.step);
        if (existingIndex >= 0) stages[existingIndex] = { ...stages[existingIndex], ...nextStage };
        else stages.push(nextStage);
        panel.__thinkingStages = stages;

        if (label) {
            label.textContent = nextStage.detail || `${nextStage.label} (${nextStage.status})`;
        }
        timeline.innerHTML = this.renderThinkingTimeline(stages);
    }

    finalizeThinkingPanel(panel, payload) {
        if (!panel) return;
        const retrieval = payload?.retrieval || null;
        const thinkingTrace = payload?.thinking_trace || retrieval?.thinking_trace || null;
        const finalCard = panel.querySelector('.thinking-final-card');
        const stages = Array.isArray(retrieval?.stage_trace) && retrieval.stage_trace.length
            ? retrieval.stage_trace
            : (Array.isArray(panel.__thinkingStages) ? panel.__thinkingStages : []);
        panel.classList.add('ready');
        const label = panel.querySelector('.thinking-step-label');
        const timeline = panel.querySelector('.thinking-timeline');
        if (label) label.textContent = 'Retrieval complete.';
        if (timeline) timeline.innerHTML = this.renderThinkingTimeline(stages);
        if (finalCard) finalCard.innerHTML = this.renderThinkingTraceCard(thinkingTrace, { ...(retrieval || {}), stage_trace: stages });
        this.scrollChatToBottom();
    }


    formatAssistantOutput(rawContent) {
        const text = String(rawContent || '')
            .replace(/^```[a-z]*\n?/i, '')
            .replace(/```$/g, '')
            .replace(/^\s{0,3}#{1,6}\s+/gm, '')
            .trim();
        return text || 'No response.';
    }

    scrollChatToBottom() {
        if (!this.chatMessages) return;
        this.chatMessages.scrollTop = this.chatMessages.scrollHeight;
    }

    formatInlineMarkdown(raw) {
        const safe = this.escapeHtml(String(raw || ''));
        return safe
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/\[(.+?)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer">$1</a>');
    }

    renderAssistantHTML(content, retrieval = null, thinkingTrace = null) {
        const cleaned = this.formatAssistantOutput(content);
        const lines = cleaned.split('\n');
        const blocks = [];
        let i = 0;

        const isTableSep = (line) => /^\s*\|?[\s:-]+\|[\s|:-]*$/.test(line);

        while (i < lines.length) {
            const line = lines[i].trim();
            if (!line) {
                i += 1;
                continue;
            }

            if (/^\s*[-*]\s+/.test(line)) {
                const items = [];
                while (i < lines.length && /^\s*[-*]\s+/.test(lines[i])) {
                    items.push(lines[i].replace(/^\s*[-*]\s+/, '').trim());
                    i += 1;
                }
                blocks.push(`<ul>${items.map((it) => `<li>${this.formatInlineMarkdown(it)}</li>`).join('')}</ul>`);
                continue;
            }

            if (/^\s*\d+\.\s+/.test(line)) {
                const items = [];
                while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
                    items.push(lines[i].replace(/^\s*\d+\.\s+/, '').trim());
                    i += 1;
                }
                blocks.push(`<ol>${items.map((it) => `<li>${this.formatInlineMarkdown(it)}</li>`).join('')}</ol>`);
                continue;
            }

            if (line.includes('|') && (i + 1 < lines.length) && isTableSep(lines[i + 1])) {
                const header = line.split('|').map((s) => s.trim()).filter(Boolean);
                i += 2; // skip header + separator
                const rows = [];
                while (i < lines.length && lines[i].includes('|')) {
                    const cols = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
                    if (cols.length) rows.push(cols);
                    i += 1;
                }
                const headHtml = `<tr>${header.map((h) => `<th>${this.formatInlineMarkdown(h)}</th>`).join('')}</tr>`;
                const rowHtml = rows.map((r) => `<tr>${r.map((c) => `<td>${this.formatInlineMarkdown(c)}</td>`).join('')}</tr>`).join('');
                blocks.push(`<table class="msg-table"><thead>${headHtml}</thead><tbody>${rowHtml}</tbody></table>`);
                continue;
            }

            blocks.push(`<p class="msg-p">${this.formatInlineMarkdown(line)}</p>`);
            i += 1;
        }

        const traceHtml = this.renderThinkingTraceCard(thinkingTrace || retrieval?.thinking_trace || null, retrieval);
        return `${traceHtml}<div class="msg-rich">${blocks.join('')}</div>`;
    }

    renderThinkingTraceCard(thinkingTrace, retrieval = null) {
        if (!thinkingTrace && !retrieval) return '';
        const trace = thinkingTrace || {};
        const filters = Array.isArray(trace.filters) ? trace.filters : [];
        const contextQueries = Array.isArray(trace?.search_queries?.context) ? trace.search_queries.context : [];
        const messageQueries = Array.isArray(trace?.search_queries?.messages) ? trace.search_queries.messages : [];
        const lexicalTerms = Array.isArray(trace?.search_queries?.lexical) ? trace.search_queries.lexical : [];
        const webQueries = Array.isArray(trace?.search_queries?.web) ? trace.search_queries.web : [];
        const resultHeadline = trace?.results_summary?.headline || 'No retrieval results were available.';
        const resultDetails = Array.isArray(trace?.results_summary?.details) ? trace.results_summary.details : [];
        const dataSources = Array.isArray(trace?.memory_sources) ? trace.memory_sources : (Array.isArray(trace?.data_sources) ? trace.data_sources : []);
        const webSources = Array.isArray(trace?.web_sources) ? trace.web_sources : [];
        const seedResults = Array.isArray(trace?.seed_results) ? trace.seed_results.slice(0, 4) : [];
        const primaryNodes = Array.isArray(trace?.primary_nodes) ? trace.primary_nodes.slice(0, 4) : [];
        const supportNodes = Array.isArray(trace?.support_nodes) ? trace.support_nodes.slice(0, 4) : [];
        const evidenceNodes = Array.isArray(trace?.evidence_nodes) ? trace.evidence_nodes.slice(0, 4) : [];
        const expandedResults = Array.isArray(trace?.graph_expansion_results) ? trace.graph_expansion_results.slice(0, 5) : [];
        const webResults = Array.isArray(trace?.web_results_summary) ? trace.web_results_summary.slice(0, 4) : [];
        const temporalReasoning = Array.isArray(trace?.temporal_reasoning) ? trace.temporal_reasoning : [];
        const router = trace?.router || retrieval?.router || {};
        const stageTrace = Array.isArray(trace?.stage_trace) ? trace.stage_trace : (Array.isArray(retrieval?.stage_trace) ? retrieval.stage_trace : []);
        const strategy = trace?.strategy || {};
        const layers = Array.isArray(trace?.layers) ? trace.layers : [];
        const maxDepth = layers.includes("raw") || layers.includes("event") ? 3 : (layers.includes("episode") ? 2 : (layers.includes("insight") || layers.includes("semantic") ? 1 : 0));
        const depthLabels = ["Core", "Insight", "Episode", "Raw"];
        const penetrationHtml = `
            <div class="penetration-breadcrumb">
                ${depthLabels.map((label, idx) => {
                    const active = idx <= maxDepth;
                    return `<span class="penetration-node ${active ? "active" : ""}">${this.escapeHtml(label)}</span>`;
                }).join(`<span class="penetration-sep">/</span>`)}
            </div>
        `;
        const summaryBits = [];

        if (trace?.thinking_summary) summaryBits.push(trace.thinking_summary);
        if (strategy?.summary_vs_raw) summaryBits.push(`Mode: ${strategy.summary_vs_raw}`);
        if (trace?.applied_date_range?.start && trace?.applied_date_range?.end) {
            summaryBits.push(`Window: ${trace.applied_date_range.start} -> ${trace.applied_date_range.end}`);
        }
        if (trace?.date_filter_status && trace.date_filter_status !== "not_used") {
            summaryBits.push(`Filter ${trace.date_filter_status}`);
        }
        if (trace?.results_summary?.seed_count !== undefined) {
            summaryBits.push(`${trace.results_summary.seed_count} Seeds`);
        }
        if (trace?.results_summary?.evidence_count !== undefined) {
            summaryBits.push(`${trace.results_summary.evidence_count} Evidence`);
        }
        if (strategy?.recursion_depth > 0) {
            summaryBits.push(`Deep Expansion`);
        }

        const filterHtml = filters.length
            ? filters.map((item) => `<div class="thinking-trace-row"><span>${this.escapeHtml(item.label || 'Filter')}</span><span>${this.escapeHtml(item.value || '')}</span></div>`).join('')
            : '<div class="thinking-trace-empty">No explicit filters were applied.</div>';
        const queryHtml = (contextQueries.length || messageQueries.length || lexicalTerms.length)
            ? `
                ${contextQueries.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Context</div><div class="thinking-trace-list">${contextQueries.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
                ${messageQueries.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Messages</div><div class="thinking-trace-list">${messageQueries.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
                ${webQueries.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Web</div><div class="thinking-trace-list">${webQueries.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
                ${lexicalTerms.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Lexical</div><div class="thinking-trace-list">${lexicalTerms.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
            `
            : '<div class="thinking-trace-empty">No semantic queries were needed.</div>';
        const resultHtml = `
            <div class="thinking-trace-result-headline">${this.escapeHtml(resultHeadline)}</div>
            ${resultDetails.length ? `<div class="thinking-trace-list">${resultDetails.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div>` : ''}
            ${primaryNodes.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Primary nodes</div><div class="thinking-trace-list">${primaryNodes.map((item) => `<div>${this.escapeHtml(item.title || item.id)}${item.reason ? ` — ${this.escapeHtml(item.reason)}` : ''}</div>`).join('')}</div></div>` : ''}
            ${seedResults.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Seed results</div><div class="thinking-trace-list">${seedResults.map((item) => `<div>${this.escapeHtml(item.title || item.id)}${item.reason ? ` — ${this.escapeHtml(item.reason)}` : ''}</div>`).join('')}</div></div>` : ''}
            ${supportNodes.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Support nodes</div><div class="thinking-trace-list">${supportNodes.map((item) => `<div>${this.escapeHtml(item.title || item.id)}</div>`).join('')}</div></div>` : ''}
            ${evidenceNodes.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Evidence nodes</div><div class="thinking-trace-list">${evidenceNodes.map((item) => `<div>${this.escapeHtml(item.title || item.id)}</div>`).join('')}</div></div>` : ''}
            ${expandedResults.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Edge expansion</div><div class="thinking-trace-list">${expandedResults.map((item) => `<div>${this.escapeHtml(item.title || item.id)}</div>`).join('')}</div></div>` : ''}
            ${trace?.web_search_used && webResults.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Web results</div><div class="thinking-trace-list">${webResults.map((item) => `<div>${this.escapeHtml(item.title || item.url || '')}${item.url ? ` — ${this.escapeHtml(item.url)}` : ''}</div>`).join('')}</div></div>` : ''}
        `;
        const routerHtml = `
            <div class="thinking-trace-body">${this.escapeHtml(router?.router_reason || trace?.thinking_summary || 'No retrieval summary available.')}</div>
            <div class="thinking-trace-list" style="margin-top:8px;">
                ${router?.source_mode ? `<div>Source mode: ${this.escapeHtml(router.source_mode)}</div>` : ''}
                ${router?.summary_vs_raw ? `<div>Summary mode: ${this.escapeHtml(router.summary_vs_raw)}</div>` : ''}
                ${router?.time_scope?.label ? `<div>Time scope: ${this.escapeHtml(router.time_scope.label)}</div>` : ''}
            </div>
        `;
        const sourcesHtml = (dataSources.length || webSources.length)
            ? `
                ${dataSources.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Memory</div><div class="thinking-trace-chips">${dataSources.map((item) => `<span class="thinking-trace-chip">${this.escapeHtml(item)}</span>`).join('')}</div></div>` : ''}
                ${webSources.length ? `<div class="thinking-trace-subgroup"><div class="thinking-trace-subtitle">Web</div><div class="thinking-trace-list">${webSources.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
            `
            : '<div class="thinking-trace-empty">No supporting data sources were identified.</div>';
        const stageHtml = stageTrace.length
            ? `<div class="thinking-trace-list">${stageTrace.map((item) => `<div>${this.escapeHtml(item.label || item.step)} — ${this.escapeHtml(item.status || 'completed')}${item.detail ? `: ${this.escapeHtml(item.detail)}` : ''}</div>`).join('')}</div>`
            : '<div class="thinking-trace-empty">No pipeline stages were recorded.</div>';
        const strategyHtml = `
            <div class="thinking-trace-body">${this.escapeHtml(trace?.thinking_summary || 'No retrieval summary available.')}</div>
            <div class="thinking-trace-list" style="margin-top:8px;">
                ${strategy?.strategy_mode ? `<div>Strategy mode: ${this.escapeHtml(strategy.strategy_mode)}</div>` : ''}
                ${strategy?.summary_vs_raw ? `<div>Summary mode: ${this.escapeHtml(strategy.summary_vs_raw)}</div>` : ''}
                ${strategy?.time_scope ? `<div>Time scope: ${this.escapeHtml(strategy.time_scope)}</div>` : ''}
                ${Array.isArray(strategy?.app_scope) && strategy.app_scope.length ? `<div>App scope: ${this.escapeHtml(strategy.app_scope.join(', '))}</div>` : ''}
                ${Array.isArray(strategy?.source_scope) && strategy.source_scope.length ? `<div>Source scope: ${this.escapeHtml(strategy.source_scope.join(', '))}</div>` : ''}
                ${strategy?.web_gate_reason ? `<div>Web gate: ${this.escapeHtml(strategy.web_gate_reason)}</div>` : ''}
                ${trace?.answer_basis ? `<div>Answer basis: ${this.escapeHtml(trace.answer_basis)}</div>` : ''}
                ${temporalReasoning.map((item) => `<div>${this.escapeHtml(item)}</div>`).join('')}
            </div>
        `;

        return `
            <details class="thinking-trace-card" ${retrieval ? '' : 'open'}>
                <summary>
                    <span>Thinking context</span>
                    <span class="thinking-trace-summary">${this.escapeHtml(summaryBits.join(' • ') || 'Open to inspect the retrieval path.')}</span>
                </summary>
                <div class="thinking-trace-sections">
                    <div class="thinking-trace-penetration">
                        ${penetrationHtml}
                    </div>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Router decision</div>
                        ${routerHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Strategy & intent analysis</div>
                        ${strategyHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Applied filters</div>
                        ${filterHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Generated search queries</div>
                        ${queryHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Ranking / packed context</div>
                        ${resultHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Pipeline stages</div>
                        ${stageHtml}
                    </section>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Source list</div>
                        ${sourcesHtml}
                    </section>
                </div>
            </details>
        `;
    }

    async loadChatSessions() {
        try {
            const fromMain = await window.electronAPI?.getChatSessions?.({ limit: 25 });
            if (Array.isArray(fromMain) && fromMain.length) {
                return fromMain
                    .filter((session) => session && session.id)
                    .map((session) => ({
                        id: session.id,
                        title: session.title || 'New chat',
                        createdAt: Number(session.createdAt || Date.now()),
                        updatedAt: Number(session.updatedAt || session.createdAt || Date.now()),
                        messages: Array.isArray(session.messages) ? session.messages.slice(-120) : []
                    }))
                    .slice(0, 25);
            }
            const key = 'weave_chat_sessions_v2';
            const legacy = `${String.fromCharCode(97, 110, 113, 101, 114)}_chat_sessions_v2`;
            const saved = localStorage.getItem(key) || localStorage.getItem(legacy) || '[]';
            const raw = JSON.parse(saved);
            if (!Array.isArray(raw)) return [];
            return raw
                .filter((session) => session && session.id)
                .map((session) => ({
                    id: session.id,
                    title: session.title || 'New chat',
                    createdAt: Number(session.createdAt || Date.now()),
                    updatedAt: Number(session.updatedAt || session.createdAt || Date.now()),
                    messages: Array.isArray(session.messages) ? session.messages.slice(-120) : []
                }))
                .slice(0, 25);
        } catch (_) {
            return [];
        }
    }

    createSeedChatSession() {
        return {
            id: `chat_${Date.now()}`,
            title: 'New chat',
            createdAt: Date.now(),
            updatedAt: Date.now(),
            messages: []
        };
    }

    saveChatSessions() {
        const trimmed = (this.chatSessions || []).map((session) => ({
            ...session,
            messages: (session.messages || []).slice(-120)
        })).slice(0, 25);
        localStorage.setItem('weave_chat_sessions_v2', JSON.stringify(trimmed));
    try { if (window.electronAPI?.saveChatSessions) window.electronAPI.saveChatSessions(trimmed); } catch (e) { console.warn('Failed to save chat sessions in main DB:', e); }
    // Push a snapshot to main process for long-term memory ingestion (best-effort)
    try { if (window.electronAPI?.saveChatSessionsToMemory) window.electronAPI.saveChatSessionsToMemory(trimmed); } catch (e) { console.warn('Failed to push chat snapshot to main:', e); }
    }

    getActiveChat() {
        if (!this.activeChatId) return null;
        return this.chatSessions.find((session) => session.id === this.activeChatId) || null;
    }

    createNewChatSession(seed = '') {
        const chat = {
            id: `chat_${Date.now()}`,
            title: seed ? seed.slice(0, 30) : 'New chat',
            createdAt: Date.now(),
            updatedAt: Date.now(),
            messages: []
        };
        this.chatSessions.unshift(chat);
        this.activeChatId = chat.id;
        this.saveChatSessions();
        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();
        this.chatInput?.focus();
        return chat;
    }

    startNewChatDraft() {
        this.activeChatId = null;
        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();
        this.chatInput?.focus();
    }

    setActiveChat(chatId) {
        if (!this.chatSessions.some((session) => session.id === chatId)) return;
        this.activeChatId = chatId;
        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();

        // Respond to main process requests to flush chat sessions to memory
    }

    pushMessageToActiveChat(role, content, retrieval = null, thinkingTrace = null) {
        const chat = this.getActiveChat();
        if (!chat) return;
        if (!chat.messages.length && role === 'user') {
            chat.title = content.slice(0, 30);
        }
        chat.messages.push({ role, content, retrieval, thinking_trace: thinkingTrace, ts: Date.now() });
        chat.updatedAt = Date.now();
        this.saveChatSessions();
    }

    renderChatHistory() {
        if (!this.chatHistoryList) return;
        const sorted = [...(this.chatSessions || [])].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
        if (!sorted.length) {
            this.chatHistoryList.innerHTML = `
                <div class="chat-history-empty">No chats yet</div>
            `;
            return;
        }
        this.chatHistoryList.innerHTML = sorted.map((session) => {
            const last = session.messages?.[session.messages.length - 1];
            return `
                <button class="chat-history-item ${session.id === this.activeChatId ? 'active' : ''}" data-chat-id="${this.escapeHtml(session.id)}">
                    <div class="chat-history-title">${this.escapeHtml(session.title || 'New chat')}</div>
                    <div class="chat-history-preview">${this.escapeHtml((last?.content || 'No messages yet').slice(0, 60))}</div>
                </button>
            `;
        }).join('');

        this.chatHistoryList.querySelectorAll('.chat-history-item').forEach((item) => {
            item.addEventListener('click', () => this.setActiveChat(item.dataset.chatId));
        });
    }

    renderActiveChat() {
        if (!this.chatMessages) return;
        const chat = this.getActiveChat();
        if (!chat) {
            if (this.chatThreadTitle) this.chatThreadTitle.textContent = 'New Chat';
            this.chatMessages.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 60px 20px;">
                    <div class="empty-icon" style="font-size: 48px; opacity: 0.3; margin-bottom: 16px;">
                        <span class="material-symbols-outlined">forum</span>
                    </div>
                    <div class="empty-text" style="font-size: 16px; font-weight: 500; color: var(--text-secondary); margin-bottom: 8px;">Type a message to start a new chat</div>
                    <div class="empty-sub" style="font-size: 13px; color: var(--text-tertiary); line-height: 1.5;">A new session is created only when your first message is sent</div>
                </div>
            `;
            return;
        }
        if (this.chatThreadTitle) this.chatThreadTitle.textContent = chat.title || 'Current Chat';

        this.chatMessages.innerHTML = '';
        if (!chat.messages?.length) {
            const prompts = this.getChatStarterPrompts();
            this.chatMessages.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 60px 20px;">
                    <div class="empty-icon" style="font-size: 48px; opacity: 0.3; margin-bottom: 16px;">
                        <span class="material-symbols-outlined">forum</span>
                    </div>
                    <div class="empty-text" style="font-size: 16px; font-weight: 500; color: var(--text-secondary); margin-bottom: 8px;">Start a conversation</div>
                    <div class="empty-sub" style="font-size: 13px; color: var(--text-tertiary); line-height: 1.5;">Ask about your work, context, or next steps</div>
                    <div style="margin-top:16px; display:flex; flex-wrap:wrap; gap:8px; justify-content:center;">
                        ${prompts.map((p, idx) => `<button class="pill-btn" data-starter-prompt="${idx}" type="button">${this.escapeHtml(p)}</button>`).join('')}
                    </div>
                </div>
            `;
            this.chatMessages.querySelectorAll('[data-starter-prompt]').forEach((btn) => {
                btn.addEventListener('click', () => {
                    const idx = Number(btn.getAttribute('data-starter-prompt'));
                    const prompt = prompts[idx];
                    if (!prompt) return;
                    this.chatInput.value = prompt;
                    this.sendChatMessage();
                });
            });
            return;
        }

        chat.messages.forEach((message) => {
            this.appendChatMessage(message.role, message);
        });
        this.scrollChatToBottom();
    }

    getChatStarterPrompts() {
        const prompts = [];
        const todos = this.getVisibleTodos().slice(0, 3);
        if (todos[0]) prompts.push(`What is the single next step for "${todos[0].title}"?`);
        const brief = (this.morningBriefs || [])[0];
        const firstPriority = brief?.priorities?.[0]?.title;
        if (firstPriority) prompts.push(`Help me execute this priority fast: "${firstPriority}".`);
        prompts.push('What should I focus on today based on my latest context?');
        prompts.push('What is one thing I can close in the next 30 minutes?');
        return Array.from(new Set(prompts)).slice(0, 3);
    }

    setupSettings() {
        this.captureToggle = document.getElementById("capture-toggle");
        this.captureToggle?.addEventListener("click", async () => {
            this.captureToggle.classList.toggle("active");
            const enabled = this.captureToggle.classList.contains("active");
            await this.setDesktopCaptureEnabled(enabled);
        });

        // Delete all data button (factory reset)
        this.deleteAllDataButton?.addEventListener('click', async () => {
            try {
                const ok = confirm('Delete all local data and reset the app? This will relaunch the app.');
                if (!ok) return;
                this.deleteAllDataButton.disabled = true;
                this.deleteAllDataButton.textContent = 'Deleting...';
                if (window.electronAPI?.deleteAllSettings) {
                    await window.electronAPI.deleteAllSettings();
                    // main process will relaunch/exit; if it returns, notify user
                    this.showToast('All data deleted; app will relaunch');
                } else {
                    // Fallback: clear local storage and persist empty suggestions
                    try { localStorage.clear(); } catch (_) {}
                    try { await window.electronAPI?.clearSuggestions?.(); } catch (_) {}
                    this.showToast('Local data cleared (fallback)');
                    setTimeout(() => location.reload(), 800);
                }
            } catch (error) {
                console.error('Delete all data failed:', error);
                this.showToast('Delete failed');
            } finally {
                try { this.deleteAllDataButton.disabled = false; this.deleteAllDataButton.textContent = 'Delete all data'; } catch (_) {}
            }
        });
    }

    syncSettingsUI() {
        const currentTheme = localStorage.getItem('theme') || 'light';
        document.getElementById('dark-mode-toggle')?.classList.toggle('active', currentTheme === 'dark');
        document.getElementById('compact-view-toggle')?.classList.toggle('active', this.compactMode);
        document.getElementById('notifications-toggle')?.classList.toggle('active', this.notificationsEnabled);
        document.getElementById('sound-toggle')?.classList.toggle('active', this.soundEnabled);
        document.getElementById('capture-toggle')?.classList.toggle('active', this.desktopCaptureEnabled);
    }

    async loadSettingsData() {
        await Promise.all([
            this.loadGoogleStatus(),
            this.loadExtensionStatus(),
            this.loadVoiceControlStatus(),
            this.loadBrowserHistory(),
            this.loadSensorData(),
            this.loadMemoryGraphStatus(),
            this.loadSuggestionProviderSettings()
        ]);
        this.renderDesktopTimeline();
    }

    renderSuggestionProviderStatus(settings = null) {
        if (!this.suggestionProviderStatus) return;
        const provider = String(settings?.provider || this.suggestionProviderSelect?.value || 'deepseek').toLowerCase();
        const model = String(settings?.model || this.suggestionModelInput?.value || '').trim();
        const hasApiKey = Boolean(settings?.hasApiKey || String(this.suggestionApiKeyInput?.value || '').trim());
        const baseUrl = String(settings?.baseUrl || this.suggestionBaseUrlInput?.value || '').trim();
        if (provider === 'ollama') {
            this.suggestionProviderStatus.textContent = `Provider: Ollama • Model: ${model || 'llama3.1:8b'} • URL: ${baseUrl || 'http://127.0.0.1:11434'}${hasApiKey ? ' • key set' : ''}`;
            return;
        }
        this.suggestionProviderStatus.textContent = `Provider: DeepSeek • Model: ${model || 'deepseek-chat'}${hasApiKey ? ' • key set' : ' • missing key'}`;
    }

    async loadSuggestionProviderSettings() {
        if (!window.electronAPI?.getSuggestionLLMSettings) return;
        try {
            const settings = await window.electronAPI.getSuggestionLLMSettings();
            if (this.suggestionProviderSelect) this.suggestionProviderSelect.value = settings?.provider || 'deepseek';
            if (this.suggestionModelInput) this.suggestionModelInput.value = settings?.model || '';
            if (this.suggestionBaseUrlInput) this.suggestionBaseUrlInput.value = settings?.baseUrl || '';
            if (this.suggestionApiKeyInput) this.suggestionApiKeyInput.value = '';
            if (this.suggestionBaseUrlInput) this.suggestionBaseUrlInput.disabled = (settings?.provider || 'deepseek') !== 'ollama';
            this.renderSuggestionProviderStatus(settings);
        } catch (error) {
            console.error('Failed to load suggestion provider settings:', error);
            if (this.suggestionProviderStatus) {
                this.suggestionProviderStatus.textContent = 'Suggestion provider settings failed to load';
            }
        }
    }

    async loadBrowserHistory() {
        if (!this.historyList) return;
        this.historyList.innerHTML = '<div class="history-row"><div class="history-row-title">Loading browser history...</div></div>';

        try {
            const data = await window.electronAPI.getExtensionData();
            const urls = Array.isArray(data?.urls) ? data.urls : [];
            const recent = urls
                .filter((item) => item && (item.url || item.title))
                .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
                .slice(0, 100);

            if (this.historyMeta) {
                this.historyMeta.textContent = recent.length
                    ? `${recent.length} recent pages captured`
                    : 'No extension browsing data captured yet';
            }

            if (!recent.length) {
                this.historyList.innerHTML = '<div class="history-row"><div class="history-row-title">No browser history found</div><div class="history-row-meta">Open Chrome with the extension enabled to populate this list.</div></div>';
                return;
            }

            this.historyList.innerHTML = recent.map((item) => `
                <div class="history-row">
                    <div class="history-row-title">${this.escapeHtml(item.title || item.url || 'Untitled')}</div>
                    <div class="history-row-meta">
                        <span>${this.escapeHtml(item.domain || this.safeDomain(item.url) || 'unknown')}</span>
                        <span>•</span>
                        <span>${this.friendlyTime(item.timestamp)}</span>
                    </div>
                </div>
            `).join('');
        } catch (error) {
            console.error('Failed to load browser history:', error);
            this.historyList.innerHTML = '<div class="history-row"><div class="history-row-title">Failed to load browser history</div></div>';
        }
    }

    async setDesktopCaptureEnabled(enabled) {
        try {
            await window.electronAPI.saveSensorSettings({ enabled, intervalMinutes: 0.5 });
            this.desktopCaptureEnabled = Boolean(enabled);
            localStorage.setItem('desktopCaptureEnabled', String(this.desktopCaptureEnabled));
            this.showToast(`Desktop capture ${enabled ? 'enabled' : 'disabled'}`);
            await this.loadSensorData();
        } catch (error) {
            console.error('Failed to update desktop capture setting:', error);
            this.showToast('Failed to update desktop capture');
            document.getElementById('capture-toggle')?.classList.toggle('active', this.desktopCaptureEnabled);
        }
    }

    async captureNow() {
        try {
            await window.electronAPI.captureSensorSnapshot();
            this.showToast('Capture requested');
            setTimeout(() => this.loadSensorData(), 1200);
        } catch (error) {
            console.error('Capture failed:', error);
            this.showToast('Manual capture failed');
        }
    }


    async runDesktopPromptTask() {
        const prompt = String(this.desktopPromptInput?.value || '').trim();
        if (!prompt) {
            this.setDesktopPromptStatus('Enter a desktop command first.');
            this.showToast('Enter a desktop prompt first');
            return;
        }

        if (this.desktopPromptRunButton) {
            this.desktopPromptRunButton.disabled = true;
            this.desktopPromptRunButton.textContent = 'Running...';
        }
        this.setDesktopPromptStatus('Starting desktop prompt…');
        this.setDesktopTestStatus(`Prompt run: ${prompt}`);

        try {
            const status = await window.electronAPI.getAccessibilityStatus();
            if (!status?.trusted) throw new Error(status?.error || 'Accessibility permission is not enabled');

            const result = await window.electronAPI.executeAITask({
                id: `settings_prompt_${Date.now()}`,
                title: prompt,
                description: `Settings desktop prompt: ${prompt}`,
                raw_goal: prompt,
                ai_draft: prompt,
                source: 'settings_prompt',
                execution_mode: 'settings_prompt',
                agentMode: true
            });

            const summary = result?.result || 'desktop action completed';
            this.setDesktopTestStatus(`Result: ${String(summary).slice(0, 200)}`);
            this.showToast('Desktop prompt completed');
        } catch (err) {
            console.error('Desktop prompt failed:', err);
            this.showToast('Desktop prompt failed');
        } finally {
            if (this.desktopPromptRunButton) {
                this.desktopPromptRunButton.disabled = false;
                this.desktopPromptRunButton.textContent = 'Run';
            }
        }
    }

    setDesktopPromptStatus(text) {
        if (this.desktopPromptStatus) this.desktopPromptStatus.textContent = text;
    }

    updateDiscoveryOrbit(payload = {}) {
        if (!this.discoveryOrbitContainer) return;
        const isThinking = false;
        if (isThinking) {
            this.discoveryOrbitContainer.classList.remove("hidden");
            if (this.discoveryOrbitLabel) {
                const action = payload.action ? "Thinking: " + payload.action.replace(/_/g, " ") + "..." : "Discovery Thinking...";
                this.discoveryOrbitLabel.textContent = action;
            }
        } else {
            this.discoveryOrbitContainer.classList.add("hidden");
        }
    }

    async openFullControlSettings() {
        const button = document.getElementById('allow-full-control-btn');
        if (button) button.textContent = 'Opening...';
        try {
            const result = await window.electronAPI.openAccessibilitySettings();
            if (result?.status === 'error') {
                throw new Error(result.error || 'Unable to open Accessibility settings');
            }
            this.showToast('Opened Accessibility settings');
            setTimeout(() => this.loadExtensionStatus(), 1200);
        } catch (error) {
            console.error('Failed to open Accessibility settings:', error);
            this.showToast('Failed to open Accessibility settings');
        } finally {
            if (button) button.textContent = 'Allow Full Control';
        }
    }

    async loadExtensionStatus() {
        const el = document.getElementById('extension-status-text');
        const button = document.getElementById('allow-full-control-btn');
        if (!el) return;
        try {
            const status = await window.electronAPI.getAccessibilityStatus();
            if (!status?.trusted) {
                el.textContent = 'Accessibility permission is not enabled. Enable full control in System Settings to allow AI actions on your Mac.';
                if (this.desktopPerceptionMode) this.desktopPerceptionMode.textContent = 'Current mode: unavailable until full control is enabled';
                if (button) button.disabled = false;
                return;
            }
            const frontmost = status.frontmostApp ? ` • frontmost ${status.frontmostApp}` : '';
            const mode = status?.adaptiveVision ? ' • adaptive vision active' : '';
            const managed = status?.managedBrowser?.running ? ' • managed Chrome ready' : '';
            el.textContent = `Full control enabled${frontmost}${mode}${managed}`;
            if (this.desktopPerceptionMode) {
                const perception = String(status?.perceptionMode || 'ax_only').replace(/_/g, ' ');
                const managedMode = status?.managedBrowser?.running ? ' • Managed Chrome (CDP)' : '';
                this.desktopPerceptionMode.textContent = `Current mode: ${perception}${managedMode}`;
            }
            if (button) button.disabled = true;
        } catch (error) {
            console.error('Failed to read accessibility status:', error);
            el.textContent = 'Status unavailable';
            if (this.desktopPerceptionMode) this.desktopPerceptionMode.textContent = 'Perception mode unavailable';
            if (button) button.disabled = false;
        }
    }

    async loadVoiceControlStatus() {
        try {
            const status = await window.electronAPI.getVoiceControlStatus();
            this.voiceControlEnabled = Boolean(status?.enabled);
            this.voiceControlToggle?.classList.toggle('active', this.voiceControlEnabled);
            if (this.voiceShortcutValue) {
                const shortcut = status?.shortcut || 'Command/Ctrl + Shift + Space';
                this.voiceShortcutValue.textContent = `${shortcut} • Works globally with a floating HUD`;
            }
            const micStatus = await this.getMicrophonePermissionState();
            if (this.voiceControlStatus) {
                const registration = status?.registered ? 'ready' : 'not registered';
                const hudMode = status?.hud_mode === 'floating' ? 'floating HUD' : 'in-app overlay';
                const engine = status?.speech_engine === 'native_local_first'
                    ? 'Local speech first'
                    : (status?.speech_engine === 'openai_cloud' ? 'OpenAI STT fallback' : 'Speech engine unavailable');
                this.voiceControlStatus.textContent = `Shortcut ${registration} • Mic ${micStatus} • ${engine} • ${hudMode}`;
            }
        } catch (error) {
            console.error('Failed to load voice control status:', error);
            if (this.voiceControlStatus) this.voiceControlStatus.textContent = 'Voice control unavailable';
        }
    }

    async setVoiceControlEnabled(enabled) {
        try {
            const status = await window.electronAPI.setVoiceControlEnabled(enabled);
            this.voiceControlEnabled = Boolean(status?.enabled);
            this.voiceControlToggle?.classList.toggle('active', this.voiceControlEnabled);
            await this.loadVoiceControlStatus();
            this.showToast(`Voice control ${this.voiceControlEnabled ? 'enabled' : 'disabled'}`);
        } catch (error) {
            console.error('Failed to update voice control:', error);
            this.showToast('Failed to update voice control');
        }
    }

    async getMicrophonePermissionState() {
        try {
            if (!navigator.permissions?.query) return 'unknown';
            const permission = await navigator.permissions.query({ name: 'microphone' });
            return permission?.state || 'unknown';
        } catch (_) {
            return 'unknown';
        }
    }

    async handleVoiceCommandToggle(payload = {}) {
        if (payload.action === 'start') {
            await this.startVoiceCapture(payload.session || {});
        } else if (payload.action === 'stop') {
            await this.stopVoiceCapture();
        }
    }

    getSpeechRecognitionCtor() {
        return window.SpeechRecognition || window.webkitSpeechRecognition || null;
    }

    async startNativeVoiceRecognition(session = {}) {
        const SpeechRecognitionCtor = this.getSpeechRecognitionCtor();
        if (!SpeechRecognitionCtor) return false;

        this.voiceSession = session;
        this.voiceRecognitionStartedAt = Date.now();
        this.voicePartialTranscript = '';
        this.voiceFinalTranscript = '';
        this.voiceRecognitionMode = 'native_local';

        try {
            const recognition = new SpeechRecognitionCtor();
            recognition.lang = 'en-US';
            recognition.continuous = true;
            recognition.interimResults = true;
            recognition.maxAlternatives = 1;

            recognition.onresult = async (event) => {
                let finalTranscript = this.voiceFinalTranscript || '';
                let interimTranscript = '';
                for (let index = event.resultIndex; index < event.results.length; index += 1) {
                    const chunk = String(event.results[index]?.[0]?.transcript || '');
                    if (!chunk) continue;
                    if (event.results[index].isFinal) finalTranscript += `${chunk} `;
                    else interimTranscript += chunk;
                }
                this.voiceFinalTranscript = finalTranscript.trim();
                this.voicePartialTranscript = `${this.voiceFinalTranscript} ${interimTranscript}`.trim();
                this.updateVoiceOverlay({
                    title: 'Listening…',
                    status: 'Speak your instruction. Press the shortcut again to stop.',
                    transcript: this.voicePartialTranscript || 'Listening for your command…',
                    meta: 'Local speech'
                });
                await window.electronAPI.updateVoiceSessionTranscript?.({
                    sessionId: this.voiceSession?.id,
                    partial_transcript: this.voicePartialTranscript,
                    engine: 'native_local',
                    latency_ms: Date.now() - this.voiceRecognitionStartedAt
                });
            };

            recognition.onerror = async (event) => {
                const error = String(event?.error || 'native speech recognition failed');
                if (/not-allowed|service-not-allowed|audio-capture/i.test(error)) {
                    await window.electronAPI.voiceCaptureFailed({
                        sessionId: this.voiceSession?.id,
                        error: /not-allowed/i.test(error) ? 'Microphone permission denied' : error
                    });
                }
            };

            recognition.onend = async () => {
                if (this.voiceRecognition !== recognition) return;
                const transcript = String(this.voiceFinalTranscript || this.voicePartialTranscript || '').trim();
                this.voiceRecognition = null;
                this.voiceRecognitionMode = null;
                if (!transcript) {
                    await window.electronAPI.voiceCaptureFailed({
                        sessionId: this.voiceSession?.id,
                        error: 'Could not hear anything'
                    });
                    return;
                }
                await window.electronAPI.submitVoiceTranscript?.({
                    sessionId: this.voiceSession?.id,
                    transcript,
                    partial_transcript: this.voicePartialTranscript,
                    engine: 'native_local',
                    latency_ms: Date.now() - this.voiceRecognitionStartedAt
                });
            };

            this.voiceRecognition = recognition;
            recognition.start();
            return true;
        } catch (error) {
            console.error('Native speech recognition failed to start:', error);
            this.voiceRecognition = null;
            this.voiceRecognitionMode = null;
            return false;
        }
    }

    async startVoiceCapture(session = {}) {
        if ((this.voiceRecorder && this.voiceRecorder.state === 'recording') || this.voiceRecognition) return;
        this.voiceSession = session;
        this.voiceChunks = [];
        this.voicePartialTranscript = '';
        this.voiceFinalTranscript = '';
        this.updateVoiceOverlay({
            title: 'Listening…',
            status: 'Speak your instruction. Press the shortcut again to stop.',
            transcript: 'Listening for your command…',
            meta: session?.shortcut || ''
        });
        const startedNative = await this.startNativeVoiceRecognition(session);
        if (startedNative) return;
        try {
            this.voiceStream = await navigator.mediaDevices.getUserMedia({ audio: true });
            const mimeType = MediaRecorder.isTypeSupported('audio/webm;codecs=opus')
                ? 'audio/webm;codecs=opus'
                : 'audio/webm';
            this.voiceRecorder = new MediaRecorder(this.voiceStream, { mimeType });
            this.voiceRecorder.ondataavailable = (event) => {
                if (event.data?.size) this.voiceChunks.push(event.data);
            };
            this.voiceRecorder.onstop = async () => {
                try {
                    const blob = new Blob(this.voiceChunks, { type: this.voiceRecorder?.mimeType || mimeType });
                    const buffer = await blob.arrayBuffer();
                    const bytes = new Uint8Array(buffer);
                    let binary = '';
                    for (let index = 0; index < bytes.byteLength; index += 1) binary += String.fromCharCode(bytes[index]);
                    await window.electronAPI.submitVoiceAudio({
                        sessionId: this.voiceSession?.id,
                        mimeType: this.voiceRecorder?.mimeType || mimeType,
                        audioBase64: btoa(binary)
                    });
                } catch (error) {
                    console.error('Voice audio submission failed:', error);
                    await window.electronAPI.voiceCaptureFailed({
                        sessionId: this.voiceSession?.id,
                        error: error?.message || 'Voice submission failed'
                    });
                } finally {
                    this.cleanupVoiceStream();
                }
            };
            this.voiceRecorder.start();
        } catch (error) {
            console.error('Voice capture failed:', error);
            await window.electronAPI.voiceCaptureFailed({
                sessionId: this.voiceSession?.id,
                error: /denied|permission/i.test(error?.message || '') ? 'Microphone permission denied' : (error?.message || 'Voice capture failed')
            });
            this.cleanupVoiceStream();
        }
    }

    async stopVoiceCapture() {
        if (this.voiceRecognition) {
            this.updateVoiceOverlay({
                title: 'Transcribing…',
                status: 'Turning your speech into a command.',
                transcript: this.voicePartialTranscript || this.voiceOverlayTranscript?.textContent || 'Processing speech…',
                meta: 'Local speech'
            });
            try {
                this.voiceRecognition.stop();
            } catch (_) {}
            return;
        }
        if (this.voiceRecorder && this.voiceRecorder.state === 'recording') {
            this.updateVoiceOverlay({
                title: 'Transcribing…',
                status: 'Turning your speech into a command.',
                transcript: this.voiceOverlayTranscript?.textContent || 'Processing audio…',
                meta: ''
            });
            this.voiceRecorder.stop();
        }
    }

    cleanupVoiceStream() {
        if (this.voiceRecognition) {
            try {
                this.voiceRecognition.onresult = null;
                this.voiceRecognition.onerror = null;
                this.voiceRecognition.onend = null;
                this.voiceRecognition.stop();
            } catch (_) {}
        }
        this.voiceRecognition = null;
        this.voiceRecognitionMode = null;
        if (this.voiceStream) {
            this.voiceStream.getTracks().forEach((track) => track.stop());
        }
        this.voiceStream = null;
        this.voiceRecorder = null;
        this.voiceChunks = [];
    }

    handleVoiceSessionUpdate(payload) {
        this.voiceSession = payload || null;
        if (!payload) {
            this.hideVoiceOverlay();
            return;
        }
        const transcript = payload.transcript || 'Waiting for speech…';
        if (payload.status === 'listening') {
            this.updateVoiceOverlay({
                title: 'Listening…',
                status: 'Speak your instruction. Press the shortcut again to stop.',
                transcript: payload.partial_transcript || transcript,
                meta: payload.shortcut || ''
            });
            return;
        }
        if (payload.status === 'transcribing') {
            this.updateVoiceOverlay({
                title: 'Transcribing…',
                status: 'Turning your speech into text.',
                transcript: payload.partial_transcript || transcript,
                meta: payload.engine === 'native_local' ? 'Local speech' : ''
            });
            return;
        }
        if (payload.status === 'acting') {
            this.updateVoiceOverlay({
                title: 'Acting…',
                status: 'The AI agent is now handling your command autonomously.',
                transcript,
                meta: payload.transcription_meta?.provider === 'openai_cloud' ? 'OpenAI STT fallback' : 'Local speech'
            });
            return;
        }
        if (payload.status === 'completed') {
            this.updateVoiceOverlay({
                title: 'Done',
                status: payload.result || 'Voice command completed.',
                transcript,
                meta: ''
            });
            setTimeout(() => this.hideVoiceOverlay(), 2200);
            return;
        }
        if (payload.status === 'failed') {
            this.updateVoiceOverlay({
                title: 'Voice Failed',
                status: payload.error || 'Voice command failed.',
                transcript,
                meta: ''
            });
            setTimeout(() => this.hideVoiceOverlay(), 3200);
        }
    }

    updateVoiceOverlay({ title = 'Voice Control', status = '', transcript = '', meta = '' } = {}) {
        if (this.voiceOverlayTitle) this.voiceOverlayTitle.textContent = title;
        if (this.voiceOverlayStatus) this.voiceOverlayStatus.textContent = status;
        if (this.voiceOverlayTranscript) this.voiceOverlayTranscript.textContent = transcript;
        if (this.voiceOverlayMeta) this.voiceOverlayMeta.textContent = meta;
        if (this.voiceOverlay) {
            this.voiceOverlay.classList.remove('hidden');
            this.voiceOverlay.style.display = 'flex';
        }
    }

    hideVoiceOverlay() {
        if (!this.voiceOverlay) return;
        this.voiceOverlay.classList.add('hidden');
        this.voiceOverlay.style.display = 'none';
    }


    async loadSensorData() {
        try {
            const [status, events] = await Promise.all([
                window.electronAPI.getSensorStatus(),
                window.electronAPI.getSensorEvents()
            ]);

            this.desktopCaptureEnabled = Boolean(status?.enabled);
            localStorage.setItem('desktopCaptureEnabled', String(this.desktopCaptureEnabled));
            document.getElementById('capture-toggle')?.classList.toggle('active', this.desktopCaptureEnabled);

            if (this.captureStatusText) {
                const permission = status?.screenPermission ? `Permission: ${status.screenPermission}` : 'Permission unknown';
                const captures = status?.totalCaptures || 0;
                this.captureStatusText.textContent = `${status?.active ? 'Active' : 'Idle'} • ${captures} captures • ${permission}`;
            }

            this.renderScreenCaptures(events || []);
        } catch (error) {
            console.error('Failed to load sensor data:', error);
            if (this.captureStatusText) this.captureStatusText.textContent = 'Failed to read capture status';
            if (this.screenCapturesList) {
                this.screenCapturesList.innerHTML = '<div class="history-row"><div class="history-row-title">No screen captures available</div></div>';
            }
        }
    }

    renderScreenCaptures(events) {
        if (!this.screenCapturesList) return;
        const captures = (Array.isArray(events) ? events : []).slice(0, 40);
        if (!captures.length) {
            this.screenCapturesList.innerHTML = '<div class="history-row"><div class="history-row-title">No captures yet</div><div class="history-row-meta">Turn on Desktop Capture and wait for snapshots.</div></div>';
            return;
        }

        this.screenCapturesList.innerHTML = captures.map((event) => {
            const title = event.activeWindowTitle || event.title || 'Screen capture';
            const app = event.activeApp || event.sourceName || 'Desktop';
            const snippet = (event.text || '').slice(0, 120).trim();
            const imageInfo = event.imagePath ? `<span>${this.escapeHtml(this.basename(event.imagePath))}</span><span>•</span>` : '';
            const exactTime = event.captured_at_local || (event.timestamp ? new Date(event.timestamp).toLocaleString() : 'Unknown time');
            const inSession = Boolean(event.study_context?.in_session || event.study_session_id);
            const studyMeta = inSession
                ? `Study • ${event.study_subject || event.study_context?.subject || 'general'} • ${event.study_signal || 'active'}`
                : '';
            return `
                <div class="history-row">
                    <div class="history-row-title">${this.escapeHtml(title)}</div>
                    <div class="history-row-meta">
                        <span>${this.escapeHtml(app)}</span>
                        <span>•</span>
                        <span>${this.friendlyTime(event.timestamp)}</span>
                    </div>
                    <div class="history-row-meta">
                        <span>${this.escapeHtml(exactTime)}</span>
                    </div>
                    ${studyMeta ? `<div class="history-row-meta">${this.escapeHtml(studyMeta)}</div>` : ''}
                    ${snippet ? `<div class="history-row-meta">${this.escapeHtml(snippet)}</div>` : ''}
                    ${event.imagePath ? `<div class="history-row-meta">${imageInfo}<span>${this.escapeHtml(event.imagePath)}</span></div>` : ''}
                </div>
            `;
        }).join('');
    }

    async loadMemoryGraphStatus() {
        try {
            const status = await window.electronAPI.getMemoryGraphStatus();
            const counts = status?.counts || {};
            const nodeCounts = counts?.nodes || {};
            const totalNodes = Object.values(nodeCounts).reduce((sum, value) => sum + (Number(value) || 0), 0);

            this.setText('mem-count-events', String(counts.events || 0));
            this.setText('mem-count-nodes', String(totalNodes));
            this.setText('mem-count-edges', String(counts.edges || 0));

            const proc = document.getElementById('mem-processing-status');
            const active = Boolean(status?.status?.processingActive);
            if (proc) {
                proc.textContent = active ? 'Processing' : 'Idle';
                proc.classList.toggle('active', active);
                proc.classList.toggle('inactive', !active);
            }

            await this.searchMemoryGraph();
        } catch (error) {
            console.error('Failed to load memory graph status:', error);
            this.setText('mem-processing-status', 'Unavailable');
        }
    }

    async searchMemoryGraph() {
        if (!this.memoryResults) return;
        this.memoryResults.innerHTML = '<div class="history-row"><div class="history-row-title">Searching memory graph...</div></div>';

        const query = (this.memorySearchInput?.value || '').trim();
        const filterType = this.memoryFilterType?.value || 'all';
        const includeRaw = filterType === 'all' || filterType === 'raw_event';
        const nodeTypes = filterType === 'all' || filterType === 'raw_event' ? [] : [filterType];

        try {
            const [nodes, rawEvents] = await Promise.all([
                window.electronAPI.searchMemoryGraph(query, { nodeTypes, limit: 120 }),
                includeRaw ? window.electronAPI.searchRawEvents(query) : Promise.resolve([])
            ]);

            const normalizedNodes = (nodes || []).map((item) => ({
                id: item.id,
                type: item.type,
                data: item.data || {},
                timestamp: item.data?.timestamp || item.data?.date || 0
            }));

            const normalizedEvents = (rawEvents || []).map((event) => ({
                id: event.id,
                type: 'raw_event',
                data: {
                    title: event.text || event.type || 'Raw Event',
                    source: event.source,
                    metadata: event.metadata || {},
                    text: event.text || ''
                },
                timestamp: event.timestamp || 0
            }));

            this.memorySearchResults = [...normalizedNodes, ...normalizedEvents]
                .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime())
                .slice(0, 200);

            this.renderMemoryResults(this.memorySearchResults);
        } catch (error) {
            console.error('Memory graph search failed:', error);
            this.memoryResults.innerHTML = '<div class="history-row"><div class="history-row-title">Memory graph search failed</div></div>';
        }
    }

    renderMemoryResults(results) {
        if (!this.memoryResults) return;
        if (!Array.isArray(results) || !results.length) {
            this.memoryResults.innerHTML = '<div class="history-row"><div class="history-row-title">No matching memory records</div></div>';
            return;
        }

        this.memoryResults.innerHTML = results.map((item, index) => {
            const title = item.data?.title || item.data?.name || item.data?.fact || item.id;
            const body = item.data?.summary || item.data?.narrative || item.data?.text || '';
            const timestamp = item.timestamp ? this.friendlyTime(new Date(item.timestamp).getTime()) : 'No timestamp';
            return `
                <button type="button" class="history-row memory-row-trigger" data-memory-index="${index}">
                    <div class="history-row-title">${this.escapeHtml(title)}</div>
                    <div class="history-row-meta">
                        <span>${this.escapeHtml(item.type)}</span>
                        <span>•</span>
                        <span>${this.escapeHtml(timestamp)}</span>
                    </div>
                    ${body ? `<div class="history-row-meta">${this.escapeHtml(String(body).slice(0, 180))}</div>` : ''}
                </button>
            `;
        }).join('');

        this.memoryResults.querySelectorAll('.memory-row-trigger').forEach((button) => {
            button.addEventListener('click', () => {
                const index = Number(button.dataset.memoryIndex);
                this.openMemoryModal(index);
            });
        });
    }

    openMemoryModal(index) {
        const item = this.memorySearchResults[index];
        if (!item) return;

        const modal = document.getElementById('memory-detail-modal');
        const layer = document.getElementById('modal-memory-layer');
        const title = document.getElementById('modal-memory-title');
        const time = document.getElementById('modal-memory-time');
        const narrative = document.getElementById('modal-memory-narrative');
        const raw = document.getElementById('modal-memory-raw');
        if (!modal || !layer || !title || !time || !narrative || !raw) return;

        layer.textContent = `${String(item.type || 'NODE').toUpperCase()} • MEMORY`;
        title.textContent = item.data?.title || item.data?.name || item.id;
        time.textContent = item.timestamp ? new Date(item.timestamp).toLocaleString() : 'No timestamp';
        narrative.textContent = item.data?.narrative || item.data?.summary || item.data?.text || 'No narrative available.';
        raw.textContent = JSON.stringify(item, null, 2);

        modal.classList.remove('hidden');
        modal.style.display = 'flex';
    }

    closeMemoryModal() {
        const modal = document.getElementById('memory-detail-modal');
        if (!modal) return;
        modal.classList.add('hidden');
        modal.style.display = 'none';
    }

    async loadGoogleStatus() {
        try {
            const tokens = await window.electronAPI.getGoogleTokens();
            if (this.googleStatus) {
                this.googleStatus.textContent = tokens ? 'Connected' : 'Not connected';
            }
            const button = document.getElementById('google-connect-btn');
            if (button) button.textContent = tokens ? 'Reconnect' : 'Connect';
        } catch (error) {
            console.error('Failed to load Google status:', error);
            if (this.googleStatus) this.googleStatus.textContent = 'Unavailable';
        }
    }

    async loadMorningBriefs() {
        try {
            let briefs = await window.electronAPI.getMorningBriefs();
            if (!Array.isArray(briefs) || !briefs.length) {
                await window.electronAPI.generateMorningBrief({ force: false });
                briefs = await window.electronAPI.getMorningBriefs();
            }
            this.morningBriefs = Array.isArray(briefs) ? briefs : [];
            this.activeBriefId = this.morningBriefs[0]?.id || null;
            this.renderSuggestions();
            this.renderMorningBriefList();
            this.showMorningBrief(this.activeBriefId);
        } catch (error) {
            console.error('Failed to load morning briefs:', error);
        }
    }

    openMorningBriefModal(briefId = null) {
        if (!this.morningBriefModal) return;
        if (briefId) this.activeBriefId = briefId;
        if (!this.activeBriefId) this.activeBriefId = this.morningBriefs[0]?.id || null;
        this.renderMorningBriefList();
        this.showMorningBrief(this.activeBriefId);
        this.morningBriefModal.classList.remove('hidden');
        this.morningBriefModal.style.display = 'flex';
    }

    closeMorningBriefModal() {
        if (!this.morningBriefModal) return;
        this.morningBriefModal.classList.add('hidden');
        this.morningBriefModal.style.display = 'none';
    }

    renderMorningBriefList() {
        if (!this.morningBriefList) return;
        if (!this.morningBriefs.length) {
            this.morningBriefList.innerHTML = '<div class="history-row"><div class="history-row-title">No briefs yet</div></div>';
            return;
        }
        this.morningBriefList.innerHTML = this.morningBriefs.map((brief) => `
            <button type="button" class="brief-list-item ${brief.id === this.activeBriefId ? 'active' : ''}" data-brief-id="${this.escapeHtml(brief.id)}">
                <div class="brief-list-item-title">${this.escapeHtml(brief.dateLabel || brief.date || 'Morning brief')}</div>
            </button>
        `).join('');
        this.morningBriefList.querySelectorAll('[data-brief-id]').forEach((btn) => {
            btn.addEventListener('click', () => {
                this.activeBriefId = btn.dataset.briefId;
                this.renderMorningBriefList();
                this.showMorningBrief(this.activeBriefId);
            });
        });
    }

    showMorningBrief(briefId) {
        if (!this.morningBriefContent) return;
        const brief = (this.morningBriefs || []).find((b) => b.id === briefId) || this.morningBriefs[0];
        if (!brief) {
            this.morningBriefContent.textContent = 'No morning brief available yet.';
            return;
        }
        this.activeBriefId = brief.id;
        const priorities = Array.isArray(brief.priorities) ? brief.priorities.slice(0, 3) : [];
        const calendarRows = Array.isArray(brief.calendar) ? brief.calendar.slice(0, 8) : [];
        const rollovers = Array.isArray(brief.rollovers) ? brief.rollovers.slice(0, 8) : [];
        const wins = Array.isArray(brief.wins) ? brief.wins.slice(0, 6) : [];
        const activityLog = Array.isArray(brief.activityLog) ? brief.activityLog.slice(0, 8) : [];
        const articles = Array.isArray(brief.articles) ? brief.articles.slice(0, 4) : [];
        const videos = Array.isArray(brief.videos) ? brief.videos.slice(0, 3) : [];

        const priorityRows = priorities.length
            ? priorities.map((p, idx) => `
                <tr>
                    <td>${idx + 1}</td>
                    <td>${this.escapeHtml(p.title || 'Priority')}</td>
                    <td>${this.escapeHtml(p.task || '')}</td>
                </tr>
              `).join('')
            : '<tr><td colspan="3">No priorities available yet.</td></tr>';

        const renderBullets = (rows, fallback) => rows.length
            ? rows.map((r) => `<li>${this.escapeHtml(String(r))}</li>`).join('')
            : `<li>${this.escapeHtml(fallback)}</li>`;

        const articleLinks = articles.length
            ? articles.map((a) => `<li><a href="${this.escapeHtml(a.url || '#')}" target="_blank" rel="noreferrer">${this.escapeHtml(a.title || 'Article')}</a></li>`).join('')
            : '<li>No articles yet.</li>';
        const videoLinks = videos.length
            ? videos.map((v) => `<li><a href="${this.escapeHtml(v.url || '#')}" target="_blank" rel="noreferrer">${this.escapeHtml(v.title || 'Video')}</a></li>`).join('')
            : '';

        this.morningBriefContent.innerHTML = `
            <h1 class="brief-doc-h1">${this.escapeHtml(brief.dateLabel || brief.date || 'Morning Brief')} ☀️</h1>
            <p class="brief-doc-meta">${this.escapeHtml(brief.quote || '')}</p>

            <h2 class="brief-doc-h2">✅ Top 3 Priorities</h2>
            <p class="brief-doc-p">Start with these core outcomes today:</p>
            <table class="brief-doc-table">
                <thead>
                    <tr><th>#</th><th>Priority</th><th>Suggested task</th></tr>
                </thead>
                <tbody>${priorityRows}</tbody>
            </table>

            <h2 class="brief-doc-h2">📅 Calendar Snapshot</h2>
            <ul class="brief-doc-list">${renderBullets(calendarRows, 'No meetings scheduled today.')}</ul>
            <p class="brief-doc-p"><strong>Prep:</strong> ${this.escapeHtml(brief.calendarPrep || 'Use your first block for deep work.')}</p>

            <h2 class="brief-doc-h2">🔁 Rollovers</h2>
            <ul class="brief-doc-list">${renderBullets(rollovers, 'No major rollover tasks from yesterday.')}</ul>

            <h2 class="brief-doc-h2">🏆 Wins From Yesterday</h2>
            <ul class="brief-doc-list">${renderBullets(wins, 'You maintained continuity and kept progress moving.')}</ul>

            <h2 class="brief-doc-h2">🧾 What Actually Happened Yesterday</h2>
            <ul class="brief-doc-list">${renderBullets(activityLog, 'No detailed memory activity available yet.')}</ul>

            <h2 class="brief-doc-h2">🧠 Leadership Prompts</h2>
            <ul class="brief-doc-list">
                <li><strong>Success:</strong> ${this.escapeHtml(brief.leadership?.success || '')}</li>
                <li><strong>Leadership:</strong> ${this.escapeHtml(brief.leadership?.leader || '')}</li>
                <li><strong>Avoidance:</strong> ${this.escapeHtml(brief.leadership?.avoidance || '')}</li>
            </ul>

            <h2 class="brief-doc-h2">🔗 Quick Links</h2>
            <p class="brief-doc-p"><strong>Headline:</strong> <a href="${this.escapeHtml(brief.newsHeadline?.url || '#')}" target="_blank" rel="noreferrer">${this.escapeHtml(brief.newsHeadline?.title || 'No headline available')}</a></p>
            <ul class="brief-doc-links">${articleLinks}${videoLinks}</ul>
        `;
    }

    updatePersonalityPreview() {
        const formalityValue = Number(document.getElementById('formality-slider')?.value || 50);
        const verbosityValue = Number(document.getElementById('verbosity-slider')?.value || 50);

        const formalityLabel = document.getElementById('formality-value');
        const verbosityLabel = document.getElementById('verbosity-value');
        if (formalityLabel) formalityLabel.textContent = `${formalityValue}%`;
        if (verbosityLabel) verbosityLabel.textContent = `${verbosityValue}%`;

        if (!this.previewMessage) return;

        let preview = formalityValue > 65
            ? 'I can help you prioritize the most relevant actions for today.'
            : 'I can help sort out what matters next.';

        if (verbosityValue > 65) {
            preview += ' I will include a bit more context so each suggestion feels grounded in your recent work.';
        } else if (verbosityValue < 35) {
            preview += ' Short version first.';
        }

        this.previewMessage.textContent = `"${preview}"`;
    }

    applyTheme(theme) {
        if (theme === 'dark') {
            document.documentElement.setAttribute('data-theme', 'dark');
        } else {
            document.documentElement.removeAttribute('data-theme');
        }
    }

    applyCompactMode() {
        document.body.classList.toggle('compact-mode', this.compactMode);
    }

    updateGreeting() {
        const element = document.getElementById('today-greeting');
        if (!element) return;

        const hour = new Date().getHours();
        const name = this.userName || 'Willem';
        const room = this.roomName || 'Focus Room';
        if (hour < 12) {
            element.textContent = `Good morning, ${name} · ${room}`;
        } else if (hour < 18) {
            element.textContent = `Good afternoon, ${name} · ${room}`;
        } else {
            element.textContent = `Good evening, ${name} · ${room}`;
        }
    }

    normalizeCategory(category) {
        const value = String(category || '').trim().toLowerCase();
        if (value.includes('follow')) return 'followup';
        if (value.includes('relationship')) return 'relationship';
        if (['work', 'creative', 'personal', 'study', 'relationship'].includes(value)) return value;
        return 'work';
    }

    prettyCategory(category) {
        if (category === 'followup') return 'Follow-up';
        if (category === 'relationship') return 'Relationship';
        return category.charAt(0).toUpperCase() + category.slice(1);
    }

    prettyPriority(priority) {
        const value = String(priority || 'medium');
        return value.charAt(0).toUpperCase() + value.slice(1);
    }

    isConcreteActionLabel(label) {
        const text = String(label || '').trim().toLowerCase();
        if (!text) return false;
        if (/^(action \d+|open source context|open context|execute next action|do it)$/.test(text)) return false;
        return /\b(open|draft|reply|send|prepare|review|confirm|research|finish|complete|schedule|summarize|update|submit|resolve|fix|call|book|share)\b/.test(text);
    }

    categoryIcon(category) {
        const icons = {
            work: 'work',
            personal: 'favorite',
            creative: 'lightbulb',
            followup: 'reply',
            study: 'school',
            relationship: 'group'
        };
        return icons[category] || 'task';
    }

    todoKey(todo) {
        const compact = (value) => String(value || '')
            .toLowerCase()
            .replace(/[^a-z0-9\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
        const title = compact(todo.title);
        const action = compact(todo.primary_action?.label || todo.recommended_action || '');
        return `${title}|${action}`;
    }

    priorityWeight(priority) {
        return { high: 3, medium: 2, low: 1 }[priority] || 0;
    }

    truncate(value, max = 14) {
        const text = String(value || '').trim();
        if (text.length <= max) return text;
        return `${text.slice(0, max - 1)}…`;
    }

    emptyStateHTML(icon, title, subtitle) {
        return `
            <div class="empty-state" style="text-align:center; padding:60px 20px;">
                <div class="empty-icon" style="font-size:48px; opacity:0.3; margin-bottom:16px;">
                    <span class="material-symbols-outlined">${icon}</span>
                </div>
                <div class="empty-text" style="font-size:16px; font-weight:500; color:var(--text-secondary); margin-bottom:8px;">${this.escapeHtml(title)}</div>
                <div class="empty-sub" style="font-size:13px; color:var(--text-tertiary); line-height:1.5;">${this.escapeHtml(subtitle)}</div>
            </div>
        `;
    }

    showToast(message) {
        let toast = document.querySelector('.toast');
        if (!toast) {
            toast = document.createElement('div');
            toast.className = 'toast';
            document.body.appendChild(toast);
        }

        toast.textContent = message;
        toast.classList.add('show');
        if (this.soundEnabled) this.playUiSound('toast');
        if (this.notificationsEnabled) this.showSystemNotification(message);
        clearTimeout(this.toastTimer);
        this.toastTimer = setTimeout(() => toast.classList.remove('show'), 2400);
    }

    showSystemNotification(message) {
        if (!('Notification' in window)) return;
        if (document.visibilityState === 'visible') return;
        if (Notification.permission !== 'granted') return;
        try {
            new Notification('Weave', { body: String(message || '').slice(0, 120) });
        } catch (_) {}
    }

    playUiSound(kind = 'toast') {
        try {
            const AudioCtx = window.AudioContext || window.webkitAudioContext;
            if (!AudioCtx) return;
            if (!this.audioCtx) this.audioCtx = new AudioCtx();

            const now = this.audioCtx.currentTime;
            const oscillator = this.audioCtx.createOscillator();
            const gain = this.audioCtx.createGain();

            oscillator.type = 'sine';
            oscillator.frequency.value = kind === 'toggle' ? 640 : 520;
            gain.gain.setValueAtTime(0.0001, now);
            gain.gain.exponentialRampToValueAtTime(0.06, now + 0.01);
            gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.12);
            oscillator.connect(gain);
            gain.connect(this.audioCtx.destination);
            oscillator.start(now);
            oscillator.stop(now + 0.13);
        } catch (_) {}
    }

    setText(id, value) {
        const element = document.getElementById(id);
        if (element) element.textContent = value;
    }

    setDesktopTestStatus(message) {
        if (this.desktopTestStatus) this.desktopTestStatus.textContent = message;
    }

    appendDesktopTimelineEntry(payload = {}) {
        if (!this.desktopTimeline || payload.phase !== 'executed') return;
        const entry = {
            ts: Date.now(),
            stage: payload.stage || 'working',
            action: payload.action || 'READ_UI_STATE',
            effect: payload.effect_summary || 'No visible change recorded',
            remaining: payload.remaining_gap || '',
            visionMode: payload.perception_mode || payload.vision_mode || (payload.vision_used ? 'visual' : 'ax_only'),
            thumbnail: payload.timeline_thumbnail?.data_url || '',
            result: payload.result || ''
        };
        this.desktopTimelineEntries = [entry, ...this.desktopTimelineEntries].slice(0, 10);
        this.renderDesktopTimeline();
    }

    renderDesktopTimeline() {
        if (!this.desktopTimeline) return;
        if (!this.desktopTimelineEntries.length) {
            this.desktopTimeline.innerHTML = '<div class="history-row"><div class="history-row-title">No desktop steps yet</div><div class="history-row-meta">Run a desktop task to see the live execution timeline.</div></div>';
            return;
        }

        this.desktopTimeline.innerHTML = this.desktopTimelineEntries.map((entry) => {
            const thumb = entry.thumbnail
                ? `<img src="${this.escapeHtml(entry.thumbnail)}" alt="Step thumbnail" style="width:68px;height:44px;object-fit:cover;border-radius:8px;border:1px solid rgba(255,255,255,0.08);margin-top:8px;" />`
                : '';
            return `
                <div class="history-row">
                    <div class="history-row-title">${this.escapeHtml(this.capitalize(entry.stage))}: ${this.escapeHtml(String(entry.action).replace(/_/g, ' ').toLowerCase())}</div>
                    <div class="history-row-meta">
                        <span>${this.escapeHtml(String(entry.visionMode).replace(/_/g, ' '))}</span>
                        <span>•</span>
                        <span>${this.escapeHtml(entry.result || 'executed')}</span>
                    </div>
                    <div class="history-row-meta">${this.escapeHtml(entry.effect)}</div>
                    ${entry.remaining ? `<div class="history-row-meta">Next: ${this.escapeHtml(entry.remaining)}</div>` : ''}
                    ${thumb}
                </div>
            `;
        }).join('');
    }

    capitalize(value) {
        const text = String(value || '');
        return text ? `${text.charAt(0).toUpperCase()}${text.slice(1)}` : '';
    }

    basename(filePath) {
        if (!filePath) return '';
        const normalized = String(filePath).replace(/\\/g, '/');
        const index = normalized.lastIndexOf('/');
        return index === -1 ? normalized : normalized.slice(index + 1);
    }

    safeDomain(url) {
        try {
            return new URL(url).hostname;
        } catch (_) {
            return '';
        }
    }

    friendlyTime(ts) {
        const numeric = Number(ts);
        const value = Number.isFinite(numeric) && numeric > 0 ? numeric : new Date(ts).getTime();
        if (!value || Number.isNaN(value)) return 'Unknown time';
        const diffMs = Date.now() - value;
        const minutes = Math.floor(diffMs / 60000);
        if (minutes < 1) return 'Just now';
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours}h ago`;
        const days = Math.floor(hours / 24);
        if (days < 7) return `${days}d ago`;
        return new Date(value).toLocaleDateString();
    }

    escapeHtml(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    handleMemoryGraphUpdate(payload = {}) {
        const syncStatusEl = document.getElementById("chat-sync-status");
        const processingStatusEl = document.getElementById("mem-processing-status");
        
        if (syncStatusEl) {
            const textEl = syncStatusEl.querySelector(".sync-text");
            if (textEl) textEl.textContent = "Syncing memory...";
            syncStatusEl.classList.add("syncing");
            
            clearTimeout(this._syncResetTimer);
            this._syncResetTimer = setTimeout(() => {
                if (textEl) textEl.textContent = "Memory synced";
                syncStatusEl.classList.remove("syncing");
            }, 3000);
        }

        if (processingStatusEl) {
            processingStatusEl.textContent = "Processing...";
            processingStatusEl.classList.remove("inactive");
            processingStatusEl.classList.add("active");
            
            clearTimeout(this._procResetTimer);
            this._procResetTimer = setTimeout(() => {
                processingStatusEl.textContent = "Idle";
                processingStatusEl.classList.remove("active");
                processingStatusEl.classList.add("inactive");
            }, 3000);
        }

        try {
            if (payload && payload.type === "request_chat_save") {
                const trimmed = (this.chatSessions || []).map((session) => ({
                    ...session,
                    messages: (session.messages || []).slice(-120)
                })).slice(0, 25);
                if (window.electronAPI?.saveChatSessionsToMemory) window.electronAPI.saveChatSessionsToMemory(trimmed);
            }
        } catch (e) { }
    }


    setupLibrary() {
        this.libraryFilters.forEach(chip => {
            chip.addEventListener('click', () => {
                this.libraryFilters.forEach(c => c.classList.remove('active'));
                chip.classList.add('active');
                this.renderLibrary(chip.dataset.libFilter);
            });
        });

        this.librarySearchButton?.addEventListener("click", () => this.handleLibrarySearch());
        this.librarySearchInput?.addEventListener("keydown", (e) => {
            if (e.key === "Enter") this.handleLibrarySearch();
        });
    }

    async handleLibrarySearch() {
        const query = this.librarySearchInput?.value.trim();
        if (!query) {
            if (this.librarySearchQueries) this.librarySearchQueries.classList.add('hidden');
            this.renderLibrary();
            return;
        }

        if (!this.libraryList) return;
        this.libraryList.innerHTML = '<div class="graph-placeholder">Searching memory graph...</div>';

        try {
            const activeFilter = this.libraryFilters.find(f => f.classList.contains('active'))?.dataset.libFilter || 'all';
            const nodeTypes = activeFilter === 'all' ? [] : [activeFilter];

            const results = await window.electronAPI.searchMemoryGraph(query, {
                limit: 40,
                nodeTypes
            });

            if (this.librarySearchQueries) {
                this.librarySearchQueries.innerHTML = '';
                this.librarySearchQueries.classList.remove('hidden');
                
                const displayQueries = results.generated_queries?.semantic || [query];
                displayQueries.forEach(q => {
                    const chip = document.createElement('div');
                    chip.className = 'context-chip';
                    chip.style.fontSize = '10px';
                    chip.style.padding = '4px 8px';
                    chip.textContent = q;
                    this.librarySearchQueries.appendChild(chip);
                });
            }

            this.renderLibraryResults(results);
        } catch (err) {
            console.error("Library search error:", err);
            this.libraryList.innerHTML = '<div class="empty-state">Search failed.</div>';
        }
    }

    renderLibraryResults(results) {
        const nodes = Array.isArray(results) ? results : (results.evidence || results.primary_nodes || []);
        
        if (!nodes.length) {
            this.libraryList.innerHTML = '<div class="empty-state">No matching memories found.</div>';
            return;
        }

        this.libraryList.innerHTML = nodes.map(node => {
            const metadata = typeof node.metadata === 'string' ? JSON.parse(node.metadata) : (node.metadata || {});
            const date = node.anchor_date || metadata.anchor_date || metadata.latest_activity_at?.slice(0, 10) || "";
            const layer = node.layer || node.type || "memory";
            
            return `
                <div class="library-card" onclick="app.openMemoryDetailById('${node.id}')">
                    <div class="library-card-layer">${layer}</div>
                    <div class="library-card-title">${this.escapeHtml(node.title || "")}</div>
                    <div class="library-card-summary">${this.escapeHtml(node.summary || node.text || "")}</div>
                    <div class="library-card-footer">
                        <span>${node.subtype || ""}</span><span class="library-card-date">${date}</span>
                    </div>
                </div>
            `;
        }).join("");
    }

    async renderLibrary(filter = "all") {
        if (this.librarySearchInput) this.librarySearchInput.value = "";
        if (this.librarySearchQueries) this.librarySearchQueries.classList.add('hidden');
        if (!this.libraryList) return;
        this.libraryList.innerHTML = '<div class="graph-placeholder">Loading library...</div>';
        try {
            const { nodes } = await window.electronAPI.getFullMemoryGraph();
            const filtered = nodes.filter(n => filter === "all" || n.layer === filter);
            if (!filtered.length) {
                this.libraryList.innerHTML = '<div class="empty-state">No items found in this category.</div>';
                return;
            }
            this.libraryList.innerHTML = filtered.map(node => {
                const date = node.anchor_date || (node.metadata ? (typeof node.metadata === 'string' ? JSON.parse(node.metadata).anchor_date : node.metadata.anchor_date) : "");
                return `
                <div class="library-card" onclick="app.openMemoryDetailById('${node.id}')">
                    <div class="library-card-layer">${node.layer}</div>
                    <div class="library-card-title">${this.escapeHtml(node.title || "")}</div>
                    <div class="library-card-summary">${this.escapeHtml(node.summary || "")}</div>
                    <div class="library-card-footer">
                        <span>${node.subtype || ""}</span><span class="library-card-date">${date || ""}</span>
                    </div>
                </div>
            `}).join("");
    
        } catch (err) {
            console.error("Library load error:", err);
            this.libraryList.innerHTML = '<div class="empty-state">Failed to load library.</div>';
        }
    }

    async renderFullMemoryGraph() {
        if (!this.settingsGraphContainer) return;
        this.settingsGraphContainer.innerHTML = '<div class="graph-placeholder">Initializing graph...</div>';
        try {
            const { nodes, edges } = await window.electronAPI.getFullMemoryGraph();
            if (!nodes || !nodes.length) {
                this.settingsGraphContainer.innerHTML = '<div class="graph-placeholder">No memory nodes yet.</div>';
                return;
            }

            // Filter out orphaned edges
            const filteredNodes = nodes.filter(n => n.layer !== "cloud");
            const nodeIds = new Set(filteredNodes.map(n => n.id));
            const filteredEdges = (edges || []).filter(e => {
                const s = typeof e.source === 'object' ? e.source.id : e.source;
                const t = typeof e.target === 'object' ? e.target.id : e.target;
                return nodeIds.has(s) && nodeIds.has(t);
            });

            this.settingsGraphContainer.innerHTML = "";
            const width = this.settingsGraphContainer.clientWidth || 800;
            const height = 600;

            const layers = ['core', 'insight', 'semantic', 'episode', 'raw'];
            const layerY = (layer) => {
                const index = layers.indexOf(layer === 'event' ? 'raw' : layer);
                if (index === -1) return width / 2;
                return (height / (layers.length + 1)) * (index + 1);
            };

            const svg = d3.select(this.settingsGraphContainer)
                .append("svg")
                .attr("width", width)
                .attr("height", height);

            // Add background brackets and labels
            layers.forEach((layer) => {
                const y = layerY(layer);
                
                svg.append("line")
                    .attr("x1", 40)
                    .attr("y1", y)
                    .attr("x2", width - 40)
                    .attr("y2", y)
                    .attr("stroke", "var(--glass-border)")
                    .attr("stroke-width", 1);

                svg.append("line")
                    .attr("x1", 40)
                    .attr("y1", y - 5)
                    .attr("x2", 40)
                    .attr("y2", y + 5)
                    .attr("stroke", "var(--glass-border)")
                    .attr("stroke-width", 1);

                svg.append("line")
                    .attr("x1", width - 40)
                    .attr("y1", y - 5)
                    .attr("x2", width - 40)
                    .attr("y2", y + 5)
                    .attr("stroke", "var(--glass-border)")
                    .attr("stroke-width", 1);
                
                svg.append("text")
                    .attr("x", 10)
                    .attr("y", y + 4)
                    .attr("text-anchor", "start")
                    .attr("fill", "var(--text-tertiary)")
                    .attr("font-size", "10px")
                    .attr("font-weight", "600")
                    .attr("style", "text-transform: uppercase; letter-spacing: 0.05em;")
                    .text(layer);
            });

            const simulation = d3.forceSimulation(filteredNodes)
                .force("link", d3.forceLink(filteredEdges).id(d => d.id).distance(80))
                .force("charge", d3.forceManyBody().strength(-120))
                .force("center", d3.forceCenter(width / 2, height / 2))
                .force("x", d3.forceX(width / 2).strength(0.2))
                .force("y", d3.forceY(d => layerY(d.layer)).strength(1.5))
                .force("collide", d3.forceCollide(25));
            const linkGroup = svg.append("g");
            const link = linkGroup.selectAll(".graph-link-container").data(filteredEdges).enter().append("g").attr("class", "graph-link-container");
            link.append("line").attr("class", "graph-link").attr("stroke", "var(--accent-blue)").attr("stroke-opacity", 0.25).attr("stroke-width", d => Math.sqrt(d.weight || 1));
            link.append("text").attr("class", "graph-edge-label").attr("text-anchor", "middle").attr("font-size", "8px").attr("fill", "var(--text-tertiary)").text(d => d.edge_type || d.trace_label || "");

            const node = svg.append("g")
                .selectAll("circle")
                .data(filteredNodes)
                .enter().append("circle")
                .attr("class", "graph-node")
                .attr("r", d => d.layer === "core" ? 9 : 5)
                .attr("fill", d => {
                    if (d.layer === "core") return "var(--accent-coral)";
                    if (d.layer === "insight") return "var(--accent-amber)";
                    if (d.layer === "episode") return "var(--accent-teal)";
                    if (d.layer === "semantic") return "var(--accent-blue)";
                    return "var(--text-tertiary)";
                })
                .attr("stroke", "var(--glass-bg)")
                .attr("stroke-width", 1.5)
                .call(d3.drag()
                    .on("start", (event) => {
                        if (!event.active) simulation.alphaTarget(0.3).restart();
                        event.subject.fx = event.subject.x;
                        event.subject.fy = event.subject.y;
                    })
                    .on("drag", (event) => {
                        event.subject.fx = event.x;
                        event.subject.fy = event.y;
                    })
                    .on("end", (event) => {
                        if (!event.active) simulation.alphaTarget(0);
                        event.subject.fx = null;
                        event.subject.fy = null;
                    }))
                .on("click", (event, d) => this.openMemoryDetailById(d.id));

            node.append("title").text(d => `${d.title}\n(${d.layer})`);

            simulation.on("tick", () => {
                link.select("line")
                    .attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);
                
                link.select("text")
                    .attr("x", d => (d.source.x + d.target.x) / 2)
                    .attr("y", d => (d.source.y + d.target.y) / 2);

                node.attr("cx", d => d.x)
                    .attr("cy", d => d.y);
            });
        } catch (err) {
            console.error("Graph render error:", err);
            this.settingsGraphContainer.innerHTML = '<div class="graph-placeholder">Failed to load graph.</div>';
        }
    }

    async openMemoryDetailById(nodeId) {
        try {
            const { nodes } = await window.electronAPI.getFullMemoryGraph();
            const node = nodes.find(n => n.id === nodeId);
            if (node) {
                this.memorySearchResults = [node];
                this.openMemoryModal(0);
            }
        } catch (err) {
            console.error("Failed to open memory detail:", err);
        }
    }

    triggerSearchAnimation(active) {
        const scanningIndicator = document.getElementById("scanning-indicator");
        if (scanningIndicator) scanningIndicator.classList.toggle("active", active);
        const chatPanel = document.querySelector(".chat-panel");
        if (chatPanel) {
            chatPanel.classList.toggle("searching", active);
        }
    }
}
document.addEventListener('DOMContentLoaded', () => {
    window.app = new WeaveApp();
});
