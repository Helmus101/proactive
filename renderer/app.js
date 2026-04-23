'use strict';

class WeaveApp {
    constructor() {
        this.todos = [];
        this.contacts = [];
        this.selectedContact = null;
        this.currentFilter = 'all';
        this.activeView = 'presence-view';
        this.expandedCards = new Set();
        this.compactMode = localStorage.getItem('compactMode') === 'true';
        this.notificationsEnabled = localStorage.getItem('notificationsEnabled') !== 'false';
        this.soundEnabled = localStorage.getItem('soundEnabled') === 'true';
        this.desktopCaptureEnabled = localStorage.getItem('desktopCaptureEnabled') !== 'false';
        this.suggestionRefreshInFlight = null;
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
        this.showExtendedToday = localStorage.getItem('showExtendedToday') === 'true';
        this.showPeopleSection = localStorage.getItem('showPeopleSection') !== 'false';
        this.chatHistoryCollapsed = localStorage.getItem('chatHistoryCollapsed') === 'true';
        this.presenceMode = 'waiting';
        this.contactsLoaded = false;
        this.settingsDataLoaded = false;
        this.settingsDataLoading = null;
        this.init();
    }

    async init() {
        this.cacheDom();
        this.setPresenceMode('waiting');
        this.applyCompactMode();
        this.applyTheme(localStorage.getItem('theme') || 'light');
        this.updateGreeting();
        this.setupMicroInteractions();
        this.setupNavigation();
        this.setupSuggestions();
        await this.setupChat();
        this.setupContactsView();
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
        window.electronAPI.onAutomationResult?.((payload) => this.handleAutomationResult(payload));
        await this.loadInitialData();
        setInterval(() => {
            if (document.hidden) return;
            this.updateChatSyncStatus();
        }, 60000);
    }

    cacheDom() {
        this.views = Array.from(document.querySelectorAll('.view'));
        this.navButtons = Array.from(document.querySelectorAll('.nav-pill'));
        this.filterChips = Array.from(document.querySelectorAll('#presence-view .context-chip'));
        this.remindersList = document.getElementById('reminders-list');
        this.refreshButton = document.getElementById('refresh-tasks-btn');
        this.chatMessages = document.getElementById('chat-messages');
        this.chatInput = document.getElementById('chat-input');
        this.chatCharCounter = document.getElementById('chat-char-counter');
        this.sendChatButton = document.getElementById('send-chat-btn');
        this.chatRefreshPrioritiesButton = document.getElementById('chat-refresh-priorities-btn');
        this.chatHistoryList = document.getElementById('chat-history-list');
        this.chatThreadTitle = document.getElementById('chat-thread-title');
        this.newChatButton = document.getElementById('new-chat-btn');
        this.previewMessage = document.getElementById('preview-message');
        this.googleStatus = document.getElementById('google-status');
        this.historyList = document.getElementById('settings-history-list');
        this.historyMeta = document.getElementById('settings-history-meta');
        this.screenCapturesList = document.getElementById('screen-captures-list');
        this.captureStatusText = document.getElementById('capture-status-text');
        this.openScreenRecordingButton = document.getElementById('open-screen-recording-btn');
        this.dailyOcrMeta = document.getElementById('daily-ocr-meta');
        this.dailyOcrText = document.getElementById('daily-ocr-text');
        this.copyDailyOcrButton = document.getElementById('copy-daily-ocr-btn');
        this.refreshDailyOcrButton = document.getElementById('refresh-daily-ocr-btn');
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
        this.clearAllSuggestionsButton = document.getElementById('clear-all-suggestions-btn');
        this.focusModeSelect = document.getElementById('focus-mode-select');
        this.customizeTodayButton = document.getElementById('customize-today-btn');
        this.todayWhisperPrompt = document.getElementById('today-whisper-prompt');
        this.presenceState = document.getElementById('presence-state');
        this.presenceSummary = document.getElementById('presence-summary');
        this.presencePrimaryAction = document.getElementById('presence-primary-action');
        this.todayOpenCount = document.getElementById('today-open-count');
        this.todayClientCount = document.getElementById('today-client-count');
        this.todayDeliveryCount = document.getElementById('today-delivery-count');
        this.toggleChatHistoryButton = document.getElementById('toggle-chat-history-btn');
        this.chatView = document.getElementById('action-view');
        this.chatSyncStatus = document.getElementById('chat-sync-status');
        this.suggestionProviderSelect = document.getElementById('suggestion-llm-provider');
        this.suggestionModelInput = document.getElementById('suggestion-llm-model');
        this.suggestionBaseUrlInput = document.getElementById('suggestion-llm-base-url');
        this.suggestionApiKeyInput = document.getElementById('suggestion-llm-api-key');
        this.suggestionProviderStatus = document.getElementById('suggestion-llm-status');
        this.saveSuggestionProviderButton = document.getElementById('save-suggestion-llm-btn');
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

        if (viewId === 'action-view') {
            this.startNewChatDraft();
            this.chatInput?.focus();
            this.updateChatSyncStatus();
            this.scrollChatToBottom();
        }
        if (viewId === 'contacts-view') {
            if (!this.contactsLoaded) {
                this.loadContacts();
            }
        }
        if (viewId === 'settings-view') {
            this.loadSettingsData();
        }
    }

    setupSuggestions() {
        this.setupTodayWhisperPrompt();
        const triggerRefresh = async (event = null) => {
            event?.preventDefault?.();
            event?.stopPropagation?.();
            await this.generateSuggestions({ replace: true, silent: false, forceEngineRefresh: true });
        };

        this.filterChips.forEach((chip) => {
            chip.addEventListener('click', () => {
                this.filterChips.forEach((item) => item.classList.remove('active'));
                chip.classList.add('active');
                this.currentFilter = chip.dataset.filter || 'all';
                this.renderSuggestions();
            });
        });

        this.refreshButton?.addEventListener('click', triggerRefresh);
        this.manualGenerateSuggestionsButton?.addEventListener('click', triggerRefresh);
        this.presencePrimaryAction?.addEventListener('click', triggerRefresh);
        this.clearAllSuggestionsButton?.addEventListener('click', async () => {
            await this.clearAllSuggestions();
        });
        this.focusModeSelect?.addEventListener('change', async () => {
            const value = this.focusModeSelect.value || 'all';
            this.currentFilter = value;
            this.filterChips.forEach((chip) => chip.classList.toggle('active', chip.dataset.filter === value));
            this.showExtendedToday = false;
            localStorage.setItem('showExtendedToday', 'false');
            this.renderSuggestions();
        });
        this.customizeTodayButton?.addEventListener('click', () => {
            this.showExtendedToday = !this.showExtendedToday;
            localStorage.setItem('showExtendedToday', String(this.showExtendedToday));
            this.showToast(this.showExtendedToday ? 'Expanded today view' : 'Focused today view');
            this.renderSuggestions();
        });

        // Defensive delegation fallback: if cached references are missing or listeners fail,
        // handle clicks at document level for the common control IDs.
        document.addEventListener('click', async (ev) => {
            try {
                if (ev.defaultPrevented) return;
                const target = ev.target.closest && ev.target.closest('#manual-generate-suggestions-btn, #refresh-tasks-btn, #presence-primary-action');
                if (!target) return;
                ev.preventDefault();
                ev.stopPropagation();
                await this.generateSuggestions({ replace: true, silent: false, forceEngineRefresh: true });
            } catch (err) {
                console.error('[Renderer] fallback generateSuggestions handler failed:', err);
                try { this.showToast('Priority refresh failed'); } catch (_) {}
            }
        });

        this.remindersList?.addEventListener('click', async (event) => {
            const globalAction = event.target.closest('[data-action]')?.dataset.action;
            if (globalAction === 'toggle-more') {
                event.stopPropagation();
                this.showExtendedToday = !this.showExtendedToday;
                localStorage.setItem('showExtendedToday', String(this.showExtendedToday));
                this.renderSuggestions();
                return;
            }

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
                this.generateSuggestions({ replace: false, silent: true }).catch(() => {});
            }, 60 * 60 * 1000);
        }

        document.getElementById('morning-brief-close-btn')?.addEventListener('click', () => this.closeMorningBriefModal());
        this.morningBriefModal?.addEventListener('click', (event) => {
            if (event.target?.id === 'morning-brief-modal') this.closeMorningBriefModal();
        });
    }

    async withTimeout(promise, timeoutMs, label = 'operation') {
        let timer = null;
        try {
            return await Promise.race([
                Promise.resolve(promise),
                new Promise((_, reject) => {
                    timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
                })
            ]);
        } finally {
            if (timer) clearTimeout(timer);
        }
    }

    async generateSuggestions({ replace = true, silent = false, forceEngineRefresh = false } = {}) {
        if (!this.remindersList) return;
        if (this.suggestionRefreshInFlight) {
            return this.suggestionRefreshInFlight;
        }
        this.setPresenceMode('thinking');

        const activeSuggestions = (Array.isArray(this.todos) ? this.todos : []).filter((todo) => !todo?.completed);
        const remainingCapacity = Math.max(0, 7 - activeSuggestions.length);

        if (remainingCapacity <= 0) {
            if (!silent) this.showToast('You already have 7 active priorities');
            this.renderSuggestions();
            this.setPresenceMode('waiting');
            return;
        }

        if (!silent) {
            this.remindersList.innerHTML = this.emptyStateHTML(
                'refresh',
                'Refreshing priorities...',
                'Reviewing recent client work, meetings, and open loops.'
            );
        }

        const run = async () => {
            if (forceEngineRefresh && typeof window.electronAPI?.triggerSuggestionRefresh === 'function') {
                const refreshResult = await this.withTimeout(
                    window.electronAPI.triggerSuggestionRefresh({
                        requested_at: Date.now(),
                        source: this.activeView || 'presence-view'
                    }),
                    20000,
                    'triggerSuggestionRefresh'
                );
                if (Array.isArray(refreshResult?.suggestions) && refreshResult.suggestions.length) {
                    const normalizedRefreshed = this.normalizeTodos(refreshResult.suggestions);
                    this.todos = this.mergeTodos(replace ? [] : this.todos, normalizedRefreshed).filter((todo) => !todo.completed).slice(0, 7);
                    await this.withTimeout(window.electronAPI.savePersistentTodos(this.todos), 5000, 'savePersistentTodos');
                    this.renderSuggestions();
                    this.setPresenceMode('suggesting');
                    if (!silent) this.showToast('Priorities refreshed from memory');
                    return;
                }
            }
            console.debug('[Renderer] generateSuggestions invoked; electronAPI present=', !!window.electronAPI, 'generateProactiveTodos=', typeof window.electronAPI?.generateProactiveTodos);
            if (!window.electronAPI || typeof window.electronAPI.generateProactiveTodos !== 'function') {
                throw new Error('electronAPI.generateProactiveTodos is not available');
            }
            const generated = await this.withTimeout(
                window.electronAPI.generateProactiveTodos({
                    maxNewSuggestions: remainingCapacity,
                    activeSuggestionCount: activeSuggestions.length
                }),
                20000,
                'generateProactiveTodos'
            );
            console.debug('[Renderer] generateSuggestions received', Array.isArray(generated) ? `${generated.length} items` : typeof generated);
            const normalized = this.normalizeTodos(generated);

            if (replace && !activeSuggestions.length) {
                this.todos = normalized;
            } else {
                this.todos = this.mergeTodos(this.todos, normalized);
            }
            this.todos = this.todos.filter((todo) => !todo.completed).slice(0, 7);

            await this.withTimeout(window.electronAPI.savePersistentTodos(this.todos), 5000, 'savePersistentTodos');
            this.renderSuggestions();
            this.setPresenceMode('suggesting');
        };

        this.suggestionRefreshInFlight = run().catch((error) => {
            console.error('Failed to generate suggestions:', error);
            this.showToast('Priority refresh failed');
            this.renderSuggestions();
            this.setPresenceMode('waiting');
        }).finally(() => {
            this.suggestionRefreshInFlight = null;
        });

        return this.suggestionRefreshInFlight;
    }

    async clearAllSuggestions() {
        try {
            this.todos = [];
            this.expandedCards.clear();
            await window.electronAPI.savePersistentTodos([]);
            await window.electronAPI.clearSuggestions?.();
            this.renderSuggestions();
            this.showToast('All priorities cleared');
        } catch (error) {
            console.error('Failed to clear all suggestions:', error);
            this.showToast('Failed to clear priorities');
        }
    }

    normalizeTodos(items) {
        const compact = (value, limit = 90) => {
            const text = String(value || '').replace(/\s+/g, ' ').trim();
            if (!text) return '';
            return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1)).trim()}…` : text;
        };
        const normalized = (Array.isArray(items) ? items : [])
            .map((item, index) => {
                const category = this.normalizeCategory(item.category);
                const plan = Array.isArray(item.plan)
                    ? item.plan.filter(Boolean).map((step) => compact(step, 42)).slice(0, 2)
                    : [];
                const suggestedActions = Array.isArray(item.suggested_actions)
                    ? item.suggested_actions.slice(0, 1).map((action) => ({
                        ...action,
                        label: compact(action?.label, 46) || 'Review details'
                    }))
                    : [];
                const mappedActionPlan = suggestedActions.map((action, i) => ({
                    step: action?.label || `Action ${i + 1}`,
                    target: action?.type || 'browser_operator',
                    url: action?.payload?.url || null
                }));
                const primaryAction = item.primary_action || suggestedActions.find((action) => this.isConcreteActionLabel(action?.label));
                const whyNow = compact(item.display?.summary || item.trigger_summary || item.reason || item.description || item.body || '', 88);
                const evidenceLine = Array.isArray(item.epistemic_trace) && item.epistemic_trace.length
                    ? compact(`${item.epistemic_trace[0].source || 'Source'}: ${item.epistemic_trace[0].text || ''}`, 86)
                    : '';
                return {
                    id: item.id || `suggestion_${Date.now()}_${index}`,
                    title: compact(item.title || 'Untitled priority', 62),
                    description: compact(item.description || item.body || whyNow || 'No extra context yet.', 88),
                    intent: compact(item.intent || item.description || '', 88),
                    reason: compact(item.reason || item.description || '', 88),
                    why_now: whyNow,
                    evidence_line: evidenceLine,
                    trigger_summary: compact(item.trigger_summary || item.triggerSummary || '', 88),
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
                    expected_benefit: compact(item.expected_benefit || '', 88),
                    expected_impact: compact(item.expected_impact || item.expected_benefit || '', 88),
                    prerequisites: Array.isArray(item.prerequisites) ? item.prerequisites : [],
                    step_plan: Array.isArray(item.step_plan) ? item.step_plan.filter(Boolean).map((step) => compact(step, 42)).slice(0, 2) : plan,
                    snoozedUntil: item.snoozedUntil || 0,
                    study_subject: (item.study_subject || '').trim(),
                    risk_level: (item.risk_level || '').trim().toLowerCase(),
                    evidence_path: Array.isArray(item.evidence_path) ? item.evidence_path : [],
                    recommended_action: compact(item.recommended_action || '', 46),
                    suggestion_group: (item.suggestion_group || '').trim().toLowerCase(),
                    opportunity_type: (item.opportunity_type || '').trim().toLowerCase(),
                    time_anchor: compact(item.time_anchor || '', 36),
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
            .slice(0, 7);
    }

    renderSuggestions() {
        if (!this.remindersList) return;

        const visibleTodos = this.getVisibleTodos();
        const briefBanner = this.renderMorningBriefBanner();
        this.remindersList.classList.remove('today-dissolve');
        void this.remindersList.offsetWidth;
        this.remindersList.classList.add('today-dissolve');
        this.updatePresenceSummary(visibleTodos);

        if (!visibleTodos.length) {
            this.remindersList.innerHTML = briefBanner + this.emptyStateHTML(
                'task_alt',
                'No urgent client work found',
                'Sync Google, refresh priorities, or ask Weave what needs attention.'
            );
            this.setPresenceMode('waiting');
            return;
        }

        const primaryNow = visibleTodos.slice(0, 1);
        const supportingNow = visibleTodos.slice(1, 3);
        const extraTasks = visibleTodos.slice(3, 7);
        this.setPresenceMode('suggesting');

        const section = (items, offset = 0) => {
            if (!items.length) return '';
            return `
                <div class="study-suggestion-group conversation-stream">
                    ${items.map((todo, index) => this.renderSuggestionCard(todo, index + offset)).join('')}
                </div>
            `;
        };

        const moreContent = [
            section(supportingNow, 1),
            section(extraTasks, 3)
        ].join('');

        this.remindersList.innerHTML = `
            ${briefBanner}
            ${section(primaryNow, 0)}
            ${moreContent && !this.showExtendedToday ? '<div class="today-more-wrap"><button class="pill-btn" type="button" data-action="toggle-more">Show more</button></div>' : ''}
            ${this.showExtendedToday ? `<div class="today-more-block">${moreContent}<div class="today-more-wrap"><button class="pill-btn" type="button" data-action="toggle-more">Show less</button></div></div>` : ''}
        `;
    }

    renderSuggestionCard(todo, index = 0) {
        const expanded = this.expandedCards.has(todo.id);
        const suggestionCategory = todo.suggestion_category || todo.category || 'work';
        const isStudy = suggestionCategory === 'study';
        const aiCanAutomate = Boolean(todo.ai_doable && todo.action_type && todo.action_type !== 'manual_next_step');
        const compact = (value, limit = 90) => {
            const text = String(value || '').replace(/\s+/g, ' ').trim();
            return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1)).trim()}…` : text;
        };
        const riskLabel = todo.risk_level ? `Risk: ${todo.risk_level}` : '';
        const bundleSummary = todo.display?.summary || '';
        const primaryAction = todo.primary_action || (Array.isArray(todo.suggested_actions) ? todo.suggested_actions.find((a) => this.isConcreteActionLabel(a?.label)) : null);
        const rawSubtitle = todo.reason || todo.trigger_summary || bundleSummary || todo.why_now || todo.description || '';
        const whyNowCompact = compact(rawSubtitle, 88);
        const evidenceCompact = todo.evidence_line || (Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? compact(`${todo.epistemic_trace[0].source || 'Source'}: ${todo.epistemic_trace[0].text || ''}`, 86)
            : '');
        const receiptSummary = Array.isArray(todo.epistemic_trace) && todo.epistemic_trace.length
            ? todo.epistemic_trace.slice(0, 1).map((r) => compact(`${r.source || 'Source'}: ${r.text || ''}`, 86)).join(' • ')
            : '';
        const categoryLabel = this.prettyCategory(todo.category);
        const actionLabel = primaryAction?.label || 'Review details';
        const cardTone = this.cardToneClass(todo.category);
        
        return `
                <article class="suggestion-card conversation-entry ${this.escapeHtml(cardTone)}${isStudy ? ' suggestion-study' : ''}" data-id="${this.escapeHtml(todo.id)}" tabindex="0" style="animation-delay:${index * 40}ms;">
                    <div class="suggestion-header">
                        <div class="suggestion-content">
                            <div class="suggestion-kicker">
                                <span>${this.escapeHtml(categoryLabel)}</span>
                                <span>${this.escapeHtml(this.prettyPriority(todo.priority))}</span>
                                ${todo.time_anchor ? `<span>${this.escapeHtml(todo.time_anchor)}</span>` : ''}
                            </div>
                            <div class="suggestion-title">${this.escapeHtml(todo.title)}</div>
                            ${whyNowCompact ? `<div class="suggestion-description">${this.escapeHtml(whyNowCompact)}</div>` : ''}
                            ${evidenceCompact ? `<div class="suggestion-context" style="font-style: italic; opacity: 0.8;">${this.escapeHtml(evidenceCompact)}</div>` : ''}
                            <div class="suggestion-primary-line" style="font-weight: 500; color: var(--accent-blue-strong);">${this.escapeHtml(actionLabel)}</div>
                            ${aiCanAutomate ? '<div class="suggestion-context suggestion-subtle-note" style="font-size: 11px; margin-top: 4px;">AI-assisted automation available</div>' : ''}
                            ${!expanded ? '<div class="suggestion-primary-wrap"><button class="presence-primary-action" type="button" data-action="info" style="padding: 6px 12px; font-size: 11px;">View details</button></div>' : ''}
                        </div>
                    </div>
                    ${expanded ? `
                        <div class="suggestion-details suggestion-details-quick" style="display:block; margin-top: 16px; border-top: 1px solid var(--glass-border); padding-top: 16px;">
                            ${todo.reason ? `<div class="suggestion-why" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Context</span>${this.escapeHtml(todo.reason)}</div>` : ''}
                            ${todo.step_plan.length ? `<div class="suggestion-steps" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Plan</span>${todo.step_plan.map((step, stepIndex) => `<div class="suggestion-step">${stepIndex + 1}. ${this.escapeHtml(step)}</div>`).join('')}</div>` : ''}
                            ${receiptSummary ? `<div class="suggestion-why" style="margin-bottom: 12px;"><span style="display: block; font-size: 10px; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 4px;">Source</span>${this.escapeHtml(receiptSummary)}</div>` : ''}
                            <div class="reminder-actions" style="opacity:1; margin-top:20px; display: flex; gap: 8px;">
                                ${todo.source === 'morning-brief'
                                    ? '<button class="pill-btn" type="button" data-action="brief-open">Open brief</button><button class="pill-btn" type="button" data-action="brief-archive">Archive</button>'
                                    : `${todo.ai_doable ? '<button class="pill-btn pill-btn-primary" type="button" data-action="execute">Execute</button>' : ''}<button class="pill-btn" type="button" data-action="done">Mark done</button><button class="pill-btn" type="button" data-action="snooze">Snooze</button><button class="pill-btn destructive" type="button" data-action="remove">Dismiss</button>`
                                }
                            </div>
                        </div>
                    ` : ''}
                </article>
            `;
    }    renderMorningBriefBanner() {
        const latest = (this.morningBriefs || [])[0];
        if (!latest) return '';
        const subtitle = (latest.priorities || []).slice(0, 3).map((p) => p.title).filter(Boolean).join(' • ');
        return `
            <article class="morning-brief-flow" data-id="brief_banner" tabindex="0">
                <div class="morning-brief-date">${this.escapeHtml(latest.dateLabel || '')}</div>
                <p class="morning-brief-text">${this.escapeHtml(subtitle || 'Since yesterday: a short snapshot of client work and open priorities.')}</p>
                <button class="ghost-button" type="button" data-action="brief-open">Open brief</button>
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
        const card = this.remindersList?.querySelector(`.suggestion-card[data-id="${taskId}"]`);
        if (card) {
            card.classList.add('completion-flash');
            await new Promise((resolve) => setTimeout(resolve, 220));
        }
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

        this.showToast('Marked done');
    }

    async removeSuggestion(taskId) {
        this.todos = this.todos.filter((todo) => todo.id !== taskId);
        this.expandedCards.delete(taskId);
        try {
            await window.electronAPI.savePersistentTodos(this.todos);
            this.showToast('Priority removed');
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
        const filtered = this.todos.filter((todo) => {
            if (todo.completed) return false;
            if (todo.snoozedUntil && Number(todo.snoozedUntil) > now) return false;
            if (this.currentFilter === 'all') return true;
            if (this.currentFilter === 'client') return ['followup', 'relationship_intelligence'].includes(todo.category) || this.hasClientSignal(todo);
            if (this.currentFilter === 'delivery') return ['work', 'creative', 'study'].includes(todo.category);
            if (this.currentFilter === 'followups') return ['followup', 'relationship_intelligence'].includes(todo.category);
            if (this.currentFilter === 'focus') return ['work', 'creative', 'study'].includes(todo.category);
            if (this.currentFilter === 'people') return ['followup', 'relationship_intelligence'].includes(todo.category);
            return todo.category === this.currentFilter;
        });

        return filtered
            .sort((a, b) => {
                const priorityDelta = this.priorityWeight(b.priority) - this.priorityWeight(a.priority);
                if (priorityDelta !== 0) return priorityDelta;
                return (b.createdAt || 0) - (a.createdAt || 0);
            })
            .slice(0, 10);
    }

    formatConfidence(value) {
        const pct = Math.max(0, Math.min(100, Math.round(Number(value || 0) * 100)));
        return `${pct}% intent confidence`;
    }

    async loadContacts() {
        try {
            const contacts = await window.electronAPI.getRelationshipContacts();
            this.contacts = (contacts || []).map(c => ({
                id: c.id,
                name: c.display_name, company: c.company, role: c.role, warmth: c.warmth_score, depth: c.depth_score, centrality: c.network_centrality, summary: c.relationship_summary, metadata: c.metadata,
                last_contact_at: c.last_interaction_at,
                is_overdue_followup: c.status === 'needs_followup',
                is_weak_tie: c.status === 'cooling' || c.status === 'decaying',
                strength: c.strength_score,
                interaction_count: c.interaction_count_30d,
                recommendation: c.metadata?.recommendation || '',
                emails: c.metadata?.emails || [],
                phones: c.metadata?.phones || [],
                interests: c.metadata?.interests || []
            }));
            this.renderContactsList();
            console.log(`[App] Loaded ${this.contacts.length} contacts`);
        } catch (error) {
            console.warn('[App] Failed to load contacts:', error);
            this.contacts = [];
        }
    }

    renderContactsList() {
        const itemsContainer = document.getElementById('contacts-items');
        if (!itemsContainer) return;

        if (!this.contacts || this.contacts.length === 0) {
            itemsContainer.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 40px 20px;">
                    <div class="empty-icon" style="font-size: 48px; opacity: 0.3; margin-bottom: 16px;">
                        <span class="material-symbols-outlined">group</span>
                    </div>
                    <div class="empty-text">No contacts found</div>
                    <div class="empty-sub" style="font-size: 12px; color: var(--text-tertiary); margin-top: 8px;">Add email, calendar, or person records to detect contacts</div>
                </div>
            `;
            return;
        }

        const sorted = this.contacts.sort((a, b) => (b.strength || 0) - (a.strength || 0));
        
        itemsContainer.innerHTML = sorted.map((contact, idx) => {
            const lastContact = contact.last_contact_at ? new Date(contact.last_contact_at).toLocaleDateString() : 'Never';
            const statusIcon = contact.is_overdue_followup ? '⚠️' : contact.is_weak_tie ? '📉' : '✓';
            
            return `
                <div class="contact-item ${idx === 0 ? 'active' : ''}" data-contact-id="${contact.id}">
                    <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 4px;">
                        <div class="contact-name">${statusIcon} ${contact.name}</div>
                    </div>
                    <div class="contact-meta">Last: ${lastContact}</div>
                    <div class="strength-bar">
                        <div class="strength-bar-fill" style="width: ${(contact.strength || 0) * 100}%"></div>
                    </div>
                </div>
            `;
        }).join('');

        // Add click handlers
        document.querySelectorAll('.contact-item').forEach((item) => {
            item.addEventListener('click', () => {
                const id = item.dataset.contactId;
                this.loadContactDetailById(id);
            });
        });

        // Show first contact by default
        if (sorted.length > 0) {
            this.loadContactDetailById(sorted[0].id);
        }
    }
    showContactDetail(contact) {
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

    async setupContactsView() {
        // Setup search
        const searchInput = document.getElementById('contacts-search');
        if (searchInput) {
            searchInput.addEventListener('input', () => {
                const query = searchInput.value.toLowerCase();
                document.querySelectorAll('.contact-item').forEach(item => {
                    const name = item.querySelector('.contact-name').textContent.toLowerCase();
                    item.style.display = name.includes(query) ? '' : 'none';
                });
            });
        }

        // Setup sync button
        const syncBtn = document.getElementById('sync-contacts-btn');
        if (syncBtn) {
            syncBtn.addEventListener('click', async () => {
                syncBtn.classList.add('syncing');
                await this.loadContacts();
                syncBtn.classList.remove('syncing');
            });
        }

    }

    async updateChatSyncStatus() {
        const statusEl = document.getElementById('chat-sync-status');
        if (!statusEl) return;
        if (document.hidden || this.activeView !== 'action-view') return;

        try {
            const status = await window.electronAPI.getMemoryGraphStatus();
            const isSyncing = status?.episodeStatus === 'running' || status?.syncStatus === 'running';
            const textEl = statusEl.querySelector('.sync-text');
            
            if (isSyncing) {
                statusEl.classList.add('syncing');
                if (textEl) textEl.textContent = 'Updating work memory...';
                this.setPresenceMode('remembering');
            } else {
                statusEl.classList.remove('syncing');
                if (textEl) textEl.textContent = 'Work memory updated';
                this.setPresenceMode('waiting');
            }
        } catch (e) {
            console.warn('Failed to update sync status:', e);
        }
    }

    async setupChat() {
        this.chatSessions = await this.loadChatSessions();
        this.activeChatId = null;
        this.chatView?.classList.toggle('chat-history-collapsed', this.chatHistoryCollapsed);
        if (this.toggleChatHistoryButton) {
            this.toggleChatHistoryButton.textContent = this.chatHistoryCollapsed ? 'Show threads' : 'Hide threads';
        }
        this.chatDraftPrompts = this.getChatStarterPrompts();
        this.chatRefreshPrioritiesButton?.addEventListener('click', async () => {
            await this.generateSuggestions({ replace: true, silent: false, forceEngineRefresh: true });
            this.startNewChatDraft();
        });
        this.newChatButton?.addEventListener('click', () => this.startNewChatDraft());
        this.toggleChatHistoryButton?.addEventListener('click', () => {
            this.chatHistoryCollapsed = !this.chatHistoryCollapsed;
            localStorage.setItem('chatHistoryCollapsed', String(this.chatHistoryCollapsed));
            this.chatView?.classList.toggle('chat-history-collapsed', this.chatHistoryCollapsed);
            if (this.toggleChatHistoryButton) this.toggleChatHistoryButton.textContent = this.chatHistoryCollapsed ? 'Show threads' : 'Hide threads';
        });
        this.chatSyncStatus?.addEventListener('click', () => {
            this.showToast('Work memory is updating in the background');
        });

        this.sendChatButton?.addEventListener('click', () => this.sendChatMessage());

        this.chatMessages?.addEventListener('click', async (event) => {
            const toggle = event.target.closest('[data-thinking-trace-toggle]');
            if (toggle) {
                const panel = toggle.closest('.thinking-panel');
                if (!panel) return;
                const expanded = panel.dataset.expanded !== 'true';
                panel.dataset.expanded = String(expanded);
                toggle.setAttribute('aria-expanded', String(expanded));
                const chevron = panel.querySelector('.thinking-chevron');
                if (chevron) chevron.textContent = expanded ? '▾' : '▸';
                return;
            }

            const copy = event.target.closest('[data-thinking-trace-copy]');
            if (copy) {
                try {
                    await navigator.clipboard?.writeText(copy.dataset.thinkingTraceCopy || '');
                    copy.textContent = 'Copied';
                    setTimeout(() => { copy.textContent = 'Copy thinking trace'; }, 1500);
                } catch (_) {
                    copy.textContent = 'Copy failed';
                    setTimeout(() => { copy.textContent = 'Copy thinking trace'; }, 1500);
                }
            }
        });

        this.chatInput?.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                this.sendChatMessage();
            }
        });

        this.chatInput?.addEventListener('input', () => {
            this.chatInput.style.height = 'auto';
            this.chatInput.style.height = `${Math.min(this.chatInput.scrollHeight, 120)}px`;
            this.updateChatCharacterCounter();
        });

        this.startChatPromptRotation();

        this.renderChatHistory();
        this.renderActiveChat();
        this.updateChatSyncStatus();
    }

    async sendChatMessage() {
        const message = this.chatInput?.value.trim();
        if (!message) return;
        this.setPresenceMode('thinking');

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
        this.updateChatCharacterCounter();

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
            this.setPresenceMode('waiting');
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
            this.setPresenceMode('waiting');
        }
    }

    appendChatMessage(role, payload) {
        if (!this.chatMessages) return;

        const emptyState = this.chatMessages.querySelector('.empty-state, .claude-empty-state');
        if (emptyState) emptyState.remove();

        const message = document.createElement('div');
        message.className = `claude-message ${role}`;
        
        const avatarInitial = role === 'assistant' ? 'W' : 'Y';
        const roleLabel = role === 'assistant' ? 'Weave' : 'You';
        
        const content = typeof payload === 'object' && payload !== null ? (payload.content || '') : payload;
        const retrieval = typeof payload === 'object' && payload !== null ? (payload.retrieval || null) : null;
        const thinkingTrace = typeof payload === 'object' && payload !== null ? (payload.thinking_trace || retrieval?.thinking_trace || null) : null;
        
        const contentHtml = role === 'assistant'
            ? this.renderAssistantHTML(content, retrieval, thinkingTrace)
            : this.escapeHtml(content).replace(/\n/g, '<br>');

        message.innerHTML = `
            <div class="claude-avatar ${role}">${avatarInitial}</div>
            <div class="claude-message-body">
                <div class="claude-message-role">${roleLabel}</div>
                <div class="claude-message-content">${contentHtml}</div>
            </div>
        `;
        
        this.chatMessages.appendChild(message);
        this.scrollChatToBottom();
    }

    async typeAssistantResponse(rawPayload, options = {}) {
        const content = this.formatAssistantOutput(typeof rawPayload === 'object' && rawPayload !== null ? rawPayload.content : rawPayload);
        const retrieval = typeof rawPayload === 'object' && rawPayload !== null ? (rawPayload.retrieval || null) : null;
        const thinkingTrace = typeof rawPayload === 'object' && rawPayload !== null ? (rawPayload.thinking_trace || retrieval?.thinking_trace || null) : null;
        const includeThinkingTrace = options.includeThinkingTrace !== false;
        const message = document.createElement('div');
        message.className = 'claude-message assistant';
        if (!this.chatMessages) return;
        contentArea.innerHTML = `
            <div class="claude-avatar assistant">W</div>
            <div class="claude-message-body">
                <div class="claude-message-role">Weave</div>
                <div class="claude-message-content"></div>
            </div>
        `;
        this.chatMessages.appendChild(message);
        const contentArea = message.querySelector('.claude-message-content');

        let i = 0;
        const maxChars = 2600;
        const bounded = content.length > maxChars ? `${content.slice(0, maxChars)}

Would you like me to continue with more detail?` : content;
        const chunkSize = bounded.length > 900 ? 10 : 4;
        const frameDelay = bounded.length > 900 ? 6 : 12;
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

        while (i < bounded.length) {
            const slice = bounded.slice(0, i + chunkSize);
            contentArea.innerHTML = this.escapeHtml(slice).replace(/\n/g, '<br>');
            i += chunkSize;
            this.scrollChatToBottom();
            await sleep(frameDelay);
        }

        // Final pass: render concise rich text (no markdown headings).
        const uiBlocks = typeof rawPayload === 'object' && rawPayload !== null ? (rawPayload.ui_blocks || []) : [];
        message.innerHTML = this.renderAssistantHTML(bounded, retrieval, includeThinkingTrace ? thinkingTrace : null);

        // Append interactive action cards if present
        if (uiBlocks.length > 0) {
            const blocksContainer = document.createElement('div');
            blocksContainer.className = 'ui-blocks-container';
            blocksContainer.innerHTML = uiBlocks.map((block) => this.renderUICard(block)).join('');
            message.appendChild(blocksContainer);
            blocksContainer.querySelectorAll('[data-action]').forEach((btn) => {
                btn.addEventListener('click', () => {
                    try {
                        const actionData = JSON.parse(btn.dataset.actionData || '{}');
                        this.handleChatCardAction(btn.dataset.action, actionData);
                    } catch (_) {}
                });
            });
        }

        this.scrollChatToBottom();
        this.pushMessageToActiveChat('assistant', bounded, retrieval, thinkingTrace);
        this.renderChatHistory();
    }

    renderUICard(block) {
        const title = this.escapeHtml(block.title || '');
        const body = this.escapeHtml(block.body || '');
        const icon = block.type === 'error' ? '✗' : '✓';
        const actions = Array.isArray(block.actions) ? block.actions : [];
        const actionsHtml = actions.map((a) => {
            const safeData = this.escapeHtml(JSON.stringify(a.data || {}));
            return `<button class="ui-card-btn" data-action="${this.escapeHtml(a.action)}" data-action-data="${safeData}">${this.escapeHtml(a.label)}</button>`;
        }).join('');
        return `<div class="ui-card ui-card--${this.escapeHtml(block.type || 'info')}">
            <div class="ui-card-header"><span class="ui-card-icon">${icon}</span><span class="ui-card-title">${title}</span></div>
            ${body ? `<div class="ui-card-body">${body}</div>` : ''}
            ${actionsHtml ? `<div class="ui-card-actions">${actionsHtml}</div>` : ''}
        </div>`;
    }

    handleChatCardAction(action, data) {
        if (action === 'view_contact') {
            this.switchView('contacts-view');
        } else if (action === 'view_automations') {
            this.switchView('settings-view');
        } else if (action === 'open_url' && data.url) {
            window.open(data.url, '_blank', 'noopener');
        }
    }

    appendThinkingPanel() {
        const wrapper = document.createElement("div");
        wrapper.innerHTML = `
            <div class="claude-thinking-container claude-thinking-live" data-expanded="false">
                <div class="claude-thinking-header" type="button" aria-expanded="false">
                    <div class="claude-thinking-icon thinking">
                        <span class="material-symbols-outlined" style="font-size: 14px;">psychology</span>
                    </div>
                    <div class="claude-thinking-title">Thinking...</div>
                    <div class="claude-thinking-toggle">
                        <span class="material-symbols-outlined">expand_more</span>
                    </div>
                </div>
                <div class="claude-thinking-content">
                    <div class="claude-thinking-steps"></div>
                </div>
            </div>
        `;
        const panel = wrapper.firstElementChild;
        const startedAt = Date.now();
        const initialStages = [
            {
                id: 'query_analysis',
                step: 'query_analysis',
                title: 'Query Analysis',
                label: 'Analyzing your question',
                status: 'in_progress',
                detail: 'Intent, time scope, source mode, and retrieval plan are being prepared.',
                counts: {},
                preview_items: [],
                progress: 8,
                startedAt,
                updatedAt: startedAt,
                endedAt: null,
                durationMs: 0,
                order: 1
            },
            {
                id: 'hybrid_retrieval',
                step: 'hybrid_retrieval',
                title: 'Hybrid Retrieval',
                label: 'Hybrid Retrieval',
                status: 'pending',
                detail: 'Awaiting query analysis. Will apply metadata filters, vector search, lexical search, and graph expansion.',
                counts: {},
                preview_items: [],
                progress: 0,
                startedAt: null,
                updatedAt: startedAt,
                endedAt: null,
                durationMs: 0,
                order: 2
            },
            {
                id: 'reranking',
                step: 'reranking',
                title: 'Reranking & Assembly',
                label: 'Reranking & Assembly',
                status: 'pending',
                detail: 'Awaiting retrieval payload. Will assemble the most useful memory evidence.',
                counts: {},
                preview_items: [],
                progress: 0,
                startedAt: null,
                updatedAt: startedAt,
                endedAt: null,
                durationMs: 0,
                order: 3
            },
            {
                id: 'synthesis',
                step: 'synthesis',
                title: 'Synthesis',
                label: 'Synthesis',
                status: 'pending',
                detail: 'Awaiting evidence. Will generate the final answer from grounded context.',
                counts: {},
                preview_items: [],
                progress: 0,
                startedAt: null,
                updatedAt: startedAt,
                endedAt: null,
                durationMs: 0,
                order: 4
            }
        ];
        panel.__thinkingState = {
            stage: 'query_analysis',
            progress: 0,
            label: 'Analyzing your question',
            startedAt,
            completedAt: null,
            expanded: false,
            stages: initialStages,
            stageMap: new Map(initialStages.map((stage) => [stage.id, stage])),
            trace: []
        };
        panel.dataset.expanded = 'false';
        const toggleExpanded = () => {
            const state = panel.__thinkingState;
            state.expanded = !state.expanded;
            panel.__thinkingState = state;
            panel.dataset.expanded = String(state.expanded);
            const header = panel.querySelector('.thinking-live-header');
            if (header) header.setAttribute('aria-expanded', String(state.expanded));
            this.renderLiveThinkingPanel(panel);
        };
        panel.querySelector('.claude-thinking-header')?.addEventListener('click', toggleExpanded);
        panel.querySelector('.claude-copy-trace')?.addEventListener('click', async (event) => {
            const button = event.currentTarget;
            const state = panel.__thinkingState || {};
            const payload = JSON.stringify({
                total_ms: Math.max(0, (state.completedAt || Date.now()) - (state.startedAt || Date.now())),
                stages: state.stages || [],
                trace: state.trace || []
            }, null, 2);
            try {
                await navigator.clipboard?.writeText(payload);
                button.textContent = 'Copied';
                setTimeout(() => { button.textContent = 'Copy thinking trace'; }, 1500);
            } catch (_) {
                button.textContent = 'Copy failed';
                setTimeout(() => { button.textContent = 'Copy thinking trace'; }, 1500);
            }
        });
        panel.__thinkingTimer = setInterval(() => {
            if (document.hidden) return;
            this.renderLiveThinkingPanel(panel);
        }, 250);
        this.chatMessages.appendChild(panel);
        this.renderLiveThinkingPanel(panel);
        this.scrollChatToBottom();
        return panel;
    }

    getThinkingStageConfig(stage = '') {
        const key = String(stage || 'query_analysis').toLowerCase();
        const configs = {
            query_analysis: { title: 'Query Analysis', progress: 15, order: 1 },
            hybrid_retrieval: { title: 'Hybrid Retrieval', progress: 65, order: 2 },
            reranking: { title: 'Reranking & Assembly', progress: 80, order: 3 },
            synthesis: { title: 'Synthesis', progress: 95, order: 4 },
            complete: { title: 'Complete', progress: 100, order: 5 }
        };
        return configs[key] || { title: key.replace(/_/g, ' '), progress: 50, order: 9 };
    }

    canonicalThinkingStage(raw = {}, fallbackState = {}) {
        if (!raw || typeof raw !== 'object') return fallbackState.stage || 'query_analysis';
        if (raw.type === 'thinking_stage') {
            return String(raw.stage || fallbackState.stage || 'query_analysis');
        }
        const step = String(raw.step || '').toLowerCase();
        if (['routing', 'query_generation', 'planning'].includes(step)) return 'query_analysis';
        if (['memory_search', 'search_stage', 'vector_search_started', 'candidates_loaded', 'metadata_prefilter'].includes(step)) return 'hybrid_retrieval';
        if (['seed_selection', 'edge_expansion', 'ranking', 'direct_memory_fallback', 'direct_raw_event_lookup', 'actionable_todo_generation'].includes(step)) return 'reranking';
        if (['judging', 'web_search', 'synthesis', 'reflecting'].includes(step)) return 'synthesis';
        if (step === 'memory_writeback') return 'complete';
        return fallbackState.stage || 'query_analysis';
    }

    mapThinkingStage(raw = {}, fallbackState = {}) {
        if (!raw || typeof raw !== 'object') return null;

        if (raw.type === 'thinking_stage') {
            const progress = Number.isFinite(Number(raw.progress))
                ? Math.max(5, Math.min(100, Number(raw.progress)))
                : (Number(fallbackState.progress) || 15);
            return {
                stage: String(raw.stage || fallbackState.stage || 'query_analysis'),
                progress,
                label: String(raw.label || fallbackState.label || 'Thinking...').trim() || 'Thinking...'
            };
        }

        const step = String(raw.step || '').toLowerCase();
        if (!step) return null;

        if (['routing', 'query_generation', 'planning'].includes(step)) {
            return { stage: 'query_analysis', progress: 20, label: 'Analyzing your question' };
        }
        if (['memory_search', 'search_stage', 'vector_search_started', 'candidates_loaded', 'metadata_prefilter'].includes(step)) {
            return { stage: 'hybrid_retrieval', progress: 60, label: 'Searching your memory' };
        }
        if (['seed_selection', 'edge_expansion', 'ranking', 'direct_memory_fallback', 'direct_raw_event_lookup', 'actionable_todo_generation'].includes(step)) {
            return { stage: 'reranking', progress: 80, label: 'Organizing results' };
        }
        if (['judging', 'web_search', 'synthesis', 'reflecting'].includes(step)) {
            return { stage: 'synthesis', progress: 95, label: 'Generating response' };
        }
        if (step === 'memory_writeback') {
            return { stage: 'complete', progress: 100, label: 'Done' };
        }
        return null;
    }

    formatThinkingDuration(ms = 0) {
        const value = Math.max(0, Number(ms || 0));
        if (value < 1000) return `${Math.round(value)}ms`;
        return `${(value / 1000).toFixed(value < 10000 ? 1 : 0)}s`;
    }

    statusRank(status = '') {
        const key = String(status || '').toLowerCase();
        if (['completed', 'complete', 'done'].includes(key)) return 3;
        if (['retry', 'in_progress', 'started', 'running'].includes(key)) return 2;
        if (['pending', 'queued'].includes(key)) return 1;
        return 2;
    }

    normalizeLiveThinkingEvent(raw = {}, state = {}) {
        const now = Date.now();
        const canonical = this.canonicalThinkingStage(raw, state);
        const config = this.getThinkingStageConfig(canonical);
        const status = raw.status || (raw.type === 'thinking_stage' ? 'in_progress' : 'in_progress');
        const startedAt = state.startedAt || now;
        const existing = state.stageMap?.get(canonical) || null;
        const counts = raw.counts && typeof raw.counts === 'object' ? raw.counts : {};
        const previewItems = Array.isArray(raw.preview_items) ? raw.preview_items.filter(Boolean).slice(0, 6) : [];
        const detail = raw.detail || '';
        const label = raw.label || config.title;
        const next = {
            id: canonical,
            step: raw.step || canonical,
            label,
            title: config.title,
            status,
            detail,
            counts,
            preview_items: previewItems,
            progress: Number.isFinite(Number(raw.progress)) ? Number(raw.progress) : config.progress,
            startedAt: existing?.startedAt || now,
            updatedAt: now,
            endedAt: ['completed', 'complete', 'done'].includes(String(status).toLowerCase()) ? now : existing?.endedAt || null,
            order: config.order
        };
        if (!existing && canonical !== 'query_analysis') {
            const previous = (state.stages || [])
                .filter((item) => item.order < next.order && !item.endedAt)
                .sort((a, b) => b.order - a.order)[0];
            if (previous) previous.endedAt = now;
        }
        if (canonical === 'complete') {
            next.startedAt = existing?.startedAt || now;
            next.endedAt = now;
        }
        next.durationMs = Math.max(0, (next.endedAt || now) - (next.startedAt || startedAt));
        return next;
    }

    mergeThinkingStage(state, stage) {
        if (!state.stageMap) state.stageMap = new Map();
        for (const item of state.stageMap.values()) {
            const itemStatus = String(item.status || '').toLowerCase();
            if (item.order < stage.order && !item.endedAt && !['pending', 'queued'].includes(itemStatus)) {
                item.endedAt = stage.startedAt || Date.now();
                item.status = 'completed';
                item.durationMs = Math.max(0, item.endedAt - (item.startedAt || item.endedAt));
            }
        }
        const existing = state.stageMap.get(stage.id);
        if (existing) {
            const merged = {
                ...existing,
                ...stage,
                counts: { ...(existing.counts || {}), ...(stage.counts || {}) },
                preview_items: stage.preview_items?.length ? stage.preview_items : (existing.preview_items || []),
                detail: stage.detail || existing.detail || '',
                startedAt: existing.startedAt || stage.startedAt,
                endedAt: stage.endedAt || existing.endedAt || null,
                status: this.statusRank(stage.status) >= this.statusRank(existing.status) ? stage.status : existing.status
            };
            merged.durationMs = Math.max(0, (merged.endedAt || Date.now()) - (merged.startedAt || Date.now()));
            state.stageMap.set(stage.id, merged);
        } else {
            state.stageMap.set(stage.id, stage);
        }
        state.stages = Array.from(state.stageMap.values()).sort((a, b) => a.order - b.order);
        return state;
    }

    renderStageMetrics(stage) {
        const rows = [];
        const counts = stage?.counts && typeof stage.counts === 'object' ? stage.counts : {};
        Object.entries(counts)
            .filter(([, value]) => value !== null && value !== undefined && value !== 0)
            .slice(0, 8)
            .forEach(([key, value]) => {
                rows.push(`<div class="thinking-metric-row"><span>${this.escapeHtml(String(key).replace(/_/g, ' '))}</span><strong>${this.escapeHtml(String(value))}</strong></div>`);
            });
        if (stage?.detail) rows.unshift(`<div class="thinking-stage-detail">${this.escapeHtml(stage.detail)}</div>`);
        const previews = Array.isArray(stage?.preview_items) ? stage.preview_items.slice(0, 5) : [];
        if (previews.length) {
            rows.push(`<div class="thinking-query-preview">${previews.map((item) => `<span title="${this.escapeHtml(String(item))}">${this.escapeHtml(String(item))}</span>`).join('')}</div>`);
        }
        return rows.length ? `<div class="thinking-stage-metrics">${rows.join('')}</div>` : '';
    }

    renderLiveThinkingPanel(panel) {
        if (!panel?.isConnected) {
            if (panel?.__thinkingTimer) clearInterval(panel.__thinkingTimer);
            return;
        }
        const state = panel.__thinkingState || {};
        const now = Date.now();
        const elapsedMs = Math.max(0, (state.completedAt || now) - (state.startedAt || now));
        
        const header = panel.querySelector('.claude-thinking-header');
        const toggle = panel.querySelector('.claude-thinking-toggle');
        const titleLabel = panel.querySelector('.claude-thinking-title');
        const list = panel.querySelector('.claude-thinking-steps');
        const content = panel.querySelector('.claude-thinking-content');

        if (header) header.setAttribute('aria-expanded', String(Boolean(state.expanded)));
        if (toggle) toggle.classList.toggle('expanded', Boolean(state.expanded));
        if (content) content.classList.toggle('expanded', Boolean(state.expanded));
        
        const currentStage = state.stages?.find((item) => !item.endedAt && !['pending', 'queued'].includes(String(item.status || '').toLowerCase()) && item.id !== 'complete')
            || state.stages?.filter((item) => item.endedAt).slice(-1)[0]
            || state.stages?.find((item) => !item.endedAt)
            || { title: state.label || 'Thinking', status: 'in_progress', durationMs: elapsedMs };

        if (titleLabel) {
            titleLabel.textContent = state.completedAt 
                ? `Thought for ${this.formatThinkingDuration(elapsedMs)}`
                : `Thinking: ${currentStage?.title || state.label || 'Analyzing'}`;
        }

        if (list) {
            const stages = state.stages?.length ? state.stages : [{
                id: 'query_analysis',
                title: 'Query Analysis',
                label: 'Analyzing your question',
                status: 'in_progress',
                durationMs: elapsedMs,
                order: 1
            }];
            list.innerHTML = stages.map((stage, index) => {
                const rawStatus = String(stage.status || '').toLowerCase();
                const isComplete = ['completed', 'complete', 'done'].includes(rawStatus) || !!stage.endedAt;
                const isPending = ['pending', 'queued'].includes(rawStatus) && !stage.endedAt;
                
                const duration = isPending
                    ? ''
                    : this.formatThinkingDuration(stage.durationMs || ((stage.endedAt || now) - (stage.startedAt || now)));
                
                return `
                    <div class="claude-thinking-step">
                        <div class="claude-thinking-step-dot"></div>
                        <div class="claude-thinking-step-text">
                            ${this.escapeHtml(stage.title || stage.label || stage.id)}
                            ${duration ? `<span class="claude-thinking-step-time">(${duration})</span>` : ''}
                        </div>
                    </div>
                `;
            }).join('');
        }
        panel.dataset.expanded = String(Boolean(state.expanded));
        if (state.completedAt) {
            panel.classList.remove('claude-thinking-live');
        }
    }

    renderAnimatedThinkingTrace(thinkingTrace, retrieval = null) {
        const trace = thinkingTrace || retrieval?.thinking_trace || {};
        const stageTrace = Array.isArray(trace?.stage_trace)
            ? trace.stage_trace
            : (Array.isArray(retrieval?.stage_trace) ? retrieval.stage_trace : []);
        if (!stageTrace.length && !trace?.thinking_summary) return '';

        const stages = stageTrace.map((raw, index) => {
            const canonical = this.canonicalThinkingStage(raw, { stage: 'query_analysis' });
            const config = this.getThinkingStageConfig(canonical);
            const counts = raw?.counts && typeof raw.counts === 'object' ? raw.counts : {};
            const previews = Array.isArray(raw?.preview_items) ? raw.preview_items.filter(Boolean).slice(0, 5) : [];
            return {
                id: canonical,
                title: raw?.label || config.title,
                status: raw?.status || 'completed',
                detail: raw?.detail || '',
                counts,
                preview_items: previews,
                progress: raw?.progress || config.progress,
                durationMs: raw?.duration_ms || raw?.durationMs || Math.max(120, 220 + (index * 160)),
                order: config.order || index
            };
        });
        const searchQueries = trace?.search_queries || {};
        const filters = Array.isArray(trace?.filters) ? trace.filters : [];
        const resultSummary = trace?.results_summary || {};
        const uniqueStages = [];
        const seen = new Set();
        for (const stage of stages) {
            const key = `${stage.id}:${stage.title}:${stage.detail}`;
            if (seen.has(key)) continue;
            seen.add(key);
            if (stage.id === 'query_analysis') {
                const semantic = Array.isArray(searchQueries.context) ? searchQueries.context : [];
                const lexical = Array.isArray(searchQueries.lexical) ? searchQueries.lexical : [];
                const web = Array.isArray(searchQueries.web) ? searchQueries.web : [];
                stage.counts = {
                    semantic_queries: semantic.length,
                    lexical_terms: lexical.length,
                    web_queries: web.length,
                    filters_applied: filters.length,
                    ...(stage.counts || {})
                };
                stage.preview_items = [
                    ...semantic.slice(0, 3),
                    ...lexical.slice(0, 2).map((term) => `lexical: ${term}`),
                    ...web.slice(0, 2).map((term) => `web: ${term}`),
                    ...filters.slice(0, 3).map((filter) => `filter: ${filter.label || 'Filter'}=${filter.value || ''}`)
                ].filter(Boolean);
            }
            if (stage.id === 'hybrid_retrieval' || stage.id === 'reranking') {
                stage.counts = {
                    seeds: resultSummary.seed_count,
                    evidence: resultSummary.evidence_count,
                    primary_nodes: Array.isArray(trace?.primary_nodes) ? trace.primary_nodes.length : undefined,
                    support_nodes: Array.isArray(trace?.support_nodes) ? trace.support_nodes.length : undefined,
                    ...(stage.counts || {})
                };
                if (!stage.preview_items?.length && Array.isArray(resultSummary.details)) {
                    stage.preview_items = resultSummary.details.slice(0, 4);
                }
            }
            uniqueStages.push(stage);
        }
        const totalMs = uniqueStages.reduce((sum, stage) => sum + Number(stage.durationMs || 0), 0);
        const summary = trace?.thinking_summary || retrieval?.retrieval_plan?.router_reason || 'Thinking trace captured.';
        const traceJson = this.escapeHtml(JSON.stringify({
            summary,
            stages: uniqueStages,
            filters: trace?.filters || [],
            search_queries: trace?.search_queries || {},
            results_summary: trace?.results_summary || {}
        }, null, 2));
        const stageHtml = uniqueStages.length
            ? uniqueStages.map((stage, index) => {
                const duration = this.formatThinkingDuration(stage.durationMs || 0);
                return `
                    <div class="thinking-stage-item status-complete" style="--stage-index:${index};">
                        <div class="thinking-stage-head">
                            <span class="thinking-stage-icon">✓</span>
                            <span class="thinking-stage-title">${this.escapeHtml(stage.title || stage.id)}</span>
                            <span class="thinking-stage-time">${this.escapeHtml(duration)}</span>
                        </div>
                        ${this.renderStageMetrics(stage)}
                    </div>
                `;
            }).join('')
            : `<div class="thinking-stage-detail">${this.escapeHtml(summary)}</div>`;

        return `
            <div class="thinking-panel thinking-trace-live complete" data-expanded="false">
                <button class="thinking-live-header" type="button" aria-expanded="false" data-thinking-trace-toggle>
                    <span class="thinking-chevron">▸</span>
                    <span class="thinking-live-title">Thinking</span>
                    <span class="thinking-live-stage">${this.escapeHtml(summary)}</span>
                    <span class="thinking-live-elapsed">${this.escapeHtml(this.formatThinkingDuration(totalMs))} total</span>
                </button>
                <div class="thinking-progress" aria-label="Thinking progress">
                    <div class="thinking-bar" style="width:100%"></div>
                    <span class="thinking-progress-label">100%</span>
                </div>
                <div class="thinking-live-content">
                    <div class="thinking-stage-list">${stageHtml}</div>
                    <div class="thinking-live-actions">
                        <button class="thinking-copy-trace" type="button" data-thinking-trace-copy="${traceJson}">Copy thinking trace</button>
                    </div>
                </div>
            </div>
        `;
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
        const currentState = panel.__thinkingState || { stage: 'query_analysis', progress: 15, label: 'Analyzing your question' };
        const mapped = this.mapThinkingStage(payload, currentState);
        const eventStage = this.normalizeLiveThinkingEvent(payload, currentState);
        let nextState = this.mergeThinkingStage(currentState, eventStage);

        // Ensure progress never regresses visually.
        const progress = Math.max(Number(currentState.progress || 0), Number(mapped?.progress || eventStage.progress || 0));
        nextState = {
            ...nextState,
            ...(mapped || {}),
            progress,
            stage: eventStage.id,
            label: mapped?.label || eventStage.label || currentState.label,
            trace: [...(currentState.trace || []), payload].slice(-80)
        };
        panel.__thinkingState = nextState;
        this.renderLiveThinkingPanel(panel);
    }

    finalizeThinkingPanel(panel, payload) {
        if (!panel) return;
        const state = panel.__thinkingState || {};
        if (panel.__thinkingTimer) clearInterval(panel.__thinkingTimer);
        const now = Date.now();
        const trace = payload?.thinking_trace || payload?.retrieval?.thinking_trace || null;
        const finalStageTrace = Array.isArray(trace?.stage_trace)
            ? trace.stage_trace
            : (Array.isArray(payload?.retrieval?.stage_trace) ? payload.retrieval.stage_trace : []);
        for (const rawStage of finalStageTrace) {
            const stage = this.normalizeLiveThinkingEvent({ ...rawStage, status: rawStage.status || 'completed' }, state);
            stage.endedAt = stage.endedAt || now;
            stage.durationMs = Math.max(0, stage.endedAt - (stage.startedAt || now));
            this.mergeThinkingStage(state, stage);
        }
        state.completedAt = now;
        state.progress = 100;
        state.stage = 'complete';
        state.label = 'Complete';
        state.expanded = true;
        state.finalPayload = payload || null;
        panel.__thinkingState = state;
        panel.classList.add('ready', 'complete');
        panel.classList.remove('live');
        this.renderLiveThinkingPanel(panel);
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

            if (/^```/.test(line)) {
                const info = line.replace(/^```/, '').trim() || 'text';
                i += 1;
                const codeLines = [];
                while (i < lines.length && !/^```/.test(lines[i].trim())) {
                    codeLines.push(lines[i]);
                    i += 1;
                }
                if (i < lines.length && /^```/.test(lines[i].trim())) i += 1;
                const code = codeLines.join('\n');
                const artifactTitle = info.toLowerCase().includes('artifact') ? 'Artifact' : `${info.toUpperCase()} block`;
                blocks.push(`
                    <div class="chat-artifact">
                        <div class="chat-artifact-header">
                            <span>${this.escapeHtml(artifactTitle)}</span>
                            <span>${this.escapeHtml(info)}</span>
                        </div>
                        <pre><code>${this.escapeHtml(code)}</code></pre>
                    </div>
                `);
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
                blocks.push(`<div class="msg-table-wrap"><table class="msg-table"><thead>${headHtml}</thead><tbody>${rowHtml}</tbody></table></div>`);
                continue;
            }

            blocks.push(`<p class="msg-p">${this.formatInlineMarkdown(line)}</p>`);
            i += 1;
        }

        const traceHtml = this.renderAnimatedThinkingTrace(thinkingTrace || retrieval?.thinking_trace || null, retrieval);
        return `${traceHtml}<div class="msg-rich">${blocks.join('')}</div>`;
    }

    renderThinkingTraceCard(thinkingTrace, retrieval = null) {
        if (!thinkingTrace && !retrieval) return '';
        const trace = thinkingTrace || {};
        const sourceBadges = '';
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
        const reasoningChain = Array.isArray(trace?.reasoning_chain) ? trace.reasoning_chain : [];
        const reasoningHtml = reasoningChain.length
            ? `<div class="thinking-trace-list">${reasoningChain.map((item) => `<div><strong>${this.escapeHtml((item.stage || '').replace(/_/g, ' '))}</strong> — ${this.escapeHtml(item.summary || '')}${item.detail ? `: ${this.escapeHtml(item.detail)}` : ''}</div>`).join('')}</div>`
            : '<div class="thinking-trace-empty">No reasoning chain was captured.</div>';
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
                    ${sourceBadges ? `<span class="ai-section-legend" style="margin-left:auto;">${sourceBadges}</span>` : ''}
                </summary>
                <div class="thinking-trace-sections">
                    <div class="thinking-trace-penetration">
                        ${penetrationHtml}
                    </div>
                    <section class="thinking-trace-section">
                        <div class="thinking-trace-section-title">Reasoning steps</div>
                        ${reasoningHtml}
                    </section>
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
        this.chatDraftPrompts = this.getChatStarterPrompts();
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
                <div class="chat-history-empty">No threads yet</div>
            `;
            return;
        }
        this.chatHistoryList.innerHTML = sorted.map((session) => {
            const last = session.messages?.[session.messages.length - 1];
            const updatedAt = session.updatedAt || last?.ts || session.createdAt;
            return `
                <button class="chat-history-item ${session.id === this.activeChatId ? 'active' : ''}" data-chat-id="${this.escapeHtml(session.id)}">
                    <span class="chat-history-dot" aria-hidden="true"></span>
                    <div class="chat-history-title" title="${this.escapeHtml(session.title || 'New thread')}">${this.escapeHtml(session.title || 'New thread')}</div>
                    <div class="chat-history-preview">${this.escapeHtml((last?.content || 'No messages yet').slice(0, 60))}</div>
                    <div class="chat-history-time">${this.escapeHtml(this.relativeTime(updatedAt))}</div>
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
            const prompts = Array.isArray(this.chatDraftPrompts) && this.chatDraftPrompts.length
                ? this.chatDraftPrompts
                : this.getChatStarterPrompts();
            if (this.chatThreadTitle) this.chatThreadTitle.textContent = 'New chat';
            this.chatMessages.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 60px 20px;">
                    <div class="empty-icon" style="font-size: 48px; opacity: 0.3; margin-bottom: 16px;">
                        <span class="material-symbols-outlined">forum</span>
                    </div>
                    <div class="empty-text" style="font-size: 16px; font-weight: 500; color: var(--text-secondary); margin-bottom: 8px;">Start a new chat</div>
                    <div class="empty-sub" style="font-size: 13px; color: var(--text-tertiary); line-height: 1.5;">Use one of these context-aware prompts or open a previous thread from the left.</div>
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
        if (this.chatThreadTitle) this.chatThreadTitle.textContent = chat.title || 'Current work thread';

        this.chatMessages.innerHTML = '';
        if (!chat.messages?.length) {
            const prompts = this.getChatStarterPrompts();
            this.chatMessages.innerHTML = `
                <div class="empty-state" style="text-align: center; padding: 60px 20px;">
                    <div class="empty-icon" style="font-size: 48px; opacity: 0.3; margin-bottom: 16px;">
                        <span class="material-symbols-outlined">forum</span>
                    </div>
                    <div class="empty-text" style="font-size: 16px; font-weight: 500; color: var(--text-secondary); margin-bottom: 8px;">Ask about your work</div>
                    <div class="empty-sub" style="font-size: 13px; color: var(--text-tertiary); line-height: 1.5;">Use work memory to prepare for meetings, inspect client history, or close follow-ups.</div>
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
        const todos = this.getVisibleTodos().slice(0, 4);
        const topTodo = todos[0];
        const clientTodo = todos.find((todo) => ['followup', 'relationship_intelligence'].includes(todo.category) || this.hasClientSignal(todo));
        const deliveryTodo = todos.find((todo) => ['work', 'creative', 'study'].includes(todo.category));
        const brief = (this.morningBriefs || [])[0];
        const firstPriority = brief?.priorities?.[0]?.title;
        if (topTodo?.title) prompts.push(`What is the best next move for "${topTodo.title}" right now?`);
        if (topTodo?.title) prompts.push(`What are my top priorities right now based on memory, starting with "${topTodo.title}"?`);
        if (clientTodo?.title) prompts.push(`Draft the right outreach or follow-up for "${clientTodo.title}".`);
        if (deliveryTodo?.title) prompts.push(`What could block delivery on "${deliveryTodo.title}", and how should I handle it?`);
        if (firstPriority) prompts.push(`Turn "${firstPriority}" into a concrete plan for today.`);
        prompts.push('Which client follow-up matters most today, and why?');
        prompts.push('Prepare me for the next meeting or commitment on my plate.');
        prompts.push('What is the riskiest open delivery item in my current work?');
        return Array.from(new Set(prompts.filter(Boolean))).slice(0, 3);
    }

    setupSettings() {
        this.captureToggle = document.getElementById("capture-toggle");
        this.captureToggle?.addEventListener("click", async () => {
            this.captureToggle.classList.toggle("active");
            const enabled = this.captureToggle.classList.contains("active");
            await this.setDesktopCaptureEnabled(enabled);
        });
        this.openScreenRecordingButton?.addEventListener('click', async () => {
            await this.openScreenRecordingSettings();
        });
        this.copyDailyOcrButton?.addEventListener('click', async () => {
            await this.copyDailyOcrText();
        });
        this.refreshDailyOcrButton?.addEventListener('click', async () => {
            await this.refreshDailyOcrText();
        });
        this.suggestionProviderSelect?.addEventListener('change', () => {
            if (this.suggestionBaseUrlInput) {
                this.suggestionBaseUrlInput.disabled = this.suggestionProviderSelect.value !== 'ollama';
            }
            this.renderSuggestionProviderStatus();
        });
        [this.suggestionModelInput, this.suggestionBaseUrlInput, this.suggestionApiKeyInput].forEach((input) => {
            input?.addEventListener('input', () => this.renderSuggestionProviderStatus());
        });
        this.saveSuggestionProviderButton?.addEventListener('click', async () => {
            await this.saveSuggestionProviderSettings();
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
        if (this.settingsDataLoading) {
            await this.settingsDataLoading;
            return;
        }

        this.settingsDataLoading = Promise.all([
            this.loadGoogleStatus(),
            this.loadExtensionStatus(),
            this.loadVoiceControlStatus(),
            this.loadBrowserHistory(),
            this.loadSensorData()
        ]);
        try {
            await this.settingsDataLoading;
            this.settingsDataLoaded = true;
        } finally {
            this.settingsDataLoading = null;
        }
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
        this.suggestionProviderStatus.textContent = `Provider: ${provider || 'default'} • Model: ${model || 'deepseek-chat'}${hasApiKey ? ' • key set' : ' • missing key'}`;
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

    async saveSuggestionProviderSettings() {
        if (!window.electronAPI?.saveSuggestionLLMSettings) {
            this.showToast('Provider settings are unavailable');
            return;
        }
        const button = this.saveSuggestionProviderButton;
        if (button) {
            button.disabled = true;
            button.textContent = 'Saving...';
        }
        try {
            const payload = {
                provider: this.suggestionProviderSelect?.value || 'deepseek',
                model: this.suggestionModelInput?.value || '',
                baseUrl: this.suggestionBaseUrlInput?.value || '',
                apiKey: this.suggestionApiKeyInput?.value || ''
            };
            const saved = await window.electronAPI.saveSuggestionLLMSettings(payload);
            if (this.suggestionApiKeyInput) this.suggestionApiKeyInput.value = '';
            this.renderSuggestionProviderStatus(saved || payload);
            this.showToast('Provider settings saved');
        } catch (error) {
            console.error('Failed to save suggestion provider settings:', error);
            this.showToast('Provider settings failed to save');
        } finally {
            if (button) {
                button.disabled = false;
                button.textContent = 'Save provider';
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

    async openScreenRecordingSettings() {
        const button = this.openScreenRecordingButton;
        if (button) button.textContent = 'Opening...';
        try {
            const result = await window.electronAPI.openScreenRecordingSettings?.();
            if (result?.status === 'error') {
                throw new Error(result.error || 'Unable to open Screen Recording settings');
            }
            this.showToast('Opened Screen Recording settings');
            setTimeout(() => this.loadSensorData(), 1200);
        } catch (error) {
            console.error('Failed to open Screen Recording settings:', error);
            this.showToast('Failed to open Screen Recording settings');
        } finally {
            if (button) button.textContent = 'Enable Screen Recording';
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
        const transcript = payload.transcript || "I'm here...";
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
            this.latestSensorEvents = Array.isArray(events) ? events : [];

            this.desktopCaptureEnabled = Boolean(status?.enabled);
            localStorage.setItem('desktopCaptureEnabled', String(this.desktopCaptureEnabled));
            document.getElementById('capture-toggle')?.classList.toggle('active', this.desktopCaptureEnabled);

            if (this.captureStatusText) {
                const permission = status?.screenPermission ? `Permission: ${status.screenPermission}` : 'Permission unknown';
                const captures = status?.totalCaptures || 0;
                const interval = status?.intervalSeconds ? `${status.intervalSeconds}s interval` : '30s interval';
                const transport = status?.transport ? ` • ${String(status.transport).replace(/-/g, ' ')}` : '';
                this.captureStatusText.textContent = `${status?.active ? 'Active' : 'Idle'} • ${captures} captures • ${permission} • ${interval}${transport}`;
            }
            if (this.openScreenRecordingButton) {
                const permitted = ['granted', 'authorized'].includes(String(status?.screenPermission || '').toLowerCase());
                this.openScreenRecordingButton.disabled = permitted;
                this.openScreenRecordingButton.textContent = permitted ? 'Screen Recording Enabled' : 'Enable Screen Recording';
            }

            this.renderScreenCaptures(events || []);
            this.renderDailyOcrExport(this.latestSensorEvents);
        } catch (error) {
            console.error('Failed to load sensor data:', error);
            if (this.captureStatusText) this.captureStatusText.textContent = 'Failed to read capture status';
            if (this.screenCapturesList) {
                this.screenCapturesList.innerHTML = '<div class="history-row"><div class="history-row-title">No screen captures available</div></div>';
            }
            this.renderDailyOcrExport([]);
        }
    }

    async refreshDailyOcrText() {
        try {
            const events = await window.electronAPI.getSensorEvents();
            this.latestSensorEvents = Array.isArray(events) ? events : [];
            this.renderDailyOcrExport(this.latestSensorEvents);
            this.showToast('Daily OCR refreshed');
        } catch (error) {
            console.error('Failed to refresh daily OCR:', error);
            this.showToast('Failed to refresh OCR text');
        }
    }

    renderDailyOcrExport(events = []) {
        if (!this.dailyOcrText && !this.dailyOcrMeta) return;
        const result = this.buildTodayOcrExport(events);
        if (this.dailyOcrText) this.dailyOcrText.value = result.text;
        if (this.dailyOcrMeta) this.dailyOcrMeta.textContent = result.meta;
    }

    buildTodayOcrExport(events = []) {
        const rows = (Array.isArray(events) ? events : [])
            .filter((event) => this.isTodayTimestamp(event?.timestamp) && String(event?.text || '').trim())
            .sort((a, b) => Number(a?.timestamp || 0) - Number(b?.timestamp || 0));

        if (!rows.length) {
            return {
                text: '',
                meta: 'No OCR text captured yet today.'
            };
        }

        const text = rows.map((event, index) => {
            const time = event?.captured_at_local || this.formatFullTimestamp(event?.timestamp);
            const app = String(event?.activeApp || event?.sourceName || 'Desktop').trim();
            const windowTitle = String(event?.activeWindowTitle || event?.title || '').trim();
            const header = [`[${index + 1}] ${time}`, app].concat(windowTitle ? [windowTitle] : []).join(' | ');
            return `${header}\n${String(event?.text || '').trim()}`;
        }).join('\n\n---\n\n');

        const totalChars = rows.reduce((sum, event) => sum + String(event?.text || '').trim().length, 0);
        return {
            text,
            meta: `${rows.length} captures with OCR today • ${totalChars.toLocaleString()} characters`
        };
    }

    isTodayTimestamp(value) {
        const date = new Date(Number(value) || value || 0);
        if (!date || Number.isNaN(date.getTime())) return false;
        const now = new Date();
        return date.getFullYear() === now.getFullYear()
            && date.getMonth() === now.getMonth()
            && date.getDate() === now.getDate();
    }

    formatFullTimestamp(value) {
        const date = new Date(Number(value) || value || 0);
        if (!date || Number.isNaN(date.getTime())) return 'Unknown time';
        return date.toLocaleString();
    }

    async copyDailyOcrText() {
        const text = String(this.dailyOcrText?.value || '').trim();
        if (!text) {
            this.showToast('No OCR text available for today');
            return;
        }
        try {
            await this.writeToClipboard(text);
            this.showToast('Copied today\'s OCR');
        } catch (error) {
            console.error('Failed to copy daily OCR:', error);
            this.showToast('Copy failed');
        }
    }

    async writeToClipboard(text) {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(text);
            return;
        }

        const helper = document.createElement('textarea');
        helper.value = text;
        helper.setAttribute('readonly', 'readonly');
        helper.style.position = 'fixed';
        helper.style.opacity = '0';
        helper.style.pointerEvents = 'none';
        document.body.appendChild(helper);
        helper.select();
        helper.setSelectionRange(0, helper.value.length);
        const success = document.execCommand('copy');
        document.body.removeChild(helper);
        if (!success) throw new Error('execCommand copy failed');
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

        } catch (error) {
            console.error('Failed to load memory graph status:', error);
            this.setText('mem-processing-status', 'Unavailable');
        }
    }

    async searchMemoryGraph() {
        if (!this.memoryResults) return;
        this.memoryResults.innerHTML = '<div class="history-row"><div class="history-row-title">Searching work memory...</div></div>';

        const query = (this.memorySearchInput?.value || '').trim();
        const filterType = this.memoryFilterType?.value || 'all';
        const includeRaw = false; // Search now only returns episodes and semantics
        const nodeTypes = filterType === 'all' ? ['episode', 'semantic', 'insight'] : 
                         filterType === 'episode' ? ['episode'] : 
                         filterType === 'semantic' ? ['semantic'] : 
                         filterType === 'insight' ? ['insight'] : [];

        try {
            const nodes = await window.electronAPI.searchMemoryGraph(query, { nodeTypes, limit: 120 });

            const normalizedNodes = (nodes || []).map((item) => ({
                id: item.id,
                type: item.type,
                data: item.data || {},
                timestamp: item.data?.timestamp || item.data?.date || 0
            }));

            this.memorySearchResults = normalizedNodes
                .sort((a, b) => new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime())
                .slice(0, 200);

            this.renderMemoryResults(this.memorySearchResults);
        } catch (error) {
            console.error('Memory graph search failed:', error);
            this.memoryResults.innerHTML = '<div class="history-row"><div class="history-row-title">Work memory search failed</div></div>';
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

    setupMicroInteractions() {
        document.addEventListener('pointerdown', (event) => {
            const target = event.target.closest('button, .suggestion-card, .chat-history-item');
            if (!target || target.disabled || target.classList.contains('no-ripple')) return;
            if (!target.classList.contains('ripple-ready')) target.classList.add('ripple-ready');

            const rect = target.getBoundingClientRect();
            const size = Math.max(rect.width, rect.height) * 1.35;
            const ripple = document.createElement('span');
            ripple.className = 'ui-ripple';
            ripple.style.width = `${size}px`;
            ripple.style.height = `${size}px`;
            ripple.style.left = `${event.clientX - rect.left - size / 2}px`;
            ripple.style.top = `${event.clientY - rect.top - size / 2}px`;
            target.appendChild(ripple);
            setTimeout(() => ripple.remove(), 480);
        }, { passive: true });
    }

    updateChatCharacterCounter() {
        if (!this.chatCharCounter || !this.chatInput) return;
        const count = this.chatInput.value.length;
        this.chatCharCounter.textContent = count > 120 ? `${count}/250` : '';
        this.chatCharCounter.classList.toggle('visible', count > 120);
    }

    relativeTime(timestamp) {
        const time = Number(timestamp || 0);
        if (!time) return 'Just now';
        const delta = Math.max(0, Date.now() - time);
        const minute = 60 * 1000;
        const hour = 60 * minute;
        const day = 24 * hour;
        if (delta < minute) return 'Just now';
        if (delta < hour) return `${Math.floor(delta / minute)}m ago`;
        if (delta < day) return `${Math.floor(delta / hour)}h ago`;
        if (delta < 2 * day) return 'Yesterday';
        return `${Math.floor(delta / day)}d ago`;
    }

    updateGreeting() {
        const element = document.getElementById('today-greeting');
        if (!element) return;

        const hour = new Date().getHours();
        const name = this.userName || 'Willem';
        const visibleCount = this.getVisibleTodos ? this.getVisibleTodos().length : (this.todos || []).length;
        const period = hour < 12 ? 'Morning' : (hour < 18 ? 'Afternoon' : 'Evening');
        const focusLine = visibleCount === 0
            ? 'no urgent priorities.'
            : (visibleCount === 1 ? '1 priority open.' : `${visibleCount} priorities open.`);
        element.textContent = `${period}, ${name} - ${focusLine}`;
        this.applyAmbientTone(hour);
        this.updatePresenceSummary(this.getVisibleTodos ? this.getVisibleTodos() : (this.todos || []));
    }

    updatePresenceSummary(visibleTodos = []) {
        if (!this.presenceSummary || !this.presencePrimaryAction) return;
        const count = Array.isArray(visibleTodos) ? visibleTodos.length : 0;
        this.updateTodaySnapshot(visibleTodos);
        if (count === 0) {
            this.presenceSummary.textContent = 'Your agenda is currently clear. Sync your accounts or refresh to find new priorities.';
            this.presencePrimaryAction.textContent = 'Review priorities';
            return;
        }

        const top = visibleTodos[0];
        const title = this.truncate(top?.title || 'Start here', 72);
        this.presenceSummary.textContent = count === 1
            ? `One priority needs attention: ${title}`
            : `${count} priorities are open. Start with: ${title}`;
        this.presencePrimaryAction.textContent = 'Review priorities';
    }

    updateTodaySnapshot(visibleTodos = []) {
        const items = Array.isArray(visibleTodos) ? visibleTodos : [];
        const clientCount = items.filter((todo) => ['followup', 'relationship_intelligence'].includes(todo.category) || this.hasClientSignal(todo)).length;
        const deliveryCount = items.filter((todo) => ['work', 'creative', 'study'].includes(todo.category)).length;
        if (this.todayOpenCount) this.todayOpenCount.textContent = String(items.length);
        if (this.todayClientCount) this.todayClientCount.textContent = String(clientCount);
        if (this.todayDeliveryCount) this.todayDeliveryCount.textContent = String(deliveryCount);
    }

    setPresenceMode(mode = 'waiting') {
        this.presenceMode = mode;
        if (!this.presenceState) return;
        const labels = {
            idle: 'Today',
            thinking: 'Reviewing context',
            remembering: 'Updating memory',
            suggesting: 'Priorities ready',
            waiting: 'Today'
        };
        const label = labels[mode] || labels.waiting;
        this.presenceState.textContent = label;
        this.presenceState.setAttribute('data-mode', mode);
    }

    applyAmbientTone(hour = new Date().getHours()) {
        const tone = hour >= 19 || hour < 5 ? 'evening' : (hour < 11 ? 'morning' : 'day');
        document.body.setAttribute('data-ambient', tone);
    }

    setupTodayWhisperPrompt() {
        const el = this.todayWhisperPrompt;
        if (!el) return;
        const prompts = [
            'Ask about a client, project, or next step',
            'Review open client follow-ups',
            'Prepare for an upcoming meeting',
            'Find the next delivery item to close'
        ];
        const initial = Math.floor(Math.random() * prompts.length);
        el.textContent = prompts[initial];
        el.addEventListener('click', async () => {
            await this.generateSuggestions({ replace: true, silent: false });
        });
        let index = initial;
        if (this.todayWhisperTimer) clearInterval(this.todayWhisperTimer);
        this.todayWhisperTimer = window.setInterval(() => {
            index = (index + 1) % prompts.length;
            el.classList.add('is-fading');
            window.setTimeout(() => {
                el.textContent = prompts[index];
                el.classList.remove('is-fading');
            }, 170);
        }, 8500);
    }

    startChatPromptRotation() {
        if (!this.chatInput) return;
        const prompts = [
            'Ask about a client, project, meeting, or next step',
            'What client follow-up should I handle first?',
            'Prepare me for my next client meeting',
            'What work can I close in the next 30 minutes?'
        ];
        let index = Math.floor(Math.random() * prompts.length);
        this.chatInput.placeholder = prompts[index];
        if (this.chatPromptTimer) clearInterval(this.chatPromptTimer);
        this.chatPromptTimer = window.setInterval(() => {
            if (!this.chatInput || this.chatInput.value.trim()) return;
            index = (index + 1) % prompts.length;
            this.chatInput.classList.add('placeholder-fade');
            window.setTimeout(() => {
                if (!this.chatInput) return;
                this.chatInput.placeholder = prompts[index];
                this.chatInput.classList.remove('placeholder-fade');
            }, 160);
        }, 10000);
    }

    normalizeCategory(category) {
        const value = String(category || '').trim().toLowerCase();
        if (value.includes('follow')) return 'followup';
        if (value.includes('relationship')) return 'relationship_intelligence';
        if (['work', 'creative', 'personal', 'study', 'relationship_intelligence'].includes(value)) return value;
        return 'work';
    }

    prettyCategory(category) {
        if (category === 'followup') return 'Follow-up';
        if (category === 'relationship_intelligence') return 'Client relationship';
        if (['work', 'creative', 'study'].includes(category)) return 'Delivery';
        if (category === 'personal') return 'General';
        return category.charAt(0).toUpperCase() + category.slice(1);
    }

    hasClientSignal(todo = {}) {
        const haystack = [
            todo.title,
            todo.reason,
            todo.description,
            todo.trigger_summary,
            todo.intent,
            todo.primary_action?.label,
            todo.opportunity_type,
            todo.suggestion_group
        ].map((value) => String(value || '').toLowerCase()).join(' ');
        return /\b(client|customer|proposal|invoice|contract|retainer|stakeholder|meeting|follow ?up|reply|scope|brief)\b/.test(haystack);
    }

    cardToneClass(category = '') {
        if (['followup', 'relationship_intelligence'].includes(category)) return 'suggestion-client';
        if (['work', 'creative', 'study'].includes(category)) return 'suggestion-delivery';
        return 'suggestion-general';
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
            relationship_intelligence: 'group'
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
            if (textEl) textEl.textContent = "Updating work memory...";
            syncStatusEl.classList.add("syncing");
            
            clearTimeout(this._syncResetTimer);
            this._syncResetTimer = setTimeout(() => {
                if (textEl) textEl.textContent = "Work memory updated";
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

    handleAutomationResult(payload = {}) {
        if (!this.chatMessages) return;
        const name = payload.name || 'Automation';
        const content = payload.content || '';
        const uiBlocks = payload.ui_blocks || [];
        const notice = document.createElement('div');
        notice.className = 'message assistant';
        notice.innerHTML = this.renderAssistantHTML(`[Scheduled: ${name}]\n${content}`, null, null);
        if (uiBlocks.length > 0) {
            const blocksContainer = document.createElement('div');
            blocksContainer.className = 'ui-blocks-container';
            blocksContainer.innerHTML = uiBlocks.map((block) => this.renderUICard(block)).join('');
            notice.appendChild(blocksContainer);
            blocksContainer.querySelectorAll('[data-action]').forEach((btn) => {
                btn.addEventListener('click', () => {
                    try {
                        const actionData = JSON.parse(btn.dataset.actionData || '{}');
                        this.handleChatCardAction(btn.dataset.action, actionData);
                    } catch (_) {}
                });
            });
        }
        this.chatMessages.appendChild(notice);
        this.scrollChatToBottom();
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
        this.libraryList.innerHTML = '<div class="graph-placeholder">Searching work memory...</div>';

        try {
            const activeFilter = this.libraryFilters.find(f => f.classList.contains('active'))?.dataset.libFilter || 'all';
            const nodeTypes = activeFilter === 'all' ? ['episode', 'semantic', 'insight'] : [activeFilter];

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
        return;
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
