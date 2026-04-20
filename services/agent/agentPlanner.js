/**
 * Desktop Accessibility Agent Planner
 * Uses LLM to decide the next granular macOS Accessibility action from desktop observations.
 */

const axios = require('axios');
const { EventEmitter } = require('events');
const crypto = require('crypto');
const Store = require('electron-store');
const db = require('../db');
const store = new Store();
let visionUnsupported = false;

async function updateLlmMetrics(cached = false) {
  try {
    const key = 'llm_metrics';
    const cachedRow = await db.getQuery('SELECT value FROM kv_cache WHERE key = ?', [key]);
    let metrics = { total_calls: 0, cached_calls: 0, actual_calls: 0 };
    if (cachedRow && cachedRow.value) {
      metrics = JSON.parse(cachedRow.value);
    } else {
      const oldMetrics = store.get('llm_metrics');
      if (oldMetrics) metrics = oldMetrics;
    }
    metrics.total_calls += 1;
    if (cached) metrics.cached_calls += 1;
    else metrics.actual_calls += 1;
    await db.runQuery(
      'INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)',
      [key, JSON.stringify(metrics), 'metrics', new Date().toISOString()]
    );
    store.set('llm_metrics', metrics);
  } catch (e) {
    console.warn('[Planner] Failed to update metrics:', e.message);
  }
}

function getComputerUseProvider() {
  const preferred = String(process.env.COMPUTER_USE_PROVIDER || '').toLowerCase().trim();
  const preferredModel = String(process.env.COMPUTER_USE_MODEL || '').trim();
  if ((preferred === 'anthropic' || !preferred) && process.env.ANTHROPIC_API_KEY) {
    return {
      provider: 'anthropic',
      apiKey: process.env.ANTHROPIC_API_KEY,
      model: preferredModel || 'claude-3-5-sonnet-20241022'
    };
  }
  if ((preferred === 'openai' || !preferred) && process.env.OPENAI_API_KEY) {
    return {
      provider: 'openai',
      apiKey: process.env.OPENAI_API_KEY,
      model: preferredModel || 'gpt-4o'
    };
  }
  if ((preferred === 'deepseek' || !preferred) && process.env.DEEPSEEK_API_KEY) {
    return {
      provider: 'deepseek',
      apiKey: process.env.DEEPSEEK_API_KEY,
      model: preferredModel || 'deepseek-chat'
    };
  }
  if (process.env.OPENAI_API_KEY) {
    return {
      provider: 'openai',
      apiKey: process.env.OPENAI_API_KEY,
      model: preferredModel || 'gpt-4o'
    };
  }
  if (process.env.DEEPSEEK_API_KEY) {
    return {
      provider: 'deepseek',
      apiKey: process.env.DEEPSEEK_API_KEY,
      model: preferredModel || 'deepseek-chat'
    };
  }
  if (process.env.ANTHROPIC_API_KEY) {
    return {
      provider: 'anthropic',
      apiKey: process.env.ANTHROPIC_API_KEY,
      model: preferredModel || 'claude-3-5-sonnet-20241022'
    };
  }
  throw new Error('No computer-use planner API key configured');
}

function normalizeDesktopGoal(goal = '') {
  const text = String(goal || '');
  const lower = text.toLowerCase();
  const ordinalMap = { first: 1, second: 2, third: 3, fourth: 4, fifth: 5 };
  const ordinalMatch = lower.match(/\b(first|second|third|fourth|fifth|\d+(?:st|nd|rd|th)?)\b/);
  let ordinalTarget = null;
  if (ordinalMatch) {
    const raw = ordinalMatch[1];
    ordinalTarget = ordinalMap[raw] || Number(String(raw).replace(/\D/g, '')) || null;
  }
  let queryText = '';
  const searchMatch = text.match(/search(?:\s+(?:google|for))?\s+for\s+(.+?)(?:,| and | then |$)/i) || text.match(/search\s+(.+?)(?:,| and | then |$)/i);
  if (searchMatch) queryText = String(searchMatch[1] || '').trim().replace(/^["']|["']$/g, '');
  const entityTerms = Array.from(new Set(
    text
      .replace(/[^a-zA-Z0-9\s._/-]/g, ' ')
      .split(/\s+/)
      .map((item) => item.trim())
      .filter((item) => item.length >= 3)
      .filter((item) => !/^(search|google|open|click|result|draft|reply|email|meeting|calendar|then|and|the|for|with|from)$/i.test(item))
  )).slice(0, 6);

  const targetKind = /\bresult\b/.test(lower) ? 'search_result'
    : /\bemail|reply|draft\b/.test(lower) ? 'email'
    : /\bmeeting|calendar|agenda\b/.test(lower) ? 'calendar'
    : 'navigation_target';

  const surfaceGoal = queryText || /\bgoogle\b/.test(lower) ? 'web_search'
    : /\bgmail|email|reply|draft\b/.test(lower) ? 'email_flow'
    : /\bcalendar|meeting\b/.test(lower) ? 'calendar_flow'
    : 'generic_navigation';

  const completionCheck = surfaceGoal === 'web_search' && ordinalTarget
    ? `Search results are visible and the ${ordinalTarget} result has been opened.`
    : surfaceGoal === 'web_search'
      ? 'Search results are visible for the requested query.'
      : 'The requested destination or ready state is visibly reached.';

  const successPredicate = surfaceGoal === 'web_search' && ordinalTarget
    ? `Only complete after query "${queryText || ''}" is submitted, results are visible, and the ${ordinalTarget} result has opened a non-search destination page.`
    : surfaceGoal === 'web_search'
      ? `Only complete after search results for "${queryText || ''}" are visibly loaded.`
      : 'Only complete after the requested destination or ready state is visibly confirmed.';

  const stepHints = surfaceGoal === 'web_search'
    ? ['open the search surface', 'read the current page', 'enter the query', 'submit the search', ordinalTarget ? `open result ${ordinalTarget}` : 'inspect results', 'confirm destination changed']
    : surfaceGoal === 'email_flow'
      ? ['open the mailbox or thread', 'read the current email context', 'focus the compose or reply field', 'draft the response', 'confirm the draft is prepared']
      : surfaceGoal === 'calendar_flow'
        ? ['open the calendar context', 'read the event or meeting details', 'focus the relevant event or prep surface', 'prepare the meeting context', 'confirm the ready state']
        : ['open the requested surface', 'read the current UI', 'navigate toward the requested destination', 'confirm the destination or ready state'];

  return {
    raw_goal: text,
    surface_goal: surfaceGoal,
    query_text: queryText,
    ordinal_target: ordinalTarget,
    target_kind: targetKind,
    completion_check: completionCheck,
    success_predicate: successPredicate,
    step_hints: stepHints,
    entity_terms: entityTerms
  };
}

function sanitizeObservation(observation) {
  const obs = observation || {};
  const screenshotMeta = obs.screenshot ? {
    present: Boolean(obs.screenshot?.data_url),
    truncated: Boolean(obs.screenshot?.truncated),
    captured_at: obs.screenshot?.captured_at || null,
    error: obs.screenshot?.error || null
  } : { present: false };

  return {
    ...obs,
    surface_driver: obs.surface_driver || 'ax',
    browser_tree: obs.browser_tree ? {
      url: obs.browser_tree.url || '',
      title: obs.browser_tree.title || '',
      ax_nodes_count: Array.isArray(obs.browser_tree.ax_nodes) ? obs.browser_tree.ax_nodes.length : 0
    } : null,
    ax_tree: obs.ax_tree ? {
      id: obs.ax_tree.id,
      role: obs.ax_tree.role,
      title: obs.ax_tree.title,
      identifier: obs.ax_tree.identifier,
      child_count: Array.isArray(obs.ax_tree.children) ? obs.ax_tree.children.length : 0
    } : null,
    perception_mode: obs.perception_mode || 'ax_tree',
    window_id: obs.window_id || null,
    bounds: obs.bounds || null,
    focused_element_id: obs.focused_element_id || null,
    vision_used: Boolean(obs.vision_used),
    vision_mode: obs.vision_mode || 'ax_only',
    window_clip_used: Boolean(obs.window_clip_used),
    visual_change_summary: obs.visual_change_summary || '',
    target_confidence: obs.target_confidence ?? null,
    frame_summaries: Array.isArray(obs.frame_summaries) ? obs.frame_summaries.slice(0, 3) : [],
    observation_budget: obs.observation_budget || {},
    screenshot_summary: obs.screenshot_summary || null,
    screenshot: screenshotMeta,
    interactive_candidates: Array.isArray(obs.interactive_candidates)
      ? obs.interactive_candidates.slice(0, 12).map((item) => ({
          index: item.index,
          role: item.role,
          name: item.name,
          description: item.description,
          value: item.value,
          group: item.group,
          hint: item.hint,
          id: item.id || null,
          identifier: item.identifier || null,
          frame: item.frame || null
        }))
      : [],
    visible_elements: Array.isArray(obs.visible_elements)
      ? obs.visible_elements.slice(0, 25).map((item) => ({
          index: item.index,
          role: item.role,
          name: item.name,
          description: item.description,
          value: item.value,
          enabled: item.enabled,
          hint: item.hint
        }))
      : []
  };
}

async function callPlannerLLM(goal, history, observation, plannerContext = {}) {
  const providerConfig = getComputerUseProvider();

  const goalBundle = normalizeDesktopGoal(goal);
  const cleanObservation = sanitizeObservation(observation);
  const screenshotDataUrl = observation?.screenshot?.data_url || '';
  const canUseVision = !visionUnsupported && screenshotDataUrl.startsWith('data:image/');

  // Use persistent cache for planner actions
  const screenshotHash = screenshotDataUrl ? crypto.createHash('sha1').update(screenshotDataUrl).digest('hex') : null;
  const cacheKey = crypto.createHash('sha1').update(JSON.stringify({
    provider: providerConfig.provider,
    model: providerConfig.model,
    goal: goalBundle,
    history: (history || []).slice(-5), // only last 5 actions for cache key stability
    observation: { ...cleanObservation, screenshot: undefined },
    screenshotHash,
    plannerContext
  })).digest('hex');

  const CACHE_TTL = 30 * 60 * 1000; // 30 mins
  try {
    const cachedRow = await db.getQuery('SELECT value, created_at FROM kv_cache WHERE key = ? AND type = ?', [cacheKey, 'llm_planner_response']);
    if (cachedRow && (Date.now() - new Date(cachedRow.created_at).getTime()) < CACHE_TTL) {
      await updateLlmMetrics(true);
      return JSON.parse(cachedRow.value);
    }
  } catch (e) {
    console.warn('[Planner] Cache lookup failed:', e.message);
  }

  const prompt = `[System]
  Desktop Agent controlling macOS. Choose NEXT low-level action.
  Goal: "${goal}"
  GOAL BUNDLE: ${JSON.stringify(goalBundle)}
  HISTORY: ${JSON.stringify(history)}
  OBSERVATION:
  APP: ${cleanObservation.frontmost_app} | WINDOW: ${cleanObservation.window_title} | URL: ${cleanObservation.url || ''}
  FOCUSED: ${JSON.stringify(cleanObservation.focused_element)}
  VISIBLE: ${JSON.stringify(cleanObservation.visible_elements)}
  CANDIDATES: ${JSON.stringify(cleanObservation.interactive_candidates)}
  AX TREE: ${JSON.stringify(cleanObservation.ax_tree)}
  BROWSER TREE: ${JSON.stringify(cleanObservation.browser_tree)}
  PERMISSION: ${JSON.stringify(cleanObservation.permission_state)}
  SCREENSHOT: ${JSON.stringify(cleanObservation.screenshot)}
  VISION: ${JSON.stringify({
  vision_used: cleanObservation.vision_used,
  vision_mode: cleanObservation.vision_mode,
  perception_mode: cleanObservation.perception_mode,
  window_clip_used: cleanObservation.window_clip_used,
  visual_change_summary: cleanObservation.visual_change_summary,
  screenshot_summary: cleanObservation.screenshot_summary,
  target_confidence: cleanObservation.target_confidence,
  frame_summaries: cleanObservation.frame_summaries,
  observation_budget: cleanObservation.observation_budget
  })}
  PLANNER CONTEXT: ${JSON.stringify({
  goal_bundle: plannerContext.goal_bundle || goalBundle,
  remaining_gap: plannerContext.remaining_gap || '',
  last_effect_summary: plannerContext.last_effect_summary || '',
  recent_failures: plannerContext.recent_failures || [],
  current_stage: plannerContext.current_stage || '',
  observation_before: plannerContext.observation_before || null,
  observation_after: plannerContext.observation_after || null,
  observation_changed: Boolean(plannerContext.observation_changed)
  })}

  Action Options: CDP_NAVIGATE, CDP_CLICK, CDP_TYPE, CDP_KEY_PRESS, CDP_SCROLL, CDP_WAIT_FOR, ACTIVATE_APP, FOCUS_WINDOW, OPEN_URL, CLICK_AX, PRESS_AX, TYPE_TEXT, SET_VALUE, WAIT_FOR_AX, SCROLL_AX, READ_UI_STATE, DONE.
  Respond with ONLY valid JSON action object.
  Rules: Loop inspect->action until goal reached. Avoid DONE until success visibly confirmed. Change tactics if no effect. Use vision_request if AX not enough.`;

  const parsePlannerJson = (raw = '') => {
    const txt = String(raw || '');
    // Strip fences and trim
    const cleaned = txt.replace(/^```json/i, '').replace(/^```[a-zA-Z0-9]*\n?/, '').replace(/```$/g, '').trim();

    const safeParse = (s) => {
      try {
        return JSON.parse(s);
      } catch (err) {
        return null;
      }
    };

    // Try direct parse
    let parsed = safeParse(cleaned);
    if (parsed !== null) return { action: parsed, raw: cleaned };

    // Remove trailing commas and smart quotes
    const relaxed = cleaned.replace(/,\s*(?=[}\]])/g, '').replace(/[“”]/g, '"').replace(/[‘’]/g, "'").replace(/\u0000/g, '');
    parsed = safeParse(relaxed);
    if (parsed !== null) return { action: parsed, raw: relaxed };

    // Fallback: try to extract a JSON substring between first {/[ and last}/]
    const firstOpen = Math.min(...['{', '['].map((ch) => { const i = cleaned.indexOf(ch); return i === -1 ? Number.MAX_SAFE_INTEGER : i; }));
    const lastClose = Math.max(cleaned.lastIndexOf('}'), cleaned.lastIndexOf(']'));
    if (firstOpen <= lastClose) {
      const sub = cleaned.slice(firstOpen, lastClose + 1);
      parsed = safeParse(sub);
      if (parsed !== null) return { action: parsed, raw: sub };
    }

    // Give up — return raw and let caller handle errors
    console.error('Planner LLM returned unparsable JSON:', cleaned.slice(0, 400));
    return { action: null, raw: cleaned };
  };

  const makeRequest = async (withVision) => {
    if (providerConfig.provider === 'anthropic') {
      const imageSource = withVision
        ? (() => {
            const match = String(screenshotDataUrl || '').match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$/);
            if (!match) return null;
            return {
              type: 'base64',
              media_type: match[1],
              data: match[2]
            };
          })()
        : null;

      const content = [{ type: 'text', text: prompt }];
      if (imageSource) {
        content.push({
          type: 'image',
          source: imageSource
        });
      }

      const response = await axios.post('https://api.anthropic.com/v1/messages', {
        model: providerConfig.model,
        max_tokens: 450,
        temperature: 0.1,
        system: 'You are a precise macOS accessibility automation planner. Respond with JSON actions only.',
        messages: [{ role: 'user', content }]
      }, {
        headers: {
          'x-api-key': providerConfig.apiKey,
          'anthropic-version': '2023-06-01',
          'Content-Type': 'application/json'
        }
      });

      const result = Array.isArray(response.data?.content)
        ? response.data.content.filter((chunk) => chunk?.type === 'text').map((chunk) => chunk.text || '').join('\n').trim()
        : '';
      return parsePlannerJson(result);
    }

    const userContent = withVision
      ? [
          { type: 'text', text: prompt },
          { type: 'image_url', image_url: { url: screenshotDataUrl } }
        ]
      : prompt;
    const requestBody = {
      model: providerConfig.model,
      max_tokens: 450,
      temperature: 0.1,
      messages: [
        { role: 'system', content: 'You are a precise macOS accessibility automation planner. Respond with JSON actions only.' },
        { role: 'user', content: userContent }
      ],
      response_format: { type: 'json_object' }
    };
    const url = providerConfig.provider === 'openai'
      ? 'https://api.openai.com/v1/chat/completions'
      : 'https://api.deepseek.com/v1/chat/completions';
    const response = await axios.post(url, requestBody, {
      headers: {
        'Authorization': `Bearer ${providerConfig.apiKey}`,
        'Content-Type': 'application/json'
      }
    });

    const result = response.data.choices?.[0]?.message?.content?.trim() || '';
    return parsePlannerJson(result);
  };

  try {
    const res = await makeRequest(canUseVision);
    // If action is null (parse failure), try once more without vision if possible
    if ((!res || !res.action) && canUseVision) {
      visionUnsupported = true;
      const fallbackRes = await makeRequest(false);
      if (fallbackRes && fallbackRes.action) {
        await updateLlmMetrics(false);
        await db.runQuery(
          'INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)',
          [cacheKey, JSON.stringify(fallbackRes), 'llm_planner_response', new Date().toISOString()]
        );
      }
      return fallbackRes;
    }
    if (res && res.action) {
      await updateLlmMetrics(false);
      await db.runQuery(
        'INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)',
        [cacheKey, JSON.stringify(res), 'llm_planner_response', new Date().toISOString()]
      );
    }
    return res;
  } catch (e) {
    if (canUseVision) {
      visionUnsupported = true;
      return await makeRequest(false);
    }
    throw e;
  }
}

function heuristicPlan(goal, history, observation, plannerContext = {}) {
  const goalBundle = normalizeDesktopGoal(goal);
  const surfaceDriver = String(observation?.surface_driver || 'ax').toLowerCase();
  const appName = String(observation?.frontmost_app || '').toLowerCase();
  const textSample = String(observation?.text_sample || '').toLowerCase();
  const surfaceType = String(observation?.surface_type || '').toLowerCase();
  const elems = observation?.visible_elements || [];
  const candidates = observation?.interactive_candidates || [];
  const goalText = String(goal || '').toLowerCase();
  const didType = history.some((h) => ['TYPE_TEXT', 'SET_VALUE', 'SET_AX_VALUE', 'CDP_TYPE'].includes(h?.action?.kind));
  const didOpen = history.some((h) => h?.action?.kind === 'OPEN_URL');
  const didCdpNavigate = history.some((h) => h?.action?.kind === 'CDP_NAVIGATE');
  const didActivate = history.some((h) => h?.action?.kind === 'ACTIVATE_APP');
  const didRead = history.some((h) => ['READ_UI_STATE', 'CDP_GET_TREE'].includes(h?.action?.kind));
  const didWait = history.some((h) => ['WAIT_FOR_AX', 'CDP_WAIT_FOR'].includes(h?.action?.kind));
  const didClick = history.some((h) => ['CLICK_AX', 'PRESS_AX', 'CDP_CLICK'].includes(h?.action?.kind));
  const didScroll = history.some((h) => ['SCROLL_AX', 'CDP_SCROLL'].includes(h?.action?.kind));
  const didSubmit = history.some((h) => (h?.action?.kind === 'KEY_PRESS' && String(h?.action?.key || '').toLowerCase() === 'enter') || h?.action?.kind === 'CDP_KEY_PRESS');
  const draftMatch = String(goal || '').match(/draft content:\s*([\s\S]+)/i);
  const draftText = draftMatch ? draftMatch[1].trim() : '';
  const findElement = (predicate) => elems.find((item) => predicate(String(item.role || '').toLowerCase(), `${item.name || ''} ${item.description || ''} ${item.value || ''}`.toLowerCase()));
  const editable = findElement((role, hay) => /text/.test(role) || /compose|reply|message|search/.test(hay));
  const searchBox = findElement((role, hay) => /text/.test(role) && /search|query|ask/.test(hay));
  const obviousButton = findElement((role, hay) => /(button|link|tab|row)/.test(role) && /send|reply|compose|search|open|continue|next|result|article|read|show more|details/.test(hay));
  const resultCandidates = candidates.filter((item) => item.group === 'result_link');
  const topSearchField = candidates.find((item) => item.group === 'search_field') || searchBox || editable;
  const searchButton = candidates.find((item) => item.group === 'primary_button' && /search|google search|go/.test(`${item.name || ''} ${item.description || ''} ${item.value || ''}`.toLowerCase()));
  const lastFailure = Array.isArray(plannerContext.recent_failures) ? plannerContext.recent_failures[plannerContext.recent_failures.length - 1] : null;
  const noEffect = plannerContext.last_effect_summary === 'no visible change after action' || /no visible change after (click|action)/i.test(String(plannerContext.last_effect_summary || ''));
  const alternateResultCandidate = goalBundle.ordinal_target && resultCandidates.length > goalBundle.ordinal_target
    ? resultCandidates[goalBundle.ordinal_target]
    : resultCandidates[0];

  if (observation?.permission_state && observation.permission_state.trusted === false) {
    return { kind: 'DONE', message: 'Accessibility permission is required before desktop automation can continue.' };
  }

  if (goalBundle.surface_goal === 'web_search' && surfaceDriver === 'cdp' && !didCdpNavigate) {
    return {
      kind: 'CDP_NAVIGATE',
      url: 'https://www.google.com',
      reason: 'Open the managed browser on the search engine before beginning the search flow.',
      expected_outcome: 'Google is loaded in the managed Chrome tab.'
    };
  }

  if (goalBundle.surface_goal === 'web_search' && !didOpen && !didActivate && !/chrome|safari/.test(appName) && surfaceDriver !== 'cdp') {
    return { kind: 'OPEN_URL', url: 'https://www.google.com', app: 'Google Chrome', reason: 'Open the search engine before beginning the search flow.' };
  }

  if (/\bemail|gmail|follow-up|follow up|reply|draft\b/.test(goalText) && !/chrome|safari/.test(appName) && !didOpen && !didActivate) {
    return { kind: 'OPEN_URL', url: 'https://mail.google.com', app: 'Google Chrome' };
  }

  if (/\bcalendar|meeting|agenda|brief|schedule\b/.test(goalText) && !/calendar/.test(appName) && !didOpen && !didActivate) {
    return { kind: 'OPEN_URL', url: 'https://calendar.google.com', app: 'Google Chrome' };
  }

  if (/\bresearch|look up|check|compare|search\b/.test(goalText) && !/chrome|safari/.test(appName) && !didOpen && !didActivate) {
    return { kind: 'OPEN_URL', url: 'https://duckduckgo.com', app: 'Google Chrome' };
  }

  if ((didOpen || didCdpNavigate || didActivate || /browser_page|search_home|search_results|email_view|calendar_view|finder_window|editor_view/.test(surfaceType)) && !didRead) {
    if (surfaceDriver === 'cdp') {
      return { kind: 'CDP_GET_TREE', reason: 'Inspect the loaded browser state before choosing the next navigation step.' };
    }
    return { kind: 'READ_UI_STATE', reason: 'Inspect the loaded app or page before choosing the next navigation step.' };
  }

  if (plannerContext.last_effect_summary === 'no visible change after action' && /browser_page|search_home|search_results|auth_interstitial/.test(surfaceType)) {
    if (surfaceDriver === 'cdp') {
      return {
        kind: 'CDP_GET_TREE',
        reason: 'The last browser action had no visible effect; re-read the managed Chrome state before choosing the next tactic.',
        vision_request: 'window_clip'
      };
    }
    return {
      kind: 'READ_UI_STATE',
      reason: 'The last action had no visible effect; re-read with richer context before choosing the next tactic.',
      vision_request: 'window_clip'
    };
  }

  if (noEffect && lastFailure?.error === 'click_had_no_effect' && surfaceType === 'search_results' && alternateResultCandidate) {
    if (surfaceDriver === 'cdp') {
      return {
        kind: 'CDP_CLICK',
        target_id: alternateResultCandidate.id || null,
        label: alternateResultCandidate.name || alternateResultCandidate.description || alternateResultCandidate.value || 'search result',
        reason: 'The prior browser click had no effect, so try the next strongest visible result candidate.',
        expected_outcome: 'A destination page opens or the search results page changes.'
      };
    }
    return {
      kind: 'PRESS_AX',
      label: alternateResultCandidate.name || alternateResultCandidate.description || alternateResultCandidate.value || 'search result',
      role: alternateResultCandidate.role || 'link',
      target_index: alternateResultCandidate.index,
      reason: 'The prior click had no effect, so try the next strongest visible result candidate.',
      expected_outcome: 'A destination page opens or the results page changes.'
    };
  }

  if (goalBundle.surface_goal === 'web_search') {
    if (!didRead) {
      return surfaceDriver === 'cdp'
        ? { kind: 'CDP_GET_TREE', reason: 'Read the managed browser page before interacting.' }
        : { kind: 'READ_UI_STATE', reason: 'Read the search page before interacting.' };
    }
    if (topSearchField && !didType) {
      if (surfaceDriver === 'cdp') {
        return {
          kind: 'CDP_TYPE',
          target_id: topSearchField.id || null,
          label: topSearchField.name || topSearchField.description || 'search',
          text: goalBundle.query_text || 'hello',
          submit: false,
          reason: 'Enter the requested search query into the visible browser search field.',
          expected_outcome: 'The managed browser search field contains the requested query.'
        };
      }
      return {
        kind: 'SET_VALUE',
        target_id: topSearchField.id || null,
        label: topSearchField.name || topSearchField.description || 'search',
        text: goalBundle.query_text || 'hello',
        reason: 'Enter the requested search query into the visible search field.',
        expected_outcome: 'The search field contains the requested query.'
      };
    }
    if (didType && !didSubmit) {
      if (surfaceDriver === 'cdp') {
        return {
          kind: 'CDP_KEY_PRESS',
          key: 'Enter',
          reason: 'Submit the search after the query has been entered in the managed browser.',
          expected_outcome: 'The search results page loads in the current tab.'
        };
      }
      return {
        kind: 'KEY_PRESS',
        key: 'enter',
        reason: 'Submit the search after the query has been entered.',
        expected_outcome: 'The results page loads.'
      };
    }
    if (surfaceType === 'search_results' && goalBundle.ordinal_target && resultCandidates.length >= goalBundle.ordinal_target && !didClick) {
      const target = resultCandidates[goalBundle.ordinal_target - 1];
      if (surfaceDriver === 'cdp') {
        return {
          kind: 'CDP_CLICK',
          target_id: target.id || null,
          label: target.name || target.description || target.value || 'search result',
          reason: `Open the ${goalBundle.ordinal_target} result from the visible search results in managed Chrome.`,
          expected_outcome: 'The destination page opens in the current browser tab.',
          completion_signal: goalBundle.completion_check
        };
      }
      return {
        kind: 'PRESS_AX',
        target_id: target.id || null,
        label: target.name || target.description || target.value || 'search result',
        role: target.role || 'link',
        target_index: target.index,
        reason: `Open the ${goalBundle.ordinal_target} result from the visible search results.`,
        expected_outcome: 'The destination page opens.',
        completion_signal: goalBundle.completion_check
      };
    }
    if (surfaceType === 'search_results' && noEffect && resultCandidates.length) {
      if (surfaceDriver === 'cdp') {
        return {
          kind: 'CDP_SCROLL',
          direction: 'down',
          amount: 1,
          reason: 'The previous browser result interaction did not change the page, so scan for a clearer result target.'
        };
      }
      return {
        kind: 'SCROLL_AX',
        direction: 'down',
        amount: 1,
        reason: 'The previous result interaction did not change the page, so scan for a clearer result target.'
      };
    }
    if (surfaceType === 'search_results' && resultCandidates.length < (goalBundle.ordinal_target || 1) && !didScroll) {
      if (surfaceDriver === 'cdp') {
        return {
          kind: 'CDP_SCROLL',
          direction: 'down',
          amount: 1,
          reason: 'Scroll the managed browser page to find more search results before failing.'
        };
      }
      return {
        kind: 'SCROLL_AX',
        direction: 'down',
        amount: 1,
        reason: 'Scroll to find more search results before failing.'
      };
    }
  }

  if ((didOpen || didActivate) && didRead && !candidates.length && !didWait) {
    return {
      kind: 'WAIT_FOR_AX',
      label: '',
      role: '',
      timeout_ms: 1500,
      reason: 'The surface is still settling; wait briefly and read again.'
    };
  }

  if (candidates.length && !didType) {
    const firstEditable = candidates.find((item) => /text|search|textbox|input|textarea/.test(String(item.role || '').toLowerCase()) || item.group === 'search_field');
    if (firstEditable && goalBundle.query_text) {
      return surfaceDriver === 'cdp'
        ? {
            kind: 'CDP_TYPE',
            target_id: firstEditable.id || null,
            text: goalBundle.query_text,
            submit: false,
            reason: 'A writable field is visible; type first before waiting.'
          }
        : {
            kind: 'SET_VALUE',
            target_id: firstEditable.id || null,
            label: firstEditable.name || firstEditable.description || 'input',
            text: goalBundle.query_text,
            reason: 'A writable field is visible; type first before waiting.'
          };
    }
  }

  if (searchBox && /\bresearch|look up|search\b/.test(goalText) && !didType) {
    return { kind: 'TYPE_TEXT', label: searchBox.name || searchBox.description || 'search', text: goal };
  }

  if (editable && draftText && !didType) {
    return { kind: 'TYPE_TEXT', label: editable.name || editable.description || '', text: draftText };
  }

  if (obviousButton && (didType || /browser_page/.test(surfaceType)) && !didClick) {
    return {
      kind: 'PRESS_AX',
      target_id: obviousButton.id || null,
      label: obviousButton.name || obviousButton.description || 'button',
      role: obviousButton.role || 'button',
      target_index: obviousButton.index,
      reason: 'A likely interactive target is visible and should be opened before stopping.'
    };
  }

  if ((elems || []).length > 12 && !didScroll) {
    return { kind: 'SCROLL_AX', direction: 'down', amount: 1, reason: 'Scan further down the current surface for the requested content.' };
  }

  if (draftText && didType) {
    return { kind: 'DONE', message: 'Draft content has been prepared in the current desktop context.' };
  }

  if ((goalBundle.surface_goal === 'email_flow' || goalBundle.surface_goal === 'calendar_flow') && noEffect && !didScroll) {
    return {
      kind: 'READ_UI_STATE',
      reason: 'The prior action did not visibly advance the workflow; inspect the surface again before retrying.',
      vision_request: 'burst'
    };
  }

  if (/browser_page|search_results|auth_interstitial/.test(surfaceType) && !candidates.length && !didScroll) {
    if (surfaceDriver === 'cdp') {
      return {
        kind: 'CDP_GET_TREE',
        reason: 'The managed browser surface exposes too little structure; request browser visual fallback on the next read.',
        vision_request: 'browser_vlm'
      };
    }
    return {
      kind: 'READ_UI_STATE',
      reason: 'The browser surface exposes too little AX structure; request browser visual fallback on the next read.',
      vision_request: 'browser_vlm'
    };
  }

  if ((didClick || didScroll) && !didWait) {
    if (surfaceDriver === 'cdp') {
      return {
        kind: 'CDP_WAIT_FOR',
        text: goalBundle.query_text || '',
        timeout_ms: 2000,
        reason: 'Allow the managed browser UI to settle after interaction.'
      };
    }
    return { kind: 'WAIT_FOR_AX', label: '', role: '', timeout_ms: 2000, reason: 'Allow the UI to settle after interaction.' };
  }

  if (textSample && /\bmeeting|reply|draft|sent|message|calendar|waitlist|weave\b/.test(textSample) && (didClick || didType || didScroll)) {
    return surfaceDriver === 'cdp'
      ? { kind: 'CDP_GET_TREE', reason: 'Refresh browser observation to confirm whether the intended target has been reached.' }
      : { kind: 'READ_UI_STATE', reason: 'Refresh observation to confirm whether the intended target has been reached.' };
  }

  return null;
}

async function planNextActionWithLLM(goal, history, observation, plannerContext = {}) {
  try {
    const { action } = await callPlannerLLM(goal, history, observation, plannerContext);
    return action;
  } catch (e) {
    console.error('LLM Planning failed:', e.response?.data || e.message);
    throw e;
  }
}

/**
 * planNextAction: attempts a cheap heuristic plan first. If none found, calls LLM and returns either
 * - a plain action object (if heuristic matched)
 * - an EventEmitter that will emit 'chunk' events with partial text and a final 'done' with the JSON action
 */
function planNextAction(goal, history, observation, plannerContext = {}) {
  const easy = heuristicPlan(goal, history, observation, plannerContext);
  if (easy) return easy;

  // No heuristic match: call LLM (single call), but return an emitter to stream chunks to UI.
  const emitter = new EventEmitter();

  (async () => {
    try {
      const { raw } = await callPlannerLLM(goal, history, observation, plannerContext);
      const text = String(raw || '');
      const preview = text.length > 180 ? `${text.slice(0, 180)}…` : text;
      emitter.emit('chunk', preview);

      const clean = text.replace(/^```json/i, '').replace(/```$/g, '').trim();
      emitter.emit('done', clean);
      } catch (e) {
        // If the provider supports streaming and env var enables it, attempt a streaming connection
        if (process.env.DEEPSEEK_STREAM === 'true') {
          try {
            const streamEmitter = new EventEmitter();
            (async () => {
              const providerConfig = getComputerUseProvider();
              const streamUrl = providerConfig.provider === 'openai'
                ? 'https://api.openai.com/v1/chat/completions'
                : 'https://api.deepseek.com/v1/chat/completions';
              const req = await axios({
                method: 'post',
                url: streamUrl,
                headers: { 'Authorization': `Bearer ${providerConfig.apiKey}`, 'Content-Type': 'application/json' },
                data: {
                  model: providerConfig.model,
                  messages: [ { role: 'system', content: 'You are a precise macOS accessibility automation planner. Respond with JSON actions only.' }, { role: 'user', content: `Goal: ${goal}\nObservation: ${JSON.stringify(sanitizeObservation(observation)||{})}` } ],
                  temperature: 0.08,
                  stream: true
                },
                responseType: 'stream',
                timeout: 120000
              });

              const stream = req.data;
              stream.on('data', (chunk) => {
                const txt = chunk.toString('utf8');
                // Emit raw chunks — renderer will assemble
                streamEmitter.emit('chunk', txt);
              });
              stream.on('end', () => streamEmitter.emit('done', ''));
              stream.on('error', (err) => streamEmitter.emit('error', err));
            })();
            return streamEmitter;
          } catch (sErr) {
            // Fall back to previous (non-streaming) emitter failure path
            emitter.emit('error', sErr);
          }
        } else {
          emitter.emit('error', e);
        }
      }
    })();

    return emitter;
}

module.exports = { planNextActionWithLLM, planNextAction, normalizeDesktopGoal };
