// Agent state
let agentState = {
  isRunning: false,
  goal: '',
  step: 0,
  maxSteps: 30,
  apiKey: '',
  model: 'deepseek-chat',
  history: [],
  tabId: null,
  tabGroupId: null,
  retryCount: 0,
  maxRetries: 3,
  lastActionHash: '',
  sameActionCount: 0,
  failedSelectors: [],
  visitedUrls: [],
  stuckCount: 0
};

// System prompt for the agent
const SYSTEM_PROMPT = `You are an autonomous web browsing agent. Your job is to help users accomplish tasks on the web by analyzing page content and deciding what actions to take.

You will receive:
1. The user's goal
2. The current page URL and title
3. A simplified representation of interactive elements on the page
4. History of previous actions
5. Failed selectors (DO NOT use these again)
6. Loop warnings if you're repeating actions

You must respond with a JSON object containing:
{
  "thinking": "Your reasoning about what to do next (1-2 sentences)",
  "action": {
    "type": "click" | "type" | "scroll" | "navigate" | "complete" | "wait" | "pressEnter",
    "selector": "CSS selector for the element (for click/type)",
    "text": "Text to type (for type action)",
    "url": "URL to navigate to (for navigate action)",
    "direction": "up" | "down" (for scroll action),
    "amount": 300 (pixels for scroll),
    "summary": "Summary of what was accomplished (for complete action)"
  }
}

CRITICAL ANTI-LOOP RULES:
- NEVER repeat the exact same action twice in a row
- If a selector failed before (listed in "Failed Selectors"), DO NOT use it again - find an alternative
- If you've tried clicking something 2+ times and it's not working, try a completely different approach
- Look at ALL available elements on the page - there may be alternative buttons, links, or methods
- If stuck on a page, consider: scrolling to find more elements, using keyboard shortcuts, or navigating elsewhere

IMPORTANT RULES:
- Analyze the page THOROUGHLY - read all element descriptions before choosing
- Use specific, unique selectors when possible
- ONLY use "complete" when the goal is 100% FULLY accomplished and verified
- DO NOT use "complete" prematurely - keep working until done
- If search results appeared, you may need to click on a result
- After typing, use "pressEnter" to submit or click a search/submit button
- If a button doesn't work, look for alternative ways: keyboard, different button, different approach
- When you see many similar elements, pick the most relevant one based on text content
- Only return valid JSON, nothing else`;

// Send message to popup
function sendToPopup(message) {
  chrome.runtime.sendMessage(message).catch(() => {
    // Popup might be closed, ignore error
  });
}

// Check if URL is accessible (not a restricted chrome:// or extension page)
function isAccessibleUrl(url) {
  if (!url) return false;
  const restrictedPrefixes = [
    'chrome://',
    'chrome-extension://',
    'edge://',
    'about:',
    'chrome-search://',
    'devtools://',
    'view-source:',
    'file://'
  ];
  return !restrictedPrefixes.some(prefix => url.toLowerCase().startsWith(prefix));
}

// Create a new tab group for the agent
async function createAgentTabGroup(initialUrl = 'https://www.google.com') {
  // Create a new tab
  const tab = await chrome.tabs.create({ 
    url: initialUrl,
    active: true 
  });
  
  // Wait for tab to start loading
  await new Promise(resolve => setTimeout(resolve, 500));
  
  // Create a tab group
  const groupId = await chrome.tabs.group({ tabIds: tab.id });
  
  // Update the tab group appearance
  await chrome.tabGroups.update(groupId, {
    title: 'Weave',
    color: 'green',
    collapsed: false
  });
  
  return { tabId: tab.id, groupId };
}

// Inject content script into tab with retries
async function injectContentScript(tabId, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      // Check if content script is already loaded
      const response = await chrome.tabs.sendMessage(tabId, { type: 'PING' }).catch(() => null);
      if (response?.pong) return true;
    } catch (e) {
      // Script not loaded, continue to inject
    }
    
    try {
      await chrome.scripting.executeScript({
        target: { tabId },
        files: ['content.js']
      });
      await chrome.scripting.insertCSS({
        target: { tabId },
        files: ['content.css']
      });
      // Wait for script to initialize
      await new Promise(resolve => setTimeout(resolve, 800));
      return true;
    } catch (error) {
      console.error(`Injection attempt ${attempt + 1} failed:`, error);
      if (attempt < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }
  return false;
}

// Get page state from content script with retries
async function getPageState(tabId) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      // First ensure content script is injected
      const injected = await injectContentScript(tabId);
      if (!injected) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }
      
      const response = await chrome.tabs.sendMessage(tabId, { type: 'GET_PAGE_STATE' });
      if (response && response.elements) {
        return response;
      }
    } catch (error) {
      console.error(`Get page state attempt ${attempt + 1} failed:`, error);
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  return null;
}

// Execute action in tab with retries
async function executeAction(tabId, action) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const result = await chrome.tabs.sendMessage(tabId, { type: 'EXECUTE_ACTION', action });
      return result;
    } catch (error) {
      console.error(`Execute action attempt ${attempt + 1} failed:`, error);
      // Try re-injecting content script
      await injectContentScript(tabId);
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  return { error: 'Failed to execute action after retries' };
}

// Call DeepSeek API with retries
async function callLLM(messages) {
  let lastError;
  
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch('https://api.deepseek.com/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${agentState.apiKey}`
        },
        body: JSON.stringify({
          model: agentState.model,
          messages,
          temperature: 0.7,
          max_tokens: 2000
        })
      });

      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new Error(error.error?.message || `API request failed with status ${response.status}`);
      }

      const data = await response.json();
      if (data.choices && data.choices[0] && data.choices[0].message) {
        return data.choices[0].message.content;
      }
      throw new Error('Invalid API response format');
    } catch (error) {
      lastError = error;
      console.error(`LLM call attempt ${attempt + 1} failed:`, error);
      if (attempt < 2) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  }
  
  throw lastError;
}

// Parse LLM response
function parseLLMResponse(response) {
  try {
    // Try to extract JSON from the response
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      if (parsed.action && parsed.action.type) {
        return parsed;
      }
    }
    throw new Error('No valid action found in response');
  } catch (error) {
    throw new Error(`Failed to parse LLM response: ${error.message}`);
  }
}

// Wait for tab to finish loading
async function waitForTabLoad(tabId, maxWait = 15000) {
  const startTime = Date.now();
  
  while (Date.now() - startTime < maxWait) {
    try {
      const tab = await chrome.tabs.get(tabId);
      if (tab.status === 'complete') {
        // Additional wait for dynamic content
        await new Promise(resolve => setTimeout(resolve, 800));
        return true;
      }
    } catch (error) {
      return false;
    }
    await new Promise(resolve => setTimeout(resolve, 300));
  }
  
  // Even if timeout, return true to continue
  return true;
}

// Check if tab still exists
async function isTabValid(tabId) {
  try {
    await chrome.tabs.get(tabId);
    return true;
  } catch {
    return false;
  }
}

// Create a hash for an action to detect loops
function getActionHash(action) {
  return JSON.stringify({
    type: action.type,
    selector: action.selector,
    text: action.text,
    url: action.url,
    direction: action.direction
  });
}

// Check if we're stuck in a loop
function detectLoop(action) {
  const currentHash = getActionHash(action);
  
  if (currentHash === agentState.lastActionHash) {
    agentState.sameActionCount++;
  } else {
    agentState.sameActionCount = 1;
    agentState.lastActionHash = currentHash;
  }
  
  // If same action repeated 3+ times, we're in a loop
  return agentState.sameActionCount >= 3;
}

// Track failed selectors
function markSelectorFailed(selector) {
  if (selector && !agentState.failedSelectors.includes(selector)) {
    agentState.failedSelectors.push(selector);
    // Keep only last 20 failed selectors
    if (agentState.failedSelectors.length > 20) {
      agentState.failedSelectors.shift();
    }
  }
}

// Check if page has changed meaningfully
function trackPageVisit(url) {
  const baseUrl = url.split('?')[0].split('#')[0];
  const visitCount = agentState.visitedUrls.filter(u => u === baseUrl).length;
  agentState.visitedUrls.push(baseUrl);
  
  // Keep only last 30 URLs
  if (agentState.visitedUrls.length > 30) {
    agentState.visitedUrls.shift();
  }
  
  return visitCount; // Returns how many times we've visited this URL
}

// Main agent loop
async function runAgent() {
  sendToPopup({ type: 'LOG', content: 'Creating agent workspace...', logType: 'think' });
  
  // Create a new tab group for the agent
  try {
    const { tabId, groupId } = await createAgentTabGroup('https://www.google.com');
    agentState.tabId = tabId;
    agentState.tabGroupId = groupId;
    sendToPopup({ type: 'LOG', content: 'Agent workspace created - check the "Weave" tab group', logType: 'navigate' });
  } catch (error) {
    sendToPopup({ type: 'ERROR', error: `Failed to create agent workspace: ${error.message}` });
    agentState.isRunning = false;
    return;
  }

  // Wait for the initial page to load
  await waitForTabLoad(agentState.tabId);
  
  // Main agent loop - continues until explicitly completed or max steps reached
  while (agentState.isRunning && agentState.step < agentState.maxSteps) {
    agentState.step++;
    sendToPopup({ type: 'PROGRESS', step: agentState.step, maxSteps: agentState.maxSteps });

    try {
      // Verify tab still exists
      if (!await isTabValid(agentState.tabId)) {
        sendToPopup({ type: 'ERROR', error: 'Agent tab was closed' });
        agentState.isRunning = false;
        return;
      }

      // Get the agent's tab info
      const tab = await chrome.tabs.get(agentState.tabId);

      // Check if the current URL is accessible
      if (!isAccessibleUrl(tab.url)) {
        sendToPopup({ type: 'LOG', content: 'Navigating to accessible page...', logType: 'navigate' });
        await chrome.tabs.update(agentState.tabId, { url: 'https://www.google.com' });
        await waitForTabLoad(agentState.tabId);
        agentState.retryCount = 0;
        continue;
      }

      // Get current page state
      sendToPopup({ type: 'LOG', content: 'Analyzing page...', logType: 'think' });
      
      const pageState = await getPageState(agentState.tabId);

      if (!pageState || !pageState.elements) {
        agentState.retryCount++;
        if (agentState.retryCount >= agentState.maxRetries) {
          sendToPopup({ type: 'LOG', content: 'Page unreadable, navigating to Google...', logType: 'error' });
          await chrome.tabs.update(agentState.tabId, { url: 'https://www.google.com' });
          await waitForTabLoad(agentState.tabId);
          agentState.retryCount = 0;
        } else {
          sendToPopup({ type: 'LOG', content: `Could not read page, retrying (${agentState.retryCount}/${agentState.maxRetries})...`, logType: 'error' });
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
        continue;
      }
      
      agentState.retryCount = 0;

      // Track page visit
      const visitCount = trackPageVisit(tab.url);
      
      // Build loop warning if needed
      let loopWarning = '';
      if (agentState.sameActionCount >= 2) {
        loopWarning = `\n\nWARNING: You are repeating the same action (${agentState.sameActionCount} times). TRY SOMETHING DIFFERENT. Look for alternative elements, scroll to find more options, or navigate elsewhere.`;
      }
      if (visitCount >= 3) {
        loopWarning += `\n\nWARNING: You have visited this page ${visitCount + 1} times. You may be going in circles. Try a completely different approach.`;
      }
      
      // Build failed selectors warning
      const failedSelectorsWarning = agentState.failedSelectors.length > 0 
        ? `\n\nFailed Selectors (DO NOT USE THESE - they don't work):\n${agentState.failedSelectors.slice(-10).join('\n')}`
        : '';

      // Build the prompt
      const userMessage = `
Goal: ${agentState.goal}

Current Page:
- URL: ${tab.url}
- Title: ${tab.title}

Interactive Elements (${pageState.elements.length} found):
${pageState.elements.slice(0, 60).map((el, i) => 
  `[${i}] ${el.tag}${el.id ? '#' + el.id : ''}${el.classes ? '.' + el.classes.split(' ')[0] : ''} | ${el.type || el.tag} | "${(el.text || el.placeholder || el.value || '').substring(0, 50)}" | selector: ${el.selector}`
).join('\n')}

Previous Actions (last 10):
${agentState.history.slice(-10).map(h => `- ${h.action}: ${h.detail} ${h.success === false ? '(FAILED - try different approach)' : '(ok)'}`).join('\n') || 'None yet'}
${failedSelectorsWarning}${loopWarning}

Step ${agentState.step} of ${agentState.maxSteps}. Analyze ALL elements carefully and choose the best action. If previous attempts failed, try a DIFFERENT approach. Only use "complete" when goal is 100% done. Respond with JSON only.`;

      // Call LLM
      sendToPopup({ type: 'LOG', content: 'Thinking...', logType: 'think' });
      
      const messages = [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: userMessage }
      ];

      let parsed;
      try {
        const llmResponse = await callLLM(messages);
        parsed = parseLLMResponse(llmResponse);
      } catch (error) {
        sendToPopup({ type: 'LOG', content: `AI Error: ${error.message}`, logType: 'error' });
        
        // Check if it's an auth error
        if (error.message.includes('401') || error.message.includes('403') || error.message.includes('Invalid')) {
          sendToPopup({ type: 'ERROR', error: 'API authentication failed. Please check your DeepSeek API key.' });
          agentState.isRunning = false;
          return;
        }
        
        // Continue to next iteration for other errors
        await new Promise(resolve => setTimeout(resolve, 2000));
        continue;
      }

      // Log thinking
      if (parsed.thinking) {
        sendToPopup({ type: 'LOG', content: parsed.thinking, logType: 'think' });
      }

      const action = parsed.action;
      if (!action || !action.type) {
        sendToPopup({ type: 'LOG', content: 'No valid action received, retrying...', logType: 'error' });
        continue;
      }

      // Check for loop before executing
      if (detectLoop(action)) {
        sendToPopup({ type: 'LOG', content: 'Loop detected - forcing alternative action...', logType: 'error' });
        agentState.stuckCount++;
        
        // If truly stuck, try recovery strategies
        if (agentState.stuckCount >= 2) {
          sendToPopup({ type: 'LOG', content: 'Stuck in loop, trying to recover...', logType: 'navigate' });
          
          // Try scrolling first
          if (agentState.stuckCount === 2) {
            await executeAction(agentState.tabId, { type: 'scroll', direction: 'down', amount: 500 });
            agentState.history.push({ action: 'scroll', detail: 'recovery scroll down', success: true });
          } else if (agentState.stuckCount === 3) {
            await executeAction(agentState.tabId, { type: 'scroll', direction: 'up', amount: 500 });
            agentState.history.push({ action: 'scroll', detail: 'recovery scroll up', success: true });
          } else {
            // Go back to Google
            await chrome.tabs.update(agentState.tabId, { url: 'https://www.google.com' });
            await waitForTabLoad(agentState.tabId);
            agentState.history.push({ action: 'navigate', detail: 'recovery: back to Google', success: true });
            agentState.stuckCount = 0;
            agentState.failedSelectors = [];
          }
          agentState.sameActionCount = 0;
          agentState.lastActionHash = '';
          await new Promise(resolve => setTimeout(resolve, 1000));
          continue;
        }
      } else {
        agentState.stuckCount = 0;
      }
      
      // Execute action based on type
      let actionSuccess = true;
      
      switch (action.type) {
        case 'click':
          sendToPopup({ 
            type: 'LOG', 
            content: `Clicking: ${action.selector}`, 
            logType: 'click' 
          });
          const clickResult = await executeAction(agentState.tabId, action);
          if (clickResult?.error) {
            sendToPopup({ type: 'LOG', content: `Click failed: ${clickResult.error}`, logType: 'error' });
            actionSuccess = false;
            markSelectorFailed(action.selector);
          }
          agentState.history.push({ action: 'click', detail: action.selector, success: actionSuccess });
          await waitForTabLoad(agentState.tabId, 5000);
          await new Promise(resolve => setTimeout(resolve, 1500));
          break;

        case 'type':
          sendToPopup({ 
            type: 'LOG', 
            content: `Typing "${action.text}" into ${action.selector}`, 
            logType: 'type' 
          });
          const typeResult = await executeAction(agentState.tabId, action);
          if (typeResult?.error) {
            sendToPopup({ type: 'LOG', content: `Type failed: ${typeResult.error}`, logType: 'error' });
            actionSuccess = false;
            markSelectorFailed(action.selector);
          }
          agentState.history.push({ action: 'type', detail: `"${action.text}" in ${action.selector}`, success: actionSuccess });
          await new Promise(resolve => setTimeout(resolve, 800));
          break;

        case 'scroll':
          const scrollAmount = action.amount || 400;
          const scrollDir = action.direction || 'down';
          sendToPopup({ 
            type: 'LOG', 
            content: `Scrolling ${scrollDir} ${scrollAmount}px`, 
            logType: 'scroll' 
          });
          await executeAction(agentState.tabId, { ...action, amount: scrollAmount, direction: scrollDir });
          agentState.history.push({ action: 'scroll', detail: `${scrollDir} ${scrollAmount}px`, success: true });
          await new Promise(resolve => setTimeout(resolve, 800));
          break;

        case 'navigate':
          // Validate and fix URL
          let targetUrl = action.url;
          if (!targetUrl.startsWith('http://') && !targetUrl.startsWith('https://')) {
            targetUrl = 'https://' + targetUrl;
          }
          
          if (!isAccessibleUrl(targetUrl)) {
            sendToPopup({ type: 'LOG', content: `Cannot navigate to restricted URL: ${targetUrl}`, logType: 'error' });
            agentState.history.push({ action: 'navigate', detail: targetUrl, success: false });
            continue;
          }
          
          sendToPopup({ 
            type: 'LOG', 
            content: `Navigating to ${targetUrl}`, 
            logType: 'navigate' 
          });
          await chrome.tabs.update(agentState.tabId, { url: targetUrl });
          agentState.history.push({ action: 'navigate', detail: targetUrl, success: true });
          await waitForTabLoad(agentState.tabId);
          await new Promise(resolve => setTimeout(resolve, 1500));
          break;

        case 'wait':
          const waitTime = action.duration || 2000;
          sendToPopup({ type: 'LOG', content: `Waiting ${waitTime}ms for page...`, logType: 'think' });
          await new Promise(resolve => setTimeout(resolve, waitTime));
          agentState.history.push({ action: 'wait', detail: `${waitTime}ms`, success: true });
          break;

        case 'pressEnter':
          sendToPopup({ 
            type: 'LOG', 
            content: 'Pressing Enter key', 
            logType: 'type' 
          });
          const enterResult = await executeAction(agentState.tabId, { type: 'pressEnter' });
          if (enterResult?.error) {
            sendToPopup({ type: 'LOG', content: `Press Enter failed: ${enterResult.error}`, logType: 'error' });
            actionSuccess = false;
          }
          agentState.history.push({ action: 'pressEnter', detail: 'submitted form', success: actionSuccess });
          await waitForTabLoad(agentState.tabId, 5000);
          await new Promise(resolve => setTimeout(resolve, 1500));
          break;
        
        case 'complete':
          // Task completed successfully
          sendToPopup({ type: 'COMPLETE', summary: action.summary || 'Task completed.' });
          agentState.isRunning = false;
          
  // Update tab group to indicate completion - keep as Weave
  if (agentState.tabGroupId) {
  try {
await chrome.tabGroups.update(agentState.tabGroupId, {
  title: 'Weave',
  color: 'blue'
  });
            } catch (e) {
              // Group might not exist anymore
            }
          }
          return;

        default:
          sendToPopup({ type: 'LOG', content: `Unknown action type: ${action.type}`, logType: 'error' });
          agentState.history.push({ action: action.type, detail: 'unknown', success: false });
      }

    } catch (error) {
      console.error('Agent loop error:', error);
      sendToPopup({ type: 'LOG', content: `Error: ${error.message}`, logType: 'error' });
      
      // Don't stop on errors, try to recover
      agentState.retryCount++;
      if (agentState.retryCount >= 5) {
        sendToPopup({ type: 'ERROR', error: `Too many errors: ${error.message}` });
        agentState.isRunning = false;
        return;
      }
      
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  // Reached max steps without completing
  if (agentState.step >= agentState.maxSteps) {
    sendToPopup({ 
      type: 'COMPLETE', 
      summary: `Reached maximum steps (${agentState.maxSteps}). Progress: ${agentState.history.slice(-3).map(h => h.action + ': ' + h.detail).join(', ')}` 
    });
    
  // Update tab group - keep as Weave
  if (agentState.tabGroupId) {
  try {
await chrome.tabGroups.update(agentState.tabGroupId, {
  title: 'Weave',
  color: 'yellow'
  });
      } catch (e) {
        // Group might not exist anymore
      }
    }
  }
  
  agentState.isRunning = false;
}

// Cleanup when agent stops
async function cleanupAgent() {
  agentState.isRunning = false;
  
  // Update the tab group - keep as Weave
  if (agentState.tabGroupId) {
  try {
await chrome.tabGroups.update(agentState.tabGroupId, {
  title: 'Weave',
  color: 'grey'
  });
    } catch (e) {
      // Group might not exist anymore
    }
  }
}

// Message handler
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  switch (message.type) {
    case 'START_AGENT':
      if (agentState.isRunning) {
        sendResponse({ success: false, error: 'Agent is already running' });
        return true;
      }
      
      agentState = {
        isRunning: true,
        goal: message.goal,
        step: 0,
        maxSteps: message.maxSteps || 30,
        apiKey: message.apiKey,
        model: message.model || 'deepseek-chat',
        history: [],
        tabId: null,
        tabGroupId: null,
        retryCount: 0,
        maxRetries: 3,
        lastActionHash: '',
        sameActionCount: 0,
        failedSelectors: [],
        visitedUrls: [],
        stuckCount: 0
      };
      
      // Start the agent asynchronously
      runAgent().catch(error => {
        console.error('Agent crashed:', error);
        sendToPopup({ type: 'ERROR', error: `Agent crashed: ${error.message}` });
        agentState.isRunning = false;
      });
      
      sendResponse({ success: true });
      break;

    case 'STOP_AGENT':
      cleanupAgent();
      sendToPopup({ type: 'STOPPED' });
      sendResponse({ success: true });
      break;

    case 'GET_STATUS':
      sendResponse({
        isRunning: agentState.isRunning,
        goal: agentState.goal,
        step: agentState.step,
        maxSteps: agentState.maxSteps
      });
      break;

    default:
      sendResponse({ error: 'Unknown message type' });
  }
  return true;
});

// Handle tab close - stop agent if its tab is closed
chrome.tabs.onRemoved.addListener((tabId) => {
  if (tabId === agentState.tabId && agentState.isRunning) {
    cleanupAgent();
    sendToPopup({ type: 'STOPPED', reason: 'Agent tab was closed' });
  }
});
