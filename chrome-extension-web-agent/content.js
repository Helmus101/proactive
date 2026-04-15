// Content script - runs on web pages to interact with them

// Highlight element being interacted with
let highlightElement = null;

function createHighlight() {
  if (highlightElement) return highlightElement;
  
  highlightElement = document.createElement('div');
  highlightElement.className = 'web-agent-highlight';
  document.body.appendChild(highlightElement);
  return highlightElement;
}

function showHighlight(element) {
  if (!element) return;
  
  const highlight = createHighlight();
  const rect = element.getBoundingClientRect();
  
  highlight.style.cssText = `
    position: fixed;
    top: ${rect.top - 4}px;
    left: ${rect.left - 4}px;
    width: ${rect.width + 8}px;
    height: ${rect.height + 8}px;
    pointer-events: none;
    z-index: 999999;
    border: 2px solid #22c55e;
    border-radius: 4px;
    background: rgba(34, 197, 94, 0.1);
    transition: all 0.2s ease;
  `;
  
  highlight.classList.add('visible');
  
  setTimeout(() => {
    highlight.classList.remove('visible');
    highlight.style.opacity = '0';
  }, 1500);
}

function hideHighlight() {
  if (highlightElement) {
    highlightElement.classList.remove('visible');
    highlightElement.style.opacity = '0';
  }
}

// Generate a unique selector for an element
function generateSelector(element) {
  if (element.id) {
    return `#${CSS.escape(element.id)}`;
  }
  
  if (element.name) {
    const byName = document.querySelectorAll(`[name="${CSS.escape(element.name)}"]`);
    if (byName.length === 1) {
      return `[name="${element.name}"]`;
    }
  }
  
  // Try using specific attributes
  const specificAttrs = ['data-testid', 'data-test', 'aria-label', 'placeholder', 'title'];
  for (const attr of specificAttrs) {
    const value = element.getAttribute(attr);
    if (value) {
      const selector = `[${attr}="${CSS.escape(value)}"]`;
      const matches = document.querySelectorAll(selector);
      if (matches.length === 1) {
        return selector;
      }
    }
  }
  
  // Build path-based selector
  const path = [];
  let current = element;
  
  while (current && current !== document.body && path.length < 5) {
    let selector = current.tagName.toLowerCase();
    
    if (current.className && typeof current.className === 'string') {
      const classes = current.className.trim().split(/\s+/).filter(c => 
        c && !c.includes(':') && !c.startsWith('js-') && c.length < 30
      ).slice(0, 2);
      if (classes.length) {
        selector += '.' + classes.map(c => CSS.escape(c)).join('.');
      }
    }
    
    // Add nth-child if needed
    const parent = current.parentElement;
    if (parent) {
      const siblings = Array.from(parent.children).filter(c => 
        c.tagName === current.tagName
      );
      if (siblings.length > 1) {
        const index = siblings.indexOf(current) + 1;
        selector += `:nth-child(${index})`;
      }
    }
    
    path.unshift(selector);
    current = current.parentElement;
  }
  
  return path.join(' > ');
}

// Check if element is visible
function isVisible(element) {
  if (!element) return false;
  
  const rect = element.getBoundingClientRect();
  const style = window.getComputedStyle(element);
  
  return (
    rect.width > 0 &&
    rect.height > 0 &&
    style.visibility !== 'hidden' &&
    style.display !== 'none' &&
    style.opacity !== '0' &&
    rect.top < window.innerHeight &&
    rect.bottom > 0 &&
    rect.left < window.innerWidth &&
    rect.right > 0
  );
}

// Get interactive elements on the page
function getInteractiveElements() {
  const selectors = [
    'a[href]',
    'button',
    'input',
    'textarea',
    'select',
    '[role="button"]',
    '[role="link"]',
    '[role="textbox"]',
    '[role="searchbox"]',
    '[onclick]',
    '[tabindex]:not([tabindex="-1"])',
    'summary',
    '[contenteditable="true"]'
  ];
  
  const elements = [];
  const seen = new Set();
  
  for (const selector of selectors) {
    const matches = document.querySelectorAll(selector);
    
    for (const el of matches) {
      if (seen.has(el) || !isVisible(el)) continue;
      seen.add(el);
      
      const rect = el.getBoundingClientRect();
      const text = (el.textContent || el.value || el.placeholder || el.getAttribute('aria-label') || '')
        .trim()
        .replace(/\s+/g, ' ')
        .substring(0, 100);
      
      elements.push({
        tag: el.tagName.toLowerCase(),
        id: el.id || null,
        classes: el.className && typeof el.className === 'string' ? el.className.trim() : null,
        type: el.type || el.getAttribute('role') || null,
        text: text,
        href: el.href || null,
        name: el.name || null,
        placeholder: el.placeholder || null,
        selector: generateSelector(el),
        rect: {
          top: Math.round(rect.top),
          left: Math.round(rect.left),
          width: Math.round(rect.width),
          height: Math.round(rect.height)
        }
      });
    }
  }
  
  // Sort by position (top to bottom, left to right)
  elements.sort((a, b) => {
    const yDiff = a.rect.top - b.rect.top;
    if (Math.abs(yDiff) > 20) return yDiff;
    return a.rect.left - b.rect.left;
  });
  
  return elements;
}

// Get page text content (simplified)
function getPageText() {
  const walker = document.createTreeWalker(
    document.body,
    NodeFilter.SHOW_TEXT,
    {
      acceptNode: (node) => {
        const parent = node.parentElement;
        if (!parent) return NodeFilter.FILTER_REJECT;
        
        const tag = parent.tagName.toLowerCase();
        if (['script', 'style', 'noscript', 'svg'].includes(tag)) {
          return NodeFilter.FILTER_REJECT;
        }
        
        if (!isVisible(parent)) return NodeFilter.FILTER_REJECT;
        
        const text = node.textContent.trim();
        if (!text) return NodeFilter.FILTER_REJECT;
        
        return NodeFilter.FILTER_ACCEPT;
      }
    }
  );
  
  const texts = [];
  let node;
  while ((node = walker.nextNode()) && texts.length < 200) {
    texts.push(node.textContent.trim());
  }
  
  return texts.join(' ').replace(/\s+/g, ' ').substring(0, 5000);
}

// Execute click action
async function executeClick(selector) {
  const element = document.querySelector(selector);
  if (!element) {
    throw new Error(`Element not found: ${selector}`);
  }
  
  showHighlight(element);
  
  // Scroll into view if needed
  element.scrollIntoView({ behavior: 'smooth', block: 'center' });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // Focus and click
  element.focus();
  element.click();
  
  return { success: true };
}

// Execute type action
async function executeType(selector, text) {
  const element = document.querySelector(selector);
  if (!element) {
    throw new Error(`Element not found: ${selector}`);
  }
  
  showHighlight(element);
  
  // Scroll into view
  element.scrollIntoView({ behavior: 'smooth', block: 'center' });
  await new Promise(resolve => setTimeout(resolve, 300));
  
  // Focus
  element.focus();
  
  // Clear existing content
  if (element.value !== undefined) {
    element.value = '';
  } else if (element.textContent !== undefined) {
    element.textContent = '';
  }
  
  // Type text character by character (simulates real typing)
  for (const char of text) {
    // Dispatch keydown
    element.dispatchEvent(new KeyboardEvent('keydown', {
      key: char,
      code: `Key${char.toUpperCase()}`,
      bubbles: true
    }));
    
    // Set value/text
    if (element.value !== undefined) {
      element.value += char;
    } else if (element.textContent !== undefined) {
      element.textContent += char;
    }
    
    // Dispatch input event
    element.dispatchEvent(new Event('input', { bubbles: true }));
    
    // Dispatch keyup
    element.dispatchEvent(new KeyboardEvent('keyup', {
      key: char,
      code: `Key${char.toUpperCase()}`,
      bubbles: true
    }));
    
    await new Promise(resolve => setTimeout(resolve, 30));
  }
  
  // Trigger change event
  element.dispatchEvent(new Event('change', { bubbles: true }));
  
  // If it's a search input, might need to submit
  if (element.form && (element.type === 'search' || element.name === 'q' || element.name === 'query')) {
    // Press Enter
    element.dispatchEvent(new KeyboardEvent('keydown', {
      key: 'Enter',
      code: 'Enter',
      keyCode: 13,
      bubbles: true
    }));
  }
  
  return { success: true };
}

// Execute scroll action
function executeScroll(direction, amount = 300) {
  const scrollAmount = direction === 'up' ? -amount : amount;
  window.scrollBy({ top: scrollAmount, behavior: 'smooth' });
  return { success: true };
}

// Execute press Enter action
function executePressEnter() {
  // Find the active element or the last focused input
  let activeElement = document.activeElement;
  
  // If no active element, try to find a search input
  if (!activeElement || activeElement === document.body) {
    const searchInput = document.querySelector('input[type="search"], input[name="q"], input[name="query"], input[type="text"]');
    if (searchInput) {
      activeElement = searchInput;
      searchInput.focus();
    }
  }
  
  if (activeElement) {
    // Dispatch Enter key events
    const enterEvent = new KeyboardEvent('keydown', {
      key: 'Enter',
      code: 'Enter',
      keyCode: 13,
      which: 13,
      bubbles: true,
      cancelable: true
    });
    
    activeElement.dispatchEvent(enterEvent);
    
    // Also try keypress and keyup
    activeElement.dispatchEvent(new KeyboardEvent('keypress', {
      key: 'Enter',
      code: 'Enter',
      keyCode: 13,
      which: 13,
      bubbles: true
    }));
    
    activeElement.dispatchEvent(new KeyboardEvent('keyup', {
      key: 'Enter',
      code: 'Enter',
      keyCode: 13,
      which: 13,
      bubbles: true
    }));
    
    // If element is in a form, try to submit it
    if (activeElement.form) {
      const submitButton = activeElement.form.querySelector('button[type="submit"], input[type="submit"]');
      if (submitButton) {
        submitButton.click();
      } else {
        // Try form submit
        try {
          activeElement.form.submit();
        } catch (e) {
          // Form might prevent default submit
        }
      }
    }
  }
  
  return { success: true };
}

// Message handler
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    try {
      switch (message.type) {
        case 'PING':
          sendResponse({ pong: true });
          break;
          
        case 'GET_PAGE_STATE':
          const elements = getInteractiveElements();
          const pageText = getPageText();
          sendResponse({
            url: window.location.href,
            title: document.title,
            elements,
            text: pageText
          });
          break;
          
        case 'EXECUTE_ACTION':
          const { action } = message;
          let result;
          
          switch (action.type) {
            case 'click':
              result = await executeClick(action.selector);
              break;
            case 'type':
              result = await executeType(action.selector, action.text);
              break;
            case 'scroll':
              result = executeScroll(action.direction, action.amount);
              break;
            case 'pressEnter':
              result = executePressEnter();
              break;
            default:
              result = { error: `Unknown action type: ${action.type}` };
          }
          
          sendResponse(result);
          break;
          
        case 'HIGHLIGHT':
          const el = document.querySelector(message.selector);
          if (el) showHighlight(el);
          sendResponse({ success: true });
          break;
          
        default:
          sendResponse({ error: 'Unknown message type' });
      }
    } catch (error) {
      sendResponse({ error: error.message });
    }
  })();
  
  return true; // Keep message channel open for async response
});

// Add indicator that content script is loaded
window.__webAgentLoaded = true;
console.log('[Web Agent] Content script loaded');
