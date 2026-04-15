const fs = require('fs');
const os = require('os');
const path = require('path');
const puppeteer = require('puppeteer-core');

const MANAGED_CHROME_PORT = Number(process.env.MANAGED_CHROME_PORT || 9222);
const PROFILE_DIR = process.env.MANAGED_CHROME_PROFILE_DIR
  || path.join(os.homedir(), '.proactive', 'managed-chrome-profile');

let browserPromise = null;
let browserInstance = null;
let managedPage = null;

function chromeLaunchOptions() {
  const chromePath = process.env.CHROME_PATH || null;
  const opts = {
    headless: false,
    userDataDir: PROFILE_DIR,
    args: [
      `--remote-debugging-port=${MANAGED_CHROME_PORT}`,
      '--no-default-browser-check',
      '--no-first-run',
      '--disable-features=TranslateUI'
    ]
  };
  if (chromePath) opts.executablePath = chromePath;
  else opts.channel = 'chrome';
  return opts;
}

async function ensureManagedBrowser() {
  if (browserInstance) return browserInstance;
  if (browserPromise) return browserPromise;

  browserPromise = (async () => {
    await fs.promises.mkdir(PROFILE_DIR, { recursive: true }).catch(() => {});
    const browser = await puppeteer.launch(chromeLaunchOptions());
    browser.on('disconnected', () => {
      browserInstance = null;
      browserPromise = null;
      managedPage = null;
    });
    browserInstance = browser;
    return browser;
  })();

  try {
    return await browserPromise;
  } finally {
    if (!browserInstance) browserPromise = null;
  }
}

async function getManagedPage() {
  const browser = await ensureManagedBrowser();
  if (managedPage && !managedPage.isClosed()) return managedPage;
  const pages = await browser.pages();
  managedPage = pages.find((page) => !page.isClosed()) || await browser.newPage();
  managedPage.setDefaultNavigationTimeout(30000);
  return managedPage;
}

function classifyBrowserSurface({ url = '', title = '', textSample = '', candidates = [] } = {}) {
  const hay = `${url} ${title} ${textSample}`.toLowerCase();
  if (/google\./.test(hay) && /search/.test(hay) && !/result/.test(hay)) return 'search_home';
  if (/google\./.test(hay) && (/[\?&]q=/.test(url) || candidates.some((item) => item.group === 'result_link'))) return 'search_results';
  if (/sign in|captcha|consent|verify|choose an account/.test(hay)) return 'auth_interstitial';
  return 'browser_page';
}

async function collectBrowserTree(page) {
  const client = await page.target().createCDPSession();
  const axTree = await client.send('Accessibility.getFullAXTree').catch(() => ({ nodes: [] }));
  const candidates = await page.evaluate(() => {
    const nodes = Array.from(document.querySelectorAll('a, button, input, textarea, [role="button"], [role="link"], [contenteditable="true"]'));
    const seen = new Set();
    return nodes
      .map((element, index) => {
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        const text = (element.innerText || element.textContent || element.value || element.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim();
        if (!rect.width || !rect.height || style.visibility === 'hidden' || style.display === 'none') return null;
        if (!element.dataset.weaveOperatorId) {
          element.dataset.weaveOperatorId = `cdp-${index}-${Math.random().toString(36).slice(2, 8)}`;
        }
        const id = element.dataset.weaveOperatorId;
        if (seen.has(id)) return null;
        seen.add(id);
        const tag = element.tagName.toLowerCase();
        const type = (element.getAttribute('type') || '').toLowerCase();
        let group = 'generic';
        if (tag === 'input' || tag === 'textarea' || /search/.test(type) || /search/.test(text.toLowerCase())) group = 'search_field';
        else if (tag === 'a' && text) group = /result|hotel|article|definition|world|official|contact/.test(text.toLowerCase()) ? 'result_link' : 'link';
        else if (tag === 'button' || element.getAttribute('role') === 'button') group = 'primary_button';
        return {
          id,
          role: element.getAttribute('role') || tag,
          name: text.slice(0, 160),
          description: element.getAttribute('aria-label') || element.getAttribute('title') || '',
          value: element.value || '',
          enabled: !element.disabled,
          hint: `${index + 1}. ${tag} ${text || element.getAttribute('aria-label') || ''}`.trim(),
          identifier: element.id || '',
          frame: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
          group,
          selector_hint: element.id ? `#${element.id}` : tag
        };
      })
      .filter(Boolean)
      .slice(0, 60);
  });

  const title = await page.title().catch(() => '');
  const url = page.url();
  const textSample = await page.evaluate(() => (document.body?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 2000)).catch(() => '');
  const surfaceType = classifyBrowserSurface({ url, title, textSample, candidates });

  return {
    url,
    title,
    textSample,
    surfaceType,
    candidates,
    axTree
  };
}

async function browserScreenshot(page) {
  try {
    const base64 = await page.screenshot({ type: 'jpeg', encoding: 'base64', quality: 70, fullPage: false });
    return {
      present: true,
      data_url: `data:image/jpeg;base64,${base64}`,
      captured_at: new Date().toISOString(),
      truncated: false
    };
  } catch (error) {
    return {
      present: false,
      error: error.message,
      captured_at: new Date().toISOString()
    };
  }
}

async function ensurePageForUrl(url = '') {
  const page = await getManagedPage();
  await page.bringToFront().catch(() => {});
  if (url && (page.url() === 'about:blank' || page.url() !== url)) {
    await page.goto(url, { waitUntil: 'domcontentloaded' }).catch((error) => {
      throw new Error(error.message || 'cdp_navigation_failed');
    });
  }
  return page;
}

async function observeManagedBrowserState(options = {}) {
  const page = await getManagedPage();
  await page.bringToFront().catch(() => {});
  const tree = await collectBrowserTree(page);
  const screenshot = options.includeScreenshot || options.visionRequest === 'browser_vlm'
    ? await browserScreenshot(page)
    : { present: false, skipped: true, captured_at: new Date().toISOString() };

  return {
    surface_driver: 'cdp',
    frontmost_app: 'Managed Chrome',
    window_title: tree.title || '',
    tab_title: tree.title || '',
    url: tree.url,
    browser_tree: {
      url: tree.url,
      title: tree.title,
      ax_nodes: Array.isArray(tree.axTree?.nodes) ? tree.axTree.nodes.slice(0, 120) : []
    },
    ax_tree: null,
    window_id: null,
    bounds: null,
    focused_element_id: null,
    focused_element: null,
    visible_elements: tree.candidates.map((item, index) => ({
      index,
      id: item.id,
      role: item.role,
      name: item.name,
      description: item.description,
      value: item.value,
      identifier: item.identifier,
      frame: item.frame,
      enabled: item.enabled,
      hint: item.hint
    })),
    interactive_candidates: tree.candidates.map((item, index) => ({
      ...item,
      index
    })),
    text_sample: tree.textSample,
    selection: null,
    permission_state: { trusted: true, status: 'trusted' },
    status: 'complete',
    surface_type: tree.surfaceType,
    perception_mode: options.visionRequest === 'browser_vlm' ? 'browser_vlm' : 'managed_chrome_cdp',
    vision_mode: options.visionRequest === 'browser_vlm' ? 'browser_vlm' : 'ax_only',
    window_clip_used: false,
    frame_summaries: [],
    visual_change_summary: '',
    target_confidence: tree.candidates.length ? 0.82 : 0.3,
    screenshot,
    vision_used: Boolean(screenshot.present),
    observation_budget: {
      include_screenshot: Boolean(screenshot.present),
      vision_mode: options.visionRequest === 'browser_vlm' ? 'browser_vlm' : 'ax_only',
      vision_reason: options.visionRequest || 'cdp_primary'
    }
  };
}

async function executeManagedBrowserAction(action = {}, context = {}) {
  const kind = String(action.kind || '').toUpperCase();
  const page = await getManagedPage();
  await page.bringToFront().catch(() => {});

  if (kind === 'CDP_GET_TREE' || kind === 'READ_UI_STATE') {
    return { status: 'success', observation: await observeManagedBrowserState(context.observeOptions || {}) };
  }
  if (kind === 'CDP_NAVIGATE' || kind === 'OPEN_URL') {
    const url = action.url || context.url;
    if (!url) throw new Error('Missing browser URL');
    await ensurePageForUrl(url);
    return { status: 'success', observation: await observeManagedBrowserState(context.observeOptions || {}) };
  }
  if (kind === 'CDP_CLICK') {
    const targetId = action.target_id;
    const clicked = await page.evaluate((id) => {
      const element = document.querySelector(`[data-weave-operator-id="${id}"]`);
      if (!element) return false;
      element.click();
      return true;
    }, targetId);
    if (!clicked) throw new Error('browser_target_not_found');
    return { status: 'success' };
  }
  if (kind === 'CDP_TYPE') {
    const targetId = action.target_id;
    const focused = await page.evaluate((id) => {
      const element = document.querySelector(`[data-weave-operator-id="${id}"]`);
      if (!element) return false;
      element.focus();
      return true;
    }, targetId);
    if (!focused) throw new Error('browser_target_not_found');
    await page.keyboard.down('Meta').catch(() => {});
    await page.keyboard.press('A').catch(() => {});
    await page.keyboard.up('Meta').catch(() => {});
    await page.keyboard.type(String(action.text || ''), { delay: 15 });
    if (action.submit) {
      await page.keyboard.press('Enter');
    }
    return { status: 'success' };
  }
  if (kind === 'CDP_KEY_PRESS') {
    await page.keyboard.press(String(action.key || 'Enter'));
    return { status: 'success' };
  }
  if (kind === 'CDP_SCROLL') {
    await page.evaluate((amount) => window.scrollBy(0, Math.max(240, Number(amount || 1) * 520)), action.amount || 1);
    return { status: 'success' };
  }
  if (kind === 'CDP_WAIT_FOR') {
    const timeout = Math.max(500, Number(action.timeout_ms || 2500));
    if (action.text) {
      await page.waitForFunction((needle) => (document.body?.innerText || '').toLowerCase().includes(String(needle || '').toLowerCase()), { timeout }, action.text);
    } else {
      await new Promise((resolve) => setTimeout(resolve, timeout));
    }
    return { status: 'success', observation: await observeManagedBrowserState(context.observeOptions || {}) };
  }
  throw new Error(`Unsupported managed browser action: ${kind}`);
}

async function getManagedBrowserStatus() {
  const running = Boolean(browserInstance || browserPromise);
  let url = '';
  let title = '';
  try {
    const page = running ? await getManagedPage() : null;
    url = page?.url() || '';
    title = page ? await page.title().catch(() => '') : '';
  } catch (_) {}
  return {
    running,
    websocket_endpoint: browserInstance?.wsEndpoint?.() || null,
    profile_mode: 'dedicated_debug_chrome',
    active_url: url,
    active_title: title,
    port: MANAGED_CHROME_PORT
  };
}

module.exports = {
  ensureManagedBrowser,
  observeManagedBrowserState,
  executeManagedBrowserAction,
  getManagedBrowserStatus
};
