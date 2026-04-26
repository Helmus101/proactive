const path = require('path');
const os = require('os');
const fs = require('fs');
const sqlite3 = (process.env.NODE_ENV === 'production') ? require('sqlite3') : require('sqlite3').verbose();
const { existsAsync, withTimeout } = require('./utils');

const SCREENSHOT_BROWSER_HISTORY_MAX_AGE_MS = 2 * 60 * 1000;
const BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS = 10 * 60 * 1000;
const BROWSER_HISTORY_REFRESH_MIN_GAP_MS = 5 * 60 * 1000;
const BROWSER_HISTORY_REFRESH_TIMEOUT_MS = 12000;

const KNOWN_BROWSER_APPS = new Set([
  'google chrome',
  'chrome',
  'safari',
  'arc',
  'brave browser',
  'brave',
  'microsoft edge',
  'edge',
  'firefox'
]);

let browserHistoryCache = {
  urls: [],
  fetchedAt: 0
};
let browserHistoryRefreshPromise = null;
let browserHistoryRefreshMeta = {
  lastAttemptAt: 0,
  lastSuccessAt: 0,
  lastError: null,
  lastErrorAt: 0
};

let safariFullDiskAccessWarned = false;

let deps = {
  store: null,
  updateMemoryGraphHealth: () => {},
  emitMemoryGraphUpdate: () => {}
};

function init(options) {
  Object.assign(deps, options);
}

function isBrowserAppName(appName = '') {
  return KNOWN_BROWSER_APPS.has(String(appName || '').toLowerCase());
}

function normalizeBrowserHistoryItem(item = {}) {
  const url = String(item.url || '').trim();
  if (!url || url.startsWith('chrome://') || url.startsWith('about:') || url.startsWith('file:')) return null;
  return {
    url,
    title: String(item.title || item.name || '').trim(),
    timestamp: Number(item.timestamp || item.last_visit_time || Date.now()),
    browser: String(item.browser || 'unknown').toLowerCase()
  };
}

function flattenStoredBrowserHistory() {
  if (!deps.store) return [];
  const googleData = deps.store.get('googleData') || {};
  const history = [];
  if (Array.isArray(googleData.tabs)) history.push(...googleData.tabs);
  if (Array.isArray(googleData.history)) history.push(...googleData.history);
  return history;
}

function updateBrowserHistoryCache(rows = [], source = 'background') {
  const normalized = (rows || []).map(normalizeBrowserHistoryItem).filter(Boolean);
  if (!normalized.length) return browserHistoryCache.urls;
  browserHistoryCache = {
    urls: normalized,
    fetchedAt: Date.now()
  };
  deps.updateMemoryGraphHealth({
    browserHistoryCacheSize: normalized.length,
    browserHistoryCacheSource: source,
    browserHistoryFetchedAt: new Date(browserHistoryCache.fetchedAt).toISOString()
  });
  return browserHistoryCache.urls;
}

function getCachedBrowserHistory({ maxAgeMs = SCREENSHOT_BROWSER_HISTORY_MAX_AGE_MS } = {}) {
  const now = Date.now();
  if (browserHistoryCache.urls.length && (now - browserHistoryCache.fetchedAt) < maxAgeMs) {
    return browserHistoryCache.urls;
  }
  const stored = flattenStoredBrowserHistory();
  if (stored.length) {
    updateBrowserHistoryCache(stored, 'stored_snapshot');
  }
  return browserHistoryCache.urls;
}

async function refreshBrowserHistory(options = {}) {
  const force = options?.force === true;
  const reason = String(options?.reason || 'background');
  const timeoutMs = Math.max(2000, Number(options?.timeoutMs || BROWSER_HISTORY_REFRESH_TIMEOUT_MS));
  const minGapMs = Math.max(1000, Number(options?.minGapMs || BROWSER_HISTORY_REFRESH_MIN_GAP_MS));
  const maxAgeMs = Math.max(1000, Number(options?.maxAgeMs || BACKGROUND_BROWSER_HISTORY_MAX_AGE_MS));
  const now = Date.now();

  if (!force && browserHistoryCache.urls.length && (now - browserHistoryCache.fetchedAt) < maxAgeMs) {
    console.log(`[BrowserHistory] Serving cached history for ${reason}; age=${Math.round((now - browserHistoryCache.fetchedAt) / 1000)}s`);
    return browserHistoryCache.urls;
  }

  if (!force && browserHistoryRefreshPromise) {
    console.log(`[BrowserHistory] Reusing in-flight refresh for ${reason}`);
    return browserHistoryRefreshPromise;
  }

  if (!force && browserHistoryRefreshMeta.lastAttemptAt && (now - browserHistoryRefreshMeta.lastAttemptAt) < minGapMs) {
    console.log(`[BrowserHistory] Skipping refresh for ${reason}; min gap not reached`);
    return browserHistoryCache.urls;
  }

  browserHistoryRefreshMeta.lastAttemptAt = now;
  deps.updateMemoryGraphHealth({
    browserHistoryRefreshStatus: 'running',
    browserHistoryRefreshReason: reason,
    browserHistoryRefreshStartedAt: new Date(now).toISOString()
  });

  browserHistoryRefreshPromise = (async () => {
    const startedAt = Date.now();
    try {
      const fresh = await withTimeout(getBrowserHistory(), timeoutMs, 'refreshBrowserHistory');
      const normalized = updateBrowserHistoryCache(fresh, reason);
      browserHistoryRefreshMeta.lastSuccessAt = Date.now();
      browserHistoryRefreshMeta.lastError = null;
      browserHistoryRefreshMeta.lastErrorAt = 0;
      const durationMs = Date.now() - startedAt;
      console.log(`[BrowserHistory] Refreshed ${normalized.length} URLs for ${reason} in ${durationMs}ms`);
      deps.updateMemoryGraphHealth({
        browserHistoryRefreshStatus: 'idle',
        browserHistoryRefreshDurationMs: durationMs,
        browserHistoryLastRefreshAt: new Date(browserHistoryRefreshMeta.lastSuccessAt).toISOString()
      });
      deps.emitMemoryGraphUpdate({
        type: 'job_status',
        job: 'browser_history_refresh',
        status: 'completed',
        duration_ms: durationMs,
        source: reason
      });
      return normalized;
    } catch (error) {
      browserHistoryRefreshMeta.lastError = error?.message || String(error);
      browserHistoryRefreshMeta.lastErrorAt = Date.now();
      console.warn(`[BrowserHistory] Failed refresh for ${reason}:`, browserHistoryRefreshMeta.lastError);
      deps.updateMemoryGraphHealth({
        browserHistoryRefreshStatus: 'error',
        browserHistoryRefreshError: browserHistoryRefreshMeta.lastError,
        browserHistoryRefreshErrorAt: new Date(browserHistoryRefreshMeta.lastErrorAt).toISOString()
      });
      deps.emitMemoryGraphUpdate({
        type: 'job_status',
        job: 'browser_history_refresh',
        status: 'failed',
        error: browserHistoryRefreshMeta.lastError
      });
      return browserHistoryCache.urls;
    } finally {
      browserHistoryRefreshPromise = null;
    }
  })();

  return browserHistoryRefreshPromise;
}

async function getBrowserHistory() {
  const history = [];

  const chromiumHistory = await getChromiumHistory();
  history.push(...chromiumHistory);

  if (process.platform === 'darwin') {
    const safariHistory = await getSafariHistory();
    history.push(...safariHistory);
  }

  history.sort((a, b) => b.timestamp - a.timestamp);

  const seen = new Set();
  const deduped = history.filter(item => {
    const key = `${item.url}|${Math.floor(item.timestamp / 60000)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`Browser history total: ${deduped.length} URLs from ${history.length} raw entries`);
  return deduped.slice(0, 300);
}

async function readChromiumHistoryDB(dbPath, browserName) {
  if (!(await existsAsync(dbPath))) return [];

  const tmpPath = path.join(os.tmpdir(), `chromium_hist_${Date.now()}_${Math.random().toString(36).slice(2)}.db`);
  try {
    await fs.promises.copyFile(dbPath, tmpPath);
  } catch (e) {
    console.warn(`Could not copy ${browserName} DB at ${dbPath}:`, e.message);
    return [];
  }

  try {
    return await new Promise((resolve) => {
      const db = new sqlite3.Database(tmpPath, sqlite3.OPEN_READONLY, (err) => {
        if (err) {
          console.warn(`Could not open ${browserName} DB copy:`, err.message);
          resolve([]);
        }
      });

      const EPOCH_OFFSET_US = 11644473600 * 1_000_000;
      const sevenDaysAgo_us = (Date.now() * 1000) - (7 * 24 * 60 * 60 * 1_000_000) + EPOCH_OFFSET_US;

      db.all(
        `SELECT url, title, visit_count, last_visit_time
         FROM urls
         WHERE last_visit_time > ?
         ORDER BY last_visit_time DESC
         LIMIT 500`,
        [sevenDaysAgo_us],
        (err, rows) => {
          db.close(async () => { try { await fs.promises.unlink(tmpPath); } catch (_) {} });
          if (err) {
            console.warn(`${browserName} query error:`, err.message);
            resolve([]);
            return;
          }
          const entries = (rows || []).map(row => ({
            url: row.url,
            title: row.title && row.title.trim() ? row.title : extractDomain(row.url),
            domain: extractDomain(row.url),
            category: categorizeURL(row.url),
            timestamp: (row.last_visit_time - EPOCH_OFFSET_US) / 1000,
            visitCount: row.visit_count || 1,
            browser: browserName
          }));
          resolve(entries);
        }
      );
    });
  } catch (sqlErr) {
    try { await fs.promises.unlink(tmpPath); } catch (_) {}
    console.warn(`sqlite3 unavailable for ${browserName}:`, sqlErr.message);
    return [];
  }
}

async function getChromiumHistory() {
  const home = os.homedir();
  const all  = [];
  const browserBases = [];

  if (process.platform === 'darwin') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, 'Library/Application Support/Google/Chrome') },
      { name: 'Brave',   base: path.join(home, 'Library/Application Support/BraveSoftware/Brave-Browser') },
      { name: 'Arc',     base: path.join(home, 'Library/Application Support/Arc/User Data') },
      { name: 'Edge',    base: path.join(home, 'Library/Application Support/Microsoft Edge') }
    );
  } else if (process.platform === 'linux') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, '.config/google-chrome') },
      { name: 'Brave',   base: path.join(home, '.config/BraveSoftware/Brave-Browser') }
    );
  } else if (process.platform === 'win32') {
    browserBases.push(
      { name: 'Chrome',  base: path.join(home, 'AppData/Local/Google/Chrome/User Data') },
      { name: 'Brave',   base: path.join(home, 'AppData/Local/BraveSoftware/Brave-Browser/User Data') },
      { name: 'Edge',    base: path.join(home, 'AppData/Local/Microsoft/Edge/User Data') }
    );
  }

  for (const { name, base } of browserBases) {
    if (!(await existsAsync(base))) continue;
    let profileDirs;
    try {
      profileDirs = ['Default', ...(await fs.promises.readdir(base)).filter(d => /^Profile \d+$/.test(d))];
    } catch (_) {
      profileDirs = ['Default'];
    }
    for (const profile of profileDirs) {
      const dbPath = path.join(base, profile, 'History');
      const entries = await readChromiumHistoryDB(dbPath, name);
      all.push(...entries);
    }
  }
  return all;
}

async function getSafariHistory() {
  try {
    if (process.platform !== 'darwin') return [];
    const safariHistoryPath = path.join(os.homedir(), 'Library/Safari/History.db');
    if (!(await existsAsync(safariHistoryPath))) return [];

    const tmpPath = path.join(os.tmpdir(), `safari_history_${Date.now()}.db`);
    try {
      await fs.promises.copyFile(safariHistoryPath, tmpPath);
    } catch (e) {
      if (e.code === 'EPERM') {
        if (!safariFullDiskAccessWarned) {
          console.log('Skipping Safari history — Full Disk Access is required.');
          safariFullDiskAccessWarned = true;
        }
      } else {
        console.warn('Could not copy Safari history DB:', e.message);
      }
      return [];
    }

    try {
      return await new Promise((resolve) => {
        const db = new sqlite3.Database(tmpPath, sqlite3.OPEN_READONLY, (err) => {
          if (err) {
            console.error('Could not open Safari history DB copy:', err.message);
            resolve([]);
            return;
          }
        });

        const APPLE_EPOCH_MS = new Date('2001-01-01T00:00:00Z').getTime();
        const sevenDaysAgo_apple = ((Date.now() - APPLE_EPOCH_MS) / 1000) - 7 * 24 * 60 * 60;

        const query = `
          SELECT history_items.url, history_items.title, history_items.visit_count, history_visits.visit_time
          FROM history_items
          JOIN history_visits ON history_items.id = history_visits.history_item
          WHERE history_visits.visit_time > ?
          ORDER BY history_visits.visit_time DESC
          LIMIT 200
        `;

        db.all(query, [sevenDaysAgo_apple], (err, rows) => {
          db.close(async () => { try { await fs.promises.unlink(tmpPath); } catch (_) {} });
          if (err) {
            console.error('Safari history query error:', err.message);
            resolve([]);
            return;
          }
          const history = rows.map(row => ({
            url: row.url,
            title: row.title || 'Untitled',
            domain: extractDomain(row.url),
            category: categorizeURL(row.url),
            timestamp: (row.visit_time * 1000) + APPLE_EPOCH_MS,
            visitCount: row.visit_count || 1,
            browser: 'Safari'
          }));
          resolve(history);
        });
      });
    } catch (sqliteError) {
      try { await fs.promises.unlink(tmpPath); } catch (_) {}
      return [];
    }
  } catch (error) {
    return [];
  }
}

function extractDomain(url) {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch (error) {
    return 'unknown';
  }
}

function categorizeURL(url) {
  const domain = extractDomain(url);
  if (domain.includes('github') || domain.includes('stackoverflow') || domain.includes('linkedin')) return 'work';
  if (domain.includes('twitter') || domain.includes('facebook') || domain.includes('instagram')) return 'social';
  if (domain.includes('youtube') || domain.includes('netflix')) return 'entertainment';
  if (domain.includes('amazon') || domain.includes('ebay')) return 'shopping';
  if (domain.includes('coursera') || domain.includes('udemy')) return 'learning';
  return 'general';
}

module.exports = {
  init,
  isBrowserAppName,
  normalizeBrowserHistoryItem,
  flattenStoredBrowserHistory,
  updateBrowserHistoryCache,
  getCachedBrowserHistory,
  refreshBrowserHistory,
  getBrowserHistory,
  extractDomain,
  categorizeURL,
  browserHistoryCache,
  browserHistoryRefreshMeta
};
