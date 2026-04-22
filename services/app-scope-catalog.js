function safeText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function normalizeToken(value = '') {
  return safeText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9.+#\s_-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function unique(items = [], limit = 80) {
  const out = [];
  const seen = new Set();
  for (const item of items) {
    const value = safeText(item).trim();
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
    if (out.length >= limit) break;
  }
  return out;
}

const MAC_APP_CATALOG = [
  { name: 'Google Chrome', appIds: ['com.google.chrome'], aliases: ['chrome', 'google chrome', 'youtube', 'google meet', 'meet.google.com'] },
  { name: 'Safari', appIds: ['com.apple.Safari', 'com.apple.safari'], aliases: ['safari'] },
  { name: 'Arc', appIds: ['company.thebrowser.Browser'], aliases: ['arc', 'arc browser', 'the browser company'] },
  { name: 'Firefox', appIds: ['org.mozilla.firefox'], aliases: ['firefox', 'mozilla firefox'] },
  { name: 'Brave Browser', appIds: ['com.brave.Browser'], aliases: ['brave', 'brave browser'] },
  { name: 'Microsoft Edge', appIds: ['com.microsoft.edgemac'], aliases: ['edge', 'microsoft edge'] },
  { name: 'Cursor', appIds: ['com.todesktop.230313mzl4w4u92'], aliases: ['cursor', 'cursor editor'] },
  { name: 'VSCode', appIds: ['com.microsoft.vscode'], aliases: ['vscode', 'vs code', 'visual studio code', 'code editor'] },
  { name: 'Xcode', appIds: ['com.apple.dt.Xcode'], aliases: ['xcode', 'ios dev', 'swift', 'swiftui'] },
  { name: 'Terminal', appIds: ['com.apple.Terminal'], aliases: ['terminal', 'shell', 'zsh', 'bash'] },
  { name: 'iTerm', appIds: ['com.googlecode.iterm2'], aliases: ['iterm', 'iterm2', 'i term'] },
  { name: 'Warp', appIds: ['dev.warp.Warp-Stable', 'dev.warp.Warp'], aliases: ['warp', 'warp terminal'] },
  { name: 'GitHub Desktop', appIds: ['com.github.GitHubClient'], aliases: ['github desktop'] },
  { name: 'Docker Desktop', appIds: ['com.docker.docker'], aliases: ['docker', 'docker desktop'] },
  { name: 'Postman', appIds: ['com.postmanlabs.mac'], aliases: ['postman'] },
  { name: 'Slack', appIds: ['com.tinyspeck.slackmacgap'], aliases: ['slack', 'channel', 'dm', 'direct message'] },
  { name: 'Discord', appIds: ['com.hnc.Discord'], aliases: ['discord'] },
  { name: 'Microsoft Teams', appIds: ['com.microsoft.teams2', 'com.microsoft.teams'], aliases: ['teams', 'microsoft teams'] },
  { name: 'Zoom', appIds: ['us.zoom.xos'], aliases: ['zoom', 'zoom call', 'video call'] },
  { name: 'Messages', appIds: ['com.apple.MobileSMS'], aliases: ['messages', 'imessage', 'sms', 'text message'] },
  { name: 'WhatsApp', appIds: ['net.whatsapp.WhatsApp'], aliases: ['whatsapp'] },
  { name: 'Signal', appIds: ['org.whispersystems.signal-desktop'], aliases: ['signal'] },
  { name: 'Telegram', appIds: ['ru.keepcoder.Telegram'], aliases: ['telegram'] },
  { name: 'Mail', appIds: ['com.apple.mail'], aliases: ['mail', 'apple mail'] },
  { name: 'Gmail', appIds: ['com.google.gmail'], aliases: ['gmail', 'google mail', 'inbox'] },
  { name: 'Microsoft Outlook', appIds: ['com.microsoft.Outlook'], aliases: ['outlook', 'microsoft outlook'] },
  { name: 'Calendar', appIds: ['com.apple.iCal', 'com.google.calendar'], aliases: ['calendar', 'google calendar', 'meeting', 'agenda', 'invite', 'event'] },
  { name: 'Notion', appIds: ['notion.id'], aliases: ['notion', 'notion workspace'] },
  { name: 'Obsidian', appIds: ['md.obsidian'], aliases: ['obsidian'] },
  { name: 'Notes', appIds: ['com.apple.Notes'], aliases: ['notes', 'apple notes'] },
  { name: 'Reminders', appIds: ['com.apple.reminders'], aliases: ['reminders'] },
  { name: 'Finder', appIds: ['com.apple.finder'], aliases: ['finder'] },
  { name: 'Preview', appIds: ['com.apple.Preview'], aliases: ['preview', 'pdf'] },
  { name: 'Photos', appIds: ['com.apple.Photos'], aliases: ['photos'] },
  { name: 'Music', appIds: ['com.apple.Music'], aliases: ['music', 'apple music'] },
  { name: 'Spotify', appIds: ['com.spotify.client'], aliases: ['spotify'] },
  { name: 'Figma', appIds: ['com.figma.Desktop'], aliases: ['figma', 'design', 'prototype'] },
  { name: 'Linear', appIds: ['com.linear'], aliases: ['linear'] },
  { name: 'Jira', appIds: ['com.atlassian.jira'], aliases: ['jira'] },
  { name: 'Google Docs', appIds: ['com.google.docs'], aliases: ['google docs', 'docs', 'document', 'doc'] },
  { name: 'Google Sheets', appIds: ['com.google.sheets'], aliases: ['google sheets', 'sheets', 'spreadsheet'] },
  { name: 'Google Slides', appIds: ['com.google.slides'], aliases: ['google slides', 'slides', 'deck'] }
];

const CATEGORY_SCOPES = [
  { family: 'browser', pattern: /\b(browser|website|webpage|url|visited|youtube|video|trailer|watching|watched|playing|streaming)\b/i, apps: ['Google Chrome', 'Safari', 'Arc', 'Firefox', 'Brave Browser', 'Microsoft Edge'] },
  { family: 'coding', pattern: /\b(code editor|coding|ide|implementation|bug|error|stack trace|repo|repository|commit|pull request|\bpr\b|manifest|websocket)\b/i, apps: ['Cursor', 'VSCode', 'Xcode', 'Terminal', 'iTerm', 'Warp'] },
  { family: 'email', pattern: /\b(email|inbox|thread|reply|correspondence|from:|to:)\b/i, apps: ['Gmail', 'Mail', 'Microsoft Outlook'] },
  { family: 'messaging', pattern: /\b(message|chat|dm|direct message|channel|slack|discord|teams|whatsapp|signal|telegram|imessage|sms)\b/i, apps: ['Slack', 'Messages', 'WhatsApp', 'Signal', 'Telegram', 'Discord', 'Microsoft Teams'] },
  { family: 'meeting', pattern: /\b(meeting|calendar|agenda|invite|event|sync|call)\b/i, apps: ['Calendar', 'Google Chrome', 'Zoom', 'Microsoft Teams'] },
  { family: 'document', pattern: /\b(doc|document|notes|proposal|brief|spec|deck|spreadsheet|sheet)\b/i, apps: ['Notion', 'Notes', 'Obsidian', 'Google Docs', 'Google Sheets', 'Google Slides'] }
];

function getKnownMacApps() {
  return MAC_APP_CATALOG.map((entry) => ({
    name: entry.name,
    appIds: [...entry.appIds],
    aliases: [...entry.aliases]
  }));
}

function findAppEntry(value = '') {
  const normalized = normalizeToken(value);
  if (!normalized) return null;
  return MAC_APP_CATALOG.find((entry) => {
    const candidates = [entry.name, ...(entry.appIds || []), ...(entry.aliases || [])].map(normalizeToken);
    return candidates.includes(normalized);
  }) || null;
}

function normalizeAppName(value = '') {
  const entry = findAppEntry(value);
  return entry ? entry.name : safeText(value).trim();
}

function canonicalAppIdForName(value = '') {
  const raw = safeText(value).trim();
  const entry = findAppEntry(raw);
  if (entry?.appIds?.length) return entry.appIds[0];
  if (/^(com|app|org|io|dev|net|md|ru|us|company|co|tv|fm)\.[a-z0-9_.-]+$/i.test(raw)) return raw;
  return null;
}

function expandAppScopeValues(values = []) {
  const input = Array.isArray(values) ? values : (values ? [values] : []);
  const expanded = [];
  for (const value of input) {
    const raw = safeText(value).trim();
    if (!raw) continue;
    expanded.push(raw);
    const normalized = normalizeToken(raw);
    const direct = findAppEntry(raw);
    const entries = direct ? [direct] : MAC_APP_CATALOG.filter((entry) => {
      const candidates = [entry.name, ...(entry.appIds || []), ...(entry.aliases || [])].map(normalizeToken);
      return candidates.some((candidate) => candidate && (candidate.includes(normalized) || normalized.includes(candidate)));
    });
    for (const entry of entries) {
      expanded.push(entry.name, ...(entry.appIds || []), ...(entry.aliases || []));
    }
  }
  return unique(expanded.map((item) => safeText(item).toLowerCase()), 120);
}

function inferExplicitAppsFromText(text = '', limit = 8) {
  const source = safeText(text);
  const lower = normalizeToken(source);
  const apps = [];

  for (const entry of MAC_APP_CATALOG) {
    const aliases = [entry.name, ...(entry.aliases || [])]
      .map(normalizeToken)
      .filter(Boolean)
      .sort((a, b) => b.length - a.length);
    if (aliases.some((alias) => new RegExp(`(^|\\b)${alias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(\\b|$)`, 'i').test(lower))) {
      apps.push(entry.name);
    }
  }

  return unique(apps, limit);
}

function inferAppsFromText(text = '', limit = 8) {
  const source = safeText(text);
  const apps = inferExplicitAppsFromText(source, limit * 2);
  const hasExplicitCommunicationApp = apps.some((app) => [
    'slack',
    'messages',
    'whatsapp',
    'signal',
    'telegram',
    'discord',
    'microsoft teams',
    'gmail',
    'mail',
    'microsoft outlook'
  ].includes(normalizeToken(app)));

  for (const scope of CATEGORY_SCOPES) {
    // If a communication app is explicitly named, do not broaden to every
    // peer messenger/email client. Explicit app names are hard retrieval
    // intent; categories are only recall helpers when the app is ambiguous.
    if (hasExplicitCommunicationApp && ['messaging', 'email'].includes(scope.family)) continue;
    if (scope.pattern.test(source)) apps.push(...scope.apps);
  }

  return unique(apps, limit);
}

module.exports = {
  MAC_APP_CATALOG,
  getKnownMacApps,
  normalizeAppName,
  canonicalAppIdForName,
  expandAppScopeValues,
  inferExplicitAppsFromText,
  inferAppsFromText
};
