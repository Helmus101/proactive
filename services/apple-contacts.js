const { execFile } = require('child_process');

const DEFAULT_LIMIT = 500;
const VCARD_SPLIT = /END:VCARD\s*/g;

const APPLE_CONTACTS_VCARD_SCRIPT = `
tell application "Contacts"
  set cardsText to vcard of every person
end tell
return cardsText
`;

function runAppleScript(script, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    execFile('osascript', ['-e', script], { timeout: timeoutMs, maxBuffer: 24 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        const detail = String(stderr || error.message || '').trim();
        reject(new Error(detail || 'osascript failed'));
        return;
      }
      resolve(String(stdout || ''));
    });
  });
}

function unfoldVCardLines(text = '') {
  return String(text || '').replace(/\r\n[ \t]/g, '').replace(/\n[ \t]/g, '');
}

function decodeVCardValue(value = '') {
  return String(value || '')
    .replace(/\\n/gi, '\n')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .replace(/\\\\/g, '\\')
    .trim();
}

function parseDateCandidate(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  return text;
}

function parseVCard(raw = '') {
  const lines = unfoldVCardLines(raw).split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const contact = {
    name: '',
    company: '',
    role: '',
    emails: [],
    phones: [],
    addresses: [],
    urls: [],
    birthday: '',
    notes: ''
  };

  for (const line of lines) {
    const idx = line.indexOf(':');
    if (idx < 0) continue;
    const keyPart = line.slice(0, idx);
    const valuePart = decodeVCardValue(line.slice(idx + 1));
    const upperKey = keyPart.toUpperCase();

    if (upperKey.startsWith('FN')) {
      contact.name = valuePart || contact.name;
      continue;
    }
    if (upperKey.startsWith('ORG')) {
      contact.company = valuePart.split(';').filter(Boolean).join(' ').trim() || contact.company;
      continue;
    }
    if (upperKey.startsWith('TITLE')) {
      contact.role = valuePart || contact.role;
      continue;
    }
    if (upperKey.startsWith('EMAIL')) {
      if (valuePart) contact.emails.push(valuePart);
      continue;
    }
    if (upperKey.startsWith('TEL')) {
      if (valuePart) contact.phones.push(valuePart);
      continue;
    }
    if (upperKey.startsWith('ADR')) {
      const address = valuePart.split(';').filter(Boolean).join(', ').replace(/\s+,/g, ',').trim();
      if (address) contact.addresses.push(address);
      continue;
    }
    if (upperKey.startsWith('URL')) {
      if (valuePart) contact.urls.push(valuePart);
      continue;
    }
    if (upperKey.startsWith('BDAY')) {
      contact.birthday = parseDateCandidate(valuePart) || contact.birthday;
      continue;
    }
    if (upperKey.startsWith('NOTE')) {
      contact.notes = valuePart || contact.notes;
    }
  }

  contact.emails = Array.from(new Set(contact.emails));
  contact.phones = Array.from(new Set(contact.phones));
  contact.addresses = Array.from(new Set(contact.addresses));
  contact.urls = Array.from(new Set(contact.urls));
  return contact;
}

function parseVCardContacts(raw = '') {
  return String(raw || '')
    .split(VCARD_SPLIT)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => parseVCard(`${chunk}\nEND:VCARD`))
    .filter((item) => item.name || item.emails.length || item.phones.length);
}

async function fetchAppleContacts({ limit = DEFAULT_LIMIT } = {}) {
  if (process.platform !== 'darwin') return [];
  const raw = await runAppleScript(APPLE_CONTACTS_VCARD_SCRIPT);
  return parseVCardContacts(raw).slice(0, Math.max(1, Math.min(2000, Number(limit || DEFAULT_LIMIT))));
}

module.exports = {
  fetchAppleContacts,
  parseVCardContacts
};
