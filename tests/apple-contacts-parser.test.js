const assert = require('assert');

const { parseVCardContacts } = require('../services/apple-contacts');

const RAW = `BEGIN:VCARD
VERSION:3.0
FN:Alice Martin
ORG:Acme Ventures
TITLE:Partner
EMAIL;type=INTERNET;type=WORK;type=pref:alice@example.com
EMAIL;type=INTERNET;type=WORK:alice@acme.com
TEL;type=CELL;type=VOICE;type=pref:+1 (555) 111-2222
ADR;type=WORK:;;123 Main St;;;\\;
URL;type=LinkedIn:https://linkedin.com/in/alice
BDAY:20240102
NOTE:Met at demo day
END:VCARD
BEGIN:VCARD
VERSION:3.0
FN:Bob Lee
END:VCARD`;

const parsed = parseVCardContacts(RAW);

assert.strictEqual(parsed.length, 2, 'should parse two Apple Contacts rows');
assert.strictEqual(parsed[0].name, 'Alice Martin');
assert.deepStrictEqual(parsed[0].emails, ['alice@example.com', 'alice@acme.com']);
assert.deepStrictEqual(parsed[0].phones, ['+1 (555) 111-2222']);
assert.deepStrictEqual(parsed[0].addresses, ['123 Main St']);
assert.deepStrictEqual(parsed[0].urls, ['https://linkedin.com/in/alice']);
assert.strictEqual(parsed[0].birthday, '2024-01-02');
assert.strictEqual(parsed[0].company, 'Acme Ventures');
assert.strictEqual(parsed[0].role, 'Partner');

console.log('apple-contacts-parser.test.js passed');
