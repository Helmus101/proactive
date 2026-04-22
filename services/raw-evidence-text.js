function safeText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function dedupeParts(parts = []) {
  const out = [];
  const seen = new Set();
  for (const part of parts) {
    const text = safeText(part).replace(/\s+/g, ' ').trim();
    if (!text) continue;
    const key = text.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(text);
  }
  return out;
}

function buildRawEvidenceText(row = {}, metadata = {}, { maxChars = 16000 } = {}) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const header = [
    row.app || meta.source_app || meta.app ? `App: ${row.app || meta.source_app || meta.app}` : '',
    row.title || meta.context_title || meta.window_title || meta.title ? `Window: ${row.title || meta.context_title || meta.window_title || meta.title}` : '',
    row.occurred_at || row.timestamp || meta.occurred_at ? `Time: ${row.occurred_at || row.timestamp || meta.occurred_at}` : '',
    meta.data_source || meta.storage_data_source ? `Source: ${meta.data_source || meta.storage_data_source}` : ''
  ].filter(Boolean).join('\n');

  const ocrParts = dedupeParts([
    meta.full_ocr_text,
    meta.raw_ocr_text,
    meta.ocr_text,
    meta.cleaned_capture_text,
    meta.compact_capture_text,
    meta.window_text,
    meta.visible_text,
    meta.screen_text,
    meta.capture_text,
    meta.ax_text,
    meta.raw_ax_text,
    row.text,
    row.redacted_text,
    row.raw_text
  ]);

  const body = ocrParts.length ? `Full OCR / raw capture:\n${ocrParts.join('\n\n')}` : '';
  return [header, body].filter(Boolean).join('\n').slice(0, Math.max(1000, Number(maxChars || 16000)));
}

module.exports = {
  buildRawEvidenceText
};
