/**
 * Embeddings & Semantic Search Engine
 * Adheres to the Deep Tech Spec for 1536-dimensional embeddings.
 */

const crypto = require('crypto');
const db = require('./db');

let lastEmbeddingWarnAt = 0;
let remoteEmbeddingDisabledUntil = 0;
let consecutiveEmbeddingFailures = 0;

function localDeterministicEmbedding(text = '') {
  // Deterministic 1536-dim vector fallback so ranking still has non-zero semantics offline.
  const vec = new Array(1536).fill(0);
  const input = String(text || '');
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    hash = ((hash << 5) - hash) + input.charCodeAt(i);
    hash |= 0;
  }
  const seed = Math.abs(hash);
  for (let i = 0; i < 1536; i++) vec[i] = Math.sin(seed + i);
  const mag = Math.sqrt(vec.reduce((sum, val) => sum + (val * val), 0)) || 1;
  return vec.map((v) => v / mag);
}

function warnEmbeddingFallbackOnce(message) {
  const now = Date.now();
  if ((now - lastEmbeddingWarnAt) < 2 * 60 * 1000) return;
  console.warn(message);
  lastEmbeddingWarnAt = now;
}

async function generateEmbedding(text, apiKey = process.env.OPENAI_API_KEY) {
  if (!text) return localDeterministicEmbedding('');
  
  // 1. Hash the input text for cache lookup
  const cleanText = String(text).slice(0, 8000);
  const hash = crypto.createHash('sha256').update(cleanText).digest('hex');
  
  // 2. Check local SQLite cache
  try {
    const cached = await db.getQuery('SELECT value FROM kv_cache WHERE key = ? AND type = ?', [hash, 'embedding']);
    if (cached && cached.value) {
      return JSON.parse(cached.value);
    }
  } catch (e) {
    console.warn('[Embedding] Cache lookup failed:', e.message);
  }

  if (!apiKey) return localDeterministicEmbedding(text);
  if (Date.now() < remoteEmbeddingDisabledUntil) return localDeterministicEmbedding(text);

  try {
    const res = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "text-embedding-3-small", // 1536 dims natively
        input: cleanText
      })
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}${body ? `: ${body.slice(0, 160)}` : ''}`);
    }
    const data = await res.json().catch(() => null);
    const embedding = data?.data?.[0]?.embedding;
    if (!Array.isArray(embedding) || !embedding.length) {
      throw new Error('invalid embedding payload');
    }
    consecutiveEmbeddingFailures = 0;

    // 3. Store in local SQLite cache
    try {
      await db.runQuery(
        'INSERT OR REPLACE INTO kv_cache (key, value, type, created_at) VALUES (?, ?, ?, ?)',
        [hash, JSON.stringify(embedding), 'embedding', new Date().toISOString()]
      );
    } catch (e) {
      console.warn('[Embedding] Cache storage failed:', e.message);
    }

    return embedding;
  } catch (e) {
    consecutiveEmbeddingFailures += 1;
    // If upstream/network is down repeatedly, back off remote calls for 10 minutes.
    if (consecutiveEmbeddingFailures >= 3) {
      remoteEmbeddingDisabledUntil = Date.now() + (10 * 60 * 1000);
    }
    warnEmbeddingFallbackOnce(`Embedding generation unavailable; using local fallback. ${String(e?.message || e)}`);
    return localDeterministicEmbedding(text);
  }
}

function cosineSimilarity(vecA, vecB) {
  if (!vecA || !vecB || vecA.length !== vecB.length) return 0;
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < vecA.length; i++) {
    dotProduct += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }
  if (normA === 0 || normB === 0) return 0;
  return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
}

module.exports = {
  generateEmbedding,
  cosineSimilarity
};
