const axios = require('axios');

async function callCohereRerank({ query, documents = [], topN = 5 }) {
  const apiKey = process.env.COHERE_API_KEY;
  if (!apiKey) return null;

  const model = process.env.COHERE_RERANK_MODEL || 'rerank-v3.5';
  const payload = {
    model,
    query: String(query || ''),
    documents: (documents || []).map((d) => String(d || '')),
    top_n: Math.max(1, Math.min(Number(topN || 5), documents.length || 5))
  };

  const response = await axios.post('https://api.cohere.com/v2/rerank', payload, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    timeout: 12000
  });

  const rows = response?.data?.results;
  if (!Array.isArray(rows) || !rows.length) return null;
  return rows.map((row) => ({
    index: Number(row.index),
    score: Number(row.relevance_score || row.score || 0),
    provider: 'cohere'
  })).filter((item) => Number.isFinite(item.index));
}

async function callLocalBgeRerank({ query, documents = [], topN = 5 }) {
  const endpoint = process.env.BGE_RERANK_URL;
  if (!endpoint) return null;

  const payload = {
    query: String(query || ''),
    documents: (documents || []).map((d) => String(d || '')),
    top_n: Math.max(1, Math.min(Number(topN || 5), documents.length || 5))
  };

  const response = await axios.post(endpoint, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: 10000
  });

  const body = response?.data;
  const rows = Array.isArray(body?.results)
    ? body.results
    : (Array.isArray(body?.data) ? body.data : []);
  if (!rows.length) return null;

  return rows.map((row) => ({
    index: Number(row.index),
    score: Number(row.score || row.relevance_score || row.similarity || 0),
    provider: 'bge-local'
  })).filter((item) => Number.isFinite(item.index));
}

async function externalRerank({ query, documents = [], topN = 5 }) {
  if (!query || !Array.isArray(documents) || !documents.length) return null;

  const providerPreference = String(process.env.RERANK_PROVIDER || '').toLowerCase().trim();
  const candidates = providerPreference === 'bge'
    ? [callLocalBgeRerank, callCohereRerank]
    : [callCohereRerank, callLocalBgeRerank];

  for (const providerCall of candidates) {
    try {
      const rows = await providerCall({ query, documents, topN });
      if (Array.isArray(rows) && rows.length) return rows;
    } catch (_) {
      // silent fallback to next provider
    }
  }

  return null;
}

module.exports = {
  externalRerank
};
