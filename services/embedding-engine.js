/**
 * Embeddings & Semantic Search Engine
 * Adheres to the Deep Tech Spec for 1536-dimensional embeddings.
 */

async function generateEmbedding(text, apiKey = process.env.OPENAI_API_KEY) {
  if (!apiKey) {
    // Fallback: Generate a deterministic 1536-dim vector based on text hash
    // (Used when API key is missing to allow architecture to function locally)
    const vec = new Array(1536).fill(0);
    let hash = 0;
    for (let i = 0; i < text.length; i++) {
        hash = ((hash << 5) - hash) + text.charCodeAt(i);
        hash |= 0;
    }
    const seed = Math.abs(hash);
    for (let i = 0; i < 1536; i++) {
        vec[i] = Math.sin(seed + i);
    }
    // Normalize fallback
    const mag = Math.sqrt(vec.reduce((sum, val) => sum + val * val, 0));
    return vec.map(v => v / mag);
  }

  try {
    const res = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "text-embedding-3-small", // 1536 dims natively
        input: text.slice(0, 8000) // Stay within context window
      })
    });
    const data = await res.json();
    return data.data[0].embedding; // 1536 size array
  } catch (e) {
    console.warn("Embedding generation failed, falling back to empty vector.", e.message);
    return new Array(1536).fill(0);
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
