const db = require('../db');

const ARTIFACT_TYPES = ['task', 'suggestion', 'person', 'fact', 'semantic', 'artifact'];
const INSIGHT_TYPES = ['insight'];
const EPISODE_TYPES = ['episode'];
const RAW_EVENT_TYPES = ['BrowserVisit', 'ScreenCapture', 'EmailThread', 'CalendarEvent', 'email', 'calendar_event'];

function asObj(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (_) {
    return {};
  }
}

function parseTs(value) {
  if (!value && value !== 0) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const n = Date.parse(value);
  return Number.isFinite(n) ? n : null;
}

function toText(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function normalizeTerms(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 3)
    .slice(0, 16);
}

function termScore(text, terms) {
  if (!terms.length) return 0;
  const hay = String(text || '').toLowerCase();
  let score = 0;
  for (const term of terms) {
    if (hay.includes(term)) score += 1;
  }
  return score;
}

function pickTop(entries, limit) {
  return (entries || [])
    .sort((a, b) => {
      if ((b.score || 0) !== (a.score || 0)) return (b.score || 0) - (a.score || 0);
      return (b.timestamp || 0) - (a.timestamp || 0);
    })
    .slice(0, limit);
}

function nodeTimestamp(node) {
  const data = asObj(node?.data);
  return parseTs(
    data.timestamp ||
      data.startTimestamp ||
      data.start_time ||
      data.start ||
      data.updated_at ||
      data.updated ||
      data.date ||
      null
  );
}

function eventTimestamp(eventRow) {
  return parseTs(eventRow?.timestamp) || parseTs(asObj(eventRow?.metadata).timestamp) || null;
}

function makeEvidenceEntry(entry) {
  return {
    id: entry.id,
    type: entry.type,
    source: entry.source,
    timestamp: entry.timestamp || null,
    score: Number(entry.score || 0)
  };
}

async function loadEdgesForIds(ids) {
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  return db.allQuery(
    `SELECT from_id, to_id, relation, data
     FROM edges
     WHERE from_id IN (${placeholders}) OR to_id IN (${placeholders})`,
    [...ids, ...ids]
  );
}

async function loadNodesByIds(ids) {
  if (!ids.length) return [];
  const placeholders = ids.map(() => '?').join(',');
  return db.allQuery(
    `SELECT id, type, data
     FROM nodes
     WHERE id IN (${placeholders})`,
    ids
  );
}

function summarizeEvidenceText(evidenceRows) {
  return evidenceRows
    .slice(0, 8)
    .map((row) => {
      if (row.source === 'events') {
        const meta = asObj(row.metadata);
        const text = String(row.text || meta.subject || meta.summary || '').slice(0, 180);
        return `- [raw:${row.type}] ${text}`;
      }
      const data = asObj(row.data);
      const text = String(data.title || data.summary || data.fact || data.insight || data.description || row.id).slice(0, 180);
      return `- [${row.type}] ${text}`;
    })
    .join('\n');
}

function computeConfidence(layerStats, evidenceCount, adaptiveUsedRaw) {
  let confidence = 0.34;
  if (layerStats.core > 0) confidence += 0.18;
  if (layerStats.artifacts > 0) confidence += 0.18;
  if ((layerStats.insights + layerStats.episodes) > 0) confidence += 0.18;
  confidence += Math.min(0.18, evidenceCount * 0.02);
  if (adaptiveUsedRaw) confidence += 0.06;
  return Math.max(0, Math.min(0.97, confidence));
}

async function retrieveCoreFirstContext({
  candidate = {},
  queryText = '',
  maxArtifacts = 8,
  maxInsights = 6,
  maxEpisodes = 8,
  maxRaw = 10,
  minConfidenceForRawSkip = 0.72
} = {}) {
  const now = Date.now();
  const candidateText = `${candidate.type || ''} ${toText(candidate.data)} ${candidate.title || ''} ${candidate.id || ''}`;
  const terms = normalizeTerms(`${queryText} ${candidateText}`);

  const retrievalPath = [];
  const skippedLayers = [];
  const layerStats = { core: 0, artifacts: 0, insights: 0, episodes: 0, raw: 0 };
  const evidenceRows = [];

  // 1) Core
  const coreRow = await db.getQuery(`SELECT id, type, data FROM nodes WHERE id = 'global_core' LIMIT 1`);
  let startIds = [];
  if (coreRow) {
    retrievalPath.push('global_core');
    layerStats.core = 1;
    evidenceRows.push({ ...coreRow, source: 'nodes', score: 1.0, timestamp: nodeTimestamp(coreRow) });
    startIds = [coreRow.id];
  } else {
    skippedLayers.push({ layer: 'core', reason: 'empty' });
  }

  // 2) Related artifacts
  const artifactRows = await db.allQuery(
    `SELECT id, type, data
     FROM nodes
     WHERE type IN (${ARTIFACT_TYPES.map(() => '?').join(',')})
     LIMIT 350`,
    ARTIFACT_TYPES
  );
  const artifactScored = artifactRows.map((row) => ({
    ...row,
    source: 'nodes',
    score: termScore(`${row.id} ${row.type} ${toText(row.data)}`, terms),
    timestamp: nodeTimestamp(row)
  }));
  const topArtifacts = pickTop(artifactScored.filter((r) => r.score > 0), maxArtifacts);
  if (topArtifacts.length) {
    retrievalPath.push('artifacts');
    layerStats.artifacts = topArtifacts.length;
    evidenceRows.push(...topArtifacts);
    startIds.push(...topArtifacts.map((r) => r.id));
  } else {
    skippedLayers.push({ layer: 'artifacts', reason: terms.length ? 'not_relevant' : 'empty' });
  }

  // 3) Insights + Episodes via core/related edges
  const uniqueStartIds = Array.from(new Set(startIds.concat(candidate.id ? [candidate.id] : []))).filter(Boolean);
  const firstHopEdges = await loadEdgesForIds(uniqueStartIds);
  const firstHopIds = new Set();
  firstHopEdges.forEach((edge) => {
    firstHopIds.add(edge.from_id);
    firstHopIds.add(edge.to_id);
  });
  const secondHopEdges = await loadEdgesForIds(Array.from(firstHopIds));
  secondHopEdges.forEach((edge) => {
    firstHopIds.add(edge.from_id);
    firstHopIds.add(edge.to_id);
  });
  const connectedNodes = await loadNodesByIds(Array.from(firstHopIds));

  const insightCandidates = connectedNodes
    .filter((row) => INSIGHT_TYPES.includes(row.type))
    .map((row) => ({
      ...row,
      source: 'nodes',
      score: termScore(`${row.id} ${row.type} ${toText(row.data)}`, terms) + 0.3,
      timestamp: nodeTimestamp(row)
    }));
  const topInsights = pickTop(insightCandidates, maxInsights);
  if (topInsights.length) {
    retrievalPath.push('insights');
    layerStats.insights = topInsights.length;
    evidenceRows.push(...topInsights);
  } else {
    skippedLayers.push({ layer: 'insights', reason: 'empty' });
  }

  const episodeCandidates = connectedNodes
    .filter((row) => EPISODE_TYPES.includes(row.type))
    .map((row) => ({
      ...row,
      source: 'nodes',
      score: termScore(`${row.id} ${row.type} ${toText(row.data)}`, terms) + 0.25,
      timestamp: nodeTimestamp(row)
    }));
  let topEpisodes = pickTop(episodeCandidates, maxEpisodes);
  if (!topEpisodes.length) {
    const fallbackEpisodes = await db.allQuery(`SELECT id, type, data FROM nodes WHERE type = 'episode' LIMIT 120`);
    topEpisodes = pickTop(
      fallbackEpisodes.map((row) => ({
        ...row,
        source: 'nodes',
        score: termScore(`${row.id} ${toText(row.data)}`, terms),
        timestamp: nodeTimestamp(row)
      })),
      Math.max(3, Math.min(6, maxEpisodes))
    );
  }
  if (topEpisodes.length) {
    retrievalPath.push('episodes');
    layerStats.episodes = topEpisodes.length;
    evidenceRows.push(...topEpisodes);
  } else {
    skippedLayers.push({ layer: 'episodes', reason: 'empty' });
  }

  let adaptiveUsedRaw = false;
  let roughConfidence = computeConfidence(layerStats, evidenceRows.length, false);

  // 4) Raw drilldown when confidence is insufficient
  if (roughConfidence < minConfidenceForRawSkip) {
    const rawRows = await db.allQuery(
      `SELECT id, type, timestamp, source, text, metadata
       FROM events
       WHERE type IN (${RAW_EVENT_TYPES.map(() => '?').join(',')})
       ORDER BY timestamp DESC
       LIMIT 400`,
      RAW_EVENT_TYPES
    );

    const episodeSourceEventIds = new Set();
    topEpisodes.forEach((ep) => {
      const data = asObj(ep.data);
      const ids = Array.isArray(data.source_events) ? data.source_events : [];
      ids.forEach((id) => episodeSourceEventIds.add(id));
    });

    const scoredRaw = rawRows.map((row) => {
      const linkedBoost = episodeSourceEventIds.has(row.id) ? 2 : 0;
      const textScore = termScore(`${row.type} ${row.source} ${row.text || ''} ${toText(row.metadata)}`, terms);
      const ts = eventTimestamp(row);
      const freshnessBoost = ts ? Math.max(0, 1 - ((now - ts) / (7 * 24 * 60 * 60 * 1000))) : 0;
      return {
        ...row,
        source: 'events',
        score: textScore + linkedBoost + freshnessBoost,
        timestamp: ts
      };
    });

    const topRaw = pickTop(scoredRaw.filter((row) => row.score > 0), maxRaw);
    if (topRaw.length) {
      retrievalPath.push('raw');
      layerStats.raw = topRaw.length;
      adaptiveUsedRaw = true;
      evidenceRows.push(...topRaw);
    } else {
      skippedLayers.push({ layer: 'raw', reason: 'not_relevant' });
    }
  } else {
    skippedLayers.push({ layer: 'raw', reason: 'confidence_high' });
  }

  const confidence = computeConfidence(layerStats, evidenceRows.length, adaptiveUsedRaw);
  const evidence = pickTop(
    evidenceRows.map((row) => ({
      id: row.id,
      type: row.type,
      source: row.source === 'events' ? (row.source || 'events') : 'graph',
      timestamp: row.timestamp || null,
      score: row.score || 0
    })),
    20
  ).map(makeEvidenceEntry);

  return {
    retrievalPath,
    layerStats,
    skippedLayers,
    evidence,
    confidence,
    contextText: summarizeEvidenceText(evidenceRows),
    sourceMix: {
      core: layerStats.core,
      insights: layerStats.insights,
      episodes: layerStats.episodes,
      raw: layerStats.raw,
      artifacts: layerStats.artifacts
    }
  };
}

module.exports = {
  retrieveCoreFirstContext
};
