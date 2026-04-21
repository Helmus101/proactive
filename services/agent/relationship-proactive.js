/**
 * Relationship Proactive Instance (RPI)
 * Monitors person nodes for relationship decay and surfaces proactive nudges
 * Uses hard metadata filters to detect cooling relationships
 */

const db = require('../db');
const { buildHybridGraphRetrieval } = require('./hybrid-graph-retrieval');

const DECAY_THRESHOLD_DAYS = 7;
const DECAY_SENTIMENT_THRESHOLD = -0.3;
const HIGH_IMPORTANCE_MIN = 8;

/**
 * Detect person nodes where relationship is decaying
 * Returns nodes that are: (a) high importance, (b) recent negative sentiment, (c) no recent interaction
 */
async function detectDecayingRelationships() {
  try {
    const now = new Date();
    const decayThresholdDate = new Date(now.getTime() - (DECAY_THRESHOLD_DAYS * 24 * 60 * 60 * 1000)).toISOString();
    
    // Find person nodes with declining sentiment
    const decayingPersons = await db.allQuery(`
      SELECT 
        mn.id,
        mn.title,
        mn.layer,
        mn.subtype,
        mn.importance,
        mn.connection_count,
        mn.last_reheated,
        mn.summary,
        mn.metadata,
        (
          SELECT GROUP_CONCAT(e.sentiment_score, ',')
          FROM events e
          WHERE e.id IN (SELECT json_each.value FROM json_each(mn.source_refs))
          AND e.occurred_at > datetime('now', '-14 days')
          LIMIT 5
        ) as recent_sentiments,
        (
          SELECT MAX(e.occurred_at)
          FROM events e
          WHERE e.id IN (SELECT json_each.value FROM json_each(mn.source_refs))
        ) as latest_event_at
      FROM memory_nodes mn
      WHERE mn.subtype = 'person'
      AND mn.importance >= ?
      AND mn.status IN ('active', 'decaying')
      ORDER BY mn.importance DESC, mn.last_reheated DESC
    `, [HIGH_IMPORTANCE_MIN]);
    
    const riskyRelationships = [];
    
    for (const person of decayingPersons || []) {
      const metadataObj = typeof person.metadata === 'string'
        ? (() => { try { return JSON.parse(person.metadata); } catch (_) { return {}; } })()
        : (person.metadata || {});
      
      const lastReheated = person.last_reheated || person.latest_event_at;
      const daysSinceInteraction = lastReheated
        ? Math.floor((now.getTime() - new Date(lastReheated).getTime()) / (24 * 60 * 60 * 1000))
        : DECAY_THRESHOLD_DAYS + 1;
      
      const recentSentiments = (person.recent_sentiments || '').split(',').map(s => parseFloat(s)).filter(s => !isNaN(s));
      const avgRecentSentiment = recentSentiments.length > 0
        ? recentSentiments.reduce((a, b) => a + b, 0) / recentSentiments.length
        : 0;
      
      // Decay detection: low/negative sentiment OR inactive for too long
      const isDecaying = (daysSinceInteraction > DECAY_THRESHOLD_DAYS) || (avgRecentSentiment < DECAY_SENTIMENT_THRESHOLD);
      
      if (isDecaying) {
        riskyRelationships.push({
          person_node_id: person.id,
          person_name: person.title,
          importance: person.importance,
          connection_count: person.connection_count,
          days_since_interaction: daysSinceInteraction,
          recent_sentiment_avg: Number(avgRecentSentiment.toFixed(2)),
          status_reason: daysSinceInteraction > DECAY_THRESHOLD_DAYS ? 'no_recent_interaction' : 'negative_sentiment',
          last_interaction_at: lastReheated,
          summary: person.summary
        });
      }
    }
    
    return riskyRelationships;
  } catch (e) {
    console.error('[RPI] Decay detection failed:', e.message);
    return [];
  }
}

/**
 * Generate proactive nudge for a decaying relationship
 * Retrieves recent context about the person and their current blockers
 */
async function generateProactiveNudge(decayingRelationship, apiKey) {
  try {
    const personNodeId = decayingRelationship.person_node_id;
    const personName = decayingRelationship.person_name;
    
    // Query for recent interactions with this person
    // Use hard metadata filter: {entity_refs: [personName]}
    const recentContext = await buildHybridGraphRetrieval({
      query: `Recent interactions with ${personName}`,
      options: {
        mode: 'chat',
        metadata_filters: {
          // Filter for events mentioning this person
          // This requires events table entity_refs support
        },
        date_range: {
          start: new Date(Date.now() - (30 * 24 * 60 * 60 * 1000)).toISOString(),
          end: new Date().toISOString()
        }
      },
      seedLimit: 3,
      hopLimit: 2,
      recursionDepth: 0,
      apiKey
    }).catch(() => null);
    
    if (!recentContext) {
      return null;
    }
    
    // Build nudge based on decay reason
    let nudgeTitle = '';
    let nudgeAction = '';
    
    if (decayingRelationship.status_reason === 'no_recent_interaction') {
      nudgeTitle = `Reconnect with ${personName}`;
      nudgeAction = `You haven't interacted with ${personName} in ${decayingRelationship.days_since_interaction} days. Consider reaching out.`;
    } else {
      nudgeTitle = `Resolution needed with ${personName}`;
      nudgeAction = `Recent interactions with ${personName} had negative sentiment (${decayingRelationship.recent_sentiment_avg}). Consider addressing any concerns.`;
    }
    
    return {
      type: 'relationship_decay',
      priority: 'high',
      person_node_id: personNodeId,
      person_name: personName,
      title: nudgeTitle,
      action: nudgeAction,
      context_summary: recentContext?.contextSummary || 'Recent interactions recorded',
      importance_score: decayingRelationship.importance,
      recommendation: `Send a message to ${personName} or schedule a 1-on-1.`,
      created_at: new Date().toISOString()
    };
  } catch (e) {
    console.error('[RPI] Nudge generation failed:', e.message);
    return null;
  }
}

/**
 * Main RPI monitoring loop
 * Runs periodically to detect relationship decay and emit proactive nudges
 */
async function monitorRelationshipHealth(apiKey = null) {
  try {
    console.log('[RPI] Starting relationship health monitoring');
    
    const decayingRelationships = await detectDecayingRelationships();
    
    if (!decayingRelationships.length) {
      console.log('[RPI] No decaying relationships detected');
      return { detected: 0, nudges: [] };
    }
    
    console.log(`[RPI] Detected ${decayingRelationships.length} decaying relationships`);
    
    // Generate nudges for top 3 at-risk relationships
    const nudges = [];
    for (const decayingRel of decayingRelationships.slice(0, 3)) {
      const nudge = await generateProactiveNudge(decayingRel, apiKey);
      if (nudge) {
        nudges.push(nudge);
      }
    }
    
    console.log(`[RPI] Generated ${nudges.length} proactive nudges`);
    
    // Update person node status to 'decaying' for detected relationships
    for (const rel of decayingRelationships) {
      try {
        await db.runQuery(
          'UPDATE memory_nodes SET status = ? WHERE id = ?',
          ['decaying', rel.person_node_id]
        );
      } catch (e) {
        console.warn(`[RPI] Failed to update status for ${rel.person_node_id}:`, e.message);
      }
    }
    
    return {
      detected: decayingRelationships.length,
      nudges,
      relationships: decayingRelationships
    };
  } catch (e) {
    console.error('[RPI] Monitoring failed:', e.message);
    return { detected: 0, nudges: [], error: e.message };
  }
}

module.exports = {
  detectDecayingRelationships,
  generateProactiveNudge,
  monitorRelationshipHealth,
  DECAY_THRESHOLD_DAYS,
  DECAY_SENTIMENT_THRESHOLD,
  HIGH_IMPORTANCE_MIN
};
