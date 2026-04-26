let RELATIONSHIP_FEATURE_ENABLED = false;
let deps = {
  debouncedStoreSet: null
};

function init(options) {
  Object.assign(deps, options);
  if (options.RELATIONSHIP_FEATURE_ENABLED !== undefined) {
    RELATIONSHIP_FEATURE_ENABLED = options.RELATIONSHIP_FEATURE_ENABLED;
  }
}

function looksRelationshipSuggestion(item = {}) {
  const category = String(item.category || item.type || '').toLowerCase();
  const opportunityType = String(item.opportunity_type || '').toLowerCase();
  const displayPerson = String(item.display?.person || item.display?.target || '').trim();
  const haystack = [
    item.title,
    item.reason,
    item.description,
    item.trigger_summary,
    item.display?.headline,
    item.display?.summary,
    item.primary_action?.label,
    opportunityType,
    category
  ].map((value) => String(value || '').toLowerCase()).join(' ');

  if (displayPerson) return true;
  if (['followup', 'relationship', 'relationship_intelligence', 'social'].includes(category)) return true;
  if (/(reconnect|follow up|intro|introduction|meeting prep|brief|stakeholder|warm|cold|relationship|network|reach out|investor|client|partner|contact)/.test(haystack)) return true;
  return ['reconnect_risk', 'timely_follow_up', 'intro_opportunity', 'meeting_prep', 'value_add_share', 'emerging_connection'].includes(opportunityType);
}

function sanitizeRadarStateForFeatures(radarState = {}) {
  if (RELATIONSHIP_FEATURE_ENABLED) return radarState;
  const notRelationship = (item) => String(item?.signal_type || '').toLowerCase() !== 'relationship' && !looksRelationshipSuggestion(item);
  const centralSignals = Array.isArray(radarState.centralSignals) ? radarState.centralSignals.filter(notRelationship) : [];
  const todoSignals = Array.isArray(radarState.todoSignals) ? radarState.todoSignals.filter(notRelationship) : [];
  return {
    ...radarState,
    allSignals: Array.isArray(radarState.allSignals) ? radarState.allSignals.filter(notRelationship) : [...centralSignals, ...todoSignals],
    centralSignals,
    relationshipSignals: [],
    todoSignals,
    sections: radarState.sections || {}
  };
}

function persistRadarState(radarState = {}) {
  const incomingAllSignals = Array.isArray(radarState.allSignals) ? radarState.allSignals : [];
  const allSignals = RELATIONSHIP_FEATURE_ENABLED
    ? incomingAllSignals
    : incomingAllSignals.filter((item) => String(item?.signal_type || '').toLowerCase() !== 'relationship' && !looksRelationshipSuggestion(item));
  const clean = sanitizeRadarStateForFeatures({
    generated_at: radarState.generated_at || new Date().toISOString(),
    allSignals,
    centralSignals: Array.isArray(radarState.centralSignals) ? radarState.centralSignals : [],
    relationshipSignals: Array.isArray(radarState.relationshipSignals) ? radarState.relationshipSignals : [],
    todoSignals: Array.isArray(radarState.todoSignals) ? radarState.todoSignals : [],
    sections: radarState.sections || {}
  });
  
  if (deps.debouncedStoreSet) {
    deps.debouncedStoreSet('radarState', clean);
    deps.debouncedStoreSet('suggestions', clean.allSignals.filter((item) => !item?.completed));
  }
  return clean;
}

module.exports = {
  init,
  persistRadarState,
  sanitizeRadarStateForFeatures,
  looksRelationshipSuggestion
};
