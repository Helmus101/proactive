/**
 * Relationship Strength Scoring Algorithm
 * 
 * Scores contact relationships based on:
 * - Frequency: interaction count in last 90 days
 * - Recency: days since last contact (decay over time)
 * - Depth: conversation depth (topical vs superficial)
 * - Tenure: how long you've known them
 */

function scoreFrequency(interactionCount, windowDays = 90) {
  // Decay: 0 interactions = 0, 1+ per week = 1.0, 1+ per month = 0.7
  if (interactionCount === 0) return 0;
  if (interactionCount >= (windowDays / 7)) return 1.0; // At least weekly
  if (interactionCount >= (windowDays / 30)) return 0.7; // At least monthly
  if (interactionCount >= 1) return 0.4; // At least once in window
  return 0;
}

function scoreRecency(lastContactMs, nowMs) {
  // Normalize: days since last contact
  const daysSince = Math.max(0, (nowMs - lastContactMs) / (24 * 60 * 60 * 1000));
  
  // Decay curve: 0 days = 1.0, 7 days = 0.8, 30 days = 0.5, 90+ days = 0
  if (daysSince <= 0) return 1.0;
  if (daysSince <= 7) return Math.max(0.8, 1.0 - (daysSince / 35)); // 35 day halflife
  if (daysSince <= 30) return Math.max(0.5, 1.0 - (daysSince / 60));
  if (daysSince <= 90) return Math.max(0.1, 1.0 - (daysSince / 200));
  return 0;
}

function scoreDepth(conversationTopics = [], averageMessageLength = 0, isDeepConversation = false) {
  // Depth based on conversation properties
  let depthScore = 0;
  
  // Topic diversity: more topics = deeper conversations
  if (conversationTopics && conversationTopics.length > 0) {
    depthScore += Math.min(1.0, conversationTopics.length / 5) * 0.4;
  }
  
  // Message length: longer messages suggest engagement
  if (averageMessageLength > 200) depthScore += 0.3;
  else if (averageMessageLength > 50) depthScore += 0.15;
  
  // Deep conversation flag (from LLM analysis)
  if (isDeepConversation) depthScore += 0.3;
  
  return Math.min(1.0, depthScore);
}

function scoreTenure(firstContactMs, nowMs) {
  // How long you've known someone
  const daysSince = Math.max(0, (nowMs - firstContactMs) / (24 * 60 * 60 * 1000));
  
  // Longer tenure = slightly higher score (stability bonus)
  if (daysSince <= 0) return 0.3; // Just met
  if (daysSince <= 30) return 0.5;
  if (daysSince <= 180) return 0.7;
  if (daysSince <= 365) return 0.8;
  return 0.9; // > 1 year
}

function calculateRelationshipStrength(contact = {}, nowMs = Date.now()) {
  const {
    interaction_count = 0,
    last_contact_at = null,
    first_contact_at = null,
    conversation_topics = [],
    average_message_length = 0,
    is_deep_conversation = false
  } = contact;

  const lastContact = last_contact_at ? new Date(last_contact_at).getTime() : nowMs - (365 * 24 * 60 * 60 * 1000);
  const firstContact = first_contact_at ? new Date(first_contact_at).getTime() : lastContact;

  // Calculate component scores
  const frequencyScore = scoreFrequency(interaction_count, 90);
  const recencyScore = scoreRecency(lastContact, nowMs);
  const depthScore = scoreDepth(conversation_topics, average_message_length, is_deep_conversation);
  const tenureScore = scoreTenure(firstContact, nowMs);

  // Weighted combination: frequency 30%, recency 30%, depth 20%, tenure 20%
  const relationshipStrength = 
    (frequencyScore * 0.3) +
    (recencyScore * 0.3) +
    (depthScore * 0.2) +
    (tenureScore * 0.2);

  const daysSinceContact = Math.max(0, (nowMs - lastContact) / (24 * 60 * 60 * 1000));
  const isWeakTie = relationshipStrength < 0.4 && daysSinceContact > 30;
  const isOverdue = daysSinceContact > 14 && relationshipStrength > 0.5; // Strong relationship but overdue

  return {
    strength: Math.max(0, Math.min(1.0, relationshipStrength)),
    frequency_score: frequencyScore,
    recency_score: recencyScore,
    depth_score: depthScore,
    tenure_score: tenureScore,
    days_since_contact: daysSinceContact,
    is_weak_tie: isWeakTie,
    is_overdue_followup: isOverdue,
    recommendation: deriveRecommendation(relationshipStrength, daysSinceContact, interaction_count)
  };
}

function deriveRecommendation(strength, daysSince, interactionCount) {
  // Suggest action based on relationship state
  if (daysSince < 1) return 'maintain';
  if (daysSince > 365 && strength > 0.5) return 'reconnect_important';
  if (daysSince > 90 && strength < 0.4) return 'reconnect_weak_tie';
  if (daysSince > 30 && strength > 0.6) return 'followup_overdue';
  if (daysSince > 14 && interactionCount > 5) return 'maintain_momentum';
  if (strength < 0.2 && daysSince > 60) return 'dormant';
  return 'normal';
}

module.exports = {
  scoreFrequency,
  scoreRecency,
  scoreDepth,
  scoreTenure,
  calculateRelationshipStrength,
  deriveRecommendation
};