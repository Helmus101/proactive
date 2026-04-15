// Lightweight extractor regexes and helper functions for open-loop detection
module.exports = {
  // Phrases that often indicate a promise / action to take
  promisePatterns: [
    /i will (?:send|attach|share)\b/i,
    /i'll (?:send|attach|share)\b/i,
    /could you (?:share|send)\b/i,
    /let's schedule\b/i,
    /can we meet\b/i,
    /i can send by\b/i,
    /i will follow up/i,
    /please (?:advise|confirm)\b/i,
    /can you confirm\b/i,
    /i'll (?:get back to you|follow up)/i,
    /please (?:send|share) the/i
  ],

  // Deadlines / time hints
  deadlinePatterns: [
    /by\s+(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?|sat(urday)?|sun(day)?)/i,
    /by\s+\b(next\s+)?\d{1,2}(?:st|nd|rd|th)?\b/i,
    /by\s+tomorrow\b/i,
    /deadline\b/i,
    /due\b/i
  ],

  // Phrases indicating awaiting reply or follow-up
  awaitingReplyPatterns: [
    /awaiting your reply/i,
    /have you had a chance/i,
    /just following up/i,
    /following up on/i,
    /any update/i,
    /did you see my/i
  ],

  // Phrases indicating scheduling requests
  schedulingPatterns: [
    /(?:can we|could we|let's) (?:meet|schedule)/i,
    /are you available/i,
    /what time fits/i,
    /available (?:tomorrow|today|next week)/i
  ],

  // Job / application signals
  jobPagePatterns: [
    /careers|jobs|apply|positions|openings/i
  ],

  // Heuristics to detect recruiter/professional emails
  recruiterPatterns: [
    /recruiter|talent|hiring|careers@|job opportunity|position/i
  ],

  // Utility: test a text against a list of patterns
  matchesAny(text, patterns) {
    if (!text) return false;
    for (const p of patterns) {
      if (p.test(text)) return true;
    }
    return false;
  }
};
