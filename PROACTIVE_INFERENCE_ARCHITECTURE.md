# AI-First Proactive Inference Architecture

This document captures the operating model for proactive suggestions in Weave.

## Core Principle

Treat the memory graph as a **prompt generator**, not only a database.  
Suggestions must be AI-generated context bundles:

1. Trigger
2. Evidence (receipts)
3. Insight (reasoned synthesis)
4. Action (direct, executable next step)

## Memory Layers

- Raw memory: original artifacts (emails, calendar, messages, captures, notes).
- Episode: compact events with time, actors, and outcome/context.
- Semantic insight: extracted facts and inferred meaning.
- Task hypothesis: candidate proactive actions (follow-up, unfinished task, prep).
- Confidence/rationale: scoring and explanation for user trust and suppression.

## Pulse & Hop Loop

Recurring loop:

1. Choose high-weight seed nodes (time-sensitive, unresolved, follow-up signals).
2. Run 2-hop context expansion (episodic + semantic + insight).
3. Run short 24h recall hedge from raw events to enrich/contradict context.
4. Ask LLM to produce reasoning-first suggestions with citations.
5. Score/rank and suppress low-confidence or low-specificity items.

## Suggestion Quality Contract

A suggestion is valid only if it is:

- AI-authored from live memory context.
- Specific and actionable as a to-do item.
- Grounded in receipts (source + evidence text + timestamp).
- Aligned across title, summary, and action.
- Anchored to time/context ("why now").

Reject suggestions that are:

- Template-like.
- Generic ("take next step", "keep momentum", etc.).
- Missing concrete unfinished obligation/opportunity.
- Not supported by evidence.

## Minimum Output Shape

Each suggestion should provide:

- `display`: `headline`, `summary`, `insight`
- `epistemic_trace[]`: `node_id`, `source`, `text`, `timestamp`
- `suggested_actions[]`: executable labels + payload
- `reason`: human-readable receipt-based rationale

## Product Behavior

- Show few high-confidence suggestions, not many.
- Allow no-suggestion outcome when confidence is low.
- Prefer stable feed quality over noisy generation.
- Use retry-once generation; if still invalid, keep previous valid suggestions.

## MVP Priorities

Start with high-value categories:

- Birthdays/anniversaries
- Unreplied or unresolved threads
- Dormant but important contacts
- Unfinished study/work loops

Then expand:

- Assignment and deadline risk
- Study spaced-repetition opportunities
- Routine and relationship maintenance opportunities

