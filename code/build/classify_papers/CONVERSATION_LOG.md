# Issue #22: Flag Papers by Policy & Economic Outcome

## Conversation Log

This file documents the design decisions and iterations for classifying papers
as evaluating the causal effect of a policy on an economic outcome using the
Gemini API.

---

### 2026-02-16 — Initial Design Session

**Goal:** Build a Python script that calls the Gemini API to classify each paper
in our unified dataset as TRUE/FALSE for: "Does this paper empirically evaluate
the effect of {policy} on an economic outcome?"

**Approach decisions:**

1. **Model choice:** Gemini 3 Flash (`gemini-3-flash-preview`) for cost
   efficiency ($0.50/1M input, $3/1M output). Gemini 3 Pro available as
   fallback if quality is insufficient.

2. **Mode:** Real-time (synchronous) API calls first. Batch mode only after
   pilot validation.

3. **Pilot strategy:** Sample ~20 papers per policy (TCJA, ACA, NCLB) that have
   abstracts. Output results to CSV for manual inspection.

4. **Prompt design (v1):**

   - **System prompt:**
     ```
     You are a research paper classifier. Given a paper's title and abstract,
     determine whether the paper empirically evaluates the effect of
     {policy_name} ({policy_abbreviation}) on an economic outcome.

     Return TRUE if the paper uses data or empirical methods to estimate the
     effect of {policy_name} on a measurable economic outcome (e.g., employment,
     wages, income, investment, prices, tax revenue, insurance coverage,
     healthcare costs, educational attainment, housing values, firm performance,
     or other economic variables).

     Return FALSE if:
     - The paper is not actually about {policy_name} (e.g., acronym collision)
     - The paper discusses the policy without empirically evaluating its effects
     - The paper is purely theoretical, legal analysis, or political commentary
     - The paper does not measure an economic outcome

     Respond with exactly one word: TRUE or FALSE
     ```

   - **User prompt:**
     ```
     Title: {title}
     Abstract: {abstract}
     ```

5. **Parameters:** temperature=0.0 for maximum determinism, thinking_level="minimal"
   to minimize latency/cost since this is a simple classification task.

6. **Scope for pilot:** Only papers with non-empty abstracts. Papers without
   abstracts will be handled separately in a future iteration.

---

### Prompt Iteration Notes

(Will be updated as we inspect pilot results and refine the prompt.)

| Version | Change | Reason |
|---------|--------|--------|
| v1 | Initial prompt | Starting point |

