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

### 2026-02-24 — Econ-Unified Dataset & Prompt Variants

**Context:** Issue #24 produced EconLit (175 papers) and APIs (4,207 papers)
datasets. Issue #22 continues with unification and classification.

**Changes to classify_papers_main.py:**

1. **v2 prompt variant** (`v2_no_examples`): Identical to v1 but removes the
   parenthetical examples list from the TRUE criterion. Tests whether
   explicit outcome examples bias the classifier.

2. **New parameters**: `--prompt_version`, `--run_label`, `--input_dir`,
   `--all`, `--merge` — enables reading from the econ-unified dataset and
   running full classification with checkpoint/resume.

3. **Econ-unified data source**: When `--input_dir` points to
   `scrape_econ/output/`, reads `TCJA_econ_unified.parquet` and uses
   `econ_unified_id` as the paper identifier.

**Changes to Pipeline B (scrape_econ_apis_main.py):**

4. **Enhanced abstract recovery**: Added 6 new sources beyond CrossRef + SS:
   Europe PMC, OpenAlex re-fetch, OA URL scraping, PDF extraction, NBER
   website scraping, SS title search, and Selenium (for JS-rendered pages).
   Sources ordered by success rate based on complement_abstracts TCJA data.

5. **NBER non-paper filter**: Drops conference listings, meeting agendas,
   book front/back matter from NBER search results (~266 of 322 NBER
   papers without abstracts are non-paper content).

6. **Recovery now processes ALL papers** without abstracts, not just those
   with DOIs. Previously ~1,300 papers without DOIs were skipped entirely.

**Pilot results (6 runs on 15 papers):**

(To be filled after running pilot classification.)

---

### Prompt Iteration Notes

| Version | Change | Reason |
|---------|--------|--------|
| v1 | Initial prompt with outcome examples | Starting point |
| v2 | Removed parenthetical examples | Test prompt sensitivity |

