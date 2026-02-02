# Coverage Analysis Report: TCJA

**Generated:** 2026-01-28 16:30:47

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 2320 |
| In OpenAlex | 1217 |
| In Semantic Scholar | 1257 |
| In NBER | 48 |
| In All Three Sources | 1 |
| In Exactly One Source | 2119 |
| In Exactly Two Sources | 200 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 1023
- Papers in Semantic Scholar only: 1063
- Papers in both: 194

**Papers in Openalex only:**
- 66.7% have abstracts
- 86.4% have DOIs
- Median citations: 6.0
- Top venues: SSRN Electronic Journal, National Tax Journal, The Lancet

**Papers in Semantic Scholar only:**
- 28.9% have abstracts
- 0.0% have DOIs
- Median citations: 4.0
- Top venues: Social Science Research Network, International Tax and Public Finance, US-Steuerreform - Der Tax Cuts and Jobs Act 2017

### Openalex vs Nber

- Papers in Openalex only: 1216
- Papers in Nber only: 47
- Papers in both: 1

**Papers in Openalex only:**
- 69.3% have abstracts
- 72.7% have DOIs
- Median citations: 5.0
- Top venues: SSRN Electronic Journal, National Tax Journal, The Lancet

**Papers in Nber only:**
- 27.7% have abstracts
- 0.0% have DOIs
- Median citations: 10.5
- Top venues: Social Science Research Network, Tax Policy and the Economy

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 1249
- Papers in Nber only: 40
- Papers in both: 8

**Papers in Semantic Scholar only:**
- 36.9% have abstracts
- 0.0% have DOIs
- Median citations: 3.0
- Top venues: Social Science Research Network, SSRN Electronic Journal, International Tax and Public Finance

**Papers in Nber only:**
- 15.0% have abstracts
- 0.0% have DOIs
- Median citations: nan

## Hypotheses: Why Coverage Differs

### Hypothesis 1: NBER source only indexes NBER working papers

**Evidence:**
- nber_only_papers: 40
- total_nber_papers: 48
- top_venues_in_nber_only: {}

**Conclusion:** NBER has 40 papers not in other sources, likely because NBER specifically scrapes NBER working papers.

### Hypothesis 2: OpenAlex has more complete DOI metadata than Semantic Scholar

**Evidence:**
- openalex_only_papers: 1023
- semantic_scholar_only_papers: 1063
- openalex_only_doi_pct: 86.4
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 86.4% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 220, 'National Tax Journal': 23, 'The Lancet': 17, 'The Contemporary Tax Journal': 16, 'FOXBusiness': 12}
- top_venues_in_semantic_scholar_only: {'Social Science Research Network': 81, 'International Tax and Public Finance': 26, 'US-Steuerreform - Der Tax Cuts and Jobs Act 2017': 24, 'National tax journal': 23, 'Florida Tax Review': 8}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2020.0
- semantic_scholar_only_median_year: 2019.0
- openalex_only_year_range: 2017-2026
- semantic_scholar_only_year_range: 2017-2026

**Conclusion:** OpenAlex-only papers have median year 2020.0, Semantic Scholar-only have median year 2019.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: {'Tax Cuts and Jobs Act': {'openalex': 789, 'semantic_scholar': 723, 'nber': 32}, 'TCJA': {'openalex': 215, 'semantic_scholar': 84, 'nber': 6}, '2017 tax reform': {'openalex': 154, 'semantic_scholar': 336, 'nber': 8}, 'Trump tax cuts': {'openalex': 89, 'semantic_scholar': 144, 'nber': 0}, 'Tax Reform 2017': {'openalex': 1, 'semantic_scholar': 1, 'nber': 2}}

**Conclusion:** Search API differences mean the same query returns different results across sources.
