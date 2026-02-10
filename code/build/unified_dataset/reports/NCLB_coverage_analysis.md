# Coverage Analysis Report: NCLB

**Generated:** 2026-02-09 02:45:59

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 21838 |
| In OpenAlex | 19162 |
| In Semantic Scholar | 3197 |
| In NBER | 37 |
| In All Three Sources | 4 |
| In Exactly One Source | 21284 |
| In Exactly Two Sources | 550 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 18618
- Papers in Semantic Scholar only: 2653
- Papers in both: 544

**Papers in Openalex only:**
- 38.0% have abstracts
- 65.1% have DOIs
- Median citations: 16.0
- Top venues: PsycEXTRA Dataset, Phi Delta Kappan, SSRN Electronic Journal

**Papers in Semantic Scholar only:**
- 7.4% have abstracts
- 0.0% have DOIs
- Median citations: 28.0
- Top venues: Teachers College Record, American Journal of Education, Education Finance and Policy

### Openalex vs Nber

- Papers in Openalex only: 19155
- Papers in Nber only: 30
- Papers in both: 7

**Papers in Openalex only:**
- 38.9% have abstracts
- 63.3% have DOIs
- Median citations: 16.0
- Top venues: PsycEXTRA Dataset, Phi Delta Kappan, SSRN Electronic Journal

**Papers in Nber only:**
- 40.0% have abstracts
- 0.0% have DOIs
- Median citations: 33.0
- Top venues: Education Finance and Policy, Educational Evaluation and Policy Analysis

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 3186
- Papers in Nber only: 26
- Papers in both: 11

**Papers in Semantic Scholar only:**
- 18.3% have abstracts
- 0.0% have DOIs
- Median citations: 26.0
- Top venues: Teachers College Record, Education next, American Journal of Education

**Papers in Nber only:**
- 30.8% have abstracts
- 0.0% have DOIs
- Median citations: 10.0
- Top venues: National Bureau of Economic Research, RePEc: Research Papers in Economics, SSRN Electronic Journal

## Hypotheses: Why Coverage Differs

### Hypothesis 1: NBER source only indexes NBER working papers

**Evidence:**
- nber_only_papers: 23
- total_nber_papers: 37
- top_venues_in_nber_only: {}

**Conclusion:** NBER has 23 papers not in other sources, likely because NBER specifically scrapes NBER working papers.

### Hypothesis 2: OpenAlex has more complete DOI metadata than Semantic Scholar

**Evidence:**
- openalex_only_papers: 18618
- semantic_scholar_only_papers: 2653
- openalex_only_doi_pct: 65.1
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 65.1% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'PsycEXTRA Dataset': 588, 'Phi Delta Kappan': 319, 'SSRN Electronic Journal': 198, 'The Lancet': 154, 'Journal of General Internal Medicine': 152}
- top_venues_in_semantic_scholar_only: {'Teachers College Record': 24, 'American Journal of Education': 17, 'Education Finance and Policy': 9, 'The Elementary school journal': 8, 'American Annals of the Deaf': 7}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2011.0
- semantic_scholar_only_median_year: 2008.0
- openalex_only_year_range: 2001-2026
- semantic_scholar_only_year_range: 2001-2025

**Conclusion:** OpenAlex-only papers have median year 2011.0, Semantic Scholar-only have median year 2008.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: (showing top 5)
  - None: {'openalex': 18615, 'semantic_scholar': 0, 'nber': 0}
  - No Child Left Behind: {'openalex': 165, 'semantic_scholar': 900, 'nber': 28}
  - NCLB: {'openalex': 200, 'semantic_scholar': 660, 'nber': 6}
  - No Child Left Behind Act: {'openalex': 83, 'semantic_scholar': 351, 'nber': 1}
  - ESEA reauthorization 2001: {'openalex': 81, 'semantic_scholar': 488, 'nber': 1}

**Conclusion:** Search API differences mean the same query returns different results across sources.
