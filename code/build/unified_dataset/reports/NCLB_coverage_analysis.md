# Coverage Analysis Report: NCLB

**Generated:** 2026-02-19 20:27:32

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 18520 |
| In OpenAlex | 15844 |
| In Semantic Scholar | 3197 |
| In NBER | 37 |
| In All Three Sources | 4 |
| In Exactly One Source | 17966 |
| In Exactly Two Sources | 550 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 15300
- Papers in Semantic Scholar only: 2653
- Papers in both: 544

**Papers in Openalex only:**
- 47.2% have abstracts
- 57.6% have DOIs
- Median citations: 10.0
- Top venues: SSRN Electronic Journal, PsycEXTRA Dataset, Phi Delta Kappan

**Papers in Semantic Scholar only:**
- 7.4% have abstracts
- 0.0% have DOIs
- Median citations: 28.0
- Top venues: Teachers College Record, American Journal of Education, Education Finance and Policy

### Openalex vs Nber

- Papers in Openalex only: 15837
- Papers in Nber only: 30
- Papers in both: 7

**Papers in Openalex only:**
- 48.1% have abstracts
- 55.6% have DOIs
- Median citations: 11.0
- Top venues: SSRN Electronic Journal, PsycEXTRA Dataset, Phi Delta Kappan

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
- openalex_only_papers: 15300
- semantic_scholar_only_papers: 2653
- openalex_only_doi_pct: 57.6
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 57.6% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 794, 'PsycEXTRA Dataset': 588, 'Phi Delta Kappan': 319, 'CSUN ScholarWorks (California State University, Northridge)': 107, 'Digital Education Resource Archive (University College London)': 103}
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
  - NCLB: {'openalex': 5623, 'semantic_scholar': 323, 'nber': 4}
  - No Child Left Behind: {'openalex': 10140, 'semantic_scholar': 467, 'nber': 7}
  - No Child Left Behind Act: {'openalex': 7582, 'semantic_scholar': 350, 'nber': 5}
  - ESEA reauthorization 2001: {'openalex': 490, 'semantic_scholar': 45, 'nber': 0}
  - education reform 2001: {'openalex': 2456, 'semantic_scholar': 69, 'nber': 2}

**Conclusion:** Search API differences mean the same query returns different results across sources.
