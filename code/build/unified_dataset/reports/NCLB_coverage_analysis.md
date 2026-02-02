# Coverage Analysis Report: NCLB

**Generated:** 2026-01-28 16:30:54

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 6461 |
| In OpenAlex | 3686 |
| In Semantic Scholar | 3198 |
| In NBER | 37 |
| In All Three Sources | 2 |
| In Exactly One Source | 6003 |
| In Exactly Two Sources | 456 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 3238
- Papers in Semantic Scholar only: 2750
- Papers in both: 448

**Papers in Openalex only:**
- 66.2% have abstracts
- 71.6% have DOIs
- Median citations: 13.0
- Top venues: PsycEXTRA Dataset, Phi Delta Kappan, Education Policy Analysis Archives

**Papers in Semantic Scholar only:**
- 7.4% have abstracts
- 0.0% have DOIs
- Median citations: 27.0
- Top venues: Teachers College Record, American Journal of Education, Education Finance and Policy

### Openalex vs Nber

- Papers in Openalex only: 3683
- Papers in Nber only: 34
- Papers in both: 3

**Papers in Openalex only:**
- 66.7% have abstracts
- 62.9% have DOIs
- Median citations: 14.0
- Top venues: PsycEXTRA Dataset, Phi Delta Kappan, Education week

**Papers in Nber only:**
- 47.1% have abstracts
- 0.0% have DOIs
- Median citations: 42.0
- Top venues: Education Finance and Policy, Educational Evaluation and Policy Analysis

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 3187
- Papers in Nber only: 26
- Papers in both: 11

**Papers in Semantic Scholar only:**
- 15.9% have abstracts
- 0.0% have DOIs
- Median citations: 26.0
- Top venues: Teachers College Record, American Journal of Education, Education next

**Papers in Nber only:**
- 30.8% have abstracts
- 0.0% have DOIs
- Median citations: 10.0
- Top venues: SSRN Electronic Journal

## Hypotheses: Why Coverage Differs

### Hypothesis 1: NBER source only indexes NBER working papers

**Evidence:**
- nber_only_papers: 25
- total_nber_papers: 37
- top_venues_in_nber_only: {}

**Conclusion:** NBER has 25 papers not in other sources, likely because NBER specifically scrapes NBER working papers.

### Hypothesis 2: OpenAlex has more complete DOI metadata than Semantic Scholar

**Evidence:**
- openalex_only_papers: 3238
- semantic_scholar_only_papers: 2750
- openalex_only_doi_pct: 71.6
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 71.6% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'PsycEXTRA Dataset': 195, 'Phi Delta Kappan': 91, 'Education Policy Analysis Archives': 46, 'Education week': 40, 'Educational Policy': 38}
- top_venues_in_semantic_scholar_only: {'Teachers College Record': 24, 'American Journal of Education': 17, 'Education Finance and Policy': 10, 'The Elementary school journal': 8, 'American Annals of the Deaf': 7}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2008.0
- semantic_scholar_only_median_year: 2008.0
- openalex_only_year_range: 2001-2025
- semantic_scholar_only_year_range: 2001-2025

**Conclusion:** OpenAlex-only papers have median year 2008.0, Semantic Scholar-only have median year 2008.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: {'No Child Left Behind': {'openalex': 1433, 'semantic_scholar': 981, 'nber': 28}, 'NCLB': {'openalex': 1148, 'semantic_scholar': 673, 'nber': 6}, 'No Child Left Behind Act': {'openalex': 752, 'semantic_scholar': 381, 'nber': 2}, 'education reform 2001': {'openalex': 207, 'semantic_scholar': 812, 'nber': 5}, 'ESEA reauthorization 2001': {'openalex': 283, 'semantic_scholar': 490, 'nber': 1}}

**Conclusion:** Search API differences mean the same query returns different results across sources.
