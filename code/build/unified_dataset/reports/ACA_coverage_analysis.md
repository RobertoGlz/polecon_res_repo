# Coverage Analysis Report: ACA

**Generated:** 2026-02-19 20:27:16

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 54356 |
| In OpenAlex | 51361 |
| In Semantic Scholar | 3551 |
| In NBER | 135 |
| In All Three Sources | 11 |
| In Exactly One Source | 53676 |
| In Exactly Two Sources | 669 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 50734
- Papers in Semantic Scholar only: 2924
- Papers in both: 627

**Papers in Openalex only:**
- 53.3% have abstracts
- 70.7% have DOIs
- Median citations: 5.0
- Top venues: SSRN Electronic Journal, Forefront Group, PubMed

**Papers in Semantic Scholar only:**
- 21.4% have abstracts
- 0.0% have DOIs
- Median citations: 24.0
- Top venues: Health Affairs, Journal of the American Medical Association (JAMA), New England Journal of Medicine

### Openalex vs Nber

- Papers in Openalex only: 51320
- Papers in Nber only: 94
- Papers in both: 41

**Papers in Openalex only:**
- 53.6% have abstracts
- 69.9% have DOIs
- Median citations: 5.0
- Top venues: SSRN Electronic Journal, PubMed, Forefront Group

**Papers in Nber only:**
- 77.7% have abstracts
- 0.0% have DOIs
- Median citations: 88.0
- Top venues: Social Science Research Network, Journal of policy analysis and management, Journal of Health Economics

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 3517
- Papers in Nber only: 101
- Papers in both: 34

**Papers in Semantic Scholar only:**
- 31.2% have abstracts
- 0.0% have DOIs
- Median citations: 21.0
- Top venues: Health Affairs, PubMed, Journal of the American Medical Association (JAMA)

**Papers in Nber only:**
- 79.2% have abstracts
- 0.0% have DOIs
- Median citations: 1.5
- Top venues: RePEc: Research Papers in Economics, SSRN Electronic Journal, PubMed

## Hypotheses: Why Coverage Differs

### Hypothesis 1: NBER source only indexes NBER working papers

**Evidence:**
- nber_only_papers: 71
- total_nber_papers: 135
- top_venues_in_nber_only: {}

**Conclusion:** NBER has 71 papers not in other sources, likely because NBER specifically scrapes NBER working papers.

### Hypothesis 2: OpenAlex has more complete DOI metadata than Semantic Scholar

**Evidence:**
- openalex_only_papers: 50734
- semantic_scholar_only_papers: 2924
- openalex_only_doi_pct: 70.7
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 70.7% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 1963, 'Forefront Group': 1118, 'PubMed': 1105, 'Health Affairs': 563, 'The Lancet': 295}
- top_venues_in_semantic_scholar_only: {'Health Affairs': 208, 'Journal of the American Medical Association (JAMA)': 77, 'New England Journal of Medicine': 73, 'Journal of Health Politics Policy and Law': 72, 'American Journal of Public Health': 70}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2016.0
- semantic_scholar_only_median_year: 2014.0
- openalex_only_year_range: 2010-2026
- semantic_scholar_only_year_range: 2010-2025

**Conclusion:** OpenAlex-only papers have median year 2016.0, Semantic Scholar-only have median year 2014.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: (showing top 5)
  - ACA: {'openalex': 27320, 'semantic_scholar': 176, 'nber': 32}
  - Affordable Care Act: {'openalex': 22750, 'semantic_scholar': 366, 'nber': 35}
  - Obamacare: {'openalex': 2682, 'semantic_scholar': 356, 'nber': 2}
  - Patient Protection and Affordable Care Act: {'openalex': 10546, 'semantic_scholar': 210, 'nber': 6}
  - health insurance reform 2010: {'openalex': 8612, 'semantic_scholar': 112, 'nber': 9}

**Conclusion:** Search API differences mean the same query returns different results across sources.
