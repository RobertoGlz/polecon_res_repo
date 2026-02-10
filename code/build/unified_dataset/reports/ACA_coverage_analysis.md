# Coverage Analysis Report: ACA

**Generated:** 2026-02-09 02:45:45

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 92394 |
| In OpenAlex | 89407 |
| In Semantic Scholar | 3551 |
| In NBER | 135 |
| In All Three Sources | 11 |
| In Exactly One Source | 91706 |
| In Exactly Two Sources | 677 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 88772
- Papers in Semantic Scholar only: 2916
- Papers in both: 635

**Papers in Openalex only:**
- 58.8% have abstracts
- 78.1% have DOIs
- Median citations: 7.0
- Top venues: SSRN Electronic Journal, PubMed, Forefront Group

**Papers in Semantic Scholar only:**
- 21.5% have abstracts
- 0.0% have DOIs
- Median citations: 24.0
- Top venues: Health Affairs, Journal of the American Medical Association (JAMA), New England Journal of Medicine

### Openalex vs Nber

- Papers in Openalex only: 89366
- Papers in Nber only: 94
- Papers in both: 41

**Papers in Openalex only:**
- 58.9% have abstracts
- 77.5% have DOIs
- Median citations: 7.0
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
- 31.4% have abstracts
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
- openalex_only_papers: 88772
- semantic_scholar_only_papers: 2916
- openalex_only_doi_pct: 78.1
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 78.1% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 2024, 'PubMed': 1133, 'Forefront Group': 1118, 'Health Affairs': 564, 'Journal of General Internal Medicine': 552}
- top_venues_in_semantic_scholar_only: {'Health Affairs': 208, 'Journal of the American Medical Association (JAMA)': 77, 'New England Journal of Medicine': 73, 'Journal of Health Politics Policy and Law': 72, 'American Journal of Public Health': 70}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2017.0
- semantic_scholar_only_median_year: 2014.0
- openalex_only_year_range: 2010-2026
- semantic_scholar_only_year_range: 2010-2025

**Conclusion:** OpenAlex-only papers have median year 2017.0, Semantic Scholar-only have median year 2014.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: (showing top 5)
  - None: {'openalex': 88742, 'semantic_scholar': 0, 'nber': 0}
  - Obamacare: {'openalex': 362, 'semantic_scholar': 941, 'nber': 8}
  - Affordable Care Act: {'openalex': 121, 'semantic_scholar': 980, 'nber': 82}
  - health insurance reform 2010: {'openalex': 93, 'semantic_scholar': 673, 'nber': 5}
  - Patient Protection and Affordable Care Act: {'openalex': 72, 'semantic_scholar': 658, 'nber': 21}

**Conclusion:** Search API differences mean the same query returns different results across sources.
