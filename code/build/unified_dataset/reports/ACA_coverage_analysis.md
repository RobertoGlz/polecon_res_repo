# Coverage Analysis Report: ACA

**Generated:** 2026-01-28 16:30:51

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 8240 |
| In OpenAlex | 5024 |
| In Semantic Scholar | 3553 |
| In NBER | 135 |
| In All Three Sources | 2 |
| In Exactly One Source | 7770 |
| In Exactly Two Sources | 468 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 4588
- Papers in Semantic Scholar only: 3117
- Papers in both: 436

**Papers in Openalex only:**
- 71.2% have abstracts
- 86.6% have DOIs
- Median citations: 12.0
- Top venues: Health Affairs, PubMed, SSRN Electronic Journal

**Papers in Semantic Scholar only:**
- 20.6% have abstracts
- 0.0% have DOIs
- Median citations: 24.0
- Top venues: Health Affairs, Journal of the American Medical Association (JAMA), Journal of Health Politics Policy and Law

### Openalex vs Nber

- Papers in Openalex only: 5020
- Papers in Nber only: 131
- Papers in both: 4

**Papers in Openalex only:**
- 71.4% have abstracts
- 79.2% have DOIs
- Median citations: 12.0
- Top venues: PubMed, Health Affairs, SSRN Electronic Journal

**Papers in Nber only:**
- 84.0% have abstracts
- 0.0% have DOIs
- Median citations: 70.5
- Top venues: Journal of policy analysis and management, Journal of Health Economics, Social Science Research Network

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 3519
- Papers in Nber only: 101
- Papers in both: 34

**Papers in Semantic Scholar only:**
- 26.5% have abstracts
- 0.0% have DOIs
- Median citations: 21.0
- Top venues: Health Affairs, PubMed, Journal of the American Medical Association (JAMA)

**Papers in Nber only:**
- 79.2% have abstracts
- 0.0% have DOIs
- Median citations: 4.5
- Top venues: RePEc: Research Papers in Economics, SSRN Electronic Journal

## Hypotheses: Why Coverage Differs

### Hypothesis 1: NBER source only indexes NBER working papers

**Evidence:**
- nber_only_papers: 99
- total_nber_papers: 135
- top_venues_in_nber_only: {}

**Conclusion:** NBER has 99 papers not in other sources, likely because NBER specifically scrapes NBER working papers.

### Hypothesis 2: OpenAlex has more complete DOI metadata than Semantic Scholar

**Evidence:**
- openalex_only_papers: 4588
- semantic_scholar_only_papers: 3117
- openalex_only_doi_pct: 86.6
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 86.6% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'Health Affairs': 167, 'PubMed': 156, 'SSRN Electronic Journal': 126, 'Journal of Health Politics Policy and Law': 99, 'The Lancet': 88}
- top_venues_in_semantic_scholar_only: {'Health Affairs': 211, 'Journal of the American Medical Association (JAMA)': 79, 'Journal of Health Politics Policy and Law': 74, 'New England Journal of Medicine': 74, 'American Journal of Public Health': 70}

**Conclusion:** Different sources index different venues, leading to non-overlapping coverage.

### Hypothesis 4: Sources may have different temporal coverage

**Evidence:**
- openalex_only_median_year: 2015.0
- semantic_scholar_only_median_year: 2014.0
- openalex_only_year_range: 2010-2025
- semantic_scholar_only_year_range: 2010-2025

**Conclusion:** OpenAlex-only papers have median year 2015.0, Semantic Scholar-only have median year 2014.0.

### Hypothesis 5: Different search terms have different effectiveness across sources

**Evidence:**
- search_term_coverage: {'Affordable Care Act': {'openalex': 1517, 'semantic_scholar': 1011, 'nber': 82}, 'ACA': {'openalex': 838, 'semantic_scholar': 313, 'nber': 25}, 'Obamacare': {'openalex': 1180, 'semantic_scholar': 943, 'nber': 8}, 'health insurance reform 2010': {'openalex': 490, 'semantic_scholar': 674, 'nber': 5}, 'Patient Protection and Affordable Care Act': {'openalex': 1059, 'semantic_scholar': 675, 'nber': 21}}

**Conclusion:** Search API differences mean the same query returns different results across sources.
