# Coverage Analysis Report: TCJA

**Generated:** 2026-02-19 20:26:30

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 10500 |
| In OpenAlex | 9426 |
| In Semantic Scholar | 1256 |
| In NBER | 48 |
| In All Three Sources | 1 |
| In Exactly One Source | 10271 |
| In Exactly Two Sources | 228 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 9204
- Papers in Semantic Scholar only: 1034
- Papers in both: 222

**Papers in Openalex only:**
- 6.8% have abstracts
- 68.6% have DOIs
- Median citations: 3.0
- Top venues: SSRN Electronic Journal, Scholarworks (University of Massachusetts Amherst), ANU Press eBooks

**Papers in Semantic Scholar only:**
- 29.6% have abstracts
- 0.0% have DOIs
- Median citations: 4.0
- Top venues: Social Science Research Network, International Tax and Public Finance, US-Steuerreform - Der Tax Cuts and Jobs Act 2017

### Openalex vs Nber

- Papers in Openalex only: 9425
- Papers in Nber only: 47
- Papers in both: 1

**Papers in Openalex only:**
- 8.6% have abstracts
- 67.0% have DOIs
- Median citations: 3.0
- Top venues: SSRN Electronic Journal, Scholarworks (University of Massachusetts Amherst), ANU Press eBooks

**Papers in Nber only:**
- 27.7% have abstracts
- 0.0% have DOIs
- Median citations: 10.5
- Top venues: Social Science Research Network, Tax Policy and the Economy

### Semantic Scholar vs Nber

- Papers in Semantic Scholar only: 1248
- Papers in Nber only: 40
- Papers in both: 8

**Papers in Semantic Scholar only:**
- 39.0% have abstracts
- 0.0% have DOIs
- Median citations: 3.0
- Top venues: SSRN Electronic Journal, Social Science Research Network, International Tax and Public Finance

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
- openalex_only_papers: 9204
- semantic_scholar_only_papers: 1034
- openalex_only_doi_pct: 68.6
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 68.6% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 1157, 'Scholarworks (University of Massachusetts Amherst)': 152, 'ANU Press eBooks': 134, 'Policy Press eBooks': 129, 'DigitalGeorgetown (Georgetown University Library)': 127}
- top_venues_in_semantic_scholar_only: {'Social Science Research Network': 81, 'International Tax and Public Finance': 26, 'US-Steuerreform - Der Tax Cuts and Jobs Act 2017': 24, 'National tax journal': 22, 'Florida Tax Review': 8}

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
- search_term_coverage: (showing top 5)
  - TCJA: {'openalex': 582, 'semantic_scholar': 97, 'nber': 1}
  - 2017 tax reform: {'openalex': 6878, 'semantic_scholar': 79, 'nber': 0}
  - Tax Cuts and Jobs Act: {'openalex': 3837, 'semantic_scholar': 196, 'nber': 1}
  - Tax Reform 2017: {'openalex': 6878, 'semantic_scholar': 79, 'nber': 0}
  - Trump tax cuts: {'openalex': 1097, 'semantic_scholar': 34, 'nber': 0}

**Conclusion:** Search API differences mean the same query returns different results across sources.
