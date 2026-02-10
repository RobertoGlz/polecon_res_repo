# Coverage Analysis Report: TCJA

**Generated:** 2026-02-09 02:44:48

This report analyzes why papers appear in some sources but not others.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Total Unified Papers | 11930 |
| In OpenAlex | 10856 |
| In Semantic Scholar | 1256 |
| In NBER | 48 |
| In All Three Sources | 1 |
| In Exactly One Source | 11701 |
| In Exactly Two Sources | 228 |

## Pairwise Comparisons

### Openalex vs Semantic Scholar

- Papers in Openalex only: 10634
- Papers in Semantic Scholar only: 1034
- Papers in both: 222

**Papers in Openalex only:**
- 5.6% have abstracts
- 72.9% have DOIs
- Median citations: 4.0
- Top venues: SSRN Electronic Journal, Scholarworks (University of Massachusetts Amherst), Policy Press eBooks

**Papers in Semantic Scholar only:**
- 29.6% have abstracts
- 0.0% have DOIs
- Median citations: 4.0
- Top venues: Social Science Research Network, International Tax and Public Finance, US-Steuerreform - Der Tax Cuts and Jobs Act 2017

### Openalex vs Nber

- Papers in Openalex only: 10855
- Papers in Nber only: 47
- Papers in both: 1

**Papers in Openalex only:**
- 7.2% have abstracts
- 71.4% have DOIs
- Median citations: 4.0
- Top venues: SSRN Electronic Journal, Scholarworks (University of Massachusetts Amherst), Policy Press eBooks

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
- openalex_only_papers: 10634
- semantic_scholar_only_papers: 1034
- openalex_only_doi_pct: 72.9
- semantic_scholar_only_doi_pct: 0.0

**Conclusion:** OpenAlex-only papers have 72.9% DOI coverage vs 0.0% for Semantic Scholar-only papers.

### Hypothesis 3: Sources have different venue coverage patterns

**Evidence:**
- top_venues_in_openalex_only: {'SSRN Electronic Journal': 1225, 'Scholarworks (University of Massachusetts Amherst)': 152, 'Policy Press eBooks': 141, 'ANU Press eBooks': 138, 'DigitalGeorgetown (Georgetown University Library)': 127}
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
  - None: {'openalex': 10634, 'semantic_scholar': 0, 'nber': 0}
  - Tax Cuts and Jobs Act: {'openalex': 188, 'semantic_scholar': 720, 'nber': 32}
  - TCJA: {'openalex': 17, 'semantic_scholar': 57, 'nber': 6}
  - 2017 tax reform: {'openalex': 15, 'semantic_scholar': 336, 'nber': 8}
  - Trump tax cuts: {'openalex': 2, 'semantic_scholar': 142, 'nber': 0}

**Conclusion:** Search API differences mean the same query returns different results across sources.
