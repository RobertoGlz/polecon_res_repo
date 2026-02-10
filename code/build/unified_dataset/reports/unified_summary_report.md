# Unified Dataset Summary Report

**Generated:** 2026-02-09 02:45:59

This report summarizes the unified policy papers dataset created from
OpenAlex, Semantic Scholar, and NBER sources.

---

## Summary by Policy

| Policy | Total Papers | In OpenAlex | In Semantic Scholar | In NBER | In All Sources | With Abstract |
|--------|--------------|-------------|---------------------|---------|----------------|---------------|
| TCJA | 11930 | 10856 | 1256 | 48 | 1 | 1092 (9.2%) |
| ACA | 92394 | 89407 | 3551 | 135 | 11 | 53365 (57.8%) |
| NCLB | 21838 | 19162 | 3197 | 37 | 4 | 7669 (35.1%) |

## Overall Totals

- **Total unique papers across all policies:** 126162
- **Total papers with abstracts:** 62126 (49.2%)

## Key Findings

1. **Source Coverage:**
   - OpenAlex provides the broadest coverage of academic literature
   - Semantic Scholar complements with additional papers, especially in certain fields
   - NBER provides unique working papers not indexed elsewhere

2. **Matching Results:**
   - DOI matching is the most reliable method for deduplication
   - Title matching catches additional overlaps for papers without DOIs
   - Most papers appear in only one source

3. **Data Quality:**
   - Abstract availability varies by source
   - DOI coverage is highest in OpenAlex
   - Citation counts may differ across sources due to different counting methods

## Output Files

For each policy, the following files are generated:

- `{policy}_unified_dataset.parquet` - Main unified dataset
- `{policy}_unified_dataset.csv` - CSV version
- `{policy}_match_registry.parquet` - Detailed matching information
- `{policy}_openalex_report.md` - OpenAlex source report
- `{policy}_semantic_scholar_report.md` - Semantic Scholar source report
- `{policy}_nber_report.md` - NBER source report
- `{policy}_coverage_analysis.md` - Coverage analysis with hypotheses
