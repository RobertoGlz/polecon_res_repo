# Sample Construction Analysis: TCJA

**Generated:** 2026-02-02 09:36:38

This document provides detailed documentation of how the research sample was 
constructed, suitable for inclusion in a research paper's methodology section 
and supplementary materials.

---

## 1. Methodology

### Data Collection Methodology

We collected academic papers related to Tax Cuts and Jobs Act 
from three complementary sources: OpenAlex, Semantic Scholar, and the NBER Working Papers database.

**Search Terms:**

We used the following search terms to query each source:

- "Tax Cuts and Jobs Act"
- "TCJA"
- "2017 tax reform"
- "Trump tax cuts"
- "Tax Reform 2017"

**Source-Specific Collection:**

*OpenAlex:* We queried the OpenAlex API using each search term, retrieving up to 1,500 
results per term. Papers were filtered to those published on or after the policy year 
(2017). 
We applied relevance filtering, keeping only papers where at least one search term 
appeared in the title or abstract (or papers without abstracts, which could not be filtered).

*Semantic Scholar:* We queried the Semantic Scholar Academic Graph API with the same 
search terms and filtering criteria. Semantic Scholar uses AI-powered semantic search, 
which may return different results than keyword-based search.

*NBER:* We searched the NBER working papers database, which contains only NBER-affiliated 
working papers. This provides a focused but limited subset of economics research.

**Deduplication and Matching:**

Papers were deduplicated across sources using a two-stage matching process:

1. *DOI Matching (Primary):* Papers with identical normalized DOIs were considered the same paper.
2. *Title Matching (Secondary):* For papers without DOI matches, we compared normalized titles 
   (lowercase, punctuation removed). Papers with identical normalized titles were considered matches.

**Final Sample:**

The final unified sample contains 2,320 unique papers:
- 1,217 papers from OpenAlex (52.5%)
- 1,257 papers from Semantic Scholar (54.2%)
- 48 papers from NBER (2.1%)


---

## 2. Sample Composition

### 2.1 Overview by Source Combination

| Category | Count | % of Total | Description |
|----------|-------|------------|-------------|
| all_three | 1 | 0.0% | In all three sources |
| oa_and_ss_only | 193 | 8.3% | In OpenAlex and Semantic Scholar only |
| oa_and_nber_only | 0 | 0.0% | In OpenAlex and NBER only |
| ss_and_nber_only | 7 | 0.3% | In Semantic Scholar and NBER only |
| oa_only | 1,023 | 44.1% | In OpenAlex only |
| ss_only | 1,056 | 45.5% | In Semantic Scholar only |
| nber_only | 40 | 1.7% | In NBER only |
| **Total** | **2,320** | **100%** | |

### 2.2 How Papers Were Matched

| Match Method | Count | % | Interpretation |
|--------------|-------|---|----------------|
| none | 1,235 | 53.2% | N/A... |
| doi | 884 | 38.1% | N/A... |
| title | 201 | 8.7% | N/A... |

---

## 3. Understanding Source Differences

### 3.1 Why do papers appear in one source but not another?

#### OpenAlex

**Coverage:** Broad academic coverage including journals, conferences, books, dissertations, and preprints

**Search Method:** Full-text search across titles, abstracts, and full text (when available)

**Why papers may be unique to this source:**
- Indexes venues not covered by Semantic Scholar (e.g., law reviews, policy journals)
- Better coverage of older publications
- Includes non-English publications
- Indexes institutional repositories and working papers beyond NBER

**Limitations:**
- Abstracts stored as inverted index (may have reconstruction issues)
- Some newer papers may have delayed indexing
- Search relevance algorithm may differ from other sources

#### Semantic Scholar

**Coverage:** Strong in computer science, biomedical; expanding to other fields

**Search Method:** Semantic search using AI/ML models to understand query intent

**Why papers may be unique to this source:**
- Different relevance algorithm returns different papers for same query
- Better coverage of certain preprint servers
- May surface papers based on semantic similarity not just keyword match
- Indexes some venues not in OpenAlex

**Limitations:**
- Historically focused on STEM fields
- May have gaps in social sciences, humanities, law
- DOI coverage less complete than OpenAlex
- API rate limits more restrictive

#### NBER Working Papers

**Coverage:** NBER working papers ONLY - a specific subset of economics research

**Search Method:** Keyword search on NBER website

**Why papers may be unique to this source:**
- NBER working papers may not be indexed elsewhere before publication
- Some working papers never get published in journals
- Working paper versions have different titles/metadata than published versions

**Limitations:**
- ONLY includes NBER working papers
- Does not include published versions of same papers
- Limited to economics and related fields
- Much smaller scope than general academic indexes

### 3.2 Why do papers appear in multiple sources?

Papers appear in multiple sources when:

1. **Both sources index the same venue/journal** - Major academic databases have 
   overlapping coverage of prominent journals and conferences.

2. **DOI matching succeeds** - When a paper has a DOI and both sources have indexed it 
   with that DOI, we can confidently identify it as the same paper.

3. **Title matching succeeds** - When DOIs are unavailable, papers with identical 
   titles (after normalization) are considered the same paper.

---

## 4. Detailed Category Analysis

### 4.1 In all three sources

**Count:** 1 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2019-2019 (median: 2019)
- Citations: median=4.0, mean=4.0, max=4

**Top Venues:**
- RePEc: Research Papers in Economics: 1 papers

**Sample Papers:**
1. "Tax Prices and Charitable Giving: Projected Changes in Donations Under the 2017 ..." (2019.0)

**Interpretation:**
These papers appear in all three sources, indicating they are well-established 
papers with consistent metadata across databases. They are likely published in 
major venues and have DOIs.

### 4.2 In OpenAlex and Semantic Scholar only

**Count:** 193 papers

**Characteristics:**
- 83.4% have abstracts
- 0.0% have DOIs
- Publication years: 2017-2021 (median: 2019)
- Citations: median=2.0, mean=4.4, max=33

**Top Venues:**
- SSRN Electronic Journal: 68 papers
- RePEc: Research Papers in Economics: 9 papers
- Library of Congress. Congressional Research Service: 6 papers
- Liberty Street Economics: 5 papers
- Penn Carey Law Legal Scholarship Repository (University of Pennsylvania): 5 papers

**Sample Papers:**
1. "Is the Tax Cuts and Jobs Act GILTI of Anti-Simplification?..." (2019.0)
2. "Charitable Giving and the Tax Cuts and Jobs Act..." (2018.0)
3. "Effective Tax Rates on Business Investment under the Tax Cuts and Jobs Act..." (2018.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.4 In Semantic Scholar and NBER only

**Count:** 7 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2018-2025 (median: 2022)
- Citations: median=10.5, mean=20.3, max=57

**Top Venues:**
- Social Science Research Network: 5 papers
- Tax Policy and the Economy: 2 papers

**Sample Papers:**
1. "Did the Tax Cuts and Jobs Act Reduce Profit Shifting by US Multinational Compani..." (2022.0)
2. "The Tax Cuts and Jobs Act: Which Firms Won? Which Lost?..." (2020.0)
3. "Tax Prices and Charitable Giving: Projected Changes in Donations under the 2017 ..." (2019.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.5 In OpenAlex only

**Count:** 1,023 papers

**Characteristics:**
- 66.7% have abstracts
- 86.4% have DOIs
- Publication years: 2017-2026 (median: 2020)
- Citations: median=6.0, mean=76.5, max=3515

**Top Venues:**
- SSRN Electronic Journal: 220 papers
- National Tax Journal: 23 papers
- The Lancet: 17 papers
- The Contemporary Tax Journal: 16 papers
- FOXBusiness: 12 papers

**Sample Papers:**
1. "PROFIT SHIFTING BEFORE AND AFTER THE TAX CUTS AND JOBS ACT..." (2020.0)
2. "Effects of the Tax Cuts and Jobs Act: A Preliminary Analysis..." (2018.0)
3. "Profit Shifting Before and After the Tax Cuts and Jobs Act..." (2018.0)

**Interpretation:**
These papers are indexed by OpenAlex but not found in Semantic Scholar or NBER. 
This typically occurs because: (1) the paper is from a venue not well-covered by 
Semantic Scholar, (2) the paper's metadata differs between sources preventing matching, 
or (3) the search relevance algorithms returned different results.

### 4.6 In Semantic Scholar only

**Count:** 1,056 papers

**Characteristics:**
- 28.4% have abstracts
- 0.0% have DOIs
- Publication years: 2017-2026 (median: 2019)
- Citations: median=4.0, mean=11.2, max=465

**Top Venues:**
- Social Science Research Network: 76 papers
- International Tax and Public Finance: 26 papers
- US-Steuerreform - Der Tax Cuts and Jobs Act 2017: 24 papers
- National tax journal: 23 papers
- Florida Tax Review: 8 papers

**Sample Papers:**
1. "PROFIT SHIFTING BEFORE AND AFTER THE TAX CUTS AND JOBS ACT..." (2020.0)
2. "Effects of the Tax Cuts and Jobs Act: A Preliminary Analysis..." (2018.0)
3. "Sweeping Changes and an Uncertain Legacy: The Tax Cuts and Jobs Act of 2017..." (2024.0)

**Interpretation:**
These papers are indexed by Semantic Scholar but not found in OpenAlex or NBER. 
Semantic Scholar's AI-powered search may surface papers based on semantic similarity 
that keyword-based searches miss.

### 4.7 In NBER only

**Count:** 40 papers

**Characteristics:**
- 15.0% have abstracts
- 0.0% have DOIs
- Publication years: 2017-2026 (median: 2019)

**Sample Papers:**
1. "Investment Effects of the 2017 Tax Cuts and Jobs Act..." (2024.0)
2. "Anders Jensen..." (nan)
3. "Anders Anderson..." (nan)

**Interpretation:**
These are NBER working papers not found in the other sources. This may occur because: 
(1) the working paper version has a different title than any published version, 
(2) the paper has not been indexed by OpenAlex/Semantic Scholar yet, or 
(3) the paper was never published in a journal.

---

## 5. Implications for Research

### 5.1 Sample Completeness

By combining three sources, we capture a more complete picture of research on this policy 
than any single source would provide. However, researchers should be aware that:

- The sample is not exhaustive; papers not indexed by any of these sources are not included
- Search term selection affects which papers are retrieved
- Relevance filtering may exclude marginally relevant papers

### 5.2 Potential Biases

- **Publication bias:** Working papers (NBER) may differ systematically from published papers
- **Field bias:** Semantic Scholar has historically emphasized STEM fields
- **Recency bias:** Newer papers may have incomplete indexing
- **Language bias:** English-language papers are over-represented

### 5.3 Robustness Checks

To assess sensitivity of results to sample construction, researchers may:

1. **Restrict to papers in multiple sources:** Analyze only papers found in 2+ sources
2. **Source-specific analysis:** Run analyses separately by source
3. **Exclude NBER:** Analyze only published papers (OpenAlex + Semantic Scholar)
