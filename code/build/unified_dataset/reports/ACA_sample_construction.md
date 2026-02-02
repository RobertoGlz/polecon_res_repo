# Sample Construction Analysis: ACA

**Generated:** 2026-02-02 09:36:38

This document provides detailed documentation of how the research sample was 
constructed, suitable for inclusion in a research paper's methodology section 
and supplementary materials.

---

## 1. Methodology

### Data Collection Methodology

We collected academic papers related to Affordable Care Act 
from three complementary sources: OpenAlex, Semantic Scholar, and the NBER Working Papers database.

**Search Terms:**

We used the following search terms to query each source:

- "Affordable Care Act"
- "ACA"
- "Obamacare"
- "health insurance reform 2010"
- "Patient Protection and Affordable Care Act"

**Source-Specific Collection:**

*OpenAlex:* We queried the OpenAlex API using each search term, retrieving up to 1,500 
results per term. Papers were filtered to those published on or after the policy year 
(2010). 
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

The final unified sample contains 8,240 unique papers:
- 5,024 papers from OpenAlex (61.0%)
- 3,553 papers from Semantic Scholar (43.1%)
- 135 papers from NBER (1.6%)


---

## 2. Sample Composition

### 2.1 Overview by Source Combination

| Category | Count | % of Total | Description |
|----------|-------|------------|-------------|
| all_three | 2 | 0.0% | In all three sources |
| oa_and_ss_only | 434 | 5.3% | In OpenAlex and Semantic Scholar only |
| oa_and_nber_only | 2 | 0.0% | In OpenAlex and NBER only |
| ss_and_nber_only | 32 | 0.4% | In Semantic Scholar and NBER only |
| oa_only | 4,586 | 55.7% | In OpenAlex only |
| ss_only | 3,085 | 37.4% | In Semantic Scholar only |
| nber_only | 99 | 1.2% | In NBER only |
| **Total** | **8,240** | **100%** | |

### 2.2 How Papers Were Matched

| Match Method | Count | % | Interpretation |
|--------------|-------|---|----------------|
| doi | 3,974 | 48.2% | N/A... |
| none | 3,796 | 46.1% | N/A... |
| title | 470 | 5.7% | N/A... |

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

**Count:** 2 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2013-2016 (median: 2014)
- Citations: median=46.0, mean=46.0, max=57

**Top Venues:**
- RePEc: Research Papers in Economics: 1 papers

**Sample Papers:**
1. "Average Marginal Labor Income Tax Rates under the Affordable Care Act *..." (2013.0)
2. "Employment Effects of the ACA Medicaid Expansions..." (2016.0)

**Interpretation:**
These papers appear in all three sources, indicating they are well-established 
papers with consistent metadata across databases. They are likely published in 
major venues and have DOIs.

### 4.2 In OpenAlex and Semantic Scholar only

**Count:** 434 papers

**Characteristics:**
- 73.7% have abstracts
- 0.2% have DOIs
- Publication years: 2010-2020 (median: 2013)
- Citations: median=8.0, mean=22.9, max=573

**Top Venues:**
- PubMed: 120 papers
- SSRN Electronic Journal: 32 papers
- Medical economics: 22 papers
- London School of Economics and Political Science Research Online (London School of Economics and Political Science): 14 papers
- Medical Entomology and Zoology: 6 papers

**Sample Papers:**
1. "Key Features of the Affordable Care Act by Year..." (2014.0)
2. "The Implications of the Affordable Care Act for Mental Health Care..." (2014.0)
3. "Population Health in the Affordable Care Act Era..." (2013.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.3 In OpenAlex and NBER only

**Count:** 2 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2013-2016 (median: 2014)
- Citations: median=4.5, mean=4.5, max=5

**Top Venues:**
- RePEc: Research Papers in Economics: 1 papers
- SSRN Electronic Journal: 1 papers

**Sample Papers:**
1. "Is the Affordable Care Act Different from Romneycare? A Labor Economics Perspect..." (2013.0)
2. "The Impact of Health Insurance on Preventive Care and Health Behaviors: Evidence..." (2016.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.4 In Semantic Scholar and NBER only

**Count:** 32 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2011-2022 (median: 2017)
- Citations: median=70.5, mean=108.5, max=426

**Top Venues:**
- Journal of policy analysis and management: 5 papers
- Journal of Health Economics: 4 papers
- Social Science Research Network: 4 papers
- National tax journal: 2 papers
- Tax Policy and the Economy: 2 papers

**Sample Papers:**
1. "Impacts of the Affordable Care Act on Health Insurance Coverage in Medicaid Expa..." (2016.0)
2. "The Affordable Care Act's Effects on Patients, Providers and the Economy: What W..." (2019.0)
3. "Premium Subsidies, the Mandate, and Medicaid Expansion: Coverage Effects of the ..." (2016.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.5 In OpenAlex only

**Count:** 4,586 papers

**Characteristics:**
- 71.2% have abstracts
- 86.7% have DOIs
- Publication years: 2010-2025 (median: 2015)
- Citations: median=12.0, mean=51.2, max=14881

**Top Venues:**
- Health Affairs: 167 papers
- PubMed: 156 papers
- SSRN Electronic Journal: 125 papers
- Journal of Health Politics Policy and Law: 99 papers
- The Lancet: 88 papers

**Sample Papers:**
1. "The Patient Protection and Affordable Care Act..." (2018.0)
2. "Hospital Readmissions and the Affordable Care Act..." (2011.0)
3. "Promoting Prevention through the Affordable Care Act..." (2010.0)

**Interpretation:**
These papers are indexed by OpenAlex but not found in Semantic Scholar or NBER. 
This typically occurs because: (1) the paper is from a venue not well-covered by 
Semantic Scholar, (2) the paper's metadata differs between sources preventing matching, 
or (3) the search relevance algorithms returned different results.

### 4.6 In Semantic Scholar only

**Count:** 3,085 papers

**Characteristics:**
- 19.8% have abstracts
- 0.0% have DOIs
- Publication years: 2010-2025 (median: 2014)
- Citations: median=23.0, mean=53.4, max=2006

**Top Venues:**
- Health Affairs: 211 papers
- Journal of the American Medical Association (JAMA): 79 papers
- New England Journal of Medicine: 74 papers
- Journal of Health Politics Policy and Law: 74 papers
- American Journal of Public Health: 70 papers

**Sample Papers:**
1. "Changes in Self-reported Insurance Coverage, Access to Care, and Health Under th..." (2015.0)
2. "Geographic access to federally qualified health centers before and after the aff..." (2022.0)
3. "Medicaid Expansion and mental health treatment: Evidence from the Affordable Car..." (2022.0)

**Interpretation:**
These papers are indexed by Semantic Scholar but not found in OpenAlex or NBER. 
Semantic Scholar's AI-powered search may surface papers based on semantic similarity 
that keyword-based searches miss.

### 4.7 In NBER only

**Count:** 99 papers

**Characteristics:**
- 78.8% have abstracts
- 0.0% have DOIs
- Publication years: 2012-2025 (median: 2017)

**Sample Papers:**
1. "2019, Mark Duggan, "The Affordable Care Act"..." (2019.0)
2. "Affordable Care Act (ACA) Exchange Data..." (nan)
3. "The 2010 Affordable Care Act Dependent Coverage Mandate..." (2012.0)

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
