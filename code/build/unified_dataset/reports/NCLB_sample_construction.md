# Sample Construction Analysis: NCLB

**Generated:** 2026-02-02 09:36:38

This document provides detailed documentation of how the research sample was 
constructed, suitable for inclusion in a research paper's methodology section 
and supplementary materials.

---

## 1. Methodology

### Data Collection Methodology

We collected academic papers related to No Child Left Behind Act 
from three complementary sources: OpenAlex, Semantic Scholar, and the NBER Working Papers database.

**Search Terms:**

We used the following search terms to query each source:

- "No Child Left Behind"
- "NCLB"
- "No Child Left Behind Act"
- "education reform 2001"
- "ESEA reauthorization 2001"

**Source-Specific Collection:**

*OpenAlex:* We queried the OpenAlex API using each search term, retrieving up to 1,500 
results per term. Papers were filtered to those published on or after the policy year 
(2001). 
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

The final unified sample contains 6,461 unique papers:
- 3,686 papers from OpenAlex (57.0%)
- 3,198 papers from Semantic Scholar (49.5%)
- 37 papers from NBER (0.6%)


---

## 2. Sample Composition

### 2.1 Overview by Source Combination

| Category | Count | % of Total | Description |
|----------|-------|------------|-------------|
| all_three | 2 | 0.0% | In all three sources |
| oa_and_ss_only | 446 | 6.9% | In OpenAlex and Semantic Scholar only |
| oa_and_nber_only | 1 | 0.0% | In OpenAlex and NBER only |
| ss_and_nber_only | 9 | 0.1% | In Semantic Scholar and NBER only |
| oa_only | 3,237 | 50.1% | In OpenAlex only |
| ss_only | 2,741 | 42.4% | In Semantic Scholar only |
| nber_only | 25 | 0.4% | In NBER only |
| **Total** | **6,461** | **100%** | |

### 2.2 How Papers Were Matched

| Match Method | Count | % | Interpretation |
|--------------|-------|---|----------------|
| none | 3,685 | 57.0% | N/A... |
| doi | 2,318 | 35.9% | N/A... |
| title | 458 | 7.1% | N/A... |

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
- Publication years: 2003-2017 (median: 2010)
- Citations: median=18.0, mean=18.0, max=20

**Top Venues:**
- RePEc: Research Papers in Economics: 1 papers
- Econstor (Econstor): 1 papers

**Sample Papers:**
1. "Differentiated Accountability and Education Production: Evidence from Nclb Waive..." (2017.0)
2. "Randomized Trials and Quasi-Experiments in Education Research..." (2003.0)

**Interpretation:**
These papers appear in all three sources, indicating they are well-established 
papers with consistent metadata across databases. They are likely published in 
major venues and have DOIs.

### 4.2 In OpenAlex and Semantic Scholar only

**Count:** 446 papers

**Characteristics:**
- 70.2% have abstracts
- 0.0% have DOIs
- Publication years: 2001-2019 (median: 2007)
- Citations: median=19.0, mean=41.6, max=888

**Top Venues:**
- Education next: 13 papers
- Education week: 11 papers
- Medical Entomology and Zoology: 10 papers
- eScholarship (California Digital Library): 9 papers
- Center on Education Policy: 8 papers

**Sample Papers:**
1. "No Child Left Behind Act of 2001...." (2001.0)
2. "The No Child Left Behind Act..." (2002.0)
3. "No Child Left Behind: A Desktop Reference...." (2002.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.3 In OpenAlex and NBER only

**Count:** 1 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2005-2005 (median: 2005)
- Citations: median=10.0, mean=10.0, max=10

**Top Venues:**
- SSRN Electronic Journal: 1 papers

**Sample Papers:**
1. "Testing, Crime and Punishment..." (2005.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.4 In Semantic Scholar and NBER only

**Count:** 9 papers

**Characteristics:**
- 100.0% have abstracts
- 0.0% have DOIs
- Publication years: 2003-2017 (median: 2011)
- Citations: median=42.0, mean=193.9, max=865

**Top Venues:**
- Education Finance and Policy: 2 papers
- Educational Evaluation and Policy Analysis: 1 papers

**Sample Papers:**
1. "The Impact of No Child Left Behind on Student Achievement *..." (2009.0)
2. "The Impact of No Child Left Behind's Accountability Sanctions on School Performa..." (2014.0)
3. "No Child Left Behind: Estimating the Impact on Choices and Student Outcomes..." (2007.0)

**Interpretation:**
These papers appear in two sources but not the third, suggesting partial coverage 
overlap between the sources.

### 4.5 In OpenAlex only

**Count:** 3,237 papers

**Characteristics:**
- 66.2% have abstracts
- 71.6% have DOIs
- Publication years: 2001-2025 (median: 2008)
- Citations: median=13.0, mean=47.4, max=3515

**Top Venues:**
- PsycEXTRA Dataset: 195 papers
- Phi Delta Kappan: 91 papers
- Education Policy Analysis Archives: 46 papers
- Education week: 40 papers
- Educational Policy: 38 papers

**Sample Papers:**
1. "Assessing No Child Left Behind and the Rise of Neoliberal Education Policies..." (2007.0)
2. "Race, inequality and educational accountability: the irony of ‘No Child Left Beh..." (2007.0)
3. "The Impact of No Child Left Behind on Student Achievement..." (2009.0)

**Interpretation:**
These papers are indexed by OpenAlex but not found in Semantic Scholar or NBER. 
This typically occurs because: (1) the paper is from a venue not well-covered by 
Semantic Scholar, (2) the paper's metadata differs between sources preventing matching, 
or (3) the search relevance algorithms returned different results.

### 4.6 In Semantic Scholar only

**Count:** 2,741 papers

**Characteristics:**
- 7.1% have abstracts
- 0.0% have DOIs
- Publication years: 2001-2025 (median: 2008)
- Citations: median=27.0, mean=63.0, max=11473

**Top Venues:**
- Teachers College Record: 24 papers
- American Journal of Education: 17 papers
- The Elementary school journal: 8 papers
- Education Finance and Policy: 8 papers
- American Annals of the Deaf: 7 papers

**Sample Papers:**
1. "Attitudes, Beliefs, and Perceived Effects of “No Filipino Child Left Behind” Pri..." (2025.0)
2. "Assessing No Child Left Behind and the Rise of Neoliberal Education Policies..." (2007.0)
3. "Race, inequality and educational accountability: the irony of ‘No Child Left Beh..." (2007.0)

**Interpretation:**
These papers are indexed by Semantic Scholar but not found in OpenAlex or NBER. 
Semantic Scholar's AI-powered search may surface papers based on semantic similarity 
that keyword-based searches miss.

### 4.7 In NBER only

**Count:** 25 papers

**Characteristics:**
- 28.0% have abstracts
- 0.0% have DOIs
- Publication years: 2002-2024 (median: 2010)

**Sample Papers:**
1. "No Child Left Behind Rules Raise Student Performance..." (2007.0)
2. "The Impact on School Performance of No Child Left Behind Program Sanctions..." (2015.0)
3. "Left Behind By Design: Proficiency Counts and Test-Based Accountability..." (2007.0)

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
