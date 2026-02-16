# Abstract Recovery Pipeline

Recovers missing abstracts for papers scraped from OpenAlex, Semantic Scholar, and NBER using a multi-source fallback approach.

## Architecture

```
OpenAlex Scraped Papers (Parquet)
            |
            v
  complement_abstracts_main.py    (8-step API pipeline)
            |
    +-------+-------+-------+-------+-------+-------+-------+
    |       |       |       |       |       |       |       |
    v       v       v       v       v       v       v       v
 CrossRef  OA URL  SSRN   NBER  Semantic  Europe  DOI     Failure
  API     Scrape  (Selen) Web   Scholar    PMC   Resolve   Log
    |       |       |       |       |       |       |
    +-------+-------+-------+-------+-------+-------+
            |
            v
  Relevance Filter (search terms)
            |
            v
  Complemented Dataset (Parquet/CSV)
            |
            v
  unified_dataset_main.py  (merge all sources)
            |
            v
  Unified Dataset (Parquet/CSV)
            |
            v
  scrape_abstracts_web.py  (optional, authenticated)
            |
    +-------+-------+-------+-------+
    |       |       |       |       |
    v       v       v       v       v
 ProQuest  DOI    EconLit  Science  All
          Proxy           Direct
            |
            v
  Enhanced Unified Dataset (Parquet)
```

## Recovery Sources

| Step | Source | Type | Auth | Rate Limit | Workers | Best For |
|------|--------|------|------|------------|---------|----------|
| 1 | CrossRef API | API | No (polite pool) | 0.1s | 5 | Papers with DOIs |
| 2 | OA URL Scraping | HTTP | No | 0.05s | 8 | PubMed, arXiv, repos |
| 2b | PDF Extraction | HTTP+PDF | No | 0.05s | 4 | PDF-only OA papers |
| 2c | Selenium JS | Browser | No | 0.8s | 3 | JS-rendered pages |
| 3 | SSRN | Browser | No | 0.8s | 3 | SSRN papers |
| 4 | NBER Website | HTTP | No | 0.3s | 5 | NBER working papers |
| 5 | Semantic Scholar | API | API key | 1.0s | 2 | Papers with DOIs |
| 6 | Europe PMC | API | No | 0.2s | 5 | Biomedical + econ papers |
| 7 | DOI Resolution | HTTP | No | 0.5s | 4 | Any DOI (publisher page) |
| Web | ProQuest | Browser | Stanford SSO | 7s | 1 | Paywalled papers |
| Web | DOI Proxy | Browser | Stanford SSO | 7s | 1 | Publisher paywalls |
| Web | EconLit | Browser | Stanford SSO | 7s | 1 | Economics papers |
| Web | ScienceDirect | Browser | Stanford SSO | 7s | 1 | Elsevier papers |

## Quick Start

### 1. Run the API pipeline (Steps 1-7)

```bash
# Single policy
python complement_abstracts_main.py TCJA

# All policies
python complement_abstracts_main.py

# Resume (skip policies already completed today)
python complement_abstracts_main.py --resume
```

### 2. Merge into unified dataset

```bash
python ../unified_dataset/unified_dataset_main.py
```

### 3. (Optional) Authenticated web scraping for remaining gaps

```bash
# Scrape via Stanford DOI proxy (most versatile)
python scrape_abstracts_web.py TCJA --source doi --max-papers 500

# Resume interrupted session
python scrape_abstracts_web.py TCJA --source doi --resume

# Merge results back into dataset
python scrape_abstracts_web.py TCJA --source doi --resume --merge
```

## Detailed Usage

### complement_abstracts_main.py

```
python complement_abstracts_main.py [POLICY ...] [--resume]

Arguments:
  POLICY      Policy abbreviation(s): TCJA, ACA, NCLB (default: all)
  --resume    Skip policies already completed today

Environment Variables:
  SEMANTIC_SCHOLAR_API_KEY   API key for Semantic Scholar (optional but recommended)
```

**Input:** OpenAlex scraped papers from `scrape_policies_openalex/output/`
**Output:**
- `output/{POLICY}_papers_complemented.parquet` - All papers after abstract recovery
- `output/{POLICY}_papers_complemented_filtered.parquet` - After relevance filter
- `output/{POLICY}_papers_complemented_filtered.csv` - CSV format
- `output/{POLICY}_complement_metadata.json` - Statistics and metadata
- `tmp/crossref_responses.json` - Raw CrossRef API responses
- `tmp/oa_url_responses.json` - Raw OA URL scraping responses
- `tmp/ssrn_responses.json` - Raw SSRN responses
- `tmp/nber_responses.json` - Raw NBER responses
- `tmp/semantic_scholar_responses.json` - Raw Semantic Scholar responses
- `tmp/europepmc_responses.json` - Raw Europe PMC responses
- `tmp/doi_resolution_responses.json` - Raw DOI resolution responses
- `tmp/abstract_recovery_failures.json` - Detailed failure log

### scrape_abstracts_web.py

```
python scrape_abstracts_web.py POLICY --source SOURCE [options]

Arguments:
  POLICY                  Policy abbreviation (TCJA, ACA, NCLB)
  --source SOURCE         proquest | doi | econlit | sciencedirect | all
  --delay SECONDS         Delay between requests (default: 7)
  --max-papers N          Maximum papers to scrape
  --resume                Resume from last checkpoint
  --merge                 Merge results into unified dataset
```

**Input:** Unified dataset from `unified_dataset/output/`
**Output:**
- `output_web_scraping/{POLICY}_{SOURCE}_results.json` - Scraped abstracts
- `output_web_scraping/{POLICY}_{SOURCE}_summary.json` - Stats
- `output_web_scraping/checkpoints/{POLICY}_{SOURCE}_checkpoint.json` - Resume state
- `output_web_scraping/{POLICY}_unified_dataset_enhanced.parquet` - (with `--merge`)

## Output Directory Structure

```
complement_abstracts/
  output/
    {POLICY}_papers_complemented.parquet
    {POLICY}_papers_complemented_filtered.parquet
    {POLICY}_papers_complemented_filtered.csv
    {POLICY}_complement_metadata.json
  tmp/
    crossref_responses.json
    oa_url_responses.json
    pdf_responses.json
    ssrn_responses.json
    selenium_responses.json
    nber_responses.json
    semantic_scholar_responses.json
    europepmc_responses.json
    doi_resolution_responses.json
    abstract_recovery_failures.json
  output_web_scraping/
    {POLICY}_{SOURCE}_results.json
    {POLICY}_{SOURCE}_summary.json
    checkpoints/
      {POLICY}_{SOURCE}_checkpoint.json
  selenium_profile/       (persistent browser profile)
```

## Configuration

### Worker and Rate Limit Tuning

Edit constants at the top of `complement_abstracts_main.py`:

```python
# Worker counts (parallel threads per step)
MAX_WORKERS_CROSSREF = 5           # CrossRef is generous
MAX_WORKERS_OA = 8                 # Diverse servers
MAX_WORKERS_PDF = 4                # CPU-bound extraction
MAX_WORKERS_SELENIUM = 3           # Browser memory limits
MAX_WORKERS_SSRN = 3               # SSRN is sensitive
MAX_WORKERS_NBER = 5               # NBER is moderate
MAX_WORKERS_SEMANTIC_SCHOLAR = 2   # Strict rate limit
MAX_WORKERS_EUROPEPMC = 5          # Free API
MAX_WORKERS_DOI_RESOLUTION = 4     # Publisher diversity

# Rate limiters (minimum seconds between requests per API)
crossref_limiter = RateLimiter(0.1)
oa_url_limiter = RateLimiter(0.05)
nber_limiter = RateLimiter(0.3)
ssrn_limiter = RateLimiter(0.8)
semantic_scholar_limiter = RateLimiter(1.0)    # DO NOT decrease
europepmc_limiter = RateLimiter(0.2)
doi_resolution_limiter = RateLimiter(0.5)
```

### Semantic Scholar API Key

Get a free API key from [Semantic Scholar](https://www.semanticscholar.org/product/api) and set it:

```bash
# .env file
SEMANTIC_SCHOLAR_API_KEY=your_key_here

# Or export directly
export SEMANTIC_SCHOLAR_API_KEY=your_key_here
```

Without a key, Semantic Scholar limits to ~100 req/5min. With a key: 1 req/sec sustained.

## Troubleshooting

### Semantic Scholar API errors
- **429 Rate limit**: The limiter is set to 1 req/sec. If you still hit limits, reduce `MAX_WORKERS_SEMANTIC_SCHOLAR` to 1.
- **404 Not found**: Paper's DOI is not indexed in Semantic Scholar. This is expected for many papers.

### PDF extraction issues
- Install pdfplumber: `pip install pdfplumber`
- Some PDFs are image-based (scanned). These cannot be extracted without OCR.

### Selenium crashes
- Make sure Chrome is installed and up to date
- On memory-limited machines, reduce `MAX_WORKERS_SELENIUM` and `MAX_WORKERS_SSRN` to 1
- Clear the `selenium_profile/` directory if the browser profile becomes corrupted

### IP blocks during web scraping
- The scraper auto-detects blocks and halts with a message
- Wait 15-30 minutes before resuming with `--resume`
- Increase `--delay` (recommended: 8-12s for aggressive sites)
- If persistent, try a different `--source`

### Stanford SSO login
- On first run, a browser window opens for manual SSO login
- Complete 2FA if prompted
- The session persists in `selenium_profile/` for future runs
- If login expires, delete `selenium_profile/` and re-login

### Europe PMC no results
- Europe PMC has excellent biomedical coverage but limited economics coverage
- Title search fallback helps when DOI search fails
- Very long or special-character titles may fail the title search

### DOI resolution blocked
- Some publishers aggressively block scrapers (403/429)
- The pipeline handles this gracefully and logs the failure
- These papers are candidates for the authenticated web scraper

## Performance by Policy Size

| Policy | Papers | Estimated API pipeline time | Notes |
|--------|--------|---------------------------|-------|
| NCLB | ~19K | 30-45 min | Good for testing |
| TCJA | ~11K | 20-30 min | Smallest, but high % missing |
| ACA | ~90K | 2-3 hours | Largest; use `--resume` if interrupted |

Web scraping is significantly slower (~7s/paper). For 500 papers: ~1 hour.

## Dependencies

```
pip install requests beautifulsoup4 pandas pyarrow selenium pdfplumber
```

- **requests**: HTTP client for API calls
- **beautifulsoup4**: HTML parsing
- **pandas + pyarrow**: Dataset I/O (Parquet)
- **selenium**: Browser automation (SSRN, web scraping)
- **pdfplumber**: PDF text extraction (optional)
- **Chrome**: Must be installed on the system
