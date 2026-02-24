# Task Instructions — EconLit Pipeline (Pipeline A)

## GOAL
Compare EconLit coverage against OpenAlex + Semantic Scholar + NBER + SSRN
to assess how comparable these sources are for economics research papers.

Pipeline B (OpenAlex + SS + NBER) is already built and working.
Pipeline A (EconLit) needs to be built to enable the comparison.

We are using TCJA as the test case, but the pipeline must scale to
hundreds/thousands of policies.

## CURRENT TASK
Implement Pipeline A — a fully automated browser script that emulates what
the user would do by hand on EBSCO Research:

1. Open a browser
2. Go to the EBSCO website (accessed via Stanford credentials)
3. Browse in the EconLit database (db=eoh)
4. Input one of our search terms into the search bar
5. Click the button to select all the results from the search
6. Download all the metadata in CSV format
7. Handle pagination so we get ALL results, not just the first page
8. Repeat for every search term
9. Parse the exported CSVs, deduplicate, recover abstracts, filter

(While waiting for EBSCO EDS API credentials from Stanford Libraries,
we are building this browser automation approach. If API credentials
arrive, we can add an API path later.)

## KEY REQUIREMENTS
1. **Fully automated and scalable** — NO manual steps, NO manual clicking,
   NO manual CSV exports. The script must run end-to-end for any policy
   without human intervention (except initial Stanford SSO login on first run).
   It must work for hundreds/thousands of policies.
2. **Abstracts are essential** — the fewer steps to get abstracts, the better
3. **Pagination** — the script must handle pagination so ALL results are
   exported, not just the first page
4. **Checkpoint/resume** support (same pattern as Pipeline B)
5. **Output compatible with Pipeline B** for overlap analysis

## EBSCO RESEARCH DETAILS
- Stanford proxy URL: https://stanford.idm.oclc.org/login?qurl=https%3A%2F%2Fresearch.ebsco.com%2Fc%2Fqmsjx4%3Fdb%3Deoh
- Landing URL: https://research-ebsco-com.stanford.idm.oclc.org/c/qmsjx4/search?db=eoh
- Database: eoh (EconLit with Full Text)
- Customer ID: s4392798, base path: /c/qmsjx4
- React SPA — content renders client-side, use Selenium waits
- URL params don't auto-execute search — must type into search box and press Enter
- maxBulkSelectionCountLimit: 50
- bulk25kExport: unrestricted (isExportBulk25kRestricted: false)
- "Export results (Up to 25,000)" sends via email (requires MyEBSCO) — NOT a direct download
- To download directly: select records (max 50) → toolbar Download button → CSV format
- For >50 results: load all results, then batch-select 50 at a time using individual checkboxes

## DISCOVERED SELECTORS (from --discover run, 2026-02-23)
- Search input: id="search-input" or input[placeholder*="Search"]
- Result count: h1[data-auto="result-count"] → text "Results: 170"
- Result items: article[data-auto="search-result-item"]
- Select-all checkbox: input[data-auto="bulk-record-checkbox"][aria-label*="Select all displayed"]
- Bulk selection dropdown: button[data-auto="bulk-record-arrow-dropdown"] → aria="Choose the number of results to select"
- Bulk dropdown menu: ul[data-auto="bulk-record-arrow-dropdown-menu"]
- Download button: button[data-auto="tool-button"][aria-label="Download"]
- Selected count: p[data-auto="bulk-record-header-count"] → text "0 selected"
- Results per page: button[data-auto="results-per-page-dropdown-toggle"] → text "Show: 10"
- Pagination: button.eb-pagination__button → text "Show more results"
- Search submit: button.eb-search-button[aria-label="Search"]

## EBSCO EDS API (BACKUP — waiting for credentials)
- Auth endpoint: POST https://eds-api.ebscohost.com/authservice/rest/uidauth
- Session endpoint: GET/POST https://eds-api.ebscohost.com/edsapi/rest/createsession
- Search endpoint: GET/POST https://eds-api.ebscohost.com/edsapi/rest/Search
- Pagination: resultsperpage + pagenumber parameters
- Headers: x-authenticationToken + x-sessionToken
- Python library: ebscopy (pip install ebscopy)
- EconLit database codes: ecn (bibliographic), eoh (with full text)

## WHAT NOT TO DO
- Don't rely on manual CSV exports (doesn't scale)
- Don't add manual fallbacks like "press Enter when done" (defeats the purpose)
- Don't lose sight of the comparison goal — Pipeline A exists to validate Pipeline B

## READ THIS FILE AT THE START OF EVERY ITERATION AND NEVER EDIT THE GOAL
