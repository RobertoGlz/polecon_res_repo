"""
Economics-restricted scraping from OpenAlex + Semantic Scholar + NBER.

Issue #24: Economics-restricted scraping pipelines

Pipeline B: Searches OpenAlex, Semantic Scholar, and NBER for TCJA papers
restricted to economics/business fields. Merges results across sources,
recovers missing abstracts, and applies relevance filtering.

Pipeline Overview:
------------------
1. Load TCJA search terms from policies.csv
2. OpenAlex: cursor-based search with topics.field.id:20|14 filter
3. Semantic Scholar: bulk search with fieldsOfStudy=Economics,Business
4. NBER: standard search (already all economics)
5. Merge across sources (DOI + title matching)
6. Recover missing abstracts via CrossRef + Semantic Scholar APIs
7. Apply relevance filtering (search terms in title/abstract)
8. Save raw and filtered outputs

Key Implementation Notes:
-------------------------
- OpenAlex filter: topics.field.id:20|14 covers Economics/Econometrics/Finance
  (field 20) and Business/Management/Accounting (field 14). Many tax papers
  are classified under Business, so both fields are needed.
- Semantic Scholar: fieldsOfStudy=Economics,Business matches OpenAlex scope.
- NBER: all papers are economics working papers by definition.
- paper_type column: 'journal_article' or 'working_paper' for each paper.
- Abstract recovery times first 20 papers to estimate total time.

Output Files:
-------------
- TCJA_papers_econ_apis_raw.parquet: Merged dataset before relevance filtering
- TCJA_papers_econ_apis_filtered.parquet: After relevance filtering
- TCJA_econ_apis_metadata.json: Scraping and processing statistics
- tmp/TCJA_openalex_econ_raw.parquet: OpenAlex-only raw results
- tmp/TCJA_ss_econ_raw.parquet: Semantic Scholar-only raw results
- tmp/TCJA_nber_econ_raw.parquet: NBER-only raw results

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 2026
"""

import argparse
import requests
import json
import pandas as pd
import time
import re
from datetime import datetime
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv

# =============================================================================
# PATHS
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")
POLICIES_FILE = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv"))

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Load .env from repo root
REPO_ROOT = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
load_dotenv(os.path.join(REPO_ROOT, ".env"))

# =============================================================================
# API CONFIGURATION
# =============================================================================
# OpenAlex
OPENALEX_API = "https://api.openalex.org/works"
OPENALEX_ECON_FILTER = "topics.field.id:20|14"
USER_EMAIL = "rob98@stanford.edu"
OA_PER_PAGE = 200
OA_RATE_LIMIT = 0.1
OA_MAX_WORKERS = 3

# Semantic Scholar
SS_API = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
SS_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
SS_FIELDS = "paperId,title,abstract,authors,year,citationCount,venue,publicationDate,isOpenAccess,openAccessPdf,s2FieldsOfStudy,publicationTypes"
SS_FIELDS_OF_STUDY = "Economics,Business"
SS_PER_PAGE = 1000
SS_RATE_LIMIT = 1.1
SS_MAX_WORKERS = 2

# NBER
NBER_API = "https://www.nber.org/api/v1/search"
NBER_PER_PAGE = 100
NBER_MAX_RESULTS = 10000
NBER_RATE_LIMIT = 0.2
NBER_MAX_WORKERS = 3

# CrossRef (for abstract recovery)
CROSSREF_API = "https://api.crossref.org/works"
CROSSREF_RATE_LIMIT = 0.1
RECOVERY_WORKERS = 5  # Parallel workers for abstract recovery

# =============================================================================
# RATE LIMITERS
# =============================================================================
class RateLimiter:
    """Thread-safe rate limiter for API requests."""
    def __init__(self, delay):
        self.delay = delay
        self.lock = threading.Lock()
        self.last_request = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()

oa_rate_limiter = RateLimiter(OA_RATE_LIMIT)
ss_rate_limiter = RateLimiter(SS_RATE_LIMIT)
nber_rate_limiter = RateLimiter(NBER_RATE_LIMIT)
crossref_rate_limiter = RateLimiter(CROSSREF_RATE_LIMIT)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def load_policies(policies_file):
    """Load policy configurations from CSV file."""
    if not os.path.exists(policies_file):
        print(f"ERROR: Policies file not found: {policies_file}")
        sys.exit(1)

    df = pd.read_csv(policies_file)
    required_cols = ['policy_name', 'policy_abbreviation', 'policy_year',
                     'policy_category', 'search_terms']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        sys.exit(1)

    print(f"Loaded {len(df)} policies from {policies_file}")
    return df


def normalize_title(title):
    """Normalize title for deduplication: lowercase, no punctuation, normalized whitespace."""
    if not title or pd.isna(title):
        return ''
    title = str(title).lower()
    title = re.sub(r'[^\w\s]', '', title)
    title = ' '.join(title.split())
    return title


def normalize_doi(doi):
    """Normalize DOI: strip URL prefix, lowercase."""
    if not doi or pd.isna(doi):
        return ''
    doi = str(doi).strip()
    doi = re.sub(r'^https?://doi\.org/', '', doi)
    return doi.lower()


def reconstruct_abstract(abstract_inverted_index):
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not abstract_inverted_index:
        return ''
    max_position = -1
    for positions in abstract_inverted_index.values():
        if positions:
            max_position = max(max_position, max(positions))
    if max_position < 0:
        return ''
    words = [''] * (max_position + 1)
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            words[pos] = word
    return ' '.join(words)


def classify_paper_type_openalex(work):
    """
    Classify paper as journal_article or working_paper from OpenAlex data.

    Parameters:
    -----------
    work : dict
        Raw OpenAlex work object

    Returns:
    --------
    str : 'journal_article' or 'working_paper'
    """
    pub_info = work.get('primary_location', {}) or {}
    source = pub_info.get('source', {}) or {}
    source_type = source.get('type', '')
    source_name = (source.get('display_name', '') or '').lower()

    if source_type == 'repository':
        return 'working_paper'
    if any(wp in source_name for wp in ['ssrn', 'nber', 'working paper', 'discussion paper', 'repec']):
        return 'working_paper'
    if source_type == 'journal':
        return 'journal_article'
    return 'journal_article'


def classify_paper_type_ss(paper):
    """
    Classify paper as journal_article or working_paper from Semantic Scholar data.

    Parameters:
    -----------
    paper : dict
        Raw Semantic Scholar paper object

    Returns:
    --------
    str : 'journal_article' or 'working_paper'
    """
    pub_types = paper.get('publicationTypes', []) or []
    venue = (paper.get('venue', '') or '').lower()

    if 'JournalArticle' in pub_types:
        if any(wp in venue for wp in ['ssrn', 'nber', 'working paper']):
            return 'working_paper'
        return 'journal_article'
    if 'Repository' in pub_types:
        return 'working_paper'
    if any(wp in venue for wp in ['ssrn', 'nber', 'working paper', 'discussion paper']):
        return 'working_paper'
    return 'journal_article'


def validate_acronym_matches(df):
    """
    For papers matched only by short all-caps acronyms (e.g., 'TCJA'),
    validate that the acronym appears case-sensitively as a whole word
    in the title or abstract. Drop papers that fail validation.

    Returns:
        (filtered_df, n_dropped)
    """
    if 'search_terms_matched' not in df.columns:
        return df, 0

    all_terms = set()
    for val in df['search_terms_matched'].dropna():
        for t in str(val).split('|'):
            all_terms.add(t.strip())
    acronyms = {t for t in all_terms if len(t) <= 4 and t == t.upper() and t.isalpha()}

    if not acronyms:
        return df, 0

    print(f"    Case-sensitive validation for acronyms: {acronyms}")

    text = df['title'].fillna('').astype(str) + ' ' + df['abstract'].fillna('').astype(str)

    acronym_found = pd.Series(False, index=df.index)
    for acr in acronyms:
        acronym_found = acronym_found | text.str.contains(
            r'\b' + re.escape(acr) + r'\b', regex=True, na=False
        )

    def has_regular_term(terms_str):
        terms = [t.strip() for t in str(terms_str).split('|')]
        return any(t not in acronyms for t in terms if t)

    has_regular = df['search_terms_matched'].apply(has_regular_term)
    keep = has_regular | acronym_found
    n_dropped = int((~keep).sum())

    if n_dropped > 0:
        print(f"    Acronym filter: dropped {n_dropped} of {len(df)} papers ({n_dropped/len(df)*100:.1f}%)")

    return df[keep].copy(), n_dropped


def filter_by_relevance(df, search_terms):
    """
    Filter papers by search term presence in title or abstract.
    Papers without abstracts are kept (can't verify relevance).

    Parameters:
    -----------
    df : pd.DataFrame
        Papers DataFrame
    search_terms : list
        List of search term strings

    Returns:
    --------
    pd.DataFrame : Filtered DataFrame
    """
    if len(df) == 0:
        return df

    def is_relevant(row):
        title = str(row.get('title', '')).lower()
        abstract = str(row.get('abstract', '')).lower()

        if not abstract or abstract in ('nan', '', 'none'):
            return True

        text = title + ' ' + abstract
        for term in search_terms:
            if term.lower() in text:
                return True
        return False

    mask = df.apply(is_relevant, axis=1)
    return df[mask].copy()


# =============================================================================
# OPENALEX SCRAPER (econ-filtered)
# =============================================================================
def search_openalex_econ(query, per_page=OA_PER_PAGE, from_year=None):
    """
    Search OpenAlex with economics/business topic filter using cursor pagination.

    Parameters:
    -----------
    query : str
        Search query string
    per_page : int
        Results per page (max 200)
    from_year : int or None
        If set, restrict to publications from this year onward

    Returns:
    --------
    list : List of raw work dicts from OpenAlex API
    """
    all_results = []
    cursor = '*'
    batch_num = 0

    # Build filter: econ topics + optional date
    oa_filter = OPENALEX_ECON_FILTER
    if from_year:
        oa_filter += f',from_publication_date:{from_year}-01-01'

    print(f"  [OpenAlex] Searching: '{query}' (filter: {oa_filter})")

    while True:
        params = {
            'search': query,
            'filter': oa_filter,
            'per_page': per_page,
            'cursor': cursor,
            'mailto': USER_EMAIL
        }

        try:
            oa_rate_limiter.wait()
            response = requests.get(OPENALEX_API, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            if not results:
                print(f"    [OA] [{query[:30]}...] No more results after {len(all_results)} total")
                break

            all_results.extend(results)
            batch_num += 1

            meta = data.get('meta', {})
            total_available = meta.get('count', '?')
            if batch_num <= 2 or batch_num % 5 == 0:
                print(f"    [OA] [{query[:30]}...] Batch {batch_num}: {len(results)} "
                      f"(total: {len(all_results)}/{total_available})")

            next_cursor = meta.get('next_cursor')
            if not next_cursor:
                print(f"    [OA] [{query[:30]}...] Reached end at {len(all_results)} results")
                break
            cursor = next_cursor

        except requests.exceptions.RequestException as e:
            print(f"    [OA] [{query[:30]}...] ERROR at batch {batch_num + 1}: {e}")
            break

    return all_results


def extract_openalex_paper(work):
    """Extract paper info from OpenAlex work object, including paper_type."""
    authors = []
    author_affiliations = []
    for authorship in work.get('authorships', []):
        author = authorship.get('author', {})
        author_name = author.get('display_name', '')
        if author_name:
            authors.append(author_name)
        institutions = authorship.get('institutions', [])
        if institutions:
            inst_names = [inst.get('display_name') or '' for inst in institutions]
            inst_names = [name for name in inst_names if name]
            author_affiliations.append('; '.join(inst_names) if inst_names else '')
        else:
            author_affiliations.append('')

    concepts = [c.get('display_name', '') for c in work.get('concepts', [])]
    pub_info = work.get('primary_location', {}) or {}
    source = pub_info.get('source', {}) or {}
    abstract_text = reconstruct_abstract(work.get('abstract_inverted_index', None))

    # Extract primary topic field
    primary_topic = work.get('primary_topic', {}) or {}
    field_info = primary_topic.get('field', {}) or {}
    primary_topic_field = field_info.get('display_name', '')

    return {
        'openalex_id': work.get('id', ''),
        'doi': work.get('doi', ''),
        'title': work.get('title', ''),
        'abstract': abstract_text,
        'publication_year': work.get('publication_year', ''),
        'publication_date': work.get('publication_date', ''),
        'authors': ' | '.join(authors),
        'author_count': len(authors),
        'author_affiliations': ' | '.join(author_affiliations),
        'source_name': source.get('display_name', ''),
        'source_type': source.get('type', ''),
        'is_open_access': work.get('open_access', {}).get('is_oa', False),
        'open_access_url': work.get('open_access', {}).get('oa_url', ''),
        'cited_by_count': work.get('cited_by_count', 0),
        'concepts': ' | '.join(concepts),
        'primary_topic_field': primary_topic_field,
        'type': work.get('type', ''),
        'language': work.get('language', ''),
        'paper_type': classify_paper_type_openalex(work),
        'url': work.get('id', ''),
        'data_source': 'OpenAlex'
    }


def scrape_openalex(search_terms, policy_abbr, from_year=None):
    """
    Scrape OpenAlex for all search terms with economics filter.

    Returns:
    --------
    pd.DataFrame : OpenAlex results with paper_type and search_terms_matched
    """
    all_papers = []
    results_lock = threading.Lock()

    def search_single_term(term):
        results = search_openalex_econ(term, from_year=from_year)
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_oa_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        papers = []
        for work in results:
            paper_info = extract_openalex_paper(work)
            paper_info['search_term'] = term
            papers.append(paper_info)
        print(f"    [OA] Extracted {len(papers)} papers for '{term}'")
        return papers

    print(f"\n  === OPENALEX (econ-filtered) ===")
    print(f"  Searching {len(search_terms)} terms (max {OA_MAX_WORKERS} workers)...")

    with ThreadPoolExecutor(max_workers=OA_MAX_WORKERS) as executor:
        future_to_term = {executor.submit(search_single_term, t): t for t in search_terms}
        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                papers = future.result()
                with results_lock:
                    all_papers.extend(papers)
            except Exception as e:
                print(f"    [OA] ERROR for '{term}': {e}")

    if not all_papers:
        return pd.DataFrame()

    df = pd.DataFrame(all_papers)

    # Aggregate search terms and dedup
    search_terms_agg = df.groupby('openalex_id')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['openalex_id', 'search_terms_matched']

    df_unique = df.drop_duplicates(subset=['openalex_id'], keep='first').copy()
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='openalex_id', how='left')

    print(f"  [OA] Total: {len(df)} | Unique: {len(df_unique)}")

    # Save per-source raw
    raw_file = os.path.join(TMP_DIR, f"{policy_abbr}_openalex_econ_raw.parquet")
    df_unique.to_parquet(raw_file, index=False, engine='pyarrow')
    print(f"  [OA] Saved: {raw_file}")

    return df_unique


# =============================================================================
# SEMANTIC SCHOLAR SCRAPER (econ-filtered)
# =============================================================================
def search_ss_econ(query, limit=SS_PER_PAGE, max_retries=3, from_year=None):
    """
    Search Semantic Scholar bulk API with Economics/Business field filter.

    Parameters:
    -----------
    query : str
        Search query string
    limit : int
        Results per page (up to 1000 for bulk)
    max_retries : int
        Max retries for rate limit errors
    from_year : int or None
        If set, restrict to publications from this year onward

    Returns:
    --------
    list : List of raw paper dicts
    """
    all_results = []
    continuation_token = None

    year_range = f"{from_year}-" if from_year else None
    print(f"  [SS] Searching: '{query}' (fieldsOfStudy={SS_FIELDS_OF_STUDY}"
          + (f", year={year_range}" if year_range else "") + ")")

    headers = {}
    if SS_API_KEY:
        headers['x-api-key'] = SS_API_KEY

    while True:
        params = {
            'query': query,
            'fields': SS_FIELDS,
            'limit': limit,
            'fieldsOfStudy': SS_FIELDS_OF_STUDY
        }
        if year_range:
            params['year'] = year_range
        if continuation_token is not None:
            params['token'] = continuation_token

        for retry in range(max_retries):
            try:
                ss_rate_limiter.wait()
                response = requests.get(SS_API, params=params, headers=headers)

                if response.status_code == 429:
                    wait_time = (2 ** retry) * 5
                    print(f"    [SS] Rate limited. Waiting {wait_time}s (retry {retry + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()

                results = data.get('data', [])
                if not results:
                    print(f"    [SS] No more results (fetched {len(all_results)} total)")
                    return all_results

                all_results.extend(results)
                total = data.get('total', 0)
                print(f"    [SS] Page: {len(results)} results (total: {len(all_results)}/{total})")

                continuation_token = data.get('token')
                if continuation_token is None:
                    return all_results

                break

            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = (2 ** retry) * 5
                    print(f"    [SS] ERROR: {e}. Waiting {wait_time}s (retry {retry + 1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    print(f"    [SS] ERROR retrieving results: {e}")
                    return all_results

    return all_results


def extract_ss_paper(paper):
    """Extract paper info from Semantic Scholar paper object, including paper_type."""
    authors = paper.get('authors', [])
    if isinstance(authors, list):
        author_names = [a.get('name', '') if isinstance(a, dict) else str(a) for a in authors]
        authors_str = ' | '.join(author_names)
    else:
        authors_str = str(authors) if authors else ''
        author_names = []

    oa_pdf = paper.get('openAccessPdf', {})
    oa_url = oa_pdf.get('url', '') if isinstance(oa_pdf, dict) else ''

    # Extract fields of study for tracking
    s2_fields = paper.get('s2FieldsOfStudy', []) or []
    fields_str = ' | '.join([f.get('category', '') for f in s2_fields if isinstance(f, dict)])

    return {
        'semantic_scholar_id': paper.get('paperId', ''),
        'title': paper.get('title', ''),
        'abstract': paper.get('abstract', ''),
        'authors': authors_str,
        'author_count': len(author_names),
        'publication_year': paper.get('year'),
        'publication_date': paper.get('publicationDate', ''),
        'venue': paper.get('venue', ''),
        'cited_by_count': paper.get('citationCount', 0),
        'is_open_access': paper.get('isOpenAccess', False),
        'open_access_url': oa_url,
        's2_fields_of_study': fields_str,
        'paper_type': classify_paper_type_ss(paper),
        'data_source': 'SemanticScholar'
    }


def scrape_semantic_scholar(search_terms, policy_abbr, from_year=None):
    """
    Scrape Semantic Scholar for all search terms with Economics/Business filter.

    Returns:
    --------
    pd.DataFrame : SS results with paper_type and search_terms_matched
    """
    all_papers = []
    results_lock = threading.Lock()

    def search_single_term(term):
        results = search_ss_econ(term, from_year=from_year)
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_ss_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        papers = []
        for p in results:
            paper_info = extract_ss_paper(p)
            paper_info['search_term'] = term
            papers.append(paper_info)
        print(f"    [SS] Extracted {len(papers)} papers for '{term}'")
        return papers

    print(f"\n  === SEMANTIC SCHOLAR (econ-filtered) ===")
    print(f"  Searching {len(search_terms)} terms (max {SS_MAX_WORKERS} workers)...")

    with ThreadPoolExecutor(max_workers=SS_MAX_WORKERS) as executor:
        future_to_term = {executor.submit(search_single_term, t): t for t in search_terms}
        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                papers = future.result()
                with results_lock:
                    all_papers.extend(papers)
            except Exception as e:
                print(f"    [SS] ERROR for '{term}': {e}")

    if not all_papers:
        return pd.DataFrame()

    df = pd.DataFrame(all_papers)

    # Aggregate search terms and dedup
    search_terms_agg = df.groupby('semantic_scholar_id')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['semantic_scholar_id', 'search_terms_matched']

    df_unique = df.drop_duplicates(subset=['semantic_scholar_id'], keep='first').copy()
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='semantic_scholar_id', how='left')

    print(f"  [SS] Total: {len(df)} | Unique: {len(df_unique)}")

    # Save per-source raw
    raw_file = os.path.join(TMP_DIR, f"{policy_abbr}_ss_econ_raw.parquet")
    df_unique.to_parquet(raw_file, index=False, engine='pyarrow')
    print(f"  [SS] Saved: {raw_file}")

    return df_unique


# =============================================================================
# NBER SCRAPER
# =============================================================================
def search_nber(query, per_page=NBER_PER_PAGE, max_results=NBER_MAX_RESULTS):
    """
    Search NBER API for papers matching the query.

    Parameters:
    -----------
    query : str
        Search query string
    per_page : int
        Results per page (max 100)
    max_results : int
        Maximum total results

    Returns:
    --------
    list : List of raw paper dicts
    """
    all_results = []
    page = 1

    print(f"  [NBER] Searching: '{query}'")

    while len(all_results) < max_results:
        params = {'q': query, 'page': page, 'perPage': per_page}

        try:
            nber_rate_limiter.wait()
            response = requests.get(NBER_API, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            if not results:
                print(f"    [NBER] [{query[:30]}...] No more results at page {page}")
                break

            all_results.extend(results)
            total = data.get('totalResults', 0)
            print(f"    [NBER] [{query[:30]}...] Page {page}: {len(results)} (total: {len(all_results)}/{total})")

            if page * per_page >= total:
                break
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"    [NBER] [{query[:30]}...] ERROR at page {page}: {e}")
            break

    return all_results[:max_results]


def extract_nber_paper(paper):
    """Extract paper info from NBER paper object."""
    authors = paper.get('authors', [])
    if isinstance(authors, list):
        author_names = [a.get('name', '') if isinstance(a, dict) else str(a) for a in authors]
        authors_str = ' | '.join(author_names)
    else:
        authors_str = str(authors)
        author_names = []

    display_date = paper.get('displaydate', '')
    publication_year = None
    if display_date:
        year_match = re.search(r'\b(19|20)\d{2}\b', display_date)
        if year_match:
            publication_year = int(year_match.group())

    return {
        'nber_id': paper.get('id', ''),
        'title': paper.get('title', ''),
        'abstract': paper.get('abstract', ''),
        'authors': authors_str,
        'author_count': len(author_names),
        'publication_date': display_date,
        'publication_year': publication_year,
        'url': paper.get('url', ''),
        'nber_type': paper.get('type', ''),
        'paper_type': 'working_paper',
        'data_source': 'NBER'
    }


def is_likely_person_name(title):
    """
    Detect if an NBER search result title is a person's name rather than a paper.

    NBER search sometimes returns author/people pages alongside papers.
    These waste time during abstract recovery and pollute results.

    Heuristics (all must be true):
    - Title is short (≤ 40 chars, ≤ 4 words)
    - No digits, colons, question marks, or parentheses
    - No common function words (the, a, of, in, on, for, and, or, etc.)
    - Every word starts with an uppercase letter

    This catches "Alex Kearing", "John Smith", "María García-López"
    but NOT "Tax Reform" (too short but... also caught — see non_name_words),
    "Economic Growth and Policy" (has "and"), "The TCJA" (has "The").
    """
    if not title or pd.isna(title):
        return False

    title = str(title).strip()
    if len(title) > 40:
        return False

    # No digits, colons, question marks, parens, semicolons
    if any(c in title for c in '0123456789:?!();[]'):
        return False

    words = title.split()
    if len(words) < 2 or len(words) > 5:
        return False

    # Function words and common academic terms that appear in paper titles
    # but not in personal names
    non_name_words = {
        'the', 'a', 'an', 'of', 'in', 'on', 'for', 'and', 'or', 'with',
        'to', 'at', 'by', 'is', 'are', 'was', 'were', 'not', 'no', 'its',
        'tax', 'policy', 'economic', 'economics', 'reform', 'act', 'market',
        'trade', 'labor', 'income', 'wage', 'price', 'rate', 'growth',
        'effect', 'effects', 'impact', 'analysis', 'evidence', 'model',
        'theory', 'review', 'new', 'public', 'private', 'fiscal', 'monetary',
        'financial', 'social', 'political', 'international', 'federal',
        'state', 'local', 'how', 'why', 'what', 'when', 'does', 'do',
        'can', 'from', 'about', 'between', 'under', 'over', 'after',
    }
    if any(w.lower() in non_name_words for w in words):
        return False

    # Every word should start with uppercase (name-like)
    for word in words:
        clean = word.strip('.,')
        if not clean:
            continue
        # Allow single-letter initials
        if len(clean) == 1:
            if not clean.isupper():
                return False
            continue
        # Allow hyphenated names (e.g., "García-López")
        parts = clean.split('-')
        for part in parts:
            if not part:
                continue
            if not part[0].isupper():
                return False

    return True


def scrape_nber(search_terms, policy_abbr, from_year=None):
    """
    Scrape NBER for all search terms. No economics filter needed.
    Post-filters by from_year if specified (NBER API has no year parameter).

    Returns:
    --------
    pd.DataFrame : NBER results with paper_type and search_terms_matched
    """
    all_papers = []
    results_lock = threading.Lock()

    def search_single_term(term):
        results = search_nber(term)
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_nber_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        papers = []
        for p in results:
            paper_info = extract_nber_paper(p)
            paper_info['search_term'] = term
            papers.append(paper_info)
        print(f"    [NBER] Extracted {len(papers)} papers for '{term}'")
        return papers

    print(f"\n  === NBER ===")
    print(f"  Searching {len(search_terms)} terms (max {NBER_MAX_WORKERS} workers)...")

    with ThreadPoolExecutor(max_workers=NBER_MAX_WORKERS) as executor:
        future_to_term = {executor.submit(search_single_term, t): t for t in search_terms}
        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                papers = future.result()
                with results_lock:
                    all_papers.extend(papers)
            except Exception as e:
                print(f"    [NBER] ERROR for '{term}': {e}")

    if not all_papers:
        return pd.DataFrame()

    df = pd.DataFrame(all_papers)

    # Dedup by normalized title (NBER has no stable paper ID across search terms)
    df['normalized_title'] = df['title'].apply(normalize_title)
    search_terms_agg = df.groupby('normalized_title')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['normalized_title', 'search_terms_matched']

    df_unique = df.drop_duplicates(subset=['normalized_title'], keep='first').copy()
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='normalized_title', how='left')

    # Filter out results that are person names (not papers)
    name_mask = df_unique['title'].apply(is_likely_person_name)
    n_names = name_mask.sum()
    if n_names > 0:
        sample = df_unique.loc[name_mask, 'title'].head(5).tolist()
        print(f"  [NBER] Filtered {n_names} likely person-name results (e.g., {sample})")
        df_unique = df_unique[~name_mask].copy()

    # Filter by publication year if from_year is set
    if from_year and 'publication_year' in df_unique.columns:
        pre_filter = len(df_unique)
        df_unique = df_unique[
            df_unique['publication_year'].fillna(0).astype(int) >= from_year
        ].copy()
        filtered_out = pre_filter - len(df_unique)
        if filtered_out > 0:
            print(f"  [NBER] Year filter (>= {from_year}): removed {filtered_out}, kept {len(df_unique)}")

    print(f"  [NBER] Total: {len(df)} | Unique (after filters): {len(df_unique)}")

    # Save per-source raw
    raw_file = os.path.join(TMP_DIR, f"{policy_abbr}_nber_econ_raw.parquet")
    df_unique.to_parquet(raw_file, index=False, engine='pyarrow')
    print(f"  [NBER] Saved: {raw_file}")

    return df_unique


# =============================================================================
# CROSS-SOURCE MERGE
# =============================================================================
def _fill_abstracts_from_source(merged, source_df, match_mask, source_name):
    """
    Vectorized abstract fill: for merged rows that matched a source and lack
    abstracts, pull abstracts from the source DataFrame via DOI/title lookup.
    """
    needs_abstract = match_mask & (merged['abstract'].fillna('').str.len() <= 50)
    if not needs_abstract.any():
        return

    # Build lookup dicts (DOI → abstract, title → abstract) from source
    src_has_abstract = source_df['abstract'].fillna('').str.len() > 50
    src_with_abs = source_df[src_has_abstract]
    if len(src_with_abs) == 0:
        return

    doi_lookup = {}
    if 'doi_norm' in src_with_abs.columns:
        for doi, abstract in zip(src_with_abs['doi_norm'], src_with_abs['abstract']):
            if doi and doi != '':
                doi_lookup[doi] = abstract

    title_lookup = {}
    if 'normalized_title' in src_with_abs.columns:
        for title, abstract in zip(src_with_abs['normalized_title'], src_with_abs['abstract']):
            if title and title != '':
                title_lookup[title] = abstract

    # Map abstracts onto merged rows that need them
    filled = 0
    for idx in merged.index[needs_abstract]:
        doi = merged.loc[idx, 'doi_norm'] if 'doi_norm' in merged.columns else ''
        abstract = doi_lookup.get(doi)
        if not abstract:
            title = merged.loc[idx, 'normalized_title']
            abstract = title_lookup.get(title)
        if abstract:
            merged.loc[idx, 'abstract'] = abstract
            merged.loc[idx, 'abstract_source'] = source_name
            filled += 1

    if filled > 0:
        print(f"    Filled {filled} abstracts from {source_name}")


def merge_sources(oa_df, ss_df, nber_df):
    """
    Merge papers from OpenAlex, Semantic Scholar, and NBER.
    Match by DOI (primary) and normalized title (fallback).
    Track which sources found each paper.

    Uses vectorized set-based matching instead of row-by-row iteration
    for performance with large datasets.

    Parameters:
    -----------
    oa_df : pd.DataFrame
        OpenAlex papers
    ss_df : pd.DataFrame
        Semantic Scholar papers
    nber_df : pd.DataFrame
        NBER papers

    Returns:
    --------
    pd.DataFrame : Merged dataset with data_sources column
    """
    print(f"\n  === MERGING ACROSS SOURCES ===")
    print(f"  OpenAlex: {len(oa_df)} | SS: {len(ss_df)} | NBER: {len(nber_df)}")

    # Normalize DOIs and titles across all sources
    for df in [oa_df, ss_df, nber_df]:
        if len(df) == 0:
            continue
        if 'doi' in df.columns:
            df['doi_norm'] = df['doi'].apply(normalize_doi)
        else:
            df['doi_norm'] = ''
        if 'normalized_title' not in df.columns:
            df['normalized_title'] = df['title'].apply(normalize_title)

    # Start with OpenAlex as the base
    merged = oa_df.copy() if len(oa_df) > 0 else pd.DataFrame()
    if len(merged) > 0:
        merged['data_sources'] = 'OpenAlex'
        merged['in_openalex'] = True
        merged['in_semantic_scholar'] = False
        merged['in_nber'] = False

    # Add Semantic Scholar papers (vectorized matching)
    if len(ss_df) > 0:
        if len(merged) > 0:
            # Identify which SS papers match merged (by DOI or title)
            merged_dois = set(merged['doi_norm'].dropna()) - {''}
            merged_titles = set(merged['normalized_title'].dropna()) - {''}
            ss_df_copy = ss_df.copy()
            ss_df_copy['matched_by_doi'] = ss_df_copy['doi_norm'].isin(merged_dois) & (ss_df_copy['doi_norm'] != '')
            ss_df_copy['matched_by_title'] = (~ss_df_copy['matched_by_doi']) & \
                                              ss_df_copy['normalized_title'].isin(merged_titles) & \
                                              (ss_df_copy['normalized_title'] != '')
            ss_is_matched = ss_df_copy['matched_by_doi'] | ss_df_copy['matched_by_title']
            ss_matched = ss_df_copy[ss_is_matched]
            ss_new = ss_df_copy[~ss_is_matched]

            # Vectorized: mark merged rows that have a matching SS paper
            ss_matched_dois = set(ss_matched.loc[ss_matched['matched_by_doi'], 'doi_norm'].dropna()) - {''}
            ss_matched_titles = set(ss_matched.loc[ss_matched['matched_by_title'], 'normalized_title'].dropna()) - {''}

            doi_match_mask = merged['doi_norm'].isin(ss_matched_dois) & (merged['doi_norm'] != '')
            title_match_mask = merged['normalized_title'].isin(ss_matched_titles) & \
                               (merged['normalized_title'] != '') & ~doi_match_mask
            ss_match_mask = doi_match_mask | title_match_mask

            merged.loc[ss_match_mask, 'in_semantic_scholar'] = True
            merged.loc[ss_match_mask, 'data_sources'] = \
                merged.loc[ss_match_mask, 'data_sources'] + ' | SemanticScholar'

            # Fill abstracts from SS where merged is missing (targeted loop on small subset)
            _fill_abstracts_from_source(merged, ss_matched, ss_match_mask, 'SemanticScholar')

            # Add new SS papers
            if len(ss_new) > 0:
                ss_new_std = ss_new.copy()
                ss_new_std['data_sources'] = 'SemanticScholar'
                ss_new_std['in_openalex'] = False
                ss_new_std['in_semantic_scholar'] = True
                ss_new_std['in_nber'] = False
                merged = pd.concat([merged, ss_new_std], ignore_index=True)

            print(f"  SS: {len(ss_matched)} matched, {len(ss_new)} new")
        else:
            merged = ss_df.copy()
            merged['data_sources'] = 'SemanticScholar'
            merged['in_openalex'] = False
            merged['in_semantic_scholar'] = True
            merged['in_nber'] = False

    # Add NBER papers (vectorized matching)
    if len(nber_df) > 0:
        if len(merged) > 0:
            merged_titles = set(merged['normalized_title'].dropna()) - {''}
            nber_df_copy = nber_df.copy()
            nber_df_copy['matched'] = nber_df_copy['normalized_title'].isin(merged_titles) & \
                                       (nber_df_copy['normalized_title'] != '')
            nber_matched = nber_df_copy[nber_df_copy['matched']]
            nber_new = nber_df_copy[~nber_df_copy['matched']]

            # Vectorized: mark merged rows that have a matching NBER paper
            nber_matched_titles = set(nber_matched['normalized_title'].dropna()) - {''}
            nber_match_mask = merged['normalized_title'].isin(nber_matched_titles) & \
                              (merged['normalized_title'] != '')

            merged.loc[nber_match_mask, 'in_nber'] = True
            merged.loc[nber_match_mask, 'data_sources'] = \
                merged.loc[nber_match_mask, 'data_sources'] + ' | NBER'

            # Fill abstracts from NBER where merged is missing
            _fill_abstracts_from_source(merged, nber_matched, nber_match_mask, 'NBER')

            # Add new NBER papers
            if len(nber_new) > 0:
                nber_new_std = nber_new.copy()
                nber_new_std['data_sources'] = 'NBER'
                nber_new_std['in_openalex'] = False
                nber_new_std['in_semantic_scholar'] = False
                nber_new_std['in_nber'] = True
                merged = pd.concat([merged, nber_new_std], ignore_index=True)

            print(f"  NBER: {len(nber_matched)} matched, {len(nber_new)} new")
        else:
            merged = nber_df.copy()
            merged['data_sources'] = 'NBER'
            merged['in_openalex'] = False
            merged['in_semantic_scholar'] = False
            merged['in_nber'] = True

    # Clean up temp columns
    for col in ['doi_norm', 'matched_by_doi', 'matched_by_title', 'matched']:
        if col in merged.columns:
            merged = merged.drop(columns=[col])

    print(f"  Merged total: {len(merged)}")
    return merged


# =============================================================================
# ABSTRACT RECOVERY (lightweight: CrossRef + SS API)
# =============================================================================
def recover_abstract_crossref(doi):
    """Recover abstract from CrossRef API using DOI."""
    if not doi or pd.isna(doi):
        return None, 'no_doi'

    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None, 'empty_doi'

    try:
        crossref_rate_limiter.wait()
        url = f"{CROSSREF_API}/{clean_doi}"
        response = requests.get(url, params={'mailto': USER_EMAIL}, timeout=15)

        if response.status_code == 404:
            return None, 'crossref_not_found'
        response.raise_for_status()

        data = response.json()
        work = data.get('message', {})
        abstract_html = work.get('abstract', '')

        if abstract_html:
            from bs4 import BeautifulSoup
            abstract = BeautifulSoup(abstract_html, 'html.parser').get_text(separator=' ').strip()
            if len(abstract) > 50:
                return abstract, None
        return None, 'crossref_no_abstract'

    except Exception as e:
        return None, f'crossref_error: {str(e)[:80]}'


def recover_abstract_ss(doi):
    """Recover abstract from Semantic Scholar API using DOI."""
    if not doi or pd.isna(doi):
        return None, 'no_doi'

    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None, 'empty_doi'

    try:
        ss_rate_limiter.wait()
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{clean_doi}"
        headers = {}
        if SS_API_KEY:
            headers['x-api-key'] = SS_API_KEY

        response = requests.get(url, params={'fields': 'abstract'}, headers=headers, timeout=15)

        if response.status_code == 404:
            return None, 'ss_not_found'
        response.raise_for_status()

        data = response.json()
        abstract = data.get('abstract', '')
        if abstract and len(abstract) > 50:
            return abstract, None
        return None, 'ss_no_abstract'

    except Exception as e:
        return None, f'ss_error: {str(e)[:80]}'


def recover_single_paper(doi):
    """
    Recover abstract for a single paper: try CrossRef, then Semantic Scholar.

    Returns:
    --------
    tuple : (abstract_text or None, source_name or None)
    """
    abstract, error = recover_abstract_crossref(doi)
    if abstract:
        return abstract, 'CrossRef'

    abstract, error = recover_abstract_ss(doi)
    if abstract:
        return abstract, 'SemanticScholar_recovery'

    return None, None


def recover_missing_abstracts(df, checkpoint_path=None):
    """
    Recover missing abstracts using CrossRef and Semantic Scholar APIs.
    Uses parallel workers for speed. Times first 20 papers to estimate total.
    Saves checkpoints periodically so recovery can resume after interruptions.

    Parameters:
    -----------
    df : pd.DataFrame
        Papers DataFrame with 'abstract' and 'doi' columns
    checkpoint_path : str or None
        Path to save periodic checkpoints during recovery. If None, no
        checkpoints are saved.

    Returns:
    --------
    pd.DataFrame : Updated DataFrame with recovered abstracts
    """
    if 'abstract_source' not in df.columns:
        df['abstract_source'] = ''

    # Mark existing abstracts
    has_abstract = df['abstract'].fillna('').astype(str).str.len() > 50
    df.loc[has_abstract & (df['abstract_source'] == ''), 'abstract_source'] = 'original'

    missing_mask = ~has_abstract & df['doi'].notna() & (df['doi'] != '')
    missing_indices = df.index[missing_mask].tolist()

    if not missing_indices:
        print(f"  No papers need abstract recovery (all {len(df)} have abstracts)")
        return df

    print(f"\n  === ABSTRACT RECOVERY ({RECOVERY_WORKERS} parallel workers) ===")
    print(f"  Papers missing abstracts with DOIs: {len(missing_indices)}")

    recovered = 0
    completed = 0
    timing_start = time.time()
    timing_reported = False
    results_lock = threading.Lock()

    def process_paper(idx):
        doi = df.loc[idx, 'doi']
        return idx, *recover_single_paper(doi)

    with ThreadPoolExecutor(max_workers=RECOVERY_WORKERS) as executor:
        futures = {executor.submit(process_paper, idx): idx for idx in missing_indices}

        for future in as_completed(futures):
            try:
                idx, abstract, source = future.result()
                with results_lock:
                    completed += 1
                    if abstract:
                        df.loc[idx, 'abstract'] = abstract
                        df.loc[idx, 'abstract_source'] = source
                        recovered += 1

                    # Timing estimate after first 20
                    if completed == 20 and not timing_reported:
                        elapsed = time.time() - timing_start
                        per_paper = elapsed / 20
                        total_estimate = per_paper * len(missing_indices)
                        print(f"  Timing: 20 papers in {elapsed:.1f}s ({per_paper:.2f}s/paper). "
                              f"Estimated total for {len(missing_indices)} papers: ~{total_estimate/60:.1f} min")
                        timing_reported = True

                    if completed % 50 == 0:
                        print(f"  Progress: {completed}/{len(missing_indices)} | Recovered: {recovered}")
                        # Save checkpoint every 50 papers
                        if checkpoint_path:
                            df.to_parquet(checkpoint_path, index=False, engine='pyarrow')
                            print(f"  [CHECKPOINT] Saved recovery progress ({completed}/{len(missing_indices)})")

            except Exception as e:
                print(f"  Recovery error: {e}")

    # Save final checkpoint before returning
    if checkpoint_path:
        df.to_parquet(checkpoint_path, index=False, engine='pyarrow')

    print(f"  Abstract recovery complete: {recovered}/{len(missing_indices)} recovered")
    return df


# =============================================================================
# MAIN PROCESS
# =============================================================================
def process_policy(policy_row, resume=False):
    """
    Process a single policy through all three econ-restricted sources.

    Parameters:
    -----------
    policy_row : pd.Series
        Row from policies DataFrame
    resume : bool
        If True, skip completed stages by loading checkpoints from tmp/

    Returns:
    --------
    dict : Processing summary statistics
    """
    policy_name = policy_row['policy_name']
    policy_abbr = policy_row['policy_abbreviation']
    policy_year = policy_row['policy_year']
    policy_category = policy_row['policy_category']
    search_terms = [t.strip() for t in policy_row['search_terms'].split('|')]

    print(f"\n{'='*80}")
    print(f"PIPELINE B: {policy_name} ({policy_abbr}) — Econ-Restricted APIs")
    print(f"Year: {policy_year} | Category: {policy_category}")
    print(f"Search terms: {search_terms}")
    if resume:
        print(f"Mode: RESUME (loading checkpoints where available)")
    print(f"{'='*80}")

    start_time = time.time()

    # Checkpoint paths
    oa_raw_path = os.path.join(TMP_DIR, f"{policy_abbr}_openalex_econ_raw.parquet")
    ss_raw_path = os.path.join(TMP_DIR, f"{policy_abbr}_ss_econ_raw.parquet")
    nber_raw_path = os.path.join(TMP_DIR, f"{policy_abbr}_nber_econ_raw.parquet")
    merged_checkpoint_path = os.path.join(TMP_DIR, f"{policy_abbr}_merged_pre_recovery.parquet")
    recovery_checkpoint_path = os.path.join(TMP_DIR, f"{policy_abbr}_recovery_checkpoint.parquet")
    raw_output_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_raw.parquet")
    filtered_output_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_filtered.parquet")

    # If final outputs already exist on resume, skip entirely
    if resume and os.path.exists(raw_output_path) and os.path.exists(filtered_output_path):
        print(f"  [RESUME] Final outputs already exist, skipping entirely")
        metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econ_apis_metadata.json")
        if os.path.exists(metadata_file):
            with open(metadata_file) as f:
                return json.load(f)
        return {'policy_abbreviation': policy_abbr, 'total_papers': 0, 'skipped': True}

    # Determine resume stage: can we skip scrape+merge?
    skip_to_recovery = False
    if resume:
        if os.path.exists(recovery_checkpoint_path):
            print(f"  [RESUME] Recovery checkpoint found — resuming abstract recovery")
            merged = pd.read_parquet(recovery_checkpoint_path)
            skip_to_recovery = True
        elif os.path.exists(merged_checkpoint_path):
            print(f"  [RESUME] Merged checkpoint found — skipping scrape + merge")
            merged = pd.read_parquet(merged_checkpoint_path)
            skip_to_recovery = True

    if not skip_to_recovery:
        # Step 1: Scrape all three sources (load cached or scrape fresh)
        oa_df = pd.DataFrame()
        ss_df = pd.DataFrame()
        nber_df = pd.DataFrame()

        # Check which sources have cached results
        from_year = int(policy_year)
        sources_to_scrape = {}
        if resume and os.path.exists(oa_raw_path):
            oa_df = pd.read_parquet(oa_raw_path)
            print(f"  [RESUME] Loaded OpenAlex from checkpoint ({len(oa_df)} papers)")
        else:
            sources_to_scrape['OpenAlex'] = scrape_openalex

        if resume and os.path.exists(ss_raw_path):
            ss_df = pd.read_parquet(ss_raw_path)
            print(f"  [RESUME] Loaded Semantic Scholar from checkpoint ({len(ss_df)} papers)")
        else:
            sources_to_scrape['SemanticScholar'] = scrape_semantic_scholar

        if resume and os.path.exists(nber_raw_path):
            nber_df = pd.read_parquet(nber_raw_path)
            print(f"  [RESUME] Loaded NBER from checkpoint ({len(nber_df)} papers)")
        else:
            sources_to_scrape['NBER'] = scrape_nber

        # Scrape remaining sources in parallel
        if sources_to_scrape:
            print(f"\n  Scraping {len(sources_to_scrape)} source(s): {list(sources_to_scrape.keys())}")
            print(f"  Publication year filter: >= {from_year}")
            with ThreadPoolExecutor(max_workers=len(sources_to_scrape)) as executor:
                futures = {
                    executor.submit(func, search_terms, policy_abbr, from_year=from_year): name
                    for name, func in sources_to_scrape.items()
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        if name == 'OpenAlex':
                            oa_df = result
                        elif name == 'SemanticScholar':
                            ss_df = result
                        else:
                            nber_df = result
                    except Exception as e:
                        print(f"  ERROR in {name}: {e}")
                        import traceback
                        traceback.print_exc()

        # Step 2: Merge across sources
        merged = merge_sources(oa_df, ss_df, nber_df)

    if not skip_to_recovery:
        if len(merged) == 0:
            print(f"\n  WARNING: No papers found for {policy_name}")
            return {'policy_abbreviation': policy_abbr, 'total_papers': 0}

        # Step 3: Acronym validation
        merged, acronym_filtered = validate_acronym_matches(merged)

        # Step 4: Filter by publication year
        print(f"\n  Filtering by publication year (>= {policy_year})...")
        pre_year = len(merged)
        merged['publication_year'] = pd.to_numeric(merged['publication_year'], errors='coerce')
        merged = merged[merged['publication_year'].isna() | (merged['publication_year'] >= policy_year)]
        year_filtered = pre_year - len(merged)
        print(f"    Before: {pre_year} | Filtered: {year_filtered} | After: {len(merged)}")

        # Add normalized_title if not present
        if 'normalized_title' not in merged.columns:
            merged['normalized_title'] = merged['title'].apply(normalize_title)

        # Add policy metadata
        merged['policy_studied'] = policy_name
        merged['policy_year'] = policy_year
        merged['policy_abbreviation'] = policy_abbr
        merged['policy_category'] = policy_category
        merged['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

        # Save merged checkpoint (before abstract recovery, so recovery can resume)
        merged.to_parquet(merged_checkpoint_path, index=False, engine='pyarrow')
        print(f"  Saved merged checkpoint: {merged_checkpoint_path}")
    else:
        acronym_filtered = 0
        year_filtered = 0

    # Step 5: Abstract recovery (with periodic checkpointing)
    merged = recover_missing_abstracts(merged, checkpoint_path=recovery_checkpoint_path)

    # Save raw (before relevance filtering)
    merged.to_parquet(raw_output_path, index=False, engine='pyarrow')
    print(f"\n  Saved RAW: {raw_output_path}")

    raw_csv = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_raw.csv")
    merged.to_csv(raw_csv, index=False, encoding='utf-8')
    print(f"  Saved RAW CSV: {raw_csv}")

    # Step 6: Relevance filtering
    print(f"\n  Applying relevance filtering (search terms in title/abstract)...")
    pre_relevance = len(merged)
    filtered = filter_by_relevance(merged, search_terms)
    relevance_filtered = pre_relevance - len(filtered)
    print(f"    Before: {pre_relevance} | Filtered: {relevance_filtered} | After: {len(filtered)}")

    # Apply acronym validation on filtered too
    filtered, _ = validate_acronym_matches(filtered)

    filtered.to_parquet(filtered_output_path, index=False, engine='pyarrow')
    print(f"  Saved FILTERED: {filtered_output_path}")

    filtered_csv = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_filtered.csv")
    filtered.to_csv(filtered_csv, index=False, encoding='utf-8')
    print(f"  Saved FILTERED CSV: {filtered_csv}")

    elapsed = time.time() - start_time

    # Abstract stats
    has_abstract_raw = merged['abstract'].fillna('').astype(str).str.len() > 50
    has_abstract_filtered = filtered['abstract'].fillna('').astype(str).str.len() > 50

    # Paper type breakdown
    raw_types = merged['paper_type'].value_counts().to_dict() if 'paper_type' in merged.columns else {}
    filtered_types = filtered['paper_type'].value_counts().to_dict() if 'paper_type' in filtered.columns else {}

    # Source breakdown
    source_counts = {
        'openalex': int(merged.get('in_openalex', pd.Series(dtype=bool)).sum()) if 'in_openalex' in merged.columns else 0,
        'semantic_scholar': int(merged.get('in_semantic_scholar', pd.Series(dtype=bool)).sum()) if 'in_semantic_scholar' in merged.columns else 0,
        'nber': int(merged.get('in_nber', pd.Series(dtype=bool)).sum()) if 'in_nber' in merged.columns else 0,
    }

    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': int(policy_year),
        'policy_category': policy_category,
        'search_terms': search_terms,
        'scrape_date': datetime.now().isoformat(),
        'elapsed_seconds': round(elapsed, 1),
        'openalex_filter': OPENALEX_ECON_FILTER,
        'ss_fields_of_study': SS_FIELDS_OF_STUDY,
        'source_counts': source_counts,
        'raw': {
            'total_papers': len(merged),
            'acronym_filtered': acronym_filtered,
            'year_filtered': year_filtered,
            'with_abstract': int(has_abstract_raw.sum()),
            'without_abstract': int((~has_abstract_raw).sum()),
            'abstract_pct': round(100 * has_abstract_raw.mean(), 1) if len(merged) > 0 else 0,
            'paper_types': raw_types,
        },
        'filtered': {
            'total_papers': len(filtered),
            'relevance_filtered': relevance_filtered,
            'with_abstract': int(has_abstract_filtered.sum()),
            'without_abstract': int((~has_abstract_filtered).sum()),
            'abstract_pct': round(100 * has_abstract_filtered.mean(), 1) if len(filtered) > 0 else 0,
            'paper_types': filtered_types,
        }
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econ_apis_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    # Clean up intermediate checkpoints on success
    for cp in [merged_checkpoint_path, recovery_checkpoint_path]:
        if os.path.exists(cp):
            os.remove(cp)
            print(f"  Cleaned up checkpoint: {os.path.basename(cp)}")

    # Print summary
    print(f"\n  {'='*60}")
    print(f"  SUMMARY: {policy_abbr} — Pipeline B (Econ-Restricted APIs)")
    print(f"  {'='*60}")
    print(f"  Sources: OA={source_counts['openalex']} | SS={source_counts['semantic_scholar']} | NBER={source_counts['nber']}")
    print(f"  Raw papers: {len(merged)}")
    print(f"  Filtered papers: {len(filtered)}")
    print(f"  Abstracts (raw): {has_abstract_raw.sum()}/{len(merged)} ({100*has_abstract_raw.mean():.1f}%)")
    print(f"  Paper types (raw): {raw_types}")
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)")

    return metadata


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Scrape economics papers from OpenAlex + SS + NBER (Pipeline B)')
    parser.add_argument('policies', nargs='*', default=[],
                       help='Policy abbreviations to process (default: all)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from checkpoints (skip completed stages)')
    args = parser.parse_args()

    print("=" * 80)
    print("PIPELINE B: Economics-Restricted API Scraping")
    print(f"OpenAlex filter: {OPENALEX_ECON_FILTER}")
    print(f"SS fieldsOfStudy: {SS_FIELDS_OF_STUDY}")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    policies_df = load_policies(POLICIES_FILE)

    # Filter to requested policies
    if args.policies:
        policies_df = policies_df[policies_df['policy_abbreviation'].isin(args.policies)]
        if len(policies_df) == 0:
            print(f"ERROR: No matching policies found for: {args.policies}")
            sys.exit(1)

    print(f"\nPolicies to process:")
    for _, row in policies_df.iterrows():
        print(f"  - {row['policy_name']} ({row['policy_abbreviation']})")

    all_metadata = []
    for _, row in policies_df.iterrows():
        try:
            metadata = process_policy(row, resume=args.resume)
            all_metadata.append(metadata)
        except Exception as e:
            print(f"\n  ERROR processing {row['policy_name']}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
