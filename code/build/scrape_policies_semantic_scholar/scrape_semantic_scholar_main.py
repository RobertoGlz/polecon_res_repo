"""
Script to scrape papers related to policies from Semantic Scholar.

Issue #12: Scrape papers from Semantic Scholar and compare with OpenAlex/NBER

This script searches Semantic Scholar for academic papers related to each policy
and compares coverage with OpenAlex and NBER results.

Pipeline Overview:
------------------
1. Load policy configurations from ../get_policies/output/policies.csv
2. For each policy, search Semantic Scholar using ALL search terms (pipe-separated in CSV)
3. Extract paper metadata (title, authors, abstract, year, etc.)
4. Deduplicate results, tracking ALL search terms that found each paper
5. Filter papers by publication date (must be >= policy year)
6. Save RAW results (no relevance filtering) to Parquet/CSV
7. Compare with OpenAlex and NBER results to check coverage

Key Implementation Notes:
-------------------------
- Pipeline is identical to OpenAlex and NBER scrapers for methodological consistency.
- Searches all search terms from policies.csv.
- Title normalization is used for deduplication and matching papers between sources.
- Papers published before the policy year are filtered out.
- Semantic Scholar API: GET https://api.semanticscholar.org/graph/v1/paper/search/bulk
- Uses bulk search endpoint (/paper/search/bulk) with token-based pagination (no 10K cap)
- API key required: Set SEMANTIC_SCHOLAR_API_KEY environment variable
- Rate limit: 1 request per second (enforced by thread-safe RateLimiter)
- Search terms are processed in parallel using ThreadPoolExecutor (MAX_WORKERS=2 due to strict rate limits)
- NO relevance filtering at this stage - filtering happens after abstract recovery.
- Tracks ALL search terms that found each paper (pipe-separated in search_terms_matched column).

Output Files:
-------------
- {abbr}_papers_semantic_scholar_raw.parquet: Raw dataset (efficient storage)
- {abbr}_papers_semantic_scholar_raw.csv: Raw dataset (compatibility)
- {abbr}_semantic_scholar_metadata.json: Scraping metadata and statistics

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
Updated: January 27, 2026 - Increased max_results to 1500, added relevance filtering
Updated: February 4, 2026 - Increased max_results to 10000, consistent config
Updated: February 4, 2026 - Removed relevance filtering, track all search terms, save raw data
Updated: February 6, 2026 - Switch to bulk API (no 10K cap), parallelize search terms with ThreadPoolExecutor
"""

import requests
import argparse
import json
import pandas as pd
import time
import re
from datetime import datetime
import os
import sys
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load environment variables from .env file
# Look for .env in the repo root (three levels up from this script)
REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
load_dotenv(os.path.join(REPO_ROOT, ".env"))

# Semantic Scholar API configuration (bulk endpoint for token-based pagination, no 10K cap)
SEMANTIC_SCHOLAR_API = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")

if not SEMANTIC_SCHOLAR_API_KEY:
    print("WARNING: SEMANTIC_SCHOLAR_API_KEY not found in environment.")
    print("Set it in .env file or as environment variable.")
    print("Requests will be unauthenticated (shared rate limit, may hit 429 errors).")

# Fields to request from API
API_FIELDS = "paperId,title,abstract,authors,year,citationCount,venue,publicationDate,isOpenAccess,openAccessPdf"

# =============================================================================
# SCRAPING CONFIGURATION - Keep consistent across all scrapers
# =============================================================================
MAX_RESULTS_PER_TERM = None   # No limit with token-based bulk pagination
PER_PAGE = 1000               # Results per API page (bulk endpoint allows up to 1000)
RATE_LIMIT_DELAY = 1.1        # Seconds between API requests (strict for SS)
MAX_WORKERS = 2               # Conservative due to SS strict rate limits

# Output paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Policies file location: ../get_policies/output/policies.csv
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv")
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

# OpenAlex and NBER output for comparison
OPENALEX_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_openalex", "output")
OPENALEX_OUTPUT_DIR = os.path.normpath(OPENALEX_OUTPUT_DIR)

NBER_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_nber", "output")
NBER_OUTPUT_DIR = os.path.normpath(NBER_OUTPUT_DIR)

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


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


rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def load_policies(policies_file):
    """
    Load policy configurations from CSV file.

    Expected CSV columns:
    - policy_name: Full name of the policy
    - policy_abbreviation: Short abbreviation for file naming
    - policy_year: Year the policy was enacted
    - policy_category: Category (e.g., tax, health, finance)
    - search_terms: Pipe-separated list of search terms

    Parameters:
    -----------
    policies_file : str
        Path to policies CSV file

    Returns:
    --------
    pd.DataFrame : DataFrame with policy configurations
    """
    if not os.path.exists(policies_file):
        print(f"ERROR: Policies file not found: {policies_file}")
        print(f"Please create {policies_file} with required columns:")
        print("  policy_name, policy_abbreviation, policy_year, policy_category, search_terms")
        sys.exit(1)

    df = pd.read_csv(policies_file)

    # Validate required columns
    required_cols = ['policy_name', 'policy_abbreviation', 'policy_year',
                     'policy_category', 'search_terms']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        sys.exit(1)

    print(f"Loaded {len(df)} policies from {policies_file}")
    return df


def search_semantic_scholar(query, limit=PER_PAGE, max_results=MAX_RESULTS_PER_TERM, max_retries=3):
    """
    Search Semantic Scholar for papers matching the query.

    Uses the Semantic Scholar bulk search endpoint with token-based pagination
    (no 10,000 result cap unlike the standard search endpoint).

    Parameters:
    -----------
    query : str
        Search query string
    limit : int
        Number of results per page (bulk endpoint allows up to 1000)
    max_results : int or None
        Maximum total number of results to retrieve (None for no limit)
    max_retries : int
        Maximum number of retries for rate limit errors

    Returns:
    --------
    list : List of paper dictionaries from Semantic Scholar API
    """
    all_results = []
    continuation_token = None

    print(f"  Searching Semantic Scholar for: '{query}'")

    # Set up headers with API key if available
    headers = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers['x-api-key'] = SEMANTIC_SCHOLAR_API_KEY

    while True:
        # Check max_results limit
        if max_results is not None and len(all_results) >= max_results:
            break

        params = {
            'query': query,
            'fields': API_FIELDS,
            'limit': limit
        }

        # Add continuation token for subsequent pages
        if continuation_token is not None:
            params['token'] = continuation_token

        # Retry logic for rate limit errors
        for retry in range(max_retries):
            try:
                rate_limiter.wait()
                response = requests.get(SEMANTIC_SCHOLAR_API, params=params, headers=headers)

                # Handle rate limit (429) with exponential backoff
                if response.status_code == 429:
                    wait_time = (2 ** retry) * 5  # 5, 10, 20 seconds
                    print(f"    Rate limited. Waiting {wait_time}s before retry {retry + 1}/{max_retries}...")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()

                results = data.get('data', [])
                if not results:
                    print(f"    No more results (fetched {len(all_results)} total)")
                    if max_results is not None:
                        return all_results[:max_results]
                    return all_results

                all_results.extend(results)
                total = data.get('total', 0)
                print(f"    Page: {len(results)} results (total: {len(all_results)}/{total})")

                # Get continuation token for next page
                continuation_token = data.get('token')
                if continuation_token is None:
                    # No more pages
                    if max_results is not None:
                        return all_results[:max_results]
                    return all_results

                break  # Success, exit retry loop

            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = (2 ** retry) * 5
                    print(f"    ERROR: {e}. Waiting {wait_time}s before retry {retry + 1}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    print(f"    ERROR retrieving results: {e}")
                    if max_results is not None:
                        return all_results[:max_results]
                    return all_results

    if max_results is not None:
        return all_results[:max_results]
    return all_results


def extract_paper_info(paper):
    """
    Extract relevant information from Semantic Scholar paper object.

    Parameters:
    -----------
    paper : dict
        Semantic Scholar paper object from API response

    Returns:
    --------
    dict : Extracted paper information
    """
    # Extract authors
    authors = paper.get('authors', [])
    if isinstance(authors, list):
        author_names = [a.get('name', '') if isinstance(a, dict) else str(a) for a in authors]
        authors_str = ' | '.join(author_names)
    else:
        authors_str = str(authors) if authors else ''

    # Extract open access PDF URL
    oa_pdf = paper.get('openAccessPdf', {})
    oa_url = oa_pdf.get('url', '') if isinstance(oa_pdf, dict) else ''

    paper_info = {
        'semantic_scholar_id': paper.get('paperId', ''),
        'title': paper.get('title', ''),
        'abstract': paper.get('abstract', ''),
        'authors': authors_str,
        'author_count': len(author_names) if isinstance(authors, list) else 0,
        'publication_year': paper.get('year'),
        'publication_date': paper.get('publicationDate', ''),
        'venue': paper.get('venue', ''),
        'cited_by_count': paper.get('citationCount', 0),
        'is_open_access': paper.get('isOpenAccess', False),
        'open_access_url': oa_url,
        'data_source': 'SemanticScholar'
    }

    return paper_info


def normalize_title(title):
    """
    Normalize title for comparison.

    Parameters:
    -----------
    title : str
        Paper title

    Returns:
    --------
    str : Normalized title (lowercase, no punctuation, normalized whitespace)
    """
    if not title or pd.isna(title):
        return ''
    title = str(title).lower()
    title = re.sub(r'[^\w\s]', '', title)  # Remove punctuation
    title = ' '.join(title.split())  # Normalize whitespace
    return title


def validate_acronym_matches(df):
    """
    For papers matched only by short all-caps acronyms (e.g., 'ACA'),
    validate that the acronym appears case-sensitively as a whole word
    in the title or abstract. Drop papers that fail validation.

    Returns:
        (filtered_df, n_dropped)
    """
    if 'search_terms_matched' not in df.columns:
        return df, 0

    # Identify which search terms are short all-caps acronyms
    all_terms = set()
    for val in df['search_terms_matched'].dropna():
        for t in str(val).split('|'):
            all_terms.add(t.strip())
    acronyms = {t for t in all_terms if len(t) <= 4 and t == t.upper() and t.isalpha()}

    if not acronyms:
        return df, 0

    print(f"    Case-sensitive validation for acronyms: {acronyms}")

    # Build combined text from title + abstract
    text = df['title'].fillna('').astype(str) + ' ' + df['abstract'].fillna('').astype(str)

    # Check if any acronym appears case-sensitively as a whole word
    acronym_found = pd.Series(False, index=df.index)
    for acr in acronyms:
        acronym_found = acronym_found | text.str.contains(
            r'\b' + re.escape(acr) + r'\b', regex=True, na=False
        )

    # Check if paper has any non-acronym search term (always keep these)
    def has_regular_term(terms_str):
        terms = [t.strip() for t in str(terms_str).split('|')]
        return any(t not in acronyms for t in terms if t)

    has_regular = df['search_terms_matched'].apply(has_regular_term)

    # Keep: has a regular term OR passes case-sensitive check
    keep = has_regular | acronym_found
    n_dropped = int((~keep).sum())

    if n_dropped > 0:
        print(f"    Acronym filter: dropped {n_dropped} of {len(df)} papers ({n_dropped/len(df)*100:.1f}%)")

    return df[keep].copy(), n_dropped


def compare_with_openalex(policy_abbr):
    """
    Compare Semantic Scholar papers with OpenAlex coverage.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Comparison statistics
    """
    print(f"\n  Comparing Semantic Scholar vs OpenAlex for {policy_abbr}...")

    # Load Semantic Scholar papers (try raw first, then regular)
    ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.csv")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.csv")

    if not os.path.exists(ss_file):
        print(f"    ERROR: Semantic Scholar file not found for {policy_abbr}")
        return None

    if ss_file.endswith('.parquet'):
        ss_df = pd.read_parquet(ss_file)
    else:
        ss_df = pd.read_csv(ss_file)

    # Load OpenAlex papers (try raw first, then regular)
    openalex_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.parquet")
    if not os.path.exists(openalex_file):
        openalex_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.csv")
    if not os.path.exists(openalex_file):
        openalex_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet")
    if not os.path.exists(openalex_file):
        openalex_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex.csv")

    if not os.path.exists(openalex_file):
        print(f"    ERROR: OpenAlex file not found for {policy_abbr}")
        return None

    if openalex_file.endswith('.parquet'):
        openalex_df = pd.read_parquet(openalex_file)
    else:
        openalex_df = pd.read_csv(openalex_file)

    print(f"    Semantic Scholar papers: {len(ss_df)}")
    print(f"    OpenAlex papers: {len(openalex_df)}")

    # Normalize titles for matching
    ss_df['normalized_title'] = ss_df['title'].apply(normalize_title)
    openalex_df['normalized_title'] = openalex_df['title'].apply(normalize_title)

    # Create sets for fast lookup
    ss_titles = set(ss_df['normalized_title'].dropna())
    openalex_titles = set(openalex_df['normalized_title'].dropna())

    # Add indicators
    ss_df['in_openalex'] = ss_df['normalized_title'].isin(openalex_titles)
    openalex_df['in_semantic_scholar'] = openalex_df['normalized_title'].isin(ss_titles)

    # Calculate statistics
    ss_in_openalex = ss_df['in_openalex'].sum()
    openalex_in_ss = openalex_df['in_semantic_scholar'].sum()

    print(f"    Semantic Scholar papers also in OpenAlex: {ss_in_openalex} ({100*ss_in_openalex/len(ss_df):.1f}%)")
    print(f"    OpenAlex papers also in Semantic Scholar: {openalex_in_ss} ({100*openalex_in_ss/len(openalex_df):.1f}%)")

    # Save with indicators
    ss_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_semantic_scholar_openalex_indicator.csv")
    ss_df.to_csv(ss_indicator_file, index=False)
    print(f"    Saved: {ss_indicator_file}")

    openalex_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_semantic_scholar_indicator.csv")
    openalex_df.to_csv(openalex_indicator_file, index=False)
    print(f"    Saved: {openalex_indicator_file}")

    return {
        'policy_abbr': policy_abbr,
        'semantic_scholar_total': len(ss_df),
        'openalex_total': len(openalex_df),
        'ss_in_openalex': int(ss_in_openalex),
        'ss_in_openalex_pct': 100 * ss_in_openalex / len(ss_df) if len(ss_df) > 0 else 0,
        'openalex_in_ss': int(openalex_in_ss),
        'openalex_in_ss_pct': 100 * openalex_in_ss / len(openalex_df) if len(openalex_df) > 0 else 0
    }


def compare_with_nber(policy_abbr):
    """
    Compare Semantic Scholar papers with NBER coverage.

    Only compares against papers found in NBER (the NBER subset).

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Comparison statistics
    """
    print(f"\n  Comparing Semantic Scholar vs NBER for {policy_abbr}...")

    # Load Semantic Scholar papers (try raw first, then regular)
    ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.csv")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.csv")

    if not os.path.exists(ss_file):
        print(f"    ERROR: Semantic Scholar file not found for {policy_abbr}")
        return None

    if ss_file.endswith('.parquet'):
        ss_df = pd.read_parquet(ss_file)
    else:
        ss_df = pd.read_csv(ss_file)

    # Load NBER papers (try raw first, then regular)
    nber_file = os.path.join(NBER_OUTPUT_DIR, f"{policy_abbr}_papers_nber_raw.parquet")
    if not os.path.exists(nber_file):
        nber_file = os.path.join(NBER_OUTPUT_DIR, f"{policy_abbr}_papers_nber_raw.csv")
    if not os.path.exists(nber_file):
        nber_file = os.path.join(NBER_OUTPUT_DIR, f"{policy_abbr}_papers_nber.parquet")
    if not os.path.exists(nber_file):
        nber_file = os.path.join(NBER_OUTPUT_DIR, f"{policy_abbr}_papers_nber.csv")

    if not os.path.exists(nber_file):
        print(f"    ERROR: NBER file not found for {policy_abbr}")
        return None

    if nber_file.endswith('.parquet'):
        nber_df = pd.read_parquet(nber_file)
    else:
        nber_df = pd.read_csv(nber_file)

    print(f"    Semantic Scholar papers: {len(ss_df)}")
    print(f"    NBER papers: {len(nber_df)}")

    # Normalize titles for matching
    ss_df['normalized_title'] = ss_df['title'].apply(normalize_title)
    nber_df['normalized_title'] = nber_df['title'].apply(normalize_title)

    # Create sets for fast lookup
    ss_titles = set(ss_df['normalized_title'].dropna())
    nber_titles = set(nber_df['normalized_title'].dropna())

    # Add indicators
    ss_df['in_nber'] = ss_df['normalized_title'].isin(nber_titles)
    nber_df['in_semantic_scholar'] = nber_df['normalized_title'].isin(ss_titles)

    # Calculate statistics
    ss_in_nber = ss_df['in_nber'].sum()
    nber_in_ss = nber_df['in_semantic_scholar'].sum()

    print(f"    Semantic Scholar papers also in NBER: {ss_in_nber} ({100*ss_in_nber/len(ss_df):.1f}%)")
    print(f"    NBER papers also in Semantic Scholar: {nber_in_ss} ({100*nber_in_ss/len(nber_df):.1f}%)")

    # Show NBER papers NOT in Semantic Scholar (important for coverage analysis)
    nber_not_in_ss = nber_df[~nber_df['in_semantic_scholar']]
    if len(nber_not_in_ss) > 0:
        print(f"    WARNING: {len(nber_not_in_ss)} NBER papers NOT in Semantic Scholar:")
        for _, row in nber_not_in_ss.head(5).iterrows():
            print(f"      - {row['title'][:60]}...")

    # Save with indicators
    ss_nber_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_semantic_scholar_nber_indicator.csv")
    ss_df.to_csv(ss_nber_indicator_file, index=False)
    print(f"    Saved: {ss_nber_indicator_file}")

    nber_ss_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_nber_semantic_scholar_indicator.csv")
    nber_df.to_csv(nber_ss_indicator_file, index=False)
    print(f"    Saved: {nber_ss_indicator_file}")

    return {
        'policy_abbr': policy_abbr,
        'semantic_scholar_total': len(ss_df),
        'nber_total': len(nber_df),
        'ss_in_nber': int(ss_in_nber),
        'ss_in_nber_pct': 100 * ss_in_nber / len(ss_df) if len(ss_df) > 0 else 0,
        'nber_in_ss': int(nber_in_ss),
        'nber_in_ss_pct': 100 * nber_in_ss / len(nber_df) if len(nber_df) > 0 else 0,
        'nber_missing_from_ss': len(nber_not_in_ss)
    }


def process_policy(policy_row):
    """
    Process a single policy: search Semantic Scholar and save results.

    Parameters:
    -----------
    policy_row : pd.Series
        Row from policies DataFrame

    Returns:
    --------
    dict : Summary statistics
    """
    policy_name = policy_row['policy_name']
    policy_abbr = policy_row['policy_abbreviation']
    policy_year = int(policy_row['policy_year'])
    policy_category = policy_row['policy_category']
    search_terms_str = policy_row['search_terms']

    # Parse search terms (pipe-separated)
    search_terms = [term.strip() for term in search_terms_str.split('|')]

    print(f"\n{'='*80}")
    print(f"Processing: {policy_name} ({policy_abbr})")
    print(f"Year: {policy_year} | Category: {policy_category}")
    print(f"Search terms: {len(search_terms)}")
    print(f"{'='*80}")

    all_papers = []
    search_metadata = []
    results_lock = threading.Lock()

    def search_single_term(term):
        """Search for a single term and return results with metadata."""
        results = search_semantic_scholar(term, limit=PER_PAGE, max_results=MAX_RESULTS_PER_TERM)

        # Save raw results for this term
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"    Saved raw results to: {raw_file}")

        # Extract paper info
        papers = []
        for paper in results:
            paper_info = extract_paper_info(paper)
            paper_info['search_term'] = term
            paper_info['policy_studied'] = policy_name
            paper_info['policy_year'] = policy_year
            paper_info['policy_abbreviation'] = policy_abbr
            paper_info['policy_category'] = policy_category
            paper_info['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
            papers.append(paper_info)

        metadata = {
            'search_term': term,
            'results_count': len(results),
            'timestamp': datetime.now().isoformat()
        }

        print(f"    Extracted info from {len(results)} papers for '{term}'")
        return papers, metadata

    # Search for each term in parallel
    print(f"\n  Searching {len(search_terms)} terms in parallel (max {MAX_WORKERS} workers)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_term = {executor.submit(search_single_term, term): term for term in search_terms}

        for future in as_completed(future_to_term):
            term = future_to_term[future]
            try:
                papers, metadata = future.result()
                with results_lock:
                    all_papers.extend(papers)
                    search_metadata.append(metadata)
            except Exception as e:
                print(f"    ERROR processing term '{term}': {e}")

    # Create DataFrame
    df = pd.DataFrame(all_papers)

    if len(df) == 0:
        print(f"\n  WARNING: No papers found for {policy_name}")
        return {
            'policy_abbreviation': policy_abbr,
            'policy_name': policy_name,
            'total_papers': 0,
            'duplicates_removed': 0,
            'pre_policy_filtered': 0,
            'unique_papers_raw': 0
        }

    initial_count = len(df)
    print(f"\n  Total papers found: {initial_count}")

    # Aggregate all search terms for each paper (instead of keeping just the first)
    print(f"\n  Aggregating search terms and removing duplicates...")
    search_terms_agg = df.groupby('semantic_scholar_id')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['semantic_scholar_id', 'search_terms_matched']

    # Keep first occurrence of each paper (for other columns)
    df_unique = df.drop_duplicates(subset=['semantic_scholar_id'], keep='first').copy()

    # Merge aggregated search terms
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='semantic_scholar_id', how='left')

    duplicate_count = initial_count - len(df_unique)
    print(f"    Initial: {initial_count} | Duplicates: {duplicate_count} | Unique: {len(df_unique)}")

    # Case-sensitive validation for short acronym search terms
    df_unique, acronym_filtered_count = validate_acronym_matches(df_unique)

    # Filter out papers published before the policy year
    print(f"\n  Filtering papers by publication date (>= {policy_year})...")
    pre_filter_count = len(df_unique)
    # Keep papers where publication_year >= policy_year, or where publication_year is None (unknown)
    df_unique = df_unique[
        (df_unique['publication_year'].isna()) |
        (df_unique['publication_year'] >= policy_year)
    ]
    filtered_count = pre_filter_count - len(df_unique)
    print(f"    Before filter: {pre_filter_count} | Filtered out: {filtered_count} | After filter: {len(df_unique)}")

    # NO relevance filtering at scrape stage - will be done after abstract recovery
    print(f"\n  Skipping relevance filtering (will be applied after abstract recovery)")

    # Add normalized title for comparison
    df_unique['normalized_title'] = df_unique['title'].apply(normalize_title)

    # Reorder columns (search_terms_matched contains all matched terms)
    column_order = [
        'semantic_scholar_id', 'title', 'authors', 'author_count',
        'publication_year', 'publication_date', 'abstract', 'venue',
        'cited_by_count', 'is_open_access', 'open_access_url',
        'search_terms_matched', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'normalized_title'
    ]
    df_unique = df_unique[[c for c in column_order if c in df_unique.columns]]

    # Save RAW outputs (no relevance filtering applied)
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved RAW Parquet: {parquet_file}")

    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.csv")
    df_unique.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved RAW CSV: {csv_file}")

    # Save metadata
    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': policy_year,
        'policy_category': policy_category,
        'search_terms': search_terms,
        'scrape_date': datetime.now().isoformat(),
        'total_papers_found': initial_count,
        'duplicates_removed': duplicate_count,
        'acronym_filtered': acronym_filtered_count,
        'pre_policy_filtered': filtered_count,
        'unique_papers_raw': len(df_unique),
        'note': 'Raw data without relevance filtering. Filtering applied after abstract recovery.',
        'search_details': search_metadata
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_semantic_scholar_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    # Summary
    print(f"\n  SUMMARY for {policy_abbr} (RAW DATA):")
    print(f"    Unique papers (raw): {len(df_unique)}")
    if len(df_unique) > 0 and df_unique['publication_year'].notna().any():
        years = df_unique['publication_year'].dropna()
        print(f"    Date range: {int(years.min())}-{int(years.max())}")
    abstracts_count = (df_unique['abstract'].notna() & (df_unique['abstract'] != '')).sum()
    print(f"    Papers with abstracts: {abstracts_count} ({100*abstracts_count/len(df_unique):.1f}%)")

    return {
        'policy_abbreviation': policy_abbr,
        'policy_name': policy_name,
        'total_papers': initial_count,
        'duplicates_removed': duplicate_count,
        'pre_policy_filtered': filtered_count,
        'unique_papers_raw': len(df_unique)
    }


def is_policy_complete(policy_abbr, source_name):
    """Check if a policy was already completed today (for --resume mode)."""
    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_{source_name}_metadata.json")
    if not os.path.exists(metadata_file):
        return False
    mod_time = datetime.fromtimestamp(os.path.getmtime(metadata_file))
    return mod_time.date() == datetime.now().date()


def main():
    """
    Main execution function
    """
    parser = argparse.ArgumentParser(description="Semantic Scholar paper scraping and comparison")
    parser.add_argument('policies', nargs='*', help='Policy abbreviations to process (default: all)')
    parser.add_argument('--resume', action='store_true', help='Skip policies already completed today')
    args = parser.parse_args()

    print("="*80)
    print("SEMANTIC SCHOLAR PAPER SCRAPING AND COMPARISON")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.resume:
        print("  Mode: RESUME (skipping policies completed today)")
    print()

    # Load policies configuration
    policies_df = load_policies(POLICIES_FILE)

    # Filter to requested policies if specified
    if args.policies:
        policies_df = policies_df[policies_df['policy_abbreviation'].isin(args.policies)]
        if policies_df.empty:
            print(f"ERROR: No matching policies found for {args.policies}")
            return

    print(f"\nPolicies to process:")
    for _, row in policies_df.iterrows():
        print(f"  - {row['policy_name']} ({row['policy_abbreviation']})")

    # Process each policy
    all_summaries = []
    for idx, row in policies_df.iterrows():
        policy_abbr = row['policy_abbreviation']

        # Check checkpoint in resume mode
        if args.resume and is_policy_complete(policy_abbr, 'semantic_scholar'):
            print(f"\n  SKIP {row['policy_name']} — already completed today (--resume)")
            continue

        try:
            summary = process_policy(row)
            all_summaries.append(summary)
        except Exception as e:
            print(f"\n  ERROR processing {row['policy_name']}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Compare with OpenAlex
    print(f"\n{'='*80}")
    print("COMPARING WITH OPENALEX")
    print(f"{'='*80}")

    openalex_comparison_results = []
    for _, row in policies_df.iterrows():
        policy_abbr = row['policy_abbreviation']
        try:
            comparison = compare_with_openalex(policy_abbr)
            if comparison:
                openalex_comparison_results.append(comparison)
        except Exception as e:
            print(f"  ERROR comparing {policy_abbr} with OpenAlex: {e}")
            import traceback
            traceback.print_exc()

    # Save OpenAlex comparison results
    if openalex_comparison_results:
        comparison_df = pd.DataFrame(openalex_comparison_results)
        comparison_file = os.path.join(OUTPUT_DIR, "semantic_scholar_openalex_comparison.csv")
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\nSaved OpenAlex comparison: {comparison_file}")

    # Compare with NBER
    print(f"\n{'='*80}")
    print("COMPARING WITH NBER")
    print(f"{'='*80}")

    nber_comparison_results = []
    for _, row in policies_df.iterrows():
        policy_abbr = row['policy_abbreviation']
        try:
            comparison = compare_with_nber(policy_abbr)
            if comparison:
                nber_comparison_results.append(comparison)
        except Exception as e:
            print(f"  ERROR comparing {policy_abbr} with NBER: {e}")
            import traceback
            traceback.print_exc()

    # Save NBER comparison results
    if nber_comparison_results:
        comparison_df = pd.DataFrame(nber_comparison_results)
        comparison_file = os.path.join(OUTPUT_DIR, "semantic_scholar_nber_comparison.csv")
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\nSaved NBER comparison: {comparison_file}")

    # Save combined comparison report
    comparison_report = {
        'scrape_date': datetime.now().isoformat(),
        'openalex_comparison': openalex_comparison_results,
        'nber_comparison': nber_comparison_results
    }
    report_file = os.path.join(OUTPUT_DIR, "semantic_scholar_comparison_report.json")
    with open(report_file, 'w') as f:
        json.dump(comparison_report, f, indent=2)
    print(f"Saved comparison report: {report_file}")

    # Create overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")

    if all_summaries:
        summary_df = pd.DataFrame(all_summaries)
        print(summary_df.to_string(index=False))

        # Save overall summary
        summary_file = os.path.join(OUTPUT_DIR, "semantic_scholar_scrape_summary.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSaved overall summary: {summary_file}")
    else:
        print("  No policies were processed (all skipped or failed).")

    # Print comparison results
    if openalex_comparison_results:
        print(f"\n{'='*80}")
        print("SEMANTIC SCHOLAR vs OPENALEX COVERAGE")
        print(f"{'='*80}")
        for result in openalex_comparison_results:
            print(f"\n{result['policy_abbr']}:")
            print(f"  Semantic Scholar papers: {result['semantic_scholar_total']}")
            print(f"  OpenAlex papers: {result['openalex_total']}")
            print(f"  SS in OpenAlex: {result['ss_in_openalex']} ({result['ss_in_openalex_pct']:.1f}%)")
            print(f"  OpenAlex in SS: {result['openalex_in_ss']} ({result['openalex_in_ss_pct']:.1f}%)")

    if nber_comparison_results:
        print(f"\n{'='*80}")
        print("SEMANTIC SCHOLAR vs NBER COVERAGE")
        print(f"{'='*80}")
        for result in nber_comparison_results:
            print(f"\n{result['policy_abbr']}:")
            print(f"  Semantic Scholar papers: {result['semantic_scholar_total']}")
            print(f"  NBER papers: {result['nber_total']}")
            print(f"  SS in NBER: {result['ss_in_nber']} ({result['ss_in_nber_pct']:.1f}%)")
            print(f"  NBER in SS: {result['nber_in_ss']} ({result['nber_in_ss_pct']:.1f}%)")
            if result.get('nber_missing_from_ss', 0) > 0:
                print(f"  *** {result['nber_missing_from_ss']} NBER papers MISSING from Semantic Scholar ***")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
