"""
Script to scrape papers related to policies from OpenAlex.

Issue #1: Scrape policy papers from OpenAlex

This script reads a list of policies from policies.csv and systematically
searches OpenAlex for academic papers related to each policy.

Pipeline Overview:
------------------
1. Load policy configurations from ../get_policies/output/policies.csv
2. For each policy, search OpenAlex using configured search terms (in parallel)
3. Extract paper metadata (title, authors, abstract, citations, etc.)
4. Deduplicate results, tracking ALL search terms that found each paper
5. Save RAW results (no relevance filtering) to Parquet/CSV formats

Key Implementation Notes:
-------------------------
- OpenAlex stores abstracts as "inverted indices" (word -> positions mapping),
  not plain text. The reconstruct_abstract() function handles this conversion.
- Uses OpenAlex "polite pool" (via mailto parameter) for better rate limits.
- Raw API responses are saved to tmp/ for debugging and reproducibility.
- Search terms are processed in parallel using ThreadPoolExecutor for speed.
- NO relevance filtering at this stage - filtering happens after abstract recovery.
- Tracks ALL search terms that found each paper (pipe-separated in search_terms column).

Output Files:
-------------
- {abbr}_papers_openalex_raw.parquet: Raw dataset (efficient storage)
- {abbr}_papers_openalex_raw.csv: Raw dataset (compatibility)
- {abbr}_openalex_metadata.json: Scraping metadata and statistics

Author: claude ai with modifications by roberto gonzalez
Date: January 9, 2026
Updated: January 14, 2026 - Fixed abstract extraction from inverted index
Updated: January 27, 2026 - Increased max_results to 1500, added relevance filtering
Updated: February 4, 2026 - Increased max_results to 10000, added parallel processing
Updated: February 4, 2026 - Removed relevance filtering, track all search terms, save raw data
"""

import argparse
import requests
import json
import pandas as pd
import time
from datetime import datetime
import os
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# OpenAlex API endpoint
OPENALEX_API = "https://api.openalex.org/works"

# User email for OpenAlex polite pool - REPLACE WITH YOUR EMAIL
USER_EMAIL = "rob98@stanford.edu"

# =============================================================================
# SCRAPING CONFIGURATION - Keep consistent across all scrapers
# =============================================================================
MAX_RESULTS_PER_TERM = None   # No limit - use cursor pagination to get all results
PER_PAGE = 200                # Results per API page (max 200 for OpenAlex)
RATE_LIMIT_DELAY = 0.1        # Seconds between API requests
MAX_WORKERS = 3               # Number of parallel threads for search terms

# Thread-safe rate limiter for parallel requests
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

# Output paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Policies file location: ../get_policies/output/policies.csv
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv")
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

# NBER and Semantic Scholar output for comparison
NBER_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_nber", "output")
NBER_OUTPUT_DIR = os.path.normpath(NBER_OUTPUT_DIR)

SEMANTIC_SCHOLAR_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_semantic_scholar", "output")
SEMANTIC_SCHOLAR_OUTPUT_DIR = os.path.normpath(SEMANTIC_SCHOLAR_OUTPUT_DIR)

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


def reconstruct_abstract(abstract_inverted_index):
    """
    Reconstruct abstract text from OpenAlex's inverted index format.

    OpenAlex stores abstracts as an inverted index to save space. The format is:
    {
        "word1": [position1, position2, ...],
        "word2": [position3, ...],
        ...
    }

    Each word maps to a list of positions where it appears in the abstract.
    This function reconstructs the original text by placing each word at its
    position(s) and joining them with spaces.

    Parameters:
    -----------
    abstract_inverted_index : dict or None
        The inverted index from OpenAlex API response

    Returns:
    --------
    str : Reconstructed abstract text, or empty string if not available

    Example:
    --------
    >>> idx = {"Hello": [0], "world": [1]}
    >>> reconstruct_abstract(idx)
    "Hello world"
    """
    if not abstract_inverted_index:
        return ''

    # Find the maximum position to determine abstract length
    max_position = -1
    for positions in abstract_inverted_index.values():
        if positions:
            max_position = max(max_position, max(positions))

    if max_position < 0:
        return ''

    # Create a list to hold words at their positions
    words = [''] * (max_position + 1)

    # Place each word at its position(s)
    for word, positions in abstract_inverted_index.items():
        for pos in positions:
            words[pos] = word

    # Join words with spaces to form the abstract
    return ' '.join(words)


def load_policies(policies_file):
    """
    Load policy configurations from CSV file
    
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


def search_openalex(query, per_page=PER_PAGE, max_results=MAX_RESULTS_PER_TERM):
    """
    Search OpenAlex for papers matching the query using cursor-based pagination.

    Uses the OpenAlex Works API to search for academic papers. The API
    supports full-text search across titles, abstracts, and full text.
    Uses cursor-based pagination (no result cap), unlike page-based
    pagination which is limited to 10,000 results.

    API Documentation: https://docs.openalex.org/api-entities/works

    Parameters:
    -----------
    query : str
        Search query string (searches title, abstract, and full text)
    per_page : int
        Number of results per page (max 200 per OpenAlex limits)
    max_results : int or None
        Maximum total number of results to retrieve. None means no limit
        (retrieve all available results).

    Returns:
    --------
    list : List of work dictionaries from OpenAlex API

    Notes:
    ------
    - Uses cursor-based pagination ('cursor' parameter) for unlimited results
    - Uses the 'mailto' parameter for polite pool access (faster rate limits)
    - Uses thread-safe rate limiter for parallel processing
    - Stops early if no more results are available
    """
    all_results = []
    cursor = '*'  # Initial cursor value for first request
    batch_num = 0

    print(f"  Searching OpenAlex for: '{query}'")

    while True:
        # Check max_results limit
        if max_results is not None and len(all_results) >= max_results:
            break

        # OpenAlex API parameters with cursor pagination
        params = {
            'search': query,
            'per_page': per_page,
            'cursor': cursor,
            'mailto': USER_EMAIL
        }

        try:
            rate_limiter.wait()  # Thread-safe rate limiting
            response = requests.get(OPENALEX_API, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            if not results:
                print(f"    [{query[:30]}...] No more results after {len(all_results)} total")
                break

            all_results.extend(results)
            batch_num += 1

            # Print progress every 5 batches or on first batch
            meta = data.get('meta', {})
            total_available = meta.get('count', '?')
            if batch_num <= 2 or batch_num % 5 == 0:
                print(f"    [{query[:30]}...] Batch {batch_num}: {len(results)} results "
                      f"(total: {len(all_results)}/{total_available})")

            # Get next cursor for pagination
            next_cursor = meta.get('next_cursor')
            if not next_cursor:
                print(f"    [{query[:30]}...] Reached end at {len(all_results)} results")
                break
            cursor = next_cursor

        except requests.exceptions.RequestException as e:
            print(f"    [{query[:30]}...] ERROR at batch {batch_num + 1}: {e}")
            break

    if max_results is not None:
        return all_results[:max_results]
    return all_results


def extract_paper_info(work):
    """
    Extract relevant information from OpenAlex work object.

    This function parses the nested JSON structure returned by OpenAlex API
    and extracts key bibliometric fields into a flat dictionary suitable
    for tabular storage (CSV/Parquet).

    Parameters:
    -----------
    work : dict
        OpenAlex work object (single paper from API response)

    Returns:
    --------
    dict : Extracted paper information with the following keys:
        - openalex_id: Unique identifier in OpenAlex
        - doi: Digital Object Identifier
        - title: Paper title
        - abstract: Reconstructed abstract text (from inverted index)
        - publication_year/date: When the paper was published
        - authors: Pipe-separated list of author names
        - author_count: Number of authors
        - author_affiliations: Pipe-separated list of institutions
        - source_name/type: Journal or venue information
        - is_open_access, open_access_url: OA status and link
        - cited_by_count: Number of citations
        - concepts: Pipe-separated list of topics/concepts
        - type: Article type (e.g., 'article', 'review')
        - language: Language code (e.g., 'en')
        - url: Link to OpenAlex page
    """
    # --- Extract author information ---
    # Each authorship contains author details and their institutional affiliations
    authors = []
    author_affiliations = []
    for authorship in work.get('authorships', []):
        author = authorship.get('author', {})
        author_name = author.get('display_name', '')
        if author_name:
            authors.append(author_name)

        # Get affiliations - each author may have multiple institutions
        institutions = authorship.get('institutions', [])
        if institutions:
            inst_names = [inst.get('display_name') or '' for inst in institutions]
            inst_names = [name for name in inst_names if name]
            author_affiliations.append('; '.join(inst_names) if inst_names else '')
        else:
            author_affiliations.append('')

    # --- Extract concepts/topics ---
    # OpenAlex assigns weighted concepts to each paper
    concepts = [c.get('display_name', '') for c in work.get('concepts', [])]

    # --- Extract publication source info ---
    # primary_location contains the main venue (journal, repository, etc.)
    pub_info = work.get('primary_location', {}) or {}
    source = pub_info.get('source', {}) or {}

    # --- Reconstruct abstract from inverted index ---
    # OpenAlex stores abstracts as inverted indices, not plain text
    # See reconstruct_abstract() docstring for format details
    abstract_inverted_index = work.get('abstract_inverted_index', None)
    abstract_text = reconstruct_abstract(abstract_inverted_index)

    # --- Build output dictionary ---
    paper_info = {
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
        'type': work.get('type', ''),
        'language': work.get('language', ''),
        'url': work.get('id', '')
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


def compare_with_nber(policy_abbr):
    """
    Compare OpenAlex papers with NBER coverage.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Comparison statistics
    """
    print(f"\n  Comparing OpenAlex vs NBER for {policy_abbr}...")

    # Load OpenAlex papers (try raw first, then regular)
    oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.parquet")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.csv")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.csv")

    if not os.path.exists(oa_file):
        print(f"    ERROR: OpenAlex file not found for {policy_abbr}")
        return None

    if oa_file.endswith('.parquet'):
        oa_df = pd.read_parquet(oa_file)
    else:
        oa_df = pd.read_csv(oa_file)

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

    print(f"    OpenAlex papers: {len(oa_df)}")
    print(f"    NBER papers: {len(nber_df)}")

    # Normalize titles for matching
    oa_df['normalized_title'] = oa_df['title'].apply(normalize_title)
    nber_df['normalized_title'] = nber_df['title'].apply(normalize_title)

    # Create sets for fast lookup
    oa_titles = set(oa_df['normalized_title'].dropna())
    nber_titles = set(nber_df['normalized_title'].dropna())

    # Add indicators
    oa_df['in_nber'] = oa_df['normalized_title'].isin(nber_titles)
    nber_df['in_openalex'] = nber_df['normalized_title'].isin(oa_titles)

    # Calculate statistics
    oa_in_nber = oa_df['in_nber'].sum()
    nber_in_oa = nber_df['in_openalex'].sum()

    print(f"    OpenAlex papers also in NBER: {oa_in_nber} ({100*oa_in_nber/len(oa_df):.1f}%)")
    print(f"    NBER papers also in OpenAlex: {nber_in_oa} ({100*nber_in_oa/len(nber_df):.1f}%)")

    # Show NBER papers NOT in OpenAlex (important for coverage analysis)
    nber_not_in_oa = nber_df[~nber_df['in_openalex']]
    if len(nber_not_in_oa) > 0:
        print(f"    WARNING: {len(nber_not_in_oa)} NBER papers NOT in OpenAlex:")
        for _, row in nber_not_in_oa.head(5).iterrows():
            print(f"      - {row['title'][:60]}...")

    # Save with indicators
    oa_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_nber_indicator.csv")
    oa_df.to_csv(oa_indicator_file, index=False)
    print(f"    Saved: {oa_indicator_file}")

    nber_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_nber_openalex_indicator.csv")
    nber_df.to_csv(nber_indicator_file, index=False)
    print(f"    Saved: {nber_indicator_file}")

    return {
        'policy_abbr': policy_abbr,
        'openalex_total': len(oa_df),
        'nber_total': len(nber_df),
        'oa_in_nber': int(oa_in_nber),
        'oa_in_nber_pct': 100 * oa_in_nber / len(oa_df) if len(oa_df) > 0 else 0,
        'nber_in_oa': int(nber_in_oa),
        'nber_in_oa_pct': 100 * nber_in_oa / len(nber_df) if len(nber_df) > 0 else 0,
        'nber_missing_from_oa': len(nber_not_in_oa)
    }


def compare_with_semantic_scholar(policy_abbr):
    """
    Compare OpenAlex papers with Semantic Scholar coverage.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Comparison statistics
    """
    print(f"\n  Comparing OpenAlex vs Semantic Scholar for {policy_abbr}...")

    # Load OpenAlex papers (try raw first, then regular)
    oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.parquet")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.csv")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet")
    if not os.path.exists(oa_file):
        oa_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.csv")

    if not os.path.exists(oa_file):
        print(f"    ERROR: OpenAlex file not found for {policy_abbr}")
        return None

    if oa_file.endswith('.parquet'):
        oa_df = pd.read_parquet(oa_file)
    else:
        oa_df = pd.read_csv(oa_file)

    # Load Semantic Scholar papers (try raw first, then regular)
    ss_file = os.path.join(SEMANTIC_SCHOLAR_OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(SEMANTIC_SCHOLAR_OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar_raw.csv")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(SEMANTIC_SCHOLAR_OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.parquet")
    if not os.path.exists(ss_file):
        ss_file = os.path.join(SEMANTIC_SCHOLAR_OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.csv")

    if not os.path.exists(ss_file):
        print(f"    ERROR: Semantic Scholar file not found for {policy_abbr}")
        return None

    if ss_file.endswith('.parquet'):
        ss_df = pd.read_parquet(ss_file)
    else:
        ss_df = pd.read_csv(ss_file)

    print(f"    OpenAlex papers: {len(oa_df)}")
    print(f"    Semantic Scholar papers: {len(ss_df)}")

    # Normalize titles for matching
    oa_df['normalized_title'] = oa_df['title'].apply(normalize_title)
    ss_df['normalized_title'] = ss_df['title'].apply(normalize_title)

    # Create sets for fast lookup
    oa_titles = set(oa_df['normalized_title'].dropna())
    ss_titles = set(ss_df['normalized_title'].dropna())

    # Add indicators
    oa_df['in_semantic_scholar'] = oa_df['normalized_title'].isin(ss_titles)
    ss_df['in_openalex'] = ss_df['normalized_title'].isin(oa_titles)

    # Calculate statistics
    oa_in_ss = oa_df['in_semantic_scholar'].sum()
    ss_in_oa = ss_df['in_openalex'].sum()

    print(f"    OpenAlex papers also in Semantic Scholar: {oa_in_ss} ({100*oa_in_ss/len(oa_df):.1f}%)")
    print(f"    Semantic Scholar papers also in OpenAlex: {ss_in_oa} ({100*ss_in_oa/len(ss_df):.1f}%)")

    # Save with indicators
    oa_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_semantic_scholar_indicator.csv")
    oa_df.to_csv(oa_indicator_file, index=False)
    print(f"    Saved: {oa_indicator_file}")

    return {
        'policy_abbr': policy_abbr,
        'openalex_total': len(oa_df),
        'semantic_scholar_total': len(ss_df),
        'oa_in_ss': int(oa_in_ss),
        'oa_in_ss_pct': 100 * oa_in_ss / len(oa_df) if len(oa_df) > 0 else 0,
        'ss_in_oa': int(ss_in_oa),
        'ss_in_oa_pct': 100 * ss_in_oa / len(ss_df) if len(ss_df) > 0 else 0
    }


def process_policy(policy_row):
    """
    Process a single policy: search OpenAlex and save results.

    This is the main orchestration function for each policy. It:
    1. Parses search terms from the policy configuration
    2. Searches OpenAlex for each term and saves raw API responses
    3. Extracts structured paper information from results
    4. Deduplicates papers (same paper may match multiple search terms)
    5. Saves final dataset in Parquet and CSV formats
    6. Generates metadata with scraping statistics

    Parameters:
    -----------
    policy_row : pd.Series
        Row from policies DataFrame with columns:
        - policy_name: Full name (e.g., "Tax Cuts and Jobs Act")
        - policy_abbreviation: Short code for filenames (e.g., "TCJA")
        - policy_year: Year enacted (e.g., 2017)
        - policy_category: Category (e.g., "tax", "health")
        - search_terms: Pipe-separated search queries

    Returns:
    --------
    dict : Summary statistics including:
        - policy_abbreviation, policy_name
        - total_papers: Count before deduplication
        - unique_papers: Count after deduplication
        - duplicates_removed: Number of duplicates found

    Output Files:
    -------------
    - {abbr}_papers_openalex.parquet: Main dataset (efficient storage)
    - {abbr}_papers_openalex.csv: Main dataset (compatibility)
    - {abbr}_metadata.json: Scraping metadata and statistics
    - tmp/raw_{abbr}_{term}.json: Raw API responses for each search term
    """
    policy_name = policy_row['policy_name']
    policy_abbr = policy_row['policy_abbreviation']
    policy_year = policy_row['policy_year']
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
        results = search_openalex(term, per_page=PER_PAGE, max_results=MAX_RESULTS_PER_TERM)

        # Save raw results for this term
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"    Saved raw results to: {raw_file}")

        # Extract paper info
        papers = []
        for work in results:
            paper_info = extract_paper_info(work)
            paper_info['search_term'] = term
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
            'unique_papers': 0,
            'duplicates_removed': 0,
            'pre_policy_filtered': 0
        }

    initial_count = len(df)

    # Aggregate all search terms for each paper (instead of keeping just the first)
    print(f"\n  Aggregating search terms and removing duplicates...")
    search_terms_agg = df.groupby('openalex_id')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['openalex_id', 'search_terms_matched']

    # Keep first occurrence of each paper (for other columns)
    df_unique = df.drop_duplicates(subset=['openalex_id'], keep='first').copy()

    # Merge aggregated search terms
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='openalex_id', how='left')

    duplicate_count = initial_count - len(df_unique)
    print(f"    Initial: {initial_count} | Duplicates: {duplicate_count} | Unique: {len(df_unique)}")

    # Filter out papers published before the policy year
    print(f"\n  Filtering papers by publication date (>= {policy_year})...")
    pre_filter_count = len(df_unique)
    df_unique = df_unique[df_unique['publication_year'] >= policy_year]
    filtered_count = pre_filter_count - len(df_unique)
    print(f"    Before filter: {pre_filter_count} | Filtered out: {filtered_count} | After filter: {len(df_unique)}")

    # NO relevance filtering at scrape stage - will be done after abstract recovery
    print(f"\n  Skipping relevance filtering (will be applied after abstract recovery)")

    # Add metadata columns
    df_unique['policy_studied'] = policy_name
    df_unique['policy_year'] = policy_year
    df_unique['policy_abbreviation'] = policy_abbr
    df_unique['policy_category'] = policy_category
    df_unique['data_source'] = 'OpenAlex'
    df_unique['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

    # Reorder columns (search_terms_matched contains all matched terms)
    column_order = [
        'openalex_id', 'doi', 'title', 'authors', 'author_count',
        'author_affiliations', 'publication_year', 'publication_date',
        'source_name', 'source_type', 'abstract', 'cited_by_count',
        'is_open_access', 'open_access_url', 'concepts', 'type', 'language',
        'search_terms_matched', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'url'
    ]
    df_unique = df_unique[[c for c in column_order if c in df_unique.columns]]

    # Save RAW outputs (no relevance filtering applied)
    # Save as Parquet (primary format)
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved RAW Parquet: {parquet_file}")

    # Save as CSV (for compatibility)
    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex_raw.csv")
    df_unique.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved RAW CSV: {csv_file}")

    # Save metadata
    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': int(policy_year),
        'policy_category': policy_category,
        'search_terms': search_terms,
        'scrape_date': datetime.now().isoformat(),
        'total_papers_found': initial_count,
        'duplicates_removed': duplicate_count,
        'pre_policy_filtered': filtered_count,
        'unique_papers_raw': len(df_unique),
        'note': 'Raw data without relevance filtering. Filtering applied after abstract recovery.',
        'search_details': search_metadata
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    # Print summary
    print(f"\n  SUMMARY for {policy_abbr} (RAW DATA):")
    print(f"    Unique papers (raw): {len(df_unique)}")
    print(f"    Date range: {df_unique['publication_year'].min()}-{df_unique['publication_year'].max()}")
    print(f"    Open access: {df_unique['is_open_access'].sum()} ({df_unique['is_open_access'].sum()/len(df_unique)*100:.1f}%)")
    print(f"    Median citations: {df_unique['cited_by_count'].median():.0f}")
    print(f"    Papers with abstracts: {(df_unique['abstract'].notna() & (df_unique['abstract'] != '')).sum()}")

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
    parser = argparse.ArgumentParser(description="Policy papers scraping from OpenAlex")
    parser.add_argument('policies', nargs='*', help='Policy abbreviations to process (default: all)')
    parser.add_argument('--resume', action='store_true', help='Skip policies already completed today')
    args = parser.parse_args()

    print("="*80)
    print("POLICY PAPERS SCRAPING FROM OPENALEX")
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
        if args.resume and is_policy_complete(policy_abbr, 'openalex'):
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
    
    # Create overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")

    if all_summaries:
        summary_df = pd.DataFrame(all_summaries)
        print(summary_df.to_string(index=False))

        # Save overall summary
        summary_file = os.path.join(OUTPUT_DIR, "all_policies_summary.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSaved overall summary: {summary_file}")
    else:
        print("  No policies were processed (all skipped or failed).")

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
        comparison_file = os.path.join(OUTPUT_DIR, "openalex_nber_comparison.csv")
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\nSaved NBER comparison: {comparison_file}")

    # Compare with Semantic Scholar
    print(f"\n{'='*80}")
    print("COMPARING WITH SEMANTIC SCHOLAR")
    print(f"{'='*80}")

    ss_comparison_results = []
    for _, row in policies_df.iterrows():
        policy_abbr = row['policy_abbreviation']
        try:
            comparison = compare_with_semantic_scholar(policy_abbr)
            if comparison:
                ss_comparison_results.append(comparison)
        except Exception as e:
            print(f"  ERROR comparing {policy_abbr} with Semantic Scholar: {e}")
            import traceback
            traceback.print_exc()

    # Save Semantic Scholar comparison results
    if ss_comparison_results:
        comparison_df = pd.DataFrame(ss_comparison_results)
        comparison_file = os.path.join(OUTPUT_DIR, "openalex_semantic_scholar_comparison.csv")
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\nSaved Semantic Scholar comparison: {comparison_file}")

    # Print comparison results
    if nber_comparison_results:
        print(f"\n{'='*80}")
        print("OPENALEX vs NBER COVERAGE")
        print(f"{'='*80}")
        for result in nber_comparison_results:
            print(f"\n{result['policy_abbr']}:")
            print(f"  OpenAlex papers: {result['openalex_total']}")
            print(f"  NBER papers: {result['nber_total']}")
            print(f"  OpenAlex in NBER: {result['oa_in_nber']} ({result['oa_in_nber_pct']:.1f}%)")
            print(f"  NBER in OpenAlex: {result['nber_in_oa']} ({result['nber_in_oa_pct']:.1f}%)")
            if result['nber_missing_from_oa'] > 0:
                print(f"  *** {result['nber_missing_from_oa']} NBER papers MISSING from OpenAlex ***")

    if ss_comparison_results:
        print(f"\n{'='*80}")
        print("OPENALEX vs SEMANTIC SCHOLAR COVERAGE")
        print(f"{'='*80}")
        for result in ss_comparison_results:
            print(f"\n{result['policy_abbr']}:")
            print(f"  OpenAlex papers: {result['openalex_total']}")
            print(f"  Semantic Scholar papers: {result['semantic_scholar_total']}")
            print(f"  OpenAlex in SS: {result['oa_in_ss']} ({result['oa_in_ss_pct']:.1f}%)")
            print(f"  SS in OpenAlex: {result['ss_in_oa']} ({result['ss_in_oa_pct']:.1f}%)")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
