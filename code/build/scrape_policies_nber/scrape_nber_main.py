"""
Script to scrape papers related to policies from NBER (National Bureau of Economic Research).

Issue #9: Cross validate papers with NBER

This script searches NBER for working papers related to each policy and compares
coverage with OpenAlex results to validate data quality.

Pipeline Overview:
------------------
1. Load policy configurations from ../get_policies/output/policies.csv
2. For each policy, search NBER using ALL search terms (in parallel)
3. Extract paper metadata (title, authors, abstract, url, etc.)
4. Deduplicate results, tracking ALL search terms that found each paper
5. Filter papers by publication date (must be >= policy year)
6. Save RAW results (no relevance filtering) to Parquet/CSV
7. Compare with OpenAlex results to check coverage

Key Implementation Notes:
-------------------------
- Pipeline is identical to OpenAlex scraper for methodological consistency.
- Searches all search terms from policies.csv (not just abbreviations).
- Title normalization is used for deduplication and matching papers between sources.
- Papers published before the policy year are filtered out.
- Search terms are processed in parallel using ThreadPoolExecutor for speed.
- NO relevance filtering at this stage - filtering happens after abstract recovery.
- Tracks ALL search terms that found each paper (pipe-separated in search_terms_matched column).
- Note: NBER API returns truncated abstracts. Full abstracts recovered in complement_abstracts step.

Output Files:
-------------
- {abbr}_papers_nber_raw.parquet: Raw dataset (efficient storage)
- {abbr}_papers_nber_raw.csv: Raw dataset (compatibility)
- {abbr}_nber_metadata.json: Scraping metadata and statistics

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
Updated: January 27, 2026 - Increased max_results to 1500, added relevance filtering
Updated: February 4, 2026 - Increased max_results to 10000, added parallel processing
Updated: February 4, 2026 - Removed relevance filtering, track all search terms, fixed pagination bug
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# NBER API endpoint
NBER_API = "https://www.nber.org/api/v1/search"

# =============================================================================
# SCRAPING CONFIGURATION - Keep consistent across all scrapers
# =============================================================================
MAX_RESULTS_PER_TERM = 10000  # Maximum results to retrieve per search term
PER_PAGE = 100                # Results per API page
RATE_LIMIT_DELAY = 0.2        # Seconds between API requests
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

# OpenAlex output for comparison
OPENALEX_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_openalex", "output")
OPENALEX_OUTPUT_DIR = os.path.normpath(OPENALEX_OUTPUT_DIR)

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


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


def search_nber(query, per_page=PER_PAGE, max_results=MAX_RESULTS_PER_TERM):
    """
    Search NBER for papers matching the query.

    Uses the NBER search API which returns JSON with paper metadata.

    Parameters:
    -----------
    query : str
        Search query string
    per_page : int
        Number of results per page (max 100)
    max_results : int
        Maximum total number of results to retrieve

    Returns:
    --------
    list : List of paper dictionaries from NBER API
    """
    all_results = []
    page = 1

    print(f"  Searching NBER for: '{query}'")

    while len(all_results) < max_results:
        params = {
            'q': query,
            'page': page,
            'perPage': per_page
        }

        try:
            rate_limiter.wait()  # Thread-safe rate limiting
            response = requests.get(NBER_API, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            if not results:
                print(f"    [{query[:30]}...] No more results at page {page}")
                break

            all_results.extend(results)

            # NBER API uses 'totalResults' not 'total'
            total = data.get('totalResults', 0)
            print(f"    [{query[:30]}...] Page {page}: {len(results)} results (total: {len(all_results)}/{total})")

            # Check if we've reached the end
            if page * per_page >= total:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            print(f"    [{query[:30]}...] ERROR retrieving page {page}: {e}")
            break

    return all_results[:max_results]


def extract_paper_info(paper):
    """
    Extract relevant information from NBER paper object.

    Parameters:
    -----------
    paper : dict
        NBER paper object from API response

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
        authors_str = str(authors)

    # Parse display date to get year
    display_date = paper.get('displaydate', '')
    publication_year = None
    if display_date:
        # Try to extract year from various date formats
        year_match = re.search(r'\b(19|20)\d{2}\b', display_date)
        if year_match:
            publication_year = int(year_match.group())

    paper_info = {
        'nber_id': paper.get('id', ''),
        'title': paper.get('title', ''),
        'abstract': paper.get('abstract', ''),
        'authors': authors_str,
        'author_count': len(author_names) if isinstance(authors, list) else 0,
        'publication_date': display_date,
        'publication_year': publication_year,
        'url': paper.get('url', ''),
        'type': paper.get('type', ''),
        'data_source': 'NBER'
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


def compare_with_openalex(policy_abbr, policy_year):
    """
    Compare NBER papers with OpenAlex coverage.

    Creates indicators showing which NBER papers appear in OpenAlex
    and vice versa, using title matching.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")
    policy_year : int
        Year the policy was enacted (for filtering)

    Returns:
    --------
    dict : Comparison statistics
    """
    print(f"\n  Comparing NBER vs OpenAlex for {policy_abbr}...")

    # Load NBER papers
    nber_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_nber.parquet")
    if not os.path.exists(nber_file):
        nber_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_nber.csv")

    if not os.path.exists(nber_file):
        print(f"    ERROR: NBER file not found for {policy_abbr}")
        return None

    if nber_file.endswith('.parquet'):
        nber_df = pd.read_parquet(nber_file)
    else:
        nber_df = pd.read_csv(nber_file)

    # Load OpenAlex papers
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

    print(f"    NBER papers: {len(nber_df)}")
    print(f"    OpenAlex papers: {len(openalex_df)}")

    # Normalize titles for matching
    nber_df['normalized_title'] = nber_df['title'].apply(normalize_title)
    openalex_df['normalized_title'] = openalex_df['title'].apply(normalize_title)

    # Create sets for fast lookup
    nber_titles = set(nber_df['normalized_title'].dropna())
    openalex_titles = set(openalex_df['normalized_title'].dropna())

    # Add indicator to NBER df: is this paper in OpenAlex?
    nber_df['in_openalex'] = nber_df['normalized_title'].isin(openalex_titles)

    # Add indicator to OpenAlex df: is this paper in NBER?
    openalex_df['in_nber'] = openalex_df['normalized_title'].isin(nber_titles)

    # Calculate statistics
    nber_in_openalex = nber_df['in_openalex'].sum()
    openalex_in_nber = openalex_df['in_nber'].sum()

    print(f"    NBER papers also in OpenAlex: {nber_in_openalex} ({100*nber_in_openalex/len(nber_df):.1f}%)")
    print(f"    OpenAlex papers also in NBER: {openalex_in_nber} ({100*openalex_in_nber/len(openalex_df):.1f}%)")

    # Save NBER df with indicator
    nber_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_nber_openalex_indicator.csv")
    nber_df.to_csv(nber_indicator_file, index=False)
    print(f"    Saved: {nber_indicator_file}")

    # Save OpenAlex df with indicator
    openalex_indicator_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_nber_indicator.csv")
    openalex_df.to_csv(openalex_indicator_file, index=False)
    print(f"    Saved: {openalex_indicator_file}")

    return {
        'policy_abbr': policy_abbr,
        'nber_total': len(nber_df),
        'openalex_total': len(openalex_df),
        'nber_in_openalex': int(nber_in_openalex),
        'nber_in_openalex_pct': 100 * nber_in_openalex / len(nber_df) if len(nber_df) > 0 else 0,
        'openalex_in_nber': int(openalex_in_nber),
        'openalex_in_nber_pct': 100 * openalex_in_nber / len(openalex_df) if len(openalex_df) > 0 else 0
    }


def process_policy(policy_row):
    """
    Process a single policy: search NBER and save results.

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
        results = search_nber(term, per_page=PER_PAGE, max_results=MAX_RESULTS_PER_TERM)

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

    # Add normalized title for deduplication
    df['normalized_title'] = df['title'].apply(normalize_title)

    # Aggregate all search terms for each paper (instead of keeping just the first)
    print(f"\n  Aggregating search terms and removing duplicates...")
    search_terms_agg = df.groupby('normalized_title')['search_term'].apply(
        lambda x: ' | '.join(sorted(set(x)))
    ).reset_index()
    search_terms_agg.columns = ['normalized_title', 'search_terms_matched']

    # Keep first occurrence of each paper (for other columns)
    df_unique = df.drop_duplicates(subset=['normalized_title'], keep='first').copy()

    # Merge aggregated search terms
    df_unique = df_unique.drop(columns=['search_term'])
    df_unique = df_unique.merge(search_terms_agg, on='normalized_title', how='left')

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
    print(f"    Note: NBER API returns truncated abstracts. Full abstracts recovered later.")

    # Reorder columns (search_terms_matched contains all matched terms)
    column_order = [
        'nber_id', 'title', 'authors', 'author_count',
        'publication_date', 'publication_year', 'abstract', 'url', 'type',
        'search_terms_matched', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'normalized_title'
    ]
    df_unique = df_unique[[c for c in column_order if c in df_unique.columns]]

    # Save RAW outputs (no relevance filtering applied)
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_nber_raw.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved RAW Parquet: {parquet_file}")

    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_nber_raw.csv")
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
        'note': 'Raw data without relevance filtering. NBER abstracts are truncated - full abstracts recovered in complement_abstracts step.',
        'search_details': search_metadata
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_nber_metadata.json")
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
    print(f"    Papers with abstracts (truncated): {abstracts_count} ({100*abstracts_count/len(df_unique):.1f}%)")

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
    parser = argparse.ArgumentParser(description="NBER paper scraping and OpenAlex comparison")
    parser.add_argument('policies', nargs='*', help='Policy abbreviations to process (default: all)')
    parser.add_argument('--resume', action='store_true', help='Skip policies already completed today')
    args = parser.parse_args()

    print("="*80)
    print("NBER PAPER SCRAPING AND OPENALEX COMPARISON")
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
        if args.resume and is_policy_complete(policy_abbr, 'nber'):
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

    comparison_results = []
    for _, row in policies_df.iterrows():
        policy_abbr = row['policy_abbreviation']
        policy_year = int(row['policy_year'])
        try:
            comparison = compare_with_openalex(policy_abbr, policy_year)
            if comparison:
                comparison_results.append(comparison)
        except Exception as e:
            print(f"  ERROR comparing {policy_abbr}: {e}")
            import traceback
            traceback.print_exc()

    # Save comparison results
    if comparison_results:
        comparison_df = pd.DataFrame(comparison_results)
        comparison_file = os.path.join(OUTPUT_DIR, "nber_openalex_comparison.csv")
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\nSaved comparison: {comparison_file}")

        # Save as JSON too
        comparison_json = os.path.join(OUTPUT_DIR, "nber_openalex_comparison_report.json")
        with open(comparison_json, 'w') as f:
            json.dump(comparison_results, f, indent=2)
        print(f"Saved comparison report: {comparison_json}")

    # Create overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")

    if all_summaries:
        summary_df = pd.DataFrame(all_summaries)
        print(summary_df.to_string(index=False))

        # Save overall summary
        summary_file = os.path.join(OUTPUT_DIR, "nber_scrape_summary.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSaved overall summary: {summary_file}")
    else:
        print("  No policies were processed (all skipped or failed).")

    # Print comparison results
    if comparison_results:
        print(f"\n{'='*80}")
        print("COVERAGE COMPARISON")
        print(f"{'='*80}")
        for result in comparison_results:
            print(f"\n{result['policy_abbr']}:")
            print(f"  NBER papers: {result['nber_total']}")
            print(f"  OpenAlex papers: {result['openalex_total']}")
            print(f"  NBER in OpenAlex: {result['nber_in_openalex']} ({result['nber_in_openalex_pct']:.1f}%)")
            print(f"  OpenAlex in NBER: {result['openalex_in_nber']} ({result['openalex_in_nber_pct']:.1f}%)")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
