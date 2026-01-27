"""
Script to scrape papers related to policies from OpenAlex.

Issue #1: Scrape policy papers from OpenAlex

This script reads a list of policies from policies.csv and systematically
searches OpenAlex for academic papers related to each policy.

Pipeline Overview:
------------------
1. Load policy configurations from ../get_policies/output/policies.csv
2. For each policy, search OpenAlex using configured search terms
3. Extract paper metadata (title, authors, abstract, citations, etc.)
4. Deduplicate results and save to Parquet/CSV formats

Key Implementation Notes:
-------------------------
- OpenAlex stores abstracts as "inverted indices" (word -> positions mapping),
  not plain text. The reconstruct_abstract() function handles this conversion.
- Uses OpenAlex "polite pool" (via mailto parameter) for better rate limits.
- Raw API responses are saved to tmp/ for debugging and reproducibility.

Author: claude ai with modifications by roberto gonzalez
Date: January 9, 2026
Updated: January 14, 2026 - Fixed abstract extraction from inverted index
Updated: January 27, 2026 - Increased max_results to 1500, added relevance filtering
"""

import requests
import json
import pandas as pd
import time
from datetime import datetime
import os
import sys
import re

# OpenAlex API endpoint
OPENALEX_API = "https://api.openalex.org/works"

# User email for OpenAlex polite pool - REPLACE WITH YOUR EMAIL
USER_EMAIL = "rob98@stanford.edu"

# Output paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Policies file location: ../get_policies/output/policies.csv
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv")
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

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


def search_openalex(query, per_page=100, max_results=1000):
    """
    Search OpenAlex for papers matching the query.

    Uses the OpenAlex Works API to search for academic papers. The API
    supports full-text search across titles, abstracts, and full text.
    Results are paginated, and this function handles pagination automatically.

    API Documentation: https://docs.openalex.org/api-entities/works

    Parameters:
    -----------
    query : str
        Search query string (searches title, abstract, and full text)
    per_page : int
        Number of results per page (max 200 per OpenAlex limits)
    max_results : int
        Maximum total number of results to retrieve across all pages

    Returns:
    --------
    list : List of work dictionaries from OpenAlex API

    Notes:
    ------
    - Uses the 'mailto' parameter for polite pool access (faster rate limits)
    - Includes 0.1s delay between requests to avoid rate limiting
    - Stops early if no more results are available
    """
    all_results = []
    page = 1

    print(f"  Searching OpenAlex for: '{query}'")
    
    while len(all_results) < max_results:
        # OpenAlex API parameters
        params = {
            'search': query,
            'per_page': per_page,
            'page': page,
            'mailto': USER_EMAIL
        }
        
        try:
            response = requests.get(OPENALEX_API, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            if not results:
                print(f"    No more results at page {page}")
                break
            
            all_results.extend(results)
            print(f"    Page {page}: {len(results)} results (total: {len(all_results)})")
            
            # Check if we've reached the end
            meta = data.get('meta', {})
            if page >= meta.get('count', 0) // per_page + 1:
                break
            
            page += 1
            time.sleep(0.1)  # Be polite to the API
            
        except requests.exceptions.RequestException as e:
            print(f"    ERROR retrieving page {page}: {e}")
            break
    
    return all_results[:max_results]


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


def filter_by_relevance(df, search_terms):
    """
    Filter papers by relevance based on search term presence in title/abstract.

    Logic:
    - If paper has title AND abstract: keep only if at least one search term
      appears in either title or abstract (case-insensitive)
    - If paper has only title (no abstract): keep the paper

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with 'title' and 'abstract' columns
    search_terms : list
        List of search terms to look for

    Returns:
    --------
    pd.DataFrame : Filtered DataFrame with only relevant papers
    """
    if len(df) == 0:
        return df

    def is_relevant(row):
        title = str(row.get('title', '')).lower()
        abstract = str(row.get('abstract', '')).lower()

        # If no abstract, keep the paper (we'll filter again after complementing)
        if not abstract or abstract == 'nan' or abstract == '':
            return True

        # If we have both title and abstract, check for search term presence
        text = title + ' ' + abstract
        for term in search_terms:
            # Use word boundary matching for better precision
            term_lower = term.lower()
            if term_lower in text:
                return True

        return False

    # Apply filter
    mask = df.apply(is_relevant, axis=1)
    return df[mask].copy()


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
    
    # Search for each term
    for term in search_terms:
        results = search_openalex(term, per_page=100, max_results=1500)
        
        # Save raw results for this term
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"    Saved raw results to: {raw_file}")
        
        # Extract paper info
        for work in results:
            paper_info = extract_paper_info(work)
            paper_info['search_term'] = term
            all_papers.append(paper_info)
        
        search_metadata.append({
            'search_term': term,
            'results_count': len(results),
            'timestamp': datetime.now().isoformat()
        })
        
        print(f"    Extracted info from {len(results)} papers")
    
    # Create DataFrame
    df = pd.DataFrame(all_papers)
    
    if len(df) == 0:
        print(f"\n  WARNING: No papers found for {policy_name}")
        return {
            'policy_abbreviation': policy_abbr,
            'policy_name': policy_name,
            'total_papers': 0,
            'unique_papers': 0,
            'duplicates_removed': 0
        }
    
    # Remove duplicates (based on OpenAlex ID)
    print(f"\n  Removing duplicates...")
    initial_count = len(df)
    df_unique = df.drop_duplicates(subset=['openalex_id'], keep='first')
    duplicate_count = initial_count - len(df_unique)
    print(f"    Initial: {initial_count} | Duplicates: {duplicate_count} | Unique: {len(df_unique)}")

    # Filter out papers published before the policy year
    print(f"\n  Filtering papers by publication date (>= {policy_year})...")
    pre_filter_count = len(df_unique)
    df_unique = df_unique[df_unique['publication_year'] >= policy_year]
    filtered_count = pre_filter_count - len(df_unique)
    print(f"    Before filter: {pre_filter_count} | Filtered out: {filtered_count} | After filter: {len(df_unique)}")

    # Filter by relevance (search terms in title/abstract)
    print(f"\n  Filtering by relevance (search terms in title/abstract)...")
    pre_relevance_count = len(df_unique)
    df_unique = filter_by_relevance(df_unique, search_terms)
    relevance_filtered = pre_relevance_count - len(df_unique)
    print(f"    Before filter: {pre_relevance_count} | Filtered out: {relevance_filtered} | After filter: {len(df_unique)}")

    # Add metadata columns
    df_unique = df_unique.copy()
    df_unique['policy_studied'] = policy_name
    df_unique['policy_year'] = policy_year
    df_unique['policy_abbreviation'] = policy_abbr
    df_unique['policy_category'] = policy_category
    df_unique['data_source'] = 'OpenAlex'
    df_unique['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
    
    # Reorder columns
    column_order = [
        'openalex_id', 'doi', 'title', 'authors', 'author_count', 
        'author_affiliations', 'publication_year', 'publication_date',
        'source_name', 'source_type', 'abstract', 'cited_by_count',
        'is_open_access', 'open_access_url', 'concepts', 'type', 'language',
        'search_term', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'url'
    ]
    df_unique = df_unique[column_order]
    
    # Save outputs with policy abbreviation in filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as Parquet (primary format)
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved Parquet: {parquet_file}")
    
    # Save as CSV (for compatibility)
    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.csv")
    df_unique.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved CSV: {csv_file}")
    
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
        'relevance_filtered': relevance_filtered,
        'unique_papers': len(df_unique),
        'search_details': search_metadata
    }
    
    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")
    
    # Print summary
    print(f"\n  SUMMARY for {policy_abbr}:")
    print(f"    Unique papers: {len(df_unique)}")
    print(f"    Date range: {df_unique['publication_year'].min()}-{df_unique['publication_year'].max()}")
    print(f"    Open access: {df_unique['is_open_access'].sum()} ({df_unique['is_open_access'].sum()/len(df_unique)*100:.1f}%)")
    print(f"    Median citations: {df_unique['cited_by_count'].median():.0f}")
    
    return {
        'policy_abbreviation': policy_abbr,
        'policy_name': policy_name,
        'total_papers': initial_count,
        'duplicates_removed': duplicate_count,
        'pre_policy_filtered': filtered_count,
        'relevance_filtered': relevance_filtered,
        'unique_papers': len(df_unique)
    }


def main():
    """
    Main execution function
    """
    print("="*80)
    print("POLICY PAPERS SCRAPING FROM OPENALEX")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Load policies configuration
    policies_df = load_policies(POLICIES_FILE)
    print(f"\nPolicies to process:")
    for _, row in policies_df.iterrows():
        print(f"  - {row['policy_name']} ({row['policy_abbreviation']})")
    
    # Process each policy
    all_summaries = []
    for idx, row in policies_df.iterrows():
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
    
    summary_df = pd.DataFrame(all_summaries)
    print(summary_df.to_string(index=False))
    
    # Save overall summary
    summary_file = os.path.join(OUTPUT_DIR, "all_policies_summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"\nSaved overall summary: {summary_file}")
    
    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
