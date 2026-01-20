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
4. Deduplicate results across all search terms
5. Filter papers by publication date (must be >= policy year)
6. Compare with OpenAlex and NBER results to check coverage
7. Save results and comparison report

Key Implementation Notes:
-------------------------
- Pipeline is identical to OpenAlex and NBER scrapers for methodological consistency.
- Searches all search terms from policies.csv.
- Title normalization is used for deduplication and matching papers between sources.
- Papers published before the policy year are filtered out.
- Semantic Scholar API: GET https://api.semanticscholar.org/graph/v1/paper/search

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
"""

import requests
import json
import pandas as pd
import time
import re
from datetime import datetime
import os
import sys

# Semantic Scholar API endpoint
SEMANTIC_SCHOLAR_API = "https://api.semanticscholar.org/graph/v1/paper/search"

# Fields to request from API
API_FIELDS = "paperId,title,abstract,authors,year,citationCount,venue,publicationDate,isOpenAccess,openAccessPdf"

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


def search_semantic_scholar(query, limit=100, max_results=1000):
    """
    Search Semantic Scholar for papers matching the query.

    Uses the Semantic Scholar Academic Graph API.

    Parameters:
    -----------
    query : str
        Search query string
    limit : int
        Number of results per page (max 100)
    max_results : int
        Maximum total number of results to retrieve

    Returns:
    --------
    list : List of paper dictionaries from Semantic Scholar API
    """
    all_results = []
    offset = 0

    print(f"  Searching Semantic Scholar for: '{query}'")

    while len(all_results) < max_results:
        params = {
            'query': query,
            'fields': API_FIELDS,
            'offset': offset,
            'limit': limit
        }

        try:
            response = requests.get(SEMANTIC_SCHOLAR_API, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get('data', [])
            if not results:
                print(f"    No more results at offset {offset}")
                break

            all_results.extend(results)
            total = data.get('total', 0)
            print(f"    Offset {offset}: {len(results)} results (total: {len(all_results)}/{total})")

            # Check if we've reached the end
            if offset + limit >= total:
                break

            offset += limit
            time.sleep(0.1)  # Be polite to the API (rate limit: 1000 req/sec unauthenticated)

        except requests.exceptions.RequestException as e:
            print(f"    ERROR retrieving offset {offset}: {e}")
            break

    return all_results[:max_results]


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

    # Load Semantic Scholar papers
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

    # Load Semantic Scholar papers
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

    # Load NBER papers
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
        'nber_in_ss_pct': 100 * nber_in_ss / len(nber_df) if len(nber_df) > 0 else 0
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

    # Search for each term
    for term in search_terms:
        results = search_semantic_scholar(term, limit=100, max_results=1000)

        # Save raw results for this term
        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"    Saved raw results to: {raw_file}")

        # Extract paper info
        for paper in results:
            paper_info = extract_paper_info(paper)
            paper_info['search_term'] = term
            paper_info['policy_studied'] = policy_name
            paper_info['policy_year'] = policy_year
            paper_info['policy_abbreviation'] = policy_abbr
            paper_info['policy_category'] = policy_category
            paper_info['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
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
            'duplicates_removed': 0,
            'pre_policy_filtered': 0,
            'unique_papers': 0
        }

    initial_count = len(df)
    print(f"\n  Total papers found: {initial_count}")

    # Remove duplicates (based on Semantic Scholar ID)
    print(f"\n  Removing duplicates...")
    df_unique = df.drop_duplicates(subset=['semantic_scholar_id'], keep='first')
    duplicate_count = initial_count - len(df_unique)
    print(f"    Initial: {initial_count} | Duplicates: {duplicate_count} | Unique: {len(df_unique)}")

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

    # Add normalized title for comparison
    df_unique = df_unique.copy()
    df_unique['normalized_title'] = df_unique['title'].apply(normalize_title)

    # Reorder columns
    column_order = [
        'semantic_scholar_id', 'title', 'authors', 'author_count',
        'publication_year', 'publication_date', 'abstract', 'venue',
        'cited_by_count', 'is_open_access', 'open_access_url',
        'search_term', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'normalized_title'
    ]
    df_unique = df_unique[[c for c in column_order if c in df_unique.columns]]

    # Save outputs
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved Parquet: {parquet_file}")

    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_semantic_scholar.csv")
    df_unique.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved CSV: {csv_file}")

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
        'pre_policy_filtered': filtered_count,
        'unique_papers': len(df_unique),
        'search_details': search_metadata
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_semantic_scholar_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    # Summary
    print(f"\n  SUMMARY for {policy_abbr}:")
    print(f"    Unique papers: {len(df_unique)}")
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
        'unique_papers': len(df_unique)
    }


def main():
    """
    Main execution function
    """
    print("="*80)
    print("SEMANTIC SCHOLAR PAPER SCRAPING AND COMPARISON")
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

    summary_df = pd.DataFrame(all_summaries)
    print(summary_df.to_string(index=False))

    # Save overall summary
    summary_file = os.path.join(OUTPUT_DIR, "semantic_scholar_scrape_summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"\nSaved overall summary: {summary_file}")

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

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
