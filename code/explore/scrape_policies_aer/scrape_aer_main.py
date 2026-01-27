"""
Script to scrape policy-related papers from the American Economic Review (AER).

Issue #16: Scrape QJE/AER for policy papers and cross-validate

This script uses the OpenAlex API with a source filter to search for papers
related to each policy that were published specifically in AER.

Pipeline Overview:
------------------
1. Load policy configurations from ../../build/get_policies/output/policies.csv
2. For each policy, search OpenAlex with AER source filter + policy search terms
3. Extract paper metadata (title, authors, abstract, citations, etc.)
4. Deduplicate results and save to Parquet/CSV formats

AER OpenAlex Source ID: S24807848

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
"""

import requests
import json
import pandas as pd
import time
from datetime import datetime
import os
import sys

# OpenAlex API endpoint
OPENALEX_API = "https://api.openalex.org/works"

# AER Source ID in OpenAlex
AER_SOURCE_ID = "S24807848"

# User email for OpenAlex polite pool
USER_EMAIL = "rob98@stanford.edu"

# Output paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Policies file location (in build folder)
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "..", "build", "get_policies", "output", "policies.csv")
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


def reconstruct_abstract(abstract_inverted_index):
    """
    Reconstruct abstract text from OpenAlex's inverted index format.
    """
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


def load_policies(policies_file):
    """
    Load policy configurations from CSV file.
    """
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


def search_aer(query, per_page=100, max_results=500):
    """
    Search OpenAlex for papers matching the query within AER only.

    Uses the OpenAlex filter parameter to restrict results to papers
    published in the American Economic Review.

    Parameters:
    -----------
    query : str
        Search query string
    per_page : int
        Number of results per page (max 200)
    max_results : int
        Maximum total results to retrieve

    Returns:
    --------
    list : List of work dictionaries from OpenAlex API
    """
    all_results = []
    page = 1

    print(f"  Searching AER for: '{query}'")

    while len(all_results) < max_results:
        params = {
            'search': query,
            'filter': f'primary_location.source.id:{AER_SOURCE_ID}',
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

            meta = data.get('meta', {})
            if page >= meta.get('count', 0) // per_page + 1:
                break

            page += 1
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"    ERROR retrieving page {page}: {e}")
            break

    return all_results[:max_results]


def extract_paper_info(work):
    """
    Extract relevant information from OpenAlex work object.
    """
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
    abstract_inverted_index = work.get('abstract_inverted_index', None)
    abstract_text = reconstruct_abstract(abstract_inverted_index)

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


def process_policy(policy_row):
    """
    Process a single policy: search AER and save results.
    """
    policy_name = policy_row['policy_name']
    policy_abbr = policy_row['policy_abbreviation']
    policy_year = policy_row['policy_year']
    policy_category = policy_row['policy_category']
    search_terms_str = policy_row['search_terms']

    search_terms = [term.strip() for term in search_terms_str.split('|')]

    print(f"\n{'='*80}")
    print(f"Processing: {policy_name} ({policy_abbr}) in AER")
    print(f"Year: {policy_year} | Category: {policy_category}")
    print(f"Search terms: {len(search_terms)}")
    print(f"{'='*80}")

    all_papers = []
    search_metadata = []

    for term in search_terms:
        results = search_aer(term, per_page=100, max_results=500)

        safe_term = term.replace(' ', '_').replace('/', '_').lower()
        raw_file = os.path.join(TMP_DIR, f"raw_{policy_abbr}_{safe_term}.json")
        with open(raw_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"    Saved raw results to: {raw_file}")

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

    df = pd.DataFrame(all_papers)

    if len(df) == 0:
        print(f"\n  WARNING: No AER papers found for {policy_name}")
        return {
            'policy_abbreviation': policy_abbr,
            'policy_name': policy_name,
            'total_papers': 0,
            'unique_papers': 0,
            'duplicates_removed': 0,
            'pre_policy_filtered': 0
        }

    print(f"\n  Removing duplicates...")
    initial_count = len(df)
    df_unique = df.drop_duplicates(subset=['openalex_id'], keep='first')
    duplicate_count = initial_count - len(df_unique)
    print(f"    Initial: {initial_count} | Duplicates: {duplicate_count} | Unique: {len(df_unique)}")

    print(f"\n  Filtering papers by publication date (>= {policy_year})...")
    pre_filter_count = len(df_unique)
    df_unique = df_unique[df_unique['publication_year'] >= policy_year]
    filtered_count = pre_filter_count - len(df_unique)
    print(f"    Before filter: {pre_filter_count} | Filtered out: {filtered_count} | After filter: {len(df_unique)}")

    df_unique = df_unique.copy()
    df_unique['policy_studied'] = policy_name
    df_unique['policy_year'] = policy_year
    df_unique['policy_abbreviation'] = policy_abbr
    df_unique['policy_category'] = policy_category
    df_unique['data_source'] = 'AER'
    df_unique['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

    # Add normalized title for cross-validation
    df_unique['normalized_title'] = df_unique['title'].str.lower().str.replace(r'[^\w\s]', '', regex=True).str.strip()

    column_order = [
        'openalex_id', 'doi', 'title', 'normalized_title', 'authors', 'author_count',
        'author_affiliations', 'publication_year', 'publication_date',
        'source_name', 'source_type', 'abstract', 'cited_by_count',
        'is_open_access', 'open_access_url', 'concepts', 'type', 'language',
        'search_term', 'policy_studied', 'policy_year', 'policy_abbreviation',
        'policy_category', 'data_source', 'scrape_date', 'url'
    ]
    df_unique = df_unique[column_order]

    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_aer.parquet")
    df_unique.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"\n  Saved Parquet: {parquet_file}")

    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_aer.csv")
    df_unique.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved CSV: {csv_file}")

    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': int(policy_year),
        'policy_category': policy_category,
        'search_terms': search_terms,
        'journal': 'American Economic Review',
        'journal_source_id': AER_SOURCE_ID,
        'scrape_date': datetime.now().isoformat(),
        'total_papers_found': initial_count,
        'duplicates_removed': duplicate_count,
        'pre_policy_filtered': filtered_count,
        'unique_papers': len(df_unique),
        'search_details': search_metadata
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    if len(df_unique) > 0:
        print(f"\n  SUMMARY for {policy_abbr} in AER:")
        print(f"    Unique papers: {len(df_unique)}")
        print(f"    Date range: {df_unique['publication_year'].min()}-{df_unique['publication_year'].max()}")
        print(f"    Median citations: {df_unique['cited_by_count'].median():.0f}")

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
    Main execution function.
    """
    print("="*80)
    print("POLICY PAPERS SCRAPING FROM AER (via OpenAlex)")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"AER Source ID: {AER_SOURCE_ID}")
    print()

    policies_df = load_policies(POLICIES_FILE)
    print(f"\nPolicies to process:")
    for _, row in policies_df.iterrows():
        print(f"  - {row['policy_name']} ({row['policy_abbreviation']})")

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

    print(f"\n{'='*80}")
    print("OVERALL SUMMARY - AER")
    print(f"{'='*80}")

    summary_df = pd.DataFrame(all_summaries)
    print(summary_df.to_string(index=False))

    summary_file = os.path.join(OUTPUT_DIR, "all_policies_aer_summary.csv")
    summary_df.to_csv(summary_file, index=False)
    print(f"\nSaved overall summary: {summary_file}")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
