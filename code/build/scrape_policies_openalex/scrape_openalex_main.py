"""
Script to scrape papers related to policies from OpenAlex
Issue #1: Scrape policy papers from OpenAlex

This script reads a list of policies from policies.csv and systematically
searches OpenAlex for academic papers related to each policy.

Author: claude ai with modifications by roberto gonzalez
Date: January 9, 2026
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

# User email for OpenAlex polite pool - REPLACE WITH YOUR EMAIL
USER_EMAIL = "your_email@example.com"

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
    Search OpenAlex for papers matching the query
    
    Parameters:
    -----------
    query : str
        Search query string
    per_page : int
        Number of results per page (max 200)
    max_results : int
        Maximum number of results to retrieve
    
    Returns:
    --------
    list : List of paper dictionaries
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
    Extract relevant information from OpenAlex work object
    
    Parameters:
    -----------
    work : dict
        OpenAlex work object
    
    Returns:
    --------
    dict : Extracted paper information
    """
    # Extract author names
    authors = []
    author_affiliations = []
    for authorship in work.get('authorships', []):
        author = authorship.get('author', {})
        author_name = author.get('display_name', '')
        if author_name:
            authors.append(author_name)
        
        # Get affiliations
        institutions = authorship.get('institutions', [])
        if institutions:
            inst_names = [inst.get('display_name', '') for inst in institutions]
            author_affiliations.append('; '.join(inst_names))
        else:
            author_affiliations.append('')
    
    # Extract concepts (topics)
    concepts = [c.get('display_name', '') for c in work.get('concepts', [])]
    
    # Extract publication info
    pub_info = work.get('primary_location', {}) or {}
    source = pub_info.get('source', {}) or {}
    
    paper_info = {
        'openalex_id': work.get('id', ''),
        'doi': work.get('doi', ''),
        'title': work.get('title', ''),
        'abstract': work.get('abstract', ''),
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
    Process a single policy: search OpenAlex and save results
    
    Parameters:
    -----------
    policy_row : pd.Series
        Row from policies DataFrame containing policy information
    
    Returns:
    --------
    dict : Summary statistics for this policy
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
        results = search_openalex(term, per_page=100, max_results=500)
        
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
    
    # Save as Stata file
    stata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_openalex.dta")
    df_unique.to_stata(stata_file, write_index=False, version=118)
    print(f"  Saved Stata: {stata_file}")
    
    # Save metadata
    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': int(policy_year),
        'policy_category': policy_category,
        'search_terms': search_terms,
        'scrape_date': datetime.now().isoformat(),
        'total_papers_found': initial_count,
        'unique_papers': len(df_unique),
        'duplicates_removed': duplicate_count,
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
        'unique_papers': len(df_unique),
        'duplicates_removed': duplicate_count
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
