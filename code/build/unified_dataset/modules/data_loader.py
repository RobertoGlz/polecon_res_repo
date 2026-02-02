"""
Data Loader Module

Loads and standardizes data from multiple sources:
- OpenAlex (via complement_abstracts output or direct scrape)
- Semantic Scholar
- NBER

Provides consistent column naming and DOI normalization.
"""

import pandas as pd
import os
import re
from typing import Dict, Optional, Tuple


# Standard column mapping for unified dataset
STANDARD_COLUMNS = {
    'title': 'title',
    'abstract': 'abstract',
    'authors': 'authors',
    'author_count': 'author_count',
    'publication_year': 'publication_year',
    'publication_date': 'publication_date',
    'cited_by_count': 'cited_by_count',
    'is_open_access': 'is_open_access',
    'open_access_url': 'open_access_url',
    'search_term': 'search_term',
    'policy_studied': 'policy_studied',
    'policy_year': 'policy_year',
    'policy_abbreviation': 'policy_abbreviation',
    'policy_category': 'policy_category',
    'data_source': 'data_source',
    'scrape_date': 'scrape_date',
}


def normalize_doi(doi: Optional[str]) -> Optional[str]:
    """
    Normalize DOI to a standard format.

    - Removes URL prefix (https://doi.org/, http://dx.doi.org/, etc.)
    - Converts to lowercase
    - Strips whitespace

    Parameters:
    -----------
    doi : str or None
        DOI string to normalize

    Returns:
    --------
    str or None : Normalized DOI, or None if input is empty/invalid
    """
    if not doi or pd.isna(doi):
        return None

    doi = str(doi).strip()
    if not doi:
        return None

    # Remove common URL prefixes
    prefixes = [
        'https://doi.org/',
        'http://doi.org/',
        'https://dx.doi.org/',
        'http://dx.doi.org/',
        'doi.org/',
        'doi:',
    ]

    doi_lower = doi.lower()
    for prefix in prefixes:
        if doi_lower.startswith(prefix.lower()):
            doi = doi[len(prefix):]
            break

    # Normalize to lowercase and strip
    doi = doi.lower().strip()

    # Validate DOI format (should start with 10.)
    if doi and doi.startswith('10.'):
        return doi

    return None


def normalize_title(title: Optional[str]) -> Optional[str]:
    """
    Normalize title for matching purposes.

    - Converts to lowercase
    - Removes punctuation
    - Removes extra whitespace

    Parameters:
    -----------
    title : str or None
        Title string to normalize

    Returns:
    --------
    str or None : Normalized title, or None if input is empty
    """
    if not title or pd.isna(title):
        return None

    title = str(title).lower()
    # Remove punctuation except spaces
    title = re.sub(r'[^\w\s]', '', title)
    # Normalize whitespace
    title = ' '.join(title.split())

    return title if title else None


def load_openalex_data(policy_abbr: str, base_dir: str) -> Optional[pd.DataFrame]:
    """
    Load OpenAlex data for a policy.

    Tries to load from complement_abstracts output first (has recovered abstracts),
    falls back to direct scrape output.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")
    base_dir : str
        Base directory of the repository

    Returns:
    --------
    pd.DataFrame or None : Loaded dataframe, or None if not found
    """
    # Try complement_abstracts output first (has recovered abstracts)
    complement_dir = os.path.join(base_dir, "code", "build", "complement_abstracts", "output")
    complement_file = os.path.join(complement_dir, f"{policy_abbr}_papers_complemented_filtered.parquet")

    if os.path.exists(complement_file):
        print(f"  Loading OpenAlex from complement_abstracts: {complement_file}")
        df = pd.read_parquet(complement_file)
        df['source'] = 'openalex'
        return df

    # Fall back to direct OpenAlex scrape
    openalex_dir = os.path.join(base_dir, "code", "build", "scrape_policies_openalex", "output")
    openalex_file = os.path.join(openalex_dir, f"{policy_abbr}_papers_openalex.parquet")

    if os.path.exists(openalex_file):
        print(f"  Loading OpenAlex from scrape: {openalex_file}")
        df = pd.read_parquet(openalex_file)
        df['source'] = 'openalex'
        return df

    print(f"  WARNING: No OpenAlex data found for {policy_abbr}")
    return None


def load_semantic_scholar_data(policy_abbr: str, base_dir: str) -> Optional[pd.DataFrame]:
    """
    Load Semantic Scholar data for a policy.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")
    base_dir : str
        Base directory of the repository

    Returns:
    --------
    pd.DataFrame or None : Loaded dataframe, or None if not found
    """
    ss_dir = os.path.join(base_dir, "code", "build", "scrape_policies_semantic_scholar", "output")
    ss_file = os.path.join(ss_dir, f"{policy_abbr}_papers_semantic_scholar.parquet")

    if os.path.exists(ss_file):
        print(f"  Loading Semantic Scholar: {ss_file}")
        df = pd.read_parquet(ss_file)
        df['source'] = 'semantic_scholar'
        return df

    print(f"  WARNING: No Semantic Scholar data found for {policy_abbr}")
    return None


def load_nber_data(policy_abbr: str, base_dir: str) -> Optional[pd.DataFrame]:
    """
    Load NBER data for a policy.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")
    base_dir : str
        Base directory of the repository

    Returns:
    --------
    pd.DataFrame or None : Loaded dataframe, or None if not found
    """
    nber_dir = os.path.join(base_dir, "code", "build", "scrape_policies_nber", "output")
    nber_file = os.path.join(nber_dir, f"{policy_abbr}_papers_nber.parquet")

    if os.path.exists(nber_file):
        print(f"  Loading NBER: {nber_file}")
        df = pd.read_parquet(nber_file)
        df['source'] = 'nber'
        return df

    print(f"  WARNING: No NBER data found for {policy_abbr}")
    return None


def standardize_dataframe(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Standardize a dataframe to common column schema.

    Parameters:
    -----------
    df : pd.DataFrame
        Source dataframe
    source : str
        Source name ('openalex', 'semantic_scholar', 'nber')

    Returns:
    --------
    pd.DataFrame : Standardized dataframe with consistent columns
    """
    df = df.copy()

    # Add source-specific ID column name mapping
    id_columns = {
        'openalex': 'openalex_id',
        'semantic_scholar': 'semantic_scholar_id',
        'nber': 'nber_id'
    }

    # Ensure source ID column exists
    source_id_col = id_columns.get(source)
    if source_id_col and source_id_col in df.columns:
        df['source_id'] = df[source_id_col]
    else:
        df['source_id'] = None

    # Normalize DOI (only OpenAlex has DOI)
    if 'doi' in df.columns:
        df['doi_normalized'] = df['doi'].apply(normalize_doi)
    else:
        df['doi'] = None
        df['doi_normalized'] = None

    # Normalize title
    if 'normalized_title' not in df.columns:
        df['normalized_title'] = df['title'].apply(normalize_title)

    # Handle venue/source_name
    if 'venue' in df.columns:
        df['venue'] = df['venue']
    elif 'source_name' in df.columns:
        df['venue'] = df['source_name']
    else:
        df['venue'] = None

    # Ensure all standard columns exist
    for col in STANDARD_COLUMNS.keys():
        if col not in df.columns:
            df[col] = None

    # Ensure open_access_url exists
    if 'open_access_url' not in df.columns:
        if 'url' in df.columns:
            df['open_access_url'] = df['url']
        else:
            df['open_access_url'] = None

    # Add source identifier
    df['source'] = source

    return df


def calculate_quality_metrics(df: pd.DataFrame, source: str) -> Dict:
    """
    Calculate quality metrics for a source dataset.

    Parameters:
    -----------
    df : pd.DataFrame
        Source dataframe
    source : str
        Source name

    Returns:
    --------
    dict : Quality metrics
    """
    total = len(df)
    if total == 0:
        return {
            'source': source,
            'total_papers': 0,
            'pct_with_abstract': 0,
            'pct_with_doi': 0,
            'pct_with_oa_url': 0,
            'year_range': None,
            'median_citations': None
        }

    # Count papers with abstracts
    has_abstract = df['abstract'].notna() & (df['abstract'] != '')

    # Count papers with DOI
    has_doi = df['doi_normalized'].notna() if 'doi_normalized' in df.columns else pd.Series([False] * total)

    # Count papers with open access URL
    has_oa_url = df['open_access_url'].notna() & (df['open_access_url'] != '')

    # Year range
    years = df['publication_year'].dropna()
    year_range = f"{int(years.min())}-{int(years.max())}" if len(years) > 0 else None

    # Median citations
    if 'cited_by_count' in df.columns:
        citations = df['cited_by_count'].dropna()
        median_citations = float(citations.median()) if len(citations) > 0 else None
    else:
        median_citations = None

    return {
        'source': source,
        'total_papers': total,
        'pct_with_abstract': round(100 * has_abstract.sum() / total, 1),
        'pct_with_doi': round(100 * has_doi.sum() / total, 1),
        'pct_with_oa_url': round(100 * has_oa_url.sum() / total, 1),
        'year_range': year_range,
        'median_citations': median_citations
    }


def load_all_sources(policy_abbr: str, base_dir: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict]]:
    """
    Load data from all sources for a policy.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")
    base_dir : str
        Base directory of the repository

    Returns:
    --------
    tuple : (dict of dataframes by source, dict of quality metrics by source)
    """
    print(f"\nLoading data for {policy_abbr}...")

    dataframes = {}
    metrics = {}

    # Load OpenAlex
    df_oa = load_openalex_data(policy_abbr, base_dir)
    if df_oa is not None:
        df_oa = standardize_dataframe(df_oa, 'openalex')
        dataframes['openalex'] = df_oa
        metrics['openalex'] = calculate_quality_metrics(df_oa, 'openalex')
        print(f"    OpenAlex: {len(df_oa)} papers, {metrics['openalex']['pct_with_abstract']}% with abstracts")

    # Load Semantic Scholar
    df_ss = load_semantic_scholar_data(policy_abbr, base_dir)
    if df_ss is not None:
        df_ss = standardize_dataframe(df_ss, 'semantic_scholar')
        dataframes['semantic_scholar'] = df_ss
        metrics['semantic_scholar'] = calculate_quality_metrics(df_ss, 'semantic_scholar')
        print(f"    Semantic Scholar: {len(df_ss)} papers, {metrics['semantic_scholar']['pct_with_abstract']}% with abstracts")

    # Load NBER
    df_nber = load_nber_data(policy_abbr, base_dir)
    if df_nber is not None:
        df_nber = standardize_dataframe(df_nber, 'nber')
        dataframes['nber'] = df_nber
        metrics['nber'] = calculate_quality_metrics(df_nber, 'nber')
        print(f"    NBER: {len(df_nber)} papers, {metrics['nber']['pct_with_abstract']}% with abstracts")

    return dataframes, metrics
