"""
Merger Module

Merges matched papers from different sources into unified records.

Conflict Resolution Strategy:
- Abstract: OpenAlex > Semantic Scholar > NBER (first non-empty, track source)
- DOI: First non-null (typically from OpenAlex)
- open_access_url: First non-null
- cited_by_count: Maximum across sources
- search_terms: Aggregate unique terms
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional


# Priority order for abstract selection
ABSTRACT_PRIORITY = ['openalex', 'semantic_scholar', 'nber']


def resolve_conflicts(rows: List[Dict], dataframes: Dict[str, pd.DataFrame]) -> Dict:
    """
    Resolve conflicts between multiple source records for the same paper.

    Parameters:
    -----------
    rows : list
        List of registry entries for the same unified_id
    dataframes : dict
        Source dataframes

    Returns:
    --------
    dict : Merged record with resolved conflicts
    """
    # Get source data for each entry
    source_data = []
    for entry in rows:
        source = entry['source']
        idx = entry['source_index']
        df = dataframes[source]
        if idx in df.index:
            row_data = df.loc[idx].to_dict()
            row_data['_source'] = source
            source_data.append(row_data)

    if not source_data:
        return None

    # Start with first source as base
    merged = {}

    # === Resolve Title ===
    # Take the longest non-empty title (usually most complete)
    titles = [(d.get('title', ''), d['_source']) for d in source_data if d.get('title')]
    if titles:
        merged['title'] = max(titles, key=lambda x: len(x[0]) if x[0] else 0)[0]
    else:
        merged['title'] = None

    # === Resolve Abstract (priority order) ===
    merged['abstract'] = None
    merged['abstract_source'] = None

    for priority_source in ABSTRACT_PRIORITY:
        for data in source_data:
            if data['_source'] == priority_source:
                abstract = data.get('abstract')
                if abstract and pd.notna(abstract) and str(abstract).strip():
                    merged['abstract'] = abstract
                    # Check if there's an existing abstract_source field
                    if 'abstract_source' in data and data['abstract_source']:
                        merged['abstract_source'] = data['abstract_source']
                    else:
                        merged['abstract_source'] = priority_source
                    break
        if merged['abstract']:
            break

    # === Resolve DOI ===
    merged['doi'] = None
    for data in source_data:
        doi = data.get('doi_normalized') or data.get('doi')
        if doi and pd.notna(doi):
            merged['doi'] = doi
            break

    # === Resolve Open Access URL ===
    merged['open_access_url'] = None
    for data in source_data:
        url = data.get('open_access_url')
        if url and pd.notna(url) and str(url).strip():
            merged['open_access_url'] = url
            break

    # === Resolve Authors ===
    # Take the longest authors string (usually most complete)
    authors_list = [(d.get('authors', ''), d['_source']) for d in source_data if d.get('authors')]
    if authors_list:
        merged['authors'] = max(authors_list, key=lambda x: len(str(x[0])) if x[0] else 0)[0]
    else:
        merged['authors'] = None

    # === Resolve Author Count ===
    author_counts = [d.get('author_count') for d in source_data if d.get('author_count') and pd.notna(d.get('author_count'))]
    merged['author_count'] = max(author_counts) if author_counts else None

    # === Resolve Publication Year ===
    years = [d.get('publication_year') for d in source_data if d.get('publication_year') and pd.notna(d.get('publication_year'))]
    merged['publication_year'] = int(min(years)) if years else None  # Earliest year

    # === Resolve Publication Date ===
    dates = [d.get('publication_date') for d in source_data if d.get('publication_date') and pd.notna(d.get('publication_date'))]
    merged['publication_date'] = min(dates) if dates else None  # Earliest date

    # === Resolve Cited By Count (max) ===
    citations = [d.get('cited_by_count') for d in source_data if d.get('cited_by_count') and pd.notna(d.get('cited_by_count'))]
    merged['cited_by_count'] = int(max(citations)) if citations else None

    # === Resolve Venue ===
    venues = [d.get('venue') or d.get('source_name') for d in source_data]
    venues = [v for v in venues if v and pd.notna(v)]
    merged['venue'] = venues[0] if venues else None

    # === Aggregate Search Terms ===
    all_terms = set()
    for data in source_data:
        term = data.get('search_term')
        if term and pd.notna(term):
            all_terms.add(term)
    merged['search_terms'] = ' | '.join(sorted(all_terms)) if all_terms else None

    # === Source Indicators ===
    sources_present = set(d['_source'] for d in source_data)
    merged['in_openalex'] = 1 if 'openalex' in sources_present else 0
    merged['in_semantic_scholar'] = 1 if 'semantic_scholar' in sources_present else 0
    merged['in_nber'] = 1 if 'nber' in sources_present else 0

    # === Source IDs ===
    merged['openalex_id'] = None
    merged['semantic_scholar_id'] = None
    merged['nber_id'] = None

    for data in source_data:
        source = data['_source']
        if source == 'openalex' and 'openalex_id' in data:
            merged['openalex_id'] = data['openalex_id']
        elif source == 'semantic_scholar' and 'semantic_scholar_id' in data:
            merged['semantic_scholar_id'] = data['semantic_scholar_id']
        elif source == 'nber' and 'nber_id' in data:
            merged['nber_id'] = data['nber_id']

    # === Policy Info (should be same across sources) ===
    for field in ['policy_studied', 'policy_year', 'policy_abbreviation', 'policy_category']:
        values = [d.get(field) for d in source_data if d.get(field) and pd.notna(d.get(field))]
        merged[field] = values[0] if values else None

    # === Normalized Title ===
    merged['normalized_title'] = rows[0].get('normalized_title')

    # === Match Method ===
    merged['match_method'] = rows[0].get('match_method', 'none')

    return merged


def merge_papers(registry: pd.DataFrame, dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all papers from the registry into a unified dataset.

    Parameters:
    -----------
    registry : pd.DataFrame
        Match registry from matcher module
    dataframes : dict
        Source dataframes

    Returns:
    --------
    pd.DataFrame : Unified dataset with one row per unique paper
    """
    print("\nMerging papers into unified dataset...")

    merged_records = []
    unified_ids = registry['unified_id'].unique()

    for i, uid in enumerate(unified_ids):
        if i % 500 == 0 and i > 0:
            print(f"  Progress: {i}/{len(unified_ids)} papers merged")

        # Get all registry entries for this unified ID
        entries = registry[registry['unified_id'] == uid].to_dict('records')

        # Resolve conflicts and merge
        merged = resolve_conflicts(entries, dataframes)

        if merged:
            merged['unified_id'] = uid
            merged_records.append(merged)

    print(f"  Merged {len(merged_records)} unique papers")

    # Create DataFrame
    unified_df = pd.DataFrame(merged_records)

    # Reorder columns
    column_order = [
        'unified_id',
        'title',
        'normalized_title',
        'abstract',
        'abstract_source',
        'doi',
        'open_access_url',
        'authors',
        'author_count',
        'publication_year',
        'publication_date',
        'venue',
        'cited_by_count',
        'search_terms',
        'in_openalex',
        'in_semantic_scholar',
        'in_nber',
        'match_method',
        'openalex_id',
        'semantic_scholar_id',
        'nber_id',
        'policy_studied',
        'policy_year',
        'policy_abbreviation',
        'policy_category',
    ]

    # Only include columns that exist
    final_columns = [c for c in column_order if c in unified_df.columns]
    unified_df = unified_df[final_columns]

    # Print summary
    print(f"\nUnified Dataset Summary:")
    print(f"  Total papers: {len(unified_df)}")
    print(f"  Papers with abstracts: {(unified_df['abstract'].notna() & (unified_df['abstract'] != '')).sum()}")
    print(f"  Papers with DOIs: {unified_df['doi'].notna().sum()}")
    print(f"  Papers in OpenAlex only: {((unified_df['in_openalex'] == 1) & (unified_df['in_semantic_scholar'] == 0) & (unified_df['in_nber'] == 0)).sum()}")
    print(f"  Papers in Semantic Scholar only: {((unified_df['in_openalex'] == 0) & (unified_df['in_semantic_scholar'] == 1) & (unified_df['in_nber'] == 0)).sum()}")
    print(f"  Papers in NBER only: {((unified_df['in_openalex'] == 0) & (unified_df['in_semantic_scholar'] == 0) & (unified_df['in_nber'] == 1)).sum()}")
    print(f"  Papers in multiple sources: {((unified_df['in_openalex'] + unified_df['in_semantic_scholar'] + unified_df['in_nber']) > 1).sum()}")

    return unified_df
