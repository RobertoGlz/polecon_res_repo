"""
Matcher Module

Matches papers across different sources using:
1. DOI matching (primary) - exact match on normalized DOI
2. Title matching (secondary) - fuzzy match on normalized title

Creates a match registry that tracks which papers from each source
correspond to the same underlying paper.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


def match_by_doi(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, List[int]]:
    """
    Match papers across sources by DOI.

    Parameters:
    -----------
    dataframes : dict
        Dictionary of source name -> DataFrame

    Returns:
    --------
    dict : DOI -> list of (source, index) tuples for papers with that DOI
    """
    doi_matches = defaultdict(list)

    for source, df in dataframes.items():
        if 'doi_normalized' not in df.columns:
            continue

        for idx, row in df.iterrows():
            doi = row.get('doi_normalized')
            if doi and pd.notna(doi):
                doi_matches[doi].append((source, idx))

    # Filter to only DOIs that appear in at least one source
    return {doi: matches for doi, matches in doi_matches.items() if len(matches) >= 1}


def match_by_title(dataframes: Dict[str, pd.DataFrame],
                   already_matched_indices: Dict[str, set]) -> Dict[str, List[Tuple[str, int]]]:
    """
    Match papers across sources by normalized title.

    Only considers papers that weren't already matched by DOI.

    Parameters:
    -----------
    dataframes : dict
        Dictionary of source name -> DataFrame
    already_matched_indices : dict
        Dictionary of source name -> set of indices already matched by DOI

    Returns:
    --------
    dict : normalized_title -> list of (source, index) tuples
    """
    title_matches = defaultdict(list)

    for source, df in dataframes.items():
        matched_indices = already_matched_indices.get(source, set())

        for idx, row in df.iterrows():
            # Skip if already matched by DOI
            if idx in matched_indices:
                continue

            title = row.get('normalized_title')
            if title and pd.notna(title) and len(title) > 10:  # Minimum title length
                title_matches[title].append((source, idx))

    # Filter to only titles that appear in multiple sources or at least one source
    return {title: matches for title, matches in title_matches.items() if len(matches) >= 1}


def create_match_registry(dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Create a registry of matched papers across all sources.

    The registry assigns a unique unified_id to each distinct paper and
    tracks which source(s) contain that paper and how it was matched.

    Parameters:
    -----------
    dataframes : dict
        Dictionary of source name -> DataFrame

    Returns:
    --------
    pd.DataFrame : Match registry with columns:
        - unified_id: Unique identifier for the unified paper
        - source: Which source this entry is from
        - source_index: Index in the source DataFrame
        - match_method: 'doi', 'title', or 'none'
        - doi: DOI if available
        - normalized_title: Normalized title
    """
    registry_entries = []
    unified_id = 0

    # Track which indices have been assigned to a unified paper
    assigned_indices = {source: set() for source in dataframes.keys()}

    # Step 1: Match by DOI
    print("  Matching by DOI...")
    doi_matches = match_by_doi(dataframes)

    # Group DOI matches into unified papers
    for doi, matches in doi_matches.items():
        # All papers with this DOI are the same paper
        for source, idx in matches:
            if idx not in assigned_indices[source]:
                registry_entries.append({
                    'unified_id': unified_id,
                    'source': source,
                    'source_index': idx,
                    'match_method': 'doi',
                    'doi': doi,
                    'normalized_title': dataframes[source].loc[idx, 'normalized_title']
                })
                assigned_indices[source].add(idx)

        unified_id += 1

    doi_matched_count = unified_id
    print(f"    DOI matches: {doi_matched_count} unique papers")

    # Step 2: Match by title (for papers not matched by DOI)
    print("  Matching by title...")
    title_matches = match_by_title(dataframes, assigned_indices)

    # Group title matches into unified papers
    for title, matches in title_matches.items():
        # Check if this title matches across multiple sources
        sources_in_match = set(m[0] for m in matches)

        if len(sources_in_match) > 1:
            # Title appears in multiple sources - likely same paper
            for source, idx in matches:
                if idx not in assigned_indices[source]:
                    registry_entries.append({
                        'unified_id': unified_id,
                        'source': source,
                        'source_index': idx,
                        'match_method': 'title',
                        'doi': dataframes[source].loc[idx].get('doi_normalized'),
                        'normalized_title': title
                    })
                    assigned_indices[source].add(idx)
            unified_id += 1
        else:
            # Title only in one source - unique paper
            for source, idx in matches:
                if idx not in assigned_indices[source]:
                    registry_entries.append({
                        'unified_id': unified_id,
                        'source': source,
                        'source_index': idx,
                        'match_method': 'none',
                        'doi': dataframes[source].loc[idx].get('doi_normalized'),
                        'normalized_title': title
                    })
                    assigned_indices[source].add(idx)
                    unified_id += 1

    title_matched_count = unified_id - doi_matched_count
    print(f"    Title matches: {title_matched_count} additional papers")

    # Step 3: Add any remaining papers that weren't matched
    print("  Adding unmatched papers...")
    unmatched_count = 0
    for source, df in dataframes.items():
        for idx in df.index:
            if idx not in assigned_indices[source]:
                registry_entries.append({
                    'unified_id': unified_id,
                    'source': source,
                    'source_index': idx,
                    'match_method': 'none',
                    'doi': df.loc[idx].get('doi_normalized'),
                    'normalized_title': df.loc[idx].get('normalized_title')
                })
                assigned_indices[source].add(idx)
                unified_id += 1
                unmatched_count += 1

    print(f"    Unmatched papers: {unmatched_count}")
    print(f"    Total unified papers: {unified_id}")

    # Create DataFrame
    registry_df = pd.DataFrame(registry_entries)

    return registry_df


def get_match_statistics(registry: pd.DataFrame, dataframes: Dict[str, pd.DataFrame]) -> Dict:
    """
    Calculate statistics about the matching process.

    Parameters:
    -----------
    registry : pd.DataFrame
        Match registry
    dataframes : dict
        Source dataframes

    Returns:
    --------
    dict : Matching statistics
    """
    stats = {
        'total_unified_papers': registry['unified_id'].nunique(),
        'match_method_counts': registry['match_method'].value_counts().to_dict(),
        'papers_per_source': {},
        'overlap_matrix': {},
    }

    # Papers per source
    for source in dataframes.keys():
        source_entries = registry[registry['source'] == source]
        stats['papers_per_source'][source] = len(source_entries)

    # Calculate overlap matrix
    sources = list(dataframes.keys())
    for i, source1 in enumerate(sources):
        for source2 in sources[i:]:
            if source1 == source2:
                continue

            # Find papers that appear in both sources
            ids_in_source1 = set(registry[registry['source'] == source1]['unified_id'])
            ids_in_source2 = set(registry[registry['source'] == source2]['unified_id'])
            overlap = len(ids_in_source1 & ids_in_source2)

            stats['overlap_matrix'][f"{source1}_and_{source2}"] = overlap

    # Papers in all sources
    if len(sources) == 3:
        ids_by_source = {
            source: set(registry[registry['source'] == source]['unified_id'])
            for source in sources
        }
        all_three = ids_by_source[sources[0]] & ids_by_source[sources[1]] & ids_by_source[sources[2]]
        stats['papers_in_all_sources'] = len(all_three)

    # Papers unique to each source
    stats['unique_to_source'] = {}
    for source in sources:
        source_ids = set(registry[registry['source'] == source]['unified_id'])
        other_ids = set()
        for other_source in sources:
            if other_source != source:
                other_ids |= set(registry[registry['source'] == other_source]['unified_id'])
        unique = source_ids - other_ids
        stats['unique_to_source'][source] = len(unique)

    return stats


def match_papers(dataframes: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, Dict]:
    """
    Main function to match papers across all sources.

    Parameters:
    -----------
    dataframes : dict
        Dictionary of source name -> DataFrame

    Returns:
    --------
    tuple : (match_registry DataFrame, match_statistics dict)
    """
    print("\nMatching papers across sources...")

    # Create match registry
    registry = create_match_registry(dataframes)

    # Calculate statistics
    stats = get_match_statistics(registry, dataframes)

    # Print summary
    print(f"\nMatch Summary:")
    print(f"  Total unified papers: {stats['total_unified_papers']}")
    print(f"  Match methods: {stats['match_method_counts']}")
    print(f"  Papers per source: {stats['papers_per_source']}")

    if 'papers_in_all_sources' in stats:
        print(f"  Papers in all 3 sources: {stats['papers_in_all_sources']}")

    print(f"  Unique to each source: {stats['unique_to_source']}")

    return registry, stats
