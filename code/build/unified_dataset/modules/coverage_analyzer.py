"""
Coverage Analyzer Module

Analyzes why papers appear in some sources but not others.
Generates hypotheses with supporting evidence about coverage differences.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from collections import defaultdict


def analyze_source_pair(unified_df: pd.DataFrame,
                        source1: str,
                        source2: str) -> Dict:
    """
    Analyze coverage differences between two sources.

    Parameters:
    -----------
    unified_df : pd.DataFrame
        Unified dataset
    source1, source2 : str
        Source names to compare (e.g., 'openalex', 'semantic_scholar')

    Returns:
    --------
    dict : Analysis results
    """
    col1 = f'in_{source1}'
    col2 = f'in_{source2}'

    # Papers in source1 only
    in_s1_only = unified_df[(unified_df[col1] == 1) & (unified_df[col2] == 0)]
    # Papers in source2 only
    in_s2_only = unified_df[(unified_df[col1] == 0) & (unified_df[col2] == 1)]
    # Papers in both
    in_both = unified_df[(unified_df[col1] == 1) & (unified_df[col2] == 1)]

    analysis = {
        'source1': source1,
        'source2': source2,
        'in_source1_only': len(in_s1_only),
        'in_source2_only': len(in_s2_only),
        'in_both': len(in_both),
        'source1_only_analysis': {},
        'source2_only_analysis': {},
    }

    # Analyze papers in source1 only
    if len(in_s1_only) > 0:
        analysis['source1_only_analysis'] = {
            'year_distribution': in_s1_only['publication_year'].value_counts().head(10).to_dict(),
            'pct_with_abstract': round(100 * (in_s1_only['abstract'].notna() & (in_s1_only['abstract'] != '')).mean(), 1),
            'pct_with_doi': round(100 * in_s1_only['doi'].notna().mean(), 1),
            'median_citations': in_s1_only['cited_by_count'].median() if 'cited_by_count' in in_s1_only.columns else None,
            'top_venues': in_s1_only['venue'].value_counts().head(5).to_dict() if 'venue' in in_s1_only.columns else {},
            'sample_titles': in_s1_only['title'].head(5).tolist(),
        }

    # Analyze papers in source2 only
    if len(in_s2_only) > 0:
        analysis['source2_only_analysis'] = {
            'year_distribution': in_s2_only['publication_year'].value_counts().head(10).to_dict(),
            'pct_with_abstract': round(100 * (in_s2_only['abstract'].notna() & (in_s2_only['abstract'] != '')).mean(), 1),
            'pct_with_doi': round(100 * in_s2_only['doi'].notna().mean(), 1),
            'median_citations': in_s2_only['cited_by_count'].median() if 'cited_by_count' in in_s2_only.columns else None,
            'top_venues': in_s2_only['venue'].value_counts().head(5).to_dict() if 'venue' in in_s2_only.columns else {},
            'sample_titles': in_s2_only['title'].head(5).tolist(),
        }

    return analysis


def generate_hypotheses(unified_df: pd.DataFrame,
                        source_metrics: Dict[str, Dict]) -> List[Dict]:
    """
    Generate hypotheses about why papers appear in different sources.

    Parameters:
    -----------
    unified_df : pd.DataFrame
        Unified dataset
    source_metrics : dict
        Quality metrics per source

    Returns:
    --------
    list : List of hypothesis dictionaries with evidence
    """
    hypotheses = []

    # === Hypothesis 1: NBER only indexes NBER working papers ===
    nber_only = unified_df[
        (unified_df['in_nber'] == 1) &
        (unified_df['in_openalex'] == 0) &
        (unified_df['in_semantic_scholar'] == 0)
    ]

    if len(nber_only) > 0:
        venues = nber_only['venue'].value_counts().head(3).to_dict() if 'venue' in nber_only.columns else {}
        hypotheses.append({
            'hypothesis': 'NBER source only indexes NBER working papers',
            'evidence': {
                'nber_only_papers': len(nber_only),
                'total_nber_papers': unified_df['in_nber'].sum(),
                'top_venues_in_nber_only': venues,
            },
            'conclusion': f"NBER has {len(nber_only)} papers not in other sources, likely because NBER specifically scrapes NBER working papers."
        })

    # === Hypothesis 2: OpenAlex has better DOI coverage ===
    oa_only = unified_df[
        (unified_df['in_openalex'] == 1) &
        (unified_df['in_semantic_scholar'] == 0)
    ]
    ss_only = unified_df[
        (unified_df['in_openalex'] == 0) &
        (unified_df['in_semantic_scholar'] == 1)
    ]

    if len(oa_only) > 0 or len(ss_only) > 0:
        oa_doi_pct = round(100 * oa_only['doi'].notna().mean(), 1) if len(oa_only) > 0 else 0
        ss_doi_pct = round(100 * ss_only['doi'].notna().mean(), 1) if len(ss_only) > 0 else 0

        hypotheses.append({
            'hypothesis': 'OpenAlex has more complete DOI metadata than Semantic Scholar',
            'evidence': {
                'openalex_only_papers': len(oa_only),
                'semantic_scholar_only_papers': len(ss_only),
                'openalex_only_doi_pct': oa_doi_pct,
                'semantic_scholar_only_doi_pct': ss_doi_pct,
            },
            'conclusion': f"OpenAlex-only papers have {oa_doi_pct}% DOI coverage vs {ss_doi_pct}% for Semantic Scholar-only papers."
        })

    # === Hypothesis 3: Semantic Scholar may favor certain venues ===
    ss_only_venues = ss_only['venue'].value_counts().head(5).to_dict() if len(ss_only) > 0 and 'venue' in ss_only.columns else {}
    oa_only_venues = oa_only['venue'].value_counts().head(5).to_dict() if len(oa_only) > 0 and 'venue' in oa_only.columns else {}

    if ss_only_venues or oa_only_venues:
        hypotheses.append({
            'hypothesis': 'Sources have different venue coverage patterns',
            'evidence': {
                'top_venues_in_openalex_only': oa_only_venues,
                'top_venues_in_semantic_scholar_only': ss_only_venues,
            },
            'conclusion': "Different sources index different venues, leading to non-overlapping coverage."
        })

    # === Hypothesis 4: Year distribution differences ===
    if len(oa_only) > 0 and len(ss_only) > 0:
        oa_median_year = oa_only['publication_year'].median()
        ss_median_year = ss_only['publication_year'].median()

        hypotheses.append({
            'hypothesis': 'Sources may have different temporal coverage',
            'evidence': {
                'openalex_only_median_year': oa_median_year,
                'semantic_scholar_only_median_year': ss_median_year,
                'openalex_only_year_range': f"{int(oa_only['publication_year'].min())}-{int(oa_only['publication_year'].max())}" if len(oa_only) > 0 else None,
                'semantic_scholar_only_year_range': f"{int(ss_only['publication_year'].min())}-{int(ss_only['publication_year'].max())}" if len(ss_only) > 0 else None,
            },
            'conclusion': f"OpenAlex-only papers have median year {oa_median_year}, Semantic Scholar-only have median year {ss_median_year}."
        })

    # === Hypothesis 5: Search term effectiveness differs ===
    search_term_coverage = {}
    if 'search_terms' in unified_df.columns:
        for _, row in unified_df.iterrows():
            terms = str(row.get('search_terms', '')).split(' | ')
            for term in terms:
                if term and term.strip():
                    if term not in search_term_coverage:
                        search_term_coverage[term] = {'openalex': 0, 'semantic_scholar': 0, 'nber': 0}
                    if row['in_openalex'] == 1:
                        search_term_coverage[term]['openalex'] += 1
                    if row['in_semantic_scholar'] == 1:
                        search_term_coverage[term]['semantic_scholar'] += 1
                    if row['in_nber'] == 1:
                        search_term_coverage[term]['nber'] += 1

    if search_term_coverage:
        hypotheses.append({
            'hypothesis': 'Different search terms have different effectiveness across sources',
            'evidence': {
                'search_term_coverage': search_term_coverage
            },
            'conclusion': "Search API differences mean the same query returns different results across sources."
        })

    return hypotheses


def analyze_coverage(unified_df: pd.DataFrame,
                     source_metrics: Dict[str, Dict]) -> Dict:
    """
    Main function to analyze coverage differences across sources.

    Parameters:
    -----------
    unified_df : pd.DataFrame
        Unified dataset
    source_metrics : dict
        Quality metrics per source

    Returns:
    --------
    dict : Complete coverage analysis
    """
    print("\nAnalyzing coverage differences...")

    analysis = {
        'overall_stats': {},
        'pairwise_comparisons': {},
        'hypotheses': [],
    }

    # Overall statistics
    total_papers = len(unified_df)
    analysis['overall_stats'] = {
        'total_unified_papers': total_papers,
        'papers_in_openalex': int(unified_df['in_openalex'].sum()),
        'papers_in_semantic_scholar': int(unified_df['in_semantic_scholar'].sum()),
        'papers_in_nber': int(unified_df['in_nber'].sum()),
        'papers_in_all_three': int(((unified_df['in_openalex'] == 1) &
                                     (unified_df['in_semantic_scholar'] == 1) &
                                     (unified_df['in_nber'] == 1)).sum()),
        'papers_in_exactly_one': int(((unified_df['in_openalex'] +
                                        unified_df['in_semantic_scholar'] +
                                        unified_df['in_nber']) == 1).sum()),
        'papers_in_exactly_two': int(((unified_df['in_openalex'] +
                                        unified_df['in_semantic_scholar'] +
                                        unified_df['in_nber']) == 2).sum()),
    }

    # Pairwise comparisons
    pairs = [
        ('openalex', 'semantic_scholar'),
        ('openalex', 'nber'),
        ('semantic_scholar', 'nber'),
    ]

    for s1, s2 in pairs:
        key = f"{s1}_vs_{s2}"
        analysis['pairwise_comparisons'][key] = analyze_source_pair(unified_df, s1, s2)

    # Generate hypotheses
    analysis['hypotheses'] = generate_hypotheses(unified_df, source_metrics)

    # Print summary
    print(f"\nCoverage Analysis Summary:")
    print(f"  Total unified papers: {analysis['overall_stats']['total_unified_papers']}")
    print(f"  In all three sources: {analysis['overall_stats']['papers_in_all_three']}")
    print(f"  In exactly one source: {analysis['overall_stats']['papers_in_exactly_one']}")
    print(f"  In exactly two sources: {analysis['overall_stats']['papers_in_exactly_two']}")
    print(f"  Generated {len(analysis['hypotheses'])} hypotheses about coverage differences")

    return analysis
