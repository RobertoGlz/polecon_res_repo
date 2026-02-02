"""
Sample Construction Analysis for Research Documentation

This script generates detailed analysis to explain:
1. How the sample was formed from each source
2. Why papers appear in one source but not another
3. Characteristics of overlapping vs. non-overlapping papers

Output: A comprehensive Markdown document suitable for a research paper's
methodology section and supplementary materials.

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 2026
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")

# Policies file
POLICIES_FILE = os.path.join(BASE_DIR, "code", "build", "get_policies", "output", "policies.csv")


def load_unified_dataset(policy_abbr):
    """Load the unified dataset for a policy."""
    file_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_unified_dataset.parquet")
    if os.path.exists(file_path):
        return pd.read_parquet(file_path)
    return None


def load_source_metadata(policy_abbr):
    """Load metadata from each source's scraping run."""
    metadata = {}

    # OpenAlex metadata
    oa_meta_file = os.path.join(BASE_DIR, "code", "build", "scrape_policies_openalex", "output",
                                f"{policy_abbr}_metadata.json")
    if os.path.exists(oa_meta_file):
        with open(oa_meta_file, 'r') as f:
            metadata['openalex'] = json.load(f)

    # Semantic Scholar metadata
    ss_meta_file = os.path.join(BASE_DIR, "code", "build", "scrape_policies_semantic_scholar", "output",
                                f"{policy_abbr}_metadata.json")
    if os.path.exists(ss_meta_file):
        with open(ss_meta_file, 'r') as f:
            metadata['semantic_scholar'] = json.load(f)

    # NBER metadata
    nber_meta_file = os.path.join(BASE_DIR, "code", "build", "scrape_policies_nber", "output",
                                  f"{policy_abbr}_metadata.json")
    if os.path.exists(nber_meta_file):
        with open(nber_meta_file, 'r') as f:
            metadata['nber'] = json.load(f)

    return metadata


def load_policies():
    """Load policy definitions including search terms."""
    if os.path.exists(POLICIES_FILE):
        return pd.read_csv(POLICIES_FILE)
    return None


def categorize_papers(df):
    """
    Categorize papers by source presence.

    Returns dict with paper indices for each category.
    """
    categories = {
        'all_three': df[(df['in_openalex'] == 1) & (df['in_semantic_scholar'] == 1) & (df['in_nber'] == 1)].index.tolist(),
        'oa_and_ss_only': df[(df['in_openalex'] == 1) & (df['in_semantic_scholar'] == 1) & (df['in_nber'] == 0)].index.tolist(),
        'oa_and_nber_only': df[(df['in_openalex'] == 1) & (df['in_semantic_scholar'] == 0) & (df['in_nber'] == 1)].index.tolist(),
        'ss_and_nber_only': df[(df['in_openalex'] == 0) & (df['in_semantic_scholar'] == 1) & (df['in_nber'] == 1)].index.tolist(),
        'oa_only': df[(df['in_openalex'] == 1) & (df['in_semantic_scholar'] == 0) & (df['in_nber'] == 0)].index.tolist(),
        'ss_only': df[(df['in_openalex'] == 0) & (df['in_semantic_scholar'] == 1) & (df['in_nber'] == 0)].index.tolist(),
        'nber_only': df[(df['in_openalex'] == 0) & (df['in_semantic_scholar'] == 0) & (df['in_nber'] == 1)].index.tolist(),
    }
    return categories


def analyze_category_characteristics(df, indices, category_name):
    """Analyze characteristics of papers in a category."""
    if not indices:
        return None

    subset = df.loc[indices]

    analysis = {
        'count': len(subset),
        'pct_with_abstract': round(100 * (subset['abstract'].notna() & (subset['abstract'] != '')).mean(), 1),
        'pct_with_doi': round(100 * subset['doi'].notna().mean(), 1),
        'year_distribution': subset['publication_year'].value_counts().sort_index().to_dict(),
        'year_min': int(subset['publication_year'].min()) if subset['publication_year'].notna().any() else None,
        'year_max': int(subset['publication_year'].max()) if subset['publication_year'].notna().any() else None,
        'year_median': int(subset['publication_year'].median()) if subset['publication_year'].notna().any() else None,
    }

    # Citation analysis
    if 'cited_by_count' in subset.columns and subset['cited_by_count'].notna().any():
        citations = subset['cited_by_count'].dropna()
        analysis['citations_median'] = float(citations.median())
        analysis['citations_mean'] = round(float(citations.mean()), 1)
        analysis['citations_max'] = int(citations.max())

    # Venue analysis
    if 'venue' in subset.columns:
        venues = subset['venue'].dropna()
        if len(venues) > 0:
            analysis['top_venues'] = venues.value_counts().head(10).to_dict()
            analysis['unique_venues'] = venues.nunique()

    # Match method (how was this paper matched across sources)
    if 'match_method' in subset.columns:
        analysis['match_methods'] = subset['match_method'].value_counts().to_dict()

    # Sample papers
    sample_cols = ['title', 'publication_year', 'venue', 'doi']
    sample_cols = [c for c in sample_cols if c in subset.columns]
    analysis['sample_papers'] = subset[sample_cols].head(5).to_dict('records')

    return analysis


def explain_source_differences():
    """
    Return detailed explanations of why sources differ.
    Based on understanding of each API/source.
    """
    explanations = {
        'openalex': {
            'name': 'OpenAlex',
            'description': 'OpenAlex is a comprehensive open catalog of scholarly works, authors, venues, institutions, and concepts.',
            'api': 'https://api.openalex.org/works',
            'search_method': 'Full-text search across titles, abstracts, and full text (when available)',
            'coverage': 'Broad academic coverage including journals, conferences, books, dissertations, and preprints',
            'strengths': [
                'Comprehensive DOI coverage',
                'Rich metadata including concepts, affiliations',
                'Open access status tracking',
                'Citation counts from multiple sources',
            ],
            'limitations': [
                'Abstracts stored as inverted index (may have reconstruction issues)',
                'Some newer papers may have delayed indexing',
                'Search relevance algorithm may differ from other sources',
            ],
            'why_unique_papers': [
                'Indexes venues not covered by Semantic Scholar (e.g., law reviews, policy journals)',
                'Better coverage of older publications',
                'Includes non-English publications',
                'Indexes institutional repositories and working papers beyond NBER',
            ],
        },
        'semantic_scholar': {
            'name': 'Semantic Scholar',
            'description': 'Semantic Scholar is an AI-powered research tool developed by the Allen Institute for AI.',
            'api': 'https://api.semanticscholar.org/graph/v1/paper/search',
            'search_method': 'Semantic search using AI/ML models to understand query intent',
            'coverage': 'Strong in computer science, biomedical; expanding to other fields',
            'strengths': [
                'AI-powered relevance ranking',
                'Good coverage of preprints (arXiv, bioRxiv)',
                'Influential citation metrics',
                'Strong CS and biomedical coverage',
            ],
            'limitations': [
                'Historically focused on STEM fields',
                'May have gaps in social sciences, humanities, law',
                'DOI coverage less complete than OpenAlex',
                'API rate limits more restrictive',
            ],
            'why_unique_papers': [
                'Different relevance algorithm returns different papers for same query',
                'Better coverage of certain preprint servers',
                'May surface papers based on semantic similarity not just keyword match',
                'Indexes some venues not in OpenAlex',
            ],
        },
        'nber': {
            'name': 'NBER Working Papers',
            'description': 'The National Bureau of Economic Research (NBER) working paper series.',
            'api': 'Web scraping from nber.org',
            'search_method': 'Keyword search on NBER website',
            'coverage': 'NBER working papers ONLY - a specific subset of economics research',
            'strengths': [
                'Authoritative source for NBER working papers',
                'High-quality economics research',
                'Often early versions of influential papers',
                'Consistent metadata quality',
            ],
            'limitations': [
                'ONLY includes NBER working papers',
                'Does not include published versions of same papers',
                'Limited to economics and related fields',
                'Much smaller scope than general academic indexes',
            ],
            'why_unique_papers': [
                'NBER working papers may not be indexed elsewhere before publication',
                'Some working papers never get published in journals',
                'Working paper versions have different titles/metadata than published versions',
            ],
        },
    }
    return explanations


def explain_overlap_reasons():
    """Explain why papers appear in multiple sources."""
    return {
        'doi_match': {
            'description': 'Papers matched by Digital Object Identifier (DOI)',
            'reliability': 'High - DOIs are unique identifiers assigned by publishers',
            'interpretation': 'Same paper indexed by multiple sources with consistent DOI metadata',
        },
        'title_match': {
            'description': 'Papers matched by normalized title (lowercase, no punctuation)',
            'reliability': 'Medium - may have false positives for common titles',
            'interpretation': 'Likely same paper, but sources may have different metadata (e.g., one has DOI, other does not)',
        },
        'no_match': {
            'description': 'Papers that appear in only one source',
            'reliability': 'N/A',
            'interpretation': 'Either unique to source, or matching failed due to metadata differences',
        },
    }


def generate_methodology_text(policy_abbr, df, categories, source_metadata, policy_info):
    """Generate methodology section text for a research paper."""

    lines = []

    # Get search terms
    search_terms = policy_info['search_terms'].split('|') if policy_info is not None else []
    search_terms = [t.strip() for t in search_terms]

    lines.append("### Data Collection Methodology")
    lines.append("")
    lines.append(f"We collected academic papers related to {policy_info['policy_name'] if policy_info is not None else policy_abbr} ")
    lines.append(f"from three complementary sources: OpenAlex, Semantic Scholar, and the NBER Working Papers database.")
    lines.append("")

    # Search terms
    lines.append("**Search Terms:**")
    lines.append("")
    lines.append("We used the following search terms to query each source:")
    lines.append("")
    for term in search_terms:
        lines.append(f"- \"{term}\"")
    lines.append("")

    # Source-specific methodology
    lines.append("**Source-Specific Collection:**")
    lines.append("")

    lines.append("*OpenAlex:* We queried the OpenAlex API using each search term, retrieving up to 1,500 ")
    lines.append("results per term. Papers were filtered to those published on or after the policy year ")
    lines.append(f"({policy_info['policy_year'] if policy_info is not None else 'N/A'}). ")
    lines.append("We applied relevance filtering, keeping only papers where at least one search term ")
    lines.append("appeared in the title or abstract (or papers without abstracts, which could not be filtered).")
    lines.append("")

    lines.append("*Semantic Scholar:* We queried the Semantic Scholar Academic Graph API with the same ")
    lines.append("search terms and filtering criteria. Semantic Scholar uses AI-powered semantic search, ")
    lines.append("which may return different results than keyword-based search.")
    lines.append("")

    lines.append("*NBER:* We searched the NBER working papers database, which contains only NBER-affiliated ")
    lines.append("working papers. This provides a focused but limited subset of economics research.")
    lines.append("")

    # Deduplication
    lines.append("**Deduplication and Matching:**")
    lines.append("")
    lines.append("Papers were deduplicated across sources using a two-stage matching process:")
    lines.append("")
    lines.append("1. *DOI Matching (Primary):* Papers with identical normalized DOIs were considered the same paper.")
    lines.append("2. *Title Matching (Secondary):* For papers without DOI matches, we compared normalized titles ")
    lines.append("   (lowercase, punctuation removed). Papers with identical normalized titles were considered matches.")
    lines.append("")

    # Sample sizes
    total = len(df)
    in_oa = df['in_openalex'].sum()
    in_ss = df['in_semantic_scholar'].sum()
    in_nber = df['in_nber'].sum()

    lines.append("**Final Sample:**")
    lines.append("")
    lines.append(f"The final unified sample contains {total:,} unique papers:")
    lines.append(f"- {in_oa:,} papers from OpenAlex ({round(100*in_oa/total, 1)}%)")
    lines.append(f"- {in_ss:,} papers from Semantic Scholar ({round(100*in_ss/total, 1)}%)")
    lines.append(f"- {in_nber:,} papers from NBER ({round(100*in_nber/total, 1)}%)")
    lines.append("")

    return '\n'.join(lines)


def generate_sample_construction_report(policy_abbr):
    """Generate comprehensive sample construction report."""

    print(f"\nAnalyzing sample construction for {policy_abbr}...")

    # Load data
    df = load_unified_dataset(policy_abbr)
    if df is None:
        print(f"  ERROR: Could not load unified dataset for {policy_abbr}")
        return None

    source_metadata = load_source_metadata(policy_abbr)
    policies_df = load_policies()
    policy_info = None
    if policies_df is not None:
        policy_row = policies_df[policies_df['policy_abbreviation'] == policy_abbr]
        if len(policy_row) > 0:
            policy_info = policy_row.iloc[0].to_dict()

    # Categorize papers
    categories = categorize_papers(df)

    # Analyze each category
    category_analyses = {}
    for cat_name, indices in categories.items():
        category_analyses[cat_name] = analyze_category_characteristics(df, indices, cat_name)

    # Get explanations
    source_explanations = explain_source_differences()
    overlap_explanations = explain_overlap_reasons()

    # Generate report
    lines = []
    lines.append(f"# Sample Construction Analysis: {policy_abbr}")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("This document provides detailed documentation of how the research sample was ")
    lines.append("constructed, suitable for inclusion in a research paper's methodology section ")
    lines.append("and supplementary materials.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Methodology text
    lines.append("## 1. Methodology")
    lines.append("")
    lines.append(generate_methodology_text(policy_abbr, df, categories, source_metadata, policy_info))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Sample composition table
    lines.append("## 2. Sample Composition")
    lines.append("")
    lines.append("### 2.1 Overview by Source Combination")
    lines.append("")
    lines.append("| Category | Count | % of Total | Description |")
    lines.append("|----------|-------|------------|-------------|")

    total = len(df)
    category_descriptions = {
        'all_three': 'In all three sources',
        'oa_and_ss_only': 'In OpenAlex and Semantic Scholar only',
        'oa_and_nber_only': 'In OpenAlex and NBER only',
        'ss_and_nber_only': 'In Semantic Scholar and NBER only',
        'oa_only': 'In OpenAlex only',
        'ss_only': 'In Semantic Scholar only',
        'nber_only': 'In NBER only',
    }

    for cat_name, desc in category_descriptions.items():
        count = len(categories[cat_name])
        pct = round(100 * count / total, 1) if total > 0 else 0
        lines.append(f"| {cat_name} | {count:,} | {pct}% | {desc} |")

    lines.append(f"| **Total** | **{total:,}** | **100%** | |")
    lines.append("")

    # Match method breakdown
    lines.append("### 2.2 How Papers Were Matched")
    lines.append("")
    if 'match_method' in df.columns:
        match_counts = df['match_method'].value_counts()
        lines.append("| Match Method | Count | % | Interpretation |")
        lines.append("|--------------|-------|---|----------------|")
        for method, count in match_counts.items():
            pct = round(100 * count / total, 1)
            interp = overlap_explanations.get(method, {}).get('interpretation', 'N/A')
            lines.append(f"| {method} | {count:,} | {pct}% | {interp[:50]}... |")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Source explanations
    lines.append("## 3. Understanding Source Differences")
    lines.append("")
    lines.append("### 3.1 Why do papers appear in one source but not another?")
    lines.append("")

    for source, info in source_explanations.items():
        lines.append(f"#### {info['name']}")
        lines.append("")
        lines.append(f"**Coverage:** {info['coverage']}")
        lines.append("")
        lines.append(f"**Search Method:** {info['search_method']}")
        lines.append("")
        lines.append("**Why papers may be unique to this source:**")
        for reason in info['why_unique_papers']:
            lines.append(f"- {reason}")
        lines.append("")
        lines.append("**Limitations:**")
        for lim in info['limitations']:
            lines.append(f"- {lim}")
        lines.append("")

    lines.append("### 3.2 Why do papers appear in multiple sources?")
    lines.append("")
    lines.append("Papers appear in multiple sources when:")
    lines.append("")
    lines.append("1. **Both sources index the same venue/journal** - Major academic databases have ")
    lines.append("   overlapping coverage of prominent journals and conferences.")
    lines.append("")
    lines.append("2. **DOI matching succeeds** - When a paper has a DOI and both sources have indexed it ")
    lines.append("   with that DOI, we can confidently identify it as the same paper.")
    lines.append("")
    lines.append("3. **Title matching succeeds** - When DOIs are unavailable, papers with identical ")
    lines.append("   titles (after normalization) are considered the same paper.")
    lines.append("")

    lines.append("---")
    lines.append("")

    # Detailed category analysis
    lines.append("## 4. Detailed Category Analysis")
    lines.append("")

    for cat_name, analysis in category_analyses.items():
        if analysis is None or analysis['count'] == 0:
            continue

        lines.append(f"### 4.{list(category_analyses.keys()).index(cat_name)+1} {category_descriptions[cat_name]}")
        lines.append("")
        lines.append(f"**Count:** {analysis['count']:,} papers")
        lines.append("")

        # Characteristics
        lines.append("**Characteristics:**")
        lines.append(f"- {analysis['pct_with_abstract']}% have abstracts")
        lines.append(f"- {analysis['pct_with_doi']}% have DOIs")
        if analysis.get('year_median'):
            lines.append(f"- Publication years: {analysis['year_min']}-{analysis['year_max']} (median: {analysis['year_median']})")
        if analysis.get('citations_median') is not None:
            lines.append(f"- Citations: median={analysis['citations_median']}, mean={analysis['citations_mean']}, max={analysis['citations_max']}")
        lines.append("")

        # Top venues
        if analysis.get('top_venues'):
            lines.append("**Top Venues:**")
            for venue, count in list(analysis['top_venues'].items())[:5]:
                lines.append(f"- {venue}: {count} papers")
            lines.append("")

        # Sample papers
        if analysis.get('sample_papers'):
            lines.append("**Sample Papers:**")
            for i, paper in enumerate(analysis['sample_papers'][:3], 1):
                title = paper.get('title', 'Unknown')[:80]
                year = paper.get('publication_year', 'N/A')
                lines.append(f"{i}. \"{title}...\" ({year})")
            lines.append("")

        # Interpretation
        lines.append("**Interpretation:**")
        if cat_name == 'oa_only':
            lines.append("These papers are indexed by OpenAlex but not found in Semantic Scholar or NBER. ")
            lines.append("This typically occurs because: (1) the paper is from a venue not well-covered by ")
            lines.append("Semantic Scholar, (2) the paper's metadata differs between sources preventing matching, ")
            lines.append("or (3) the search relevance algorithms returned different results.")
        elif cat_name == 'ss_only':
            lines.append("These papers are indexed by Semantic Scholar but not found in OpenAlex or NBER. ")
            lines.append("Semantic Scholar's AI-powered search may surface papers based on semantic similarity ")
            lines.append("that keyword-based searches miss.")
        elif cat_name == 'nber_only':
            lines.append("These are NBER working papers not found in the other sources. This may occur because: ")
            lines.append("(1) the working paper version has a different title than any published version, ")
            lines.append("(2) the paper has not been indexed by OpenAlex/Semantic Scholar yet, or ")
            lines.append("(3) the paper was never published in a journal.")
        elif cat_name == 'all_three':
            lines.append("These papers appear in all three sources, indicating they are well-established ")
            lines.append("papers with consistent metadata across databases. They are likely published in ")
            lines.append("major venues and have DOIs.")
        elif 'and' in cat_name:
            lines.append("These papers appear in two sources but not the third, suggesting partial coverage ")
            lines.append("overlap between the sources.")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Implications for research
    lines.append("## 5. Implications for Research")
    lines.append("")
    lines.append("### 5.1 Sample Completeness")
    lines.append("")
    lines.append("By combining three sources, we capture a more complete picture of research on this policy ")
    lines.append("than any single source would provide. However, researchers should be aware that:")
    lines.append("")
    lines.append("- The sample is not exhaustive; papers not indexed by any of these sources are not included")
    lines.append("- Search term selection affects which papers are retrieved")
    lines.append("- Relevance filtering may exclude marginally relevant papers")
    lines.append("")

    lines.append("### 5.2 Potential Biases")
    lines.append("")
    lines.append("- **Publication bias:** Working papers (NBER) may differ systematically from published papers")
    lines.append("- **Field bias:** Semantic Scholar has historically emphasized STEM fields")
    lines.append("- **Recency bias:** Newer papers may have incomplete indexing")
    lines.append("- **Language bias:** English-language papers are over-represented")
    lines.append("")

    lines.append("### 5.3 Robustness Checks")
    lines.append("")
    lines.append("To assess sensitivity of results to sample construction, researchers may:")
    lines.append("")
    lines.append("1. **Restrict to papers in multiple sources:** Analyze only papers found in 2+ sources")
    lines.append("2. **Source-specific analysis:** Run analyses separately by source")
    lines.append("3. **Exclude NBER:** Analyze only published papers (OpenAlex + Semantic Scholar)")
    lines.append("")

    # Write report
    report_path = os.path.join(REPORTS_DIR, f"{policy_abbr}_sample_construction.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"  Generated: {report_path}")
    return report_path


def main():
    """Generate sample construction reports for all policies."""
    print("=" * 80)
    print("SAMPLE CONSTRUCTION ANALYSIS")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get policies
    if len(sys.argv) > 1:
        policy_abbrs = sys.argv[1:]
    elif os.path.exists(POLICIES_FILE):
        policies_df = pd.read_csv(POLICIES_FILE)
        policy_abbrs = policies_df['policy_abbreviation'].tolist()
    else:
        policy_abbrs = ['TCJA', 'ACA', 'NCLB']

    print(f"\nPolicies to analyze: {policy_abbrs}")

    # Generate reports
    for policy_abbr in policy_abbrs:
        generate_sample_construction_report(policy_abbr)

    print(f"\n{'='*80}")
    print("COMPLETE")
    print(f"Reports saved to: {REPORTS_DIR}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
