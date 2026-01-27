"""
Script to cross-validate QJE/AER papers against OpenAlex datasets.

Issue #18: Fix cross-validation logic for QJE/AER

This script compares journal-specific scrapes (QJE/AER) against OpenAlex papers
filtered by source. The validation checks:
- Intersection between QJE scrape and OpenAlex (source=QJE)
- Intersection between AER scrape and OpenAlex (source=AER)
- Papers in QJE scrape but NOT in OpenAlex (source=QJE)
- Papers in AER scrape but NOT in OpenAlex (source=AER)
- Papers in OpenAlex (source=QJE) but NOT in QJE scrape
- Papers in OpenAlex (source=AER) but NOT in AER scrape

Pipeline Overview:
------------------
1. Load QJE papers for each policy (from journal-specific scrape)
2. Load AER papers for each policy (from journal-specific scrape)
3. Load OpenAlex (general) papers for each policy
4. Filter OpenAlex to source=QJE and source=AER subsets
5. Cross-validate using normalized title matching
6. Generate detailed comparison statistics

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
"""

import pandas as pd
import json
import os
import re
from datetime import datetime

# Directory paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

# Input directories (QJE/AER in explore folder, others in build folder)
QJE_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_qje", "output")
AER_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_aer", "output")
OPENALEX_DIR = os.path.join(SCRIPT_DIR, "..", "..", "build", "scrape_policies_openalex", "output")

# Policies file (in build folder)
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "..", "build", "get_policies", "output", "policies.csv")

# Journal source names in OpenAlex
QJE_SOURCE_NAME = "The Quarterly Journal of Economics"
AER_SOURCE_NAME = "The American Economic Review"

# Normalize paths
QJE_DIR = os.path.normpath(QJE_DIR)
AER_DIR = os.path.normpath(AER_DIR)
OPENALEX_DIR = os.path.normpath(OPENALEX_DIR)
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

os.makedirs(OUTPUT_DIR, exist_ok=True)


def normalize_title(title):
    """
    Normalize a title for comparison by:
    - Converting to lowercase
    - Removing punctuation
    - Normalizing whitespace
    """
    if not title or pd.isna(title):
        return ''
    title = str(title).lower()
    title = re.sub(r'[^\w\s]', '', title)
    title = ' '.join(title.split())
    return title


def load_parquet_safe(filepath):
    """
    Load a parquet file, returning empty DataFrame if file doesn't exist.
    """
    if os.path.exists(filepath):
        df = pd.read_parquet(filepath)
        print(f"    Loaded {len(df)} papers from {os.path.basename(filepath)}")
        return df
    else:
        print(f"    WARNING: File not found: {filepath}")
        return pd.DataFrame()


def filter_openalex_by_source(openalex_df, source_name):
    """
    Filter OpenAlex DataFrame to papers from a specific journal source.
    Uses case-insensitive partial matching on source_name.
    """
    if len(openalex_df) == 0:
        return pd.DataFrame()

    # Extract journal name without "The" for flexible matching
    journal_key = source_name.replace("The ", "")
    mask = openalex_df['source_name'].str.contains(journal_key, case=False, na=False)
    filtered = openalex_df[mask].copy()
    return filtered


def crossvalidate_journal(journal_df, openalex_filtered_df, journal_name):
    """
    Cross-validate journal-specific scrape against OpenAlex filtered by source.

    Returns:
        dict with comparison results:
        - journal_df with 'in_openalex' indicator
        - openalex_filtered_df with 'in_journal_scrape' indicator
        - counts for each category
    """
    results = {
        'journal_total': len(journal_df),
        'openalex_filtered_total': len(openalex_filtered_df),
        'intersection': 0,
        'in_journal_not_openalex': 0,
        'in_openalex_not_journal': 0,
    }

    if len(journal_df) == 0 and len(openalex_filtered_df) == 0:
        return results, journal_df, openalex_filtered_df

    # Ensure normalized_title exists in journal_df
    if len(journal_df) > 0:
        journal_df = journal_df.copy()
        if 'normalized_title' not in journal_df.columns:
            journal_df['normalized_title'] = journal_df['title'].apply(normalize_title)
        journal_titles = set(journal_df['normalized_title'].dropna())
    else:
        journal_titles = set()

    # Ensure normalized_title exists in openalex_filtered_df
    if len(openalex_filtered_df) > 0:
        openalex_filtered_df = openalex_filtered_df.copy()
        if 'normalized_title' not in openalex_filtered_df.columns:
            openalex_filtered_df['normalized_title'] = openalex_filtered_df['title'].apply(normalize_title)
        openalex_titles = set(openalex_filtered_df['normalized_title'].dropna())
    else:
        openalex_titles = set()

    # Calculate set operations
    intersection = journal_titles & openalex_titles
    in_journal_not_openalex = journal_titles - openalex_titles
    in_openalex_not_journal = openalex_titles - journal_titles

    results['intersection'] = len(intersection)
    results['in_journal_not_openalex'] = len(in_journal_not_openalex)
    results['in_openalex_not_journal'] = len(in_openalex_not_journal)

    # Add indicator columns
    if len(journal_df) > 0:
        journal_df['in_openalex'] = journal_df['normalized_title'].isin(openalex_titles)

    if len(openalex_filtered_df) > 0:
        openalex_filtered_df['in_journal_scrape'] = openalex_filtered_df['normalized_title'].isin(journal_titles)

    return results, journal_df, openalex_filtered_df


def process_policy(policy_abbr, policy_name):
    """
    Process cross-validation for a single policy.
    """
    print(f"\n{'='*80}")
    print(f"Cross-validating: {policy_name} ({policy_abbr})")
    print(f"{'='*80}")

    # Load all datasets
    print("\n  Loading datasets...")
    qje_df = load_parquet_safe(os.path.join(QJE_DIR, f"{policy_abbr}_papers_qje.parquet"))
    aer_df = load_parquet_safe(os.path.join(AER_DIR, f"{policy_abbr}_papers_aer.parquet"))
    openalex_df = load_parquet_safe(os.path.join(OPENALEX_DIR, f"{policy_abbr}_papers_openalex.parquet"))

    # Filter OpenAlex by source
    print("\n  Filtering OpenAlex by journal source...")
    openalex_qje = filter_openalex_by_source(openalex_df, QJE_SOURCE_NAME)
    openalex_aer = filter_openalex_by_source(openalex_df, AER_SOURCE_NAME)
    print(f"    OpenAlex papers with source=QJE: {len(openalex_qje)}")
    print(f"    OpenAlex papers with source=AER: {len(openalex_aer)}")

    results = {
        'policy_abbreviation': policy_abbr,
        'policy_name': policy_name,
        'openalex_total': len(openalex_df),
    }

    # Cross-validate QJE
    print("\n  Cross-validating QJE papers...")
    qje_results, qje_validated, openalex_qje_validated = crossvalidate_journal(
        qje_df, openalex_qje, 'QJE'
    )

    results['qje_scrape_total'] = qje_results['journal_total']
    results['openalex_qje_total'] = qje_results['openalex_filtered_total']
    results['qje_intersection'] = qje_results['intersection']
    results['qje_in_scrape_not_openalex'] = qje_results['in_journal_not_openalex']
    results['qje_in_openalex_not_scrape'] = qje_results['in_openalex_not_journal']

    print(f"    QJE scrape total: {qje_results['journal_total']}")
    print(f"    OpenAlex (source=QJE) total: {qje_results['openalex_filtered_total']}")
    print(f"    Intersection: {qje_results['intersection']}")
    print(f"    In QJE scrape but NOT in OpenAlex: {qje_results['in_journal_not_openalex']}")
    print(f"    In OpenAlex (source=QJE) but NOT in QJE scrape: {qje_results['in_openalex_not_journal']}")

    # Save QJE cross-validation results
    if len(qje_validated) > 0:
        qje_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_qje_scrape_crossvalidation.csv")
        qje_validated.to_csv(qje_output, index=False)
        print(f"    Saved: {qje_output}")

    if len(openalex_qje_validated) > 0:
        openalex_qje_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_qje_crossvalidation.csv")
        openalex_qje_validated.to_csv(openalex_qje_output, index=False)
        print(f"    Saved: {openalex_qje_output}")

    # Cross-validate AER
    print("\n  Cross-validating AER papers...")
    aer_results, aer_validated, openalex_aer_validated = crossvalidate_journal(
        aer_df, openalex_aer, 'AER'
    )

    results['aer_scrape_total'] = aer_results['journal_total']
    results['openalex_aer_total'] = aer_results['openalex_filtered_total']
    results['aer_intersection'] = aer_results['intersection']
    results['aer_in_scrape_not_openalex'] = aer_results['in_journal_not_openalex']
    results['aer_in_openalex_not_scrape'] = aer_results['in_openalex_not_journal']

    print(f"    AER scrape total: {aer_results['journal_total']}")
    print(f"    OpenAlex (source=AER) total: {aer_results['openalex_filtered_total']}")
    print(f"    Intersection: {aer_results['intersection']}")
    print(f"    In AER scrape but NOT in OpenAlex: {aer_results['in_journal_not_openalex']}")
    print(f"    In OpenAlex (source=AER) but NOT in AER scrape: {aer_results['in_openalex_not_journal']}")

    # Save AER cross-validation results
    if len(aer_validated) > 0:
        aer_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_aer_scrape_crossvalidation.csv")
        aer_validated.to_csv(aer_output, index=False)
        print(f"    Saved: {aer_output}")

    if len(openalex_aer_validated) > 0:
        openalex_aer_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_openalex_aer_crossvalidation.csv")
        openalex_aer_validated.to_csv(openalex_aer_output, index=False)
        print(f"    Saved: {openalex_aer_output}")

    # Save policy summary
    summary_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_journal_summary.json")
    with open(summary_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\n  Saved summary: {summary_file}")

    return results


def main():
    """
    Main execution function.
    """
    print("="*80)
    print("CROSS-VALIDATION: QJE/AER Scrapes vs OpenAlex (filtered by source)")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load policies
    if not os.path.exists(POLICIES_FILE):
        print(f"ERROR: Policies file not found: {POLICIES_FILE}")
        return

    policies_df = pd.read_csv(POLICIES_FILE)
    print(f"Loaded {len(policies_df)} policies")

    # Process each policy
    all_results = []
    for _, row in policies_df.iterrows():
        try:
            result = process_policy(row['policy_abbreviation'], row['policy_name'])
            all_results.append(result)
        except Exception as e:
            print(f"\n  ERROR processing {row['policy_name']}: {e}")
            import traceback
            traceback.print_exc()
            continue

    # Create overall summary
    print(f"\n{'='*80}")
    print("OVERALL CROSS-VALIDATION SUMMARY")
    print(f"{'='*80}")

    summary_df = pd.DataFrame(all_results)

    # Display summary for QJE
    print("\nQJE Cross-Validation:")
    qje_cols = [
        'policy_abbreviation',
        'qje_scrape_total', 'openalex_qje_total', 'qje_intersection',
        'qje_in_scrape_not_openalex', 'qje_in_openalex_not_scrape'
    ]
    available_qje_cols = [c for c in qje_cols if c in summary_df.columns]
    print(summary_df[available_qje_cols].to_string(index=False))

    # Display summary for AER
    print("\nAER Cross-Validation:")
    aer_cols = [
        'policy_abbreviation',
        'aer_scrape_total', 'openalex_aer_total', 'aer_intersection',
        'aer_in_scrape_not_openalex', 'aer_in_openalex_not_scrape'
    ]
    available_aer_cols = [c for c in aer_cols if c in summary_df.columns]
    print(summary_df[available_aer_cols].to_string(index=False))

    # Save overall summary
    overall_file = os.path.join(OUTPUT_DIR, "overall_journal_summary.csv")
    summary_df.to_csv(overall_file, index=False)
    print(f"\nSaved overall summary: {overall_file}")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
