"""
Script to cross-validate QJE/AER papers against OpenAlex and Semantic Scholar datasets.

Issue #16: Scrape QJE/AER for policy papers and cross-validate

This script takes the scraped QJE and AER papers and checks how many of them
appear in our OpenAlex (general) and Semantic Scholar datasets.

Pipeline Overview:
------------------
1. Load QJE papers for each policy
2. Load AER papers for each policy
3. Load OpenAlex (general) papers for each policy
4. Load Semantic Scholar papers for each policy
5. Cross-validate using normalized title matching
6. Generate indicators and summary statistics

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
SEMANTIC_SCHOLAR_DIR = os.path.join(SCRIPT_DIR, "..", "..", "build", "scrape_policies_semantic_scholar", "output")

# Policies file (in build folder)
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "..", "build", "get_policies", "output", "policies.csv")

# Normalize paths
QJE_DIR = os.path.normpath(QJE_DIR)
AER_DIR = os.path.normpath(AER_DIR)
OPENALEX_DIR = os.path.normpath(OPENALEX_DIR)
SEMANTIC_SCHOLAR_DIR = os.path.normpath(SEMANTIC_SCHOLAR_DIR)
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


def crossvalidate_journal(journal_df, openalex_df, semantic_scholar_df, journal_name):
    """
    Cross-validate journal papers against OpenAlex and Semantic Scholar.

    Returns the journal DataFrame with added indicator columns.
    """
    if len(journal_df) == 0:
        return journal_df

    # Ensure normalized_title exists
    if 'normalized_title' not in journal_df.columns:
        journal_df = journal_df.copy()
        journal_df['normalized_title'] = journal_df['title'].apply(normalize_title)

    # Create sets of normalized titles for fast lookup
    openalex_titles = set()
    if len(openalex_df) > 0:
        if 'normalized_title' not in openalex_df.columns:
            openalex_df = openalex_df.copy()
            openalex_df['normalized_title'] = openalex_df['title'].apply(normalize_title)
        openalex_titles = set(openalex_df['normalized_title'].dropna())

    semantic_scholar_titles = set()
    if len(semantic_scholar_df) > 0:
        if 'normalized_title' not in semantic_scholar_df.columns:
            semantic_scholar_df = semantic_scholar_df.copy()
            semantic_scholar_df['normalized_title'] = semantic_scholar_df['title'].apply(normalize_title)
        semantic_scholar_titles = set(semantic_scholar_df['normalized_title'].dropna())

    # Add indicator columns
    journal_df = journal_df.copy()
    journal_df['in_openalex'] = journal_df['normalized_title'].isin(openalex_titles)
    journal_df['in_semantic_scholar'] = journal_df['normalized_title'].isin(semantic_scholar_titles)

    return journal_df


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
    semantic_scholar_df = load_parquet_safe(os.path.join(SEMANTIC_SCHOLAR_DIR, f"{policy_abbr}_papers_semantic_scholar.parquet"))

    results = {
        'policy_abbreviation': policy_abbr,
        'policy_name': policy_name,
        'qje_total': len(qje_df),
        'aer_total': len(aer_df),
        'openalex_total': len(openalex_df),
        'semantic_scholar_total': len(semantic_scholar_df),
    }

    # Cross-validate QJE
    if len(qje_df) > 0:
        print("\n  Cross-validating QJE papers...")
        qje_validated = crossvalidate_journal(qje_df, openalex_df, semantic_scholar_df, 'QJE')

        qje_in_openalex = qje_validated['in_openalex'].sum()
        qje_in_ss = qje_validated['in_semantic_scholar'].sum()

        results['qje_in_openalex'] = int(qje_in_openalex)
        results['qje_in_openalex_pct'] = round(qje_in_openalex / len(qje_df) * 100, 1) if len(qje_df) > 0 else 0
        results['qje_in_semantic_scholar'] = int(qje_in_ss)
        results['qje_in_semantic_scholar_pct'] = round(qje_in_ss / len(qje_df) * 100, 1) if len(qje_df) > 0 else 0

        # Save QJE cross-validation results
        qje_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_qje_crossvalidation.csv")
        qje_validated.to_csv(qje_output, index=False)
        print(f"    Saved: {qje_output}")

        print(f"    QJE papers in OpenAlex: {qje_in_openalex}/{len(qje_df)} ({results['qje_in_openalex_pct']}%)")
        print(f"    QJE papers in Semantic Scholar: {qje_in_ss}/{len(qje_df)} ({results['qje_in_semantic_scholar_pct']}%)")
    else:
        results['qje_in_openalex'] = 0
        results['qje_in_openalex_pct'] = 0
        results['qje_in_semantic_scholar'] = 0
        results['qje_in_semantic_scholar_pct'] = 0

    # Cross-validate AER
    if len(aer_df) > 0:
        print("\n  Cross-validating AER papers...")
        aer_validated = crossvalidate_journal(aer_df, openalex_df, semantic_scholar_df, 'AER')

        aer_in_openalex = aer_validated['in_openalex'].sum()
        aer_in_ss = aer_validated['in_semantic_scholar'].sum()

        results['aer_in_openalex'] = int(aer_in_openalex)
        results['aer_in_openalex_pct'] = round(aer_in_openalex / len(aer_df) * 100, 1) if len(aer_df) > 0 else 0
        results['aer_in_semantic_scholar'] = int(aer_in_ss)
        results['aer_in_semantic_scholar_pct'] = round(aer_in_ss / len(aer_df) * 100, 1) if len(aer_df) > 0 else 0

        # Save AER cross-validation results
        aer_output = os.path.join(OUTPUT_DIR, f"{policy_abbr}_aer_crossvalidation.csv")
        aer_validated.to_csv(aer_output, index=False)
        print(f"    Saved: {aer_output}")

        print(f"    AER papers in OpenAlex: {aer_in_openalex}/{len(aer_df)} ({results['aer_in_openalex_pct']}%)")
        print(f"    AER papers in Semantic Scholar: {aer_in_ss}/{len(aer_df)} ({results['aer_in_semantic_scholar_pct']}%)")
    else:
        results['aer_in_openalex'] = 0
        results['aer_in_openalex_pct'] = 0
        results['aer_in_semantic_scholar'] = 0
        results['aer_in_semantic_scholar_pct'] = 0

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
    print("CROSS-VALIDATION: QJE/AER vs OpenAlex/Semantic Scholar")
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

    # Display summary
    display_cols = [
        'policy_abbreviation',
        'qje_total', 'qje_in_openalex_pct', 'qje_in_semantic_scholar_pct',
        'aer_total', 'aer_in_openalex_pct', 'aer_in_semantic_scholar_pct'
    ]
    available_cols = [c for c in display_cols if c in summary_df.columns]
    print(summary_df[available_cols].to_string(index=False))

    # Save overall summary
    overall_file = os.path.join(OUTPUT_DIR, "overall_journal_summary.csv")
    summary_df.to_csv(overall_file, index=False)
    print(f"\nSaved overall summary: {overall_file}")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
