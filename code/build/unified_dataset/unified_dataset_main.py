"""
Unified Dataset Main Script

Creates a comprehensive unified policy papers dataset by:
1. Loading data from OpenAlex, Semantic Scholar, and NBER
2. Matching papers across sources (DOI + title matching)
3. Merging with conflict resolution
4. Analyzing coverage differences
5. Generating reports

Output:
- Unified dataset (Parquet/CSV)
- Match registry
- Per-source reports
- Coverage analysis report
- Summary report

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime

# Add modules to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from modules.data_loader import load_all_sources
from modules.matcher import match_papers
from modules.merger import merge_papers
from modules.coverage_analyzer import analyze_coverage
from modules.report_generator import generate_all_reports, generate_summary_report

# Paths
BASE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")

# Policies file
POLICIES_FILE = os.path.join(BASE_DIR, "code", "build", "get_policies", "output", "policies.csv")

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)


def process_policy(policy_abbr: str) -> dict:
    """
    Process a single policy: load, match, merge, analyze, report.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Processing results and statistics
    """
    print(f"\n{'='*80}")
    print(f"PROCESSING POLICY: {policy_abbr}")
    print(f"{'='*80}")

    result = {
        'policy_abbr': policy_abbr,
        'success': False,
        'error': None,
    }

    try:
        # Step 1: Load data from all sources
        print("\n[Step 1/5] Loading data from all sources...")
        dataframes, source_metrics = load_all_sources(policy_abbr, BASE_DIR)

        if not dataframes:
            result['error'] = "No data loaded from any source"
            return result

        result['source_metrics'] = source_metrics

        # Step 2: Match papers across sources
        print("\n[Step 2/5] Matching papers across sources...")
        registry, match_stats = match_papers(dataframes)

        result['match_stats'] = match_stats

        # Save match registry
        registry_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_match_registry.parquet")
        registry.to_parquet(registry_file, index=False, engine='pyarrow')
        print(f"  Saved match registry: {registry_file}")

        # Step 3: Merge papers into unified dataset
        print("\n[Step 3/5] Merging papers into unified dataset...")
        unified_df = merge_papers(registry, dataframes)

        result['unified_df'] = unified_df

        # Save unified dataset
        parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_unified_dataset.parquet")
        unified_df.to_parquet(parquet_file, index=False, engine='pyarrow')
        print(f"  Saved unified dataset: {parquet_file}")

        csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_unified_dataset.csv")
        unified_df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"  Saved CSV: {csv_file}")

        # Step 4: Analyze coverage differences
        print("\n[Step 4/5] Analyzing coverage differences...")
        coverage_analysis = analyze_coverage(unified_df, source_metrics)

        result['coverage_analysis'] = coverage_analysis
        result['coverage_stats'] = coverage_analysis.get('overall_stats', {})

        # Save coverage analysis
        coverage_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_coverage_analysis.json")
        with open(coverage_file, 'w', encoding='utf-8') as f:
            # Convert non-serializable objects
            analysis_serializable = {
                'overall_stats': coverage_analysis.get('overall_stats', {}),
                'hypotheses': [
                    {
                        'hypothesis': h.get('hypothesis'),
                        'conclusion': h.get('conclusion'),
                        'evidence_keys': list(h.get('evidence', {}).keys())
                    }
                    for h in coverage_analysis.get('hypotheses', [])
                ],
            }
            json.dump(analysis_serializable, f, indent=2, default=str)
        print(f"  Saved coverage analysis: {coverage_file}")

        # Step 5: Generate reports
        print("\n[Step 5/5] Generating reports...")
        report_paths = generate_all_reports(
            policy_abbr,
            source_metrics,
            coverage_analysis,
            REPORTS_DIR
        )

        result['report_paths'] = report_paths
        result['success'] = True

        # Print final summary
        print(f"\n{'='*80}")
        print(f"COMPLETED: {policy_abbr}")
        print(f"{'='*80}")
        print(f"  Unified papers: {len(unified_df)}")
        print(f"  With abstracts: {(unified_df['abstract'].notna() & (unified_df['abstract'] != '')).sum()}")
        print(f"  In OpenAlex: {unified_df['in_openalex'].sum()}")
        print(f"  In Semantic Scholar: {unified_df['in_semantic_scholar'].sum()}")
        print(f"  In NBER: {unified_df['in_nber'].sum()}")
        print(f"  In all sources: {((unified_df['in_openalex'] == 1) & (unified_df['in_semantic_scholar'] == 1) & (unified_df['in_nber'] == 1)).sum()}")

    except Exception as e:
        result['error'] = str(e)
        import traceback
        traceback.print_exc()

    return result


def main():
    """
    Main execution function.

    Processes all policies and generates unified datasets.
    """
    print("=" * 80)
    print("UNIFIED POLICY PAPERS DATASET CREATION")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base directory: {BASE_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Reports directory: {REPORTS_DIR}")

    # Get policy abbreviations
    if len(sys.argv) > 1:
        # Process specific policies from command line
        policy_abbrs = sys.argv[1:]
    elif os.path.exists(POLICIES_FILE):
        # Load from policies file
        policies_df = pd.read_csv(POLICIES_FILE)
        policy_abbrs = policies_df['policy_abbreviation'].tolist()
    else:
        # Default policies
        policy_abbrs = ['TCJA', 'ACA', 'NCLB']

    print(f"\nPolicies to process: {policy_abbrs}")

    # Process each policy
    all_results = []
    for policy_abbr in policy_abbrs:
        result = process_policy(policy_abbr)
        all_results.append(result)

    # Generate summary report
    print("\n" + "=" * 80)
    print("GENERATING SUMMARY REPORT")
    print("=" * 80)

    summary_path = generate_summary_report(all_results, REPORTS_DIR)
    print(f"  Generated: {summary_path}")

    # Final summary
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)

    successful = [r for r in all_results if r.get('success')]
    failed = [r for r in all_results if not r.get('success')]

    print(f"\nProcessed {len(all_results)} policies:")
    print(f"  Successful: {len(successful)}")
    print(f"  Failed: {len(failed)}")

    if failed:
        print("\nFailed policies:")
        for r in failed:
            print(f"  - {r.get('policy_abbr')}: {r.get('error')}")

    print(f"\nOutput files saved to: {OUTPUT_DIR}")
    print(f"Reports saved to: {REPORTS_DIR}")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
