"""
Report Generator Module

Generates Markdown reports for:
1. Per-source progress reports
2. Coverage analysis report with hypotheses
3. Overall unified summary report
"""

import os
from datetime import datetime
from typing import Dict, List


def generate_source_report(source: str,
                           metrics: Dict,
                           policy_abbr: str,
                           output_dir: str) -> str:
    """
    Generate a Markdown report for a single source.

    Parameters:
    -----------
    source : str
        Source name
    metrics : dict
        Quality metrics for this source
    policy_abbr : str
        Policy abbreviation
    output_dir : str
        Directory to save report

    Returns:
    --------
    str : Path to generated report
    """
    source_names = {
        'openalex': 'OpenAlex',
        'semantic_scholar': 'Semantic Scholar',
        'nber': 'NBER'
    }

    lines = []
    lines.append(f"# {source_names.get(source, source)} Report: {policy_abbr}")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Summary Statistics
    lines.append("## Summary Statistics")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Papers | {metrics.get('total_papers', 0)} |")
    lines.append(f"| Papers with Abstract | {metrics.get('pct_with_abstract', 0)}% |")
    lines.append(f"| Papers with DOI | {metrics.get('pct_with_doi', 0)}% |")
    lines.append(f"| Papers with Open Access URL | {metrics.get('pct_with_oa_url', 0)}% |")
    lines.append(f"| Year Range | {metrics.get('year_range', 'N/A')} |")
    lines.append(f"| Median Citations | {metrics.get('median_citations', 'N/A')} |")
    lines.append("")

    # Source-specific notes
    lines.append("## Data Source Notes")
    lines.append("")

    if source == 'openalex':
        lines.append("- **API:** OpenAlex API (https://api.openalex.org/)")
        lines.append("- **Coverage:** Comprehensive academic literature index")
        lines.append("- **Abstracts:** Stored as inverted index, reconstructed during scraping")
        lines.append("- **DOIs:** Generally well-covered")
        lines.append("")
    elif source == 'semantic_scholar':
        lines.append("- **API:** Semantic Scholar API (https://api.semanticscholar.org/)")
        lines.append("- **Coverage:** Computer science focus, expanding to other fields")
        lines.append("- **Abstracts:** Direct text when available")
        lines.append("- **DOIs:** Coverage varies by field")
        lines.append("")
    elif source == 'nber':
        lines.append("- **Source:** NBER Working Papers website")
        lines.append("- **Coverage:** NBER working papers only")
        lines.append("- **Abstracts:** Scraped from paper pages")
        lines.append("- **Note:** Limited to NBER publications, not general academic literature")
        lines.append("")

    # Write report
    report_path = os.path.join(output_dir, f"{policy_abbr}_{source}_report.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return report_path


def generate_coverage_report(coverage_analysis: Dict,
                             policy_abbr: str,
                             output_dir: str) -> str:
    """
    Generate a Markdown report for coverage analysis.

    Parameters:
    -----------
    coverage_analysis : dict
        Coverage analysis from coverage_analyzer module
    policy_abbr : str
        Policy abbreviation
    output_dir : str
        Directory to save report

    Returns:
    --------
    str : Path to generated report
    """
    lines = []
    lines.append(f"# Coverage Analysis Report: {policy_abbr}")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("This report analyzes why papers appear in some sources but not others.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Overall Statistics
    stats = coverage_analysis.get('overall_stats', {})
    lines.append("## Overall Statistics")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Unified Papers | {stats.get('total_unified_papers', 0)} |")
    lines.append(f"| In OpenAlex | {stats.get('papers_in_openalex', 0)} |")
    lines.append(f"| In Semantic Scholar | {stats.get('papers_in_semantic_scholar', 0)} |")
    lines.append(f"| In NBER | {stats.get('papers_in_nber', 0)} |")
    lines.append(f"| In All Three Sources | {stats.get('papers_in_all_three', 0)} |")
    lines.append(f"| In Exactly One Source | {stats.get('papers_in_exactly_one', 0)} |")
    lines.append(f"| In Exactly Two Sources | {stats.get('papers_in_exactly_two', 0)} |")
    lines.append("")

    # Pairwise Comparisons
    lines.append("## Pairwise Comparisons")
    lines.append("")

    pairwise = coverage_analysis.get('pairwise_comparisons', {})
    for key, comparison in pairwise.items():
        s1 = comparison.get('source1', '').replace('_', ' ').title()
        s2 = comparison.get('source2', '').replace('_', ' ').title()

        lines.append(f"### {s1} vs {s2}")
        lines.append("")
        lines.append(f"- Papers in {s1} only: {comparison.get('in_source1_only', 0)}")
        lines.append(f"- Papers in {s2} only: {comparison.get('in_source2_only', 0)}")
        lines.append(f"- Papers in both: {comparison.get('in_both', 0)}")
        lines.append("")

        # Source 1 only analysis
        s1_analysis = comparison.get('source1_only_analysis', {})
        if s1_analysis:
            lines.append(f"**Papers in {s1} only:**")
            lines.append(f"- {s1_analysis.get('pct_with_abstract', 0)}% have abstracts")
            lines.append(f"- {s1_analysis.get('pct_with_doi', 0)}% have DOIs")
            if s1_analysis.get('median_citations') is not None:
                lines.append(f"- Median citations: {s1_analysis.get('median_citations')}")
            if s1_analysis.get('top_venues'):
                lines.append(f"- Top venues: {', '.join(list(s1_analysis.get('top_venues', {}).keys())[:3])}")
            lines.append("")

        # Source 2 only analysis
        s2_analysis = comparison.get('source2_only_analysis', {})
        if s2_analysis:
            lines.append(f"**Papers in {s2} only:**")
            lines.append(f"- {s2_analysis.get('pct_with_abstract', 0)}% have abstracts")
            lines.append(f"- {s2_analysis.get('pct_with_doi', 0)}% have DOIs")
            if s2_analysis.get('median_citations') is not None:
                lines.append(f"- Median citations: {s2_analysis.get('median_citations')}")
            if s2_analysis.get('top_venues'):
                lines.append(f"- Top venues: {', '.join(list(s2_analysis.get('top_venues', {}).keys())[:3])}")
            lines.append("")

    # Hypotheses
    lines.append("## Hypotheses: Why Coverage Differs")
    lines.append("")

    hypotheses = coverage_analysis.get('hypotheses', [])
    for i, hyp in enumerate(hypotheses, 1):
        lines.append(f"### Hypothesis {i}: {hyp.get('hypothesis', 'Unknown')}")
        lines.append("")
        lines.append(f"**Evidence:**")
        evidence = hyp.get('evidence', {})
        for key, value in evidence.items():
            if isinstance(value, dict) and len(value) > 5:
                # Truncate long dicts
                lines.append(f"- {key}: (showing top 5)")
                for k, v in list(value.items())[:5]:
                    lines.append(f"  - {k}: {v}")
            else:
                lines.append(f"- {key}: {value}")
        lines.append("")
        lines.append(f"**Conclusion:** {hyp.get('conclusion', 'N/A')}")
        lines.append("")

    # Write report
    report_path = os.path.join(output_dir, f"{policy_abbr}_coverage_analysis.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return report_path


def generate_summary_report(all_policy_results: List[Dict],
                            output_dir: str) -> str:
    """
    Generate an overall summary report across all policies.

    Parameters:
    -----------
    all_policy_results : list
        List of result dictionaries from processing each policy
    output_dir : str
        Directory to save report

    Returns:
    --------
    str : Path to generated report
    """
    lines = []
    lines.append("# Unified Dataset Summary Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("This report summarizes the unified policy papers dataset created from")
    lines.append("OpenAlex, Semantic Scholar, and NBER sources.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Summary Table
    lines.append("## Summary by Policy")
    lines.append("")
    lines.append("| Policy | Total Papers | In OpenAlex | In Semantic Scholar | In NBER | In All Sources | With Abstract |")
    lines.append("|--------|--------------|-------------|---------------------|---------|----------------|---------------|")

    total_papers = 0
    total_abstracts = 0

    for result in all_policy_results:
        policy = result.get('policy_abbr', 'Unknown')
        stats = result.get('coverage_stats', {})
        unified_df = result.get('unified_df')

        if unified_df is not None:
            total = len(unified_df)
            total_papers += total
            with_abstract = (unified_df['abstract'].notna() & (unified_df['abstract'] != '')).sum()
            total_abstracts += with_abstract

            lines.append(f"| {policy} | {total} | "
                         f"{stats.get('papers_in_openalex', 0)} | "
                         f"{stats.get('papers_in_semantic_scholar', 0)} | "
                         f"{stats.get('papers_in_nber', 0)} | "
                         f"{stats.get('papers_in_all_three', 0)} | "
                         f"{with_abstract} ({round(100*with_abstract/total, 1)}%) |")

    lines.append("")

    # Overall totals
    lines.append("## Overall Totals")
    lines.append("")
    lines.append(f"- **Total unique papers across all policies:** {total_papers}")
    lines.append(f"- **Total papers with abstracts:** {total_abstracts} ({round(100*total_abstracts/total_papers, 1)}%)")
    lines.append("")

    # Key Findings
    lines.append("## Key Findings")
    lines.append("")
    lines.append("1. **Source Coverage:**")
    lines.append("   - OpenAlex provides the broadest coverage of academic literature")
    lines.append("   - Semantic Scholar complements with additional papers, especially in certain fields")
    lines.append("   - NBER provides unique working papers not indexed elsewhere")
    lines.append("")
    lines.append("2. **Matching Results:**")
    lines.append("   - DOI matching is the most reliable method for deduplication")
    lines.append("   - Title matching catches additional overlaps for papers without DOIs")
    lines.append("   - Most papers appear in only one source")
    lines.append("")
    lines.append("3. **Data Quality:**")
    lines.append("   - Abstract availability varies by source")
    lines.append("   - DOI coverage is highest in OpenAlex")
    lines.append("   - Citation counts may differ across sources due to different counting methods")
    lines.append("")

    # Output Files
    lines.append("## Output Files")
    lines.append("")
    lines.append("For each policy, the following files are generated:")
    lines.append("")
    lines.append("- `{policy}_unified_dataset.parquet` - Main unified dataset")
    lines.append("- `{policy}_unified_dataset.csv` - CSV version")
    lines.append("- `{policy}_match_registry.parquet` - Detailed matching information")
    lines.append("- `{policy}_openalex_report.md` - OpenAlex source report")
    lines.append("- `{policy}_semantic_scholar_report.md` - Semantic Scholar source report")
    lines.append("- `{policy}_nber_report.md` - NBER source report")
    lines.append("- `{policy}_coverage_analysis.md` - Coverage analysis with hypotheses")
    lines.append("")

    # Write report
    report_path = os.path.join(output_dir, "unified_summary_report.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return report_path


def generate_all_reports(policy_abbr: str,
                         source_metrics: Dict[str, Dict],
                         coverage_analysis: Dict,
                         output_dir: str) -> List[str]:
    """
    Generate all reports for a single policy.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation
    source_metrics : dict
        Quality metrics per source
    coverage_analysis : dict
        Coverage analysis results
    output_dir : str
        Directory to save reports

    Returns:
    --------
    list : Paths to generated reports
    """
    report_paths = []

    # Generate source reports
    for source, metrics in source_metrics.items():
        path = generate_source_report(source, metrics, policy_abbr, output_dir)
        report_paths.append(path)
        print(f"  Generated: {path}")

    # Generate coverage report
    path = generate_coverage_report(coverage_analysis, policy_abbr, output_dir)
    report_paths.append(path)
    print(f"  Generated: {path}")

    return report_paths
