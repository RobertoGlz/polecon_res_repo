"""
Generate diagnostic report for abstract recovery failures.

This script analyzes the failure logs from complement_abstracts_main.py
and generates a comprehensive Markdown report explaining:
1. Overall recovery statistics
2. Failure breakdown by reason
3. Top domains with failed recoveries
4. Sample papers for manual verification

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 2026
"""

import json
import os
from datetime import datetime
from collections import defaultdict
from urllib.parse import urlparse

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")

os.makedirs(REPORTS_DIR, exist_ok=True)


def load_json_file(filepath):
    """Load JSON file if it exists."""
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def extract_domain(url):
    """Extract domain from URL."""
    if not url:
        return 'unknown'
    try:
        parsed = urlparse(url)
        return parsed.netloc or 'unknown'
    except Exception:
        return 'unknown'


def generate_report():
    """Generate comprehensive diagnostic report."""
    print("=" * 80)
    print("GENERATING ABSTRACT RECOVERY DIAGNOSTIC REPORT")
    print("=" * 80)

    # Load failure log
    failure_log_path = os.path.join(TMP_DIR, "abstract_recovery_failures.json")
    failure_log = load_json_file(failure_log_path)

    if not failure_log:
        print(f"ERROR: Failure log not found at {failure_log_path}")
        print("Run complement_abstracts_main.py first to generate the failure log.")
        return

    # Load response files for additional context
    crossref_responses = load_json_file(os.path.join(TMP_DIR, "crossref_responses.json")) or []
    oa_url_responses = load_json_file(os.path.join(TMP_DIR, "oa_url_responses.json")) or []
    pdf_responses = load_json_file(os.path.join(TMP_DIR, "pdf_responses.json")) or []
    ssrn_responses = load_json_file(os.path.join(TMP_DIR, "ssrn_responses.json")) or []

    # Calculate statistics
    total_failures = failure_log.get('total_failures', 0)
    failure_breakdown = failure_log.get('failure_breakdown', {})
    failure_descriptions = failure_log.get('failure_reason_descriptions', {})
    all_failures = failure_log.get('failures', [])

    # Analyze domains with most failures
    domain_failures = defaultdict(list)
    for failure in all_failures:
        url = failure.get('url') or failure.get('open_access_url', '')
        domain = extract_domain(url)
        domain_failures[domain].append(failure)

    # Sort domains by failure count
    sorted_domains = sorted(domain_failures.items(), key=lambda x: -len(x[1]))[:20]

    # Analyze failures by source
    source_failures = defaultdict(list)
    for failure in all_failures:
        source = failure.get('source', 'unknown')
        source_failures[source].append(failure)

    # Generate Markdown report
    report_lines = []
    report_lines.append("# Abstract Recovery Diagnostic Report")
    report_lines.append("")
    report_lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Executive Summary
    report_lines.append("## Executive Summary")
    report_lines.append("")
    report_lines.append(f"- **Total recovery attempts that failed:** {total_failures}")
    report_lines.append(f"- **CrossRef attempts:** {len(crossref_responses)} (failed: {len(source_failures.get('CrossRef', []))})")
    report_lines.append(f"- **Open Access URL attempts:** {len(oa_url_responses)} (failed: {len(source_failures.get('OpenAccess', []))})")
    report_lines.append(f"- **PDF extraction attempts:** {len(pdf_responses)} (failed: {len(source_failures.get('PDF', []))})")
    report_lines.append(f"- **SSRN attempts:** {len(ssrn_responses)} (failed: {len(source_failures.get('SSRN', []))})")
    report_lines.append("")

    # Calculate success rates
    if len(crossref_responses) > 0:
        crossref_success = len([r for r in crossref_responses if r.get('has_abstract')])
        report_lines.append(f"- **CrossRef success rate:** {crossref_success}/{len(crossref_responses)} ({100*crossref_success/len(crossref_responses):.1f}%)")

    if len(oa_url_responses) > 0:
        oa_success = len([r for r in oa_url_responses if r.get('has_abstract')])
        report_lines.append(f"- **OA URL success rate:** {oa_success}/{len(oa_url_responses)} ({100*oa_success/len(oa_url_responses):.1f}%)")

    if len(pdf_responses) > 0:
        pdf_success = len([r for r in pdf_responses if r.get('has_abstract')])
        report_lines.append(f"- **PDF extraction success rate:** {pdf_success}/{len(pdf_responses)} ({100*pdf_success/len(pdf_responses):.1f}%)")

    if len(ssrn_responses) > 0:
        ssrn_success = len([r for r in ssrn_responses if r.get('has_abstract')])
        report_lines.append(f"- **SSRN success rate:** {ssrn_success}/{len(ssrn_responses)} ({100*ssrn_success/len(ssrn_responses):.1f}%)")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Failure Breakdown by Reason
    report_lines.append("## Failure Breakdown by Reason")
    report_lines.append("")
    report_lines.append("| Failure Reason | Count | Description |")
    report_lines.append("|----------------|-------|-------------|")

    for reason, count in sorted(failure_breakdown.items(), key=lambda x: -x[1]):
        description = failure_descriptions.get(reason, 'Unknown')
        report_lines.append(f"| `{reason}` | {count} | {description} |")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Key Insights
    report_lines.append("## Key Insights")
    report_lines.append("")

    # Analyze patterns
    oa_url_no_selector = failure_breakdown.get('oa_url_no_matching_selector', 0)
    oa_url_js_required = failure_breakdown.get('oa_url_javascript_required', 0)
    oa_url_blocked = failure_breakdown.get('oa_url_blocked_403', 0) + failure_breakdown.get('oa_url_blocked_401', 0)
    pdf_no_abstract = failure_breakdown.get('pdf_no_abstract_found', 0)
    crossref_no_abstract = failure_breakdown.get('crossref_no_abstract_in_db', 0)

    if oa_url_no_selector > 0:
        report_lines.append(f"### 1. HTML Selector Misses ({oa_url_no_selector} cases)")
        report_lines.append("")
        report_lines.append("Pages were successfully fetched but our CSS selectors couldn't find the abstract element.")
        report_lines.append("**Action:** Analyze HTML snippets below to identify missing selector patterns.")
        report_lines.append("")

    if oa_url_js_required > 0:
        report_lines.append(f"### 2. JavaScript-Rendered Pages ({oa_url_js_required} cases)")
        report_lines.append("")
        report_lines.append("Pages require JavaScript to render content. Our HTTP requests only get the initial HTML.")
        report_lines.append("**Action:** Consider using Selenium for these domains (like we do for SSRN).")
        report_lines.append("")

    if oa_url_blocked > 0:
        report_lines.append(f"### 3. Access Blocked ({oa_url_blocked} cases)")
        report_lines.append("")
        report_lines.append("Sites returned 403/401 errors, indicating paywalls or bot detection.")
        report_lines.append("**Action:** These may require institutional access or different scraping approaches.")
        report_lines.append("")

    if pdf_no_abstract > 0:
        report_lines.append(f"### 4. PDF Abstract Not Found ({pdf_no_abstract} cases)")
        report_lines.append("")
        report_lines.append("PDFs were downloaded but no 'Abstract' section was found in the text.")
        report_lines.append("**Action:** Review PDF structure - some papers may not have labeled abstracts.")
        report_lines.append("")

    if crossref_no_abstract > 0:
        report_lines.append(f"### 5. CrossRef Has No Abstract ({crossref_no_abstract} cases)")
        report_lines.append("")
        report_lines.append("Papers exist in CrossRef but don't have abstracts in the metadata.")
        report_lines.append("**Action:** This is a data limitation - publishers didn't provide abstracts to CrossRef.")
        report_lines.append("")

    report_lines.append("---")
    report_lines.append("")

    # Top Domains with Failures
    report_lines.append("## Top Domains with Failures")
    report_lines.append("")
    report_lines.append("These domains have the most failed abstract recovery attempts:")
    report_lines.append("")
    report_lines.append("| Domain | Failure Count | Common Failure Reasons |")
    report_lines.append("|--------|---------------|------------------------|")

    for domain, failures in sorted_domains[:15]:
        if domain == 'unknown':
            continue
        # Count failure reasons for this domain
        reasons = defaultdict(int)
        for f in failures:
            reasons[f.get('failure_reason', 'unknown')] += 1
        top_reasons = sorted(reasons.items(), key=lambda x: -x[1])[:2]
        reasons_str = ', '.join([f"{r[0]} ({r[1]})" for r in top_reasons])
        report_lines.append(f"| {domain} | {len(failures)} | {reasons_str} |")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

    # Sample Failures for Manual Verification
    report_lines.append("## Sample Failures for Manual Verification")
    report_lines.append("")
    report_lines.append("Review these examples to understand why abstracts weren't recovered:")
    report_lines.append("")

    # Get samples for each major failure type
    sample_categories = [
        ('oa_url_no_matching_selector', 'HTML Parsed but No Abstract Found'),
        ('oa_url_javascript_required', 'JavaScript Required'),
        ('oa_url_blocked_403', 'Access Blocked (403)'),
        ('pdf_no_abstract_found', 'PDF Extracted but No Abstract Section'),
    ]

    for reason_code, category_name in sample_categories:
        samples = [f for f in all_failures if f.get('failure_reason') == reason_code][:5]
        if samples:
            report_lines.append(f"### {category_name}")
            report_lines.append("")
            for i, sample in enumerate(samples, 1):
                title = sample.get('paper_title', 'Unknown')[:60]
                url = sample.get('url', sample.get('open_access_url', 'N/A'))
                doi = sample.get('doi', 'N/A')
                report_lines.append(f"**{i}. {title}...**")
                report_lines.append(f"- URL: {url}")
                report_lines.append(f"- DOI: {doi}")

                # Include HTML snippet if available
                snippet = sample.get('html_snippet', '')
                if snippet:
                    # Truncate and escape markdown
                    snippet = snippet[:300].replace('|', '\\|').replace('\n', ' ')
                    report_lines.append(f"- HTML Snippet: `{snippet}...`")
                report_lines.append("")
            report_lines.append("")

    # OA URL Responses with HTML Snippets
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Detailed OA URL Failures with HTML Context")
    report_lines.append("")
    report_lines.append("Papers where HTML was fetched successfully but no abstract was extracted:")
    report_lines.append("")

    # Find OA URL failures with HTML snippets
    oa_failures_with_snippets = [
        r for r in oa_url_responses
        if not r.get('has_abstract') and r.get('html_snippet')
    ][:10]

    for i, failure in enumerate(oa_failures_with_snippets, 1):
        title = failure.get('title', 'Unknown')[:50]
        url = failure.get('open_access_url', 'N/A')
        snippet = failure.get('html_snippet', '')[:500]
        reason = failure.get('failure_reason', 'unknown')

        report_lines.append(f"### {i}. {title}")
        report_lines.append(f"- **URL:** {url}")
        report_lines.append(f"- **Failure Reason:** `{reason}`")
        report_lines.append(f"- **HTML Snippet:**")
        report_lines.append("```html")
        report_lines.append(snippet)
        report_lines.append("```")
        report_lines.append("")

    # Recommendations
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## Recommendations")
    report_lines.append("")
    report_lines.append("Based on the failure analysis:")
    report_lines.append("")
    report_lines.append("1. **Add more CSS selectors** for the top failing domains")
    report_lines.append("2. **Use Selenium** for domains that require JavaScript")
    report_lines.append("3. **Improve PDF parsing** to handle papers without 'Abstract' headers")
    report_lines.append("4. **Accept data limitations** for CrossRef/publisher metadata gaps")
    report_lines.append("")

    # Write report
    report_content = '\n'.join(report_lines)
    report_path = os.path.join(REPORTS_DIR, "abstract_recovery_diagnostic.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)

    print(f"\nReport generated: {report_path}")
    print(f"Total failures analyzed: {total_failures}")

    # Also print summary to console
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTop failure reasons:")
    for reason, count in sorted(failure_breakdown.items(), key=lambda x: -x[1])[:5]:
        print(f"  {reason}: {count}")

    print(f"\nTop failing domains:")
    for domain, failures in sorted_domains[:5]:
        if domain != 'unknown':
            print(f"  {domain}: {len(failures)}")

    return report_path


if __name__ == "__main__":
    generate_report()
