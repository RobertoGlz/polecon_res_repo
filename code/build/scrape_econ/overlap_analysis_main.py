"""
Overlap analysis: Compare EconLit (Pipeline A) vs API sources (Pipeline B).

Issue #24: Economics-restricted scraping pipelines

Compares papers found by EconLit browser scraping against OpenAlex + Semantic Scholar
+ NBER API scraping (econ-filtered). Matching uses DOI (primary) and normalized title
(fallback). Reports overlap at two stages: raw and relevance-filtered.

Usage:
    python overlap_analysis_main.py TCJA
    python overlap_analysis_main.py TCJA --raw-only      # Skip filtered stage
    python overlap_analysis_main.py TCJA --filtered-only  # Skip raw stage

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 2026
"""

import copy
import html
import pandas as pd
import json
import os
import re
import sys
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize_title(title):
    """Normalize title for matching: strip HTML, decode entities, lowercase, no punctuation."""
    if not title or pd.isna(title):
        return ''
    title = str(title)
    title = re.sub(r'<[^>]+>', '', title)   # Strip HTML tags (<scp>, </scp>, etc.)
    title = html.unescape(title)             # Decode HTML entities (&amp; → &)
    title = title.lower()
    title = title.replace('&', ' and ')      # Normalize & → and
    title = re.sub(r'[\u2013\u2014-]+', ' ', title)  # Normalize dashes (en/em/hyphen) → space
    title = re.sub(r'[^\w\s]', '', title)
    title = ' '.join(title.split())
    return title


def normalize_doi(doi):
    """Normalize DOI: lowercase, strip whitespace, remove URL prefix."""
    if not doi or pd.isna(doi):
        return ''
    doi = str(doi).strip().lower()
    for prefix in ['https://doi.org/', 'http://doi.org/', 'doi:']:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
    return doi.strip()


def load_parquet(filepath, label):
    """Load a parquet file, print count."""
    if not os.path.exists(filepath):
        print(f"  WARNING: {label} not found: {filepath}")
        return pd.DataFrame()
    df = pd.read_parquet(filepath)
    print(f"  Loaded {label}: {len(df):,} papers")
    return df


# ── Matching ───────────────────────────────────────────────────────────────────

def compute_overlap(econlit_df, apis_df, stage_label):
    """
    Match EconLit papers against API papers using DOI + normalized title.

    Returns:
        overlap_df: paper-level DataFrame with in_econlit, in_apis, match_method
        stats: dict with counts and breakdowns
    """
    print(f"\n  === {stage_label} Overlap ===")

    if len(econlit_df) == 0 or len(apis_df) == 0:
        print("  WARNING: One or both datasets are empty")
        return pd.DataFrame(), {}

    # Ensure normalized columns
    el = econlit_df.copy()
    ap = apis_df.copy()

    if 'normalized_title' not in el.columns:
        el['normalized_title'] = el['title'].apply(normalize_title)
    if 'normalized_title' not in ap.columns:
        ap['normalized_title'] = ap['title'].apply(normalize_title)

    el['doi_norm'] = el['doi'].apply(normalize_doi)
    ap['doi_norm'] = ap['doi'].apply(normalize_doi)

    # ── DOI matching ──
    el_dois = set(el.loc[el['doi_norm'] != '', 'doi_norm'])
    ap_dois = set(ap.loc[ap['doi_norm'] != '', 'doi_norm'])
    doi_intersection = el_dois & ap_dois
    print(f"  DOI match: {len(doi_intersection)} papers")

    # ── Title matching ──
    el_titles = set(el.loc[el['normalized_title'] != '', 'normalized_title'])
    ap_titles = set(ap.loc[ap['normalized_title'] != '', 'normalized_title'])
    title_intersection = el_titles & ap_titles
    print(f"  Title match: {len(title_intersection)} papers")

    # ── Union of matches (assign match_method per EconLit paper) ──
    el['match_method'] = 'none'
    el['in_apis'] = False

    # DOI matches
    doi_mask = el['doi_norm'].isin(doi_intersection) & (el['doi_norm'] != '')
    el.loc[doi_mask, 'match_method'] = 'doi'
    el.loc[doi_mask, 'in_apis'] = True

    # Title matches (for papers not already matched by DOI)
    title_mask = (
        el['normalized_title'].isin(title_intersection)
        & (el['normalized_title'] != '')
        & (~el['in_apis'])
    )
    el.loc[title_mask, 'match_method'] = 'title'
    el.loc[title_mask, 'in_apis'] = True

    # Check for papers matched by both methods
    both_mask = (
        (el['doi_norm'].isin(doi_intersection) & (el['doi_norm'] != ''))
        & (el['normalized_title'].isin(title_intersection) & (el['normalized_title'] != ''))
    )
    el.loc[both_mask, 'match_method'] = 'doi+title'

    matched_count = el['in_apis'].sum()
    econlit_only = len(el) - matched_count

    # API papers matched to EconLit
    ap['in_econlit'] = False
    ap_doi_mask = ap['doi_norm'].isin(doi_intersection) & (ap['doi_norm'] != '')
    ap_title_mask = (
        ap['normalized_title'].isin(title_intersection)
        & (ap['normalized_title'] != '')
        & (~ap_doi_mask)
    )
    ap.loc[ap_doi_mask | ap_title_mask, 'in_econlit'] = True
    apis_only = len(ap) - ap['in_econlit'].sum()

    # ── Stats ──
    stats = {
        'econlit_total': len(el),
        'apis_total': len(ap),
        'intersection': int(matched_count),
        'econlit_only': int(econlit_only),
        'apis_only': int(apis_only),
        'doi_matches': len(doi_intersection),
        'title_only_matches': int((el['match_method'] == 'title').sum()),
        'jaccard': round(matched_count / (len(el) + apis_only) * 100, 1) if (len(el) + apis_only) > 0 else 0,
        'econlit_recall': round(matched_count / len(el) * 100, 1) if len(el) > 0 else 0,
    }

    print(f"\n  Pipeline A (EconLit):    {stats['econlit_total']:>6,}")
    print(f"  Pipeline B (APIs):       {stats['apis_total']:>6,}")
    print(f"  Intersection:            {stats['intersection']:>6,}")
    print(f"    - DOI matches:         {stats['doi_matches']:>6,}")
    print(f"    - Title-only matches:  {stats['title_only_matches']:>6,}")
    print(f"  Unique to EconLit:       {stats['econlit_only']:>6,}")
    print(f"  Unique to APIs:          {stats['apis_only']:>6,}")
    print(f"  Jaccard similarity:      {stats['jaccard']:>5}%")
    print(f"  EconLit recall (in APIs):{stats['econlit_recall']:>5}%")

    # ── Breakdowns ──

    # By paper_type
    if 'paper_type' in el.columns:
        pt_breakdown = {}
        for pt in el['paper_type'].unique():
            pt_papers = el[el['paper_type'] == pt]
            pt_matched = pt_papers['in_apis'].sum()
            pt_breakdown[pt] = {
                'total': len(pt_papers),
                'matched': int(pt_matched),
                'unmatched': int(len(pt_papers) - pt_matched),
                'match_rate': round(pt_matched / len(pt_papers) * 100, 1) if len(pt_papers) > 0 else 0,
            }
        stats['by_paper_type'] = pt_breakdown
        print(f"\n  By paper_type:")
        for pt, info in pt_breakdown.items():
            print(f"    {pt}: {info['total']} total, {info['matched']} matched ({info['match_rate']}%)")

    # By journal (top 20 EconLit sources)
    if 'source_name' in el.columns:
        journal_data = []
        for src, grp in el.groupby('source_name'):
            journal_data.append({
                'journal': src,
                'total': len(grp),
                'matched': int(grp['in_apis'].sum()),
                'match_rate': round(grp['in_apis'].sum() / len(grp) * 100, 1),
            })
        journal_df = pd.DataFrame(journal_data).sort_values('total', ascending=False)
        stats['by_journal_top20'] = journal_df.head(20).to_dict('records')
        print(f"\n  By journal (top 10):")
        for _, row in journal_df.head(10).iterrows():
            print(f"    {row['journal'][:50]:50s} {row['total']:>3} papers, {row['matched']:>3} matched ({row['match_rate']}%)")

    # By data_sources within Pipeline B (which API found the matched papers)
    if 'data_sources' in ap.columns:
        matched_ap = ap[ap['in_econlit']]
        if len(matched_ap) > 0:
            ds_counts = matched_ap['data_sources'].value_counts().to_dict()
            stats['matched_by_api_source'] = {str(k): int(v) for k, v in ds_counts.items()}
            print(f"\n  Matched API papers by data_sources:")
            for ds, cnt in sorted(ds_counts.items(), key=lambda x: -x[1]):
                print(f"    {ds}: {cnt}")

    # Sample of papers unique to EconLit (for manual inspection)
    unique_el = el[~el['in_apis']].copy()
    if len(unique_el) > 0:
        sample_cols = ['title', 'doi', 'source_name', 'publication_year', 'paper_type']
        sample_cols = [c for c in sample_cols if c in unique_el.columns]
        sample = unique_el[sample_cols].head(20)
        stats['sample_unique_to_econlit'] = sample.to_dict('records')
        print(f"\n  Sample papers unique to EconLit ({min(20, len(unique_el))} of {len(unique_el)}):")
        for _, row in sample.iterrows():
            title = str(row.get('title', ''))[:80]
            year = row.get('publication_year', '?')
            print(f"    [{year}] {title}")

    # Sample of matched API papers NOT in EconLit (for context on what APIs add)
    unique_ap = ap[~ap['in_econlit']].copy()
    if len(unique_ap) > 0 and 'cited_by_count' in unique_ap.columns:
        # Show highly-cited API papers not in EconLit
        top_cited = unique_ap.nlargest(10, 'cited_by_count')
        sample_cols = ['title', 'doi', 'source_name', 'publication_year', 'cited_by_count', 'data_sources']
        sample_cols = [c for c in sample_cols if c in top_cited.columns]
        stats['sample_unique_to_apis_top_cited'] = top_cited[sample_cols].to_dict('records')
        print(f"\n  Top-cited API papers NOT in EconLit ({min(10, len(unique_ap))} of {len(unique_ap):,}):")
        for _, row in top_cited.iterrows():
            title = str(row.get('title', ''))[:60]
            cites = row.get('cited_by_count', 0)
            year = row.get('publication_year', '?')
            print(f"    [{year}] {title}  (cited: {cites})")

    return el, ap, stats


# ── Report generation ──────────────────────────────────────────────────────────

def generate_markdown_report(policy_abbr, policy_name, raw_stats, filtered_stats):
    """Generate a markdown report summarizing overlap at both stages."""
    lines = []
    lines.append(f"# {policy_name} ({policy_abbr}) — EconLit vs API Overlap Report")
    lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    for label, stats in [("Raw (after abstract recovery)", raw_stats),
                         ("Filtered (after relevance filtering)", filtered_stats)]:
        if not stats:
            continue

        lines.append(f"## {label}")
        lines.append("")
        lines.append("| Metric | Count |")
        lines.append("|--------|------:|")
        lines.append(f"| Pipeline A (EconLit) | {stats['econlit_total']:,} |")
        lines.append(f"| Pipeline B (APIs) | {stats['apis_total']:,} |")
        lines.append(f"| Intersection | {stats['intersection']:,} |")
        lines.append(f"| — DOI matches | {stats['doi_matches']:,} |")
        lines.append(f"| — Title-only matches | {stats['title_only_matches']:,} |")
        lines.append(f"| Unique to EconLit | {stats['econlit_only']:,} |")
        lines.append(f"| Unique to APIs | {stats['apis_only']:,} |")
        lines.append(f"| Jaccard similarity | {stats['jaccard']}% |")
        lines.append(f"| EconLit recall (% found in APIs) | {stats['econlit_recall']}% |")
        lines.append("")

        # Paper type breakdown
        if 'by_paper_type' in stats:
            lines.append("### By paper type")
            lines.append("")
            lines.append("| Type | EconLit | Matched | Match Rate |")
            lines.append("|------|--------:|--------:|-----------:|")
            for pt, info in stats['by_paper_type'].items():
                lines.append(f"| {pt} | {info['total']} | {info['matched']} | {info['match_rate']}% |")
            lines.append("")

        # Journal breakdown
        if 'by_journal_top20' in stats:
            lines.append("### By journal (top 20)")
            lines.append("")
            lines.append("| Journal | Papers | Matched | Match Rate |")
            lines.append("|---------|-------:|--------:|-----------:|")
            for row in stats['by_journal_top20']:
                journal = row['journal'][:60]
                lines.append(f"| {journal} | {row['total']} | {row['matched']} | {row['match_rate']}% |")
            lines.append("")

        # API source breakdown for matched papers
        if 'matched_by_api_source' in stats:
            lines.append("### Matched papers by API data source")
            lines.append("")
            lines.append("| Data Sources | Count |")
            lines.append("|-------------|------:|")
            for ds, cnt in sorted(stats['matched_by_api_source'].items(), key=lambda x: -x[1]):
                lines.append(f"| {ds} | {cnt} |")
            lines.append("")

        # Sample unique to EconLit
        if 'sample_unique_to_econlit' in stats and stats['sample_unique_to_econlit']:
            lines.append(f"### Sample papers unique to EconLit ({stats['econlit_only']} total)")
            lines.append("")
            for paper in stats['sample_unique_to_econlit'][:10]:
                title = str(paper.get('title', 'N/A'))[:100]
                year = paper.get('publication_year', '?')
                doi = paper.get('doi', 'N/A')
                lines.append(f"- [{year}] {title}")
                if doi and doi != 'N/A':
                    lines.append(f"  - DOI: {doi}")
            lines.append("")

        # Sample unique to APIs (top cited)
        if 'sample_unique_to_apis_top_cited' in stats and stats['sample_unique_to_apis_top_cited']:
            lines.append(f"### Top-cited papers unique to APIs ({stats['apis_only']:,} total)")
            lines.append("")
            for paper in stats['sample_unique_to_apis_top_cited']:
                title = str(paper.get('title', 'N/A'))[:100]
                year = paper.get('publication_year', '?')
                cites = paper.get('cited_by_count', 0)
                ds = paper.get('data_sources', '?')
                lines.append(f"- [{year}] {title} (cited: {cites}, source: {ds})")
            lines.append("")

    return '\n'.join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python overlap_analysis_main.py <POLICY_ABBR> [--raw-only|--filtered-only]")
        sys.exit(1)

    policy_abbr = sys.argv[1]
    raw_only = '--raw-only' in sys.argv
    filtered_only = '--filtered-only' in sys.argv

    # Load policy info
    policies_file = os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv")
    policies_df = pd.read_csv(policies_file)
    policy_row = policies_df[policies_df['policy_abbreviation'] == policy_abbr]
    if len(policy_row) == 0:
        print(f"ERROR: Policy '{policy_abbr}' not found in policies.csv")
        sys.exit(1)
    policy_name = policy_row.iloc[0]['policy_name']

    print("=" * 80)
    print(f"OVERLAP ANALYSIS: {policy_name} ({policy_abbr})")
    print(f"Pipeline A (EconLit) vs Pipeline B (OpenAlex + SS + NBER)")
    print("=" * 80)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ── Load datasets ──
    print(f"\nLoading datasets...")
    raw_stats = {}
    filtered_stats = {}

    if not filtered_only:
        el_raw = load_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_raw.parquet"),
            "EconLit raw"
        )
        ap_raw = load_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_raw.parquet"),
            "APIs raw"
        )

    if not raw_only:
        el_filt = load_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_filtered.parquet"),
            "EconLit filtered"
        )
        ap_filt = load_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_filtered.parquet"),
            "APIs filtered"
        )

    # ── Compute overlaps ──
    el_raw_annotated = pd.DataFrame()
    ap_raw_annotated = pd.DataFrame()
    el_filt_annotated = pd.DataFrame()
    ap_filt_annotated = pd.DataFrame()

    if not filtered_only and len(el_raw) > 0 and len(ap_raw) > 0:
        el_raw_annotated, ap_raw_annotated, raw_stats = compute_overlap(el_raw, ap_raw, "RAW")

    if not raw_only and len(el_filt) > 0 and len(ap_filt) > 0:
        el_filt_annotated, ap_filt_annotated, filtered_stats = compute_overlap(el_filt, ap_filt, "FILTERED")

    # ── Save paper-level overlap data ──
    print(f"\n{'='*80}")
    print("Saving outputs...")

    # EconLit annotated with in_apis flag (use filtered stage if available, else raw)
    if len(el_filt_annotated) > 0:
        save_cols = [c for c in el_filt_annotated.columns if c != 'doi_norm']
        el_filt_annotated[save_cols].to_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_overlap.parquet"), index=False
        )
        el_filt_annotated[save_cols].to_csv(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_overlap.csv"), index=False
        )
        print(f"  Saved: {policy_abbr}_econlit_overlap.parquet/csv")
    elif len(el_raw_annotated) > 0:
        save_cols = [c for c in el_raw_annotated.columns if c != 'doi_norm']
        el_raw_annotated[save_cols].to_parquet(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_overlap.parquet"), index=False
        )
        el_raw_annotated[save_cols].to_csv(
            os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_overlap.csv"), index=False
        )
        print(f"  Saved: {policy_abbr}_econlit_overlap.parquet/csv")

    # ── Generate markdown report (before stripping samples) ──
    report = generate_markdown_report(policy_abbr, policy_name, raw_stats, filtered_stats)
    report_path = os.path.join(REPORTS_DIR, f"{policy_abbr}_econ_overlap_report.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"  Saved: {report_path}")

    # ── Save summary JSON (strip sample lists for clean JSON) ──
    summary = {
        'policy_abbreviation': policy_abbr,
        'policy_name': policy_name,
        'analysis_date': datetime.now().isoformat(),
        'raw': copy.deepcopy(raw_stats),
        'filtered': copy.deepcopy(filtered_stats),
    }
    for stage_stats in [summary['raw'], summary['filtered']]:
        if stage_stats:
            stage_stats.pop('sample_unique_to_econlit', None)
            stage_stats.pop('sample_unique_to_apis_top_cited', None)

    summary_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_overlap_summary.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Saved: {summary_path}")

    print(f"\n{'='*80}")
    print(f"Done: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
