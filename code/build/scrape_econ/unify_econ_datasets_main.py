"""
unify_econ_datasets_main.py
Issue #22 / #24: Unify EconLit + APIs datasets into a single economics-restricted dataset.

Merges Pipeline A (EconLit) and Pipeline B (OpenAlex + Semantic Scholar + NBER) into
one deduplicated dataset. Uses pre-computed overlap from overlap_analysis_main.py.

Also checks abstract coverage for papers in top-10 economics journals.

Usage:
    python unify_econ_datasets_main.py TCJA
"""

import html
import json
import os
import re
import sys
import argparse
import pandas as pd
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Top-10 Economics Journals ─────────────────────────────────────────────────
# Regex patterns for matching (case-insensitive). Anchored to avoid false
# positives like "Latin American Economic Review" matching "American Economic
# Review" or "European Journal of Political Economy" matching JPE.

TOP10_ECON_JOURNALS = {
    "Quarterly Journal of Economics":
        re.compile(r'^(the\s+)?quarterly journal of economics$', re.I),
    "American Economic Review":
        re.compile(r'^(the\s+)?american economic review$', re.I),
    "Journal of Political Economy":
        re.compile(r'^(the\s+)?journal of political economy', re.I),  # includes JPE Micro/Macro
    "Econometrica":
        re.compile(r'^econometrica$', re.I),
    "Review of Economic Studies":
        re.compile(r'^(the\s+)?review of economic studies$', re.I),
    "Review of Economics and Statistics":
        re.compile(r'^(the\s+)?review of economics and statistics$', re.I),
    "Journal of the European Economic Association":
        re.compile(r'^(the\s+)?journal of the european economic association$', re.I),
    "Economic Journal":
        re.compile(r'^(the\s+)?economic journal$', re.I),
    "American Economic Journal":
        re.compile(r'^american economic journal', re.I),  # includes all AEJ sub-journals
    "Journal of Finance":
        re.compile(r'^(the\s+)?journal of finance$', re.I),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def has_abstract(row):
    """Check if a row has a non-empty abstract."""
    a = row.get('abstract')
    return pd.notna(a) and str(a).strip() != ''


def match_top_journal(source_name):
    """Return the canonical top-10 journal name if source_name matches, else None."""
    if not source_name or pd.isna(source_name):
        return None
    s = str(source_name).strip()
    for name, pattern in TOP10_ECON_JOURNALS.items():
        if pattern.match(s):
            return name
    return None


# ── Abstract Check ────────────────────────────────────────────────────────────

def check_top_journal_abstracts(econlit_df, apis_df):
    """
    Check abstract coverage for papers in top-10 economics journals
    across both datasets. Prints a report and returns stats.
    """
    print("\n" + "=" * 60)
    print("TOP-10 ECONOMICS JOURNALS — ABSTRACT CHECK")
    print("=" * 60)

    results = {}

    for dataset_label, df in [("EconLit", econlit_df), ("APIs", apis_df)]:
        print(f"\n  --- {dataset_label} ({len(df):,} papers) ---")
        df_copy = df.copy()
        df_copy['_top_journal'] = df_copy['source_name'].apply(match_top_journal)
        top_papers = df_copy[df_copy['_top_journal'].notna()]

        if len(top_papers) == 0:
            print("  No papers in top-10 journals.")
            results[dataset_label] = {"total": 0, "with_abstract": 0, "missing": 0, "journals": {}}
            continue

        total = len(top_papers)
        has_abs = top_papers.apply(has_abstract, axis=1)
        n_with = has_abs.sum()
        n_missing = total - n_with

        print(f"  Total in top-10 journals: {total}")
        print(f"  With abstract: {n_with}")
        print(f"  Missing abstract: {n_missing}")

        journal_stats = {}
        for journal_name in sorted(top_papers['_top_journal'].unique()):
            j_papers = top_papers[top_papers['_top_journal'] == journal_name]
            j_has_abs = j_papers.apply(has_abstract, axis=1)
            j_missing = len(j_papers) - j_has_abs.sum()
            print(f"    {journal_name}: {len(j_papers)} papers, {j_missing} missing abstracts")
            journal_stats[journal_name] = {
                "count": len(j_papers),
                "missing_abstracts": int(j_missing),
            }

            # Print details for papers missing abstracts
            if j_missing > 0:
                missing_papers = j_papers[~j_has_abs]
                for _, row in missing_papers.iterrows():
                    print(f"      MISSING: {row['title'][:80]}")

        results[dataset_label] = {
            "total": total,
            "with_abstract": int(n_with),
            "missing": int(n_missing),
            "journals": journal_stats,
        }

    return results


# ── Unification ───────────────────────────────────────────────────────────────

def unify_datasets(econlit_df, apis_df, overlap_df, policy_abbr):
    """
    Merge EconLit and APIs into a single deduplicated dataset.

    - APIs papers form the base (~4,207 rows)
    - Matched EconLit papers enrich existing APIs rows (add econlit_id, in_econlit=True)
    - Unmatched EconLit papers are appended as new rows

    Returns: unified DataFrame
    """
    print("\n" + "=" * 60)
    print("UNIFYING DATASETS")
    print("=" * 60)

    # Use the overlap data to identify which EconLit papers are in APIs
    matched_el = overlap_df[overlap_df['in_apis'] == True].copy()
    unmatched_el = overlap_df[overlap_df['in_apis'] == False].copy()
    print(f"  EconLit matched in APIs: {len(matched_el)}")
    print(f"  EconLit NOT in APIs: {len(unmatched_el)}")

    # --- Start with APIs as base ---
    unified = apis_df.copy()
    unified['in_econlit'] = False
    unified['econlit_id'] = None
    unified['match_method'] = 'none'

    # Normalize DOIs for matching
    unified['_doi_norm'] = unified['doi'].apply(normalize_doi)
    matched_el['_doi_norm'] = matched_el['doi'].apply(normalize_doi)

    # Ensure normalized_title exists
    if 'normalized_title' not in unified.columns:
        unified['normalized_title'] = unified['title'].apply(normalize_title)
    if 'normalized_title' not in matched_el.columns:
        matched_el['normalized_title'] = matched_el['title'].apply(normalize_title)

    # --- Enrich matched APIs rows with EconLit info ---
    # Build lookup from EconLit matched papers: doi -> econlit_id, title -> econlit_id
    doi_to_elid = {}
    title_to_elid = {}
    for _, row in matched_el.iterrows():
        elid = row['econlit_id']
        mm = row['match_method']
        doi_n = row['_doi_norm']
        title_n = row['normalized_title']

        if doi_n and mm in ('doi', 'doi+title'):
            doi_to_elid[doi_n] = elid
        if title_n and mm in ('title', 'doi+title'):
            title_to_elid[title_n] = elid

    # Match APIs rows to EconLit papers. Some EconLit papers match multiple APIs rows
    # (e.g., journal DOI + SSRN DOI). Only enrich ONE APIs row per EconLit paper:
    # prefer journal_article over working_paper, then first encountered.
    enriched_count = 0
    claimed_elids = set()  # track which econlit_ids have been assigned

    # First pass: collect all candidate matches (idx -> elid, match_type)
    candidates = {}  # elid -> list of (api_idx, match_type)
    for idx, row in unified.iterrows():
        doi_n = row['_doi_norm']
        title_n = row.get('normalized_title', '')
        elid = doi_to_elid.get(doi_n) or title_to_elid.get(title_n)
        if elid:
            match_type = 'doi' if doi_n in doi_to_elid else 'title'
            if elid not in candidates:
                candidates[elid] = []
            candidates[elid].append((idx, match_type, row.get('paper_type', '')))

    # Second pass: for each EconLit paper, pick the best APIs row
    for elid, matches in candidates.items():
        # Prefer journal_article, then first match
        best = None
        for idx, mt, pt in matches:
            if best is None:
                best = (idx, mt)
            elif pt == 'journal_article' and matches[0][2] != 'journal_article':
                best = (idx, mt)
                break
        if best:
            idx, mt = best
            unified.at[idx, 'in_econlit'] = True
            unified.at[idx, 'econlit_id'] = elid
            unified.at[idx, 'match_method'] = mt
            enriched_count += 1

    print(f"  APIs rows enriched with EconLit info: {enriched_count} (1:1 with EconLit papers)")

    # --- Append unmatched EconLit papers ---
    if len(unmatched_el) > 0:
        new_rows = []
        for _, row in unmatched_el.iterrows():
            new_row = {
                'openalex_id': None,
                'doi': row.get('doi'),
                'title': row.get('title'),
                'abstract': row.get('abstract'),
                'publication_year': row.get('publication_year'),
                'publication_date': row.get('publication_date'),
                'authors': row.get('authors'),
                'author_count': row.get('author_count'),
                'author_affiliations': None,
                'source_name': row.get('source_name'),
                'source_type': None,
                'is_open_access': row.get('is_open_access'),
                'open_access_url': None,
                'cited_by_count': None,
                'concepts': None,
                'primary_topic_field': None,
                'type': None,
                'language': row.get('language'),
                'paper_type': row.get('paper_type'),
                'url': row.get('econlit_url'),
                'data_source': 'EconLit',
                'search_terms_matched': row.get('search_terms_matched'),
                'normalized_title': row.get('normalized_title'),
                'data_sources': 'EconLit',
                'in_openalex': False,
                'in_semantic_scholar': False,
                'in_nber': False,
                'abstract_source': row.get('abstract_source'),
                'semantic_scholar_id': None,
                'venue': row.get('source_name'),
                's2_fields_of_study': None,
                'nber_id': None,
                'nber_type': None,
                'policy_studied': row.get('policy_studied'),
                'policy_year': row.get('policy_year'),
                'policy_abbreviation': row.get('policy_abbreviation'),
                'policy_category': row.get('policy_category'),
                'scrape_date': row.get('scrape_date'),
                'in_econlit': True,
                'econlit_id': row.get('econlit_id'),
                'match_method': 'none',
            }
            new_rows.append(new_row)

        new_df = pd.DataFrame(new_rows)
        unified = pd.concat([unified, new_df], ignore_index=True)
        print(f"  Appended {len(new_rows)} EconLit-only papers")

    # --- Update data_sources for enriched rows ---
    for idx, row in unified.iterrows():
        if row['in_econlit'] and row.get('data_sources') and 'EconLit' not in str(row['data_sources']):
            unified.at[idx, 'data_sources'] = str(row['data_sources']) + ' | EconLit'

    # --- Assign econ_unified_id ---
    unified = unified.reset_index(drop=True)
    unified.insert(0, 'econ_unified_id', range(1, len(unified) + 1))

    # --- Clean up temp columns ---
    unified.drop(columns=['_doi_norm'], inplace=True, errors='ignore')

    print(f"\n  Unified dataset: {len(unified):,} papers")
    print(f"  in_econlit: {unified['in_econlit'].sum()}")
    print(f"  in_apis (in_openalex | in_ss | in_nber): "
          f"{(unified['in_openalex'] | unified['in_semantic_scholar'] | unified['in_nber']).sum()}")

    # Abstract coverage
    has_abs = unified['abstract'].notna() & (unified['abstract'].str.strip() != '')
    print(f"  With abstract: {has_abs.sum()} ({has_abs.mean()*100:.1f}%)")

    return unified


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Unify EconLit + APIs datasets into one economics-restricted dataset"
    )
    parser.add_argument("policy_abbr", help="Policy abbreviation (e.g., TCJA)")
    args = parser.parse_args()
    policy_abbr = args.policy_abbr.upper()

    # Load datasets
    econlit_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_filtered.parquet")
    apis_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econ_apis_filtered.parquet")
    overlap_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_overlap.parquet")

    for path, label in [(econlit_path, "EconLit filtered"),
                        (apis_path, "APIs filtered"),
                        (overlap_path, "Overlap")]:
        if not os.path.exists(path):
            print(f"ERROR: {label} not found: {path}")
            sys.exit(1)

    print(f"Loading datasets for {policy_abbr}...")
    econlit_df = pd.read_parquet(econlit_path)
    apis_df = pd.read_parquet(apis_path)
    overlap_df = pd.read_parquet(overlap_path)
    print(f"  EconLit: {len(econlit_df):,} papers")
    print(f"  APIs: {len(apis_df):,} papers")
    print(f"  Overlap: {len(overlap_df):,} papers")

    # Step 1: Check abstract coverage for top-10 journal papers
    top_journal_stats = check_top_journal_abstracts(econlit_df, apis_df)

    # Step 2: Unify
    unified_df = unify_datasets(econlit_df, apis_df, overlap_df, policy_abbr)

    # Step 3: Top-10 journal check on unified data
    print("\n  --- Top-10 journals in unified dataset ---")
    unified_df['_top_journal'] = unified_df['source_name'].apply(match_top_journal)
    top_unified = unified_df[unified_df['_top_journal'].notna()]
    if len(top_unified) > 0:
        for j in sorted(top_unified['_top_journal'].unique()):
            j_papers = top_unified[top_unified['_top_journal'] == j]
            j_abs = j_papers['abstract'].notna() & (j_papers['abstract'].str.strip() != '')
            print(f"    {j}: {len(j_papers)} papers, {(~j_abs).sum()} missing abstracts")
    else:
        print("    No papers in top-10 journals.")
    unified_df.drop(columns=['_top_journal'], inplace=True, errors='ignore')

    # Step 4: Save
    out_parquet = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econ_unified.parquet")
    out_csv = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econ_unified.csv")
    out_meta = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econ_unified_metadata.json")

    unified_df.to_parquet(out_parquet, index=False)
    unified_df.to_csv(out_csv, index=False)
    print(f"\n  Saved: {out_parquet}")
    print(f"  Saved: {out_csv}")

    # Metadata
    has_abs = unified_df['abstract'].notna() & (unified_df['abstract'].str.strip() != '')
    metadata = {
        "policy_abbreviation": policy_abbr,
        "created": datetime.now().isoformat(),
        "total_papers": len(unified_df),
        "from_econlit": int(unified_df['in_econlit'].sum()),
        "from_apis": int((unified_df['in_openalex'] | unified_df['in_semantic_scholar']
                          | unified_df['in_nber']).sum()),
        "in_both": int((unified_df['in_econlit'] &
                        (unified_df['in_openalex'] | unified_df['in_semantic_scholar']
                         | unified_df['in_nber'])).sum()),
        "econlit_only": int(unified_df['in_econlit'].sum() -
                            (unified_df['in_econlit'] &
                             (unified_df['in_openalex'] | unified_df['in_semantic_scholar']
                              | unified_df['in_nber'])).sum()),
        "with_abstract": int(has_abs.sum()),
        "abstract_pct": round(has_abs.mean() * 100, 1),
        "paper_types": unified_df['paper_type'].value_counts().to_dict(),
        "top10_journal_check": top_journal_stats,
    }

    with open(out_meta, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    print(f"  Saved: {out_meta}")

    print(f"\nDone. Unified dataset: {len(unified_df):,} papers.")


if __name__ == "__main__":
    main()
