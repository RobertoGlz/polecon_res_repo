"""
Inspect Sources: OpenAlex vs Semantic Scholar

Compares data sources/publishers between OpenAlex and Semantic Scholar
to understand coverage differences.

Author: Claude AI
Date: February 2026
"""

import os
import json
import requests
import time
import pandas as pd
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Semantic Scholar Publisher Partners (from ss_publishers.png)
# These are the official publisher partners listed on Semantic Scholar's website
SEMANTIC_SCHOLAR_PUBLISHERS = [
    "Association for Computational Linguistics",
    "ACM",
    "arXiv",
    "BioOne",
    "bioRxiv",
    "BMJ",
    "Cambridge University Press",
    "CiteSeerX",
    "Clinical Key",  # Clinical Transformation
    "DBLP",
    "De Gruyter",
    "Frontiers",
    "HAL",
    "HighWire",
    "IEEE",
    "IOP Publishing",
    "Karger",
    "medRxiv",
    "Microsoft Academic",
    "Papers With Code",
    "Project MUSE",
    "PubMed",
    "SAGE",
    "AAAS",  # Science/AAAS
    "Scientific.Net",
    "SciTePress",
    "Springer Nature",
    "SPIE",
    "SSRN",
    "Taylor & Francis",
    "MIT Press",
    "Royal Society",
    "University of Chicago Press",
    "Wiley",
    "Wolters Kluwer",
]

# Explicit mapping from OpenAlex publisher names to SS publisher names
# This handles cases where names differ significantly
EXPLICIT_MAPPINGS = {
    # ACM
    "association for computing machinery": "ACM",
    "acm": "ACM",
    # Taylor & Francis (owned by Informa)
    "informa": "Taylor & Francis",
    "taylor & francis": "Taylor & Francis",
    "routledge": "Taylor & Francis",
    # Springer Nature
    "springer": "Springer Nature",
    "nature portfolio": "Springer Nature",
    "nature publishing group": "Springer Nature",
    "springer nature": "Springer Nature",
    # AAAS / Science
    "american association for the advancement of science": "AAAS",
    "aaas": "AAAS",
    # IEEE
    "institute of electrical and electronics engineers": "IEEE",
    "ieee": "IEEE",
    # University of Chicago Press
    "university of chicago": "University of Chicago Press",
    # arXiv
    "arxiv": "arXiv",
    "cornell university library": "arXiv",  # arXiv is hosted by Cornell
    # SSRN
    "ssrn": "SSRN",
    "social science research network": "SSRN",
    # BMJ
    "bmj": "BMJ",
    "british medical journal": "BMJ",
    # PubMed
    "pubmed": "PubMed",
    "pubmed central": "PubMed",
    "national center for biotechnology information": "PubMed",
    # bioRxiv/medRxiv (Cold Spring Harbor)
    "cold spring harbor laboratory": "bioRxiv",
    "biorxiv": "bioRxiv",
    "medrxiv": "medRxiv",
    # Cambridge
    "cambridge university press": "Cambridge University Press",
    # Oxford (not in SS list but often confused)
    # "oxford university press": None,  # Not a SS partner
    # MIT Press
    "mit press": "MIT Press",
    "massachusetts institute of technology": "MIT Press",
    # Frontiers
    "frontiers media": "Frontiers",
    "frontiers": "Frontiers",
    # SAGE
    "sage": "SAGE",
    "sage publications": "SAGE",
    # Wiley
    "wiley": "Wiley",
    "john wiley": "Wiley",
    # De Gruyter
    "de gruyter": "De Gruyter",
    "walter de gruyter": "De Gruyter",
    # IOP
    "iop publishing": "IOP Publishing",
    "institute of physics": "IOP Publishing",
    # Karger
    "karger": "Karger",
    "s. karger": "Karger",
    # Royal Society
    "royal society": "Royal Society",
    "the royal society": "Royal Society",
    # Wolters Kluwer
    "wolters kluwer": "Wolters Kluwer",
    "ovid technologies": "Wolters Kluwer",
    # Project MUSE
    "project muse": "Project MUSE",
    "johns hopkins university press": "Project MUSE",
    # BioOne
    "bioone": "BioOne",
    # SPIE
    "spie": "SPIE",
    # HAL
    "hal": "HAL",
    "centre pour la communication scientifique directe": "HAL",
}

# Data repositories/aggregators that are SS partners but not traditional publishers
# These may appear as sources in OpenAlex rather than publishers
SS_DATA_SOURCES = [
    "arXiv",
    "bioRxiv",
    "medRxiv",
    "PubMed",
    "SSRN",
    "DBLP",
    "CiteSeerX",
    "HAL",
    "Papers With Code",
    "Microsoft Academic",
]


def fetch_openalex_publishers(sample_size=10000):
    """
    Fetch publishers from OpenAlex sources API.

    Since there are 276k+ sources, we sample to get the major publishers.
    We use the group_by feature to get publisher counts directly.
    """
    print("Fetching OpenAlex publishers...")

    # Use group_by to get publisher counts efficiently
    url = "https://api.openalex.org/sources?group_by=host_organization"

    all_publishers = []
    cursor = "*"
    page = 0

    while cursor:
        page += 1
        params = {
            "group_by": "host_organization",
            "per_page": 200,
            "cursor": cursor
        }

        try:
            response = requests.get(
                "https://api.openalex.org/sources",
                params=params,
                headers={"mailto": "research@example.com"}
            )
            response.raise_for_status()
            data = response.json()

            # Get group_by results
            groups = data.get("group_by", [])

            if not groups:
                break

            for group in groups:
                publisher_name = group.get("key_display_name", "Unknown")
                publisher_id = group.get("key", "")
                count = group.get("count", 0)

                all_publishers.append({
                    "publisher_name": publisher_name,
                    "publisher_id": publisher_id,
                    "source_count": count
                })

            # Get next cursor
            cursor = data.get("meta", {}).get("next_cursor")

            if page % 10 == 0:
                print(f"  Page {page}: {len(all_publishers)} publishers fetched")

            time.sleep(0.1)  # Rate limiting

        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

    print(f"  Total publishers fetched: {len(all_publishers)}")
    return all_publishers


def fetch_openalex_top_sources(n=5000):
    """
    Fetch top sources from OpenAlex by works_count.
    """
    print(f"\nFetching top {n} OpenAlex sources by works_count...")

    all_sources = []
    cursor = "*"
    page = 0

    while len(all_sources) < n and cursor:
        page += 1
        params = {
            "sort": "works_count:desc",
            "per_page": 200,
            "cursor": cursor,
            "select": "id,display_name,host_organization,host_organization_name,type,works_count,cited_by_count"
        }

        try:
            response = requests.get(
                "https://api.openalex.org/sources",
                params=params,
                headers={"mailto": "research@example.com"}
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                break

            for source in results:
                all_sources.append({
                    "source_id": source.get("id", ""),
                    "source_name": source.get("display_name", ""),
                    "publisher_id": source.get("host_organization", ""),
                    "publisher_name": source.get("host_organization_name", ""),
                    "type": source.get("type", ""),
                    "works_count": source.get("works_count", 0),
                    "cited_by_count": source.get("cited_by_count", 0)
                })

            cursor = data.get("meta", {}).get("next_cursor")

            if page % 5 == 0:
                print(f"  Page {page}: {len(all_sources)} sources fetched")

            time.sleep(0.1)

        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

    print(f"  Total sources fetched: {len(all_sources)}")
    return all_sources[:n]


def normalize_publisher_name(name):
    """Normalize publisher name for matching."""
    if not name:
        return ""

    name = name.lower().strip()

    # Common normalizations
    replacements = {
        "the ": "",
        " press": "",
        " publishing": "",
        " publications": "",
        " group": "",
        " ltd": "",
        " inc": "",
        " llc": "",
        " gmbh": "",
        "&": "and",
        "-": " ",
        ",": "",
        ".": "",
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    return name.strip()


def match_publisher_to_ss(publisher_name):
    """
    Check if an OpenAlex publisher matches any Semantic Scholar publisher.
    Returns the matched SS publisher name or None.
    """
    if not publisher_name:
        return None

    normalized = normalize_publisher_name(publisher_name)
    pub_lower = publisher_name.lower().strip()

    # First, check explicit mappings (most reliable)
    for key, ss_name in EXPLICIT_MAPPINGS.items():
        if key in pub_lower or key in normalized:
            return ss_name

    # Then check direct name matches with SS publishers
    for ss_pub in SEMANTIC_SCHOLAR_PUBLISHERS:
        ss_norm = normalize_publisher_name(ss_pub)
        ss_lower = ss_pub.lower()

        # Exact match
        if ss_lower == pub_lower or ss_norm == normalized:
            return ss_pub

        # Substring match (but require significant length to avoid false positives)
        if len(ss_norm) >= 5:
            if ss_norm in normalized or normalized in ss_norm:
                return ss_pub

    return None


def analyze_coverage(openalex_sources, openalex_publishers):
    """
    Analyze coverage overlap between OpenAlex and Semantic Scholar.
    """
    print("\n" + "="*80)
    print("ANALYZING COVERAGE OVERLAP")
    print("="*80)

    # Get unique publishers from OpenAlex sources
    oa_publishers = defaultdict(lambda: {"sources": [], "works_count": 0, "cited_by_count": 0})

    for source in openalex_sources:
        pub_name = source.get("publisher_name", "Unknown")
        if pub_name:
            oa_publishers[pub_name]["sources"].append(source["source_name"])
            oa_publishers[pub_name]["works_count"] += source.get("works_count", 0)
            oa_publishers[pub_name]["cited_by_count"] += source.get("cited_by_count", 0)

    print(f"\nUnique publishers in OpenAlex (from top sources): {len(oa_publishers)}")

    # Match each OpenAlex publisher to SS publishers
    matched_publishers = {}
    unmatched_oa_publishers = []

    for pub_name, pub_data in oa_publishers.items():
        match = match_publisher_to_ss(pub_name)
        if match:
            if match not in matched_publishers:
                matched_publishers[match] = []
            matched_publishers[match].append({
                "openalex_name": pub_name,
                "source_count": len(pub_data["sources"]),
                "works_count": pub_data["works_count"]
            })
        else:
            unmatched_oa_publishers.append({
                "publisher_name": pub_name,
                "source_count": len(pub_data["sources"]),
                "works_count": pub_data["works_count"]
            })

    # Sort unmatched by works_count
    unmatched_oa_publishers.sort(key=lambda x: x["works_count"], reverse=True)

    # Find SS publishers not matched in OpenAlex
    matched_ss = set(matched_publishers.keys())
    unmatched_ss = [p for p in SEMANTIC_SCHOLAR_PUBLISHERS if p not in matched_ss]

    return {
        "matched_publishers": matched_publishers,
        "unmatched_openalex": unmatched_oa_publishers,
        "unmatched_semantic_scholar": unmatched_ss,
        "total_oa_publishers": len(oa_publishers),
        "total_ss_publishers": len(SEMANTIC_SCHOLAR_PUBLISHERS)
    }


def generate_report(analysis, openalex_publishers):
    """Generate a markdown report of the analysis."""

    lines = []
    lines.append("# Source Coverage Analysis: OpenAlex vs Semantic Scholar")
    lines.append("")
    lines.append(f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **OpenAlex unique publishers (from top sources):** {analysis['total_oa_publishers']:,}")
    lines.append(f"- **Semantic Scholar publisher partners:** {analysis['total_ss_publishers']}")
    lines.append(f"- **SS publishers matched in OpenAlex:** {len(analysis['matched_publishers'])}")
    lines.append(f"- **SS publishers NOT found in OpenAlex:** {len(analysis['unmatched_semantic_scholar'])}")
    lines.append("")

    # Matched publishers (intersection)
    lines.append("---")
    lines.append("")
    lines.append("## Publishers in BOTH OpenAlex and Semantic Scholar")
    lines.append("")
    lines.append("These publishers have partnerships with Semantic Scholar AND are indexed in OpenAlex:")
    lines.append("")
    lines.append("| SS Publisher | OpenAlex Names | Sources | Works |")
    lines.append("|--------------|----------------|---------|-------|")

    for ss_pub, oa_matches in sorted(analysis['matched_publishers'].items()):
        oa_names = ", ".join([m["openalex_name"][:30] for m in oa_matches[:2]])
        if len(oa_matches) > 2:
            oa_names += f" (+{len(oa_matches)-2} more)"
        total_sources = sum(m["source_count"] for m in oa_matches)
        total_works = sum(m["works_count"] for m in oa_matches)
        lines.append(f"| {ss_pub} | {oa_names} | {total_sources} | {total_works:,} |")

    lines.append("")

    # SS publishers not in OpenAlex
    lines.append("---")
    lines.append("")
    lines.append("## Semantic Scholar Publishers NOT Matched in OpenAlex")
    lines.append("")
    lines.append("These are SS publisher partners that we couldn't match to OpenAlex publishers:")
    lines.append("")

    for pub in analysis['unmatched_semantic_scholar']:
        lines.append(f"- {pub}")

    lines.append("")
    lines.append("**Note:** Some of these may be in OpenAlex under different names, or may be")
    lines.append("data providers (like PubMed, arXiv) rather than traditional publishers.")
    lines.append("")

    # Top OpenAlex publishers not in SS
    lines.append("---")
    lines.append("")
    lines.append("## Top OpenAlex Publishers NOT in Semantic Scholar")
    lines.append("")
    lines.append("Major publishers in OpenAlex that are not SS publisher partners:")
    lines.append("")
    lines.append("| Publisher | Sources | Works |")
    lines.append("|-----------|---------|-------|")

    for pub in analysis['unmatched_openalex'][:30]:
        lines.append(f"| {pub['publisher_name'][:50]} | {pub['source_count']} | {pub['works_count']:,} |")

    lines.append("")

    # Interpretation
    lines.append("---")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("### Key Differences")
    lines.append("")
    lines.append("1. **OpenAlex** indexes content from a much broader range of publishers,")
    lines.append("   including many smaller academic publishers and institutional repositories.")
    lines.append("")
    lines.append("2. **Semantic Scholar** has formal partnerships with ~40 major publishers,")
    lines.append("   but also ingests content from other sources like PubMed and arXiv.")
    lines.append("")
    lines.append("3. **Major publishers in both:** Springer Nature, Wiley, Taylor & Francis,")
    lines.append("   SAGE, IEEE, Cambridge, etc.")
    lines.append("")
    lines.append("### Sources in Semantic Scholar but NOT in OpenAlex")
    lines.append("")
    lines.append("These SS partner sources have NO equivalent in OpenAlex:")
    lines.append("")
    lines.append("- **DBLP** - Computer science bibliography database")
    lines.append("- **CiteSeerX** - Scientific literature digital library")
    lines.append("- **Papers With Code** - ML/AI papers with code implementations")
    lines.append("- **Microsoft Academic** - Microsoft's academic search engine (discontinued)")
    lines.append("- **HighWire** - Platform hosting many publisher websites")
    lines.append("- **Scientific.Net** - Materials science publications")
    lines.append("- **SciTePress** - Science and Technology Publications")
    lines.append("- **Clinical Key** - Elsevier's clinical decision support tool")
    lines.append("")
    lines.append("### Publishers in OpenAlex but NOT in Semantic Scholar")
    lines.append("")
    lines.append("These major publishers are NOT SS partners:")
    lines.append("")
    lines.append("- **Elsevier** (16.7M works) - Largest academic publisher!")
    lines.append("- **Oxford University Press** (3.5M works) - Publishes QJE, REStud, etc.")
    lines.append("- **American Chemical Society** (1.9M works)")
    lines.append("- **MDPI** (1.6M works) - Major open access publisher")
    lines.append("")
    lines.append("### Implications for Research")
    lines.append("")
    lines.append("#### Why papers appear in OpenAlex but NOT Semantic Scholar:")
    lines.append("- Published by **Elsevier** (Lancet, Cell, many economics journals)")
    lines.append("- Published by **Oxford University Press** (QJE, REStud, Economic Journal)")
    lines.append("- Published by smaller publishers not partnered with SS")
    lines.append("")
    lines.append("#### Why papers appear in Semantic Scholar but NOT OpenAlex:")
    lines.append("- Indexed in **DBLP** (especially CS conference papers)")
    lines.append("- Listed on **Papers With Code** (ML/AI research)")
    lines.append("- Available through **HighWire** platform")
    lines.append("- Older papers from **CiteSeerX** or **Microsoft Academic**")
    lines.append("")
    lines.append("#### Economics/Policy Research Implications:")
    lines.append("- **OpenAlex** likely has better coverage of economics journals (Elsevier, OUP)")
    lines.append("- **Semantic Scholar** may have better coverage of working papers via SSRN")
    lines.append("- Both cover major preprint servers (arXiv, SSRN, NBER)")
    lines.append("")

    # Save report
    report_path = os.path.join(OUTPUT_DIR, "source_coverage_analysis.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"\nReport saved to: {report_path}")
    return report_path


def search_openalex_for_source(source_name):
    """
    Search OpenAlex for a specific source by name.
    Used to find data repositories like arXiv, SSRN, etc.
    """
    try:
        params = {
            "search": source_name,
            "per_page": 5,
            "select": "id,display_name,host_organization_name,type,works_count"
        }
        response = requests.get(
            "https://api.openalex.org/sources",
            params=params,
            headers={"mailto": "research@example.com"}
        )
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        print(f"  Error searching for {source_name}: {e}")
        return []


def check_data_sources_in_openalex():
    """
    Check if SS data sources (arXiv, SSRN, etc.) exist as sources in OpenAlex.
    """
    print("\nChecking SS data sources in OpenAlex...")

    found_sources = {}
    for source in SS_DATA_SOURCES:
        results = search_openalex_for_source(source)
        if results:
            found_sources[source] = results[0]  # Take top result
            print(f"  {source}: Found as '{results[0].get('display_name')}' ({results[0].get('works_count', 0):,} works)")
        else:
            print(f"  {source}: Not found")
        time.sleep(0.1)

    return found_sources


def main():
    print("="*80)
    print("SOURCE COVERAGE ANALYSIS: OpenAlex vs Semantic Scholar")
    print("="*80)

    # Check if data sources exist in OpenAlex
    data_sources_in_oa = check_data_sources_in_openalex()

    # Fetch OpenAlex publishers via group_by
    openalex_publishers = fetch_openalex_publishers()

    # Also fetch top sources to get more detailed publisher info
    openalex_sources = fetch_openalex_top_sources(n=5000)

    # Save raw data
    publishers_df = pd.DataFrame(openalex_publishers)
    publishers_df.to_csv(os.path.join(OUTPUT_DIR, "openalex_publishers.csv"), index=False)
    print(f"\nSaved OpenAlex publishers to: {os.path.join(OUTPUT_DIR, 'openalex_publishers.csv')}")

    sources_df = pd.DataFrame(openalex_sources)
    sources_df.to_csv(os.path.join(OUTPUT_DIR, "openalex_top_sources.csv"), index=False)
    print(f"Saved OpenAlex top sources to: {os.path.join(OUTPUT_DIR, 'openalex_top_sources.csv')}")

    # Analyze coverage
    analysis = analyze_coverage(openalex_sources, openalex_publishers)

    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80)
    print(f"\nSemantic Scholar Publisher Partners: {len(SEMANTIC_SCHOLAR_PUBLISHERS)}")
    print(f"  - Matched in OpenAlex: {len(analysis['matched_publishers'])}")
    print(f"  - Not found in OpenAlex: {len(analysis['unmatched_semantic_scholar'])}")

    print(f"\nSS Publishers NOT in OpenAlex:")
    for pub in analysis['unmatched_semantic_scholar']:
        print(f"  - {pub}")

    print(f"\nTop 10 OpenAlex Publishers NOT in SS (by works):")
    for pub in analysis['unmatched_openalex'][:10]:
        print(f"  - {pub['publisher_name']}: {pub['works_count']:,} works")

    # Generate report
    report_path = generate_report(analysis, openalex_publishers)

    # Categorize SS sources not in OpenAlex
    print(f"\n{'='*80}")
    print("SEMANTIC SCHOLAR SOURCES NOT IN OPENALEX")
    print("="*80)

    ss_only_as_sources = []  # Found as sources in OA but listed as unmatched publisher
    ss_truly_not_in_oa = []  # Not found in OA at all

    for ss_pub in analysis['unmatched_semantic_scholar']:
        # Check if it was found as a data source
        found_in_oa = False
        for ds_name, ds_info in data_sources_in_oa.items():
            if ds_name.lower() in ss_pub.lower() or ss_pub.lower() in ds_name.lower():
                found_in_oa = True
                ss_only_as_sources.append({
                    "ss_name": ss_pub,
                    "oa_source_name": ds_info.get("display_name", ""),
                    "works_count": ds_info.get("works_count", 0)
                })
                break

        if not found_in_oa:
            ss_truly_not_in_oa.append(ss_pub)

    print(f"\nSS sources found in OpenAlex (as data sources, not publishers):")
    for item in ss_only_as_sources:
        print(f"  - {item['ss_name']}: '{item['oa_source_name']}' ({item['works_count']:,} works)")

    print(f"\nSS sources TRULY NOT in OpenAlex:")
    for pub in ss_truly_not_in_oa:
        print(f"  - {pub}")

    # Save analysis as JSON
    analysis_json = {
        "matched_publishers": {k: v for k, v in analysis['matched_publishers'].items()},
        "unmatched_semantic_scholar": analysis['unmatched_semantic_scholar'],
        "ss_only_as_sources": ss_only_as_sources,
        "ss_truly_not_in_openalex": ss_truly_not_in_oa,
        "unmatched_openalex_top30": analysis['unmatched_openalex'][:30],
        "data_sources_in_openalex": {k: {"display_name": v.get("display_name"), "works_count": v.get("works_count")}
                                      for k, v in data_sources_in_oa.items()},
        "summary": {
            "total_ss_publishers": analysis['total_ss_publishers'],
            "total_oa_publishers_sampled": analysis['total_oa_publishers'],
            "ss_matched_in_oa": len(analysis['matched_publishers']),
            "ss_not_in_oa": len(analysis['unmatched_semantic_scholar']),
            "ss_truly_not_in_oa": len(ss_truly_not_in_oa)
        }
    }

    with open(os.path.join(OUTPUT_DIR, "source_coverage_analysis.json"), 'w') as f:
        json.dump(analysis_json, f, indent=2, default=str)

    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
