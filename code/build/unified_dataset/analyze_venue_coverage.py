"""
Analyze venue coverage differences between OpenAlex and Semantic Scholar.

Identifies venues that are indexed by one source but not the other,
ranked by academic relevance (total citations).
"""

import pandas as pd
import os
import re

# Load all unified datasets
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")

policies = ['TCJA', 'ACA', 'NCLB']

all_dfs = []
for policy in policies:
    path = os.path.join(OUTPUT_DIR, f'{policy}_unified_dataset.parquet')
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df['policy'] = policy
        all_dfs.append(df)
        print(f'{policy}: {len(df)} papers')

combined = pd.concat(all_dfs, ignore_index=True)
print(f'\nTotal combined: {len(combined)} papers')

# Filter to papers with venues
combined = combined[combined['venue'].notna() & (combined['venue'].astype(str) != '')]
print(f'Papers with venue info: {len(combined)}')


def normalize_venue(v):
    """Normalize venue name for comparison."""
    if pd.isna(v):
        return ''
    v = str(v).lower().strip()
    # Remove common suffixes/variations
    v = re.sub(r'\s*\(.*?\)', '', v)  # Remove parentheticals
    v = re.sub(r'[^a-z0-9\s]', '', v)  # Keep only alphanumeric
    v = re.sub(r'\s+', ' ', v).strip()
    return v


combined['venue_normalized'] = combined['venue'].apply(normalize_venue)

# Get venues that appear in OpenAlex papers
oa_papers = combined[combined['in_openalex'] == 1]
oa_venues = set(oa_papers['venue_normalized'].unique())

# Get venues that appear in Semantic Scholar papers
ss_papers = combined[combined['in_semantic_scholar'] == 1]
ss_venues = set(ss_papers['venue_normalized'].unique())

print(f'\nUnique venues in OpenAlex: {len(oa_venues)}')
print(f'Unique venues in Semantic Scholar: {len(ss_venues)}')
print(f'Venues in both: {len(oa_venues & ss_venues)}')
print(f'Venues ONLY in OpenAlex: {len(oa_venues - ss_venues)}')
print(f'Venues ONLY in Semantic Scholar: {len(ss_venues - oa_venues)}')

# Get venues ONLY in OpenAlex (never appear in any SS paper)
oa_exclusive_venues = oa_venues - ss_venues

# Get venues ONLY in Semantic Scholar (never appear in any OA paper)
ss_exclusive_venues = ss_venues - oa_venues

# For OpenAlex-exclusive venues, get paper counts and total citations
oa_exclusive_df = oa_papers[oa_papers['venue_normalized'].isin(oa_exclusive_venues)]
oa_venue_stats = oa_exclusive_df.groupby('venue').agg({
    'unified_id': 'count',
    'cited_by_count': 'sum'
}).rename(columns={'unified_id': 'paper_count', 'cited_by_count': 'total_citations'})
oa_venue_stats['total_citations'] = oa_venue_stats['total_citations'].fillna(0)
oa_venue_stats['avg_citations'] = (oa_venue_stats['total_citations'] / oa_venue_stats['paper_count']).round(1)
oa_venue_stats = oa_venue_stats.sort_values('total_citations', ascending=False)

print('\n' + '='*80)
print('TOP 15 VENUES INDEXED BY OPENALEX BUT NOT SEMANTIC SCHOLAR')
print('(Ranked by total citations - proxy for academic relevance)')
print('='*80)
for i, (venue, row) in enumerate(oa_venue_stats.head(15).iterrows(), 1):
    print(f'{i:2}. {venue}')
    print(f'    Papers: {int(row["paper_count"]):,} | Total Citations: {int(row["total_citations"]):,} | Avg: {row["avg_citations"]}')

# For Semantic Scholar-exclusive venues
ss_exclusive_df = ss_papers[ss_papers['venue_normalized'].isin(ss_exclusive_venues)]
ss_venue_stats = ss_exclusive_df.groupby('venue').agg({
    'unified_id': 'count',
    'cited_by_count': 'sum'
}).rename(columns={'unified_id': 'paper_count', 'cited_by_count': 'total_citations'})
ss_venue_stats['total_citations'] = ss_venue_stats['total_citations'].fillna(0)
ss_venue_stats['avg_citations'] = (ss_venue_stats['total_citations'] / ss_venue_stats['paper_count']).round(1)
ss_venue_stats = ss_venue_stats.sort_values('total_citations', ascending=False)

print('\n' + '='*80)
print('TOP 15 VENUES INDEXED BY SEMANTIC SCHOLAR BUT NOT OPENALEX')
print('(Ranked by total citations - proxy for academic relevance)')
print('='*80)
for i, (venue, row) in enumerate(ss_venue_stats.head(15).iterrows(), 1):
    print(f'{i:2}. {venue}')
    print(f'    Papers: {int(row["paper_count"]):,} | Total Citations: {int(row["total_citations"]):,} | Avg: {row["avg_citations"]}')


# Also show by average citations (high-impact venues with fewer papers)
print('\n' + '='*80)
print('TOP 10 OPENALEX-ONLY VENUES BY AVERAGE CITATIONS (min 3 papers)')
print('='*80)
oa_by_avg = oa_venue_stats[oa_venue_stats['paper_count'] >= 3].sort_values('avg_citations', ascending=False)
for i, (venue, row) in enumerate(oa_by_avg.head(10).iterrows(), 1):
    print(f'{i:2}. {venue}')
    print(f'    Papers: {int(row["paper_count"]):,} | Avg Citations: {row["avg_citations"]}')

print('\n' + '='*80)
print('TOP 10 SEMANTIC SCHOLAR-ONLY VENUES BY AVERAGE CITATIONS (min 3 papers)')
print('='*80)
ss_by_avg = ss_venue_stats[ss_venue_stats['paper_count'] >= 3].sort_values('avg_citations', ascending=False)
for i, (venue, row) in enumerate(ss_by_avg.head(10).iterrows(), 1):
    print(f'{i:2}. {venue}')
    print(f'    Papers: {int(row["paper_count"]):,} | Avg Citations: {row["avg_citations"]}')


# Generate a markdown report
report_lines = []
report_lines.append("# Venue Coverage Analysis: OpenAlex vs Semantic Scholar")
report_lines.append("")
report_lines.append(f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
report_lines.append("")
report_lines.append("This report identifies venues that appear exclusively in one source.")
report_lines.append("")
report_lines.append("---")
report_lines.append("")

report_lines.append("## Summary Statistics")
report_lines.append("")
report_lines.append(f"- **Total papers analyzed:** {len(combined):,}")
report_lines.append(f"- **Unique venues in OpenAlex:** {len(oa_venues):,}")
report_lines.append(f"- **Unique venues in Semantic Scholar:** {len(ss_venues):,}")
report_lines.append(f"- **Venues in both sources:** {len(oa_venues & ss_venues):,}")
report_lines.append(f"- **Venues ONLY in OpenAlex:** {len(oa_exclusive_venues):,}")
report_lines.append(f"- **Venues ONLY in Semantic Scholar:** {len(ss_exclusive_venues):,}")
report_lines.append("")

report_lines.append("---")
report_lines.append("")
report_lines.append("## Top Venues Indexed by OpenAlex but NOT Semantic Scholar")
report_lines.append("")
report_lines.append("Ranked by total citations (proxy for academic relevance):")
report_lines.append("")
report_lines.append("| Rank | Venue | Papers | Total Citations | Avg Citations |")
report_lines.append("|------|-------|--------|-----------------|---------------|")

for i, (venue, row) in enumerate(oa_venue_stats.head(15).iterrows(), 1):
    report_lines.append(f"| {i} | {venue} | {int(row['paper_count']):,} | {int(row['total_citations']):,} | {row['avg_citations']} |")

report_lines.append("")
report_lines.append("---")
report_lines.append("")
report_lines.append("## Top Venues Indexed by Semantic Scholar but NOT OpenAlex")
report_lines.append("")
report_lines.append("Ranked by total citations (proxy for academic relevance):")
report_lines.append("")
report_lines.append("| Rank | Venue | Papers | Total Citations | Avg Citations |")
report_lines.append("|------|-------|--------|-----------------|---------------|")

for i, (venue, row) in enumerate(ss_venue_stats.head(15).iterrows(), 1):
    report_lines.append(f"| {i} | {venue} | {int(row['paper_count']):,} | {int(row['total_citations']):,} | {row['avg_citations']} |")

report_lines.append("")
report_lines.append("---")
report_lines.append("")
report_lines.append("## Interpretation")
report_lines.append("")
report_lines.append("### Why Some Venues Appear Only in OpenAlex")
report_lines.append("")
report_lines.append("1. **PsycEXTRA Dataset**: Psychology grey literature indexed by APA, which OpenAlex includes")
report_lines.append("2. **Choice Reviews Online**: Library review publication not in Semantic Scholar's scope")
report_lines.append("3. **Forefront Group**: Think tank publications indexed by OpenAlex")
report_lines.append("4. **Various specialized journals**: OpenAlex has broader coverage of smaller venues")
report_lines.append("")
report_lines.append("### Why Some Venues Appear Only in Semantic Scholar")
report_lines.append("")
report_lines.append("1. **Different venue naming conventions**: Same journal may have different names")
report_lines.append("2. **Preprint/working paper repositories**: Different coverage of preprints")
report_lines.append("3. **Conference proceedings**: Semantic Scholar emphasizes CS/AI conferences")
report_lines.append("")
report_lines.append("### Important Caveat")
report_lines.append("")
report_lines.append("A venue appearing 'only' in one source doesn't necessarily mean the other source")
report_lines.append("doesn't index it at all. It may mean:")
report_lines.append("")
report_lines.append("- The specific papers from that venue weren't returned by our search queries")
report_lines.append("- The venue name is stored differently in each database")
report_lines.append("- Coverage varies by publication year or paper type")
report_lines.append("")

# Save report
report_path = os.path.join(REPORTS_DIR, "venue_coverage_analysis.md")
os.makedirs(REPORTS_DIR, exist_ok=True)
with open(report_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(report_lines))

print(f'\n\nReport saved to: {report_path}')
