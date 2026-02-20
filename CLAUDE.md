# CLAUDE.md — Political Economy of Research

## Project Overview

Research project studying how major U.S. policies generate academic research. We systematically collect papers evaluating three policies (TCJA, ACA, NCLB), recover missing metadata, build unified datasets, and classify papers by whether they empirically evaluate policy effects on economic outcomes.

## Pipeline Architecture

The build pipeline runs in stages. Each stage lives in `code/build/<stage>/` with a `*_main.py` entry point, `output/` for results, and `tmp/` for intermediary files.

```
get_policies → scrape (OpenAlex | Semantic Scholar | NBER)
            → inspect_sources
            → complement_abstracts → unified_dataset → classify_papers
```

**Stage details:**

| Stage | Script | What it does |
|-------|--------|-------------|
| `get_policies` | `get_policies_main.py` | Defines policies and search terms in `policies.csv` |
| `scrape_policies_openalex` | `scrape_openalex_main.py` | Primary source. Cursor pagination, no result cap |
| `scrape_policies_semantic_scholar` | `scrape_semantic_scholar_main.py` | Bulk API. Needs `SEMANTIC_SCHOLAR_API_KEY` in `.env` |
| `scrape_policies_nber` | `scrape_nber_main.py` | NBER website API. Truncated abstracts |
| `inspect_sources` | `inspect_sources_openalex_ss.py` | Analyzes source coverage gaps |
| `complement_abstracts` | `complement_abstracts_main.py` | 8-step fallback: CrossRef → OA URLs → PDF → Selenium → SSRN → NBER → S2 → Europe PMC |
| `unified_dataset` | `unified_dataset_main.py` | Matches/merges across sources. Modules in `modules/` |
| `classify_papers` | `classify_papers_main.py` | Gemini API classification (pilot mode) |

All scrapers support `--resume` and `python script.py [POLICY ...]` for selective runs.

## File Conventions

- **Data formats**: Parquet (primary) + CSV (compatibility). Reports in Markdown.
- **Naming**: `{POLICY}_{stage}_{qualifier}.parquet` (e.g., `ACA_papers_openalex_raw.parquet`)
- **Raw vs filtered**: `_raw` = no relevance filter; `_complemented_filtered` = after abstract recovery + relevance filter
- **Policies file**: `code/build/get_policies/output/policies.csv` — single source of truth for policy names, abbreviations, years, categories, and search terms (pipe-separated)

## Git & GitHub Workflow

- **Commit messages**: `#<issue> <Description>` (e.g., `#7 Extend abstract recovery pipeline`)
- **Branches**: Feature branches named `i#<issue>_<short_description>`, merged via PR
- **Issues**: Each pipeline stage and enhancement is tracked as a GitHub issue
- **Do not commit**: `.env`, data files (CSV/JSON/Parquet), `output/`, `tmp/` — all in `.gitignore`
- **Do commit**: Python scripts, Markdown reports in `code/build/*/reports/`, `requirements.txt`
- When commenting on issues, include tables with quantitative results and flag next steps

## Code Style

- Python 3.11. Dependencies in `requirements.txt`.
- Comprehensive module-level docstrings with pipeline overview, key notes, and update history
- Function docstrings with Parameters/Returns sections
- `snake_case` functions and variables, `UPPER_CASE` constants
- Each scraper is self-contained (no shared utility module across scrapers)
- Parallelization uses `ThreadPoolExecutor` with per-API `RateLimiter` classes
- Prefer `argparse` for CLI interfaces

## API Keys & Secrets

All keys in `.env` (see `.env.example` for template):
- `SEMANTIC_SCHOLAR_API_KEY` — required for Semantic Scholar bulk API
- `GEMINI_API_KEY1` through `GEMINI_API_KEY10` — rotated for rate-limit management
- OpenAlex, CrossRef, NBER, Europe PMC — no keys needed (polite pool via `mailto`)

## Key Design Decisions

- **No relevance filtering at scrape stage**: Raw output preserves everything. Filtering happens after abstract recovery so we can use abstract content for better relevance judgments.
- **Case-sensitive acronym validation**: Short all-caps search terms (ACA, TCJA, NCLB) undergo post-scrape validation — papers matched only by the acronym must contain it case-sensitively as a whole word in title/abstract. Prevents false positives (e.g., "academic" matching "aca").
- **Dedup keys**: OpenAlex uses `openalex_id`, Semantic Scholar uses `semantic_scholar_id`, NBER uses `normalized_title`.
- **Search term tracking**: `search_terms_matched` column stores all terms that found each paper (pipe-separated), enabling downstream analysis of which terms contribute most.

## Common Tasks

```bash
# Run a single scraper for one policy
python code/build/scrape_policies_openalex/scrape_openalex_main.py ACA

# Resume a scraper run (skip policies completed today)
python code/build/unified_dataset/unified_dataset_main.py --resume

# Run abstract recovery for specific policies
python code/build/complement_abstracts/complement_abstracts_main.py TCJA ACA

# Run classification pilot
python code/build/classify_papers/classify_papers_main.py --sample_size 50

# Check data quality
python -c "import pandas as pd; df = pd.read_parquet('code/build/unified_dataset/output/ACA_unified_dataset.parquet'); print(df.info())"
```

## Downstream Analysis

Stata integration exists via `code/config.do` and `code/make_file_polecon_res.do`. The Stata pipeline reads from the Python pipeline's output. The three-folder structure (`polecon_res_repo/`, `polecon_res_src/`, `polecon_res_work/`) follows Stata project conventions.

## Current State & Open Issues

Track progress via `gh issue list`. Key open issues:
- **#20** — Unified dataset: stale NBER/Semantic Scholar data (loads old Jan 27 files, not Feb 6 raw)
- **#7** — Abstract recovery: TCJA coverage critically low (9.2%)
- **#9** — Cross-validate with NBER: near-zero source overlap
- **#1** — OpenAlex scraper: concept-based retrieval adds noise
- **#3** — Get policies: stable, 3 policies defined
- **#21** — Source coverage analysis: Elsevier/OUP gap explains OpenAlex-unique papers
