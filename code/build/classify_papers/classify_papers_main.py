"""
classify_papers_main.py
Issue #22: Flag papers that evaluate the effect of a policy on an economic outcome.

Calls the Google Gemini API to classify each paper as TRUE/FALSE.
Pilot mode: runs a small sample in real-time for manual inspection.

Supports multiple prompt versions:
  v1_with_examples: includes economic outcome examples (employment, wages, ...)
  v2_no_examples:   same prompt but without the examples list

Usage:
    python classify_papers_main.py                          # pilot (10 papers/policy, old data)
    python classify_papers_main.py --sample_size 50         # larger pilot
    python classify_papers_main.py --policies TCJA ACA      # subset of policies

    # Econ-unified data with prompt variants:
    python classify_papers_main.py --policies TCJA --sample_size 15 \\
        --prompt_version v1_with_examples --run_label v1_run1 \\
        --input_dir code/build/scrape_econ/output

    # Full-dataset classification (all papers, with checkpoint/resume):
    python classify_papers_main.py --policies TCJA --all \\
        --prompt_version v2_no_examples --run_label v2_full \\
        --input_dir code/build/scrape_econ/output

    # Merge results from multiple runs:
    python classify_papers_main.py --merge
"""

import os
import sys
import time
import json
import itertools
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]
UNIFIED_DIR = REPO_ROOT / "code" / "build" / "unified_dataset" / "output"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Load API keys
# ---------------------------------------------------------------------------
load_dotenv(REPO_ROOT / ".env")

def _load_gemini_keys():
    """Load all GEMINI_API_KEY* from .env and return as a list."""
    keys = []
    for i in range(1, 20):  # support up to 20 keys
        k = os.getenv(f"GEMINI_API_KEY{i}")
        if k:
            keys.append(k)
    if not keys:
        k = os.getenv("GEMINI_API_KEY")
        if k:
            keys.append(k)
    if not keys:
        print("ERROR: No GEMINI_API_KEY* found in .env")
        sys.exit(1)
    return keys

GEMINI_KEYS = _load_gemini_keys()
print(f"Loaded {len(GEMINI_KEYS)} Gemini API key(s)")

_key_cycle = itertools.cycle(GEMINI_KEYS)

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
MODEL = "gemini-3-flash-preview"

# ---------------------------------------------------------------------------
# Prompt templates  (see CONVERSATION_LOG.md for iteration history)
# ---------------------------------------------------------------------------
SYSTEM_PROMPTS = {
    "v1_with_examples": (
        "You are a research paper classifier. Given a paper's title and abstract, "
        "determine whether the paper empirically evaluates the effect of "
        "{policy_name} ({policy_abbreviation}) on an economic outcome.\n\n"
        "Return TRUE if the paper uses data or empirical methods to estimate the "
        "effect of {policy_name} on a measurable economic outcome (e.g., employment, "
        "wages, income, investment, prices, tax revenue, insurance coverage, "
        "healthcare costs, educational attainment, housing values, firm performance, "
        "or other economic variables).\n\n"
        "Return FALSE if:\n"
        "- The paper is not actually about {policy_name} (e.g., acronym collision)\n"
        "- The paper discusses the policy without empirically evaluating its effects\n"
        "- The paper is purely theoretical, legal analysis, or political commentary\n"
        "- The paper does not measure an economic outcome\n\n"
        "Respond with exactly one word: TRUE or FALSE"
    ),
    "v2_no_examples": (
        "You are a research paper classifier. Given a paper's title and abstract, "
        "determine whether the paper empirically evaluates the effect of "
        "{policy_name} ({policy_abbreviation}) on an economic outcome.\n\n"
        "Return TRUE if the paper uses data or empirical methods to estimate the "
        "effect of {policy_name} on a measurable economic outcome.\n\n"
        "Return FALSE if:\n"
        "- The paper is not actually about {policy_name} (e.g., acronym collision)\n"
        "- The paper discusses the policy without empirically evaluating its effects\n"
        "- The paper is purely theoretical, legal analysis, or political commentary\n"
        "- The paper does not measure an economic outcome\n\n"
        "Respond with exactly one word: TRUE or FALSE"
    ),
}

# Backward compatibility alias
SYSTEM_PROMPT_TEMPLATE = SYSTEM_PROMPTS["v1_with_examples"]

USER_PROMPT_TEMPLATE = "Title: {title}\nAbstract: {abstract}"

# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------
def classify_paper(title: str, abstract: str, policy_name: str,
                   policy_abbreviation: str,
                   prompt_version: str = "v1_with_examples") -> dict:
    """
    Call Gemini to classify a single paper.
    Returns dict with 'classification', 'raw_response', 'error'.
    """
    api_key = next(_key_cycle)
    client = genai.Client(api_key=api_key)

    template = SYSTEM_PROMPTS.get(prompt_version, SYSTEM_PROMPTS["v1_with_examples"])
    system_prompt = template.format(
        policy_name=policy_name,
        policy_abbreviation=policy_abbreviation,
    )

    abstract_text = abstract if pd.notna(abstract) and abstract.strip() else "[No abstract available]"
    user_prompt = USER_PROMPT_TEMPLATE.format(title=title, abstract=abstract_text)

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.0,
                max_output_tokens=5,
                thinking_config=types.ThinkingConfig(thinking_level="minimal"),
            ),
        )
        raw = response.text.strip().upper()
        if "TRUE" in raw:
            classification = True
        elif "FALSE" in raw:
            classification = False
        else:
            classification = None
        return {"classification": classification, "raw_response": raw, "error": None}

    except Exception as e:
        return {"classification": None, "raw_response": None, "error": str(e)}


def run_pilot(policies: list[str], sample_size: int, seed: int = 1602,
              prompt_version: str = "v1_with_examples",
              run_label: str = "",
              input_dir: str = None):
    """
    Run pilot classification on a sample of papers for each policy.
    Only papers WITH abstracts are sampled.

    Args:
        policies: list of policy abbreviation strings
        sample_size: number of papers to sample per policy
        seed: random seed for reproducible sampling
        prompt_version: key into SYSTEM_PROMPTS dict
        run_label: label for output file (e.g. 'v1_run1'). If empty, uses
                   default 'pilot_classifications.csv' name.
        input_dir: directory containing parquet files. If set, reads
                   {POLICY}_econ_unified.parquet. If None, reads from
                   unified_dataset/output/{POLICY}_unified_dataset.parquet.
    """
    all_results = []

    # Determine data source
    using_econ_unified = input_dir is not None

    for policy_abbr in policies:
        if using_econ_unified:
            parquet_path = Path(input_dir) / f"{policy_abbr}_econ_unified.parquet"
        else:
            parquet_path = UNIFIED_DIR / f"{policy_abbr}_unified_dataset.parquet"

        if not parquet_path.exists():
            print(f"WARNING: {parquet_path} not found, skipping {policy_abbr}")
            continue

        df = pd.read_parquet(parquet_path)
        policy_name = df["policy_studied"].iloc[0]

        # Determine ID column
        id_col = "econ_unified_id" if using_econ_unified and "econ_unified_id" in df.columns else "unified_id"

        has_abstract = df["abstract"].notna() & (df["abstract"].str.strip() != "")
        df_with_abs = df[has_abstract]
        print(f"\n{'='*60}")
        print(f"Policy: {policy_abbr} ({policy_name})")
        print(f"Prompt: {prompt_version}")
        print(f"Total papers: {len(df):,} | With abstracts: {len(df_with_abs):,}")

        n = min(sample_size, len(df_with_abs))
        sample = df_with_abs.sample(n=n, random_state=seed)
        print(f"Pilot sample: {n} papers")
        print(f"{'='*60}")

        for i, (idx, row) in enumerate(sample.iterrows()):
            print(f"  [{i+1}/{n}] {row['title'][:80]}...", end=" ", flush=True)

            result = classify_paper(
                title=row["title"],
                abstract=row["abstract"],
                policy_name=policy_name,
                policy_abbreviation=policy_abbr,
                prompt_version=prompt_version,
            )

            label = result["classification"]
            print(f"-> {label}" + (f" (ERROR: {result['error']})" if result["error"] else ""))

            all_results.append({
                id_col: row[id_col],
                "policy_abbreviation": policy_abbr,
                "policy_name": policy_name,
                "title": row["title"],
                "abstract": row["abstract"][:500] if pd.notna(row["abstract"]) else "",
                "publication_year": row.get("publication_year"),
                "source_name": row.get("source_name", row.get("venue")),
                "cited_by_count": row.get("cited_by_count"),
                "classification": label,
                "raw_response": result["raw_response"],
                "error": result["error"],
            })

            time.sleep(0.2)

    results_df = pd.DataFrame(all_results)

    # Output filename
    if run_label:
        out_path = OUTPUT_DIR / f"econ_classifications_{run_label}.csv"
    else:
        out_path = OUTPUT_DIR / "pilot_classifications.csv"

    results_df.to_csv(out_path, index=False)
    print(f"\nResults saved to {out_path}")

    print(f"\n{'='*60}")
    print("PILOT SUMMARY")
    print(f"{'='*60}")
    for policy_abbr in policies:
        subset = results_df[results_df["policy_abbreviation"] == policy_abbr]
        if len(subset) == 0:
            continue
        n_true = (subset["classification"] == True).sum()
        n_false = (subset["classification"] == False).sum()
        n_err = subset["classification"].isna().sum()
        print(f"  {policy_abbr}: {n_true} TRUE, {n_false} FALSE, {n_err} errors "
              f"(out of {len(subset)})")

    return results_df


def run_full(policies: list[str],
             prompt_version: str = "v2_no_examples",
             run_label: str = "v2_full",
             input_dir: str = None):
    """
    Classify ALL papers in the dataset (not just a sample).
    Includes checkpoint/resume: saves progress every 50 papers so a crash
    doesn't lose work. On re-run, skips already-classified papers.

    Args:
        policies: list of policy abbreviation strings
        prompt_version: key into SYSTEM_PROMPTS dict
        run_label: label for output file
        input_dir: directory containing econ_unified parquet files
    """
    using_econ_unified = input_dir is not None

    for policy_abbr in policies:
        if using_econ_unified:
            parquet_path = Path(input_dir) / f"{policy_abbr}_econ_unified.parquet"
        else:
            parquet_path = UNIFIED_DIR / f"{policy_abbr}_unified_dataset.parquet"

        if not parquet_path.exists():
            print(f"WARNING: {parquet_path} not found, skipping {policy_abbr}")
            continue

        df = pd.read_parquet(parquet_path)
        policy_name = df["policy_studied"].iloc[0]
        id_col = "econ_unified_id" if using_econ_unified and "econ_unified_id" in df.columns else "unified_id"

        print(f"\n{'='*60}")
        print(f"FULL CLASSIFICATION: {policy_abbr} ({policy_name})")
        print(f"Prompt: {prompt_version}")
        print(f"Total papers: {len(df):,}")
        has_abs = df["abstract"].notna() & (df["abstract"].str.strip() != "")
        print(f"With abstract: {has_abs.sum():,} | Without: {(~has_abs).sum():,} (skipped)")
        print(f"{'='*60}")

        # Only classify papers that have abstracts
        df = df[has_abs]

        # --- Checkpoint/resume ---
        out_path = OUTPUT_DIR / f"econ_classifications_{run_label}.csv"
        checkpoint_path = OUTPUT_DIR / f".checkpoint_{run_label}.json"
        done_ids = set()

        if out_path.exists() and checkpoint_path.exists():
            existing = pd.read_csv(out_path)
            done_ids = set(existing[id_col].tolist())
            print(f"  Resuming: {len(done_ids)} papers already classified")

        remaining = df[~df[id_col].isin(done_ids)]
        total = len(df)
        n_remaining = len(remaining)

        if n_remaining == 0:
            print("  All papers already classified. Done.")
            continue

        print(f"  Papers to classify: {n_remaining}")

        # Estimate time
        est_per_paper = 0.8  # ~0.8s per paper (API latency + 0.1s sleep)
        est_minutes = n_remaining * est_per_paper / 60
        print(f"  Estimated time: ~{est_minutes:.0f} minutes")

        # --- Classify ---
        results = []
        start_time = time.time()
        errors_consecutive = 0
        initial_done = len(done_ids)

        for i, (idx, row) in enumerate(remaining.iterrows()):
            paper_num = initial_done + i + 1
            title_preview = str(row['title'])[:70] if pd.notna(row['title']) else '[No title]'
            print(f"  [{paper_num}/{total}] {title_preview}...", end=" ", flush=True)

            result = classify_paper(
                title=row["title"] if pd.notna(row["title"]) else "",
                abstract=row["abstract"] if pd.notna(row["abstract"]) else "",
                policy_name=policy_name,
                policy_abbreviation=policy_abbr,
                prompt_version=prompt_version,
            )

            label = result["classification"]
            if result["error"]:
                errors_consecutive += 1
                print(f"-> ERROR: {result['error']}")
                if errors_consecutive >= 10:
                    print("\n  10 consecutive errors — stopping. Re-run to resume.")
                    break
            else:
                errors_consecutive = 0
                print(f"-> {label}")

            results.append({
                id_col: row[id_col],
                "policy_abbreviation": policy_abbr,
                "policy_name": policy_name,
                "title": row["title"],
                "abstract": row["abstract"][:500] if pd.notna(row["abstract"]) else "",
                "publication_year": row.get("publication_year"),
                "source_name": row.get("source_name", row.get("venue")),
                "cited_by_count": row.get("cited_by_count"),
                "classification": label,
                "raw_response": result["raw_response"],
                "error": result["error"],
            })

            # Checkpoint every 10 papers
            if (i + 1) % 10 == 0:
                _save_checkpoint(results, done_ids, out_path, checkpoint_path,
                                 id_col, paper_num, total, start_time)

            time.sleep(0.1)

        # Final save (results may be empty if last checkpoint just cleared it)
        if results:
            _save_checkpoint(results, done_ids, out_path, checkpoint_path,
                             id_col, initial_done + n_remaining, total, start_time)

        elapsed = time.time() - start_time
        print(f"\n  Classified {n_remaining} papers in {elapsed:.0f}s "
              f"({elapsed/max(n_remaining,1):.2f}s/paper)")

        # Summary
        all_df = pd.read_csv(out_path)
        n_true = (all_df["classification"] == True).sum()
        n_false = (all_df["classification"] == False).sum()
        n_err = all_df["classification"].isna().sum()
        print(f"\n{'='*60}")
        print(f"FULL RUN SUMMARY — {policy_abbr}")
        print(f"{'='*60}")
        print(f"  Total: {len(all_df)}")
        print(f"  TRUE: {n_true} ({n_true/len(all_df)*100:.1f}%)")
        print(f"  FALSE: {n_false} ({n_false/len(all_df)*100:.1f}%)")
        print(f"  Errors/None: {n_err} ({n_err/len(all_df)*100:.1f}%)")

        # Clean up checkpoint
        if checkpoint_path.exists():
            checkpoint_path.unlink()
            print(f"  Checkpoint cleaned up.")


def _save_checkpoint(new_results, done_ids, out_path, checkpoint_path,
                     id_col, paper_num, total, start_time):
    """Append new results to output CSV and update checkpoint."""
    if not new_results:
        return
    new_df = pd.DataFrame(new_results)
    if out_path.exists():
        existing = pd.read_csv(out_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_csv(out_path, index=False)

    # Update checkpoint
    with open(checkpoint_path, 'w') as f:
        json.dump({"classified": paper_num, "total": total,
                   "timestamp": datetime.now().isoformat()}, f)

    elapsed = time.time() - start_time
    rate = paper_num / max(elapsed, 1)
    remaining = (total - paper_num) / max(rate, 0.01)
    print(f"    [checkpoint] {paper_num}/{total} saved | "
          f"~{remaining/60:.0f} min remaining")

    # Clear the buffer so next checkpoint only writes new results
    new_results.clear()
    done_ids.update(combined[id_col].tolist())


def merge_econ_runs():
    """
    Merge all econ_classifications_v{1,2}_run{1,2,3}.csv files into a comparison CSV.
    """
    run_files = {}
    for v in ["v1", "v2"]:
        for r in ["run1", "run2", "run3"]:
            label = f"{v}_{r}"
            path = OUTPUT_DIR / f"econ_classifications_{label}.csv"
            if path.exists():
                run_files[label] = pd.read_csv(path)
            else:
                print(f"  WARNING: {path} not found, skipping")

    if not run_files:
        print("No econ_classifications files found to merge.")
        return

    # Use first file as base for paper metadata
    first_key = list(run_files.keys())[0]
    base = run_files[first_key].copy()

    # Determine ID column
    id_col = "econ_unified_id" if "econ_unified_id" in base.columns else "unified_id"

    # Build comparison DataFrame
    comparison = base[[id_col, "policy_abbreviation", "policy_name",
                       "title", "abstract", "publication_year",
                       "source_name", "cited_by_count"]].copy()

    for label, df in run_files.items():
        comparison[label] = df["classification"]

    # Agreement columns
    v1_cols = [c for c in comparison.columns if c.startswith("v1_run")]
    v2_cols = [c for c in comparison.columns if c.startswith("v2_run")]

    if v1_cols:
        comparison["v1_agreement"] = comparison[v1_cols].apply(
            lambda row: row.nunique() == 1, axis=1)
    if v2_cols:
        comparison["v2_agreement"] = comparison[v2_cols].apply(
            lambda row: row.nunique() == 1, axis=1)
    if v1_cols and v2_cols:
        # Check if v1 majority == v2 majority
        def majority(row, cols):
            vals = row[cols].dropna()
            if len(vals) == 0:
                return None
            return vals.mode().iloc[0] if len(vals.mode()) > 0 else None
        comparison["v1_majority"] = comparison.apply(lambda r: majority(r, v1_cols), axis=1)
        comparison["v2_majority"] = comparison.apply(lambda r: majority(r, v2_cols), axis=1)
        comparison["v1_v2_agree"] = comparison["v1_majority"] == comparison["v2_majority"]

    out_path = OUTPUT_DIR / "econ_classifications_comparison.csv"
    comparison.to_csv(out_path, index=False)
    print(f"\nMerged comparison saved to {out_path}")

    # Print summary
    print(f"\n{'='*60}")
    print("MERGE SUMMARY")
    print(f"{'='*60}")
    print(f"  Papers: {len(comparison)}")
    print(f"  Runs merged: {list(run_files.keys())}")
    if v1_cols:
        print(f"  v1 all-agree: {comparison['v1_agreement'].sum()}/{len(comparison)}")
    if v2_cols:
        print(f"  v2 all-agree: {comparison['v2_agreement'].sum()}/{len(comparison)}")
    if "v1_v2_agree" in comparison.columns:
        print(f"  v1 vs v2 majority agree: {comparison['v1_v2_agree'].sum()}/{len(comparison)}")

    return comparison


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Classify papers as evaluating policy effects on economic outcomes"
    )
    parser.add_argument(
        "--policies", nargs="+", default=["TCJA", "ACA", "NCLB"],
        help="Policy abbreviations to classify (default: TCJA ACA NCLB)"
    )
    parser.add_argument(
        "--sample_size", type=int, default=10,
        help="Number of papers to sample per policy (default: 10)"
    )
    parser.add_argument(
        "--seed", type=int, default=1602,
        help="Random seed for reproducible sampling (default: 1602)"
    )
    parser.add_argument(
        "--prompt_version", choices=list(SYSTEM_PROMPTS.keys()),
        default="v1_with_examples",
        help="Prompt version to use (default: v1_with_examples)"
    )
    parser.add_argument(
        "--run_label", type=str, default="",
        help="Label for output file (e.g. 'v1_run1'). Saves as econ_classifications_{label}.csv"
    )
    parser.add_argument(
        "--input_dir", type=str, default=None,
        help="Directory with {POLICY}_econ_unified.parquet (default: unified_dataset/output)"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Classify ALL papers (not just a sample). Includes checkpoint/resume."
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Merge all econ_classifications_v*_run*.csv into a comparison file"
    )
    args = parser.parse_args()

    if args.merge:
        merge_econ_runs()
    elif args.all:
        run_full(args.policies,
                 prompt_version=args.prompt_version,
                 run_label=args.run_label or "full",
                 input_dir=args.input_dir)
    else:
        run_pilot(args.policies, args.sample_size, args.seed,
                  prompt_version=args.prompt_version,
                  run_label=args.run_label,
                  input_dir=args.input_dir)
