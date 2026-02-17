"""
classify_papers_main.py
Issue #22: Flag papers that evaluate the effect of a policy on an economic outcome.

Calls the Google Gemini API to classify each paper as TRUE/FALSE.
Pilot mode: runs a small sample in real-time for manual inspection.

Usage:
    python classify_papers_main.py                     # pilot (10 papers/policy)
    python classify_papers_main.py --sample_size 50    # larger pilot
    python classify_papers_main.py --policies TCJA ACA # subset of policies
"""

import os
import sys
import time
import itertools
import argparse
import pandas as pd
from pathlib import Path
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
# Prompt templates  (v1 — see CONVERSATION_LOG.md for iteration history)
# ---------------------------------------------------------------------------
SYSTEM_PROMPT_TEMPLATE = (
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
)

USER_PROMPT_TEMPLATE = "Title: {title}\nAbstract: {abstract}"

# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------
def classify_paper(title: str, abstract: str, policy_name: str,
                   policy_abbreviation: str) -> dict:
    """
    Call Gemini to classify a single paper.
    Returns dict with 'classification', 'raw_response', 'error'.
    """
    api_key = next(_key_cycle)
    client = genai.Client(api_key=api_key)

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
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


def run_pilot(policies: list[str], sample_size: int, seed: int = 1602):
    """
    Run pilot classification on a sample of papers for each policy.
    Only papers WITH abstracts are sampled.
    """
    all_results = []

    for policy_abbr in policies:
        parquet_path = UNIFIED_DIR / f"{policy_abbr}_unified_dataset.parquet"
        if not parquet_path.exists():
            print(f"WARNING: {parquet_path} not found, skipping {policy_abbr}")
            continue

        df = pd.read_parquet(parquet_path)
        policy_name = df["policy_studied"].iloc[0]

        has_abstract = df["abstract"].notna() & (df["abstract"].str.strip() != "")
        df_with_abs = df[has_abstract]
        print(f"\n{'='*60}")
        print(f"Policy: {policy_abbr} ({policy_name})")
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
            )

            label = result["classification"]
            print(f"-> {label}" + (f" (ERROR: {result['error']})" if result["error"] else ""))

            all_results.append({
                "unified_id": row["unified_id"],
                "policy_abbreviation": policy_abbr,
                "policy_name": policy_name,
                "title": row["title"],
                "abstract": row["abstract"][:500] if pd.notna(row["abstract"]) else "",
                "publication_year": row.get("publication_year"),
                "venue": row.get("venue"),
                "cited_by_count": row.get("cited_by_count"),
                "classification": label,
                "raw_response": result["raw_response"],
                "error": result["error"],
            })

            time.sleep(0.2)

    results_df = pd.DataFrame(all_results)
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
    args = parser.parse_args()

    run_pilot(args.policies, args.sample_size, args.seed)
