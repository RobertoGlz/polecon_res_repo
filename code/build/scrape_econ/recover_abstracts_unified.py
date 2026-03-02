"""
recover_abstracts_unified.py
Recover missing abstracts for the unified econ dataset.

The APIs pipeline already tried CrossRef + Semantic Scholar (DOI-based).
This script adds sources NOT yet tried:
  1. Europe PMC API (DOI-based)
  2. OpenAlex individual re-fetch (by openalex_id — may have new data)
  3. Open access URL scraping (HTTP GET + HTML parsing)
  4. NBER website scraping (for NBER papers)
  5. Semantic Scholar title search (for papers without DOIs)

Usage:
    python recover_abstracts_unified.py TCJA
    python recover_abstracts_unified.py TCJA --resume      # resume interrupted run
    python recover_abstracts_unified.py TCJA --source epmc  # run only one source
"""

import os
import re
import sys
import json
import time
import argparse
import threading
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "output"
TMP_DIR = SCRIPT_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ── Rate limiters ─────────────────────────────────────────────────────────────
USER_EMAIL = "rgonzalezhernandez@stanford.edu"

class RateLimiter:
    def __init__(self, min_interval):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.last_call = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

epmc_limiter = RateLimiter(0.15)    # ~7 req/sec
oa_limiter = RateLimiter(0.12)      # ~8 req/sec
ss_limiter = RateLimiter(0.35)      # ~3 req/sec (more conservative)
nber_limiter = RateLimiter(0.5)     # 2 req/sec
oa_url_limiter = RateLimiter(0.3)   # ~3 req/sec

WORKERS = 4


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize_doi(doi):
    if not doi or pd.isna(doi):
        return ''
    doi = str(doi).strip().lower()
    for prefix in ['https://doi.org/', 'http://doi.org/', 'doi:']:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
    return doi.strip()


# ── Recovery source 1: Europe PMC ─────────────────────────────────────────────

def recover_epmc(doi):
    """Recover abstract from Europe PMC API."""
    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None

    try:
        epmc_limiter.wait()
        url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        params = {
            'query': f'DOI:"{clean_doi}"',
            'format': 'json',
            'resultType': 'core',
            'pageSize': 1,
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get('resultList', {}).get('result', [])
        if results:
            abstract = results[0].get('abstractText', '')
            if abstract and len(abstract.strip()) > 50:
                return abstract.strip()
    except Exception:
        pass
    return None


# ── Recovery source 2: OpenAlex re-fetch ──────────────────────────────────────

def reconstruct_abstract(inverted_index):
    """Reconstruct abstract from OpenAlex inverted index."""
    if not inverted_index:
        return ''
    max_pos = -1
    for positions in inverted_index.values():
        if positions:
            max_pos = max(max_pos, max(positions))
    if max_pos < 0:
        return ''
    words = [''] * (max_pos + 1)
    for word, positions in inverted_index.items():
        for pos in positions:
            words[pos] = word
    return ' '.join(words)


def recover_openalex(openalex_id):
    """Re-fetch from OpenAlex individual works endpoint."""
    if not openalex_id or pd.isna(openalex_id):
        return None

    oa_id = str(openalex_id).strip()
    # Convert full URL to just the ID part
    if 'openalex.org' in oa_id:
        oa_id = oa_id.split('/')[-1]

    try:
        oa_limiter.wait()
        url = f"https://api.openalex.org/works/{oa_id}"
        resp = requests.get(url, params={'mailto': USER_EMAIL}, timeout=15)
        if resp.status_code != 200:
            return None

        work = resp.json()
        abstract = reconstruct_abstract(work.get('abstract_inverted_index'))
        if abstract and len(abstract.strip()) > 50:
            return abstract.strip()
    except Exception:
        pass
    return None


# ── Recovery source 3: Open access URL scraping ──────────────────────────────

def recover_oa_url(url_str):
    """Scrape abstract from an open access URL."""
    if not url_str or pd.isna(url_str):
        return None

    url_str = str(url_str).strip()
    if not url_str.startswith('http'):
        return None

    try:
        oa_url_limiter.wait()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
        }
        resp = requests.get(url_str, headers=headers, timeout=20, allow_redirects=True)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get('content-type', '')
        if 'pdf' in content_type.lower():
            return None  # Skip PDFs

        html = resp.text
        if len(html) < 200:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # Try common abstract selectors
        selectors = [
            {'name': 'meta', 'attrs': {'name': 'citation_abstract'}},
            {'name': 'meta', 'attrs': {'name': 'DC.description'}},
            {'name': 'meta', 'attrs': {'name': 'description'}},
            {'name': 'meta', 'attrs': {'property': 'og:description'}},
            {'name': 'div', 'attrs': {'class': re.compile(r'abstract', re.I)}},
            {'name': 'section', 'attrs': {'class': re.compile(r'abstract', re.I)}},
            {'name': 'p', 'attrs': {'class': re.compile(r'abstract', re.I)}},
            {'name': 'div', 'attrs': {'id': re.compile(r'abstract', re.I)}},
            {'name': 'section', 'attrs': {'id': re.compile(r'abstract', re.I)}},
        ]

        for sel in selectors:
            elem = soup.find(**sel)
            if elem:
                if elem.name == 'meta':
                    text = elem.get('content', '')
                else:
                    text = elem.get_text(separator=' ', strip=True)
                # Clean up
                text = re.sub(r'\s+', ' ', text).strip()
                # Remove "Abstract" prefix
                text = re.sub(r'^Abstract[\s.:]*', '', text, flags=re.I).strip()
                if len(text) > 50:
                    return text
    except Exception:
        pass
    return None


# ── Recovery source 4: NBER website ───────────────────────────────────────────

def recover_nber(nber_id_or_doi):
    """Scrape abstract from NBER website."""
    if not nber_id_or_doi or pd.isna(nber_id_or_doi):
        return None

    nber_id = str(nber_id_or_doi).strip()

    # Extract NBER working paper number
    match = re.search(r'w?(\d{4,6})', nber_id)
    if not match:
        return None

    wp_num = match.group(1)

    try:
        nber_limiter.wait()
        url = f"https://www.nber.org/papers/w{wp_num}"
        headers = {'User-Agent': 'Mozilla/5.0 (Academic research)'}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Try various selectors
        for selector in [
            'div.page-header__intro-inner',
            'div.page-header__intro',
            'div[class*="abstract"]',
            'meta[name="description"]',
        ]:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get('content', '') if elem.name == 'meta' else elem.get_text(separator=' ', strip=True)
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 50:
                    return text
    except Exception:
        pass
    return None


# ── Recovery source 5: Semantic Scholar title search ──────────────────────────

def recover_ss_title(title):
    """Search Semantic Scholar by title to find abstract."""
    if not title or pd.isna(title):
        return None

    title_clean = str(title).strip()
    if len(title_clean) < 10:
        return None

    try:
        ss_limiter.wait()
        url = "https://api.semanticscholar.org/graph/v1/paper/search"
        params = {
            'query': title_clean[:200],
            'fields': 'title,abstract',
            'limit': 3,
        }
        ss_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY')
        headers = {'x-api-key': ss_key} if ss_key else {}

        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        papers = data.get('data', [])
        # Find best title match
        title_lower = title_clean.lower()
        for paper in papers:
            p_title = (paper.get('title') or '').lower()
            # Simple similarity: check if most words overlap
            if p_title and _title_similar(title_lower, p_title):
                abstract = paper.get('abstract', '')
                if abstract and len(abstract.strip()) > 50:
                    return abstract.strip()
    except Exception:
        pass
    return None


def _title_similar(a, b):
    """Check if two titles are similar enough to be the same paper."""
    words_a = set(re.sub(r'[^\w\s]', '', a).split())
    words_b = set(re.sub(r'[^\w\s]', '', b).split())
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b)
    return overlap / min(len(words_a), len(words_b)) > 0.8


# ── Main recovery pipeline ───────────────────────────────────────────────────

SOURCES = ['epmc', 'openalex', 'oa_url', 'nber', 'ss_title']

def recover_single_paper(row, sources=None):
    """
    Try multiple recovery sources for a single paper.
    Returns (abstract, source_name) or (None, None).
    """
    if sources is None:
        sources = SOURCES

    doi = row.get('doi')
    openalex_id = row.get('openalex_id')
    oa_url = row.get('open_access_url')
    nber_id = row.get('nber_id')
    title = row.get('title')
    is_nber = row.get('in_nber', False)

    for source in sources:
        if source == 'epmc' and doi:
            abstract = recover_epmc(doi)
            if abstract:
                return abstract, 'EuropePMC'

        elif source == 'openalex' and openalex_id:
            abstract = recover_openalex(openalex_id)
            if abstract:
                return abstract, 'OpenAlex_recovery'

        elif source == 'oa_url' and oa_url:
            abstract = recover_oa_url(oa_url)
            if abstract:
                return abstract, 'OA_URL'

        elif source == 'nber' and is_nber and nber_id:
            abstract = recover_nber(nber_id)
            if abstract:
                return abstract, 'NBER_recovery'

        elif source == 'ss_title' and title and not doi:
            # Only use title search for papers without DOIs
            abstract = recover_ss_title(title)
            if abstract:
                return abstract, 'SS_title_search'

    return None, None


def run_recovery(policy_abbr, sources=None, resume=False):
    """
    Run abstract recovery on the unified dataset for a given policy.
    """
    parquet_path = OUTPUT_DIR / f"{policy_abbr}_econ_unified.parquet"
    if not parquet_path.exists():
        print(f"ERROR: {parquet_path} not found")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df):,} papers for {policy_abbr}")

    has_abs = df['abstract'].notna() & (df['abstract'].str.strip() != '')
    print(f"  With abstract: {has_abs.sum():,}")
    print(f"  Missing: {(~has_abs).sum():,}")

    # Identify papers to process
    missing_mask = ~has_abs
    if sources is None:
        sources = SOURCES

    # Checkpoint file
    checkpoint_path = TMP_DIR / f"{policy_abbr}_recovery_checkpoint.json"
    tried_ids = set()

    if resume and checkpoint_path.exists():
        with open(checkpoint_path) as f:
            ckpt = json.load(f)
        tried_ids = set(ckpt.get('tried_ids', []))
        print(f"  Resuming: {len(tried_ids)} papers already attempted")

    missing_df = df[missing_mask & ~df['econ_unified_id'].isin(tried_ids)]
    print(f"  Papers to process: {len(missing_df):,}")
    print(f"  Sources: {sources}")

    if len(missing_df) == 0:
        print("  Nothing to recover.")
        return df

    # Run recovery with parallel workers
    recovered = 0
    completed = 0
    start_time = time.time()
    lock = threading.Lock()

    def process(idx_row):
        idx, row = idx_row
        return idx, *recover_single_paper(row.to_dict(), sources)

    items = list(missing_df.iterrows())
    total = len(items)

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process, item): item[0] for item in items}

        for future in as_completed(futures):
            try:
                idx, abstract, source = future.result()

                with lock:
                    completed += 1
                    tried_ids.add(int(df.loc[idx, 'econ_unified_id']))

                    if abstract:
                        df.loc[idx, 'abstract'] = abstract
                        df.loc[idx, 'abstract_source'] = source
                        recovered += 1

                    if completed == 20:
                        elapsed = time.time() - start_time
                        per_paper = elapsed / 20
                        est_total = per_paper * total
                        print(f"\n  Timing: 20 papers in {elapsed:.1f}s ({per_paper:.2f}s/paper). "
                              f"Est. total: ~{est_total/60:.0f} min")

                    if completed % 100 == 0:
                        pct = completed / total * 100
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        remaining = (total - completed) / rate if rate > 0 else 0
                        print(f"  Progress: {completed}/{total} ({pct:.0f}%) | "
                              f"Recovered: {recovered} | ~{remaining/60:.0f} min left")

                        # Save checkpoint
                        with open(checkpoint_path, 'w') as f:
                            json.dump({
                                'tried_ids': list(tried_ids),
                                'recovered': recovered,
                                'completed': completed,
                                'timestamp': datetime.now().isoformat(),
                            }, f)
                        # Save progress to parquet
                        df.to_parquet(parquet_path, index=False)

            except Exception as e:
                with lock:
                    completed += 1
                    print(f"  Error: {e}")

    # Final save
    df.to_parquet(parquet_path, index=False)
    df.to_csv(OUTPUT_DIR / f"{policy_abbr}_econ_unified.csv", index=False)

    elapsed = time.time() - start_time
    has_abs_after = df['abstract'].notna() & (df['abstract'].str.strip() != '')

    print(f"\n{'='*60}")
    print(f"RECOVERY COMPLETE — {policy_abbr}")
    print(f"{'='*60}")
    print(f"  Processed: {completed}")
    print(f"  Recovered: {recovered}")
    print(f"  Time: {elapsed/60:.1f} minutes ({elapsed/max(completed,1):.2f}s/paper)")
    print(f"  Abstract coverage: {has_abs.sum()} -> {has_abs_after.sum()} "
          f"({has_abs.mean()*100:.1f}% -> {has_abs_after.mean()*100:.1f}%)")

    # Update metadata
    meta_path = OUTPUT_DIR / f"{policy_abbr}_econ_unified_metadata.json"
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
        meta['with_abstract'] = int(has_abs_after.sum())
        meta['abstract_pct'] = round(has_abs_after.mean() * 100, 1)
        meta['recovery'] = {
            'sources_tried': sources,
            'recovered': recovered,
            'processed': completed,
            'elapsed_seconds': round(elapsed, 1),
            'timestamp': datetime.now().isoformat(),
        }
        # Breakdown by source
        src_counts = df.loc[has_abs_after, 'abstract_source'].value_counts().to_dict()
        meta['recovery']['by_source'] = {str(k): int(v) for k, v in src_counts.items()}
        with open(meta_path, 'w') as f:
            json.dump(meta, f, indent=2, default=str)

    # Clean up checkpoint
    if checkpoint_path.exists():
        checkpoint_path.unlink()

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Recover missing abstracts for the unified econ dataset"
    )
    parser.add_argument("policy_abbr", help="Policy abbreviation (e.g., TCJA)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--source", choices=SOURCES,
                        help="Run only one recovery source")
    args = parser.parse_args()

    sources = [args.source] if args.source else None
    run_recovery(args.policy_abbr.upper(), sources=sources, resume=args.resume)
