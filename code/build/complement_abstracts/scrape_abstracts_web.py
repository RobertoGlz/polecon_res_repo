"""
Authenticated web scraping for abstract recovery using Stanford SSO.

Issue #7: Extend abstract recovery pipeline
Parent Issue #1: Scrape policies with OpenAlex

This script launches a browser with a persistent profile to scrape abstracts
from paywalled sources using Stanford SSO authentication. On first run, a
browser window opens for manual SSO login (including 2FA). The session is
saved so subsequent runs skip the login step.

Uses undetected-chromedriver to avoid Cloudflare bot detection, and resolves
DOIs through Stanford's generic proxy (doi-org.stanford.idm.oclc.org) which
handles all publishers uniformly.

Available sources:
    - doi:      Resolve DOIs through Stanford proxy (works for all publishers)
    - proquest: Search ProQuest by title (independent full-text database)
    - econlit:  Search EconLit/EBSCOhost by title (economics-specific)
    - all:      Try doi first, then proquest, then econlit

Safety Features:
    - Conservative rate limiting (default 7s between requests)
    - Exponential backoff on errors
    - IP block detection (pauses for user intervention)
    - Checkpoint save/resume for interrupted sessions
    - Cloudflare CAPTCHA detection (pauses for user to solve)

Usage:
    python scrape_abstracts_web.py POLICY --source doi
    python scrape_abstracts_web.py TCJA --source doi --delay 8 --max-papers 500
    python scrape_abstracts_web.py ACA --source proquest --resume
    python scrape_abstracts_web.py NCLB --source all

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 2026
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException,
    StaleElementReferenceException
)

# =============================================================================
# PATHS
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output_web_scraping")
CHECKPOINT_DIR = os.path.join(OUTPUT_DIR, "checkpoints")
PROFILE_DIR = os.path.join(SCRIPT_DIR, "selenium_profile")

# Input directories (complement_abstracts output preferred, unified dataset as fallback)
COMPLEMENT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "output"))
UNIFIED_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "unified_dataset", "output"))

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(PROFILE_DIR, exist_ok=True)

# =============================================================================
# RATE LIMITING & SAFETY DEFAULTS
# =============================================================================
DEFAULT_DELAY = 7          # Seconds between requests
MAX_RETRIES = 2            # Retries per paper per source
BACKOFF_FACTOR = 2         # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 25   # Save checkpoint every N papers
CONSECUTIVE_BLOCK_LIMIT = 3  # Halt after this many consecutive blocks

# IP block detection phrases (checked against visible page text only)
BLOCK_INDICATORS = [
    'too many requests',
    'rate limit',
    'please verify you are a human',
    'unusual traffic',
    'automated access',
    'bot detection',
    'please try again later',
]


# =============================================================================
# SESSION MANAGER — persistent profile with undetected-chromedriver
# =============================================================================
class SessionManager:
    """
    Manages Chrome browser with persistent profile for Stanford SSO.

    Uses undetected-chromedriver to avoid Cloudflare bot detection.
    The browser profile is saved to disk so Stanford SSO login persists
    across runs — log in once, and subsequent runs skip authentication.
    """

    def __init__(self):
        self.browser = None

    def create_browser(self):
        """Create undetected Chrome browser with persistent profile."""
        options = uc.ChromeOptions()
        options.add_argument(f'--user-data-dir={PROFILE_DIR}')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')

        self.browser = uc.Chrome(options=options, use_subprocess=True)
        self.browser.set_page_load_timeout(60)
        print(f"  Browser launched (profile: {PROFILE_DIR})")
        return self.browser

    def verify_stanford_access(self):
        """
        Check Stanford proxy access and prompt for login if needed.
        Navigates to Stanford DOI proxy and waits for login if redirected.
        """
        print("  Verifying Stanford proxy access...")
        try:
            self.browser.get('https://doi-org.stanford.idm.oclc.org/')
            time.sleep(4)

            # Wait for Cloudflare if needed
            wait_for_cloudflare(self.browser)

            url = self.browser.current_url.lower()
            if 'login.stanford.edu' in url or 'idp' in url:
                print("\n" + "=" * 60)
                print("STANFORD SSO LOGIN REQUIRED")
                print("=" * 60)
                print("A browser window has opened. Please:")
                print("  1. Log in with your Stanford credentials")
                print("  2. Complete 2FA if prompted")
                print("  3. Wait for the login to complete")
                print("  Your session will be saved for future runs.")
                print("=" * 60)
                return self._wait_for_login(timeout=300)
            print("  Stanford proxy access confirmed (session active).")
            return True
        except Exception as e:
            print(f"  Warning: Could not verify Stanford access: {e}")
            return True

    def _wait_for_login(self, timeout=300):
        """Wait for the user to complete Stanford SSO login."""
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(3)
            try:
                url = self.browser.current_url.lower()
                if 'login.stanford.edu' not in url and 'idp' not in url:
                    print("  Login successful!")
                    time.sleep(2)
                    return True
            except Exception:
                pass
        print("  Login timeout — proceeding anyway.")
        return False

    def close(self):
        """Close browser (profile is preserved for next session)."""
        if self.browser:
            try:
                self.browser.quit()
            except Exception:
                pass
            self.browser = None


# =============================================================================
# CHECKPOINT MANAGER
# =============================================================================
class CheckpointManager:
    """Saves and loads progress for interrupted sessions."""

    def __init__(self, policy, source):
        self.policy = policy
        self.source = source
        self.checkpoint_file = os.path.join(
            CHECKPOINT_DIR, f"{policy}_{source}_checkpoint.json"
        )
        self.processed_ids = set()
        self.results = []

    def load(self):
        """Load checkpoint from disk if it exists."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            self.processed_ids = set(data.get('processed_ids', []))
            self.results = data.get('results', [])
            print(f"  Resumed from checkpoint: {len(self.processed_ids)} papers already processed")
            return True
        return False

    def save(self):
        """Save current progress to disk."""
        data = {
            'policy': self.policy,
            'source': self.source,
            'timestamp': datetime.now().isoformat(),
            'processed_ids': list(self.processed_ids),
            'results': self.results,
            'total_processed': len(self.processed_ids),
            'total_recovered': sum(1 for r in self.results if r.get('has_abstract'))
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(data, f, indent=2)

    def add_result(self, paper_id, result):
        """Add a result and save checkpoint periodically."""
        self.processed_ids.add(paper_id)
        self.results.append(result)
        if len(self.processed_ids) % CHECKPOINT_INTERVAL == 0:
            self.save()
            recovered = sum(1 for r in self.results if r.get('has_abstract'))
            print(f"  Checkpoint saved: {len(self.processed_ids)} processed, {recovered} recovered")


# =============================================================================
# PAGE ANALYSIS UTILITIES
# =============================================================================
def detect_ip_block(browser):
    """
    Check if the current page indicates an IP block or rate limit.
    Only triggers on visible page text (not hidden HTML attributes).
    """
    try:
        visible_text = browser.find_element(By.TAG_NAME, 'body').text.lower()
        for indicator in BLOCK_INDICATORS:
            if indicator in visible_text:
                url = browser.current_url
                print(f"    [BLOCK] Matched '{indicator}' on {url[:80]}")
                idx = visible_text.find(indicator)
                snippet = visible_text[max(0, idx-40):idx+len(indicator)+40]
                print(f"    [BLOCK] Context: ...{snippet}...")
                return True
    except Exception:
        pass
    return False


def wait_for_cloudflare(browser, timeout=120):
    """
    Detect Cloudflare challenge page and wait for user to solve CAPTCHA.
    Returns True if resolved or not a Cloudflare page, False on timeout.
    """
    try:
        title = browser.title.strip()
    except Exception:
        return True

    if title != 'Just a moment...':
        return True

    print("    [CAPTCHA] Cloudflare challenge detected — please solve it in the browser")
    start = time.time()
    while time.time() - start < timeout:
        time.sleep(2)
        try:
            title = browser.title.strip()
            if title != 'Just a moment...':
                print(f"    [CAPTCHA] Solved! Page title: {title[:60]}")
                time.sleep(2)
                return True
        except Exception:
            return False
    print(f"    [CAPTCHA] Timeout after {timeout}s")
    return False


def clean_abstract_text(text):
    """Clean and normalize abstract text."""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = ' '.join(text.split())
    text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY)[.:]?\s*', '', text)
    return text.strip()


def clean_ssrn_abstract(text):
    """
    Remove SSRN page metadata from extracted abstract text.

    SSRN pages wrap the abstract in a container that also includes paper
    metadata (page count, posting date, author affiliations) and footer
    info (keywords, JEL codes, eJournal references). This function strips
    all of that, leaving only the actual abstract content.
    """
    # Remove header block: "N Pages Posted: ... Date Written: DATE"
    text = re.sub(
        r'^\d+\s+Pages\s+Posted:.*?(?:Date Written:.*?\d{4})\s*',
        '', text, flags=re.DOTALL
    )
    # If header removal didn't work, try simpler pattern:
    # remove everything before the first sentence-like content
    # (SSRN metadata lines don't end with periods)
    if re.match(r'^\d+\s+Pages\s+Posted:', text):
        # Find first line that looks like abstract content (starts with uppercase, >50 chars)
        lines = text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if len(line) > 50 and line[0].isupper() and not line.startswith(('Posted', 'Last revised')):
                text = '\n'.join(lines[i:])
                break

    # Remove trailing keywords, JEL codes, and eJournal references
    text = re.sub(r'\s*Keywords?:.*$', '', text, flags=re.DOTALL)
    text = re.sub(r'\s*JEL Classification:.*$', '', text, flags=re.DOTALL)
    text = re.sub(r'\s*Suggested Citation:.*$', '', text, flags=re.DOTALL)
    # Remove trailing eJournal lines (e.g., "Behavioral & Experimental Finance eJournal")
    text = re.sub(r'\s+[\w\s&]+eJournal.*$', '', text, flags=re.DOTALL)
    # Remove trailing "Research Paper Series" lines
    text = re.sub(r'\s+[\w\s&]+Research Paper Series.*$', '', text, flags=re.DOTALL)

    return text.strip()


def build_doi_proxy_url(doi):
    """
    Build a Stanford-proxied URL for a DOI.

    Uses the generic Stanford DOI proxy which handles all publishers uniformly.
    The proxy resolves the DOI and redirects through Stanford's OCLC proxy,
    preserving authentication for paywalled content.
    """
    clean_doi = doi
    if doi.startswith('http'):
        clean_doi = re.sub(r'^https?://doi\.org/', '', doi)
    return f'https://doi-org.stanford.idm.oclc.org/{clean_doi}'


# =============================================================================
# ABSTRACT EXTRACTION
# =============================================================================
def extract_abstract_from_page(browser):
    """
    Extract abstract from the current page using multiple strategies.

    Strategy order:
        1. citation_abstract meta tag (full abstract, most reliable)
        2. CSS selectors for publisher abstract containers (full text)
        3. Heading-based fallback (find "Abstract" heading, grab content)
        4. og:description / description meta tags (often truncated — last resort)

    Returns:
        str or None: Abstract text if found (>100 chars), else None
    """
    # Strategy 1: citation_abstract meta tag (always full abstract when present)
    try:
        for meta_sel in ['meta[name="citation_abstract"]', 'meta[name="DC.description"]']:
            metas = browser.find_elements(By.CSS_SELECTOR, meta_sel)
            for meta in metas:
                content = meta.get_attribute('content')
                if content and len(content.strip()) > 100:
                    return clean_abstract_text(content)
    except Exception:
        pass

    # Strategy 2: CSS selectors for publisher abstract containers
    abstract_selectors = [
        # Generic patterns (work across many publishers)
        'div[class*="abstract"] p',
        'section[class*="abstract"] p',
        'div[id*="abstract"] p',
        'div.abstract p',
        'section.abstract p',
        # Springer / Nature
        'div.c-article-section__content p',
        # Wiley
        'div.article-section__content p',
        # Taylor & Francis
        'div.abstractSection p',
        'div.NLM_abstract p',
        # SAGE
        'div.hlFld-Abstract p',
        # Elsevier / ScienceDirect
        'div.abstract.author p',
        'div#abstracts div.abstract p',
        # Oxford University Press
        'section.abstract p',
        'div.abstract-body p',
        # SSRN
        'div.abstract-text p',
        'div#abstract p',
    ]

    for selector in abstract_selectors:
        try:
            elements = browser.find_elements(By.CSS_SELECTOR, selector)
            texts = []
            for elem in elements:
                text = elem.text.strip()
                if text and len(text) > 20:
                    texts.append(text)
            combined = ' '.join(texts)
            if len(combined) > 100:
                return clean_abstract_text(combined)
        except (StaleElementReferenceException, NoSuchElementException):
            continue
        except Exception:
            continue

    # Strategy 3: Find heading with "Abstract" and grab sibling/parent text
    try:
        headings = browser.find_elements(By.CSS_SELECTOR, 'h1, h2, h3, h4')
        for heading in headings:
            if 'abstract' in heading.text.lower():
                parent = heading.find_element(By.XPATH, '..')
                parent_text = parent.text.strip()
                content = parent_text.replace(heading.text.strip(), '').strip()
                if len(content) > 100:
                    return clean_abstract_text(content)
    except Exception:
        pass

    # Strategy 4: og:description / description meta tags (often truncated)
    # Only use these as a last resort since they're typically ~160 chars
    try:
        for meta_sel in ['meta[property="og:description"]', 'meta[name="description"]']:
            metas = browser.find_elements(By.CSS_SELECTOR, meta_sel)
            for meta in metas:
                content = meta.get_attribute('content')
                if not content:
                    continue
                content = content.strip()
                # Skip if clearly truncated (ends with ellipsis and short)
                if len(content) < 250 and content.endswith(('\u2026', '...')):
                    continue
                if len(content) > 100:
                    return clean_abstract_text(content)
    except Exception:
        pass

    return None


# =============================================================================
# SCRAPING FUNCTIONS
# =============================================================================
def scrape_abstract_doi(browser, doi, delay=DEFAULT_DELAY):
    """
    Resolve DOI through Stanford proxy and extract abstract from publisher page.

    Navigates to doi-org.stanford.idm.oclc.org/{doi} which resolves through
    Stanford's proxy, providing authenticated access to paywalled content.
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'DOI_Authenticated'
    }

    if not doi:
        result['error'] = 'No DOI provided'
        return result

    try:
        proxy_url = build_doi_proxy_url(doi)
        browser.get(proxy_url)
        time.sleep(delay)

        landed_url = browser.current_url[:120]
        page_title = browser.title[:80] if browser.title else '(no title)'
        print(f"    [DOI] Landed: {landed_url}")
        print(f"    [DOI] Title: {page_title}")

        # Handle Cloudflare challenge
        if not wait_for_cloudflare(browser):
            result['error'] = 'Cloudflare CAPTCHA timeout'
            return result

        # Check for blocks
        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Check for error pages
        title_lower = page_title.lower()
        if any(err in title_lower for err in ['not found', 'error', 'page not found', '404']):
            result['error'] = f'Error page: {page_title}'
            return result

        # Extract abstract
        abstract = extract_abstract_from_page(browser)
        if abstract:
            # Clean SSRN metadata if we landed on an SSRN page
            current_url = browser.current_url.lower()
            if 'ssrn.com' in current_url:
                abstract = clean_ssrn_abstract(abstract)
            result['abstract'] = abstract
            result['has_abstract'] = True
            return result

        result['error'] = 'Abstract not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def scrape_abstract_proquest(browser, title, doi, delay=DEFAULT_DELAY):
    """
    Search ProQuest by title and extract abstract.
    ProQuest is an independent full-text database with strong abstract coverage.
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'ProQuest'
    }

    try:
        search_url = 'https://www-proquest-com.stanford.idm.oclc.org/advanced'
        browser.get(search_url)
        time.sleep(delay)

        if not wait_for_cloudflare(browser):
            result['error'] = 'Cloudflare CAPTCHA timeout'
            return result
        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Search by title
        try:
            search_box = WebDriverWait(browser, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    'input[name="queryTermField"], input#queryTermField, '
                    'input[type="text"][class*="search"], textarea[name="queryTermField"]'))
            )
            search_box.clear()
            clean_title = title.replace('"', '')[:200]
            search_box.send_keys(f'"{clean_title}"')
            search_box.send_keys(Keys.RETURN)
            time.sleep(delay)
        except TimeoutException:
            result['error'] = 'Search box not found'
            return result

        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Click first result
        try:
            first_result = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    'a.doc_link, a[class*="resultTitle"], h3 a, '
                    'a[class*="previewTitle"]'))
            )
            first_result.click()
            time.sleep(delay)
        except TimeoutException:
            result['error'] = 'No search results found'
            return result

        # Extract abstract
        abstract = extract_abstract_from_page(browser)
        if abstract:
            result['abstract'] = abstract
            result['has_abstract'] = True
            return result

        # ProQuest-specific selectors as fallback
        for selector in ['div.abstract p', 'div.abstractText', 'div.Abstract p']:
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    text = elem.text.strip()
                    if len(text) > 100:
                        result['abstract'] = clean_abstract_text(text)
                        result['has_abstract'] = True
                        return result
            except Exception:
                continue

        result['error'] = 'Abstract not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def scrape_abstract_econlit(browser, title, doi, delay=DEFAULT_DELAY):
    """
    Search EconLit/EBSCOhost by title and extract abstract.
    EconLit is an economics-specific database with curated abstracts.
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'EconLit'
    }

    try:
        clean_title = title.replace('"', '').replace('&', ' ')[:200]
        search_url = (
            'https://search-ebscohost-com.stanford.idm.oclc.org/login.aspx'
            f'?direct=true&db=ecn&bquery=TI+"{clean_title}"'
        )
        browser.get(search_url)
        time.sleep(delay)

        if not wait_for_cloudflare(browser):
            result['error'] = 'Cloudflare CAPTCHA timeout'
            return result
        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Click first result
        try:
            first_result = WebDriverWait(browser, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    'a[class*="title-link"], a.record-title, '
                    'a[data-auto="result-title"]'))
            )
            first_result.click()
            time.sleep(delay)
        except TimeoutException:
            result['error'] = 'No search results found'
            return result

        # Extract abstract
        abstract = extract_abstract_from_page(browser)
        if abstract:
            result['abstract'] = abstract
            result['has_abstract'] = True
            return result

        # EconLit-specific selectors
        for selector in ['div.abstract-text', 'p[class*="abstract"]', 'div.record-abstract p']:
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    text = elem.text.strip()
                    if len(text) > 100:
                        result['abstract'] = clean_abstract_text(text)
                        result['has_abstract'] = True
                        return result
            except Exception:
                continue

        result['error'] = 'Abstract not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


# =============================================================================
# DATA LOADING
# =============================================================================
def load_input_dataset(policy_abbr):
    """
    Load papers for a policy.
    Tries complement_abstracts output first, falls back to unified dataset.
    """
    complement_file = os.path.join(
        COMPLEMENT_DIR, f"{policy_abbr}_papers_complemented_filtered.parquet"
    )
    if os.path.exists(complement_file):
        print(f"  Loading from complement_abstracts output: {complement_file}")
        return pd.read_parquet(complement_file)

    parquet_file = os.path.join(UNIFIED_DIR, f"{policy_abbr}_unified_dataset.parquet")
    csv_file = os.path.join(UNIFIED_DIR, f"{policy_abbr}_unified_dataset.csv")

    if os.path.exists(parquet_file):
        print(f"  Loading from unified dataset: {parquet_file}")
        return pd.read_parquet(parquet_file)
    elif os.path.exists(csv_file):
        print(f"  Loading from unified dataset: {csv_file}")
        return pd.read_csv(csv_file)

    print(f"ERROR: No input dataset found for {policy_abbr}")
    print(f"  Looked for: {complement_file}")
    print(f"  Looked for: {parquet_file}")
    return None


def get_papers_to_scrape(df, source):
    """
    Select papers needing abstract recovery, prioritized by DOI availability.
    """
    missing_mask = (df['abstract'].isna()) | (df['abstract'] == '') | (df['abstract'] == 'None')
    papers = df[missing_mask].copy()

    if source == 'doi':
        has_doi = papers['doi'].notna() & (papers['doi'] != '')
        papers = papers[has_doi]

    # Prioritize papers with DOIs
    papers['_has_doi'] = papers['doi'].notna() & (papers['doi'] != '')
    papers = papers.sort_values(by=['_has_doi'], ascending=False)
    papers = papers.drop(columns=['_has_doi'])

    return papers


# =============================================================================
# MAIN SCRAPING PIPELINE
# =============================================================================
def run_scraper(policy_abbr, source, delay, max_papers, resume):
    """
    Main scraping function.
    Launches browser, loads papers, and scrapes abstracts.
    """
    print(f"\n{'='*70}")
    print(f"WEB SCRAPING: {policy_abbr} via {source}")
    print(f"{'='*70}")
    print(f"  Delay: {delay}s | Max papers: {max_papers or 'all'} | Resume: {resume}")
    print(f"  Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load dataset
    df = load_input_dataset(policy_abbr)
    if df is None:
        return

    print(f"  Loaded {len(df)} papers")

    # Get papers to scrape
    papers = get_papers_to_scrape(df, source)
    print(f"  Papers missing abstracts (eligible for {source}): {len(papers)}")

    if len(papers) == 0:
        print("  No papers to scrape. Exiting.")
        return

    # Initialize checkpoint
    checkpoint = CheckpointManager(policy_abbr, source)
    if resume:
        checkpoint.load()

    # Filter out already-processed papers
    if checkpoint.processed_ids:
        id_col = 'openalex_id' if 'openalex_id' in papers.columns else papers.index.name or '_idx'
        if id_col == '_idx':
            papers['_idx'] = papers.index.astype(str)
        papers = papers[~papers[id_col].astype(str).isin(checkpoint.processed_ids)]
        if id_col == '_idx':
            papers = papers.drop(columns=['_idx'])
        print(f"  Papers remaining after checkpoint filter: {len(papers)}")

    # Apply max_papers limit
    if max_papers and len(papers) > max_papers:
        papers = papers.head(max_papers)
        print(f"  Limited to {max_papers} papers")

    if len(papers) == 0:
        print("  All papers already processed.")
        if checkpoint.results:
            merge_results(policy_abbr, source, df, checkpoint.results)
        return

    # Map sources to scraping functions
    scrape_functions = {
        'doi': scrape_abstract_doi,
        'proquest': scrape_abstract_proquest,
        'econlit': scrape_abstract_econlit,
    }

    if source == 'all':
        sources_to_try = ['doi', 'proquest', 'econlit']
    else:
        sources_to_try = [source]

    # Launch browser with persistent profile
    session = SessionManager()
    browser = session.create_browser()

    try:
        # Verify Stanford SSO access
        session.verify_stanford_access()

        # Scrape papers
        total = len(papers)
        recovered = 0
        errors = 0
        consecutive_blocks = 0
        blocked = False

        id_col = 'openalex_id' if 'openalex_id' in papers.columns else None

        for i, (idx, row) in enumerate(papers.iterrows()):
            if blocked:
                break

            paper_id = str(row[id_col]) if id_col else str(idx)
            title = str(row.get('title', ''))
            doi = str(row.get('doi', '')) if pd.notna(row.get('doi')) else ''

            print(f"\n  [{i+1}/{total}] {title[:70]}...")
            if doi:
                print(f"    DOI: {doi}")

            abstract_found = False
            last_error = None
            paper_blocked = False

            for src in sources_to_try:
                if abstract_found:
                    break

                scrape_fn = scrape_functions[src]

                # Retry with exponential backoff
                for attempt in range(MAX_RETRIES):
                    if src == 'doi':
                        result = scrape_fn(browser, doi, delay=delay)
                    else:
                        result = scrape_fn(browser, title, doi, delay=delay)

                    if result.get('error') == 'IP_BLOCKED':
                        print(f"    Block detected on {src} — skipping")
                        paper_blocked = True
                        break

                    if result['has_abstract']:
                        abstract_found = True
                        recovered += 1
                        print(f"    -> Recovered via {src} ({len(result['abstract'])} chars)")
                        break

                    last_error = result.get('error', 'Unknown')

                    if attempt < MAX_RETRIES - 1:
                        backoff = delay * (BACKOFF_FACTOR ** (attempt + 1))
                        print(f"    Retry {attempt + 2}/{MAX_RETRIES} in {backoff:.0f}s...")
                        time.sleep(backoff)

            # Track consecutive blocks
            if paper_blocked and not abstract_found:
                consecutive_blocks += 1
                if consecutive_blocks >= CONSECUTIVE_BLOCK_LIMIT:
                    print(f"\n  *** {CONSECUTIVE_BLOCK_LIMIT} consecutive blocks — halting ***")
                    print("  Try again later with --resume, or increase --delay.")
                    blocked = True
            else:
                consecutive_blocks = 0

            # Record result
            checkpoint_result = {
                'paper_id': paper_id,
                'title': title[:100],
                'doi': doi,
                'has_abstract': abstract_found,
                'abstract': result['abstract'] if abstract_found else '',
                'source': result.get('source', source),
                'error': last_error if not abstract_found else None,
                'timestamp': datetime.now().isoformat()
            }
            checkpoint.add_result(paper_id, checkpoint_result)

            if not abstract_found:
                errors += 1
                if not paper_blocked:
                    print(f"    -> Failed: {last_error}")

            # Progress report every 25 papers
            if (i + 1) % 25 == 0:
                pct = recovered / (i + 1) * 100
                print(f"\n  --- Progress: {i+1}/{total} | Recovered: {recovered} ({pct:.0f}%) | Failed: {errors} ---\n")

        # Final checkpoint save
        checkpoint.save()

        # Summary
        total_done = i + 1 if not blocked else i
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE: {policy_abbr} via {source}")
        print(f"{'='*70}")
        print(f"  Total processed: {total_done}")
        print(f"  Abstracts recovered: {recovered} ({recovered/max(total_done,1)*100:.1f}%)")
        print(f"  Failed: {errors}")
        if blocked:
            print(f"  ** Stopped early due to consecutive blocks **")
        print(f"  End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Save results and merge into complemented dataset
        save_results(policy_abbr, source, checkpoint.results)
        merge_results(policy_abbr, source, df, checkpoint.results)

    finally:
        session.close()


def save_results(policy_abbr, source, results):
    """Save scraping results to JSON."""
    results_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_{source}_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Saved results to: {results_file}")

    summary = {
        'policy': policy_abbr,
        'source': source,
        'timestamp': datetime.now().isoformat(),
        'total_processed': len(results),
        'total_recovered': sum(1 for r in results if r.get('has_abstract')),
        'total_failed': sum(1 for r in results if not r.get('has_abstract')),
    }
    summary_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_{source}_summary.json")
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved summary to: {summary_file}")


def merge_results(policy_abbr, source, df, results):
    """
    Merge recovered abstracts back into the complemented dataset.
    Updates the complement_abstracts output so downstream stages pick it up.
    """
    print(f"\n  Merging recovered abstracts into complemented dataset...")

    recovered_results = [r for r in results if r.get('has_abstract')]
    if not recovered_results:
        print("  No abstracts to merge.")
        return

    df = df.copy()
    id_col = 'openalex_id' if 'openalex_id' in df.columns else None
    merged = 0

    for result in recovered_results:
        paper_id = result['paper_id']
        abstract = result['abstract']
        src = result.get('source', source)

        if id_col:
            mask = df[id_col].astype(str) == paper_id
        else:
            try:
                mask = df.index == int(paper_id)
            except ValueError:
                continue

        if mask.any():
            df.loc[mask, 'abstract'] = abstract
            if 'abstract_source' in df.columns:
                df.loc[mask, 'abstract_source'] = f'Web_{src}'
            merged += 1

    print(f"  Merged {merged} abstracts into dataset")

    complement_file = os.path.join(
        COMPLEMENT_DIR, f"{policy_abbr}_papers_complemented_filtered.parquet"
    )
    df.to_parquet(complement_file, index=False, engine='pyarrow')
    print(f"  Updated complemented dataset: {complement_file}")

    csv_file = os.path.join(
        COMPLEMENT_DIR, f"{policy_abbr}_papers_complemented_filtered.csv"
    )
    df.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Updated complemented CSV: {csv_file}")


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Authenticated web scraping for abstract recovery (Stanford SSO)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_abstracts_web.py TCJA --source doi --max-papers 100
  python scrape_abstracts_web.py ACA --source proquest --delay 10 --resume
  python scrape_abstracts_web.py NCLB --source all

On first run, a browser window opens for Stanford SSO login (including 2FA).
The session is saved so subsequent runs skip login automatically.
        """
    )

    parser.add_argument('policy', help='Policy abbreviation (e.g., TCJA, ACA, NCLB)')
    parser.add_argument('--source', required=True,
                        choices=['proquest', 'doi', 'econlit', 'all'],
                        help='Scraping source (doi recommended)')
    parser.add_argument('--delay', type=float, default=DEFAULT_DELAY,
                        help=f'Delay between requests in seconds (default: {DEFAULT_DELAY})')
    parser.add_argument('--max-papers', type=int, default=None,
                        help='Maximum number of papers to scrape')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')

    args = parser.parse_args()

    if args.delay < 3:
        print(f"WARNING: Delay of {args.delay}s is very aggressive. Minimum recommended: 5s")

    run_scraper(
        policy_abbr=args.policy,
        source=args.source,
        delay=args.delay,
        max_papers=args.max_papers,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()
