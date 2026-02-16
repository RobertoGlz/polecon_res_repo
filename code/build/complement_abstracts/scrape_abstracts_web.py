"""
Authenticated web scraping for abstract recovery using Stanford SSO.

Issue #22: Flag papers with policy + economic outcome
Parent Issue #1: Scrape policies with OpenAlex

This script uses authenticated browser sessions (via Stanford SSO) to scrape
abstracts from paywalled sources that the API-based pipeline cannot access:
- ProQuest: Full-text database with strong abstract coverage
- EconLit/EBSCOhost: Economics-specific database
- ScienceDirect: Elsevier's platform (authenticated access)
- DOI Resolution via Stanford proxy: Resolve DOIs through institutional proxy

Prerequisites:
- Chrome browser installed
- Selenium 4.6+ (pip install selenium)
- Valid Stanford SSO credentials (manual one-time login)
- Unified dataset already generated (run unified_dataset_main.py first)

Safety Features:
- Conservative rate limiting (default 7s between requests)
- Exponential backoff on errors
- IP block detection (halts and prompts user)
- Checkpoint save/resume for interrupted sessions
- Non-headless browser by default for monitoring

Usage:
    python scrape_abstracts_web.py POLICY --source {proquest|doi|econlit|sciencedirect|all}
    python scrape_abstracts_web.py TCJA --source doi --delay 8 --max-papers 500
    python scrape_abstracts_web.py ACA --source proquest --resume
    python scrape_abstracts_web.py NCLB --source all --merge

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 16, 2026
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
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

# Unified dataset directory (input)
UNIFIED_DIR = os.path.join(SCRIPT_DIR, "..", "unified_dataset", "output")
UNIFIED_DIR = os.path.normpath(UNIFIED_DIR)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(PROFILE_DIR, exist_ok=True)

# =============================================================================
# RATE LIMITING & SAFETY DEFAULTS
# =============================================================================
DEFAULT_DELAY = 7  # Seconds between requests
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # Exponential backoff multiplier
CHECKPOINT_INTERVAL = 50  # Save checkpoint every N papers

# IP block detection phrases
BLOCK_INDICATORS = [
    'too many requests',
    'rate limit',
    'captcha',
    'blocked',
    'access denied',
    'please verify you are a human',
    'unusual traffic',
    'automated access',
    'bot detection',
    'please try again later',
]


# =============================================================================
# SESSION MANAGER
# =============================================================================
class SessionManager:
    """Manages Chrome browser with persistent profile for Stanford SSO."""

    def __init__(self, headless=False):
        self.headless = headless
        self.browser = None

    def create_browser(self):
        """Create Chrome browser with persistent profile for cookie reuse."""
        chrome_options = ChromeOptions()

        # Persistent profile for cookie/session reuse
        chrome_options.add_argument(f'--user-data-dir={PROFILE_DIR}')

        if self.headless:
            chrome_options.add_argument('--headless=new')

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.browser = webdriver.Chrome(options=chrome_options)
        self.browser.set_page_load_timeout(60)
        return self.browser

    def is_authenticated(self):
        """Check if the browser has an authenticated Stanford SSO session."""
        if not self.browser:
            return False
        try:
            # Try accessing a Stanford proxy page
            self.browser.get('https://login.stanford.edu/idp/profile/SAML2/Redirect/SSO')
            time.sleep(2)
            page_text = self.browser.page_source.lower()
            # If we see the login form, we're not authenticated
            if 'username' in page_text and 'password' in page_text:
                return False
            return True
        except Exception:
            return False

    def wait_for_manual_login(self, timeout=300):
        """
        Navigate to Stanford SSO and wait for manual login.

        Opens the Stanford login page and waits for the user to complete
        authentication manually (including 2FA if needed).
        """
        if not self.browser:
            self.create_browser()

        print("\n" + "=" * 60)
        print("STANFORD SSO LOGIN REQUIRED")
        print("=" * 60)
        print("A browser window has opened. Please:")
        print("  1. Log in with your Stanford credentials")
        print("  2. Complete 2FA if prompted")
        print("  3. Wait for the login to complete")
        print(f"  Timeout: {timeout} seconds")
        print("=" * 60)

        self.browser.get('https://login.stanford.edu/')
        time.sleep(3)

        # Wait for login to complete (URL changes away from login page)
        start_time = time.time()
        while time.time() - start_time < timeout:
            current_url = self.browser.current_url.lower()
            if 'login.stanford.edu' not in current_url:
                print("  Login detected! Continuing...")
                time.sleep(2)
                return True
            # Also check if the page shows a logged-in state
            try:
                page_text = self.browser.page_source.lower()
                if 'logout' in page_text or 'sign out' in page_text:
                    print("  Login detected! Continuing...")
                    time.sleep(2)
                    return True
            except Exception:
                pass
            time.sleep(3)

        print("  WARNING: Login timeout. Proceeding anyway...")
        return False

    def close(self):
        """Close browser (preserves profile for next session)."""
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
# IP BLOCK DETECTION
# =============================================================================
def detect_ip_block(browser):
    """
    Check if the current page indicates an IP block or rate limit.

    Returns True if block indicators are detected, halting the scraper.
    """
    try:
        page_text = browser.page_source.lower()
        for indicator in BLOCK_INDICATORS:
            if indicator in page_text:
                return True
    except Exception:
        pass
    return False


def clean_abstract_text(text):
    """Clean and normalize abstract text."""
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = ' '.join(text.split())
    text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY):?\s*', '', text)
    return text.strip()


# =============================================================================
# SCRAPING FUNCTIONS
# =============================================================================
def scrape_abstract_proquest(browser, title, doi, delay=DEFAULT_DELAY):
    """
    Search ProQuest by title and extract abstract.

    Parameters:
    -----------
    browser : webdriver.Chrome
        Authenticated browser instance
    title : str
        Paper title to search for
    doi : str
        DOI (used for verification, not search)
    delay : float
        Delay after page load

    Returns:
    --------
    dict : Result with 'abstract', 'has_abstract', 'error', 'source'
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'ProQuest'
    }

    try:
        # Navigate to ProQuest search via Stanford proxy
        search_url = 'https://www-proquest-com.stanford.idm.oclc.org/advanced'
        browser.get(search_url)
        time.sleep(delay)

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
            # Use exact title in quotes for precision
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

        # Look for result links
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

        # Extract abstract from paper page
        abstract_selectors = [
            'div.abstract p',
            'div[class*="abstract"] p',
            'div.abstractText',
            'div#abstract p',
            'section.abstract p',
            'div.Abstract p',
        ]

        for selector in abstract_selectors:
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

        result['error'] = 'Abstract element not found on page'

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

    Parameters:
    -----------
    browser : webdriver.Chrome
        Authenticated browser instance
    title : str
        Paper title to search for
    doi : str
        DOI (used for verification)
    delay : float
        Delay after page load

    Returns:
    --------
    dict : Result with 'abstract', 'has_abstract', 'error', 'source'
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'EconLit'
    }

    try:
        # EconLit via Stanford proxy (EBSCOhost)
        search_url = 'https://search-ebscohost-com.stanford.idm.oclc.org/login.aspx?direct=true&db=ecn&bquery='
        clean_title = title.replace('"', '').replace('&', ' ')[:200]
        full_url = search_url + f'TI+"{clean_title}"'
        browser.get(full_url)
        time.sleep(delay)

        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Look for result links
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
        abstract_selectors = [
            'div[class*="abstract"] p',
            'div.abstract-text',
            'p[class*="abstract"]',
            'div#abstract-body',
            'div.record-abstract p',
        ]

        for selector in abstract_selectors:
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

        result['error'] = 'Abstract element not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def scrape_abstract_sciencedirect_doi(browser, doi, delay=DEFAULT_DELAY):
    """
    Navigate to ScienceDirect paper page via DOI and extract abstract.

    Parameters:
    -----------
    browser : webdriver.Chrome
        Authenticated browser instance
    doi : str
        Digital Object Identifier
    delay : float
        Delay after page load

    Returns:
    --------
    dict : Result with 'abstract', 'has_abstract', 'error', 'source'
    """
    result = {
        'abstract': '',
        'has_abstract': False,
        'error': None,
        'source': 'ScienceDirect'
    }

    if not doi:
        result['error'] = 'No DOI provided'
        return result

    try:
        # Clean DOI
        clean_doi = doi.replace('https://doi.org/', '').replace('http://doi.org/', '') if doi.startswith('http') else doi

        # Navigate to ScienceDirect via Stanford proxy
        sd_url = f'https://www-sciencedirect-com.stanford.idm.oclc.org/science/article/pii/'
        # Try direct DOI resolution through proxy first
        proxy_url = f'https://doi-org.stanford.idm.oclc.org/{clean_doi}'
        browser.get(proxy_url)
        time.sleep(delay)

        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Extract abstract from ScienceDirect page
        abstract_selectors = [
            'div.abstract.author div p',
            'div#abstracts div.abstract p',
            'div[class*="Abstracts"] div.abstract p',
            'div.abstract-content p',
            'section#abstract p',
            'div[id*="abstract"] p',
        ]

        for selector in abstract_selectors:
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, selector)
                texts = []
                for elem in elements:
                    text = elem.text.strip()
                    if text:
                        texts.append(text)
                combined = ' '.join(texts)
                if len(combined) > 100:
                    result['abstract'] = clean_abstract_text(combined)
                    result['has_abstract'] = True
                    return result
            except Exception:
                continue

        result['error'] = 'Abstract element not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def scrape_abstract_doi_authenticated(browser, doi, delay=DEFAULT_DELAY):
    """
    Resolve DOI through Stanford proxy and scrape abstract from publisher page.

    Parameters:
    -----------
    browser : webdriver.Chrome
        Authenticated browser instance
    doi : str
        Digital Object Identifier
    delay : float
        Delay after page load

    Returns:
    --------
    dict : Result with 'abstract', 'has_abstract', 'error', 'source'
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
        clean_doi = doi.replace('https://doi.org/', '').replace('http://doi.org/', '') if doi.startswith('http') else doi
        proxy_url = f'https://doi-org.stanford.idm.oclc.org/{clean_doi}'
        browser.get(proxy_url)
        time.sleep(delay)

        if detect_ip_block(browser):
            result['error'] = 'IP_BLOCKED'
            return result

        # Generic abstract extraction from any publisher page
        abstract_selectors = [
            # Common publisher selectors
            'div[class*="abstract"] p',
            'section[class*="abstract"] p',
            'div[id*="abstract"] p',
            'div.abstract p',
            'section.abstract p',
            # Specific publishers
            'div.c-article-section__content p',  # Springer/Nature
            'div.article-section__content p',  # Wiley
            'div.abstractSection p',  # Taylor & Francis
            'div.hlFld-Abstract p',  # SAGE
            'div.abstract.author p',  # Elsevier
            'div.NLM_abstract p',  # T&F
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
                    result['abstract'] = clean_abstract_text(combined)
                    result['has_abstract'] = True
                    return result
            except (StaleElementReferenceException, NoSuchElementException):
                continue
            except Exception:
                continue

        # Fallback: look for headings containing "Abstract"
        try:
            headings = browser.find_elements(By.CSS_SELECTOR, 'h1, h2, h3, h4')
            for heading in headings:
                if 'abstract' in heading.text.lower():
                    parent = heading.find_element(By.XPATH, '..')
                    parent_text = parent.text.strip()
                    content = parent_text.replace(heading.text.strip(), '').strip()
                    if len(content) > 100:
                        result['abstract'] = clean_abstract_text(content)
                        result['has_abstract'] = True
                        return result
        except Exception:
            pass

        # Fallback: meta tags
        try:
            meta_selectors = [
                'meta[name="citation_abstract"]',
                'meta[name="description"]',
                'meta[name="DC.description"]',
            ]
            for meta_sel in meta_selectors:
                metas = browser.find_elements(By.CSS_SELECTOR, meta_sel)
                for meta in metas:
                    content = meta.get_attribute('content')
                    if content and len(content) > 100:
                        result['abstract'] = clean_abstract_text(content)
                        result['has_abstract'] = True
                        return result
        except Exception:
            pass

        result['error'] = 'Abstract element not found on page'

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


# =============================================================================
# MAIN SCRAPING PIPELINE
# =============================================================================
def load_unified_dataset(policy_abbr):
    """Load unified dataset for a policy."""
    parquet_file = os.path.join(UNIFIED_DIR, f"{policy_abbr}_unified_dataset.parquet")
    csv_file = os.path.join(UNIFIED_DIR, f"{policy_abbr}_unified_dataset.csv")

    if os.path.exists(parquet_file):
        return pd.read_parquet(parquet_file)
    elif os.path.exists(csv_file):
        return pd.read_csv(csv_file)
    else:
        print(f"ERROR: Unified dataset not found for {policy_abbr}")
        print(f"  Looked for: {parquet_file}")
        print(f"  Looked for: {csv_file}")
        return None


def get_papers_to_scrape(df, source):
    """
    Select papers needing abstract recovery, prioritized by DOI availability.

    Parameters:
    -----------
    df : pd.DataFrame
        Unified dataset
    source : str
        Scraping source ('proquest', 'doi', 'econlit', 'sciencedirect')

    Returns:
    --------
    pd.DataFrame : Papers to scrape, sorted by priority
    """
    missing_mask = (df['abstract'].isna()) | (df['abstract'] == '') | (df['abstract'] == 'None')
    papers = df[missing_mask].copy()

    if source in ('doi', 'sciencedirect'):
        # DOI-based sources need DOIs
        has_doi = papers['doi'].notna() & (papers['doi'] != '')
        papers = papers[has_doi]

    # Prioritize: articles with DOIs first, then by type
    papers['_has_doi'] = papers['doi'].notna() & (papers['doi'] != '')
    papers = papers.sort_values(by=['_has_doi'], ascending=False)
    papers = papers.drop(columns=['_has_doi'])

    return papers


def run_scraper(policy_abbr, source, delay, max_papers, resume, merge):
    """
    Main scraping function.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation
    source : str
        Source to scrape from
    delay : float
        Delay between requests
    max_papers : int or None
        Maximum papers to scrape
    resume : bool
        Whether to resume from checkpoint
    merge : bool
        Whether to merge results back into dataset
    """
    print(f"\n{'='*70}")
    print(f"WEB SCRAPING: {policy_abbr} via {source}")
    print(f"{'='*70}")
    print(f"  Delay: {delay}s | Max papers: {max_papers or 'all'} | Resume: {resume}")
    print(f"  Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load dataset
    df = load_unified_dataset(policy_abbr)
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
        # Use index or a unique identifier
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
        print("  All papers already processed. Exiting.")
        if merge:
            merge_results(policy_abbr, source, df, checkpoint.results)
        return

    # Map source to scraping function
    scrape_functions = {
        'proquest': scrape_abstract_proquest,
        'doi': scrape_abstract_doi_authenticated,
        'econlit': scrape_abstract_econlit,
        'sciencedirect': scrape_abstract_sciencedirect_doi,
    }

    if source == 'all':
        sources_to_try = ['doi', 'proquest', 'econlit', 'sciencedirect']
    else:
        sources_to_try = [source]

    # Initialize browser
    session = SessionManager(headless=False)
    browser = session.create_browser()

    try:
        # Check authentication for proxy-based sources
        needs_auth = any(s in sources_to_try for s in ['proquest', 'doi', 'econlit', 'sciencedirect'])
        if needs_auth:
            if not session.is_authenticated():
                session.wait_for_manual_login()

        # Scrape papers
        total = len(papers)
        recovered = 0
        errors = 0
        blocked = False

        id_col = 'openalex_id' if 'openalex_id' in papers.columns else None

        for i, (idx, row) in enumerate(papers.iterrows()):
            if blocked:
                break

            paper_id = str(row[id_col]) if id_col else str(idx)
            title = str(row.get('title', ''))
            doi = str(row.get('doi', '')) if pd.notna(row.get('doi')) else ''

            print(f"\n  [{i+1}/{total}] {title[:60]}...")

            abstract_found = False
            last_error = None

            for src in sources_to_try:
                if abstract_found:
                    break

                scrape_fn = scrape_functions[src]

                # Retry with exponential backoff
                for attempt in range(MAX_RETRIES):
                    if src in ('doi', 'sciencedirect'):
                        result = scrape_fn(browser, doi, delay=delay)
                    else:
                        result = scrape_fn(browser, title, doi, delay=delay)

                    if result.get('error') == 'IP_BLOCKED':
                        print("\n  *** IP BLOCK DETECTED ***")
                        print("  The website has blocked further requests.")
                        print("  Please wait a few minutes and try again with --resume.")
                        blocked = True
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

                if blocked:
                    break

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
                print(f"    -> Failed: {last_error}")

            # Progress report every 25 papers
            if (i + 1) % 25 == 0:
                print(f"\n  --- Progress: {i+1}/{total} | Recovered: {recovered} | Failed: {errors} ---\n")

        # Final checkpoint save
        checkpoint.save()

        # Summary
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE: {policy_abbr} via {source}")
        print(f"{'='*70}")
        print(f"  Total processed: {i+1 if not blocked else i}")
        print(f"  Abstracts recovered: {recovered}")
        print(f"  Failed: {errors}")
        if blocked:
            print(f"  ** Stopped early due to IP block **")
        print(f"  End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Save results
        save_results(policy_abbr, source, checkpoint.results)

        # Merge if requested
        if merge:
            merge_results(policy_abbr, source, df, checkpoint.results)

    finally:
        session.close()


def save_results(policy_abbr, source, results):
    """Save scraping results to JSON."""
    results_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_{source}_results.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"  Saved results to: {results_file}")

    # Summary file
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
    """Merge recovered abstracts back into the unified dataset."""
    print(f"\n  Merging {len(results)} results into unified dataset...")

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
                idx = int(paper_id)
                mask = df.index == idx
            except ValueError:
                continue

        if mask.any():
            df.loc[mask, 'abstract'] = abstract
            if 'abstract_source' in df.columns:
                df.loc[mask, 'abstract_source'] = f'Web_{src}'
            merged += 1

    print(f"  Merged {merged} abstracts into dataset")

    # Save enhanced dataset
    output_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_unified_dataset_enhanced.parquet")
    df.to_parquet(output_file, index=False, engine='pyarrow')
    print(f"  Saved enhanced dataset to: {output_file}")


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
  python scrape_abstracts_web.py NCLB --source all --merge
        """
    )

    parser.add_argument('policy', help='Policy abbreviation (e.g., TCJA, ACA, NCLB)')
    parser.add_argument('--source', required=True,
                        choices=['proquest', 'doi', 'econlit', 'sciencedirect', 'all'],
                        help='Scraping source')
    parser.add_argument('--delay', type=float, default=DEFAULT_DELAY,
                        help=f'Delay between requests in seconds (default: {DEFAULT_DELAY})')
    parser.add_argument('--max-papers', type=int, default=None,
                        help='Maximum number of papers to scrape')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from last checkpoint')
    parser.add_argument('--merge', action='store_true',
                        help='Merge results into unified dataset after scraping')

    args = parser.parse_args()

    # Validate delay
    if args.delay < 3:
        print(f"WARNING: Delay of {args.delay}s is very aggressive. Minimum recommended: 5s")
        print("  Continuing with specified delay...")

    run_scraper(
        policy_abbr=args.policy,
        source=args.source,
        delay=args.delay,
        max_papers=args.max_papers,
        resume=args.resume,
        merge=args.merge
    )


if __name__ == "__main__":
    main()
