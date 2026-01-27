"""
Script to complement missing abstracts using multiple fallback sources.

Issue #7: Complement missing abstracts using CrossRef API
Parent Issue #1: Scrape policies with OpenAlex

This script reads papers scraped from OpenAlex that are missing abstracts
and attempts to retrieve them from multiple sources in order:
1. CrossRef API - for papers with DOIs
2. Open Access URL scraping - for papers with open_access_url (e.g., PubMed, arXiv)
3. SSRN web scraping - for papers from SSRN Electronic Journal

Pipeline Overview:
------------------
1. Load papers from OpenAlex scrape output (Parquet/CSV)
2. Identify papers with missing abstracts
3. For papers with DOIs: Query CrossRef API
4. For papers still missing abstracts with open_access_url: Scrape from OA URL
5. For SSRN papers still missing abstracts: Scrape from SSRN website
6. Update dataset with recovered abstracts and track source
7. Save complemented dataset in Parquet/CSV formats

Key Implementation Notes:
-------------------------
- CrossRef API returns abstracts as HTML/XML; we strip tags to get plain text.
- Uses CrossRef "polite pool" (via mailto parameter) for better rate limits.
- Open Access URL scraping looks for common abstract HTML elements/classes.
- SSRN blocks simple HTTP requests, so we use Selenium with headless Chrome.
- Selenium browser is reused across SSRN requests for efficiency.
- Tracks abstract source (OpenAlex, CrossRef, OpenAccess, SSRN) in 'abstract_source' column.
- Preserves original data; only updates rows with missing abstracts.

Dependencies:
-------------
- beautifulsoup4: For HTML parsing (pip install beautifulsoup4)
- selenium: For SSRN scraping (pip install selenium)
- Chrome browser: Must be installed on the system
- chromedriver: Automatically managed by Selenium 4.6+

Author: Claude AI with modifications by Roberto Gonzalez
Date: January 14, 2026
Updated: January 27, 2026 - Added relevance filtering after abstract recovery
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import json
from datetime import datetime
import os
import sys

# Selenium imports for SSRN scraping (SSRN blocks simple requests)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# CrossRef API endpoint
CROSSREF_API = "https://api.crossref.org/works"

# User email for CrossRef polite pool - REPLACE WITH YOUR EMAIL
USER_EMAIL = "rob98@stanford.edu"

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Input: OpenAlex scraped papers
OPENALEX_OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "scrape_policies_openalex", "output")
OPENALEX_OUTPUT_DIR = os.path.normpath(OPENALEX_OUTPUT_DIR)

# Policies file for getting search terms
POLICIES_FILE = os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv")
POLICIES_FILE = os.path.normpath(POLICIES_FILE)

# Create directories if they don't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


def strip_html_tags(text):
    """
    Remove HTML/XML tags from text.

    CrossRef often returns abstracts with JATS XML tags like:
    <jats:p>Abstract text here...</jats:p>

    This function strips all tags to return plain text.

    Parameters:
    -----------
    text : str or None
        Text potentially containing HTML/XML tags

    Returns:
    --------
    str : Plain text with tags removed, or empty string if input is None
    """
    if not text:
        return ''
    # Remove XML/HTML tags
    clean = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    clean = ' '.join(clean.split())
    return clean.strip()


def load_search_terms(policy_abbr):
    """
    Load search terms for a policy from the policies CSV file.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    list : List of search terms, or empty list if not found
    """
    if not os.path.exists(POLICIES_FILE):
        print(f"  WARNING: Policies file not found: {POLICIES_FILE}")
        return []

    policies_df = pd.read_csv(POLICIES_FILE)
    policy_row = policies_df[policies_df['policy_abbreviation'] == policy_abbr]

    if len(policy_row) == 0:
        print(f"  WARNING: Policy {policy_abbr} not found in policies file")
        return []

    search_terms_str = policy_row.iloc[0]['search_terms']
    search_terms = [term.strip() for term in search_terms_str.split('|')]
    return search_terms


def filter_by_relevance(df, search_terms):
    """
    Filter papers by relevance based on search term presence in title/abstract.

    Logic:
    - If paper has title AND abstract: keep only if at least one search term
      appears in either title or abstract (case-insensitive)
    - If paper has only title (no abstract): keep the paper

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with 'title' and 'abstract' columns
    search_terms : list
        List of search terms to look for

    Returns:
    --------
    tuple: (filtered_df, stats_dict)
        - filtered_df: DataFrame with only relevant papers
        - stats_dict: Dictionary with filtering statistics
    """
    if len(df) == 0 or len(search_terms) == 0:
        return df, {'kept': len(df), 'filtered_with_abstract': 0, 'kept_no_abstract': 0}

    stats = {
        'kept': 0,
        'filtered_with_abstract': 0,
        'kept_no_abstract': 0,
        'kept_with_abstract_match': 0
    }

    def is_relevant(row):
        title = str(row.get('title', '')).lower()
        abstract = str(row.get('abstract', '')).lower()

        # Check if abstract is missing/empty
        has_abstract = abstract and abstract != 'nan' and abstract.strip() != '' and abstract != 'none'

        if not has_abstract:
            # No abstract - keep the paper
            stats['kept_no_abstract'] += 1
            return True

        # Has abstract - check for search term presence
        text = title + ' ' + abstract
        for term in search_terms:
            term_lower = term.lower()
            if term_lower in text:
                stats['kept_with_abstract_match'] += 1
                return True

        # Has abstract but no search term match - filter out
        stats['filtered_with_abstract'] += 1
        return False

    # Apply filter
    mask = df.apply(is_relevant, axis=1)
    filtered_df = df[mask].copy()
    stats['kept'] = len(filtered_df)

    return filtered_df, stats


def get_abstract_from_crossref(doi, timeout=10):
    """
    Query CrossRef API to retrieve abstract for a given DOI.

    CrossRef API Documentation: https://api.crossref.org/swagger-ui/index.html

    Parameters:
    -----------
    doi : str
        Digital Object Identifier (can be full URL or just the DOI)
    timeout : int
        Request timeout in seconds

    Returns:
    --------
    dict : Result with keys:
        - 'abstract': Retrieved abstract text (empty string if not found)
        - 'success': Boolean indicating if API call succeeded
        - 'error': Error message if any
        - 'has_abstract': Boolean indicating if abstract was found

    Notes:
    ------
    - Extracts DOI from full URL if provided (e.g., https://doi.org/10.1234/...)
    - Strips HTML/XML tags from abstract text
    - Returns empty abstract (not failure) if paper exists but has no abstract
    """
    result = {
        'abstract': '',
        'success': False,
        'error': None,
        'has_abstract': False
    }

    if not doi:
        result['error'] = 'No DOI provided'
        return result

    # Extract DOI from URL if needed (e.g., "https://doi.org/10.1234/abc" -> "10.1234/abc")
    if doi.startswith('http'):
        doi = doi.replace('https://doi.org/', '').replace('http://doi.org/', '')

    # Build API URL
    url = f"{CROSSREF_API}/{doi}"
    headers = {
        'User-Agent': f'PolEconResearch/1.0 (mailto:{USER_EMAIL})'
    }
    params = {
        'mailto': USER_EMAIL
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)

        if response.status_code == 404:
            result['success'] = True  # API worked, but DOI not found
            result['error'] = 'DOI not found in CrossRef'
            return result

        response.raise_for_status()
        data = response.json()

        # Extract abstract from response
        message = data.get('message', {})
        abstract_raw = message.get('abstract', '')

        if abstract_raw:
            result['abstract'] = strip_html_tags(abstract_raw)
            result['has_abstract'] = True

        result['success'] = True

    except requests.exceptions.Timeout:
        result['error'] = 'Request timeout'
    except requests.exceptions.RequestException as e:
        result['error'] = f'Request error: {str(e)}'
    except json.JSONDecodeError:
        result['error'] = 'Invalid JSON response'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)}'

    return result


def get_abstract_from_oa_url(oa_url, timeout=15):
    """
    Scrape abstract from an open access URL.

    Many open access repositories (PubMed Central, arXiv, university repos, etc.)
    include abstracts in their HTML pages. This function attempts to extract
    abstracts by looking for common HTML elements and class names.

    Parameters:
    -----------
    oa_url : str
        Open access URL to scrape
    timeout : int
        Request timeout in seconds

    Returns:
    --------
    dict : Result with keys:
        - 'abstract': Retrieved abstract text (empty string if not found)
        - 'success': Boolean indicating if scraping succeeded
        - 'error': Error message if any
        - 'has_abstract': Boolean indicating if abstract was found

    Notes:
    ------
    - Uses browser-like headers to avoid being blocked
    - Searches for abstract in multiple common HTML patterns:
      * Elements with id/class containing 'abstract'
      * <meta name="description"> or <meta name="citation_abstract">
      * Semantic HTML5 elements like <section> or <article>
    - Filters out short text (< 100 chars) to avoid false positives
    - Strips HTML tags and normalizes whitespace
    """
    result = {
        'abstract': '',
        'success': False,
        'error': None,
        'has_abstract': False
    }

    if not oa_url or pd.isna(oa_url) or str(oa_url).strip() == '':
        result['error'] = 'No URL provided'
        return result

    oa_url = str(oa_url).strip()

    # Use browser-like headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    try:
        response = requests.get(oa_url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        abstract_text = None

        # Strategy 1: Look for meta tags with abstract
        meta_selectors = [
            ('meta', {'name': 'citation_abstract'}),
            ('meta', {'name': 'description'}),
            ('meta', {'name': 'DC.description'}),
            ('meta', {'property': 'og:description'}),
        ]
        for tag, attrs in meta_selectors:
            meta = soup.find(tag, attrs=attrs)
            if meta and meta.get('content'):
                text = meta.get('content', '').strip()
                if len(text) > 100:  # Likely an abstract, not just a short description
                    abstract_text = text
                    break

        # Strategy 2: Look for elements with 'abstract' in id or class
        if not abstract_text:
            # Common abstract selectors used by various repositories
            selectors = [
                {'id': re.compile(r'abstract', re.I)},
                {'class_': re.compile(r'abstract', re.I)},
                {'id': 'abs'},
                {'class_': 'abstract-content'},
                {'class_': 'abstractSection'},
                {'class_': 'abstract-text'},
            ]
            for selector in selectors:
                elements = soup.find_all(**selector)
                for elem in elements:
                    text = elem.get_text(strip=True)
                    # Skip if too short (likely just a label like "Abstract")
                    if len(text) > 100:
                        abstract_text = text
                        break
                if abstract_text:
                    break

        # Strategy 3: Look for <section> or <div> with abstract in attributes
        if not abstract_text:
            for tag in ['section', 'div', 'p', 'article']:
                elements = soup.find_all(tag)
                for elem in elements:
                    # Check if 'abstract' appears in any attribute
                    attrs_str = ' '.join(str(v) for v in elem.attrs.values() if v)
                    if 'abstract' in attrs_str.lower():
                        text = elem.get_text(strip=True)
                        if len(text) > 100:
                            abstract_text = text
                            break
                if abstract_text:
                    break

        if abstract_text:
            # Clean up the text
            abstract_text = strip_html_tags(abstract_text)
            abstract_text = ' '.join(abstract_text.split())
            # Remove common prefixes
            abstract_text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY):?\s*', '', abstract_text)
            result['abstract'] = abstract_text.strip()
            result['has_abstract'] = len(result['abstract']) > 50

        result['success'] = True

    except requests.exceptions.Timeout:
        result['error'] = 'Request timeout'
    except requests.exceptions.RequestException as e:
        result['error'] = f'Request error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def extract_ssrn_id(url):
    """
    Extract SSRN abstract ID from a URL.

    SSRN URLs can have several formats:
    - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1234567
    - https://ssrn.com/abstract=1234567
    - https://dx.doi.org/10.2139/ssrn.1234567

    Parameters:
    -----------
    url : str
        URL that may contain an SSRN abstract ID

    Returns:
    --------
    str or None : The SSRN abstract ID if found, None otherwise
    """
    if not url:
        return None

    url = str(url)

    # Pattern 1: abstract_id=XXXXXXX
    match = re.search(r'abstract_id=(\d+)', url)
    if match:
        return match.group(1)

    # Pattern 2: abstract=XXXXXXX
    match = re.search(r'abstract=(\d+)', url)
    if match:
        return match.group(1)

    # Pattern 3: ssrn.XXXXXXX (from DOI)
    match = re.search(r'ssrn\.(\d+)', url)
    if match:
        return match.group(1)

    return None


def create_selenium_browser():
    """
    Create a headless Chrome browser for SSRN scraping.

    SSRN blocks simple HTTP requests, so we use Selenium to render
    pages with a real browser. This bypasses bot detection.

    Returns:
    --------
    webdriver.Chrome : Configured Chrome browser instance

    Notes:
    ------
    - Runs in headless mode (no visible window)
    - Disables images and CSS for faster loading
    - Uses common browser settings to appear more human-like
    """
    chrome_options = ChromeOptions()

    # Run headless (no visible browser window)
    chrome_options.add_argument('--headless=new')

    # Common settings to avoid detection
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')

    # Set a realistic user agent
    chrome_options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    # Disable automation flags that might trigger detection
    chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Disable images and CSS for faster loading
    prefs = {
        'profile.managed_default_content_settings.images': 2,
    }
    chrome_options.add_experimental_option('prefs', prefs)

    # Create browser
    browser = webdriver.Chrome(options=chrome_options)

    # Set page load timeout
    browser.set_page_load_timeout(30)

    return browser


def get_abstract_from_ssrn(ssrn_id, browser, timeout=15):
    """
    Scrape abstract from SSRN paper page using Selenium.

    SSRN blocks simple HTTP requests, so we use Selenium with a real
    browser to bypass their bot detection.

    Parameters:
    -----------
    ssrn_id : str
        SSRN abstract ID (numeric string)
    browser : webdriver.Chrome
        Selenium browser instance (reused across calls for efficiency)
    timeout : int
        Maximum time to wait for page elements in seconds

    Returns:
    --------
    dict : Result with keys:
        - 'abstract': Retrieved abstract text (empty string if not found)
        - 'success': Boolean indicating if scraping succeeded
        - 'error': Error message if any
        - 'has_abstract': Boolean indicating if abstract was found

    Notes:
    ------
    - Waits for abstract element to load before extraction
    - Tries multiple CSS selectors to find abstract text
    - Cleans up extracted text (removes "Abstract:" prefix, normalizes whitespace)
    """
    result = {
        'abstract': '',
        'success': False,
        'error': None,
        'has_abstract': False
    }

    if not ssrn_id:
        result['error'] = 'No SSRN ID provided'
        return result

    # Build SSRN URL
    url = f"https://papers.ssrn.com/sol3/papers.cfm?abstract_id={ssrn_id}"

    try:
        # Navigate to page
        browser.get(url)

        # Wait for page to load - look for abstract section
        wait = WebDriverWait(browser, timeout)

        abstract_text = None

        # Try different selectors to find abstract
        selectors = [
            'div.abstract-text',
            'section.abstract',
            'div.abstract',
            '[class*="abstract"]'
        ]

        for selector in selectors:
            try:
                # Wait for element to be present
                element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                text = element.text.strip()

                # Skip if too short (likely just a label)
                if len(text) > 50:
                    abstract_text = text
                    break
            except TimeoutException:
                continue

        if abstract_text:
            # Clean up the text
            abstract_text = ' '.join(abstract_text.split())
            # Remove common prefixes like "Abstract" or "Abstract:"
            abstract_text = re.sub(r'^Abstract:?\s*', '', abstract_text, flags=re.I)
            result['abstract'] = abstract_text.strip()
            result['has_abstract'] = True

        result['success'] = True

    except TimeoutException:
        result['error'] = 'Page load timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'

    return result


def load_openalex_papers(policy_abbr):
    """
    Load papers scraped from OpenAlex for a given policy.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    pd.DataFrame : Papers dataframe, or None if file not found
    """
    # Try Parquet first (more efficient), then CSV
    parquet_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet")
    csv_file = os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex.csv")

    if os.path.exists(parquet_file):
        print(f"Loading papers from: {parquet_file}")
        return pd.read_parquet(parquet_file)
    elif os.path.exists(csv_file):
        print(f"Loading papers from: {csv_file}")
        return pd.read_csv(csv_file)
    else:
        print(f"ERROR: No papers file found for {policy_abbr}")
        print(f"  Looked for: {parquet_file}")
        print(f"  Looked for: {csv_file}")
        return None


def complement_abstracts(df, delay=0.1, oa_delay=0.5, ssrn_delay=1.0):
    """
    Complement missing abstracts using multiple fallback sources.

    This function uses a tiered approach:
    1. First, try CrossRef API for all papers with DOIs
    2. Then, try scraping from Open Access URLs for papers still missing abstracts
    3. Finally, for SSRN papers still missing abstracts, scrape from SSRN website

    Parameters:
    -----------
    df : pd.DataFrame
        Papers dataframe with 'abstract', 'doi', 'open_access_url', and 'source_name' columns
    delay : float
        Delay between CrossRef API requests in seconds
    oa_delay : float
        Delay between Open Access URL requests in seconds
    ssrn_delay : float
        Delay between SSRN scraping requests in seconds (longer to be polite)

    Returns:
    --------
    pd.DataFrame : Updated dataframe with complemented abstracts
    dict : Statistics about the complementation process
    """
    # Make a copy to avoid modifying original
    df = df.copy()

    # Add abstract_source column if not present
    if 'abstract_source' not in df.columns:
        # Mark existing abstracts as from OpenAlex
        df['abstract_source'] = df['abstract'].apply(
            lambda x: 'OpenAlex' if pd.notna(x) and str(x).strip() != '' else ''
        )

    # Identify papers needing abstracts
    missing_mask = (df['abstract'].isna()) | (df['abstract'] == '')
    has_doi_mask = df['doi'].notna() & (df['doi'] != '')
    to_fetch_crossref = df[missing_mask & has_doi_mask]

    print(f"\nAbstract Complementation Summary:")
    print(f"  Total papers: {len(df)}")
    print(f"  Papers with abstracts (OpenAlex): {(~missing_mask).sum()}")
    print(f"  Papers missing abstracts: {missing_mask.sum()}")
    print(f"  Papers missing abstracts with DOI: {len(to_fetch_crossref)}")
    print(f"  Papers missing abstracts without DOI: {(missing_mask & ~has_doi_mask).sum()}")

    # Track statistics
    stats = {
        'crossref_fetched': 0,
        'crossref_recovered': 0,
        'crossref_failed': 0,
        'crossref_not_found': 0,
        'crossref_no_abstract': 0,
        'oa_url_fetched': 0,
        'oa_url_recovered': 0,
        'oa_url_failed': 0,
        'oa_url_no_abstract': 0,
        'ssrn_fetched': 0,
        'ssrn_recovered': 0,
        'ssrn_failed': 0,
        'ssrn_no_abstract': 0
    }

    # Store raw responses for debugging
    crossref_responses = []
    oa_url_responses = []
    ssrn_responses = []

    # =========================================================================
    # STEP 1: Try CrossRef API for all papers with DOIs
    # =========================================================================
    if len(to_fetch_crossref) > 0:
        print(f"\n[Step 1/3] Fetching abstracts from CrossRef for {len(to_fetch_crossref)} papers...")

        for idx, (df_idx, row) in enumerate(to_fetch_crossref.iterrows()):
            doi = row['doi']
            title = row.get('title', 'Unknown')[:50]

            if idx % 50 == 0 and idx > 0:
                print(f"  Progress: {idx}/{len(to_fetch_crossref)} ({stats['crossref_recovered']} recovered)")

            result = get_abstract_from_crossref(doi)
            stats['crossref_fetched'] += 1

            # Store raw response for debugging
            crossref_responses.append({
                'doi': doi,
                'title': title,
                'success': result['success'],
                'has_abstract': result['has_abstract'],
                'error': result['error'],
                'abstract_preview': result['abstract'][:100] if result['abstract'] else None
            })

            if result['success'] and result['has_abstract']:
                # Update the dataframe
                df.at[df_idx, 'abstract'] = result['abstract']
                df.at[df_idx, 'abstract_source'] = 'CrossRef'
                stats['crossref_recovered'] += 1
            elif result['success'] and not result['has_abstract']:
                stats['crossref_no_abstract'] += 1
            elif 'not found' in str(result.get('error', '')).lower():
                stats['crossref_not_found'] += 1
            else:
                stats['crossref_failed'] += 1

            # Be polite to the API
            time.sleep(delay)

        print(f"  CrossRef completed: {stats['crossref_fetched']} fetched, {stats['crossref_recovered']} recovered")

    # Save CrossRef responses
    crossref_file = os.path.join(TMP_DIR, "crossref_responses.json")
    with open(crossref_file, 'w') as f:
        json.dump(crossref_responses, f, indent=2)
    print(f"  Saved CrossRef responses to: {crossref_file}")

    # =========================================================================
    # STEP 2: Try Open Access URL scraping for papers still missing abstracts
    # =========================================================================
    # Re-identify papers still missing abstracts
    still_missing_mask = (df['abstract'].isna()) | (df['abstract'] == '')
    # Identify papers with open access URLs
    has_oa_url_mask = df['open_access_url'].notna() & (df['open_access_url'] != '')
    to_fetch_oa = df[still_missing_mask & has_oa_url_mask]

    if len(to_fetch_oa) > 0:
        print(f"\n[Step 2/3] Scraping abstracts from Open Access URLs for {len(to_fetch_oa)} papers...")

        for idx, (df_idx, row) in enumerate(to_fetch_oa.iterrows()):
            oa_url = row['open_access_url']
            title = row.get('title', 'Unknown')[:50]

            if idx % 50 == 0 and idx > 0:
                print(f"  Progress: {idx}/{len(to_fetch_oa)} ({stats['oa_url_recovered']} recovered)")

            result = get_abstract_from_oa_url(oa_url)
            stats['oa_url_fetched'] += 1

            # Store raw response for debugging
            oa_url_responses.append({
                'open_access_url': oa_url,
                'title': title,
                'success': result['success'],
                'has_abstract': result['has_abstract'],
                'error': result['error'],
                'abstract_preview': result['abstract'][:100] if result['abstract'] else None
            })

            if result['success'] and result['has_abstract']:
                # Update the dataframe
                df.at[df_idx, 'abstract'] = result['abstract']
                df.at[df_idx, 'abstract_source'] = 'OpenAccess'
                stats['oa_url_recovered'] += 1
            elif result['success'] and not result['has_abstract']:
                stats['oa_url_no_abstract'] += 1
            else:
                stats['oa_url_failed'] += 1

            # Be polite to the servers
            time.sleep(oa_delay)

        print(f"  Open Access URL completed: {stats['oa_url_fetched']} fetched, {stats['oa_url_recovered']} recovered")

    # Save Open Access URL responses
    oa_url_file = os.path.join(TMP_DIR, "oa_url_responses.json")
    with open(oa_url_file, 'w') as f:
        json.dump(oa_url_responses, f, indent=2)
    print(f"  Saved Open Access URL responses to: {oa_url_file}")

    # =========================================================================
    # STEP 3: Try SSRN scraping for SSRN papers still missing abstracts
    # =========================================================================
    # Re-identify papers still missing abstracts (after OA URL step)
    still_missing_mask = (df['abstract'].isna()) | (df['abstract'] == '')
    # Identify SSRN papers (by source name or DOI pattern)
    is_ssrn = (
        (df['source_name'].str.contains('SSRN', case=False, na=False)) |
        (df['doi'].str.contains('ssrn', case=False, na=False))
    )
    to_fetch_ssrn = df[still_missing_mask & is_ssrn]

    if len(to_fetch_ssrn) > 0:
        print(f"\n[Step 3/3] Scraping abstracts from SSRN for {len(to_fetch_ssrn)} papers...")
        print("  Initializing Selenium browser (headless Chrome)...")

        # Create Selenium browser for SSRN scraping
        browser = None
        try:
            browser = create_selenium_browser()
            print("  Browser initialized successfully")

            for idx, (df_idx, row) in enumerate(to_fetch_ssrn.iterrows()):
                doi = row.get('doi', '')
                url = row.get('url', '')
                title = row.get('title', 'Unknown')[:50]

                if idx % 20 == 0 and idx > 0:
                    print(f"  Progress: {idx}/{len(to_fetch_ssrn)} ({stats['ssrn_recovered']} recovered)")

                # Try to extract SSRN ID from DOI or URL
                ssrn_id = extract_ssrn_id(doi) or extract_ssrn_id(url)

                if not ssrn_id:
                    ssrn_responses.append({
                        'doi': doi,
                        'title': title,
                        'ssrn_id': None,
                        'success': False,
                        'error': 'Could not extract SSRN ID'
                    })
                    stats['ssrn_failed'] += 1
                    continue

                result = get_abstract_from_ssrn(ssrn_id, browser)
                stats['ssrn_fetched'] += 1

                # Store raw response for debugging
                ssrn_responses.append({
                    'doi': doi,
                    'title': title,
                    'ssrn_id': ssrn_id,
                    'success': result['success'],
                    'has_abstract': result['has_abstract'],
                    'error': result['error'],
                    'abstract_preview': result['abstract'][:100] if result['abstract'] else None
                })

                if result['success'] and result['has_abstract']:
                    # Update the dataframe
                    df.at[df_idx, 'abstract'] = result['abstract']
                    df.at[df_idx, 'abstract_source'] = 'SSRN'
                    stats['ssrn_recovered'] += 1
                elif result['success'] and not result['has_abstract']:
                    stats['ssrn_no_abstract'] += 1
                else:
                    stats['ssrn_failed'] += 1

                # Be polite - SSRN may rate limit aggressive scraping
                time.sleep(ssrn_delay)

            print(f"  SSRN completed: {stats['ssrn_fetched']} fetched, {stats['ssrn_recovered']} recovered")

        except Exception as e:
            print(f"  ERROR initializing Selenium browser: {e}")
            print("  Skipping SSRN scraping")

        finally:
            # Always close the browser
            if browser:
                browser.quit()
                print("  Browser closed")

    # Save SSRN responses
    ssrn_file = os.path.join(TMP_DIR, "ssrn_responses.json")
    with open(ssrn_file, 'w') as f:
        json.dump(ssrn_responses, f, indent=2)
    print(f"  Saved SSRN responses to: {ssrn_file}")

    # Calculate total recovered
    stats['total_recovered'] = stats['crossref_recovered'] + stats['oa_url_recovered'] + stats['ssrn_recovered']

    return df, stats


def process_policy(policy_abbr):
    """
    Process a single policy: load papers, complement abstracts, apply relevance filter, save results.

    Parameters:
    -----------
    policy_abbr : str
        Policy abbreviation (e.g., "TCJA")

    Returns:
    --------
    dict : Summary statistics
    """
    print(f"\n{'='*80}")
    print(f"Processing: {policy_abbr}")
    print(f"{'='*80}")

    # Load papers
    df = load_openalex_papers(policy_abbr)
    if df is None:
        return None

    print(f"Loaded {len(df)} papers")

    # Load search terms for relevance filtering
    search_terms = load_search_terms(policy_abbr)
    print(f"Loaded {len(search_terms)} search terms for relevance filtering")

    # Count initial abstracts
    initial_with_abstract = ((df['abstract'].notna()) & (df['abstract'] != '')).sum()

    # Complement abstracts
    df_complemented, stats = complement_abstracts(df)

    # Count abstracts after complementing
    after_complement_with_abstract = ((df_complemented['abstract'].notna()) &
                                       (df_complemented['abstract'] != '')).sum()

    # Summary of complementation
    print(f"\n  COMPLEMENTATION RESULTS for {policy_abbr}:")
    print(f"    Initial papers with abstracts: {initial_with_abstract}")
    print(f"    Abstracts recovered from CrossRef: {stats['crossref_recovered']}")
    print(f"    Abstracts recovered from Open Access URLs: {stats['oa_url_recovered']}")
    print(f"    Abstracts recovered from SSRN: {stats['ssrn_recovered']}")
    print(f"    Total recovered: {stats['total_recovered']}")
    print(f"    Papers with abstracts after complementing: {after_complement_with_abstract}")
    print(f"    Papers still missing abstracts: {len(df_complemented) - after_complement_with_abstract}")

    # Abstract source breakdown
    if 'abstract_source' in df_complemented.columns:
        source_counts = df_complemented['abstract_source'].value_counts()
        print(f"\n    Abstract sources:")
        for source, count in source_counts.items():
            if source:
                print(f"      {source}: {count}")

    # =========================================================================
    # APPLY RELEVANCE FILTERING
    # =========================================================================
    # Now that we have recovered abstracts, re-apply relevance filter:
    # - Papers with abstract: keep only if search term in title/abstract
    # - Papers without abstract: keep (we couldn't verify relevance)
    print(f"\n  Applying relevance filter (search terms in title/abstract)...")
    pre_filter_count = len(df_complemented)
    df_filtered, filter_stats = filter_by_relevance(df_complemented, search_terms)

    print(f"    Before filter: {pre_filter_count}")
    print(f"    Papers with abstract but no search term match (filtered out): {filter_stats['filtered_with_abstract']}")
    print(f"    Papers with abstract and search term match (kept): {filter_stats['kept_with_abstract_match']}")
    print(f"    Papers without abstract (kept): {filter_stats['kept_no_abstract']}")
    print(f"    After filter: {len(df_filtered)}")

    # Final counts
    final_with_abstract = ((df_filtered['abstract'].notna()) & (df_filtered['abstract'] != '')).sum()
    final_without_abstract = len(df_filtered) - final_with_abstract

    print(f"\n  FINAL RESULTS for {policy_abbr}:")
    print(f"    Total papers after filtering: {len(df_filtered)}")
    print(f"    Papers with abstracts: {final_with_abstract}")
    print(f"    Papers without abstracts (kept for later recovery): {final_without_abstract}")

    # Save outputs
    # Save complemented (before filter) for reference
    parquet_file_complemented = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_complemented.parquet")
    df_complemented.to_parquet(parquet_file_complemented, index=False, engine='pyarrow')
    print(f"\n  Saved complemented (before filter): {parquet_file_complemented}")

    # Save filtered (final output)
    parquet_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_complemented_filtered.parquet")
    df_filtered.to_parquet(parquet_file, index=False, engine='pyarrow')
    print(f"  Saved filtered Parquet: {parquet_file}")

    csv_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_complemented_filtered.csv")
    df_filtered.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"  Saved filtered CSV: {csv_file}")

    # Save metadata
    metadata = {
        'policy_abbreviation': policy_abbr,
        'process_date': datetime.now().isoformat(),
        'input_file': os.path.join(OPENALEX_OUTPUT_DIR, f"{policy_abbr}_papers_openalex.parquet"),
        'search_terms': search_terms,
        'initial_papers': len(df),
        'initial_with_abstract': int(initial_with_abstract),
        'after_complement_with_abstract': int(after_complement_with_abstract),
        'abstracts_from_openalex': int((df_complemented['abstract_source'] == 'OpenAlex').sum()),
        'abstracts_from_crossref': int((df_complemented['abstract_source'] == 'CrossRef').sum()),
        'abstracts_from_openaccess': int((df_complemented['abstract_source'] == 'OpenAccess').sum()),
        'abstracts_from_ssrn': int((df_complemented['abstract_source'] == 'SSRN').sum()),
        'still_missing_after_complement': int(len(df_complemented) - after_complement_with_abstract),
        'relevance_filter': {
            'before_filter': pre_filter_count,
            'filtered_with_abstract': filter_stats['filtered_with_abstract'],
            'kept_with_abstract_match': filter_stats['kept_with_abstract_match'],
            'kept_no_abstract': filter_stats['kept_no_abstract'],
            'after_filter': len(df_filtered)
        },
        'final_papers': len(df_filtered),
        'final_with_abstract': int(final_with_abstract),
        'final_without_abstract': int(final_without_abstract),
        'crossref_stats': {k: v for k, v in stats.items() if k.startswith('crossref_')},
        'oa_url_stats': {k: v for k, v in stats.items() if k.startswith('oa_url_')},
        'ssrn_stats': {k: v for k, v in stats.items() if k.startswith('ssrn_')}
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_complement_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metadata: {metadata_file}")

    return metadata


def main():
    """
    Main execution function.

    Processes all policy abbreviations provided as command line arguments,
    or all policies from the policies.csv file if none provided.
    """
    print("=" * 80)
    print("COMPLEMENT MISSING ABSTRACTS AND APPLY RELEVANCE FILTER")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get policy abbreviations from command line or load from policies file
    if len(sys.argv) > 1:
        policy_abbrs = sys.argv[1:]
    else:
        # Load all policies from file
        if os.path.exists(POLICIES_FILE):
            policies_df = pd.read_csv(POLICIES_FILE)
            policy_abbrs = policies_df['policy_abbreviation'].tolist()
        else:
            policy_abbrs = ['TCJA', 'ACA', 'NCLB']  # Default

    print(f"\nPolicies to process: {policy_abbrs}")

    # Process each policy
    all_results = []
    for policy_abbr in policy_abbrs:
        try:
            result = process_policy(policy_abbr)
            if result:
                all_results.append(result)
        except Exception as e:
            print(f"\nERROR processing {policy_abbr}: {e}")
            import traceback
            traceback.print_exc()

    # Overall summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY")
    print(f"{'='*80}")

    for result in all_results:
        abbr = result['policy_abbreviation']
        crossref = result['abstracts_from_crossref']
        openaccess = result['abstracts_from_openaccess']
        ssrn = result['abstracts_from_ssrn']
        initial = result['initial_papers']
        final = result['final_papers']
        final_with_abs = result['final_with_abstract']
        filtered = result['relevance_filter']['filtered_with_abstract']
        print(f"  {abbr}:")
        print(f"    Recovered: CrossRef={crossref}, OpenAccess={openaccess}, SSRN={ssrn}")
        print(f"    Filtered out (no search term match): {filtered}")
        print(f"    Final: {final} papers ({final_with_abs} with abstracts)")

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    print(f"\nAll outputs saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
