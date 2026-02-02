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
import tempfile
from urllib.parse import urlparse

# PDF extraction - optional dependency
try:
    import pdfplumber
    PDF_EXTRACTION_AVAILABLE = True
except ImportError:
    PDF_EXTRACTION_AVAILABLE = False
    print("WARNING: pdfplumber not installed. PDF abstract extraction disabled.")
    print("         Install with: pip install pdfplumber")

# Failure reason constants for detailed logging
FAILURE_REASONS = {
    # CrossRef failures
    'crossref_no_abstract_in_db': 'DOI exists in CrossRef but no abstract available',
    'crossref_doi_not_found': 'DOI not found in CrossRef database',
    'crossref_api_error': 'CrossRef API request failed',
    'crossref_timeout': 'CrossRef API request timed out',

    # OA URL failures
    'oa_url_is_pdf': 'URL points to PDF file (attempted extraction)',
    'oa_url_no_matching_selector': 'HTML parsed successfully but no abstract element found',
    'oa_url_abstract_too_short': 'Found abstract element but content too short (<100 chars)',
    'oa_url_blocked_403': 'Access denied (HTTP 403)',
    'oa_url_blocked_401': 'Authentication required (HTTP 401)',
    'oa_url_not_found_404': 'Page not found (HTTP 404)',
    'oa_url_redirect_to_login': 'Page redirected to login/paywall',
    'oa_url_connection_timeout': 'Connection timed out',
    'oa_url_connection_error': 'Connection error (network issue)',
    'oa_url_javascript_required': 'Page requires JavaScript rendering',
    'oa_url_invalid_url': 'Invalid or empty URL',
    'oa_url_server_error': 'Server error (HTTP 5xx)',

    # PDF extraction failures
    'pdf_extraction_failed': 'PDF downloaded but text extraction failed',
    'pdf_no_abstract_found': 'PDF text extracted but no abstract section found',
    'pdf_download_failed': 'Failed to download PDF file',

    # SSRN failures
    'ssrn_no_id': 'Could not extract SSRN ID from URL/DOI',
    'ssrn_no_abstract_element': 'SSRN page loaded but no abstract element found',
    'ssrn_timeout': 'SSRN page did not load in time',
    'ssrn_browser_error': 'Selenium browser error',
}

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
        - 'failure_reason': Standardized failure reason code (if failed)

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
        'has_abstract': False,
        'failure_reason': None
    }

    if not doi:
        result['error'] = 'No DOI provided'
        result['failure_reason'] = 'crossref_api_error'
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
            result['failure_reason'] = 'crossref_doi_not_found'
            return result

        response.raise_for_status()
        data = response.json()

        # Extract abstract from response
        message = data.get('message', {})
        abstract_raw = message.get('abstract', '')

        if abstract_raw:
            result['abstract'] = strip_html_tags(abstract_raw)
            result['has_abstract'] = True
        else:
            result['failure_reason'] = 'crossref_no_abstract_in_db'

        result['success'] = True

    except requests.exceptions.Timeout:
        result['error'] = 'Request timeout'
        result['failure_reason'] = 'crossref_timeout'
    except requests.exceptions.RequestException as e:
        result['error'] = f'Request error: {str(e)}'
        result['failure_reason'] = 'crossref_api_error'
    except json.JSONDecodeError:
        result['error'] = 'Invalid JSON response'
        result['failure_reason'] = 'crossref_api_error'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)}'
        result['failure_reason'] = 'crossref_api_error'

    return result


def is_pdf_url(url):
    """Check if URL points to a PDF file."""
    url_lower = url.lower()
    return (
        url_lower.endswith('.pdf') or
        '/pdf/' in url_lower or
        'application/pdf' in url_lower or
        url_lower.endswith('/pdf')
    )


def requires_javascript(html_content):
    """
    Detect if page content is JavaScript-rendered.

    Checks for indicators that the page requires JavaScript to display content.
    """
    indicators = [
        'React.createElement',
        '__NEXT_DATA__',
        'ng-app=',
        'data-reactroot',
        '<noscript>Please enable JavaScript',
        'Loading...',
        'This page requires JavaScript',
        'JavaScript is required',
        'Please enable JavaScript',
        'window.__INITIAL_STATE__',
        'Vue.js',
    ]
    return any(ind in html_content for ind in indicators)


def detect_login_redirect(soup, original_url):
    """
    Detect if page has been redirected to a login/paywall page.

    Returns True if the page appears to be a login/access page.
    """
    page_text = soup.get_text().lower()
    login_indicators = [
        'sign in',
        'log in',
        'login',
        'subscribe',
        'purchase access',
        'buy access',
        'institutional access',
        'access denied',
        'you do not have access',
        'authentication required',
    ]
    # Check if multiple login indicators are present
    indicator_count = sum(1 for ind in login_indicators if ind in page_text)
    return indicator_count >= 2


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
        - 'failure_reason': Standardized failure reason code (if failed)
        - 'html_snippet': Sample HTML near expected abstract location (for debugging)
        - 'http_status': HTTP status code

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
        'has_abstract': False,
        'failure_reason': None,
        'html_snippet': None,
        'http_status': None,
        'is_pdf': False
    }

    if not oa_url or pd.isna(oa_url) or str(oa_url).strip() == '':
        result['error'] = 'No URL provided'
        result['failure_reason'] = 'oa_url_invalid_url'
        return result

    oa_url = str(oa_url).strip()

    # Check if URL is a PDF
    if is_pdf_url(oa_url):
        result['is_pdf'] = True
        result['failure_reason'] = 'oa_url_is_pdf'
        result['success'] = True  # Not a failure, just needs different handling
        return result

    # Use browser-like headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    try:
        response = requests.get(oa_url, headers=headers, timeout=timeout, allow_redirects=True)
        result['http_status'] = response.status_code

        # Check for specific HTTP errors
        if response.status_code == 403:
            result['error'] = 'Access denied (HTTP 403)'
            result['failure_reason'] = 'oa_url_blocked_403'
            return result
        elif response.status_code == 401:
            result['error'] = 'Authentication required (HTTP 401)'
            result['failure_reason'] = 'oa_url_blocked_401'
            return result
        elif response.status_code == 404:
            result['error'] = 'Page not found (HTTP 404)'
            result['failure_reason'] = 'oa_url_not_found_404'
            return result
        elif response.status_code >= 500:
            result['error'] = f'Server error (HTTP {response.status_code})'
            result['failure_reason'] = 'oa_url_server_error'
            return result

        response.raise_for_status()

        # Check if response is actually a PDF (content-type check)
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/pdf' in content_type:
            result['is_pdf'] = True
            result['failure_reason'] = 'oa_url_is_pdf'
            result['success'] = True
            return result

        # Check for JavaScript-rendered content
        if requires_javascript(response.text):
            result['error'] = 'Page requires JavaScript rendering'
            result['failure_reason'] = 'oa_url_javascript_required'
            result['success'] = True  # Request succeeded, but can't parse
            # Save snippet for debugging
            result['html_snippet'] = response.text[:2000]
            return result

        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check for login/paywall redirect
        if detect_login_redirect(soup, oa_url):
            result['error'] = 'Page redirected to login/paywall'
            result['failure_reason'] = 'oa_url_redirect_to_login'
            result['success'] = True
            return result

        abstract_text = None
        found_candidate = False  # Track if we found any candidate element

        # Strategy 1: Look for meta tags with abstract
        meta_selectors = [
            ('meta', {'name': 'citation_abstract'}),
            ('meta', {'name': 'description'}),
            ('meta', {'name': 'DC.description'}),
            ('meta', {'property': 'og:description'}),
            ('meta', {'name': 'dcterms.abstract'}),
        ]
        for tag, attrs in meta_selectors:
            meta = soup.find(tag, attrs=attrs)
            if meta and meta.get('content'):
                text = meta.get('content', '').strip()
                if len(text) > 100:  # Likely an abstract, not just a short description
                    abstract_text = text
                    break
                elif len(text) > 0:
                    found_candidate = True

        # Strategy 2: Look for elements with 'abstract' in id or class
        # EXPANDED SELECTORS for common publishers
        if not abstract_text:
            selectors = [
                # Generic abstract selectors
                {'id': re.compile(r'abstract', re.I)},
                {'class_': re.compile(r'abstract', re.I)},
                {'id': 'abs'},
                {'class_': 'abstract-content'},
                {'class_': 'abstractSection'},
                {'class_': 'abstract-text'},

                # PubMed Central patterns
                {'class_': 'tsec sec'},
                {'id': 'abstract-1'},
                {'class_': 'jig-ncbiinpagenav'},

                # arXiv patterns
                {'class_': 'abstract mathjax'},

                # Springer/Nature patterns
                {'class_': 'c-article-section__content'},
                {'data-title': 'Abstract'},
                {'id': 'Abs1-content'},
                {'class_': 'c-article-body'},

                # Wiley patterns
                {'class_': 'article-section__content'},
                {'class_': 'article-section__abstract'},

                # Taylor & Francis patterns
                {'class_': 'NLM_abstract'},
                {'class_': 'abstractInFull'},

                # SAGE patterns
                {'class_': 'hlFld-Abstract'},

                # Elsevier/ScienceDirect patterns
                {'class_': 'abstract author'},
                {'id': 'abstracts'},

                # Oxford Academic patterns
                {'class_': 'abstract-title'},

                # Cambridge patterns
                {'class_': 'abstract'},

                # Generic patterns
                {'class_': 'summary'},
                {'class_': 'article-abstract'},
                {'role': 'doc-abstract'},
                {'itemprop': 'description'},

                # Angular/React patterns (ng-star-inserted, simple-view-element, etc.)
                {'class_': re.compile(r'simple-view-element', re.I)},
                {'class_': re.compile(r'ng-star-inserted', re.I)},
                {'class_': re.compile(r'view-element.*abstract', re.I)},
            ]
            for selector in selectors:
                try:
                    elements = soup.find_all(**selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        # Skip if too short (likely just a label like "Abstract")
                        if len(text) > 100:
                            abstract_text = text
                            break
                        elif len(text) > 20:
                            found_candidate = True
                except Exception:
                    continue
                if abstract_text:
                    break

        # Strategy 3: Look for <section> or <div> with abstract in attributes
        if not abstract_text:
            for tag in ['section', 'div', 'p', 'article', 'blockquote']:
                elements = soup.find_all(tag)
                for elem in elements:
                    # Check if 'abstract' appears in any attribute
                    attrs_str = ' '.join(str(v) for v in elem.attrs.values() if v)
                    if 'abstract' in attrs_str.lower():
                        text = elem.get_text(strip=True)
                        if len(text) > 100:
                            abstract_text = text
                            break
                        elif len(text) > 20:
                            found_candidate = True
                if abstract_text:
                    break

        # Strategy 4: Look for heading "Abstract" followed by content
        if not abstract_text:
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'strong', 'b']):
                heading_text = heading.get_text(strip=True).lower()
                if heading_text in ['abstract', 'summary', 'abstract:']:
                    # Get the next sibling or parent's next content
                    next_elem = heading.find_next_sibling()
                    if next_elem:
                        text = next_elem.get_text(strip=True)
                        if len(text) > 100:
                            abstract_text = text
                            break
                    # Try parent's next sibling
                    parent = heading.parent
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            text = next_elem.get_text(strip=True)
                            if len(text) > 100:
                                abstract_text = text
                                break

        if abstract_text:
            # Clean up the text
            abstract_text = strip_html_tags(abstract_text)
            abstract_text = ' '.join(abstract_text.split())
            # Remove common prefixes
            abstract_text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY):?\s*', '', abstract_text)
            result['abstract'] = abstract_text.strip()
            result['has_abstract'] = len(result['abstract']) > 50

            if not result['has_abstract']:
                result['failure_reason'] = 'oa_url_abstract_too_short'
        else:
            # No abstract found - determine why
            if found_candidate:
                result['failure_reason'] = 'oa_url_abstract_too_short'
            else:
                result['failure_reason'] = 'oa_url_no_matching_selector'

            # Save HTML snippet around potential abstract location for debugging
            # Look for any element that might contain abstract
            body = soup.find('body')
            if body:
                body_text = str(body)[:5000]
                # Find position of 'abstract' keyword if present
                abstract_pos = body_text.lower().find('abstract')
                if abstract_pos > 0:
                    start = max(0, abstract_pos - 200)
                    end = min(len(body_text), abstract_pos + 500)
                    result['html_snippet'] = body_text[start:end]

        result['success'] = True

    except requests.exceptions.Timeout:
        result['error'] = 'Request timeout'
        result['failure_reason'] = 'oa_url_connection_timeout'
    except requests.exceptions.ConnectionError as e:
        result['error'] = f'Connection error: {str(e)[:100]}'
        result['failure_reason'] = 'oa_url_connection_error'
    except requests.exceptions.RequestException as e:
        result['error'] = f'Request error: {str(e)[:100]}'
        result['failure_reason'] = 'oa_url_connection_error'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'
        result['failure_reason'] = 'oa_url_connection_error'

    return result


def get_abstract_with_selenium(url, browser, timeout=15):
    """
    Scrape abstract from a JavaScript-rendered page using Selenium.

    This function is used as a fallback when the initial HTTP request
    detects that JavaScript is required to render the page content.

    Parameters:
    -----------
    url : str
        URL to scrape
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
        - 'failure_reason': Standardized failure reason code (if failed)
    """
    result = {
        'abstract': '',
        'success': False,
        'error': None,
        'has_abstract': False,
        'failure_reason': None
    }

    if not url:
        result['error'] = 'No URL provided'
        result['failure_reason'] = 'oa_url_invalid_url'
        return result

    try:
        # Navigate to page
        browser.get(url)

        # Wait for page to load
        wait = WebDriverWait(browser, timeout)

        abstract_text = None

        # Try various selectors to find abstract
        # Order from most specific to most generic
        selectors = [
            # Angular patterns (like simple-view-element-header with Abstract)
            'h2.simple-view-element-header',
            '.simple-view-element-header',
            '[class*="simple-view-element"]',
            '[class*="ng-star-inserted"]',

            # Common abstract selectors
            'div.abstract-text',
            'div.abstract',
            'section.abstract',
            '[class*="abstract"]',
            '[id*="abstract"]',

            # Generic content selectors
            '.c-article-section__content',
            '.article-section__content',
        ]

        for selector in selectors:
            try:
                elements = browser.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    # For headers, look for "Abstract" text and get next sibling content
                    elem_text = elem.text.strip()

                    # Check if this is a header element
                    if elem.tag_name in ['h1', 'h2', 'h3', 'h4'] or 'header' in selector:
                        if 'abstract' in elem_text.lower():
                            # Try to find the content following this header
                            try:
                                # Look for next sibling
                                parent = elem.find_element(By.XPATH, '..')
                                siblings = parent.find_elements(By.XPATH, './*')
                                found_header = False
                                for sib in siblings:
                                    if found_header:
                                        sib_text = sib.text.strip()
                                        if len(sib_text) > 100:
                                            abstract_text = sib_text
                                            break
                                    if sib == elem:
                                        found_header = True
                            except Exception:
                                pass
                    else:
                        # Regular content element
                        if len(elem_text) > 100:
                            # Check if it's likely an abstract (not navigation, etc.)
                            if 'abstract' in selector.lower() or len(elem_text) < 5000:
                                abstract_text = elem_text
                                break

                if abstract_text:
                    break
            except Exception:
                continue

        # Strategy 2: Find any element with "Abstract" heading and get following content
        if not abstract_text:
            try:
                # Find all headings
                headings = browser.find_elements(By.CSS_SELECTOR, 'h1, h2, h3, h4, strong, b')
                for heading in headings:
                    heading_text = heading.text.strip().lower()
                    if heading_text in ['abstract', 'summary', 'abstract:']:
                        # Try to get parent and find content
                        try:
                            parent = heading.find_element(By.XPATH, '..')
                            parent_text = parent.text.strip()
                            # Remove the heading text
                            content = parent_text.replace(heading.text.strip(), '').strip()
                            if len(content) > 100:
                                abstract_text = content
                                break
                        except Exception:
                            pass
            except Exception:
                pass

        if abstract_text:
            # Clean up the text
            abstract_text = ' '.join(abstract_text.split())
            # Remove common prefixes
            abstract_text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY):?\s*', '', abstract_text)
            result['abstract'] = abstract_text.strip()
            result['has_abstract'] = len(result['abstract']) > 50

            if not result['has_abstract']:
                result['failure_reason'] = 'oa_url_abstract_too_short'
        else:
            result['failure_reason'] = 'oa_url_no_matching_selector'

        result['success'] = True

    except TimeoutException:
        result['error'] = 'Page load timeout'
        result['failure_reason'] = 'oa_url_connection_timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
        result['failure_reason'] = 'oa_url_connection_error'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'
        result['failure_reason'] = 'oa_url_connection_error'

    return result


def get_abstract_from_pdf(pdf_url, timeout=30):
    """
    Download PDF and extract abstract text.

    Uses pdfplumber to extract text from the first 2-3 pages of a PDF,
    then searches for the 'Abstract' section.

    Parameters:
    -----------
    pdf_url : str
        URL pointing to a PDF file
    timeout : int
        Request timeout in seconds

    Returns:
    --------
    dict : Result with keys:
        - 'abstract': Retrieved abstract text (empty string if not found)
        - 'success': Boolean indicating if extraction succeeded
        - 'error': Error message if any
        - 'has_abstract': Boolean indicating if abstract was found
        - 'failure_reason': Standardized failure reason code (if failed)
    """
    result = {
        'abstract': '',
        'success': False,
        'error': None,
        'has_abstract': False,
        'failure_reason': None
    }

    if not PDF_EXTRACTION_AVAILABLE:
        result['error'] = 'pdfplumber not installed'
        result['failure_reason'] = 'pdf_extraction_failed'
        return result

    if not pdf_url:
        result['error'] = 'No PDF URL provided'
        result['failure_reason'] = 'pdf_download_failed'
        return result

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    try:
        # Download PDF to temporary file
        response = requests.get(pdf_url, headers=headers, timeout=timeout, stream=True)
        response.raise_for_status()

        # Create temporary file to store PDF
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
            tmp_path = tmp_file.name
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)

        try:
            # Extract text from first 3 pages
            with pdfplumber.open(tmp_path) as pdf:
                text_pages = []
                for i, page in enumerate(pdf.pages[:3]):  # First 3 pages only
                    page_text = page.extract_text()
                    if page_text:
                        text_pages.append(page_text)

                full_text = '\n'.join(text_pages)

            if not full_text:
                result['error'] = 'No text extracted from PDF'
                result['failure_reason'] = 'pdf_extraction_failed'
                result['success'] = True
                return result

            # Search for abstract section
            abstract_text = None

            # Pattern 1: Look for "Abstract" keyword followed by content
            # Common patterns: "Abstract", "ABSTRACT", "Abstract:", "A B S T R A C T"
            patterns = [
                r'(?i)(?:^|\n)\s*A\s*B\s*S\s*T\s*R\s*A\s*C\s*T\s*[:\.]?\s*\n(.+?)(?=\n\s*(?:1\.|I\.|Introduction|INTRODUCTION|Keywords|Key\s*words|1\s+Introduction))',
                r'(?i)(?:^|\n)\s*Abstract\s*[:\.]?\s*\n(.+?)(?=\n\s*(?:1\.|I\.|Introduction|INTRODUCTION|Keywords|Key\s*words|1\s+Introduction))',
                r'(?i)(?:^|\n)\s*ABSTRACT\s*[:\.]?\s*\n(.+?)(?=\n\s*(?:1\.|I\.|Introduction|INTRODUCTION|Keywords|Key\s*words|1\s+Introduction))',
                r'(?i)(?:^|\n)\s*Summary\s*[:\.]?\s*\n(.+?)(?=\n\s*(?:1\.|I\.|Introduction|INTRODUCTION|Keywords|Key\s*words))',
            ]

            for pattern in patterns:
                match = re.search(pattern, full_text, re.DOTALL)
                if match:
                    candidate = match.group(1).strip()
                    # Clean up the text
                    candidate = ' '.join(candidate.split())
                    if len(candidate) > 100:
                        abstract_text = candidate
                        break

            # Pattern 2: If no section headers, try to find text after "Abstract" keyword
            if not abstract_text:
                abstract_start = re.search(r'(?i)(?:abstract|summary)[:\.]?\s*', full_text)
                if abstract_start:
                    # Get text after the keyword, up to 2000 chars or next section
                    start_pos = abstract_start.end()
                    remaining_text = full_text[start_pos:start_pos + 2000]
                    # Try to find end of abstract (new section, keywords, etc.)
                    end_match = re.search(r'\n\s*(?:1\.|I\.|Introduction|INTRODUCTION|Keywords|Key\s*words|\d+\.\s+[A-Z])', remaining_text)
                    if end_match:
                        abstract_text = remaining_text[:end_match.start()].strip()
                    else:
                        # Take first ~1500 chars as abstract
                        abstract_text = remaining_text[:1500].strip()

                    abstract_text = ' '.join(abstract_text.split())

            if abstract_text and len(abstract_text) > 100:
                # Remove common prefixes
                abstract_text = re.sub(r'^(?:Abstract|Summary|ABSTRACT|SUMMARY)[:\.]?\s*', '', abstract_text, flags=re.I)
                result['abstract'] = abstract_text.strip()[:3000]  # Limit length
                result['has_abstract'] = True
            else:
                result['failure_reason'] = 'pdf_no_abstract_found'

            result['success'] = True

        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    except requests.exceptions.Timeout:
        result['error'] = 'PDF download timeout'
        result['failure_reason'] = 'pdf_download_failed'
    except requests.exceptions.RequestException as e:
        result['error'] = f'PDF download error: {str(e)[:100]}'
        result['failure_reason'] = 'pdf_download_failed'
    except Exception as e:
        result['error'] = f'PDF extraction error: {str(e)[:100]}'
        result['failure_reason'] = 'pdf_extraction_failed'

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
        - 'failure_reason': Standardized failure reason code (if failed)

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
        'has_abstract': False,
        'failure_reason': None
    }

    if not ssrn_id:
        result['error'] = 'No SSRN ID provided'
        result['failure_reason'] = 'ssrn_no_id'
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
        else:
            result['failure_reason'] = 'ssrn_no_abstract_element'

        result['success'] = True

    except TimeoutException:
        result['error'] = 'Page load timeout'
        result['failure_reason'] = 'ssrn_timeout'
    except WebDriverException as e:
        result['error'] = f'Browser error: {str(e)[:100]}'
        result['failure_reason'] = 'ssrn_browser_error'
    except Exception as e:
        result['error'] = f'Unexpected error: {str(e)[:100]}'
        result['failure_reason'] = 'ssrn_browser_error'

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
        'oa_url_pdf_detected': 0,
        'pdf_fetched': 0,
        'pdf_recovered': 0,
        'pdf_failed': 0,
        'ssrn_fetched': 0,
        'ssrn_recovered': 0,
        'ssrn_failed': 0,
        'ssrn_no_abstract': 0
    }

    # Store raw responses for debugging
    crossref_responses = []
    oa_url_responses = []
    pdf_responses = []
    ssrn_responses = []

    # Store detailed failure information
    all_failures = []

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
            response_entry = {
                'doi': doi,
                'title': title,
                'success': result['success'],
                'has_abstract': result['has_abstract'],
                'error': result['error'],
                'failure_reason': result.get('failure_reason'),
                'abstract_preview': result['abstract'][:100] if result['abstract'] else None
            }
            crossref_responses.append(response_entry)

            if result['success'] and result['has_abstract']:
                # Update the dataframe
                df.at[df_idx, 'abstract'] = result['abstract']
                df.at[df_idx, 'abstract_source'] = 'CrossRef'
                stats['crossref_recovered'] += 1
            elif result['success'] and not result['has_abstract']:
                stats['crossref_no_abstract'] += 1
                # Log failure
                all_failures.append({
                    'source': 'CrossRef',
                    'paper_title': title,
                    'doi': doi,
                    'failure_reason': result.get('failure_reason', 'crossref_no_abstract_in_db'),
                    'error': result.get('error')
                })
            elif 'not found' in str(result.get('error', '')).lower():
                stats['crossref_not_found'] += 1
                all_failures.append({
                    'source': 'CrossRef',
                    'paper_title': title,
                    'doi': doi,
                    'failure_reason': 'crossref_doi_not_found',
                    'error': result.get('error')
                })
            else:
                stats['crossref_failed'] += 1
                all_failures.append({
                    'source': 'CrossRef',
                    'paper_title': title,
                    'doi': doi,
                    'failure_reason': result.get('failure_reason', 'crossref_api_error'),
                    'error': result.get('error')
                })

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
        print(f"\n[Step 2/5] Scraping abstracts from Open Access URLs for {len(to_fetch_oa)} papers...")

        # Track PDFs and JavaScript-required pages for later processing
        pdf_urls_to_process = []
        js_urls_to_process = []

        for idx, (df_idx, row) in enumerate(to_fetch_oa.iterrows()):
            oa_url = row['open_access_url']
            title = row.get('title', 'Unknown')[:50]
            doi = row.get('doi', '')

            if idx % 50 == 0 and idx > 0:
                print(f"  Progress: {idx}/{len(to_fetch_oa)} ({stats['oa_url_recovered']} recovered)")

            result = get_abstract_from_oa_url(oa_url)
            stats['oa_url_fetched'] += 1

            # Store raw response for debugging
            response_entry = {
                'open_access_url': oa_url,
                'title': title,
                'doi': doi,
                'success': result['success'],
                'has_abstract': result['has_abstract'],
                'error': result['error'],
                'failure_reason': result.get('failure_reason'),
                'is_pdf': result.get('is_pdf', False),
                'http_status': result.get('http_status'),
                'abstract_preview': result['abstract'][:100] if result['abstract'] else None,
                'html_snippet': result.get('html_snippet', '')[:500] if result.get('html_snippet') else None
            }
            oa_url_responses.append(response_entry)

            if result['success'] and result['has_abstract']:
                # Update the dataframe
                df.at[df_idx, 'abstract'] = result['abstract']
                df.at[df_idx, 'abstract_source'] = 'OpenAccess'
                stats['oa_url_recovered'] += 1
            elif result.get('is_pdf'):
                # PDF detected - queue for PDF extraction
                stats['oa_url_pdf_detected'] += 1
                pdf_urls_to_process.append({
                    'df_idx': df_idx,
                    'url': oa_url,
                    'title': title,
                    'doi': doi
                })
            elif result.get('failure_reason') == 'oa_url_javascript_required':
                # JavaScript required - queue for Selenium processing
                stats['oa_url_js_detected'] = stats.get('oa_url_js_detected', 0) + 1
                js_urls_to_process.append({
                    'df_idx': df_idx,
                    'url': oa_url,
                    'title': title,
                    'doi': doi
                })
            elif result['success'] and not result['has_abstract']:
                stats['oa_url_no_abstract'] += 1
                # Log detailed failure
                all_failures.append({
                    'source': 'OpenAccess',
                    'paper_title': title,
                    'doi': doi,
                    'url': oa_url,
                    'failure_reason': result.get('failure_reason', 'oa_url_no_matching_selector'),
                    'error': result.get('error'),
                    'http_status': result.get('http_status'),
                    'html_snippet': result.get('html_snippet', '')[:1000] if result.get('html_snippet') else None
                })
            else:
                stats['oa_url_failed'] += 1
                all_failures.append({
                    'source': 'OpenAccess',
                    'paper_title': title,
                    'doi': doi,
                    'url': oa_url,
                    'failure_reason': result.get('failure_reason', 'oa_url_connection_error'),
                    'error': result.get('error'),
                    'http_status': result.get('http_status')
                })

            # Be polite to the servers
            time.sleep(oa_delay)

        print(f"  Open Access URL completed: {stats['oa_url_fetched']} fetched, {stats['oa_url_recovered']} recovered")
        print(f"  PDFs detected for extraction: {stats['oa_url_pdf_detected']}")
        print(f"  JavaScript-required pages detected: {stats.get('oa_url_js_detected', 0)}")

        # =========================================================================
        # STEP 2b: Try PDF extraction for detected PDF URLs
        # =========================================================================
        if len(pdf_urls_to_process) > 0 and PDF_EXTRACTION_AVAILABLE:
            print(f"\n[Step 2b/4] Extracting abstracts from {len(pdf_urls_to_process)} PDF files...")

            for idx, pdf_info in enumerate(pdf_urls_to_process):
                if idx % 20 == 0 and idx > 0:
                    print(f"  Progress: {idx}/{len(pdf_urls_to_process)} ({stats['pdf_recovered']} recovered)")

                result = get_abstract_from_pdf(pdf_info['url'])
                stats['pdf_fetched'] += 1

                # Store response
                pdf_response_entry = {
                    'url': pdf_info['url'],
                    'title': pdf_info['title'],
                    'doi': pdf_info['doi'],
                    'success': result['success'],
                    'has_abstract': result['has_abstract'],
                    'error': result['error'],
                    'failure_reason': result.get('failure_reason'),
                    'abstract_preview': result['abstract'][:100] if result['abstract'] else None
                }
                pdf_responses.append(pdf_response_entry)

                if result['success'] and result['has_abstract']:
                    df.at[pdf_info['df_idx'], 'abstract'] = result['abstract']
                    df.at[pdf_info['df_idx'], 'abstract_source'] = 'PDF'
                    stats['pdf_recovered'] += 1
                else:
                    stats['pdf_failed'] += 1
                    all_failures.append({
                        'source': 'PDF',
                        'paper_title': pdf_info['title'],
                        'doi': pdf_info['doi'],
                        'url': pdf_info['url'],
                        'failure_reason': result.get('failure_reason', 'pdf_extraction_failed'),
                        'error': result.get('error')
                    })

                time.sleep(oa_delay)

            print(f"  PDF extraction completed: {stats['pdf_fetched']} attempted, {stats['pdf_recovered']} recovered")
        elif len(pdf_urls_to_process) > 0:
            print(f"\n  WARNING: Skipping PDF extraction ({len(pdf_urls_to_process)} PDFs) - pdfplumber not installed")

        # =========================================================================
        # STEP 2c: Try Selenium for JavaScript-required pages
        # =========================================================================
        if len(js_urls_to_process) > 0:
            print(f"\n[Step 2c/5] Processing {len(js_urls_to_process)} JavaScript-rendered pages with Selenium...")
            print("  Initializing Selenium browser (headless Chrome)...")

            # Initialize stats for JS/Selenium
            stats['selenium_fetched'] = 0
            stats['selenium_recovered'] = 0
            stats['selenium_failed'] = 0

            selenium_responses = []
            browser = None
            try:
                browser = create_selenium_browser()
                print("  Browser initialized successfully")

                for idx, js_info in enumerate(js_urls_to_process):
                    if idx % 20 == 0 and idx > 0:
                        print(f"  Progress: {idx}/{len(js_urls_to_process)} ({stats['selenium_recovered']} recovered)")

                    result = get_abstract_with_selenium(js_info['url'], browser)
                    stats['selenium_fetched'] += 1

                    # Store response
                    selenium_response_entry = {
                        'url': js_info['url'],
                        'title': js_info['title'],
                        'doi': js_info['doi'],
                        'success': result['success'],
                        'has_abstract': result['has_abstract'],
                        'error': result['error'],
                        'failure_reason': result.get('failure_reason'),
                        'abstract_preview': result['abstract'][:100] if result['abstract'] else None
                    }
                    selenium_responses.append(selenium_response_entry)

                    if result['success'] and result['has_abstract']:
                        df.at[js_info['df_idx'], 'abstract'] = result['abstract']
                        df.at[js_info['df_idx'], 'abstract_source'] = 'Selenium'
                        stats['selenium_recovered'] += 1
                    else:
                        stats['selenium_failed'] += 1
                        all_failures.append({
                            'source': 'Selenium',
                            'paper_title': js_info['title'],
                            'doi': js_info['doi'],
                            'url': js_info['url'],
                            'failure_reason': result.get('failure_reason', 'oa_url_no_matching_selector'),
                            'error': result.get('error')
                        })

                    # Be polite
                    time.sleep(ssrn_delay)

                print(f"  Selenium completed: {stats['selenium_fetched']} attempted, {stats['selenium_recovered']} recovered")

            except Exception as e:
                print(f"  ERROR initializing Selenium browser: {e}")
                print("  Skipping JavaScript-rendered page processing")

            finally:
                if browser:
                    browser.quit()
                    print("  Browser closed")

            # Save Selenium responses
            selenium_file = os.path.join(TMP_DIR, "selenium_responses.json")
            with open(selenium_file, 'w') as f:
                json.dump(selenium_responses, f, indent=2)
            print(f"  Saved Selenium responses to: {selenium_file}")

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
        print(f"\n[Step 3/5] Scraping abstracts from SSRN for {len(to_fetch_ssrn)} papers...")
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
                        'error': 'Could not extract SSRN ID',
                        'failure_reason': 'ssrn_no_id'
                    })
                    stats['ssrn_failed'] += 1
                    all_failures.append({
                        'source': 'SSRN',
                        'paper_title': title,
                        'doi': doi,
                        'url': url,
                        'failure_reason': 'ssrn_no_id',
                        'error': 'Could not extract SSRN ID from DOI or URL'
                    })
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
                    'failure_reason': result.get('failure_reason'),
                    'abstract_preview': result['abstract'][:100] if result['abstract'] else None
                })

                if result['success'] and result['has_abstract']:
                    # Update the dataframe
                    df.at[df_idx, 'abstract'] = result['abstract']
                    df.at[df_idx, 'abstract_source'] = 'SSRN'
                    stats['ssrn_recovered'] += 1
                elif result['success'] and not result['has_abstract']:
                    stats['ssrn_no_abstract'] += 1
                    all_failures.append({
                        'source': 'SSRN',
                        'paper_title': title,
                        'doi': doi,
                        'ssrn_id': ssrn_id,
                        'failure_reason': result.get('failure_reason', 'ssrn_no_abstract_element'),
                        'error': result.get('error')
                    })
                else:
                    stats['ssrn_failed'] += 1
                    all_failures.append({
                        'source': 'SSRN',
                        'paper_title': title,
                        'doi': doi,
                        'ssrn_id': ssrn_id,
                        'failure_reason': result.get('failure_reason', 'ssrn_browser_error'),
                        'error': result.get('error')
                    })

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

    # Save PDF responses
    pdf_file = os.path.join(TMP_DIR, "pdf_responses.json")
    with open(pdf_file, 'w') as f:
        json.dump(pdf_responses, f, indent=2)
    print(f"  Saved PDF responses to: {pdf_file}")

    # =========================================================================
    # STEP 4: Save detailed failure log for diagnostic analysis
    # =========================================================================
    print(f"\n[Step 4/5] Saving detailed failure log...")

    # Aggregate failure statistics by reason
    failure_stats = {}
    for failure in all_failures:
        reason = failure.get('failure_reason', 'unknown')
        if reason not in failure_stats:
            failure_stats[reason] = 0
        failure_stats[reason] += 1

    # Create comprehensive failure log
    failure_log = {
        'timestamp': datetime.now().isoformat(),
        'total_failures': len(all_failures),
        'failure_breakdown': failure_stats,
        'failure_reason_descriptions': FAILURE_REASONS,
        'failures': all_failures
    }

    failure_log_file = os.path.join(TMP_DIR, "abstract_recovery_failures.json")
    with open(failure_log_file, 'w') as f:
        json.dump(failure_log, f, indent=2)
    print(f"  Saved detailed failure log to: {failure_log_file}")
    print(f"  Total failures logged: {len(all_failures)}")
    print(f"  Failure breakdown:")
    for reason, count in sorted(failure_stats.items(), key=lambda x: -x[1]):
        description = FAILURE_REASONS.get(reason, 'Unknown reason')
        print(f"    {reason}: {count} ({description})")

    # Calculate total recovered
    stats['total_recovered'] = (
        stats['crossref_recovered'] +
        stats['oa_url_recovered'] +
        stats['pdf_recovered'] +
        stats.get('selenium_recovered', 0) +
        stats['ssrn_recovered']
    )

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
    print(f"    Abstracts recovered from PDFs: {stats['pdf_recovered']}")
    print(f"    Abstracts recovered from Selenium (JS pages): {stats.get('selenium_recovered', 0)}")
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
        'abstracts_from_pdf': int((df_complemented['abstract_source'] == 'PDF').sum()),
        'abstracts_from_selenium': int((df_complemented['abstract_source'] == 'Selenium').sum()),
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
        'pdf_stats': {k: v for k, v in stats.items() if k.startswith('pdf_')},
        'selenium_stats': {k: v for k, v in stats.items() if k.startswith('selenium_')},
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
