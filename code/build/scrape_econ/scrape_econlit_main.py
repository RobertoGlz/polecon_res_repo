"""
EconLit Automated Pipeline (Pipeline A).

Issue #24: Economics-restricted scraping pipelines

Automates EBSCO Research / EconLit search and CSV export via browser,
then parses, deduplicates, recovers abstracts, and filters.

Workflow:
---------
1. SEARCH (automated): Opens Chrome → Stanford SSO → EBSCO Research.
   Searches EconLit (db=eoh) for each policy search term.
   Selects all results, exports as CSV. Handles pagination so all
   results are captured, not just the first page.

2. PROCESS: Loads exported CSVs, parses EBSCO format, deduplicates
   across search terms, recovers missing abstracts via CrossRef/SS,
   and applies relevance filtering.

3. OUTPUT: Parquet + CSV files compatible with Pipeline B for overlap
   analysis.

Modes:
------
  python scrape_econlit_main.py TCJA              # Full: browser + process
  python scrape_econlit_main.py TCJA --process-only  # Skip browser, just process CSVs
  python scrape_econlit_main.py TCJA --discover      # Open browser, save debug info
  python scrape_econlit_main.py TCJA --resume         # Resume from checkpoint

Requirements:
    pip install pandas pyarrow requests beautifulsoup4 undetected-chromedriver selenium

Author: Claude AI with modifications by Roberto Gonzalez
Date: February 2026
"""

import argparse
import html
import json
import os
import re
import sys
import time
import threading
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import requests
from dotenv import load_dotenv

# Selenium imports (only needed for browser mode)
try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, WebDriverException, NoSuchElementException,
        StaleElementReferenceException, ElementClickInterceptedException
    )
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False


# =============================================================================
# PATHS
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")
REPORTS_DIR = os.path.join(SCRIPT_DIR, "reports")
EXPORTS_DIR = os.path.join(TMP_DIR, "econlit_exports")
DEBUG_DIR = os.path.join(TMP_DIR, "debug")
POLICIES_FILE = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "get_policies", "output", "policies.csv"))

# Shared Selenium profile with other scraping scripts (Stanford SSO persists)
PROFILE_DIR = os.path.normpath(os.path.join(
    SCRIPT_DIR, "..", "complement_abstracts", "selenium_profile"
))

for d in [OUTPUT_DIR, TMP_DIR, REPORTS_DIR, EXPORTS_DIR, DEBUG_DIR]:
    os.makedirs(d, exist_ok=True)

# Load .env from repo root
REPO_ROOT = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
load_dotenv(os.path.join(REPO_ROOT, ".env"))

# Dropbox safety: .dropboxignore in tmp directories
for d in [TMP_DIR, EXPORTS_DIR, DEBUG_DIR]:
    ignore_path = os.path.join(d, ".dropboxignore")
    if not os.path.exists(ignore_path):
        with open(ignore_path, 'w') as f:
            f.write("*\n")


# =============================================================================
# EBSCO RESEARCH CONFIGURATION
# =============================================================================
EBSCO_BASE_URL = "https://research-ebsco-com.stanford.idm.oclc.org"
EBSCO_CUSTOMER_PATH = "/c/qmsjx4"
ECONLIT_DB = "eoh"

# Stanford proxy URL — entry point that triggers SSO if needed
STANFORD_PROXY_URL = (
    "https://stanford.idm.oclc.org/login?"
    "qurl=https%3A%2F%2Fresearch.ebsco.com%2Fc%2Fqmsjx4%3Fdb%3Deoh"
)

# Browser automation settings
SEARCH_DELAY = 5          # Seconds to wait after search navigation
PAGE_LOAD_TIMEOUT = 60    # Max seconds for page load
RESULTS_WAIT_TIMEOUT = 30 # Max seconds to wait for results to render
EXPORT_WAIT_TIMEOUT = 60  # Max seconds to wait for CSV download


# =============================================================================
# API CONFIGURATION (for abstract recovery)
# =============================================================================
CROSSREF_API = "https://api.crossref.org/works"
USER_EMAIL = "rob98@stanford.edu"
SS_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
CROSSREF_RATE_LIMIT = 0.1
SS_RATE_LIMIT = 1.1


# =============================================================================
# RATE LIMITERS
# =============================================================================
class RateLimiter:
    """Thread-safe rate limiter for API requests."""
    def __init__(self, delay):
        self.delay = delay
        self.lock = threading.Lock()
        self.last_request = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()

crossref_rate_limiter = RateLimiter(CROSSREF_RATE_LIMIT)
ss_rate_limiter = RateLimiter(SS_RATE_LIMIT)
epmc_rate_limiter = RateLimiter(0.15)  # Europe PMC: ~7 req/sec


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def normalize_title(title):
    """Normalize title for deduplication: strip HTML, decode entities, lowercase, no punctuation."""
    if not title or pd.isna(title):
        return ''
    title = str(title)
    title = re.sub(r'<[^>]+>', '', title)   # Strip HTML tags (<scp>, </scp>, etc.)
    title = html.unescape(title)             # Decode HTML entities (&amp; → &)
    title = title.lower()
    title = title.replace('&', ' and ')      # Normalize & → and
    title = re.sub(r'[\u2013\u2014-]+', ' ', title)  # Normalize dashes (en/em/hyphen) → space
    title = re.sub(r'[^\w\s]', '', title)
    title = ' '.join(title.split())
    return title


def normalize_doi(doi):
    """Normalize DOI: strip URL prefix, lowercase."""
    if not doi or pd.isna(doi):
        return ''
    doi = str(doi).strip()
    doi = re.sub(r'^https?://doi\.org/', '', doi)
    return doi.lower()


def clean_abstract_text(text):
    """Clean and normalize abstract text."""
    if not text or pd.isna(text):
        return ''
    text = str(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = ' '.join(text.split())
    text = re.sub(r'^(Abstract|Summary|ABSTRACT|SUMMARY)[.:]?\s*', '', text)
    return text.strip()


def load_policies(policies_file):
    """Load policy configurations from CSV file."""
    if not os.path.exists(policies_file):
        print(f"ERROR: Policies file not found: {policies_file}")
        sys.exit(1)
    df = pd.read_csv(policies_file)
    required_cols = ['policy_name', 'policy_abbreviation', 'policy_year',
                     'policy_category', 'search_terms']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        print(f"ERROR: Missing required columns: {missing_cols}")
        sys.exit(1)
    print(f"Loaded {len(df)} policies from {policies_file}")
    return df


# =============================================================================
# BROWSER SESSION — EBSCO Research via Stanford SSO
# =============================================================================
class EBSCOBrowser:
    """
    Manages Chrome browser for EBSCO Research EconLit automation.

    Uses undetected-chromedriver with a persistent profile shared with
    scrape_abstracts_web.py so Stanford SSO login persists across scripts.
    """

    def __init__(self, download_dir):
        self.browser = None
        self.download_dir = os.path.abspath(download_dir)
        os.makedirs(self.download_dir, exist_ok=True)

    def launch(self):
        """Launch Chrome with persistent profile and download directory."""
        if not HAS_SELENIUM:
            print("ERROR: undetected-chromedriver not installed.")
            print("  pip install undetected-chromedriver selenium")
            sys.exit(1)

        os.makedirs(PROFILE_DIR, exist_ok=True)

        options = uc.ChromeOptions()
        options.add_argument(f'--user-data-dir={PROFILE_DIR}')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')

        # Set download directory via Chrome preferences
        prefs = {
            'download.default_directory': self.download_dir,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True,
        }
        options.add_experimental_option('prefs', prefs)

        self.browser = uc.Chrome(options=options, use_subprocess=True)
        self.browser.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

        # Also set download directory via Chrome DevTools Protocol
        self.browser.execute_cdp_cmd('Page.setDownloadBehavior', {
            'behavior': 'allow',
            'downloadPath': self.download_dir.replace('/', '\\')
        })

        print(f"  Browser launched (profile: {PROFILE_DIR})")
        print(f"  Downloads → {self.download_dir}")
        return self.browser

    def verify_stanford_access(self):
        """Navigate to EBSCO via Stanford proxy. Prompt for SSO if needed."""
        print("  Navigating to EBSCO Research via Stanford proxy...")
        try:
            self.browser.get(STANFORD_PROXY_URL)
            time.sleep(4)

            # Wait for Cloudflare if present
            self._wait_for_cloudflare()

            url = self.browser.current_url.lower()
            if 'login.stanford.edu' in url or 'idp' in url:
                print("\n" + "=" * 60)
                print("STANFORD SSO LOGIN REQUIRED")
                print("=" * 60)
                print("A browser window has opened. Please:")
                print("  1. Log in with your Stanford credentials")
                print("  2. Complete 2FA (Duo) if prompted")
                print("  3. Wait for EBSCO Research to load")
                print("Your session will be saved for future runs.")
                print("=" * 60)
                return self._wait_for_login(timeout=300)

            if 'research-ebsco-com' in url or 'research.ebsco.com' in url:
                print("  Stanford proxy access confirmed — EBSCO Research loaded.")
                return True

            print(f"  Landed on: {url[:100]}")
            print("  Proceeding (may need login)...")
            return True

        except Exception as e:
            print(f"  Warning: Navigation issue: {e}")
            return True

    def _wait_for_login(self, timeout=300):
        """Wait for user to complete Stanford SSO login."""
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(3)
            try:
                url = self.browser.current_url.lower()
                if 'login.stanford.edu' not in url and 'idp' not in url:
                    print("  Login successful!")
                    time.sleep(3)
                    return True
            except Exception:
                pass
        print("  Login timeout — proceeding anyway.")
        return False

    def _wait_for_cloudflare(self, timeout=120):
        """Detect Cloudflare challenge and wait for user to solve."""
        try:
            title = self.browser.title.strip()
        except Exception:
            return True
        if title != 'Just a moment...':
            return True
        print("    [CAPTCHA] Cloudflare challenge detected — solve it in the browser")
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(2)
            try:
                if self.browser.title.strip() != 'Just a moment...':
                    print("    [CAPTCHA] Solved!")
                    time.sleep(2)
                    return True
            except Exception:
                return False
        print(f"    [CAPTCHA] Timeout after {timeout}s")
        return False

    def search_econlit(self, query, from_year=None):
        """
        Search EconLit for a query by typing into the search box and submitting.

        The EBSCO Research SPA does not auto-execute searches from URL params,
        so we navigate to the search page, type the query, and press Enter.

        Parameters:
        -----------
        query : str
            Search term (will be quoted)
        from_year : int or None
            If set, restrict to publications from this year onward using
            EBSCO field code: AND DT {year}01-

        Returns the total result count, or -1 if results couldn't be parsed.
        """
        # Navigate to the search page (with db=eoh to ensure EconLit is selected)
        search_page_url = (
            f"{EBSCO_BASE_URL}{EBSCO_CUSTOMER_PATH}/search?db={ECONLIT_DB}"
        )
        print(f"\n  Searching EconLit: \"{query}\""
              + (f" (from {from_year})" if from_year else ""))

        try:
            self.browser.get(search_page_url)
        except TimeoutException:
            print("  Page load timeout — checking if content loaded anyway...")

        time.sleep(3)
        self._wait_for_cloudflare()

        # Find the search input and type the query
        search_input = None
        input_selectors = [
            (By.ID, 'search-input'),
            (By.CSS_SELECTOR, '[data-auto="search-input"]'),
            (By.CSS_SELECTOR, 'input[placeholder*="Search"]'),
            (By.CSS_SELECTOR, 'input[type="search"]'),
            (By.CSS_SELECTOR, '.eb-search-input input'),
        ]
        for by, selector in input_selectors:
            try:
                search_input = WebDriverWait(self.browser, 10).until(
                    EC.element_to_be_clickable((by, selector))
                )
                print(f"  Found search input via: {selector}")
                break
            except (TimeoutException, NoSuchElementException):
                continue

        if not search_input:
            print("  ERROR: Could not find search input box")
            return -1

        # Clear any existing text and type the query
        search_input.clear()
        time.sleep(0.5)
        # Build search string: quoted term + optional date filter
        search_str = f'"{query}"'
        if from_year:
            search_str += f' AND DT {from_year}01-'
        search_input.send_keys(search_str)
        time.sleep(0.5)

        # Submit the search by pressing Enter
        search_input.send_keys(Keys.RETURN)
        print(f"  Search submitted: {search_str}")

        time.sleep(SEARCH_DELAY)
        self._wait_for_cloudflare()

        # Wait for results to render (React SPA)
        result_count = self._wait_for_results()
        return result_count

    def _wait_for_results(self):
        """
        Wait for EBSCO Research results to render and return total count.

        EBSCO Research is a React SPA — results render client-side.
        We wait for article elements with data-auto="search-result-item".
        """
        print("  Waiting for results to render...")

        try:
            WebDriverWait(self.browser, RESULTS_WAIT_TIMEOUT).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'article[data-auto="search-result-item"]'))
            )
            items = self.browser.find_elements(
                By.CSS_SELECTOR, 'article[data-auto="search-result-item"]')
            print(f"  Results loaded: {len(items)} items on page")
        except TimeoutException:
            # Check for zero results
            try:
                body_text = self.browser.find_element(By.TAG_NAME, 'body').text
                if 'no results' in body_text.lower() or 'results: 0' in body_text.lower():
                    print("  No results found for this search term.")
                    return 0
            except Exception:
                pass
            print("  Timeout waiting for results.")
            return -1

        return self._parse_result_count()

    def _parse_result_count(self):
        """Parse total result count from h1[data-auto='result-count']."""
        try:
            elem = WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'h1[data-auto="result-count"]'))
            )
            text = elem.text.strip()  # e.g. "Results: 170"
            match = re.search(r'(\d[\d,]*)', text)
            if match:
                count = int(match.group(1).replace(',', ''))
                print(f"  Total results: {count:,}")
                return count
        except (TimeoutException, NoSuchElementException):
            pass

        # Fallback: search page text
        try:
            body_text = self.browser.find_element(By.TAG_NAME, 'body').text
            match = re.search(r'Results:\s*(\d[\d,]*)', body_text)
            if match:
                count = int(match.group(1).replace(',', ''))
                print(f"  Total results (from page text): {count:,}")
                return count
        except Exception:
            pass

        print("  Could not parse result count.")
        return -1

    def discover_ui_elements(self, label=""):
        """
        Save screenshot, DOM snapshot, and inventory of interactive elements.
        Helps identify the correct selectors for Export/Select All buttons.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        prefix = f"{label}_{timestamp}" if label else timestamp

        # Screenshot
        screenshot_path = os.path.join(DEBUG_DIR, f"{prefix}_screenshot.png")
        try:
            self.browser.save_screenshot(screenshot_path)
            print(f"  Screenshot saved: {screenshot_path}")
        except Exception as e:
            print(f"  Screenshot failed: {e}")

        # DOM snapshot
        dom_path = os.path.join(DEBUG_DIR, f"{prefix}_dom.html")
        try:
            with open(dom_path, 'w', encoding='utf-8') as f:
                f.write(self.browser.page_source)
            print(f"  DOM saved: {dom_path}")
        except Exception as e:
            print(f"  DOM save failed: {e}")

        # Inventory of buttons and interactive elements
        inventory_path = os.path.join(DEBUG_DIR, f"{prefix}_elements.txt")
        try:
            elements = []

            # Buttons
            for btn in self.browser.find_elements(By.TAG_NAME, 'button'):
                try:
                    text = btn.text.strip()[:80]
                    data_auto = btn.get_attribute('data-auto') or ''
                    aria = btn.get_attribute('aria-label') or ''
                    cls = btn.get_attribute('class') or ''
                    if text or data_auto or aria:
                        elements.append(
                            f"BUTTON: text='{text}' data-auto='{data_auto}' "
                            f"aria='{aria}' class='{cls[:60]}'"
                        )
                except StaleElementReferenceException:
                    continue

            # Links with export/save/download in text
            for a in self.browser.find_elements(By.TAG_NAME, 'a'):
                try:
                    text = a.text.strip()[:80].lower()
                    if any(kw in text for kw in ['export', 'save', 'download', 'csv']):
                        href = a.get_attribute('href') or ''
                        data_auto = a.get_attribute('data-auto') or ''
                        elements.append(
                            f"LINK: text='{a.text.strip()[:80]}' "
                            f"data-auto='{data_auto}' href='{href[:80]}'"
                        )
                except StaleElementReferenceException:
                    continue

            # Checkboxes
            for inp in self.browser.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"]'):
                try:
                    data_auto = inp.get_attribute('data-auto') or ''
                    aria = inp.get_attribute('aria-label') or ''
                    name = inp.get_attribute('name') or ''
                    elements.append(
                        f"CHECKBOX: data-auto='{data_auto}' "
                        f"aria='{aria}' name='{name}'"
                    )
                except StaleElementReferenceException:
                    continue

            # All elements with data-auto attribute
            for elem in self.browser.find_elements(By.CSS_SELECTOR, '[data-auto]'):
                try:
                    da = elem.get_attribute('data-auto')
                    tag = elem.tag_name
                    text = elem.text.strip()[:60]
                    if any(kw in da.lower() for kw in
                           ['export', 'select', 'save', 'download', 'bulk',
                            'page', 'result', 'toolbar', 'action']):
                        elements.append(
                            f"DATA-AUTO: tag={tag} data-auto='{da}' text='{text}'"
                        )
                except StaleElementReferenceException:
                    continue

            with open(inventory_path, 'w', encoding='utf-8') as f:
                f.write(f"EBSCO Research UI Element Inventory\n")
                f.write(f"URL: {self.browser.current_url}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"{'=' * 70}\n\n")
                for elem_str in elements:
                    f.write(elem_str + '\n')

            print(f"  Element inventory saved: {inventory_path}")
            print(f"  Found {len(elements)} relevant elements")

            # Print key elements to console
            for e in elements:
                if any(kw in e.lower() for kw in ['export', 'select all', 'bulk', 'csv']):
                    print(f"    >> {e}")

        except Exception as e:
            print(f"  Element inventory failed: {e}")

        return screenshot_path, dom_path, inventory_path

    def _set_results_per_page(self, target=50):
        """
        Increase results per page via the 'Show: N' dropdown.
        Selector: button[data-auto="results-per-page-dropdown-toggle"]
        """
        try:
            toggle = self.browser.find_element(
                By.CSS_SELECTOR, 'button[data-auto="results-per-page-dropdown-toggle"]')
            current = toggle.text.strip()  # e.g. "Show: 10"
            print(f"  Current results per page: {current}")

            toggle.click()
            time.sleep(1)

            # Find the dropdown menu and select the highest option (<= target)
            # Options are typically li elements in a dropdown
            options = self.browser.find_elements(
                By.CSS_SELECTOR,
                '[data-auto="results-per-page-dropdown-toggle"] ~ ul li, '
                '[data-auto="results-per-page-dropdown-menu"] li, '
                'ul[role="listbox"] li'
            )

            if not options:
                # Try broader search for dropdown options visible on page
                options = self.browser.find_elements(
                    By.XPATH,
                    '//ul[contains(@class, "dropdown")]//li | '
                    '//div[contains(@class, "dropdown")]//li'
                )

            best_option = None
            best_val = 0
            for opt in options:
                text = opt.text.strip()
                match = re.search(r'(\d+)', text)
                if match:
                    val = int(match.group(1))
                    if val <= target and val > best_val:
                        best_val = val
                        best_option = opt

            if best_option:
                best_option.click()
                time.sleep(3)  # Wait for results to reload
                print(f"  Set results per page to: {best_val}")
                return best_val
            else:
                print(f"  Could not find per-page option (tried {len(options)} items)")
                # Click elsewhere to close dropdown
                self.browser.find_element(By.TAG_NAME, 'body').click()
                time.sleep(0.5)
        except (NoSuchElementException, TimeoutException) as e:
            print(f"  Could not change results per page: {e}")
        return 10  # default

    def _load_all_results(self, total_count):
        """
        Load all results by clicking 'Show more results' repeatedly.
        Returns the number of result items loaded in the DOM.
        """
        loaded = len(self.browser.find_elements(
            By.CSS_SELECTOR, 'article[data-auto="search-result-item"]'))
        max_attempts = (total_count // 10) + 5  # generous limit
        attempt = 0

        while loaded < total_count and attempt < max_attempts:
            if not self._click_show_more():
                break
            time.sleep(3)
            new_loaded = len(self.browser.find_elements(
                By.CSS_SELECTOR, 'article[data-auto="search-result-item"]'))
            if new_loaded <= loaded:
                break  # No new results appeared
            loaded = new_loaded
            attempt += 1
            print(f"    Loaded: {loaded}/{total_count}")

        print(f"  All results loaded: {loaded}")
        return loaded

    def _get_record_checkboxes(self):
        """
        Get per-record checkboxes (excluding the toolbar 'select all' checkbox).
        Returns list of checkbox WebElements in DOM order.
        """
        all_cbs = self.browser.find_elements(
            By.CSS_SELECTOR, 'input[data-auto="bulk-record-checkbox"]')
        record_cbs = []
        for cb in all_cbs:
            try:
                aria = (cb.get_attribute('aria-label') or '').lower()
                if 'select all' not in aria and 'displayed' not in aria:
                    record_cbs.append(cb)
            except StaleElementReferenceException:
                continue
        return record_cbs

    def _select_records_by_index(self, start, end):
        """
        Select specific records by index using individual checkboxes.
        Scrolls each checkbox into view before clicking.
        Returns number of records successfully selected.
        """
        checkboxes = self._get_record_checkboxes()
        if not checkboxes:
            print(f"  No record checkboxes found")
            return 0

        selected = 0
        actual_end = min(end, len(checkboxes))
        for i in range(start, actual_end):
            try:
                cb = checkboxes[i]
                self.browser.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", cb)
                time.sleep(0.15)
                cb.click()
                selected += 1
            except (StaleElementReferenceException, WebDriverException) as e:
                print(f"    Checkbox {i} failed: {e}")
        time.sleep(0.5)
        return selected

    def _deselect_all(self):
        """Deselect all records using 'Deselect all' from the bulk dropdown."""
        # Scroll to top and disable overlapping header pointer-events
        self.browser.execute_script("""
            window.scrollTo(0, 0);
            document.querySelectorAll(
                '[data-auto="selected-databases"], .header_header__group__a8AGL'
            ).forEach(function(el) {
                el.style.setProperty('pointer-events', 'none', 'important');
            });
        """)
        time.sleep(0.5)

        try:
            arrow = self.browser.find_element(
                By.CSS_SELECTOR, 'button[data-auto="bulk-record-arrow-dropdown"]')
            try:
                arrow.click()
            except ElementClickInterceptedException:
                self.browser.execute_script("arguments[0].click();", arrow)
            time.sleep(1)
            menu_items = self.browser.find_elements(
                By.CSS_SELECTOR, '[data-auto="bulk-record-arrow-dropdown-menu"] li')
            for item in menu_items:
                if 'deselect' in item.text.strip().lower():
                    item.click()
                    time.sleep(1)
                    print("  Deselected all records")
                    return True
            # Close dropdown if no deselect option found
            self.browser.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(0.5)
        except Exception as e:
            print(f"  Deselect failed: {e}")
        return False

    def _select_page_records(self):
        """
        Select records on the current page using 'All on this page' dropdown option.
        Falls back to the select-all checkbox. Returns number selected.
        """
        try:
            arrow = WebDriverWait(self.browser, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'button[data-auto="bulk-record-arrow-dropdown"]'))
            )
            arrow.click()
            time.sleep(1)

            menu_items = self.browser.find_elements(
                By.CSS_SELECTOR,
                '[data-auto="bulk-record-arrow-dropdown-menu"] li'
            )

            for item in menu_items:
                text = item.text.strip().lower()
                if 'all on this page' in text or ('select' in text and 'deselect' not in text and 'export' not in text):
                    item.click()
                    time.sleep(1)
                    selected = self._get_selected_count()
                    print(f"  Selected {selected} records via dropdown")
                    return selected

            self.browser.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(0.5)
        except (TimeoutException, NoSuchElementException):
            pass

        # Fallback: checkbox
        self._select_displayed_records()
        return self._get_selected_count()

    def _select_displayed_records(self):
        """
        Click the 'Select all displayed records' checkbox.
        Selector: input[data-auto="bulk-record-checkbox"][aria-label*="Select all"]
        """
        try:
            # The first bulk-record-checkbox in the toolbar is the "select all displayed"
            checkbox = self.browser.find_element(
                By.CSS_SELECTOR,
                'input[data-auto="bulk-record-checkbox"]'
            )
            aria = checkbox.get_attribute('aria-label') or ''
            if 'select all' in aria.lower() or 'displayed' in aria.lower():
                checkbox.click()
                time.sleep(1)
                print(f"  Clicked select-all checkbox (aria: {aria[:60]})")
                return True
            else:
                # It might be a per-record checkbox; find the toolbar one
                checkboxes = self.browser.find_elements(
                    By.CSS_SELECTOR, 'input[data-auto="bulk-record-checkbox"]')
                for cb in checkboxes:
                    cb_aria = cb.get_attribute('aria-label') or ''
                    if 'select all' in cb_aria.lower() or 'displayed' in cb_aria.lower():
                        cb.click()
                        time.sleep(1)
                        print(f"  Clicked select-all checkbox (aria: {cb_aria[:60]})")
                        return True
        except (NoSuchElementException, StaleElementReferenceException) as e:
            print(f"  Select-all checkbox not found: {e}")
        return False

    def _get_selected_count(self):
        """Parse the '0 selected' / 'N selected' count from the toolbar."""
        try:
            elem = self.browser.find_element(
                By.CSS_SELECTOR, 'p[data-auto="bulk-record-header-count"]')
            text = elem.text.strip()  # e.g. "10 selected"
            match = re.search(r'(\d+)', text)
            if match:
                return int(match.group(1))
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        return 0

    def _click_download(self):
        """
        Click the Download button in the toolbar.
        Selector: button[data-auto="tool-button"][aria-label="Download"]

        The toolbar can be covered by the "selected databases" or header bar
        after scrolling, so we disable pointer-events on overlapping elements.
        """
        # Scroll to top — toolbar is at the top
        self.browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        # Re-apply download path (CDP command can expire after DOM changes)
        try:
            self.browser.execute_cdp_cmd('Page.setDownloadBehavior', {
                'behavior': 'allow',
                'downloadPath': self.download_dir.replace('/', '\\')
            })
        except Exception:
            pass

        # Disable pointer-events on overlapping header elements
        self.browser.execute_script("""
            document.querySelectorAll(
                '[data-auto="selected-databases"], .header_header__group__a8AGL'
            ).forEach(function(el) {
                el.style.setProperty('pointer-events', 'none', 'important');
            });
        """)
        time.sleep(0.3)

        try:
            download_btn = self.browser.find_element(
                By.CSS_SELECTOR, 'button[data-auto="tool-button"][aria-label="Download"]')
            try:
                download_btn.click()
                time.sleep(2)
                print("  Clicked Download button")
                return True
            except ElementClickInterceptedException:
                self.browser.execute_script("arguments[0].click();", download_btn)
                time.sleep(2)
                print("  Clicked Download button (JS fallback)")
                return True
        except NoSuchElementException:
            pass
        print("  ERROR: Download button not found")
        return False

    def _handle_download_dialog(self):
        """
        Handle the bulk download modal:
        1. Select CSV format (radio button labeled "CSV")
        2. Click the Download button (data-auto="bulk-download-modal-download-button")

        Returns True if dialog was handled, False otherwise.
        """
        # Wait for modal to appear
        time.sleep(1)

        # Select CSV format
        try:
            csv_label = WebDriverWait(self.browser, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//label[contains(., "CSV")]'))
            )
            csv_label.click()
            time.sleep(0.5)
            print("  Selected CSV format")
        except (TimeoutException, NoSuchElementException):
            print("  WARNING: Could not find CSV format label")

        # Click the Download button in the modal
        try:
            download_btn = WebDriverWait(self.browser, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'button[data-auto="bulk-download-modal-download-button"]'))
            )
            download_btn.click()
            time.sleep(2)
            print("  Clicked modal Download button")
            return True
        except (TimeoutException, NoSuchElementException):
            # Fallback
            try:
                download_btn = self.browser.find_element(
                    By.XPATH,
                    '//div[contains(@class, "modal")]//button[contains(., "Download")]')
                download_btn.click()
                time.sleep(2)
                print("  Clicked modal Download button (fallback)")
                return True
            except NoSuchElementException:
                pass

        print("  Could not find Download button in modal")
        return False

    def _close_download_modal(self):
        """Close the bulk download modal after download completes."""
        # Try the Cancel/Close button
        try:
            close_btn = self.browser.find_element(
                By.CSS_SELECTOR, 'button[data-auto="bulk-download-modal-close-button"]')
            close_btn.click()
            time.sleep(1)
            print("  Closed download modal")
            return True
        except NoSuchElementException:
            pass

        # Fallback: press Escape
        try:
            self.browser.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            time.sleep(1)
            print("  Closed modal via Escape")
            return True
        except Exception:
            pass

        # Fallback: click the overlay to close
        try:
            overlay = self.browser.find_element(
                By.CSS_SELECTOR, '.eb-modal__overlay')
            self.browser.execute_script("arguments[0].click();", overlay)
            time.sleep(1)
            return True
        except Exception:
            pass

        return False

    def try_select_and_export(self, total_count):
        """
        Export all results via select + Download toolbar button.

        NOTE: "Export results (Up to 25,000)" from the bulk dropdown sends results
        via email (requires MyEBSCO account) — NOT a direct download. So we use
        the Download toolbar button instead, which gives a direct CSV download.

        EBSCO limits selection to 50 records at a time (maxBulkSelectionCountLimit).
        For >50 results, we download in batches of 50 using individual record
        checkboxes. Results are loaded incrementally (via "Show more") — only
        as many as needed for each batch, to avoid a massive DOM.

        Returns list of downloaded file paths.
        """
        downloaded_files = []
        BATCH_SIZE = 50

        # Step 1: Set results per page to 50
        per_page = self._set_results_per_page(50)
        time.sleep(2)

        loaded = len(self.browser.find_elements(
            By.CSS_SELECTOR, 'article[data-auto="search-result-item"]'))

        num_batches = max(1, (total_count + BATCH_SIZE - 1) // BATCH_SIZE)
        print(f"\n  Downloading {total_count} results in {num_batches} batch(es) of up to {BATCH_SIZE}")
        if num_batches > 5:
            est_minutes = num_batches * 0.75  # ~45s per batch
            print(f"  Estimated time: ~{est_minutes:.0f} minutes")

        batch_start_time = time.time()
        consecutive_load_failures = 0
        MAX_LOAD_FAILURES = 5

        for batch_idx in range(num_batches):
            start = batch_idx * BATCH_SIZE
            end = min(start + BATCH_SIZE, total_count)
            print(f"\n  --- Batch {batch_idx + 1}/{num_batches}: records {start + 1}-{end} ---")

            # Step 2: Load enough results for this batch (incremental)
            load_attempts = 0
            while loaded < end:
                if not self._click_show_more():
                    load_attempts += 1
                    if load_attempts >= 3:
                        break
                    time.sleep(2)
                    continue
                load_attempts = 0
                time.sleep(3)
                new_loaded = len(self.browser.find_elements(
                    By.CSS_SELECTOR, 'article[data-auto="search-result-item"]'))
                if new_loaded <= loaded:
                    # No new records appeared despite button click — retry
                    load_attempts += 1
                    if load_attempts >= 3:
                        break
                    time.sleep(2)
                    continue
                load_attempts = 0
                consecutive_load_failures = 0
                loaded = new_loaded
                if loaded % 100 == 0 or loaded == end:
                    print(f"    Loaded: {loaded}/{total_count}")

            if loaded <= start:
                consecutive_load_failures += 1
                print(f"  Not enough results loaded ({loaded} <= {start}). "
                      f"Failure {consecutive_load_failures}/{MAX_LOAD_FAILURES}")
                if consecutive_load_failures >= MAX_LOAD_FAILURES:
                    print(f"  Too many consecutive load failures. Stopping.")
                    break
                continue  # Skip this batch but try next (maybe Show more just needs another click)

            actual_end = min(end, loaded)

            # Step 3: Select records for this batch
            if num_batches == 1:
                self._select_page_records()
            else:
                selected = self._select_records_by_index(start, actual_end)
                if selected == 0:
                    print("  No records selected — skipping batch")
                    continue

            actual_selected = self._get_selected_count()
            print(f"  {actual_selected} records selected")

            if actual_selected == 0:
                continue

            # Step 4: Download
            dl_snapshot = self._snapshot_download_dir()
            if not self._click_download():
                break
            if self._handle_download_dialog():
                path = self._wait_for_download(dl_snapshot)
                if path:
                    # Rename to unique name so next download doesn't overwrite
                    base, ext = os.path.splitext(os.path.basename(path))
                    unique_name = f"{base}_batch{batch_idx + 1}_{int(time.time())}{ext}"
                    new_path = os.path.join(os.path.dirname(path), unique_name)
                    try:
                        os.rename(path, new_path)
                        path = new_path
                    except OSError:
                        pass
                    downloaded_files.append(path)
                    print(f"  Downloaded: {os.path.basename(path)}")
                self._close_download_modal()
            else:
                self._close_download_modal()
                break

            time.sleep(1)

            # Deselect before next batch
            if batch_idx < num_batches - 1:
                self._deselect_all()
                time.sleep(1)

            # Progress / ETA
            if num_batches > 3 and len(downloaded_files) > 0:
                elapsed = time.time() - batch_start_time
                per_batch = elapsed / (batch_idx + 1)
                remaining = per_batch * (num_batches - batch_idx - 1)
                print(f"  Progress: {batch_idx + 1}/{num_batches} batches, "
                      f"~{remaining / 60:.0f} min remaining")

        print(f"\n  Export complete: {len(downloaded_files)} file(s) downloaded")
        return downloaded_files

    def _click_show_more(self, max_retries=3):
        """Click 'Show more results' pagination button. Scrolls to button first."""
        selectors = [
            (By.CSS_SELECTOR, 'button.eb-pagination__button'),
            (By.XPATH, '//button[contains(., "Show more results")]'),
            (By.XPATH, '//button[contains(., "Show more")]'),
        ]

        for attempt in range(max_retries):
            # Scroll to bottom where the button lives
            self.browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

            for by, selector in selectors:
                try:
                    btn = self.browser.find_element(by, selector)
                    # Scroll the button into view
                    self.browser.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", btn)
                    time.sleep(0.5)
                    if btn.is_displayed() and btn.is_enabled():
                        try:
                            btn.click()
                        except ElementClickInterceptedException:
                            self.browser.execute_script("arguments[0].click();", btn)
                        return True
                except (NoSuchElementException, StaleElementReferenceException):
                    continue

            if attempt < max_retries - 1:
                time.sleep(2)

        print("  'Show more results' button not found — all results loaded or page issue")
        return False

    def _wait_for_download(self, initial_state=None, timeout=EXPORT_WAIT_TIMEOUT):
        """
        Wait for a new or updated file in the download directory.

        Parameters
        ----------
        initial_state : dict or set
            If dict: {filename: mtime} snapshot before download was triggered.
            If set: set of filenames (legacy — only detects new files).
            If None: captures current state automatically.

        Detects both NEW files and OVERWRITTEN files (same name, newer mtime).
        Also checks Chrome's default Downloads folder as fallback.
        """
        import shutil
        print(f"    Waiting for download (up to {timeout}s)...")

        # Normalize initial_state to {filename: mtime} dict
        if initial_state is None:
            initial_state = self._snapshot_download_dir()
        elif isinstance(initial_state, set):
            # Legacy: convert set of filenames to dict with current mtimes
            state = {}
            for f in initial_state:
                fpath = os.path.join(self.download_dir, f)
                try:
                    state[f] = os.path.getmtime(fpath)
                except OSError:
                    state[f] = 0
            initial_state = state

        # Also watch Chrome's default Downloads folder
        chrome_downloads = os.path.join(os.path.expanduser('~'), 'Downloads')
        initial_chrome = {}
        if os.path.exists(chrome_downloads):
            for f in os.listdir(chrome_downloads):
                fpath = os.path.join(chrome_downloads, f)
                try:
                    initial_chrome[f] = os.path.getmtime(fpath)
                except OSError:
                    initial_chrome[f] = 0

        start = time.time()
        while time.time() - start < timeout:
            # Check our download directory for new or updated files
            for f in os.listdir(self.download_dir):
                if f.endswith('.crdownload') or f.endswith('.tmp'):
                    continue
                fpath = os.path.join(self.download_dir, f)
                try:
                    mtime = os.path.getmtime(fpath)
                except OSError:
                    continue

                if f not in initial_state:
                    # Brand new file
                    print(f"    Downloaded: {f}")
                    return fpath
                if mtime > initial_state[f] + 1:
                    # Existing file was overwritten/updated
                    print(f"    Downloaded (overwritten): {f}")
                    return fpath

            # Fallback: check Chrome's default Downloads folder
            if os.path.exists(chrome_downloads):
                for f in os.listdir(chrome_downloads):
                    if not f.lower().endswith('.csv'):
                        continue
                    if f.endswith('.crdownload') or f.endswith('.tmp'):
                        continue
                    fpath = os.path.join(chrome_downloads, f)
                    try:
                        mtime = os.path.getmtime(fpath)
                    except OSError:
                        continue
                    if f not in initial_chrome or mtime > initial_chrome[f] + 1:
                        dst = os.path.join(self.download_dir, f)
                        try:
                            shutil.move(fpath, dst)
                            print(f"    Downloaded (moved from Downloads): {f}")
                            return dst
                        except Exception as e:
                            print(f"    File in Downloads but move failed: {e}")

            time.sleep(1)
        print(f"    Download timeout after {timeout}s")
        return None

    def _snapshot_download_dir(self):
        """Capture {filename: mtime} snapshot of the download directory."""
        state = {}
        for f in os.listdir(self.download_dir):
            fpath = os.path.join(self.download_dir, f)
            try:
                state[f] = os.path.getmtime(fpath)
            except OSError:
                state[f] = 0
        return state

    def close(self):
        """Close browser (profile is preserved for next session)."""
        if self.browser:
            try:
                self.browser.quit()
            except Exception:
                pass
            self.browser = None


# =============================================================================
# SEARCH & EXPORT AUTOMATION
# =============================================================================
def automate_search_and_export(search_terms, exports_dir, checkpoint_path=None,
                               resume=False, discover_only=False, from_year=None):
    """
    Automate EBSCO Research EconLit search and CSV export for all search terms.

    Parameters:
    -----------
    search_terms : list of str
    exports_dir : str
        Directory where CSVs will be downloaded
    checkpoint_path : str or None
        Path for tracking which search terms have been exported
    resume : bool
        Skip already-exported search terms
    discover_only : bool
        Just open browser, search, save debug info — don't try to export
    from_year : int or None
        Restrict to publications from this year onward (EBSCO DT field code)

    Returns:
    --------
    dict : Results summary with counts and file paths
    """
    # Load checkpoint
    checkpoint = {'exported_terms': [], 'files': []}
    if checkpoint_path and resume and os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            checkpoint = json.load(f)
        print(f"  [RESUME] {len(checkpoint['exported_terms'])} terms already exported")

    terms_to_process = [
        t for t in search_terms
        if t not in checkpoint['exported_terms']
    ]
    if not terms_to_process:
        print("  All search terms already exported.")
        return checkpoint

    # Launch browser
    browser = EBSCOBrowser(download_dir=exports_dir)
    browser.launch()
    browser.verify_stanford_access()

    results = {'exported_terms': list(checkpoint['exported_terms']),
               'files': list(checkpoint['files']),
               'result_counts': {}}

    try:
        for i, term in enumerate(terms_to_process):
            print(f"\n  [{i+1}/{len(terms_to_process)}] Search term: \"{term}\"")

            # Search
            count = browser.search_econlit(term, from_year=from_year)
            results['result_counts'][term] = count

            if count == 0:
                print(f"    No results — skipping export")
                results['exported_terms'].append(term)
                continue

            # Wait a moment for toolbar elements to stabilize
            time.sleep(3)

            if discover_only:
                # Just save debug info
                browser.discover_ui_elements(label=f"search_{i+1}")
                continue

            # Automated select + export (handles pagination if needed)
            print(f"  Exporting {count} results...")
            csv_paths = browser.try_select_and_export(count)

            if csv_paths:
                print(f"  Export complete: {len(csv_paths)} file(s)")
                results['files'].extend(csv_paths)
                results['exported_terms'].append(term)
            else:
                # Save debug info for troubleshooting selectors
                browser.discover_ui_elements(label=f"export_fail_{i+1}")
                print(f"\n  ERROR: Automated export failed for \"{term}\"")
                print(f"  Debug files saved to: {DEBUG_DIR}")
                print(f"  Check the element inventory to find correct selectors.")
                results['failed_terms'] = results.get('failed_terms', [])
                results['failed_terms'].append(term)

            # Save checkpoint after each term
            if checkpoint_path:
                with open(checkpoint_path, 'w') as f:
                    json.dump(results, f, indent=2)

            # Delay between searches
            if i < len(terms_to_process) - 1:
                print(f"  Waiting {SEARCH_DELAY}s before next search...")
                time.sleep(SEARCH_DELAY)

    finally:
        if discover_only:
            print("\n  Discovery mode complete. Check debug files in:")
            print(f"    {DEBUG_DIR}")
        browser.close()

    return results


# =============================================================================
# CSV LOADING & PARSING
# =============================================================================
def load_all_csvs(exports_dir):
    """Load all CSV files from the exports directory."""
    csv_files = sorted([
        os.path.join(exports_dir, f)
        for f in os.listdir(exports_dir)
        if f.lower().endswith('.csv')
    ])
    if not csv_files:
        return pd.DataFrame()

    print(f"  Found {len(csv_files)} CSV file(s)")
    dfs = []
    for path in csv_files:
        fname = os.path.basename(path)
        try:
            df = pd.read_csv(path, encoding='utf-8')
            if len(df) > 0:
                print(f"    {fname}: {len(df)} records")
                dfs.append(df)
            else:
                print(f"    {fname}: empty")
        except Exception as e:
            print(f"    {fname}: ERROR: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def parse_ebsco_dataframe(raw_df):
    """
    Parse EBSCO CSV export into standardized format.

    EBSCO CSV columns:
        an, title, abstract, publicationDate, contributors, docTypes,
        pubTypes, coverDate, peerReviewed, source, subjects, doi, ...
    """
    records = []
    for _, row in raw_df.iterrows():
        # Publication year
        pub_year = None
        pub_date_raw = str(row.get('publicationDate', ''))
        if pub_date_raw and pub_date_raw != 'nan':
            year_match = re.match(r'^(\d{4})', pub_date_raw)
            if year_match:
                pub_year = int(year_match.group(1))

        # Authors
        contributors = str(row.get('contributors', ''))
        if contributors and contributors != 'nan':
            authors = contributors.replace(' ; ', ' | ')
            author_count = len(contributors.split(' ; '))
        else:
            authors = ''
            author_count = 0

        # Document type
        doc_type = str(row.get('docTypes', row.get('pubTypes', '')))
        if doc_type == 'nan':
            doc_type = ''

        # Abstract
        abstract = clean_abstract_text(row.get('abstract', ''))

        # DOI
        doi_raw = str(row.get('doi', ''))
        doi = doi_raw if doi_raw != 'nan' else ''

        records.append({
            'econlit_id': str(row.get('an', '')),
            'title': str(row.get('title', '')).strip(),
            'abstract': abstract,
            'authors': authors,
            'author_count': author_count,
            'publication_year': pub_year,
            'publication_date': pub_date_raw if pub_date_raw != 'nan' else '',
            'source_name': str(row.get('source', '')) if str(row.get('source', '')) != 'nan' else '',
            'document_type': doc_type,
            'subject_terms': str(row.get('subjects', '')) if str(row.get('subjects', '')) != 'nan' else '',
            'peer_reviewed': str(row.get('peerReviewed', '')).lower() == 'true',
            'doi': doi,
            'issns': str(row.get('issns', '')) if str(row.get('issns', '')) != 'nan' else '',
            'volume': str(row.get('volume', '')) if str(row.get('volume', '')) != 'nan' else '',
            'issue': str(row.get('issue', '')) if str(row.get('issue', '')) != 'nan' else '',
            'page_start': str(row.get('pageStart', '')) if str(row.get('pageStart', '')) != 'nan' else '',
            'page_end': str(row.get('pageEnd', '')) if str(row.get('pageEnd', '')) != 'nan' else '',
            'is_open_access': str(row.get('isOpenAccess', '')).lower() == 'true',
            'language': str(row.get('language', '')) if str(row.get('language', '')) != 'nan' else '',
            'publisher': str(row.get('publisher', '')) if str(row.get('publisher', '')) != 'nan' else '',
            'econlit_url': str(row.get('plink', '')) if str(row.get('plink', '')) != 'nan' else '',
        })

    return pd.DataFrame(records)


# =============================================================================
# PAPER TYPE CLASSIFICATION
# =============================================================================
def classify_paper_type(row):
    """Classify paper as journal_article or working_paper from EconLit metadata."""
    doc_lower = str(row.get('document_type', '')).lower()
    source_lower = str(row.get('source_name', '')).lower()

    if any(wp in doc_lower for wp in ['working paper', 'discussion paper']):
        return 'working_paper'
    if any(wp in source_lower for wp in ['nber', 'ssrn', 'working paper', 'discussion paper']):
        return 'working_paper'
    if 'journal article' in doc_lower or 'article' in doc_lower:
        return 'journal_article'
    return 'journal_article'


# =============================================================================
# SEARCH TERM MATCHING
# =============================================================================
def match_search_terms(df, search_terms):
    """For each paper, identify which search terms appear in title or abstract."""
    title_lower = df['title'].fillna('').str.lower()
    abstract_lower = df['abstract'].fillna('').str.lower()
    text = title_lower + ' ' + abstract_lower

    matched = pd.Series([''] * len(df), index=df.index)
    for term in search_terms:
        term_lower = term.lower()
        mask = text.str.contains(re.escape(term_lower), regex=True, na=False)
        matched = matched.where(~mask, matched + term + '|')

    df['search_terms_matched'] = matched.str.rstrip('|')
    return df


# =============================================================================
# DEDUPLICATION
# =============================================================================
def deduplicate(df):
    """Deduplicate papers by normalized title, keeping the record with the most metadata."""
    if len(df) == 0:
        return df

    df['normalized_title'] = df['title'].apply(normalize_title)

    # Score each row by metadata completeness
    df['_meta_score'] = (
        df['abstract'].fillna('').str.len().clip(upper=1) +
        df['doi'].fillna('').str.len().clip(upper=1) +
        df['subject_terms'].fillna('').str.len().clip(upper=1) +
        df['authors'].fillna('').str.len().clip(upper=1)
    )
    df = df.sort_values('_meta_score', ascending=False)

    pre_dedup = len(df)
    df = df.drop_duplicates(subset=['normalized_title'], keep='first')
    df = df.drop(columns=['_meta_score'])
    post_dedup = len(df)

    if pre_dedup > post_dedup:
        print(f"  Deduplicated: {pre_dedup} -> {post_dedup} ({pre_dedup - post_dedup} duplicates)")

    return df


# =============================================================================
# ABSTRACT RECOVERY (CrossRef + Semantic Scholar + Europe PMC)
# =============================================================================
def recover_abstract_crossref(doi):
    """Recover abstract from CrossRef API using DOI."""
    if not doi or pd.isna(doi):
        return None, 'no_doi'
    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None, 'empty_doi'
    try:
        crossref_rate_limiter.wait()
        url = f"{CROSSREF_API}/{clean_doi}"
        response = requests.get(url, params={'mailto': USER_EMAIL}, timeout=15)
        if response.status_code == 404:
            return None, 'crossref_not_found'
        response.raise_for_status()
        data = response.json()
        abstract_html = data.get('message', {}).get('abstract', '')
        if abstract_html:
            from bs4 import BeautifulSoup
            abstract = BeautifulSoup(abstract_html, 'html.parser').get_text(separator=' ').strip()
            if len(abstract) > 50:
                return abstract, None
        return None, 'crossref_no_abstract'
    except Exception as e:
        return None, f'crossref_error: {str(e)[:80]}'


def recover_abstract_ss(doi):
    """Recover abstract from Semantic Scholar API using DOI."""
    if not doi or pd.isna(doi):
        return None, 'no_doi'
    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None, 'empty_doi'
    try:
        ss_rate_limiter.wait()
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{clean_doi}"
        headers = {}
        if SS_API_KEY:
            headers['x-api-key'] = SS_API_KEY
        response = requests.get(url, params={'fields': 'abstract'}, headers=headers, timeout=15)
        if response.status_code == 404:
            return None, 'ss_not_found'
        response.raise_for_status()
        abstract = response.json().get('abstract', '')
        if abstract and len(abstract) > 50:
            return abstract, None
        return None, 'ss_no_abstract'
    except Exception as e:
        return None, f'ss_error: {str(e)[:80]}'


def recover_abstract_crossref_title(title):
    """Recover DOI and abstract from CrossRef by title search (fallback for papers without DOI)."""
    if not title or pd.isna(title):
        return None, None, 'no_title'
    try:
        crossref_rate_limiter.wait()
        response = requests.get(
            CROSSREF_API,
            params={'query.title': title, 'rows': 3, 'mailto': USER_EMAIL},
            timeout=15
        )
        response.raise_for_status()
        items = response.json().get('message', {}).get('items', [])
        norm_query = normalize_title(title)
        for item in items:
            item_title = item.get('title', [''])[0] if item.get('title') else ''
            if normalize_title(item_title) == norm_query:
                doi = item.get('DOI', '')
                abstract_html = item.get('abstract', '')
                abstract = None
                if abstract_html:
                    from bs4 import BeautifulSoup
                    abstract = BeautifulSoup(abstract_html, 'html.parser').get_text(separator=' ').strip()
                    if len(abstract) <= 50:
                        abstract = None
                return doi, abstract, None
        return None, None, 'title_not_matched'
    except Exception as e:
        return None, None, f'crossref_title_error: {str(e)[:80]}'


def recover_abstract_epmc(doi):
    """Recover abstract from Europe PMC API using DOI."""
    clean_doi = normalize_doi(doi)
    if not clean_doi:
        return None, 'no_doi'
    try:
        epmc_rate_limiter.wait()
        url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        params = {
            'query': f'DOI:"{clean_doi}"',
            'format': 'json',
            'resultType': 'core',
            'pageSize': 1,
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None, f'epmc_http_{resp.status_code}'
        results = resp.json().get('resultList', {}).get('result', [])
        if results:
            abstract = results[0].get('abstractText', '')
            if abstract and len(abstract.strip()) > 50:
                return abstract.strip(), None
        return None, 'epmc_no_abstract'
    except Exception as e:
        return None, f'epmc_error: {str(e)[:80]}'


def recover_missing_abstracts(df, checkpoint_path=None):
    """
    Recover missing abstracts and DOIs using CrossRef and Semantic Scholar.
    Times the first 20 papers to estimate total recovery time.
    """
    if 'abstract_source' not in df.columns:
        df['abstract_source'] = ''

    has_abstract = df['abstract'].fillna('').astype(str).str.len() > 50
    df.loc[has_abstract & (df['abstract_source'] == ''), 'abstract_source'] = 'EconLit'

    # Pass 1: Recover DOIs via CrossRef title search for papers without DOIs
    no_doi_mask = df['doi'].fillna('').str.len() == 0
    no_doi_indices = df.index[no_doi_mask].tolist()
    if no_doi_indices:
        print(f"\n  Recovering DOIs via CrossRef title search ({len(no_doi_indices)} papers)...")
        doi_recovered = 0
        abs_recovered = 0
        t0 = time.time()
        for i, idx in enumerate(no_doi_indices):
            title = df.loc[idx, 'title']
            doi, abstract, error = recover_abstract_crossref_title(title)
            if doi:
                df.loc[idx, 'doi'] = doi
                doi_recovered += 1
            if abstract and (not df.loc[idx, 'abstract'] or len(str(df.loc[idx, 'abstract'])) <= 50):
                df.loc[idx, 'abstract'] = abstract
                df.loc[idx, 'abstract_source'] = 'CrossRef_title'
                abs_recovered += 1
            if (i + 1) == 20:
                elapsed = time.time() - t0
                rate = elapsed / 20
                est_total = rate * len(no_doi_indices)
                print(f"    Timing: 20 papers in {elapsed:.0f}s ({rate:.1f}s/paper). "
                      f"Estimated total: ~{est_total/60:.0f} min")
            if (i + 1) % 20 == 0:
                print(f"    Progress: {i+1}/{len(no_doi_indices)} | DOIs: {doi_recovered} | Abstracts: {abs_recovered}")
        print(f"  DOI recovery: {doi_recovered}/{len(no_doi_indices)} | Abstract recovery: {abs_recovered}")

    # Pass 2: Recover abstracts via DOI-based APIs
    has_abstract = df['abstract'].fillna('').astype(str).str.len() > 50
    has_doi = df['doi'].fillna('').str.len() > 0
    missing_mask = ~has_abstract & has_doi
    missing_indices = df.index[missing_mask].tolist()

    if not missing_indices:
        print(f"  No papers need abstract recovery (all have abstracts or no DOI)")
        return df

    print(f"\n  Recovering abstracts via DOI ({len(missing_indices)} papers)...")
    recovered = 0
    t0 = time.time()
    for i, idx in enumerate(missing_indices):
        doi = df.loc[idx, 'doi']
        # Try CrossRef first (18.1% success, fast)
        abstract, error = recover_abstract_crossref(doi)
        if abstract:
            df.loc[idx, 'abstract'] = abstract
            df.loc[idx, 'abstract_source'] = 'CrossRef'
            recovered += 1
        else:
            # Try Europe PMC (11.2% success, fast)
            abstract, error = recover_abstract_epmc(doi)
            if abstract:
                df.loc[idx, 'abstract'] = abstract
                df.loc[idx, 'abstract_source'] = 'EuropePMC'
                recovered += 1
            else:
                # Try Semantic Scholar (1.1% success, slow)
                abstract, error = recover_abstract_ss(doi)
                if abstract:
                    df.loc[idx, 'abstract'] = abstract
                    df.loc[idx, 'abstract_source'] = 'SemanticScholar_recovery'
                    recovered += 1
        if (i + 1) == 20:
            elapsed = time.time() - t0
            rate = elapsed / 20
            est_total = rate * len(missing_indices)
            print(f"    Timing: 20 papers in {elapsed:.0f}s ({rate:.1f}s/paper). "
                  f"Estimated total: ~{est_total/60:.0f} min")
        if (i + 1) % 20 == 0:
            print(f"    Progress: {i+1}/{len(missing_indices)} | Recovered: {recovered}")
            if checkpoint_path:
                df.to_parquet(checkpoint_path, index=False, engine='pyarrow')

    if checkpoint_path:
        df.to_parquet(checkpoint_path, index=False, engine='pyarrow')

    print(f"  Abstract recovery complete: {recovered}/{len(missing_indices)} recovered")
    return df


# =============================================================================
# RELEVANCE FILTERING
# =============================================================================
def filter_by_relevance(df, search_terms):
    """
    Filter papers by search term presence in title or abstract.
    Papers without abstracts are kept (can't verify relevance).
    """
    if len(df) == 0:
        return df

    title = df['title'].fillna('').str.lower()
    abstract = df['abstract'].fillna('').str.lower()
    has_abstract = abstract.str.len() > 10
    text = title + ' ' + abstract

    any_term_found = pd.Series(False, index=df.index)
    for term in search_terms:
        any_term_found = any_term_found | text.str.contains(
            re.escape(term.lower()), regex=True, na=False
        )

    keep = any_term_found | ~has_abstract
    return df[keep].copy()


def validate_acronym_matches(df):
    """
    For papers matched only by short acronyms (e.g., 'TCJA'),
    validate case-sensitive whole-word match.
    """
    if 'search_terms_matched' not in df.columns:
        return df, 0

    all_terms = set()
    for val in df['search_terms_matched'].dropna():
        for t in str(val).split('|'):
            all_terms.add(t.strip())
    acronyms = {t for t in all_terms if len(t) <= 4 and t == t.upper() and t.isalpha()}

    if not acronyms:
        return df, 0

    print(f"    Case-sensitive validation for acronyms: {acronyms}")

    text = df['title'].fillna('').astype(str) + ' ' + df['abstract'].fillna('').astype(str)
    acronym_found = pd.Series(False, index=df.index)
    for acr in acronyms:
        acronym_found = acronym_found | text.str.contains(
            r'\b' + re.escape(acr) + r'\b', regex=True, na=False
        )

    def has_regular_term(terms_str):
        terms = [t.strip() for t in str(terms_str).split('|')]
        return any(t not in acronyms for t in terms if t)

    has_regular = df['search_terms_matched'].apply(has_regular_term)
    keep = has_regular | acronym_found
    n_dropped = int((~keep).sum())

    if n_dropped > 0:
        print(f"    Acronym filter: dropped {n_dropped} of {len(df)} papers")

    return df[keep].copy(), n_dropped


# =============================================================================
# PIPELINE: PROCESS ONE POLICY
# =============================================================================
def process_policy(policy_row, resume=False, process_only=False, discover_only=False):
    """
    Process a single policy through the EconLit pipeline.

    Phase 1 (browser): Search EBSCO EconLit and export CSVs
    Phase 2 (processing): Parse, deduplicate, recover abstracts, filter

    Parameters:
    -----------
    policy_row : pd.Series
        Row from policies DataFrame
    resume : bool
        Resume from checkpoint
    process_only : bool
        Skip browser automation, just process existing CSVs
    discover_only : bool
        Open browser, search, save debug info — don't export
    """
    policy_name = policy_row['policy_name']
    policy_abbr = policy_row['policy_abbreviation']
    policy_year = policy_row['policy_year']
    policy_category = policy_row['policy_category']
    search_terms = [t.strip() for t in policy_row['search_terms'].split('|')]

    print(f"\n{'='*80}")
    print(f"PIPELINE A: {policy_name} ({policy_abbr}) — EconLit")
    print(f"Year: {policy_year} | Category: {policy_category}")
    print(f"Search terms: {search_terms}")
    mode = "DISCOVER" if discover_only else ("PROCESS-ONLY" if process_only else "FULL")
    print(f"Mode: {mode}" + (" (RESUME)" if resume else ""))
    print(f"{'='*80}")

    start_time = time.time()

    # Paths
    policy_exports_dir = os.path.join(EXPORTS_DIR, policy_abbr)
    os.makedirs(policy_exports_dir, exist_ok=True)
    export_checkpoint_path = os.path.join(TMP_DIR, f"{policy_abbr}_econlit_export_checkpoint.json")
    recovery_checkpoint_path = os.path.join(TMP_DIR, f"{policy_abbr}_econlit_recovery_checkpoint.parquet")
    raw_output_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_raw.parquet")
    filtered_output_path = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_filtered.parquet")

    # Dropbox safety for policy exports dir
    ignore_path = os.path.join(policy_exports_dir, ".dropboxignore")
    if not os.path.exists(ignore_path):
        with open(ignore_path, 'w') as f:
            f.write("*\n")

    # If final outputs already exist on resume, skip entirely
    if resume and os.path.exists(raw_output_path) and os.path.exists(filtered_output_path):
        print(f"  [RESUME] Final outputs already exist, skipping entirely")
        metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_metadata.json")
        if os.path.exists(metadata_file):
            with open(metadata_file) as f:
                return json.load(f)
        return {'policy_abbreviation': policy_abbr, 'skipped': True}

    # ===================== Phase 1: Browser Automation =====================
    if not process_only:
        print(f"\n  Phase 1: Browser automation — search and export")
        export_results = automate_search_and_export(
            search_terms=search_terms,
            exports_dir=policy_exports_dir,
            checkpoint_path=export_checkpoint_path,
            resume=resume,
            discover_only=discover_only,
            from_year=int(policy_year),
        )
        if discover_only:
            print("\n  Discovery mode — skipping processing phase.")
            return {'policy_abbreviation': policy_abbr, 'discover_only': True,
                    'result_counts': export_results.get('result_counts', {})}

        print(f"\n  Export summary:")
        print(f"    Terms exported: {len(export_results.get('exported_terms', []))}/{len(search_terms)}")
        print(f"    Files downloaded: {len(export_results.get('files', []))}")
        for term, count in export_results.get('result_counts', {}).items():
            print(f"    \"{term}\": {count} results")

    # ===================== Phase 2: CSV Processing =====================
    print(f"\n  Phase 2: Processing exported CSVs")

    # Check for recovery checkpoint (abstract recovery was interrupted)
    skip_to_recovery = False
    if resume and os.path.exists(recovery_checkpoint_path):
        print(f"  [RESUME] Recovery checkpoint found — resuming abstract recovery")
        df = pd.read_parquet(recovery_checkpoint_path)
        skip_to_recovery = True

    if not skip_to_recovery:
        # Step 1: Load CSV exports
        print(f"\n  Step 1: Loading CSV exports from {policy_exports_dir}")
        raw_df = load_all_csvs(policy_exports_dir)

        if len(raw_df) == 0:
            print(f"\n  WARNING: No CSV files found in: {policy_exports_dir}")
            if process_only:
                print(f"  To export CSVs, run without --process-only")
            return {'policy_abbreviation': policy_abbr, 'total_papers': 0}

        print(f"  Total raw records: {len(raw_df)}")

        # Step 2: Parse EBSCO format
        print(f"\n  Step 2: Parsing EBSCO CSV format...")
        df = parse_ebsco_dataframe(raw_df)
        print(f"  Parsed {len(df)} papers")

        # Step 3: Deduplicate
        print(f"\n  Step 3: Deduplicating...")
        df = deduplicate(df)

        # Step 4: Classify paper type
        df['paper_type'] = df.apply(classify_paper_type, axis=1)

        # Step 5: Match search terms
        print(f"\n  Step 5: Matching search terms...")
        df = match_search_terms(df, search_terms)
        matched_any = df['search_terms_matched'].str.len() > 0
        print(f"  Papers matching at least one term: {matched_any.sum()}/{len(df)}")

        # Step 6: Acronym validation
        df, acronym_filtered = validate_acronym_matches(df)

        # Step 7: Year filter
        print(f"\n  Step 7: Filtering by publication year (>= {policy_year})...")
        df['publication_year'] = pd.to_numeric(df['publication_year'], errors='coerce')
        pre_year = len(df)
        df = df[df['publication_year'].isna() | (df['publication_year'] >= policy_year)]
        year_filtered = pre_year - len(df)
        print(f"    Before: {pre_year} | Filtered: {year_filtered} | After: {len(df)}")

        # Add policy metadata
        df['policy_studied'] = policy_name
        df['policy_year'] = policy_year
        df['policy_abbreviation'] = policy_abbr
        df['policy_category'] = policy_category
        df['data_source'] = 'EconLit'
        df['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
    else:
        acronym_filtered = 0
        year_filtered = 0

    # Step 8: Recover missing abstracts
    print(f"\n  Step 8: Recovering missing abstracts...")
    df = recover_missing_abstracts(df, checkpoint_path=recovery_checkpoint_path)

    # Save raw output
    df.to_parquet(raw_output_path, index=False, engine='pyarrow')
    print(f"\n  Saved RAW: {raw_output_path}")
    raw_csv = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_raw.csv")
    df.to_csv(raw_csv, index=False, encoding='utf-8')

    # Step 9: Relevance filtering
    print(f"\n  Step 9: Applying relevance filtering...")
    pre_relevance = len(df)
    filtered = filter_by_relevance(df, search_terms)
    relevance_filtered = pre_relevance - len(filtered)
    print(f"    Before: {pre_relevance} | Filtered: {relevance_filtered} | After: {len(filtered)}")

    filtered, _ = validate_acronym_matches(filtered)

    filtered.to_parquet(filtered_output_path, index=False, engine='pyarrow')
    print(f"  Saved FILTERED: {filtered_output_path}")
    filtered_csv = os.path.join(OUTPUT_DIR, f"{policy_abbr}_papers_econlit_filtered.csv")
    filtered.to_csv(filtered_csv, index=False, encoding='utf-8')

    elapsed = time.time() - start_time

    # Stats
    has_abstract_raw = df['abstract'].fillna('').str.len() > 50
    has_abstract_filtered = filtered['abstract'].fillna('').str.len() > 50
    raw_types = df['paper_type'].value_counts().to_dict() if 'paper_type' in df.columns else {}
    filtered_types = filtered['paper_type'].value_counts().to_dict() if 'paper_type' in filtered.columns else {}

    metadata = {
        'policy_name': policy_name,
        'policy_abbreviation': policy_abbr,
        'policy_year': int(policy_year),
        'policy_category': policy_category,
        'search_terms': search_terms,
        'scrape_date': datetime.now().isoformat(),
        'elapsed_seconds': round(elapsed, 1),
        'raw': {
            'total_papers': len(df),
            'year_filtered': year_filtered,
            'acronym_filtered': acronym_filtered,
            'with_abstract': int(has_abstract_raw.sum()),
            'without_abstract': int((~has_abstract_raw).sum()),
            'abstract_pct': round(100 * has_abstract_raw.mean(), 1) if len(df) > 0 else 0,
            'paper_types': raw_types,
        },
        'filtered': {
            'total_papers': len(filtered),
            'relevance_filtered': relevance_filtered,
            'with_abstract': int(has_abstract_filtered.sum()),
            'without_abstract': int((~has_abstract_filtered).sum()),
            'abstract_pct': round(100 * has_abstract_filtered.mean(), 1) if len(filtered) > 0 else 0,
            'paper_types': filtered_types,
        }
    }

    metadata_file = os.path.join(OUTPUT_DIR, f"{policy_abbr}_econlit_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    # Clean up recovery checkpoint on success
    if os.path.exists(recovery_checkpoint_path):
        os.remove(recovery_checkpoint_path)
        print(f"  Cleaned up recovery checkpoint")

    # Summary
    print(f"\n  {'='*60}")
    print(f"  SUMMARY: {policy_abbr} — Pipeline A (EconLit)")
    print(f"  {'='*60}")
    print(f"  Raw papers: {len(df)}")
    print(f"  Filtered papers: {len(filtered)}")
    print(f"  Abstracts (raw): {has_abstract_raw.sum()}/{len(df)} ({metadata['raw']['abstract_pct']}%)")
    print(f"  Paper types (raw): {raw_types}")
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed/60:.1f} min)")

    return metadata


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description='EconLit Pipeline A — EBSCO Research browser automation + CSV processing')
    parser.add_argument('policies', nargs='*', default=[],
                       help='Policy abbreviations to process (default: all)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from checkpoint')
    parser.add_argument('--process-only', action='store_true',
                       help='Skip browser automation, just process existing CSV exports')
    parser.add_argument('--discover', action='store_true',
                       help='Discovery mode: open browser, search, save screenshots and '
                            'element inventories for debugging (no export)')
    args = parser.parse_args()

    if not args.process_only and not HAS_SELENIUM:
        print("ERROR: Browser automation requires undetected-chromedriver and selenium.")
        print("  pip install undetected-chromedriver selenium")
        print("  Or use --process-only to skip browser and process existing CSVs.")
        sys.exit(1)

    print("=" * 80)
    print("PIPELINE A: EconLit (EBSCO Research)")
    mode = "DISCOVER" if args.discover else ("PROCESS-ONLY" if args.process_only else "FULL")
    print(f"Mode: {mode}" + (" + RESUME" if args.resume else ""))
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    policies_df = load_policies(POLICIES_FILE)

    if args.policies:
        policies_df = policies_df[policies_df['policy_abbreviation'].isin(args.policies)]
        if len(policies_df) == 0:
            print(f"ERROR: No matching policies for: {args.policies}")
            sys.exit(1)

    print(f"\nPolicies to process:")
    for _, row in policies_df.iterrows():
        print(f"  - {row['policy_name']} ({row['policy_abbreviation']})")

    all_metadata = []
    for _, row in policies_df.iterrows():
        try:
            metadata = process_policy(
                row,
                resume=args.resume,
                process_only=args.process_only,
                discover_only=args.discover,
            )
            all_metadata.append(metadata)
        except Exception as e:
            print(f"\n  ERROR processing {row['policy_name']}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
