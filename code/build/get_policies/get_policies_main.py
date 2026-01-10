"""
Script to discover and identify major policies from multiple sources
Issue: Get policies for analysis

This script systematically searches multiple sources to identify frequently-studied
policies, generates search terms, and creates a policies.csv file for use with
the scrape_openalex.py script.

Author: claude ai with modifications from roberto gonzalez
Date: January 9, 2026
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import time
from datetime import datetime
from collections import Counter
import os

# Output paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
TMP_DIR = os.path.join(SCRIPT_DIR, "tmp")

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

# Seed policy (starting point)
SEED_POLICY = {
    'policy_name': 'Tax Cuts and Jobs Act',
    'policy_abbreviation': 'TCJA',
    'policy_year': 2017,
    'policy_category': 'tax',
    'search_terms': 'Tax Cuts and Jobs Act|TCJA|2017 tax reform|Trump tax cuts|Tax Reform 2017'
}


def search_nber_papers(query, max_results=100):
    """
    Search NBER working papers for a given query
    
    Parameters:
    -----------
    query : str
        Search query
    max_results : int
        Maximum number of results to retrieve
    
    Returns:
    --------
    list : List of paper dictionaries with title and abstract
    """
    print(f"\n  Searching NBER for: {query}")
    
    papers = []
    base_url = "https://www.nber.org/papers"
    
    # NBER search URL (using their search function)
    search_url = f"https://www.nber.org/search?q={query.replace(' ', '+')}"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find paper links (NBER structure may vary)
            paper_links = soup.find_all('a', href=re.compile(r'/papers/w\d+'))
            
            print(f"    Found {len(paper_links)} NBER papers")
            
            for link in paper_links[:max_results]:
                paper_url = "https://www.nber.org" + link['href']
                title = link.get_text(strip=True)
                
                papers.append({
                    'title': title,
                    'url': paper_url,
                    'source': 'NBER'
                })
                
                time.sleep(0.5)  # Be polite
        else:
            print(f"    Failed to retrieve NBER results: {response.status_code}")
            
    except Exception as e:
        print(f"    Error searching NBER: {e}")
    
    return papers


def get_major_legislation_congress():
    """
    Get major legislation from Congress.gov API
    
    Returns:
    --------
    list : List of legislation dictionaries
    """
    print("\n  Fetching major legislation from Congress.gov...")
    
    # Congress.gov API endpoint
    # Note: Requires API key from https://api.congress.gov/sign-up/
    # For now, we'll use a simplified approach
    
    legislation = []
    
    # Manually curated list of major recent legislation
    # In practice, you would use Congress.gov API or scrape their site
    major_bills = [
        {
            'title': 'Tax Cuts and Jobs Act',
            'abbreviation': 'TCJA',
            'year': 2017,
            'category': 'tax',
            'bill_number': 'H.R. 1'
        },
        {
            'title': 'Affordable Care Act',
            'abbreviation': 'ACA',
            'year': 2010,
            'category': 'health',
            'bill_number': 'H.R. 3590'
        },
        {
            'title': 'Dodd-Frank Wall Street Reform and Consumer Protection Act',
            'abbreviation': 'DFA',
            'year': 2010,
            'category': 'finance',
            'bill_number': 'H.R. 4173'
        },
        {
            'title': 'American Recovery and Reinvestment Act',
            'abbreviation': 'ARRA',
            'year': 2009,
            'category': 'fiscal',
            'bill_number': 'H.R. 1'
        },
        {
            'title': 'American Rescue Plan Act',
            'abbreviation': 'ARP',
            'year': 2021,
            'category': 'fiscal',
            'bill_number': 'H.R. 1319'
        },
        {
            'title': 'Coronavirus Aid, Relief, and Economic Security Act',
            'abbreviation': 'CARES',
            'year': 2020,
            'category': 'fiscal',
            'bill_number': 'H.R. 748'
        },
        {
            'title': 'Medicare Prescription Drug, Improvement, and Modernization Act',
            'abbreviation': 'MMA',
            'year': 2003,
            'category': 'health',
            'bill_number': 'H.R. 1'
        },
        {
            'title': 'No Child Left Behind Act',
            'abbreviation': 'NCLB',
            'year': 2001,
            'category': 'education',
            'bill_number': 'H.R. 1'
        },
        {
            'title': 'Troubled Asset Relief Program',
            'abbreviation': 'TARP',
            'year': 2008,
            'category': 'finance',
            'bill_number': 'H.R. 1424'
        },
        {
            'title': 'Personal Responsibility and Work Opportunity Reconciliation Act',
            'abbreviation': 'PRWORA',
            'year': 1996,
            'category': 'social',
            'bill_number': 'H.R. 3734'
        }
    ]
    
    print(f"    Loaded {len(major_bills)} major bills")
    return major_bills


def generate_search_terms(policy_name, abbreviation, year, category):
    """
    Generate search terms for a policy
    
    Parameters:
    -----------
    policy_name : str
        Full policy name
    abbreviation : str
        Policy abbreviation
    year : int
        Policy year
    category : str
        Policy category
    
    Returns:
    --------
    str : Pipe-separated search terms
    """
    terms = []
    
    # Full name
    terms.append(policy_name)
    
    # Abbreviation
    if abbreviation:
        terms.append(abbreviation)
    
    # Common variations
    # Remove "Act" and add year
    base_name = policy_name.replace(' Act', '').replace(' and', '')
    if year:
        terms.append(f"{base_name} {year}")
        terms.append(f"{year} {category} reform")
    
    # Add category-specific terms
    if category == 'tax':
        if 'tax cuts' in policy_name.lower():
            terms.append(f"{year} tax cuts")
        terms.append(f"{year} tax reform")
    elif category == 'health':
        if 'care act' in policy_name.lower():
            terms.append(f"{year} health reform")
    
    # Remove duplicates and empty strings
    terms = [t for t in terms if t]
    terms = list(dict.fromkeys(terms))  # Remove duplicates while preserving order
    
    return '|'.join(terms[:6])  # Limit to 6 terms


def extract_policies_from_text(text):
    """
    Extract policy mentions from text using pattern matching
    
    Parameters:
    -----------
    text : str
        Text to search
    
    Returns:
    --------
    list : List of potential policy names
    """
    policies = []
    
    # Common policy patterns
    patterns = [
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Act(?:\s+of\s+\d{4})?',  # "[Name] Act"
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Reform',  # "[Name] Reform"
        r'(\d{4})\s+([A-Z][a-z]+)\s+(?:Act|Reform)',  # "2017 Tax Act"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        if matches:
            if isinstance(matches[0], tuple):
                policies.extend([' '.join(m) if isinstance(m, tuple) else m for m in matches])
            else:
                policies.extend(matches)
    
    return policies


def search_economic_research_service():
    """
    Search USDA Economic Research Service for policy mentions
    
    Returns:
    --------
    list : List of policies mentioned
    """
    print("\n  Searching Economic Research Service...")
    
    policies = []
    
    # ERS has many policy-related publications
    # This is a simplified version - you would scrape their publications
    
    return policies


def scrape_think_tank_policies(think_tank_url, tank_name):
    """
    Scrape a think tank website for policy mentions
    
    Parameters:
    -----------
    think_tank_url : str
        Think tank website URL
    tank_name : str
        Name of the think tank
    
    Returns:
    --------
    list : List of policies mentioned
    """
    print(f"\n  Scraping {tank_name}...")
    
    policies = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(think_tank_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract text and look for policy patterns
            text = soup.get_text()
            policies = extract_policies_from_text(text)
            
            print(f"    Found {len(policies)} potential policies")
        else:
            print(f"    Failed to retrieve {tank_name}: {response.status_code}")
            
    except Exception as e:
        print(f"    Error scraping {tank_name}: {e}")
    
    return policies


def create_policies_dataframe():
    """
    Create a comprehensive policies DataFrame from multiple sources
    
    Returns:
    --------
    pd.DataFrame : DataFrame with policy information
    """
    print("\n" + "="*80)
    print("GATHERING POLICIES FROM MULTIPLE SOURCES")
    print("="*80)
    
    all_policies = []
    
    # 1. Start with seed policy
    print("\n1. Adding seed policy (TCJA)...")
    all_policies.append(SEED_POLICY)
    
    # 2. Get major legislation from Congress
    #print("\n2. Getting major legislation from Congress.gov...")
    #congress_bills = get_major_legislation_congress()
    
    #for bill in congress_bills:
    #    policy = {
    #        'policy_name': bill['title'],
    #        'policy_abbreviation': bill['abbreviation'],
    #        'policy_year': bill['year'],
    #        'policy_category': bill['category'],
    #        'search_terms': generate_search_terms(
    #            bill['title'], 
    #            bill['abbreviation'], 
    #            bill['year'], 
    #            bill['category']
    #        )
    #    }
    #    all_policies.append(policy)
    
    # 3. Search NBER for frequently mentioned policies
    #print("\n3. Searching NBER for policy-related papers...")
    #policy_keywords = ['tax reform', 'health reform', 'financial regulation', 
    #                  'fiscal policy', 'trade policy']
    
    #nber_policies = []
    #for keyword in policy_keywords[:2]:  # Limit to avoid too many requests
    #    papers = search_nber_papers(keyword, max_results=20)
        
        # Extract policy mentions from titles
    #    for paper in papers:
    #        extracted = extract_policies_from_text(paper['title'])
    #        nber_policies.extend(extracted)
        
    #    time.sleep(1)  # Be polite
    
    # Count frequency of NBER-mentioned policies
    #if nber_policies:
    #    policy_counts = Counter(nber_policies)
    #    print(f"\n    Most mentioned policies in NBER:")
    #    for policy, count in policy_counts.most_common(5):
    #        print(f"      {policy}: {count} mentions")
    
    # 4. Search think tanks
    #print("\n4. Searching think tank websites...")
    
    #think_tanks = [
    #    ('https://www.taxpolicycenter.org/', 'Tax Policy Center'),
    #    ('https://www.urban.org/policy-centers/cross-center-initiatives/taxes', 'Urban Institute'),
    #]
    
    #think_tank_policies = []
    #for url, name in think_tanks[:1]:  # Limit to one for now
    #    try:
    #        policies = scrape_think_tank_policies(url, name)
    #        think_tank_policies.extend(policies)
    #        time.sleep(1)
    #    except:
    #        continue
    
    # 5. Save intermediate results
    #print("\n5. Saving intermediate results...")
    
    #intermediate_data = {
    #    'congress_bills': congress_bills,
    #    'nber_policies': list(policy_counts.most_common(10)) if nber_policies else [],
    #    'think_tank_policies': list(set(think_tank_policies))[:10]
    #}
    
    #intermediate_file = os.path.join(TMP_DIR, 'intermediate_policies.json')
    #with open(intermediate_file, 'w') as f:
    #    json.dump(intermediate_data, f, indent=2)
    #print(f"    Saved intermediate results: {intermediate_file}")
    
    # Create DataFrame
    df = pd.DataFrame(all_policies)
    
    # Remove any duplicates
    df = df.drop_duplicates(subset=['policy_abbreviation'], keep='first')
    
    print(f"\n{'='*80}")
    print(f"TOTAL POLICIES IDENTIFIED: {len(df)}")
    print(f"{'='*80}")
    
    return df


def save_policies_csv(df, output_path):
    """
    Save policies DataFrame to CSV
    
    Parameters:
    -----------
    df : pd.DataFrame
        Policies DataFrame
    output_path : str
        Path to save CSV file
    """
    # Ensure columns are in correct order
    column_order = ['policy_name', 'policy_abbreviation', 'policy_year', 
                   'policy_category', 'search_terms']
    df = df[column_order]
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"\nSaved policies to: {output_path}")
    
    # Print summary
    print(f"\nPolicies by category:")
    print(df['policy_category'].value_counts().to_string())
    
    print(f"\nPolicies by year:")
    print(df.groupby('policy_year').size().sort_index().to_string())
    
    # Save metadata
    metadata = {
        'generation_date': datetime.now().isoformat(),
        'total_policies': len(df),
        'sources': ['seed_policy', 'congress_bills', 'nber_search', 'think_tanks'],
        'categories': df['policy_category'].value_counts().to_dict()
    }
    
    metadata_file = os.path.join(OUTPUT_DIR, 'policies_metadata.json')
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to: {metadata_file}")


def main():
    """
    Main execution function
    """
    print("="*80)
    print("POLICY DISCOVERY PIPELINE")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Create policies DataFrame from multiple sources
    policies_df = create_policies_dataframe()
    
    # Save to output folder
    output_file = os.path.join(OUTPUT_DIR, 'policies.csv')
    save_policies_csv(policies_df, output_file)
    
    print(f"\n{'='*80}")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    print("\nNext steps:")
    print("1. Review the generated policies.csv in output/ folder")
    print("2. Edit search terms if needed")
    print("3. Add or remove policies as desired")
    print("4. Run scrape_openalex_main.py (it will read from ../get_policies/output/policies.csv)")


if __name__ == "__main__":
    main()
