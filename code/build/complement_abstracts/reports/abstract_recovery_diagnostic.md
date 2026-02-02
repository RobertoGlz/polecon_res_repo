# Abstract Recovery Diagnostic Report

**Generated:** 2026-01-28 16:12:13

---

## Executive Summary

- **Total recovery attempts that failed:** 923
- **CrossRef attempts:** 715 (failed: 706)
- **Open Access URL attempts:** 322 (failed: 176)
- **PDF extraction attempts:** 60 (failed: 34)
- **SSRN attempts:** 32 (failed: 2)

- **CrossRef success rate:** 9/715 (1.3%)
- **OA URL success rate:** 49/322 (15.2%)
- **PDF extraction success rate:** 26/60 (43.3%)
- **SSRN success rate:** 30/32 (93.8%)

---

## Failure Breakdown by Reason

| Failure Reason | Count | Description |
|----------------|-------|-------------|
| `crossref_no_abstract_in_db` | 690 | DOI exists in CrossRef but no abstract available |
| `oa_url_no_matching_selector` | 82 | HTML parsed successfully but no abstract element found |
| `oa_url_blocked_403` | 41 | Access denied (HTTP 403) |
| `oa_url_abstract_too_short` | 36 | Found abstract element but content too short (<100 chars) |
| `crossref_doi_not_found` | 16 | DOI not found in CrossRef database |
| `pdf_no_abstract_found` | 16 | PDF text extracted but no abstract section found |
| `pdf_download_failed` | 15 | Failed to download PDF file |
| `oa_url_redirect_to_login` | 11 | Page redirected to login/paywall |
| `oa_url_connection_error` | 7 | Connection error (network issue) |
| `oa_url_not_found_404` | 3 | Page not found (HTTP 404) |
| `pdf_extraction_failed` | 3 | PDF downloaded but text extraction failed |
| `ssrn_no_abstract_element` | 2 | SSRN page loaded but no abstract element found |
| `oa_url_connection_timeout` | 1 | Connection timed out |

---

## Key Insights

### 1. HTML Selector Misses (82 cases)

Pages were successfully fetched but our CSS selectors couldn't find the abstract element.
**Action:** Analyze HTML snippets below to identify missing selector patterns.

### 3. Access Blocked (41 cases)

Sites returned 403/401 errors, indicating paywalls or bot detection.
**Action:** These may require institutional access or different scraping approaches.

### 4. PDF Abstract Not Found (16 cases)

PDFs were downloaded but no 'Abstract' section was found in the text.
**Action:** Review PDF structure - some papers may not have labeled abstracts.

### 5. CrossRef Has No Abstract (690 cases)

Papers exist in CrossRef but don't have abstracts in the metadata.
**Action:** This is a data limitation - publishers didn't provide abstracts to CrossRef.

---

## Top Domains with Failures

These domains have the most failed abstract recovery attempts:

| Domain | Failure Count | Common Failure Reasons |
|--------|---------------|------------------------|
| doi.org | 55 | oa_url_blocked_403 (33), oa_url_no_matching_selector (19) |
| hdl.handle.net | 33 | oa_url_no_matching_selector (21), oa_url_redirect_to_login (11) |
| scholarship.shu.edu | 9 | oa_url_abstract_too_short (6), oa_url_no_matching_selector (2) |
| digitalcommons.law.byu.edu | 8 | oa_url_abstract_too_short (5), oa_url_no_matching_selector (3) |
| digital.library.wisc.edu | 8 | oa_url_abstract_too_short (6), oa_url_no_matching_selector (2) |
| digital.library.unt.edu | 8 | oa_url_no_matching_selector (7), oa_url_abstract_too_short (1) |
| link.springer.com | 8 | pdf_no_abstract_found (7), pdf_extraction_failed (1) |
| www.thelancet.com | 7 | pdf_download_failed (7) |
| commons.lib.niu.edu | 6 | oa_url_connection_error (6) |
| www.sciencedirect.com | 5 | oa_url_blocked_403 (5) |
| brooklynworks.brooklaw.edu | 3 | oa_url_abstract_too_short (2), oa_url_no_matching_selector (1) |
| scholarsarchive.library.albany.edu | 3 | oa_url_no_matching_selector (3) |
| scholarworks.wm.edu | 3 | oa_url_no_matching_selector (3) |
| repository.usfca.edu | 2 | oa_url_abstract_too_short (1), oa_url_no_matching_selector (1) |

---

## Sample Failures for Manual Verification

Review these examples to understand why abstracts weren't recovered:

### HTML Parsed but No Abstract Found

**1. No Child Left Behind: A Toolkit for Teachers...**
- URL: https://doi.org/10.1037/e373292004-001
- DOI: https://doi.org/10.1037/e373292004-001

**2. How No Child Left Behind Benefits African American...**
- URL: https://doi.org/10.1037/e491412006-001
- DOI: https://doi.org/10.1037/e491412006-001

**3. Fact Sheet on the Major Provisions of the Conferen...**
- URL: https://doi.org/10.1037/e491652006-001
- DOI: https://doi.org/10.1037/e491652006-001

**4. No Child Left Behind Is Working...**
- URL: https://doi.org/10.1037/e491372006-001
- DOI: https://doi.org/10.1037/e491372006-001

**5. Public schools in crisis: a content analysis of ne...**
- URL: http://hdl.handle.net/10355/37951
- DOI: None


### Access Blocked (403)

**1. The Perverse Incentives of the No Child Left Behin...**
- URL: https://doi.org/10.2139/ssrn.476463
- DOI: https://doi.org/10.2139/ssrn.476463

**2. No Child Left Behind: Universal Child Care and Chi...**
- URL: https://doi.org/10.2139/ssrn.1506313
- DOI: https://doi.org/10.2139/ssrn.1506313

**3. From No Child Left Behind to Every Student Succeed...**
- URL: https://doi.org/10.2139/ssrn.3047994
- DOI: https://doi.org/10.2139/ssrn.3047994

**4. Incentives and Responses under No Child Left Behin...**
- URL: https://doi.org/10.2139/ssrn.1957719
- DOI: https://doi.org/10.2139/ssrn.1957719

**5. Unintended Consequences: No Child Left Behind and ...**
- URL: https://doi.org/10.2139/ssrn.1584345
- DOI: https://doi.org/10.2139/ssrn.1584345


### PDF Extracted but No Abstract Section

**1. The role of parent–child communication on Chinese ...**
- URL: https://www.nature.com/articles/s41599-024-04334-1.pdf
- DOI: https://doi.org/10.1057/s41599-024-04334-1

**2. No Child Left Behind: ABA Opportunity or Guilt by ...**
- URL: https://link.springer.com/content/pdf/10.5210/bsi.v16i1.370.pdf
- DOI: https://doi.org/10.5210/bsi.v16i1.370

**3. No Child Left Behind Act of 2001...**
- URL: https://link.springer.com/content/pdf/10.1007%2F978-3-319-56782-2_1576-2.pdf
- DOI: https://doi.org/10.1007/978-3-319-56782-2_1576-2

**4. No child left behind...**
- URL: https://jnis.bmj.com/content/neurintsurg/16/6/535.full.pdf
- DOI: https://doi.org/10.1136/jnis-2024-021907

**5. No Child Left Behind: The Emperor Has No Clothes....**
- URL: http://files.eric.ed.gov/fulltext/EJ854545.pdf
- DOI: None


---

## Detailed OA URL Failures with HTML Context

Papers where HTML was fetched successfully but no abstract was extracted:

### 1. No Child Left Behind: An Overview
- **URL:** http://hdl.handle.net/10365/10430
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <base href="/ir/">
  <title>No Child Left Behind: An Overview</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"></noscript><link rel="stylesheet" type="text/css" class="theme-css" href="
```

### 2. THE READING ACHIEVEMENT OF KANSAS URBAN AFRICAN AM
- **URL:** http://hdl.handle.net/2097/1383
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>The reading achievement of Kansas urban African American fifth graders before and during No Child Left Behind</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"><
```

### 3. Adequate Yearly Progress (AYP) and No Child Left B
- **URL:** http://hdl.handle.net/10365/4946
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <base href="/ir/">
  <title>Adequate Yearly Progress (AYP) and No Child Left Behind: What could AYP mean for your child, school, and community?</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca12
```

### 4. The No Child Left Behind Act: The divide between *
- **URL:** http://hdl.handle.net/2152/3445
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html data-critters-container="" lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>The No Child Left Behind Act: the divide between policy and practice</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"></noscript><lin
```

### 5. Roach Motel: Research, Policy, and Structural Ineq
- **URL:** http://hdl.handle.net/1773/35316
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <base href="/researchworks/">
  <title>Roach Motel: Research, Policy, and Structural Inequalities of the No Child Left Behind Act in California Public Schools</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles
```

### 6. Fractionalization
- **URL:** http://nrs.harvard.edu/urn-3:HUL.InstRepos:4553003
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html data-critters-container="" lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>Fractionalization</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"></noscript><link rel="stylesheet" type="text/css" class="theme-css
```

### 7. Final Report on the Evaluation of the Growth Model
- **URL:** http://nrs.harvard.edu/urn-3:HUL.InstRepos:4740801
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html data-critters-container="" lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>Final report on the evaluation of the Growth Model Pilot Project</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"></noscript><link re
```

### 8. Differentiated instruction : exploring implementat
- **URL:** http://hdl.handle.net/10976/47
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charSet="UTF-8" http-equiv="Content-Type" content="text/javascript"/>
    <meta name="google-site-verification" content="aQImwbtVJ-ZQRH64JwqaHJMrCTCJ-k5ZQOJMc9Q7nHs"/>
    <!--{this.props.head.title.toComponent()}-->
    <title>CONTENTdm</title>
    <meta name="description" content="" data-react-helmet="true"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>

    <!-- socialLinks != null and socialLinks.shareEnabled an
```

### 9. Understanding Common Core Implementation: How Educ
- **URL:** http://www.lib.ncsu.edu/resolver/1840.16/8364
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>Understanding Common Core Implementation: How Educators Intuit, Interpret, and Begin to Integrate Curriculum Reform.</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222
```

### 10. Teacher commitment in an academically improving, h
- **URL:** http://hdl.handle.net/2152/1639
- **Failure Reason:** `oa_url_javascript_required`
- **HTML Snippet:**
```html
<!DOCTYPE html><html data-critters-container="" lang="en"><head>
  <meta charset="UTF-8">
  <base href="/">
  <title>Teacher commitment in an academically improving, high-poverty public school</title>
  <meta name="viewport" content="width=device-width,minimum-scale=1">
  <meta http-equiv="cache-control" content="no-store">
<link rel="stylesheet" href="styles.e382c7dd35ca1222.css" media="print" onload="this.media='all'"><noscript><link rel="stylesheet" href="styles.e382c7dd35ca1222.css"></noscri
```

---

## Recommendations

Based on the failure analysis:

1. **Add more CSS selectors** for the top failing domains
2. **Use Selenium** for domains that require JavaScript
3. **Improve PDF parsing** to handle papers without 'Abstract' headers
4. **Accept data limitations** for CrossRef/publisher metadata gaps
