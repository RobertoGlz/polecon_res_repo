# Venue Coverage Analysis: OpenAlex vs Semantic Scholar

**Generated:** 2026-02-02 11:01:24

This report identifies venues that appear exclusively in one source.

---

## Summary Statistics

- **Total papers analyzed:** 11,595
- **Unique venues in OpenAlex:** 2,528
- **Unique venues in Semantic Scholar:** 1,286
- **Venues in both sources:** 814
- **Venues ONLY in OpenAlex:** 1,714
- **Venues ONLY in Semantic Scholar:** 472

---

## Top Venues Indexed by OpenAlex but NOT Semantic Scholar

Ranked by total citations (proxy for academic relevance):

| Rank | Venue | Papers | Total Citations | Avg Citations |
|------|-------|--------|-----------------|---------------|
| 1 | CA A Cancer Journal for Clinicians | 6 | 15,970 | 2661.7 |
| 2 | The Lancet Global Health | 12 | 13,552 | 1129.3 |
| 3 | JAMA | 57 | 4,355 | 76.4 |
| 4 | Mayo Clinic Proceedings | 6 | 3,992 | 665.3 |
| 5 | Educational Researcher | 24 | 3,646 | 151.9 |
| 6 | Choice Reviews Online | 48 | 3,389 | 70.6 |
| 7 | International Journal of Information Management | 1 | 3,122 | 3122.0 |
| 8 | Global Environmental Change | 1 | 3,119 | 3119.0 |
| 9 | Ecological Economics | 3 | 2,659 | 886.3 |
| 10 | Journal of Development Economics | 3 | 2,614 | 871.3 |
| 11 | PsycEXTRA Dataset | 205 | 2,433 | 11.9 |
| 12 | Energy Policy | 8 | 2,050 | 256.2 |
| 13 | Environmental Science and Pollution Research | 3 | 1,998 | 666.0 |
| 14 | Educational Policy | 38 | 1,901 | 50.0 |
| 15 | The Lancet Psychiatry | 2 | 1,855 | 927.5 |

---

## Top Venues Indexed by Semantic Scholar but NOT OpenAlex

Ranked by total citations (proxy for academic relevance):

| Rank | Venue | Papers | Total Citations | Avg Citations |
|------|-------|--------|-----------------|---------------|
| 1 | Health systems in transition | 21 | 4,927 | 234.6 |
| 2 | International Conference on Computer Aided Verification | 1 | 2,006 | 2006.0 |
| 3 | Teachers College Record | 24 | 1,611 | 67.1 |
| 4 | J. Assoc. Inf. Sci. Technol. | 1 | 1,487 | 1487.0 |
| 5 | Nature Reviews Disease Primers | 1 | 1,250 | 1250.0 |
| 6 | Annals of Family Medicine | 15 | 1,107 | 73.8 |
| 7 | Journal of Clinical Endocrinology and Metabolism | 4 | 842 | 210.5 |
| 8 | Definitions | 2 | 734 | 367.0 |
| 9 | Circulation | 6 | 696 | 116.0 |
| 10 | The Journal of the National Comprehensive Cancer Network | 3 | 628 | 209.3 |
| 11 | Inquiry : a journal of medical care organization, provision and financing | 21 | 594 | 28.3 |
| 12 | Federal register | 11 | 549 | 49.9 |
| 13 | Child Development | 1 | 544 | 544.0 |
| 14 | Journal of Child Psychology and Psychiatry and Allied Disciplines | 2 | 534 | 267.0 |
| 15 | Academic medicine : journal of the Association of American Medical Colleges | 17 | 532 | 31.3 |

---

## Interpretation

### Why Some Venues Appear Only in OpenAlex

1. **PsycEXTRA Dataset**: Psychology grey literature indexed by APA, which OpenAlex includes
2. **Choice Reviews Online**: Library review publication not in Semantic Scholar's scope
3. **Forefront Group**: Think tank publications indexed by OpenAlex
4. **Various specialized journals**: OpenAlex has broader coverage of smaller venues

### Why Some Venues Appear Only in Semantic Scholar

1. **Different venue naming conventions**: Same journal may have different names
2. **Preprint/working paper repositories**: Different coverage of preprints
3. **Conference proceedings**: Semantic Scholar emphasizes CS/AI conferences

### Important Caveat

A venue appearing 'only' in one source doesn't necessarily mean the other source
doesn't index it at all. It may mean:

- The specific papers from that venue weren't returned by our search queries
- The venue name is stored differently in each database
- Coverage varies by publication year or paper type
