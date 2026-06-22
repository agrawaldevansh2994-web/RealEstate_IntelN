# scraper\_easr.py — Review \& Fix Prompt

## Context

eASR scraper ran for Nagpur. 218 villages screened, most rural (no urban grid), correctly skipped. Two errors occurred mid-run:

1. `net::ERR\_ABORTED` on village धवळापेठ — `\_postback\_select` returned False, village skipped via `continue`
2. `Execution context was destroyed` on धामणा — caught by `except` block, recovery (`\_load\_district` → `\_select\_taluka`) ran and succeeded, run continued

Data did save correctly after recovery. Run worked but took \~45 min total for Nagpur.

\---

## Questions

**1. Was the recovery surviving intentional?**
The recovery block calls `page.goto()` while a secondary ASP.NET postback may still be in flight. It worked this time (timing was lucky). Is this handled robustly, or is there a race condition that could kill future runs?

**2. Are these two fixes worth keeping?**
An external review suggested two changes — confirm if they're correct before applying:

* `\_postback\_select`: settle buffer after navigation **500ms → 1200ms** — to let ASP.NET's deferred secondary postback settle before DOM is queried
* Recovery block: add `page.wait\_for\_timeout(2500)` **before** `\_load\_district(page)` — to let any in-flight navigation complete before `page.goto()` is called, preventing recovery itself from throwing and hitting the `break`

**3. Rural village pre-filter (performance)**
Currently all 218 villages are selected via postback before checking if the urban grid renders. \~112 are rural — each costs a full postback (\~4s) just to discover no grid. If village names or values have a reliable signal to identify rural entries upfront (e.g. absence of मौजा prefix for Nagpur, or known value ranges), can we skip the postback entirely for those and only navigate urban candidates? What's the right approach here without breaking Pune-style districts (which have no text marker at all)?

**4. Anything else worth reviewing in `run()` or `\_scrape\_locality\_rates()`?**





**Another query, post Nagpur run**

**Mapping coverage for Nagpur is now 40/61 villages (65.6%) in locality\_aliases. Two real ambiguities were found and isolated rather than guessed at: "Parsodi" and "Bharatwada" each have two conflicting circle-rate entries in circle\_rates — one NMRDA-tagged (low rate, few sub-zones) and one plain-named (high rate, sub\_zone\_count=150). I left both unmapped with notes rather than picking one. Can you manually check the EASR portal for these two village names directly — is the plain entry a genuine separate/broader zone, or a scraper artifact (e.g. defaulting to a "select all" dropdown state)? Also worth a sanity check: 12 different plain-named villages across the dataset all show exactly sub\_zone\_count=150 — possibly a portal display cap, possibly coincidence, but worth one manual lookup to confirm before trusting those rates at face value. Once resolved, \~12 more villages could safely be added to the mapping table, and then I can build the actual Circle Rate Gauge panel — 40 villages with real listing overlap is already enough to make it useful.**

