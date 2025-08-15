# PartySlate Miami Event Planners Parser

This script scrapes the first 50 (configurable) vendor listings from PartySlate's Miami event-planner category, extracts "Meet the Team" members, enriches with data from the agencies' official websites, and exports an Excel file.

## Features & Best Practices

- ✅ **Playwright** to render JS-heavy pages (PartySlate).
- ✅ **Resilient selectors** with text-based fallbacks.
- ✅ **Graceful retries & timeouts**; **rate limiting** between requests.
- ✅ **Robots.txt** respected for vendor-site enrichment.
- ✅ **Type hints, dataclasses, structured logging**.
- ✅ **Excel export** in the exact columns you requested.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install
```

## Run

```bash
python miami_partyslate_parser.py --limit 50 --out miami_event_agencies.xlsx
```

Headful mode (to watch the browser):

```bash
python miami_partyslate_parser.py --limit 50 --headful
```

### Output Columns

- Company Name
- Website
- Contact Person
- Job Title
- Phone
- Email
- Minimum spend
- Instagram Link
- Facebook Link

**If a value is missing or not confirmed, the cell stays empty.**

## Notes / Tips

- PartySlate’s DOM can change. This script uses text-based heuristics (e.g., sections with “Meet the Team”) and multiple selector fallbacks.
- For vendor-site enrichment, we fetch only the homepage and a few likely paths (`/contact`, `/about`, `/team`), respecting `robots.txt`.
- You may increase `VENDOR_CONCURRENCY` if you have strong network & you’re confident about rate-limits, but be polite.
- To debug selectors, run with `--headful` and use Playwright’s `page.pause()` (insert into code) to inspect the DOM using Playwright Inspector.
