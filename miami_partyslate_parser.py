import asyncio
import contextlib
import dataclasses
from dataclasses import dataclass, field
import logging
import random
import re
import sys
import time
from typing import List, Optional, Dict, Iterable, Tuple, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
import httpx
from bs4 import BeautifulSoup
import urllib.robotparser as robotparser

# ---------------------------
# Config & Constants
# ---------------------------

DEFAULT_LISTING_URL = "https://www.partyslate.com/find-vendors/event-planner/area/miami"
DEFAULT_VENDOR_LIMIT = 50
DEFAULT_OUTFILE = "miami_event_agencies.xlsx"

PAGE_TIMEOUT_MS = 35000
NAVIGATION_TIMEOUT_MS = 40000
VENDOR_CONCURRENCY = 3
HTTP_TIMEOUT = 15.0
MIN_DELAY = 0.4
MAX_DELAY = 1.0

VENDOR_TOTAL_TIMEOUT = 150
ROBOTS_TIMEOUT = 5.0
MAX_CONTACT_PAGES = 4

# Patterns
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_REGEX = re.compile(r"(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}")
SOCIAL_PATTERNS = {
    "instagram": re.compile(r"instagram\.com", re.IGNORECASE),
    "facebook": re.compile(r"(facebook|fb)\.com", re.IGNORECASE),
}
CURRENCY_VAL = re.compile(r"\$[\d,]+(?:\.\d{2})?")

# ---------------------------
# Data Models
# ---------------------------

@dataclass
class TeamMember:
    name: str = ""
    title: str = ""
    phone: str = ""
    email: str = ""


@dataclass
class VendorRecord:
    company_name: str = ""
    website: str = ""
    contact_person: str = ""
    job_title: str = ""
    phone: str = ""
    email: str = ""
    minimum_spend: str = ""
    instagram: str = ""
    facebook: str = ""


@dataclass
class VendorPageData:
    company_name: str = ""
    website: str = ""
    phone: str = ""
    minimum_spend: str = ""
    instagram: str = ""
    facebook: str = ""
    team: List[TeamMember] = field(default_factory=list)


@dataclass
class ParserConfig:
    headless: bool = True
    listing_url: str = DEFAULT_LISTING_URL
    limit: int = DEFAULT_VENDOR_LIMIT
    out_path: str = DEFAULT_OUTFILE


# ---------------------------
# Helpers
# ---------------------------

def rand_delay(a: float = MIN_DELAY, b: float = MAX_DELAY) -> float:
    return random.uniform(a, b)

def clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def normalize_phone(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^tel:", "", s, flags=re.IGNORECASE)
    return s.strip()

def unique(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def is_absolute_url(url: str) -> bool:
    return bool(urlparse(url).scheme)

def is_social(href: str) -> Tuple[bool, Optional[str]]:
    for name, pat in SOCIAL_PATTERNS.items():
        if pat.search(href or ""):
            return True, name
    return False, None

def prefer_offsite_website(links: List[str]) -> Optional[str]:
    for href in links:
        if not href:
            continue
        if "partyslate.com" in href:
            continue
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        if is_absolute_url(href):
            return href
    return None

def extract_emails_from_text(text: str) -> List[str]:
    return unique([m.group(0) for m in EMAIL_REGEX.finditer(text or "")])

def extract_phones_from_text(text: str) -> List[str]:
    return unique([normalize_phone(m.group(0)) for m in PHONE_REGEX.finditer(text or "")])

def extract_social_links_from_html(soup: BeautifulSoup) -> Dict[str, str]:
    social = {"instagram": "", "facebook": ""}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        is_soc, name = is_social(href)
        if is_soc and name and not social.get(name):
            social[name] = href
    return social

def extract_minimum_spend_text(text: str) -> str:
    if not text:
        return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    for l in lines:
        if re.search(r"(minimum|starting|start at|from)\b", l, re.IGNORECASE) and CURRENCY_VAL.search(l):
            return clean_text(l)
    for l in lines:
        if CURRENCY_VAL.search(l) and re.search(r"(budget|spend|min)", l, re.IGNORECASE):
            return clean_text(l)
    return ""

async def robots_can_fetch(client: httpx.AsyncClient, url: str, user_agent: str = "*") -> bool:
    """Fetch robots.txt with a small timeout and check can_fetch; on failure, default to True (fail-open)."""
    try:
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        robots_url = urljoin(base, "/robots.txt")
        resp = await client.get(robots_url, timeout=ROBOTS_TIMEOUT)
        if resp.status_code != 200 or not resp.text:
            return True
        rp = robotparser.RobotFileParser()
        rp.parse(resp.text.splitlines())
        return rp.can_fetch(user_agent, url)
    except Exception:
        return True

async def fetch_text_httpx(url: str, client: httpx.AsyncClient, check_robots: bool = True) -> str:
    if check_robots:
        ok = await robots_can_fetch(client, url)
        if not ok:
            return ""
    try:
        resp = await client.get(url, timeout=httpx.Timeout(connect=5, read=10, write=10, pool=5), follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (compatible; EventAgencyParser/1.0)"
        })
        if resp.status_code == 200 and resp.text:
            return resp.text
    except Exception:
        return ""
    return ""

# ---------------------------
# Playwright Scraping
# ---------------------------

async def get_vendor_links_from_listing(page: Page, base_url: str, limit: int) -> List[str]:
    """
    Traverse pagination ?page=N and collect vendor profile URLs until `limit`.
    """
    def _with_page(url: str, page_num: int) -> str:
        if "page=" in url:
            return re.sub(r"([?&])page=\d+", rf"\\1page={page_num}", url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}page={page_num}"

    collected: List[str] = []
    seen: Set[str] = set()
    page_num = 1

    while len(collected) < limit:
        listing_url = _with_page(base_url, page_num)
        logging.info("Listing page %d → %s", page_num, listing_url)
        try:
            await page.goto(listing_url, timeout=NAVIGATION_TIMEOUT_MS, wait_until="domcontentloaded")
        except PlaywrightTimeoutError:
            logging.warning("Timeout navigating to listing page %s; retrying basic goto", listing_url)
            await page.goto(listing_url, timeout=NAVIGATION_TIMEOUT_MS)
        await asyncio.sleep(rand_delay())

        with contextlib.suppress(Exception):
            await page.wait_for_selector('a[href^="/vendors/"]', timeout=6000)

        hrefs: List[Optional[str]] = await page.eval_on_selector_all(
            'a[href^="/vendors/"]',
            'els => els.map(e => e.getAttribute("href"))'
        )
        page_links = 0
        for href in hrefs:
            if not href:
                continue
            if href.startswith("/vendors/"):
                full = urljoin("https://www.partyslate.com", href)
                if full not in seen:
                    seen.add(full)
                    collected.append(full)
                    page_links += 1
                    if len(collected) >= limit:
                        break

        logging.info("Collected %d new vendor links on page %d (total=%d)", page_links, page_num, len(collected))

        if page_links == 0:
            logging.info("No new links found on page %d; stopping pagination.", page_num)
            break

        page_num += 1
        await asyncio.sleep(rand_delay())

    return collected[:limit]



async def parse_vendor_page(page: Page, url: str) -> VendorPageData:
    vpd = VendorPageData()
    start = time.time()
    logging.info("→ Parse vendor: %s", url)

    try:
        with contextlib.suppress(Exception):
            await page.goto(url, timeout=NAVIGATION_TIMEOUT_MS, wait_until="domcontentloaded")
            await asyncio.sleep(1.5)

        # 1) Company Name
        with contextlib.suppress(Exception):
            vpd.company_name = clean_text(await page.inner_text("h1"))

        # 2 & 5) Website and Phone
        try:
            call_btn = page.locator("button.css-5be8bs")
            if await call_btn.count() > 0:
                await call_btn.first.click()
                await asyncio.sleep(1)

                with contextlib.suppress(Exception):
                    site_url = await page.locator(".css-123qr35").first.inner_text()
                    vpd.website = site_url.strip()

                with contextlib.suppress(Exception):
                    phone_text = await page.locator(".css-1dvr8y4").first.inner_text()
                    vpd.phone = normalize_phone(phone_text)

                try:
                    close_btn = page.locator("button[aria-label='Close']")
                    if await close_btn.count() > 0:
                        await close_btn.first.click()
                        await asyncio.sleep(0.5)
                    else:
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(0.5)
                except Exception:
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.5)

        except Exception as e:
            logging.warning("Follow Us block not found for %s: %s", url, e)

        # 3 & 4) Contact Person
        members: List[TeamMember] = []

        team_section = None
        for label in ["Meet the Team", "Meet The Team"]:
            with contextlib.suppress(Exception):
                loc = page.locator(f"section:has-text('{label}'), div:has-text('{label}')")
                if await loc.count() > 0:
                    team_section = loc.first
                    break

        if not team_section:
            vpd.team = members
        else:
            async def read_current_name_title() -> tuple[str, str]:
                name, title = "", ""
                for sel in ["article hgroup h3", "article h3", ".css-1ham2m0", "h3, h4, strong, b"]:
                    with contextlib.suppress(Exception):
                        el = team_section.locator(sel).first
                        if await el.count() > 0:
                            txt = (await el.inner_text()).strip()
                            if txt:
                                name = clean_text(txt)
                                break
                for sel in ["article hgroup span", "span.css-1pxun7d", "article h4", "span, em, i, small"]:
                    with contextlib.suppress(Exception):
                        el = team_section.locator(sel).first
                        if await el.count() > 0:
                            txt = (await el.inner_text()).strip()
                            if txt:
                                title = clean_text(txt)
                                break

                if not name:
                    with contextlib.suppress(Exception):
                        blob = await (team_section.locator("article").first if await team_section.locator("article").count() > 0 else team_section).inner_text()
                        lines = [l.strip() for l in blob.split("\n") if l.strip()]
                        ignore = re.compile(r"^(meet\s*the\s*team|our\s*team|faqs|pricing packages|overview|follow us)$", re.I)
                        pages = re.compile(r"^\d+\s*\/\s*\d+$")
                        for i, l in enumerate(lines):
                            if ignore.match(l) or pages.match(l):
                                continue
                            if re.search(r"[A-Za-zА-Яа-я]", l) and len(l) > 1 and not re.search(r"@\w+", l):
                                name = clean_text(l)
                                if i + 1 < len(lines):
                                    nxt = lines[i + 1]
                                    if not ignore.match(nxt) and not pages.match(nxt) and len(nxt) < 120:
                                        title = clean_text(nxt)
                                break
                return (name or ""), (title or "")

            next_btn = team_section.locator(
                "button[aria-label='view next team member'],"
                "button[aria-label*='next team member'],"
                "button.css-1qz0g1e:nth-child(2)"
            )
            has_next = await next_btn.count() > 0

            total_pages = None
            with contextlib.suppress(Exception):
                pag = team_section.locator("span:has-text('/')").first
                if await pag.count() > 0:
                    pag_text = (await pag.inner_text()).strip()
                    m = re.search(r"(\d+)\s*/\s*(\d+)", pag_text)
                    if m:
                        total_pages = int(m.group(2)) or None

            collected_names: set[str] = set()

            if has_next or (total_pages and total_pages > 1):
                max_iter = total_pages if (total_pages and total_pages > 0) else 20
                for i in range(max_iter):
                    name, title = await read_current_name_title()
                    if name and name not in collected_names:
                        members.append(TeamMember(name=name[:120], title=title[:160], phone="", email=""))
                        collected_names.add(name)

                    if total_pages and i >= total_pages - 1:
                        break

                    progressed = False
                    with contextlib.suppress(Exception):
                        btn = next_btn.first
                        if await btn.count() > 0:
                            await btn.click(force=True)
                            for _ in range(20):
                                await page.wait_for_timeout(150)
                                new_name, _ = await read_current_name_title()
                                if new_name and new_name != name:
                                    progressed = True
                                    break
                    if not progressed:
                        break
            else:
                name, title = await read_current_name_title()

                if name and name not in collected_names:
                    members.append(
                        TeamMember(
                            name=name[:120],
                            title=(title or "")[:160],
                            phone="",
                            email="",
                        )
                    )
                    collected_names.add(name)

            vpd.team = members


        # 7) Minimum spend
        with contextlib.suppress(Exception):
            full_text = await page.inner_text("body")
            match = re.search(r"\$\s?([\d,]+)", full_text)
            if match:
                vpd.minimum_spend = match.group(1).replace(",", "")

        # 8) Instagram Link
        with contextlib.suppress(Exception):
            insta_href = await page.locator("a.css-6cxgxb:nth-child(3)").get_attribute("href")
            vpd.instagram = insta_href or ""

        # 9) Facebook Link
        with contextlib.suppress(Exception):
            fb_href = await page.locator("a.css-6cxgxb:nth-child(2)").get_attribute("href")
            vpd.facebook = fb_href or ""

    except Exception:
        logging.exception("Unexpected error parsing vendor page: %s", url)

    logging.info(
        "← Parsed vendor: %s | team=%d | took=%.1fs",
        vpd.company_name or url,
        len(vpd.team),
        time.time() - start
    )
    return vpd

async def enrich_from_official_site(vpd: VendorPageData) -> VendorPageData:
    try:
        if not vpd.website:
            logging.info("Enrich skipped: no website for %s", vpd.company_name)
            return vpd

        website = vpd.website if is_absolute_url(vpd.website) else f"http://{vpd.website}"
        paths = [website]
        for suffix in ["/contact", "/contact-us", "/contactus", "/about", "/team", "/about-us"]:
            paths.append(website.rstrip("/") + suffix)

        logging.info("Enrich: %s | checking up to %d pages", vpd.company_name or website, min(len(paths), MAX_CONTACT_PAGES))

        html_blobs: List[str] = []
        async with httpx.AsyncClient(follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (compatible; EventAgencyParser/1.0)"
        }) as client:
            for u in unique(paths)[:MAX_CONTACT_PAGES]:
                with contextlib.suppress(Exception):
                    html = await fetch_text_httpx(u, client, check_robots=True)
                    if html:
                        html_blobs.append(html)
                        logging.debug("Enrich fetched: %s (len=%d)", u, len(html))
                await asyncio.sleep(rand_delay(0.2, 0.5))

        if not html_blobs:
            return vpd

        all_text = "\n".join(html_blobs)

        with contextlib.suppress(Exception):
            emails = extract_emails_from_text(all_text)
            if emails:
                for i, tm in enumerate(vpd.team):
                    if not tm.email and i < len(emails):
                        tm.email = emails[i]

        with contextlib.suppress(Exception):
            phones = extract_phones_from_text(all_text)
            if not vpd.phone and phones:
                vpd.phone = phones[0]

        for html in html_blobs:
            with contextlib.suppress(Exception):
                soup = BeautifulSoup(html, "lxml")
                soc = extract_social_links_from_html(soup)
                if not vpd.instagram and soc.get("instagram"):
                    vpd.instagram = soc["instagram"]
                if not vpd.facebook and soc.get("facebook"):
                    vpd.facebook = soc["facebook"]

    except Exception:
        logging.exception("Fatal error in enrich_from_official_site for %s", vpd.company_name or "unknown")

    return vpd

# ---------------------------
# Runner
# ---------------------------

async def process_vendor(link: str, browser: Browser, idx: int, total: int) -> VendorPageData:
    start = time.time()
    page = await browser.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    logging.info("Vendor %d/%d: start %s", idx, total, link)

    vpd = VendorPageData()
    try:
        vpd = await parse_vendor_page(page, link)

        await asyncio.sleep(rand_delay())

        try:
            vpd = await enrich_from_official_site(vpd)
        except Exception:
            logging.exception("Vendor %d/%d: error in enrich_from_official_site %s", idx, total, link)

        logging.info(
            "Vendor %d/%d: done %s in %.1fs (team=%d)",
            idx, total, vpd.company_name or link, time.time() - start, len(vpd.team)
        )

    except Exception:
        logging.exception("Vendor %d/%d: fatal error in parse %s", idx, total, link)

    finally:
        with contextlib.suppress(Exception):
            await page.close()

    return vpd


async def main_async(config: ParserConfig) -> None:
    start = time.time()
    logging.info("Starting scrape: %s (limit=%d)", config.listing_url, config.limit)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=config.headless)
        try:
            page = await browser.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)
            vendor_links = await get_vendor_links_from_listing(page, config.listing_url, config.limit)
            logging.info("Collected %d vendor links", len(vendor_links))
            await page.close()

            total = len(vendor_links)
            if total == 0:
                logging.warning("No vendor links found. Exiting.")
                return

            sem = asyncio.Semaphore(VENDOR_CONCURRENCY)
            results: List[VendorPageData] = []

            async def guarded_worker(i: int, link: str):
                async with sem:
                    try:
                        return await asyncio.wait_for(process_vendor(link, browser, i+1, total), timeout=VENDOR_TOTAL_TIMEOUT)
                    except asyncio.TimeoutError:
                        logging.error("Vendor %d/%d: timed out after %ds (%s)", i+1, total, VENDOR_TOTAL_TIMEOUT, link)
                        return VendorPageData()

            logging.info("Begin vendor processing with concurrency=%d", VENDOR_CONCURRENCY)
            tasks = [asyncio.create_task(guarded_worker(i, link)) for i, link in enumerate(vendor_links)]
            for i, task in enumerate(asyncio.as_completed(tasks), start=1):
                vpd = await task
                results.append(vpd)
                logging.info("Progress: %d/%d vendors completed", i, total)

            rows: List[VendorRecord] = []
            for v in results:
                if v.team:
                    for tm in v.team:
                        rows.append(VendorRecord(
                            company_name=v.company_name or "",
                            website=v.website or "",
                            contact_person=tm.name or "",
                            job_title=tm.title or "",
                            phone=tm.phone or v.phone or "",
                            email=tm.email or "",
                            minimum_spend=v.minimum_spend or "",
                            instagram=v.instagram or "",
                            facebook=v.facebook or "",
                        ))
                else:
                    rows.append(VendorRecord(
                        company_name=v.company_name or "",
                        website=v.website or "",
                        contact_person="",
                        job_title="",
                        phone=v.phone or "",
                        email="",
                        minimum_spend=v.minimum_spend or "",
                        instagram=v.instagram or "",
                        facebook=v.facebook or "",
                    ))


            df = pd.DataFrame([dataclasses.asdict(r) for r in rows], columns=[
                "company_name",
                "website",
                "contact_person",
                "job_title",
                "phone",
                "email",
                "minimum_spend",
                "instagram",
                "facebook",
            ]).rename(columns={
                "company_name": "Company Name",
                "website": "Website",
                "contact_person": "Contact Person",
                "job_title": "Job Title",
                "phone": "Phone",
                "email": "Email",
                "minimum_spend": "Minimum spend",
                "instagram": "Instagram Link",
                "facebook": "Facebook Link",
            })

            df.to_excel(config.out_path, index=False)
            logging.info("Saved to %s (rows=%d)", config.out_path, len(df))

        finally:
            with contextlib.suppress(Exception):
                await browser.close()

    logging.info("Done in %.1fs", time.time() - start)


def parse_args() -> ParserConfig:
    import argparse
    parser = argparse.ArgumentParser(description="Scrape PartySlate Miami Event Planners and export to Excel.")
    parser.add_argument("--limit", type=int, default=DEFAULT_VENDOR_LIMIT, help="Number of vendors to scrape (default 50)")
    parser.add_argument("--out", type=str, default=DEFAULT_OUTFILE, help="Output Excel path")
    parser.add_argument("--listing-url", type=str, default=DEFAULT_LISTING_URL, help="PartySlate listing URL")
    parser.add_argument("--headful", action="store_true", help="Run browser in headful mode (default: headless)")
    args = parser.parse_args()
    return ParserConfig(
        headless=not args.headful,
        listing_url=args.listing_url,
        limit=args.limit,
        out_path=args.out,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    config = parse_args()
    try:
        asyncio.run(main_async(config))
    except KeyboardInterrupt:
        print("Interrupted by user")


if __name__ == "__main__":
    main()
