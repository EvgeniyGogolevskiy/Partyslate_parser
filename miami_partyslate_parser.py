#!/usr/bin/env python3
"""
PartySlate Miami Event Planners Parser (Improved)
-------------------------------------------------
- Pagination across ?page=N
- Robust selectors (has_text)
- Non-blocking robots.txt fetch with timeouts
- Vendor-level watchdog timeout
- Rich logging for visibility into progress & bottlenecks
- No mutable global flags (config object passed through)
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from dataclasses import dataclass, field
import logging
import os
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

PAGE_TIMEOUT_MS = 30000
NAVIGATION_TIMEOUT_MS = 35000
VENDOR_CONCURRENCY = 3
HTTP_TIMEOUT = 12.0
MIN_DELAY = 0.4
MAX_DELAY = 1.0

# Hard caps
VENDOR_TOTAL_TIMEOUT = 75  # seconds per vendor task
ROBOTS_TIMEOUT = 5.0       # seconds for robots.txt
MAX_CONTACT_PAGES = 6      # cap enrichment fetches

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

        # Wait a bit for content hydrate
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

        # Heuristic: if this page yielded 0 new links, stop paginating
        if page_links == 0:
            logging.info("No new links found on page %d; stopping pagination.", page_num)
            break

        page_num += 1
        await asyncio.sleep(rand_delay())

    return collected[:limit]



async def parse_vendor_page(page: Page, url: str) -> VendorPageData:
    vpd = VendorPageData()  # всегда создаём пустую структуру
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
        except Exception as e:
            logging.warning("Follow Us block not found for %s: %s", url, e)

        # 3 & 4) Contact Person
        members: List[TeamMember] = []
        team_section = None
        for label in ["Meet the Team", "Meet The Team", "Our Team", "Team"]:
            with contextlib.suppress(Exception):
                loc = page.locator("section", has_text=label)
                if await loc.count() > 0:
                    team_section = loc.first
                    break

        if team_section:
            for sel in ["article", "div[role='listitem']", "li", "div", "a"]:
                with contextlib.suppress(Exception):
                    cards = team_section.locator(sel)
                    cnt = await cards.count()
                    if cnt == 0:
                        continue
                    for i in range(cnt):
                        card = cards.nth(i)

                        # name
                        name = ""
                        with contextlib.suppress(Exception):
                            name = clean_text(await card.locator("h3, h4, strong, b").first.inner_text())
                        if not name:
                            with contextlib.suppress(Exception):
                                text = clean_text(await card.inner_text())
                                parts = [p for p in text.split("\n") if p.strip()]
                                if parts:
                                    name = parts[0][:120]

                        # job title
                        title = ""
                        with contextlib.suppress(Exception):
                            title = clean_text(await card.locator("em, i, small, span").first.inner_text())
                        if not title:
                            with contextlib.suppress(Exception):
                                text = clean_text(await card.inner_text())
                                parts = [p for p in text.split("\n") if p.strip()]
                                if len(parts) >= 2:
                                    title = parts[1][:160]

                        if name or title:
                            members.append(TeamMember(name=name, title=title, phone="", email=""))

                    if members:
                        break
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

    website = vpd.website if is_absolute_url(vpd.website) else f"http://{vpd.website}"
    paths = [website]
    for suffix in ["/contact", "/contact-us", "/contactus", "/about", "/team", "/about-us"]:
        paths.append(website.rstrip("/") + suffix)

    logging.info("Enrich: %s | checking up to %d pages", vpd.company_name or website, min(len(paths), MAX_CONTACT_PAGES))

    async with httpx.AsyncClient(follow_redirects=True, headers={
        "User-Agent": "Mozilla/5.0 (compatible; EventAgencyParser/1.0)"
    }) as client:
        html_blobs: List[str] = []
        for u in unique(paths)[:MAX_CONTACT_PAGES]:
            html = await fetch_text_httpx(u, client, check_robots=True)
            if html:
                html_blobs.append(html)
                logging.debug("Enrich fetched: %s (len=%d)", u, len(html))
            await asyncio.sleep(rand_delay(0.2, 0.5))

    if not html_blobs:
        return vpd

    all_text = "\n".join(html_blobs)
    emails = extract_emails_from_text(all_text)
    phones = extract_phones_from_text(all_text)

    if not vpd.phone and phones:
        vpd.phone = phones[0]

    if emails:
        for i, tm in enumerate(vpd.team):
            if not tm.email and i < len(emails):
                tm.email = emails[i]

    for html in html_blobs:
        soup = BeautifulSoup(html, "lxml")
        soc = extract_social_links_from_html(soup)
        if not vpd.instagram and soc.get("instagram"):
            vpd.instagram = soc["instagram"]
        if not vpd.facebook and soc.get("facebook"):
            vpd.facebook = soc["facebook"]

    return vpd


# ---------------------------
# Runner
# ---------------------------

async def process_vendor(link: str, browser: Browser, idx: int, total: int) -> VendorPageData:
    start = time.time()
    page = await browser.new_page()
    page.set_default_timeout(PAGE_TIMEOUT_MS)
    logging.info("Vendor %d/%d: start %s", idx, total, link)
    try:
        vpd = await parse_vendor_page(page, link)
        await asyncio.sleep(rand_delay())
        vpd = await enrich_from_official_site(vpd)
        logging.info("Vendor %d/%d: done %s in %.1fs (team=%d)", idx, total, vpd.company_name or link, time.time() - start, len(vpd.team))
        return vpd
    except Exception:
        logging.exception("Vendor %d/%d: fatal error %s", idx, total, link)
        return VendorPageData()
    finally:
        with contextlib.suppress(Exception):
            await page.close()


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
                if not v.company_name and not v.team and not v.website and not v.phone:
                    continue
                if v.team:
                    for tm in v.team:
                        rows.append(VendorRecord(
                            company_name=v.company_name,
                            website=v.website,
                            contact_person=tm.name,
                            job_title=tm.title,
                            phone=tm.phone or v.phone,
                            email=tm.email,
                            minimum_spend=v.minimum_spend,
                            instagram=v.instagram,
                            facebook=v.facebook,
                        ))
                else:
                    rows.append(VendorRecord(
                        company_name=v.company_name,
                        website=v.website,
                        contact_person="",
                        job_title="",
                        phone=v.phone,
                        email="",
                        minimum_spend=v.minimum_spend,
                        instagram=v.instagram,
                        facebook=v.facebook,
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


#ddkfjsdljkfjfdjfjdg
    #dkljfdfkg
