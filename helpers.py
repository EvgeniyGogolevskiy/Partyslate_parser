import random
from typing import List, Optional, Dict, Iterable, Tuple
import re
from urllib.parse import urlparse
import os
from bs4 import BeautifulSoup

# Patterns
EMAIL_REGEX = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_REGEX = re.compile(r"(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}")
SOCIAL_PATTERNS = {
    "instagram": re.compile(r"instagram\.com", re.IGNORECASE),
    "facebook": re.compile(r"(facebook|fb)\.com", re.IGNORECASE),
}
CURRENCY_VAL = re.compile(r"\$[\d,]+(?:\.\d{2})?")

def rand_delay(a: float = float(os.getenv("MIN_DELAY")), b: float = float(os.getenv("MAX_DELAY"))) -> float:
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