from __future__ import annotations

import json
import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from ..config.models import RecoveryConfig
from .base import RecoveryResult

logger = logging.getLogger(__name__)

_DOI_PREFIX_TO_PUBLISHER: dict[str, str] = {
    "10.1007": "Springer",
    "10.1038": "Nature",
    "10.1023": "Springer",
    "10.1057": "Springer",
    "10.1140": "Springer",
    "10.1245": "Springer",
    "10.1617": "Springer",
    "10.1365": "Springer",
    "10.1134": "Springer",
    "10.1186": "Springer",
    "10.1016": "Elsevier",
    "10.1053": "Elsevier",
    "10.1067": "Elsevier",
    "10.1078": "Elsevier",
    "10.1054": "Elsevier",
    "10.1109": "IEEE",
    "10.1145": "ACM",
    "10.1002": "Wiley",
    "10.1111": "Wiley",
    "10.1080": "Taylor & Francis",
    "10.1081": "Taylor & Francis",
    "10.1017": "Cambridge UP",
    "10.1093": "Oxford UP",
    "10.3389": "Frontiers",
    "10.3390": "MDPI",
    "10.1371": "PLOS",
    "10.1162": "MIT Press",
    "10.1609": "AAAI",
    "10.24963": "IJCAI",
    "10.1561": "Now Publishers",
}

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}


def _publisher_from_doi(doi: str | None) -> str:
    if not doi:
        return "Unknown"
    prefix = doi.split("/")[0]
    return _DOI_PREFIX_TO_PUBLISHER.get(prefix, "Unknown")


def _publisher_from_url(url: str) -> str:
    u = url.lower()
    if "link.springer.com" in u:
        return "Springer"
    if "nature.com" in u:
        return "Nature"
    if "sciencedirect.com" in u or "linkinghub.elsevier.com" in u:
        return "Elsevier"
    if "ieeexplore.ieee.org" in u:
        return "IEEE"
    if "dl.acm.org" in u:
        return "ACM"
    if "onlinelibrary.wiley.com" in u:
        return "Wiley"
    if "tandfonline.com" in u:
        return "Taylor & Francis"
    if "cambridge.org" in u:
        return "Cambridge UP"
    if "academic.oup.com" in u or "oup.com" in u:
        return "Oxford UP"
    if "frontiersin.org" in u:
        return "Frontiers"
    if "mdpi.com" in u:
        return "MDPI"
    if "journals.plos.org" in u or "plos.org" in u:
        return "PLOS"
    if "aaai.org" in u:
        return "AAAI"
    if "ijcai.org" in u:
        return "IJCAI"
    return "Unknown"



def _clean(text: str | None, min_len: int) -> str | None:
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^(abstract[:\s]+)', '', text, flags=re.IGNORECASE)
    return text if len(text) >= min_len else None


def _from_json_ld(soup: BeautifulSoup, min_len: int) -> str | None:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.string or ""
            data = json.loads(raw)
            if isinstance(data, list):
                data = data[0] if data else {}
            for key in ("description", "abstract"):
                val = data.get(key)
                if val and isinstance(val, str):
                    return _clean(val, min_len)
        except Exception:
            pass
    return None


def _from_meta(soup: BeautifulSoup, name: str, prop: str = "name", min_len: int = 80) -> str | None:
    tag = soup.find("meta", attrs={prop: name})
    if tag:
        return _clean(tag.get("content", ""), min_len)
    return None


def _generic_fallback(soup: BeautifulSoup, min_len: int) -> str | None:
    abstract = _from_json_ld(soup, min_len)
    if abstract:
        return abstract
    abstract = _from_meta(soup, "og:description", prop="property", min_len=min_len)
    if abstract:
        return abstract
    abstract = _from_meta(soup, "description", min_len=min_len)
    if abstract:
        return abstract
    abstract = _from_meta(soup, "twitter:description", min_len=min_len)
    if abstract:
        return abstract
    return None



def _parse_springer(soup: BeautifulSoup, min_len: int) -> str | None:
    section = soup.find(
        "section",
        attrs={"data-title": re.compile(r"^abstract$", re.IGNORECASE)},
    )
    if section:
        content_div = section.find("div", class_=re.compile(r"c-article-section__content"))
        target = content_div if content_div else section
        return _clean(target.get_text(separator=" ", strip=True), min_len)

    abs_div = soup.find(id=re.compile(r"Abs\d+-content"))
    if abs_div:
        return _clean(abs_div.get_text(separator=" ", strip=True), min_len)

    for cls_pattern in [
        re.compile(r"c-article-teaser-text", re.IGNORECASE),
        re.compile(r"abstract-content", re.IGNORECASE),
    ]:
        div = soup.find(class_=cls_pattern)
        if div:
            return _clean(div.get_text(separator=" ", strip=True), min_len)

    return _generic_fallback(soup, min_len)


def _parse_ieee(html: str, soup: BeautifulSoup, min_len: int) -> str | None:
    marker = "xplGlobal.document.metadata="
    pos = html.find(marker)
    if pos != -1:
        start = pos + len(marker)
        depth = 0
        end = start
        for i in range(start, min(start + 200_000, len(html))):
            ch = html[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end > start:
            try:
                data = json.loads(html[start:end])
                abstract = data.get("abstract") or data.get("Abstract")
                if abstract:
                    return _clean(abstract, min_len)
            except Exception:
                pass
    return _generic_fallback(soup, min_len)


def _parse_elsevier(soup: BeautifulSoup, min_len: int) -> str | None:
    return _generic_fallback(soup, min_len)


def _parse_wiley(soup: BeautifulSoup, min_len: int) -> str | None:
    for attrs in [
        {"class": re.compile(r"article-section__content\s+en\s+main", re.IGNORECASE)},
        {"class": re.compile(r"abstract-group", re.IGNORECASE)},
        {"id": re.compile(r"abstract", re.IGNORECASE)},
    ]:
        div = soup.find("div", attrs)
        if div:
            result = _clean(div.get_text(separator=" ", strip=True), min_len)
            if result:
                return result

    section = soup.find("section", class_=re.compile(r"abstract", re.IGNORECASE))
    if section:
        paras = section.find_all("p")
        if paras:
            return _clean(" ".join(p.get_text(strip=True) for p in paras), min_len)

    return _generic_fallback(soup, min_len)


def _parse_taylor_francis(soup: BeautifulSoup, min_len: int) -> str | None:
    div = soup.find(
        "div",
        class_=re.compile(r"abstractSection|abstract-wrap|hlFld-Abstract", re.IGNORECASE),
    )
    if div:
        paras = div.find_all("p")
        text = (
            " ".join(p.get_text(strip=True) for p in paras)
            if paras
            else div.get_text(separator=" ", strip=True)
        )
        return _clean(text, min_len)
    return _generic_fallback(soup, min_len)


def _parse_oxford(soup: BeautifulSoup, min_len: int) -> str | None:
    for tag in ("section", "div"):
        el = soup.find(tag, class_=re.compile(r"\babstract\b", re.IGNORECASE))
        if el:
            paras = el.find_all("p")
            if paras:
                return _clean(" ".join(p.get_text(strip=True) for p in paras), min_len)
            return _clean(el.get_text(separator=" ", strip=True), min_len)
    return _generic_fallback(soup, min_len)


def _parse_cambridge(soup: BeautifulSoup, min_len: int) -> str | None:
    for attrs in [
        {"class": re.compile(r"\babstract\b", re.IGNORECASE)},
        {"id": "abstract"},
    ]:
        div = soup.find("div", attrs)
        if div:
            paras = div.find_all("p")
            text = (
                " ".join(p.get_text(strip=True) for p in paras)
                if paras
                else div.get_text(separator=" ", strip=True)
            )
            result = _clean(text, min_len)
            if result:
                return result
    return _generic_fallback(soup, min_len)


def _parse_frontiers(soup: BeautifulSoup, min_len: int) -> str | None:
    div = soup.find(
        class_=re.compile(r"JournalAbstract|abstract-text|AbstractSection", re.IGNORECASE)
    )
    if div:
        return _clean(div.get_text(separator=" ", strip=True), min_len)
    return _generic_fallback(soup, min_len)


def _parse_mdpi(soup: BeautifulSoup, min_len: int) -> str | None:
    for cls in [
        re.compile(r"art-abstract", re.IGNORECASE),
        re.compile(r"abstract-div", re.IGNORECASE),
    ]:
        div = soup.find(class_=cls)
        if div:
            return _clean(div.get_text(separator=" ", strip=True), min_len)
    return _generic_fallback(soup, min_len)


def _parse_acm(soup: BeautifulSoup, min_len: int) -> str | None:
    for attrs in [
        {"class": re.compile(r"abstractSection", re.IGNORECASE)},
        {"class": re.compile(r"article__abstract", re.IGNORECASE)},
        {"role": "paragraph", "class": re.compile(r"abstract", re.IGNORECASE)},
    ]:
        div = soup.find("div", attrs)
        if div:
            paras = div.find_all("p")
            text = (
                " ".join(p.get_text(strip=True) for p in paras)
                if paras
                else div.get_text(separator=" ", strip=True)
            )
            result = _clean(text, min_len)
            if result:
                return result
    return _generic_fallback(soup, min_len)


def _parse_plos(soup: BeautifulSoup, min_len: int) -> str | None:
    div = soup.find("div", class_=re.compile(r"abstract", re.IGNORECASE))
    if div:
        paras = div.find_all("p")
        if paras:
            return _clean(" ".join(p.get_text(strip=True) for p in paras), min_len)
        return _clean(div.get_text(separator=" ", strip=True), min_len)
    return _generic_fallback(soup, min_len)


def _parse_aaai(soup: BeautifulSoup, min_len: int) -> str | None:
    div = soup.find("div", class_=re.compile(r"abstract", re.IGNORECASE))
    if div:
        return _clean(div.get_text(separator=" ", strip=True), min_len)
    return _generic_fallback(soup, min_len)


def _dispatch(publisher: str, html: str, soup: BeautifulSoup, min_len: int) -> str | None:
    if publisher in ("Springer", "Nature"):
        return _parse_springer(soup, min_len)
    if publisher == "Elsevier":
        return _parse_elsevier(soup, min_len)
    if publisher == "IEEE":
        return _parse_ieee(html, soup, min_len)
    if publisher == "Wiley":
        return _parse_wiley(soup, min_len)
    if publisher == "Taylor & Francis":
        return _parse_taylor_francis(soup, min_len)
    if publisher == "Oxford UP":
        return _parse_oxford(soup, min_len)
    if publisher == "Cambridge UP":
        return _parse_cambridge(soup, min_len)
    if publisher == "Frontiers":
        return _parse_frontiers(soup, min_len)
    if publisher == "MDPI":
        return _parse_mdpi(soup, min_len)
    if publisher == "ACM":
        return _parse_acm(soup, min_len)
    if publisher == "PLOS":
        return _parse_plos(soup, min_len)
    if publisher in ("AAAI", "IJCAI"):
        return _parse_aaai(soup, min_len)
    return _generic_fallback(soup, min_len)



def _safe_get(
    url: str,
    session: requests.Session,
    timeout: tuple,
    max_retries: int,
    allow_redirects: bool = True,
) -> requests.Response | None:
    for attempt in range(max_retries):
        try:
            resp = session.get(
                url,
                headers=_HEADERS,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 20))
                logger.warning("Rate limited on %s — waiting %ds", url, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp
            return None
        except requests.exceptions.Timeout:
            logger.warning("Timeout on %s (attempt %d)", url, attempt + 1)
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as exc:
            logger.warning("Request error on %s: %s", url, exc)
            return None
    return None


def _scrape_abstract(
    doi: str,
    session: requests.Session,
    request_delay: float,
    timeout: tuple,
    max_retries: int,
    min_len: int,
) -> tuple[str | None, str]:
    if not doi:
        return None, "Unknown"

    publisher_from_doi = _publisher_from_doi(doi)

    if publisher_from_doi in ("Springer", "Nature"):
        for url_template in [
            f"https://link.springer.com/article/{doi}",
            f"https://link.springer.com/chapter/{doi}",
            f"https://www.nature.com/articles/{doi.split('/')[-1]}",
        ]:
            time.sleep(request_delay)
            resp = _safe_get(url_template, session, timeout, max_retries)
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                abstract = _parse_springer(soup, min_len)
                if abstract:
                    return abstract, publisher_from_doi

    time.sleep(request_delay)
    resp = _safe_get(f"https://doi.org/{doi}", session, timeout, max_retries, allow_redirects=True)
    if not resp:
        return None, publisher_from_doi

    final_url = resp.url
    resolved_publisher = _publisher_from_url(final_url)
    if resolved_publisher == "Unknown":
        resolved_publisher = publisher_from_doi

    soup = BeautifulSoup(resp.text, "html.parser")
    abstract = _dispatch(resolved_publisher, resp.text, soup, min_len)
    return abstract, resolved_publisher



class ScrapeRecoveryProvider:
    name = "scrape"

    def __init__(self, config: RecoveryConfig) -> None:
        self._request_delay = config.request_delay
        self._timeout = config.scrape_timeout
        self._max_retries = config.scrape_max_retries
        self._min_len = config.min_abstract_len
        self._session = requests.Session()

    def __del__(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass

    def recover(self, paper: dict) -> RecoveryResult | None:
        doi = (paper.get("externalIds") or {}).get("DOI", "")
        if not doi:
            return None
        abstract, resolved_publisher = _scrape_abstract(
            doi=doi,
            session=self._session,
            request_delay=self._request_delay,
            timeout=self._timeout,
            max_retries=self._max_retries,
            min_len=self._min_len,
        )
        if abstract:
            return RecoveryResult(abstract=abstract, source=f"scrape:{resolved_publisher}")
        return None