from __future__ import annotations

import logging
import re
import time
from difflib import SequenceMatcher

import arxiv
import requests
from bs4 import BeautifulSoup

from ..config.models import RecoveryConfig
from .base import RecoveryResult

logger = logging.getLogger(__name__)

_TIMEOUT_FAST = (5, 8)
_TIMEOUT_SLOW = (5, 12)
_TIMEOUT_SCRAPE = (5, 10)



def _clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())


def _title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_norm = re.sub(r'[^a-z0-9\s]', '', a.lower()).strip()
    b_norm = re.sub(r'[^a-z0-9\s]', '', b.lower()).strip()
    return SequenceMatcher(None, a_norm, b_norm).ratio()


def _get(url: str, headers: dict | None = None, params: dict | None = None, timeout=_TIMEOUT_FAST):
    try:
        return requests.get(url, headers=headers or {}, params=params, timeout=timeout)
    except Exception:
        return None


def _try(source_name: str, fetch_fn) -> tuple[str | None, str | None]:
    try:
        abstract = fetch_fn()
        if abstract:
            return abstract, source_name
    except Exception:
        pass
    return None, None



def fetch_from_arxiv(title: str, arxiv_id: str | None) -> str | None:
    client = arxiv.Client()

    def attempt(search_obj):
        sleep_time = 10
        for _ in range(3):
            try:
                result = next(client.results(search_obj), None)
                if result and result.summary:
                    return result, "SUCCESS"
                return None, "NOT_FOUND"
            except Exception as e:
                if '429' in str(e):
                    time.sleep(sleep_time)
                    sleep_time *= 2
                    continue
                return None, "ERROR"
        return None, "RATE_LIMITED"

    if arxiv_id:
        result, status = attempt(arxiv.Search(id_list=[arxiv_id]))
        if result:
            return _clean_text(result.summary.replace('\n', ' '))
        if status == "RATE_LIMITED":
            return None

    if title:
        result, status = attempt(arxiv.Search(query=f'ti:"{title}"', max_results=1))
        if result and _title_similarity(title, result.title) >= _ARXIV_THRESHOLD:
            return _clean_text(result.summary.replace('\n', ' '))
        if status == "RATE_LIMITED":
            return None

        clean_title = re.sub(r'[^a-zA-Z0-9\s]', '', title)
        if clean_title and clean_title != title:
            result, status = attempt(arxiv.Search(query=f'ti:"{clean_title}"', max_results=1))
            if result and _title_similarity(title, result.title) >= _ARXIV_THRESHOLD:
                return _clean_text(result.summary.replace('\n', ' '))

    return None


def fetch_from_pubmed(pmid: str) -> str | None:
    url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        f"?db=pubmed&id={pmid}&rettype=abstract&retmode=text"
    )
    response = _get(url, timeout=_TIMEOUT_SLOW)
    if response and response.status_code == 200 and len(response.text) > 20:
        return _clean_text(response.text)
    return None


def fetch_from_crossref(doi: str) -> str | None:
    response = _get(f"https://api.crossref.org/works/{doi}", timeout=_TIMEOUT_SLOW)
    if response and response.status_code == 200:
        abstract_xml = response.json().get('message', {}).get('abstract')
        if abstract_xml:
            return _clean_text(re.sub(r'<[^>]+>', '', abstract_xml))
    return None


def fetch_from_acl(acl_id: str) -> str | None:
    response = _get(f"https://aclanthology.org/{acl_id}/", timeout=_TIMEOUT_SCRAPE)
    if response and response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        abstract_div = soup.find('div', class_='acl-abstract')
        if abstract_div:
            span = abstract_div.find('span')
            if span:
                span.decompose()
            return _clean_text(abstract_div.get_text(strip=True))
    return None


def fetch_from_semantic_scholar(corpus_id: str, ss_api_key: str = "") -> str | None:
    url = f"https://api.semanticscholar.org/graph/v1/paper/CorpusId:{corpus_id}?fields=abstract"
    headers = {"x-api-key": ss_api_key} if ss_api_key else {}
    for _ in range(3):
        response = _get(url, headers=headers, timeout=_TIMEOUT_FAST)
        if not response:
            return None
        if response.status_code == 200:
            abstract = response.json().get('abstract')
            return _clean_text(abstract) if abstract else None
        if response.status_code == 429:
            time.sleep(3)
            continue
        return None
    return None


def fetch_from_openalex(doi: str | None, title: str | None, similarity_threshold: float) -> str | None:
    def reconstruct_abstract(inverted_index: dict) -> str | None:
        if not inverted_index:
            return None
        index_length = max(pos for positions in inverted_index.values() for pos in positions) + 1
        words = [''] * index_length
        for word, positions in inverted_index.items():
            for pos in positions:
                words[pos] = word
        return _clean_text(' '.join(words))

    if doi:
        response = _get(
            f"https://api.openalex.org/works/doi:{doi}",
            params={"select": "abstract_inverted_index"},
            timeout=_TIMEOUT_FAST,
        )
        if response and response.status_code == 200:
            abstract = reconstruct_abstract(response.json().get('abstract_inverted_index'))
            if abstract:
                return abstract

    if title:
        response = _get(
            "https://api.openalex.org/works",
            params={
                "filter": f"title.search:{title}",
                "select": "title,abstract_inverted_index",
                "per_page": 1,
            },
            timeout=_TIMEOUT_FAST,
        )
        if response and response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                hit = results[0]
                if _title_similarity(title, hit.get('title', '')) >= similarity_threshold:
                    abstract = reconstruct_abstract(hit.get('abstract_inverted_index'))
                    if abstract:
                        return abstract
    return None


def fetch_from_europe_pmc(
    doi: str | None,
    pmid: str | None,
    title: str | None,
    similarity_threshold: float,
) -> str | None:
    base = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    def parse_result(results: list, query_title: str | None = None) -> str | None:
        if not results:
            return None
        hit = results[0]
        if query_title and _title_similarity(query_title, hit.get('title', '')) < similarity_threshold:
            return None
        abstract = hit.get('abstractText')
        return _clean_text(abstract) if abstract else None

    if doi:
        response = _get(
            base,
            params={"query": f"DOI:{doi}", "format": "json", "resultType": "core", "pageSize": 1},
            timeout=_TIMEOUT_SLOW,
        )
        if response and response.status_code == 200:
            abstract = parse_result(response.json().get('resultList', {}).get('result', []))
            if abstract:
                return abstract

    if pmid:
        response = _get(
            base,
            params={"query": f"EXT_ID:{pmid} AND SRC:MED", "format": "json", "resultType": "core", "pageSize": 1},
            timeout=_TIMEOUT_SLOW,
        )
        if response and response.status_code == 200:
            abstract = parse_result(response.json().get('resultList', {}).get('result', []))
            if abstract:
                return abstract

    if title:
        response = _get(
            base,
            params={"query": f'TITLE:"{title}"', "format": "json", "resultType": "core", "pageSize": 1},
            timeout=_TIMEOUT_SLOW,
        )
        if response and response.status_code == 200:
            abstract = parse_result(
                response.json().get('resultList', {}).get('result', []),
                query_title=title,
            )
            if abstract:
                return abstract

    return None


def fetch_from_core(
    doi: str | None,
    title: str | None,
    core_api_key: str,
    similarity_threshold: float,
) -> str | None:
    base_url = "https://api.core.ac.uk/v3/search/works"
    headers = {"Accept": "application/json"}
    if core_api_key:
        headers["Authorization"] = f"Bearer {core_api_key}"

    def _query(q: str) -> list[dict]:
        try:
            response = requests.get(
                base_url,
                params={"q": q, "limit": 3},
                headers=headers,
                timeout=_TIMEOUT_SLOW,
            )
        except Exception:
            return []
        if response.status_code in (429, 401, 403) or response.status_code >= 400:
            return []
        try:
            payload = response.json()
        except ValueError:
            return []
        return [r for r in (payload.get("results") or []) if isinstance(r, dict)]

    if doi:
        results = _query(f'doi:"{doi.strip()}"')
        for work in results:
            abstract = work.get("abstract")
            if abstract and isinstance(abstract, str):
                cleaned = _clean_text(abstract)
                if cleaned:
                    return cleaned

    if title:
        results = _query(f'title:"{title}"')
        for work in results:
            work_title = work.get("title") or work.get("displayTitle") or ""
            if _title_similarity(title, work_title) < similarity_threshold:
                continue
            abstract = work.get("abstract")
            if abstract and isinstance(abstract, str):
                cleaned = _clean_text(abstract)
                if cleaned:
                    return cleaned

    return None



_ARXIV_THRESHOLD: float = 0.85


class ApiRecoveryProvider:
    name = "api"

    def __init__(self, config: RecoveryConfig, ss_api_key: str = "", core_api_key: str = "") -> None:
        self._threshold = config.similarity_threshold
        self._sleep = config.api_sleep_between_papers
        self._ss_api_key = ss_api_key
        self._core_api_key = core_api_key

        global _ARXIV_THRESHOLD
        _ARXIV_THRESHOLD = config.similarity_threshold

    def recover(self, paper: dict) -> RecoveryResult | None:
        title = paper.get("title", "")
        external_ids = paper.get("externalIds") or {}
        abstract, source = self._extract_abstract(title, external_ids)
        if abstract:
            return RecoveryResult(abstract=abstract, source=source)
        return None

    def _extract_abstract(self, title: str, external_ids: dict) -> tuple[str | None, str | None]:
        if not isinstance(external_ids, dict):
            external_ids = {}

        arxiv_id = external_ids.get('ArXiv')
        if not arxiv_id:
            doi_raw = external_ids.get('DOI', '')
            dblp = external_ids.get('DBLP', '')
            if doi_raw and 'arXiv' in doi_raw:
                arxiv_id = doi_raw.split('arXiv.')[-1]
            elif dblp and 'journals/corr/abs-' in dblp:
                arxiv_id = dblp.split('abs-')[-1].replace('-', '.')

        doi = external_ids.get('DOI')
        pmid = external_ids.get('PubMed')
        acl_id = external_ids.get('ACL')
        corpus_id = external_ids.get('CorpusId')

        threshold = self._threshold
        ss_key = self._ss_api_key
        core_key = self._core_api_key

        abstract, source = None, None

        if arxiv_id:
          

            abstract, source = _try("ArXiv", lambda: fetch_from_arxiv(title, arxiv_id))
            if abstract:
                return abstract, source

            if doi:
                abstract, source = _try("OpenAlex", lambda: fetch_from_openalex(doi, None, threshold))
                if abstract:
                    return abstract, source

            abstract, source = _try("OpenAlex", lambda: fetch_from_openalex(None, title, threshold))
            if abstract:
                return abstract, source

            if pmid:
                abstract, source = _try("PubMed", lambda: fetch_from_pubmed(pmid))
                if abstract:
                    return abstract, source

            if acl_id:
                abstract, source = _try("ACL Anthology", lambda: fetch_from_acl(acl_id))
                if abstract:
                    return abstract, source

            if doi or pmid:
                abstract, source = _try("Europe PMC", lambda: fetch_from_europe_pmc(doi, pmid, None, threshold))
                if abstract:
                    return abstract, source

            if doi:
                abstract, source = _try("Crossref", lambda: fetch_from_crossref(doi))
                if abstract:
                    return abstract, source

        else:
           

            if doi:
                abstract, source = _try("OpenAlex", lambda: fetch_from_openalex(doi, None, threshold))
                if abstract:
                    return abstract, source

            if pmid:
                abstract, source = _try("PubMed", lambda: fetch_from_pubmed(pmid))
                if abstract:
                    return abstract, source

            if acl_id:
                abstract, source = _try("ACL Anthology", lambda: fetch_from_acl(acl_id))
                if abstract:
                    return abstract, source

            if doi:
                abstract, source = _try("Europe PMC", lambda: fetch_from_europe_pmc(doi, None, None, threshold))
                if abstract:
                    return abstract, source

            if pmid:
                abstract, source = _try("Europe PMC", lambda: fetch_from_europe_pmc(None, pmid, None, threshold))
                if abstract:
                    return abstract, source

            if doi:
                abstract, source = _try("Crossref", lambda: fetch_from_crossref(doi))
                if abstract:
                    return abstract, source

            abstract, source = _try("OpenAlex", lambda: fetch_from_openalex(None, title, threshold))
            if abstract:
                return abstract, source

            abstract, source = _try("Europe PMC", lambda: fetch_from_europe_pmc(None, None, title, threshold))
            if abstract:
                return abstract, source

       
        abstract, source = _try("CORE", lambda: fetch_from_core(doi, title, core_key, threshold))
        if abstract:
            return abstract, source

        
        if corpus_id:
            abstract, source = _try("Semantic Scholar", lambda: fetch_from_semantic_scholar(corpus_id, ss_key))
            if abstract:
                return abstract, source

        return None, None