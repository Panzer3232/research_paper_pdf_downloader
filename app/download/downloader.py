from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import requests

from app.config.models import DownloadConfig
from app.core.exceptions import DownloadError, PDFValidationError
from app.storage.writers import ensure_parent_dir


@dataclass(slots=True)
class DownloadResult:
    url: str
    final_url: str
    output_path: str
    content_type: str | None
    size_bytes: int
    sha256: str
    reused_existing: bool = False

    def to_dict(self) -> dict[str, str | int | bool | None]:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "output_path": self.output_path,
            "content_type": self.content_type,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "reused_existing": self.reused_existing,
        }


class PDFDownloader:
    def __init__(self, config: DownloadConfig) -> None:
        self.config = config

    def download(
        self,
        url: str,
        output_path: str | Path,
        *,
        skip_if_valid: bool = True,
    ) -> DownloadResult:
        target_path = Path(output_path)

        if skip_if_valid and target_path.exists():
            size_bytes = target_path.stat().st_size
            self._validate_existing_file(target_path, size_bytes=size_bytes)
            return DownloadResult(
                url=url,
                final_url=url,
                output_path=str(target_path),
                content_type="application/pdf",
                size_bytes=size_bytes,
                sha256=self._sha256_file(target_path),
                reused_existing=True,
            )

        ensure_parent_dir(target_path)
        tmp_path = target_path.with_suffix(target_path.suffix + ".part")

        try:
            with requests.Session() as session:
                session.headers.update(self._request_headers(url))
                try:
                    response = session.get(
                        url,
                        stream=True,
                        timeout=(self.config.connect_timeout_seconds, self.config.read_timeout_seconds),
                        allow_redirects=True,
                        verify=self.config.verify_ssl,
                    )
                except requests.RequestException as exc:
                    raise DownloadError(f"Failed to download PDF from url: {url}") from exc

                if response.status_code >= 400:
                    raise DownloadError(f"Download returned HTTP {response.status_code} for url: {url}")

                content_type = response.headers.get("Content-Type")
                final_url = str(response.url)
                total_bytes = 0
                first_bytes = b""
                sha256 = hashlib.sha256()

                try:
                    with tmp_path.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 128):
                            if not chunk:
                                continue

                            if not first_bytes:
                                first_bytes = chunk[:32]

                            total_bytes += len(chunk)
                            if total_bytes > self.config.max_pdf_bytes:
                                raise PDFValidationError(
                                    f"Downloaded file exceeds max size limit: {self.config.max_pdf_bytes}"
                                )

                            sha256.update(chunk)
                            handle.write(chunk)
                except Exception:
                    if tmp_path.exists():
                        tmp_path.unlink(missing_ok=True)
                    raise
        except (DownloadError, PDFValidationError):
            raise
        except Exception as exc:
            raise DownloadError(f"Failed to download PDF from url: {url}") from exc

        self._validate_downloaded_file(
            tmp_path,
            content_type=content_type,
            first_bytes=first_bytes,
            size_bytes=total_bytes,
        )

        tmp_path.replace(target_path)

        return DownloadResult(
            url=url,
            final_url=final_url,
            output_path=str(target_path),
            content_type=content_type,
            size_bytes=total_bytes,
            sha256=sha256.hexdigest(),
            reused_existing=False,
        )

    def _domain_from_url(self, url: str) -> str:
        try:
            from urllib.parse import urlparse
            netloc = urlparse(url).netloc.lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return netloc
        except Exception:
            return ""

    def _request_headers(self, url: str) -> dict[str, str]:
        """
        Return HTTP headers appropriate for the target domain.
        """
        _BROWSER_UA = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        _BROWSER_ACCEPT = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/pdf,*/*;q=0.8"
        )
        _BROWSER_DOMAINS = frozenset({
            "biorxiv.org",
            "medrxiv.org",
            "chemrxiv.org",
            "mdpi.com",
            "frontiersin.org",
            "f1000research.com",
            "peerj.com",
            "royalsocietypublishing.org",
            "plos.org",
            "journals.plos.org",
            "hindawi.com",
            "onlinelibrary.wiley.com",
            "wiley.com",
        })

        domain = self._domain_from_url(url)
        needs_browser_ua = any(
            domain == d or domain.endswith(f".{d}")
            for d in _BROWSER_DOMAINS
        )

        if needs_browser_ua:
            return {
                "User-Agent": _BROWSER_UA,
                "Accept": _BROWSER_ACCEPT,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }

        return {
            "User-Agent": self.config.user_agent,
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        }

    def _validate_existing_file(self, path: Path, *, size_bytes: int) -> None:
        if size_bytes < self.config.min_pdf_bytes:
            raise PDFValidationError(f"Existing PDF is too small to be valid: {path}")
        with path.open("rb") as handle:
            first_bytes = handle.read(8)
        if not self._looks_like_pdf(first_bytes):
            raise PDFValidationError(f"Existing file is not a valid PDF: {path}")

    def _validate_downloaded_file(
        self,
        path: Path,
        *,
        content_type: str | None,
        first_bytes: bytes,
        size_bytes: int,
    ) -> None:
        if size_bytes < self.config.min_pdf_bytes:
            raise PDFValidationError(
                f"Downloaded file is too small to be a valid PDF: {path}"
            )

        if not self._looks_like_pdf(first_bytes):
            raise PDFValidationError(f"Downloaded file does not look like a PDF: {path}")

        if content_type:
            lowered = content_type.split(";")[0].strip().lower()
            allowed = {item.lower() for item in self.config.allowed_content_types}
            if lowered not in allowed and not lowered.endswith("/pdf"):
                raise PDFValidationError(
                    f"Server returned unexpected content type '{lowered}' for url: {path}"
                )

    def _looks_like_pdf(self, first_bytes: bytes) -> bool:
        return first_bytes.startswith(b"%PDF-")

    def _sha256_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 128), b""):
                digest.update(chunk)
        return digest.hexdigest()