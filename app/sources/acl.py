from __future__ import annotations

from dataclasses import dataclass
import re

from app.models.paper import PaperRecord
from app.resolve.resolver import SourceCandidate


_ACL_DOI_RE = re.compile(r"^10\.18653/v1/(.+)$", re.IGNORECASE)


def _extract_acl_id_from_doi(doi: str | None) -> str | None:
    if not doi:
        return None
    match = _ACL_DOI_RE.match(doi.strip())
    if not match:
        return None
    return match.group(1).strip()


@dataclass(slots=True)
class ACLSourceProvider:
    name: str = "acl"

    def resolve(self, paper: PaperRecord) -> list[SourceCandidate]:
        acl_id = paper.acl_id or _extract_acl_id_from_doi(paper.doi)
        if not acl_id:
            return []

        landing_page_url = f"https://aclanthology.org/{acl_id}/"
        pdf_url = f"https://aclanthology.org/{acl_id}.pdf"

        return [
            SourceCandidate(
                source_name=self.name,
                pdf_url=pdf_url,
                landing_page_url=landing_page_url,
                version_type="publisher",
                host_type="publisher",
                license=None,
                domain="aclanthology.org",
                confidence=0.97,
                title_match_score=1.0,
                is_direct_pdf=True,
                reason="exact acl anthology id",
                metadata={
                    "acl_id": acl_id,
                },
            )
        ]