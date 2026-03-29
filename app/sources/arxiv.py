from __future__ import annotations

from dataclasses import dataclass

from app.models.paper import PaperRecord
from app.resolve.resolver import SourceCandidate


@dataclass(slots=True)
class ArxivSourceProvider:
    name: str = "arxiv"

    def resolve(self, paper: PaperRecord) -> list[SourceCandidate]:
        if not paper.arxiv_id:
            return []

        abs_url = f"https://arxiv.org/abs/{paper.arxiv_id}"
        pdf_url = f"https://arxiv.org/pdf/{paper.arxiv_id}"

        return [
            SourceCandidate(
                source_name=self.name,
                pdf_url=pdf_url,
                landing_page_url=abs_url,
                version_type="preprint",
                host_type="preprint",
                license=None,
                domain="arxiv.org",
                confidence=0.92,
                is_direct_pdf=True,
                reason="exact arxiv id",
                metadata={
                    "arxiv_id": paper.arxiv_id,
                },
            )
        ]