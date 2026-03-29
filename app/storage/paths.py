from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from app.config.models import OutputConfig


_SAFE_COMPONENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_path_component(value: str, *, max_length: int = 180) -> str:
    cleaned = _SAFE_COMPONENT_RE.sub("_", value.strip()).strip("._")
    if not cleaned:
        cleaned = "unknown"
    return cleaned[:max_length]


@dataclass(slots=True)
class PathResolver:
    output: OutputConfig

    @property
    def root_dir(self) -> Path:
        return Path(self.output.root_dir)

    @property
    def input_dir(self) -> Path:
        return self.root_dir / self.output.input_dir_name

    @property
    def metadata_dir(self) -> Path:
        return self.root_dir / self.output.metadata_dir_name

    @property
    def manifests_dir(self) -> Path:
        return self.root_dir / self.output.manifests_dir_name

    @property
    def pdfs_dir(self) -> Path:
        return self.root_dir / self.output.pdfs_dir_name

    @property
    def reports_dir(self) -> Path:
        return self.root_dir / self.output.reports_dir_name

    def ensure_base_dirs(self) -> None:
        for path in (
            self.root_dir,
            self.input_dir,
            self.metadata_dir,
            self.manifests_dir,
            self.pdfs_dir,
            self.reports_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def normalized_key(self, paper_key: str) -> str:
        return sanitize_path_component(paper_key)

    def metadata_path(self, paper_key: str) -> Path:
        return self.metadata_dir / f"{self.normalized_key(paper_key)}.json"

    def manifest_path(self, paper_key: str) -> Path:
        return self.manifests_dir / f"{self.normalized_key(paper_key)}.json"

    def pdf_path(self, paper_key: str, extension: str = ".pdf") -> Path:
        ext = extension if extension.startswith(".") else f".{extension}"
        return self.pdfs_dir / f"{self.normalized_key(paper_key)}{ext}"

    def report_path(self, filename: str) -> Path:
        return self.reports_dir / sanitize_path_component(filename, max_length=220)