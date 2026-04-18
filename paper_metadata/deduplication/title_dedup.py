from __future__ import annotations

import re

_VERSION_SUFFIXES = re.compile(
    r'\s*[\(\[]?\s*'
    r'(extended version|extended abstract|preprint|arxiv|v\d+|version \d+|'
    r'workshop version|camera ready|camera-ready|under review|technical report)'
    r'\s*[\)\]]?\s*$',
    flags=re.IGNORECASE,
)


def normalize_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    t = title.lower()
    t = _VERSION_SUFFIXES.sub("", t)
    t = re.sub(r'[^a-z0-9\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _resolve_pair(existing: dict, challenger: dict) -> tuple[dict, dict]:
    existing_cit = existing.get("citationCount") or 0
    challenger_cit = challenger.get("citationCount") or 0
    if challenger_cit > existing_cit:
        return challenger, existing
    return existing, challenger


def deduplicate_intra_title(
    papers: list[dict],
    category_name: str,
) -> tuple[list[dict], list[dict]]:
    seen: dict[str, dict] = {}
    duplicate_rows: list[dict] = []

    for paper in papers:
        norm = normalize_title(paper.get("title", ""))
        if not norm:
            continue
        if norm not in seen:
            seen[norm] = paper
        else:
            keeper, dropped = _resolve_pair(seen[norm], paper)
            seen[norm] = keeper
            duplicate_rows.append({
                "category": category_name,
                "scope": "intra",
                "normalized_title": norm,
                "kept_paperId": keeper.get("paperId", "N/A"),
                "kept_citations": keeper.get("citationCount") or 0,
                "kept_title": keeper.get("title", ""),
                "dropped_paperId": dropped.get("paperId", "N/A"),
                "dropped_citations": dropped.get("citationCount") or 0,
                "dropped_title": dropped.get("title", ""),
            })

    return list(seen.values()), duplicate_rows


def deduplicate_inter_title(
    category_data: dict[str, list[dict]],
) -> tuple[dict[str, list[dict]], list[dict], int]:
    category_order = list(category_data.keys())
    global_seen: dict[str, dict] = {}
    retained_by_cat: dict[str, list[dict]] = {cat: [] for cat in category_order}
    duplicate_rows: list[dict] = []
    inter_dropped_count = 0

    for category_name in category_order:
        for paper in category_data[category_name]:
            norm = normalize_title(paper.get("title", ""))
            if not norm:
                retained_by_cat[category_name].append(paper)
                continue

            if norm not in global_seen:
                idx = len(retained_by_cat[category_name])
                retained_by_cat[category_name].append(paper)
                global_seen[norm] = {
                    "owner_category": category_name,
                    "paper": paper,
                    "idx": idx,
                }
            else:
                entry = global_seen[norm]
                owner_category = entry["owner_category"]
                existing = entry["paper"]
                keeper, dropped = _resolve_pair(existing, paper)

                if keeper is not existing:
                    retained_by_cat[owner_category][entry["idx"]] = keeper
                    entry["paper"] = keeper

                inter_dropped_count += 1
                duplicate_rows.append({
                    "owner_category": owner_category,
                    "challenger_category": category_name,
                    "scope": "inter",
                    "normalized_title": norm,
                    "kept_paperId": keeper.get("paperId", "N/A"),
                    "kept_citations": keeper.get("citationCount") or 0,
                    "kept_title": keeper.get("title", ""),
                    "dropped_paperId": dropped.get("paperId", "N/A"),
                    "dropped_citations": dropped.get("citationCount") or 0,
                    "dropped_title": dropped.get("title", ""),
                    "citation_swap_performed": keeper is not existing,
                })

    assigned = {cat: retained_by_cat[cat] for cat in category_order}
    return assigned, duplicate_rows, inter_dropped_count