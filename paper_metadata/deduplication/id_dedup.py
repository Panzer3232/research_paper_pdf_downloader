from __future__ import annotations


def deduplicate_intra(papers: list[dict]) -> tuple[list[dict], int]:
    seen: set[str] = set()
    unique: list[dict] = []
    dupe_count = 0

    for paper in papers:
        pid = paper.get("paperId")
        if not pid:
            unique.append(paper)
            continue
        if pid in seen:
            dupe_count += 1
        else:
            seen.add(pid)
            unique.append(paper)

    return unique, dupe_count


def deduplicate_inter(
    category_data: dict[str, list[dict]],
) -> dict[str, dict]:
    global_seen: set[str] = set()
    assigned: dict[str, dict] = {}

    for category_name, papers in category_data.items():
        retained: list[dict] = []
        removed = 0

        for paper in papers:
            pid = paper.get("paperId")
            if not pid or pid not in global_seen:
                retained.append(paper)
                if pid:
                    global_seen.add(pid)
            else:
                removed += 1

        assigned[category_name] = {
            "papers": retained,
            "inter_removed": removed,
        }

    return assigned