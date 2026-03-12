from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable


def write_dataset(records: list[dict], output_path: Path) -> None:
    output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def write_missing_report(
    missing_rows: Iterable[dict[str, str]],
    path: Path = Path("logs") / "missing_transcripts.csv",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id", "title", "reason"])
        writer.writeheader()
        for row in missing_rows:
            writer.writerow(
                {
                    "video_id": row.get("video_id", ""),
                    "title": row.get("title", ""),
                    "reason": row.get("reason", ""),
                }
            )


def write_success_report(
    records: Iterable[dict],
    path: Path = Path("logs") / "processed_videos.csv",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "video_id",
        "title",
        "transcript_chars",
        "career_sectors",
        "skills",
        "topics",
        "competency",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow(
                {
                    "video_id": row.get("video_id", ""),
                    "title": row.get("title", ""),
                    "transcript_chars": len(row.get("transcript", "") or ""),
                    "career_sectors": "|".join(row.get("career_sectors", []) or []),
                    "skills": "|".join(row.get("skills", []) or []),
                    "topics": "|".join(row.get("topics", []) or []),
                    "competency": row.get("competency", "") or "",
                }
            )


def write_match_debug_report(
    rows: Iterable[dict[str, str | float]],
    path: Path = Path("logs") / "match_debug.csv",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "video_id",
        "title",
        "activity_type",
        "status",
        "match_source",
        "matched_path",
        "match_score",
        "reason",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "video_id": row.get("video_id", ""),
                    "title": row.get("title", ""),
                    "activity_type": row.get("activity_type", ""),
                    "status": row.get("status", ""),
                    "match_source": row.get("match_source", ""),
                    "matched_path": row.get("matched_path", ""),
                    "match_score": row.get("match_score", ""),
                    "reason": row.get("reason", ""),
                }
            )
