from __future__ import annotations

import re

TIMESTAMP_RE = re.compile(
    r"^(?:\d{2}:)?\d{2}:\d{2}[,\.]\d{3}\s+-->\s+(?:\d{2}:)?\d{2}:\d{2}[,\.]\d{3}(?:\s+.*)?$"
)
INDEX_RE = re.compile(r"^\d+$")
WHITESPACE_RE = re.compile(r"\s+")


def srt_to_clean_text(srt_content: str) -> str:
    lines = srt_content.splitlines()
    cleaned_lines: list[str] = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.upper() == "WEBVTT":
            continue
        if line.startswith("NOTE ") or line == "NOTE":
            continue
        if line.startswith("STYLE") or line.startswith("REGION"):
            continue
        if INDEX_RE.match(line):
            continue
        if TIMESTAMP_RE.match(line):
            continue
        cleaned_lines.append(line)

    deduped_lines: list[str] = []
    for line in cleaned_lines:
        if deduped_lines and deduped_lines[-1] == line:
            continue
        deduped_lines.append(line)

    text = " ".join(deduped_lines)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text
