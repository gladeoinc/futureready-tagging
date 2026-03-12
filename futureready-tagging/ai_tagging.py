from __future__ import annotations

import hashlib
import json
import random
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from config import AppConfig


ALLOWED_CAREER_SECTORS = [
    "STEM",
    "Health Science",
    "Business",
    "Information Technology",
    "Human Services",
    "Education",
    "Hospitality",
    "Marketing",
    "All Careers",
]

ALLOWED_SKILLS = [
    "Communication",
    "Critical Thinking",
    "Leadership",
    "Teamwork",
    "Creativity",
    "Adaptability",
    "Self Awareness",
    "Professionalism",
    "Problem Solving",
    "Goal Setting",
]

ALLOWED_TOPICS = [
    "Career Exploration",
    "Passions",
    "Resume Writing",
    "Interview Skills",
    "Networking",
    "Workplace Culture",
    "Entrepreneurship",
    "Financial Literacy",
    "Growth Mindset",
    "Transferable Skills",
]

ALLOWED_COMPETENCIES = [
    "Self Awareness",
    "Career Exploration",
    "Workforce Readiness",
]


def _build_canonical_map(values: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for value in values:
        mapping[_normalize_key(value)] = value
    return mapping


def _normalize_key(value: str) -> str:
    return " ".join(value.replace("_", " ").replace("-", " ").strip().lower().split())


CAREER_MAP = _build_canonical_map(ALLOWED_CAREER_SECTORS)
SKILL_MAP = _build_canonical_map(ALLOWED_SKILLS)
TOPIC_MAP = _build_canonical_map(ALLOWED_TOPICS)
COMP_MAP = _build_canonical_map(ALLOWED_COMPETENCIES)


class TaggingError(ValueError):
    pass


class AITagger:
    def __init__(self, config: AppConfig):
        self.config = config
        self.client = OpenAI(api_key=config.openai_api_key)

    def generate_tags(self, video_id: str, title: str, transcript: str) -> tuple[dict[str, Any], bool]:
        cache_path = self._tag_cache_path(video_id, title, transcript)
        if cache_path.exists():
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            return payload, True

        raw_json = self._call_openai_with_retry(title, transcript)
        parsed = self._parse_json(raw_json)
        normalized = self._normalize_tag_payload(parsed)

        cache_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
        return normalized, False

    def _call_openai_with_retry(self, title: str, transcript: str) -> str:
        prompt = self._build_prompt(title, transcript)
        last_err: Exception | None = None

        for attempt in range(1, self.config.openai_max_retries + 1):
            try:
                response = self.client.responses.create(
                    model=self.config.openai_model,
                    input=prompt,
                    temperature=0,
                )
                text = response.output_text.strip()
                if not text:
                    raise TaggingError("OpenAI returned empty output")
                return text
            except Exception as exc:
                last_err = exc
                if attempt >= self.config.openai_max_retries:
                    break
                delay = self.config.openai_retry_base_delay * (2 ** (attempt - 1))
                delay += random.uniform(0, 0.5)
                time.sleep(delay)

        raise TaggingError(f"OpenAI tagging failed after retries: {last_err}")

    def _build_prompt(self, title: str, transcript: str) -> str:
        return f"""You are tagging educational videos for the FutureReady career development curriculum.

Based on the title and transcript, generate:
1. Career sectors
2. Skills taught
3. Learning topics
4. FutureReady competency

Allowed values:
Career sectors: {', '.join(ALLOWED_CAREER_SECTORS)}
Skills: {', '.join(ALLOWED_SKILLS)}
Topics: {', '.join(ALLOWED_TOPICS)}
Competencies: {', '.join(ALLOWED_COMPETENCIES)}

Return JSON only using this schema:
{{
  "career_sectors": ["..."],
  "skills": ["..."],
  "topics": ["..."],
  "competency": "..."
}}

TITLE:
{title}

TRANSCRIPT:
{transcript}
"""

    def _parse_json(self, raw_json: str) -> dict[str, Any]:
        raw = raw_json.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise TaggingError(f"Model output was not valid JSON: {raw_json}") from exc

        if not isinstance(parsed, dict):
            raise TaggingError("Model output JSON must be an object")
        return parsed

    def _normalize_tag_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        career = _normalize_list(payload.get("career_sectors", []), CAREER_MAP, ALLOWED_CAREER_SECTORS)
        skills = _normalize_list(payload.get("skills", []), SKILL_MAP, ALLOWED_SKILLS)
        topics = _normalize_list(payload.get("topics", []), TOPIC_MAP, ALLOWED_TOPICS)

        competency_raw = payload.get("competency")
        competency = None
        if isinstance(competency_raw, str):
            competency = COMP_MAP.get(_normalize_key(competency_raw))

        return {
            "career_sectors": career,
            "skills": skills,
            "topics": topics,
            "competency": competency,
        }

    def _tag_cache_path(self, video_id: str, title: str, transcript: str) -> Path:
        digest = hashlib.sha256(f"{title}\n{transcript}".encode("utf-8")).hexdigest()[:16]
        safe_video_id = video_id.replace("/", "_")
        return self.config.cache_tags_dir / f"{safe_video_id}_{digest}.json"


def _normalize_list(
    value: Any,
    canonical_map: dict[str, str],
    allowed_order: list[str],
) -> list[str]:
    if not isinstance(value, list):
        return []

    seen: set[str] = set()
    selected: list[str] = []

    for item in value:
        if not isinstance(item, str):
            continue
        canonical = canonical_map.get(_normalize_key(item))
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        selected.append(canonical)

    order_index = {item: idx for idx, item in enumerate(allowed_order)}
    selected.sort(key=lambda x: order_index[x])
    return selected
