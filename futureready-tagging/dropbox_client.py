from __future__ import annotations

import csv
import difflib
import re
from pathlib import Path

from config import AppConfig

MATCH_THRESHOLD = 0.60
LOWER_MATCH_THRESHOLD = 0.45
STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "your",
    "into",
    "from",
    "that",
    "this",
    "what",
    "about",
    "how",
    "why",
    "who",
    "have",
    "are",
    "you",
    "our",
    "out",
    "all",
    "now",
    "part",
    "video",
    "career",
}
IGNORE_FILENAME_TOKENS = {"mdeleon", "1080", "720", "480", "v1"}
TRANSCRIPT_EXTENSIONS = (".srt", ".vtt")


class DropboxTranscriptClient:
    def __init__(self, config: AppConfig):
        self.config = config
        # Local transcript mode is strict: never call Dropbox when a local directory is provided.
        use_dropbox = (
            bool(config.dropbox_access_token)
            and not config.disable_dropbox
            and config.local_transcripts_dir is None
        )
        self._dropbox_api_error = None
        self._dropbox_file_metadata = None
        self.client = None
        if use_dropbox:
            import dropbox
            from dropbox.exceptions import ApiError
            from dropbox.files import FileMetadata

            self.client = dropbox.Dropbox(config.dropbox_access_token)
            self._dropbox_api_error = ApiError
            self._dropbox_file_metadata = FileMetadata
        self._transcript_index: dict[str, str] | None = None
        self._local_index: list[dict[str, str | Path]] | None = None
        self._manual_by_video_id, self._manual_by_title = self._load_manual_overrides()

    def get_transcript_srt(
        self, video_id: str, title: str | None = None
    ) -> tuple[str | None, str, dict[str, str | float]]:
        manual_path = self._resolve_manual_override(video_id, title)
        if manual_path is not None:
            return (
                manual_path.read_text(encoding="utf-8", errors="ignore"),
                "manual_override",
                {"matched_path": str(manual_path), "match_score": 1.0},
            )

        cache_path = self._cache_path(video_id)
        if cache_path.exists():
            return (
                cache_path.read_text(encoding="utf-8"),
                "cache_video_id",
                {"matched_path": str(cache_path), "match_score": 1.0},
            )

        local_direct, local_direct_path = self._load_local_by_video_id(video_id)
        if local_direct is not None and local_direct_path is not None:
            cache_path.write_text(local_direct, encoding="utf-8")
            return (
                local_direct,
                "local_video_id",
                {"matched_path": str(local_direct_path), "match_score": 1.0},
            )

        if title:
            local_title, local_title_path, local_title_score = self._load_local_by_title(title)
            if local_title is not None and local_title_path is not None:
                cache_path.write_text(local_title, encoding="utf-8")
                return (
                    local_title,
                    "local_title_match",
                    {
                        "matched_path": str(local_title_path),
                        "match_score": float(local_title_score),
                    },
                )

        if self.client is not None:
            for ext in TRANSCRIPT_EXTENSIONS:
                dropbox_path = f"{self.config.dropbox_transcripts_path}/{video_id}{ext}"
                try:
                    _, response = self.client.files_download(path=dropbox_path)
                    srt_content = response.content.decode("utf-8", errors="ignore")
                    cache_path.write_text(srt_content, encoding="utf-8")
                    return (
                        srt_content,
                        "dropbox_video_id",
                        {"matched_path": dropbox_path, "match_score": 1.0},
                    )
                except self._dropbox_api_error as exc:
                    if not getattr(exc.error, "is_path", lambda: False)():
                        raise

            if title:
                matched_path, matched_score = self._find_transcript_by_title(title)
                if matched_path:
                    _, response = self.client.files_download(path=matched_path)
                    srt_content = response.content.decode("utf-8", errors="ignore")
                    cache_path.write_text(srt_content, encoding="utf-8")
                    return (
                        srt_content,
                        "title_match",
                        {"matched_path": matched_path, "match_score": float(matched_score)},
                    )

        return None, "not_found", {"matched_path": "", "match_score": 0.0}

    def _load_manual_overrides(self) -> tuple[dict[str, Path], dict[str, Path]]:
        by_video_id: dict[str, Path] = {}
        by_title: dict[str, Path] = {}
        path = self.config.manual_overrides_file
        if not path.exists():
            return by_video_id, by_title

        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_srt = str(row.get("srt_path", "")).strip()
                if not raw_srt:
                    continue
                resolved = self._resolve_override_path(raw_srt)
                if resolved is None:
                    continue

                video_id = str(row.get("video_id", "")).strip()
                if video_id:
                    by_video_id[video_id] = resolved

                for key in ("match_title", "title"):
                    raw_title = str(row.get(key, "")).strip()
                    if raw_title:
                        by_title[_normalize_title(raw_title)] = resolved

        return by_video_id, by_title

    def _resolve_override_path(self, raw_path: str) -> Path | None:
        candidate = Path(raw_path).expanduser()
        options = [candidate]

        if not candidate.is_absolute():
            options.append(Path.cwd() / candidate)
            if self.config.local_transcripts_dir is not None:
                options.append(self.config.local_transcripts_dir / candidate)

        for option in options:
            if option.exists() and option.is_file():
                return option
        return None

    def _resolve_manual_override(self, video_id: str, title: str | None) -> Path | None:
        by_id = self._manual_by_video_id.get(video_id)
        if by_id is not None:
            return by_id
        if title:
            return self._manual_by_title.get(_normalize_title(title))
        return None

    def _load_local_by_video_id(self, video_id: str) -> tuple[str | None, Path | None]:
        local_dir = self.config.local_transcripts_dir
        if local_dir is None:
            return None, None
        for ext in TRANSCRIPT_EXTENSIONS:
            direct = local_dir / f"{video_id}{ext}"
            if direct.exists():
                return direct.read_text(encoding="utf-8", errors="ignore"), direct
            for path in local_dir.rglob(f"{video_id}{ext}"):
                if path.is_file():
                    return path.read_text(encoding="utf-8", errors="ignore"), path
        return None, None

    def _load_local_by_title(self, title: str) -> tuple[str | None, Path | None, float]:
        local_dir = self.config.local_transcripts_dir
        if local_dir is None:
            return None, None, 0.0

        normalized_title = _normalize_title(title)
        if not normalized_title:
            return None, None, 0.0

        entries = self._get_local_index()
        if not entries:
            return None, None, 0.0

        best_entry = None
        best_score = 0.0
        best_valid_entry = None
        best_valid_score = 0.0
        for entry in entries:
            stem = entry["stem_norm"]
            rel = entry["rel_norm"]
            stem_tail = entry["stem_tail_norm"]
            score = max(
                difflib.SequenceMatcher(None, normalized_title, stem).ratio(),
                difflib.SequenceMatcher(None, normalized_title, rel).ratio(),
                difflib.SequenceMatcher(None, normalized_title, stem_tail).ratio(),
                _token_overlap_score(normalized_title, stem),
                _token_overlap_score(normalized_title, rel),
                _token_overlap_score(normalized_title, stem_tail),
            )
            if score > best_score:
                best_score = score
                best_entry = entry

            is_valid = (
                score >= MATCH_THRESHOLD
                or (score >= LOWER_MATCH_THRESHOLD and _has_keyword_overlap(normalized_title, stem))
                or (score >= LOWER_MATCH_THRESHOLD and _has_keyword_overlap(normalized_title, rel))
                or (score >= LOWER_MATCH_THRESHOLD and _has_keyword_overlap(normalized_title, stem_tail))
            )
            if is_valid and score > best_valid_score:
                best_valid_score = score
                best_valid_entry = entry

        if best_valid_entry is not None:
            path = best_valid_entry["path"]
            return (
                path.read_text(encoding="utf-8", errors="ignore"),
                path,
                float(best_valid_score),
            )

        return None, None, float(best_score)

    def _get_local_index(self) -> list[dict[str, str | Path]]:
        if self._local_index is not None:
            return self._local_index

        index: list[dict[str, str | Path]] = []
        local_dir = self.config.local_transcripts_dir
        if local_dir is None:
            self._local_index = index
            return index

        for ext in TRANSCRIPT_EXTENSIONS:
            for path in local_dir.rglob(f"*{ext}"):
                if not path.is_file():
                    continue
                rel_path = str(path.relative_to(local_dir))
                parent_norm = _normalize_title(path.parent.name)
                stem_norm = _normalize_title(path.stem)
                index.append(
                    {
                        "path": path,
                        "stem_norm": stem_norm,
                        "stem_tail_norm": _strip_folder_prefix_from_stem(stem_norm, parent_norm),
                        "rel_norm": _normalize_title(rel_path),
                    }
                )

        self._local_index = index
        return index

    def _find_transcript_by_title(self, title: str) -> tuple[str | None, float]:
        index = self._get_transcript_index()
        normalized_title = _normalize_title(title)
        if not normalized_title:
            return None, 0.0

        exact = index.get(normalized_title)
        if exact:
            return exact, 1.0

        candidates = list(index.keys())
        if not candidates:
            return None, 0.0

        best = max(candidates, key=lambda candidate: _match_score(normalized_title, candidate))
        score = _match_score(normalized_title, best)
        if score >= MATCH_THRESHOLD or (
            score >= LOWER_MATCH_THRESHOLD and _has_keyword_overlap(normalized_title, best)
        ):
            return index[best], float(score)

        return None, float(score)

    def _get_transcript_index(self) -> dict[str, str]:
        if self._transcript_index is not None:
            return self._transcript_index

        index: dict[str, str] = {}
        if self.client is None:
            self._transcript_index = index
            return index

        result = self.client.files_list_folder(self.config.dropbox_transcripts_path)

        while True:
            for entry in result.entries:
                if isinstance(entry, self._dropbox_file_metadata) and any(
                    entry.name.lower().endswith(ext) for ext in TRANSCRIPT_EXTENSIONS
                ):
                    stem = Path(entry.name).stem
                    normalized = _normalize_title(stem)
                    if normalized and normalized not in index:
                        index[normalized] = entry.path_lower or entry.path_display
            if not result.has_more:
                break
            result = self.client.files_list_folder_continue(result.cursor)

        self._transcript_index = index
        return index

    def _cache_path(self, video_id: str) -> Path:
        safe_video_id = video_id.replace("/", "_")
        return self.config.cache_transcripts_dir / f"{safe_video_id}.srt"


def _normalize_title(value: str) -> str:
    normalized = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    normalized = re.sub(r"([A-Za-z])(\d)", r"\1 \2", normalized)
    normalized = re.sub(r"(\d)([A-Za-z])", r"\1 \2", normalized)
    normalized = normalized.lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    tokens = []
    for token in normalized.split():
        if token in IGNORE_FILENAME_TOKENS:
            continue
        # Ignore chapter markers like CH, CH1, CH12, etc.
        if token == "ch" or re.fullmatch(r"ch\d+", token):
            continue
        tokens.append(token)
    return " ".join(tokens)


def _token_overlap_score(a: str, b: str) -> float:
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return 0.0
    overlap = len(a_tokens & b_tokens)
    return overlap / max(len(a_tokens), 1)


def _match_score(a: str, b: str) -> float:
    return max(
        difflib.SequenceMatcher(None, a, b).ratio(),
        _token_overlap_score(a, b),
    )


def _strip_folder_prefix_from_stem(stem_norm: str, folder_norm: str) -> str:
    if not stem_norm:
        return stem_norm
    if not folder_norm:
        return stem_norm

    stem_tokens = stem_norm.split()
    folder_tokens = folder_norm.split()
    i = 0
    while i < len(stem_tokens) and i < len(folder_tokens) and stem_tokens[i] == folder_tokens[i]:
        i += 1

    if i == 0:
        return stem_norm

    tail_tokens = stem_tokens[i:]
    if not tail_tokens:
        return stem_norm

    return " ".join(tail_tokens)


def _has_keyword_overlap(a: str, b: str) -> bool:
    a_tokens = {
        t for t in a.split()
        if len(t) >= 4 and t not in STOPWORDS
    }
    b_tokens = {
        t for t in b.split()
        if len(t) >= 4 and t not in STOPWORDS
    }
    return len(a_tokens & b_tokens) >= 1
