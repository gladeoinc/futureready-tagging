from __future__ import annotations

import argparse
import logging
import sys

from ai_tagging import AITagger, TaggingError
from config import ConfigError, load_config
from data_loader import DataLoadError, load_video_metadata
from dropbox_client import DropboxTranscriptClient
from output_writer import (
    write_dataset,
    write_match_debug_report,
    write_missing_report,
    write_success_report,
)
from transcript_parser import srt_to_clean_text


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("futureready_pipeline")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler("logs/pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def run(*, strict: bool = False, local_only: bool = False) -> int:
    try:
        config = load_config()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 1

    if local_only:
        if config.local_transcripts_dir is None:
            print(
                "Configuration error: --local-only requires LOCAL_TRANSCRIPTS_DIR in .env",
                file=sys.stderr,
            )
            return 1
        config.disable_dropbox = True

    logger = setup_logging()

    try:
        metadata = load_video_metadata(config)
    except DataLoadError as exc:
        logger.error("Failed to load metadata: %s", exc)
        return 1
    except Exception as exc:  # Catch API/auth issues cleanly for MVP
        logger.exception("Unexpected metadata load failure: %s", exc)
        return 1

    logger.info("Loaded %d metadata rows", len(metadata))

    dropbox_client = DropboxTranscriptClient(config)
    ai_tagger = AITagger(config)

    records: list[dict] = []
    missing_rows: list[dict[str, str]] = []
    match_debug_rows: list[dict[str, str | float]] = []

    processed = 0
    skipped = 0
    api_called = 0
    api_cached = 0

    total = len(metadata)

    for idx, row in enumerate(metadata, start=1):
        video_id = row["video_id"]
        title = row["title"]
        match_title = str(row.get("match_title", title)).strip() or title
        activity_type = str(row.get("activity_type", "")).strip().casefold()
        allow_missing_transcript = (not strict) and _is_career_spotlight(activity_type, title)
        logger.info("[%d/%d] %s - %s", idx, total, video_id, title)

        try:
            srt_content, transcript_source, match_meta = dropbox_client.get_transcript_srt(
                video_id, match_title
            )
        except Exception as exc:
            logger.warning("Transcript lookup error for %s: %s", video_id, exc)
            missing_rows.append(
                {"video_id": video_id, "title": title, "reason": f"lookup_error: {exc}"}
            )
            match_debug_rows.append(
                {
                    "video_id": video_id,
                    "title": title,
                    "activity_type": activity_type,
                    "status": "error",
                    "match_source": "lookup_error",
                    "matched_path": "",
                    "match_score": 0.0,
                    "reason": f"lookup_error: {exc}",
                }
            )
            skipped += 1
            continue

        if not srt_content:
            if allow_missing_transcript:
                logger.info(
                    "Transcript missing for %s but continuing because activity type is Career Spotlight",
                    video_id,
                )
                transcript = ""
                missing_rows.append(
                    {
                        "video_id": video_id,
                        "title": title,
                        "reason": "allowed_missing_transcript_career_spotlight",
                    }
                )
                match_debug_rows.append(
                    {
                        "video_id": video_id,
                        "title": title,
                        "activity_type": activity_type,
                        "status": "processed_without_transcript",
                        "match_source": transcript_source,
                        "matched_path": match_meta.get("matched_path", ""),
                        "match_score": match_meta.get("match_score", 0.0),
                        "reason": "career_spotlight_allowed_missing",
                    }
                )
            else:
                logger.warning("Transcript missing for %s", video_id)
                missing_rows.append(
                    {"video_id": video_id, "title": title, "reason": "transcript_not_found"}
                )
                match_debug_rows.append(
                    {
                        "video_id": video_id,
                        "title": title,
                        "activity_type": activity_type,
                        "status": "skipped",
                        "match_source": transcript_source,
                        "matched_path": match_meta.get("matched_path", ""),
                        "match_score": match_meta.get("match_score", 0.0),
                        "reason": "transcript_not_found",
                    }
                )
                skipped += 1
                continue
        if transcript_source == "title_match":
            logger.info(
                "Transcript matched by title for %s using %s",
                video_id,
                match_meta.get("matched_path", ""),
            )
        if srt_content:
            transcript = srt_to_clean_text(srt_content)
            if not transcript and not allow_missing_transcript:
                logger.warning("Transcript empty after parsing for %s", video_id)
                missing_rows.append(
                    {"video_id": video_id, "title": title, "reason": "empty_transcript"}
                )
                match_debug_rows.append(
                    {
                        "video_id": video_id,
                        "title": title,
                        "activity_type": activity_type,
                        "status": "skipped",
                        "match_source": transcript_source,
                        "matched_path": match_meta.get("matched_path", ""),
                        "match_score": match_meta.get("match_score", 0.0),
                        "reason": "empty_transcript",
                    }
                )
                skipped += 1
                continue

        try:
            tags, was_cached = ai_tagger.generate_tags(video_id, title, transcript)
        except TaggingError as exc:
            logger.warning("Tagging failed for %s: %s", video_id, exc)
            missing_rows.append(
                {"video_id": video_id, "title": title, "reason": f"tagging_failed: {exc}"}
            )
            match_debug_rows.append(
                {
                    "video_id": video_id,
                    "title": title,
                    "activity_type": activity_type,
                    "status": "skipped",
                    "match_source": transcript_source,
                    "matched_path": match_meta.get("matched_path", ""),
                    "match_score": match_meta.get("match_score", 0.0),
                    "reason": f"tagging_failed: {exc}",
                }
            )
            skipped += 1
            continue
        except Exception as exc:
            logger.warning("Unexpected tagging error for %s: %s", video_id, exc)
            missing_rows.append(
                {"video_id": video_id, "title": title, "reason": f"tagging_error: {exc}"}
            )
            match_debug_rows.append(
                {
                    "video_id": video_id,
                    "title": title,
                    "activity_type": activity_type,
                    "status": "skipped",
                    "match_source": transcript_source,
                    "matched_path": match_meta.get("matched_path", ""),
                    "match_score": match_meta.get("match_score", 0.0),
                    "reason": f"tagging_error: {exc}",
                }
            )
            skipped += 1
            continue

        if was_cached:
            api_cached += 1
        else:
            api_called += 1

        records.append(
            {
                "video_id": video_id,
                "title": title,
                "transcript": transcript,
                "career_sectors": tags.get("career_sectors", []),
                "skills": tags.get("skills", []),
                "topics": tags.get("topics", []),
                "competency": tags.get("competency"),
            }
        )
        match_debug_rows.append(
            {
                "video_id": video_id,
                "title": title,
                "activity_type": activity_type,
                "status": "processed",
                "match_source": transcript_source,
                "matched_path": match_meta.get("matched_path", ""),
                "match_score": match_meta.get("match_score", 0.0),
                "reason": "",
            }
        )
        processed += 1

    write_dataset(records, config.output_json)
    write_missing_report(missing_rows)
    write_success_report(records)
    write_match_debug_report(match_debug_rows)

    logger.info("Wrote dataset: %s", config.output_json)
    logger.info("Wrote success report: logs/processed_videos.csv")
    logger.info("Wrote match debug report: logs/match_debug.csv")
    logger.info(
        "Summary | processed=%d skipped=%d api_called=%d api_cached=%d",
        processed,
        skipped,
        api_called,
        api_cached,
    )
    return 0


def _is_career_spotlight(activity_type: str, title: str) -> bool:
    normalized_activity = " ".join(activity_type.split())
    if "career" in normalized_activity and "spotlight" in normalized_activity:
        return True

    # Fallback heuristic for spotlight interview-style titles.
    normalized_title = title.strip().casefold()
    if normalized_title.startswith("meet "):
        return True

    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FutureReady video tagging pipeline")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Require transcripts for all rows (including Career Spotlight rows).",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Disable Dropbox lookups and use LOCAL_TRANSCRIPTS_DIR only.",
    )
    args = parser.parse_args()
    raise SystemExit(run(strict=args.strict, local_only=args.local_only))
