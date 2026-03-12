from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class AppConfig:
    openai_api_key: str
    dropbox_access_token: str | None
    local_transcripts_dir: Path | None
    disable_dropbox: bool
    google_sheets_id: str | None
    google_application_credentials: str | None
    dropbox_transcripts_path: str
    openai_model: str
    output_json: Path
    manual_overrides_file: Path
    metadata_file: Path | None
    openai_max_retries: int
    openai_retry_base_delay: float
    cache_transcripts_dir: Path
    cache_tags_dir: Path
    logs_dir: Path


class ConfigError(ValueError):
    pass


def load_config() -> AppConfig:
    load_dotenv()

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    dropbox_access_token = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip() or None
    local_transcripts_dir_raw = os.getenv("LOCAL_TRANSCRIPTS_DIR", "").strip()
    local_transcripts_dir = (
        Path(local_transcripts_dir_raw).expanduser() if local_transcripts_dir_raw else None
    )
    disable_dropbox = os.getenv("DISABLE_DROPBOX", "").strip().lower() in {"1", "true", "yes"}
    google_sheets_id = os.getenv("GOOGLE_SHEETS_ID", "").strip() or None
    google_application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip() or None

    dropbox_transcripts_path = os.getenv("DROPBOX_TRANSCRIPTS_PATH", "/FutureReady_Transcripts").strip()
    if not dropbox_transcripts_path.startswith("/"):
        dropbox_transcripts_path = "/" + dropbox_transcripts_path

    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

    output_json = Path(os.getenv("OUTPUT_JSON", "futureready_videos.json")).expanduser()
    manual_overrides_file = Path(
        os.getenv("MANUAL_OVERRIDES_FILE", "manual_overrides.csv")
    ).expanduser()

    metadata_file_raw = os.getenv("METADATA_FILE", "").strip()
    metadata_file = Path(metadata_file_raw).expanduser() if metadata_file_raw else None

    try:
        openai_max_retries = int(os.getenv("OPENAI_MAX_RETRIES", "4"))
    except ValueError as exc:
        raise ConfigError("OPENAI_MAX_RETRIES must be an integer") from exc

    try:
        openai_retry_base_delay = float(os.getenv("OPENAI_RETRY_BASE_DELAY", "1.5"))
    except ValueError as exc:
        raise ConfigError("OPENAI_RETRY_BASE_DELAY must be a number") from exc

    if openai_max_retries < 1:
        raise ConfigError("OPENAI_MAX_RETRIES must be >= 1")
    if openai_retry_base_delay <= 0:
        raise ConfigError("OPENAI_RETRY_BASE_DELAY must be > 0")

    if not openai_api_key:
        raise ConfigError("Missing required environment variable: OPENAI_API_KEY")
    if not local_transcripts_dir and not dropbox_access_token:
        raise ConfigError(
            "Set either LOCAL_TRANSCRIPTS_DIR for local transcript mode or DROPBOX_ACCESS_TOKEN for Dropbox mode"
        )

    if local_transcripts_dir and not local_transcripts_dir.exists():
        raise ConfigError(f"LOCAL_TRANSCRIPTS_DIR does not exist: {local_transcripts_dir}")
    if local_transcripts_dir and not local_transcripts_dir.is_dir():
        raise ConfigError(f"LOCAL_TRANSCRIPTS_DIR is not a directory: {local_transcripts_dir}")

    if metadata_file is None:
        if not google_sheets_id:
            raise ConfigError(
                "GOOGLE_SHEETS_ID is required when METADATA_FILE is not provided"
            )
        if not google_application_credentials:
            raise ConfigError(
                "GOOGLE_APPLICATION_CREDENTIALS is required for Google Sheets mode"
            )

    cache_transcripts_dir = Path("cache") / "transcripts"
    cache_tags_dir = Path("cache") / "tags"
    logs_dir = Path("logs")

    for directory in (cache_transcripts_dir, cache_tags_dir, logs_dir):
        directory.mkdir(parents=True, exist_ok=True)

    if output_json.parent != Path("."):
        output_json.parent.mkdir(parents=True, exist_ok=True)

    return AppConfig(
        openai_api_key=openai_api_key,
        dropbox_access_token=dropbox_access_token,
        local_transcripts_dir=local_transcripts_dir,
        disable_dropbox=disable_dropbox,
        google_sheets_id=google_sheets_id,
        google_application_credentials=google_application_credentials,
        dropbox_transcripts_path=dropbox_transcripts_path.rstrip("/"),
        openai_model=openai_model,
        output_json=output_json,
        manual_overrides_file=manual_overrides_file,
        metadata_file=metadata_file,
        openai_max_retries=openai_max_retries,
        openai_retry_base_delay=openai_retry_base_delay,
        cache_transcripts_dir=cache_transcripts_dir,
        cache_tags_dir=cache_tags_dir,
        logs_dir=logs_dir,
    )
