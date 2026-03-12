from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd

from config import AppConfig


REQUIRED_COLUMNS = ("title",)
INVALID_TITLE_VALUES = {"nan", "none", "null", "n/a", "na"}


class DataLoadError(ValueError):
    pass


def load_video_metadata(config: AppConfig) -> list[dict[str, str]]:
    if config.metadata_file:
        df = _load_local_file(config.metadata_file)
    else:
        df = _load_google_sheet(config)

    return _normalize_metadata(df)


def _load_local_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise DataLoadError(f"Metadata file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        sheets = pd.read_excel(path, sheet_name=None, dtype=str, keep_default_na=False)
        if not sheets:
            raise DataLoadError(f"No worksheets found in metadata file: {path}")
        frames: list[pd.DataFrame] = []
        for sheet_name, sheet_df in sheets.items():
            if sheet_df is None or sheet_df.empty:
                continue
            copy_df = sheet_df.copy()
            copy_df.columns = [str(col).strip().lower() for col in copy_df.columns]
            if "chapter name" in copy_df.columns:
                chapter_series = copy_df["chapter name"].astype(str).str.strip()
                chapter_series = chapter_series.replace("", pd.NA).ffill().fillna("")
                copy_df["chapter name"] = chapter_series
            copy_df["_source_sheet"] = sheet_name
            frames.append(copy_df)
        if not frames:
            raise DataLoadError(f"All worksheets were empty in metadata file: {path}")
        return pd.concat(frames, ignore_index=True, sort=False)
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    raise DataLoadError("METADATA_FILE must be a .xlsx or .csv file")


def _load_google_sheet(config: AppConfig) -> pd.DataFrame:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        config.google_application_credentials,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=config.google_sheets_id, range="A:Z")
        .execute()
    )
    values = result.get("values", [])
    if not values:
        raise DataLoadError("Google Sheet returned no data")

    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)


def _normalize_metadata(df: pd.DataFrame) -> list[dict[str, str]]:
    df = _filter_and_sort_activity_type(df)

    lowered = {col.strip().lower(): col for col in df.columns}
    missing = [c for c in REQUIRED_COLUMNS if c not in lowered]
    if missing:
        raise DataLoadError(
            f"Metadata is missing required columns: {', '.join(missing)}"
        )

    selected_cols: dict[str, str] = {"title": lowered["title"]}
    if "video_id" in lowered:
        selected_cols["video_id"] = lowered["video_id"]
    if "chapter name" in lowered:
        selected_cols["chapter_name"] = lowered["chapter name"]
    if "_source_sheet" in df.columns:
        selected_cols["source_sheet"] = "_source_sheet"

    normalized = df[list(selected_cols.values())].copy()
    normalized = normalized.rename(columns={v: k for k, v in selected_cols.items()})
    if "video_id" not in normalized.columns:
        normalized["video_id"] = ""
    if "chapter_name" not in normalized.columns:
        normalized["chapter_name"] = ""
    if "source_sheet" not in normalized.columns:
        normalized["source_sheet"] = ""
    normalized["video_id"] = normalized["video_id"].astype(str).str.strip()

    normalized = normalized.reset_index(drop=True)
    normalized["title"] = normalized["title"].astype(str).str.strip()
    normalized["chapter_name"] = normalized["chapter_name"].astype(str).str.strip()
    normalized["source_sheet"] = normalized["source_sheet"].astype(str).str.strip()
    activity_col = _resolve_activity_column(df)
    if activity_col is not None:
        normalized["activity_type"] = (
            df[activity_col].reset_index(drop=True).astype(str).str.strip().str.casefold()
        )
    else:
        normalized["activity_type"] = ""

    normalized = normalized[normalized["title"] != ""]
    normalized = normalized[
        ~normalized["title"].str.casefold().isin(INVALID_TITLE_VALUES)
    ]
    normalized["match_title"] = normalized["title"]
    course2_mask = normalized["source_sheet"].str.casefold().eq("course 2 career exploration")
    with_chapter_mask = course2_mask & (normalized["chapter_name"] != "")
    normalized.loc[with_chapter_mask, "match_title"] = (
        normalized.loc[with_chapter_mask, "chapter_name"]
        + " "
        + normalized.loc[with_chapter_mask, "title"]
    ).str.strip()
    normalized["id_seed"] = (
        normalized["source_sheet"] + " | " + normalized["match_title"]
    ).str.strip()
    normalized["video_id"] = normalized.apply(_ensure_video_id, axis=1)
    normalized = normalized.drop_duplicates(subset=["video_id"], keep="first")

    return normalized.to_dict(orient="records")


def _filter_and_sort_activity_type(df: pd.DataFrame) -> pd.DataFrame:
    activity_col = _resolve_activity_column(df)
    if activity_col is None:
        return df

    allowed = {"video", "career spotlight"}

    activity_values = df[activity_col].fillna("").astype(str).str.strip()
    filtered = df[activity_values.str.casefold().isin(allowed)].copy()
    filtered["_activity_sort"] = (
        filtered[activity_col].fillna("").astype(str).str.strip().str.casefold()
    )
    filtered = filtered.sort_values(by=["_activity_sort"]).drop(columns=["_activity_sort"])
    return filtered


def _ensure_video_id(row: pd.Series) -> str:
    video_id = str(row.get("video_id", "")).strip()
    if video_id:
        return video_id

    seed = str(row.get("id_seed", "")).strip()
    if not seed:
        seed = str(row.get("match_title", "")).strip() or str(row.get("title", "")).strip()
    slug = re.sub(r"[^a-z0-9]+", "-", seed.lower()).strip("-")
    if not slug:
        slug = "untitled"
    short_hash = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8]
    return f"AUTO-{slug[:40]}-{short_hash}"


def _resolve_activity_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if str(col).strip().casefold() == "type of activity":
            return col
    if len(df.columns) >= 3:
        return df.columns[2]
    return None
