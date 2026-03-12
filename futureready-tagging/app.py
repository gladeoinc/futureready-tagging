from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st


DATA_PATH = Path("futureready_videos.json")


def _load_data(path: Path) -> list[dict]:
    if not path.exists():
        st.error(f"Dataset not found: {path}")
        st.stop()
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        st.error("Expected futureready_videos.json to contain a JSON array.")
        st.stop()
    return data


def _options(data: list[dict], key: str) -> list[str]:
    values: set[str] = set()
    for row in data:
        value = row.get(key)
        if isinstance(value, list):
            values.update(v for v in value if isinstance(v, str) and v.strip())
        elif isinstance(value, str) and value.strip():
            values.add(value)
    return sorted(values)


def _matches(row: dict, selected: dict, query: str) -> bool:
    if selected["skills"] and not set(selected["skills"]).issubset(set(row.get("skills", []))):
        return False
    if selected["topics"] and not set(selected["topics"]).issubset(set(row.get("topics", []))):
        return False
    if selected["sectors"] and not set(selected["sectors"]).issubset(set(row.get("career_sectors", []))):
        return False
    if selected["competencies"]:
        competency = row.get("competency")
        if competency not in selected["competencies"]:
            return False
    if query:
        haystack = " ".join(
            [
                str(row.get("title", "")),
                str(row.get("transcript", "")),
                " ".join(row.get("skills", [])),
                " ".join(row.get("topics", [])),
                " ".join(row.get("career_sectors", [])),
                str(row.get("competency", "")),
            ]
        ).lower()
        if query.lower() not in haystack:
            return False
    return True


def main() -> None:
    st.set_page_config(page_title="FutureReady Video Explorer", layout="wide")
    st.title("FutureReady Video Explorer")
    st.caption("Internal viewer for topics, skills, and competencies in curriculum videos.")

    data = _load_data(DATA_PATH)
    skills = _options(data, "skills")
    topics = _options(data, "topics")
    sectors = _options(data, "career_sectors")
    competencies = _options(data, "competency")

    with st.sidebar:
        st.header("Filters")
        selected_skills = st.multiselect("Skills", skills)
        selected_topics = st.multiselect("Topics", topics)
        selected_sectors = st.multiselect("Career Sectors", sectors)
        selected_competencies = st.multiselect("Competency", competencies)
        query = st.text_input("Search (title/transcript)")

    selected = {
        "skills": selected_skills,
        "topics": selected_topics,
        "sectors": selected_sectors,
        "competencies": selected_competencies,
    }

    filtered = [row for row in data if _matches(row, selected, query)]
    st.subheader(f"Results: {len(filtered)} of {len(data)} videos")

    table_rows = []
    for row in filtered:
        table_rows.append(
            {
                "video_id": row.get("video_id", ""),
                "title": row.get("title", ""),
                "competency": row.get("competency", ""),
                "skills": ", ".join(row.get("skills", [])),
                "topics": ", ".join(row.get("topics", [])),
                "career_sectors": ", ".join(row.get("career_sectors", [])),
            }
        )
    df = pd.DataFrame(table_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    if filtered:
        st.markdown("---")
        st.subheader("Video Detail")
        labels = [f"{r.get('video_id', '')} - {r.get('title', '')}" for r in filtered]
        idx = st.selectbox("Select a video", range(len(labels)), format_func=lambda i: labels[i])
        item = filtered[idx]
        st.markdown(f"**Title:** {item.get('title', '')}")
        st.markdown(f"**Video ID:** {item.get('video_id', '')}")
        st.markdown(f"**Competency:** {item.get('competency', '')}")
        st.markdown(f"**Skills:** {', '.join(item.get('skills', []))}")
        st.markdown(f"**Topics:** {', '.join(item.get('topics', []))}")
        st.markdown(f"**Career Sectors:** {', '.join(item.get('career_sectors', []))}")
        with st.expander("Transcript", expanded=False):
            st.write(item.get("transcript", ""))


if __name__ == "__main__":
    main()
