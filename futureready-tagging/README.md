# FutureReady Video Tagging MVP

A lightweight Python pipeline that converts FutureReady video metadata + Dropbox transcripts into a structured JSON dataset for AI-powered learning tools.

## What It Does

1. Loads video metadata from a local Excel/CSV file or Google Sheets.
2. Matches each `video_id` to a Dropbox transcript file (`.srt`).
3. Cleans captions into plain transcript text.
4. Calls OpenAI to generate curriculum tags.
5. Writes `futureready_videos.json` and a missing transcript report.

## Project Structure

```text
futureready-tagging/
  main.py
  config.py
  transcript_parser.py
  dropbox_client.py
  ai_tagging.py
  data_loader.py
  output_writer.py
  requirements.txt
  README.md
  cache/
    transcripts/
    tags/
  logs/
```

## Requirements

- Python 3.10+
- Dropbox API token with file read access
- OpenAI API key
- (Optional) Google service account for Sheets mode

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in `futureready-tagging/`:

```env
OPENAI_API_KEY=your_openai_key
DROPBOX_ACCESS_TOKEN=your_dropbox_token
GOOGLE_SHEETS_ID=your_google_sheet_id
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json

# Optional overrides
METADATA_FILE=./video_metadata.xlsx
DROPBOX_TRANSCRIPTS_PATH=/FutureReady_Transcripts
OPENAI_MODEL=gpt-4o-mini
OUTPUT_JSON=futureready_videos.json
OPENAI_MAX_RETRIES=4
OPENAI_RETRY_BASE_DELAY=1.5
```

## Metadata Input

Required columns:

- `video_id`
- `title`

Example CSV:

```csv
video_id,title
FR-01,Finding Your Drive: Passions and Interests
FR-02,Transferable Skills: Your Career Superpower
FR-03,Do You Have a Growth Mindset?
```

### Source Priority

- If `METADATA_FILE` is set, local file mode is used (`.xlsx` or `.csv`).
- If `METADATA_FILE` is not set, Google Sheets mode is used.

## Google Sheets Setup (Service Account)

1. Create a Google Cloud project.
2. Enable the Google Sheets API.
3. Create a service account and download its JSON key.
4. Set `GOOGLE_APPLICATION_CREDENTIALS` to that JSON key path.
5. Share the target sheet with the service account email.
6. Set `GOOGLE_SHEETS_ID` from the sheet URL.

## Dropbox Setup

1. Create a Dropbox app and generate an access token.
2. Ensure transcripts are in:

```text
/FutureReady_Transcripts/FR-01.srt
/FutureReady_Transcripts/FR-02.srt
...
```

3. Set `DROPBOX_ACCESS_TOKEN`.

## Run

```bash
python main.py
```

Execution flow:

1. Load spreadsheet metadata.
2. Pull transcript from cache or Dropbox.
3. Generate tags from OpenAI (or reuse cached tags if transcript unchanged).
4. Write JSON output and missing transcript log.

## Internal App (Topic/Skill Explorer)

Use the generated JSON dataset to browse videos by skills, topics, sectors, and competency.

Run:

```bash
streamlit run app.py
```

Features:
- Filter by `skills`, `topics`, `career_sectors`, and `competency`
- Search title/transcript text
- View detailed transcript and tags for each video
- Quick table view for internal review

## Output Files

- `futureready_videos.json`
- `logs/missing_transcripts.csv`
- `logs/pipeline.log`
- `cache/transcripts/*.srt`
- `cache/tags/*.json`

Example output item:

```json
{
  "video_id": "FR-01",
  "title": "Finding Your Drive: Passions and Interests",
  "transcript": "Welcome to FutureReady. In this lesson we explore passions and interests.",
  "career_sectors": ["All Careers"],
  "skills": ["Self Awareness"],
  "topics": ["Career Exploration", "Passions"],
  "competency": "Self Awareness"
}
```

## Troubleshooting

- `Configuration error`: verify required env vars are set.
- Missing transcript rows: check `logs/missing_transcripts.csv` and Dropbox path naming (`<video_id>.srt`).
- Sheets errors: verify service account file path, API enabled, and sheet sharing.
- Tagging JSON parse failures: inspect `logs/pipeline.log`; retries are automatic.
