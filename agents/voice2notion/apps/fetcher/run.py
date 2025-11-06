"""
Fetcher (Step 2)

Downloads audio files from Google Drive based on Watcher payloads.

Inputs (choose one):
  --in /path/to/payloads.json
      JSON array of payload objects
  --payload '{"file_id":"...","file_name":"..."}'
      Single payload object
  (CI) env MATRIX_FILE='{"file_id":"...","file_name":"..."}'
      One-at-a-time payload via environment

Env vars:
  GOOGLE_SERVICE_JSON   Inline JSON, base64 JSON, or path to JSON file
  AUDIO_OUTPUT_DIR      Directory for downloads (default: downloads/audio)

Payload schema (emitted by Watcher):
  {
    "file_id": "1xxXYZa...",
    "file_name": "clip.m4a",
    "mime_type": "audio/mp4",
    "modified_time": "2025-11-03T12:34:56.000Z"
  }
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

OUTPUT_DIR = os.getenv("AUDIO_OUTPUT_DIR", "downloads/audio")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_service_account_info(value: str) -> dict[str, Any]:
    """Parse service-account credentials from inline/base64/path JSON."""
    if not value:
        raise EnvironmentError(
            "Missing GOOGLE_SERVICE_JSON environment variable."
        )
    v = value.strip()

    # Inline JSON?
    if v.startswith("{"):
        return json.loads(v)

    # Path to file on disk?
    if os.path.exists(v):
        with open(v, "r", encoding="utf-8") as f:
            return json.load(f)

    # Base64?
    try:
        decoded = base64.b64decode(v)
        return json.loads(decoded)
    except Exception as exc:  # noqa: BLE001 (broad for robust UX)
        raise ValueError(
            "GOOGLE_SERVICE_JSON must be inline JSON, base64 JSON, or a path "
            "to a JSON file."
        ) from exc


def get_drive_service() -> Any:
    """Build and return a Google Drive v3 service (readonly scope)."""
    creds_data = load_service_account_info(os.getenv("GOOGLE_SERVICE_JSON", ""))
    creds = service_account.Credentials.from_service_account_info(
        creds_data,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def download_audio(service: Any, file_id: str, file_name: str) -> str:
    """Download an audio file by ID to OUTPUT_DIR and return the local path."""
    # Basic filename safety
    safe_name = file_name.replace("/", "_").replace("\\", "_")
    local_path = os.path.join(OUTPUT_DIR, safe_name)

    request = service.files().get_media(fileId=file_id)
    with io.FileIO(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                print(f"⬇️  Download {pct}%: {safe_name}")

    print(f"✅ Saved: {local_path}")
    return local_path


def load_payloads(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Load payloads from --payload, --in file, or MATRIX_FILE env."""
    # 1) CLI single object
    if args.payload:
        obj = json.loads(args.payload)
        if isinstance(obj, dict):
            return [obj]
        raise SystemExit("--payload must be a single JSON object")

    # 2) CLI file containing an array
    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        raise SystemExit("--in must be a JSON array of payload objects")

    # 3) CI matrix case: env MATRIX_FILE with a single object
    matrix_env = os.getenv("MATRIX_FILE")
    if matrix_env:
        obj = json.loads(matrix_env)
        if isinstance(obj, dict):
            return [obj]
        raise SystemExit("MATRIX_FILE env must contain a single JSON object")

    raise SystemExit(
        "Provide either --in <file>, --payload <json>, or MATRIX_FILE env"
    )


def main() -> None:
    """CLI entrypoint for the Fetcher app."""
    parser = argparse.ArgumentParser(description="Voice2Notion Fetcher")
    parser.add_argument(
        "--in",
        dest="input",
        help="Path to JSON array of payloads.",
    )
    parser.add_argument(
        "--payload",
        help="Single payload as JSON string.",
    )
    args = parser.parse_args()

    service = get_drive_service()
    payloads = load_payloads(args)

    print(f"🧾 Processing {len(payloads)} file(s)…")
    for p in payloads:
        file_id = p["file_id"]
        file_name = p.get("file_name", f"{file_id}.bin")
        download_audio(service, file_id, file_name)


if __name__ == "__main__":
    main()
