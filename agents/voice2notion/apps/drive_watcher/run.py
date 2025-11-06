"""
Voice2Notion — Drive Watcher (Step 1)
Lists new audio files in a Google Drive folder and emits a JSON array.

Env vars (required):
  GOOGLE_SERVICE_JSON   # inline JSON, base64 JSON, or path to service account JSON
  DRIVE_FOLDER_ID       # Google Drive folder ID to watch

Usage (examples):
  python -m agents.voice2notion.apps.drive_watcher.run --out /tmp/new_files.json --limit 5
  python -m agents.voice2notion.apps.drive_watcher.run --out /tmp/new_files.json --since 2025-01-01T00:00:00Z
  python -m agents.voice2notion.apps.drive_watcher.run --out /tmp/new_files.json --mime-prefix audio/
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import List

from agents.voice2notion.shared.config import DriveWatcherConfig
from agents.voice2notion.shared.google_drive import DriveClient
from agents.voice2notion.shared.io_models import DriveFilePayload
from agents.voice2notion.shared.logging_utils import get_logger

log = get_logger("drive_watcher")


def discover_files(cfg: DriveWatcherConfig) -> List[DriveFilePayload]:
    client = DriveClient(cfg.service_account_info())
    results: List[DriveFilePayload] = []

    for f in client.list_audio_files(
        folder_id=cfg.drive_folder_id,
        mime_prefix=cfg.mime_prefix,
        since_iso=cfg.since_iso,
        page_size=100,
    ):
        results.append(
            DriveFilePayload(
                file_id=f["id"],
                file_name=f["name"],
                mime_type=f["mimeType"],
                modified_time=f["modifiedTime"],
            )
        )
        if len(results) >= cfg.limit:
            break

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Voice2Notion Drive Watcher")
    parser.add_argument("--out", required=True, help="Path to write JSON array of file payloads")
    parser.add_argument("--since", dest="since_iso", default=None, help="RFC3339 timestamp filter")
    parser.add_argument("--limit", type=int, default=50, help="Max number of files to emit")
    parser.add_argument("--mime-prefix", default="audio/", help="MIME prefix to match (default: audio/)")
    args = parser.parse_args()

    cfg = DriveWatcherConfig.from_env()
    cfg.since_iso = args.since_iso
    cfg.limit = args.limit
    cfg.mime_prefix = args.mime_prefix

    if not cfg.drive_folder_id:
        raise SystemExit("DRIVE_FOLDER_ID env var is required")
    # validate service account early
    _ = cfg.service_account_info()

    files = discover_files(cfg)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump([p.model_dump() for p in files], fh, ensure_ascii=False, indent=2)

    log.info("Emitted %d file(s) to %s", len(files), out_path)


if __name__ == "__main__":
    main()
