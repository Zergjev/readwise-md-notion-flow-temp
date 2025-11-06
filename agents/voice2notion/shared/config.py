from __future__ import annotations
import os
import json
import base64
from pydantic import BaseModel, Field


class DriveWatcherConfig(BaseModel):
    # Raw value from env: inline JSON, base64 JSON, or path to JSON
    google_service_json: str = Field(default_factory=lambda: os.getenv("GOOGLE_SERVICE_JSON", ""))
    # Folder to watch
    drive_folder_id: str = Field(default_factory=lambda: os.getenv("DRIVE_FOLDER_ID", ""))
    # MIME filter (prefix or exact)
    mime_prefix: str = Field(default="audio/")
    # Optional time filter (RFC3339)
    since_iso: str | None = Field(default=None)
    # Max number of files to return
    limit: int = Field(default=50)

    @classmethod
    def from_env(cls) -> "DriveWatcherConfig":
        return cls()

    def service_account_info(self) -> dict:
        """
        Parse GOOGLE_SERVICE_JSON which may be:
          - inline JSON (starts with '{')
          - a filesystem path to a JSON file
          - a base64-encoded JSON string
        """
        raw = self.google_service_json.strip()
        if not raw:
            raise RuntimeError("GOOGLE_SERVICE_JSON is required")

        # Inline JSON
        if raw.startswith("{"):
            return json.loads(raw)

        # File path
        if os.path.exists(raw):
            with open(raw, "r", encoding="utf-8") as f:
                return json.load(f)

        # Base64
        try:
            decoded = base64.b64decode(raw)
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(
                "GOOGLE_SERVICE_JSON must be inline JSON, base64 JSON, or a path to a JSON file."
            ) from e

