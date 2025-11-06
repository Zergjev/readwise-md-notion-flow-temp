from __future__ import annotations
from typing import Iterable, Dict, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class DriveClient:
    def __init__(self, service_account_info: dict):
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def list_audio_files(
        self,
        folder_id: str,
        mime_prefix: str = "audio/",
        since_iso: Optional[str] = None,
        page_size: int = 100,
    ) -> Iterable[Dict]:
        """
        Yield Drive File dicts with fields: id, name, mimeType, modifiedTime.
        Filters to files under `folder_id` with MIME type matching `mime_prefix`.
        If since_iso is provided, only returns files modified after that timestamp.
        """
        if not folder_id:
            raise ValueError("folder_id is required")

        # Build query
        clauses = [
            f"'{folder_id}' in parents",
            "trashed = false",
        ]

        # MIME filter: if a prefix like "audio/", use contains; else exact.
        if "/" not in mime_prefix or mime_prefix.endswith("/"):
            top = mime_prefix.split("/")[0]
            clauses.append(f"mimeType contains '{top}/'")
        else:
            clauses.append(f"mimeType = '{mime_prefix}'")

        if since_iso:
            clauses.append(f"modifiedTime > '{since_iso}'")

        q = " and ".join(clauses)
        fields = "nextPageToken, files(id, name, mimeType, modifiedTime)"
        page_token = None

        try:
            while True:
                resp = (
                    self.service.files()
                    .list(
                        q=q,
                        spaces="drive",
                        fields=fields,
                        orderBy="modifiedTime desc",
                        pageSize=min(page_size, 100),
                        pageToken=page_token,
                    )
                    .execute()
                )
                for f in resp.get("files", []):
                    yield f
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
        except HttpError as e:
            raise RuntimeError(f"Drive API error: {e}") from e

