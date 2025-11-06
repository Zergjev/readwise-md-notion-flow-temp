# shared/io_models.py
from __future__ import annotations

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator


class DriveFilePayload(BaseModel):
    """
    Canonical descriptor for a Google Drive file.
    Kept exactly as provided to avoid breaking upstream code.
    """
    file_id: str
    file_name: str
    mime_type: str
    modified_time: str  # RFC3339 timestamp from Drive


class SummaryPayload(BaseModel):
    """
    Output of the summarizer step (and inputs to the resolver).
    - `main_points` should be bullet-like lines (we lightly normalize).
    - `keywords` are the 5 generated single-word keywords from your upstream step.
      (Tagging into Notion is handled later, not here.)
    """
    file: DriveFilePayload
    transcript: Optional[str] = Field(
        default=None,
        description="Full transcript text (any language)."
    )
    main_points: List[str] = Field(
        default_factory=list,
        description="Bullet points summarizing the transcript."
    )
    title_guess: Optional[str] = Field(
        default=None,
        description="Optional title guess derived from the transcript."
    )
    keywords: Optional[List[str]] = Field(
        default=None,
        description="Five generated keywords for later tagging/searching."
    )

    @validator("main_points", pre=True)
    def _normalize_bullets(cls, v: Optional[List[str]]) -> List[str]:
        """
        Strip leading bullet characters and whitespace, drop empties.
        """
        if not v:
            return []
        out: List[str] = []
        for item in v:
            if not isinstance(item, str):
                continue
            cleaned = item.lstrip("•*- ").strip()
            if cleaned:
                out.append(cleaned)
        return out

    @validator("keywords", pre=True)
    def _normalize_keywords(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Normalize keywords by trimming whitespace and dropping empties.
        Keep order, no dedup to avoid altering upstream scoring/intent.
        We don't hard-enforce count==5 to stay non-destructive;
        downstream can validate/alert if fewer/more are present.
        """
        if v is None:
            return None
        out: List[str] = []
        for kw in v:
            if not isinstance(kw, str):
                continue
            cleaned = kw.strip()
            if cleaned:
                out.append(cleaned)
        return out or None


class ResolvePayload(SummaryPayload):
    """
    Output of the resolver step: same as SummaryPayload plus an optional Notion page link.
    """
    notion_page_id: Optional[str] = Field(
        default=None,
        description="Resolved Notion page ID for this content (or None if not found)."
    )


class WriteResult(BaseModel):
    """
    Output of the Notion writer step.
    """
    file: DriveFilePayload
    notion_page_id: str
    status: Literal["ok", "error"] = Field(
        default="ok",
        description="Write status indicator."
    )
    notion_url: Optional[str] = Field(
        default=None,
        description="Canonical Notion URL of the target page."
    )
    message: Optional[str] = Field(
        default=None,
        description="Optional human-readable message (e.g., error detail)."
    )


# Optional: keep Pydantic config conservative and whitespace-friendly.
# (Pydantic v1 style; adjust if you later migrate to v2.)
class _CommonConfig:
    anystr_strip_whitespace = True


DriveFilePayload.Config = _CommonConfig
SummaryPayload.Config = _CommonConfig
ResolvePayload.Config = _CommonConfig
WriteResult.Config = _CommonConfig
