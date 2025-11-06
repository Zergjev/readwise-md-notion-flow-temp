"""
Shared path helpers for Voice2Notion.

Creates a per-run transcripts folder named:
    outputs/NNN-transcripts

In GitHub Actions, uses GITHUB_RUN_NUMBER for NNN (001, 002, ...).
Locally, scans existing numbered folders and picks the next one.
"""

import os
from pathlib import Path


def ensure_transcripts_dir() -> str:
    """Create and return a run-specific transcripts folder under outputs/."""
    root = (
        os.getenv("TRANSCRIPTS_OUTPUT_DIR")
        or os.getenv("TRANSCRIPT_OUTPUT_DIR")
        or "outputs/transcripts"
    )

    base = Path(root)
    # If the env path ends with "transcripts", use its parent ("outputs")
    if base.name == "transcripts":
        base = base.parent

    base.mkdir(parents=True, exist_ok=True)

    run_no = os.getenv("GITHUB_RUN_NUMBER")
    if run_no and run_no.isdigit():
        folder = base / f"{int(run_no):03d}-transcripts"
        folder.mkdir(parents=True, exist_ok=True)
        print(f"[transcripts] Using folder: {folder}")
        return str(folder)

    # Local fallback
    existing = [
        int(p.name[:3])
        for p in base.iterdir()
        if p.is_dir() and p.name.endswith("-transcripts") and p.name[:3].isdigit()
    ]
    next_idx = (max(existing) + 1) if existing else 1
    folder = base / f"{next_idx:03d}-transcripts"
    folder.mkdir(parents=True, exist_ok=True)
    print(f"[transcripts] Using folder: {folder}")
    return str(folder)
