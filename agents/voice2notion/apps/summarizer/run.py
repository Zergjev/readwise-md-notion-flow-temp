"""
Voice2Notion — Summarizer (Step 4)

Reads transcript JSON files produced by the Transcriber, summarizes the main
points, and saves bullet-point summaries and JSON metadata in the same folder.

For each `<stem>.json` file, produces:
  • <stem>-summ.txt        (plain-text summary)
  • <stem>-summary.json    (structured output)
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

from agents.voice2notion.shared.logging_utils import get_logger
from agents.voice2notion.shared.openai_utils import chat_completion

log = get_logger("summarizer")


def _iter_json_targets(root: Path) -> Iterable[Path]:
    """
    Yield transcript JSON files from either:
    - a numbered transcripts folder: outputs/NNN-transcripts/*.json
    - the outputs root, recursively: outputs/*-transcripts/*.json
    """
    if root.is_dir() and root.name.endswith("-transcripts"):
        yield from sorted(root.glob("*.json"))
        return

    # Common case: called with "outputs" → search numbered subfolders
    yield from sorted(root.glob("*-transcripts/*.json"))


def summarize_text(text: str, language: str = "en") -> Dict[str, Any]:
    """
    Generate a bullet-point summary and a title guess for the transcript text.

    Args:
        text: Full transcript text.
        language: Original transcript language code.

    Returns:
        Dictionary with "main_points" (list[str]) and "title_guess" (str).
    """
    prompt = (
        "You are an expert summarizer. Read the following transcript and return:\n"
        "1) A concise bullet-point summary (3–6 key ideas, start each with •).\n"
        "2) A short possible title for the talk or excerpt.\n\n"
        f"Transcript ({language}):\n{text.strip()[:8000]}"
    )

    messages = [
        {"role": "system", "content": "You summarize audio transcripts."},
        {"role": "user", "content": prompt},
    ]

    try:
        response = chat_completion(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.3,
        )
        content = response["choices"][0]["message"]["content"]
    except Exception as exc:
        log.error("OpenAI summarization failed: %s", exc)
        return {"main_points": [], "title_guess": "Untitled"}

    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    bullets: List[str] = [
        ln for ln in lines if ln.startswith("•") or ln.startswith("-")
    ]
    title_guess = next(
        (ln for ln in lines if not ln.startswith("•") and not ln.startswith("-")), ""
    )
    title_guess = title_guess.replace("Title:", "").strip() or "Untitled"

    return {"main_points": bullets, "title_guess": title_guess}


def process_root(in_dir: Path) -> None:
    """Summarize all transcript JSON files under the given directory."""
    json_files = list(_iter_json_targets(in_dir))
    if not json_files:
        log.info("No transcript JSON files found in %s", in_dir)
        return

    for json_path in json_files:
        # Skip if already summarized
        summ_txt = json_path.with_name(f"{json_path.stem}-summ.txt")
        out_json = json_path.with_name(f"{json_path.stem}-summary.json")
        if summ_txt.exists() and out_json.exists():
            log.info("Skipping (already summarized): %s", json_path.name)
            continue

        log.info("Summarizing %s", json_path.name)
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.error("Failed to read %s: %s", json_path, exc)
            continue

        transcript_text = (
            data.get("english_text")
            or data.get("spanish_text")
            or data.get("text")
            or ""
        )
        language = data.get("language_detected", "en")

        summary = summarize_text(transcript_text, language)

        # Save plain text summary
        summ_txt.write_text("\n".join(summary["main_points"]), encoding="utf-8")

        # Save structured JSON
        structured = {
            "file": {"name": json_path.name},
            "language": language,
            "main_points": summary["main_points"],
            "title_guess": summary["title_guess"],
        }
        out_json.write_text(
            json.dumps(structured, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        log.info("✅ Saved summary: %s, %s", summ_txt.name, out_json.name)


def main() -> None:
    """CLI entry point for the Summarizer agent."""
    parser = argparse.ArgumentParser(description="Voice2Notion Summarizer")
    parser.add_argument(
        "--in-dir",
        default=os.getenv("TRANSCRIPT_OUTPUT_DIR", "outputs"),
        help=(
            "Directory containing transcript JSON files. "
            "Can be the outputs root or a numbered transcripts folder."
        ),
    )
    args = parser.parse_args()
    in_dir = Path(args.in_dir)

    if not in_dir.exists():
        log.warning("Input directory not found: %s", in_dir)
        return

    process_root(in_dir)


if __name__ == "__main__":
    main()
