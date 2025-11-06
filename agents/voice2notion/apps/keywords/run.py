"""
Voice2Notion — Keywords (Step 4.5)

Extracts 5 single-word keywords that synthesize the main aspects emphasized by
the Summarizer. Saves a plain-text file alongside other outputs:

  • <stem>-keywrd.txt   (exactly 5 lines, one keyword per line)

Preference is given to summarized content (<stem>-summary.json); falls back to
raw transcript JSON (<stem>.json) if a summary is not present.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Iterable, List, Optional

from agents.voice2notion.shared.logging_utils import get_logger
from agents.voice2notion.shared.openai_utils import chat_completion

log = get_logger("keywords")


def _iter_candidate_pairs(root: Path) -> Iterable[tuple[Path, Optional[Path]]]:
    """
    Yield pairs (base_json, summary_json_or_none) for each transcript set found.

    We look under:
      outputs/*-transcripts/
    and for each <stem>, prefer <stem>-summary.json if present; otherwise <stem>.json.
    """
    for folder in sorted(root.glob("*-transcripts")):
        if not folder.is_dir():
            continue

        # Summary-first pass
        for s in sorted(folder.glob("*-summary.json")):
            base = s.with_name(s.stem.replace("-summary", "") + ".json")
            yield (base if base.exists() else s, s)

        # Fallback: any base json without a summary
        for b in sorted(folder.glob("*.json")):
            # skip if it is a summary json
            if b.name.endswith("-summary.json"):
                continue
            s = b.with_name(b.stem + "-summary.json")
            if not s.exists():
                yield (b, None)


def _pick_text(base_json: Path, summary_json: Optional[Path]) -> str:
    """
    Choose the best source text for keyword extraction.

    Priority:
      1) From summary JSON: title_guess + main_points
      2) From base transcript JSON: english_text / spanish_text / text
    """
    if summary_json and summary_json.exists():
        try:
            data = json.loads(summary_json.read_text(encoding="utf-8"))
            title = (data.get("title_guess") or "").strip()
            points = data.get("main_points") or []
            joined = "\n".join([title] + points if title else points)
            if joined.strip():
                return joined
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to read summary %s: %s", summary_json, exc)

    # Fallback to base transcript json
    try:
        data = json.loads(base_json.read_text(encoding="utf-8"))
        return (
            data.get("english_text")
            or data.get("spanish_text")
            or data.get("text")
            or ""
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to read base %s: %s", base_json, exc)
        return ""


def _extract_keywords(text: str) -> List[str]:
    """
    Ask the LLM for 5 single-word keywords and sanitize to enforce one token.

    Returns:
        List of exactly 5 keywords (unique, trimmed, single tokens).
    """
    prompt = (
        "From the following notes/transcript, extract exactly FIVE keywords that "
        "capture the fundamental concepts or arguments. RULES:\n"
        "- ONE word per keyword (no spaces, no punctuation, no hashtags, no numbers)\n"
        "- Return EXACTLY five lines, one keyword per line\n"
        "- Use the original language of the text if it improves clarity\n\n"
        f"TEXT:\n{text.strip()[:8000]}"
    )

    messages = [
        {"role": "system", "content": "You extract concise, one-word keywords."},
        {"role": "user", "content": prompt},
    ]

    try:
        resp = chat_completion(model="gpt-4o-mini", messages=messages, temperature=0.2)
        content = resp["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        log.error("OpenAI keyword extraction failed: %s", exc)
        return []

    # Parse lines, enforce single token, dedupe, and keep only 5.
    keywords: List[str] = []
    seen = set()
    for raw in content.splitlines():
        token = raw.strip()

        # Strip bullets/numbers/common prefixes
        token = re.sub(r"^(\d+[\).\s-]+|[-•*]\s+)", "", token)

        # Keep only word characters and unicode letters; drop punctuation
        token = re.sub(r"[^\wÀ-ÖØ-öø-ÿĀ-žŻ-ž]+", "", token, flags=re.UNICODE)

        # Enforce single token and non-empty
        if not token:
            continue

        # Normalize to lower for uniqueness, keep original casing minimal
        norm = token.lower()
        if norm in seen:
            continue
        seen.add(norm)
        keywords.append(token)

        if len(keywords) == 5:
            break

    return keywords[:5]


def _write_keywords(out_path: Path, keywords: List[str]) -> None:
    """Write keywords as 5 lines to <stem>-keywrd.txt."""
    out_path.write_text("\n".join(keywords), encoding="utf-8")


def process_root(root: Path) -> None:
    """Process all transcripts under outputs/*-transcripts and write -keywrd.txt files."""
    any_found = False
    for base_json, summary_json in _iter_candidate_pairs(root):
        stem = base_json.stem  # e.g., "clip"
        out_txt = base_json.with_name(f"{stem}-keywrd.txt")

        if out_txt.exists():
            log.info("Skipping (already has keywords): %s", out_txt.name)
            continue

        any_found = True
        text = _pick_text(base_json, summary_json)
        if not text.strip():
            log.info("Empty text for %s; skipping.", base_json.name)
            continue

        log.info("Extracting keywords for %s", base_json.name)
        keywords = _extract_keywords(text)
        if len(keywords) != 5:
            log.warning("Expected 5 keywords for %s, got %d; writing what we have.",
                        base_json.name, len(keywords))

        _write_keywords(out_txt, keywords)
        log.info("✅ Saved keywords: %s", out_txt.name)

    if not any_found:
        log.info("No transcript sets found under %s", root)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Voice2Notion Keywords Extractor")
    parser.add_argument(
        "--in-dir",
        default=os.getenv("TRANSCRIPT_OUTPUT_DIR", "outputs"),
        help="Outputs root (e.g., 'outputs') or a specific transcripts folder.",
    )
    args = parser.parse_args()

    root = Path(args.in_dir)
    if not root.exists():
        log.warning("Input directory not found: %s", root)
        return

    process_root(root)


if __name__ == "__main__":
    main()

