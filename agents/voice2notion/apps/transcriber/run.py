"""
Voice2Notion — Transcriber (Step 3)

Pipeline per audio file:
  1) Whisper "transcribe" → original-language text (auto-detected).
  2) Whisper "translate"  → English text.
  3) Spanish:
     - If original language is Spanish, reuse original text.
     - Otherwise, translate to Spanish via OpenAI.

Outputs saved under a single per-run folder:
  <stem>-en.txt, <stem>-sp.txt, and <stem>.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import List

from agents.voice2notion.shared.logging_utils import get_logger
from agents.voice2notion.shared.openai_utils import translate_text
from agents.voice2notion.shared.paths import ensure_transcripts_dir
from .whisper_utils import load_model, transcribe_file

AUDIO_EXTS = {".m4a", ".mp3", ".wav", ".aac", ".flac", ".ogg"}

log = get_logger("transcriber")


def find_audio_files(in_dir: Path) -> List[Path]:
    """Return audio files in `in_dir` filtered by known extensions."""
    files: List[Path] = []
    if not in_dir.exists():
        return files
    for p in in_dir.iterdir():
        if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
            files.append(p)
    return sorted(files)


def write_text(path: Path, text: str) -> None:
    """Write UTF-8 text to `path`, ensuring the directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8")


def save_json(path: Path, data: dict) -> None:
    """Write JSON (pretty, UTF-8) to `path`, ensuring the directory exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    """CLI entrypoint for the Transcriber app."""
    parser = argparse.ArgumentParser(description="Voice2Notion Transcriber")
    parser.add_argument(
        "--in-dir",
        default=os.getenv("AUDIO_INPUT_DIR", "downloads/audio"),
        help="Directory containing audio files.",
    )
    parser.add_argument(
        "--out-dir",
        default=os.getenv("TRANSCRIPT_OUTPUT_DIR", "outputs/transcripts"),
        help="Base transcripts directory (per-run folder will be created above).",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("WHISPER_MODEL", "base"),
        help="faster-whisper model size (e.g., tiny, base, small, medium, large).",
    )
    parser.add_argument(
        "--compute-type",
        default=os.getenv("WHISPER_COMPUTE_TYPE", "int8"),
        help="Compute type for faster-whisper (e.g., int8, int8_float16, float16).",
    )
    # language is ignored for English translation (Whisper translate is always English)
    args = parser.parse_args()

    in_dir = Path(args.in_dir)

    # Per-run transcripts folder:
    # - In GitHub Actions, uses GITHUB_RUN_NUMBER → NNN-transcripts
    # - Locally, falls back to scanning and picks the next index
    out_dir = Path(ensure_transcripts_dir())
    log.info("📁 Saving transcripts for this run in: %s", out_dir)

    files = find_audio_files(in_dir)
    if not files:
        log.info("No audio files found in %s", in_dir)
        return

    log.info(
        "Loading faster-whisper model='%s' compute_type='%s'",
        args.model,
        args.compute_type,
    )
    model = load_model(args.model, args.compute_type)

    produced_txts: List[Path] = []
    log.info("Transcribing %d file(s) from %s", len(files), in_dir)

    for audio in files:
        stem = audio.stem
        log.info("Processing: %s", audio.name)

        # 1) Original-language transcript
        orig = transcribe_file(model, str(audio), task="transcribe")
        original_text = orig.get("text", "") or ""
        detected_lang = (orig.get("language") or "").lower()

        # 2) English (Whisper translate => always English)
        en = transcribe_file(model, str(audio), task="translate")
        english_text = en.get("text", "") or original_text

        # 3) Spanish
        if detected_lang == "es":
            spanish_text = original_text
        else:
            # translate to Spanish via OpenAI (requires OPENAI_API_KEY)
            base_text = english_text if detected_lang == "en" else original_text
            spanish_text = translate_text(base_text, target_lang="es")

        # Save outputs
        en_txt = out_dir / f"{stem}-en.txt"
        sp_txt = out_dir / f"{stem}-sp.txt"
        write_text(en_txt, english_text)
        write_text(sp_txt, spanish_text)
        produced_txts.extend([en_txt, sp_txt])

        # Optional structured JSON per audio (for downstream steps)
        save_json(
            out_dir / f"{stem}.json",
            {
                "file_name": audio.name,
                "language_detected": detected_lang,
                "english_text": english_text,
                "spanish_text": spanish_text,
            },
        )

        log.info("Saved: %s, %s", en_txt.name, sp_txt.name)

    log.info("Done.")


if __name__ == "__main__":
    main()
