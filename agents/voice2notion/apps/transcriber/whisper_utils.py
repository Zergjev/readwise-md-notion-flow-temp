from __future__ import annotations
from typing import Optional, Dict, Any, List
from faster_whisper import WhisperModel


def load_model(model_name: str = "base", compute_type: str = "int8") -> WhisperModel:
    """
    compute_type options: "int8", "int8_float16", "float16", "float32"
    On GitHub runners (CPU), "int8" is usually fastest with reasonable quality.
    """
    return WhisperModel(model_name, device="cpu", compute_type=compute_type)


def transcribe_file(
    model: WhisperModel,
    audio_path: str,
    *,
    language: Optional[str] = None,
    task: str = "transcribe",  # "transcribe" (original language) or "translate" (to English)
    beam_size: int = 1,
    vad_filter: bool = True,
) -> Dict[str, Any]:
    """
    Returns:
      {
        "text": "...",
        "segments": [{"start": float, "end": float, "text": str}],
        "language": "xx"  # detected or provided,
        "task": "transcribe" | "translate"
      }
    """
    segments_iter, info = model.transcribe(
        audio_path,
        language=language,   # keep None to auto-detect when task="transcribe"
        task=task,           # when "translate", output is English regardless of source
        beam_size=beam_size,
        vad_filter=vad_filter,
    )

    segments: List[Dict[str, Any]] = []
    full_text = []
    for s in segments_iter:
        seg = {"start": float(s.start), "end": float(s.end), "text": s.text}
        segments.append(seg)
        full_text.append(s.text)

    return {
        "text": "".join(full_text).strip(),
        "segments": segments,
        "language": info.language,
        "task": task,
    }
