"""
Shared OpenAI utilities for Voice2Notion agents.

Includes translation and general chat completion helpers.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from openai import OpenAI


def _get_api_key() -> str:
    """
    Return an OpenAI API key from environment.

    Supports both OPENAI_API_KEY (preferred) and OPEN_AI_KEY (fallback).
    """
    key = os.getenv("OPENAI_API_KEY") or os.getenv("OPEN_AI_KEY")
    if not key:
        raise EnvironmentError(
            "Missing OpenAI API key. Set OPENAI_API_KEY or OPEN_AI_KEY."
        )
    return key


def _get_client() -> OpenAI:
    """Create and return an OpenAI API client instance."""
    return OpenAI(api_key=_get_api_key())


def chat_completion(
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = 0.3,
) -> Dict[str, Any]:
    """
    Call the OpenAI Chat Completions API.

    Args:
        model: Name of the model (e.g., 'gpt-4o-mini').
        messages: List of message dicts with 'role' and 'content'.
        temperature: Sampling temperature.

    Returns:
        The raw completion response as a dict.
    """
    client = _get_client()
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    return response.to_dict()


def translate_text(text: str, target_lang: str = "en") -> str:
    """
    Translate a given text using the OpenAI API.

    Args:
        text: The input text.
        target_lang: Target language code (e.g., 'en' or 'es').

    Returns:
        Translated text as a string.
    """
    client = _get_client()
    prompt = f"Translate this text to {target_lang}:\n{text.strip()[:5000]}"
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional translator."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    return response.choices[0].message.content.strip()
