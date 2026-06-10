"""Deterministic text and URL parsing helpers."""

from __future__ import annotations

import re
from typing import List


COUPANG_RE = re.compile(
    r"(?:https?://)?link\.coupang\.com/[^\s\"'<>]+",
    re.IGNORECASE,
)

URL_RE = re.compile(
    r"(?:https?://)?(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/[^\s\"'<>]*)?",
    re.IGNORECASE,
)


def normalize_url_tail(url: str) -> str:
    return url.rstrip(".,)」』]}”’")


def extract_urls(text: str) -> List[str]:
    if not text:
        return []

    urls = [normalize_url_tail(url) for url in URL_RE.findall(text)]
    return [url for url in urls if "." in url]


def extract_coupang_urls(text: str) -> List[str]:
    if not text:
        return []

    urls = [normalize_url_tail(url) for url in COUPANG_RE.findall(text)]
    seen = set()
    result = []

    for url in urls:
        if url not in seen:
            seen.add(url)
            result.append(url)

    return result


def split_text_lines(text: str) -> List[str]:
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not normalized:
        return []
    return normalized.split("\n")
